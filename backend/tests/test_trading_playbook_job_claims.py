import asyncio
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import AsyncMock

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


class _NullAsyncSessionContext:
    async def __aenter__(self):
        return SimpleNamespace()

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class TradingPlaybookJobClaimTests(unittest.IsolatedAsyncioTestCase):
    async def test_complete_persists_exact_result_ids_behind_the_claim_fence(self):
        from app.database import Base
        from app.models.trading_playbook import (
            TradingPlaybookJobClaim,
            TradingPlaybookJobResult,
        )
        from app.services.trading_playbook.job_claim_service import (
            TradingPlaybookClaimToken,
            TradingPlaybookJobClaimService,
        )

        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        maker = async_sessionmaker(engine, expire_on_commit=False)
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        service = TradingPlaybookJobClaimService(lease_seconds=30)
        now = datetime(2026, 7, 13, 15, 30)
        try:
            async with maker() as db:
                token = await service.claim(
                    db,
                    job_key="exact-plan-result",
                    job_type="stage",
                    phase="build",
                    owner="worker-a",
                    now=now,
                )
                self.assertFalse(
                    await service.complete(
                        db,
                        TradingPlaybookClaimToken(
                            token.job_key,
                            "wrong-owner",
                            token.attempt_no,
                        ),
                        now=now,
                        result_entity_type="plan",
                        result_entity_ids=(11,),
                    )
                )
                self.assertEqual(
                    await service.get_completed_result_ids(
                        db,
                        "exact-plan-result",
                        "plan",
                    ),
                    (),
                )
                self.assertTrue(
                    await service.complete(
                        db,
                        token,
                        now=now,
                        result_entity_type="plan",
                        result_entity_ids=(11, 7, 11),
                    )
                )
                self.assertEqual(
                    await service.get_completed_result_ids(
                        db,
                        "exact-plan-result",
                        "plan",
                    ),
                    (7, 11),
                )
                claim = await db.get(TradingPlaybookJobClaim, 1)
                self.assertEqual(claim.status, "completed")
                self.assertEqual(
                    len((await db.execute(TradingPlaybookJobResult.__table__.select())).all()),
                    2,
                )
        finally:
            await engine.dispose()

    async def test_complete_rolls_back_claim_when_result_insert_fails(self):
        from sqlalchemy import event, select

        from app.database import Base
        from app.models.trading_playbook import (
            TradingPlaybookJobClaim,
            TradingPlaybookJobResult,
        )
        from app.services.trading_playbook.job_claim_service import (
            TradingPlaybookJobClaimService,
        )

        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        maker = async_sessionmaker(engine, expire_on_commit=False)
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        service = TradingPlaybookJobClaimService(lease_seconds=30)
        now = datetime(2026, 7, 13, 15, 30)

        def fail_result_insert(
            connection,
            cursor,
            statement,
            parameters,
            context,
            executemany,
        ):
            if "INSERT INTO trading_playbook_job_results" in statement:
                raise RuntimeError("result insert failed")

        try:
            async with maker() as db:
                token = await service.claim(
                    db,
                    job_key="atomic-plan-result",
                    job_type="stage",
                    phase="build",
                    owner="worker-a",
                    now=now,
                )
                event.listen(
                    engine.sync_engine,
                    "before_cursor_execute",
                    fail_result_insert,
                )
                try:
                    with self.assertRaisesRegex(
                        RuntimeError,
                        "result insert failed",
                    ):
                        await service.complete(
                            db,
                            token,
                            now=now,
                            result_entity_type="plan",
                            result_entity_ids=(7,),
                        )
                finally:
                    event.remove(
                        engine.sync_engine,
                        "before_cursor_execute",
                        fail_result_insert,
                    )

            async with maker() as db:
                claim = (
                    await db.execute(
                        select(TradingPlaybookJobClaim).where(
                            TradingPlaybookJobClaim.job_key
                            == "atomic-plan-result"
                        )
                    )
                ).scalar_one()
                self.assertEqual(claim.status, "running")
                self.assertIsNone(claim.completed_at)
                self.assertEqual(
                    (
                        await db.execute(
                            select(TradingPlaybookJobResult).where(
                                TradingPlaybookJobResult.job_key
                                == "atomic-plan-result"
                            )
                        )
                    ).scalars().all(),
                    [],
                )
        finally:
            await engine.dispose()

    async def test_get_status_distinguishes_missing_running_and_completed(self):
        from app.models.trading_playbook import TradingPlaybookJobClaim
        from app.services.trading_playbook.job_claim_service import (
            TradingPlaybookJobClaimService,
        )

        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        maker = async_sessionmaker(engine, expire_on_commit=False)
        async with engine.begin() as connection:
            await connection.run_sync(TradingPlaybookJobClaim.__table__.create)
        service = TradingPlaybookJobClaimService(lease_seconds=30)
        now = datetime(2026, 7, 13, 15, 30)
        try:
            async with maker() as db:
                self.assertIsNone(await service.get_status(db, "missing"))
                token = await service.claim(
                    db,
                    job_key="status-probe",
                    job_type="stage",
                    phase="build",
                    owner="worker-a",
                    now=now,
                )
                self.assertEqual(
                    await service.get_status(db, "status-probe"),
                    "running",
                )
                await service.complete(db, token, now=now)
                self.assertEqual(
                    await service.get_status(db, "status-probe"),
                    "completed",
                )
        finally:
            await engine.dispose()

    async def test_dual_engine_claim_is_single_winner_and_completed_is_terminal(self):
        from app.models.trading_playbook import TradingPlaybookJobClaim
        from app.services.trading_playbook.job_claim_service import (
            TradingPlaybookJobClaimService,
        )

        with TemporaryDirectory() as directory:
            path = Path(directory) / "claims.db"
            url = f"sqlite+aiosqlite:///{path.as_posix()}"
            engines = [
                create_async_engine(url, connect_args={"timeout": 30})
                for _ in range(2)
            ]
            makers = [
                async_sessionmaker(engine, expire_on_commit=False)
                for engine in engines
            ]
            async with engines[0].begin() as connection:
                await connection.run_sync(TradingPlaybookJobClaim.__table__.create)
            service = TradingPlaybookJobClaimService(lease_seconds=30)
            now = datetime(2026, 7, 13, 15, 30)

            async def claim(index):
                async with makers[index]() as db:
                    return await service.claim(
                        db,
                        job_key="build:2026-07-13:2026-07-14:after_close",
                        job_type="stage",
                        phase="build",
                        owner=f"worker-{index}",
                        now=now,
                    )

            try:
                tokens = await asyncio.gather(claim(0), claim(1))
                winners = [token for token in tokens if token is not None]
                self.assertEqual(len(winners), 1)

                async with makers[0]() as db:
                    self.assertTrue(
                        await service.complete(db, winners[0], now=now)
                    )
                async with makers[1]() as db:
                    terminal = await service.claim(
                        db,
                        job_key="build:2026-07-13:2026-07-14:after_close",
                        job_type="stage",
                        phase="build",
                        owner="late-worker",
                        now=now + timedelta(hours=1),
                    )
                self.assertIsNone(terminal)
            finally:
                for engine in engines:
                    await engine.dispose()

    async def test_expired_lease_can_be_taken_over_but_live_lease_cannot(self):
        from app.models.trading_playbook import TradingPlaybookJobClaim
        from app.services.trading_playbook.job_claim_service import (
            TradingPlaybookJobClaimService,
        )

        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        maker = async_sessionmaker(engine, expire_on_commit=False)
        async with engine.begin() as connection:
            await connection.run_sync(TradingPlaybookJobClaim.__table__.create)
        service = TradingPlaybookJobClaimService(lease_seconds=30)
        now = datetime(2026, 7, 13, 15, 30)
        try:
            async with maker() as db:
                first = await service.claim(
                    db,
                    job_key="notify:plan:7",
                    job_type="plan",
                    phase="notify",
                    owner="worker-a",
                    now=now,
                )
            async with maker() as db:
                live = await service.claim(
                    db,
                    job_key="notify:plan:7",
                    job_type="plan",
                    phase="notify",
                    owner="worker-b",
                    now=now + timedelta(seconds=29),
                )
            async with maker() as db:
                takeover = await service.claim(
                    db,
                    job_key="notify:plan:7",
                    job_type="plan",
                    phase="notify",
                    owner="worker-b",
                    now=now + timedelta(seconds=31),
                )

            self.assertIsNotNone(first)
            self.assertIsNone(live)
            self.assertIsNotNone(takeover)
            self.assertEqual(takeover.owner, "worker-b")
            self.assertEqual(takeover.attempt_no, 2)
        finally:
            await engine.dispose()

    async def test_renew_fences_by_owner_and_attempt_then_expiry_allows_takeover(self):
        from app.models.trading_playbook import TradingPlaybookJobClaim
        from app.services.trading_playbook.job_claim_service import (
            TradingPlaybookClaimToken,
            TradingPlaybookJobClaimService,
        )

        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        maker = async_sessionmaker(engine, expire_on_commit=False)
        async with engine.begin() as connection:
            await connection.run_sync(TradingPlaybookJobClaim.__table__.create)
        service = TradingPlaybookJobClaimService(lease_seconds=1)
        now = datetime(2026, 7, 13, 15, 30)
        try:
            async with maker() as db:
                token = await service.claim(
                    db,
                    job_key="slow-build",
                    job_type="stage",
                    phase="build",
                    owner="worker-a",
                    now=now,
                )
            async with maker() as db:
                self.assertFalse(
                    await service.renew(
                        db,
                        TradingPlaybookClaimToken(
                            token.job_key, "wrong-owner", token.attempt_no
                        ),
                        now=now + timedelta(milliseconds=300),
                    )
                )
            async with maker() as db:
                self.assertTrue(
                    await service.renew(
                        db,
                        token,
                        now=now + timedelta(milliseconds=600),
                    )
                )
            async with maker() as db:
                self.assertIsNone(
                    await service.claim(
                        db,
                        job_key="slow-build",
                        job_type="stage",
                        phase="build",
                        owner="worker-b",
                        now=now + timedelta(seconds=1.1),
                    )
                )
            async with maker() as db:
                takeover = await service.claim(
                    db,
                    job_key="slow-build",
                    job_type="stage",
                    phase="build",
                    owner="worker-b",
                    now=now + timedelta(seconds=1.7),
                )
            self.assertEqual(takeover.owner, "worker-b")
            self.assertEqual(takeover.attempt_no, 2)
        finally:
            await engine.dispose()


if __name__ == "__main__":
    unittest.main()


class TradingPlaybookSchedulerClaimTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _calendar():
        from app.services.trading_playbook.calendar_service import (
            TradingCalendarService,
        )

        return TradingCalendarService(
            loader=lambda _start, _end: [
                date(2026, 7, 13),
                date(2026, 7, 14),
            ],
            today_provider=lambda: date(2026, 7, 13),
        )

    async def test_completed_jobs_replay_only_their_exact_persisted_entities(self):
        from app.data_collectors.scheduler import DataScheduler
        from app.database import Base
        from app.models.trading_playbook import (
            TradingExecutionReview,
            TradingExecutionReviewPhaseSnapshot,
            TradingPlanVersion,
            TradingPlaybookJobClaim,
            TradingPlaybookJobResult,
        )
        from app.utils.time_utils import CN_TZ

        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        maker = async_sessionmaker(engine, expire_on_commit=False)
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        now = CN_TZ.localize(datetime(2026, 7, 13, 9, 26))
        built_at = datetime(2026, 7, 13, 9, 26)
        async with maker() as db:
            degraded = TradingPlanVersion(
                source_trade_date=now.date(),
                target_trade_date=now.date(),
                stage="auction",
                version_no=1,
                status="draft",
                input_hash="degraded-v1",
                generated_at=built_at,
            )
            ready = TradingPlanVersion(
                source_trade_date=now.date(),
                target_trade_date=now.date(),
                stage="auction",
                version_no=2,
                status="draft",
                input_hash="ready-v2",
                generated_at=built_at + timedelta(minutes=1),
            )
            db.add_all([degraded, ready])
            await db.flush()
            initial_one = TradingExecutionReview(
                trade_date=now.date(),
                plan_version_id=degraded.id,
                generated_at=datetime(2026, 7, 13, 15, 10),
                finalized_at=datetime(2026, 7, 13, 15, 30),
            )
            initial_two = TradingExecutionReview(
                trade_date=now.date(),
                plan_version_id=ready.id,
                generated_at=datetime(2026, 7, 13, 15, 11),
            )
            db.add_all([initial_one, initial_two])
            await db.flush()
            db.add(
                TradingExecutionReviewPhaseSnapshot(
                    review_id=initial_one.id,
                    phase="initial_review",
                    trade_date=now.date(),
                    plan_version_id=degraded.id,
                    snapshot_json={"phase": "initial_review"},
                    created_at=datetime(2026, 7, 13, 15, 10),
                )
            )
            plan_job_key = (
                "playbook:build:2026-07-13:2026-07-13:auction:forced"
            )
            review_job_key = (
                "playbook:initial_review:2026-07-13:original-generation"
            )
            for job_key, job_type, phase in (
                (plan_job_key, "stage", "build"),
                (review_job_key, "review", "initial_review"),
            ):
                db.add(
                    TradingPlaybookJobClaim(
                        job_key=job_key,
                        job_type=job_type,
                        phase=phase,
                        owner="finished-worker",
                        status="completed",
                        attempt_no=1,
                        completed_at=built_at,
                        created_at=built_at,
                        updated_at=built_at,
                    )
                )
            db.add_all(
                [
                    TradingPlaybookJobResult(
                        job_key=plan_job_key,
                        entity_type="plan",
                        entity_id=degraded.id,
                        created_at=built_at,
                    ),
                    TradingPlaybookJobResult(
                        job_key=review_job_key,
                        entity_type="review",
                        entity_id=initial_one.id,
                        created_at=built_at,
                    ),
                ]
            )
            await db.commit()
            degraded_id = int(degraded.id)
            ready_id = int(ready.id)
            initial_one_id = int(initial_one.id)
            initial_two_id = int(initial_two.id)

        orchestrator = SimpleNamespace(build_stage=AsyncMock())
        notified_ids = []

        async def notify_plan(_db, plan, *, send):
            self.assertTrue(send)
            notified_ids.append(plan.id)

        alert = SimpleNamespace(
            durable_delivery=True,
            notify_plan_ready=notify_plan,
        )
        review = SimpleNamespace(
            generation_key=AsyncMock(return_value="original-generation"),
            build=AsyncMock(),
        )
        coordinator = SimpleNamespace(
            enqueue_stage=AsyncMock(),
            reconcile_committed_facts=AsyncMock(),
            process_due=AsyncMock(),
            startup_reconcile=AsyncMock(),
        )
        scheduler = DataScheduler(
            trading_playbook_orchestrator=orchestrator,
            trading_playbook_alert_service=alert,
            trading_playbook_review_service=review,
            session_factory=maker,
            now_provider=lambda: now,
            calendar_service=self._calendar(),
        )
        scheduler.install_trading_playbook_obsidian_sync(coordinator)
        try:
            replayed_plan = await scheduler._build_trading_playbook_plan(
                "auction",
                degraded=True,
            )
            replayed_review = await scheduler._run_trading_playbook_review_phase(
                now.date(),
                finalized=False,
            )
        finally:
            await engine.dispose()

        self.assertEqual(replayed_plan.id, degraded_id)
        self.assertNotEqual(replayed_plan.id, ready_id)
        self.assertEqual(notified_ids, [degraded_id])
        self.assertIsNone(replayed_review)
        orchestrator.build_stage.assert_not_awaited()
        review.build.assert_not_awaited()
        self.assertEqual(
            coordinator.enqueue_stage.await_args_list,
            [
                unittest.mock.call(
                    now.date(),
                    "auction",
                    plan_version_ids=(degraded_id,),
                    review_ids=(),
                    include_rules=True,
                ),
                unittest.mock.call(
                    now.date(),
                    "initial_review",
                    plan_version_ids=(),
                    review_ids=(initial_one_id,),
                    include_rules=False,
                ),
            ],
        )
        self.assertNotIn(
            ready_id,
            coordinator.enqueue_stage.await_args_list[0].kwargs[
                "plan_version_ids"
            ],
        )
        self.assertNotIn(
            initial_two_id,
            coordinator.enqueue_stage.await_args_list[1].kwargs["review_ids"],
        )

    async def _notification_retry_fairness_case(self, *, valid_is_oldest):
        from app.models.trading_playbook import (
            TradingPlanVersion,
            TradingPlaybookJobClaim,
        )

        scheduler, engine, _orchestrator, alert, _review = (
            await self._scheduler_fixture()
        )
        failing_time = datetime(2026, 7, 2, 9, 0)
        valid_time = datetime(2026, 7, 1, 9, 0)
        if not valid_is_oldest:
            failing_time, valid_time = valid_time, failing_time
        try:
            async with scheduler._playbook_session_factory() as db:
                plans = [
                    TradingPlanVersion(
                        source_trade_date=date(2026, 7, 13),
                        target_trade_date=date(2026, 7, 14),
                        stage="auction",
                        version_no=index + 1,
                        status="draft",
                        input_hash=f"retry-fairness-{index}",
                        generated_at=datetime(2026, 7, 13, 9, 0)
                        + timedelta(seconds=index),
                    )
                    for index in range(101)
                ]
                db.add_all(plans)
                await db.flush()
                valid_plan_id = plans[-1].id
                db.add_all(
                    [
                        TradingPlaybookJobClaim(
                            job_key=f"playbook:notify:plan:{plan.id}",
                            job_type="plan",
                            phase="notify",
                            generation_key=str(plan.id),
                            owner="retired-worker",
                            status="retry",
                            attempt_no=1,
                            lease_expires_at=(
                                valid_time
                                if plan.id == valid_plan_id
                                else failing_time
                            ),
                            last_error="notify offline",
                            created_at=failing_time,
                            updated_at=(
                                valid_time
                                if plan.id == valid_plan_id
                                else failing_time
                            ),
                        )
                        for plan in plans
                    ]
                )
                await db.commit()

            attempted = []

            async def notify(_db, plan, *, send):
                self.assertTrue(send)
                attempted.append(plan.id)
                if plan.id != valid_plan_id:
                    raise RuntimeError("notify offline")

            alert.notify_plan_ready.side_effect = notify
            await scheduler._retry_incomplete_playbook_notifications(None, None)
            valid_attempted_first = valid_plan_id in attempted
            await scheduler._retry_incomplete_playbook_notifications(None, None)
            valid_attempted_second = valid_plan_id in attempted
            return valid_attempted_first, valid_attempted_second
        finally:
            await engine.dispose()

    async def test_oldest_valid_notification_is_not_starved_by_newer_failures(self):
        first, second = await self._notification_retry_fairness_case(
            valid_is_oldest=True
        )

        self.assertTrue(first)
        self.assertTrue(second)

    async def test_newer_valid_notification_runs_after_one_failure_rotation(self):
        first, second = await self._notification_retry_fairness_case(
            valid_is_oldest=False
        )

        self.assertFalse(first)
        self.assertTrue(second)

    async def test_invalid_notification_claims_are_retired_without_touching_other_phases(self):
        from sqlalchemy import select

        from app.models.trading_playbook import (
            TradingPlanVersion,
            TradingPlaybookJobClaim,
        )

        scheduler, engine, _orchestrator, alert, _review = (
            await self._scheduler_fixture()
        )
        old = datetime(2026, 7, 1, 9, 0)
        try:
            async with scheduler._playbook_session_factory() as db:
                expired = TradingPlanVersion(
                    source_trade_date=date(2026, 7, 13),
                    target_trade_date=date(2026, 7, 14),
                    stage="overnight",
                    version_no=1,
                    status="expired",
                    input_hash="expired-notification-plan",
                    generated_at=old,
                )
                db.add(expired)
                await db.flush()
                db.add_all(
                    [
                        TradingPlaybookJobClaim(
                            job_key="playbook:notify:invalid:none",
                            job_type="plan",
                            phase="notify",
                            generation_key=None,
                            owner="retired-worker",
                            status="retry",
                            lease_expires_at=old,
                            created_at=old,
                            updated_at=old,
                        ),
                        TradingPlaybookJobClaim(
                            job_key="playbook:notify:invalid:text",
                            job_type="plan",
                            phase="notify",
                            generation_key="not-an-id",
                            owner="retired-worker",
                            status="retry",
                            lease_expires_at=old,
                            created_at=old,
                            updated_at=old,
                        ),
                        TradingPlaybookJobClaim(
                            job_key="playbook:notify:missing-plan",
                            job_type="plan",
                            phase="notify",
                            generation_key="999999",
                            owner="retired-worker",
                            status="retry",
                            lease_expires_at=old,
                            created_at=old,
                            updated_at=old,
                        ),
                        TradingPlaybookJobClaim(
                            job_key="playbook:notify:expired-plan",
                            job_type="plan",
                            phase="notify",
                            generation_key=str(expired.id),
                            owner="retired-worker",
                            status="retry",
                            lease_expires_at=old,
                            created_at=old,
                            updated_at=old,
                        ),
                        TradingPlaybookJobClaim(
                            job_key="playbook:build:unrelated",
                            job_type="stage",
                            phase="build",
                            generation_key="not-an-id",
                            owner="builder",
                            status="retry",
                            lease_expires_at=old,
                            created_at=old,
                            updated_at=old,
                        ),
                        TradingPlaybookJobClaim(
                            job_key="playbook:notify:already-completed",
                            job_type="plan",
                            phase="notify",
                            generation_key="999998",
                            owner="retired-worker",
                            status="completed",
                            completed_at=old,
                            last_error="preserve-completed",
                            created_at=old,
                            updated_at=old,
                        ),
                    ]
                )
                await db.commit()

            await scheduler._retry_incomplete_playbook_notifications(None, None)
            async with scheduler._playbook_session_factory() as db:
                claims = {
                    claim.job_key: claim
                    for claim in (
                        await db.execute(select(TradingPlaybookJobClaim))
                    )
                    .scalars()
                    .all()
                }
        finally:
            await engine.dispose()

        for key in (
            "playbook:notify:invalid:none",
            "playbook:notify:invalid:text",
            "playbook:notify:missing-plan",
            "playbook:notify:expired-plan",
        ):
            self.assertEqual(claims[key].status, "completed")
            self.assertIn("notification retry retired", claims[key].last_error)
        self.assertEqual(claims["playbook:build:unrelated"].status, "retry")
        self.assertEqual(
            claims["playbook:notify:already-completed"].last_error,
            "preserve-completed",
        )
        alert.notify_plan_ready.assert_not_awaited()

    async def _historical_notification_claim_case(self, *, calendar_failure):
        from sqlalchemy import select

        from app.models.trading_playbook import (
            TradingPlanVersion,
            TradingPlaybookJobClaim,
        )
        from app.services.trading_playbook.calendar_service import (
            TradingCalendarLookupError,
            TradingCalendarService,
        )
        from app.utils.time_utils import CN_TZ

        scheduler, engine, _orchestrator, alert, _review = (
            await self._scheduler_fixture()
        )
        current = [CN_TZ.localize(datetime(2026, 7, 14, 10, 5))]
        scheduler._playbook_now_provider = lambda: current[0]
        scheduler._playbook_calendar = TradingCalendarService(
            loader=lambda _start, _end: [
                date(2026, 7, 14),
                date(2026, 7, 15),
            ],
            today_provider=lambda: date(2026, 7, 14),
        )
        if calendar_failure:
            scheduler._ensure_playbook_calendar = AsyncMock(
                side_effect=TradingCalendarLookupError(
                    "calendar unavailable"
                )
            )
        try:
            old = datetime(2026, 6, 2, 9, 0)
            async with scheduler._playbook_session_factory() as db:
                plan = TradingPlanVersion(
                    source_trade_date=date(2026, 6, 1),
                    target_trade_date=date(2026, 6, 2),
                    stage="overnight",
                    version_no=1,
                    status="active",
                    input_hash=(
                        "historical-calendar-failure"
                        if calendar_failure
                        else "historical-calendar-healthy"
                    ),
                    generated_at=old,
                )
                db.add(plan)
                await db.flush()
                db.add(
                    TradingPlaybookJobClaim(
                        job_key=f"playbook:notify:plan:{plan.id}",
                        job_type="plan",
                        phase="notify",
                        generation_key=str(plan.id),
                        owner="stopped-worker",
                        status="retry",
                        attempt_no=1,
                        lease_expires_at=old,
                        last_error="notify interrupted",
                        created_at=old,
                        updated_at=old,
                    )
                )
                await db.commit()
                plan_id = plan.id

            await scheduler._monitor_trading_playbook()
            async with scheduler._playbook_session_factory() as db:
                first = (
                    await db.execute(
                        select(TradingPlaybookJobClaim).where(
                            TradingPlaybookJobClaim.generation_key
                            == str(plan_id),
                            TradingPlaybookJobClaim.phase == "notify",
                        )
                    )
                ).scalar_one()
                first_updated_at = first.updated_at

            current[0] += timedelta(seconds=3)
            await scheduler._monitor_trading_playbook()
            async with scheduler._playbook_session_factory() as db:
                second = (
                    await db.execute(
                        select(TradingPlaybookJobClaim).where(
                            TradingPlaybookJobClaim.generation_key
                            == str(plan_id),
                            TradingPlaybookJobClaim.phase == "notify",
                        )
                    )
                ).scalar_one()

            self.assertEqual(second.status, "completed")
            self.assertIn("stale target date", second.last_error)
            self.assertEqual(second.updated_at, first_updated_at)
            alert.notify_plan_ready.assert_not_awaited()
        finally:
            await engine.dispose()

    async def test_healthy_calendar_terminalizes_historical_notification_once(self):
        await self._historical_notification_claim_case(
            calendar_failure=False
        )

    async def test_calendar_failure_terminalizes_historical_notification_once(self):
        await self._historical_notification_claim_case(
            calendar_failure=True
        )

    async def test_broken_business_sessions_fail_claims_in_fresh_sessions(self):
        from sqlalchemy import select
        from sqlalchemy.exc import IntegrityError
        from sqlalchemy.pool import NullPool

        from app.data_collectors.scheduler import DataScheduler
        from app.database import Base
        from app.models.trading_playbook import (
            TradingPlanVersion,
            TradingPlaybookJobClaim,
        )
        from app.utils.time_utils import CN_TZ

        with TemporaryDirectory() as directory:
            path = Path(directory) / "broken-phases.db"
            engine = create_async_engine(
                f"sqlite+aiosqlite:///{path.as_posix()}",
                poolclass=NullPool,
            )
            maker = async_sessionmaker(engine, expire_on_commit=False)
            async with engine.begin() as connection:
                await connection.run_sync(Base.metadata.create_all)
            async with maker() as db:
                plan = TradingPlanVersion(
                    source_trade_date=date(2026, 7, 13),
                    target_trade_date=date(2026, 7, 14),
                    stage="after_close",
                    version_no=1,
                    status="draft",
                    input_hash="existing-plan",
                    generated_at=datetime(2026, 7, 13, 15, 30),
                )
                db.add(plan)
                await db.commit()
                await db.refresh(plan)

            async def poison(db):
                db.add(
                    TradingPlanVersion(
                        source_trade_date=date(2026, 7, 13),
                        target_trade_date=date(2026, 7, 14),
                        stage="after_close",
                        version_no=1,
                        status="draft",
                        input_hash="duplicate-plan",
                        generated_at=datetime(2026, 7, 13, 15, 31),
                    )
                )
                await db.commit()

            class BrokenOrchestrator:
                async def build_stage(self, db, *_args, **_kwargs):
                    await poison(db)

            class BrokenAlert:
                durable_delivery = True

                async def notify_plan_ready(self, db, *_args, **_kwargs):
                    await poison(db)

            class BrokenReview:
                async def build(self, db, *_args, **_kwargs):
                    await poison(db)

            now = CN_TZ.localize(datetime(2026, 7, 13, 15, 35))
            scheduler = DataScheduler(
                trading_playbook_orchestrator=BrokenOrchestrator(),
                trading_playbook_alert_service=BrokenAlert(),
                trading_playbook_review_service=BrokenReview(),
                session_factory=maker,
                now_provider=lambda: now,
                calendar_service=self._calendar(),
            )
            try:
                try:
                    await scheduler._build_trading_playbook_plan(
                        "after_close",
                        send_notifications=False,
                    )
                except IntegrityError:
                    pass
                else:
                    self.fail("broken build commit must preserve IntegrityError")
                self.assertIsNone(
                    await scheduler._notify_trading_playbook_plan(plan)
                )
                self.assertIsNone(
                    await scheduler._run_trading_playbook_review_phase(
                        date(2026, 7, 13),
                        finalized=False,
                    )
                )

                async with maker() as db:
                    claims = list(
                        (
                            await db.execute(
                                select(TradingPlaybookJobClaim).order_by(
                                    TradingPlaybookJobClaim.id
                                )
                            )
                        )
                        .scalars()
                        .all()
                    )
            finally:
                await engine.dispose()
                # Release traceback-held aiosqlite adapters before Windows
                # removes the temporary database file.
                import gc

                gc.collect()
                await asyncio.sleep(0.05)

        self.assertEqual(
            {claim.phase: claim.status for claim in claims},
            {
                "build": "retry",
                "notify": "retry",
                "initial_review": "retry",
            },
        )

    async def test_two_schedulers_run_build_notify_and_finalize_exactly_once(self):
        from app.data_collectors.scheduler import DataScheduler
        from app.models.trading_playbook import (
            TradingExecutionReview,
            TradingPlanVersion,
            TradingPlaybookJobClaim,
            TradingPlaybookJobResult,
        )
        from app.utils.time_utils import CN_TZ

        with TemporaryDirectory() as directory:
            path = Path(directory) / "scheduler-claims.db"
            url = f"sqlite+aiosqlite:///{path.as_posix()}"
            engines = [
                create_async_engine(url, connect_args={"timeout": 30})
                for _ in range(2)
            ]
            makers = [
                async_sessionmaker(engine, expire_on_commit=False)
                for engine in engines
            ]
            async with engines[0].begin() as connection:
                for table in (
                    TradingPlanVersion.__table__,
                    TradingExecutionReview.__table__,
                    TradingPlaybookJobClaim.__table__,
                    TradingPlaybookJobResult.__table__,
                ):
                    await connection.run_sync(table.create)

            build_calls = 0

            class Orchestrator:
                async def build_stage(self, *_args, **_kwargs):
                    nonlocal build_calls
                    build_calls += 1
                    await asyncio.sleep(0.03)
                    return SimpleNamespace(
                        id=9,
                        source_trade_date=date(2026, 7, 13),
                        target_trade_date=date(2026, 7, 14),
                        stage="after_close",
                    )

            alert = SimpleNamespace(notify_plan_ready=AsyncMock())
            review = SimpleNamespace(
                build=AsyncMock(),
                generation_key=AsyncMock(return_value="stable-plan-set"),
            )
            now = CN_TZ.localize(datetime(2026, 7, 13, 15, 30))
            schedulers = [
                DataScheduler(
                    trading_playbook_orchestrator=Orchestrator(),
                    trading_playbook_alert_service=alert,
                    trading_playbook_review_service=review,
                    session_factory=makers[index],
                    now_provider=lambda: now,
                    calendar_service=self._calendar(),
                )
                for index in range(2)
            ]
            for scheduler in schedulers:
                scheduler._wait_for_trading_playbook_data = AsyncMock(
                    return_value=True
                )
            try:
                await asyncio.gather(
                    *(scheduler._build_trading_playbook_after_close() for scheduler in schedulers)
                )
            finally:
                for engine in engines:
                    await engine.dispose()

        self.assertEqual(build_calls, 1)
        self.assertEqual(alert.notify_plan_ready.await_count, 1)
        self.assertEqual(review.build.await_count, 1)

    async def test_notification_failure_does_not_prevent_final_review(self):
        scheduler, engine, orchestrator, alert, review = await self._scheduler_fixture(
            notify_side_effect=RuntimeError("notify offline")
        )
        try:
            await scheduler._build_trading_playbook_after_close()
        finally:
            await engine.dispose()

        self.assertEqual(orchestrator.calls, 1)
        self.assertEqual(alert.notify_plan_ready.await_count, 1)
        self.assertEqual(review.build.await_count, 1)

    async def test_calendar_failure_retries_only_existing_failed_notification_claim(self):
        from sqlalchemy import select

        from app.data_collectors.scheduler import (
            DataScheduler,
            TradingCalendarLookupError,
        )
        from app.models.trading_playbook import (
            TradingPlanVersion,
            TradingPlaybookJobClaim,
        )
        from app.utils.time_utils import CN_TZ

        scheduler, engine, orchestrator, alert, review = await self._scheduler_fixture(
            notify_side_effect=RuntimeError("notify offline")
        )
        try:
            await scheduler._build_trading_playbook_after_close()
            async with scheduler._playbook_session_factory() as db:
                historical = TradingPlanVersion(
                    source_trade_date=date(2026, 7, 13),
                    target_trade_date=date(2026, 7, 14),
                    stage="overnight",
                    version_no=1,
                    status="draft",
                    input_hash="historical-no-notify-claim",
                    generated_at=datetime(2026, 7, 13, 8, 50),
                )
                db.add(historical)
                await db.flush()
                historical_plan_id = historical.id
                old_claim_time = datetime(2026, 7, 1, 9, 0)
                db.add_all(
                    [
                        TradingPlaybookJobClaim(
                            job_key=f"playbook:notify:plan:{10000 + index}",
                            job_type="plan",
                            phase="notify",
                            generation_key=str(10000 + index),
                            owner="retired-worker",
                            status="retry",
                            attempt_no=1,
                            lease_expires_at=old_claim_time,
                            last_error="plan superseded",
                            created_at=old_claim_time,
                            updated_at=old_claim_time,
                        )
                        for index in range(100)
                    ]
                )
                await db.commit()

            alert.notify_plan_ready.side_effect = None
            restarted = DataScheduler(
                trading_playbook_alert_service=alert,
                session_factory=scheduler._playbook_session_factory,
                now_provider=lambda: CN_TZ.localize(
                    datetime(2026, 7, 14, 8, 50)
                ),
                calendar_service=self._calendar(),
            )
            restarted._ensure_playbook_calendar = AsyncMock(
                side_effect=TradingCalendarLookupError(
                    "calendar unavailable"
                )
            )
            await restarted._monitor_trading_playbook()
            async with restarted._playbook_session_factory() as db:
                first_round_claims = list(
                    (
                        await db.execute(
                            select(TradingPlaybookJobClaim).where(
                                TradingPlaybookJobClaim.phase == "notify"
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
            self.assertEqual(
                len(
                    [
                        claim
                        for claim in first_round_claims
                        if claim.status == "completed"
                    ]
                ),
                100,
            )
            self.assertEqual(alert.notify_plan_ready.await_count, 1)
            await restarted._monitor_trading_playbook()
            await restarted._monitor_trading_playbook()
            async with restarted._playbook_session_factory() as db:
                notify_claims = list(
                    (
                        await db.execute(
                            select(TradingPlaybookJobClaim).where(
                                TradingPlaybookJobClaim.phase == "notify"
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
        finally:
            await engine.dispose()

        self.assertEqual(orchestrator.calls, 1)
        self.assertEqual(alert.notify_plan_ready.await_count, 2)
        completed_claims = [
            claim for claim in notify_claims if claim.status == "completed"
        ]
        self.assertEqual(len(completed_claims), 101)
        retired = [
            claim
            for claim in notify_claims
            if (claim.generation_key or "").startswith("10")
        ]
        self.assertEqual(len(retired), 100)
        self.assertTrue(
            all(
                "notification retry retired" in (claim.last_error or "")
                for claim in retired
            )
        )
        self.assertNotIn(
            str(historical_plan_id),
            {claim.generation_key for claim in notify_claims},
        )

    async def test_targeted_finalization_keeps_plan_ids_in_claim_and_build(self):
        scheduler, engine, orchestrator, alert, review = await self._scheduler_fixture()
        try:
            await scheduler._finalize_trading_playbook_review(
                date(2026, 7, 13),
                plan_version_id=101,
            )
            await scheduler._finalize_trading_playbook_review(
                date(2026, 7, 13),
                plan_version_id=202,
            )
        finally:
            await engine.dispose()

        self.assertEqual(review.build.await_count, 2)
        self.assertEqual(
            review.build.await_args_list,
            [
                unittest.mock.call(
                    unittest.mock.ANY,
                    date(2026, 7, 13),
                    finalized=True,
                    plan_version_id=101,
                ),
                unittest.mock.call(
                    unittest.mock.ANY,
                    date(2026, 7, 13),
                    finalized=True,
                    plan_version_id=202,
                ),
            ],
        )

    async def test_new_plan_set_after_completed_final_claim_runs_one_compensation(self):
        scheduler, engine, _orchestrator, _alert, review = (
            await self._scheduler_fixture()
        )
        review.generation_key = AsyncMock(
            side_effect=["empty-plan-set", "plan-set-101", "plan-set-101"]
        )
        try:
            await scheduler._finalize_trading_playbook_review(
                date(2026, 7, 13)
            )
            await scheduler._finalize_trading_playbook_review(
                date(2026, 7, 13)
            )
            await scheduler._finalize_trading_playbook_review(
                date(2026, 7, 13)
            )
        finally:
            await engine.dispose()

        self.assertEqual(review.generation_key.await_count, 3)
        self.assertEqual(review.build.await_count, 2)

    async def test_monitor_retries_failed_finalization_without_rebuild_or_renotify(self):
        scheduler, engine, orchestrator, alert, review = await self._scheduler_fixture(
            review_side_effect=[RuntimeError("review offline"), None]
        )
        try:
            await scheduler._build_trading_playbook_after_close()
            await scheduler._monitor_trading_playbook()
        finally:
            await engine.dispose()

        self.assertEqual(orchestrator.calls, 1)
        self.assertEqual(alert.notify_plan_ready.await_count, 1)
        self.assertEqual(review.build.await_count, 2)

    async def test_validated_mapping_dates_are_coerced_before_notification_claim(self):
        from app.data_collectors.scheduler import DataScheduler
        from app.models.trading_playbook import TradingPlaybookJobClaim
        from app.utils.time_utils import CN_TZ

        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        maker = async_sessionmaker(engine, expire_on_commit=False)
        async with engine.begin() as connection:
            await connection.run_sync(TradingPlaybookJobClaim.__table__.create)
        alert = SimpleNamespace(notify_plan_ready=AsyncMock())
        scheduler = DataScheduler(
            trading_playbook_alert_service=alert,
            session_factory=maker,
            now_provider=lambda: CN_TZ.localize(datetime(2026, 7, 13, 15, 30)),
        )
        plan = {
            "id": 77,
            "source_trade_date": "2026-07-13",
            "target_trade_date": "2026-07-14",
            "stage": "after_close",
        }
        try:
            await scheduler._notify_trading_playbook_plan(plan)
            await scheduler._notify_trading_playbook_plan(plan)
            async with maker() as db:
                claim = (
                    await db.execute(
                        __import__("sqlalchemy").select(TradingPlaybookJobClaim)
                    )
                ).scalar_one()
        finally:
            await engine.dispose()

        self.assertEqual(alert.notify_plan_ready.await_count, 1)
        self.assertEqual(claim.status, "completed")
        self.assertEqual(claim.source_trade_date, date(2026, 7, 13))

    async def test_slow_notification_renews_lease_and_runs_once(self):
        from app.data_collectors.scheduler import DataScheduler
        from app.models.trading_playbook import TradingPlaybookJobClaim
        from app.services.trading_playbook.job_claim_service import (
            TradingPlaybookJobClaimService,
        )
        from app.utils.time_utils import CN_TZ

        with TemporaryDirectory() as directory:
            path = Path(directory) / "slow-notify.db"
            url = f"sqlite+aiosqlite:///{path.as_posix()}"
            engines = [create_async_engine(url) for _ in range(2)]
            makers = [
                async_sessionmaker(engine, expire_on_commit=False)
                for engine in engines
            ]
            async with engines[0].begin() as connection:
                await connection.run_sync(TradingPlaybookJobClaim.__table__.create)
            notify_calls = 0

            async def notify(*_args, **_kwargs):
                nonlocal notify_calls
                notify_calls += 1
                await asyncio.sleep(2.0)

            alert = SimpleNamespace(notify_plan_ready=notify)
            schedulers = [
                DataScheduler(
                    trading_playbook_alert_service=alert,
                    session_factory=makers[index],
                    now_provider=lambda: datetime.now(CN_TZ),
                    job_claim_service=TradingPlaybookJobClaimService(
                        lease_seconds=1
                    ),
                )
                for index in range(2)
            ]
            plan = SimpleNamespace(
                id=88,
                source_trade_date=date(2026, 7, 13),
                target_trade_date=date(2026, 7, 14),
                stage="after_close",
            )
            try:
                first = asyncio.create_task(
                    schedulers[0]._notify_trading_playbook_plan(plan)
                )
                await asyncio.sleep(1.05)
                second = asyncio.create_task(
                    schedulers[1]._notify_trading_playbook_plan(plan)
                )
                await asyncio.gather(first, second)
            finally:
                for engine in engines:
                    await engine.dispose()

        self.assertEqual(notify_calls, 1)

    async def test_slow_build_renews_lease_and_runs_once(self):
        from app.data_collectors.scheduler import DataScheduler
        from app.models.trading_playbook import (
            TradingPlanVersion,
            TradingPlaybookJobClaim,
            TradingPlaybookJobResult,
        )
        from app.services.trading_playbook.job_claim_service import (
            TradingPlaybookJobClaimService,
        )
        from app.utils.time_utils import CN_TZ

        with TemporaryDirectory() as directory:
            path = Path(directory) / "slow-build.db"
            url = f"sqlite+aiosqlite:///{path.as_posix()}"
            engines = [create_async_engine(url) for _ in range(2)]
            makers = [
                async_sessionmaker(engine, expire_on_commit=False)
                for engine in engines
            ]
            async with engines[0].begin() as connection:
                await connection.run_sync(TradingPlanVersion.__table__.create)
                await connection.run_sync(TradingPlaybookJobClaim.__table__.create)
                await connection.run_sync(TradingPlaybookJobResult.__table__.create)
            build_calls = 0

            class SlowOrchestrator:
                async def build_stage(self, *_args, **_kwargs):
                    nonlocal build_calls
                    build_calls += 1
                    await asyncio.sleep(1.3)
                    return SimpleNamespace(
                        id=99,
                        source_trade_date=date(2026, 7, 13),
                        target_trade_date=date(2026, 7, 14),
                        stage="after_close",
                    )

            started = asyncio.get_running_loop().time()

            def now():
                elapsed = asyncio.get_running_loop().time() - started
                return CN_TZ.localize(datetime(2026, 7, 13, 15, 30)) + timedelta(
                    seconds=elapsed
                )

            schedulers = [
                DataScheduler(
                    trading_playbook_orchestrator=SlowOrchestrator(),
                    session_factory=makers[index],
                    now_provider=now,
                    calendar_service=self._calendar(),
                    job_claim_service=TradingPlaybookJobClaimService(
                        lease_seconds=1
                    ),
                )
                for index in range(2)
            ]
            try:
                first = asyncio.create_task(
                    schedulers[0]._build_trading_playbook_plan(
                        "after_close",
                        send_notifications=False,
                    )
                )
                await asyncio.sleep(1.05)
                second = asyncio.create_task(
                    schedulers[1]._build_trading_playbook_plan(
                        "after_close",
                        send_notifications=False,
                    )
                )
                await asyncio.gather(first, second)
            finally:
                for engine in engines:
                    await engine.dispose()

        self.assertEqual(build_calls, 1)

    async def test_lost_heartbeat_cancels_work_and_fences_downstream_notification(self):
        from app.data_collectors.scheduler import DataScheduler
        from app.services.trading_playbook.job_claim_service import (
            TradingPlaybookClaimToken,
        )
        from app.utils.time_utils import CN_TZ

        cancelled = asyncio.Event()

        class LostClaimService:
            lease_seconds = 1

            async def claim(self, _db, **kwargs):
                return TradingPlaybookClaimToken(
                    kwargs["job_key"], kwargs["owner"], 1
                )

            async def renew(self, *_args, **_kwargs):
                return False

            async def fail(self, *_args, **_kwargs):
                return False

            async def complete(self, *_args, **_kwargs):
                raise AssertionError("lost claim must not complete")

        class SlowOrchestrator:
            async def build_stage(self, *_args, **_kwargs):
                try:
                    await asyncio.sleep(5)
                except asyncio.CancelledError:
                    cancelled.set()
                    raise

        scheduler = DataScheduler(
            trading_playbook_orchestrator=SlowOrchestrator(),
            trading_playbook_alert_service=SimpleNamespace(
                notify_plan_ready=AsyncMock()
            ),
            session_factory=lambda: _NullAsyncSessionContext(),
            now_provider=lambda: CN_TZ.localize(datetime(2026, 7, 13, 15, 30)),
            calendar_service=self._calendar(),
            job_claim_service=LostClaimService(),
        )

        result = await scheduler._build_trading_playbook_plan("after_close")

        self.assertIsNone(result)
        self.assertTrue(cancelled.is_set())
        scheduler._trading_playbook_alert_service.notify_plan_ready.assert_not_awaited()

    async def _scheduler_fixture(
        self,
        *,
        notify_side_effect=None,
        review_side_effect=None,
    ):
        from app.data_collectors.scheduler import DataScheduler
        from app.models.trading_playbook import (
            TradingExecutionReview,
            TradingPlanVersion,
            TradingPlaybookJobClaim,
            TradingPlaybookJobResult,
        )
        from app.utils.time_utils import CN_TZ

        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        maker = async_sessionmaker(engine, expire_on_commit=False)
        async with engine.begin() as connection:
            for table in (
                TradingPlanVersion.__table__,
                TradingExecutionReview.__table__,
                TradingPlaybookJobClaim.__table__,
                TradingPlaybookJobResult.__table__,
            ):
                await connection.run_sync(table.create)

        class PersistingOrchestrator:
            def __init__(self):
                self.calls = 0

            async def build_stage(
                self,
                db,
                source_trade_date,
                stage,
                as_of,
                degraded=False,
                **_kwargs,
            ):
                self.calls += 1
                plan = TradingPlanVersion(
                    source_trade_date=source_trade_date,
                    target_trade_date=date(2026, 7, 14),
                    stage=stage,
                    version_no=self.calls,
                    status="draft",
                    market_state_json={},
                    theme_ranking_json=[],
                    mode_radar_json=[],
                    rule_snapshot_json=[],
                    risk_settings_json={},
                    data_quality_json={"status": "ready", "forced_degraded": False},
                    change_summary_json={},
                    input_hash=f"plan-{self.calls}",
                    generated_at=as_of.replace(tzinfo=None),
                )
                db.add(plan)
                await db.commit()
                await db.refresh(plan)
                return plan

        orchestrator = PersistingOrchestrator()
        notify = AsyncMock(side_effect=notify_side_effect)
        alert = SimpleNamespace(
            notify_plan_ready=notify,
            monitor=AsyncMock(),
        )
        review = SimpleNamespace(
            build=AsyncMock(side_effect=review_side_effect),
            generation_key=AsyncMock(
                side_effect=lambda _db, _trade_date, *, plan_version_id=None: (
                    f"fixture-plan-set:{plan_version_id or 'all'}"
                )
            ),
        )
        now = CN_TZ.localize(datetime(2026, 7, 13, 15, 35))
        scheduler = DataScheduler(
            trading_playbook_orchestrator=orchestrator,
            trading_playbook_alert_service=alert,
            trading_playbook_review_service=review,
            session_factory=maker,
            now_provider=lambda: now,
            calendar_service=self._calendar(),
        )
        scheduler._wait_for_trading_playbook_data = AsyncMock(return_value=True)
        return scheduler, engine, orchestrator, alert, review
