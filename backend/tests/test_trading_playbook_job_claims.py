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
            review = SimpleNamespace(build=AsyncMock())
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

    async def test_restart_retries_only_existing_failed_notification_claim(self):
        from app.data_collectors.scheduler import DataScheduler
        from app.models.trading_playbook import TradingPlanVersion
        from app.utils.time_utils import CN_TZ

        scheduler, engine, orchestrator, alert, review = await self._scheduler_fixture(
            notify_side_effect=RuntimeError("notify offline")
        )
        try:
            await scheduler._build_trading_playbook_after_close()
            async with scheduler._playbook_session_factory() as db:
                db.add(
                    TradingPlanVersion(
                        source_trade_date=date(2026, 7, 13),
                        target_trade_date=date(2026, 7, 14),
                        stage="overnight",
                        version_no=1,
                        status="draft",
                        input_hash="historical-no-notify-claim",
                        generated_at=datetime(2026, 7, 13, 8, 50),
                    )
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
            await restarted._retry_incomplete_playbook_notifications(
                date(2026, 7, 14),
                date(2026, 7, 14),
            )
            await restarted._retry_incomplete_playbook_notifications(
                date(2026, 7, 14),
                date(2026, 7, 14),
            )
        finally:
            await engine.dispose()

        self.assertEqual(orchestrator.calls, 1)
        self.assertEqual(alert.notify_plan_ready.await_count, 2)

    async def test_multiple_after_close_plan_ids_finalize_trade_date_once(self):
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

        self.assertEqual(review.build.await_count, 1)
        review.build.assert_awaited_once_with(
            unittest.mock.ANY,
            date(2026, 7, 13),
            finalized=True,
        )

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
        )
        from app.utils.time_utils import CN_TZ

        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        maker = async_sessionmaker(engine, expire_on_commit=False)
        async with engine.begin() as connection:
            for table in (
                TradingPlanVersion.__table__,
                TradingExecutionReview.__table__,
                TradingPlaybookJobClaim.__table__,
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
            build=AsyncMock(side_effect=review_side_effect)
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
