import asyncio
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import AsyncMock

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


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
