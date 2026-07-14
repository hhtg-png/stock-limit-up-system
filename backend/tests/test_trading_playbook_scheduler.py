import asyncio
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.data_collectors.scheduler import (
    DataScheduler,
    TradingCalendarLookupError,
)
from app.models.market_review import MarketReviewDailyMetric
from app.models.trading_playbook import (
    TradingExecutionReview,
    TradingPlanVersion,
    TradingPlaybookJobClaim,
)
from app.utils.time_utils import CN_TZ


class FakeScheduler:
    def __init__(self):
        self.jobs = []
        self.started = False
        self.shutdown_called = False

    def add_job(self, func, trigger, **kwargs):
        self.jobs.append({"func": func, "trigger": trigger, **kwargs})

    def start(self):
        self.started = True

    def shutdown(self):
        self.shutdown_called = True


class AsyncSessionContext:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class ScalarResult:
    def __init__(self, value):
        self.value = value

    def scalar_one_or_none(self):
        return self.value


class InMemoryClaimService:
    """Deterministic phase claim fake for tests that use mock DB sessions."""

    def __init__(self):
        self.active = set()
        self.completed = set()

    async def claim(self, _db, *, job_key, owner, **_kwargs):
        if job_key in self.active or job_key in self.completed:
            return None
        self.active.add(job_key)
        return SimpleNamespace(
            job_key=job_key,
            owner=owner,
            attempt_no=1,
        )

    async def complete(self, _db, token, **_kwargs):
        self.active.discard(token.job_key)
        self.completed.add(token.job_key)
        return True

    async def fail(self, _db, token, **_kwargs):
        self.active.discard(token.job_key)
        return True


class TradingPlaybookSchedulerRegistrationTests(unittest.TestCase):
    def _scheduler(self):
        scheduler = DataScheduler()
        scheduler.scheduler = FakeScheduler()
        return scheduler

    def test_start_registers_all_playbook_jobs_with_exact_china_times(self):
        scheduler = self._scheduler()

        with patch(
            "app.data_collectors.scheduler.settings.TRADING_PLAYBOOK_ENABLED",
            True,
        ), patch(
            "app.data_collectors.scheduler.settings.TRADING_PLAYBOOK_MONITOR_INTERVAL_SECONDS",
            7,
        ):
            scheduler.start()

        jobs = {job["id"]: job for job in scheduler.scheduler.jobs}
        expected = {
            "trading_playbook_preclose": (14, 40, 19 * 60),
            "trading_playbook_review": (15, 10, 19 * 60),
            "trading_playbook_after_close": (15, 30, 30 * 60),
            "trading_playbook_overnight": (8, 50, 35 * 60),
            "trading_playbook_auction": (9, 26, 3 * 60),
        }
        for job_id, (hour, minute, misfire_seconds) in expected.items():
            with self.subTest(job_id=job_id):
                job = jobs[job_id]
                trigger = job["trigger"]
                self.assertEqual(str(trigger.fields[5]), str(hour))
                self.assertEqual(str(trigger.fields[6]), str(minute))
                self.assertIs(trigger.timezone, CN_TZ)
                self.assertEqual(job["max_instances"], 1)
                self.assertTrue(job["coalesce"])
                self.assertEqual(job["misfire_grace_time"], misfire_seconds)

        monitor = jobs["trading_playbook_monitor"]
        self.assertEqual(monitor["trigger"].interval.total_seconds(), 7)
        self.assertIs(monitor["trigger"].timezone, CN_TZ)
        self.assertEqual(monitor["max_instances"], 1)
        self.assertTrue(monitor["coalesce"])
        self.assertEqual(monitor["misfire_grace_time"], 21)
        catchup = jobs["trading_playbook_startup_catchup"]
        self.assertTrue(catchup["coalesce"])
        self.assertEqual(catchup["misfire_grace_time"], 60)
        self.assertEqual(len(jobs), len(set(jobs)))

    def test_start_is_idempotent_and_disabled_mode_registers_no_playbook_jobs(self):
        scheduler = self._scheduler()
        with patch(
            "app.data_collectors.scheduler.settings.TRADING_PLAYBOOK_ENABLED",
            False,
        ):
            scheduler.start()
            first_count = len(scheduler.scheduler.jobs)
            scheduler.start()

        self.assertEqual(len(scheduler.scheduler.jobs), first_count)
        self.assertFalse(
            any(
                job["id"].startswith("trading_playbook_")
                for job in scheduler.scheduler.jobs
            )
        )


class TradingPlaybookSchedulerStageTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.now = datetime(2026, 7, 13, 15, 30, tzinfo=CN_TZ)
        self.db = MagicMock()
        self.orchestrator = SimpleNamespace(build_stage=AsyncMock(return_value={"id": 9}))
        self.scheduler = DataScheduler(
            trading_playbook_orchestrator=self.orchestrator,
            session_factory=lambda: AsyncSessionContext(self.db),
            now_provider=lambda: self.now,
            sleep=AsyncMock(),
            job_claim_service=InMemoryClaimService(),
        )
        self.scheduler._playbook_review_exists = AsyncMock(return_value=False)

    async def test_build_plan_uses_aware_china_now_same_session_and_notifies_when_installed(self):
        alert_service = SimpleNamespace(notify_plan_ready=AsyncMock())
        self.scheduler.install_trading_playbook_alert_service(alert_service)

        with patch(
            "app.data_collectors.scheduler._get_cn_trading_dates",
            return_value=[self.now.date(), self.now.date() + timedelta(days=1)],
        ):
            result = await self.scheduler._build_trading_playbook_plan(
                "after_close",
                degraded=True,
            )

        self.assertEqual(result, {"id": 9})
        self.orchestrator.build_stage.assert_awaited_once_with(
            self.db,
            self.now.date(),
            "after_close",
            self.now,
            degraded=True,
        )
        alert_service.notify_plan_ready.assert_awaited_once_with(
            self.db,
            {"id": 9},
            send=True,
        )

    async def test_build_plan_skips_non_trading_day_and_never_notifies_backfill(self):
        alert_service = SimpleNamespace(notify_plan_ready=AsyncMock())
        self.scheduler.install_trading_playbook_alert_service(alert_service)

        with patch(
            "app.data_collectors.scheduler._get_cn_trading_dates",
            return_value=[],
        ):
            result = await self.scheduler._build_trading_playbook_plan("after_close")

        self.assertIsNone(result)
        self.orchestrator.build_stage.assert_not_awaited()
        alert_service.notify_plan_ready.assert_not_awaited()

        with patch(
            "app.data_collectors.scheduler._get_cn_trading_dates",
            return_value=[self.now.date(), self.now.date() + timedelta(days=1)],
        ):
            await self.scheduler._build_trading_playbook_plan(
                "after_close",
                send_notifications=False,
            )
        alert_service.notify_plan_ready.assert_not_awaited()

    async def test_unconfigured_future_services_are_controlled_noops(self):
        with patch(
            "app.data_collectors.scheduler._get_cn_trading_dates",
            return_value=[self.now.date()],
        ):
            await self.scheduler._review_trading_playbook()
            await self.scheduler._finalize_trading_playbook_review()
            await self.scheduler._monitor_trading_playbook()

    async def test_configured_review_and_monitor_services_receive_same_db_and_now(self):
        review = SimpleNamespace(build=AsyncMock())
        alert = SimpleNamespace(monitor=AsyncMock())
        self.scheduler.install_trading_playbook_review_service(review)
        self.scheduler.install_trading_playbook_alert_service(alert)

        with patch(
            "app.data_collectors.scheduler._get_cn_trading_dates",
            return_value=[self.now.date()],
        ):
            await self.scheduler._review_trading_playbook()
            await self.scheduler._finalize_trading_playbook_review()
            await self.scheduler._monitor_trading_playbook()

        self.assertEqual(
            review.build.await_args_list,
            [
                unittest.mock.call(self.db, self.now.date(), finalized=False),
                unittest.mock.call(self.db, self.now.date(), finalized=True),
            ],
        )
        alert.monitor.assert_awaited_once_with(self.db, self.now)


class TradingPlaybookDataReadyBarrierTests(unittest.IsolatedAsyncioTestCase):
    def _scheduler(self, values, sleeps):
        now = datetime(2026, 7, 13, 15, 30, tzinfo=CN_TZ)
        db = MagicMock()
        db.execute = AsyncMock(
            side_effect=[ScalarResult(value) for value in values]
        )

        async def record_sleep(seconds):
            sleeps.append(seconds)

        scheduler = DataScheduler(
            session_factory=lambda: AsyncSessionContext(db),
            now_provider=lambda: now,
            sleep=record_sleep,
        )
        return scheduler, db

    async def test_barrier_accepts_naive_or_aware_updates_after_china_1500(self):
        sleeps = []
        stale = SimpleNamespace(updated_at=datetime(2026, 7, 13, 14, 59, 59))
        ready = SimpleNamespace(updated_at=datetime(2026, 7, 13, 15, 0, 1, tzinfo=CN_TZ))
        scheduler, db = self._scheduler([stale, ready], sleeps)

        result = await scheduler._wait_for_trading_playbook_data(date(2026, 7, 13))

        self.assertTrue(result)
        self.assertEqual(sleeps, [10])
        self.assertEqual(db.execute.await_count, 2)

    async def test_barrier_reopens_session_and_observes_writer_commit(self):
        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "playbook-ready.db"
            url = f"sqlite+aiosqlite:///{database_path.as_posix()}"
            reader_engine = create_async_engine(url)
            writer_engine = create_async_engine(url)
            reader_maker = async_sessionmaker(
                reader_engine,
                expire_on_commit=False,
            )
            writer_maker = async_sessionmaker(
                writer_engine,
                expire_on_commit=False,
            )

            async with writer_engine.begin() as connection:
                await connection.run_sync(
                    MarketReviewDailyMetric.__table__.create
                )
            async with writer_maker() as writer:
                writer.add(
                    MarketReviewDailyMetric(
                        trade_date=date(2026, 7, 13),
                        updated_at=datetime(2026, 7, 13, 14, 59),
                    )
                )
                await writer.commit()

            lifecycle = {"entered": 0, "exited": 0}

            class TrackedSession:
                def __init__(self, session):
                    self.session = session

                async def __aenter__(self):
                    lifecycle["entered"] += 1
                    return self.session

                async def __aexit__(self, exc_type, exc, traceback):
                    await self.session.close()
                    lifecycle["exited"] += 1
                    return False

            def reader_sessions():
                return TrackedSession(reader_maker())

            sleeps = []

            async def publish_ready(seconds):
                sleeps.append(seconds)
                async with writer_maker() as writer:
                    row = await writer.get(MarketReviewDailyMetric, 1)
                    row.updated_at = datetime(2026, 7, 13, 15, 1)
                    await writer.commit()

            scheduler = DataScheduler(
                session_factory=reader_sessions,
                now_provider=lambda: datetime(
                    2026,
                    7,
                    13,
                    15,
                    30,
                    tzinfo=CN_TZ,
                ),
                sleep=publish_ready,
            )
            try:
                ready = await scheduler._wait_for_trading_playbook_data(
                    date(2026, 7, 13)
                )
            finally:
                await reader_engine.dispose()
                await writer_engine.dispose()

        self.assertTrue(ready)
        self.assertEqual(sleeps, [10])
        self.assertEqual(lifecycle, {"entered": 2, "exited": 2})

    async def test_barrier_times_out_at_180_seconds_with_each_sleep_at_most_10(self):
        sleeps = []
        stale = SimpleNamespace(updated_at=datetime(2026, 7, 13, 14, 59))
        scheduler, db = self._scheduler([stale] * 18, sleeps)

        result = await scheduler._wait_for_trading_playbook_data(date(2026, 7, 13))

        self.assertFalse(result)
        self.assertEqual(sum(sleeps), 180)
        self.assertTrue(sleeps)
        self.assertLessEqual(max(sleeps), 10)
        self.assertEqual(db.execute.await_count, 18)

    async def test_barrier_treats_query_failure_as_not_ready_and_closes_session(self):
        lifecycle = {"exited": 0}
        db = SimpleNamespace(
            execute=AsyncMock(side_effect=RuntimeError("database unavailable"))
        )

        class FailingSession(AsyncSessionContext):
            async def __aexit__(self, exc_type, exc, traceback):
                lifecycle["exited"] += 1
                return False

        sleeps = []

        async def record_sleep(seconds):
            sleeps.append(seconds)

        scheduler = DataScheduler(
            session_factory=lambda: FailingSession(db),
            sleep=record_sleep,
        )

        ready = await scheduler._wait_for_trading_playbook_data(
            date(2026, 7, 13),
            timeout_seconds=10,
        )

        self.assertFalse(ready)
        self.assertEqual(sleeps, [10])
        self.assertEqual(lifecycle["exited"], 1)

    async def test_after_close_timeout_builds_degraded_then_finalizes_in_order(self):
        scheduler = DataScheduler(now_provider=lambda: datetime(2026, 7, 13, 15, 30, tzinfo=CN_TZ))
        calls = []
        scheduler._wait_for_trading_playbook_data = AsyncMock(return_value=False)

        async def build(
            stage,
            *,
            degraded=False,
            degradation_reason=None,
            send_notifications=True,
        ):
            calls.append(
                (
                    "build",
                    stage,
                    degraded,
                    degradation_reason,
                    send_notifications,
                )
            )
            return SimpleNamespace(id=7)

        async def finalize(*, plan_version_id=None):
            calls.append(("finalize", plan_version_id))

        scheduler._build_trading_playbook_plan = build
        scheduler._finalize_trading_playbook_review = finalize

        with patch(
            "app.data_collectors.scheduler._get_cn_trading_dates",
            return_value=[date(2026, 7, 13)],
        ):
            await scheduler._build_trading_playbook_after_close(
                send_notifications=False
            )

        self.assertEqual(
            calls,
            [
                (
                    "build",
                    "after_close",
                    True,
                    "after_close_barrier_timeout",
                    False,
                ),
                ("finalize", 7),
            ],
        )

    async def test_after_close_later_ready_calls_orchestrator_again_without_mutation(self):
        scheduler = DataScheduler(now_provider=lambda: datetime(2026, 7, 13, 15, 30, tzinfo=CN_TZ))
        scheduler._wait_for_trading_playbook_data = AsyncMock(
            side_effect=[False, True]
        )
        scheduler._build_trading_playbook_plan = AsyncMock()
        scheduler._finalize_trading_playbook_review = AsyncMock()

        with patch(
            "app.data_collectors.scheduler._get_cn_trading_dates",
            return_value=[date(2026, 7, 13)],
        ):
            await scheduler._build_trading_playbook_after_close()
            await scheduler._build_trading_playbook_after_close()

        self.assertEqual(
            scheduler._build_trading_playbook_plan.await_args_list,
            [
                unittest.mock.call(
                    "after_close",
                    degraded=True,
                    degradation_reason="after_close_barrier_timeout",
                    send_notifications=True,
                ),
                unittest.mock.call(
                    "after_close",
                    degraded=False,
                    degradation_reason=None,
                    send_notifications=True,
                ),
            ],
        )


class TradingPlaybookForcedUpgradeTests(unittest.IsolatedAsyncioTestCase):
    def test_forced_upgrade_marker_prefers_structured_quality_with_legacy_compatibility(self):
        structured = SimpleNamespace(
            data_quality_json={
                "status": "degraded",
                "forced_degraded": True,
                "degradation_reason": "after_close_barrier_timeout",
                "warnings": ["ordinary warning"],
            }
        )
        explicit_false = SimpleNamespace(
            data_quality_json={
                "forced_degraded": False,
                "warnings": ["force_degraded requested"],
            }
        )
        legacy = SimpleNamespace(
            data_quality_json={"warnings": ["force_degraded requested"]}
        )

        self.assertTrue(DataScheduler._is_forced_degraded_plan(structured))
        self.assertFalse(DataScheduler._is_forced_degraded_plan(explicit_false))
        self.assertTrue(DataScheduler._is_forced_degraded_plan(legacy))

    @staticmethod
    def _after_close_plan(source_date, target_date, version_no, warning):
        return TradingPlanVersion(
            source_trade_date=source_date,
            target_trade_date=target_date,
            stage="after_close",
            version_no=version_no,
            status="draft",
            market_state_json={},
            theme_ranking_json=[],
            mode_radar_json=[],
            rule_snapshot_json=[],
            risk_settings_json={},
            data_quality_json={"warnings": [warning]},
            change_summary_json={},
            input_hash=f"{source_date}-{target_date}-{version_no}",
            generated_at=datetime(2026, 7, 13, 15, 35),
        )

    async def test_old_source_targeting_today_is_not_upgraded(self):
        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "playbook-old-source.db"
            engine = create_async_engine(
                f"sqlite+aiosqlite:///{database_path.as_posix()}"
            )
            maker = async_sessionmaker(engine, expire_on_commit=False)
            async with engine.begin() as connection:
                await connection.run_sync(TradingPlanVersion.__table__.create)
            orchestrator = SimpleNamespace(build_stage=AsyncMock())
            try:
                async with maker() as db:
                    db.add(
                        self._after_close_plan(
                            date(2026, 7, 10),
                            date(2026, 7, 13),
                            9,
                            "force_degraded requested",
                        )
                    )
                    await db.commit()
                scheduler = DataScheduler(
                    trading_playbook_orchestrator=orchestrator,
                    session_factory=maker,
                    now_provider=lambda: datetime(
                        2026, 7, 13, 15, 35, tzinfo=CN_TZ
                    ),
                )
                scheduler._trading_playbook_data_ready_once = AsyncMock(
                    return_value=True
                )

                with patch(
                    "app.data_collectors.scheduler._get_cn_trading_dates",
                    return_value=[date(2026, 7, 13), date(2026, 7, 14)],
                ):
                    result = (
                        await scheduler._upgrade_forced_trading_playbook_after_close(
                            send_notifications=True
                        )
                    )
            finally:
                await engine.dispose()

        self.assertIsNone(result)
        orchestrator.build_stage.assert_not_awaited()
        scheduler._trading_playbook_data_ready_once.assert_not_awaited()

    async def test_latest_plan_uses_exact_next_target_not_far_future(self):
        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "playbook-exact-target.db"
            engine = create_async_engine(
                f"sqlite+aiosqlite:///{database_path.as_posix()}"
            )
            maker = async_sessionmaker(engine, expire_on_commit=False)
            async with engine.begin() as connection:
                await connection.run_sync(TradingPlanVersion.__table__.create)
            try:
                async with maker() as db:
                    db.add_all(
                        [
                            self._after_close_plan(
                                date(2026, 7, 13),
                                date(2026, 7, 14),
                                1,
                                "old",
                            ),
                            self._after_close_plan(
                                date(2026, 7, 13),
                                date(2026, 7, 14),
                                2,
                                "latest",
                            ),
                            self._after_close_plan(
                                date(2026, 7, 13),
                                date(2026, 7, 20),
                                99,
                                "far-future",
                            ),
                        ]
                    )
                    await db.commit()
                scheduler = DataScheduler(session_factory=maker)

                with patch(
                    "app.data_collectors.scheduler._get_cn_trading_dates",
                    return_value=[date(2026, 7, 13), date(2026, 7, 14)],
                ):
                    result = await scheduler._latest_relevant_after_close_plan(
                        date(2026, 7, 13)
                    )
            finally:
                await engine.dispose()

        self.assertIsNotNone(result)
        self.assertEqual(result.target_trade_date, date(2026, 7, 14))
        self.assertEqual(result.version_no, 2)

    async def test_monitor_isolates_empty_next_calendar_and_runs_alerts(self):
        alert_service = SimpleNamespace(monitor=AsyncMock())
        scheduler = DataScheduler(
            trading_playbook_orchestrator=SimpleNamespace(
                build_stage=AsyncMock()
            ),
            trading_playbook_alert_service=alert_service,
            session_factory=lambda: AsyncSessionContext(MagicMock()),
            now_provider=lambda: datetime(
                2026, 7, 13, 15, 35, tzinfo=CN_TZ
            ),
        )

        with patch(
            "app.data_collectors.scheduler._get_cn_trading_dates",
            side_effect=[[date(2026, 7, 13)], []],
        ), patch("app.data_collectors.scheduler.logger.error") as log_error:
            await scheduler._monitor_trading_playbook()

        alert_service.monitor.assert_awaited_once()
        log_error.assert_called_once()
        self.assertIsInstance(
            log_error.call_args.args[1],
            TradingCalendarLookupError,
        )

    async def test_monitor_upgrades_forced_timeout_once_after_writer_is_ready(self):
        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "playbook-upgrade.db"
            url = f"sqlite+aiosqlite:///{database_path.as_posix()}"
            scheduler_engine = create_async_engine(url)
            writer_engine = create_async_engine(url)
            scheduler_maker = async_sessionmaker(
                scheduler_engine,
                expire_on_commit=False,
            )
            writer_maker = async_sessionmaker(
                writer_engine,
                expire_on_commit=False,
            )
            async with writer_engine.begin() as connection:
                await connection.run_sync(
                    MarketReviewDailyMetric.__table__.create
                )
                await connection.run_sync(TradingPlanVersion.__table__.create)
                await connection.run_sync(
                    TradingExecutionReview.__table__.create
                )
                await connection.run_sync(
                    TradingPlaybookJobClaim.__table__.create
                )
            async with writer_maker() as writer:
                writer.add(
                    MarketReviewDailyMetric(
                        trade_date=date(2026, 7, 13),
                        updated_at=datetime(2026, 7, 13, 14, 59),
                    )
                )
                await writer.commit()

            clock = [datetime(2026, 7, 13, 15, 30, tzinfo=CN_TZ)]

            class PersistingOrchestrator:
                def __init__(self):
                    self.calls = []

                async def build_stage(
                    self,
                    db,
                    source_trade_date,
                    stage,
                    as_of,
                    degraded=False,
                    degradation_reason=None,
                ):
                    latest = (
                        await db.execute(
                            select(TradingPlanVersion)
                            .order_by(TradingPlanVersion.version_no.desc())
                            .limit(1)
                        )
                    ).scalar_one_or_none()
                    version_no = 1 if latest is None else latest.version_no + 1
                    plan = TradingPlanVersion(
                        source_trade_date=source_trade_date,
                        target_trade_date=date(2026, 7, 14),
                        stage=stage,
                        version_no=version_no,
                        status="draft",
                        market_state_json={},
                        theme_ranking_json=[],
                        mode_radar_json=[],
                        rule_snapshot_json=[],
                        risk_settings_json={},
                        data_quality_json={
                            "status": "degraded" if degraded else "ready",
                            "forced_degraded": degradation_reason is not None,
                            "degradation_reason": degradation_reason,
                            "warnings": (
                                ["force_degraded requested"]
                                if degraded
                                else []
                            ),
                        },
                        change_summary_json={},
                        input_hash=f"stage-{version_no}-{degraded}",
                        generated_at=as_of.replace(tzinfo=None),
                    )
                    db.add(plan)
                    await db.commit()
                    await db.refresh(plan)
                    self.calls.append(
                        (source_trade_date, stage, as_of, degraded)
                    )
                    return plan

            orchestrator = PersistingOrchestrator()
            alert_service = SimpleNamespace(
                notify_plan_ready=AsyncMock(),
                monitor=AsyncMock(),
            )
            review_service = SimpleNamespace(build=AsyncMock())
            scheduler = DataScheduler(
                trading_playbook_orchestrator=orchestrator,
                trading_playbook_alert_service=alert_service,
                trading_playbook_review_service=review_service,
                session_factory=scheduler_maker,
                now_provider=lambda: clock[0],
                sleep=AsyncMock(),
            )

            try:
                with patch(
                    "app.data_collectors.scheduler._get_cn_trading_dates",
                    return_value=[date(2026, 7, 13), date(2026, 7, 14)],
                ):
                    await scheduler._build_trading_playbook_after_close()

                async with writer_maker() as writer:
                    metric = await writer.get(MarketReviewDailyMetric, 1)
                    metric.updated_at = datetime(2026, 7, 13, 15, 1)
                    await writer.commit()
                clock[0] = datetime(2026, 7, 13, 15, 35, tzinfo=CN_TZ)

                with patch(
                    "app.data_collectors.scheduler._get_cn_trading_dates",
                    return_value=[date(2026, 7, 13), date(2026, 7, 14)],
                ):
                    await asyncio.gather(
                        scheduler._monitor_trading_playbook(),
                        scheduler._monitor_trading_playbook(),
                    )
                    await scheduler._monitor_trading_playbook()

                async with scheduler_maker() as db:
                    plans = (
                        await db.execute(
                            select(TradingPlanVersion).order_by(
                                TradingPlanVersion.version_no
                            )
                        )
                    ).scalars().all()
            finally:
                await scheduler_engine.dispose()
                await writer_engine.dispose()

        self.assertEqual([call[3] for call in orchestrator.calls], [True, False])
        self.assertEqual(len(plans), 2)
        self.assertEqual(
            plans[0].data_quality_json["warnings"],
            ["force_degraded requested"],
        )
        self.assertEqual(plans[1].data_quality_json["warnings"], [])
        self.assertEqual(alert_service.notify_plan_ready.await_count, 2)
        self.assertEqual(alert_service.monitor.await_count, 3)
        self.assertEqual(review_service.build.await_count, 2)
        review_service.build.assert_awaited_with(
            unittest.mock.ANY,
            date(2026, 7, 13),
            finalized=True,
        )

    async def test_natural_degraded_plan_never_enters_forced_upgrade(self):
        scheduler = DataScheduler(
            trading_playbook_orchestrator=SimpleNamespace(
                build_stage=AsyncMock()
            ),
            now_provider=lambda: datetime(
                2026,
                7,
                13,
                15,
                35,
                tzinfo=CN_TZ,
            ),
        )
        scheduler._latest_relevant_after_close_plan = AsyncMock(
            return_value=SimpleNamespace(
                source_trade_date=date(2026, 7, 13),
                data_quality_json={
                    "status": "degraded",
                    "warnings": ["quote source unavailable"],
                },
            )
        )
        scheduler._trading_playbook_data_ready_once = AsyncMock()

        with patch(
            "app.data_collectors.scheduler._get_cn_trading_dates",
            return_value=[date(2026, 7, 13), date(2026, 7, 14)],
        ):
            result = await scheduler._upgrade_forced_trading_playbook_after_close(
                send_notifications=True
            )

        self.assertIsNone(result)
        scheduler._trading_playbook_data_ready_once.assert_not_awaited()
        scheduler._trading_playbook_orchestrator.build_stage.assert_not_awaited()

    async def test_startup_upgrade_is_silent_and_still_finalizes_review(self):
        now = datetime(2026, 7, 13, 16, 0, tzinfo=CN_TZ)
        db = MagicMock()
        plan = SimpleNamespace(id=2)
        orchestrator = SimpleNamespace(build_stage=AsyncMock(return_value=plan))
        alert_service = SimpleNamespace(notify_plan_ready=AsyncMock())
        review_service = SimpleNamespace(build=AsyncMock())
        scheduler = DataScheduler(
            trading_playbook_orchestrator=orchestrator,
            trading_playbook_alert_service=alert_service,
            trading_playbook_review_service=review_service,
            session_factory=lambda: AsyncSessionContext(db),
            now_provider=lambda: now,
            job_claim_service=InMemoryClaimService(),
        )
        scheduler._playbook_stage_exists = AsyncMock(return_value=True)
        scheduler._playbook_review_exists = AsyncMock(
            side_effect=[True, False, False]
        )
        scheduler._latest_relevant_after_close_plan = AsyncMock(
            return_value=SimpleNamespace(
                id=2,
                source_trade_date=now.date(),
                data_quality_json={
                    "warnings": ["force_degraded requested"],
                },
            )
        )
        scheduler._trading_playbook_data_ready_once = AsyncMock(return_value=True)

        with patch(
            "app.data_collectors.scheduler._get_cn_trading_dates",
            return_value=[date(2026, 7, 13), date(2026, 7, 14)],
        ):
            await scheduler._run_trading_playbook_catchup(now)

        orchestrator.build_stage.assert_awaited_once_with(
            db,
            now.date(),
            "after_close",
            now,
            degraded=False,
        )
        alert_service.notify_plan_ready.assert_not_awaited()
        review_service.build.assert_awaited_once_with(
            db,
            now.date(),
            finalized=True,
        )

    async def test_forced_upgrade_skips_before_1530_and_on_non_trading_day(self):
        for now, trading_dates in (
            (datetime(2026, 7, 13, 15, 29, tzinfo=CN_TZ), [date(2026, 7, 13)]),
            (datetime(2026, 7, 12, 15, 35, tzinfo=CN_TZ), []),
        ):
            with self.subTest(now=now):
                scheduler = DataScheduler(now_provider=lambda now=now: now)
                scheduler._latest_relevant_after_close_plan = AsyncMock()
                with patch(
                    "app.data_collectors.scheduler._get_cn_trading_dates",
                    return_value=trading_dates,
                ):
                    result = await scheduler._upgrade_forced_trading_playbook_after_close(
                        send_notifications=True
                    )
                self.assertIsNone(result)
                scheduler._latest_relevant_after_close_plan.assert_not_awaited()

    async def test_upgrade_error_does_not_prevent_future_alert_monitor(self):
        alert_service = SimpleNamespace(monitor=AsyncMock())
        scheduler = DataScheduler(
            trading_playbook_alert_service=alert_service,
            session_factory=lambda: AsyncSessionContext(MagicMock()),
            now_provider=lambda: datetime(
                2026,
                7,
                13,
                15,
                35,
                tzinfo=CN_TZ,
            ),
        )
        scheduler._upgrade_forced_trading_playbook_after_close = AsyncMock(
            side_effect=RuntimeError("upgrade failed")
        )

        with patch(
            "app.data_collectors.scheduler._get_cn_trading_dates",
            return_value=[date(2026, 7, 13), date(2026, 7, 14)],
        ):
            await scheduler._monitor_trading_playbook()

        scheduler._upgrade_forced_trading_playbook_after_close.assert_awaited_once_with(
            send_notifications=True,
            trade_date=date(2026, 7, 13),
            next_trade_date=date(2026, 7, 14),
        )
        alert_service.monitor.assert_awaited_once()


class TradingPlaybookCatchupTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.today = date(2026, 7, 13)
        self.next_day = date(2026, 7, 14)
        self.scheduler = DataScheduler()
        self.scheduler._build_trading_playbook_plan = AsyncMock()
        self.scheduler._build_trading_playbook_after_close = AsyncMock()
        self.scheduler._review_trading_playbook = AsyncMock()
        self.scheduler._playbook_review_exists = AsyncMock(return_value=True)

    async def _run(self, local_time, existing_stages):
        async def exists(target_date, stage):
            return (target_date, stage) in existing_stages

        self.scheduler._playbook_stage_exists = AsyncMock(side_effect=exists)
        now = datetime.combine(self.today, local_time, tzinfo=CN_TZ)
        with patch(
            "app.data_collectors.scheduler._get_cn_trading_dates",
            return_value=[self.today, self.next_day],
        ):
            await self.scheduler._run_trading_playbook_catchup(now)

    async def test_0850_catches_up_overnight_without_notifications(self):
        await self._run(datetime.min.replace(hour=8, minute=50).time(), set())
        self.scheduler._build_trading_playbook_plan.assert_awaited_once_with(
            "overnight",
            send_notifications=False,
        )

    async def test_0926_catches_up_auction_degraded_without_notifications(self):
        await self._run(datetime.min.replace(hour=9, minute=26).time(), set())
        self.scheduler._build_trading_playbook_plan.assert_awaited_once_with(
            "auction",
            degraded=True,
            send_notifications=False,
        )

    async def test_1440_catches_up_preclose_for_next_trade_date(self):
        await self._run(
            datetime.min.replace(hour=14, minute=40).time(),
            {(self.today, "auction")},
        )
        self.scheduler._build_trading_playbook_plan.assert_awaited_once_with(
            "preclose",
            send_notifications=False,
        )
        self.scheduler._playbook_stage_exists.assert_any_await(
            self.next_day,
            "preclose",
        )

    async def test_1500_does_not_catch_up_auction_or_preclose(self):
        await self._run(datetime.min.replace(hour=15).time(), set())
        self.scheduler._build_trading_playbook_plan.assert_not_awaited()
        self.assertNotIn(
            unittest.mock.call(self.next_day, "preclose"),
            self.scheduler._playbook_stage_exists.await_args_list,
        )

    async def test_1510_catches_up_review_when_missing(self):
        self.scheduler._playbook_review_exists.return_value = False
        await self._run(datetime.min.replace(hour=15, minute=10).time(), set())
        self.scheduler._review_trading_playbook.assert_awaited_once_with()

    async def test_1530_catches_up_after_close_without_notifications(self):
        await self._run(datetime.min.replace(hour=15, minute=30).time(), set())
        self.scheduler._build_trading_playbook_after_close.assert_awaited_once_with(
            send_notifications=False,
        )

    async def test_startup_catchup_checks_existing_forced_plan_without_notifications(self):
        self.scheduler._upgrade_forced_trading_playbook_after_close = AsyncMock()
        await self._run(
            datetime.min.replace(hour=16).time(),
            {(self.next_day, "after_close")},
        )
        self.scheduler._build_trading_playbook_after_close.assert_not_awaited()
        self.scheduler._upgrade_forced_trading_playbook_after_close.assert_awaited_once_with(
            send_notifications=False,
            trade_date=self.today,
            next_trade_date=self.next_day,
        )

    async def test_non_trading_day_skips_all_catchup_work(self):
        now = datetime(2026, 7, 12, 15, 30, tzinfo=CN_TZ)
        with patch(
            "app.data_collectors.scheduler._get_cn_trading_dates",
            return_value=[],
        ):
            await self.scheduler._run_trading_playbook_catchup(now)

        self.scheduler._build_trading_playbook_plan.assert_not_awaited()
        self.scheduler._build_trading_playbook_after_close.assert_not_awaited()
        self.scheduler._review_trading_playbook.assert_not_awaited()

    async def test_empty_next_trade_calendar_raises_explicit_error(self):
        now = datetime(2026, 7, 13, 14, 40, tzinfo=CN_TZ)
        with patch(
            "app.data_collectors.scheduler._get_cn_trading_dates",
            side_effect=[[self.today], []],
        ):
            with self.assertRaises(TradingCalendarLookupError):
                await self.scheduler._run_trading_playbook_catchup(now)


if __name__ == "__main__":
    unittest.main()
