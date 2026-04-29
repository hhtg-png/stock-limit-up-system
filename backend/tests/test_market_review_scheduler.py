import unittest
from datetime import date, timedelta, tzinfo
import sys
import types
from unittest.mock import AsyncMock, patch

apscheduler_module = types.ModuleType("apscheduler")
schedulers_module = types.ModuleType("apscheduler.schedulers")
asyncio_module = types.ModuleType("apscheduler.schedulers.asyncio")
triggers_module = types.ModuleType("apscheduler.triggers")
cron_module = types.ModuleType("apscheduler.triggers.cron")
interval_module = types.ModuleType("apscheduler.triggers.interval")


class StubAsyncIOScheduler:
    def add_job(self, *args, **kwargs):
        return None

    def start(self):
        return None

    def shutdown(self):
        return None


class StubCronTrigger:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


class StubIntervalTrigger:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


asyncio_module.AsyncIOScheduler = StubAsyncIOScheduler
cron_module.CronTrigger = StubCronTrigger
interval_module.IntervalTrigger = StubIntervalTrigger

sys.modules.setdefault("apscheduler", apscheduler_module)
sys.modules.setdefault("apscheduler.schedulers", schedulers_module)
sys.modules.setdefault("apscheduler.schedulers.asyncio", asyncio_module)
sys.modules.setdefault("apscheduler.triggers", triggers_module)
sys.modules.setdefault("apscheduler.triggers.cron", cron_module)
sys.modules.setdefault("apscheduler.triggers.interval", interval_module)


class StubShanghaiTimezone(tzinfo):
    zone = "Asia/Shanghai"

    def utcoffset(self, dt):
        return timedelta(hours=8)

    def dst(self, dt):
        return timedelta(0)

    def tzname(self, dt):
        return self.zone


pytz_module = types.ModuleType("pytz")
pytz_module.timezone = lambda _: StubShanghaiTimezone()
sys.modules.setdefault("pytz", pytz_module)

from app.data_collectors.scheduler import DataScheduler, _get_cn_trading_dates
from app.utils.time_utils import CN_TZ
from scripts.backfill_market_review import backfill_market_review


class FakeScheduler:
    def __init__(self):
        self.jobs = []
        self.started = False
        self.shutdown_called = False

    def add_job(self, func, trigger, **kwargs):
        self.jobs.append({
            "func": func,
            "trigger": trigger,
            **kwargs,
        })

    def start(self):
        self.started = True

    def shutdown(self):
        self.shutdown_called = True


class MarketReviewSchedulerTests(unittest.IsolatedAsyncioTestCase):
    def _create_scheduler(self):
        scheduler = DataScheduler()
        scheduler.scheduler = FakeScheduler()
        return scheduler

    def test_start_registers_market_review_jobs_when_enabled(self):
        scheduler = self._create_scheduler()

        with patch("app.data_collectors.scheduler.settings") as mock_settings:
            mock_settings.L2_COLLECT_INTERVAL = 3
            mock_settings.CRAWLER_INTERVAL_THS = 300
            mock_settings.CRAWLER_INTERVAL_KPL = 600
            mock_settings.MARKET_REVIEW_ENABLED = True
            mock_settings.MARKET_REVIEW_BUILD_HOUR = 15
            mock_settings.MARKET_REVIEW_BUILD_MINUTE = 5
            mock_settings.MARKET_REVIEW_REPAIR_ENABLED = True
            mock_settings.MARKET_REVIEW_REPAIR_HOUR = 20
            mock_settings.MARKET_REVIEW_REPAIR_MINUTE = 15

            scheduler.start()

        jobs_by_id = {job["id"]: job for job in scheduler.scheduler.jobs}
        job_ids = set(jobs_by_id)
        self.assertIn("market_review_build", job_ids)
        self.assertIn("market_review_repair", job_ids)
        self.assertEqual(getattr(CN_TZ, "zone", None), "Asia/Shanghai")
        self.assertIs(jobs_by_id["market_review_build"]["trigger"].kwargs["timezone"], CN_TZ)
        self.assertIs(jobs_by_id["market_review_repair"]["trigger"].kwargs["timezone"], CN_TZ)
        self.assertEqual(
            getattr(jobs_by_id["market_review_build"]["trigger"].kwargs["timezone"], "zone", None),
            "Asia/Shanghai",
        )

    def test_start_skips_market_review_jobs_when_disabled(self):
        scheduler = self._create_scheduler()

        with patch("app.data_collectors.scheduler.settings") as mock_settings:
            mock_settings.L2_COLLECT_INTERVAL = 3
            mock_settings.CRAWLER_INTERVAL_THS = 300
            mock_settings.CRAWLER_INTERVAL_KPL = 600
            mock_settings.MARKET_REVIEW_ENABLED = False
            mock_settings.MARKET_REVIEW_BUILD_HOUR = 15
            mock_settings.MARKET_REVIEW_BUILD_MINUTE = 5
            mock_settings.MARKET_REVIEW_REPAIR_ENABLED = True
            mock_settings.MARKET_REVIEW_REPAIR_HOUR = 20
            mock_settings.MARKET_REVIEW_REPAIR_MINUTE = 15

            scheduler.start()

        job_ids = {job["id"] for job in scheduler.scheduler.jobs}
        self.assertNotIn("market_review_build", job_ids)
        self.assertNotIn("market_review_repair", job_ids)

    def test_start_skips_market_review_repair_when_repair_disabled(self):
        scheduler = self._create_scheduler()

        with patch("app.data_collectors.scheduler.settings") as mock_settings:
            mock_settings.L2_COLLECT_INTERVAL = 3
            mock_settings.CRAWLER_INTERVAL_THS = 300
            mock_settings.CRAWLER_INTERVAL_KPL = 600
            mock_settings.MARKET_REVIEW_ENABLED = True
            mock_settings.MARKET_REVIEW_BUILD_HOUR = 15
            mock_settings.MARKET_REVIEW_BUILD_MINUTE = 5
            mock_settings.MARKET_REVIEW_REPAIR_ENABLED = False
            mock_settings.MARKET_REVIEW_REPAIR_HOUR = 20
            mock_settings.MARKET_REVIEW_REPAIR_MINUTE = 15

            scheduler.start()

        job_ids = {job["id"] for job in scheduler.scheduler.jobs}
        self.assertIn("market_review_build", job_ids)
        self.assertNotIn("market_review_repair", job_ids)

    async def test_build_market_review_skips_pipeline_on_non_trading_day(self):
        scheduler = self._create_scheduler()

        with patch(
            "app.data_collectors.scheduler._resolve_cn_trade_date_for_market_review",
            return_value=None,
            create=True,
        ), patch(
            "app.data_collectors.scheduler.market_review_pipeline_service.run_for_date",
            AsyncMock(),
        ) as run_for_date:
            await scheduler._build_market_review()

        run_for_date.assert_not_awaited()

    async def test_build_market_review_runs_pipeline_with_calc_version_one(self):
        scheduler = self._create_scheduler()

        with patch(
            "app.data_collectors.scheduler._resolve_cn_trade_date_for_market_review",
            return_value=date(2026, 4, 27),
            create=True,
        ), patch(
            "app.data_collectors.scheduler.market_review_pipeline_service.run_for_date",
            AsyncMock(),
        ) as run_for_date:
            await scheduler._build_market_review()

        run_for_date.assert_awaited_once_with(date(2026, 4, 27), calc_version=1)

    async def test_build_market_review_propagates_calendar_lookup_failure(self):
        scheduler = self._create_scheduler()

        with patch(
            "app.data_collectors.scheduler._resolve_cn_trade_date_for_market_review",
            side_effect=RuntimeError("calendar unavailable"),
            create=True,
        ), patch(
            "app.data_collectors.scheduler.market_review_pipeline_service.run_for_date",
            AsyncMock(),
        ) as run_for_date:
            with self.assertRaisesRegex(RuntimeError, "calendar unavailable"):
                await scheduler._build_market_review()

        run_for_date.assert_not_awaited()

    async def test_repair_market_review_skips_pipeline_on_non_trading_day(self):
        scheduler = self._create_scheduler()

        with patch(
            "app.data_collectors.scheduler._resolve_cn_trade_date_for_market_review",
            return_value=None,
            create=True,
        ), patch(
            "app.data_collectors.scheduler.market_review_pipeline_service.run_for_date",
            AsyncMock(),
        ) as run_for_date:
            await scheduler._repair_market_review()

        run_for_date.assert_not_awaited()

    async def test_repair_market_review_runs_pipeline_with_calc_version_two(self):
        scheduler = self._create_scheduler()

        with patch(
            "app.data_collectors.scheduler._resolve_cn_trade_date_for_market_review",
            return_value=date(2026, 4, 28),
            create=True,
        ), patch(
            "app.data_collectors.scheduler.market_review_pipeline_service.run_for_date",
            AsyncMock(),
        ) as run_for_date:
            await scheduler._repair_market_review()

        run_for_date.assert_awaited_once_with(date(2026, 4, 28), calc_version=2)

    async def test_repair_market_review_propagates_calendar_lookup_failure(self):
        scheduler = self._create_scheduler()

        with patch(
            "app.data_collectors.scheduler._resolve_cn_trade_date_for_market_review",
            side_effect=RuntimeError("calendar unavailable"),
            create=True,
        ), patch(
            "app.data_collectors.scheduler.market_review_pipeline_service.run_for_date",
            AsyncMock(),
        ) as run_for_date:
            with self.assertRaisesRegex(RuntimeError, "calendar unavailable"):
                await scheduler._repair_market_review()

        run_for_date.assert_not_awaited()

    def test_get_cn_trading_dates_raises_when_calendar_schema_is_invalid(self):
        class InvalidCalendar:
            def __contains__(self, key):
                return False

        fake_akshare = types.SimpleNamespace(tool_trade_date_hist_sina=lambda: InvalidCalendar())

        with patch.dict(sys.modules, {"akshare": fake_akshare}):
            with self.assertRaisesRegex(RuntimeError, "trade_date"):
                _get_cn_trading_dates(date(2026, 4, 27), date(2026, 4, 28))


class MarketReviewBackfillTests(unittest.IsolatedAsyncioTestCase):
    async def test_backfill_market_review_propagates_calendar_lookup_failure(self):
        with patch(
            "scripts.backfill_market_review._get_cn_trading_dates",
            side_effect=RuntimeError("calendar unavailable"),
        ), patch("builtins.print") as print_mock:
            with self.assertRaisesRegex(RuntimeError, "calendar unavailable"):
                await backfill_market_review(date(2026, 4, 27), date(2026, 4, 28), calc_version=1)

        print_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
