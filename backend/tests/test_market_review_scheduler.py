import unittest
from datetime import date, timezone
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

pytz_module = types.ModuleType("pytz")
pytz_module.timezone = lambda _: timezone.utc
sys.modules.setdefault("pytz", pytz_module)

from app.data_collectors.scheduler import DataScheduler


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

        job_ids = {job["id"] for job in scheduler.scheduler.jobs}
        self.assertIn("market_review_build", job_ids)
        self.assertIn("market_review_repair", job_ids)

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

    async def test_build_market_review_runs_pipeline_with_calc_version_one(self):
        scheduler = self._create_scheduler()

        with patch(
            "app.data_collectors.scheduler.market_review_pipeline_service.run_for_date",
            AsyncMock(),
        ) as run_for_date:
            await scheduler._build_market_review()

        run_for_date.assert_awaited_once_with(date.today(), calc_version=1)

    async def test_repair_market_review_runs_pipeline_with_calc_version_two(self):
        scheduler = self._create_scheduler()

        with patch(
            "app.data_collectors.scheduler.market_review_pipeline_service.run_for_date",
            AsyncMock(),
        ) as run_for_date:
            await scheduler._repair_market_review()

        run_for_date.assert_awaited_once_with(date.today(), calc_version=2)


if __name__ == "__main__":
    unittest.main()
