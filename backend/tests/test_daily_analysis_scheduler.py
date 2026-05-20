import asyncio
import unittest
from datetime import date
import sys
import types
from unittest.mock import AsyncMock, patch

apscheduler_module = types.ModuleType("apscheduler")
schedulers_module = types.ModuleType("apscheduler.schedulers")
asyncio_module = types.ModuleType("apscheduler.schedulers.asyncio")
triggers_module = types.ModuleType("apscheduler.triggers")
cron_module = types.ModuleType("apscheduler.triggers.cron")
date_module = types.ModuleType("apscheduler.triggers.date")
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


class StubDateTrigger:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


asyncio_module.AsyncIOScheduler = StubAsyncIOScheduler
cron_module.CronTrigger = StubCronTrigger
date_module.DateTrigger = StubDateTrigger
interval_module.IntervalTrigger = StubIntervalTrigger

sys.modules.setdefault("apscheduler", apscheduler_module)
sys.modules.setdefault("apscheduler.schedulers", schedulers_module)
sys.modules.setdefault("apscheduler.schedulers.asyncio", asyncio_module)
sys.modules.setdefault("apscheduler.triggers", triggers_module)
sys.modules.setdefault("apscheduler.triggers.cron", cron_module)
sys.modules.setdefault("apscheduler.triggers.date", date_module)
sys.modules.setdefault("apscheduler.triggers.interval", interval_module)

from app.data_collectors.scheduler import DataScheduler


class FakeScheduler:
    def __init__(self):
        self.jobs = []
        self.started = False

    def add_job(self, func, trigger, **kwargs):
        self.jobs.append({
            "func": func,
            "trigger": trigger,
            **kwargs,
        })

    def start(self):
        self.started = True


class DailyAnalysisSchedulerTests(unittest.TestCase):
    def test_start_registers_daily_analysis_after_close_job(self):
        scheduler = DataScheduler()
        scheduler.scheduler = FakeScheduler()

        scheduler.start()

        job_ids = {job["id"] for job in scheduler.scheduler.jobs}
        self.assertIn("daily_analysis", job_ids)

    def test_start_registers_daily_analysis_intraday_job_at_1450(self):
        scheduler = DataScheduler()
        scheduler.scheduler = FakeScheduler()

        scheduler.start()

        intraday_job = next(job for job in scheduler.scheduler.jobs if job["id"] == "daily_analysis_intraday")
        self.assertEqual(intraday_job["trigger"].kwargs["hour"], 14)
        self.assertEqual(intraday_job["trigger"].kwargs["minute"], 50)

    def test_start_registers_intelligence_sync_jobs(self):
        scheduler = DataScheduler()
        scheduler.scheduler = FakeScheduler()

        scheduler.start()

        job_ids = {job["id"] for job in scheduler.scheduler.jobs}
        self.assertIn("intelligence_sync_0845", job_ids)
        self.assertIn("intelligence_sync_1145", job_ids)
        self.assertIn("intelligence_sync_1520", job_ids)
        self.assertIn("intelligence_sync_2030", job_ids)
        self.assertIn("intelligence_startup_sync", job_ids)
        self.assertIn("intelligence_probe", job_ids)

        probe_job = next(job for job in scheduler.scheduler.jobs if job["id"] == "intelligence_probe")
        self.assertEqual(probe_job["trigger"].kwargs["seconds"], 60)

    def test_calculate_daily_analysis_skips_non_trading_day(self):
        scheduler = DataScheduler()

        with patch(
            "app.data_collectors.scheduler._resolve_cn_trade_date_for_market_review",
            return_value=None,
        ):
            asyncio.run(scheduler._calculate_daily_analysis())

    def test_calculate_intraday_daily_analysis_refreshes_market_review_first(self):
        scheduler = DataScheduler()
        trade_date = date(2026, 5, 19)

        with patch(
            "app.data_collectors.scheduler._resolve_cn_trade_date_for_market_review",
            return_value=trade_date,
        ), patch(
            "app.data_collectors.scheduler.market_review_pipeline_service.run_for_date",
            new_callable=AsyncMock,
        ) as run_market_review, patch(
            "app.services.daily_analysis_service.daily_analysis_service.build_for_date",
            new_callable=AsyncMock,
        ) as build_daily_analysis:
            asyncio.run(scheduler._calculate_intraday_daily_analysis())

        run_market_review.assert_awaited_once_with(trade_date, calc_version=0)
        build_daily_analysis.assert_awaited_once()
        self.assertEqual(build_daily_analysis.await_args.args[1], trade_date)
        self.assertEqual(build_daily_analysis.await_args.kwargs["session"], "intraday")


if __name__ == "__main__":
    unittest.main()
