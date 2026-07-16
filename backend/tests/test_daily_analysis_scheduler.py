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
_installed_stub_modules = []


def _install_stub(name, module):
    if name not in sys.modules:
        sys.modules[name] = module
        _installed_stub_modules.append(name)


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

try:
    import apscheduler  # noqa: F401
except ImportError:
    _install_stub("apscheduler", apscheduler_module)
    _install_stub("apscheduler.schedulers", schedulers_module)
    _install_stub("apscheduler.schedulers.asyncio", asyncio_module)
    _install_stub("apscheduler.triggers", triggers_module)
    _install_stub("apscheduler.triggers.cron", cron_module)
    _install_stub("apscheduler.triggers.date", date_module)
    _install_stub("apscheduler.triggers.interval", interval_module)

from app.data_collectors.scheduler import DataScheduler

for _stub_name in reversed(_installed_stub_modules):
    sys.modules.pop(_stub_name, None)


def _cron_value(trigger, name):
    if hasattr(trigger, "kwargs"):
        return trigger.kwargs[name]
    if name == "seconds":
        return int(trigger.interval.total_seconds())
    field_index = {"hour": 5, "minute": 6}[name]
    return int(str(trigger.fields[field_index]))


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

        daily_job = next(job for job in scheduler.scheduler.jobs if job["id"] == "daily_analysis")
        self.assertEqual(_cron_value(daily_job["trigger"], "hour"), 15)
        self.assertEqual(_cron_value(daily_job["trigger"], "minute"), 6)

    def test_start_registers_limit_up_classification_archive_after_close_job(self):
        scheduler = DataScheduler()
        scheduler.scheduler = FakeScheduler()

        scheduler.start()

        archive_job = next(
            job for job in scheduler.scheduler.jobs
            if job["id"] == "limit_up_classification_archive"
        )
        self.assertEqual(_cron_value(archive_job["trigger"], "hour"), 15)
        self.assertEqual(_cron_value(archive_job["trigger"], "minute"), 6)

    def test_start_registers_daily_analysis_intraday_job_at_1450(self):
        scheduler = DataScheduler()
        scheduler.scheduler = FakeScheduler()

        scheduler.start()

        intraday_job = next(job for job in scheduler.scheduler.jobs if job["id"] == "daily_analysis_intraday")
        self.assertEqual(_cron_value(intraday_job["trigger"], "hour"), 14)
        self.assertEqual(_cron_value(intraday_job["trigger"], "minute"), 50)

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
        self.assertEqual(_cron_value(probe_job["trigger"], "seconds"), 60)

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

    def test_archive_limit_up_classification_skips_non_trading_day(self):
        scheduler = DataScheduler()

        with patch(
            "app.data_collectors.scheduler._resolve_cn_trade_date_for_market_review",
            return_value=None,
        ):
            asyncio.run(scheduler._archive_limit_up_classification())

    def test_after_close_catchup_runs_limit_up_classification_archive(self):
        scheduler = DataScheduler()
        trade_date = date(2026, 5, 19)

        with patch(
            "app.data_collectors.scheduler._should_run_after_close_catchup",
            return_value=True,
        ), patch(
            "app.data_collectors.scheduler._resolve_latest_cn_trade_date_for_market_review",
            return_value=trade_date,
        ), patch.object(
            scheduler,
            "_build_market_review",
            new_callable=AsyncMock,
        ), patch.object(
            scheduler,
            "_calculate_daily_analysis",
            new_callable=AsyncMock,
        ), patch.object(
            scheduler,
            "_archive_limit_up_classification",
            new_callable=AsyncMock,
        ) as archive_classification:
            asyncio.run(scheduler._run_after_close_catchup())

        archive_classification.assert_awaited_once_with(trade_date)


if __name__ == "__main__":
    unittest.main()
