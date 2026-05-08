import unittest
import sys
import types

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


if __name__ == "__main__":
    unittest.main()
