import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from app.data_collectors.scheduler import DataScheduler
from app.utils.time_utils import CN_TZ


class FakeScheduler:
    def __init__(self):
        self.jobs = []
    def add_job(self, func, trigger, **options):
        self.jobs.append(dict(func=func, trigger=trigger, **options))
    def start(self):
        pass
    def shutdown(self, **kwargs):
        pass


class HistorySchedulerTests(unittest.IsolatedAsyncioTestCase):
    def test_daily_repair_startup_and_bounded_reconciliation_are_registered(self):
        scheduler = DataScheduler(scheduler_factory=FakeScheduler)
        scheduler.start()
        jobs = {job["id"]: job for job in scheduler.scheduler.jobs}
        for key in ("history_daily_1535", "history_daily_2035", "history_startup_backfill", "history_backfill"):
            self.assertIn(key, jobs)
            self.assertEqual(jobs[key]["max_instances"], 1)
            self.assertTrue(jobs[key]["coalesce"])
        for key, hour in (("history_daily_1535", 15), ("history_daily_2035", 20)):
            trigger = jobs[key]["trigger"]
            self.assertEqual(int(str(trigger.fields[5])), hour)
            self.assertEqual(int(str(trigger.fields[6])), 35)
            self.assertEqual(getattr(trigger.timezone, "zone", None), getattr(CN_TZ, "zone", None))
        self.assertEqual(jobs["history_backfill"]["trigger"].interval.total_seconds(), 300)

    def test_no_legacy_obsidian_background_work_is_registered(self):
        scheduler = DataScheduler(scheduler_factory=FakeScheduler)
        # Even stale runtime state from an earlier version must not restore retired jobs.
        scheduler._trading_playbook_obsidian_sync = object()
        scheduler.start()
        ids = {job["id"] for job in scheduler.scheduler.jobs}
        self.assertFalse(any("obsidian" in key for key in ids))
        self.assertIn("trading_playbook_monitor", ids)

    async def test_daily_and_reconciliation_handlers_use_persistent_preparation_and_propagate_cancel(self):
        scheduler = DataScheduler(scheduler_factory=FakeScheduler)
        with patch("app.data_collectors.scheduler.history_preparation_service", create=True) as service:
            service.prepare_latest = AsyncMock(return_value={"monitor_ready": True})
            service.prepare_pending = AsyncMock(return_value={"dates": []})
            await scheduler._prepare_daily_history()
            await scheduler._reconcile_history_snapshots()
            service.prepare_latest.assert_awaited_once()
            service.prepare_pending.assert_awaited_once_with(limit=2)
            service.prepare_pending.side_effect = asyncio.CancelledError()
            with self.assertRaises(asyncio.CancelledError):
                await scheduler._reconcile_history_snapshots()


if __name__ == "__main__":
    unittest.main()
