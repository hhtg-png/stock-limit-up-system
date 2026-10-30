import asyncio
import unittest
from datetime import date, datetime
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


class DynamicGroupSchedulerTests(unittest.IsolatedAsyncioTestCase):
    def test_browser_independent_jobs_registered(self):
        scheduler = DataScheduler(scheduler_factory=FakeScheduler)
        scheduler.start()
        jobs = {job["id"]: job for job in scheduler.scheduler.jobs}
        for key in ("dynamic_group_sample", "dynamic_group_history", "dynamic_group_startup"):
            self.assertIn(key, jobs)
            self.assertEqual(jobs[key]["max_instances"], 1)
            self.assertTrue(jobs[key]["coalesce"])
        self.assertEqual(jobs["dynamic_group_sample"]["trigger"].interval.total_seconds(), 30)

    async def test_sampling_uses_authoritative_day_and_session(self):
        from app.services.dynamic_group_service import dynamic_group_service
        from app.services.dynamic_group_source_service import dynamic_group_source_service
        for instant, actual, expected in ((datetime(2026, 10, 30, 10), date(2026, 10, 30), 1),
                (datetime(2026, 10, 30, 12), date(2026, 10, 30), 0),
                (datetime(2026, 10, 30, 15, 3), date(2026, 10, 30), 1),
                (datetime(2026, 10, 31, 10), date(2026, 10, 30), 0)):
            scheduler = DataScheduler(scheduler_factory=FakeScheduler, now_provider=lambda: CN_TZ.localize(instant))
            with patch.object(dynamic_group_service, "tick", AsyncMock()) as tick, \
                 patch.object(dynamic_group_source_service, "sessions", AsyncMock(return_value=(actual, date(2026, 10, 29)))):
                await scheduler._refresh_dynamic_groups()
                self.assertEqual(tick.await_count, expected)

    async def test_history_recovery_independent_of_active_groups_and_cancellation_propagates(self):
        from app.services.dynamic_group_history_service import dynamic_group_history_service
        scheduler = DataScheduler(scheduler_factory=FakeScheduler)
        with patch.object(dynamic_group_history_service, "tick", AsyncMock()) as tick:
            await scheduler._refresh_dynamic_group_history()
            tick.assert_awaited_once()
            tick.side_effect = asyncio.CancelledError()
            with self.assertRaises(asyncio.CancelledError):
                await scheduler._refresh_dynamic_group_history()
