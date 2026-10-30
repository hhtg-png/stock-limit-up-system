"""Final archive commits must refresh history-based groups after sampling ends."""
import unittest
from datetime import date, datetime
from unittest.mock import AsyncMock, patch

from app.data_collectors.scheduler import DataScheduler
from app.utils.time_utils import CN_TZ


class StoredScheduler:
    def __init__(self):
        self.jobs = []

    def add_job(self, func, trigger, **kwargs):
        self.jobs.append({"func": func, "trigger": trigger, **kwargs})


class DynamicGroupArchiveRefreshTests(unittest.IsolatedAsyncioTestCase):
    def scheduler(self):
        return DataScheduler(scheduler_factory=StoredScheduler,
            now_provider=lambda: CN_TZ.localize(datetime(2026, 10, 30, 20, 15)))

    async def test_final_build_and_repair_schedule_after_the_archive_finishes(self):
        from app.data_collectors.scheduler import market_review_pipeline_service
        for action in ("_build_market_review", "_repair_market_review"):
            with self.subTest(action=action):
                scheduler = self.scheduler()
                async def build(*args, **kwargs):
                    self.assertEqual(scheduler.scheduler.jobs, [])
                with patch.object(market_review_pipeline_service, "run_for_date", AsyncMock(side_effect=build)), \
                     patch("app.data_collectors.scheduler._resolve_cn_trade_date_for_market_review", return_value=date(2026, 10, 30)):
                    await getattr(scheduler, action)()
                self.assertEqual(len(scheduler.scheduler.jobs), 1)
                job = scheduler.scheduler.jobs[0]
                self.assertEqual(job["id"], "dynamic_group_archive_refresh")
                self.assertTrue(job["replace_existing"])
                self.assertTrue(job["coalesce"])
                self.assertEqual(job["max_instances"], 1)
                self.assertGreater(job["trigger"].run_date,
                    CN_TZ.localize(datetime(2026, 10, 30, 20, 15)))

    async def test_failed_final_build_does_not_schedule_a_success_refresh(self):
        from app.data_collectors.scheduler import market_review_pipeline_service
        scheduler = self.scheduler()
        with patch.object(market_review_pipeline_service, "run_for_date", AsyncMock(side_effect=RuntimeError("failed"))), \
             patch("app.data_collectors.scheduler._resolve_cn_trade_date_for_market_review", return_value=date(2026, 10, 30)):
            with self.assertRaises(RuntimeError):
                await scheduler._build_market_review()
        self.assertEqual(scheduler.scheduler.jobs, [])

    async def test_reconciliation_only_schedules_after_a_recovered_commit(self):
        from app.data_collectors.scheduler import market_review_pipeline_service
        scheduler = self.scheduler()
        with patch("app.data_collectors.scheduler._should_run_after_close_catchup", return_value=True), \
             patch("app.data_collectors.scheduler._resolve_latest_cn_trade_date_for_market_review", return_value=date(2026, 10, 30)), \
             patch.object(market_review_pipeline_service, "has_after_close_data", AsyncMock(side_effect=[False, True])), \
             patch.object(market_review_pipeline_service, "run_for_date", AsyncMock()):
            await scheduler._reconcile_market_review()
            await scheduler._reconcile_market_review()
        self.assertEqual(len(scheduler.scheduler.jobs), 1)
        self.assertEqual(scheduler.scheduler.jobs[0]["id"], "dynamic_group_archive_refresh")
