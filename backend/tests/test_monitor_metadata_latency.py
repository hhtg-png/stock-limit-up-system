import asyncio
import time
import unittest
from datetime import date, datetime
from unittest.mock import AsyncMock, patch

import app.services.realtime_limit_up_service as realtime_module
from app.services.limit_up_context_service import LimitUpContextService
from app.services.realtime_limit_up_service import RealtimeLimitUpService


DAY = date(2026, 10, 23)
NEXT_DAY = date(2026, 10, 26)
ROW = {"stock_code": "000001", "stock_name": "Alpha", "limit_up_price": 11,
       "limit_up_reason": "银行", "is_final_sealed": True}
QUOTES = {"000001": {"datetime": "20261023100000", "price": 11, "open": 10,
                     "pre_close": 10, "change_pct": 10, "amount": 100}}


async def no_history(*args):
    return {}


async def no_pool(*args):
    return []


class MonitorMetadataLatencyTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.service = RealtimeLimitUpService()
        self.service.get_fast_limit_up_pool = AsyncMock(side_effect=lambda *args, **kwargs: [dict(ROW)])
        context = LimitUpContextService(calendar_loader=lambda *args: [date(2026, 10, 22), DAY, NEXT_DAY],
                                       session_factory=None, archive_service=None, history_fetcher=no_history,
                                       pool_fetcher=no_pool, today_provider=lambda: DAY,
                                       now_provider=lambda: datetime(2026, 10, 23, 10))
        patches = [patch("app.services.limit_up_context_service.limit_up_context_service", context),
                   patch.object(realtime_module.tencent_api, "get_quotes_batch", AsyncMock(return_value=QUOTES)),
                   patch.object(realtime_module.ths_crawler, "close_client", AsyncMock())]
        for patcher in patches:
            patcher.start()
            self.addCleanup(patcher.stop)

    async def test_monitor_returns_quotes_while_cold_metadata_waits_then_uses_refreshed_values(self):
        release = asyncio.Event()

        async def reasons():
            await release.wait()
            return [{"stock_code": "000001", "limit_up_reason": "机器人"}]

        async def shares(*args):
            await release.wait()
            return {"000001": 100}

        with patch.object(realtime_module.ths_crawler, "crawl", AsyncMock(side_effect=reasons)) as reason_fetch, \
             patch.object(realtime_module.tradable_market_value_service, "get_float_share_map", AsyncMock(side_effect=shares)) as share_fetch:
            try:
                first = await asyncio.wait_for(self.service.get_realtime_limit_up_list(DAY, include_monitor_context=True), 0.2)
                self.assertEqual(first[0]["current_price"], 11)
                self.assertEqual(first[0]["today_open_change_pct"], 0)
                self.assertEqual(first[0]["limit_up_reason"], "银行")
                self.assertIsNone(first[0]["tradable_market_value"])
                repeated = await asyncio.wait_for(self.service.get_realtime_limit_up_list(DAY, include_monitor_context=True), 0.2)
                self.assertIsNone(repeated[0]["tradable_market_value"])
                reason_fetch.assert_awaited_once()
                share_fetch.assert_awaited_once()
            finally:
                release.set()
                await asyncio.gather(*(task for task in (
                    self.service._ths_reason_refresh_task,
                    getattr(self.service, "_monitor_float_share_task", None),
                ) if task), return_exceptions=True)
            updated = await self.service.get_realtime_limit_up_list(DAY, include_monitor_context=True)
        self.assertEqual(updated[0]["tradable_market_value"], 1100)
        self.assertEqual(updated[0]["limit_up_reason"], "机器人")
        share_fetch.assert_awaited_once()
        reason_fetch.assert_awaited_once()

    async def test_monitor_uses_cached_reasons_immediately_when_refresh_is_stale(self):
        self.service._ths_reason_cache = {"000001": "锂电池"}
        self.service._ths_reason_cache_time = time.time() - 301
        release = asyncio.Event()

        async def held():
            await release.wait()
            return []

        with patch.object(realtime_module.ths_crawler, "crawl", AsyncMock(side_effect=held)), \
             patch.object(realtime_module.tradable_market_value_service, "get_float_share_map", AsyncMock(return_value={})):
            try:
                result = await asyncio.wait_for(self.service.get_realtime_limit_up_list(DAY, include_monitor_context=True), 0.2)
                self.assertEqual(result[0]["limit_up_reason"], "锂电池")
                self.assertEqual(result[0]["today_open_change_pct"], 0)
            finally:
                release.set()
                await asyncio.gather(*(task for task in (
                    self.service._ths_reason_refresh_task,
                    getattr(self.service, "_monitor_float_share_task", None),
                ) if task), return_exceptions=True)

    async def test_share_fetcher_is_bound_before_background_task_can_run(self):
        restored = AsyncMock(side_effect=AssertionError("must not call a later replacement provider"))
        captured = AsyncMock(return_value={"000001": 100})
        with patch.object(realtime_module.tradable_market_value_service, "get_float_share_map", restored):
            with patch.object(realtime_module.tradable_market_value_service, "get_float_share_map", captured):
                cached = self.service._get_monitor_float_share_map(DAY, ["000001"])
            self.assertEqual(cached, {})
            await self.service._monitor_float_share_task
            cached = self.service._get_monitor_float_share_map(DAY, ["000001"])
        self.assertEqual(cached, {"000001": 100})
        captured.assert_awaited_once_with(DAY, ["000001"])
        restored.assert_not_awaited()

    async def test_expired_reasons_do_not_replace_current_pool_reason(self):
        self.service._ths_reason_cache = {"000001": "过期原因"}
        self.service._ths_reason_cache_time = time.time() - 1801
        with patch.object(self.service, "_ensure_reason_refresh") as refresh, \
             patch.object(realtime_module.tradable_market_value_service, "get_float_share_map", AsyncMock(return_value={})):
            result = await self.service.get_realtime_limit_up_list(DAY, include_monitor_context=True)
            pending = self.service._monitor_float_share_task
            if pending:
                await pending
        self.assertEqual(result[0]["limit_up_reason"], "银行")
        refresh.assert_called_once()

    async def test_empty_new_day_cancels_old_metadata_refresh(self):
        entered = asyncio.Event()

        async def shares(*args):
            entered.set()
            await asyncio.Event().wait()

        with patch.object(realtime_module.tradable_market_value_service, "get_float_share_map", AsyncMock(side_effect=shares)), \
             patch.object(self.service, "_ensure_reason_refresh"):
            self.service._get_monitor_float_share_map(DAY, ["000001"])
            pending = self.service._monitor_float_share_task
            await asyncio.wait_for(entered.wait(), 0.2)
            self.service.get_fast_limit_up_pool = AsyncMock(return_value=[])
            try:
                result = await self.service.get_realtime_limit_up_list(NEXT_DAY, include_monitor_context=True)
                await asyncio.sleep(0)
                self.assertEqual(result, [])
                self.assertTrue(pending.cancelled())
                self.assertEqual(self.service._monitor_float_share_date, NEXT_DAY)
            finally:
                pending.cancel()
                await asyncio.gather(pending, return_exceptions=True)

    async def test_day_rollover_cancels_old_refresh_and_does_not_leak_previous_values(self):
        old_started, old_cancelled, new_release = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def shares(day, codes):
            if day == DAY and codes == ["000001"]:
                return {"000001": 100}
            if day == DAY:
                old_started.set()
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    old_cancelled.set()
                    return {"000002": 999}
            await new_release.wait()
            return {"000001": 200}

        with patch.object(realtime_module.tradable_market_value_service, "get_float_share_map", AsyncMock(side_effect=shares)):
            self.service._get_monitor_float_share_map(DAY, ["000001"])
            await self.service._monitor_float_share_task
            self.service._get_monitor_float_share_map(DAY, ["000001", "000002"])
            old_task = self.service._monitor_float_share_task
            await asyncio.wait_for(old_started.wait(), 0.2)
            result = self.service._get_monitor_float_share_map(NEXT_DAY, ["000001"])
            self.assertEqual(result, {})
            await asyncio.wait_for(old_cancelled.wait(), 0.2)
            await old_task
            new_task = self.service._monitor_float_share_task
            new_release.set()
            await new_task
            result = self.service._get_monitor_float_share_map(NEXT_DAY, ["000001"])
        self.assertEqual(result, {"000001": 200})

    async def test_plain_consumers_keep_waiting_for_metadata(self):
        entered, release = asyncio.Event(), asyncio.Event()

        async def shares(*args):
            entered.set()
            await release.wait()
            return {"000001": 100}

        with patch.object(self.service, "_fetch_ths_reason_map", AsyncMock(return_value={})), \
             patch.object(realtime_module.tradable_market_value_service, "get_float_share_map", AsyncMock(side_effect=shares)):
            pending = asyncio.create_task(self.service.get_realtime_limit_up_list(DAY))
            await asyncio.wait_for(entered.wait(), 0.2)
            self.assertFalse(pending.done())
            release.set()
            result = await pending
        self.assertEqual(result[0]["tradable_market_value"], 1100)
        self.assertNotIn("today_open_change_pct", result[0])


if __name__ == "__main__":
    unittest.main()
