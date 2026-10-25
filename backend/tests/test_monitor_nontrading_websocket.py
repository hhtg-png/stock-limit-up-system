import asyncio
import unittest
from datetime import date, datetime
from unittest.mock import AsyncMock, patch

from app.api.v1 import websocket as websocket_api


SUNDAY = date(2026, 10, 25)
FRIDAY = date(2026, 10, 23)


class MonitorNontradingWebsocketTests(unittest.IsolatedAsyncioTestCase):
    async def test_closed_session_does_not_send_cached_mislabeled_snapshot(self):
        stale = {"type": "limit_up_snapshot", "data": {"items": [{"stock_code": "600838"}]}}
        with patch.object(websocket_api, "is_a_share_trading_day", AsyncMock(return_value=False), create=True), \
             patch.object(websocket_api.realtime_stream_tracker, "get_cached_snapshot", return_value=stale), \
             patch.object(websocket_api.manager, "send_personal_message", AsyncMock()) as send:
            await websocket_api.send_realtime_limit_up_snapshot("test-client")
        send.assert_not_awaited()

    async def test_closed_session_does_not_publish_rich_rows_as_current(self):
        with patch.object(websocket_api, "is_a_share_trading_day", AsyncMock(return_value=False), create=True), \
             patch.object(websocket_api.realtime_limit_up_service, "get_realtime_limit_up_list", AsyncMock(return_value=[])) as fetch, \
             patch.object(websocket_api.realtime_stream_tracker, "sync", return_value={"type": "limit_up_snapshot"}), \
             patch.object(websocket_api, "broadcast_realtime_sync_message", AsyncMock()) as broadcast:
            await websocket_api.process_realtime_rich_limit_up_sync(SUNDAY)
        fetch.assert_not_awaited()
        broadcast.assert_not_awaited()

    async def test_trading_day_rich_sync_preserves_context(self):
        rows = [{"stock_code": "600838", "today_open_change_pct": 10, "yesterday_open_change_pct": 0.55}]
        message = {"type": "limit_up_snapshot", "data": {"trade_date": FRIDAY.isoformat(), "items": rows}}
        with patch.object(websocket_api, "is_a_share_trading_day", AsyncMock(return_value=True), create=True), \
             patch.object(websocket_api.realtime_limit_up_service, "get_realtime_limit_up_list", AsyncMock(return_value=rows)), \
             patch.object(websocket_api.realtime_stream_tracker, "sync", return_value=message) as sync, \
             patch.object(websocket_api, "broadcast_realtime_sync_message", AsyncMock()) as broadcast:
            await websocket_api.process_realtime_rich_limit_up_sync(FRIDAY)
        sync.assert_called_once_with(rows, FRIDAY)
        broadcast.assert_awaited_once_with(message)

    async def test_weekday_holiday_loop_never_polls_or_alerts(self):
        holiday = date(2026, 10, 1)
        with patch.object(websocket_api, "is_a_share_trading_day", AsyncMock(return_value=False), create=True), \
             patch.object(websocket_api, "today_cn", return_value=holiday, create=True), \
             patch.object(websocket_api, "is_trading_time", return_value=True), \
             patch.object(type(websocket_api.manager), "connection_count", property(lambda self: 1)), \
             patch.object(websocket_api, "process_realtime_hot_limit_up_tick", AsyncMock(return_value=0)) as hot, \
             patch.object(websocket_api, "process_realtime_rich_limit_up_sync", AsyncMock()) as rich, \
             patch.object(websocket_api.asyncio, "sleep", AsyncMock(side_effect=asyncio.CancelledError)):
            with self.assertRaises(asyncio.CancelledError):
                await websocket_api.realtime_sync_loop()
        hot.assert_not_awaited()
        rich.assert_not_awaited()

    def test_weekend_clock_inside_session_hours_is_still_closed(self):
        clock = datetime(2026, 10, 25, 10)
        with patch.object(websocket_api, "datetime") as dt, \
             patch.object(websocket_api, "now_cn", return_value=clock, create=True):
            dt.now.return_value = clock
            self.assertFalse(websocket_api.is_trading_time())

    def test_trading_weekday_clock_inside_session_hours_remains_open(self):
        clock = datetime(2026, 10, 23, 10)
        with patch.object(websocket_api, "datetime") as dt, \
             patch.object(websocket_api, "now_cn", return_value=clock, create=True):
            dt.now.return_value = clock
            self.assertTrue(websocket_api.is_trading_time())


if __name__ == "__main__":
    unittest.main()
