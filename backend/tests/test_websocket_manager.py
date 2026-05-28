import unittest
from unittest.mock import AsyncMock

from app.core.websocket_manager import ConnectionManager


class WebSocketManagerTests(unittest.IsolatedAsyncioTestCase):
    async def test_connect_subscribes_clients_to_realtime_list_sync_messages(self):
        manager = ConnectionManager()
        websocket = AsyncMock()

        await manager.connect(websocket, "client-1")

        self.assertIn("limit_up_snapshot", manager.message_types["client-1"])
        self.assertIn("limit_up_delta", manager.message_types["client-1"])
        self.assertIn("tdx_limit_up_event", manager.message_types["client-1"])
        self.assertIn("tdx_stock_move_event", manager.message_types["client-1"])
        self.assertIn("tdx_news_event", manager.message_types["client-1"])
        self.assertIn("tdx_plate_strength_update", manager.message_types["client-1"])

    async def test_broadcast_tdx_plugin_event_uses_plugin_message_type(self):
        manager = ConnectionManager()
        manager.broadcast = AsyncMock()

        await manager.broadcast_tdx_plugin_event(
            "tdx_limit_up_event",
            {"stock_code": "001259", "stock_name": "利仁科技", "event_label": "封死涨停"},
            stock_code="001259",
        )

        self.assertEqual(manager.broadcast.await_args.args[1], "tdx_limit_up_event")
        self.assertEqual(manager.broadcast.await_args.args[2], "001259")

    async def test_broadcast_limit_up_alert_deduplicates_same_stock(self):
        manager = ConnectionManager()
        manager.broadcast = AsyncMock()

        await manager.broadcast_limit_up_alert(
            "000001",
            "平安银行",
            "10:05:00",
            "联调验证",
            1,
        )
        await manager.broadcast_limit_up_alert(
            "000001",
            "平安银行",
            "10:05:01",
            "联调验证",
            1,
        )

        self.assertEqual(manager.broadcast.await_count, 1)
        payload = manager.broadcast.await_args_list[0].args[0]
        self.assertEqual(payload["stock_code"], "000001")
        self.assertEqual(payload["reason"], "联调验证")


if __name__ == "__main__":
    unittest.main()
