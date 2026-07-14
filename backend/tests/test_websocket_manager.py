import asyncio
import unittest
from unittest.mock import AsyncMock

from app.core.websocket_manager import ConnectionManager


class WebSocketManagerTests(unittest.IsolatedAsyncioTestCase):
    async def test_slow_client_times_out_without_blocking_healthy_client(self):
        from unittest.mock import patch

        manager = ConnectionManager()
        slow_cancelled = asyncio.Event()

        class SlowWebSocket:
            async def accept(self):
                return None

            async def send_json(self, _message):
                try:
                    await asyncio.Event().wait()
                finally:
                    slow_cancelled.set()

        healthy = AsyncMock()
        await manager.connect(SlowWebSocket(), "slow")
        await manager.connect(healthy, "healthy")

        with patch(
            "app.core.websocket_manager.settings.WS_SEND_TIMEOUT_SECONDS",
            0.05,
        ):
            started = asyncio.get_running_loop().time()
            await manager.broadcast(
                {"id": 1},
                "trading_plan_alert",
            )
            elapsed = asyncio.get_running_loop().time() - started

        self.assertLess(elapsed, 0.5)
        healthy.send_json.assert_awaited_once()
        self.assertNotIn("slow", manager.active_connections)
        self.assertIn("healthy", manager.active_connections)
        self.assertTrue(slow_cancelled.is_set())

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
        self.assertIn("trading_plan_alert", manager.message_types["client-1"])

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

    async def test_broadcast_trading_plan_alert_uses_durable_inbox_message_type(self):
        manager = ConnectionManager()
        manager.broadcast = AsyncMock()
        payload = {"id": 7, "dedup_key": "plan:1:in_app:plan_ready"}

        await manager.broadcast_trading_plan_alert(payload)

        manager.broadcast.assert_awaited_once_with(
            payload,
            "trading_plan_alert",
        )

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
