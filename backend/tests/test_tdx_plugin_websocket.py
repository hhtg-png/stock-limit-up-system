import unittest
from unittest.mock import AsyncMock, patch

from app.api.v1 import websocket as websocket_api


class TdxPluginWebSocketTests(unittest.IsolatedAsyncioTestCase):
    async def test_broadcast_tdx_limit_up_event_uses_plugin_payload(self):
        alert = {
            "stock_code": "001259",
            "stock_name": "利仁科技",
            "time": "09:35:00",
            "reason": "家电催化",
            "continuous_days": 7,
        }

        with patch.object(
            websocket_api.manager,
            "broadcast_tdx_plugin_event",
            AsyncMock(),
        ) as broadcast:
            await websocket_api.broadcast_tdx_limit_up_event(alert)

        self.assertEqual(broadcast.await_args.args[0], "tdx_limit_up_event")
        self.assertEqual(broadcast.await_args.args[1]["event_label"], "封死涨停")
        self.assertEqual(broadcast.await_args.args[1]["stock_code"], "001259")
        self.assertEqual(broadcast.await_args.kwargs["stock_code"], "001259")


if __name__ == "__main__":
    unittest.main()
