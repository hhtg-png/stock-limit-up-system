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
        payload = broadcast.await_args.args[1]
        self.assertEqual(payload["event_label"], "封死涨停")
        self.assertEqual(payload["target_status_label"], "7板")
        self.assertEqual(payload["speech_text"], "利仁科技7板")
        self.assertNotIn("封死涨停", payload["speech_text"])
        self.assertEqual(payload["stock_code"], "001259")
        self.assertEqual(broadcast.await_args.kwargs["stock_code"], "001259")

    async def test_broadcast_tdx_news_event_uses_plugin_payload_without_stock_filter(self):
        item = {
            "news_id": "glh-2474876",
            "source": "格隆汇",
            "time": "17:58:00",
            "title": "罗博特科：股东减持股份",
            "content": "格隆汇正文",
            "importance": 70,
        }

        with patch.object(
            websocket_api.manager,
            "broadcast_tdx_plugin_event",
            AsyncMock(),
        ) as broadcast:
            await websocket_api.broadcast_tdx_news_event(item)

        self.assertEqual(broadcast.await_args.args[0], "tdx_news_event")
        payload = broadcast.await_args.args[1]
        self.assertEqual(payload["news_id"], "glh-2474876")
        self.assertEqual(payload["speech_text"], "罗博特科：股东减持股份")
        self.assertIsNone(broadcast.await_args.kwargs["stock_code"])

    async def test_warm_tdx_news_speech_cache_uses_title_only(self):
        items = [
            {"news_id": "ths-1", "source": "同花顺", "title": "第一条标题", "content": "正文"},
            {"news_id": "jygs-1", "source": "韭研公社", "title": "第二条标题", "content": "正文"},
        ]

        with patch.object(
            websocket_api.edge_tts_service,
            "synthesize_to_file",
            AsyncMock(),
        ) as synthesize:
            await websocket_api.warm_tdx_news_speech_cache(items)

        self.assertEqual(synthesize.await_args_list[0].args[0], "第一条标题")
        self.assertEqual(synthesize.await_args_list[1].args[0], "韭研公社新帖，第二条标题")


if __name__ == "__main__":
    unittest.main()
