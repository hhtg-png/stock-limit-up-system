import unittest
from datetime import date
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
        self.assertEqual(payload["event_type"], "limit_up_touched")
        self.assertEqual(payload["event_label"], "摸板")
        self.assertEqual(payload["target_status_label"], "7板")
        self.assertEqual(payload["speech_text"], "利仁科技7板，家电催化")
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

    async def test_hot_limit_up_tick_uses_nonblocking_fast_pool_for_sub_second_alerts(self):
        trade_date = date(2026, 6, 1)
        alert = {
            "stock_code": "001259",
            "stock_name": "利仁科技",
            "time": "09:35:00",
            "reason": "家电",
            "continuous_days": 7,
        }

        with patch.object(
            websocket_api.realtime_limit_up_service,
            "get_fast_limit_up_pool",
            AsyncMock(return_value=[alert]),
        ) as fast_pool, patch.object(
            websocket_api.realtime_limit_up_service,
            "get_realtime_limit_up_list",
            AsyncMock(),
        ) as rich_list, patch.object(
            websocket_api.realtime_alert_tracker,
            "collect_new_alerts",
            return_value=[alert],
        ) as collect_alerts, patch.object(
            websocket_api.manager,
            "broadcast_limit_up_alert",
            AsyncMock(),
        ) as broadcast_alert, patch.object(
            websocket_api,
            "broadcast_tdx_limit_up_event",
            AsyncMock(),
        ) as broadcast_tdx, patch.object(
            websocket_api,
            "schedule_tdx_stock_move_cache_refresh",
        ) as schedule_cache_refresh:
            alert_count = await websocket_api.process_realtime_hot_limit_up_tick(trade_date)

        self.assertLess(websocket_api.REALTIME_HOT_SYNC_INTERVAL, 1)
        self.assertLessEqual(websocket_api.REALTIME_HOT_POOL_MAX_CACHE_AGE, 0.3)
        self.assertEqual(alert_count, 1)
        fast_pool.assert_awaited_once()
        self.assertFalse(fast_pool.await_args.kwargs["wait_for_refresh"])
        self.assertEqual(
            fast_pool.await_args.kwargs["max_cache_age"],
            websocket_api.REALTIME_HOT_POOL_MAX_CACHE_AGE,
        )
        rich_list.assert_not_called()
        collect_alerts.assert_called_once_with([alert], trade_date)
        broadcast_alert.assert_awaited_once()
        broadcast_tdx.assert_awaited_once_with(alert)
        schedule_cache_refresh.assert_called_once_with(alert, trade_date)


if __name__ == "__main__":
    unittest.main()
