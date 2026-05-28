import unittest
from datetime import date, datetime
from unittest.mock import AsyncMock, patch

from app.services.tdx_plugin_service import TdxPluginService


def make_limit_up_item(
    code,
    name,
    reason_category,
    *,
    sealed=True,
    status=None,
    board=1,
    open_count=0,
    first_time=None,
    final_time=None,
):
    return {
        "stock_code": code,
        "stock_name": name,
        "reason_category": reason_category,
        "limit_up_reason": f"{reason_category}催化",
        "is_sealed": sealed,
        "is_final_sealed": sealed,
        "current_status": status or ("sealed" if sealed else "opened"),
        "continuous_limit_up_days": board,
        "open_count": open_count,
        "first_limit_up_time": first_time or datetime(2026, 5, 28, 9, 35, 0),
        "final_seal_time": final_time or datetime(2026, 5, 28, 10, 12, 0),
        "seal_amount": 50000000,
        "amount": 800000000,
        "turnover_rate": 12.3,
        "industry": "计算机",
    }


class TdxPluginServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_limit_up_live_normalizes_events_and_response_shape(self):
        service = TdxPluginService()
        trade_date = date(2026, 5, 28)

        with patch.object(
            service.realtime_limit_up_service,
            "get_realtime_limit_up_list",
            AsyncMock(
                return_value=[
                    make_limit_up_item("001259", "利仁科技", "家电", board=7),
                    make_limit_up_item(
                        "002421",
                        "达实智能",
                        "AI应用",
                        sealed=False,
                        status="opened",
                        open_count=1,
                    ),
                    make_limit_up_item(
                        "603115",
                        "海星股份",
                        "机器人",
                        sealed=True,
                        status="resealed",
                        open_count=2,
                    ),
                ]
            ),
        ):
            payload = await service.get_limit_up_live(trade_date)

        self.assertEqual(payload["updated_at"][:10], "2026-05-28")
        self.assertFalse(payload["is_cache"])
        self.assertEqual(payload["source_status"]["limit_up_pool"], "ok")
        self.assertEqual(payload["items"][0]["stock_code"], "001259")
        self.assertEqual(payload["items"][0]["event_type"], "limit_up_sealed")
        self.assertEqual(payload["items"][0]["event_label"], "封死涨停")
        self.assertEqual(payload["items"][1]["event_label"], "涨停打开")
        self.assertEqual(payload["items"][2]["event_label"], "涨停回封")

    async def test_plate_strength_groups_limit_up_items_into_ranked_board(self):
        service = TdxPluginService()

        with patch.object(
            service.realtime_limit_up_service,
            "get_realtime_limit_up_list",
            AsyncMock(
                return_value=[
                    make_limit_up_item("001259", "利仁科技", "家电", board=7),
                    make_limit_up_item("603311", "金海高科", "家电", board=3),
                    make_limit_up_item("002421", "达实智能", "AI应用", board=3, sealed=False),
                ]
            ),
        ):
            payload = await service.get_plate_strength(date(2026, 5, 28))

        self.assertEqual(payload["items"][0]["plate_name"], "家电")
        self.assertEqual(payload["items"][0]["limit_up_count"], 2)
        self.assertEqual(payload["items"][0]["sealed_count"], 2)
        self.assertEqual(payload["items"][0]["max_board"], 7)
        self.assertEqual(payload["items"][0]["core_stocks"][0]["stock_name"], "利仁科技")
        self.assertGreater(payload["items"][0]["strength_score"], payload["items"][1]["strength_score"])

    async def test_stock_move_combines_limit_up_reason_and_metadata(self):
        service = TdxPluginService()

        with patch.object(
            service.realtime_limit_up_service,
            "get_realtime_limit_up_item",
            AsyncMock(return_value=make_limit_up_item("001259", "利仁科技", "家电", board=7)),
        ):
            payload = await service.get_stock_move("001259", date(2026, 5, 28), source_scope="mixed")

        self.assertEqual(payload["items"][0]["stock_code"], "001259")
        self.assertEqual(payload["items"][0]["stock_name"], "利仁科技")
        self.assertEqual(payload["items"][0]["source_scope"], "mixed")
        self.assertEqual(payload["items"][0]["latest_limit_up"]["board"], 7)
        self.assertIn("家电催化", payload["items"][0]["reasons"][0]["content"])
        self.assertEqual(payload["source_status"]["stock_move"], "ok")

    async def test_ths_move_marks_ths_only_scope(self):
        service = TdxPluginService()

        with patch.object(
            service.realtime_limit_up_service,
            "get_realtime_limit_up_item",
            AsyncMock(return_value=make_limit_up_item("001259", "利仁科技", "家电", board=7)),
        ):
            payload = await service.get_stock_move("001259", date(2026, 5, 28), source_scope="ths")

        self.assertEqual(payload["items"][0]["source_scope"], "ths")
        self.assertEqual(payload["items"][0]["sources"], ["同花顺"])

    def test_compare_samples_reports_missing_extra_field_and_order_differences(self):
        service = TdxPluginService()

        payload = service.compare_samples(
            target_items=[
                {"stock_code": "001259", "stock_name": "利仁科技", "event_label": "封死涨停"},
                {"stock_code": "002421", "stock_name": "达实智能", "event_label": "涨停打开"},
            ],
            ours_items=[
                {"stock_code": "002421", "stock_name": "达实智能", "event_label": "封死涨停"},
                {"stock_code": "603311", "stock_name": "金海高科", "event_label": "封死涨停"},
            ],
            key_field="stock_code",
        )

        self.assertEqual(payload["summary"]["target_count"], 2)
        self.assertEqual(payload["summary"]["ours_count"], 2)
        self.assertEqual(payload["missing_items"][0]["stock_code"], "001259")
        self.assertEqual(payload["extra_items"][0]["stock_code"], "603311")
        self.assertEqual(payload["field_diffs"][0]["field"], "event_label")
        self.assertEqual(payload["order_diffs"][0]["key"], "002421")


if __name__ == "__main__":
    unittest.main()
