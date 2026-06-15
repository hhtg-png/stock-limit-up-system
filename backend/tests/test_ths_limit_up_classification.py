import unittest
from datetime import date, datetime
from unittest.mock import AsyncMock, patch

from app.api.v1 import limit_up
from app.services.ths_limit_up_classification_service import ThsLimitUpClassificationService


class FakeRealtimeLimitUpService:
    def __init__(self, items):
        self.items = items
        self.requested_dates = []

    async def get_realtime_limit_up_list(self, trade_date):
        self.requested_dates.append(trade_date)
        return self.items


class FakeRowsResult:
    def __init__(self, rows):
        self.rows = rows

    def all(self):
        return self.rows


class SequencedSession:
    def __init__(self, results):
        self.results = list(results)
        self.queries = []

    async def execute(self, query):
        self.queries.append(query)
        return self.results.pop(0)


def make_realtime_item(
    code,
    name,
    reason,
    *,
    first_time,
    final_time=None,
    sealed=True,
    board=1,
    open_count=0,
):
    return {
        "stock_code": code,
        "stock_name": name,
        "limit_up_reason": reason,
        "reason_category": "其他",
        "first_limit_up_time": first_time,
        "final_seal_time": final_time,
        "continuous_limit_up_days": board,
        "open_count": open_count,
        "is_sealed": sealed,
        "is_final_sealed": sealed,
        "current_status": "sealed" if sealed else "opened",
        "seal_amount": 12880.0,
        "turnover_rate": 7.2,
        "amount": 186000.0,
    }


class ThsLimitUpClassificationServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_groups_realtime_items_by_strict_ths_reason_and_preserves_seal_times(self):
        trade_date = date(2026, 6, 15)
        service = ThsLimitUpClassificationService(
            realtime_service=FakeRealtimeLimitUpService(
                [
                    make_realtime_item(
                        "603000",
                        "人民网",
                        "AI大模型+数据要素",
                        first_time=datetime(2026, 6, 15, 9, 31, 20),
                        final_time=datetime(2026, 6, 15, 9, 45, 5),
                        board=2,
                    ),
                    make_realtime_item(
                        "002230",
                        "科大讯飞",
                        "人工智能+机器人",
                        first_time=datetime(2026, 6, 15, 10, 2, 1),
                        sealed=False,
                        open_count=1,
                    ),
                    make_realtime_item(
                        "300750",
                        "宁德时代",
                        "锂电池+储能",
                        first_time=datetime(2026, 6, 15, 9, 40, 0),
                    ),
                ]
            )
        )

        with patch(
            "app.services.tdx_plugin_service.tdx_plugin_service.get_limit_up_live",
            AsyncMock(side_effect=AssertionError("classification must not use TDX attribution")),
        ) as tdx_live:
            payload = await service.get_classification(trade_date)

        tdx_live.assert_not_called()
        self.assertEqual(payload["requested_date"], trade_date)
        self.assertEqual(payload["trade_date"], trade_date)
        self.assertFalse(payload["is_fallback"])
        self.assertEqual(payload["source_status"]["limit_up_pool"], "ok")
        self.assertEqual(payload["source_status"]["classification_scope"], "strict_ths")
        self.assertEqual(payload["total_count"], 3)

        ai_group = payload["groups"][0]
        self.assertEqual(ai_group["plate_name"], "人工智能")
        self.assertEqual(ai_group["count"], 2)
        self.assertEqual(ai_group["sealed_count"], 1)
        self.assertEqual(ai_group["opened_count"], 1)
        self.assertEqual(ai_group["earliest_first_limit_time"], "09:31:20")
        self.assertEqual(ai_group["latest_first_limit_time"], "10:02:01")
        self.assertEqual([stock["stock_code"] for stock in ai_group["stocks"]], ["603000", "002230"])
        self.assertEqual(ai_group["stocks"][0]["first_limit_up_time"], "09:31:20")
        self.assertEqual(ai_group["stocks"][0]["final_seal_time"], "09:45:05")
        self.assertEqual(ai_group["stocks"][1]["current_status"], "opened")
        self.assertEqual(ai_group["stocks"][1]["final_seal_time"], "")

        new_energy_group = payload["groups"][1]
        self.assertEqual(new_energy_group["plate_name"], "新能源")
        self.assertEqual(new_energy_group["stocks"][0]["classified_plate"], "新能源")

    async def test_falls_back_to_database_records_when_realtime_pool_is_empty(self):
        requested_date = date(2026, 6, 16)
        service = ThsLimitUpClassificationService(
            realtime_service=FakeRealtimeLimitUpService([])
        )
        db = SequencedSession(
            [
                FakeRowsResult(
                    [
                        (
                            "603019",
                            "中科曙光",
                            date(2026, 6, 14),
                            datetime(2026, 6, 14, 9, 35, 0),
                            datetime(2026, 6, 14, 10, 8, 30),
                            "AI算力+服务器",
                            3,
                            0,
                            True,
                            "sealed",
                            9200.0,
                            6.5,
                            208000.0,
                        )
                    ]
                )
            ]
        )

        payload = await service.get_classification(requested_date, db=db)

        self.assertEqual(payload["requested_date"], requested_date)
        self.assertEqual(payload["trade_date"], date(2026, 6, 14))
        self.assertTrue(payload["is_fallback"])
        self.assertEqual(payload["source_status"]["limit_up_pool"], "empty")
        self.assertEqual(payload["source_status"]["limit_up_db"], "ok")
        self.assertEqual(payload["groups"][0]["plate_name"], "人工智能")
        self.assertEqual(payload["groups"][0]["stocks"][0]["first_limit_up_time"], "09:35:00")
        self.assertEqual(payload["groups"][0]["stocks"][0]["final_seal_time"], "10:08:30")

        query_text = str(db.queries[0])
        self.assertIn("limit_up_records.trade_date <= :trade_date_1", query_text)


class ThsLimitUpClassificationApiTests(unittest.IsolatedAsyncioTestCase):
    async def test_classification_route_is_registered_before_stock_code_route(self):
        paths = [route.path for route in limit_up.router.routes]

        self.assertIn("/classification", paths)
        self.assertLess(paths.index("/classification"), paths.index("/{stock_code}"))


if __name__ == "__main__":
    unittest.main()
