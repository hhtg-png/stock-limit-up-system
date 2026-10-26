import unittest
from datetime import date
from unittest.mock import AsyncMock, patch

from app.api.v1 import limit_up
from app.schemas.limit_up import LimitUpRealtime
from app.services.limit_up_context_service import limit_up_context_service
from test_limit_up_detail_api import FakeAllResult, SequencedSession, make_record, make_stock


DAY, PREVIOUS = date(2026, 10, 23), date(2026, 10, 22)


def item(code, final):
    return LimitUpRealtime(stock_code=code, stock_name="Alpha", trade_date=DAY,
                           limit_up_price=11, current_price=11, market="SZ",
                           yesterday_final_seal_time=final)


class YesterdaySealSortingTests(unittest.TestCase):
    def test_default_and_explicit_time_directions_keep_missing_last_and_code_ties_stable(self):
        values = [("000009", None), ("000004", "10:52:37"), ("000003", "09:25:00"),
                  ("000002", "09:25:00"), ("000008", ""), ("000007", "25:00:00"),
                  ("000006", "   "), ("000001", "00:00:00")]
        expected = {
            None: ["000001", "000002", "000003", "000004", "000006", "000007", "000008", "000009"],
            "asc": ["000001", "000002", "000003", "000004", "000006", "000007", "000008", "000009"],
            "desc": ["000004", "000002", "000003", "000001", "000006", "000007", "000008", "000009"],
        }
        for direction, codes in expected.items():
            with self.subTest(direction=direction):
                result = limit_up._filter_and_sort_limit_up_list(
                    [item(code, final) for code, final in values], None, None, None,
                    "yesterday_final_seal_time", direction)
                self.assertEqual([row.stock_code for row in result], codes)


class OpeningAuctionSortingTests(unittest.TestCase):
    def test_both_opening_percentages_sort_finite_values_with_zero_and_missing_last(self):
        values = [("000009", None), ("000004", 5), ("000003", 0), ("000002", 5),
                  ("000008", float("nan")), ("000007", float("inf")),
                  ("000006", float("-inf")), ("000001", -10)]
        expected = {
            None: ["000002", "000004", "000003", "000001", "000006", "000007", "000008", "000009"],
            "asc": ["000001", "000003", "000002", "000004", "000006", "000007", "000008", "000009"],
            "desc": ["000002", "000004", "000003", "000001", "000006", "000007", "000008", "000009"],
        }
        for field in ("yesterday_open_change_pct", "today_open_change_pct"):
            for direction, codes in expected.items():
                with self.subTest(field=field, direction=direction):
                    rows = [item(code, None).model_copy(update={field: value}) for code, value in values]
                    result = limit_up._filter_and_sort_limit_up_list(
                        rows, None, None, None, field, direction)
                    self.assertEqual([row.stock_code for row in result], codes)


class YesterdaySealApiTests(unittest.IsolatedAsyncioTestCase):
    async def test_live_endpoint_sorts_enriched_times_and_keeps_date_tags(self):
        raw = [dict(stock_code="600001", stock_name="Late", limit_up_price=11,
                    context_date=DAY, previous_trade_date=PREVIOUS, yesterday_final_seal_time="14:30:00"),
               dict(stock_code="600002", stock_name="Early", limit_up_price=11,
                    context_date=DAY, previous_trade_date=PREVIOUS, yesterday_final_seal_time="09:25:00"),
               dict(stock_code="600003", stock_name="Missing", limit_up_price=11,
                    context_date=DAY, previous_trade_date=PREVIOUS)]
        with patch.object(limit_up, "today_cn", return_value=DAY), \
             patch.object(limit_up, "is_a_share_trading_day", AsyncMock(return_value=True)), \
             patch.object(limit_up.realtime_limit_up_service, "get_realtime_limit_up_list", AsyncMock(return_value=raw)):
            result = await limit_up.get_realtime_limit_up(
                DAY, continuous_days=None, reason_category=None, sort_by="yesterday_final_seal_time", db=None)
        self.assertEqual([row.stock_code for row in result.data], ["600002", "600001", "600003"])
        wire = result.model_dump(mode="json")
        self.assertEqual(wire["data"][0].get("context_date"), "2026-10-23")
        self.assertEqual(wire["data"][0].get("previous_trade_date"), "2026-10-22")

    async def test_missing_historical_day_uses_actual_comparison_anchor_and_sorts_after_enrichment(self):
        requested, actual, previous = date(2026, 10, 21), date(2026, 10, 20), date(2026, 10, 19)
        stock1 = make_stock(stock_id=1, stock_code="600001")
        stock2 = make_stock(stock_id=2, stock_code="600002")
        rows = [(make_record(actual, stock_id=1), stock1), (make_record(actual, stock_id=2), stock2)]
        db = SequencedSession([FakeAllResult(rows), FakeAllResult([(1, True, 2), (2, True, 1)])])

        async def enrich(day, items, quotes=None):
            self.assertEqual(day, actual)
            self.assertTrue(all(row["trade_date"] == actual for row in items))
            return [{**row, "context_date": actual, "previous_trade_date": previous,
                     "yesterday_final_seal_time": "14:30:00" if row["stock_code"] == "600001" else "09:25:00"}
                    for row in items]

        with patch.object(limit_up, "get_trade_date_with_fallback", AsyncMock(return_value=(actual, True))), \
             patch.object(limit_up_context_service, "enrich", AsyncMock(side_effect=enrich)):
            result = await limit_up._get_database_limit_up_response(
                db, requested, None, None, None, "yesterday_final_seal_time", "asc")
        self.assertEqual(result.trade_date, actual)
        self.assertTrue(result.is_fallback)
        self.assertEqual([row.stock_code for row in result.data], ["600002", "600001"])
        wire = result.model_dump(mode="json")
        self.assertEqual(wire["data"][0].get("trade_date"), "2026-10-20")
        self.assertEqual(wire["data"][0].get("context_date"), "2026-10-20")
        self.assertEqual(wire["data"][0].get("previous_trade_date"), "2026-10-19")

    async def test_live_endpoint_sorts_both_opening_fields(self):
        raw = [dict(stock_code="600001", stock_name="Negative", limit_up_price=11,
                    yesterday_open_change_pct=-2, today_open_change_pct=3),
               dict(stock_code="600002", stock_name="Zero", limit_up_price=11,
                    yesterday_open_change_pct=0, today_open_change_pct=0),
               dict(stock_code="600003", stock_name="Missing", limit_up_price=11)]
        with patch.object(limit_up, "today_cn", return_value=DAY), \
             patch.object(limit_up, "is_a_share_trading_day", AsyncMock(return_value=True)), \
             patch.object(limit_up.realtime_limit_up_service, "get_realtime_limit_up_list", AsyncMock(return_value=raw)):
            for field, expected in (("yesterday_open_change_pct", ["600002", "600001", "600003"]),
                                    ("today_open_change_pct", ["600001", "600002", "600003"])):
                with self.subTest(field=field):
                    result = await limit_up.get_realtime_limit_up(
                        DAY, continuous_days=None, reason_category=None, sort_by=field, db=None)
                    self.assertEqual([row.stock_code for row in result.data], expected)


if __name__ == "__main__":
    unittest.main()
