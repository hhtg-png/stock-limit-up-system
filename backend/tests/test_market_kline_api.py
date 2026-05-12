import unittest
from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from app.api.v1 import market


class FakeScalarResult:
    def __init__(self, value):
        self.value = value

    def scalar_one_or_none(self):
        return self.value


class FakeSession:
    def __init__(self, stock):
        self.stock = stock

    async def execute(self, _query):
        return FakeScalarResult(self.stock)


class MarketKlineApiTests(unittest.IsolatedAsyncioTestCase):
    def test_format_kline_item_marks_main_board_limit_up(self):
        raw = "2026-05-12,96.10,103.42,103.42,95.60,560000,1820000000,8.20,10.00,9.40,17.42"

        point = market._format_kline_item(raw, "603893")

        self.assertEqual(point["date"], date(2026, 5, 12))
        self.assertEqual(point["open"], 96.10)
        self.assertEqual(point["close"], 103.42)
        self.assertEqual(point["high"], 103.42)
        self.assertEqual(point["low"], 95.60)
        self.assertEqual(point["volume"], 560000)
        self.assertEqual(point["amount"], 1820000000)
        self.assertEqual(point["change_pct"], 10.00)
        self.assertTrue(point["is_limit_up"])

    def test_format_kline_item_uses_twenty_percent_board_for_chinext(self):
        raw = "2026-05-12,10.00,11.00,11.00,9.90,1000,1100000,11.00,10.00,1.00,3.20"

        point = market._format_kline_item(raw, "300001")

        self.assertFalse(point["is_limit_up"])

    def test_normalize_symbol_infers_market_from_suffix_or_code(self):
        self.assertEqual(market._normalize_symbol("000001.SH"), ("000001", "SH", "1.000001"))
        self.assertEqual(market._normalize_symbol("603893"), ("603893", "SH", "1.603893"))
        self.assertEqual(market._normalize_symbol("300001"), ("300001", "SZ", "0.300001"))

    async def test_get_kline_data_fetches_by_stock_market(self):
        stock = SimpleNamespace(stock_code="603893", market="SH")
        fake_db = FakeSession(stock)
        fetched = [
            {
                "date": date(2026, 5, 12),
                "open": 96.1,
                "close": 103.42,
                "high": 103.42,
                "low": 95.6,
                "volume": 560000,
                "amount": 1820000000,
                "change_pct": 10.0,
                "is_limit_up": True,
            }
        ]

        with patch.object(market, "_fetch_kline_from_em", AsyncMock(return_value=fetched)) as fetcher:
            response = await market.get_kline_data("603893", "day", 250, fake_db)

        fetcher.assert_awaited_once_with("603893", "SH", "day", 250)
        self.assertEqual(response.stock_code, "603893")
        self.assertEqual(response.period, "day")
        self.assertEqual(response.data[0].close, 103.42)
        self.assertTrue(response.data[0].is_limit_up)


if __name__ == "__main__":
    unittest.main()
