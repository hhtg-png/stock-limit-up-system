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


class FailingAsyncClient:
    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, *args, **kwargs):
        raise RuntimeError("upstream unavailable")


class EmptyKlinesResponse:
    def raise_for_status(self):
        return None

    def json(self):
        return {"data": {"klines": []}}


class EmptyKlinesAsyncClient:
    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, *args, **kwargs):
        return EmptyKlinesResponse()


class CapturingAsyncClient:
    params = None

    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, *args, **kwargs):
        CapturingAsyncClient.params = kwargs["params"]
        return EmptyKlinesResponse()


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

    def test_format_kline_item_marks_st_five_percent_limit_up(self):
        raw = "2026-05-12,10.00,10.50,10.50,9.90,1000,1050000,5.00,5.00,0.50,3.20"

        point = market._format_kline_item(raw, "600001", stock_name="*ST示例")

        self.assertTrue(point["is_limit_up"])

    def test_format_kline_item_uses_thirty_percent_board_for_beijing_exchange(self):
        thirty_pct = "2026-05-12,10.00,13.00,13.00,9.90,1000,1300000,30.00,30.00,3.00,3.20"
        twenty_pct = "2026-05-12,10.00,12.00,12.00,9.90,1000,1200000,20.00,20.00,2.00,3.20"

        limit_up = market._format_kline_item(thirty_pct, "833171", market="BJ")
        not_limit_up = market._format_kline_item(twenty_pct, "833171", market="BJ")

        self.assertTrue(limit_up["is_limit_up"])
        self.assertFalse(not_limit_up["is_limit_up"])

    def test_format_kline_item_applies_chinext_threshold_before_st(self):
        raw = "2026-05-12,10.00,11.00,11.00,9.90,1000,1100000,10.00,10.00,1.00,3.20"

        point = market._format_kline_item(raw, "300001", market="SZ", stock_name="ST示例")

        self.assertFalse(point["is_limit_up"])

    def test_format_kline_item_applies_star_threshold_before_st(self):
        raw = "2026-05-12,10.00,11.00,11.00,9.90,1000,1100000,10.00,10.00,1.00,3.20"

        point = market._format_kline_item(raw, "688001", market="SH", stock_name="ST示例")

        self.assertFalse(point["is_limit_up"])

    def test_normalize_symbol_infers_market_from_suffix_or_code(self):
        self.assertEqual(market._normalize_symbol("000001.SH"), ("000001", "SH", "1.000001"))
        self.assertEqual(market._normalize_symbol("603893"), ("603893", "SH", "1.603893"))
        self.assertEqual(market._normalize_symbol("300001"), ("300001", "SZ", "0.300001"))
        self.assertEqual(market._normalize_symbol("833171.BJ"), ("833171", "BJ", "0.833171"))
        self.assertEqual(market._normalize_symbol("833171.BSE"), ("833171", "BSE", "0.833171"))
        self.assertEqual(market._normalize_symbol("833171"), ("833171", "BJ", "0.833171"))
        self.assertEqual(market._normalize_symbol("920001"), ("920001", "BJ", "0.920001"))

    def test_build_compare_series_normalizes_from_first_close(self):
        points = [
            {"date": date(2026, 5, 10), "close": 10.0},
            {"date": date(2026, 5, 11), "close": 11.0},
            {"date": date(2026, 5, 12), "close": 9.5},
        ]

        series = market._build_compare_series("603893", "瑞芯微", points)

        self.assertEqual(series["symbol"], "603893")
        self.assertEqual(series["name"], "瑞芯微")
        self.assertEqual(series["data"][0]["change_pct_from_start"], 0.0)
        self.assertEqual(series["data"][1]["change_pct_from_start"], 10.0)
        self.assertEqual(series["data"][2]["change_pct_from_start"], -5.0)

    async def test_get_compare_data_fetches_each_symbol(self):
        with patch.object(
            market,
            "_fetch_kline_from_em",
            AsyncMock(
                side_effect=[
                    [
                        {"date": date(2026, 5, 10), "close": 10.0},
                        {"date": date(2026, 5, 11), "close": 11.0},
                    ],
                    [
                        {"date": date(2026, 5, 10), "close": 3000.0},
                        {"date": date(2026, 5, 11), "close": 3030.0},
                    ],
                ]
            ),
        ) as fetcher:
            response = await market.get_compare_data("603893,000001.SH", "day", 250)

        self.assertEqual(fetcher.await_count, 2)
        self.assertEqual([item.symbol for item in response], ["603893", "000001.SH"])
        self.assertEqual(response[0].data[1].change_pct_from_start, 10.0)
        self.assertEqual(response[1].data[1].change_pct_from_start, 1.0)

    async def test_get_compare_data_rejects_empty_symbols(self):
        with patch.object(market, "_fetch_kline_from_em", AsyncMock()) as fetcher:
            with self.assertRaises(market.HTTPException) as raised:
                await market.get_compare_data(" , ,, ", "day", 250)

        self.assertEqual(raised.exception.status_code, 400)
        self.assertEqual(raised.exception.detail, "symbols 不能为空")
        fetcher.assert_not_awaited()

    async def test_get_compare_data_rejects_too_many_symbols(self):
        symbols = "600001,600002,600003,600004,600005,600006"

        with patch.object(market, "_fetch_kline_from_em", AsyncMock()) as fetcher:
            with self.assertRaises(market.HTTPException) as raised:
                await market.get_compare_data(symbols, "day", 250)

        self.assertEqual(raised.exception.status_code, 400)
        self.assertEqual(raised.exception.detail, "最多支持5个叠加标的")
        fetcher.assert_not_awaited()

    async def test_get_kline_data_fetches_by_stock_market(self):
        stock = SimpleNamespace(stock_code="603893", stock_name="淳中科技", market="SH", is_st=0)
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

        fetcher.assert_awaited_once_with("603893", "SH", "day", 250, stock_name="淳中科技", is_st=0)
        self.assertEqual(response.stock_code, "603893")
        self.assertEqual(response.period, "day")
        self.assertEqual(response.data[0].close, 103.42)
        self.assertTrue(response.data[0].is_limit_up)

    async def test_fetch_kline_from_em_raises_bad_gateway_on_upstream_failure(self):
        with patch.object(market.httpx, "AsyncClient", FailingAsyncClient):
            with self.assertRaises(market.HTTPException) as raised:
                await market._fetch_kline_from_em("603893", "SH", "day", 250)

        self.assertEqual(raised.exception.status_code, 502)

    async def test_fetch_kline_from_em_returns_empty_list_for_confirmed_empty_klines(self):
        with patch.object(market.httpx, "AsyncClient", EmptyKlinesAsyncClient):
            points = await market._fetch_kline_from_em("603893", "SH", "day", 250)

        self.assertEqual(points, [])

    async def test_fetch_kline_from_em_uses_sz_prefix_for_beijing_exchange(self):
        CapturingAsyncClient.params = None

        with patch.object(market.httpx, "AsyncClient", CapturingAsyncClient):
            points = await market._fetch_kline_from_em("833171", "BJ", "day", 250)

        self.assertEqual(points, [])
        self.assertEqual(CapturingAsyncClient.params["secid"], "0.833171")


if __name__ == "__main__":
    unittest.main()
