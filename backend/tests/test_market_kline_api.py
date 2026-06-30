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


class SinaKlinesResponse:
    text = (
        "var _=(["
        '{"day":"2026-05-08","open":"10.000","high":"10.200","low":"9.900","close":"10.000","volume":"1000"},'
        '{"day":"2026-05-11","open":"10.000","high":"11.000","low":"10.000","close":"11.000","volume":"1200"},'
        '{"day":"2026-05-12","open":"11.000","high":"12.100","low":"10.900","close":"12.100","volume":"1500"}'
        "]);"
    )

    def raise_for_status(self):
        return None


class EmptyKlinesAsyncClient:
    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, *args, **kwargs):
        return EmptyKlinesResponse()


class EastmoneyFailureSinaSuccessClient:
    urls = []

    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, *args, **kwargs):
        EastmoneyFailureSinaSuccessClient.urls.append(args[0])
        if args[0] == market.EASTMONEY_KLINE_URL:
            raise RuntimeError("eastmoney unavailable")
        return SinaKlinesResponse()


class CapturingAsyncClient:
    url = None
    params = None

    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, *args, **kwargs):
        CapturingAsyncClient.url = args[0]
        CapturingAsyncClient.params = kwargs["params"]
        return EmptyKlinesResponse()


class SuggestResponse:
    def json(self):
        return {
            "QuotationCodeTable": {
                "Data": [
                    {
                        "Code": "601318",
                        "Name": "中国平安",
                        "PinYin": "ZGPA",
                        "Classify": "AStock",
                        "SecurityTypeName": "沪A",
                        "QuoteID": "1.601318",
                    },
                    {
                        "Code": "000001",
                        "Name": "上证指数",
                        "PinYin": "SZZS",
                        "Classify": "Index",
                        "SecurityTypeName": "指数",
                        "QuoteID": "1.000001",
                    },
                    {
                        "Code": "000001",
                        "Name": "华夏成长混合",
                        "PinYin": "HXCZHH",
                        "Classify": "OTCFUND",
                        "SecurityTypeName": "基金",
                        "QuoteID": "150.000001",
                    },
                ]
            }
        }


class SuggestAsyncClient:
    params = None

    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, *args, **kwargs):
        SuggestAsyncClient.params = kwargs["params"]
        return SuggestResponse()


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

    def test_format_kline_item_requires_close_at_limit_up_price(self):
        touched_but_opened = "2026-06-29,14.00,15.35,15.36,13.97,3828205,5687866790,9.96,9.96,1.39,10.67"
        sealed = "2026-06-29,14.00,15.36,15.36,13.97,3828205,5687866790,9.96,10.03,1.40,10.67"

        opened_point = market._format_kline_item(touched_but_opened, "600707")
        sealed_point = market._format_kline_item(sealed, "600707")

        self.assertFalse(opened_point["is_limit_up"])
        self.assertTrue(sealed_point["is_limit_up"])

    def test_apply_change_pct_requires_close_at_limit_up_price(self):
        points = [
            {"date": date(2026, 6, 26), "open": 13.64, "close": 13.96, "high": 14.91, "low": 13.63},
            {"date": date(2026, 6, 29), "open": 14.00, "close": 15.35, "high": 15.36, "low": 13.97},
        ]

        normalized = market._apply_change_pct(points, "600707")

        self.assertAlmostEqual(normalized[1]["change_pct"], 9.96)
        self.assertFalse(normalized[1]["is_limit_up"])

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
            {"date": date(2026, 5, 10), "close": 10.0, "is_limit_up": False},
            {"date": date(2026, 5, 11), "close": 11.0, "is_limit_up": True},
            {"date": date(2026, 5, 12), "close": 9.5, "is_limit_up": False},
        ]

        series = market._build_compare_series("603893", "瑞芯微", points)

        self.assertEqual(series["symbol"], "603893")
        self.assertEqual(series["name"], "瑞芯微")
        self.assertEqual(series["data"][0]["open"], 10.0)
        self.assertEqual(series["data"][0]["close"], 10.0)
        self.assertEqual(series["data"][0]["low"], 10.0)
        self.assertEqual(series["data"][0]["high"], 10.0)
        self.assertEqual(series["data"][0]["change_pct_from_start"], 0.0)
        self.assertEqual(series["data"][0]["open_pct_from_start"], 0.0)
        self.assertEqual(series["data"][0]["close_pct_from_start"], 0.0)
        self.assertEqual(series["data"][0]["low_pct_from_start"], 0.0)
        self.assertEqual(series["data"][0]["high_pct_from_start"], 0.0)
        self.assertEqual(series["data"][1]["change_pct_from_start"], 10.0)
        self.assertTrue(series["data"][1]["is_limit_up"])
        self.assertEqual(series["data"][2]["change_pct_from_start"], -5.0)
        self.assertFalse(series["data"][2]["is_limit_up"])

    def test_build_compare_series_normalizes_ohlc_from_first_close(self):
        points = [
            {"date": date(2026, 5, 10), "open": 9.8, "close": 10.0, "low": 9.7, "high": 10.3},
            {"date": date(2026, 5, 11), "open": 10.0, "close": 11.0, "low": 9.9, "high": 11.0, "is_limit_up": True},
        ]

        series = market._build_compare_series("000858", "五粮液", points)

        self.assertEqual(series["data"][0]["open"], 9.8)
        self.assertEqual(series["data"][0]["close"], 10.0)
        self.assertEqual(series["data"][0]["low"], 9.7)
        self.assertEqual(series["data"][0]["high"], 10.3)
        self.assertEqual(series["data"][0]["open_pct_from_start"], -2.0)
        self.assertEqual(series["data"][0]["close_pct_from_start"], 0.0)
        self.assertEqual(series["data"][0]["low_pct_from_start"], -3.0)
        self.assertEqual(series["data"][0]["high_pct_from_start"], 3.0)
        self.assertEqual(series["data"][1]["open_pct_from_start"], 0.0)
        self.assertEqual(series["data"][1]["close_pct_from_start"], 10.0)
        self.assertEqual(series["data"][1]["low_pct_from_start"], -1.0)
        self.assertEqual(series["data"][1]["high_pct_from_start"], 10.0)
        self.assertTrue(series["data"][1]["is_limit_up"])

    async def test_search_symbols_supports_pinyin_name_and_market_suffix(self):
        SuggestAsyncClient.params = None

        with patch.object(market.httpx, "AsyncClient", SuggestAsyncClient):
            pinyin_results = await market.search_symbols("zgpa", 10)
            name_results = await market.search_symbols("中国平安", 10)
            suffix_results = await market.search_symbols("000001.SH", 10)

        self.assertEqual(SuggestAsyncClient.params["input"], "000001")
        self.assertEqual(pinyin_results[0].symbol, "601318.SH")
        self.assertEqual(name_results[0].stock_name, "中国平安")
        self.assertEqual(suffix_results[0].symbol, "000001.SH")
        self.assertEqual(suffix_results[0].stock_name, "上证指数")
        self.assertNotIn("OTCFUND", [item.classify for item in pinyin_results])

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

    async def test_get_kline_data_falls_back_to_inferred_market_without_local_stock(self):
        fake_db = FakeSession(None)
        fetched = [
            {
                "date": date(2026, 5, 12),
                "open": 10.0,
                "close": 11.0,
                "high": 11.0,
                "low": 10.0,
                "volume": 1000,
                "amount": 1100000,
                "change_pct": 10.0,
                "is_limit_up": True,
            }
        ]

        with patch.object(market, "_fetch_kline_from_em", AsyncMock(return_value=fetched)) as fetcher:
            response = await market.get_kline_data("603311", "day", 250, fake_db)

        fetcher.assert_awaited_once_with("603311", "SH", "day", 250, stock_name=None, is_st=None)
        self.assertEqual(response.stock_code, "603311")
        self.assertEqual(response.data[0].close, 11.0)

    async def test_get_timeline_data_falls_back_to_inferred_market_without_local_stock(self):
        fake_db = FakeSession(None)
        fetched = {
            "stock_code": "603311",
            "trade_date": "2026-05-12",
            "data": [{"time": "09:30:00", "price": 10.0, "volume": 1000}],
        }

        with patch.object(market, "_fetch_timeline_from_em", AsyncMock(return_value=fetched)) as fetcher:
            response = await market.get_timeline_data("603311", date(2026, 5, 12), fake_db)

        fetcher.assert_awaited_once_with("603311", "SH", date(2026, 5, 12))
        self.assertEqual(response["stock_code"], "603311")
        self.assertEqual(response["data"][0]["price"], 10.0)

    async def test_fetch_kline_from_em_raises_bad_gateway_on_upstream_failure(self):
        with patch.object(market.httpx, "AsyncClient", FailingAsyncClient):
            with self.assertRaises(market.HTTPException) as raised:
                await market._fetch_kline_from_em("603893", "SH", "day", 250)

        self.assertEqual(raised.exception.status_code, 502)

    async def test_fetch_kline_from_em_returns_empty_list_for_confirmed_empty_klines(self):
        with patch.object(market.httpx, "AsyncClient", EmptyKlinesAsyncClient):
            points = await market._fetch_kline_from_em("603893", "SH", "day", 250)

        self.assertEqual(points, [])

    async def test_fetch_kline_from_em_falls_back_to_sina_daily_kline(self):
        EastmoneyFailureSinaSuccessClient.urls = []

        with patch.object(market.httpx, "AsyncClient", EastmoneyFailureSinaSuccessClient):
            points = await market._fetch_kline_from_em("002466", "SZ", "day", 2)

        self.assertEqual(EastmoneyFailureSinaSuccessClient.urls, [market.EASTMONEY_KLINE_URL, market.SINA_KLINE_URL])
        self.assertEqual([point["date"] for point in points], [date(2026, 5, 11), date(2026, 5, 12)])
        self.assertEqual(points[0]["change_pct"], 10.0)
        self.assertTrue(points[0]["is_limit_up"])
        self.assertEqual(points[1]["change_pct"], 10.0)
        self.assertTrue(points[1]["is_limit_up"])
        self.assertEqual(points[1]["amount"], 0.0)

    async def test_fetch_kline_from_em_uses_sz_prefix_for_beijing_exchange(self):
        CapturingAsyncClient.url = None
        CapturingAsyncClient.params = None

        with patch.object(market.httpx, "AsyncClient", CapturingAsyncClient):
            points = await market._fetch_kline_from_em("833171", "BJ", "day", 250)

        self.assertEqual(points, [])
        self.assertEqual(CapturingAsyncClient.url, market.EASTMONEY_KLINE_URL)
        self.assertEqual(CapturingAsyncClient.params["secid"], "0.833171")
        self.assertTrue(market.EASTMONEY_KLINE_URL.startswith("http://"))


if __name__ == "__main__":
    unittest.main()
