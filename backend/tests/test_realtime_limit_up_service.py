import time
import unittest
from datetime import date, datetime
from unittest.mock import AsyncMock, patch

import app.services.realtime_limit_up_service as realtime_limit_up_module
from app.services.realtime_limit_up_service import RealtimeLimitUpService


class RealtimeLimitUpServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_get_fast_limit_up_pool_can_wait_for_stale_refresh_before_returning(self):
        service = RealtimeLimitUpService()
        trade_date = date(2026, 4, 23)
        service._POOL_CACHE_TTL = 1
        service._POOL_STALE_TTL = 60
        service._pool_cache[trade_date] = [{"stock_code": "000001", "stock_name": "Old"}]
        service._pool_cache_time[trade_date] = time.time() - 2
        service._refresh_pool_cache = AsyncMock(
            return_value=[{"stock_code": "000002", "stock_name": "Fresh"}]
        )

        data = await service.get_fast_limit_up_pool(trade_date, wait_for_refresh=True)

        self.assertEqual(data, [{"stock_code": "000002", "stock_name": "Fresh"}])
        service._refresh_pool_cache.assert_awaited_once_with(trade_date)

    async def test_get_realtime_limit_up_list_merges_fast_pool_with_tencent_quotes(self):
        service = RealtimeLimitUpService()
        service.get_fast_limit_up_pool = AsyncMock(
            return_value=[
                {
                    "stock_code": "000001",
                    "stock_name": "平安银行",
                    "first_limit_up_time": datetime(2026, 4, 23, 9, 31, 25),
                    "final_seal_time": datetime(2026, 4, 23, 9, 45, 0),
                    "limit_up_reason": "银行",
                    "reason_category": "其他",
                    "continuous_limit_up_days": 2,
                    "open_count": 0,
                    "is_final_sealed": True,
                    "seal_amount": 1000.0,
                    "limit_up_price": 10.5,
                    "turnover_rate": 1.2,
                    "amount": 5000.0,
                },
                {
                    "stock_code": "300001",
                    "stock_name": "特锐德",
                    "first_limit_up_time": datetime(2026, 4, 23, 10, 5, 0),
                    "final_seal_time": None,
                    "limit_up_reason": "电力设备",
                    "reason_category": "其他",
                    "continuous_limit_up_days": 1,
                    "open_count": 2,
                    "is_final_sealed": False,
                    "seal_amount": 0,
                    "limit_up_price": 20.0,
                    "turnover_rate": 3.3,
                    "amount": 8000.0,
                },
            ]
        )
        service._fetch_ths_reason_map = AsyncMock(
            return_value={"000001": "机器人", "300001": "充电桩"}
        )

        quotes = {
            "000001": {
                "code": "000001",
                "price": 10.5,
                "amount": 8888.0,
                "turnover_rate": 6.8,
                "change_pct": 10.0,
                "bid1_volume": 12345,
            },
            "300001": {
                "code": "300001",
                "price": 19.66,
                "amount": 9999.0,
                "turnover_rate": 9.1,
                "change_pct": 8.9,
                "bid1_volume": 0,
            },
        }

        with patch(
            "app.services.realtime_limit_up_service.tencent_api.get_quotes_batch",
            AsyncMock(return_value=quotes),
        ), patch.object(
            realtime_limit_up_module,
            "tradable_market_value_service",
            AsyncMock(),
            create=True,
        ) as tradable_market_value_service:
            tradable_market_value_service.get_float_share_map = AsyncMock(
                return_value={"000001": 1000.0, "300001": 500.0}
            )
            data = await service.get_realtime_limit_up_list(date(2026, 4, 23))

        self.assertEqual(len(data), 2)

        sealed = data[0]
        self.assertEqual(sealed["stock_code"], "000001")
        self.assertEqual(sealed["limit_up_reason"], "机器人")
        self.assertEqual(sealed["reason_category"], "人工智能")
        self.assertEqual(sealed["current_price"], 10.5)
        self.assertEqual(sealed["turnover_rate"], 6.8)
        self.assertEqual(sealed["amount"], 8888.0)
        self.assertEqual(sealed["tradable_market_value"], 10500.0)
        self.assertEqual(sealed["change_pct"], 10.0)
        self.assertEqual(sealed["bid1_volume"], 12345)
        self.assertEqual(sealed["current_status"], "sealed")
        self.assertTrue(sealed["is_sealed"])

        opened = data[1]
        self.assertEqual(opened["stock_code"], "300001")
        self.assertEqual(opened["limit_up_reason"], "充电桩")
        self.assertEqual(opened["reason_category"], "新能源")
        self.assertEqual(opened["current_price"], 19.66)
        self.assertEqual(opened["turnover_rate"], 9.1)
        self.assertEqual(opened["amount"], 9999.0)
        self.assertEqual(opened["tradable_market_value"], 9830.0)
        self.assertEqual(opened["change_pct"], 8.9)
        self.assertEqual(opened["bid1_volume"], 0)
        self.assertEqual(opened["current_status"], "opened")
        self.assertFalse(opened["is_sealed"])

    async def test_get_realtime_limit_up_list_does_not_use_tencent_circulating_value_as_free_float(self):
        service = RealtimeLimitUpService()
        service.get_fast_limit_up_pool = AsyncMock(
            return_value=[
                {
                    "stock_code": "000001",
                    "stock_name": "平安银行",
                    "first_limit_up_time": datetime(2026, 4, 23, 9, 31, 25),
                    "final_seal_time": datetime(2026, 4, 23, 9, 45, 0),
                    "limit_up_reason": "银行",
                    "reason_category": "其他",
                    "continuous_limit_up_days": 2,
                    "open_count": 0,
                    "is_final_sealed": True,
                    "seal_amount": 1000.0,
                    "limit_up_price": 10.5,
                    "turnover_rate": 1.2,
                    "amount": 5000.0,
                }
            ]
        )
        service._fetch_ths_reason_map = AsyncMock(return_value={})

        quotes = {
            "000001": {
                "code": "000001",
                "price": 10.5,
                "amount": 8888.0,
                "turnover_rate": 6.8,
                "circulating_value": 1.05,
            }
        }

        with patch(
            "app.services.realtime_limit_up_service.tencent_api.get_quotes_batch",
            AsyncMock(return_value=quotes),
        ), patch.object(
            realtime_limit_up_module,
            "tradable_market_value_service",
            AsyncMock(),
            create=True,
        ) as tradable_market_value_service:
            tradable_market_value_service.get_float_share_map = AsyncMock(return_value={})
            data = await service.get_realtime_limit_up_list(date(2026, 4, 23))

        self.assertIsNone(data[0]["tradable_market_value"])

    async def test_get_realtime_limit_up_list_drops_quote_sentinel_change_pct(self):
        service = RealtimeLimitUpService()
        service.get_fast_limit_up_pool = AsyncMock(
            return_value=[
                {
                    "stock_code": "603272",
                    "stock_name": "联翔股份",
                    "continuous_limit_up_days": 2,
                    "open_count": 0,
                    "is_final_sealed": True,
                    "limit_up_price": 26.73,
                    "turnover_rate": 0,
                    "amount": 0,
                }
            ]
        )
        service._fetch_ths_reason_map = AsyncMock(return_value={})

        quotes = {
            "603272": {
                "code": "603272",
                "price": 0,
                "amount": 0,
                "turnover_rate": 0,
                "change_pct": -100.0,
                "bid1_volume": 0,
            }
        }

        with patch(
            "app.services.realtime_limit_up_service.tencent_api.get_quotes_batch",
            AsyncMock(return_value=quotes),
        ), patch.object(
            realtime_limit_up_module,
            "tradable_market_value_service",
            AsyncMock(),
            create=True,
        ) as tradable_market_value_service:
            tradable_market_value_service.get_float_share_map = AsyncMock(return_value={})
            data = await service.get_realtime_limit_up_list(date(2026, 4, 28))

        self.assertEqual(data[0]["current_price"], 26.73)
        self.assertIsNone(data[0]["change_pct"])


if __name__ == "__main__":
    unittest.main()
