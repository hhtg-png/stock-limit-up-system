import unittest
from datetime import date
from unittest.mock import AsyncMock, patch

import app.services.tradable_market_value_service as tradable_market_value_module
from app.services.tradable_market_value_service import TradableMarketValueService


class TradableMarketValueServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_get_float_share_map_falls_back_to_f10_free_share_estimate_when_tushare_token_missing(self):
        service = TradableMarketValueService()

        with patch.object(
            tradable_market_value_module.settings,
            "TUSHARE_TOKEN",
            None,
        ), patch.object(
            service,
            "_fetch_f10_estimated_free_share_map",
            AsyncMock(return_value={"000001": 1234.5}),
        ):
            result = await service.get_float_share_map(date(2026, 4, 24), ["000001"])

        self.assertEqual(result, {"000001": 1234.5})


if __name__ == "__main__":
    unittest.main()
