import sys
import types
import unittest
from unittest import mock
from unittest.mock import AsyncMock

import pandas as pd

from app.scripts import warm_tdx_stock_move_seed as warm_seed


class TdxStockMoveSeedWarmTests(unittest.IsolatedAsyncioTestCase):
    async def test_load_akshare_stock_codes_includes_full_a_share_codes(self):
        fake_akshare = types.SimpleNamespace(
            stock_info_a_code_name=lambda: pd.DataFrame(
                [
                    {"code": "002576", "name": "通达动力"},
                    {"code": "600589", "name": "大位科技"},
                ]
            )
        )

        with mock.patch.dict(sys.modules, {"akshare": fake_akshare}):
            rows = await warm_seed.load_akshare_stock_codes()

        self.assertIn(("002576", "通达动力"), rows)
        self.assertIn(("600589", "大位科技"), rows)

    def test_merge_stock_codes_keeps_akshare_rows_and_dedupes_local_rows(self):
        rows = warm_seed.merge_stock_code_lists(
            [("002576", "通达动力"), ("000002", "万科A")],
            [("002576", "旧名称"), ("603677", "奇精机械")],
        )

        self.assertEqual(
            rows,
            [
                ("002576", "通达动力"),
                ("000002", "万科A"),
                ("603677", "奇精机械"),
            ],
        )

    async def test_build_seed_record_uses_preloaded_limit_up_item(self):
        limit_up_item = {
            "stock_code": "002576",
            "stock_name": "通达动力",
            "limit_up_reason": "机器人+新能源汽车",
            "reason_category": "机器人",
            "continuous_limit_up_days": 1,
            "open_count": 0,
            "is_sealed": True,
            "is_final_sealed": True,
            "first_limit_up_time": "09:35:00",
            "final_seal_time": "09:35:00",
            "seal_amount": 1000000,
        }

        with (
            mock.patch.object(
                warm_seed,
                "load_seed_external_stock_move",
                new=AsyncMock(return_value=None),
            ) as external_mock,
            mock.patch.object(
                warm_seed.tdx_plugin_service,
                "get_stock_move",
                new=AsyncMock(side_effect=AssertionError("slow path should not run")),
            ) as slow_mock,
        ):
            record = await warm_seed.build_seed_record(
                "002576",
                "通达动力",
                warm_seed.date(2026, 5, 29),
                "mixed",
                {"002576": limit_up_item},
            )

        self.assertTrue(record["success"])
        self.assertEqual(record["stock_code"], "002576")
        self.assertEqual(record["cache_trade_date"], "2026-05-29")
        self.assertEqual(record["payload"]["source_status"]["stock_move_live"], "preloaded")
        external_mock.assert_awaited_once()
        slow_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
