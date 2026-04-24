import unittest

from app.services.continuous_ladder_service import ContinuousLadderService


class ContinuousLadderServiceTests(unittest.TestCase):
    def setUp(self):
        self.service = ContinuousLadderService()

    def test_build_realtime_ladder_keeps_opened_continuous_stocks(self):
        ladders = self.service.build_realtime_ladder(
            [
                {
                    "stock_code": "600001",
                    "stock_name": "龙头一号",
                    "continuous_limit_up_days": 3,
                    "is_sealed": True,
                    "current_status": "sealed",
                    "first_limit_up_time": "09:30:01",
                    "limit_up_reason": "算力",
                    "change_pct": 10.0,
                    "bid1_volume": 20000,
                    "turnover_rate": 5.0,
                    "amount": 10000.0,
                    "tradable_market_value": 500000.0,
                    "open_count": 0,
                },
                {
                    "stock_code": "600002",
                    "stock_name": "龙头二号",
                    "continuous_limit_up_days": 3,
                    "is_sealed": False,
                    "current_status": "opened",
                    "first_limit_up_time": "09:32:05",
                    "limit_up_reason": "机器人",
                    "change_pct": 8.76,
                    "bid1_volume": 0,
                    "turnover_rate": 6.0,
                    "amount": 12000.0,
                    "tradable_market_value": 600000.0,
                    "open_count": 2,
                },
                {
                    "stock_code": "600003",
                    "stock_name": "首板样本",
                    "continuous_limit_up_days": 1,
                    "is_sealed": True,
                    "current_status": "sealed",
                },
            ],
            min_days=2,
        )

        self.assertEqual(len(ladders), 1)
        ladder = ladders[0]
        self.assertEqual(ladder["continuous_days"], 3)
        self.assertEqual(ladder["count"], 2)
        self.assertEqual(
            [stock["stock_code"] for stock in ladder["stocks"]],
            ["600001", "600002"],
        )
        self.assertTrue(ladder["stocks"][0]["is_sealed"])
        self.assertFalse(ladder["stocks"][1]["is_sealed"])
        self.assertEqual(ladder["stocks"][1]["open_count"], 2)
        self.assertEqual(ladder["stocks"][0]["real_turnover_rate"], 2.0)
        self.assertEqual(ladder["stocks"][1]["real_turnover_rate"], 2.0)

    def test_build_yesterday_ladder_distinguishes_opened_from_broken(self):
        ladders = self.service.build_yesterday_ladder(
            [
                {"c": "600001", "n": "龙头一号", "ylbc": 3, "zdp": 10.0},
                {"c": "600002", "n": "龙头二号", "ylbc": 2, "zdp": -1.23},
                {"c": "600003", "n": "龙头三号", "ylbc": 2, "zdp": 0.56},
            ],
            [
                {"stock_code": "600001", "current_status": "sealed"},
                {"stock_code": "600002", "current_status": "opened"},
            ],
            min_days=2,
        )

        self.assertEqual([ladder["continuous_days"] for ladder in ladders], [3, 2])

        three_board = ladders[0]
        self.assertEqual(three_board["sealed_count"], 1)
        self.assertEqual(three_board["opened_count"], 0)
        self.assertEqual(three_board["broken_count"], 0)
        self.assertEqual(three_board["stocks"][0]["today_status"], "sealed")

        two_board = ladders[1]
        self.assertEqual(two_board["sealed_count"], 0)
        self.assertEqual(two_board["opened_count"], 1)
        self.assertEqual(two_board["broken_count"], 1)
        self.assertEqual(
            [stock["today_status"] for stock in two_board["stocks"]],
            ["opened", "broken"],
        )


if __name__ == "__main__":
    unittest.main()
