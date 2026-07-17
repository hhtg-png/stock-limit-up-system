import unittest
from datetime import date

from app.services.market_review_metrics_service import MarketReviewMetricsService


class MarketReviewMetricsServiceTests(unittest.TestCase):
    def setUp(self):
        self.service = MarketReviewMetricsService()

    def test_aggregate_daily_metrics_builds_review_totals(self):
        rows = [
            {
                "stock_code": "600001",
                "board_type": "main",
                "is_st": False,
                "yesterday_limit_up": True,
                "yesterday_continuous_days": 1,
                "today_touched_limit_up": True,
                "today_sealed_close": True,
                "today_opened_close": False,
                "today_broken": False,
                "today_continuous_days": 2,
                "change_pct": 10.0,
                "amount": 120000.0,
            },
            {
                "stock_code": "300001",
                "board_type": "gem",
                "is_st": False,
                "yesterday_limit_up": True,
                "yesterday_continuous_days": 2,
                "today_touched_limit_up": True,
                "today_sealed_close": False,
                "today_opened_close": True,
                "today_broken": False,
                "today_continuous_days": 3,
                "change_pct": 4.5,
                "amount": 80000.0,
            },
            {
                "stock_code": "600002",
                "board_type": "main",
                "is_st": False,
                "yesterday_limit_up": False,
                "yesterday_continuous_days": 0,
                "today_touched_limit_up": False,
                "today_sealed_close": False,
                "today_opened_close": False,
                "today_broken": True,
                "today_continuous_days": 0,
                "change_pct": -3.2,
                "amount": 10000.0,
            },
        ]

        metric = self.service.aggregate_daily_metrics(
            trade_date=date(2026, 4, 27),
            stock_rows=rows,
            limit_down_count=5,
            market_turnover=12345.6,
            up_count_ex_st=3200,
            down_count_ex_st=1800,
        )

        self.assertEqual(metric["trade_date"], date(2026, 4, 27))
        self.assertEqual(metric["limit_up_count"], 2)
        self.assertEqual(metric["continuous_count"], 1)
        self.assertEqual(metric["max_board_height"], 2)
        self.assertEqual(metric["second_board_height"], 0)
        self.assertEqual(metric["gem_board_height"], 0)
        self.assertAlmostEqual(metric["first_to_second_rate"], 100.0)
        self.assertAlmostEqual(metric["continuous_promotion_rate"], 0.0)
        self.assertAlmostEqual(metric["seal_rate"], 50.0)
        self.assertAlmostEqual(metric["yesterday_limit_up_avg_change"], 7.25)
        self.assertAlmostEqual(metric["yesterday_continuous_avg_change"], 4.5)
        self.assertAlmostEqual(metric["market_turnover"], 12345.6)
        self.assertEqual(metric["up_count_ex_st"], 3200)
        self.assertEqual(metric["down_count_ex_st"], 1800)
        self.assertAlmostEqual(metric["limit_up_amount"], 200000.0)
        self.assertAlmostEqual(metric["broken_amount"], 80000.0)

    def test_aggregate_daily_metrics_handles_empty_rows(self):
        metric = self.service.aggregate_daily_metrics(
            trade_date=date(2026, 4, 27),
            stock_rows=[],
            limit_down_count=0,
            market_turnover=0,
            up_count_ex_st=0,
            down_count_ex_st=0,
        )

        self.assertEqual(metric["limit_up_count"], 0)
        self.assertEqual(metric["continuous_count"], 0)
        self.assertEqual(metric["max_board_height"], 0)
        self.assertEqual(metric["second_board_height"], 0)
        self.assertEqual(metric["gem_board_height"], 0)
        self.assertEqual(metric["first_to_second_rate"], 0.0)
        self.assertEqual(metric["continuous_promotion_rate"], 0.0)
        self.assertEqual(metric["seal_rate"], 0.0)
        self.assertEqual(metric["yesterday_limit_up_avg_change"], 0.0)
        self.assertEqual(metric["yesterday_continuous_avg_change"], 0.0)
        self.assertEqual(metric["market_turnover"], 0.0)
        self.assertEqual(metric["up_count_ex_st"], 0)
        self.assertEqual(metric["down_count_ex_st"], 0)
        self.assertEqual(metric["limit_up_amount"], 0.0)
        self.assertEqual(metric["broken_amount"], 0.0)

    def test_aggregate_daily_metrics_ignores_missing_feedback_change_pct(self):
        rows = [
            {
                "stock_code": "600010",
                "board_type": "main",
                "yesterday_limit_up": True,
                "yesterday_continuous_days": 1,
                "today_touched_limit_up": False,
                "today_sealed_close": False,
                "today_opened_close": False,
                "today_continuous_days": 0,
                "change_pct": None,
                "amount": 10000.0,
            },
            {
                "stock_code": "600011",
                "board_type": "main",
                "yesterday_limit_up": True,
                "yesterday_continuous_days": 1,
                "today_touched_limit_up": False,
                "today_sealed_close": False,
                "today_opened_close": False,
                "today_continuous_days": 0,
                "change_pct": 10.0,
                "amount": 10000.0,
            },
            {
                "stock_code": "600012",
                "board_type": "main",
                "yesterday_limit_up": True,
                "yesterday_continuous_days": 2,
                "today_touched_limit_up": False,
                "today_sealed_close": False,
                "today_opened_close": False,
                "today_continuous_days": 0,
                "change_pct": 4.0,
                "amount": 10000.0,
            },
            {
                "stock_code": "600013",
                "board_type": "main",
                "yesterday_limit_up": True,
                "yesterday_continuous_days": 2,
                "today_touched_limit_up": False,
                "today_sealed_close": False,
                "today_opened_close": False,
                "today_continuous_days": 0,
                "change_pct": None,
                "amount": 10000.0,
            },
        ]

        metric = self.service.aggregate_daily_metrics(
            trade_date=date(2026, 4, 28),
            stock_rows=rows,
            limit_down_count=0,
            market_turnover=0,
            up_count_ex_st=0,
            down_count_ex_st=0,
        )

        self.assertAlmostEqual(metric["yesterday_limit_up_avg_change"], 7.0)
        self.assertAlmostEqual(metric["yesterday_continuous_avg_change"], 4.0)

    def test_aggregate_daily_metrics_treats_first_board_only_as_height_one(self):
        rows = [
            {
                "stock_code": "600010",
                "board_type": "main",
                "is_st": False,
                "yesterday_limit_up": False,
                "yesterday_continuous_days": 0,
                "today_touched_limit_up": True,
                "today_sealed_close": True,
                "today_opened_close": False,
                "today_broken": False,
                "today_continuous_days": 1,
                "change_pct": 9.95,
                "amount": 50000.0,
            }
        ]

        metric = self.service.aggregate_daily_metrics(
            trade_date=date(2026, 4, 27),
            stock_rows=rows,
            limit_down_count=0,
            market_turnover=0,
            up_count_ex_st=0,
            down_count_ex_st=0,
        )

        self.assertEqual(metric["max_board_height"], 1)
        self.assertEqual(metric["second_board_height"], 0)

    def test_aggregate_daily_metrics_uses_second_distinct_board_height(self):
        rows = [
            {
                "stock_code": "600011",
                "board_type": "main",
                "is_st": False,
                "yesterday_limit_up": True,
                "yesterday_continuous_days": 2,
                "today_touched_limit_up": True,
                "today_sealed_close": True,
                "today_opened_close": False,
                "today_broken": False,
                "today_continuous_days": 3,
                "change_pct": 10.0,
                "amount": 10000.0,
            },
            {
                "stock_code": "600012",
                "board_type": "main",
                "is_st": False,
                "yesterday_limit_up": True,
                "yesterday_continuous_days": 1,
                "today_touched_limit_up": True,
                "today_sealed_close": True,
                "today_opened_close": False,
                "today_broken": False,
                "today_continuous_days": 3,
                "change_pct": 10.0,
                "amount": 20000.0,
            },
            {
                "stock_code": "600013",
                "board_type": "main",
                "is_st": False,
                "yesterday_limit_up": True,
                "yesterday_continuous_days": 0,
                "today_touched_limit_up": True,
                "today_sealed_close": True,
                "today_opened_close": False,
                "today_broken": False,
                "today_continuous_days": 2,
                "change_pct": 10.0,
                "amount": 30000.0,
            },
        ]

        metric = self.service.aggregate_daily_metrics(
            trade_date=date(2026, 4, 27),
            stock_rows=rows,
            limit_down_count=0,
            market_turnover=0,
            up_count_ex_st=0,
            down_count_ex_st=0,
        )

        self.assertEqual(metric["max_board_height"], 3)
        self.assertEqual(metric["second_board_height"], 2)

    def test_aggregate_daily_metrics_uses_only_sealed_rows_for_board_heights(self):
        rows = [
            {
                "stock_code": "600021",
                "board_type": "main",
                "today_touched_limit_up": True,
                "today_sealed_close": False,
                "today_opened_close": True,
                "today_continuous_days": 5,
                "change_pct": -0.5,
                "amount": 10000.0,
            },
            {
                "stock_code": "600022",
                "board_type": "main",
                "today_touched_limit_up": True,
                "today_sealed_close": True,
                "today_opened_close": False,
                "today_continuous_days": 4,
                "change_pct": 10.0,
                "amount": 20000.0,
            },
            {
                "stock_code": "600023",
                "board_type": "main",
                "today_touched_limit_up": True,
                "today_sealed_close": False,
                "today_opened_close": True,
                "today_continuous_days": 3,
                "change_pct": 2.0,
                "amount": 30000.0,
            },
            {
                "stock_code": "300024",
                "board_type": "gem",
                "today_touched_limit_up": True,
                "today_sealed_close": True,
                "today_opened_close": False,
                "today_continuous_days": 2,
                "change_pct": 20.0,
                "amount": 40000.0,
            },
        ]

        metric = self.service.aggregate_daily_metrics(
            trade_date=date(2026, 6, 16),
            stock_rows=rows,
            limit_down_count=0,
            market_turnover=0,
            up_count_ex_st=0,
            down_count_ex_st=0,
        )

        self.assertEqual(metric["max_board_height"], 4)
        self.assertEqual(metric["second_board_height"], 2)
        self.assertEqual(metric["gem_board_height"], 2)
        self.assertEqual(metric["continuous_count"], 2)

    def test_aggregate_daily_metrics_excludes_delisting_period_stocks(self):
        rows = [
            {
                "stock_code": "920305",
                "stock_name": "云创退",
                "board_type": "bj",
                "yesterday_limit_up": True,
                "yesterday_continuous_days": 4,
                "today_touched_limit_up": True,
                "today_sealed_close": True,
                "today_opened_close": False,
                "today_continuous_days": 5,
                "change_pct": 29.49,
                "amount": 10000.0,
            },
            {
                "stock_code": "603580",
                "stock_name": "艾艾精工",
                "board_type": "main",
                "yesterday_limit_up": True,
                "yesterday_continuous_days": 3,
                "today_touched_limit_up": True,
                "today_sealed_close": True,
                "today_opened_close": False,
                "today_continuous_days": 4,
                "change_pct": 9.99,
                "amount": 20000.0,
            },
            {
                "stock_code": "000676",
                "stock_name": "智度股份",
                "board_type": "main",
                "yesterday_limit_up": True,
                "yesterday_continuous_days": 2,
                "today_touched_limit_up": True,
                "today_sealed_close": True,
                "today_opened_close": False,
                "today_continuous_days": 3,
                "change_pct": 10.0,
                "amount": 30000.0,
            },
        ]

        metric = self.service.aggregate_daily_metrics(
            trade_date=date(2026, 7, 17),
            stock_rows=rows,
            limit_down_count=323,
            market_turnover=26710.2,
            up_count_ex_st=461,
            down_count_ex_st=4812,
        )

        self.assertEqual(metric["limit_up_count"], 2)
        self.assertEqual(metric["continuous_count"], 2)
        self.assertEqual(metric["max_board_height"], 4)
        self.assertEqual(metric["second_board_height"], 3)
        self.assertAlmostEqual(metric["continuous_promotion_rate"], 100.0)
        self.assertAlmostEqual(metric["seal_rate"], 100.0)
        self.assertAlmostEqual(metric["limit_up_amount"], 50000.0)


if __name__ == "__main__":
    unittest.main()
