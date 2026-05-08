import unittest
from datetime import date, datetime

from app.services.daily_analysis_service import (
    DAILY_ANALYSIS_COLUMNS,
    DailyAnalysisRuleEngine,
    DailyAnalysisStockFact,
)


def fact(
    stock_code: str,
    trade_date: date,
    *,
    stock_name: str | None = None,
    reason_category: str = "人工智能",
    continuous_days: int = 1,
    open_count: int = 0,
    sealed: bool = True,
    is_20cm: bool = False,
    first_time: str = "09:35:00",
    final_time: str | None = "09:35:00",
    open_price: float = 10.0,
    close_price: float = 11.0,
    high_price: float = 11.0,
    low_price: float = 9.8,
    pre_close: float = 10.0,
    amount: float = 10000.0,
    turnover_rate: float = 8.0,
) -> DailyAnalysisStockFact:
    hh, mm, ss = [int(part) for part in first_time.split(":")]
    first_limit_time = datetime(trade_date.year, trade_date.month, trade_date.day, hh, mm, ss)
    final_limit_time = None
    if final_time:
        fh, fm, fs = [int(part) for part in final_time.split(":")]
        final_limit_time = datetime(trade_date.year, trade_date.month, trade_date.day, fh, fm, fs)

    return DailyAnalysisStockFact(
        trade_date=trade_date,
        stock_code=stock_code,
        stock_name=stock_name or f"股票{stock_code[-2:]}",
        reason_category=reason_category,
        limit_up_reason=reason_category,
        continuous_days=continuous_days,
        open_count=open_count,
        is_final_sealed=sealed,
        is_20cm=is_20cm,
        first_limit_time=first_limit_time,
        final_seal_time=final_limit_time,
        open_price=open_price,
        close_price=close_price,
        high_price=high_price,
        low_price=low_price,
        pre_close=pre_close,
        change_pct=round((close_price - pre_close) / pre_close * 100, 2),
        amount=amount,
        turnover_rate=turnover_rate,
    )


class DailyAnalysisRuleEngineTests(unittest.TestCase):
    def test_build_daily_result_flags_all_required_signal_columns(self):
        trade_day = date(2026, 4, 24)
        facts = [
            fact("000001", trade_day, stock_name="唯一高标", continuous_days=4, first_time="09:31:00", amount=90000),
            fact("300101", date(2026, 4, 22), stock_name="反包二十", close_price=11.0, high_price=11.0, pre_close=10.0, is_20cm=True),
            fact("300101", date(2026, 4, 23), stock_name="反包二十", sealed=False, open_count=4, close_price=10.2, high_price=12.0, pre_close=11.0, is_20cm=True),
            fact("300101", trade_day, stock_name="反包二十", close_price=13.0, high_price=14.0, pre_close=10.8, is_20cm=True),
            fact("300202", date(2026, 4, 21), stock_name="趋势二十", close_price=10.0, high_price=10.4, pre_close=9.8, is_20cm=True),
            fact("300202", date(2026, 4, 22), stock_name="趋势二十", close_price=10.8, high_price=11.0, pre_close=10.0, is_20cm=True),
            fact("300202", date(2026, 4, 23), stock_name="趋势二十", close_price=11.7, high_price=11.9, pre_close=10.8, is_20cm=True),
            fact("300202", trade_day, stock_name="趋势二十", close_price=12.9, high_price=13.6, pre_close=11.7, is_20cm=True),
            fact("002303", date(2026, 4, 16), stock_name="弹琴票", sealed=True, close_price=8.2, high_price=8.2, pre_close=7.5),
            fact("002303", date(2026, 4, 18), stock_name="弹琴票", sealed=True, close_price=8.8, high_price=8.8, pre_close=8.0),
            fact("002303", date(2026, 4, 21), stock_name="弹琴票", sealed=False, open_count=3, close_price=8.3, high_price=8.9, pre_close=8.6),
            fact("002303", trade_day, stock_name="弹琴票", sealed=True, close_price=9.4, high_price=9.4, pre_close=8.6),
            fact("002404", date(2026, 4, 23), stock_name="炸板反包", sealed=False, open_count=5, close_price=6.5, high_price=7.3, pre_close=6.8),
            fact("002404", trade_day, stock_name="炸板反包", sealed=True, close_price=7.5, high_price=7.5, pre_close=6.5),
            fact("002505", date(2026, 4, 15), stock_name="二波票", continuous_days=4, close_price=5.5, high_price=5.5, pre_close=5.0),
            fact("002505", date(2026, 4, 19), stock_name="二波票", sealed=False, open_count=1, close_price=5.0, high_price=5.4, pre_close=5.2),
            fact("002505", trade_day, stock_name="二波票", sealed=True, close_price=6.0, high_price=6.0, pre_close=5.4),
            fact("300606", trade_day, stock_name="二十长影", sealed=False, open_count=2, is_20cm=True, close_price=10.8, high_price=12.4, pre_close=10.0),
            fact("002707", trade_day, stock_name="一字套利", open_price=11.0, close_price=11.0, high_price=11.0, low_price=11.0, pre_close=10.0, first_time="09:25:02", final_time="09:25:02"),
            fact("002808", date(2026, 4, 23), stock_name="昨日核心", continuous_days=3, first_time="09:30:00", amount=120000),
            fact("002808", trade_day, stock_name="昨日核心", sealed=False, open_count=6, close_price=8.9, high_price=10.0, pre_close=9.8),
        ]

        result = DailyAnalysisRuleEngine().build_daily_result(trade_day, facts)

        self.assertEqual(list(result.keys()), DAILY_ANALYSIS_COLUMNS[1:])
        self.assertEqual(result["连板唯一性"]["items"][0]["stock_code"], "000001")
        self.assertIn("唯一", result["连板唯一性"]["items"][0]["tags"])

        combined_items = result["反包+趋势+弹钢琴"]["items"]
        self.assertTrue(all(len(item["tags"]) == 1 for item in combined_items))
        self.assertTrue(any(item["stock_code"] == "300101" and "反包" in item["tags"] for item in combined_items))
        self.assertTrue(any(item["stock_code"] == "300202" and "趋势" in item["tags"] for item in combined_items))
        self.assertTrue(any(item["stock_code"] == "002303" and "弹钢琴" in item["tags"] for item in combined_items))

        self.assertTrue(any(item["stock_code"] == "002404" for item in result["炸板反包"]["items"]))
        self.assertTrue(any(item["stock_code"] == "000001" for item in result["辨识度"]["items"]))
        self.assertTrue(any(item["stock_code"] == "002505" for item in result["二波"]["items"]))
        self.assertTrue(any(item["stock_code"] == "300606" and "长上影" in item["tags"] for item in result["20cm"]["items"]))
        self.assertTrue(any(item["stock_code"] == "002707" for item in result["一字套利"]["items"]))
        self.assertTrue(any(item["label"] == "人工智能" for item in result["板块"]["items"]))
        self.assertTrue(any(item["stock_code"] == "002808" for item in result["负反馈"]["items"]))

    def test_formats_times_by_clock_time_even_when_source_date_is_wrong(self):
        trade_day = date(2026, 4, 24)
        wrong_source_time = datetime(2026, 4, 25, 9, 25, 2)
        stock = DailyAnalysisStockFact(
            trade_date=trade_day,
            stock_code="603318",
            stock_name="水发燃气",
            reason_category="其他",
            limit_up_reason="燃气轮机",
            continuous_days=3,
            open_count=0,
            is_final_sealed=True,
            is_20cm=False,
            first_limit_time=wrong_source_time,
            final_seal_time=wrong_source_time,
            open_price=10.0,
            close_price=11.0,
            high_price=11.0,
            low_price=10.0,
            pre_close=10.0,
            change_pct=10.0,
            amount=18000.0,
            turnover_rate=5.9,
        )

        result = DailyAnalysisRuleEngine().build_daily_result(trade_day, [stock])

        self.assertEqual(result["连板唯一性"]["items"][0]["time"], "09:25:02")

    def test_rebound_requires_one_plus_one_and_broken_rebound_requires_first_yesterday_break(self):
        trade_day = date(2026, 4, 24)
        yesterday = date(2026, 4, 23)
        earlier = date(2026, 4, 22)
        facts = [
            fact("000001", trade_day, stock_name="市场日期占位", continuous_days=3),
            fact("001111", earlier, stock_name="普通反包", close_price=11.0, high_price=11.0, pre_close=10.0),
            fact("001111", yesterday, stock_name="普通反包", sealed=False, open_count=3, close_price=10.4, high_price=11.2, pre_close=11.0),
            fact("001111", trade_day, stock_name="普通反包", close_price=11.3, high_price=11.3, pre_close=10.4),
            fact("002222", yesterday, stock_name="首次炸板", sealed=False, open_count=5, close_price=6.5, high_price=7.3, pre_close=6.8),
            fact("002222", trade_day, stock_name="首次炸板", close_price=7.5, high_price=7.5, pre_close=6.5),
            fact("002223", yesterday, stock_name="炸板连板", sealed=False, open_count=5, close_price=6.5, high_price=7.3, pre_close=6.8),
            fact("002223", trade_day, stock_name="炸板连板", continuous_days=2, close_price=7.5, high_price=7.5, pre_close=6.5),
            fact("003333", earlier, stock_name="前日炸板", sealed=False, open_count=5, close_price=8.5, high_price=9.3, pre_close=8.8),
            fact("003333", trade_day, stock_name="前日炸板", close_price=9.4, high_price=9.4, pre_close=8.5),
            fact("004444", earlier, stock_name="老票炸板", close_price=5.5, high_price=5.5, pre_close=5.0),
            fact("004444", yesterday, stock_name="老票炸板", sealed=False, open_count=5, close_price=5.1, high_price=5.7, pre_close=5.4),
            fact("004444", trade_day, stock_name="老票炸板", close_price=5.8, high_price=5.8, pre_close=5.1),
            fact("005555", earlier, stock_name="连板弹琴", close_price=11.0, high_price=11.0, pre_close=10.0),
            fact("005555", yesterday, stock_name="连板弹琴", open_count=2, close_price=12.0, high_price=12.0, pre_close=11.0),
            fact("005555", trade_day, stock_name="连板弹琴", continuous_days=2, close_price=13.2, high_price=13.2, pre_close=12.0),
        ]

        result = DailyAnalysisRuleEngine().build_daily_result(trade_day, facts)
        combined_by_code = {
            item["stock_code"]: item
            for item in result["反包+趋势+弹钢琴"]["items"]
        }
        broken_codes = {
            item["stock_code"]
            for item in result["炸板反包"]["items"]
        }

        self.assertEqual(combined_by_code["001111"]["tags"], ["反包"])
        self.assertNotIn("002222", combined_by_code)
        self.assertIn("002222", broken_codes)
        self.assertNotIn("002223", broken_codes)
        self.assertNotIn("003333", broken_codes)
        self.assertNotIn("004444", broken_codes)
        self.assertNotIn("005555", combined_by_code)

    def test_rebound_and_second_wave_use_distinct_break_windows(self):
        trade_dates = [
            date(2026, 4, 10),
            date(2026, 4, 13),
            date(2026, 4, 14),
            date(2026, 4, 15),
            date(2026, 4, 16),
            date(2026, 4, 17),
            date(2026, 4, 20),
            date(2026, 4, 21),
            date(2026, 4, 22),
            date(2026, 4, 23),
            date(2026, 4, 24),
        ]
        trade_day = trade_dates[-1]
        facts = [
            *[
                fact(
                    f"9000{index:02d}",
                    trade_date,
                    stock_name=f"日期占位{index}",
                    continuous_days=1,
                    close_price=10 + index,
                    high_price=10 + index,
                    pre_close=9 + index,
                )
                for index, trade_date in enumerate(trade_dates)
            ],
            fact("101111", trade_dates[-3], stock_name="一日反包", close_price=11.0, high_price=11.0, pre_close=10.0),
            fact("101111", trade_day, stock_name="一日反包", close_price=11.2, high_price=11.2, pre_close=10.1),
            fact("101112", trade_dates[-4], stock_name="两日不反包", close_price=9.9, high_price=10.0, pre_close=9.0),
            fact("101112", trade_day, stock_name="两日不反包", close_price=10.2, high_price=10.2, pre_close=9.4),
            fact("202222", trade_dates[4], stock_name="四板二波", continuous_days=4, close_price=20.0, high_price=20.0, pre_close=18.2),
            fact("202222", trade_day, stock_name="四板二波", close_price=20.2, high_price=20.2, pre_close=18.4),
            fact("202223", trade_dates[4], stock_name="三板不二波", continuous_days=3, close_price=15.0, high_price=15.0, pre_close=13.6),
            fact("202223", trade_day, stock_name="三板不二波", close_price=15.2, high_price=15.2, pre_close=13.8),
            fact("202224", trade_dates[-3], stock_name="一日不二波", continuous_days=4, close_price=12.0, high_price=12.0, pre_close=10.9),
            fact("202224", trade_day, stock_name="一日不二波", close_price=12.2, high_price=12.2, pre_close=11.1),
            fact("202225", trade_dates[0], stock_name="太久不二波", continuous_days=4, close_price=8.0, high_price=8.0, pre_close=7.3),
            fact("202225", trade_day, stock_name="太久不二波", close_price=8.2, high_price=8.2, pre_close=7.5),
        ]

        result = DailyAnalysisRuleEngine().build_daily_result(trade_day, facts)
        combined_by_code = {
            item["stock_code"]: item
            for item in result["反包+趋势+弹钢琴"]["items"]
        }
        second_wave_codes = {
            item["stock_code"]
            for item in result["二波"]["items"]
        }

        self.assertIn("101111", combined_by_code)
        self.assertEqual(combined_by_code["101111"]["tags"], ["反包"])
        self.assertNotIn("101112", combined_by_code)
        self.assertIn("202222", second_wave_codes)
        self.assertNotIn("202223", second_wave_codes)
        self.assertNotIn("202224", second_wave_codes)
        self.assertNotIn("202225", second_wave_codes)

    def test_ongoing_second_wave_includes_current_high_board_after_prior_wave(self):
        trade_dates = [
            date(2026, 4, 21),
            date(2026, 4, 22),
            date(2026, 4, 23),
            date(2026, 4, 24),
            date(2026, 4, 27),
            date(2026, 4, 28),
            date(2026, 4, 29),
            date(2026, 4, 30),
            date(2026, 5, 5),
            date(2026, 5, 6),
            date(2026, 5, 7),
        ]
        trade_day = trade_dates[-1]
        facts = [
            *[
                fact(
                    f"9100{index:02d}",
                    trade_date,
                    stock_name=f"日期占位{index}",
                    close_price=10 + index,
                    high_price=10 + index,
                    pre_close=9 + index,
                )
                for index, trade_date in enumerate(trade_dates)
            ],
            fact("002081", trade_dates[0], stock_name="金螳螂", continuous_days=1, close_price=4.49, high_price=4.49, pre_close=4.08),
            fact("002081", trade_dates[1], stock_name="金螳螂", continuous_days=2, close_price=4.94, high_price=4.94, pre_close=4.49),
            fact("002081", trade_dates[2], stock_name="金螳螂", continuous_days=3, close_price=5.43, high_price=5.43, pre_close=4.94),
            fact("002081", trade_dates[6], stock_name="金螳螂", continuous_days=2, close_price=5.36, high_price=5.36, pre_close=4.87),
            fact("002081", trade_dates[7], stock_name="金螳螂", continuous_days=3, close_price=5.90, high_price=5.90, pre_close=5.36),
            fact("002081", trade_dates[8], stock_name="金螳螂", continuous_days=3, close_price=5.90, high_price=5.90, pre_close=5.36),
            fact("002081", trade_dates[9], stock_name="金螳螂", continuous_days=4, close_price=6.49, high_price=6.49, pre_close=5.90),
            fact("002081", trade_day, stock_name="金螳螂", continuous_days=5, close_price=7.14, high_price=7.14, pre_close=6.49),
            fact("333333", trade_dates[6], stock_name="单波五板", continuous_days=1, close_price=3.3, high_price=3.3, pre_close=3.0),
            fact("333333", trade_dates[7], stock_name="单波五板", continuous_days=2, close_price=3.6, high_price=3.6, pre_close=3.3),
            fact("333333", trade_dates[8], stock_name="单波五板", continuous_days=3, close_price=4.0, high_price=4.0, pre_close=3.6),
            fact("333333", trade_dates[9], stock_name="单波五板", continuous_days=4, close_price=4.4, high_price=4.4, pre_close=4.0),
            fact("333333", trade_day, stock_name="单波五板", continuous_days=5, close_price=4.8, high_price=4.8, pre_close=4.4),
        ]

        result = DailyAnalysisRuleEngine().build_daily_result(trade_day, facts)
        second_wave_codes = {
            item["stock_code"]
            for item in result["二波"]["items"]
        }

        self.assertIn("002081", second_wave_codes)
        self.assertNotIn("333333", second_wave_codes)


if __name__ == "__main__":
    unittest.main()
