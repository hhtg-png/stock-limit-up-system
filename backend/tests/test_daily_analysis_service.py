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
            fact("300101", date(2026, 4, 22), stock_name="反包二十", sealed=False, open_count=4, close_price=10.2, high_price=12.0, pre_close=11.0, is_20cm=True),
            fact("300101", trade_day, stock_name="反包二十", close_price=13.0, high_price=14.0, pre_close=10.8, is_20cm=True),
            fact("300202", date(2026, 4, 21), stock_name="趋势二十", close_price=10.0, high_price=10.4, pre_close=9.8, is_20cm=True),
            fact("300202", date(2026, 4, 22), stock_name="趋势二十", close_price=10.8, high_price=11.0, pre_close=10.0, is_20cm=True),
            fact("300202", date(2026, 4, 23), stock_name="趋势二十", close_price=11.7, high_price=11.9, pre_close=10.8, is_20cm=True),
            fact("300202", trade_day, stock_name="趋势二十", close_price=12.9, high_price=13.6, pre_close=11.7, is_20cm=True),
            fact("002303", date(2026, 4, 18), stock_name="弹琴票", sealed=True, close_price=8.8, high_price=8.8, pre_close=8.0),
            fact("002303", date(2026, 4, 21), stock_name="弹琴票", sealed=False, open_count=3, close_price=8.3, high_price=8.9, pre_close=8.6),
            fact("002303", trade_day, stock_name="弹琴票", sealed=True, close_price=9.4, high_price=9.4, pre_close=8.6),
            fact("002404", date(2026, 4, 23), stock_name="炸板反包", sealed=False, open_count=5, close_price=6.5, high_price=7.3, pre_close=6.8),
            fact("002404", trade_day, stock_name="炸板反包", sealed=True, close_price=7.5, high_price=7.5, pre_close=6.5),
            fact("002505", date(2026, 4, 15), stock_name="二波票", continuous_days=2, close_price=5.5, high_price=5.5, pre_close=5.0),
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


if __name__ == "__main__":
    unittest.main()
