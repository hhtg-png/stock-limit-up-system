import unittest
from datetime import date

from app.api.v1.limit_up import _filter_and_sort_limit_up_list
from app.schemas.limit_up import LimitUpRealtime


def row(code, **values):
    item = LimitUpRealtime(stock_code=code, stock_name="Alpha", trade_date=date(2026, 10, 23),
                           limit_up_price=11, current_price=11, market="SH")
    return item.model_copy(update=values)


def sorted_codes(rows, field, direction):
    result = _filter_and_sort_limit_up_list(rows, None, None, None, field, direction)
    return [item.stock_code for item in result]


class LimitUpSortDirectionTests(unittest.TestCase):
    def test_descending_time_keeps_missing_after_valid_times(self):
        self.assertEqual(sorted_codes([row("missing", first_limit_up_time=None),
                                       row("early", first_limit_up_time="00:00:00"),
                                       row("late", first_limit_up_time="14:30:00")], "time", "desc"),
                         ["late", "early", "missing"])

    def test_ascending_seal_keeps_zero_before_missing_and_one_word_first_at_each_amount(self):
        values = [("missing", None, True), ("zero", 0, False), ("normal", 20, False),
                  ("one_word", 20, True), ("zero_one_word", 0, True)]
        self.assertEqual(sorted_codes([row(code, seal_amount=value, is_one_word=one_word)
                                       for code, value, one_word in values], "seal_amount", "asc"),
                         ["zero_one_word", "zero", "one_word", "normal", "missing"])

    def test_ascending_continuous_days_keeps_zero_before_missing(self):
        self.assertEqual(sorted_codes([row("missing", continuous_limit_up_days=None),
                                       row("two", continuous_limit_up_days=2),
                                       row("zero", continuous_limit_up_days=0)], "continuous_days", "asc"),
                         ["zero", "two", "missing"])

    def test_first_time_sorts_valid_hms_and_keeps_missing_last_with_stable_ties(self):
        values = [("missing", None), ("late", "10:52:37"), ("tie1", "09:25:00"),
                  ("tie2", "09:25:00"), ("blank", "  "), ("bad_hour", "25:00:00"),
                  ("short", "9:25:00"), ("midnight", "00:00:00"),
                  ("bad_second", "09:25:60"), ("non_string", 92500)]
        missing = ["missing", "blank", "bad_hour", "short", "bad_second", "non_string"]
        for direction in (None, "asc", "desc"):
            with self.subTest(direction=direction):
                valid = ["late", "tie1", "tie2", "midnight"] if direction == "desc" else [
                    "midnight", "tie1", "tie2", "late"]
                self.assertEqual(sorted_codes([row(code, first_limit_up_time=value) for code, value in values],
                                              "time", direction), valid + missing)

    def test_seal_amount_keeps_zero_and_one_word_priority_in_both_directions(self):
        values = [("missing", None, True), ("normal1", 20, False), ("one_word", 20, True),
                  ("normal2", 20, False), ("zero", 0, False), ("zero_one_word", 0, True),
                  ("nan", float("nan"), True), ("inf", float("inf"), False),
                  ("negative_inf", float("-inf"), True), ("invalid", "unknown", False),
                  ("blank", "", False), ("negative", -2, False)]
        missing = ["missing", "nan", "inf", "negative_inf", "invalid", "blank"]
        for direction in (None, "asc", "desc"):
            with self.subTest(direction=direction):
                valid = (["negative", "zero_one_word", "zero", "one_word", "normal1", "normal2"]
                         if direction == "asc" else
                         ["one_word", "normal1", "normal2", "zero_one_word", "zero", "negative"])
                self.assertEqual(sorted_codes([row(code, seal_amount=value, is_one_word=one_word)
                                               for code, value, one_word in values],
                                              "seal_amount", direction), valid + missing)

    def test_continuous_days_keeps_zero_and_stable_ties_with_missing_last(self):
        values = [("missing", None), ("two1", 2), ("two2", 2), ("zero", 0),
                  ("nan", float("nan")), ("inf", float("inf")),
                  ("negative_inf", float("-inf")), ("invalid", "unknown"), ("blank", "")]
        missing = ["missing", "nan", "inf", "negative_inf", "invalid", "blank"]
        for direction in (None, "asc", "desc"):
            with self.subTest(direction=direction):
                valid = ["zero", "two1", "two2"] if direction == "asc" else ["two1", "two2", "zero"]
                self.assertEqual(sorted_codes([row(code, continuous_limit_up_days=value) for code, value in values],
                                              "continuous_days", direction), valid + missing)


if __name__ == "__main__":
    unittest.main()
