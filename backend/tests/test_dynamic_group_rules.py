import unittest

from app.services.dynamic_group_rules import evaluate_rules, validate_rules


class DynamicGroupRulesTests(unittest.TestCase):
    def test_unknown_is_not_zero_or_false(self):
        rule = [{"field": "today_open_change_pct", "op": "gte", "value": 0}]
        self.assertIsNone(evaluate_rules({}, rule, "all"))
        self.assertTrue(evaluate_rules({"today_open_change_pct": 0}, rule, "all"))
        self.assertIsNone(evaluate_rules({"today_open_change_pct": float("nan")}, rule, "all"))
        self.assertIsNone(evaluate_rules({}, [{"field": "is_st", "op": "eq", "value": False}], "all"))

    def test_all_any_apply_three_valued_logic(self):
        rules = [{"field": "change_pct", "op": "gte", "value": 3},
                 {"field": "turnover_rate", "op": "gte", "value": 5}]
        self.assertIsNone(evaluate_rules({"change_pct": 4}, rules, "all"))
        self.assertFalse(evaluate_rules({"change_pct": 2}, rules, "all"))
        self.assertTrue(evaluate_rules({"change_pct": 4}, rules, "any"))
        self.assertIsNone(evaluate_rules({"change_pct": 2}, rules, "any"))

    def test_times_concepts_boolean_and_ranges(self):
        row = {"yesterday_final_seal_time": "10:52:37", "concepts": ["机器人", "人工智能"],
               "amount_yi": 3.5, "is_st": False, "market": "SH"}
        rules = [{"field": "yesterday_final_seal_time", "op": "lte", "value": "11:00"},
                 {"field": "concepts", "op": "contains", "value": "机器人"},
                 {"field": "amount_yi", "op": "between", "value": 3, "value2": 4},
                 {"field": "is_st", "op": "eq", "value": False},
                 {"field": "market", "op": "in", "value": ["SH", "SZ"]}]
        normalized = validate_rules(rules)
        self.assertEqual(normalized[0]["value"], "11:00:00")
        self.assertTrue(evaluate_rules(row, normalized, "all"))

    def test_validation_rejects_unsupported_or_ambiguous_values(self):
        for rules in ([], [{"field": "__sql__", "op": "eq", "value": 1}],
                      [{"field": "change_pct", "op": "contains", "value": "3"}],
                      [{"field": "change_pct", "op": "gte", "value": True}],
                      [{"field": "change_pct", "op": "gte", "value": "nan"}],
                      [{"field": "change_pct", "op": "between", "value": 5, "value2": 2}],
                      [{"field": "is_st", "op": "eq", "value": "wrong"}],
                      [{"field": "market", "op": "eq", "value": "US"}],
                      [{"field": "yesterday_final_seal_time", "op": "lte", "value": "29:01"}]):
            with self.subTest(rules=rules), self.assertRaises(ValueError):
                validate_rules(rules)
