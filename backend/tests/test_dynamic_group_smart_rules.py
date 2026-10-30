import unittest
from datetime import date, datetime

from app.schemas.dynamic_group import DynamicGroupDefinition, DynamicGroupUpdate
from app.services import dynamic_group_rules as rules
from app.services.dynamic_group_source_service import DynamicGroupSourceService


class SmartRuleTests(unittest.TestCase):
    def test_nested_three_valued_logic_and_leaf_traversal(self):
        nodes = rules.validate_rules([{'all': [
            {'field': 'is_st', 'op': 'eq', 'value': False},
            {'any': [{'field': 'change_pct', 'op': 'gte', 'value': 5},
                     {'field': 'turnover_rate', 'op': 'gte', 'value': 10}]},
        ]}])
        self.assertTrue(rules.evaluate_rules({'is_st': False, 'change_pct': 6}, nodes))
        self.assertIsNone(rules.evaluate_rules({'is_st': False, 'change_pct': 4}, nodes))
        self.assertFalse(rules.evaluate_rules({'is_st': True, 'change_pct': 6}, nodes))
        self.assertEqual([r['field'] for r in rules.iter_rule_leaves(nodes)],
                         ['is_st', 'change_pct', 'turnover_rate'])

    def test_rule_tree_bounds_and_ambiguous_nodes_are_rejected(self):
        leaf = {'field': 'change_pct', 'op': 'gte', 'value': 1}
        for invalid in ([{'all': []}], [{'all': [leaf], 'any': [leaf]}],
                        [{'all': [leaf], 'field': 'change_pct'}],
                        [{'all': [leaf] * 30}, leaf],
                        [{'all': [{'any': [{'all': [{'any': [leaf]}]}]}]}],
                        [{**leaf, 'days': 5}], [{**leaf, 'unknown': 1}]):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                rules.validate_rules(invalid)

    def test_history_windows_are_explicit_and_independent(self):
        nodes = rules.validate_rules([
            {'field': 'recent_limit_up_count', 'op': 'gte', 'value': 1, 'days': 5},
            {'field': 'recent_limit_up_count', 'op': 'gte', 'value': 3, 'days': 10},
        ])
        row = {'historical_features': {'recent_limit_up_count': {'5': 1, '10': 2}}}
        self.assertFalse(rules.evaluate_rules(row, nodes))
        row['historical_features']['recent_limit_up_count']['10'] = 3
        self.assertTrue(rules.evaluate_rules(row, nodes))
        for days in (None, 0, 61, True, 1.5, '5'):
            leaf = {'field': 'recent_limit_up_count', 'op': 'gte', 'value': 1, 'days': days}
            with self.subTest(days=days), self.assertRaises(ValueError):
                rules.validate_rules([leaf])

    def test_exact_five_percent_auction_threshold_keeps_real_differences(self):
        source = DynamicGroupSourceService(topic_records={})
        row = source._row('600001', {'name': 'test', 'price': 10, 'pre_close': 9, 'open': 9.45},
            datetime(2026, 10, 30, 10), date(2026, 10, 30), date(2026, 10, 29), {}, {})
        for op, expected in [('eq', True), ('gte', True), ('lt', False)]:
            with self.subTest(op=op):
                self.assertEqual(rules.evaluate_rules(row, rules.validate_rules([
                    {'field': 'today_open_change_pct', 'op': op, 'value': 5}])), expected)
        self.assertFalse(rules.evaluate_rules({'today_open_change_pct': 4.9999},
            rules.validate_rules([{'field': 'today_open_change_pct', 'op': 'gte', 'value': 5}])))

    def test_strategy_schema_and_historical_ranking_contract(self):
        definition = dict(name='smart', rules=[{'field': 'change_pct', 'op': 'gte', 'value': 0}],
            strategy=dict(prompt='近五日涨停次数最多的前十', summary='按已完成交易日次数排序', assumptions=[],
                          ranking=dict(field='recent_limit_up_count', days=5, direction='desc', limit=10)))
        parsed = DynamicGroupDefinition.model_validate(definition).model_dump()
        self.assertEqual(parsed['strategy']['ranking']['days'], 5)
        self.assertIsNone(DynamicGroupUpdate.model_validate({'strategy': None}).strategy)
        for ranking in (dict(field='recent_limit_up_count', direction='desc'),
                        dict(field='change_pct', direction='desc', days=5),
                        dict(field='stock_name', direction='asc'),
                        dict(field='change_pct', direction=['desc']),
                        dict(field='change_pct', direction='desc', limit=501)):
            with self.subTest(ranking=ranking), self.assertRaises(ValueError):
                DynamicGroupDefinition.model_validate({**definition, 'strategy': {
                    **definition['strategy'], 'ranking': ranking}})
