import unittest

from app.database import Base
import app.models  # noqa: F401


class TradingPlaybookModelTests(unittest.TestCase):
    def test_all_trading_playbook_tables_are_registered(self):
        expected = {
            "trading_rule_sources",
            "trading_mode_rules",
            "trading_plan_versions",
            "trading_plan_candidates",
            "trading_alert_events",
            "trading_execution_reviews",
            "trading_playbook_settings",
        }
        self.assertTrue(expected.issubset(set(Base.metadata.tables)))

    def test_plan_version_uses_source_target_and_parent_columns(self):
        table = Base.metadata.tables["trading_plan_versions"]
        self.assertIn("source_trade_date", table.c)
        self.assertIn("target_trade_date", table.c)
        self.assertIn("parent_plan_version_id", table.c)

    def test_candidate_has_action_trade_date(self):
        table = Base.metadata.tables["trading_plan_candidates"]
        self.assertIn("action_trade_date", table.c)


if __name__ == "__main__":
    unittest.main()
