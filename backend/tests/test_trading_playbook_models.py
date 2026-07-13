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

    def test_mode_rule_uses_exact_json_column_names(self):
        table = Base.metadata.tables["trading_mode_rules"]
        expected = {
            "prerequisites_json",
            "candidate_filters_json",
            "entry_trigger_json",
            "invalidation_json",
            "exit_trigger_json",
            "risk_guidance_json",
            "source_refs_json",
        }
        self.assertTrue(expected.issubset(table.c.keys()))

    def test_plan_version_uses_exact_json_column_names(self):
        table = Base.metadata.tables["trading_plan_versions"]
        expected = {
            "market_state_json",
            "theme_ranking_json",
            "mode_radar_json",
            "rule_snapshot_json",
            "risk_settings_json",
            "data_quality_json",
            "change_summary_json",
        }
        self.assertTrue(expected.issubset(table.c.keys()))

    def test_candidate_uses_exact_json_column_names(self):
        table = Base.metadata.tables["trading_plan_candidates"]
        expected = {
            "supporting_mode_keys_json",
            "recognition_json",
            "entry_trigger_json",
            "invalidation_json",
            "exit_trigger_json",
            "evidence_json",
            "manual_overrides_json",
        }
        self.assertTrue(expected.issubset(table.c.keys()))

    def test_alert_event_uses_exact_json_column_names(self):
        table = Base.metadata.tables["trading_alert_events"]
        expected = {"market_snapshot_json", "channel_status_json"}
        self.assertTrue(expected.issubset(table.c.keys()))

    def test_execution_review_uses_exact_json_column_names(self):
        table = Base.metadata.tables["trading_execution_reviews"]
        expected = {
            "signal_review_json",
            "manual_execution_json",
            "plan_compliance_json",
            "outcome_snapshot_json",
            "data_quality_json",
        }
        self.assertTrue(expected.issubset(table.c.keys()))

    def test_settings_use_exact_json_column_name(self):
        table = Base.metadata.tables["trading_playbook_settings"]
        self.assertIn("channel_config_json", table.c)


if __name__ == "__main__":
    unittest.main()
