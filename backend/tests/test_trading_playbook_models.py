import unittest

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    Float,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
    CheckConstraint,
)
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.database import Base
import app.models  # noqa: F401
from app.models import TradingModeRule
from app.models.trading_playbook import TradingPlaybookSettings


def test_playbook_settings_has_database_risk_invariant_check():
    names = {
        constraint.name
        for constraint in TradingPlaybookSettings.__table__.constraints
        if isinstance(constraint, CheckConstraint)
    }
    assert "ck_trading_playbook_settings_risk" in names


TABLE_COLUMNS = {
    "trading_rule_sources": {
        "id",
        "source_key",
        "source_path",
        "source_title",
        "content_hash",
        "transcript_generated_at",
        "ingested_at",
        "status",
    },
    "trading_mode_rules": {
        "id",
        "mode_key",
        "version",
        "name",
        "family",
        "style",
        "window",
        "automation_level",
        "description",
        "prerequisites_json",
        "candidate_filters_json",
        "entry_trigger_json",
        "invalidation_json",
        "exit_trigger_json",
        "risk_guidance_json",
        "source_refs_json",
        "enabled",
        "content_hash",
        "created_at",
    },
    "trading_plan_versions": {
        "id",
        "source_trade_date",
        "target_trade_date",
        "stage",
        "version_no",
        "parent_plan_version_id",
        "status",
        "market_state_json",
        "theme_ranking_json",
        "mode_radar_json",
        "rule_snapshot_json",
        "risk_settings_json",
        "data_quality_json",
        "change_summary_json",
        "input_hash",
        "generated_at",
        "confirmed_at",
        "confirmed_by",
    },
    "trading_plan_candidates": {
        "id",
        "plan_version_id",
        "stock_code",
        "stock_name",
        "action_trade_date",
        "theme_name",
        "primary_mode_key",
        "supporting_mode_keys_json",
        "role",
        "rank",
        "recognition_json",
        "entry_trigger_json",
        "invalidation_json",
        "exit_trigger_json",
        "risk_level",
        "position_reference",
        "evidence_json",
        "manual_overrides_json",
        "status",
    },
    "trading_alert_events": {
        "id",
        "plan_version_id",
        "candidate_id",
        "event_type",
        "severity",
        "dedup_key",
        "triggered_at",
        "market_snapshot_json",
        "message",
        "channel_status_json",
        "acknowledged_at",
    },
    "trading_alert_condition_states": {
        "id",
        "candidate_id",
        "event_type",
        "condition_version",
        "active",
        "occurrence_no",
        "last_matched_at",
        "last_recovered_at",
        "updated_at",
    },
    "trading_execution_reviews": {
        "id",
        "trade_date",
        "plan_version_id",
        "signal_review_json",
        "manual_execution_json",
        "plan_compliance_json",
        "outcome_snapshot_json",
        "data_quality_json",
        "generated_at",
        "finalized_at",
    },
    "trading_playbook_job_claims": {
        "id",
        "job_key",
        "job_type",
        "phase",
        "source_trade_date",
        "target_trade_date",
        "stage",
        "generation_key",
        "owner",
        "status",
        "attempt_no",
        "lease_expires_at",
        "completed_at",
        "last_error",
        "created_at",
        "updated_at",
    },
    "trading_playbook_settings": {
        "id",
        "enabled",
        "trial_position_pct",
        "confirmed_position_pct",
        "hard_stop_pct",
        "max_action_candidates",
        "in_app_enabled",
        "wechat_enabled",
        "channel_config_json",
        "updated_at",
    },
}

COLUMN_TYPES = {
    "trading_rule_sources": {
        "id": (Integer, None),
        "source_key": (String, 80),
        "source_path": (String, 500),
        "source_title": (String, 255),
        "content_hash": (String, 64),
        "transcript_generated_at": (DateTime, None),
        "ingested_at": (DateTime, None),
        "status": (String, 20),
    },
    "trading_mode_rules": {
        "id": (Integer, None),
        "mode_key": (String, 80),
        "version": (Integer, None),
        "name": (String, 120),
        "family": (String, 40),
        "style": (String, 40),
        "window": (String, 80),
        "automation_level": (String, 20),
        "description": (Text, None),
        "prerequisites_json": (JSON, None),
        "candidate_filters_json": (JSON, None),
        "entry_trigger_json": (JSON, None),
        "invalidation_json": (JSON, None),
        "exit_trigger_json": (JSON, None),
        "risk_guidance_json": (JSON, None),
        "source_refs_json": (JSON, None),
        "enabled": (Boolean, None),
        "content_hash": (String, 64),
        "created_at": (DateTime, None),
    },
    "trading_plan_versions": {
        "id": (Integer, None),
        "source_trade_date": (Date, None),
        "target_trade_date": (Date, None),
        "stage": (String, 20),
        "version_no": (Integer, None),
        "parent_plan_version_id": (Integer, None),
        "status": (String, 20),
        "market_state_json": (JSON, None),
        "theme_ranking_json": (JSON, None),
        "mode_radar_json": (JSON, None),
        "rule_snapshot_json": (JSON, None),
        "risk_settings_json": (JSON, None),
        "data_quality_json": (JSON, None),
        "change_summary_json": (JSON, None),
        "input_hash": (String, 64),
        "generated_at": (DateTime, None),
        "confirmed_at": (DateTime, None),
        "confirmed_by": (String, 80),
    },
    "trading_plan_candidates": {
        "id": (Integer, None),
        "plan_version_id": (Integer, None),
        "stock_code": (String, 10),
        "stock_name": (String, 50),
        "action_trade_date": (Date, None),
        "theme_name": (String, 120),
        "primary_mode_key": (String, 80),
        "supporting_mode_keys_json": (JSON, None),
        "role": (String, 60),
        "rank": (Integer, None),
        "recognition_json": (JSON, None),
        "entry_trigger_json": (JSON, None),
        "invalidation_json": (JSON, None),
        "exit_trigger_json": (JSON, None),
        "risk_level": (String, 20),
        "position_reference": (Float, None),
        "evidence_json": (JSON, None),
        "manual_overrides_json": (JSON, None),
        "status": (String, 20),
    },
    "trading_alert_events": {
        "id": (Integer, None),
        "plan_version_id": (Integer, None),
        "candidate_id": (Integer, None),
        "event_type": (String, 40),
        "severity": (String, 20),
        "dedup_key": (String, 255),
        "triggered_at": (DateTime, None),
        "market_snapshot_json": (JSON, None),
        "message": (Text, None),
        "channel_status_json": (JSON, None),
        "acknowledged_at": (DateTime, None),
    },
    "trading_alert_condition_states": {
        "id": (Integer, None),
        "candidate_id": (Integer, None),
        "event_type": (String, 40),
        "condition_version": (String, 64),
        "active": (Boolean, None),
        "occurrence_no": (Integer, None),
        "last_matched_at": (DateTime, None),
        "last_recovered_at": (DateTime, None),
        "updated_at": (DateTime, None),
    },
    "trading_execution_reviews": {
        "id": (Integer, None),
        "trade_date": (Date, None),
        "plan_version_id": (Integer, None),
        "signal_review_json": (JSON, None),
        "manual_execution_json": (JSON, None),
        "plan_compliance_json": (JSON, None),
        "outcome_snapshot_json": (JSON, None),
        "data_quality_json": (JSON, None),
        "generated_at": (DateTime, None),
        "finalized_at": (DateTime, None),
    },
    "trading_playbook_job_claims": {
        "id": (Integer, None),
        "job_key": (String, 255),
        "job_type": (String, 40),
        "phase": (String, 40),
        "source_trade_date": (Date, None),
        "target_trade_date": (Date, None),
        "stage": (String, 20),
        "generation_key": (String, 120),
        "owner": (String, 80),
        "status": (String, 20),
        "attempt_no": (Integer, None),
        "lease_expires_at": (DateTime, None),
        "completed_at": (DateTime, None),
        "last_error": (Text, None),
        "created_at": (DateTime, None),
        "updated_at": (DateTime, None),
    },
    "trading_playbook_settings": {
        "id": (Integer, None),
        "enabled": (Boolean, None),
        "trial_position_pct": (Float, None),
        "confirmed_position_pct": (Float, None),
        "hard_stop_pct": (Float, None),
        "max_action_candidates": (Integer, None),
        "in_app_enabled": (Boolean, None),
        "wechat_enabled": (Boolean, None),
        "channel_config_json": (JSON, None),
        "updated_at": (DateTime, None),
    },
}

NULLABLE_COLUMNS = {
    ("trading_rule_sources", "transcript_generated_at"),
    ("trading_plan_versions", "parent_plan_version_id"),
    ("trading_plan_versions", "confirmed_at"),
    ("trading_plan_versions", "confirmed_by"),
    ("trading_alert_events", "candidate_id"),
    ("trading_alert_events", "acknowledged_at"),
    ("trading_alert_condition_states", "last_matched_at"),
    ("trading_alert_condition_states", "last_recovered_at"),
    ("trading_execution_reviews", "finalized_at"),
    ("trading_playbook_job_claims", "source_trade_date"),
    ("trading_playbook_job_claims", "target_trade_date"),
    ("trading_playbook_job_claims", "stage"),
    ("trading_playbook_job_claims", "generation_key"),
    ("trading_playbook_job_claims", "lease_expires_at"),
    ("trading_playbook_job_claims", "completed_at"),
    ("trading_playbook_job_claims", "last_error"),
}

SCALAR_DEFAULTS = {
    ("trading_rule_sources", "status"): "ready",
    ("trading_mode_rules", "description"): "",
    ("trading_mode_rules", "enabled"): True,
    ("trading_plan_versions", "status"): "draft",
    ("trading_plan_candidates", "theme_name"): "",
    ("trading_plan_candidates", "position_reference"): 0,
    ("trading_plan_candidates", "status"): "waiting",
    ("trading_alert_condition_states", "active"): False,
    ("trading_alert_condition_states", "occurrence_no"): 0,
    ("trading_playbook_job_claims", "status"): "running",
    ("trading_playbook_job_claims", "attempt_no"): 1,
    ("trading_playbook_settings", "id"): 1,
    ("trading_playbook_settings", "enabled"): True,
    ("trading_playbook_settings", "trial_position_pct"): 10,
    ("trading_playbook_settings", "confirmed_position_pct"): 30,
    ("trading_playbook_settings", "hard_stop_pct"): 5,
    ("trading_playbook_settings", "max_action_candidates"): 3,
    ("trading_playbook_settings", "in_app_enabled"): True,
    ("trading_playbook_settings", "wechat_enabled"): False,
}

DATETIME_DEFAULTS = {
    ("trading_rule_sources", "ingested_at"),
    ("trading_mode_rules", "created_at"),
    ("trading_plan_versions", "generated_at"),
    ("trading_alert_events", "triggered_at"),
    ("trading_alert_condition_states", "updated_at"),
    ("trading_execution_reviews", "generated_at"),
    ("trading_playbook_job_claims", "created_at"),
    ("trading_playbook_job_claims", "updated_at"),
    ("trading_playbook_settings", "updated_at"),
}

JSON_DEFAULTS = {
    "trading_mode_rules": {
        "prerequisites_json": dict,
        "candidate_filters_json": list,
        "entry_trigger_json": dict,
        "invalidation_json": dict,
        "exit_trigger_json": dict,
        "risk_guidance_json": dict,
        "source_refs_json": list,
    },
    "trading_plan_versions": {
        "market_state_json": dict,
        "theme_ranking_json": list,
        "mode_radar_json": list,
        "rule_snapshot_json": list,
        "risk_settings_json": dict,
        "data_quality_json": dict,
        "change_summary_json": dict,
    },
    "trading_plan_candidates": {
        "supporting_mode_keys_json": list,
        "recognition_json": dict,
        "entry_trigger_json": dict,
        "invalidation_json": dict,
        "exit_trigger_json": dict,
        "evidence_json": list,
        "manual_overrides_json": dict,
    },
    "trading_alert_events": {
        "market_snapshot_json": dict,
        "channel_status_json": dict,
    },
    "trading_execution_reviews": {
        "signal_review_json": dict,
        "manual_execution_json": dict,
        "plan_compliance_json": dict,
        "outcome_snapshot_json": dict,
        "data_quality_json": dict,
    },
    "trading_playbook_settings": {"channel_config_json": dict},
}

UNIQUE_CONSTRAINTS = {
    "trading_rule_sources": {
        ("uq_trading_rule_source_hash", ("source_key", "content_hash")),
    },
    "trading_mode_rules": {
        ("uq_trading_mode_rule_version", ("mode_key", "version")),
    },
    "trading_plan_versions": {
        (
            "uq_trading_plan_stage_version",
            ("target_trade_date", "stage", "version_no"),
        ),
    },
    "trading_plan_candidates": {
        (
            "uq_trading_plan_candidate",
            ("plan_version_id", "stock_code", "primary_mode_key"),
        ),
    },
    "trading_alert_events": {
        ("uq_trading_alert_dedup", ("dedup_key",)),
    },
    "trading_alert_condition_states": {
        (
            "uq_trading_alert_condition_version",
            ("candidate_id", "event_type", "condition_version"),
        ),
    },
    "trading_execution_reviews": {
        ("uq_trading_execution_review", ("trade_date", "plan_version_id")),
    },
    "trading_playbook_job_claims": {
        ("uq_trading_playbook_job_claim_key", ("job_key",)),
    },
    "trading_playbook_settings": set(),
}

FOREIGN_KEYS = {
    ("trading_plan_versions", "parent_plan_version_id"): "trading_plan_versions.id",
    ("trading_plan_candidates", "plan_version_id"): "trading_plan_versions.id",
    ("trading_alert_events", "plan_version_id"): "trading_plan_versions.id",
    ("trading_alert_events", "candidate_id"): "trading_plan_candidates.id",
    (
        "trading_alert_condition_states",
        "candidate_id",
    ): "trading_plan_candidates.id",
    ("trading_execution_reviews", "plan_version_id"): "trading_plan_versions.id",
}

INDEXED_COLUMNS = {
    "trading_mode_rules": {"mode_key"},
    "trading_plan_versions": {"source_trade_date", "target_trade_date"},
    "trading_plan_candidates": {
        "plan_version_id",
        "stock_code",
        "action_trade_date",
    },
    "trading_alert_events": {"plan_version_id", "candidate_id"},
    "trading_alert_condition_states": {"candidate_id"},
    "trading_execution_reviews": {"trade_date"},
}

COMPOSITE_INDEXES = {
    "trading_playbook_job_claims": {
        (
            "ix_trading_playbook_job_claim_status_lease",
            ("status", "lease_expires_at"),
        ),
    },
    "trading_alert_condition_states": {
        (
            "ix_trading_alert_condition_candidate_active",
            ("candidate_id", "event_type", "active"),
        ),
    },
}


class TradingPlaybookModelTests(unittest.TestCase):
    def test_tables_have_exact_column_sets(self):
        for table_name, expected_columns in TABLE_COLUMNS.items():
            with self.subTest(table=table_name):
                table = Base.metadata.tables[table_name]
                self.assertEqual(set(table.c.keys()), expected_columns)

    def test_column_types_and_string_lengths_match_contract(self):
        for table_name, expected_columns in TABLE_COLUMNS.items():
            with self.subTest(table=table_name, coverage="type map"):
                self.assertEqual(set(COLUMN_TYPES[table_name]), expected_columns)
            table = Base.metadata.tables[table_name]
            for column_name, (expected_type, expected_length) in COLUMN_TYPES[
                table_name
            ].items():
                with self.subTest(table=table_name, column=column_name):
                    column_type = table.c[column_name].type
                    self.assertIsInstance(column_type, expected_type)
                    if expected_length is not None:
                        self.assertEqual(column_type.length, expected_length)

    def test_nullability_and_defaults_match_contract(self):
        json_default_columns = {
            (table_name, column_name)
            for table_name, defaults in JSON_DEFAULTS.items()
            for column_name in defaults
        }
        callable_defaults = DATETIME_DEFAULTS | json_default_columns

        for table_name, expected_columns in TABLE_COLUMNS.items():
            table = Base.metadata.tables[table_name]
            for column_name in expected_columns:
                key = (table_name, column_name)
                column = table.c[column_name]
                with self.subTest(table=table_name, column=column_name):
                    self.assertEqual(column.nullable, key in NULLABLE_COLUMNS)
                    if key in SCALAR_DEFAULTS:
                        self.assertIsNotNone(column.default)
                        self.assertFalse(callable(column.default.arg))
                        expected_default = SCALAR_DEFAULTS[key]
                        if isinstance(expected_default, bool):
                            self.assertIs(column.default.arg, expected_default)
                        else:
                            self.assertEqual(column.default.arg, expected_default)
                    elif key in callable_defaults:
                        self.assertIsNotNone(column.default)
                        self.assertTrue(callable(column.default.arg))
                    else:
                        self.assertIsNone(column.default)

    def test_json_defaults_are_callable_with_the_correct_shape(self):
        for table_name, expected_defaults in JSON_DEFAULTS.items():
            table = Base.metadata.tables[table_name]
            actual_json_columns = {
                column.name for column in table.c if isinstance(column.type, JSON)
            }
            with self.subTest(table=table_name, coverage="JSON columns"):
                self.assertEqual(actual_json_columns, set(expected_defaults))
            for column_name, expected_factory in expected_defaults.items():
                with self.subTest(table=table_name, column=column_name):
                    default_callable = table.c[column_name].default.arg
                    self.assertTrue(callable(default_callable))
                    factory = getattr(
                        default_callable,
                        "__wrapped__",
                        default_callable,
                    )
                    self.assertIsInstance(factory(), expected_factory)

    def test_named_unique_constraints_match_contract(self):
        for table_name, expected_constraints in UNIQUE_CONSTRAINTS.items():
            table = Base.metadata.tables[table_name]
            actual_constraints = {
                (
                    constraint.name,
                    tuple(column.name for column in constraint.columns),
                )
                for constraint in table.constraints
                if isinstance(constraint, UniqueConstraint)
            }
            with self.subTest(table=table_name):
                self.assertEqual(actual_constraints, expected_constraints)

    def test_foreign_key_targets_match_contract(self):
        for (table_name, column_name), target in FOREIGN_KEYS.items():
            with self.subTest(table=table_name, column=column_name):
                foreign_keys = Base.metadata.tables[table_name].c[
                    column_name
                ].foreign_keys
                self.assertEqual(len(foreign_keys), 1)
                self.assertEqual(next(iter(foreign_keys)).target_fullname, target)

    def test_required_single_column_indexes_are_registered(self):
        for table_name, expected_columns in INDEXED_COLUMNS.items():
            table = Base.metadata.tables[table_name]
            indexed_columns = {
                tuple(column.name for column in index.columns)
                for index in table.indexes
            }
            for column_name in expected_columns:
                with self.subTest(table=table_name, column=column_name):
                    self.assertIn((column_name,), indexed_columns)

    def test_required_composite_indexes_are_registered(self):
        for table_name, expected_indexes in COMPOSITE_INDEXES.items():
            table = Base.metadata.tables[table_name]
            actual_indexes = {
                (index.name, tuple(column.name for column in index.columns))
                for index in table.indexes
            }
            with self.subTest(table=table_name):
                self.assertTrue(expected_indexes.issubset(actual_indexes))

    def test_primary_keys_and_autoincrement_match_contract(self):
        autoincrement_tables = set(TABLE_COLUMNS) - {"trading_playbook_settings"}
        for table_name in TABLE_COLUMNS:
            table = Base.metadata.tables[table_name]
            with self.subTest(table=table_name):
                self.assertEqual(tuple(table.primary_key.columns.keys()), ("id",))
                if table_name in autoincrement_tables:
                    self.assertIs(table.c.id.autoincrement, True)

    def test_settings_updated_at_has_callable_onupdate(self):
        updated_at = Base.metadata.tables["trading_playbook_settings"].c.updated_at
        self.assertIsNotNone(updated_at.onupdate)
        self.assertTrue(callable(updated_at.onupdate.arg))

    def test_job_claim_updated_at_has_callable_onupdate(self):
        updated_at = Base.metadata.tables[
            "trading_playbook_job_claims"
        ].c.updated_at
        self.assertIsNotNone(updated_at.onupdate)
        self.assertTrue(callable(updated_at.onupdate.arg))


class TradingPlaybookPersistenceTests(unittest.IsolatedAsyncioTestCase):
    async def test_create_all_persists_rows_with_independent_json_defaults(self):
        engine = create_async_engine("sqlite+aiosqlite:///:memory:", future=True)
        try:
            async with engine.begin() as connection:
                await connection.run_sync(Base.metadata.create_all)

            session_factory = async_sessionmaker(engine, expire_on_commit=False)
            async with session_factory() as session:
                first = TradingModeRule(
                    mode_key="first-board",
                    version=1,
                    name="First Board",
                    family="momentum",
                    style="breakout",
                    window="open",
                    automation_level="manual",
                    content_hash="a" * 64,
                )
                second = TradingModeRule(
                    mode_key="weak-to-strong",
                    version=1,
                    name="Weak to Strong",
                    family="momentum",
                    style="reversal",
                    window="morning",
                    automation_level="assisted",
                    content_hash="b" * 64,
                )
                session.add_all([first, second])
                await session.flush()

                for column_name, factory in JSON_DEFAULTS[
                    "trading_mode_rules"
                ].items():
                    with self.subTest(column=column_name):
                        first_value = getattr(first, column_name)
                        second_value = getattr(second, column_name)
                        self.assertIsInstance(first_value, factory)
                        self.assertIsInstance(second_value, factory)
                        self.assertIsNot(first_value, second_value)

                first.prerequisites_json = {"market_ready": True}
                first.candidate_filters_json = ["liquid"]
                await session.commit()
                await session.refresh(first)
                await session.refresh(second)

                self.assertIsNotNone(first.id)
                self.assertIsNotNone(second.id)
                self.assertEqual(first.prerequisites_json, {"market_ready": True})
                self.assertEqual(first.candidate_filters_json, ["liquid"])
                self.assertEqual(second.prerequisites_json, {})
                self.assertEqual(second.candidate_filters_json, [])
        finally:
            await engine.dispose()


if __name__ == "__main__":
    unittest.main()
