import inspect
import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch

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
    inspect as sqlalchemy_inspect,
)
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app import database
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
    "trading_execution_review_phase_snapshots": {
        "id",
        "review_id",
        "phase",
        "trade_date",
        "plan_version_id",
        "snapshot_json",
        "created_at",
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
    "trading_playbook_job_results": {
        "id",
        "job_key",
        "entity_type",
        "entity_id",
        "created_at",
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
    "trading_playbook_obsidian_exports": {
        "id",
        "snapshot_key",
        "snapshot_version",
        "trade_date",
        "entity_type",
        "entity_id",
        "phase",
        "target_path",
        "source_hash",
        "snapshot_json",
        "immutable",
        "status",
        "attempt_no",
        "next_attempt_at",
        "last_error",
        "git_status_json",
        "exported_at",
        "created_at",
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
    "trading_execution_review_phase_snapshots": {
        "id": (Integer, None),
        "review_id": (Integer, None),
        "phase": (String, 32),
        "trade_date": (Date, None),
        "plan_version_id": (Integer, None),
        "snapshot_json": (JSON, None),
        "created_at": (DateTime, None),
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
    "trading_playbook_job_results": {
        "id": (Integer, None),
        "job_key": (String, 255),
        "entity_type": (String, 32),
        "entity_id": (Integer, None),
        "created_at": (DateTime, None),
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
    "trading_playbook_obsidian_exports": {
        "id": (Integer, None),
        "snapshot_key": (String, 255),
        "snapshot_version": (Integer, None),
        "trade_date": (Date, None),
        "entity_type": (String, 32),
        "entity_id": (Integer, None),
        "phase": (String, 32),
        "target_path": (String, 1024),
        "source_hash": (String, 64),
        "snapshot_json": (JSON, None),
        "immutable": (Boolean, None),
        "status": (String, 32),
        "attempt_no": (Integer, None),
        "next_attempt_at": (DateTime, None),
        "last_error": (Text, None),
        "git_status_json": (JSON, None),
        "exported_at": (DateTime, None),
        "created_at": (DateTime, None),
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
    ("trading_playbook_obsidian_exports", "entity_id"),
    ("trading_playbook_obsidian_exports", "next_attempt_at"),
    ("trading_playbook_obsidian_exports", "last_error"),
    ("trading_playbook_obsidian_exports", "git_status_json"),
    ("trading_playbook_obsidian_exports", "exported_at"),
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
    ("trading_playbook_obsidian_exports", "immutable"): False,
    ("trading_playbook_obsidian_exports", "status"): "pending",
    ("trading_playbook_obsidian_exports", "attempt_no"): 0,
}

DATETIME_DEFAULTS = {
    ("trading_rule_sources", "ingested_at"),
    ("trading_mode_rules", "created_at"),
    ("trading_plan_versions", "generated_at"),
    ("trading_alert_events", "triggered_at"),
    ("trading_alert_condition_states", "updated_at"),
    ("trading_execution_reviews", "generated_at"),
    ("trading_execution_review_phase_snapshots", "created_at"),
    ("trading_playbook_job_claims", "created_at"),
    ("trading_playbook_job_claims", "updated_at"),
    ("trading_playbook_job_results", "created_at"),
    ("trading_playbook_settings", "updated_at"),
    ("trading_playbook_obsidian_exports", "created_at"),
    ("trading_playbook_obsidian_exports", "updated_at"),
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

JSON_COLUMNS_WITHOUT_DEFAULTS = {
    "trading_playbook_obsidian_exports": {"snapshot_json", "git_status_json"},
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
    "trading_playbook_obsidian_exports": {
        (
            "uq_trading_playbook_obsidian_snapshot_version",
            ("snapshot_key", "snapshot_version"),
        ),
    },
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
    "trading_playbook_obsidian_exports": {
        (
            "ix_trading_playbook_obsidian_due",
            ("status", "next_attempt_at"),
        ),
        (
            "ix_trading_playbook_obsidian_trade_date",
            ("trade_date", "phase"),
        ),
    },
}


class TradingPlaybookModelTests(unittest.TestCase):
    def _table(self, table_name):
        self.assertIn(table_name, Base.metadata.tables)
        return Base.metadata.tables[table_name]

    def test_tables_have_exact_column_sets(self):
        for table_name, expected_columns in TABLE_COLUMNS.items():
            with self.subTest(table=table_name):
                table = self._table(table_name)
                self.assertEqual(set(table.c.keys()), expected_columns)

    def test_column_types_and_string_lengths_match_contract(self):
        for table_name, expected_columns in TABLE_COLUMNS.items():
            with self.subTest(table=table_name, coverage="type map"):
                self.assertEqual(set(COLUMN_TYPES[table_name]), expected_columns)
            table = self._table(table_name)
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
            table = self._table(table_name)
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
        json_tables = set(JSON_DEFAULTS) | set(JSON_COLUMNS_WITHOUT_DEFAULTS)
        for table_name in json_tables:
            expected_defaults = JSON_DEFAULTS.get(table_name, {})
            expected_without_defaults = JSON_COLUMNS_WITHOUT_DEFAULTS.get(
                table_name,
                set(),
            )
            table = self._table(table_name)
            actual_json_columns = {
                column.name for column in table.c if isinstance(column.type, JSON)
            }
            with self.subTest(table=table_name, coverage="JSON columns"):
                self.assertEqual(
                    actual_json_columns,
                    set(expected_defaults) | expected_without_defaults,
                )
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

            for column_name in expected_without_defaults:
                with self.subTest(table=table_name, column=column_name):
                    self.assertIsNone(table.c[column_name].default)

    def test_named_unique_constraints_match_contract(self):
        for table_name, expected_constraints in UNIQUE_CONSTRAINTS.items():
            table = self._table(table_name)
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
            table = self._table(table_name)
            indexed_columns = {
                tuple(column.name for column in index.columns)
                for index in table.indexes
            }
            for column_name in expected_columns:
                with self.subTest(table=table_name, column=column_name):
                    self.assertIn((column_name,), indexed_columns)

    def test_required_composite_indexes_are_registered(self):
        for table_name, expected_indexes in COMPOSITE_INDEXES.items():
            table = self._table(table_name)
            actual_indexes = {
                (index.name, tuple(column.name for column in index.columns))
                for index in table.indexes
            }
            with self.subTest(table=table_name):
                self.assertTrue(expected_indexes.issubset(actual_indexes))

    def test_primary_keys_and_autoincrement_match_contract(self):
        autoincrement_tables = set(TABLE_COLUMNS) - {"trading_playbook_settings"}
        for table_name in TABLE_COLUMNS:
            table = self._table(table_name)
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

    def test_obsidian_export_updated_at_has_callable_onupdate(self):
        table_name = "trading_playbook_obsidian_exports"
        table = self._table(table_name)
        created_at = table.c.created_at
        updated_at = table.c.updated_at

        self.assertEqual(inspect.unwrap(created_at.default.arg), datetime.now)
        self.assertEqual(inspect.unwrap(updated_at.default.arg), datetime.now)
        self.assertIsNotNone(updated_at.onupdate)
        self.assertTrue(callable(updated_at.onupdate.arg))
        self.assertEqual(inspect.unwrap(updated_at.onupdate.arg), datetime.now)

        for column in table.c:
            if column.name != "updated_at":
                with self.subTest(column=column.name):
                    self.assertIsNone(column.onupdate)

    def test_obsidian_export_datetimes_are_naive(self):
        table = self._table("trading_playbook_obsidian_exports")
        datetime_columns = {
            "next_attempt_at",
            "exported_at",
            "created_at",
            "updated_at",
        }
        actual_datetime_columns = {
            column.name
            for column in table.c
            if isinstance(column.type, DateTime)
        }
        self.assertEqual(actual_datetime_columns, datetime_columns)
        for column_name in datetime_columns:
            with self.subTest(column=column_name):
                self.assertIs(table.c[column_name].type.timezone, False)

    def test_obsidian_export_indexes_match_exact_contract(self):
        table = self._table("trading_playbook_obsidian_exports")
        actual_indexes = {
            (
                index.name,
                tuple(column.name for column in index.columns),
                index.unique,
            )
            for index in table.indexes
            if len(index.columns) > 1
        }
        self.assertEqual(
            actual_indexes,
            {
                (
                    "ix_trading_playbook_obsidian_due",
                    ("status", "next_attempt_at"),
                    False,
                ),
                (
                    "ix_trading_playbook_obsidian_trade_date",
                    ("trade_date", "phase"),
                    False,
                ),
                (
                    "ix_trading_playbook_obsidian_fact_lookup",
                    ("immutable", "entity_type", "entity_id", "phase"),
                    False,
                ),
            },
        )

    def test_obsidian_export_model_is_exported(self):
        self.assertTrue(
            hasattr(app.models, "TradingPlaybookObsidianExport"),
            "TradingPlaybookObsidianExport must be exported from app.models",
        )
        self.assertIn("TradingPlaybookObsidianExport", app.models.__all__)


class TradingPlaybookPersistenceTests(unittest.IsolatedAsyncioTestCase):
    async def test_sqlite_compat_adds_obsidian_fact_lookup_index(self):
        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        try:
            async with engine.begin() as connection:
                await connection.run_sync(Base.metadata.create_all)
                await connection.exec_driver_sql(
                    "DROP INDEX IF EXISTS "
                    "ix_trading_playbook_obsidian_fact_lookup"
                )
                await connection.run_sync(database.ensure_sqlite_schema_compat)
                indexes = (
                    await connection.exec_driver_sql(
                        "PRAGMA index_list(trading_playbook_obsidian_exports)"
                    )
                ).all()
                names = {row[1] for row in indexes}
                self.assertIn(
                    "ix_trading_playbook_obsidian_fact_lookup",
                    names,
                )
                columns = (
                    await connection.exec_driver_sql(
                        "PRAGMA index_info("
                        "ix_trading_playbook_obsidian_fact_lookup)"
                    )
                ).all()
                self.assertEqual(
                    tuple(row[2] for row in columns),
                    ("immutable", "entity_type", "entity_id", "phase"),
                )
        finally:
            await engine.dispose()

    async def test_new_result_and_review_phase_tables_are_exported_and_created(self):
        self.assertIn("TradingPlaybookJobResult", app.models.__all__)
        self.assertIn(
            "TradingExecutionReviewPhaseSnapshot",
            app.models.__all__,
        )
        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        try:
            async with engine.begin() as connection:
                await connection.run_sync(Base.metadata.create_all)
                table_names = set(
                    await connection.run_sync(
                        lambda sync_connection: sqlalchemy_inspect(
                            sync_connection
                        ).get_table_names()
                    )
                )
            self.assertIn("trading_playbook_job_results", table_names)
            self.assertIn(
                "trading_execution_review_phase_snapshots",
                table_names,
            )
        finally:
            await engine.dispose()

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

    async def test_obsidian_export_persists_snapshot_round_trip(self):
        self.assertTrue(
            hasattr(app.models, "TradingPlaybookObsidianExport"),
            "TradingPlaybookObsidianExport must exist before it can persist",
        )
        export_model = app.models.TradingPlaybookObsidianExport
        engine = create_async_engine("sqlite+aiosqlite:///:memory:", future=True)
        try:
            async with engine.begin() as connection:
                await connection.run_sync(Base.metadata.create_all)

            session_factory = async_sessionmaker(engine, expire_on_commit=False)
            async with session_factory() as session:
                snapshot = export_model(
                    snapshot_key="2026-07-15/plan/42",
                    snapshot_version=3,
                    trade_date=date(2026, 7, 15),
                    entity_type="plan",
                    entity_id=42,
                    phase="preclose",
                    target_path=(
                        "TradingPlaybook Auto/2026-07-15/preclose/plan-42.md"
                    ),
                    source_hash="c" * 64,
                    snapshot_json={"candidate_ids": [7, 11]},
                    immutable=True,
                    status="written",
                    attempt_no=2,
                    next_attempt_at=datetime(2026, 7, 15, 8, 45),
                    last_error="previous attempt failed",
                    git_status_json={"commit": "abc123"},
                    exported_at=datetime(2026, 7, 15, 9, 0),
                )
                session.add(snapshot)
                await session.commit()
                await session.refresh(snapshot)

                self.assertIsNotNone(snapshot.id)
                self.assertEqual(snapshot.snapshot_version, 3)
                self.assertEqual(snapshot.trade_date, date(2026, 7, 15))
                self.assertEqual(snapshot.phase, "preclose")
                self.assertEqual(snapshot.status, "written")
                self.assertEqual(
                    snapshot.target_path,
                    "TradingPlaybook Auto/2026-07-15/preclose/plan-42.md",
                )
                self.assertEqual(snapshot.snapshot_json, {"candidate_ids": [7, 11]})
                self.assertEqual(snapshot.git_status_json, {"commit": "abc123"})
                self.assertEqual(snapshot.next_attempt_at, datetime(2026, 7, 15, 8, 45))
                self.assertEqual(snapshot.exported_at, datetime(2026, 7, 15, 9, 0))
                self.assertIsNotNone(snapshot.created_at)
                self.assertIsNotNone(snapshot.updated_at)
        finally:
            await engine.dispose()

    async def test_init_db_adds_obsidian_export_table_to_existing_schema(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            database_path = Path(temporary_directory) / "existing-playbook.db"
            database_url = (
                f"sqlite+aiosqlite:///{database_path.as_posix()}"
            )
            engine = create_async_engine(database_url, future=True)
            session_factory = async_sessionmaker(engine, expire_on_commit=False)
            try:
                async with engine.begin() as connection:
                    await connection.run_sync(
                        TradingPlaybookSettings.__table__.create
                    )

                async with session_factory() as session:
                    session.add(
                        TradingPlaybookSettings(
                            id=1,
                            enabled=True,
                            trial_position_pct=12,
                            confirmed_position_pct=28,
                            hard_stop_pct=4.5,
                            max_action_candidates=2,
                            in_app_enabled=True,
                            wechat_enabled=False,
                            channel_config_json={"priority": "high"},
                        )
                    )
                    await session.commit()

                async with engine.begin() as connection:
                    export_table_exists = await connection.run_sync(
                        lambda sync_connection: sqlalchemy_inspect(
                            sync_connection
                        ).has_table("trading_playbook_obsidian_exports")
                    )
                    self.assertFalse(export_table_exists)

                with patch.object(database, "engine", engine), patch.object(
                    database.settings,
                    "DATABASE_URL",
                    database_url,
                ):
                    await database.init_db()

                async with engine.begin() as connection:
                    export_table_exists = await connection.run_sync(
                        lambda sync_connection: sqlalchemy_inspect(
                            sync_connection
                        ).has_table("trading_playbook_obsidian_exports")
                    )
                    self.assertTrue(export_table_exists)

                async with session_factory() as session:
                    settings_row = await session.get(TradingPlaybookSettings, 1)
                    self.assertIsNotNone(settings_row)
                    self.assertEqual(settings_row.id, 1)
                    self.assertTrue(settings_row.enabled)
                    self.assertEqual(settings_row.trial_position_pct, 12)
                    self.assertEqual(settings_row.confirmed_position_pct, 28)
                    self.assertEqual(settings_row.hard_stop_pct, 4.5)
                    self.assertEqual(settings_row.max_action_candidates, 2)
                    self.assertTrue(settings_row.in_app_enabled)
                    self.assertFalse(settings_row.wechat_enabled)
                    self.assertEqual(
                        settings_row.channel_config_json,
                        {"priority": "high"},
                    )
            finally:
                await engine.dispose()


if __name__ == "__main__":
    unittest.main()
