from __future__ import annotations

import hashlib
import json
import re
import unittest
from collections import OrderedDict
from dataclasses import FrozenInstanceError
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, localcontext

from app.services.trading_playbook.obsidian_types import (
    OBSIDIAN_ENTITY_TYPES,
    OBSIDIAN_EXPORT_STATUSES,
    OBSIDIAN_PHASES,
    TRADING_PLAYBOOK_ALLOWED_ROOTS,
    ObsidianArtifact,
    ObsidianSyncBatchResult,
    canonical_json_bytes,
    database_datetime_to_cn,
)


class CanonicalJsonBytesTests(unittest.TestCase):
    def test_dict_insertion_order_does_not_change_bytes_or_hash(self) -> None:
        first = {"z": 1, "nested": {"b": 2, "a": 3}, "a": "value"}
        second = OrderedDict(
            (("a", "value"), ("nested", OrderedDict((("a", 3), ("b", 2)))), ("z", 1))
        )

        first_bytes = canonical_json_bytes(first)
        second_bytes = canonical_json_bytes(second)

        self.assertEqual(first_bytes, second_bytes)
        self.assertEqual(
            hashlib.sha256(first_bytes).hexdigest(),
            hashlib.sha256(second_bytes).hexdigest(),
        )
        self.assertEqual(
            first_bytes,
            b'{"a":"value","nested":{"a":3,"b":2},"z":1}',
        )

    def test_allowed_scalars_and_containers_are_normalized_recursively(self) -> None:
        value = {
            "none": None,
            "bool": True,
            "int": 7,
            "float": 1.25,
            "text": "中文",
            "date": date(2026, 7, 15),
            "list": [1, (False, Decimal("2.500"))],
        }

        encoded = canonical_json_bytes(value)

        self.assertEqual(
            json.loads(encoded.decode("utf-8")),
            {
                "bool": True,
                "date": "2026-07-15",
                "float": 1.25,
                "int": 7,
                "list": [1, [False, "2.5"]],
                "none": None,
                "text": "中文",
            },
        )
        self.assertIn("中文".encode("utf-8"), encoded)
        self.assertNotIn(b" ", encoded)

    def test_aware_datetimes_are_converted_to_utc_with_z_suffix(self) -> None:
        east_eight = datetime(
            2026,
            7,
            15,
            9,
            30,
            45,
            123456,
            tzinfo=timezone(timedelta(hours=8)),
        )
        west_three_thirty = datetime(
            2026,
            7,
            14,
            20,
            0,
            tzinfo=timezone(-timedelta(hours=3, minutes=30)),
        )

        self.assertEqual(
            canonical_json_bytes([east_eight, west_three_thirty]),
            b'["2026-07-15T01:30:45.123456Z","2026-07-14T23:30:00Z"]',
        )

    def test_decimal_normalization_is_non_scientific_and_equivalent(self) -> None:
        equivalent_values = (
            Decimal("1.2300"),
            Decimal("1.23"),
            Decimal("123e-2"),
        )

        self.assertEqual(
            {canonical_json_bytes(value) for value in equivalent_values},
            {b'"1.23"'},
        )
        self.assertEqual(canonical_json_bytes(Decimal("1E+4")), b'"10000"')
        self.assertEqual(canonical_json_bytes(Decimal("1E-7")), b'"0.0000001"')
        self.assertEqual(canonical_json_bytes(Decimal("0.000")), b'"0"')
        self.assertEqual(canonical_json_bytes(Decimal("-0.000")), b'"0"')

    def test_decimal_normalization_does_not_round_under_a_small_context(self) -> None:
        with localcontext() as context:
            context.prec = 3

            encoded = canonical_json_bytes(Decimal("123456789.1234500"))

        self.assertEqual(encoded, b'"123456789.12345"')

    def test_nonfinite_numbers_are_rejected(self) -> None:
        for value in (
            float("nan"),
            float("inf"),
            float("-inf"),
            Decimal("NaN"),
            Decimal("Infinity"),
            Decimal("-Infinity"),
        ):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "finite"):
                    canonical_json_bytes(value)

    def test_naive_datetime_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "timezone-aware"):
            canonical_json_bytes(datetime(2026, 7, 15, 9, 30))

    def test_unsupported_values_are_rejected_with_clear_type_errors(self) -> None:
        rejected = (set(), frozenset(), b"bytes", bytearray(b"bytes"), object())
        for value in rejected:
            with self.subTest(value_type=type(value).__name__):
                with self.assertRaisesRegex(TypeError, "unsupported canonical JSON type"):
                    canonical_json_bytes(value)

    def test_non_string_dict_keys_are_rejected(self) -> None:
        with self.assertRaisesRegex(TypeError, "dict keys must be strings"):
            canonical_json_bytes({1: "value"})

    def test_repeated_calls_return_identical_bytes(self) -> None:
        value = {
            "captured_at": datetime(2026, 7, 15, 1, 30, tzinfo=timezone.utc),
            "amount": Decimal("10.5000"),
        }

        results = [canonical_json_bytes(value) for _ in range(5)]

        self.assertEqual(len(set(results)), 1)


class DatabaseDatetimeToCnTests(unittest.TestCase):
    def test_none_remains_none(self) -> None:
        self.assertIsNone(database_datetime_to_cn(None))

    def test_naive_database_datetime_is_interpreted_as_china_wall_time(self) -> None:
        source = datetime(2026, 1, 15, 9, 30, 12, 345678)

        converted = database_datetime_to_cn(source)

        self.assertIsNotNone(converted)
        assert converted is not None
        self.assertEqual(converted.replace(tzinfo=None), source)
        self.assertEqual(converted.utcoffset(), timedelta(hours=8))
        self.assertEqual(getattr(converted.tzinfo, "zone", None), "Asia/Shanghai")

    def test_aware_database_datetime_is_converted_to_china_time(self) -> None:
        source = datetime(2026, 7, 15, 1, 30, tzinfo=timezone.utc)

        converted = database_datetime_to_cn(source)

        self.assertIsNotNone(converted)
        assert converted is not None
        self.assertEqual(converted.isoformat(), "2026-07-15T09:30:00+08:00")
        self.assertEqual(getattr(converted.tzinfo, "zone", None), "Asia/Shanghai")


class ObsidianContractTests(unittest.TestCase):
    def test_constants_are_exact_and_notes_is_not_an_allowed_root(self) -> None:
        self.assertEqual(
            OBSIDIAN_EXPORT_STATUSES,
            ("pending", "written", "paused", "failed", "superseded"),
        )
        self.assertEqual(
            OBSIDIAN_ENTITY_TYPES,
            ("rule", "plan", "review", "alerts", "daily_index", "dashboard"),
        )
        self.assertEqual(
            OBSIDIAN_PHASES,
            (
                "catalog",
                "preclose",
                "initial_review",
                "after_close",
                "final_review",
                "overnight",
                "auction",
                "reconcile",
            ),
        )
        self.assertEqual(
            TRADING_PLAYBOOK_ALLOWED_ROOTS,
            (
                "30_TradingPlaybook/Modes/Auto",
                "30_TradingPlaybook/Daily/Auto",
                "30_TradingPlaybook/Reviews/Auto",
                "30_TradingPlaybook/Alerts/Auto",
                "Dashboards/交易预案.md",
            ),
        )
        self.assertTrue(all("Notes" not in root for root in TRADING_PLAYBOOK_ALLOWED_ROOTS))

    def test_artifact_is_frozen_and_source_hash_matches_canonical_sha256(self) -> None:
        payload = {
            "trade_date": date(2026, 7, 15),
            "captured_at": datetime(2026, 7, 15, 1, 30, tzinfo=timezone.utc),
            "levels": (Decimal("10.500"), 11),
        }
        artifact = ObsidianArtifact(
            snapshot_key="plan:42:2026-07-15",
            trade_date=date(2026, 7, 15),
            entity_type="plan",
            entity_id=42,
            phase="preclose",
            target_path="30_TradingPlaybook/Daily/Auto/2026-07-15.md",
            immutable=True,
            payload=payload,
        )
        expected = hashlib.sha256(canonical_json_bytes(payload)).hexdigest()

        self.assertEqual(artifact.source_hash, expected)
        self.assertEqual([artifact.source_hash for _ in range(5)], [expected] * 5)
        self.assertRegex(artifact.source_hash, re.compile(r"^[0-9a-f]{64}$"))
        with self.assertRaises(FrozenInstanceError):
            artifact.phase = "after_close"  # type: ignore[misc]

    def test_artifact_validates_identity_phase_path_and_payload(self) -> None:
        base = {
            "snapshot_key": "rule:1",
            "trade_date": date(2026, 7, 15),
            "entity_type": "rule",
            "entity_id": 1,
            "phase": "catalog",
            "target_path": "30_TradingPlaybook/Modes/Auto/rule-1.md",
            "immutable": True,
            "payload": {"rule_id": 1},
        }
        invalid_cases = (
            ("snapshot_key", " ", "snapshot_key"),
            ("target_path", "", "target_path"),
            ("entity_type", "mode", "entity_type"),
            ("phase", "morning", "phase"),
        )
        for field_name, value, message in invalid_cases:
            with self.subTest(field_name=field_name):
                kwargs = dict(base)
                kwargs[field_name] = value
                with self.assertRaisesRegex(ValueError, message):
                    ObsidianArtifact(**kwargs)  # type: ignore[arg-type]

        invalid_payload = dict(base)
        invalid_payload["payload"] = {"bad": datetime(2026, 7, 15, 9, 30)}
        with self.assertRaisesRegex(ValueError, "timezone-aware"):
            ObsidianArtifact(**invalid_payload)  # type: ignore[arg-type]

    def test_sync_batch_result_is_frozen_and_validates_phase(self) -> None:
        result = ObsidianSyncBatchResult(
            trade_date=date(2026, 7, 15),
            phase="reconcile",
            written_files=("written.md",),
            skipped_files=("skipped.md",),
            pending_files=("pending.md",),
            failed_files=("failed.md",),
            git_status={"branch": "main", "clean": True, "ahead": 0},
        )

        self.assertEqual(result.phase, "reconcile")
        self.assertEqual(result.git_status["clean"], True)
        with self.assertRaises(FrozenInstanceError):
            result.phase = "catalog"  # type: ignore[misc]

        with self.assertRaisesRegex(ValueError, "phase"):
            ObsidianSyncBatchResult(
                trade_date=date(2026, 7, 15),
                phase="morning",
                written_files=(),
                skipped_files=(),
                pending_files=(),
                failed_files=(),
                git_status={},
            )


if __name__ == "__main__":
    unittest.main()
