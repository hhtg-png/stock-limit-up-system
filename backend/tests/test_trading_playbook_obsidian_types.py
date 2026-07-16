from __future__ import annotations

import hashlib
import json
import re
import unittest
from collections import UserDict
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import FrozenInstanceError, asdict, replace
from datetime import date, datetime, timedelta, timezone, tzinfo
from decimal import Decimal, localcontext
from types import MappingProxyType
from typing import get_origin, get_type_hints

from app.services.trading_playbook.obsidian_types import (
    OBSIDIAN_ENTITY_TYPES,
    OBSIDIAN_EXPORT_STATUSES,
    OBSIDIAN_PHASES,
    TRADING_PLAYBOOK_ALLOWED_ROOTS,
    ObsidianArtifact,
    ObsidianSyncBatchResult,
    canonical_json_bytes,
    contains_absolute_path_fragment,
    database_datetime_to_cn,
)
from app.utils.time_utils import CN_TZ


MAX_TEST_NESTING_DEPTH = 64


def _nested_dict(depth: int) -> dict[str, object]:
    value: object = 0
    for _ in range(depth):
        value = {"child": value}
    assert isinstance(value, dict)
    return value


class AbsolutePathFragmentTests(unittest.TestCase):
    def test_detects_delimited_windows_unc_and_posix_absolute_paths(self) -> None:
        fragments = (
            r"failed opening 'C:\private\vault\file.md'",
            r"failed=(D:/private/vault/file.md)",
            r"failed opening '\\server\private\file.md'",
            r"failed opening '\Users\Admin\secret.md'",
            "//server/share/file.md",
            "failed='//server/share/file.md'",
            "failed(//server/share/file.md)",
            "failed=//server/share/file.md",
            "failed,//server/share/file.md",
            "file:///srv/private/file.md",
            "failed=(file:///C:/Users/Admin/secret.md)",
            "failed='file://server/share/file.md'",
            "failed opening '/srv/private/vault/file.md'",
            "failed(/home/admin/private/file.md)",
            "failed=/opt/private/file.md",
            "failed：/var/private/file.md",
        )

        for fragment in fragments:
            with self.subTest(fragment=fragment):
                self.assertTrue(contains_absolute_path_fragment(fragment))

    def test_does_not_misclassify_relative_obsidian_paths(self) -> None:
        relative_paths = (
            "30_TradingPlaybook/foo/bar.md",
            "failed: 30_TradingPlaybook/Daily/Auto/2026/index.md",
            "Dashboards/交易预案.md",
            "./relative/file.md",
            "../relative/file.md",
            "relative/file.md",
            r".\relative\file.md",
            r"..\relative\file.md",
            r"relative\file.md",
            "2026/07/16",
            "https://example.com/private/file.md",
            "http://example.com/private/file.md",
            "ftp://example.com/private/file.md",
        )

        for relative_path in relative_paths:
            with self.subTest(relative_path=relative_path):
                self.assertFalse(
                    contains_absolute_path_fragment(relative_path)
                )


class CanonicalJsonBytesTests(unittest.TestCase):
    def test_dict_insertion_order_does_not_change_bytes_or_hash(self) -> None:
        first = {"z": 1, "nested": {"b": 2, "a": 3}, "a": "value"}
        second = {"a": "value", "nested": {"a": 3, "b": 2}, "z": 1}

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

    def test_negative_float_zero_has_the_same_canonical_bytes_and_hash_as_zero(self) -> None:
        negative = canonical_json_bytes({"value": -0.0})
        positive = canonical_json_bytes({"value": 0.0})

        self.assertEqual(negative, positive)
        self.assertEqual(
            hashlib.sha256(negative).hexdigest(),
            hashlib.sha256(positive).hexdigest(),
        )

    def test_cycles_are_rejected_with_clear_value_errors(self) -> None:
        dict_cycle: dict[str, object] = {}
        dict_cycle["self"] = dict_cycle
        list_cycle: list[object] = []
        list_cycle.append(list_cycle)

        for value in (dict_cycle, list_cycle):
            with self.subTest(value_type=type(value).__name__):
                with self.assertRaisesRegex(ValueError, "cycle detected"):
                    canonical_json_bytes(value)  # type: ignore[arg-type]

    def test_maximum_nesting_depth_has_an_explicit_boundary(self) -> None:
        canonical_json_bytes(_nested_dict(MAX_TEST_NESTING_DEPTH))

        with self.assertRaisesRegex(
            ValueError,
            f"maximum nesting depth {MAX_TEST_NESTING_DEPTH} exceeded",
        ):
            canonical_json_bytes(_nested_dict(MAX_TEST_NESTING_DEPTH + 1))

    def test_repeated_shared_references_are_not_treated_as_cycles(self) -> None:
        shared = {"values": [1, 2]}
        value = {"left": shared, "right": shared}

        self.assertEqual(
            json.loads(canonical_json_bytes(value)),
            {"left": {"values": [1, 2]}, "right": {"values": [1, 2]}},
        )

    def test_naive_datetime_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "timezone-aware"):
            canonical_json_bytes(datetime(2026, 7, 15, 9, 30))

    def test_unsupported_values_are_rejected_with_clear_type_errors(self) -> None:
        rejected = (set(), frozenset(), b"bytes", bytearray(b"bytes"), object())
        for value in rejected:
            with self.subTest(value_type=type(value).__name__):
                with self.assertRaisesRegex(TypeError, "unsupported canonical JSON type"):
                    canonical_json_bytes(value)

    def test_arbitrary_mappings_and_container_subclasses_are_rejected(self) -> None:
        class DictSubclass(dict[str, object]):
            pass

        class ListSubclass(list[object]):
            pass

        class TupleSubclass(tuple[object, ...]):
            pass

        rejected = (
            UserDict({"value": 1}),
            MappingProxyType({"value": 1}),
            DictSubclass(value=1),
            ListSubclass([1]),
            TupleSubclass((1,)),
        )
        for value in rejected:
            with self.subTest(value_type=type(value).__name__):
                with self.assertRaisesRegex(TypeError, "unsupported canonical JSON type"):
                    canonical_json_bytes(value)  # type: ignore[arg-type]
                with self.assertRaisesRegex(TypeError, "unsupported canonical JSON type"):
                    canonical_json_bytes({"nested": value})  # type: ignore[dict-item]

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

    def test_mutable_scalar_subclasses_cannot_change_canonical_bytes(self) -> None:
        class MutableDate(date):
            rendered = "2026-07-15"

            def isoformat(self) -> str:
                return type(self).rendered

        class MutableDecimal(Decimal):
            rendered = "10.5"

            def __format__(self, format_spec: str) -> str:
                return type(self).rendered

            def as_tuple(self):  # type: ignore[no-untyped-def]
                return Decimal("999").as_tuple()

        class MutableDatetime(datetime):
            shift = timedelta(0)

            def astimezone(self, tz=None):  # type: ignore[no-untyped-def]
                return datetime.astimezone(self, tz) + type(self).shift

        payload = {
            "trade_date": MutableDate(2026, 7, 15),
            "amount": MutableDecimal("10.500"),
            "captured_at": MutableDatetime(
                2026,
                7,
                15,
                9,
                30,
                tzinfo=timezone(timedelta(hours=8)),
            ),
        }
        expected = (
            b'{"amount":"10.5","captured_at":"2026-07-15T01:30:00Z",'
            b'"trade_date":"2026-07-15"}'
        )
        first = canonical_json_bytes(payload)
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

        MutableDate.rendered = "2099-01-01"
        MutableDecimal.rendered = "999"
        MutableDatetime.shift = timedelta(hours=5)
        second = canonical_json_bytes(payload)

        self.assertEqual(first, expected)
        self.assertEqual(second, first)
        self.assertEqual(canonical_json_bytes(artifact.payload), second)  # type: ignore[arg-type]
        self.assertEqual(
            artifact.source_hash,
            hashlib.sha256(second).hexdigest(),
        )


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

    def test_direct_pytz_tzinfo_assignment_is_normalized_via_utc(self) -> None:
        source = datetime(2026, 7, 15, 9, 30, tzinfo=CN_TZ)
        self.assertEqual(source.utcoffset(), timedelta(hours=8, minutes=6))

        converted = database_datetime_to_cn(source)

        self.assertIsNotNone(converted)
        assert converted is not None
        self.assertEqual(converted.utcoffset(), timedelta(hours=8))
        self.assertEqual(converted.astimezone(timezone.utc), source.astimezone(timezone.utc))
        self.assertEqual(converted.replace(tzinfo=None), datetime(2026, 7, 15, 9, 24))


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

    def test_artifact_deeply_owns_payload_and_precomputes_a_stable_hash(self) -> None:
        payload = {
            "trade_date": date(2026, 7, 15),
            "captured_at": datetime(2026, 7, 15, 1, 30, tzinfo=timezone.utc),
            "nested": {"levels": [Decimal("10.500"), 11]},
        }
        expected_bytes = canonical_json_bytes(payload)
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
        expected_hash = artifact.source_hash

        payload["nested"]["levels"].append(12)  # type: ignore[index,union-attr]
        payload["new"] = "caller mutation"

        self.assertEqual(artifact.source_hash, expected_hash)
        self.assertEqual(canonical_json_bytes(artifact.payload), expected_bytes)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            artifact.payload["new"] = "direct mutation"  # type: ignore[index]
        frozen_nested = artifact.payload["nested"]
        with self.assertRaises(TypeError):
            frozen_nested["new"] = 1  # type: ignore[index]
        frozen_levels = frozen_nested["levels"]  # type: ignore[index]
        with self.assertRaises(TypeError):
            frozen_levels[0] = 99  # type: ignore[index]

        plain = artifact.payload_json()
        self.assertEqual(canonical_json_bytes(plain), expected_bytes)
        self.assertEqual(
            plain,
            {
                "trade_date": "2026-07-15",
                "captured_at": "2026-07-15T01:30:00Z",
                "nested": {"levels": ["10.5", 11]},
            },
        )
        plain["nested"]["levels"].append("copy mutation")  # type: ignore[index,union-attr]
        self.assertEqual(artifact.source_hash, expected_hash)
        self.assertEqual(canonical_json_bytes(artifact.payload), expected_bytes)  # type: ignore[arg-type]

    def test_artifact_detaches_aware_datetimes_from_mutable_tzinfo(self) -> None:
        class MutableOffset(tzinfo):
            def __init__(self, offset: timedelta) -> None:
                self.offset = offset

            def utcoffset(self, value: datetime | None) -> timedelta:
                return self.offset

            def dst(self, value: datetime | None) -> timedelta:
                return timedelta(0)

            def tzname(self, value: datetime | None) -> str:
                return "mutable"

        mutable_tz = MutableOffset(timedelta(hours=8))
        payload = {
            "captured_at": datetime(2026, 7, 15, 9, 30, tzinfo=mutable_tz),
        }
        expected_bytes = canonical_json_bytes(payload)
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
        expected_hash = hashlib.sha256(expected_bytes).hexdigest()

        mutable_tz.offset = timedelta(hours=9)

        self.assertEqual(canonical_json_bytes(artifact.payload), expected_bytes)  # type: ignore[arg-type]
        self.assertEqual(artifact.source_hash, expected_hash)
        self.assertEqual(
            artifact.source_hash,
            hashlib.sha256(canonical_json_bytes(artifact.payload)).hexdigest(),  # type: ignore[arg-type]
        )

    def test_artifact_detaches_mutable_date_and_decimal_subclasses(self) -> None:
        class MutableDate(date):
            rendered = "2026-07-15"

            def isoformat(self) -> str:
                return type(self).rendered

        class MutableDecimal(Decimal):
            rendered = "10.5"

            def __format__(self, format_spec: str) -> str:
                return type(self).rendered

        mutable_date = MutableDate(2026, 7, 15)
        mutable_decimal = MutableDecimal("10.500")
        payload = {
            "trade_date": mutable_date,
            "amount": mutable_decimal,
        }
        expected_bytes = canonical_json_bytes(payload)
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
        expected_hash = hashlib.sha256(expected_bytes).hexdigest()

        MutableDate.rendered = "2099-01-01"
        MutableDecimal.rendered = "999"

        self.assertEqual(canonical_json_bytes(artifact.payload), expected_bytes)  # type: ignore[arg-type]
        self.assertIs(type(artifact.payload["trade_date"]), date)
        self.assertIs(type(artifact.payload["amount"]), Decimal)
        self.assertEqual(artifact.source_hash, expected_hash)

    def test_artifact_asdict_exports_a_fresh_plain_canonical_payload(self) -> None:
        artifact = ObsidianArtifact(
            snapshot_key="plan:42:2026-07-15",
            trade_date=date(2026, 7, 15),
            entity_type="plan",
            entity_id=42,
            phase="preclose",
            target_path="30_TradingPlaybook/Daily/Auto/2026-07-15.md",
            immutable=True,
            payload={
                "trade_date": date(2026, 7, 15),
                "levels": [Decimal("10.500"), 11],
            },
        )

        exported = asdict(artifact)

        self.assertIs(type(exported["payload"]), dict)
        self.assertEqual(exported["payload"], artifact.payload_json())
        exported["payload"]["levels"].append(12)  # type: ignore[index,union-attr]
        self.assertEqual(artifact.payload_json()["levels"], ["10.5", 11])

    def test_artifact_deepcopy_preserves_the_deeply_frozen_snapshot(self) -> None:
        artifact = ObsidianArtifact(
            snapshot_key="plan:42:2026-07-15",
            trade_date=date(2026, 7, 15),
            entity_type="plan",
            entity_id=42,
            phase="preclose",
            target_path="30_TradingPlaybook/Daily/Auto/2026-07-15.md",
            immutable=True,
            payload={"nested": {"levels": [Decimal("10.500"), 11]}},
        )

        copied = deepcopy(artifact)

        self.assertIs(copied, artifact)
        self.assertEqual(copied.source_hash, artifact.source_hash)
        with self.assertRaises(TypeError):
            copied.payload["new"] = "mutation"  # type: ignore[index]

    def test_artifact_replace_reconstructs_from_the_frozen_payload(self) -> None:
        artifact = ObsidianArtifact(
            snapshot_key="plan:42:2026-07-15",
            trade_date=date(2026, 7, 15),
            entity_type="plan",
            entity_id=42,
            phase="preclose",
            target_path="30_TradingPlaybook/Daily/Auto/2026-07-15.md",
            immutable=True,
            payload={"nested": {"levels": [Decimal("10.500"), 11]}},
        )

        replaced = replace(artifact, phase="after_close")

        self.assertEqual(replaced.phase, "after_close")
        self.assertEqual(replaced.payload_json(), artifact.payload_json())
        self.assertEqual(replaced.source_hash, artifact.source_hash)
        with self.assertRaises(TypeError):
            replaced.payload["new"] = "mutation"  # type: ignore[index]

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

    def test_artifact_payload_must_be_a_real_dict(self) -> None:
        base = {
            "snapshot_key": "rule:1",
            "trade_date": date(2026, 7, 15),
            "entity_type": "rule",
            "entity_id": 1,
            "phase": "catalog",
            "target_path": "30_TradingPlaybook/Modes/Auto/rule-1.md",
            "immutable": True,
        }
        invalid_payloads = (
            7,
            [],
            UserDict({"rule_id": 1}),
            MappingProxyType({"rule_id": 1}),
        )

        for payload in invalid_payloads:
            with self.subTest(payload_type=type(payload).__name__):
                with self.assertRaisesRegex(TypeError, "payload must be a dict"):
                    ObsidianArtifact(**base, payload=payload)  # type: ignore[arg-type]

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

    def test_sync_batch_file_fields_require_exact_tuples_of_strings(self) -> None:
        base = {
            "trade_date": date(2026, 7, 15),
            "phase": "reconcile",
            "written_files": (),
            "skipped_files": (),
            "pending_files": (),
            "failed_files": (),
            "git_status": {},
        }
        for field_name in (
            "written_files",
            "skipped_files",
            "pending_files",
            "failed_files",
        ):
            for invalid_value in (["mutable.md"], ("valid.md", 7)):
                with self.subTest(field_name=field_name, value=invalid_value):
                    kwargs = dict(base)
                    kwargs[field_name] = invalid_value
                    with self.assertRaisesRegex(
                        TypeError,
                        f"{field_name} must be a tuple of strings",
                    ):
                        ObsidianSyncBatchResult(**kwargs)  # type: ignore[arg-type]

    def test_sync_batch_result_rejects_non_json_safe_git_status_values(self) -> None:
        class DictSubclass(dict[str, object]):
            pass

        class ListSubclass(list[object]):
            pass

        invalid_statuses = (
            {"bad": object()},
            {"nested": {"bad": object()}},
            {"nested": date(2026, 7, 15)},
            {"nested": datetime(2026, 7, 15, 1, 30, tzinfo=timezone.utc)},
            {"nested": Decimal("1.25")},
            {"nested": (1, 2)},
            {"nested": [1, {"bad"}]},
            {"nested": {1: "non-string key"}},
            {"nested": DictSubclass(value=1)},
            {"nested": ListSubclass([1])},
            {"bad": b"bytes"},
            {"bad": float("nan")},
            {"bad": float("inf")},
            {"bad": float("-inf")},
        )

        for git_status in invalid_statuses:
            with self.subTest(git_status=git_status):
                with self.assertRaises((TypeError, ValueError)):
                    ObsidianSyncBatchResult(
                        trade_date=date(2026, 7, 15),
                        phase="reconcile",
                        written_files=(),
                        skipped_files=(),
                        pending_files=(),
                        failed_files=(),
                        git_status=git_status,  # type: ignore[arg-type]
                    )

    def test_sync_batch_result_retains_only_strict_json_values(self) -> None:
        git_status = {
            "branch": "main",
            "clean": True,
            "ahead": 2,
            "coverage": 0.875,
            "upstream": None,
            "files": [
                {"path": "plan.md", "staged": False},
                ["nested", 1, 0.5, None],
            ],
        }

        result = ObsidianSyncBatchResult(
            trade_date=date(2026, 7, 15),
            phase="reconcile",
            written_files=(),
            skipped_files=(),
            pending_files=(),
            failed_files=(),
            git_status=git_status,
        )

        expected = {
            "branch": "main",
            "clean": True,
            "ahead": 2,
            "coverage": 0.875,
            "upstream": None,
            "files": [
                {"path": "plan.md", "staged": False},
                ["nested", 1, 0.5, None],
            ],
        }
        self.assertIsNot(result.git_status, git_status)
        self.assertEqual(result.git_status_json(), expected)

        git_status["branch"] = "caller-mutated"
        git_status["files"][0]["path"] = "caller-mutated.md"  # type: ignore[index]
        git_status["files"].append("caller-mutated")  # type: ignore[union-attr]
        self.assertEqual(result.git_status_json(), expected)

        with self.assertRaises(TypeError):
            result.git_status["branch"] = "direct mutation"  # type: ignore[index]
        frozen_files = result.git_status["files"]
        with self.assertRaises(TypeError):
            frozen_files[0] = "direct mutation"  # type: ignore[index]
        frozen_first_file = frozen_files[0]  # type: ignore[index]
        with self.assertRaises(TypeError):
            frozen_first_file["path"] = "direct mutation.md"  # type: ignore[index]

        plain = result.git_status_json()
        self.assertIsNot(plain, result.git_status_json())
        plain["files"][0]["path"] = "copy-mutated.md"  # type: ignore[index]
        self.assertEqual(result.git_status_json(), expected)

    def test_sync_batch_asdict_exports_a_fresh_plain_git_status(self) -> None:
        result = ObsidianSyncBatchResult(
            trade_date=date(2026, 7, 15),
            phase="reconcile",
            written_files=("written.md",),
            skipped_files=(),
            pending_files=(),
            failed_files=(),
            git_status={"branch": "main", "files": [{"path": "plan.md"}]},
        )

        exported = asdict(result)

        self.assertIs(type(exported["git_status"]), dict)
        self.assertEqual(exported["git_status"], result.git_status_json())
        exported["git_status"]["files"][0]["path"] = "mutated.md"  # type: ignore[index]
        self.assertEqual(result.git_status_json()["files"][0]["path"], "plan.md")  # type: ignore[index]

    def test_sync_batch_deepcopy_preserves_the_deeply_frozen_snapshot(self) -> None:
        result = ObsidianSyncBatchResult(
            trade_date=date(2026, 7, 15),
            phase="reconcile",
            written_files=("written.md",),
            skipped_files=(),
            pending_files=(),
            failed_files=(),
            git_status={"branch": "main", "files": [{"path": "plan.md"}]},
        )

        copied = deepcopy(result)

        self.assertIs(copied, result)
        with self.assertRaises(TypeError):
            copied.git_status["branch"] = "mutation"  # type: ignore[index]

    def test_sync_batch_replace_reconstructs_from_frozen_git_status(self) -> None:
        result = ObsidianSyncBatchResult(
            trade_date=date(2026, 7, 15),
            phase="reconcile",
            written_files=("written.md",),
            skipped_files=(),
            pending_files=(),
            failed_files=(),
            git_status={"branch": "main", "files": [{"path": "plan.md"}]},
        )

        replaced = replace(result, phase="catalog")

        self.assertEqual(replaced.phase, "catalog")
        self.assertEqual(replaced.git_status_json(), result.git_status_json())
        with self.assertRaises(TypeError):
            replaced.git_status["branch"] = "mutation"  # type: ignore[index]

    def test_dto_mapping_annotations_are_read_only(self) -> None:
        artifact_hints = get_type_hints(ObsidianArtifact)
        batch_hints = get_type_hints(ObsidianSyncBatchResult)

        self.assertIs(get_origin(artifact_hints["payload"]), Mapping)
        self.assertIs(get_origin(batch_hints["git_status"]), Mapping)

    def test_frozen_mapping_domains_only_reconstruct_their_own_dtos(self) -> None:
        artifact = ObsidianArtifact(
            snapshot_key="plan:42:2026-07-15",
            trade_date=date(2026, 7, 15),
            entity_type="plan",
            entity_id=42,
            phase="preclose",
            target_path="30_TradingPlaybook/Daily/Auto/2026-07-15.md",
            immutable=True,
            payload={
                "trade_date": date(2026, 7, 15),
                "captured_at": datetime(2026, 7, 15, 1, 30, tzinfo=timezone.utc),
                "amount": Decimal("10.500"),
            },
        )
        batch = ObsidianSyncBatchResult(
            trade_date=date(2026, 7, 15),
            phase="reconcile",
            written_files=(),
            skipped_files=(),
            pending_files=(),
            failed_files=(),
            git_status={"branch": "main", "clean": True},
        )

        with self.assertRaisesRegex(TypeError, "git_status must be a dict"):
            ObsidianSyncBatchResult(
                trade_date=date(2026, 7, 15),
                phase="reconcile",
                written_files=(),
                skipped_files=(),
                pending_files=(),
                failed_files=(),
                git_status=artifact.payload,  # type: ignore[arg-type]
            )
        with self.assertRaisesRegex(TypeError, "payload must be a dict"):
            ObsidianArtifact(
                snapshot_key="plan:43:2026-07-15",
                trade_date=date(2026, 7, 15),
                entity_type="plan",
                entity_id=43,
                phase="preclose",
                target_path="30_TradingPlaybook/Daily/Auto/2026-07-15-43.md",
                immutable=True,
                payload=batch.git_status,  # type: ignore[arg-type]
            )

        replaced_artifact = replace(artifact, phase="after_close")
        replaced_batch = replace(batch, phase="catalog")
        self.assertEqual(replaced_artifact.payload_json(), artifact.payload_json())
        self.assertEqual(replaced_artifact.source_hash, artifact.source_hash)
        self.assertEqual(replaced_batch.git_status_json(), batch.git_status_json())

    def test_sync_batch_json_freeze_rejects_cycles_and_excess_depth(self) -> None:
        cycle: list[object] = []
        cycle.append(cycle)
        invalid_statuses = (
            ({"cycle": cycle}, "cycle detected"),
            (
                _nested_dict(MAX_TEST_NESTING_DEPTH + 1),
                f"maximum nesting depth {MAX_TEST_NESTING_DEPTH} exceeded",
            ),
        )

        for git_status, message in invalid_statuses:
            with self.subTest(message=message):
                with self.assertRaisesRegex(ValueError, message):
                    ObsidianSyncBatchResult(
                        trade_date=date(2026, 7, 15),
                        phase="reconcile",
                        written_files=(),
                        skipped_files=(),
                        pending_files=(),
                        failed_files=(),
                        git_status=git_status,
                    )

    def test_sync_batch_json_freeze_allows_boundary_and_shared_references(self) -> None:
        shared = {"values": [1, 2]}
        boundary = _nested_dict(MAX_TEST_NESTING_DEPTH)
        boundary["shared_left"] = shared
        boundary["shared_right"] = shared

        result = ObsidianSyncBatchResult(
            trade_date=date(2026, 7, 15),
            phase="reconcile",
            written_files=(),
            skipped_files=(),
            pending_files=(),
            failed_files=(),
            git_status=boundary,
        )

        self.assertEqual(result.git_status_json(), boundary)

    def test_sync_batch_result_git_status_must_be_a_real_dict(self) -> None:
        invalid_statuses = (
            [],
            UserDict({"branch": "main"}),
            MappingProxyType({"branch": "main"}),
        )

        for git_status in invalid_statuses:
            with self.subTest(git_status_type=type(git_status).__name__):
                with self.assertRaisesRegex(TypeError, "git_status must be a dict"):
                    ObsidianSyncBatchResult(
                        trade_date=date(2026, 7, 15),
                        phase="reconcile",
                        written_files=(),
                        skipped_files=(),
                        pending_files=(),
                        failed_files=(),
                        git_status=git_status,  # type: ignore[arg-type]
                    )


if __name__ == "__main__":
    unittest.main()
