"""Point-in-time replay for transcript-derived trading playbook scenarios.

This module never invokes a notification transport. ``--no-notify`` is an
explicit safety gate for historical command-line replays.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from datetime import date, datetime
from numbers import Real
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from app.services.trading_playbook.domain import CandidateSnapshot
from app.services.trading_playbook.mode_matcher import ModeMatcher
from app.services.trading_playbook.rule_catalog import (
    RuleCatalog,
    canonical_rule_content_hash,
)


DEFAULT_CATALOG_PATH = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "trading_playbook_rules_v1.json"
)
DEFAULT_FIXTURE_PATH = (
    Path(__file__).resolve().parents[2]
    / "tests"
    / "fixtures"
    / "trading_playbook_scenarios.json"
)
_PLANNED_PRICE_FIELDS = {
    "planned_pullback_price",
    "planned_breakout_price",
    "hard_stop_price",
}
_QUALITY_STATUSES = {
    "ready",
    "computed",
    "missing",
    "degraded",
    "invalid",
    "stale",
}
_REPLAY_STAGES = {"preclose", "after_close", "overnight", "auction"}


def _load_json_strict(path: Path) -> Any:
    """Load JSON while rejecting extensions and duplicate object keys."""
    resolved = Path(path)

    def reject_constant(value: str) -> Any:
        raise ValueError(f"{resolved}: invalid constant {value}")

    def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"{resolved}: duplicate key {key}")
            result[key] = value
        return result

    try:
        with resolved.open("r", encoding="utf-8") as handle:
            return json.load(
                handle,
                parse_constant=reject_constant,
                object_pairs_hook=reject_duplicate_keys,
            )
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"{resolved}: invalid JSON at line {exc.lineno}: {exc.msg}"
        ) from exc


def _load_catalog_strict(path: Path) -> dict[str, Any]:
    resolved = Path(path)
    _load_json_strict(resolved)
    return RuleCatalog(resolved).load()


def _aware_timestamp(value: Any, *, field: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"invalid timestamp: {field}")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"invalid timestamp: {field}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"timezone-aware timestamp required: {field}")
    return parsed


def _validate_json_like(value: Any, *, path: str) -> None:
    """Validate direct-call values with JSON's exact type constraints."""
    if value is None or type(value) in {str, bool, int}:
        return
    if type(value) is float:
        if not math.isfinite(value):
            if path.rsplit(".", 1)[-1] in _PLANNED_PRICE_FIELDS:
                raise ValueError(f"{path}: positive finite price required")
            raise ValueError(f"{path}: non-finite number")
        return
    if isinstance(value, Mapping):
        for key, child in value.items():
            if not isinstance(key, str):
                raise ValueError(f"{path}: non-string key {key!r}")
            _validate_json_like(child, path=f"{path}.{key}")
        return
    if isinstance(value, list):
        for index, child in enumerate(value):
            _validate_json_like(child, path=f"{path}[{index}]")
        return
    raise ValueError(f"{path}: invalid JSON-like type {type(value).__name__}")


def _require_nonempty_string(
    source: Mapping[str, Any],
    field: str,
    *,
    path: str,
) -> str:
    value = source.get(field)
    if type(value) is not str or not value.strip():
        raise ValueError(f"{path}.{field}: non-empty string required")
    return value


def _validate_scenario_shape(scenario: Any) -> tuple[str, str]:
    unknown_path = "scenario[unknown]"
    if not isinstance(scenario, Mapping):
        raise ValueError(f"{unknown_path}: object required")

    mode_key = scenario.get("mode_key")
    if type(mode_key) is not str or not mode_key.strip():
        raise ValueError(
            f"{unknown_path}.mode_key: non-empty string required"
        )
    scenario_path = f"scenario[{mode_key}]"
    _require_nonempty_string(scenario, "as_of", path=scenario_path)
    rule_hash = _require_nonempty_string(
        scenario,
        "rule_hash",
        path=scenario_path,
    )
    if re.fullmatch(r"[0-9a-f]{64}", rule_hash) is None:
        raise ValueError(f"{scenario_path}.rule_hash: sha256 required")

    market_features = scenario.get("market_features")
    if not isinstance(market_features, Mapping):
        raise ValueError(f"{scenario_path}.market_features: object required")
    candidate = scenario.get("candidate")
    if not isinstance(candidate, Mapping):
        raise ValueError(f"{scenario_path}.candidate: object required")
    for field in ("stock_code", "stock_name", "theme_name"):
        _require_nonempty_string(
            candidate,
            field,
            path=f"{scenario_path}.candidate",
        )
    if not isinstance(candidate.get("features"), Mapping):
        raise ValueError(
            f"{scenario_path}.candidate.features: object required"
        )

    source_refs = scenario.get("source_refs")
    if not isinstance(source_refs, list) or not source_refs:
        raise ValueError(f"{scenario_path}.source_refs: non-empty list required")
    for index, source_ref in enumerate(source_refs):
        ref_path = f"{scenario_path}.source_refs[{index}]"
        if not isinstance(source_ref, Mapping):
            raise ValueError(f"{ref_path}: object required")
        for field in ("source_key", "excerpt"):
            _require_nonempty_string(source_ref, field, path=ref_path)

    facts = scenario.get("facts")
    if not isinstance(facts, list) or not facts:
        raise ValueError(f"{scenario_path}.facts: non-empty list required")
    for index, fact in enumerate(facts):
        fact_path = f"{scenario_path}.facts[{index}]"
        if not isinstance(fact, Mapping):
            raise ValueError(f"{fact_path}: object required")
        _require_nonempty_string(fact, "captured_at", path=fact_path)
        source_keys = fact.get("source_keys")
        if (
            not isinstance(source_keys, list)
            or not source_keys
            or any(
                type(source_key) is not str or not source_key.strip()
                for source_key in source_keys
            )
        ):
            raise ValueError(
                f"{fact_path}.source_keys: non-empty string list required"
            )
        if len(source_keys) != len(set(source_keys)):
            raise ValueError(f"{fact_path}.source_keys: duplicates forbidden")
        for field in ("market_features", "candidate_features"):
            if not isinstance(fact.get(field), Mapping):
                raise ValueError(f"{fact_path}.{field}: object required")
    return mode_key, scenario_path


def _same_fact_value(left: Any, right: Any) -> bool:
    """Compare JSON-like facts without treating booleans as numbers."""
    if isinstance(left, bool) or isinstance(right, bool):
        return isinstance(left, bool) and isinstance(right, bool) and left is right
    if isinstance(left, Mapping) or isinstance(right, Mapping):
        if not isinstance(left, Mapping) or not isinstance(right, Mapping):
            return False
        if set(left) != set(right):
            return False
        return all(_same_fact_value(left[key], right[key]) for key in left)
    if isinstance(left, list) or isinstance(right, list):
        if not isinstance(left, list) or not isinstance(right, list):
            return False
        return len(left) == len(right) and all(
            _same_fact_value(left_item, right_item)
            for left_item, right_item in zip(left, right)
        )
    if isinstance(left, Real) and isinstance(right, Real):
        return math.isfinite(left) and math.isfinite(right) and left == right
    return type(left) is type(right) and left == right


def _validate_matcher_controls(
    features: Mapping[str, Any],
    *,
    path: str,
) -> None:
    for field in ("_snapshot_stale", "_point_in_time_valid"):
        if field in features and type(features[field]) is not bool:
            raise ValueError(f"{path}.{field}: exact bool required")

    if "_feature_quality" in features:
        quality_map = features["_feature_quality"]
        if not isinstance(quality_map, Mapping):
            raise ValueError(f"{path}._feature_quality: mapping required")
        for feature_key, status in quality_map.items():
            status_path = f"{path}._feature_quality.{feature_key}"
            if not isinstance(feature_key, str) or not feature_key.strip():
                raise ValueError(
                    f"{path}._feature_quality: non-empty feature key required"
                )
            if type(status) is not str or status not in _QUALITY_STATUSES:
                raise ValueError(f"{status_path}: invalid quality status")

    for field in ("quality", "planned_pullback_quality"):
        if field in features:
            status = features[field]
            if type(status) is not str or status not in _QUALITY_STATUSES:
                raise ValueError(f"{path}.{field}: invalid quality status")

    if "_stage" in features:
        stage = features["_stage"]
        if type(stage) is not str or stage not in _REPLAY_STAGES:
            raise ValueError(f"{path}._stage: invalid replay stage")
    if (
        "tail_action_eligible" in features
        and type(features["tail_action_eligible"]) is not bool
    ):
        raise ValueError(f"{path}.tail_action_eligible: exact bool required")


def replay_scenario(
    scenario: Mapping[str, Any],
    *,
    catalog_path: Path = DEFAULT_CATALOG_PATH,
) -> str:
    """Evaluate one scenario against its exact versioned catalog rule."""
    mode_key, scenario_path = _validate_scenario_shape(scenario)
    _validate_json_like(scenario, path=scenario_path)
    as_of = _aware_timestamp(
        scenario.get("as_of"),
        field=f"{scenario_path}.as_of",
    )
    facts = scenario.get("facts")
    for index, fact in enumerate(facts):
        captured_at = _aware_timestamp(
            fact.get("captured_at"),
            field=f"{scenario_path}.facts[{index}].captured_at",
        )
        if captured_at > as_of:
            raise ValueError("future fact")

    catalog = _load_catalog_strict(Path(catalog_path))
    rule = next(
        (row for row in catalog["rules"] if row["mode_key"] == mode_key),
        None,
    )
    if rule is None:
        raise ValueError(f"{scenario_path}.mode_key: unknown mode: {mode_key}")
    if scenario.get("rule_hash") != canonical_rule_content_hash(rule):
        raise ValueError(f"{scenario_path}.rule_hash: rule_hash mismatch")
    if scenario.get("source_refs") != rule["source_refs"]:
        raise ValueError(
            f"{scenario_path}.source_refs: "
            "source_refs must match the exact catalog rule"
        )

    raw_candidate = scenario["candidate"]
    declared_groups = {
        "market_features": scenario["market_features"],
        "candidate_features": raw_candidate["features"],
    }
    expected_source_keys = {
        source_ref["source_key"] for source_ref in rule["source_refs"]
    }
    reconstructed_groups: dict[str, dict[str, Any]] = {
        group_name: {} for group_name in declared_groups
    }
    group_paths = {
        "market_features": f"{scenario_path}.market_features",
        "candidate_features": f"{scenario_path}.candidate.features",
    }
    for fact_index, fact in enumerate(facts):
        source_keys = fact.get("source_keys")
        if (
            not isinstance(source_keys, list)
            or len(source_keys) != len(set(source_keys))
            or set(source_keys) != expected_source_keys
        ):
            raise ValueError(
                f"{scenario_path}.facts[{fact_index}].source_keys: "
                "fact source_keys must match catalog sources"
            )
        for group_name, declared in declared_groups.items():
            fact_values = fact.get(group_name)
            if not isinstance(fact_values, Mapping):
                raise ValueError(f"fact {group_name} must be a mapping")
            for key, value in fact_values.items():
                reconstructed = reconstructed_groups[group_name]
                if key in reconstructed and not _same_fact_value(
                    reconstructed[key],
                    value,
                ):
                    raise ValueError(
                        f"{scenario_path}.facts[{fact_index}].{group_name}.{key}: "
                        "conflicting facts"
                    )
                reconstructed[key] = value

    for group_name, declared in declared_groups.items():
        if (
            not isinstance(declared, Mapping)
            or not _same_fact_value(
                reconstructed_groups[group_name],
                declared,
            )
        ):
            raise ValueError(
                f"{group_paths[group_name]}: feature map mismatch"
            )

    candidate_features = reconstructed_groups["candidate_features"]
    _validate_matcher_controls(
        reconstructed_groups["market_features"],
        path=f"{scenario_path}.market_features",
    )
    _validate_matcher_controls(
        candidate_features,
        path=f"{scenario_path}.candidate.features",
    )
    for field in _PLANNED_PRICE_FIELDS:
        value = candidate_features.get(field)
        if (
            isinstance(value, bool)
            or not isinstance(value, Real)
            or not math.isfinite(value)
            or value <= 0
        ):
            raise ValueError(
                f"{scenario_path}.candidate.features.{field}: "
                "positive finite price required"
            )

    candidate = CandidateSnapshot(
        stock_code=str(raw_candidate["stock_code"]),
        stock_name=str(raw_candidate["stock_name"]),
        theme_name=str(raw_candidate["theme_name"]),
        features=candidate_features,
        evidence=list(scenario.get("facts") or []),
    )
    evaluation = ModeMatcher(
        [rule],
        catalog_version=catalog["catalog_version"],
    ).evaluate(reconstructed_groups["market_features"], candidate)[0]
    return evaluation.status


def load_scenarios(
    *,
    fixture_path: Path = DEFAULT_FIXTURE_PATH,
    catalog_path: Path = DEFAULT_CATALOG_PATH,
) -> list[dict[str, Any]]:
    """Load the complete, duplicate-free golden scenario set."""
    payload = _load_json_strict(Path(fixture_path))
    if not isinstance(payload, dict):
        raise ValueError("scenario fixture root must be an object")
    scenarios = payload.get("scenarios")
    if not isinstance(scenarios, list):
        raise ValueError("scenarios must be a list")

    mode_keys = [
        str(scenario.get("mode_key") or "")
        if isinstance(scenario, Mapping)
        else ""
        for scenario in scenarios
    ]
    if any(not mode_key for mode_key in mode_keys):
        raise ValueError("every scenario must have a mode_key")
    if len(mode_keys) != len(set(mode_keys)):
        raise ValueError("duplicate mode_key in scenarios")

    catalog = _load_catalog_strict(Path(catalog_path))
    if payload.get("catalog_version") != catalog["catalog_version"]:
        raise ValueError("scenario catalog_version mismatch")
    catalog_mode_keys = {rule["mode_key"] for rule in catalog["rules"]}
    scenario_mode_keys = set(mode_keys)
    if len(scenarios) != 19 or scenario_mode_keys != catalog_mode_keys:
        missing = sorted(catalog_mode_keys - scenario_mode_keys)
        extra = sorted(scenario_mode_keys - catalog_mode_keys)
        raise ValueError(
            "scenario coverage mismatch: "
            f"missing={missing}, extra={extra}"
        )
    rules_by_mode = {rule["mode_key"]: rule for rule in catalog["rules"]}
    for scenario in scenarios:
        mode_key = scenario["mode_key"]
        expected_hash = canonical_rule_content_hash(rules_by_mode[mode_key])
        if scenario.get("rule_hash") != expected_hash:
            raise ValueError(f"rule_hash mismatch: {mode_key}")
    return [dict(scenario) for scenario in scenarios]


def _trade_date(value: str) -> date:
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value) is None:
        raise argparse.ArgumentTypeError("date must use YYYY-MM-DD")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "date must use YYYY-MM-DD"
        ) from exc


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Replay transcript-backed trading playbook scenarios.",
    )
    parser.add_argument("--date", required=True, type=_trade_date)
    parser.add_argument(
        "--stage",
        required=True,
        choices=("preclose", "after_close", "overnight", "auction"),
    )
    parser.add_argument("--no-notify", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _argument_parser().parse_args(argv)
    today = datetime.now(ZoneInfo("Asia/Shanghai")).date()
    if args.date < today and not args.no_notify:
        print(
            "historical replay requires --no-notify; notifications were not sent",
            file=sys.stderr,
        )
        return 2
    try:
        scenarios = load_scenarios()
        for index, scenario in enumerate(scenarios):
            mode_context = (
                scenario.get("mode_key")
                if isinstance(scenario, Mapping)
                and isinstance(scenario.get("mode_key"), str)
                else "unknown"
            )
            try:
                actual = replay_scenario(scenario)
            except Exception as exc:
                raise ValueError(
                    f"scenario index={index} mode={mode_context}: {exc}"
                ) from exc
            expected = scenario.get("expected")
            if actual != expected:
                print(
                    "replay mismatch: "
                    f"mode={scenario['mode_key']} "
                    f"expected={expected} actual={actual}",
                    file=sys.stderr,
                )
                return 1
    except Exception as exc:
        print(f"replay failed: {exc}", file=sys.stderr)
        return 1
    fixture_as_of_values = sorted({scenario["as_of"] for scenario in scenarios})
    if len(fixture_as_of_values) == 1:
        fixture_context = f"fixture_as_of={fixture_as_of_values[0]}"
    else:
        fixture_context = "fixture_as_of_set=" + "|".join(
            fixture_as_of_values
        )
    print(
        f"{len(scenarios)} evaluated; no future facts; "
        "golden_fixture_context; "
        f"requested_date={args.date.isoformat()} "
        f"requested_stage={args.stage}; {fixture_context}; "
        "facts_rewritten=false; notifications disabled"
    )
    return 0


__all__ = ["load_scenarios", "main", "replay_scenario"]


if __name__ == "__main__":
    raise SystemExit(main())
