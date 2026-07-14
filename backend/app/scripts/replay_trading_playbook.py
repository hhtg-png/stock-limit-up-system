"""Point-in-time replay for transcript-derived trading playbook scenarios.

This module never invokes a notification transport. ``--no-notify`` is an
explicit safety gate for historical command-line replays.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from app.services.trading_playbook.domain import CandidateSnapshot
from app.services.trading_playbook.mode_matcher import ModeMatcher
from app.services.trading_playbook.rule_catalog import RuleCatalog


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


def replay_scenario(
    scenario: Mapping[str, Any],
    *,
    catalog_path: Path = DEFAULT_CATALOG_PATH,
) -> str:
    """Evaluate one scenario against its exact versioned catalog rule."""
    as_of = _aware_timestamp(scenario.get("as_of"), field="as_of")
    facts = scenario.get("facts")
    if not isinstance(facts, list) or not facts:
        raise ValueError("facts must be a non-empty list")
    for index, fact in enumerate(facts):
        captured_at = _aware_timestamp(
            fact.get("captured_at") if isinstance(fact, Mapping) else None,
            field=f"facts[{index}].captured_at",
        )
        if captured_at > as_of:
            raise ValueError("future fact")

    catalog = RuleCatalog(Path(catalog_path)).load()
    mode_key = str(scenario.get("mode_key") or "")
    rule = next(
        (row for row in catalog["rules"] if row["mode_key"] == mode_key),
        None,
    )
    if rule is None:
        raise ValueError(f"unknown mode: {mode_key}")
    if scenario.get("source_refs") != rule["source_refs"]:
        raise ValueError("source_refs must match the exact catalog rule")

    raw_candidate = scenario["candidate"]
    declared_groups = {
        "market_features": scenario["market_features"],
        "candidate_features": raw_candidate["features"],
    }
    expected_source_keys = {
        source_ref["source_key"] for source_ref in rule["source_refs"]
    }
    audited_features = set()
    for fact in facts:
        source_keys = fact.get("source_keys")
        if (
            not isinstance(source_keys, list)
            or len(source_keys) != len(set(source_keys))
            or set(source_keys) != expected_source_keys
        ):
            raise ValueError("fact source_keys must match catalog sources")
        for group_name, declared in declared_groups.items():
            fact_values = fact.get(group_name)
            if not isinstance(fact_values, Mapping):
                raise ValueError(f"fact {group_name} must be a mapping")
            for key, value in fact_values.items():
                if key not in declared or declared[key] != value:
                    raise ValueError(f"fact mismatch: {group_name}.{key}")
                owner = "market" if group_name == "market_features" else "candidate"
                audited_features.add(f"{owner}.{key}")

    required_audit = {
        "market.style",
        "market.window",
        "candidate.planned_pullback_price",
        "candidate.planned_breakout_price",
        "candidate.hard_stop_price",
        *(requirement["feature"] for requirement in rule["requirements"]),
    }
    unaudited = sorted(required_audit - audited_features)
    if unaudited:
        raise ValueError(f"unaudited feature: {unaudited[0]}")

    candidate = CandidateSnapshot(
        stock_code=str(raw_candidate["stock_code"]),
        stock_name=str(raw_candidate["stock_name"]),
        theme_name=str(raw_candidate["theme_name"]),
        features=dict(raw_candidate["features"]),
        evidence=list(scenario.get("facts") or []),
    )
    evaluation = ModeMatcher(
        [rule],
        catalog_version=catalog["catalog_version"],
    ).evaluate(dict(scenario["market_features"]), candidate)[0]
    return evaluation.status


def load_scenarios(
    *,
    fixture_path: Path = DEFAULT_FIXTURE_PATH,
    catalog_path: Path = DEFAULT_CATALOG_PATH,
) -> list[dict[str, Any]]:
    """Load the complete, duplicate-free golden scenario set."""
    with Path(fixture_path).open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
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

    catalog = RuleCatalog(Path(catalog_path)).load()
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
        for scenario in scenarios:
            actual = replay_scenario(scenario)
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
    print(
        f"{len(scenarios)} evaluated; no future facts; "
        f"date={args.date.isoformat()} stage={args.stage}; notifications disabled"
    )
    return 0


__all__ = ["load_scenarios", "main", "replay_scenario"]


if __name__ == "__main__":
    raise SystemExit(main())
