"""Deterministic evaluator for the versioned transcript rule manifest."""

from __future__ import annotations

import copy
import hashlib
import json
import math
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from .domain import CandidateSnapshot, ModeEvaluation


_SUPPORTED_OPERATORS = {"eq", "in", "lte", "gte"}
_PULLBACK_ROLES = {"survivor", "trend_core", "resilient_core", "snake_arbitrage"}
_SEALED_ROLES = {
    "leader",
    "confirmed_leader",
    "first_mover",
    "high_position",
    "same_level_turnover",
}


def _finite_number(value: Any) -> Optional[float]:
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _canonical_hash(rule: Mapping[str, Any]) -> str:
    payload = {
        key: value
        for key, value in rule.items()
        if key != "content_hash"
    }
    serialized = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


class ModeMatcher:
    """Match normalized candidate facts against all catalog rules."""

    def __init__(
        self,
        rules: Iterable[Mapping[str, Any]],
        *,
        catalog_version: int = 1,
    ) -> None:
        version = _finite_number(catalog_version)
        if version is None or version < 1 or not version.is_integer():
            raise ValueError("catalog_version must be a positive integer")
        normalized = []
        seen = set()
        for source in rules:
            rule = copy.deepcopy(dict(source))
            mode_key = str(rule.get("mode_key") or "").strip()
            if not mode_key:
                raise ValueError("mode_key is required")
            if mode_key in seen:
                raise ValueError(f"duplicate mode_key: {mode_key}")
            seen.add(mode_key)
            automation = rule.get("automation_level")
            if automation not in {"automatic", "assisted", "manual_only"}:
                raise ValueError(f"invalid automation_level for {mode_key}")
            requirements = rule.get("requirements")
            if not isinstance(requirements, list):
                raise ValueError(f"requirements must be a list for {mode_key}")
            for requirement in requirements:
                if requirement.get("op") not in _SUPPORTED_OPERATORS:
                    raise ValueError(
                        f"unsupported operator for {mode_key}: {requirement.get('op')}"
                    )
                feature = requirement.get("feature")
                if not isinstance(feature, str) or "." not in feature:
                    raise ValueError(f"invalid feature for {mode_key}")
            rule["mode_key"] = mode_key
            rule["version"] = int(version)
            rule["content_hash"] = str(
                rule.get("content_hash") or _canonical_hash(rule)
            )
            if len(rule["content_hash"]) != 64:
                raise ValueError(f"invalid content_hash for {mode_key}")
            normalized.append(rule)
        self.rules = tuple(
            sorted(
                normalized,
                key=lambda item: (
                    -self._priority(item),
                    item["mode_key"],
                ),
            )
        )

    def evaluate(
        self,
        market_features: Mapping[str, Any],
        candidate: CandidateSnapshot,
    ) -> List[ModeEvaluation]:
        market = copy.deepcopy(dict(market_features))
        features = copy.deepcopy(candidate.features)
        return [
            self._evaluate_rule(rule, market, candidate, features)
            for rule in self.rules
        ]

    def rule_snapshot(self) -> List[Dict[str, Any]]:
        return [
            {
                "mode_key": rule["mode_key"],
                "version": rule["version"],
                "content_hash": rule["content_hash"],
            }
            for rule in sorted(self.rules, key=lambda item: item["mode_key"])
        ]

    def _evaluate_rule(
        self,
        rule: Mapping[str, Any],
        market: Mapping[str, Any],
        candidate: CandidateSnapshot,
        features: Mapping[str, Any],
    ) -> ModeEvaluation:
        conditions: List[Dict[str, Any]] = []
        implicit = self._implicit_rule_conditions(rule, market)
        conditions.extend(implicit)
        for requirement in rule["requirements"]:
            conditions.append(
                self._condition(requirement, market, features)
            )

        if any(item["result"] == "failed" for item in conditions):
            status, risk_level = "not_matched", "avoid"
        elif any(item["result"] == "missing" for item in conditions):
            status, risk_level = "waiting", "watch"
        else:
            data_ready = self._data_ready(rule, market, features)
            automation = rule["automation_level"]
            if automation == "manual_only":
                status, risk_level = "manual_review", "watch"
            elif not data_ready:
                status, risk_level = "waiting", "watch"
            elif automation == "automatic":
                status, risk_level = "matched", "confirmed"
            else:
                status, risk_level = "matched", "trial"

        entry_trigger, entry_ready = self._entry_trigger(rule, features)
        invalidation, invalidation_ready = self._invalidation(rule, features)
        exit_trigger, exit_ready = self._exit_trigger(rule, features)
        if status in {"matched", "manual_review"} and not (
            entry_ready and invalidation_ready and exit_ready
        ):
            status, risk_level = "waiting", "watch"

        evidence = copy.deepcopy(candidate.evidence)
        evidence.extend(
            {
                "source": "mode_requirement",
                "feature": item["feature"],
                "operator": item["op"],
                "expected": copy.deepcopy(item.get("expected")),
                "actual": copy.deepcopy(item.get("actual")),
                "result": item["result"],
            }
            for item in conditions
        )
        hard_stop = _finite_number(features.get("hard_stop_price"))
        evidence.append(
            {
                "source": "mode_risk",
                "hard_stop_price": hard_stop,
                "quality": "ready"
                if hard_stop is not None and hard_stop > 0
                else "missing",
            }
        )

        action_scope = "target"
        if (
            status == "matched"
            and rule["automation_level"] != "manual_only"
            and features.get("tail_action_eligible") is True
            and features.get("_stage", "preclose") == "preclose"
        ):
            action_scope = "tail"

        return ModeEvaluation(
            mode_key=rule["mode_key"],
            stock_code=candidate.stock_code,
            status=status,
            score=self._priority(rule),
            role=str(rule.get("role") or ""),
            risk_level=risk_level,
            entry_trigger=entry_trigger,
            invalidation=invalidation,
            exit_trigger=exit_trigger,
            evidence=evidence,
            rule_version=int(rule["version"]),
            rule_hash=str(rule["content_hash"]),
            action_scope=action_scope,
        )

    @staticmethod
    def _implicit_rule_conditions(
        rule: Mapping[str, Any],
        market: Mapping[str, Any],
    ) -> List[Dict[str, Any]]:
        rows = []
        window = str(rule.get("window") or "").strip()
        if window:
            allowed = [item.strip() for item in window.split(",") if item.strip()]
            rows.append(
                ModeMatcher._direct_condition(
                    "market.window",
                    "in",
                    allowed,
                    market.get("window"),
                )
            )
        style = str(rule.get("style") or "").strip()
        if style:
            rows.append(
                ModeMatcher._direct_condition(
                    "market.style",
                    "eq",
                    style,
                    market.get("style"),
                )
            )
        return rows

    @classmethod
    def _condition(
        cls,
        requirement: Mapping[str, Any],
        market: Mapping[str, Any],
        features: Mapping[str, Any],
    ) -> Dict[str, Any]:
        feature = requirement["feature"]
        owner, key = feature.split(".", 1)
        source = market if owner == "market" else features if owner == "candidate" else None
        if source is None or key not in source:
            return {
                "feature": feature,
                "op": requirement["op"],
                "expected": requirement.get("value"),
                "actual": None,
                "result": "missing",
            }
        quality_map = source.get("_feature_quality", {})
        if isinstance(quality_map, Mapping) and quality_map.get(key) not in {
            None,
            "ready",
            "computed",
        }:
            result = "missing"
        else:
            result = cls._compare(
                key,
                source.get(key),
                requirement["op"],
                requirement.get("value"),
            )
        return {
            "feature": feature,
            "op": requirement["op"],
            "expected": requirement.get("value"),
            "actual": source.get(key),
            "result": result,
        }

    @classmethod
    def _direct_condition(
        cls,
        feature: str,
        op: str,
        expected: Any,
        actual: Any,
    ) -> Dict[str, Any]:
        return {
            "feature": feature,
            "op": op,
            "expected": expected,
            "actual": actual,
            "result": cls._compare(feature.split(".")[-1], actual, op, expected),
        }

    @classmethod
    def _compare(
        cls,
        key: str,
        actual: Any,
        op: str,
        expected: Any,
    ) -> str:
        if actual is None:
            return "missing"
        if op == "eq":
            if isinstance(expected, bool):
                return (
                    "matched"
                    if isinstance(actual, bool) and actual is expected
                    else "failed"
                    if isinstance(actual, bool)
                    else "missing"
                )
            if isinstance(expected, (int, float)) and not isinstance(expected, bool):
                actual_number = cls._valid_numeric(key, actual)
                expected_number = _finite_number(expected)
                if actual_number is None or expected_number is None:
                    return "missing"
                return "matched" if actual_number == expected_number else "failed"
            if isinstance(expected, str):
                if not isinstance(actual, str):
                    return "missing"
                return "matched" if actual == expected else "failed"
            return "matched" if actual == expected else "failed"
        if op == "in":
            if isinstance(expected, str):
                choices: Sequence[Any] = [
                    value.strip() for value in expected.split(",") if value.strip()
                ]
            elif isinstance(expected, Sequence) and not isinstance(expected, (bytes, bytearray)):
                choices = expected
            else:
                return "missing"
            if isinstance(actual, str) and not actual.strip():
                return "missing"
            if any(isinstance(value, str) for value in choices) and not isinstance(actual, str):
                return "missing"
            numeric_choices = bool(choices) and all(
                isinstance(value, (int, float)) and not isinstance(value, bool)
                for value in choices
            )
            if numeric_choices and _finite_number(actual) is None:
                return "missing"
            return "matched" if actual in choices else "failed"
        actual_number = cls._valid_numeric(key, actual)
        expected_number = _finite_number(expected)
        if actual_number is None or expected_number is None:
            return "missing"
        if op == "lte":
            return "matched" if actual_number <= expected_number else "failed"
        if op == "gte":
            return "matched" if actual_number >= expected_number else "failed"
        raise ValueError(f"unsupported operator: {op}")

    @staticmethod
    def _valid_numeric(key: str, value: Any) -> Optional[float]:
        number = _finite_number(value)
        if number is None:
            return None
        if key.endswith("_rank") and (number < 1 or not number.is_integer()):
            return None
        if key.endswith("_days") and (number < 0 or not number.is_integer()):
            return None
        if "price" in key and number <= 0:
            return None
        return number

    @staticmethod
    def _data_ready(
        rule: Mapping[str, Any],
        market: Mapping[str, Any],
        features: Mapping[str, Any],
    ) -> bool:
        if market.get("quality", "ready") != "ready":
            return False
        if features.get("_snapshot_quality_status", "ready") != "ready":
            return False
        if features.get("_snapshot_stale", False) is True:
            return False
        if features.get("_point_in_time_valid", True) is not True:
            return False
        return True

    @staticmethod
    def _entry_trigger(
        rule: Mapping[str, Any],
        features: Mapping[str, Any],
    ) -> Tuple[Dict[str, Any], bool]:
        label = str((rule.get("entry") or {}).get("label") or "")
        role = str(rule.get("role") or "")
        reference = _finite_number(features.get("reference_price"))
        trigger: Dict[str, Any] = {"label": label}
        if reference is None or reference <= 0:
            return trigger, False
        trigger["reference_price"] = reference
        if role in _PULLBACK_ROLES:
            price = _finite_number(features.get("planned_pullback_price"))
            if price is None or price <= 0:
                return trigger, False
            trigger["price_lte"] = price
        elif role in _SEALED_ROLES:
            trigger["sealed"] = True
        else:
            price = _finite_number(features.get("planned_breakout_price"))
            if price is None or price <= 0:
                return trigger, False
            trigger["price_gte"] = price
        return trigger, True

    @staticmethod
    def _invalidation(
        rule: Mapping[str, Any],
        features: Mapping[str, Any],
    ) -> Tuple[Dict[str, Any], bool]:
        trigger: Dict[str, Any] = {
            "label": str((rule.get("invalidation") or {}).get("label") or "")
        }
        hard_stop = _finite_number(features.get("hard_stop_price"))
        if hard_stop is None or hard_stop <= 0:
            return trigger, False
        trigger["price_lte"] = hard_stop
        return trigger, True

    @staticmethod
    def _exit_trigger(
        rule: Mapping[str, Any],
        features: Mapping[str, Any],
    ) -> Tuple[Dict[str, Any], bool]:
        trigger: Dict[str, Any] = {
            "label": str((rule.get("exit") or {}).get("label") or "")
        }
        raw_floor = (rule.get("exit") or {}).get(
            "change_pct_floor",
            features.get("exit_change_pct_floor"),
        )
        floor = _finite_number(raw_floor)
        if floor is None or not -100 <= floor <= 0:
            return trigger, False
        trigger["change_pct_lte"] = floor
        return trigger, True

    @staticmethod
    def _priority(rule: Mapping[str, Any]) -> float:
        value = _finite_number(rule.get("priority"))
        return value if value is not None else 0.0


__all__ = ["ModeMatcher"]
