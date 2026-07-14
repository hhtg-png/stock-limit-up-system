"""Strict, detached JSON serialization for persisted trading plans."""

from __future__ import annotations

import copy
import math
from collections.abc import Mapping
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

from .errors import UnsafePlanDataError


CN_TZ = ZoneInfo("Asia/Shanghai")


def china_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        value = value.replace(tzinfo=CN_TZ)
    else:
        value = value.astimezone(CN_TZ)
    return value.isoformat()


def json_value(value: Any) -> Any:
    """Detach audit JSON and normalize historical non-finite numbers."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if value == math.inf:
            return "Infinity"
        if value == -math.inf:
            return "-Infinity"
        return value
    if isinstance(value, datetime):
        return china_iso(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_value(item) for item in value]
    return str(value)


def _reject_nonfinite_numbers(value: Any) -> None:
    if isinstance(value, bool) or value is None:
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise UnsafePlanDataError("plan contains an unsafe numeric value")
        return
    if isinstance(value, Mapping):
        for item in value.values():
            _reject_nonfinite_numbers(item)
        return
    if isinstance(value, (list, tuple)):
        for item in value:
            _reject_nonfinite_numbers(item)


def normalize_plan_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Validate operational numerics and normalize audit-only JSON."""
    detached = copy.deepcopy(dict(payload))
    for risk_key in ("risk_settings_json", "risk_settings"):
        if risk_key in detached:
            _reject_nonfinite_numbers(detached[risk_key])
    candidates = detached.get("candidates", [])
    if not isinstance(candidates, list):
        raise UnsafePlanDataError("plan candidates are malformed")
    for candidate in candidates:
        if not isinstance(candidate, Mapping):
            raise UnsafePlanDataError("plan candidate is malformed")
        position = candidate.get("position_reference")
        if isinstance(position, bool) or not isinstance(position, (int, float)):
            raise UnsafePlanDataError("candidate position is malformed")
        try:
            position_is_finite = math.isfinite(float(position))
        except (OverflowError, TypeError, ValueError):
            position_is_finite = False
        if not position_is_finite:
            raise UnsafePlanDataError("candidate position is unsafe")
        for trigger_key in (
            "entry_trigger_json",
            "invalidation_json",
            "exit_trigger_json",
        ):
            _reject_nonfinite_numbers(candidate.get(trigger_key))
    return json_value(detached)


__all__ = ["china_iso", "json_value", "normalize_plan_payload"]
