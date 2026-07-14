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
_PLAN_STAGES = {"preclose", "after_close", "overnight", "auction"}
_PLAN_STATUSES = {"draft", "confirmed", "active", "superseded", "expired"}
_RISK_LEVELS = {"avoid", "watch", "trial", "confirmed"}
_REQUIRED_RISK_KEYS = {"trial", "confirmed", "hard_stop", "max_candidates"}


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


def _number(value: Any, message: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise UnsafePlanDataError(message)
    try:
        number = float(value)
    except (OverflowError, TypeError, ValueError) as exc:
        raise UnsafePlanDataError(message) from exc
    if not math.isfinite(number):
        raise UnsafePlanDataError(message)
    return number


def _date_value(value: Any, message: str) -> date:
    if isinstance(value, datetime):
        raise UnsafePlanDataError(message)
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise UnsafePlanDataError(message) from exc
    raise UnsafePlanDataError(message)


def _validate_datetime(value: Any, message: str) -> None:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except ValueError as exc:
            raise UnsafePlanDataError(message) from exc
    if not isinstance(value, datetime):
        raise UnsafePlanDataError(message)
    if value.tzinfo is None or value.utcoffset() is None:
        raise UnsafePlanDataError(message)


def _validate_risk_settings(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise UnsafePlanDataError("plan risk settings are malformed")
    if not _REQUIRED_RISK_KEYS.issubset(value):
        raise UnsafePlanDataError("plan risk settings are incomplete")
    trial = _number(value["trial"], "trial position is malformed")
    confirmed = _number(
        value["confirmed"],
        "confirmed position is malformed",
    )
    hard_stop = _number(value["hard_stop"], "hard stop is malformed")
    maximum = value["max_candidates"]
    if not 0 <= trial <= confirmed <= 100:
        raise UnsafePlanDataError("plan position limits are unsafe")
    if not 0 < hard_stop <= 20:
        raise UnsafePlanDataError("plan hard stop is unsafe")
    if (
        isinstance(maximum, bool)
        or not isinstance(maximum, int)
        or not 1 <= maximum <= 3
    ):
        raise UnsafePlanDataError("plan candidate limit is unsafe")
    _reject_nonfinite_numbers(value)
    return {
        "trial": trial,
        "confirmed": confirmed,
        "hard_stop": hard_stop,
        "max_candidates": maximum,
    }


def _validate_trigger(value: Any, field: str) -> None:
    if not isinstance(value, Mapping):
        raise UnsafePlanDataError(f"candidate {field} is malformed")
    _reject_nonfinite_numbers(value)
    for key in ("reference_price", "price_gte", "price_lte"):
        if key in value and _number(
            value[key],
            f"candidate {field} price is malformed",
        ) <= 0:
            raise UnsafePlanDataError(f"candidate {field} price is unsafe")
    for key in ("change_pct_gte", "change_pct_lte"):
        if key not in value:
            continue
        percentage = _number(
            value[key],
            f"candidate {field} percentage is malformed",
        )
        if not -100 <= percentage <= 100:
            raise UnsafePlanDataError(
                f"candidate {field} percentage is unsafe"
            )
        if (
            field == "entry trigger"
            and key == "change_pct_gte"
            and percentage < 0
        ):
            raise UnsafePlanDataError("candidate entry percentage is unsafe")
        if (
            field == "exit trigger"
            and key == "change_pct_lte"
            and percentage > 0
        ):
            raise UnsafePlanDataError("candidate exit percentage is unsafe")
    lower_price = value.get("price_gte")
    upper_price = value.get("price_lte")
    if lower_price is not None and upper_price is not None:
        if _number(lower_price, "candidate price range is malformed") > _number(
            upper_price,
            "candidate price range is malformed",
        ):
            raise UnsafePlanDataError("candidate price range is unsafe")


def _validate_operational_payload(payload: Mapping[str, Any]) -> None:
    source_date = _date_value(
        payload.get("source_trade_date"),
        "plan source trade date is malformed",
    )
    target_date = _date_value(
        payload.get("target_trade_date"),
        "plan target trade date is malformed",
    )
    if source_date > target_date:
        raise UnsafePlanDataError("plan trade-date order is unsafe")
    if payload.get("stage") not in _PLAN_STAGES:
        raise UnsafePlanDataError("plan stage is malformed")
    if payload.get("status") not in _PLAN_STATUSES:
        raise UnsafePlanDataError("plan status is malformed")
    _validate_datetime(
        payload.get("generated_at"),
        "plan generation time is malformed",
    )
    if payload.get("confirmed_at") is not None:
        _validate_datetime(
            payload["confirmed_at"],
            "plan confirmation time is malformed",
        )

    risk = _validate_risk_settings(payload.get("risk_settings_json"))
    candidates = payload.get("candidates")
    if not isinstance(candidates, list):
        raise UnsafePlanDataError("plan candidates are malformed")
    if not 1 <= len(candidates) <= risk["max_candidates"]:
        raise UnsafePlanDataError("plan candidate count is unsafe")
    stock_codes: set[str] = set()
    for candidate in candidates:
        if not isinstance(candidate, Mapping):
            raise UnsafePlanDataError("plan candidate is malformed")
        stock_code = candidate.get("stock_code")
        if not isinstance(stock_code, str) or not stock_code.strip():
            raise UnsafePlanDataError("candidate stock code is malformed")
        if stock_code in stock_codes:
            raise UnsafePlanDataError("candidate stocks are not unique")
        stock_codes.add(stock_code)
        action_date = _date_value(
            candidate.get("action_trade_date"),
            "candidate action trade date is malformed",
        )
        if action_date not in {source_date, target_date}:
            raise UnsafePlanDataError("candidate action trade date is unsafe")
        position = _number(
            candidate.get("position_reference"),
            "candidate position is malformed",
        )
        if not 0 <= position <= 100:
            raise UnsafePlanDataError("candidate position is unsafe")
        if candidate.get("risk_level") not in _RISK_LEVELS:
            raise UnsafePlanDataError("candidate risk level is malformed")
        entry_trigger = candidate.get("entry_trigger_json")
        _validate_trigger(entry_trigger, "entry trigger")
        if "reference_price" not in entry_trigger:
            raise UnsafePlanDataError(
                "candidate entry reference price is missing"
            )
        _validate_trigger(
            candidate.get("invalidation_json"),
            "invalidation trigger",
        )
        _validate_trigger(candidate.get("exit_trigger_json"), "exit trigger")


def normalize_plan_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Validate operational numerics and normalize audit-only JSON."""
    detached = copy.deepcopy(dict(payload))
    _validate_operational_payload(detached)
    return json_value(detached)


__all__ = ["china_iso", "json_value", "normalize_plan_payload"]
