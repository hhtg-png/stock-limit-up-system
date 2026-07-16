"""Strict, detached JSON serialization for persisted trading plans."""

from __future__ import annotations

import copy
import math
import re
from collections.abc import Mapping
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

from .errors import UnsafePlanDataError


CN_TZ = ZoneInfo("Asia/Shanghai")
_PLAN_STAGES = {"preclose", "after_close", "overnight", "auction"}
_PLAN_STATUSES = {"draft", "confirmed", "active", "superseded", "expired"}
_FORMAL_RISK_LEVELS = {"trial", "confirmed"}
_REQUIRED_RISK_KEYS = {"trial", "confirmed", "hard_stop", "max_candidates"}
_STOCK_CODE = re.compile(r"[0-9]{6}")


class ValidatedPlanPayload(dict[str, Any]):
    """Runtime marker for a strict, detached plan response snapshot."""


class ValidatedSettingsPayload(dict[str, Any]):
    """Runtime marker for a strict, detached settings response snapshot."""


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


def materialize_candidate_risk(
    reference_price: Any,
    risk_level: Any,
    risk_settings: Mapping[str, Any],
) -> tuple[float, float]:
    """Return the authoritative formal position and rounded hard-stop price."""
    if risk_level not in _FORMAL_RISK_LEVELS:
        raise ValueError("formal candidate risk level must be actionable")
    if (
        isinstance(reference_price, bool)
        or not isinstance(reference_price, (int, float))
        or not math.isfinite(float(reference_price))
        or float(reference_price) <= 0
    ):
        raise ValueError("candidate reference price must be finite and positive")
    position = risk_settings.get(risk_level)
    hard_stop = risk_settings.get("hard_stop")
    if (
        isinstance(position, bool)
        or not isinstance(position, (int, float))
        or not math.isfinite(float(position))
        or not 0 <= float(position) <= 100
    ):
        raise ValueError("candidate position setting is unsafe")
    if (
        isinstance(hard_stop, bool)
        or not isinstance(hard_stop, (int, float))
        or not math.isfinite(float(hard_stop))
        or not 0 < float(hard_stop) <= 20
    ):
        raise ValueError("candidate hard stop setting is unsafe")
    stop_price = round(
        float(reference_price) * (1 - float(hard_stop) / 100),
        2,
    )
    return float(position), stop_price


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
    if len(candidates) > risk["max_candidates"]:
        raise UnsafePlanDataError("plan candidate count is unsafe")
    stock_codes: set[str] = set()
    ranks: set[int] = set()
    for candidate in candidates:
        if not isinstance(candidate, Mapping):
            raise UnsafePlanDataError("plan candidate is malformed")
        stock_code = candidate.get("stock_code")
        if (
            not isinstance(stock_code, str)
            or _STOCK_CODE.fullmatch(stock_code) is None
        ):
            raise UnsafePlanDataError("candidate stock code is malformed")
        if stock_code in stock_codes:
            raise UnsafePlanDataError("candidate stocks are not unique")
        stock_codes.add(stock_code)
        stock_name = candidate.get("stock_name")
        if not isinstance(stock_name, str) or not stock_name.strip():
            raise UnsafePlanDataError("candidate stock name is malformed")
        primary_mode = candidate.get("primary_mode_key")
        if not isinstance(primary_mode, str) or not primary_mode.strip():
            raise UnsafePlanDataError("candidate primary mode is malformed")
        rank = candidate.get("rank")
        if isinstance(rank, bool) or not isinstance(rank, int) or rank < 1:
            raise UnsafePlanDataError("candidate rank is malformed")
        if rank in ranks:
            raise UnsafePlanDataError("candidate ranks are not unique")
        ranks.add(rank)
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
        risk_level = candidate.get("risk_level")
        entry_trigger = candidate.get("entry_trigger_json")
        _validate_trigger(entry_trigger, "entry trigger")
        if "reference_price" not in entry_trigger:
            raise UnsafePlanDataError(
                "candidate entry reference price is missing"
            )
        try:
            expected_position, expected_stop = materialize_candidate_risk(
                entry_trigger["reference_price"],
                risk_level,
                risk,
            )
        except ValueError as exc:
            raise UnsafePlanDataError(str(exc)) from exc
        if position != expected_position:
            raise UnsafePlanDataError(
                "candidate position does not match its risk level"
            )
        invalidation = candidate.get("invalidation_json")
        _validate_trigger(invalidation, "invalidation trigger")
        if "price_lte" not in invalidation:
            raise UnsafePlanDataError("candidate hard stop price is missing")
        actual_stop = _number(
            invalidation["price_lte"],
            "candidate hard stop price is malformed",
        )
        if actual_stop != expected_stop:
            raise UnsafePlanDataError(
                "candidate hard stop does not match plan risk settings"
            )
        _validate_trigger(candidate.get("exit_trigger_json"), "exit trigger")


def normalize_plan_payload(payload: Mapping[str, Any]) -> ValidatedPlanPayload:
    """Validate operational numerics and normalize audit-only JSON."""
    detached = copy.deepcopy(dict(payload))
    _validate_operational_payload(detached)
    return ValidatedPlanPayload(json_value(detached))


def normalize_settings_payload(value: Any) -> ValidatedSettingsPayload:
    """Build a strict, detached singleton-settings response snapshot."""
    if isinstance(value, Mapping):
        source = dict(value)
    else:
        fields = (
            "id",
            "enabled",
            "trial_position_pct",
            "confirmed_position_pct",
            "hard_stop_pct",
            "max_action_candidates",
            "in_app_enabled",
            "wechat_enabled",
            "updated_at",
        )
        try:
            source = {field: getattr(value, field) for field in fields}
        except (AttributeError, TypeError) as exc:
            raise UnsafePlanDataError("settings payload is malformed") from exc

    if source.get("id") != 1 or isinstance(source.get("id"), bool):
        raise UnsafePlanDataError("settings singleton identity is malformed")
    for field in ("enabled", "in_app_enabled", "wechat_enabled"):
        if type(source.get(field)) is not bool:
            raise UnsafePlanDataError(f"settings {field} is malformed")
    if source["wechat_enabled"] is not False:
        raise UnsafePlanDataError("wechat is disabled in v1")
    risk = _validate_risk_settings(
        {
            "trial": source.get("trial_position_pct"),
            "confirmed": source.get("confirmed_position_pct"),
            "hard_stop": source.get("hard_stop_pct"),
            "max_candidates": source.get("max_action_candidates"),
        }
    )
    updated_at = source.get("updated_at")
    if isinstance(updated_at, str):
        try:
            updated_at = datetime.fromisoformat(updated_at)
        except ValueError as exc:
            raise UnsafePlanDataError("settings update time is malformed") from exc
    if not isinstance(updated_at, datetime):
        raise UnsafePlanDataError("settings update time is malformed")
    return ValidatedSettingsPayload(
        {
            "id": 1,
            "enabled": source["enabled"],
            "trial_position_pct": risk["trial"],
            "confirmed_position_pct": risk["confirmed"],
            "hard_stop_pct": risk["hard_stop"],
            "max_action_candidates": risk["max_candidates"],
            "in_app_enabled": source["in_app_enabled"],
            "wechat_enabled": False,
            "updated_at": china_iso(updated_at),
        }
    )


__all__ = [
    "ValidatedPlanPayload",
    "ValidatedSettingsPayload",
    "china_iso",
    "json_value",
    "materialize_candidate_risk",
    "normalize_plan_payload",
    "normalize_settings_payload",
]
