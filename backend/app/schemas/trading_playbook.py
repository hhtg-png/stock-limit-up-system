"""Strict request contracts for the standalone trading playbook API."""

from __future__ import annotations

import math
from datetime import date, datetime
from typing import Annotated, Literal, Optional

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    StrictBool,
    StrictInt,
    StringConstraints,
    model_validator,
)

from app.utils.time_utils import CN_TZ, is_trading_time


PlanStage = Literal["preclose", "after_close", "overnight", "auction"]
PlanStatus = Literal["draft", "confirmed", "active", "superseded", "expired"]
RiskLevel = Literal["avoid", "watch", "trial", "confirmed"]
AlertEventType = Literal[
    "plan_ready",
    "confirmation_required",
    "watch",
    "entry_triggered",
    "confirmation_triggered",
    "invalidated",
    "risk_warning",
    "exit_triggered",
    "review_ready",
]

NonEmptyText = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1),
]
FiniteNumber = Annotated[float, Field(strict=True, allow_inf_nan=False)]
PositivePrice = Annotated[
    float,
    Field(strict=True, gt=0, allow_inf_nan=False),
]
Percent = Annotated[
    float,
    Field(strict=True, ge=-100, le=100, allow_inf_nan=False),
]


def _parse_iso_date(value):
    if isinstance(value, datetime):
        raise ValueError("date must not include a time")
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        raise ValueError("date must be an ISO date string")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("date must be an ISO date string") from exc
    if value != parsed.isoformat():
        raise ValueError("date must use canonical YYYY-MM-DD format")
    return parsed


def _replace_nonfinite(value):
    if isinstance(value, float) and not math.isfinite(value):
        return "non-finite-number"
    if isinstance(value, dict):
        return {key: _replace_nonfinite(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_replace_nonfinite(item) for item in value]
    return value


def _parse_aware_datetime(value):
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("datetime must use ISO 8601 format") from exc
    else:
        raise ValueError("datetime must use ISO 8601 format")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware")
    return parsed


JsonDate = Annotated[date, BeforeValidator(_parse_iso_date)]
JsonAwareDatetime = Annotated[
    datetime,
    BeforeValidator(_parse_aware_datetime),
]


class StrictRequest(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        strict=True,
        str_strip_whitespace=True,
        allow_inf_nan=False,
    )

    @model_validator(mode="before")
    @classmethod
    def make_nonfinite_validation_errors_json_safe(cls, value):
        return _replace_nonfinite(value)


class PlanGenerateRequest(StrictRequest):
    source_trade_date: JsonDate
    stage: PlanStage


class PlanConfirmRequest(StrictRequest):
    confirmed_by: Annotated[NonEmptyText, StringConstraints(max_length=80)]


class TriggerOverride(StrictRequest):
    label: Optional[
        Annotated[str, StringConstraints(strip_whitespace=True, max_length=500)]
    ] = None
    reference_price: Optional[PositivePrice] = None
    price_gte: Optional[PositivePrice] = None
    price_lte: Optional[PositivePrice] = None
    change_pct_gte: Optional[Percent] = None
    change_pct_lte: Optional[Percent] = None
    sealed: Optional[StrictBool] = None
    open_count_gte: Optional[Annotated[StrictInt, Field(ge=0)]] = None

    @model_validator(mode="after")
    def reject_explicit_nulls(self):
        for field_name in self.model_fields_set:
            if getattr(self, field_name) is None:
                raise ValueError(f"{field_name} must not be null")
        return self


class CandidateOverride(StrictRequest):
    candidate_id: Optional[Annotated[StrictInt, Field(gt=0)]] = None
    stock_code: Optional[
        Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=10)]
    ] = None
    primary_mode_key: Optional[
        Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=80)]
    ] = None
    action_trade_date: Optional[JsonDate] = None
    entry_trigger: Optional[TriggerOverride] = None
    invalidation: Optional[TriggerOverride] = None
    exit_trigger: Optional[TriggerOverride] = None
    manual_note: Optional[
        Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=500)]
    ] = None

    @model_validator(mode="after")
    def validate_locator(self):
        by_id = self.candidate_id is not None
        by_identity = self.stock_code is not None or self.primary_mode_key is not None
        if by_id == by_identity:
            raise ValueError(
                "use either candidate_id or stock_code with primary_mode_key"
            )
        if by_identity and (
            self.stock_code is None or self.primary_mode_key is None
        ):
            raise ValueError("stock_code and primary_mode_key are both required")
        return self


class PlanRevisionRequest(StrictRequest):
    change_note: Annotated[NonEmptyText, StringConstraints(max_length=500)]
    candidate_overrides: list[CandidateOverride] = Field(
        default_factory=list,
        max_length=3,
    )


class ManualExecutionEntry(StrictRequest):
    executed: StrictBool
    execution_price: Optional[PositivePrice] = None
    quantity: Optional[Annotated[StrictInt, Field(gt=0)]] = None
    executed_at: Optional[JsonAwareDatetime] = None
    manual_note: Optional[
        Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=500)]
    ] = None

    @model_validator(mode="after")
    def reject_explicit_nulls(self):
        for field_name in self.model_fields_set - {"executed"}:
            if getattr(self, field_name) is None:
                raise ValueError(f"{field_name} must not be null")
        if self.executed_at is not None and (
            self.executed_at.tzinfo is None
            or self.executed_at.utcoffset() is None
        ):
            raise ValueError("executed_at must be timezone-aware")
        if self.executed_at is not None and not is_trading_time(
            self.executed_at.astimezone(CN_TZ)
        ):
            raise ValueError("executed_at must be in a continuous trading session")
        if not self.executed and any(
            value is not None
            for value in (
                self.execution_price,
                self.quantity,
                self.executed_at,
            )
        ):
            raise ValueError(
                "unexecuted entries must not contain execution facts"
            )
        return self


CandidateIdKey = Annotated[str, StringConstraints(pattern=r"^[1-9][0-9]*$")]


class UnplannedExecutionEntry(ManualExecutionEntry):
    executed: Literal[True]
    stock_code: Annotated[str, StringConstraints(pattern=r"^[0-9]{6}$")]
    stock_name: Annotated[NonEmptyText, StringConstraints(max_length=50)]


class ManualExecutionUpdate(StrictRequest):
    executions: dict[CandidateIdKey, ManualExecutionEntry] = Field(
        default_factory=dict,
        max_length=100,
    )
    unplanned_executions: list[UnplannedExecutionEntry] = Field(
        default_factory=list,
        max_length=100,
    )


class TradingPlaybookSettingsUpdate(StrictRequest):
    enabled: Optional[StrictBool] = None
    trial_position_pct: Optional[
        Annotated[float, Field(strict=True, ge=0, le=100, allow_inf_nan=False)]
    ] = None
    confirmed_position_pct: Optional[
        Annotated[float, Field(strict=True, ge=0, le=100, allow_inf_nan=False)]
    ] = None
    hard_stop_pct: Optional[
        Annotated[float, Field(strict=True, gt=0, le=20, allow_inf_nan=False)]
    ] = None
    max_action_candidates: Optional[Annotated[StrictInt, Field(ge=1, le=3)]] = None
    in_app_enabled: Optional[StrictBool] = None
    wechat_enabled: Optional[Literal[False]] = None

    @model_validator(mode="after")
    def validate_patch(self):
        if not self.model_fields_set:
            raise ValueError("at least one setting is required")
        for field_name in self.model_fields_set:
            if getattr(self, field_name) is None:
                raise ValueError(f"{field_name} must not be null")
        if (
            self.trial_position_pct is not None
            and self.confirmed_position_pct is not None
            and self.trial_position_pct > self.confirmed_position_pct
        ):
            raise ValueError(
                "trial_position_pct must not exceed confirmed_position_pct"
            )
        return self


__all__ = [
    "AlertEventType",
    "CandidateOverride",
    "ManualExecutionEntry",
    "ManualExecutionUpdate",
    "UnplannedExecutionEntry",
    "PlanConfirmRequest",
    "PlanGenerateRequest",
    "PlanRevisionRequest",
    "PlanStage",
    "PlanStatus",
    "RiskLevel",
    "TradingPlaybookSettingsUpdate",
    "TriggerOverride",
]
