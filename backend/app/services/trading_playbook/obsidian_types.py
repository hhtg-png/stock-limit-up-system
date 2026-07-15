"""Strict snapshot contracts for Obsidian trading-playbook exports."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import TypeAlias

from app.utils.time_utils import CN_TZ


OBSIDIAN_EXPORT_STATUSES = ("pending", "written", "paused", "failed", "superseded")
OBSIDIAN_ENTITY_TYPES = ("rule", "plan", "review", "alerts", "daily_index", "dashboard")
OBSIDIAN_PHASES = (
    "catalog",
    "preclose",
    "initial_review",
    "after_close",
    "final_review",
    "overnight",
    "auction",
    "reconcile",
)
TRADING_PLAYBOOK_ALLOWED_ROOTS = (
    "30_TradingPlaybook/Modes/Auto",
    "30_TradingPlaybook/Daily/Auto",
    "30_TradingPlaybook/Reviews/Auto",
    "30_TradingPlaybook/Alerts/Auto",
    "Dashboards/交易预案.md",
)


CanonicalScalar: TypeAlias = str | bool | int | float | Decimal | date | datetime | None
CanonicalValue: TypeAlias = (
    CanonicalScalar
    | Mapping[str, "CanonicalValue"]
    | list["CanonicalValue"]
    | tuple["CanonicalValue", ...]
)
CanonicalMapping: TypeAlias = Mapping[str, CanonicalValue]

JSONScalar: TypeAlias = str | bool | int | float | None
JSONValue: TypeAlias = (
    JSONScalar | dict[str, "JSONValue"] | list["JSONValue"]
)
JSONMapping: TypeAlias = dict[str, JSONValue]


def _decimal_string(value: Decimal) -> str:
    if not value.is_finite():
        raise ValueError("canonical JSON decimal values must be finite")
    if value.is_zero():
        return "0"
    fixed_point = format(value, "f")
    if "." in fixed_point:
        fixed_point = fixed_point.rstrip("0").rstrip(".")
    return fixed_point


def _canonical_value(value: object) -> JSONValue:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("canonical JSON float values must be finite")
        return value
    if isinstance(value, Decimal):
        return _decimal_string(value)
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("canonical JSON datetime values must be timezone-aware")
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        normalized: dict[str, JSONValue] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("canonical JSON dict keys must be strings")
            normalized[key] = _canonical_value(item)
        return normalized
    if isinstance(value, (list, tuple)):
        return [_canonical_value(item) for item in value]
    raise TypeError(
        f"unsupported canonical JSON type: {type(value).__name__}"
    )


def canonical_json_bytes(value: CanonicalValue) -> bytes:
    """Return stable UTF-8 JSON bytes for a strict canonical input value."""

    normalized = _canonical_value(value)
    return json.dumps(
        normalized,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def database_datetime_to_cn(value: datetime | None) -> datetime | None:
    """Adapt ORM datetimes to an explicit timezone-aware China timestamp."""

    if value is None:
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        return CN_TZ.localize(value)
    return value.astimezone(CN_TZ)


@dataclass(frozen=True)
class ObsidianArtifact:
    snapshot_key: str
    trade_date: date
    entity_type: str
    entity_id: int | None
    phase: str
    target_path: str
    immutable: bool
    payload: CanonicalMapping

    def __post_init__(self) -> None:
        if not isinstance(self.snapshot_key, str) or not self.snapshot_key.strip():
            raise ValueError("snapshot_key must be nonempty")
        if not isinstance(self.target_path, str) or not self.target_path.strip():
            raise ValueError("target_path must be nonempty")
        if self.entity_type not in OBSIDIAN_ENTITY_TYPES:
            raise ValueError(f"entity_type must be one of {OBSIDIAN_ENTITY_TYPES}")
        if self.phase not in OBSIDIAN_PHASES:
            raise ValueError(f"phase must be one of {OBSIDIAN_PHASES}")
        canonical_json_bytes(self.payload)

    @property
    def source_hash(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self.payload)).hexdigest()


@dataclass(frozen=True)
class ObsidianSyncBatchResult:
    trade_date: date
    phase: str
    written_files: tuple[str, ...]
    skipped_files: tuple[str, ...]
    pending_files: tuple[str, ...]
    failed_files: tuple[str, ...]
    git_status: JSONMapping

    def __post_init__(self) -> None:
        if self.phase not in OBSIDIAN_PHASES:
            raise ValueError(f"phase must be one of {OBSIDIAN_PHASES}")


__all__ = (
    "CanonicalMapping",
    "CanonicalScalar",
    "CanonicalValue",
    "JSONMapping",
    "JSONScalar",
    "JSONValue",
    "OBSIDIAN_ENTITY_TYPES",
    "OBSIDIAN_EXPORT_STATUSES",
    "OBSIDIAN_PHASES",
    "TRADING_PLAYBOOK_ALLOWED_ROOTS",
    "ObsidianArtifact",
    "ObsidianSyncBatchResult",
    "canonical_json_bytes",
    "database_datetime_to_cn",
)
