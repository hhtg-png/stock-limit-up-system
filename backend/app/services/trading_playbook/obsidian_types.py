"""Strict snapshot contracts for Obsidian trading-playbook exports."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterator, Mapping
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
    | dict[str, "CanonicalValue"]
    | list["CanonicalValue"]
    | tuple["CanonicalValue", ...]
)
CanonicalMapping: TypeAlias = dict[str, CanonicalValue]
ReadOnlyCanonicalValue: TypeAlias = (
    CanonicalScalar
    | Mapping[str, "ReadOnlyCanonicalValue"]
    | tuple["ReadOnlyCanonicalValue", ...]
)
ReadOnlyCanonicalMapping: TypeAlias = Mapping[str, ReadOnlyCanonicalValue]

JSONScalar: TypeAlias = str | bool | int | float | None
JSONValue: TypeAlias = (
    JSONScalar | dict[str, "JSONValue"] | list["JSONValue"]
)
JSONMapping: TypeAlias = dict[str, JSONValue]
ReadOnlyJSONValue: TypeAlias = (
    JSONScalar
    | Mapping[str, "ReadOnlyJSONValue"]
    | tuple["ReadOnlyJSONValue", ...]
)
ReadOnlyJSONMapping: TypeAlias = Mapping[str, ReadOnlyJSONValue]


_MAX_NESTING_DEPTH = 64


class _FrozenDict(Mapping[str, object]):
    """A small immutable mapping backed only by immutable tuples."""

    __slots__ = ("__items",)

    def __init__(self, items: tuple[tuple[str, object], ...]) -> None:
        object.__setattr__(
            self,
            "_FrozenDict__items",
            tuple((key, value) for key, value in items),
        )

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError("_FrozenDict is immutable")

    def __getitem__(self, key: str) -> object:
        for item_key, value in self.__items:
            if item_key == key:
                return value
        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        return (key for key, _ in self.__items)

    def __len__(self) -> int:
        return len(self.__items)

    def __deepcopy__(self, memo: dict[int, object]) -> JSONMapping:
        plain = _canonical_value(self)
        assert type(plain) is dict
        memo[id(self)] = plain
        return plain

    def _items(self) -> tuple[tuple[str, object], ...]:
        return self.__items


def _decimal_string(value: Decimal) -> str:
    if not value.is_finite():
        raise ValueError("canonical JSON decimal values must be finite")
    if value.is_zero():
        return "0"
    fixed_point = format(value, "f")
    if "." in fixed_point:
        fixed_point = fixed_point.rstrip("0").rstrip(".")
    return fixed_point


def _container_depth(depth: int) -> int:
    next_depth = depth + 1
    if next_depth > _MAX_NESTING_DEPTH:
        raise ValueError(
            f"maximum nesting depth {_MAX_NESTING_DEPTH} exceeded"
        )
    return next_depth


def _canonical_value(
    value: object,
    *,
    depth: int = 0,
    active_ids: set[int] | None = None,
) -> JSONValue:
    if active_ids is None:
        active_ids = set()
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("canonical JSON float values must be finite")
        if value == 0.0:
            return 0.0
        return value
    if isinstance(value, Decimal):
        return _decimal_string(value)
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("canonical JSON datetime values must be timezone-aware")
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if type(value) is dict or type(value) is _FrozenDict:
        container_depth = _container_depth(depth)
        identity = id(value)
        if identity in active_ids:
            raise ValueError("canonical JSON cycle detected")
        active_ids.add(identity)
        try:
            items = value.items() if type(value) is dict else value._items()
            normalized: dict[str, JSONValue] = {}
            for key, item in items:
                if not isinstance(key, str):
                    raise TypeError("canonical JSON dict keys must be strings")
                normalized[key] = _canonical_value(
                    item,
                    depth=container_depth,
                    active_ids=active_ids,
                )
            return normalized
        finally:
            active_ids.remove(identity)
    if type(value) in (list, tuple):
        container_depth = _container_depth(depth)
        identity = id(value)
        if identity in active_ids:
            raise ValueError("canonical JSON cycle detected")
        active_ids.add(identity)
        try:
            return [
                _canonical_value(
                    item,
                    depth=container_depth,
                    active_ids=active_ids,
                )
                for item in value
            ]
        finally:
            active_ids.remove(identity)
    raise TypeError(
        f"unsupported canonical JSON type: {type(value).__name__}"
    )


def _freeze_canonical_value(
    value: object,
    *,
    depth: int = 0,
    active_ids: set[int] | None = None,
) -> object:
    if active_ids is None:
        active_ids = set()
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("canonical JSON float values must be finite")
        return value
    if isinstance(value, Decimal):
        _decimal_string(value)
        return value
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("canonical JSON datetime values must be timezone-aware")
        normalized = value.astimezone(timezone.utc)
        return datetime(
            normalized.year,
            normalized.month,
            normalized.day,
            normalized.hour,
            normalized.minute,
            normalized.second,
            normalized.microsecond,
            tzinfo=timezone.utc,
            fold=normalized.fold,
        )
    if isinstance(value, date):
        return value
    if type(value) is dict or type(value) is _FrozenDict:
        container_depth = _container_depth(depth)
        identity = id(value)
        if identity in active_ids:
            raise ValueError("canonical JSON cycle detected")
        active_ids.add(identity)
        try:
            items = value.items() if type(value) is dict else value._items()
            frozen_items: list[tuple[str, object]] = []
            for key, item in items:
                if not isinstance(key, str):
                    raise TypeError("canonical JSON dict keys must be strings")
                frozen_items.append(
                    (
                        key,
                        _freeze_canonical_value(
                            item,
                            depth=container_depth,
                            active_ids=active_ids,
                        ),
                    )
                )
            return _FrozenDict(tuple(frozen_items))
        finally:
            active_ids.remove(identity)
    if type(value) in (list, tuple):
        container_depth = _container_depth(depth)
        identity = id(value)
        if identity in active_ids:
            raise ValueError("canonical JSON cycle detected")
        active_ids.add(identity)
        try:
            return tuple(
                _freeze_canonical_value(
                    item,
                    depth=container_depth,
                    active_ids=active_ids,
                )
                for item in value
            )
        finally:
            active_ids.remove(identity)
    raise TypeError(
        f"unsupported canonical JSON type: {type(value).__name__}"
    )


def _freeze_json_value(
    value: object,
    *,
    depth: int = 0,
    active_ids: set[int] | None = None,
) -> object:
    if active_ids is None:
        active_ids = set()
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("JSON float values must be finite")
        return value
    if type(value) is dict:
        container_depth = _container_depth(depth)
        identity = id(value)
        if identity in active_ids:
            raise ValueError("JSON cycle detected")
        active_ids.add(identity)
        try:
            frozen_items: list[tuple[str, object]] = []
            for key, item in value.items():
                if not isinstance(key, str):
                    raise TypeError("JSON dict keys must be strings")
                frozen_items.append(
                    (
                        key,
                        _freeze_json_value(
                            item,
                            depth=container_depth,
                            active_ids=active_ids,
                        ),
                    )
                )
            return _FrozenDict(tuple(frozen_items))
        finally:
            active_ids.remove(identity)
    if type(value) is list:
        container_depth = _container_depth(depth)
        identity = id(value)
        if identity in active_ids:
            raise ValueError("JSON cycle detected")
        active_ids.add(identity)
        try:
            return tuple(
                _freeze_json_value(
                    item,
                    depth=container_depth,
                    active_ids=active_ids,
                )
                for item in value
            )
        finally:
            active_ids.remove(identity)
    raise TypeError(f"unsupported JSON value type: {type(value).__name__}")


def _plain_json_value(value: object) -> JSONValue:
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    if type(value) is _FrozenDict:
        return {key: _plain_json_value(item) for key, item in value._items()}
    if type(value) is tuple:
        return [_plain_json_value(item) for item in value]
    raise TypeError(f"unsupported frozen JSON value type: {type(value).__name__}")


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
    return value.astimezone(timezone.utc).astimezone(CN_TZ)


@dataclass(frozen=True, init=False)
class ObsidianArtifact:
    snapshot_key: str
    trade_date: date
    entity_type: str
    entity_id: int | None
    phase: str
    target_path: str
    immutable: bool
    payload: ReadOnlyCanonicalMapping

    def __init__(
        self,
        snapshot_key: str,
        trade_date: date,
        entity_type: str,
        entity_id: int | None,
        phase: str,
        target_path: str,
        immutable: bool,
        payload: CanonicalMapping,
    ) -> None:
        object.__setattr__(self, "snapshot_key", snapshot_key)
        object.__setattr__(self, "trade_date", trade_date)
        object.__setattr__(self, "entity_type", entity_type)
        object.__setattr__(self, "entity_id", entity_id)
        object.__setattr__(self, "phase", phase)
        object.__setattr__(self, "target_path", target_path)
        object.__setattr__(self, "immutable", immutable)
        object.__setattr__(self, "payload", payload)
        self.__post_init__()

    def __post_init__(self) -> None:
        if not isinstance(self.snapshot_key, str) or not self.snapshot_key.strip():
            raise ValueError("snapshot_key must be nonempty")
        if not isinstance(self.target_path, str) or not self.target_path.strip():
            raise ValueError("target_path must be nonempty")
        if self.entity_type not in OBSIDIAN_ENTITY_TYPES:
            raise ValueError(f"entity_type must be one of {OBSIDIAN_ENTITY_TYPES}")
        if self.phase not in OBSIDIAN_PHASES:
            raise ValueError(f"phase must be one of {OBSIDIAN_PHASES}")
        if type(self.payload) not in (dict, _FrozenDict):
            raise TypeError("payload must be a dict")
        frozen_payload = (
            self.payload
            if type(self.payload) is _FrozenDict
            else _freeze_canonical_value(self.payload)
        )
        object.__setattr__(self, "payload", frozen_payload)
        object.__setattr__(
            self,
            "_source_hash",
            hashlib.sha256(canonical_json_bytes(frozen_payload)).hexdigest(),
        )

    @property
    def source_hash(self) -> str:
        return self._source_hash  # type: ignore[attr-defined,no-any-return]

    def payload_json(self) -> JSONMapping:
        plain = _canonical_value(self.payload)
        assert type(plain) is dict
        return plain


@dataclass(frozen=True, init=False)
class ObsidianSyncBatchResult:
    trade_date: date
    phase: str
    written_files: tuple[str, ...]
    skipped_files: tuple[str, ...]
    pending_files: tuple[str, ...]
    failed_files: tuple[str, ...]
    git_status: ReadOnlyJSONMapping

    def __init__(
        self,
        trade_date: date,
        phase: str,
        written_files: tuple[str, ...],
        skipped_files: tuple[str, ...],
        pending_files: tuple[str, ...],
        failed_files: tuple[str, ...],
        git_status: JSONMapping,
    ) -> None:
        object.__setattr__(self, "trade_date", trade_date)
        object.__setattr__(self, "phase", phase)
        object.__setattr__(self, "written_files", written_files)
        object.__setattr__(self, "skipped_files", skipped_files)
        object.__setattr__(self, "pending_files", pending_files)
        object.__setattr__(self, "failed_files", failed_files)
        object.__setattr__(self, "git_status", git_status)
        self.__post_init__()

    def __post_init__(self) -> None:
        if self.phase not in OBSIDIAN_PHASES:
            raise ValueError(f"phase must be one of {OBSIDIAN_PHASES}")
        for field_name in (
            "written_files",
            "skipped_files",
            "pending_files",
            "failed_files",
        ):
            value = getattr(self, field_name)
            if type(value) is not tuple or not all(
                type(item) is str for item in value
            ):
                raise TypeError(f"{field_name} must be a tuple of strings")
        if type(self.git_status) not in (dict, _FrozenDict):
            raise TypeError("git_status must be a dict")
        object.__setattr__(
            self,
            "git_status",
            self.git_status
            if type(self.git_status) is _FrozenDict
            else _freeze_json_value(self.git_status),
        )

    def git_status_json(self) -> JSONMapping:
        plain = _plain_json_value(self.git_status)
        assert type(plain) is dict
        return plain


__all__ = (
    "CanonicalMapping",
    "CanonicalScalar",
    "CanonicalValue",
    "JSONMapping",
    "JSONScalar",
    "JSONValue",
    "ReadOnlyCanonicalMapping",
    "ReadOnlyCanonicalValue",
    "ReadOnlyJSONMapping",
    "ReadOnlyJSONValue",
    "OBSIDIAN_ENTITY_TYPES",
    "OBSIDIAN_EXPORT_STATUSES",
    "OBSIDIAN_PHASES",
    "TRADING_PLAYBOOK_ALLOWED_ROOTS",
    "ObsidianArtifact",
    "ObsidianSyncBatchResult",
    "canonical_json_bytes",
    "database_datetime_to_cn",
)
