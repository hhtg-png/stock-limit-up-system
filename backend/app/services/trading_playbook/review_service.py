"""End-of-day execution review and final market-fact reconciliation."""

from __future__ import annotations

import asyncio
import copy
import inspect
import math
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from typing import Any, Callable, Optional

from sqlalchemy import and_, exists, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import OperationalError

from app.models.market_review import MarketReviewStockDaily
from app.models.trading_playbook import (
    TradingAlertEvent,
    TradingExecutionReview,
    TradingPlanCandidate,
    TradingPlanVersion,
)
from app.utils.time_utils import CN_TZ, now_cn

from .errors import (
    InvalidRequestError,
    InvalidTransitionError,
    PlaybookNotFoundError,
)


_TRIGGER_EVENT_TYPES = ("entry_triggered", "confirmation_triggered")
_MAX_WRITE_ATTEMPTS = 4


def _value(row: Any, key: str, default: Any = None) -> Any:
    if isinstance(row, Mapping):
        return row.get(key, default)
    return getattr(row, key, default)


def _json_value(value: Any, *, path: str = "value") -> Any:
    """Detach a strict JSON value and preserve date/time audit evidence."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise InvalidRequestError(f"{path} must be finite")
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key in sorted(value, key=lambda item: str(item)):
            if not isinstance(key, str):
                raise InvalidRequestError(f"{path} keys must be strings")
            result[key] = _json_value(value[key], path=f"{path}.{key}")
        return result
    if isinstance(value, (list, tuple)):
        return [
            _json_value(item, path=f"{path}[{index}]")
            for index, item in enumerate(value)
        ]
    raise InvalidRequestError(
        f"{path} contains unsupported {type(value).__name__}"
    )


def _db_datetime(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError("review clock must return a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        return value
    return value.astimezone(CN_TZ).replace(tzinfo=None)


def _finite_optional(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


class TradingPlaybookReviewService:
    """Build immutable-plan execution reviews without inferring account P&L."""

    def __init__(
        self,
        *,
        now_provider: Callable[[], datetime] = now_cn,
        outcome_loader: Optional[Callable[..., Any]] = None,
    ) -> None:
        self._now_provider = now_provider
        self._outcome_loader = outcome_loader or self.load_outcomes

    @classmethod
    def summarize(
        cls,
        candidates: Sequence[Any],
        events: Sequence[Any],
        manual_execution: Mapping[str, Any],
        outcomes: Optional[Mapping[str, Mapping[str, Any]]] = None,
    ) -> dict[str, Any]:
        """Classify signal and manual execution state using plan-local facts."""
        candidate_rows = list(candidates or [])
        event_rows = list(events or [])
        manual = manual_execution if isinstance(manual_execution, Mapping) else {}
        outcome_by_code = outcomes if isinstance(outcomes, Mapping) else {}

        event_types: dict[int, list[str]] = {}
        event_audit: dict[int, list[dict[str, Any]]] = {}
        for event in event_rows:
            candidate_id = _value(event, "candidate_id")
            event_type = _value(event, "event_type")
            if isinstance(candidate_id, bool) or not isinstance(candidate_id, int):
                continue
            if not isinstance(event_type, str) or not event_type:
                continue
            types = event_types.setdefault(candidate_id, [])
            if event_type not in types:
                types.append(event_type)
            event_audit.setdefault(candidate_id, []).append(
                {
                    "event_type": event_type,
                    "triggered_at": _json_value(
                        _value(event, "triggered_at")
                    ),
                    "acknowledged_at": _json_value(
                        _value(event, "acknowledged_at")
                    ),
                    "channel_status": _json_value(
                        _value(event, "channel_status_json", {}) or {}
                    ),
                }
            )

        not_triggered: list[str] = []
        invalidated: list[str] = []
        triggered_executed: list[str] = []
        triggered_not_executed: list[str] = []
        signal_outcomes: list[dict[str, Any]] = []
        candidate_ids: set[int] = set()
        planned_executed = 0

        for candidate in candidate_rows:
            candidate_id = _value(candidate, "id")
            stock_code = str(_value(candidate, "stock_code", "") or "")
            status = str(_value(candidate, "status", "waiting") or "waiting")
            if isinstance(candidate_id, bool) or not isinstance(candidate_id, int):
                continue
            candidate_ids.add(candidate_id)
            candidate_events = event_types.get(candidate_id, [])
            execution = manual.get(str(candidate_id), {})
            was_executed = (
                isinstance(execution, Mapping)
                and execution.get("executed") is True
            )
            if was_executed:
                planned_executed += 1

            is_invalidated = status == "invalidated"
            is_triggered = status in {"triggered", "exit"} or any(
                event_type in _TRIGGER_EVENT_TYPES
                for event_type in candidate_events
            )
            if is_invalidated:
                invalidated.append(stock_code)
            elif is_triggered:
                target = (
                    triggered_executed
                    if was_executed
                    else triggered_not_executed
                )
                target.append(stock_code)
            else:
                not_triggered.append(stock_code)

            signal_outcome = {
                "candidate_id": candidate_id,
                "stock_code": stock_code,
                "status": status,
                "event_types": list(candidate_events),
                "events": copy.deepcopy(event_audit.get(candidate_id, [])),
            }
            close_fact = outcome_by_code.get(stock_code)
            if isinstance(close_fact, Mapping):
                signal_outcome["close"] = copy.deepcopy(dict(close_fact))
            signal_outcomes.append(signal_outcome)

        unplanned = sum(
            1
            for raw_id, execution in manual.items()
            if (
                isinstance(execution, Mapping)
                and execution.get("executed") is True
                and (
                    not isinstance(raw_id, str)
                    or not raw_id.isdigit()
                    or int(raw_id) not in candidate_ids
                )
            )
        )
        return {
            "not_triggered": not_triggered,
            "invalidated": invalidated,
            "triggered_executed": triggered_executed,
            "triggered_not_executed": triggered_not_executed,
            "plan_compliance": {
                "planned": len(candidate_ids),
                "executed": planned_executed,
                "unplanned": unplanned,
            },
            "signal_outcomes": signal_outcomes,
        }

    async def build(
        self,
        db,
        trade_date: date,
        finalized: bool = False,
    ) -> list[TradingExecutionReview]:
        """Create preliminary rows or reconcile final facts into those rows."""
        if isinstance(trade_date, datetime) or not isinstance(trade_date, date):
            raise InvalidRequestError("trade_date must be a date")
        if not isinstance(finalized, bool):
            raise InvalidRequestError("finalized must be a boolean")
        plan_ids = await self._relevant_plan_ids(db, trade_date)
        rows: list[TradingExecutionReview] = []
        for plan_id in plan_ids:
            row = await self._ensure_review(db, trade_date, plan_id)
            rows.append(
                await self._reconcile_review(
                    db,
                    row.id,
                    trade_date,
                    plan_id,
                    finalized=finalized,
                )
            )
        return rows

    async def update_manual_execution(
        self,
        db,
        trade_date: date,
        executions: Mapping[str, Mapping[str, Any]],
    ) -> TradingExecutionReview:
        """Replace one unambiguously selected review's manual execution map."""
        if isinstance(trade_date, datetime) or not isinstance(trade_date, date):
            raise InvalidRequestError("trade_date must be a date")
        manual = self._normalize_manual_execution(executions)
        review_id, plan_id = await self._select_manual_review(
            db,
            trade_date,
            set(manual),
        )
        for attempt in range(_MAX_WRITE_ATTEMPTS):
            try:
                uses_row_lock = db.get_bind().dialect.name == "postgresql"
                row = await self._fresh_review(
                    db,
                    review_id,
                    for_update=uses_row_lock,
                )
                if row is None:
                    raise PlaybookNotFoundError("review not found")
                old_manual = copy.deepcopy(row.manual_execution_json or {})
                candidates, events = await self._review_inputs(
                    db,
                    plan_id,
                    trade_date,
                )
                outcomes = copy.deepcopy(row.outcome_snapshot_json or {})
                summary = self.summarize(
                    candidates,
                    events,
                    manual,
                    outcomes,
                )
                predicates = [TradingExecutionReview.id == review_id]
                if not uses_row_lock:
                    predicates.append(
                        TradingExecutionReview.manual_execution_json
                        == old_manual
                    )
                changed = await db.execute(
                    update(TradingExecutionReview)
                    .where(*predicates)
                    .values(
                        manual_execution_json=copy.deepcopy(manual),
                        signal_review_json=copy.deepcopy(summary),
                        plan_compliance_json=copy.deepcopy(
                            summary["plan_compliance"]
                        ),
                    )
                    .execution_options(synchronize_session=False)
                )
                if changed.rowcount == 1:
                    await db.commit()
                    refreshed = await self._fresh_review(db, review_id)
                    if refreshed is None:
                        raise PlaybookNotFoundError("review not found")
                    return refreshed
                await db.rollback()
            except OperationalError:
                await db.rollback()
                if attempt + 1 >= _MAX_WRITE_ATTEMPTS:
                    raise
                await asyncio.sleep(0)
        raise InvalidTransitionError("review changed concurrently")

    @staticmethod
    def review_insert_statement(
        dialect_name: str,
        *,
        trade_date: date,
        plan_version_id: int,
        generated_at: datetime,
    ):
        values = {
            "trade_date": trade_date,
            "plan_version_id": plan_version_id,
            "signal_review_json": {},
            "manual_execution_json": {},
            "plan_compliance_json": {},
            "outcome_snapshot_json": {},
            "data_quality_json": {},
            "generated_at": generated_at,
            "finalized_at": None,
        }
        if dialect_name == "postgresql":
            statement = postgresql_insert(TradingExecutionReview)
        elif dialect_name == "sqlite":
            statement = sqlite_insert(TradingExecutionReview)
        else:
            raise RuntimeError(f"unsupported review dialect: {dialect_name}")
        return statement.values(**values).on_conflict_do_nothing(
            index_elements=["trade_date", "plan_version_id"]
        )

    @staticmethod
    def review_select_statement(review_id: int, *, for_update: bool = False):
        statement = select(TradingExecutionReview).where(
            TradingExecutionReview.id == review_id
        )
        return statement.with_for_update() if for_update else statement

    @staticmethod
    async def load_outcomes(
        db,
        trade_date: date,
        stock_codes: Sequence[str],
    ) -> list[MarketReviewStockDaily]:
        codes = sorted({str(code) for code in stock_codes if str(code)})
        if not codes:
            return []
        return list(
            (
                await db.scalars(
                    select(MarketReviewStockDaily)
                    .where(
                        MarketReviewStockDaily.trade_date == trade_date,
                        MarketReviewStockDaily.stock_code.in_(codes),
                    )
                    .order_by(MarketReviewStockDaily.stock_code)
                )
            ).all()
        )

    async def _relevant_plan_ids(self, db, trade_date: date) -> list[int]:
        action_candidate = exists(
            select(TradingPlanCandidate.id).where(
                TradingPlanCandidate.plan_version_id == TradingPlanVersion.id,
                TradingPlanCandidate.action_trade_date == trade_date,
            )
        )
        return list(
            (
                await db.scalars(
                    select(TradingPlanVersion.id)
                    .where(
                        or_(
                            TradingPlanVersion.status.in_(
                                ("active", "superseded")
                            ),
                            and_(
                                TradingPlanVersion.status == "expired",
                                TradingPlanVersion.confirmed_at.is_not(None),
                            ),
                        ),
                        or_(
                            TradingPlanVersion.target_trade_date == trade_date,
                            action_candidate,
                        ),
                    )
                    .order_by(
                        TradingPlanVersion.generated_at,
                        TradingPlanVersion.id,
                    )
                )
            ).all()
        )

    async def _ensure_review(
        self,
        db,
        trade_date: date,
        plan_id: int,
    ) -> TradingExecutionReview:
        now = _db_datetime(self._now_provider())
        dialect = db.get_bind().dialect.name
        for attempt in range(_MAX_WRITE_ATTEMPTS):
            try:
                await db.execute(
                    self.review_insert_statement(
                        dialect,
                        trade_date=trade_date,
                        plan_version_id=plan_id,
                        generated_at=now,
                    )
                )
                await db.commit()
                row = await db.scalar(
                    select(TradingExecutionReview).where(
                        TradingExecutionReview.trade_date == trade_date,
                        TradingExecutionReview.plan_version_id == plan_id,
                    )
                )
                if row is None:
                    raise PlaybookNotFoundError("review insert was not visible")
                return row
            except OperationalError:
                await db.rollback()
                if attempt + 1 >= _MAX_WRITE_ATTEMPTS:
                    raise
                await asyncio.sleep(0)
        raise InvalidTransitionError("review could not be created")

    async def _reconcile_review(
        self,
        db,
        review_id: int,
        trade_date: date,
        plan_id: int,
        *,
        finalized: bool,
    ) -> TradingExecutionReview:
        for attempt in range(_MAX_WRITE_ATTEMPTS):
            try:
                uses_row_lock = db.get_bind().dialect.name == "postgresql"
                row = await self._fresh_review(
                    db,
                    review_id,
                    for_update=uses_row_lock,
                )
                if row is None:
                    raise PlaybookNotFoundError("review not found")
                if not finalized and row.finalized_at is not None:
                    return row
                old_manual = copy.deepcopy(row.manual_execution_json or {})
                candidates, events = await self._review_inputs(
                    db,
                    plan_id,
                    trade_date,
                )
                stock_codes = [candidate.stock_code for candidate in candidates]
                loaded = self._outcome_loader(db, trade_date, stock_codes)
                if inspect.isawaitable(loaded):
                    loaded = await loaded
                outcome_snapshot, data_quality = self._outcome_payload(
                    trade_date,
                    stock_codes,
                    loaded or [],
                    finalized=finalized,
                )
                summary = self.summarize(
                    candidates,
                    events,
                    old_manual,
                    outcome_snapshot,
                )
                predicates = [
                    TradingExecutionReview.id == review_id,
                ]
                if not uses_row_lock:
                    predicates.append(
                        TradingExecutionReview.manual_execution_json
                        == old_manual
                    )
                if not finalized:
                    predicates.append(
                        TradingExecutionReview.finalized_at.is_(None)
                    )
                values: dict[str, Any] = {
                    "signal_review_json": copy.deepcopy(summary),
                    "plan_compliance_json": copy.deepcopy(
                        summary["plan_compliance"]
                    ),
                    "outcome_snapshot_json": copy.deepcopy(outcome_snapshot),
                    "data_quality_json": copy.deepcopy(data_quality),
                }
                if finalized:
                    values["finalized_at"] = func.coalesce(
                        TradingExecutionReview.finalized_at,
                        _db_datetime(self._now_provider()),
                    )
                changed = await db.execute(
                    update(TradingExecutionReview)
                    .where(*predicates)
                    .values(**values)
                    .execution_options(synchronize_session=False)
                )
                if changed.rowcount == 1:
                    await db.commit()
                    refreshed = await self._fresh_review(db, review_id)
                    if refreshed is None:
                        raise PlaybookNotFoundError("review not found")
                    return refreshed
                await db.rollback()
            except OperationalError:
                await db.rollback()
                if attempt + 1 >= _MAX_WRITE_ATTEMPTS:
                    raise
                await asyncio.sleep(0)
        raise InvalidTransitionError("review changed concurrently")

    async def _review_inputs(
        self,
        db,
        plan_id: int,
        trade_date: date,
    ) -> tuple[list[TradingPlanCandidate], list[TradingAlertEvent]]:
        candidates = list(
            (
                await db.scalars(
                    select(TradingPlanCandidate)
                    .where(
                        TradingPlanCandidate.plan_version_id == plan_id,
                        TradingPlanCandidate.action_trade_date == trade_date,
                    )
                    .order_by(
                        TradingPlanCandidate.rank,
                        TradingPlanCandidate.id,
                    )
                )
            ).all()
        )
        candidate_ids = [candidate.id for candidate in candidates]
        if not candidate_ids:
            return candidates, []
        events = list(
            (
                await db.scalars(
                    select(TradingAlertEvent)
                    .where(
                        TradingAlertEvent.plan_version_id == plan_id,
                        TradingAlertEvent.candidate_id.in_(candidate_ids),
                    )
                    .order_by(
                        TradingAlertEvent.triggered_at,
                        TradingAlertEvent.id,
                    )
                )
            ).all()
        )
        return candidates, events

    @classmethod
    def _outcome_payload(
        cls,
        trade_date: date,
        stock_codes: Sequence[str],
        rows: Sequence[Any],
        *,
        finalized: bool,
    ) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
        wanted = sorted({str(code) for code in stock_codes if str(code)})
        outcome: dict[str, dict[str, Any]] = {}
        degraded: list[str] = []
        for row in rows:
            row_date = _value(row, "trade_date")
            stock_code = str(_value(row, "stock_code", "") or "")
            if row_date != trade_date or stock_code not in wanted:
                continue
            quality = str(_value(row, "data_quality_flag", "") or "")
            close_price = _finite_optional(_value(row, "close_price"))
            pre_close = _finite_optional(_value(row, "pre_close"))
            change_pct = _finite_optional(_value(row, "change_pct"))
            raw_open_count = _value(row, "open_count", 0)
            open_count = (
                raw_open_count
                if (
                    not isinstance(raw_open_count, bool)
                    and isinstance(raw_open_count, int)
                    and raw_open_count >= 0
                )
                else None
            )
            if (
                quality != "ok"
                or close_price is None
                or change_pct is None
                or open_count is None
            ):
                degraded.append(stock_code)
            outcome[stock_code] = {
                "trade_date": trade_date.isoformat(),
                "close_price": close_price,
                "pre_close": pre_close,
                "change_pct": change_pct,
                "today_touched_limit_up": bool(
                    _value(row, "today_touched_limit_up", False)
                ),
                "today_sealed_close": bool(
                    _value(row, "today_sealed_close", False)
                ),
                "today_opened_close": bool(
                    _value(row, "today_opened_close", False)
                ),
                "today_broken": bool(_value(row, "today_broken", False)),
                "open_count": open_count,
                "data_quality_flag": quality or "missing",
                "updated_at": _json_value(_value(row, "updated_at")),
            }
        missing = sorted(set(wanted) - set(outcome))
        status = "ready"
        if missing:
            status = "partial"
        elif degraded:
            status = "degraded"
        quality = {
            "status": status,
            "source": "market_review_stock_daily",
            "trade_date": trade_date.isoformat(),
            "finalized": finalized,
            "missing_stock_codes": missing,
            "degraded_stock_codes": sorted(set(degraded)),
        }
        return outcome, quality

    async def _select_manual_review(
        self,
        db,
        trade_date: date,
        execution_ids: set[str],
    ) -> tuple[int, int]:
        reviews = list(
            (
                await db.scalars(
                    select(TradingExecutionReview)
                    .where(TradingExecutionReview.trade_date == trade_date)
                    .order_by(TradingExecutionReview.id)
                )
            ).all()
        )
        if not reviews:
            raise PlaybookNotFoundError("review not found")
        if not execution_ids:
            if len(reviews) == 1:
                return reviews[0].id, reviews[0].plan_version_id
            raise InvalidTransitionError(
                "multiple reviews require candidate ids for selection"
            )

        review_plan_ids = {review.plan_version_id for review in reviews}
        numeric_ids = {int(value) for value in execution_ids}
        candidate_rows = list(
            (
                await db.execute(
                    select(
                        TradingPlanCandidate.id,
                        TradingPlanCandidate.plan_version_id,
                    ).where(
                        TradingPlanCandidate.id.in_(numeric_ids),
                        TradingPlanCandidate.action_trade_date == trade_date,
                    )
                )
            ).all()
        )
        touched_plans = {row.plan_version_id for row in candidate_rows}
        if not touched_plans:
            if len(reviews) == 1:
                return reviews[0].id, reviews[0].plan_version_id
            raise InvalidTransitionError(
                "manual execution does not identify exactly one review"
            )
        if len(touched_plans) != 1:
            raise InvalidTransitionError(
                "manual execution spans multiple plan reviews"
            )
        plan_id = touched_plans.pop()
        if plan_id not in review_plan_ids:
            raise InvalidTransitionError(
                "candidate plan has no review for this trade date"
            )
        selected = next(
            review for review in reviews if review.plan_version_id == plan_id
        )
        return selected.id, selected.plan_version_id

    @staticmethod
    def _normalize_manual_execution(
        executions: Mapping[str, Mapping[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        if not isinstance(executions, Mapping):
            raise InvalidRequestError("executions must be a mapping")
        normalized: dict[str, dict[str, Any]] = {}
        for candidate_id, execution in executions.items():
            if (
                not isinstance(candidate_id, str)
                or not candidate_id.isdigit()
                or candidate_id.startswith("0")
                or int(candidate_id) <= 0
            ):
                raise InvalidRequestError("candidate ids must be positive strings")
            if not isinstance(execution, Mapping):
                raise InvalidRequestError("execution entries must be mappings")
            if not isinstance(execution.get("executed"), bool):
                raise InvalidRequestError("executed must be a boolean")
            normalized[candidate_id] = _json_value(
                copy.deepcopy(dict(execution)),
                path=f"executions.{candidate_id}",
            )
        return normalized

    @staticmethod
    async def _fresh_review(
        db,
        review_id: int,
        *,
        for_update: bool = False,
    ) -> Optional[TradingExecutionReview]:
        return await db.scalar(
            TradingPlaybookReviewService.review_select_statement(
                review_id,
                for_update=for_update,
            )
            .execution_options(populate_existing=True)
        )


__all__ = ["TradingPlaybookReviewService"]
