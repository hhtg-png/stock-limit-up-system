"""Durable outbox delivery for trading-playbook alerts."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import math
import uuid
from collections.abc import Mapping
from datetime import date, datetime, time
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import IntegrityError
from loguru import logger

from app.config import settings as app_settings
from app.models.trading_playbook import (
    TradingAlertConditionState,
    TradingAlertEvent,
    TradingPlanCandidate,
    TradingPlanVersion,
    TradingPlaybookSettings,
)
from app.services.realtime_limit_up_service import RealtimeLimitUpSnapshot
from app.utils.time_utils import CN_TZ, now_cn

from .channels import TradingPlanAlertChannel


_PLAN_EVENT_TYPES = ("plan_ready", "confirmation_required")
_ACTION_EVALUATION_STATUSES = ("active", "confirmed")
_MONITOR_PLAN_STATUS = "active"
_CONDITION_METADATA = frozenset({"label", "reference_price"})
_NUMERIC_CONDITIONS = {
    "price_gte": ("price", "gte"),
    "price_lte": ("price", "lte"),
    "change_pct_gte": ("change_pct", "gte"),
    "change_pct_lte": ("change_pct", "lte"),
    "open_count_gte": ("open_count", "gte"),
}
_ACTION_EVENT_SEVERITIES = {
    "invalidated": "warning",
    "exit_triggered": "warning",
    "entry_triggered": "action",
}
_ACTION_CONDITION_SPECS = (
    ("invalidation_json", "invalidated"),
    ("exit_trigger_json", "exit_triggered"),
    ("entry_trigger_json", "entry_triggered"),
)
_TERMINAL_ACTION_EVENT_TYPES = ("exit_triggered",)


class TradingAlertDeliveryStateLost(RuntimeError):
    """Raised when this sender no longer owns the outbox state transition."""


class TradingPlaybookAlertService:
    """Persist first, then perform one at-most-once channel send attempt."""

    durable_delivery = True

    def __init__(
        self,
        channel: TradingPlanAlertChannel,
        *,
        session_factory=None,
        quote_api=None,
        realtime_limit_up_loader=None,
        trading_calendar=None,
        quote_timeout_seconds: float = 5.0,
        quote_max_age_seconds: float | None = None,
        max_monitor_candidates: int = 240,
    ) -> None:
        if not isinstance(channel, TradingPlanAlertChannel):
            raise TypeError(
                "alert channel must provide send, reconcile, healthcheck and "
                "capability metadata"
            )
        name = str(getattr(channel, "channel_name", "")).strip()
        if not name:
            raise TypeError("alert channel must declare channel_name")
        if not isinstance(
            getattr(channel, "supports_provider_idempotency", None), bool
        ):
            raise TypeError(
                "alert channel must declare supports_provider_idempotency"
            )
        self.channel = channel
        self.channel_name = name
        self.owner = uuid.uuid4().hex
        self.session_factory = session_factory
        self.quote_api = quote_api
        self.realtime_limit_up_loader = realtime_limit_up_loader
        self.trading_calendar = trading_calendar
        self.quote_timeout_seconds = max(float(quote_timeout_seconds), 0.01)
        max_age = (
            app_settings.TRADING_PLAYBOOK_ALERT_QUOTE_MAX_AGE_SECONDS
            if quote_max_age_seconds is None
            else quote_max_age_seconds
        )
        self.quote_max_age_seconds = max(float(max_age), 0.0)
        self.max_monitor_candidates = max(int(max_monitor_candidates), 1)
        self._memory_dedup: set[tuple[Any, str]] = set()

    @staticmethod
    def _plan_value(plan: Any, key: str) -> Any:
        if isinstance(plan, Mapping):
            return plan.get(key)
        return getattr(plan, key, None)

    @staticmethod
    def _json_value(value: Any) -> Any:
        if isinstance(value, (date, datetime)):
            return value.isoformat()
        return copy.deepcopy(value)

    def _dedup_key(self, plan_id: int, event_type: str) -> str:
        return f"plan:{plan_id}:{self.channel_name}:{event_type}"

    @staticmethod
    def _message(plan: Any, event_type: str) -> str:
        stage = TradingPlaybookAlertService._plan_value(plan, "stage") or ""
        target = (
            TradingPlaybookAlertService._plan_value(
                plan, "target_trade_date"
            )
            or ""
        )
        if event_type == "confirmation_required":
            return f"交易预案待确认：{target} {stage}"
        return f"交易预案已生成：{target} {stage}"

    async def notify_plan_ready(self, db, plan: Any, *, send: bool = True):
        channel_enabled = await self._channel_enabled(db)
        # Both rows are durable before either external side effect starts.
        events = [
            await self._ensure_event(
                db,
                plan,
                event_type,
                initial_channel_status=self._initial_channel_status(
                    self._dedup_key(self._plan_value(plan, "id"), event_type),
                    enabled=channel_enabled,
                ),
            )
            for event_type in _PLAN_EVENT_TYPES
        ]
        if send and channel_enabled:
            for event in events:
                await self._deliver(db, event)
        return events

    async def emit_plan_event(
        self,
        db,
        plan: Any,
        *,
        event_type: str,
        send: bool = True,
    ) -> TradingAlertEvent:
        if event_type not in _PLAN_EVENT_TYPES:
            raise ValueError(f"unsupported plan alert event: {event_type}")
        channel_enabled = await self._channel_enabled(db)
        event = await self._ensure_event(
            db,
            plan,
            event_type,
            initial_channel_status=self._initial_channel_status(
                self._dedup_key(self._plan_value(plan, "id"), event_type),
                enabled=channel_enabled,
            ),
        )
        if send and channel_enabled:
            await self._deliver(db, event)
        return event

    async def monitor(self, db, now: datetime):
        """Evaluate today's confirmed candidate conditions from one batch quote."""
        if not isinstance(now, datetime):
            raise TypeError("alert monitor now must be a datetime")
        if now.tzinfo is None or now.utcoffset() is None:
            current = CN_TZ.localize(now)
        else:
            current = now.astimezone(CN_TZ)
        trade_date = current.date()
        calendar = self.trading_calendar
        ensure_date = getattr(calendar, "ensure_date", None)
        is_trading_day = getattr(calendar, "is_trading_day", None)
        if not callable(ensure_date) or not callable(is_trading_day):
            logger.error(
                "Trading playbook monitor skipped: authoritative calendar missing"
            )
            return []
        try:
            await ensure_date(trade_date)
            trading_day = bool(is_trading_day(trade_date))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                "Trading playbook monitor calendar lookup failed: {}",
                exc,
            )
            return []
        if not trading_day:
            logger.info(
                "Trading playbook monitor skipped on closed market date {}",
                trade_date,
            )
            return []
        if not self._is_continuous_trading_time(current):
            return []

        rows = list(
            (
                await db.execute(
                    select(TradingPlanVersion, TradingPlanCandidate)
                    .join(
                        TradingPlanCandidate,
                        TradingPlanCandidate.plan_version_id
                        == TradingPlanVersion.id,
                    )
                    .where(
                        TradingPlanVersion.status == _MONITOR_PLAN_STATUS,
                        TradingPlanCandidate.action_trade_date == trade_date,
                    )
                    .order_by(
                        TradingPlanVersion.id.desc(),
                        TradingPlanCandidate.rank,
                        TradingPlanCandidate.id,
                    )
                    .limit(self.max_monitor_candidates + 1)
                )
            ).all()
        )
        if len(rows) > self.max_monitor_candidates:
            logger.warning(
                "Trading playbook monitor candidate limit reached: {}",
                self.max_monitor_candidates,
            )
            rows = rows[: self.max_monitor_candidates]
        existing_action_events = list(
            (
                await db.execute(
                    select(TradingAlertEvent).where(
                        TradingAlertEvent.event_type.in_(
                            tuple(_ACTION_EVENT_SEVERITIES)
                        ),
                        TradingAlertEvent.dedup_key.like(
                            f"action:{trade_date.isoformat()}:%"
                        ),
                    )
                )
            )
            .scalars()
            .all()
        )
        pending_action_events = [
            event
            for event in existing_action_events
            if dict(
                (event.channel_status_json or {}).get(self.channel_name) or {}
            ).get("status")
            == "pending"
        ]
        await db.commit()
        for event in pending_action_events:
            await self._deliver(db, event)

        if not rows:
            return pending_action_events
        terminal_candidate_ids = {
            event.candidate_id
            for event in existing_action_events
            if event.event_type in _TERMINAL_ACTION_EVENT_TYPES
        }
        if terminal_candidate_ids:
            rows = [
                (plan, candidate)
                for plan, candidate in rows
                if candidate.id not in terminal_candidate_ids
            ]
        if not rows:
            return pending_action_events
        if self.quote_api is None or not callable(
            getattr(self.quote_api, "get_quotes_batch", None)
        ):
            logger.warning(
                "Trading playbook monitor skipped: batch quote provider missing"
            )
            return pending_action_events

        # End the read transaction before waiting on the network.  The project
        # session factory keeps loaded rows usable with expire_on_commit=False.
        await db.commit()
        codes = sorted({candidate.stock_code for _plan, candidate in rows})
        needs_open_count = self._rows_need_open_count(rows)
        response, open_count_snapshot = await self._load_monitor_market_data(
            codes,
            trade_date,
            needs_open_count=needs_open_count,
        )
        if response is None:
            return pending_action_events
        quotes = self._normalize_quotes(
            response,
            requested_codes=set(codes),
            as_of=current,
        )
        if not quotes:
            return pending_action_events
        if needs_open_count:
            open_counts = self._normalize_open_counts(
                open_count_snapshot,
                requested_codes=set(codes),
                as_of=current,
            )
            for code, open_count in open_counts.items():
                quote = quotes.get(code)
                if quote is not None:
                    quote["open_count"] = open_count

        channel_enabled = await self._channel_enabled(db)
        persisted: list[TradingAlertEvent] = list(pending_action_events)
        newly_persisted: list[TradingAlertEvent] = []
        for plan, candidate in rows:
            quote = quotes.get(candidate.stock_code)
            if quote is None:
                continue
            event = await self._apply_candidate_tick(
                db,
                plan,
                candidate,
                quote,
                trade_date=trade_date,
                triggered_at=current,
                channel_enabled=channel_enabled,
            )
            if event is not None:
                persisted.append(event)
                newly_persisted.append(event)
        if channel_enabled:
            for event in newly_persisted:
                await self._deliver(db, event)
        return persisted

    @classmethod
    def _normalized_condition_value(cls, value: Any) -> Any:
        if isinstance(value, Mapping):
            return {
                str(key): cls._normalized_condition_value(value[key])
                for key in sorted(value, key=lambda item: str(item))
            }
        if isinstance(value, (list, tuple)):
            return [cls._normalized_condition_value(item) for item in value]
        if isinstance(value, bool) or value is None or isinstance(value, str):
            return value
        if isinstance(value, (int, float)):
            number = cls._finite_number(value)
            if number is None:
                return None
            return int(number) if number.is_integer() else number
        if isinstance(value, (date, datetime)):
            return value.isoformat()
        return str(value)

    @classmethod
    def _condition_version(cls, event_type: str, condition: Mapping[str, Any]) -> str:
        canonical = json.dumps(
            {
                "condition": cls._normalized_condition_value(condition),
                "event_type": event_type,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @staticmethod
    def _condition_state_insert_statement(
        dialect_name: str,
        values: Mapping[str, Any],
    ):
        if dialect_name == "postgresql":
            statement = postgresql_insert(TradingAlertConditionState)
        else:
            statement = sqlite_insert(TradingAlertConditionState)
        return statement.values(**dict(values)).on_conflict_do_nothing(
            index_elements=["candidate_id", "event_type", "condition_version"]
        )

    @staticmethod
    def _condition_activate_statement(
        candidate_id: int,
        event_type: str,
        condition_version: str,
        matched_at: datetime,
    ):
        return (
            update(TradingAlertConditionState)
            .where(
                TradingAlertConditionState.candidate_id == candidate_id,
                TradingAlertConditionState.event_type == event_type,
                TradingAlertConditionState.condition_version
                == condition_version,
                TradingAlertConditionState.active.is_(False),
            )
            .values(
                active=True,
                occurrence_no=TradingAlertConditionState.occurrence_no + 1,
                last_matched_at=matched_at,
                updated_at=matched_at,
            )
            .execution_options(synchronize_session=False)
        )

    @staticmethod
    def _condition_recover_statement(
        state_id: int,
        recovered_at: datetime,
    ):
        return (
            update(TradingAlertConditionState)
            .where(
                TradingAlertConditionState.id == state_id,
                TradingAlertConditionState.active.is_(True),
            )
            .values(
                active=False,
                last_recovered_at=recovered_at,
                updated_at=recovered_at,
            )
            .execution_options(synchronize_session=False)
        )

    @classmethod
    def _current_condition_specs(cls, candidate: Any, quote: Mapping[str, Any]):
        specs = []
        for condition_name, event_type in _ACTION_CONDITION_SPECS:
            condition = cls._candidate_value(candidate, condition_name) or {}
            if not isinstance(condition, Mapping) or not any(
                key not in _CONDITION_METADATA for key in condition
            ):
                continue
            specs.append(
                {
                    "event_type": event_type,
                    "condition": condition,
                    "condition_version": cls._condition_version(
                        event_type,
                        condition,
                    ),
                    "result": cls._condition_result(condition, quote),
                }
            )
        return specs

    async def _apply_candidate_tick(
        self,
        db,
        plan: Any,
        candidate: Any,
        quote: Mapping[str, Any],
        *,
        trade_date: date,
        triggered_at: datetime,
        channel_enabled: bool,
    ) -> TradingAlertEvent | None:
        candidate_id = self._candidate_value(candidate, "id")
        plan_id = self._plan_value(plan, "id")
        if not isinstance(candidate_id, int) or not isinstance(plan_id, int):
            return None
        specs = self._current_condition_specs(candidate, quote)
        if not specs:
            await db.commit()
            return None
        observed_at = triggered_at.astimezone(CN_TZ).replace(tzinfo=None)
        dialect_name = db.get_bind().dialect.name
        for spec in specs:
            await db.execute(
                self._condition_state_insert_statement(
                    dialect_name,
                    {
                        "candidate_id": candidate_id,
                        "event_type": spec["event_type"],
                        "condition_version": spec["condition_version"],
                        "active": False,
                        "occurrence_no": 0,
                        "updated_at": observed_at,
                    },
                )
            )

        locked_candidate = await db.scalar(
            select(TradingPlanCandidate)
            .where(TradingPlanCandidate.id == candidate_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if locked_candidate is None or locked_candidate.status == "exit":
            await db.commit()
            return None
        states = list(
            (
                await db.execute(
                    select(TradingAlertConditionState)
                    .where(
                        TradingAlertConditionState.candidate_id == candidate_id
                    )
                    .execution_options(populate_existing=True)
                )
            )
            .scalars()
            .all()
        )
        state_by_key = {
            (state.event_type, state.condition_version): state for state in states
        }
        invalidation_recovered = False
        effective_active = {
            key: bool(state.active) for key, state in state_by_key.items()
        }

        for spec in specs:
            key = (spec["event_type"], spec["condition_version"])
            state = state_by_key[key]
            result = spec["result"]
            if result is False and effective_active.get(key, False):
                recovered = await db.execute(
                    self._condition_recover_statement(state.id, observed_at)
                )
                if recovered.rowcount == 1:
                    effective_active[key] = False
                    invalidation_recovered = (
                        invalidation_recovered
                        or spec["event_type"] == "invalidated"
                    )
            elif result is True and effective_active.get(key, False):
                await db.execute(
                    update(TradingAlertConditionState)
                    .where(
                        TradingAlertConditionState.id == state.id,
                        TradingAlertConditionState.active.is_(True),
                    )
                    .values(
                        last_matched_at=observed_at,
                        updated_at=observed_at,
                    )
                    .execution_options(synchronize_session=False)
                )

        active_state_by_event = {
            state.event_type: state
            for state in states
            if effective_active.get(
                (state.event_type, state.condition_version),
                False,
            )
        }

        if invalidation_recovered and locked_candidate.status == "invalidated":
            had_entry = (
                await db.scalar(
                    select(TradingAlertConditionState.id)
                    .where(
                        TradingAlertConditionState.candidate_id == candidate_id,
                        TradingAlertConditionState.event_type == "entry_triggered",
                        TradingAlertConditionState.occurrence_no > 0,
                    )
                    .limit(1)
                )
            ) is not None
            recovered_status = "triggered" if had_entry else "waiting"
            await db.execute(
                update(TradingPlanCandidate)
                .where(
                    TradingPlanCandidate.id == candidate_id,
                    TradingPlanCandidate.status == "invalidated",
                )
                .values(status=recovered_status)
                .execution_options(synchronize_session=False)
            )
            locked_candidate.status = recovered_status

        chosen = None
        active_occurrence = None
        event_priority = {
            event_type: index
            for index, (_condition_name, event_type) in enumerate(
                _ACTION_CONDITION_SPECS
            )
        }
        highest_active_state = min(
            active_state_by_event.values(),
            key=lambda state: event_priority[state.event_type],
            default=None,
        )
        for spec in specs:
            key = (spec["event_type"], spec["condition_version"])
            if (
                highest_active_state is not None
                and event_priority[highest_active_state.event_type]
                < event_priority[spec["event_type"]]
            ):
                active_occurrence = {
                    "event_type": highest_active_state.event_type,
                    "condition_version": highest_active_state.condition_version,
                }
                break
            if spec["result"] is True:
                if not effective_active.get(key, False):
                    chosen = spec
                else:
                    active_occurrence = spec
                break
            blocking_state = active_state_by_event.get(spec["event_type"])
            if blocking_state is not None:
                active_occurrence = {
                    "event_type": blocking_state.event_type,
                    "condition_version": blocking_state.condition_version,
                }
                break
        if chosen is None:
            await db.commit()
            if active_occurrence is not None:
                existing = await self._existing_occurrence_event(
                    db,
                    plan,
                    candidate,
                    active_occurrence["event_type"],
                    active_occurrence["condition_version"],
                    trade_date,
                )
                channel_status = dict(
                    (existing.channel_status_json or {}).get(self.channel_name)
                    or {}
                ) if existing is not None else {}
                if channel_status.get("status") == "pending":
                    return existing
            return None

        event_type = chosen["event_type"]
        condition_version = chosen["condition_version"]
        activated = await db.execute(
            self._condition_activate_statement(
                candidate_id,
                event_type,
                condition_version,
                observed_at,
            )
        )
        if activated.rowcount != 1:
            await db.rollback()
            return await self._existing_occurrence_event(
                db,
                plan,
                candidate,
                event_type,
                condition_version,
                trade_date,
            )
        occurrence_no = await db.scalar(
            select(TradingAlertConditionState.occurrence_no).where(
                TradingAlertConditionState.candidate_id == candidate_id,
                TradingAlertConditionState.event_type == event_type,
                TradingAlertConditionState.condition_version
                == condition_version,
            )
        )
        if not isinstance(occurrence_no, int) or occurrence_no < 1:
            await db.rollback()
            return None

        target_status = {
            "entry_triggered": "triggered",
            "invalidated": "invalidated",
            "exit_triggered": "exit",
        }[event_type]
        allowed_statuses = {
            "entry_triggered": ("waiting", "triggered"),
            "invalidated": ("waiting", "triggered", "invalidated"),
            "exit_triggered": ("waiting", "triggered", "invalidated"),
        }[event_type]
        candidate_update = await db.execute(
            update(TradingPlanCandidate)
            .where(
                TradingPlanCandidate.id == candidate_id,
                TradingPlanCandidate.status.in_(allowed_statuses),
            )
            .values(status=target_status)
            .execution_options(synchronize_session=False)
        )
        if candidate_update.rowcount != 1:
            await db.rollback()
            return None

        dedup_key = self._action_dedup_key(
            plan,
            candidate,
            event_type,
            trade_date,
            condition_version=condition_version,
            occurrence_no=occurrence_no,
        )
        event = self._new_action_event(
            plan,
            candidate,
            quote,
            event_type=event_type,
            dedup_key=dedup_key,
            trade_date=trade_date,
            triggered_at=triggered_at,
            condition_version=condition_version,
            occurrence_no=occurrence_no,
            initial_channel_status=self._initial_channel_status(
                dedup_key,
                enabled=channel_enabled,
            ),
        )
        db.add(event)
        try:
            await db.commit()
            await db.refresh(event)
            return event
        except IntegrityError:
            await db.rollback()
            return (
                await db.execute(
                    select(TradingAlertEvent).where(
                        TradingAlertEvent.dedup_key == dedup_key
                    )
                )
            ).scalar_one_or_none()

    async def _existing_occurrence_event(
        self,
        db,
        plan: Any,
        candidate: Any,
        event_type: str,
        condition_version: str,
        trade_date: date,
    ) -> TradingAlertEvent | None:
        candidate_id = self._candidate_value(candidate, "id")
        occurrence_no = await db.scalar(
            select(TradingAlertConditionState.occurrence_no).where(
                TradingAlertConditionState.candidate_id == candidate_id,
                TradingAlertConditionState.event_type == event_type,
                TradingAlertConditionState.condition_version
                == condition_version,
            )
        )
        if not isinstance(occurrence_no, int) or occurrence_no < 1:
            return None
        dedup_key = self._action_dedup_key(
            plan,
            candidate,
            event_type,
            trade_date,
            condition_version=condition_version,
            occurrence_no=occurrence_no,
        )
        return (
            await db.execute(
                select(TradingAlertEvent).where(
                    TradingAlertEvent.dedup_key == dedup_key
                )
            )
        ).scalar_one_or_none()

    @classmethod
    def _rows_need_open_count(cls, rows: list[tuple[Any, Any]]) -> bool:
        for _plan, candidate in rows:
            for name in (
                "invalidation_json",
                "exit_trigger_json",
                "entry_trigger_json",
            ):
                condition = cls._candidate_value(candidate, name)
                if isinstance(condition, Mapping) and "open_count_gte" in condition:
                    return True
        return False

    async def _load_monitor_market_data(
        self,
        codes: list[str],
        trade_date: date,
        *,
        needs_open_count: bool,
    ) -> tuple[Any | None, Any | None]:
        quote_task = asyncio.create_task(self.quote_api.get_quotes_batch(codes))
        tasks = {quote_task}
        pool_task = None
        if needs_open_count and callable(self.realtime_limit_up_loader):
            pool_task = asyncio.create_task(
                self.realtime_limit_up_loader(trade_date)
            )
            tasks.add(pool_task)
        try:
            done, pending = await asyncio.wait(
                tasks,
                timeout=self.quote_timeout_seconds,
            )
        except asyncio.CancelledError:
            await self._cancel_monitor_tasks(tasks)
            raise

        if pending:
            await self._cancel_monitor_tasks(pending)
        if quote_task not in done:
            logger.error("Trading playbook monitor quote batch timed out")
            return None, None
        try:
            response = quote_task.result()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("Trading playbook monitor quote batch failed: {}", exc)
            return None, None

        snapshot = None
        if pool_task is not None:
            if pool_task in done:
                try:
                    snapshot = pool_task.result()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning(
                        "Trading playbook monitor open-count snapshot failed: {}",
                        exc,
                    )
            else:
                logger.warning(
                    "Trading playbook monitor open-count snapshot timed out"
                )
        return response, snapshot

    @staticmethod
    async def _cancel_monitor_tasks(tasks) -> None:
        pending = list(tasks)
        for task in pending:
            if not task.done():
                task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    def _normalize_open_counts(
        self,
        snapshot: Any,
        *,
        requested_codes: set[str],
        as_of: datetime,
    ) -> dict[str, int]:
        if not isinstance(snapshot, RealtimeLimitUpSnapshot):
            return {}
        if (
            snapshot.authoritative is not True
            or snapshot.complete is not True
            or snapshot.evidence_trade_date != as_of.date()
        ):
            return {}
        normalized: dict[str, int] = {}
        for item in snapshot.items:
            if not isinstance(item, Mapping):
                continue
            code = item.get("stock_code")
            if type(code) is not str or code not in requested_codes:
                continue
            collected_at = self._parse_quote_datetime(item.get("_collected_at"))
            if collected_at is None or collected_at.date() != as_of.date():
                continue
            age_seconds = (as_of - collected_at).total_seconds()
            if age_seconds < 0 or age_seconds > self.quote_max_age_seconds:
                continue
            open_count = item.get("open_count")
            if type(open_count) is not int or open_count < 0:
                continue
            normalized[code] = open_count
        return normalized

    async def evaluate_candidate(
        self,
        plan_status: str,
        candidate: Any,
        quote: Mapping[str, Any],
    ) -> list[dict[str, Any]]:
        """Evaluate one candidate with deterministic fail-closed precedence."""
        return self._evaluate_candidate(
            plan_status,
            candidate,
            quote,
            use_memory_dedup=True,
        )

    def _evaluate_candidate(
        self,
        plan_status: str,
        candidate: Any,
        quote: Mapping[str, Any],
        *,
        use_memory_dedup: bool,
    ) -> list[dict[str, Any]]:
        if plan_status not in _ACTION_EVALUATION_STATUSES:
            return self._memory_event(
                candidate,
                "watch",
                "info",
                use_memory_dedup=use_memory_dedup,
            )

        checks = (
            ("invalidation_json", "invalidated"),
            ("exit_trigger_json", "exit_triggered"),
            ("entry_trigger_json", "entry_triggered"),
        )
        for condition_name, event_type in checks:
            condition = self._candidate_value(candidate, condition_name) or {}
            if self._condition_matches(condition, quote):
                return self._memory_event(
                    candidate,
                    event_type,
                    _ACTION_EVENT_SEVERITIES[event_type],
                    use_memory_dedup=use_memory_dedup,
                )
        return []

    def _memory_event(
        self,
        candidate: Any,
        event_type: str,
        severity: str,
        *,
        use_memory_dedup: bool,
    ) -> list[dict[str, Any]]:
        candidate_id = self._candidate_value(candidate, "id")
        key = (candidate_id, event_type)
        if use_memory_dedup and key in self._memory_dedup:
            return []
        if use_memory_dedup:
            self._memory_dedup.add(key)
        return [{"event_type": event_type, "severity": severity}]

    @staticmethod
    def _finite_number(value: Any) -> float | None:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None
        number = float(value)
        return number if math.isfinite(number) else None

    @classmethod
    def _condition_result(
        cls,
        condition: Any,
        quote: Mapping[str, Any],
    ) -> bool | None:
        if not isinstance(condition, Mapping) or not isinstance(quote, Mapping):
            return None
        actionable = False
        unknown = False
        raw_missing_fields = quote.get("_missing_fields")
        missing_fields = {
            str(value)
            for value in (
                raw_missing_fields
                if isinstance(raw_missing_fields, (list, tuple, set, frozenset))
                else ()
            )
        }
        for key, expected in condition.items():
            if key in _CONDITION_METADATA:
                continue
            actionable = True
            if key == "sealed":
                if not isinstance(expected, bool):
                    unknown = True
                    continue
                if "sealed" in missing_fields:
                    unknown = True
                    continue
                actual = quote.get("sealed")
                if not isinstance(actual, bool):
                    unknown = True
                    continue
                if actual is not expected:
                    return False
                continue
            numeric = _NUMERIC_CONDITIONS.get(key)
            if numeric is None:
                unknown = True
                continue
            quote_key, operation = numeric
            if quote_key in missing_fields:
                unknown = True
                continue
            if quote_key == "open_count":
                open_count = quote.get(quote_key)
                if type(open_count) is not int or open_count < 0:
                    unknown = True
                    continue
            actual_number = cls._finite_number(quote.get(quote_key))
            expected_number = cls._finite_number(expected)
            if actual_number is None or expected_number is None:
                unknown = True
                continue
            if quote_key == "price" and actual_number <= 0:
                unknown = True
                continue
            if operation == "gte" and actual_number < expected_number:
                return False
            if operation == "lte" and actual_number > expected_number:
                return False
        if not actionable or unknown:
            return None
        return True

    @classmethod
    def _condition_matches(
        cls,
        condition: Any,
        quote: Mapping[str, Any],
    ) -> bool:
        return cls._condition_result(condition, quote) is True

    @staticmethod
    def _candidate_value(candidate: Any, key: str) -> Any:
        if isinstance(candidate, Mapping):
            return candidate.get(key)
        return getattr(candidate, key, None)

    @staticmethod
    def _is_continuous_trading_time(current: datetime) -> bool:
        local_time = current.replace(tzinfo=None).time()
        return (
            time(9, 30) <= local_time <= time(11, 30)
            or time(13, 0) <= local_time <= time(15, 0)
        )

    @staticmethod
    def _parse_quote_datetime(value: Any) -> datetime | None:
        if isinstance(value, datetime):
            parsed = value
        elif isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            parsed = None
            if len(text) == 14 and text.isdigit():
                try:
                    parsed = datetime.strptime(text, "%Y%m%d%H%M%S")
                except ValueError:
                    return None
            if parsed is None:
                try:
                    parsed = datetime.fromisoformat(
                        text.replace("Z", "+00:00")
                    )
                except ValueError:
                    return None
        else:
            return None
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return CN_TZ.localize(parsed)
        return parsed.astimezone(CN_TZ)

    def _normalize_quotes(
        self,
        response: Any,
        *,
        requested_codes: set[str],
        as_of: datetime,
    ) -> dict[str, dict[str, Any]]:
        if not isinstance(response, Mapping):
            return {}
        normalized: dict[str, dict[str, Any]] = {}
        for response_code, value in response.items():
            if not isinstance(value, Mapping):
                continue
            code = str(
                value.get("code")
                or value.get("stock_code")
                or response_code
                or ""
            ).strip()
            if code not in requested_codes:
                continue
            quote = dict(value)
            raw_missing_fields = quote.get("_missing_fields")
            missing_fields = {
                str(item)
                for item in (
                    raw_missing_fields
                    if isinstance(
                        raw_missing_fields,
                        (list, tuple, set, frozenset),
                    )
                    else ()
                )
            }
            quote["_missing_fields"] = sorted(missing_fields)
            captured_at = self._parse_quote_datetime(
                quote.get("datetime")
                or quote.get("as_of")
                or quote.get("captured_at")
                or quote.get("quote_time")
            )
            if captured_at is None or captured_at.date() != as_of.date():
                continue
            age_seconds = (as_of - captured_at).total_seconds()
            if age_seconds < 0 or age_seconds > self.quote_max_age_seconds:
                continue
            sealed = quote.get("sealed")
            if not isinstance(sealed, bool):
                sealed = quote.get("is_sealed")
            if not isinstance(sealed, bool):
                price = self._finite_number(quote.get("price"))
                limit_up_key = (
                    "limit_up" if "limit_up" in quote else "limit_up_price"
                )
                limit_up = self._finite_number(quote.get(limit_up_key))
                bid_volume = self._finite_number(quote.get("bid1_volume"))
                if (
                    "price" not in missing_fields
                    and limit_up_key not in missing_fields
                    and "bid1_volume" not in missing_fields
                    and price is not None
                    and price > 0
                    and limit_up is not None
                    and limit_up > 0
                    and bid_volume is not None
                    and bid_volume >= 0
                ):
                    sealed = price >= limit_up - 0.001 and bid_volume > 0
            if isinstance(sealed, bool):
                quote["sealed"] = sealed
            quote["code"] = code
            quote["captured_at"] = captured_at
            normalized[code] = quote
        return normalized

    def _action_dedup_key(
        self,
        plan: Any,
        candidate: Any,
        event_type: str,
        trade_date: date,
        *,
        condition_version: str | None = None,
        occurrence_no: int = 1,
    ) -> str:
        if condition_version is None:
            condition_name = next(
                (
                    name
                    for name, spec_event_type in _ACTION_CONDITION_SPECS
                    if spec_event_type == event_type
                ),
                None,
            )
            condition = (
                self._candidate_value(candidate, condition_name) or {}
                if condition_name is not None
                else {}
            )
            if not isinstance(condition, Mapping):
                condition = {}
            condition_version = self._condition_version(event_type, condition)
        return ":".join(
            (
                "action",
                trade_date.isoformat(),
                str(self._plan_value(plan, "id")),
                str(self._candidate_value(candidate, "id")),
                str(self._candidate_value(candidate, "primary_mode_key") or ""),
                event_type,
                condition_version[:16],
                str(occurrence_no),
            )
        )

    @classmethod
    def _snapshot_quote(cls, quote: Mapping[str, Any]) -> dict[str, Any]:
        snapshot: dict[str, Any] = {}
        for key in (
            "code",
            "name",
            "price",
            "change_pct",
            "sealed",
            "open_count",
            "datetime",
            "captured_at",
        ):
            value = quote.get(key)
            if isinstance(value, (date, datetime)):
                snapshot[key] = value.isoformat()
            elif isinstance(value, bool) or isinstance(value, str):
                snapshot[key] = value
            elif cls._finite_number(value) is not None:
                snapshot[key] = value
        return snapshot

    def _new_action_event(
        self,
        plan: Any,
        candidate: Any,
        quote: Mapping[str, Any],
        *,
        event_type: str,
        dedup_key: str,
        trade_date: date,
        triggered_at: datetime,
        condition_version: str,
        occurrence_no: int,
        initial_channel_status: Mapping[str, Any],
    ) -> TradingAlertEvent:
        plan_id = self._plan_value(plan, "id")
        candidate_id = self._candidate_value(candidate, "id")
        if not isinstance(plan_id, int) or not isinstance(candidate_id, int):
            raise ValueError("action alert requires persisted plan and candidate ids")
        if event_type not in _ACTION_EVENT_SEVERITIES:
            raise ValueError(f"unsupported action alert event: {event_type}")
        stock_code = str(self._candidate_value(candidate, "stock_code") or "")
        stock_name = str(self._candidate_value(candidate, "stock_name") or "")
        mode_key = str(
            self._candidate_value(candidate, "primary_mode_key") or ""
        )
        return TradingAlertEvent(
            plan_version_id=plan_id,
            candidate_id=candidate_id,
            event_type=event_type,
            severity=_ACTION_EVENT_SEVERITIES[event_type],
            dedup_key=dedup_key,
            triggered_at=triggered_at.astimezone(CN_TZ).replace(tzinfo=None),
            market_snapshot_json={
                "trade_date": trade_date.isoformat(),
                "stock_code": stock_code,
                "mode_key": mode_key,
                "condition_version": condition_version,
                "occurrence_no": occurrence_no,
                "quote": self._snapshot_quote(quote),
            },
            message=f"交易预案提醒：{stock_code} {stock_name} {event_type}",
            channel_status_json={
                self.channel_name: copy.deepcopy(dict(initial_channel_status))
            },
        )

    async def _ensure_action_event(
        self,
        db,
        plan: Any,
        candidate: Any,
        payload: Mapping[str, Any],
        quote: Mapping[str, Any],
        *,
        trade_date: date,
        triggered_at: datetime,
        initial_channel_status: Mapping[str, Any],
        lookup_existing: bool = True,
    ) -> TradingAlertEvent:
        plan_id = self._plan_value(plan, "id")
        candidate_id = self._candidate_value(candidate, "id")
        if not isinstance(plan_id, int) or not isinstance(candidate_id, int):
            raise ValueError("action alert requires persisted plan and candidate ids")
        event_type = str(payload.get("event_type") or "")
        if event_type not in _ACTION_EVENT_SEVERITIES:
            raise ValueError(f"unsupported action alert event: {event_type}")
        condition_name = next(
            name
            for name, spec_event_type in _ACTION_CONDITION_SPECS
            if spec_event_type == event_type
        )
        condition = self._candidate_value(candidate, condition_name) or {}
        if not isinstance(condition, Mapping):
            condition = {}
        condition_version = self._condition_version(event_type, condition)
        occurrence_no = 1
        dedup_key = self._action_dedup_key(
            plan,
            candidate,
            event_type,
            trade_date,
            condition_version=condition_version,
            occurrence_no=occurrence_no,
        )
        if lookup_existing:
            existing = (
                await db.execute(
                    select(TradingAlertEvent).where(
                        TradingAlertEvent.dedup_key == dedup_key
                    )
                )
            ).scalar_one_or_none()
            if existing is not None:
                return existing

        event = self._new_action_event(
            plan,
            candidate,
            quote,
            event_type=event_type,
            dedup_key=dedup_key,
            trade_date=trade_date,
            triggered_at=triggered_at,
            condition_version=condition_version,
            occurrence_no=occurrence_no,
            initial_channel_status=initial_channel_status,
        )
        db.add(event)
        try:
            await db.commit()
            await db.refresh(event)
            return event
        except IntegrityError:
            await db.rollback()
            winner = (
                await db.execute(
                    select(TradingAlertEvent).where(
                        TradingAlertEvent.dedup_key == dedup_key
                    )
                )
            ).scalar_one_or_none()
            if winner is None:
                raise
            return winner

    async def _channel_enabled(self, db) -> bool:
        if self.channel_name != "in_app":
            return True
        settings = await db.get(TradingPlaybookSettings, 1)
        return settings is not None and (
            bool(settings.enabled) and bool(settings.in_app_enabled)
        )

    @staticmethod
    def _initial_channel_status(
        idempotency_key: str,
        *,
        enabled: bool,
    ) -> dict[str, Any]:
        status: dict[str, Any] = {
            "idempotency_key": idempotency_key,
            "attempts": 0,
        }
        if enabled:
            status["status"] = "pending"
        else:
            status.update(
                {
                    "status": "skipped",
                    "reason": "disabled",
                    "skipped_at": now_cn().isoformat(),
                }
            )
        return status

    async def _ensure_event(
        self,
        db,
        plan: Any,
        event_type: str,
        *,
        initial_channel_status: Mapping[str, Any],
    ) -> TradingAlertEvent:
        plan_id = self._plan_value(plan, "id")
        if not isinstance(plan_id, int):
            raise ValueError("plan alert requires a persisted integer plan id")
        dedup_key = self._dedup_key(plan_id, event_type)
        existing = (
            await db.execute(
                select(TradingAlertEvent).where(
                    TradingAlertEvent.dedup_key == dedup_key
                )
            )
        ).scalar_one_or_none()
        if existing is not None:
            return existing

        event = TradingAlertEvent(
            plan_version_id=plan_id,
            event_type=event_type,
            severity=(
                "warning"
                if event_type == "confirmation_required"
                else "info"
            ),
            dedup_key=dedup_key,
            triggered_at=now_cn().replace(tzinfo=None),
            market_snapshot_json={
                "source_trade_date": self._plan_value(
                    plan, "source_trade_date"
                ),
                "target_trade_date": self._plan_value(
                    plan, "target_trade_date"
                ),
                "stage": self._plan_value(plan, "stage"),
                "status": self._plan_value(plan, "status"),
            },
            message=self._message(plan, event_type),
            channel_status_json={
                self.channel_name: copy.deepcopy(
                    dict(initial_channel_status)
                )
            },
        )
        event.market_snapshot_json = {
            key: self._json_value(value)
            for key, value in event.market_snapshot_json.items()
        }
        db.add(event)
        try:
            await db.commit()
            await db.refresh(event)
            return event
        except IntegrityError:
            await db.rollback()
            winner = (
                await db.execute(
                    select(TradingAlertEvent).where(
                        TradingAlertEvent.dedup_key == dedup_key
                    )
                )
            ).scalar_one_or_none()
            if winner is None:
                raise
            return winner

    def _pending_claim_statement(
        self,
        event_id: int,
        new_status: Mapping[str, Any],
    ):
        status_path = TradingAlertEvent.channel_status_json[
            self.channel_name
        ]["status"].as_string()
        return (
            update(TradingAlertEvent)
            .where(
                TradingAlertEvent.id == event_id,
                status_path == "pending",
            )
            .values(channel_status_json=copy.deepcopy(dict(new_status)))
        )

    async def _claim_pending(self, db, event: TradingAlertEvent) -> bool:
        status = copy.deepcopy(event.channel_status_json or {})
        channel_status = dict(status.get(self.channel_name) or {})
        if channel_status.get("status") != "pending":
            return False
        channel_status.update(
            {
                "status": "sending",
                "owner": self.owner,
                "attempts": int(channel_status.get("attempts") or 0) + 1,
                "sending_at": now_cn().isoformat(),
            }
        )
        status[self.channel_name] = channel_status
        result = await db.execute(
            self._pending_claim_statement(event.id, status)
        )
        await db.commit()
        if result.rowcount != 1:
            return False
        event.channel_status_json = status
        return True

    @staticmethod
    def _settings_lock_statement(dialect_name: str):
        if dialect_name == "sqlite":
            return (
                update(TradingPlaybookSettings)
                .where(TradingPlaybookSettings.id == 1)
                .values(
                    enabled=TradingPlaybookSettings.enabled,
                    updated_at=TradingPlaybookSettings.updated_at,
                )
                .execution_options(synchronize_session=False)
            )
        return (
            select(TradingPlaybookSettings)
            .where(TradingPlaybookSettings.id == 1)
            .with_for_update()
        )

    async def _lock_delivery_settings(self, db):
        dialect_name = db.get_bind().dialect.name
        statement = self._settings_lock_statement(dialect_name)
        if dialect_name == "sqlite":
            result = await db.execute(statement)
            if result.rowcount != 1:
                return None
            return await db.scalar(
                select(TradingPlaybookSettings)
                .where(TradingPlaybookSettings.id == 1)
                .execution_options(populate_existing=True)
            )
        return await db.scalar(
            statement.execution_options(populate_existing=True)
        )

    async def _skip_owned_delivery(
        self,
        db,
        event_id: int,
        *,
        reason: str,
    ) -> None:
        await self._update_owned_sending(
            db,
            event_id,
            final_status="skipped",
            detail={"reason": reason},
        )

    async def _deliver(self, db, event: TradingAlertEvent) -> None:
        if not await self._claim_pending(db, event):
            return
        if self.channel_name == "in_app":
            settings = await self._lock_delivery_settings(db)
            if settings is None:
                await self._skip_owned_delivery(
                    db,
                    event.id,
                    reason="settings_missing",
                )
                return
            if not bool(settings.enabled) or not bool(settings.in_app_enabled):
                await self._skip_owned_delivery(
                    db,
                    event.id,
                    reason="disabled",
                )
                return
        channel_status = event.channel_status_json[self.channel_name]
        idempotency_key = channel_status["idempotency_key"]
        payload = {
            "id": event.id,
            "dedup_key": event.dedup_key,
            "idempotency_key": idempotency_key,
            "plan_version_id": event.plan_version_id,
            "candidate_id": event.candidate_id,
            "event_type": event.event_type,
            "severity": event.severity,
            "triggered_at": event.triggered_at.isoformat(),
            "message": event.message,
            "market_snapshot": copy.deepcopy(event.market_snapshot_json or {}),
        }
        stock_code = str(
            (event.market_snapshot_json or {}).get("stock_code") or ""
        ).strip()
        if stock_code:
            payload["stock_code"] = stock_code
        try:
            receipt = await self.channel.send(
                payload,
                idempotency_key=idempotency_key,
            )
        except asyncio.CancelledError as exc:
            await self._mark_uncertain_safely(db, event.id, exc)
            raise
        except Exception as exc:
            await self._mark_uncertain_safely(db, event.id, exc)
            raise
        try:
            await self._mark_delivered(db, event.id, receipt=receipt)
        except asyncio.CancelledError as exc:
            await self._recover_accepted_delivery(
                db,
                event.id,
                receipt=receipt,
                error=exc,
            )
            raise
        except Exception as exc:
            await self._recover_accepted_delivery(
                db,
                event.id,
                receipt=receipt,
                error=exc,
            )
            raise

    async def _recover_accepted_delivery(
        self,
        db,
        event_id: int,
        *,
        receipt: Mapping[str, Any],
        error: BaseException,
    ) -> None:
        try:
            await db.rollback()
        except Exception as rollback_error:
            logger.error(
                "Unable to rollback failed alert delivery session for {}: {}",
                event_id,
                rollback_error,
            )
        if self.session_factory is None:
            return
        try:
            async with self.session_factory() as fresh_db:
                event = await fresh_db.get(TradingAlertEvent, event_id)
                if event is None:
                    return
                status = copy.deepcopy(event.channel_status_json or {})
                channel_status = dict(status.get(self.channel_name) or {})
                current = channel_status.get("status")
                if current == "delivered":
                    return
                if (
                    current != "sending"
                    or channel_status.get("owner") != self.owner
                ):
                    return
                channel_status.update(
                    {
                        "status": "uncertain",
                        "uncertain_at": now_cn().isoformat(),
                        "accepted": True,
                        "receipt": copy.deepcopy(dict(receipt or {})),
                        "error": str(error),
                    }
                )
                status[self.channel_name] = channel_status
                status_path = TradingAlertEvent.channel_status_json[
                    self.channel_name
                ]["status"].as_string()
                owner_path = TradingAlertEvent.channel_status_json[
                    self.channel_name
                ]["owner"].as_string()
                result = await fresh_db.execute(
                    update(TradingAlertEvent)
                    .where(
                        TradingAlertEvent.id == event_id,
                        status_path == "sending",
                        owner_path == self.owner,
                    )
                    .values(channel_status_json=status)
                )
                await fresh_db.commit()
                if result.rowcount != 1:
                    logger.error(
                        "Fresh alert compensation lost state for event {}",
                        event_id,
                    )
        except Exception as compensation_error:
            logger.error(
                "Unable to compensate accepted alert delivery for {}: {}",
                event_id,
                compensation_error,
            )

    async def _mark_uncertain_safely(
        self,
        db,
        event_id: int,
        error: BaseException,
    ) -> None:
        try:
            await asyncio.shield(
                self._mark_uncertain(db, event_id, error=str(error))
            )
        except Exception as state_error:
            logger.error(
                "Unable to persist uncertain alert delivery state for {}: {}",
                event_id,
                state_error,
            )

    async def _update_owned_sending(
        self,
        db,
        event_id: int,
        *,
        final_status: str,
        detail: Mapping[str, Any],
    ) -> None:
        event = await db.get(TradingAlertEvent, event_id)
        if event is None:
            raise TradingAlertDeliveryStateLost(
                f"alert delivery event missing: {event_id}"
            )
        status = copy.deepcopy(event.channel_status_json or {})
        channel_status = dict(status.get(self.channel_name) or {})
        if (
            final_status == "delivered"
            and channel_status.get("status") == "delivered"
        ):
            return
        if (
            channel_status.get("status") != "sending"
            or channel_status.get("owner") != self.owner
        ):
            raise TradingAlertDeliveryStateLost(
                f"alert delivery owner lost for event {event_id}"
            )
        channel_status.update(detail)
        channel_status["status"] = final_status
        channel_status[f"{final_status}_at"] = now_cn().isoformat()
        status[self.channel_name] = channel_status
        status_path = TradingAlertEvent.channel_status_json[
            self.channel_name
        ]["status"].as_string()
        owner_path = TradingAlertEvent.channel_status_json[
            self.channel_name
        ]["owner"].as_string()
        result = await db.execute(
            update(TradingAlertEvent)
            .where(
                TradingAlertEvent.id == event_id,
                status_path == "sending",
                owner_path == self.owner,
            )
            .values(channel_status_json=status)
        )
        await db.commit()
        if result.rowcount != 1:
            raise TradingAlertDeliveryStateLost(
                f"alert delivery state lost for event {event_id}"
            )

    async def _mark_delivered(
        self,
        db,
        event_id: int,
        *,
        receipt: Mapping[str, Any],
    ) -> None:
        await self._update_owned_sending(
            db,
            event_id,
            final_status="delivered",
            detail={"receipt": copy.deepcopy(dict(receipt or {}))},
        )

    async def _mark_uncertain(self, db, event_id: int, *, error: str) -> None:
        await self._update_owned_sending(
            db,
            event_id,
            final_status="uncertain",
            detail={"error": error},
        )


__all__ = [
    "TradingAlertDeliveryStateLost",
    "TradingPlaybookAlertService",
]
