"""Durable outbox delivery for trading-playbook alerts."""

from __future__ import annotations

import asyncio
import copy
import math
import uuid
from collections.abc import Mapping
from datetime import date, datetime, time
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from loguru import logger

from app.config import settings as app_settings
from app.models.trading_playbook import (
    TradingAlertEvent,
    TradingPlanCandidate,
    TradingPlanVersion,
    TradingPlaybookSettings,
)
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
_TERMINAL_ACTION_EVENT_TYPES = ("invalidated", "exit_triggered")


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
        quote_timeout_seconds: float = 5.0,
        quote_max_age_seconds: float | None = None,
        max_monitor_candidates: int = 240,
    ) -> None:
        if not isinstance(channel, TradingPlanAlertChannel):
            raise TypeError(
                "alert channel must provide send, reconcile and capability metadata"
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
        # Both rows are durable before either external side effect starts.
        events = [
            await self._ensure_event(db, plan, event_type)
            for event_type in _PLAN_EVENT_TYPES
        ]
        if send and await self._channel_enabled(db):
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
        event = await self._ensure_event(db, plan, event_type)
        if send and await self._channel_enabled(db):
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
        if not rows:
            return []

        candidate_ids = [candidate.id for _plan, candidate in rows]
        existing_action_events = list(
            (
                await db.execute(
                    select(TradingAlertEvent).where(
                        TradingAlertEvent.candidate_id.in_(candidate_ids),
                        TradingAlertEvent.dedup_key.like(
                            f"action:{trade_date.isoformat()}:%"
                        ),
                    )
                )
            )
            .scalars()
            .all()
        )
        existing_by_dedup = {
            event.dedup_key: event for event in existing_action_events
        }
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
            return []
        if self.quote_api is None or not callable(
            getattr(self.quote_api, "get_quotes_batch", None)
        ):
            logger.warning(
                "Trading playbook monitor skipped: batch quote provider missing"
            )
            return []

        # End the read transaction before waiting on the network.  The project
        # session factory keeps loaded rows usable with expire_on_commit=False.
        await db.commit()
        codes = sorted({candidate.stock_code for _plan, candidate in rows})
        try:
            response = await asyncio.wait_for(
                self.quote_api.get_quotes_batch(codes),
                timeout=self.quote_timeout_seconds,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("Trading playbook monitor quote batch failed: {}", exc)
            return []
        quotes = self._normalize_quotes(
            response,
            requested_codes=set(codes),
            as_of=current,
        )
        if not quotes:
            return []

        persisted: list[TradingAlertEvent] = []
        for plan, candidate in rows:
            quote = quotes.get(candidate.stock_code)
            if quote is None:
                continue
            evaluated = self._evaluate_candidate(
                plan.status,
                candidate,
                quote,
                use_memory_dedup=False,
            )
            for payload in evaluated:
                dedup_key = self._action_dedup_key(
                    plan,
                    candidate,
                    str(payload.get("event_type") or ""),
                    trade_date,
                )
                event = existing_by_dedup.get(dedup_key)
                if event is None:
                    event = await self._ensure_action_event(
                        db,
                        plan,
                        candidate,
                        payload,
                        quote,
                        trade_date=trade_date,
                        triggered_at=current,
                        lookup_existing=False,
                    )
                    existing_by_dedup[event.dedup_key] = event
                persisted.append(event)
        if await self._channel_enabled(db):
            for event in persisted:
                await self._deliver(db, event)
        return persisted

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
    def _condition_matches(
        cls,
        condition: Any,
        quote: Mapping[str, Any],
    ) -> bool:
        if not isinstance(condition, Mapping) or not isinstance(quote, Mapping):
            return False
        actionable = False
        for key, expected in condition.items():
            if key in _CONDITION_METADATA:
                continue
            actionable = True
            if key == "sealed":
                if not isinstance(expected, bool):
                    return False
                actual = quote.get("sealed")
                if not isinstance(actual, bool) or actual is not expected:
                    return False
                continue
            numeric = _NUMERIC_CONDITIONS.get(key)
            if numeric is None:
                return False
            quote_key, operation = numeric
            actual_number = cls._finite_number(quote.get(quote_key))
            expected_number = cls._finite_number(expected)
            if actual_number is None or expected_number is None:
                return False
            if operation == "gte" and actual_number < expected_number:
                return False
            if operation == "lte" and actual_number > expected_number:
                return False
        return actionable

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
                limit_up = self._finite_number(
                    quote.get("limit_up", quote.get("limit_up_price"))
                )
                bid_volume = self._finite_number(quote.get("bid1_volume"))
                if (
                    price is not None
                    and limit_up is not None
                    and bid_volume is not None
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
    ) -> str:
        return ":".join(
            (
                "action",
                trade_date.isoformat(),
                str(self._plan_value(plan, "id")),
                str(self._candidate_value(candidate, "id")),
                str(self._candidate_value(candidate, "primary_mode_key") or ""),
                event_type,
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
        lookup_existing: bool = True,
    ) -> TradingAlertEvent:
        plan_id = self._plan_value(plan, "id")
        candidate_id = self._candidate_value(candidate, "id")
        if not isinstance(plan_id, int) or not isinstance(candidate_id, int):
            raise ValueError("action alert requires persisted plan and candidate ids")
        event_type = str(payload.get("event_type") or "")
        if event_type not in _ACTION_EVENT_SEVERITIES:
            raise ValueError(f"unsupported action alert event: {event_type}")
        dedup_key = self._action_dedup_key(
            plan,
            candidate,
            event_type,
            trade_date,
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

        stock_code = str(self._candidate_value(candidate, "stock_code") or "")
        stock_name = str(self._candidate_value(candidate, "stock_name") or "")
        mode_key = str(
            self._candidate_value(candidate, "primary_mode_key") or ""
        )
        idempotency_key = dedup_key
        event = TradingAlertEvent(
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
                "quote": self._snapshot_quote(quote),
            },
            message=f"交易预案提醒：{stock_code} {stock_name} {event_type}",
            channel_status_json={
                self.channel_name: {
                    "status": "pending",
                    "idempotency_key": idempotency_key,
                    "attempts": 0,
                }
            },
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
        return settings is None or (
            bool(settings.enabled) and bool(settings.in_app_enabled)
        )

    async def _ensure_event(
        self,
        db,
        plan: Any,
        event_type: str,
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

        idempotency_key = dedup_key
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
                self.channel_name: {
                    "status": "pending",
                    "idempotency_key": idempotency_key,
                    "attempts": 0,
                }
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

    async def _deliver(self, db, event: TradingAlertEvent) -> None:
        if not await self._claim_pending(db, event):
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
