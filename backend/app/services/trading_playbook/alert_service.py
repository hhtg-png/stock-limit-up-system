"""Durable outbox delivery for trading-playbook alerts."""

from __future__ import annotations

import asyncio
import copy
import uuid
from collections.abc import Mapping
from datetime import date, datetime
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from loguru import logger

from app.models.trading_playbook import (
    TradingAlertEvent,
    TradingPlaybookSettings,
)
from app.utils.time_utils import now_cn

from .channels import TradingPlanAlertChannel


_PLAN_EVENT_TYPES = ("plan_ready", "confirmation_required")


class TradingAlertDeliveryStateLost(RuntimeError):
    """Raised when this sender no longer owns the outbox state transition."""


class TradingPlaybookAlertService:
    """Persist first, then perform one at-most-once channel send attempt."""

    durable_delivery = True

    def __init__(self, channel: TradingPlanAlertChannel) -> None:
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
        """Task-10 extension point; plan-ready delivery is the minimal core."""
        return []

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
        await self._mark_delivered(db, event.id, receipt=receipt)

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
            return
        status = copy.deepcopy(event.channel_status_json or {})
        channel_status = dict(status.get(self.channel_name) or {})
        if (
            channel_status.get("status") != "sending"
            or channel_status.get("owner") != self.owner
        ):
            return
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
