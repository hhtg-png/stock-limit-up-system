"""Fan out scheduled playbook notifications without duplicating monitors."""

from __future__ import annotations

from datetime import date
from typing import Any, Iterable


class TradingPlaybookScheduledAlertFanout:
    """Keep in-app action monitoring primary and fan out daily milestones."""

    def __init__(self, primary: Any, scheduled_channels: Iterable[Any]) -> None:
        self.primary = primary
        self.scheduled_channels = tuple(scheduled_channels)

    async def notify_plan_ready(self, db, plan: Any, *, send: bool = True):
        primary_events = await self.primary.notify_plan_ready(
            db,
            plan,
            send=send,
        )
        scheduled_events = []
        for service in self.scheduled_channels:
            scheduled_events.append(
                await service.emit_plan_event(
                    db,
                    plan,
                    event_type="plan_ready",
                    send=send,
                )
            )
        return {
            "primary": primary_events,
            "scheduled": scheduled_events,
        }

    async def notify_review_ready(
        self,
        db,
        plan: Any,
        trade_date: date,
        *,
        send: bool = True,
    ):
        primary_event = await self.primary.notify_review_ready(
            db,
            plan,
            trade_date,
            send=send,
        )
        scheduled_events = []
        for service in self.scheduled_channels:
            scheduled_events.append(
                await service.notify_review_ready(
                    db,
                    plan,
                    trade_date,
                    send=send,
                )
            )
        return {
            "primary": primary_event,
            "scheduled": scheduled_events,
        }

    async def monitor(self, db, observed_at):
        return await self.primary.monitor(db, observed_at)

    def __getattr__(self, name: str):
        return getattr(self.primary, name)


__all__ = ["TradingPlaybookScheduledAlertFanout"]
