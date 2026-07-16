"""Delivery channels for persisted trading-playbook alert events."""

from __future__ import annotations

from typing import Any, Mapping, Protocol, runtime_checkable

from app.core.websocket_manager import manager


@runtime_checkable
class TradingPlanAlertChannel(Protocol):
    """Provider boundary retained for future idempotent/reconcilable channels."""

    channel_name: str
    supports_provider_idempotency: bool

    async def send(
        self,
        event: Mapping[str, Any],
        *,
        idempotency_key: str,
    ) -> Mapping[str, Any]: ...

    async def reconcile(
        self,
        *,
        idempotency_key: str,
    ) -> Mapping[str, Any] | None: ...

    async def healthcheck(self) -> Mapping[str, Any]: ...


class InAppTradingPlanAlertChannel:
    """At-most-once websocket hint; the alerts API is the durable inbox."""

    channel_name = "in_app"
    supports_provider_idempotency = False

    async def send(
        self,
        event: Mapping[str, Any],
        *,
        idempotency_key: str,
    ) -> Mapping[str, Any]:
        payload = dict(event)
        payload["idempotency_key"] = idempotency_key
        stock_code = str(event.get("stock_code") or "").strip() or None
        await manager.broadcast_trading_plan_alert(
            payload,
            stock_code=stock_code,
        )
        return {
            "accepted": True,
            "delivery": "at_most_once",
            "idempotency_key": idempotency_key,
        }

    async def reconcile(
        self,
        *,
        idempotency_key: str,
    ) -> Mapping[str, Any] | None:
        # In-app websocket delivery has no provider receipt.  Consumers recover
        # from disconnects through GET /trading-playbook/alerts.
        return None

    async def healthcheck(self) -> Mapping[str, Any]:
        return {
            "channel": self.channel_name,
            "status": "ready",
            "connections": manager.connection_count,
        }


__all__ = [
    "InAppTradingPlanAlertChannel",
    "TradingPlanAlertChannel",
]
