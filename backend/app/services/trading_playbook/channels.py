"""Delivery channels for persisted trading-playbook alert events."""

from __future__ import annotations

from typing import Any, Mapping, Protocol, runtime_checkable

import httpx

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


class WxPusherTradingPlanAlertChannel:
    """Personal-WeChat delivery through WxPusher's simple-push API."""

    channel_name = "wxpusher"
    supports_provider_idempotency = False
    api_url = "https://wxpusher.zjiecode.com/api/send/message/simple-push"
    setup_qr_url = (
        "https://wxpusher.zjiecode.com/api/qrcode/"
        "RwjGLMOPTYp35zSYQr0HxbCPrV9eU0wKVBXU1D5VVtya0cQXEJWPjqBdW3gKLifS.jpg"
    )
    docs_url = "https://wxpusher.zjiecode.com/docs/"

    _STAGE_LABELS = {
        "overnight": "08:50 隔夜预案",
        "auction": "09:26 集合竞价预案",
        "preclose": "14:40 提前预案",
        "after_close": "15:30 收盘预案",
    }

    def __init__(
        self,
        simple_push_token: str | None,
        *,
        enabled: bool,
        public_url: str,
        timeout_seconds: float = 10.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._simple_push_token = str(simple_push_token or "").strip()
        self._enabled_by_config = bool(enabled)
        self.public_url = str(public_url or "").strip()
        self.timeout_seconds = max(float(timeout_seconds), 0.5)
        self._transport = transport

    @property
    def configured(self) -> bool:
        return self._simple_push_token.startswith("SPT_")

    @property
    def enabled(self) -> bool:
        return self._enabled_by_config and self.configured

    @property
    def masked_recipient(self) -> str | None:
        if not self.configured:
            return None
        suffix = self._simple_push_token[-4:]
        return f"SPT_****{suffix}"

    def status(self) -> dict[str, Any]:
        return {
            "provider": "wxpusher",
            "delivery": "personal_wechat",
            "configured": self.configured,
            "enabled": self.enabled,
            "recipient_masked": self.masked_recipient,
            "setup_qr_url": self.setup_qr_url,
            "docs_url": self.docs_url,
            "schedule": ["08:50", "09:26", "14:40", "15:10", "15:30"],
            "requires_server_configuration": not self.configured,
        }

    @classmethod
    def _title(cls, event: Mapping[str, Any]) -> str:
        event_type = str(event.get("event_type") or "")
        snapshot = event.get("market_snapshot")
        if not isinstance(snapshot, Mapping):
            snapshot = {}
        if event_type == "review_ready":
            return "15:10 交易复盘已生成"
        stage = str(snapshot.get("stage") or "")
        return cls._STAGE_LABELS.get(stage, "交易预案已生成")

    def _content(self, event: Mapping[str, Any]) -> tuple[str, str]:
        title = self._title(event)
        snapshot = event.get("market_snapshot")
        if not isinstance(snapshot, Mapping):
            snapshot = {}
        target_date = (
            snapshot.get("trade_date")
            or snapshot.get("target_trade_date")
            or ""
        )
        message = str(event.get("message") or title).strip()
        lines = [f"## {title}", "", message]
        if target_date:
            lines.extend(("", f"目标日期：{target_date}"))
        if self.public_url:
            lines.extend(("", f"[打开交易预案]({self.public_url})"))
        lines.extend(("", "> 仅作预案提醒，不构成自动交易指令。"))
        return title[:100], "\n".join(lines)

    async def send(
        self,
        event: Mapping[str, Any],
        *,
        idempotency_key: str,
    ) -> Mapping[str, Any]:
        if not self.enabled:
            raise RuntimeError("WxPusher personal WeChat delivery is not configured")
        summary, content = self._content(event)
        request_payload = {
            "content": content,
            "summary": summary,
            "contentType": 3,
            "spt": self._simple_push_token,
        }
        async with httpx.AsyncClient(
            timeout=self.timeout_seconds,
            transport=self._transport,
        ) as client:
            response = await client.post(self.api_url, json=request_payload)
        if response.status_code >= 400:
            raise RuntimeError(
                f"WxPusher request failed with HTTP {response.status_code}"
            )
        try:
            receipt = response.json()
        except ValueError as exc:
            raise RuntimeError("WxPusher returned a non-JSON response") from exc
        if not isinstance(receipt, Mapping):
            raise RuntimeError("WxPusher returned a malformed response")
        if receipt.get("success") is not True or receipt.get("code") != 1000:
            provider_message = str(receipt.get("msg") or "delivery rejected")
            raise RuntimeError(f"WxPusher rejected delivery: {provider_message}")
        return {
            "accepted": True,
            "provider": "wxpusher",
            "provider_code": receipt.get("code"),
            "idempotency_key": idempotency_key,
        }

    async def reconcile(
        self,
        *,
        idempotency_key: str,
    ) -> Mapping[str, Any] | None:
        return None

    async def healthcheck(self) -> Mapping[str, Any]:
        return {
            "channel": self.channel_name,
            "status": "ready" if self.enabled else "not_configured",
            "configured": self.configured,
            "enabled": self.enabled,
        }


__all__ = [
    "InAppTradingPlanAlertChannel",
    "TradingPlanAlertChannel",
    "WxPusherTradingPlanAlertChannel",
]
