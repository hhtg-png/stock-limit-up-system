import json
import unittest
from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx

from app.services.trading_playbook.alert_fanout import (
    TradingPlaybookScheduledAlertFanout,
)
from app.services.trading_playbook.channels import (
    WxPusherTradingPlanAlertChannel,
)


class WxPusherTradingPlanAlertChannelTests(unittest.IsolatedAsyncioTestCase):
    async def test_status_masks_secret_and_exposes_all_five_schedule_times(self):
        channel = WxPusherTradingPlanAlertChannel(
            "SPT_private-secret-1234",
            enabled=True,
            public_url="http://example.test/trading-playbook",
        )

        status = channel.status()

        self.assertTrue(status["configured"])
        self.assertTrue(status["enabled"])
        self.assertEqual(status["recipient_masked"], "SPT_****1234")
        self.assertNotIn("private-secret", str(status))
        self.assertEqual(
            status["schedule"],
            ["08:50", "09:26", "14:40", "15:10", "15:30"],
        )

    async def test_send_uses_spt_markdown_without_leaking_it_in_receipt(self):
        requests = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(json.loads(request.content.decode("utf-8")))
            return httpx.Response(
                200,
                json={"success": True, "code": 1000, "msg": "处理成功"},
            )

        channel = WxPusherTradingPlanAlertChannel(
            "SPT_private-secret-5678",
            enabled=True,
            public_url="http://example.test/trading-playbook",
            transport=httpx.MockTransport(handler),
        )
        event = {
            "event_type": "plan_ready",
            "message": "交易预案已生成：2026-07-20 overnight",
            "market_snapshot": {
                "stage": "overnight",
                "target_trade_date": "2026-07-20",
            },
        }

        receipt = await channel.send(event, idempotency_key="plan:1")

        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0]["spt"], "SPT_private-secret-5678")
        self.assertEqual(requests[0]["contentType"], 3)
        self.assertIn("08:50 隔夜预案", requests[0]["content"])
        self.assertIn("2026-07-20", requests[0]["content"])
        self.assertIn("http://example.test/trading-playbook", requests[0]["content"])
        self.assertNotIn("private-secret", str(receipt))

    async def test_rejected_provider_response_raises_without_secret(self):
        def handler(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={"success": False, "code": 1001, "msg": "无效凭证"},
            )

        channel = WxPusherTradingPlanAlertChannel(
            "SPT_private-secret-9999",
            enabled=True,
            public_url="",
            transport=httpx.MockTransport(handler),
        )

        with self.assertRaisesRegex(RuntimeError, "无效凭证") as raised:
            await channel.send(
                {"event_type": "review_ready", "market_snapshot": {}},
                idempotency_key="review:1",
            )
        self.assertNotIn("private-secret", str(raised.exception))


class TradingPlaybookScheduledAlertFanoutTests(unittest.IsolatedAsyncioTestCase):
    async def test_daily_plan_and_review_fan_out_but_monitor_stays_primary(self):
        primary = SimpleNamespace(
            notify_plan_ready=AsyncMock(return_value=["in-app-plan"]),
            notify_review_ready=AsyncMock(return_value="in-app-review"),
            monitor=AsyncMock(return_value="monitor-result"),
        )
        personal = SimpleNamespace(
            emit_plan_event=AsyncMock(return_value="wechat-plan"),
            notify_review_ready=AsyncMock(return_value="wechat-review"),
        )
        fanout = TradingPlaybookScheduledAlertFanout(primary, [personal])
        db = object()
        plan = SimpleNamespace(id=7)

        plan_result = await fanout.notify_plan_ready(db, plan, send=True)
        review_result = await fanout.notify_review_ready(
            db,
            plan,
            date(2026, 7, 20),
            send=True,
        )
        monitor_result = await fanout.monitor(db, "now")

        self.assertEqual(plan_result["scheduled"], ["wechat-plan"])
        self.assertEqual(review_result["scheduled"], ["wechat-review"])
        personal.emit_plan_event.assert_awaited_once_with(
            db,
            plan,
            event_type="plan_ready",
            send=True,
        )
        personal.notify_review_ready.assert_awaited_once()
        primary.monitor.assert_awaited_once_with(db, "now")
        self.assertEqual(monitor_result, "monitor-result")


if __name__ == "__main__":
    unittest.main()
