import asyncio
import unittest
from datetime import date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from sqlalchemy import select
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.database import Base
from app.models.trading_playbook import TradingAlertEvent, TradingPlanVersion


class _RecordingInAppChannel:
    channel_name = "in_app"
    supports_provider_idempotency = False

    def __init__(self):
        self.sends = []

    async def send(self, event, *, idempotency_key):
        self.sends.append((dict(event), idempotency_key))
        return {"accepted": True}

    async def reconcile(self, *, idempotency_key):
        return None


class TradingPlaybookDurableAlertTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = TemporaryDirectory()
        path = Path(self.directory.name) / "alerts.db"
        self.engine = create_async_engine(
            f"sqlite+aiosqlite:///{path.as_posix()}"
        )
        self.second_engine = create_async_engine(
            f"sqlite+aiosqlite:///{path.as_posix()}"
        )
        self.Session = async_sessionmaker(self.engine, expire_on_commit=False)
        self.SecondSession = async_sessionmaker(
            self.second_engine,
            expire_on_commit=False,
        )
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        async with self.Session() as db:
            plan = TradingPlanVersion(
                source_trade_date=date(2026, 7, 13),
                target_trade_date=date(2026, 7, 14),
                stage="after_close",
                version_no=1,
                status="draft",
                market_state_json={},
                theme_ranking_json=[],
                mode_radar_json=[],
                rule_snapshot_json=[],
                risk_settings_json={},
                data_quality_json={"status": "ready"},
                change_summary_json={},
                input_hash="alert-plan",
                generated_at=datetime(2026, 7, 13, 15, 30),
            )
            db.add(plan)
            await db.commit()
            await db.refresh(plan)
            self.plan_id = plan.id
            self.plan = SimpleNamespace(
                id=plan.id,
                source_trade_date=plan.source_trade_date,
                target_trade_date=plan.target_trade_date,
                stage=plan.stage,
                status=plan.status,
            )

    async def asyncTearDown(self):
        await self.engine.dispose()
        await self.second_engine.dispose()
        self.directory.cleanup()

    async def _events(self):
        async with self.Session() as db:
            return list(
                (
                    await db.execute(
                        select(TradingAlertEvent).order_by(TradingAlertEvent.id)
                    )
                )
                .scalars()
                .all()
            )

    async def test_plan_ready_and_confirmation_are_persisted_and_sent_once(self):
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        channel = _RecordingInAppChannel()
        service = TradingPlaybookAlertService(channel)
        async with self.Session() as db:
            await service.notify_plan_ready(db, self.plan, send=True)
        async with self.Session() as db:
            await service.notify_plan_ready(db, self.plan, send=True)

        events = await self._events()
        self.assertEqual(
            [event.event_type for event in events],
            ["plan_ready", "confirmation_required"],
        )
        self.assertEqual(
            [event.dedup_key for event in events],
            [
                f"plan:{self.plan_id}:in_app:plan_ready",
                f"plan:{self.plan_id}:in_app:confirmation_required",
            ],
        )
        self.assertEqual(len(channel.sends), 2)
        self.assertEqual(
            {payload["id"] for payload, _key in channel.sends},
            {event.id for event in events},
        )
        self.assertTrue(
            all(payload["dedup_key"] for payload, _key in channel.sends)
        )
        self.assertTrue(
            all(
                event.channel_status_json["in_app"]["status"] == "delivered"
                for event in events
            )
        )

    async def test_accepted_send_then_delivered_write_failure_is_not_resent(self):
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        class CrashAfterAcceptance(TradingPlaybookAlertService):
            async def _mark_delivered(self, *_args, **_kwargs):
                raise RuntimeError("process died before delivered commit")

        channel = _RecordingInAppChannel()
        crashing = CrashAfterAcceptance(channel)
        with self.assertRaisesRegex(RuntimeError, "before delivered"):
            async with self.Session() as db:
                await crashing.emit_plan_event(
                    db,
                    self.plan,
                    event_type="plan_ready",
                    send=True,
                )

        event = (await self._events())[0]
        self.assertEqual(event.channel_status_json["in_app"]["status"], "sending")
        self.assertEqual(len(channel.sends), 1)

        takeover = TradingPlaybookAlertService(channel)
        async with self.Session() as db:
            await takeover.emit_plan_event(
                db,
                self.plan,
                event_type="plan_ready",
                send=True,
            )

        self.assertEqual(len(channel.sends), 1)
        self.assertEqual(len(await self._events()), 1)

    async def test_cancel_swallowed_after_acceptance_then_takeover_does_not_resend(self):
        from app.data_collectors.scheduler import (
            DataScheduler,
            TradingPlaybookClaimLost,
        )
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )
        from app.utils.time_utils import CN_TZ

        accepted = asyncio.Event()

        class CancellationSwallowingChannel(_RecordingInAppChannel):
            async def send(self, event, *, idempotency_key):
                self.sends.append((dict(event), idempotency_key))
                accepted.set()
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    return {"accepted": True, "cancel_swallowed": True}

        channel = CancellationSwallowingChannel()
        first = TradingPlaybookAlertService(channel)

        class LostClaimService:
            lease_seconds = 0.6

            async def renew(self, *_args, **_kwargs):
                return False

        class NullSessionContext:
            async def __aenter__(self):
                return object()

            async def __aexit__(self, *_args):
                return None

        scheduler = DataScheduler(
            session_factory=lambda: NullSessionContext(),
            now_provider=lambda: datetime.now(CN_TZ),
            job_claim_service=LostClaimService(),
        )

        async def notify_owner_one():
            async with self.Session() as db:
                return await first.emit_plan_event(
                    db,
                    self.plan,
                    event_type="plan_ready",
                    send=True,
                )

        with self.assertRaises(TradingPlaybookClaimLost):
            await scheduler._run_with_playbook_claim(
                SimpleNamespace(job_key="playbook:notify:plan:fenced"),
                notify_owner_one,
            )
        self.assertTrue(accepted.is_set())

        takeover = TradingPlaybookAlertService(channel)
        async with self.Session() as db:
            await takeover.emit_plan_event(
                db,
                self.plan,
                event_type="plan_ready",
                send=True,
            )

        events = await self._events()
        self.assertEqual(len(channel.sends), 1)
        self.assertEqual(len(events), 1)
        self.assertIn(
            events[0].channel_status_json["in_app"]["status"],
            {"sending", "delivered", "uncertain"},
        )

    async def test_cross_session_cas_allows_only_one_physical_send(self):
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        channel = _RecordingInAppChannel()
        first = TradingPlaybookAlertService(channel)
        second = TradingPlaybookAlertService(channel)
        async with self.Session() as db:
            await first.emit_plan_event(
                db,
                self.plan,
                event_type="plan_ready",
                send=False,
            )

        async def deliver(service, session_factory):
            async with session_factory() as db:
                return await service.emit_plan_event(
                    db,
                    self.plan,
                    event_type="plan_ready",
                    send=True,
                )

        await asyncio.gather(
            deliver(first, self.Session),
            deliver(second, self.SecondSession),
        )

        self.assertEqual(len(channel.sends), 1)
        event = (await self._events())[0]
        self.assertEqual(event.channel_status_json["in_app"]["status"], "delivered")

    def test_pending_cas_statement_compiles_for_postgresql_json(self):
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        service = TradingPlaybookAlertService(_RecordingInAppChannel())
        statement = service._pending_claim_statement(
            1,
            {
                "in_app": {
                    "status": "sending",
                    "idempotency_key": "plan:1:in_app:plan_ready",
                }
            },
        )

        compiled = str(statement.compile(dialect=postgresql.dialect()))
        self.assertIn("UPDATE trading_alert_events", compiled)
        self.assertIn("channel_status_json", compiled)
        self.assertIn("->>", compiled)

    async def test_disabled_in_app_setting_persists_without_sending(self):
        from app.models.trading_playbook import TradingPlaybookSettings
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        channel = _RecordingInAppChannel()
        async with self.Session() as db:
            db.add(
                TradingPlaybookSettings(
                    id=1,
                    enabled=True,
                    in_app_enabled=False,
                    wechat_enabled=False,
                )
            )
            await db.commit()
            await TradingPlaybookAlertService(channel).notify_plan_ready(
                db,
                self.plan,
                send=True,
            )

        events = await self._events()
        self.assertEqual(len(events), 2)
        self.assertEqual(len(channel.sends), 0)
        self.assertTrue(
            all(
                event.channel_status_json["in_app"]["status"] == "pending"
                for event in events
            )
        )

    async def test_in_app_channel_broadcast_carries_event_identity(self):
        from app.services.trading_playbook.channels import (
            InAppTradingPlanAlertChannel,
        )

        payload = {
            "id": 41,
            "dedup_key": "plan:9:in_app:plan_ready",
            "event_type": "plan_ready",
        }
        with patch(
            "app.services.trading_playbook.channels.manager."
            "broadcast_trading_plan_alert",
            AsyncMock(),
        ) as broadcast:
            receipt = await InAppTradingPlanAlertChannel().send(
                payload,
                idempotency_key="plan:9:in_app:plan_ready",
            )

        sent = broadcast.await_args.args[0]
        self.assertEqual(sent["id"], 41)
        self.assertEqual(sent["dedup_key"], payload["dedup_key"])
        self.assertEqual(sent["idempotency_key"], payload["dedup_key"])
        self.assertTrue(receipt["accepted"])

    def test_scheduler_rejects_non_durable_alert_service(self):
        from app.data_collectors.scheduler import DataScheduler

        scheduler = DataScheduler()
        with self.assertRaisesRegex(TypeError, "durable"):
            scheduler.install_trading_playbook_alert_service(
                SimpleNamespace(notify_plan_ready=lambda *_args: None)
            )


if __name__ == "__main__":
    unittest.main()
