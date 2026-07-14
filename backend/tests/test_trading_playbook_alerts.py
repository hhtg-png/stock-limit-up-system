import asyncio
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from sqlalchemy import event, select, update
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.database import Base
from app.models.trading_playbook import (
    TradingAlertConditionState,
    TradingAlertEvent,
    TradingPlanCandidate,
    TradingPlanVersion,
    TradingPlaybookSettings,
)
from app.utils.time_utils import CN_TZ


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

    async def healthcheck(self):
        return {"channel": "in_app", "status": "ready", "connections": 0}


class _BatchQuoteAPI:
    def __init__(self, quotes, *, barrier=None):
        self.quotes = quotes
        self.calls = []
        self.barrier = barrier

    async def get_quotes_batch(self, codes):
        self.calls.append(list(codes))
        if self.barrier is not None:
            await self.barrier.wait()
        return {
            code: dict(self.quotes[code])
            for code in codes
            if code in self.quotes
        }


def _fresh_quote(code, price, *, captured_at="20260714100500"):
    return {
        "code": code,
        "price": price,
        "datetime": captured_at,
    }


class _TradingCalendar:
    def __init__(self, trading_days=(), *, ensure_error=None):
        self.trading_days = set(trading_days)
        self.ensure_error = ensure_error
        self.ensure_calls = []

    async def ensure_date(self, value):
        self.ensure_calls.append(value)
        if self.ensure_error is not None:
            raise self.ensure_error

    def is_trading_day(self, value):
        return value in self.trading_days


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
            db.add(
                TradingPlaybookSettings(
                    id=1,
                    enabled=True,
                    in_app_enabled=True,
                    wechat_enabled=False,
                )
            )
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

    async def test_real_delivered_commit_failure_uses_fresh_compensation(self):
        from app.data_collectors.scheduler import DataScheduler
        from app.models.trading_playbook import TradingPlaybookJobClaim
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )
        from app.utils.time_utils import CN_TZ

        channel = _RecordingInAppChannel()
        service = TradingPlaybookAlertService(
            channel,
            session_factory=self.SecondSession,
        )
        scheduler = DataScheduler(
            trading_playbook_alert_service=service,
            session_factory=self.Session,
            now_provider=lambda: CN_TZ.localize(
                datetime(2026, 7, 13, 15, 35)
            ),
        )
        commits = 0
        injected = False

        def fail_delivered_commit(_connection):
            nonlocal commits, injected
            commits += 1
            if commits == 6 and not injected:
                injected = True
                raise RuntimeError("real delivered commit failure")

        event.listen(self.engine.sync_engine, "commit", fail_delivered_commit)
        try:
            result = await scheduler._notify_trading_playbook_plan(self.plan)
        finally:
            event.remove(
                self.engine.sync_engine,
                "commit",
                fail_delivered_commit,
            )

        self.assertIsNone(result)
        self.assertTrue(injected)
        events = await self._events()
        statuses = {
            row.event_type: row.channel_status_json["in_app"]["status"]
            for row in events
        }
        self.assertEqual(statuses["plan_ready"], "uncertain")
        self.assertEqual(statuses["confirmation_required"], "pending")
        self.assertEqual(
            sum(
                payload["event_type"] == "plan_ready"
                for payload, _key in channel.sends
            ),
            1,
        )
        async with self.Session() as db:
            claim = (
                await db.execute(select(TradingPlaybookJobClaim))
            ).scalar_one()
        self.assertEqual(claim.status, "retry")

        await scheduler._notify_trading_playbook_plan(self.plan)
        events = await self._events()
        self.assertEqual(
            sum(
                payload["event_type"] == "plan_ready"
                for payload, _key in channel.sends
            ),
            1,
        )
        self.assertEqual(
            sum(
                payload["event_type"] == "confirmation_required"
                for payload, _key in channel.sends
            ),
            1,
        )
        self.assertTrue(
            any(
                row.event_type == "confirmation_required"
                and row.channel_status_json["in_app"]["status"]
                == "delivered"
                for row in events
            )
        )

    async def test_settings_lock_failure_returns_pre_send_to_pending_and_restart_sends(self):
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        channel = _RecordingInAppChannel()
        failing = TradingPlaybookAlertService(
            channel,
            session_factory=self.SecondSession,
        )
        async with self.Session() as db:
            row = await failing.emit_plan_event(
                db,
                self.plan,
                event_type="plan_ready",
                send=False,
            )
            event_id = row.id

        failing._lock_delivery_settings = AsyncMock(
            side_effect=RuntimeError("settings lock failed")
        )
        with self.assertRaisesRegex(RuntimeError, "settings lock failed"):
            async with self.Session() as db:
                row = await db.get(TradingAlertEvent, event_id)
                await failing._deliver(db, row)

        persisted = (await self._events())[0]
        self.assertEqual(
            persisted.channel_status_json["in_app"]["status"],
            "pending",
        )
        self.assertIn(
            "settings lock failed",
            persisted.channel_status_json["in_app"]["pre_send_error"],
        )
        self.assertEqual(channel.sends, [])

        restarted = TradingPlaybookAlertService(
            channel,
            session_factory=self.Session,
        )
        async with self.SecondSession() as db:
            row = await db.get(TradingAlertEvent, event_id)
            await restarted._deliver(db, row)

        self.assertEqual(len(channel.sends), 1)
        self.assertEqual(
            (await self._events())[0].channel_status_json["in_app"]["status"],
            "delivered",
        )

    async def test_channel_started_commit_failure_recovers_before_send(self):
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        channel = _RecordingInAppChannel()
        failing = TradingPlaybookAlertService(
            channel,
            session_factory=self.SecondSession,
        )
        async with self.Session() as db:
            row = await failing.emit_plan_event(
                db,
                self.plan,
                event_type="plan_ready",
                send=False,
            )
            event_id = row.id

        commits = 0

        def fail_second_commit(_session):
            nonlocal commits
            commits += 1
            if commits == 2:
                raise RuntimeError("channel-started commit failed")

        async with self.Session() as db:
            event.listen(db.sync_session, "before_commit", fail_second_commit)
            try:
                row = await db.get(TradingAlertEvent, event_id)
                with self.assertRaisesRegex(
                    RuntimeError,
                    "channel-started commit failed",
                ):
                    await failing._deliver(db, row)
            finally:
                event.remove(
                    db.sync_session,
                    "before_commit",
                    fail_second_commit,
                )

        self.assertEqual(commits, 2)
        self.assertEqual(channel.sends, [])
        persisted = (await self._events())[0]
        self.assertEqual(
            persisted.channel_status_json["in_app"]["status"],
            "pending",
        )
        self.assertIn(
            "channel-started commit failed",
            persisted.channel_status_json["in_app"]["pre_send_error"],
        )

        restarted = TradingPlaybookAlertService(
            channel,
            session_factory=self.Session,
        )
        async with self.SecondSession() as db:
            row = await db.get(TradingAlertEvent, event_id)
            await restarted._deliver(db, row)

        self.assertEqual(len(channel.sends), 1)
        self.assertEqual(
            (await self._events())[0].channel_status_json["in_app"]["status"],
            "delivered",
        )

    async def test_second_settings_lock_failure_after_fence_recovers_before_send(self):
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        channel = _RecordingInAppChannel()
        failing = TradingPlaybookAlertService(
            channel,
            session_factory=self.SecondSession,
        )
        async with self.Session() as db:
            row = await failing.emit_plan_event(
                db,
                self.plan,
                event_type="plan_ready",
                send=False,
            )
            event_id = row.id

        lock_settings = failing._lock_delivery_settings
        lock_calls = 0

        async def fail_second_lock(db):
            nonlocal lock_calls
            lock_calls += 1
            if lock_calls == 2:
                raise RuntimeError("second settings lock failed")
            return await lock_settings(db)

        failing._lock_delivery_settings = fail_second_lock
        with self.assertRaisesRegex(
            RuntimeError,
            "second settings lock failed",
        ):
            async with self.Session() as db:
                row = await db.get(TradingAlertEvent, event_id)
                await failing._deliver(db, row)

        self.assertEqual(lock_calls, 2)
        self.assertEqual(channel.sends, [])
        persisted = (await self._events())[0]
        self.assertEqual(
            persisted.channel_status_json["in_app"]["status"],
            "pending",
        )
        self.assertIn(
            "second settings lock failed",
            persisted.channel_status_json["in_app"]["pre_send_error"],
        )

        restarted = TradingPlaybookAlertService(
            channel,
            session_factory=self.Session,
        )
        async with self.SecondSession() as db:
            row = await db.get(TradingAlertEvent, event_id)
            await restarted._deliver(db, row)
        self.assertEqual(len(channel.sends), 1)

    async def test_cancellation_before_channel_fence_recovers_then_reraises(self):
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        channel = _RecordingInAppChannel()
        service = TradingPlaybookAlertService(
            channel,
            session_factory=self.SecondSession,
        )
        async with self.Session() as db:
            row = await service.emit_plan_event(
                db,
                self.plan,
                event_type="plan_ready",
                send=False,
            )
            event_id = row.id

        lock_started = asyncio.Event()

        async def block_settings_lock(_db):
            lock_started.set()
            await asyncio.Event().wait()

        service._lock_delivery_settings = block_settings_lock

        async def deliver():
            async with self.Session() as db:
                row = await db.get(TradingAlertEvent, event_id)
                await service._deliver(db, row)

        task = asyncio.create_task(deliver())
        await asyncio.wait_for(lock_started.wait(), timeout=1)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        self.assertEqual(channel.sends, [])
        self.assertEqual(
            (await self._events())[0].channel_status_json["in_app"]["status"],
            "pending",
        )

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

    def test_action_due_statement_filters_future_before_limit_for_postgresql(self):
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        service = TradingPlaybookAlertService(_RecordingInAppChannel())
        compiled = str(
            service._recoverable_action_events_statement(
                date(2026, 7, 14)
            ).compile(
                dialect=postgresql.dialect(),
                compile_kwargs={"literal_binds": True},
            )
        )

        self.assertIn("action:2026-07-14:%", compiled)
        self.assertIn("action:____-__-__:%", compiled)
        self.assertIn("NOT LIKE", compiled)
        self.assertIn("LIMIT 100", compiled)

    async def test_disabled_in_app_setting_persists_without_sending(self):
        from app.models.trading_playbook import TradingPlaybookSettings
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        channel = _RecordingInAppChannel()
        async with self.Session() as db:
            settings = await db.get(TradingPlaybookSettings, 1)
            settings.in_app_enabled = False
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
                event.channel_status_json["in_app"]["status"] == "skipped"
                for event in events
            )
        )
        self.assertTrue(
            all(
                event.channel_status_json["in_app"]["reason"] == "disabled"
                and event.channel_status_json["in_app"]["skipped_at"]
                for event in events
            )
        )

        async with self.Session() as db:
            settings = await db.get(TradingPlaybookSettings, 1)
            settings.in_app_enabled = True
            await db.commit()
            await TradingPlaybookAlertService(channel).notify_plan_ready(
                db,
                self.plan,
                send=True,
            )

        self.assertEqual(channel.sends, [])
        self.assertTrue(
            all(
                event.channel_status_json["in_app"]["status"] == "skipped"
                for event in await self._events()
            )
        )

    async def test_send_holds_sqlite_settings_lock_until_delivery_commit(self):
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        started = asyncio.Event()
        release = asyncio.Event()

        class BlockingChannel(_RecordingInAppChannel):
            async def send(self, event, *, idempotency_key):
                started.set()
                await release.wait()
                return await super().send(
                    event,
                    idempotency_key=idempotency_key,
                )

        channel = BlockingChannel()
        service = TradingPlaybookAlertService(channel)
        async with self.Session() as db:
            event_row = await service.emit_plan_event(
                db,
                self.plan,
                event_type="plan_ready",
                send=False,
            )
            event_id = event_row.id

        async def deliver():
            async with self.Session() as db:
                row = await db.get(TradingAlertEvent, event_id)
                await service._deliver(db, row)

        async def disable():
            async with self.SecondSession() as db:
                await db.execute(
                    update(TradingPlaybookSettings)
                    .where(TradingPlaybookSettings.id == 1)
                    .values(in_app_enabled=False)
                )
                await db.commit()

        delivery_task = asyncio.create_task(deliver())
        await asyncio.wait_for(started.wait(), timeout=1)
        disable_task = asyncio.create_task(disable())
        await asyncio.sleep(0.05)
        try:
            self.assertFalse(disable_task.done())
        finally:
            release.set()
            await asyncio.gather(
                delivery_task,
                disable_task,
                return_exceptions=True,
            )

        self.assertEqual(len(channel.sends), 1)
        async with self.Session() as db:
            settings = await db.get(TradingPlaybookSettings, 1)
            persisted = await db.get(TradingAlertEvent, event_id)
        self.assertFalse(settings.in_app_enabled)
        self.assertEqual(
            persisted.channel_status_json["in_app"]["status"],
            "delivered",
        )

    async def test_disable_committed_first_skips_claimed_event_without_send(self):
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        channel = _RecordingInAppChannel()
        service = TradingPlaybookAlertService(channel)
        async with self.Session() as db:
            event_row = await service.emit_plan_event(
                db,
                self.plan,
                event_type="plan_ready",
                send=False,
            )
            event_id = event_row.id
        async with self.SecondSession() as db:
            await db.execute(
                update(TradingPlaybookSettings)
                .where(TradingPlaybookSettings.id == 1)
                .values(in_app_enabled=False)
            )
            await db.commit()
        async with self.Session() as db:
            row = await db.get(TradingAlertEvent, event_id)
            await service._deliver(db, row)

        self.assertEqual(channel.sends, [])
        event_row = (await self._events())[0]
        self.assertEqual(event_row.channel_status_json["in_app"]["status"], "skipped")
        self.assertEqual(event_row.channel_status_json["in_app"]["reason"], "disabled")

    async def test_missing_settings_fails_closed_after_pending_claim(self):
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        channel = _RecordingInAppChannel()
        service = TradingPlaybookAlertService(channel)
        async with self.Session() as db:
            event_row = await service.emit_plan_event(
                db,
                self.plan,
                event_type="plan_ready",
                send=False,
            )
            event_id = event_row.id
            settings = await db.get(TradingPlaybookSettings, 1)
            await db.delete(settings)
            await db.commit()
        async with self.Session() as db:
            row = await db.get(TradingAlertEvent, event_id)
            await service._deliver(db, row)

        self.assertEqual(channel.sends, [])
        event_row = (await self._events())[0]
        self.assertEqual(event_row.channel_status_json["in_app"]["status"], "skipped")
        self.assertEqual(
            event_row.channel_status_json["in_app"]["reason"],
            "settings_missing",
        )

    def test_settings_delivery_lock_statements_compile_per_dialect(self):
        from sqlalchemy.dialects import sqlite

        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        service = TradingPlaybookAlertService(_RecordingInAppChannel())
        sqlite_sql = str(
            service._settings_lock_statement("sqlite").compile(
                dialect=sqlite.dialect()
            )
        )
        postgres_sql = str(
            service._settings_lock_statement("postgresql").compile(
                dialect=postgresql.dialect()
            )
        )

        self.assertIn("UPDATE trading_playbook_settings", sqlite_sql)
        self.assertIn("FOR UPDATE", postgres_sql)

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
        broadcast.assert_awaited_once_with(sent, stock_code=None)
        self.assertTrue(receipt["accepted"])

    async def test_in_app_channel_passes_action_stock_to_subscription_filter(self):
        from app.services.trading_playbook.channels import (
            InAppTradingPlanAlertChannel,
        )

        payload = {
            "id": 42,
            "dedup_key": "action:2026-07-14:1:2:mode:entry_triggered",
            "event_type": "entry_triggered",
            "stock_code": "000001",
        }
        with patch(
            "app.services.trading_playbook.channels.manager."
            "broadcast_trading_plan_alert",
            AsyncMock(),
        ) as broadcast:
            await InAppTradingPlanAlertChannel().send(
                payload,
                idempotency_key=payload["dedup_key"],
            )

        sent = broadcast.await_args.args[0]
        broadcast.assert_awaited_once_with(sent, stock_code="000001")

    def test_scheduler_rejects_non_durable_alert_service(self):
        from app.data_collectors.scheduler import DataScheduler

        scheduler = DataScheduler()
        with self.assertRaisesRegex(TypeError, "durable"):
            scheduler.install_trading_playbook_alert_service(
                SimpleNamespace(notify_plan_ready=lambda *_args: None)
            )

    def test_channel_protocol_rejects_missing_healthcheck(self):
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        channel = SimpleNamespace(
            channel_name="in_app",
            supports_provider_idempotency=False,
            send=AsyncMock(),
            reconcile=AsyncMock(),
        )
        with self.assertRaisesRegex(TypeError, "healthcheck"):
            TradingPlaybookAlertService(channel)

    async def test_in_app_healthcheck_reports_real_manager_connections(self):
        from app.core.websocket_manager import ConnectionManager
        from app.services.trading_playbook.channels import (
            InAppTradingPlanAlertChannel,
        )

        local_manager = ConnectionManager()
        await local_manager.connect(AsyncMock(), "health-client")
        with patch(
            "app.services.trading_playbook.channels.manager",
            local_manager,
        ):
            result = await InAppTradingPlanAlertChannel().healthcheck()

        self.assertEqual(
            result,
            {"channel": "in_app", "status": "ready", "connections": 1},
        )


class TradingPlaybookActionMonitorTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = TemporaryDirectory()
        path = Path(self.directory.name) / "action-alerts.db"
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
            db.add(
                TradingPlaybookSettings(
                    id=1,
                    enabled=True,
                    in_app_enabled=True,
                    wechat_enabled=False,
                )
            )
            await db.commit()
        self.today = date(2026, 7, 14)
        self.now = CN_TZ.localize(datetime(2026, 7, 14, 10, 5))
        self.calendar = _TradingCalendar({self.today})

    def _monitor_service(self, channel, **kwargs):
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        return TradingPlaybookAlertService(
            channel,
            trading_calendar=self.calendar,
            **kwargs,
        )

    async def asyncTearDown(self):
        await self.engine.dispose()
        await self.second_engine.dispose()
        self.directory.cleanup()

    async def _create_candidate(
        self,
        *,
        plan_status="active",
        action_trade_date=None,
        stock_code="000001",
        mode_key="leader_turn_two",
        entry=None,
        invalidation=None,
        exit_trigger=None,
        target_trade_date=None,
        stage="auction",
    ):
        async with self.Session() as db:
            plan = TradingPlanVersion(
                source_trade_date=date(2026, 7, 13),
                target_trade_date=target_trade_date or self.today,
                stage=stage,
                version_no=1,
                status=plan_status,
                market_state_json={},
                theme_ranking_json=[],
                mode_radar_json=[],
                rule_snapshot_json=[],
                risk_settings_json={},
                data_quality_json={"status": "ready"},
                change_summary_json={},
                input_hash=f"monitor-{stock_code}-{mode_key}",
                generated_at=datetime(2026, 7, 14, 9, 26),
                confirmed_at=datetime(2026, 7, 14, 9, 27),
                confirmed_by="tester",
            )
            db.add(plan)
            await db.flush()
            candidate = TradingPlanCandidate(
                plan_version_id=plan.id,
                stock_code=stock_code,
                stock_name=f"股票{stock_code}",
                action_trade_date=action_trade_date or self.today,
                theme_name="测试题材",
                primary_mode_key=mode_key,
                supporting_mode_keys_json=[],
                role="leader",
                rank=1,
                recognition_json={},
                entry_trigger_json=entry or {"price_gte": 10},
                invalidation_json=invalidation or {},
                exit_trigger_json=exit_trigger or {},
                risk_level="trial",
                position_reference=10,
                evidence_json=[],
                manual_overrides_json={},
                status="waiting",
            )
            db.add(candidate)
            await db.commit()
            await db.refresh(plan)
            await db.refresh(candidate)
            return plan.id, candidate.id

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

    async def _seed_action_event(
        self,
        plan_id,
        candidate_id,
        *,
        action_date,
        status="pending",
        suffix="seed",
        triggered_at=None,
        owner="stopped-worker",
        channel_started_at=None,
        event_type="entry_triggered",
    ):
        snapshot = {"stock_code": "000001"}
        if isinstance(action_date, date):
            snapshot["trade_date"] = action_date.isoformat()
        elif action_date is not None:
            snapshot["trade_date"] = action_date
        if isinstance(action_date, date):
            dedup_key = f"action:{action_date.isoformat()}:{suffix}"
        elif action_date is not None:
            dedup_key = f"action:{action_date}:{suffix}"
        else:
            dedup_key = f"action:{suffix}"
        channel_status = {
            "status": status,
            "idempotency_key": dedup_key,
            "attempts": 1 if status == "sending" else 0,
        }
        if status == "sending":
            channel_status.update(
                {
                    "owner": owner,
                    "sending_at": "2026-07-14T09:59:00+08:00",
                }
            )
        if channel_started_at is not None:
            channel_status["channel_started_at"] = channel_started_at
        async with self.Session() as db:
            row = TradingAlertEvent(
                plan_version_id=plan_id,
                candidate_id=candidate_id,
                event_type=event_type,
                severity=(
                    "action" if event_type == "entry_triggered" else "info"
                ),
                dedup_key=dedup_key,
                triggered_at=triggered_at
                or datetime(2026, 7, 14, 10, 0),
                market_snapshot_json=snapshot,
                message=f"seed action {suffix}",
                channel_status_json={"in_app": channel_status},
            )
            db.add(row)
            await db.commit()
            await db.refresh(row)
            return row.id

    async def test_after_hours_restart_drains_today_pending_action(self):
        plan_id, candidate_id = await self._create_candidate()
        event_id = await self._seed_action_event(
            plan_id,
            candidate_id,
            action_date=self.today,
            suffix="after-hours",
        )
        channel = _RecordingInAppChannel()
        after_hours = CN_TZ.localize(datetime(2026, 7, 14, 15, 5))

        async with self.Session() as db:
            events = await self._monitor_service(channel).monitor(
                db,
                after_hours,
            )

        self.assertEqual([event.id for event in events], [event_id])
        self.assertEqual(len(channel.sends), 1)
        self.assertEqual(
            (await self._events())[0].channel_status_json["in_app"]["status"],
            "delivered",
        )

    async def test_calendar_failure_still_drains_today_pending_action(self):
        plan_id, candidate_id = await self._create_candidate()
        await self._seed_action_event(
            plan_id,
            candidate_id,
            action_date=self.today,
            suffix="calendar-failure",
        )
        self.calendar.ensure_error = RuntimeError("calendar offline")
        channel = _RecordingInAppChannel()

        async with self.Session() as db:
            await self._monitor_service(channel).monitor(db, self.now)

        self.assertEqual(len(channel.sends), 1)
        self.assertEqual(
            (await self._events())[0].channel_status_json["in_app"]["status"],
            "delivered",
        )

    async def test_next_day_restart_terminalizes_stale_and_leaves_future_pending(self):
        plan_id, candidate_id = await self._create_candidate()
        stale_id = await self._seed_action_event(
            plan_id,
            candidate_id,
            action_date=date(2026, 7, 13),
            suffix="stale",
        )
        future_id = await self._seed_action_event(
            plan_id,
            candidate_id,
            action_date=date(2026, 7, 15),
            suffix="future",
            triggered_at=datetime(2026, 7, 14, 10, 1),
        )
        channel = _RecordingInAppChannel()

        async with self.Session() as db:
            await self._monitor_service(channel).monitor(db, self.now)

        events = {event.id: event for event in await self._events()}
        self.assertEqual(channel.sends, [])
        self.assertEqual(
            events[stale_id].channel_status_json["in_app"]["status"],
            "skipped",
        )
        self.assertEqual(
            events[stale_id].channel_status_json["in_app"]["reason"],
            "stale",
        )
        self.assertEqual(
            events[future_id].channel_status_json["in_app"]["status"],
            "pending",
        )

    async def test_malformed_action_date_is_terminal_invalid(self):
        plan_id, candidate_id = await self._create_candidate()
        malformed_id = await self._seed_action_event(
            plan_id,
            candidate_id,
            action_date="2026-7-14",
            suffix="not-a-date:key",
        )
        missing_id = await self._seed_action_event(
            plan_id,
            candidate_id,
            action_date=None,
            suffix="missing-date",
            triggered_at=datetime(2026, 7, 14, 10, 1),
        )

        async with self.Session() as db:
            await self._monitor_service(_RecordingInAppChannel()).monitor(
                db,
                self.now,
            )

        events = {event.id: event for event in await self._events()}
        for event_id in (malformed_id, missing_id):
            channel_status = events[event_id].channel_status_json["in_app"]
            self.assertEqual(channel_status["status"], "skipped")
            self.assertEqual(channel_status["reason"], "invalid_action_date")

    async def test_restart_recovers_pre_send_sending_action_once(self):
        plan_id, candidate_id = await self._create_candidate()
        event_id = await self._seed_action_event(
            plan_id,
            candidate_id,
            action_date=self.today,
            status="sending",
            suffix="recoverable-sending",
        )
        channel = _RecordingInAppChannel()

        async with self.SecondSession() as db:
            await self._monitor_service(channel).monitor(db, self.now)

        self.assertEqual(len(channel.sends), 1)
        event = next(row for row in await self._events() if row.id == event_id)
        self.assertEqual(
            event.channel_status_json["in_app"]["status"],
            "delivered",
        )

    async def test_action_drain_excludes_plan_events_and_post_fence_sending(self):
        plan_id, candidate_id = await self._create_candidate()
        plan_event_id = await self._seed_action_event(
            plan_id,
            candidate_id,
            action_date=None,
            suffix="non-action-plan-ready",
            event_type="plan_ready",
        )
        fenced_id = await self._seed_action_event(
            plan_id,
            candidate_id,
            action_date=self.today,
            status="sending",
            suffix="channel-already-started",
            triggered_at=datetime(2026, 7, 14, 10, 1),
            channel_started_at="2026-07-14T10:00:00+08:00",
        )
        channel = _RecordingInAppChannel()
        after_hours = CN_TZ.localize(datetime(2026, 7, 14, 15, 5))

        async with self.Session() as db:
            await self._monitor_service(channel).monitor(db, after_hours)

        events = {event.id: event for event in await self._events()}
        self.assertEqual(channel.sends, [])
        self.assertEqual(
            events[plan_event_id].channel_status_json["in_app"]["status"],
            "pending",
        )
        self.assertEqual(
            events[fenced_id].channel_status_json["in_app"]["status"],
            "sending",
        )

    async def test_two_engines_drain_one_pending_action_once_after_hours(self):
        plan_id, candidate_id = await self._create_candidate()
        await self._seed_action_event(
            plan_id,
            candidate_id,
            action_date=self.today,
            suffix="two-engine-drain",
        )
        channel = _RecordingInAppChannel()
        after_hours = CN_TZ.localize(datetime(2026, 7, 14, 15, 5))

        async def run(service, sessions):
            async with sessions() as db:
                return await service.monitor(db, after_hours)

        await asyncio.gather(
            run(self._monitor_service(channel), self.Session),
            run(self._monitor_service(channel), self.SecondSession),
        )

        self.assertEqual(len(channel.sends), 1)
        self.assertEqual(
            (await self._events())[0].channel_status_json["in_app"]["status"],
            "delivered",
        )

    async def test_action_drain_batch_is_oldest_first_and_limited_to_100(self):
        plan_id, candidate_id = await self._create_candidate()
        event_ids = []
        for index in range(101):
            event_ids.append(
                await self._seed_action_event(
                    plan_id,
                    candidate_id,
                    action_date=date(2026, 7, 13),
                    suffix=f"batch-{index:03d}",
                    triggered_at=datetime(2026, 7, 13, 9, 0)
                    + timedelta(seconds=index),
                )
            )

        async with self.Session() as db:
            await self._monitor_service(_RecordingInAppChannel()).monitor(
                db,
                self.now,
            )

        events = {event.id: event for event in await self._events()}
        self.assertTrue(
            all(
                events[event_id].channel_status_json["in_app"]["status"]
                == "skipped"
                for event_id in event_ids[:100]
            )
        )
        self.assertEqual(
            events[event_ids[100]].channel_status_json["in_app"]["status"],
            "pending",
        )

    async def test_future_actions_do_not_starve_today_outside_first_100(self):
        plan_id, candidate_id = await self._create_candidate()
        future_ids = []
        for index in range(100):
            future_ids.append(
                await self._seed_action_event(
                    plan_id,
                    candidate_id,
                    action_date=date(2026, 7, 15),
                    suffix=f"future-starvation-{index:03d}",
                    triggered_at=datetime(2026, 7, 13, 9, 0)
                    + timedelta(seconds=index),
                )
            )
        today_id = await self._seed_action_event(
            plan_id,
            candidate_id,
            action_date=self.today,
            suffix="today-after-future-batch",
            triggered_at=datetime(2026, 7, 14, 10, 0),
        )
        channel = _RecordingInAppChannel()
        after_hours = CN_TZ.localize(datetime(2026, 7, 14, 15, 5))

        for sessions in (self.Session, self.SecondSession):
            async with sessions() as db:
                await self._monitor_service(channel).monitor(db, after_hours)

        events = {event.id: event for event in await self._events()}
        self.assertEqual(
            [payload[0]["id"] for payload in channel.sends],
            [today_id],
        )
        self.assertEqual(
            events[today_id].channel_status_json["in_app"]["status"],
            "delivered",
        )
        self.assertTrue(
            all(
                events[event_id].channel_status_json["in_app"]["status"]
                == "pending"
                for event_id in future_ids
            )
        )

    async def test_unconfirmed_candidate_is_watch_only(self):
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        service = TradingPlaybookAlertService(_RecordingInAppChannel())
        events = await service.evaluate_candidate(
            "draft",
            {
                "id": 1,
                "entry_trigger_json": {"price_gte": 10},
                "invalidation_json": {"price_lte": 11},
                "exit_trigger_json": {"change_pct_gte": 2},
            },
            {"price": 10.5, "change_pct": 3},
        )

        self.assertEqual([row["event_type"] for row in events], ["watch"])

    async def test_confirmed_candidate_uses_invalidation_exit_entry_priority(self):
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        service = TradingPlaybookAlertService(_RecordingInAppChannel())
        candidates = [
            {
                "id": 1,
                "invalidation_json": {"price_lte": 11},
                "exit_trigger_json": {"change_pct_gte": 2},
                "entry_trigger_json": {"price_gte": 10},
            },
            {
                "id": 2,
                "invalidation_json": {"price_lte": 9},
                "exit_trigger_json": {"change_pct_gte": 2},
                "entry_trigger_json": {"price_gte": 10},
            },
            {
                "id": 3,
                "invalidation_json": {"price_lte": 9},
                "exit_trigger_json": {"change_pct_lte": -2},
                "entry_trigger_json": {"price_gte": 10},
            },
        ]

        actual = []
        for candidate in candidates:
            result = await service.evaluate_candidate(
                "confirmed",
                candidate,
                {"price": 10.5, "change_pct": 3},
            )
            actual.append(result[0]["event_type"])

        self.assertEqual(
            actual,
            ["invalidated", "exit_triggered", "entry_triggered"],
        )

    async def test_all_supported_conditions_match_and_metadata_is_ignored(self):
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        service = TradingPlaybookAlertService(_RecordingInAppChannel())
        events = await service.evaluate_candidate(
            "active",
            {
                "id": 4,
                "invalidation_json": {},
                "exit_trigger_json": {},
                "entry_trigger_json": {
                    "label": "突破确认",
                    "reference_price": 10,
                    "price_gte": 10,
                    "price_lte": 11,
                    "change_pct_gte": 2,
                    "change_pct_lte": 5,
                    "sealed": True,
                    "open_count_gte": 1,
                },
            },
            {
                "price": 10.5,
                "change_pct": 3,
                "sealed": True,
                "open_count": 2,
            },
        )

        self.assertEqual(events[0]["event_type"], "entry_triggered")
        metadata_only = await service.evaluate_candidate(
            "active",
            {
                "id": 5,
                "invalidation_json": {},
                "exit_trigger_json": {},
                "entry_trigger_json": {
                    "label": "仅说明",
                    "reference_price": 10,
                },
            },
            {"price": 10.5},
        )
        self.assertEqual(metadata_only, [])

    async def test_missing_bad_and_nonfinite_values_fail_closed(self):
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        service = TradingPlaybookAlertService(_RecordingInAppChannel())
        cases = [
            ({"price_gte": 10}, {}),
            ({"price_gte": "bad"}, {"price": 11}),
            ({"price_gte": 10}, {"price": float("nan")}),
            ({"change_pct_lte": -2}, {"change_pct": None}),
            ({"sealed": True}, {"sealed": "yes"}),
            ({"open_count_gte": 1}, {"open_count": "unknown"}),
            ({"unsupported": 1}, {"unsupported": 1}),
        ]
        for index, (condition, quote) in enumerate(cases, start=10):
            with self.subTest(condition=condition, quote=quote):
                events = await service.evaluate_candidate(
                    "active",
                    {
                        "id": index,
                        "invalidation_json": {},
                        "exit_trigger_json": {},
                        "entry_trigger_json": condition,
                    },
                    quote,
                )
                self.assertEqual(events, [])

    async def test_condition_result_distinguishes_unknown_from_recovery(self):
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        condition = {"price_gte": 10, "change_pct_gte": 2}

        self.assertIsNone(
            TradingPlaybookAlertService._condition_result(
                condition,
                {"price": 11, "_missing_fields": ["change_pct"]},
            )
        )
        self.assertFalse(
            TradingPlaybookAlertService._condition_result(
                condition,
                {"price": 9, "_missing_fields": ["change_pct"]},
            )
        )
        self.assertTrue(
            TradingPlaybookAlertService._condition_result(
                condition,
                {"price": 11, "change_pct": 3},
            )
        )

    def test_condition_version_is_stable_and_event_specific(self):
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        first = TradingPlaybookAlertService._condition_version(
            "entry_triggered",
            {"price_gte": 10, "nested": {"b": 2.0, "a": 1}},
        )
        reordered = TradingPlaybookAlertService._condition_version(
            "entry_triggered",
            {"nested": {"a": 1.0, "b": 2}, "price_gte": 10.0},
        )
        different_event = TradingPlaybookAlertService._condition_version(
            "invalidated",
            {"price_gte": 10, "nested": {"b": 2, "a": 1}},
        )

        self.assertEqual(first, reordered)
        self.assertEqual(len(first), 64)
        self.assertNotEqual(first, different_event)

    def test_condition_state_statements_compile_for_postgresql(self):
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        observed_at = datetime(2026, 7, 14, 10, 5)
        insert_sql = str(
            TradingPlaybookAlertService._condition_state_insert_statement(
                "postgresql",
                {
                    "candidate_id": 1,
                    "event_type": "entry_triggered",
                    "condition_version": "a" * 64,
                    "active": False,
                    "occurrence_no": 0,
                    "updated_at": observed_at,
                },
            ).compile(dialect=postgresql.dialect())
        )
        activate_sql = str(
            TradingPlaybookAlertService._condition_activate_statement(
                1,
                "entry_triggered",
                "a" * 64,
                observed_at,
            ).compile(dialect=postgresql.dialect())
        )
        recover_sql = str(
            TradingPlaybookAlertService._condition_recover_statement(
                1,
                observed_at,
            ).compile(dialect=postgresql.dialect())
        )

        self.assertIn(
            "ON CONFLICT (candidate_id, event_type, condition_version) DO NOTHING",
            insert_sql,
        )
        self.assertIn("occurrence_no +", activate_sql)
        self.assertIn("active IS false", activate_sql)
        self.assertIn("active IS true", recover_sql)

    async def test_real_tencent_missing_numeric_fields_never_trigger_terminal_actions(self):
        from app.data_collectors.tencent_api import TencentStockAPI

        def parsed_quote(
            code,
            *,
            price="10",
            change_pct="1",
            bid1_volume="100",
            limit_up="11",
        ):
            fields = ["0"] * 50
            fields[1] = f"股票{code}"
            fields[2] = code
            fields[3] = price
            fields[4] = "10"
            fields[10] = bid1_volume
            fields[30] = "20260714100500"
            fields[32] = change_pct
            fields[47] = limit_up
            return TencentStockAPI()._parse_response(
                f'v_test="{"~".join(fields)}";'
            )

        candidate_ids = []
        for index, (code, invalidation) in enumerate(
            (
                ("000001", {"price_lte": 9}),
                ("000002", {"change_pct_gte": -1}),
                ("000003", {"sealed": True}),
            )
        ):
            _plan_id, candidate_id = await self._create_candidate(
                stock_code=code,
                mode_key=f"quality-{code}",
                entry={"price_gte": 999},
                invalidation=invalidation,
                target_trade_date=self.today + timedelta(days=index),
            )
            candidate_ids.append(candidate_id)

        quote_api = _BatchQuoteAPI(
            {
                "000001": parsed_quote("000001", price=""),
                "000002": parsed_quote("000002", change_pct="bad"),
                "000003": parsed_quote(
                    "000003",
                    price="",
                    limit_up="",
                ),
            }
        )
        async with self.Session() as db:
            result = await self._monitor_service(
                _RecordingInAppChannel(),
                quote_api=quote_api,
            ).monitor(db, self.now)
        async with self.Session() as db:
            candidate_statuses = [
                (await db.get(TradingPlanCandidate, candidate_id)).status
                for candidate_id in candidate_ids
            ]

        self.assertEqual(result, [])
        self.assertEqual(await self._events(), [])
        self.assertEqual(candidate_statuses, ["waiting", "waiting", "waiting"])

    async def test_authoritative_fresh_open_count_snapshot_triggers_condition(self):
        from app.services.realtime_limit_up_service import RealtimeLimitUpSnapshot

        await self._create_candidate(entry={"open_count_gte": 2})
        loader = AsyncMock(
            return_value=RealtimeLimitUpSnapshot(
                items=[
                    {
                        "stock_code": "000001",
                        "open_count": 2,
                        "_collected_at": self.now - timedelta(seconds=5),
                    }
                ],
                authoritative=True,
                complete=True,
                evidence_trade_date=self.today,
            )
        )
        async with self.Session() as db:
            events = await self._monitor_service(
                _RecordingInAppChannel(),
                quote_api=_BatchQuoteAPI(
                    {"000001": _fresh_quote("000001", 10.5)}
                ),
                realtime_limit_up_loader=loader,
            ).monitor(db, self.now)

        self.assertEqual([event.event_type for event in events], ["entry_triggered"])
        loader.assert_awaited_once_with(self.today)
        self.assertEqual(
            events[0].market_snapshot_json["quote"]["open_count"],
            2,
        )

    async def test_unusable_open_count_evidence_is_unknown_not_zero(self):
        from app.services.realtime_limit_up_service import RealtimeLimitUpSnapshot

        for index, code in enumerate(("000001", "000002", "000003")):
            await self._create_candidate(
                stock_code=code,
                mode_key=f"open-quality-{code}",
                entry={"open_count_gte": 0},
                target_trade_date=self.today + timedelta(days=index),
            )
        loader = AsyncMock(
            return_value=RealtimeLimitUpSnapshot(
                items=[
                    {
                        "stock_code": "000002",
                        "open_count": True,
                        "_collected_at": self.now,
                    },
                    {
                        "stock_code": "000003",
                        "open_count": 3,
                        "_collected_at": self.now - timedelta(minutes=5),
                    },
                ],
                authoritative=True,
                complete=True,
                evidence_trade_date=self.today,
            )
        )
        async with self.Session() as db:
            events = await self._monitor_service(
                _RecordingInAppChannel(),
                quote_api=_BatchQuoteAPI(
                    {
                        code: _fresh_quote(code, 10.5)
                        for code in ("000001", "000002", "000003")
                    }
                ),
                realtime_limit_up_loader=loader,
            ).monitor(db, self.now)

        self.assertEqual(events, [])
        self.assertEqual(await self._events(), [])

    async def test_open_count_timeout_is_cancelled_without_blocking_price_condition(self):
        await self._create_candidate(
            entry={"price_gte": 10},
            invalidation={"open_count_gte": 1},
        )
        cancelled = asyncio.Event()

        async def slow_loader(_trade_date):
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        async with self.Session() as db:
            events = await self._monitor_service(
                _RecordingInAppChannel(),
                quote_api=_BatchQuoteAPI(
                    {"000001": _fresh_quote("000001", 10.5)}
                ),
                realtime_limit_up_loader=slow_loader,
                quote_timeout_seconds=0.03,
            ).monitor(db, self.now)

        self.assertTrue(cancelled.is_set())
        self.assertEqual([event.event_type for event in events], ["entry_triggered"])

    async def test_price_only_candidates_never_call_open_count_loader(self):
        await self._create_candidate(entry={"price_gte": 10})
        loader = AsyncMock(side_effect=AssertionError("must not load pool"))

        async with self.Session() as db:
            events = await self._monitor_service(
                _RecordingInAppChannel(),
                quote_api=_BatchQuoteAPI(
                    {"000001": _fresh_quote("000001", 10.5)}
                ),
                realtime_limit_up_loader=loader,
            ).monitor(db, self.now)

        self.assertEqual([event.event_type for event in events], ["entry_triggered"])
        loader.assert_not_awaited()

    async def test_monitor_persists_entry_and_restart_does_not_resend(self):
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        plan_id, candidate_id = await self._create_candidate()
        quote_api = _BatchQuoteAPI(
            {"000001": _fresh_quote("000001", 10.5)}
        )
        channel = _RecordingInAppChannel()
        async with self.Session() as db:
            await self._monitor_service(
                channel,
                quote_api=quote_api,
            ).monitor(db, self.now)
        async with self.SecondSession() as db:
            await self._monitor_service(
                channel,
                quote_api=quote_api,
            ).monitor(db, self.now)

        events = await self._events()
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].event_type, "entry_triggered")
        self.assertEqual(events[0].plan_version_id, plan_id)
        self.assertEqual(events[0].candidate_id, candidate_id)
        self.assertEqual(
            events[0].market_snapshot_json["quote"]["captured_at"],
            "2026-07-14T10:05:00+08:00",
        )
        self.assertEqual(len(channel.sends), 1)
        self.assertEqual(channel.sends[0][0]["stock_code"], "000001")
        async with self.Session() as db:
            candidate = await db.get(TradingPlanCandidate, candidate_id)
            state = (
                await db.execute(
                    select(TradingAlertConditionState).where(
                        TradingAlertConditionState.candidate_id == candidate_id,
                        TradingAlertConditionState.event_type
                        == "entry_triggered",
                    )
                )
            ).scalar_one()
        self.assertEqual(candidate.status, "triggered")
        self.assertTrue(state.active)
        self.assertEqual(state.occurrence_no, 1)
        self.assertEqual(
            events[0].market_snapshot_json["condition_version"],
            state.condition_version,
        )
        self.assertEqual(events[0].market_snapshot_json["occurrence_no"], 1)
        self.assertEqual(
            events[0].dedup_key,
            f"action:{self.today.isoformat()}:{plan_id}:{candidate_id}:"
            "leader_turn_two:entry_triggered:"
            f"{state.condition_version}:1",
        )
        self.assertEqual(len(state.condition_version), 64)
        self.assertEqual(TradingAlertEvent.dedup_key.type.length, 255)
        self.assertLessEqual(
            len(events[0].dedup_key),
            TradingAlertEvent.dedup_key.type.length,
        )

    async def test_restart_delivers_committed_pending_occurrence(self):
        await self._create_candidate()
        quote_api = _BatchQuoteAPI(
            {"000001": _fresh_quote("000001", 10.5)}
        )
        channel = _RecordingInAppChannel()
        first = self._monitor_service(channel, quote_api=quote_api)
        first._deliver = AsyncMock()
        async with self.Session() as db:
            await first.monitor(db, self.now)

        persisted = (await self._events())[0]
        self.assertEqual(
            persisted.channel_status_json["in_app"]["status"],
            "pending",
        )

        async with self.SecondSession() as db:
            await self._monitor_service(
                channel,
                quote_api=_BatchQuoteAPI(
                    {"000001": _fresh_quote("000001", 9.0)}
                ),
            ).monitor(db, self.now)

        self.assertEqual(len(await self._events()), 1)
        self.assertEqual(len(channel.sends), 1)

    async def test_pending_occurrence_drains_before_higher_priority_trigger(self):
        await self._create_candidate(
            entry={"price_gte": 10},
            invalidation={"price_lte": 9},
        )
        channel = _RecordingInAppChannel()
        first = self._monitor_service(
            channel,
            quote_api=_BatchQuoteAPI(
                {"000001": _fresh_quote("000001", 11.0)}
            ),
        )
        first._deliver = AsyncMock()
        async with self.Session() as db:
            await first.monitor(db, self.now)

        later = CN_TZ.localize(datetime(2026, 7, 14, 10, 6))
        async with self.SecondSession() as db:
            await self._monitor_service(
                channel,
                quote_api=_BatchQuoteAPI(
                    {
                        "000001": _fresh_quote(
                            "000001",
                            8.0,
                            captured_at="20260714100600",
                        )
                    }
                ),
            ).monitor(db, later)

        self.assertEqual(
            [payload[0]["event_type"] for payload in channel.sends],
            ["entry_triggered", "invalidated"],
        )

    async def test_two_engines_drain_pending_occurrence_once(self):
        await self._create_candidate()
        channel = _RecordingInAppChannel()
        first = self._monitor_service(
            channel,
            quote_api=_BatchQuoteAPI(
                {"000001": _fresh_quote("000001", 10.5)}
            ),
        )
        first._deliver = AsyncMock()
        async with self.Session() as db:
            await first.monitor(db, self.now)

        quote_barrier = asyncio.Barrier(2)
        quote_api = _BatchQuoteAPI(
            {"000001": _fresh_quote("000001", 9.0)},
            barrier=quote_barrier,
        )

        async def run(service, sessions):
            async with sessions() as db:
                return await service.monitor(db, self.now)

        await asyncio.gather(
            run(
                self._monitor_service(channel, quote_api=quote_api),
                self.Session,
            ),
            run(
                self._monitor_service(channel, quote_api=quote_api),
                self.SecondSession,
            ),
        )

        self.assertEqual(len(await self._events()), 1)
        self.assertEqual(len(channel.sends), 1)

    async def test_two_engines_racing_monitor_send_once(self):
        await self._create_candidate()
        quote_barrier = asyncio.Barrier(2)
        quote_api = _BatchQuoteAPI(
            {"000001": _fresh_quote("000001", 10.5)},
            barrier=quote_barrier,
        )
        channel = _RecordingInAppChannel()
        first = self._monitor_service(channel, quote_api=quote_api)
        second = self._monitor_service(channel, quote_api=quote_api)

        async def run(service, sessions):
            async with sessions() as db:
                return await service.monitor(db, self.now)

        await asyncio.gather(
            run(first, self.Session),
            run(second, self.SecondSession),
        )

        self.assertEqual(len(await self._events()), 1)
        self.assertEqual(len(channel.sends), 1)
        async with self.Session() as db:
            candidate = await db.scalar(select(TradingPlanCandidate))
            states = list(
                (await db.scalars(select(TradingAlertConditionState))).all()
            )
        self.assertEqual(candidate.status, "triggered")
        entry_state = next(
            state for state in states if state.event_type == "entry_triggered"
        )
        self.assertTrue(entry_state.active)
        self.assertEqual(entry_state.occurrence_no, 1)

    async def test_event_state_and_candidate_rollback_together_on_commit_failure(self):
        _plan_id, candidate_id = await self._create_candidate()
        quote_api = _BatchQuoteAPI(
            {"000001": _fresh_quote("000001", 10.5)}
        )

        async with self.Session() as db:
            commit_count = 0

            def fail_action_commit(_session):
                nonlocal commit_count
                commit_count += 1
                if commit_count == 2:
                    raise RuntimeError("injected action transaction failure")

            event.listen(db.sync_session, "before_commit", fail_action_commit)
            try:
                with self.assertRaisesRegex(
                    RuntimeError,
                    "injected action transaction failure",
                ):
                    await self._monitor_service(
                        _RecordingInAppChannel(),
                        quote_api=quote_api,
                    ).monitor(db, self.now)
            finally:
                event.remove(
                    db.sync_session,
                    "before_commit",
                    fail_action_commit,
                )
                await db.rollback()

        async with self.SecondSession() as db:
            candidate = await db.get(TradingPlanCandidate, candidate_id)
            states = list(
                (await db.scalars(select(TradingAlertConditionState))).all()
            )
            events = list((await db.scalars(select(TradingAlertEvent))).all())
        self.assertEqual(candidate.status, "waiting")
        self.assertEqual(states, [])
        self.assertEqual(events, [])

    async def test_monitor_filters_non_today_action_before_fetching_quotes(self):
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        await self._create_candidate(
            action_trade_date=date(2026, 7, 15),
        )
        quote_api = _BatchQuoteAPI(
            {"000001": _fresh_quote("000001", 10.5)}
        )
        async with self.Session() as db:
            events = await self._monitor_service(
                _RecordingInAppChannel(),
                quote_api=quote_api,
            ).monitor(db, self.now)

        self.assertEqual(events, [])
        self.assertEqual(quote_api.calls, [])
        self.assertEqual(await self._events(), [])

    async def test_monitor_ignores_confirmed_but_not_active_database_plan(self):
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        await self._create_candidate(plan_status="confirmed")
        quote_api = _BatchQuoteAPI(
            {"000001": _fresh_quote("000001", 10.5)}
        )
        async with self.Session() as db:
            events = await self._monitor_service(
                _RecordingInAppChannel(),
                quote_api=quote_api,
            ).monitor(db, self.now)

        self.assertEqual(events, [])
        self.assertEqual(quote_api.calls, [])
        self.assertEqual(await self._events(), [])

    async def test_calendar_missing_failure_and_closed_days_stop_after_outbox_drain(self):
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        class EmptyResult:
            def scalars(self):
                return self

            def all(self):
                return []

        class DrainOnlyDB:
            def __init__(self):
                self.execute_calls = 0
                self.commit_calls = 0

            async def execute(self, *_args, **_kwargs):
                self.execute_calls += 1
                if self.execute_calls > 1:
                    raise AssertionError(
                        "candidate database access must stay gated"
                    )
                return EmptyResult()

            async def commit(self):
                self.commit_calls += 1

        cases = [
            (
                "missing",
                None,
                CN_TZ.localize(datetime(2026, 7, 14, 10, 5)),
            ),
            (
                "loader_failure",
                _TradingCalendar(
                    ensure_error=RuntimeError("calendar unavailable")
                ),
                CN_TZ.localize(datetime(2026, 7, 14, 10, 5)),
            ),
            (
                "weekend",
                _TradingCalendar(),
                CN_TZ.localize(datetime(2026, 7, 18, 10, 5)),
            ),
            (
                "weekday_holiday",
                _TradingCalendar(),
                CN_TZ.localize(datetime(2026, 10, 1, 10, 5)),
            ),
        ]
        for label, calendar, current in cases:
            with self.subTest(label=label):
                quote_api = _BatchQuoteAPI({})
                kwargs = {"quote_api": quote_api}
                if calendar is not None:
                    kwargs["trading_calendar"] = calendar
                service = TradingPlaybookAlertService(
                    _RecordingInAppChannel(),
                    **kwargs,
                )
                db = DrainOnlyDB()

                result = await service.monitor(db, current)

                self.assertEqual(result, [])
                self.assertEqual(db.execute_calls, 1)
                self.assertEqual(db.commit_calls, 1)
                self.assertEqual(quote_api.calls, [])
                if calendar is not None:
                    self.assertEqual(calendar.ensure_calls, [current.date()])

    async def test_monitor_only_fetches_during_cn_continuous_trading_sessions(self):
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        await self._create_candidate()
        cases = [
            ((9, 29, 59), False),
            ((9, 30, 0), True),
            ((11, 30, 0), True),
            ((11, 30, 1), False),
            ((12, 59, 59), False),
            ((13, 0, 0), True),
            ((15, 0, 0), True),
            ((15, 0, 1), False),
        ]
        for (hour, minute, second), expected_fetch in cases:
            with self.subTest(time=(hour, minute, second)):
                current = CN_TZ.localize(
                    datetime(2026, 7, 14, hour, minute, second)
                )
                quote_api = _BatchQuoteAPI(
                    {
                        "000001": _fresh_quote(
                            "000001",
                            10.5,
                            captured_at=current.strftime("%Y%m%d%H%M%S"),
                        )
                    }
                )
                async with self.Session() as db:
                    await self._monitor_service(
                        _RecordingInAppChannel(),
                        quote_api=quote_api,
                    ).monitor(db, current)
                self.assertEqual(bool(quote_api.calls), expected_fetch)

    async def test_preclose_target_tomorrow_can_trigger_today_at_1440(self):
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        await self._create_candidate(
            target_trade_date=date(2026, 7, 15),
            stage="preclose",
        )
        current = CN_TZ.localize(datetime(2026, 7, 14, 14, 40))
        quote_api = _BatchQuoteAPI(
            {
                "000001": _fresh_quote(
                    "000001",
                    10.5,
                    captured_at="20260714144000",
                )
            }
        )
        channel = _RecordingInAppChannel()
        async with self.Session() as db:
            await self._monitor_service(
                channel,
                quote_api=quote_api,
            ).monitor(db, current)

        self.assertEqual(len(await self._events()), 1)
        self.assertEqual(len(channel.sends), 1)

    async def test_stale_missing_future_and_prior_day_quotes_fail_closed(self):
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        await self._create_candidate()
        cases = [
            (None, False),
            ("20260714100359", False),
            ("20260713100500", False),
            ("20260714100501", False),
            ("2026-07-14T10:04:00+08:00", True),
        ]
        for captured_at, expected_trigger in cases:
            with self.subTest(captured_at=captured_at):
                quote = {"code": "000001", "price": 10.5}
                if captured_at is not None:
                    quote["datetime"] = captured_at
                quote_api = _BatchQuoteAPI({"000001": quote})
                channel = _RecordingInAppChannel()
                async with self.Session() as db:
                    await self._monitor_service(
                        channel,
                        quote_api=quote_api,
                        quote_max_age_seconds=60,
                    ).monitor(db, self.now)
                self.assertEqual(bool(channel.sends), expected_trigger)

    async def test_monitor_accepts_real_tencent_quote_shape(self):
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        await self._create_candidate(
            entry={
                "price_gte": 10,
                "change_pct_gte": 9.5,
                "sealed": True,
            }
        )
        quote_api = _BatchQuoteAPI(
            {
                "000001": {
                    "code": "000001",
                    "name": "平安银行",
                    "price": 10.0,
                    "change_pct": 10.01,
                    "limit_up": 10.0,
                    "bid1_volume": 12345.0,
                    "datetime": "20260714100500",
                }
            }
        )
        async with self.Session() as db:
            await self._monitor_service(
                _RecordingInAppChannel(),
                quote_api=quote_api,
            ).monitor(db, self.now)

        event = (await self._events())[0]
        self.assertEqual(event.event_type, "entry_triggered")
        self.assertTrue(event.market_snapshot_json["quote"]["sealed"])
        self.assertEqual(
            event.market_snapshot_json["quote"]["captured_at"],
            "2026-07-14T10:05:00+08:00",
        )

    async def test_quote_timeout_is_cancelled_and_writes_no_event(self):
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        await self._create_candidate()
        cancelled = asyncio.Event()

        class SlowQuoteAPI:
            async def get_quotes_batch(self, _codes):
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelled.set()

        async with self.Session() as db:
            events = await self._monitor_service(
                _RecordingInAppChannel(),
                quote_api=SlowQuoteAPI(),
                quote_timeout_seconds=0.01,
            ).monitor(db, self.now)

        self.assertEqual(events, [])
        self.assertTrue(cancelled.is_set())
        self.assertEqual(await self._events(), [])

    async def test_quote_fetch_cancellation_propagates_and_writes_no_event(self):
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        await self._create_candidate()
        started = asyncio.Event()
        cancelled = asyncio.Event()

        class SlowQuoteAPI:
            async def get_quotes_batch(self, _codes):
                started.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelled.set()

        async def run_monitor():
            async with self.Session() as db:
                return await self._monitor_service(
                    _RecordingInAppChannel(),
                    quote_api=SlowQuoteAPI(),
                ).monitor(db, self.now)

        task = asyncio.create_task(run_monitor())
        await started.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        self.assertTrue(cancelled.is_set())
        self.assertEqual(await self._events(), [])

    async def test_invalidated_candidate_recovers_and_can_trigger_new_occurrence(self):
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        await self._create_candidate(
            entry={"price_gte": 20},
            invalidation={"price_lte": 9},
        )
        channel = _RecordingInAppChannel()
        first_quotes = _BatchQuoteAPI(
            {"000001": _fresh_quote("000001", 8.0)}
        )
        async with self.Session() as db:
            await self._monitor_service(
                channel,
                quote_api=first_quotes,
            ).monitor(db, self.now)

        later = CN_TZ.localize(datetime(2026, 7, 14, 10, 6))
        second_quotes = _BatchQuoteAPI(
            {
                "000001": _fresh_quote(
                    "000001",
                    11.0,
                    captured_at="20260714100600",
                )
            }
        )
        async with self.SecondSession() as db:
            await self._monitor_service(
                channel,
                quote_api=second_quotes,
            ).monitor(db, later)
            candidate = await db.scalar(select(TradingPlanCandidate))
            self.assertEqual(candidate.status, "waiting")

        latest = CN_TZ.localize(datetime(2026, 7, 14, 10, 7))
        third_quotes = _BatchQuoteAPI(
            {
                "000001": _fresh_quote(
                    "000001",
                    8.0,
                    captured_at="20260714100700",
                )
            }
        )
        async with self.Session() as db:
            await self._monitor_service(
                channel,
                quote_api=third_quotes,
            ).monitor(db, latest)

        events = await self._events()
        self.assertEqual(
            [event.event_type for event in events],
            ["invalidated", "invalidated"],
        )
        self.assertEqual(len(channel.sends), 2)
        self.assertEqual(len(second_quotes.calls), 1)
        self.assertEqual(len(third_quotes.calls), 1)
        self.assertNotEqual(events[0].dedup_key, events[1].dedup_key)
        self.assertEqual(
            [
                event.market_snapshot_json["occurrence_no"]
                for event in events
            ],
            [1, 2],
        )

    async def test_active_higher_priority_condition_blocks_lower_trigger(self):
        _plan_id, candidate_id = await self._create_candidate(
            entry={"price_gte": 10},
            invalidation={"price_lte": 11},
            exit_trigger={"price_gte": 10},
        )
        channel = _RecordingInAppChannel()
        first_quote_api = _BatchQuoteAPI(
            {"000001": _fresh_quote("000001", 10.5)}
        )
        async with self.Session() as db:
            await self._monitor_service(
                channel,
                quote_api=first_quote_api,
            ).monitor(db, self.now)

        later = CN_TZ.localize(datetime(2026, 7, 14, 10, 6))
        second_quote_api = _BatchQuoteAPI(
            {
                "000001": _fresh_quote(
                    "000001",
                    10.5,
                    captured_at="20260714100600",
                )
            }
        )
        async with self.SecondSession() as db:
            await self._monitor_service(
                channel,
                quote_api=second_quote_api,
            ).monitor(db, later)

        events = await self._events()
        self.assertEqual([event.event_type for event in events], ["invalidated"])
        async with self.Session() as db:
            states = list(
                (
                    await db.execute(
                        select(TradingAlertConditionState).where(
                            TradingAlertConditionState.candidate_id
                            == candidate_id
                        )
                    )
                )
                .scalars()
                .all()
            )
        by_type = {state.event_type: state for state in states}
        self.assertTrue(by_type["invalidated"].active)
        self.assertEqual(by_type["invalidated"].occurrence_no, 1)
        self.assertFalse(by_type["exit_triggered"].active)
        self.assertEqual(by_type["exit_triggered"].occurrence_no, 0)
        self.assertFalse(by_type["entry_triggered"].active)
        self.assertEqual(by_type["entry_triggered"].occurrence_no, 0)

    async def test_invalidated_candidate_recovers_to_triggered_after_prior_entry(self):
        _plan_id, candidate_id = await self._create_candidate(
            entry={"price_gte": 10},
            invalidation={"price_lte": 5},
        )
        channel = _RecordingInAppChannel()
        ticks = (
            (self.now, 11.0, "20260714100500"),
            (CN_TZ.localize(datetime(2026, 7, 14, 10, 6)), 4.0, "20260714100600"),
            (CN_TZ.localize(datetime(2026, 7, 14, 10, 7)), 6.0, "20260714100700"),
        )
        for current, price, captured_at in ticks:
            async with self.Session() as db:
                await self._monitor_service(
                    channel,
                    quote_api=_BatchQuoteAPI(
                        {
                            "000001": _fresh_quote(
                                "000001",
                                price,
                                captured_at=captured_at,
                            )
                        }
                    ),
                ).monitor(db, current)

        self.assertEqual(
            [event.event_type for event in await self._events()],
            ["entry_triggered", "invalidated"],
        )
        async with self.Session() as db:
            candidate = await db.get(TradingPlanCandidate, candidate_id)
        self.assertEqual(candidate.status, "triggered")

    async def test_unknown_tick_does_not_rearm_active_condition(self):
        await self._create_candidate(
            entry={"price_gte": 20},
            invalidation={"price_lte": 9},
        )
        channel = _RecordingInAppChannel()
        ticks = (
            (self.now, {"price": 8.0, "datetime": "20260714100500"}),
            (
                CN_TZ.localize(datetime(2026, 7, 14, 10, 6)),
                {
                    "price": 0.0,
                    "datetime": "20260714100600",
                    "_missing_fields": ["price"],
                },
            ),
            (
                CN_TZ.localize(datetime(2026, 7, 14, 10, 7)),
                {"price": 8.0, "datetime": "20260714100700"},
            ),
            (
                CN_TZ.localize(datetime(2026, 7, 14, 10, 8)),
                {"price": 10.0, "datetime": "20260714100800"},
            ),
            (
                CN_TZ.localize(datetime(2026, 7, 14, 10, 9)),
                {"price": 8.0, "datetime": "20260714100900"},
            ),
        )
        for current, quote in ticks:
            async with self.Session() as db:
                await self._monitor_service(
                    channel,
                    quote_api=_BatchQuoteAPI(
                        {"000001": {"code": "000001", **quote}}
                    ),
                ).monitor(db, current)

        events = await self._events()
        self.assertEqual(
            [event.market_snapshot_json["occurrence_no"] for event in events],
            [1, 2],
        )
        self.assertEqual(len(channel.sends), 2)

    async def test_unknown_replacement_does_not_recover_obsolete_active_version(self):
        _plan_id, candidate_id = await self._create_candidate(
            entry={"price_gte": 20},
            invalidation={"price_lte": 9},
            exit_trigger={"price_gte": 10},
        )
        channel = _RecordingInAppChannel()
        async with self.Session() as db:
            await self._monitor_service(
                channel,
                quote_api=_BatchQuoteAPI(
                    {"000001": _fresh_quote("000001", 8.0)}
                ),
            ).monitor(db, self.now)
            candidate = await db.get(TradingPlanCandidate, candidate_id)
            candidate.invalidation_json = {"change_pct_lte": -5}
            await db.commit()

        later = CN_TZ.localize(datetime(2026, 7, 14, 10, 6))
        async with self.SecondSession() as db:
            await self._monitor_service(
                channel,
                quote_api=_BatchQuoteAPI(
                    {
                        "000001": {
                            **_fresh_quote(
                                "000001",
                                10.0,
                                captured_at="20260714100600",
                            ),
                            "_missing_fields": ["change_pct"],
                        }
                    }
                ),
            ).monitor(db, later)
            candidate = await db.get(TradingPlanCandidate, candidate_id)
            candidate.invalidation_json = {}
            await db.commit()

        latest = CN_TZ.localize(datetime(2026, 7, 14, 10, 7))
        async with self.Session() as db:
            await self._monitor_service(
                channel,
                quote_api=_BatchQuoteAPI(
                    {
                        "000001": _fresh_quote(
                            "000001",
                            10.0,
                            captured_at="20260714100700",
                        )
                    }
                ),
            ).monitor(db, latest)

        self.assertEqual(
            [event.event_type for event in await self._events()],
            ["invalidated"],
        )
        async with self.Session() as db:
            candidate = await db.get(TradingPlanCandidate, candidate_id)
            states = list(
                (
                    await db.scalars(
                        select(TradingAlertConditionState).where(
                            TradingAlertConditionState.candidate_id
                            == candidate_id,
                            TradingAlertConditionState.event_type
                            == "invalidated",
                        )
                    )
                ).all()
            )
        self.assertEqual(candidate.status, "invalidated")
        self.assertEqual(sum(state.active for state in states), 1)

    async def test_exit_candidate_is_terminal_across_monitor_ticks(self):
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        await self._create_candidate(
            entry={"price_gte": 10},
            invalidation={"price_lte": 5},
            exit_trigger={"change_pct_lte": -3},
        )
        first_channel = _RecordingInAppChannel()
        first_quotes = _BatchQuoteAPI(
            {
                "000001": {
                    **_fresh_quote("000001", 9.0),
                    "change_pct": -4.0,
                }
            }
        )
        async with self.Session() as db:
            await self._monitor_service(
                first_channel,
                quote_api=first_quotes,
            ).monitor(db, self.now)

        later = CN_TZ.localize(datetime(2026, 7, 14, 10, 6))
        second_channel = _RecordingInAppChannel()
        second_quotes = _BatchQuoteAPI(
            {
                "000001": {
                    **_fresh_quote(
                        "000001",
                        11.0,
                        captured_at="20260714100600",
                    ),
                    "change_pct": 1.0,
                }
            }
        )
        async with self.SecondSession() as db:
            await self._monitor_service(
                second_channel,
                quote_api=second_quotes,
            ).monitor(db, later)

        events = await self._events()
        self.assertEqual([event.event_type for event in events], ["exit_triggered"])
        self.assertEqual(len(first_channel.sends), 1)
        self.assertEqual(second_channel.sends, [])
        self.assertEqual(second_quotes.calls, [])

    async def test_disabled_entry_is_skipped_and_not_pushed_after_reenable(self):
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        await self._create_candidate()
        async with self.Session() as db:
            settings = await db.get(TradingPlaybookSettings, 1)
            settings.in_app_enabled = False
            await db.commit()
        quote_api = _BatchQuoteAPI(
            {"000001": _fresh_quote("000001", 10.5)}
        )
        channel = _RecordingInAppChannel()
        async with self.Session() as db:
            await self._monitor_service(
                channel,
                quote_api=quote_api,
            ).monitor(db, self.now)

        event = (await self._events())[0]
        self.assertEqual(event.channel_status_json["in_app"]["status"], "skipped")
        self.assertEqual(event.channel_status_json["in_app"]["reason"], "disabled")
        self.assertTrue(event.channel_status_json["in_app"]["skipped_at"])
        self.assertEqual(channel.sends, [])

        async with self.Session() as db:
            settings = await db.get(TradingPlaybookSettings, 1)
            settings.in_app_enabled = True
            await db.commit()
            await self._monitor_service(
                channel,
                quote_api=quote_api,
            ).monitor(db, self.now)

        self.assertEqual(channel.sends, [])
        self.assertEqual(
            (await self._events())[0].channel_status_json["in_app"]["status"],
            "skipped",
        )

    async def test_disabled_invalidation_continues_observing_without_resend(self):
        await self._create_candidate(
            entry={"price_gte": 20},
            invalidation={"price_lte": 9},
        )
        async with self.Session() as db:
            settings = await db.get(TradingPlaybookSettings, 1)
            settings.in_app_enabled = False
            await db.commit()
        first_quotes = _BatchQuoteAPI(
            {"000001": _fresh_quote("000001", 8.0)}
        )
        channel = _RecordingInAppChannel()
        async with self.Session() as db:
            await self._monitor_service(
                channel,
                quote_api=first_quotes,
            ).monitor(db, self.now)

        event = (await self._events())[0]
        self.assertEqual(event.event_type, "invalidated")
        self.assertEqual(event.channel_status_json["in_app"]["status"], "skipped")

        async with self.Session() as db:
            settings = await db.get(TradingPlaybookSettings, 1)
            settings.in_app_enabled = True
            await db.commit()
        later_quotes = _BatchQuoteAPI(
            {
                "000001": _fresh_quote(
                    "000001",
                    8.0,
                    captured_at="20260714100600",
                )
            }
        )
        later = CN_TZ.localize(datetime(2026, 7, 14, 10, 6))
        async with self.SecondSession() as db:
            await self._monitor_service(
                channel,
                quote_api=later_quotes,
            ).monitor(db, later)

        self.assertEqual(channel.sends, [])
        self.assertEqual(later_quotes.calls, [["000001"]])
        self.assertEqual(len(await self._events()), 1)

    async def test_monitor_persists_all_matching_events_before_first_send(self):
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        plan_id, _candidate_id = await self._create_candidate()
        async with self.Session() as db:
            db.add(
                TradingPlanCandidate(
                    plan_version_id=plan_id,
                    stock_code="000002",
                    stock_name="股票000002",
                    action_trade_date=self.today,
                    theme_name="测试题材",
                    primary_mode_key="trend_turn_two",
                    supporting_mode_keys_json=[],
                    role="middle_army",
                    rank=2,
                    recognition_json={},
                    entry_trigger_json={"price_gte": 20},
                    invalidation_json={},
                    exit_trigger_json={},
                    risk_level="trial",
                    position_reference=10,
                    evidence_json=[],
                    manual_overrides_json={},
                    status="waiting",
                )
            )
            await db.commit()

        class RejectingChannel(_RecordingInAppChannel):
            async def send(self, event, *, idempotency_key):
                self.sends.append((dict(event), idempotency_key))
                raise RuntimeError("websocket unavailable")

        channel = RejectingChannel()
        quote_api = _BatchQuoteAPI(
            {
                "000001": _fresh_quote("000001", 10.5),
                "000002": _fresh_quote("000002", 20.5),
            }
        )
        with self.assertRaisesRegex(RuntimeError, "websocket unavailable"):
            async with self.Session() as db:
                await self._monitor_service(
                    channel,
                    quote_api=quote_api,
                ).monitor(db, self.now)

        events = await self._events()
        self.assertEqual(len(events), 2)
        self.assertEqual(
            [event.channel_status_json["in_app"]["status"] for event in events],
            ["uncertain", "pending"],
        )


if __name__ == "__main__":
    unittest.main()
