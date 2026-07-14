import asyncio
import unittest
from datetime import date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from sqlalchemy import event, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.database import Base
from app.models.trading_playbook import (
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
            if commits == 5 and not injected:
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
        self.today = date(2026, 7, 14)
        self.now = CN_TZ.localize(datetime(2026, 7, 14, 10, 5))

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
            await TradingPlaybookAlertService(
                channel,
                quote_api=quote_api,
            ).monitor(db, self.now)
        async with self.SecondSession() as db:
            await TradingPlaybookAlertService(
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
        self.assertEqual(
            events[0].dedup_key,
            f"action:{self.today.isoformat()}:{plan_id}:{candidate_id}:"
            "leader_turn_two:entry_triggered",
        )

    async def test_two_engines_racing_monitor_send_once(self):
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        await self._create_candidate()
        quote_api = _BatchQuoteAPI(
            {"000001": _fresh_quote("000001", 10.5)},
        )
        channel = _RecordingInAppChannel()
        first = TradingPlaybookAlertService(channel, quote_api=quote_api)
        second = TradingPlaybookAlertService(channel, quote_api=quote_api)

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
            await first.monitor(db, self.now)
            settings = await db.get(TradingPlaybookSettings, 1)
            settings.in_app_enabled = True
            await db.commit()
        self.assertEqual(channel.sends, [])

        async def run(service, sessions):
            async with sessions() as db:
                return await service.monitor(db, self.now)

        await asyncio.gather(
            run(first, self.Session),
            run(second, self.SecondSession),
        )

        self.assertEqual(len(await self._events()), 1)
        self.assertEqual(len(channel.sends), 1)

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
            events = await TradingPlaybookAlertService(
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
            events = await TradingPlaybookAlertService(
                _RecordingInAppChannel(),
                quote_api=quote_api,
            ).monitor(db, self.now)

        self.assertEqual(events, [])
        self.assertEqual(quote_api.calls, [])
        self.assertEqual(await self._events(), [])

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
                    await TradingPlaybookAlertService(
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
            await TradingPlaybookAlertService(
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
                    await TradingPlaybookAlertService(
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
            await TradingPlaybookAlertService(
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
            events = await TradingPlaybookAlertService(
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
                return await TradingPlaybookAlertService(
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

    async def test_invalidated_candidate_is_terminal_across_monitor_ticks(self):
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        await self._create_candidate(
            entry={"price_gte": 10},
            invalidation={"price_lte": 9},
        )
        first_channel = _RecordingInAppChannel()
        first_quotes = _BatchQuoteAPI(
            {"000001": _fresh_quote("000001", 8.0)}
        )
        async with self.Session() as db:
            await TradingPlaybookAlertService(
                first_channel,
                quote_api=first_quotes,
            ).monitor(db, self.now)

        later = CN_TZ.localize(datetime(2026, 7, 14, 10, 6))
        second_channel = _RecordingInAppChannel()
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
            await TradingPlaybookAlertService(
                second_channel,
                quote_api=second_quotes,
            ).monitor(db, later)

        events = await self._events()
        self.assertEqual([event.event_type for event in events], ["invalidated"])
        self.assertEqual(len(first_channel.sends), 1)
        self.assertEqual(second_channel.sends, [])
        self.assertEqual(second_quotes.calls, [])

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
            await TradingPlaybookAlertService(
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
            await TradingPlaybookAlertService(
                second_channel,
                quote_api=second_quotes,
            ).monitor(db, later)

        events = await self._events()
        self.assertEqual([event.event_type for event in events], ["exit_triggered"])
        self.assertEqual(len(first_channel.sends), 1)
        self.assertEqual(second_channel.sends, [])
        self.assertEqual(second_quotes.calls, [])

    async def test_disabled_in_app_persists_action_as_pending_without_push(self):
        from app.services.trading_playbook.alert_service import (
            TradingPlaybookAlertService,
        )

        await self._create_candidate()
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
        quote_api = _BatchQuoteAPI(
            {"000001": _fresh_quote("000001", 10.5)}
        )
        channel = _RecordingInAppChannel()
        async with self.Session() as db:
            await TradingPlaybookAlertService(
                channel,
                quote_api=quote_api,
            ).monitor(db, self.now)

        event = (await self._events())[0]
        self.assertEqual(event.channel_status_json["in_app"]["status"], "pending")
        self.assertEqual(channel.sends, [])

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
                await TradingPlaybookAlertService(
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
