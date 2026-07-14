import asyncio
import copy
import json
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock

from sqlalchemy import event, func, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.models.market_review import MarketReviewStockDaily
from app.models.trading_playbook import (
    TradingAlertEvent,
    TradingExecutionReview,
    TradingPlanCandidate,
    TradingPlanVersion,
    TradingPlaybookSettings,
)
from app.services.trading_playbook.alert_service import TradingPlaybookAlertService
from app.services.trading_playbook.errors import (
    InvalidTransitionError,
    PlaybookNotFoundError,
)
from app.services.trading_playbook.review_service import (
    TradingPlaybookReviewService,
)
from app.utils.time_utils import CN_TZ


TRADE_DATE = date(2026, 7, 14)
FINALIZED_AT = CN_TZ.localize(datetime(2026, 7, 14, 15, 31))


class _ReviewRecordingChannel:
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
        return {"channel": "in_app", "status": "ready"}


class TradingPlaybookReviewSummaryTests(unittest.TestCase):
    def test_review_separates_not_triggered_invalidated_and_not_executed(self):
        result = TradingPlaybookReviewService().summarize(
            candidates=[
                {"id": 1, "stock_code": "000001", "status": "waiting"},
                {"id": 2, "stock_code": "000002", "status": "invalidated"},
                {"id": 3, "stock_code": "000003", "status": "triggered"},
            ],
            events=[{"candidate_id": 3, "event_type": "entry_triggered"}],
            manual_execution={"3": {"executed": False}},
        )

        self.assertEqual(result["not_triggered"], ["000001"])
        self.assertEqual(result["invalidated"], ["000002"])
        self.assertEqual(result["triggered_not_executed"], ["000003"])
        self.assertEqual(
            result["plan_compliance"],
            {
                "planned": 3,
                "executed": 0,
                "unplanned": 0,
                "violations": 0,
                "violation_details": [],
            },
        )

    def test_review_classifies_execution_and_unplanned_without_profit_inference(self):
        result = TradingPlaybookReviewService().summarize(
            candidates=[
                {"id": 1, "stock_code": "000001", "status": "triggered"},
                {"id": 2, "stock_code": "000002", "status": "waiting"},
            ],
            events=[{"candidate_id": 1, "event_type": "entry_triggered"}],
            manual_execution={
                "1": {
                    "executed": True,
                    "execution_price": 10.0,
                    "quantity": 100,
                },
                "_unplanned": [
                    {
                        "executed": True,
                        "stock_code": "000999",
                        "stock_name": "计划外股票",
                        "execution_price": 9.0,
                    }
                ],
            },
        )

        self.assertEqual(result["triggered_executed"], ["000001"])
        self.assertEqual(result["not_triggered"], ["000002"])
        self.assertEqual(
            result["plan_compliance"],
            {
                "planned": 2,
                "executed": 1,
                "unplanned": 1,
                "violations": 0,
                "violation_details": [],
            },
        )
        self.assertEqual(result["unplanned_executions"][0]["stock_code"], "000999")
        serialized = json.dumps(result, ensure_ascii=False)
        self.assertNotIn("account_profit", serialized)
        self.assertNotIn("profit", serialized)
        self.assertNotIn("pnl", serialized.lower())

    def test_summary_has_the_complete_stable_shape_when_empty(self):
        result = TradingPlaybookReviewService().summarize([], [], {})

        self.assertEqual(
            result,
            {
                "not_triggered": [],
                "invalidated": [],
                "triggered_executed": [],
                "triggered_not_executed": [],
                "plan_compliance": {
                    "planned": 0,
                    "executed": 0,
                    "unplanned": 0,
                    "violations": 0,
                    "violation_details": [],
                },
                "signal_outcomes": [],
                "unplanned_executions": [],
            },
        )

    def test_signal_outcome_keeps_delivery_and_acknowledgement_audit(self):
        result = TradingPlaybookReviewService().summarize(
            [{"id": 1, "stock_code": "000001", "status": "triggered"}],
            [
                {
                    "candidate_id": 1,
                    "event_type": "entry_triggered",
                    "id": 17,
                    "triggered_at": datetime(2026, 7, 14, 10, 0),
                    "acknowledged_at": datetime(2026, 7, 14, 10, 1),
                    "market_snapshot_json": {
                        "trade_date": "2026-07-14",
                        "quote": {"price": 10.0},
                    },
                    "message": "价格触发",
                    "channel_status_json": {"in_app": "sent"},
                }
            ],
            {"1": {"executed": False}},
        )

        audit = result["signal_outcomes"][0]["events"][0]
        self.assertEqual(audit["event_id"], 17)
        self.assertEqual(audit["event_type"], "entry_triggered")
        self.assertEqual(audit["triggered_at"], "2026-07-14T10:00:00")
        self.assertEqual(audit["acknowledged_at"], "2026-07-14T10:01:00")
        self.assertEqual(audit["channel_status"], {"in_app": "sent"})
        self.assertEqual(audit["market_snapshot"]["quote"]["price"], 10.0)
        self.assertEqual(audit["message"], "价格触发")

    def test_execution_timing_violations_and_signal_return_are_transparent(self):
        events = [
            {
                "id": 101,
                "candidate_id": 1,
                "event_type": "entry_triggered",
                "triggered_at": datetime(2026, 7, 14, 2, 0, tzinfo=timezone.utc),
                "market_snapshot_json": {
                    "trade_date": "2026-07-14",
                    "quote": {"price": 10.0},
                },
                "message": "entry",
            },
            {
                "id": 102,
                "candidate_id": 2,
                "event_type": "entry_triggered",
                "triggered_at": datetime(2026, 7, 14, 10, 0),
                "market_snapshot_json": {
                    "trade_date": "2026-07-14",
                    "quote": {"price": 10.0},
                },
            },
            {
                "id": 103,
                "candidate_id": 3,
                "event_type": "entry_triggered",
                "triggered_at": datetime(2026, 7, 14, 10, 0),
                "market_snapshot_json": {
                    "trade_date": "2026-07-14",
                    "quote": {"price": 10.0},
                },
            },
            {
                "id": 104,
                "candidate_id": 3,
                "event_type": "invalidated",
                "triggered_at": datetime(2026, 7, 14, 10, 5),
                "market_snapshot_json": {"trade_date": "2026-07-14"},
            },
            {
                "id": 105,
                "candidate_id": 5,
                "event_type": "confirmation_triggered",
                "triggered_at": datetime(2026, 7, 14, 10, 0),
                "market_snapshot_json": {"trade_date": "2026-07-14"},
            },
        ]
        result = TradingPlaybookReviewService().summarize(
            candidates=[
                {"id": 1, "stock_code": "000001", "status": "triggered"},
                {"id": 2, "stock_code": "000002", "status": "triggered"},
                {"id": 3, "stock_code": "000003", "status": "invalidated"},
                {"id": 4, "stock_code": "000004", "status": "waiting"},
                {"id": 5, "stock_code": "000005", "status": "triggered"},
            ],
            events=events,
            manual_execution={
                "1": {
                    "executed": True,
                    "executed_at": "2026-07-14T10:01:00+08:00",
                },
                "2": {
                    "executed": True,
                    "executed_at": "2026-07-14T09:59:00+08:00",
                },
                "3": {
                    "executed": True,
                    "executed_at": "2026-07-14T10:05:00+08:00",
                },
                "4": {"executed": True},
                "5": {"executed": True},
            },
            outcomes={
                "000001": {"close_price": 11.0},
                "000002": {"close_price": float("inf")},
            },
        )

        by_id = {
            row["candidate_id"]: row for row in result["signal_outcomes"]
        }
        self.assertEqual(by_id[1]["execution_timing"], "after_signal")
        self.assertEqual(by_id[2]["execution_timing"], "before_signal")
        self.assertEqual(by_id[3]["execution_timing"], "after_invalidation")
        self.assertEqual(by_id[4]["execution_timing"], "without_signal")
        self.assertEqual(by_id[5]["execution_timing"], "unknown_time")
        self.assertEqual(by_id[1]["entry_signal_at"], "2026-07-14T02:00:00+00:00")
        self.assertEqual(by_id[3]["invalidation_at"], "2026-07-14T10:05:00")
        self.assertEqual(by_id[1]["signal_to_close_pct"], 10.0)
        self.assertEqual(by_id[1]["signal_return_quality"], "ready")
        self.assertIsNone(by_id[2]["signal_to_close_pct"])
        self.assertEqual(by_id[2]["signal_return_quality"], "missing_close")
        self.assertEqual(result["plan_compliance"]["violations"], 3)
        self.assertEqual(
            [item["reason"] for item in result["plan_compliance"]["violation_details"]],
            ["before_signal", "after_invalidation", "without_signal"],
        )

    def test_exit_without_entry_evidence_is_not_classified_as_triggered(self):
        exit_only = TradingPlaybookReviewService().summarize(
            [{"id": 1, "stock_code": "000001", "status": "exit"}],
            [{"candidate_id": 1, "event_type": "exit_triggered"}],
            {},
        )
        entered_then_exited = TradingPlaybookReviewService().summarize(
            [{"id": 2, "stock_code": "000002", "status": "exit"}],
            [
                {"candidate_id": 2, "event_type": "entry_triggered"},
                {"candidate_id": 2, "event_type": "exit_triggered"},
            ],
            {},
        )

        self.assertEqual(exit_only["not_triggered"], ["000001"])
        self.assertEqual(exit_only["triggered_not_executed"], [])
        self.assertEqual(
            entered_then_exited["triggered_not_executed"],
            ["000002"],
        )


class TradingPlaybookReviewPersistenceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = TemporaryDirectory()
        path = Path(self.directory.name) / "review.db"
        url = f"sqlite+aiosqlite:///{path.as_posix()}"
        self.engines = [
            create_async_engine(url, connect_args={"timeout": 30})
            for _ in range(2)
        ]
        for engine in self.engines:
            event.listen(engine.sync_engine, "connect", self._configure_sqlite)
        self.sessions = [
            async_sessionmaker(engine, expire_on_commit=False)
            for engine in self.engines
        ]
        async with self.engines[0].begin() as connection:
            for table in (
                TradingPlanVersion.__table__,
                TradingPlanCandidate.__table__,
                TradingAlertEvent.__table__,
                TradingExecutionReview.__table__,
                TradingPlaybookSettings.__table__,
                MarketReviewStockDaily.__table__,
            ):
                await connection.run_sync(table.create)

    async def asyncTearDown(self):
        for engine in self.engines:
            await engine.dispose()
        self.directory.cleanup()

    @staticmethod
    def _configure_sqlite(dbapi_connection, _connection_record):
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA busy_timeout=30000")
        finally:
            cursor.close()

    async def _add_plan(
        self,
        *,
        version_no=1,
        target_trade_date=TRADE_DATE,
        stock_code="000001",
        action_trade_date=TRADE_DATE,
        status="active",
        confirmed=True,
        candidate_status="triggered",
        add_entry_event=True,
    ):
        async with self.sessions[0]() as db:
            plan = TradingPlanVersion(
                source_trade_date=date(2026, 7, 13),
                target_trade_date=target_trade_date,
                stage="auction",
                version_no=version_no,
                status=status,
                market_state_json={},
                theme_ranking_json=[],
                mode_radar_json=[],
                rule_snapshot_json=[],
                risk_settings_json={},
                data_quality_json={},
                change_summary_json={},
                input_hash=f"plan-{version_no}-{stock_code}",
                generated_at=datetime(2026, 7, 14, 9, 26),
                confirmed_at=(
                    datetime(2026, 7, 14, 9, 27)
                    if confirmed
                    else None
                ),
                confirmed_by="tester" if confirmed else None,
            )
            db.add(plan)
            await db.flush()
            candidate = TradingPlanCandidate(
                plan_version_id=plan.id,
                stock_code=stock_code,
                stock_name=f"股票{stock_code}",
                action_trade_date=action_trade_date,
                theme_name="测试",
                primary_mode_key=f"mode-{version_no}",
                supporting_mode_keys_json=[],
                role="leader",
                rank=1,
                recognition_json={},
                entry_trigger_json={"price_gte": 10.0},
                invalidation_json={"price_lte": 9.5},
                exit_trigger_json={"price_lte": 9.8},
                risk_level="trial",
                position_reference=10.0,
                evidence_json=[],
                manual_overrides_json={},
                status=candidate_status,
            )
            db.add(candidate)
            await db.flush()
            if add_entry_event:
                db.add(
                    TradingAlertEvent(
                        plan_version_id=plan.id,
                        candidate_id=candidate.id,
                        event_type="entry_triggered",
                        severity="info",
                        dedup_key=f"entry-{plan.id}-{candidate.id}",
                        triggered_at=datetime(2026, 7, 14, 10, 0),
                        market_snapshot_json={
                            "price": 10.0,
                            "trade_date": action_trade_date.isoformat(),
                        },
                        message="entry",
                        channel_status_json={"in_app": "sent"},
                    )
                )
            await db.commit()
            return plan.id, candidate.id

    async def _add_candidate(
        self,
        plan_id,
        *,
        stock_code,
        action_trade_date,
        status="waiting",
        rank=2,
    ):
        async with self.sessions[0]() as db:
            candidate = TradingPlanCandidate(
                plan_version_id=plan_id,
                stock_code=stock_code,
                stock_name=f"股票{stock_code}",
                action_trade_date=action_trade_date,
                theme_name="测试",
                primary_mode_key=f"mode-{stock_code}",
                supporting_mode_keys_json=[],
                role="follower",
                rank=rank,
                recognition_json={},
                entry_trigger_json={"price_gte": 10.0},
                invalidation_json={"price_lte": 9.5},
                exit_trigger_json={"price_lte": 9.8},
                risk_level="trial",
                position_reference=10.0,
                evidence_json=[],
                manual_overrides_json={},
                status=status,
            )
            db.add(candidate)
            await db.commit()
            return candidate.id

    async def _add_action_event(
        self,
        plan_id,
        candidate_id,
        *,
        event_type,
        snapshot_trade_date,
        suffix,
    ):
        snapshot = {"price": 10.0}
        if snapshot_trade_date is not None:
            snapshot["trade_date"] = snapshot_trade_date
        async with self.sessions[0]() as db:
            event_row = TradingAlertEvent(
                plan_version_id=plan_id,
                candidate_id=candidate_id,
                event_type=event_type,
                severity="info",
                dedup_key=f"event-{plan_id}-{candidate_id}-{suffix}",
                triggered_at=datetime(2026, 7, 14, 10, 0),
                market_snapshot_json=snapshot,
                message=event_type,
                channel_status_json={"in_app": "sent"},
            )
            db.add(event_row)
            await db.commit()
            return event_row.id

    async def _add_outcome(
        self,
        *,
        trade_date=TRADE_DATE,
        stock_code="000001",
        close_price=10.5,
        change_pct=5.0,
    ):
        async with self.sessions[0]() as db:
            row = MarketReviewStockDaily(
                trade_date=trade_date,
                stock_id=1,
                stock_code=stock_code,
                stock_name=f"股票{stock_code}",
                close_price=close_price,
                pre_close=10.0,
                change_pct=change_pct,
                today_touched_limit_up=False,
                today_sealed_close=False,
                today_opened_close=False,
                today_broken=False,
                open_count=0,
                data_quality_flag="ok",
                updated_at=datetime.combine(trade_date, datetime.min.time()).replace(
                    hour=15,
                    minute=20,
                ),
            )
            db.add(row)
            await db.commit()
            return row.id

    async def test_preliminary_and_final_build_reconcile_same_row_and_keep_manual_execution(self):
        plan_id, candidate_id = await self._add_plan()
        await self._add_outcome(close_price=10.5, change_pct=5.0)
        service = TradingPlaybookReviewService(
            now_provider=lambda: FINALIZED_AT,
        )

        async with self.sessions[0]() as db:
            preliminary = await service.build(db, TRADE_DATE, finalized=False)
        self.assertEqual(len(preliminary), 1)
        review_id = preliminary[0].id
        generated_at = preliminary[0].generated_at
        self.assertIsNone(preliminary[0].finalized_at)

        async with self.sessions[0]() as db:
            updated = await service.update_manual_execution(
                db,
                TRADE_DATE,
                {
                    str(candidate_id): {
                        "executed": True,
                        "execution_price": 10.1,
                        "quantity": 100,
                        "executed_at": FINALIZED_AT,
                    }
                },
            )
        self.assertTrue(updated.manual_execution_json[str(candidate_id)]["executed"])

        async with self.sessions[0]() as db:
            outcome = await db.scalar(
                select(MarketReviewStockDaily).where(
                    MarketReviewStockDaily.trade_date == TRADE_DATE,
                    MarketReviewStockDaily.stock_code == "000001",
                )
            )
            outcome.close_price = 11.0
            outcome.change_pct = 10.0
            db.add(
                MarketReviewStockDaily(
                    trade_date=date(2026, 7, 15),
                    stock_id=1,
                    stock_code="000001",
                    stock_name="股票000001",
                    close_price=99.0,
                    change_pct=99.0,
                )
            )
            await db.commit()

        async with self.sessions[1]() as db:
            finalized = await service.build(db, TRADE_DATE, finalized=True)

        self.assertEqual(len(finalized), 1)
        row = finalized[0]
        self.assertEqual(row.id, review_id)
        self.assertEqual(row.plan_version_id, plan_id)
        self.assertEqual(row.generated_at, generated_at)
        self.assertIsNotNone(row.finalized_at)
        self.assertEqual(
            row.manual_execution_json[str(candidate_id)]["execution_price"],
            10.1,
        )
        self.assertEqual(
            row.outcome_snapshot_json["000001"]["close_price"],
            11.0,
        )
        self.assertEqual(
            row.outcome_snapshot_json["000001"]["trade_date"],
            TRADE_DATE.isoformat(),
        )
        self.assertEqual(row.data_quality_json["status"], "ready")
        self.assertNotIn("account_profit", json.dumps(row.signal_review_json))

    async def test_generation_key_tracks_relevant_plan_set_and_target_validation(self):
        service = TradingPlaybookReviewService()
        async with self.sessions[0]() as db:
            empty_key = await service.generation_key(db, TRADE_DATE)

        plan_id, _ = await self._add_plan()
        async with self.sessions[0]() as db:
            first_key = await service.generation_key(db, TRADE_DATE)
            repeated_key = await service.generation_key(db, TRADE_DATE)
            targeted_key = await service.generation_key(
                db,
                TRADE_DATE,
                plan_version_id=plan_id,
            )
            with self.assertRaises(PlaybookNotFoundError):
                await service.generation_key(
                    db,
                    TRADE_DATE,
                    plan_version_id=999999,
                )

        self.assertNotEqual(empty_key, first_key)
        self.assertEqual(first_key, repeated_key)
        self.assertNotEqual(empty_key, targeted_key)
        self.assertEqual(len(first_key), 64)

    async def test_targeted_build_only_reconciles_selected_relevant_plan(self):
        first_plan_id, _ = await self._add_plan(
            version_no=1,
            stock_code="000001",
        )
        second_plan_id, _ = await self._add_plan(
            version_no=2,
            target_trade_date=TRADE_DATE + timedelta(days=1),
            stock_code="000002",
            action_trade_date=TRADE_DATE,
        )
        service = TradingPlaybookReviewService(now_provider=lambda: FINALIZED_AT)
        async with self.sessions[0]() as db:
            rows = await service.build(
                db,
                TRADE_DATE,
                finalized=True,
                plan_version_id=second_plan_id,
            )

        self.assertEqual([row.plan_version_id for row in rows], [second_plan_id])
        async with self.sessions[0]() as db:
            persisted = list(
                (
                    await db.scalars(
                        select(TradingExecutionReview).order_by(
                            TradingExecutionReview.plan_version_id
                        )
                    )
                ).all()
            )
        self.assertEqual(
            [row.plan_version_id for row in persisted],
            [second_plan_id],
        )
        self.assertNotEqual(first_plan_id, second_plan_id)

    async def test_new_plan_compensation_preserves_finalized_existing_review(self):
        first_plan_id, _ = await self._add_plan(
            version_no=1,
            stock_code="000001",
        )
        await self._add_outcome(stock_code="000001")
        service = TradingPlaybookReviewService(now_provider=lambda: FINALIZED_AT)
        async with self.sessions[0]() as db:
            first = (await service.build(db, TRADE_DATE, finalized=True))[0]
            first_finalized_at = first.finalized_at
            first_snapshot = copy.deepcopy(first.signal_review_json)

        second_plan_id, _ = await self._add_plan(
            version_no=2,
            target_trade_date=TRADE_DATE + timedelta(days=1),
            stock_code="000002",
            action_trade_date=TRADE_DATE,
        )
        await self._add_outcome(stock_code="000002")
        async with self.sessions[0]() as db:
            compensated = await service.build(db, TRADE_DATE, finalized=True)
        async with self.sessions[0]() as db:
            repeated = await service.build(db, TRADE_DATE, finalized=True)

        self.assertEqual(
            [row.plan_version_id for row in compensated],
            [first_plan_id, second_plan_id],
        )
        existing = next(
            row for row in repeated if row.plan_version_id == first_plan_id
        )
        self.assertEqual(existing.finalized_at, first_finalized_at)
        self.assertEqual(existing.signal_review_json, first_snapshot)
        self.assertEqual(len(repeated), 2)

    async def test_multi_plan_build_prefetches_major_facts_in_constant_queries(self):
        for index, stock_code in enumerate(("000001", "000002", "000003")):
            await self._add_plan(
                version_no=index + 1,
                target_trade_date=TRADE_DATE + timedelta(days=index),
                stock_code=stock_code,
                action_trade_date=TRADE_DATE,
            )
            await self._add_outcome(stock_code=stock_code)

        counts = {"candidates": 0, "events": 0, "outcomes": 0}

        def count_fact_queries(_conn, _cursor, statement, *_args):
            normalized = " ".join(statement.lower().split())
            if not normalized.startswith("select"):
                return
            if "from trading_plan_candidates" in normalized:
                counts["candidates"] += 1
            if "from trading_alert_events" in normalized:
                counts["events"] += 1
            if "from market_review_stock_daily" in normalized:
                counts["outcomes"] += 1

        event.listen(
            self.engines[0].sync_engine,
            "before_cursor_execute",
            count_fact_queries,
        )
        try:
            async with self.sessions[0]() as db:
                rows = await TradingPlaybookReviewService().build(
                    db,
                    TRADE_DATE,
                    finalized=True,
                )
        finally:
            event.remove(
                self.engines[0].sync_engine,
                "before_cursor_execute",
                count_fact_queries,
            )

        self.assertEqual(len(rows), 3)
        self.assertLessEqual(counts["candidates"], 2)
        self.assertEqual(counts["events"], 1)
        self.assertEqual(counts["outcomes"], 1)

    async def test_successful_review_build_emits_review_ready_through_shared_alert_service(self):
        plan_id, _ = await self._add_plan()
        alert_service = type(
            "FakeAlertService",
            (),
            {"notify_review_ready": AsyncMock()},
        )()
        service = TradingPlaybookReviewService(
            now_provider=lambda: FINALIZED_AT,
            alert_service=alert_service,
        )

        async with self.sessions[0]() as db:
            await service.build(db, TRADE_DATE, finalized=False)
        async with self.sessions[0]() as db:
            await service.build(db, TRADE_DATE, finalized=True)

        self.assertEqual(alert_service.notify_review_ready.await_count, 2)
        for call in alert_service.notify_review_ready.await_args_list:
            self.assertEqual(call.args[1]["id"], plan_id)
            self.assertEqual(call.args[2], TRADE_DATE)
            self.assertTrue(call.kwargs["send"])

    async def test_alert_failure_leaves_committed_review_for_idempotent_retry(self):
        await self._add_plan()

        class FailOnceAlertService(TradingPlaybookAlertService):
            failed = False

            async def _deliver(self, db, event):
                if not self.failed:
                    self.failed = True
                    raise RuntimeError("send failed")
                return await super()._deliver(db, event)

        channel = _ReviewRecordingChannel()
        alert_service = FailOnceAlertService(channel)
        service = TradingPlaybookReviewService(
            now_provider=lambda: FINALIZED_AT,
            alert_service=alert_service,
        )

        async with self.sessions[0]() as db:
            db.add(
                TradingPlaybookSettings(
                    id=1,
                    enabled=True,
                    in_app_enabled=True,
                    wechat_enabled=False,
                )
            )
            await db.commit()
            with self.assertRaisesRegex(RuntimeError, "send failed"):
                await service.build(db, TRADE_DATE, finalized=False)
        async with self.sessions[0]() as db:
            rows = list((await db.scalars(select(TradingExecutionReview))).all())
            self.assertEqual(len(rows), 1)
            events = list(
                (
                    await db.scalars(
                        select(TradingAlertEvent).where(
                            TradingAlertEvent.event_type == "review_ready"
                        )
                    )
                ).all()
            )
            self.assertEqual(len(events), 1)
        async with self.sessions[0]() as db:
            retried = await service.build(db, TRADE_DATE, finalized=False)
            self.assertEqual(len(retried), 1)
        self.assertEqual(len(channel.sends), 1)
        async with self.sessions[0]() as db:
            events = list(
                (
                    await db.scalars(
                        select(TradingAlertEvent).where(
                            TradingAlertEvent.event_type == "review_ready"
                        )
                    )
                ).all()
            )
            self.assertEqual(len(events), 1)

    async def test_missing_same_day_outcome_is_explicitly_partial_and_never_falls_back(self):
        await self._add_plan()
        await self._add_outcome(
            trade_date=date(2026, 7, 15),
            close_price=99.0,
            change_pct=99.0,
        )
        service = TradingPlaybookReviewService(
            now_provider=lambda: FINALIZED_AT,
        )

        async with self.sessions[0]() as db:
            rows = await service.build(db, TRADE_DATE, finalized=True)

        row = rows[0]
        self.assertEqual(row.outcome_snapshot_json, {})
        self.assertEqual(row.data_quality_json["status"], "partial")
        self.assertEqual(row.data_quality_json["missing_stock_codes"], ["000001"])
        self.assertEqual(row.data_quality_json["trade_date"], TRADE_DATE.isoformat())

    async def test_update_uses_candidate_ids_to_select_one_review_and_rejects_ambiguity(self):
        first_plan_id, first_candidate_id = await self._add_plan(
            version_no=1,
            stock_code="000001",
        )
        second_plan_id, second_candidate_id = await self._add_plan(
            version_no=2,
            target_trade_date=date(2026, 7, 15),
            stock_code="000002",
            action_trade_date=TRADE_DATE,
        )
        service = TradingPlaybookReviewService(
            now_provider=lambda: FINALIZED_AT,
        )
        async with self.sessions[0]() as db:
            rows = await service.build(db, TRADE_DATE, finalized=False)
        self.assertEqual({row.plan_version_id for row in rows}, {first_plan_id, second_plan_id})

        async with self.sessions[0]() as db:
            selected = await service.update_manual_execution(
                db,
                TRADE_DATE,
                {str(second_candidate_id): {"executed": False}},
            )
        self.assertEqual(selected.plan_version_id, second_plan_id)

        async with self.sessions[0]() as db:
            with self.assertRaises(InvalidTransitionError):
                await service.update_manual_execution(db, TRADE_DATE, {})
        async with self.sessions[0]() as db:
            with self.assertRaises(InvalidTransitionError):
                await service.update_manual_execution(
                    db,
                    TRADE_DATE,
                    {
                        str(first_candidate_id): {"executed": True},
                        str(second_candidate_id): {"executed": True},
                    },
                )

    async def test_update_rejects_known_same_plan_candidate_from_another_action_date(self):
        plan_id, today_candidate_id = await self._add_plan()
        other_date_candidate_id = await self._add_candidate(
            plan_id,
            stock_code="000009",
            action_trade_date=TRADE_DATE + timedelta(days=1),
        )
        service = TradingPlaybookReviewService(
            now_provider=lambda: FINALIZED_AT,
        )
        async with self.sessions[0]() as db:
            await service.build(db, TRADE_DATE, finalized=False)

        async with self.sessions[0]() as db:
            with self.assertRaises(InvalidTransitionError):
                await service.update_manual_execution(
                    db,
                    TRADE_DATE,
                    {
                        str(today_candidate_id): {"executed": False},
                        str(other_date_candidate_id): {"executed": True},
                    },
                )

    async def test_update_rejects_known_other_plan_candidate_from_another_action_date(self):
        _, today_candidate_id = await self._add_plan(
            version_no=1,
            stock_code="000001",
        )
        _, other_candidate_id = await self._add_plan(
            version_no=2,
            target_trade_date=TRADE_DATE + timedelta(days=1),
            stock_code="000002",
            action_trade_date=TRADE_DATE + timedelta(days=1),
        )
        service = TradingPlaybookReviewService(
            now_provider=lambda: FINALIZED_AT,
        )
        async with self.sessions[0]() as db:
            await service.build(db, TRADE_DATE, finalized=False)

        async with self.sessions[0]() as db:
            with self.assertRaises(InvalidTransitionError):
                await service.update_manual_execution(
                    db,
                    TRADE_DATE,
                    {
                        str(today_candidate_id): {"executed": False},
                        str(other_candidate_id): {"executed": True},
                    },
                )

    async def test_truly_unknown_candidate_id_is_rejected(self):
        _, today_candidate_id = await self._add_plan()
        service = TradingPlaybookReviewService(
            now_provider=lambda: FINALIZED_AT,
        )
        async with self.sessions[0]() as db:
            await service.build(db, TRADE_DATE, finalized=False)
            with self.assertRaises(InvalidTransitionError):
                await service.update_manual_execution(
                    db,
                    TRADE_DATE,
                    {
                        str(today_candidate_id): {"executed": False},
                        "999999": {"executed": True},
                    },
                )

    async def test_explicit_unplanned_execution_is_persisted_without_profit_inference(self):
        _, today_candidate_id = await self._add_plan()
        service = TradingPlaybookReviewService(
            now_provider=lambda: FINALIZED_AT,
        )
        unplanned = [
            {
                "executed": True,
                "stock_code": "600000",
                "stock_name": "浦发银行",
                "execution_price": 10.2,
                "quantity": 100,
                "executed_at": "2026-07-14T10:31:00+08:00",
                "manual_note": "临盘计划外成交",
            }
        ]
        async with self.sessions[0]() as db:
            await service.build(db, TRADE_DATE, finalized=False)
            updated = await service.update_manual_execution(
                db,
                TRADE_DATE,
                {str(today_candidate_id): {"executed": False}},
                unplanned_executions=unplanned,
            )

        self.assertEqual(updated.manual_execution_json["_unplanned"], unplanned)
        self.assertEqual(updated.plan_compliance_json["unplanned"], 1)
        self.assertEqual(
            updated.signal_review_json["unplanned_executions"],
            unplanned,
        )
        serialized = json.dumps(updated.signal_review_json, ensure_ascii=False)
        self.assertNotIn("profit", serialized.lower())
        self.assertNotIn("pnl", serialized.lower())

    async def test_only_unplanned_execution_cannot_select_among_multiple_reviews(self):
        await self._add_plan(version_no=1, stock_code="000001")
        await self._add_plan(
            version_no=2,
            target_trade_date=TRADE_DATE + timedelta(days=1),
            stock_code="000002",
            action_trade_date=TRADE_DATE,
        )
        service = TradingPlaybookReviewService(now_provider=lambda: FINALIZED_AT)
        async with self.sessions[0]() as db:
            await service.build(db, TRADE_DATE, finalized=False)
            with self.assertRaises(InvalidTransitionError):
                await service.update_manual_execution(
                    db,
                    TRADE_DATE,
                    {},
                    unplanned_executions=[
                        {
                            "executed": True,
                            "stock_code": "600000",
                            "stock_name": "浦发银行",
                        }
                    ],
                )

    async def test_update_without_a_review_is_not_found(self):
        service = TradingPlaybookReviewService()
        async with self.sessions[0]() as db:
            with self.assertRaises(PlaybookNotFoundError):
                await service.update_manual_execution(
                    db,
                    TRADE_DATE,
                    {"1": {"executed": False}},
                )

    async def test_empty_put_clears_the_only_reviews_manual_execution(self):
        _, candidate_id = await self._add_plan()
        service = TradingPlaybookReviewService(
            now_provider=lambda: FINALIZED_AT,
        )
        async with self.sessions[0]() as db:
            await service.build(db, TRADE_DATE, finalized=False)
            await service.update_manual_execution(
                db,
                TRADE_DATE,
                {str(candidate_id): {"executed": True}},
            )

        async with self.sessions[0]() as db:
            cleared = await service.update_manual_execution(
                db,
                TRADE_DATE,
                {},
            )

        self.assertEqual(cleared.manual_execution_json, {})
        self.assertEqual(cleared.plan_compliance_json["executed"], 0)

    async def test_manual_update_after_finalization_keeps_final_market_facts(self):
        _, candidate_id = await self._add_plan()
        await self._add_outcome(close_price=10.8, change_pct=8.0)
        service = TradingPlaybookReviewService(
            now_provider=lambda: FINALIZED_AT,
        )
        async with self.sessions[0]() as db:
            finalized = (
                await service.build(db, TRADE_DATE, finalized=True)
            )[0]
            original_finalized_at = finalized.finalized_at
            original_outcome = dict(finalized.outcome_snapshot_json)
            original_quality = dict(finalized.data_quality_json)

        async with self.sessions[1]() as db:
            updated = await service.update_manual_execution(
                db,
                TRADE_DATE,
                {str(candidate_id): {"executed": False}},
            )

        self.assertEqual(updated.finalized_at, original_finalized_at)
        self.assertEqual(updated.outcome_snapshot_json, original_outcome)
        self.assertEqual(updated.data_quality_json, original_quality)

    async def test_single_existing_review_never_absorbs_another_plan_candidate(self):
        first_plan_id, _ = await self._add_plan(
            version_no=1,
            stock_code="000001",
        )
        _, second_candidate_id = await self._add_plan(
            version_no=2,
            target_trade_date=date(2026, 7, 15),
            stock_code="000002",
            action_trade_date=TRADE_DATE,
        )
        service = TradingPlaybookReviewService(
            now_provider=lambda: FINALIZED_AT,
        )
        async with self.sessions[0]() as db:
            db.add(
                TradingExecutionReview(
                    trade_date=TRADE_DATE,
                    plan_version_id=first_plan_id,
                    signal_review_json={},
                    manual_execution_json={},
                    plan_compliance_json={},
                    outcome_snapshot_json={},
                    data_quality_json={},
                    generated_at=datetime(2026, 7, 14, 15, 10),
                )
            )
            await db.commit()

        async with self.sessions[0]() as db:
            with self.assertRaises(InvalidTransitionError):
                await service.update_manual_execution(
                    db,
                    TRADE_DATE,
                    {str(second_candidate_id): {"executed": True}},
                )

    async def test_draft_plan_cancelled_before_confirmation_is_not_reviewed(self):
        await self._add_plan(status="expired", confirmed=False)
        service = TradingPlaybookReviewService(
            now_provider=lambda: FINALIZED_AT,
        )

        async with self.sessions[0]() as db:
            rows = await service.build(db, TRADE_DATE, finalized=False)

        self.assertEqual(rows, [])

    async def test_other_day_and_malformed_events_are_excluded_with_quality_warnings(self):
        plan_id, candidate_id = await self._add_plan(
            candidate_status="exit",
            add_entry_event=False,
        )
        await self._add_outcome()
        await self._add_action_event(
            plan_id,
            candidate_id,
            event_type="entry_triggered",
            snapshot_trade_date=(TRADE_DATE - timedelta(days=1)).isoformat(),
            suffix="past",
        )
        await self._add_action_event(
            plan_id,
            candidate_id,
            event_type="entry_triggered",
            snapshot_trade_date=(TRADE_DATE + timedelta(days=1)).isoformat(),
            suffix="future",
        )
        await self._add_action_event(
            plan_id,
            candidate_id,
            event_type="entry_triggered",
            snapshot_trade_date=None,
            suffix="missing",
        )
        await self._add_action_event(
            plan_id,
            candidate_id,
            event_type="exit_triggered",
            snapshot_trade_date=TRADE_DATE.isoformat(),
            suffix="valid-exit",
        )
        service = TradingPlaybookReviewService(
            now_provider=lambda: FINALIZED_AT,
        )

        async with self.sessions[0]() as db:
            row = (await service.build(db, TRADE_DATE, finalized=False))[0]

        self.assertEqual(row.signal_review_json["not_triggered"], ["000001"])
        self.assertEqual(
            row.signal_review_json["signal_outcomes"][0]["event_types"],
            ["exit_triggered"],
        )
        warnings = row.data_quality_json["event_warnings"]
        self.assertEqual(len(warnings), 3)
        self.assertEqual(
            {warning["reason"] for warning in warnings},
            {"trade_date_mismatch", "missing_trade_date"},
        )
        self.assertEqual(row.data_quality_json["status"], "degraded")

    async def test_final_build_racing_manual_update_never_overwrites_manual_json(self):
        _, candidate_id = await self._add_plan()
        await self._add_outcome()
        base_service = TradingPlaybookReviewService(
            now_provider=lambda: FINALIZED_AT,
        )
        async with self.sessions[0]() as db:
            await base_service.build(db, TRADE_DATE, finalized=False)

        outcome_loaded = asyncio.Event()
        release_outcome = asyncio.Event()
        load_calls = 0

        async def blocking_outcome_loader(db, trade_date, stock_codes):
            nonlocal load_calls
            load_calls += 1
            rows = await TradingPlaybookReviewService.load_outcomes(
                db,
                trade_date,
                stock_codes,
            )
            if load_calls == 1:
                outcome_loaded.set()
                await release_outcome.wait()
            return rows

        final_service = TradingPlaybookReviewService(
            now_provider=lambda: FINALIZED_AT,
            outcome_loader=blocking_outcome_loader,
        )

        async def finalize():
            async with self.sessions[0]() as db:
                return await final_service.build(db, TRADE_DATE, finalized=True)

        final_task = asyncio.create_task(finalize())
        await asyncio.wait_for(outcome_loaded.wait(), timeout=3)
        async with self.sessions[1]() as db:
            await base_service.update_manual_execution(
                db,
                TRADE_DATE,
                {str(candidate_id): {"executed": True, "quantity": 100}},
            )
        release_outcome.set()
        await asyncio.wait_for(final_task, timeout=5)

        async with self.sessions[1]() as db:
            row = await db.scalar(select(TradingExecutionReview))
        self.assertEqual(
            row.manual_execution_json,
            {str(candidate_id): {"executed": True, "quantity": 100}},
        )
        self.assertEqual(row.signal_review_json["triggered_executed"], ["000001"])
        self.assertIsNotNone(row.finalized_at)

    async def test_repeated_final_build_is_content_idempotent_after_manual_update(self):
        _, candidate_id = await self._add_plan()
        await self._add_outcome(close_price=10.8, change_pct=8.0)
        load_calls = 0

        async def counted_loader(db, trade_date, stock_codes):
            nonlocal load_calls
            load_calls += 1
            return await TradingPlaybookReviewService.load_outcomes(
                db,
                trade_date,
                stock_codes,
            )

        service = TradingPlaybookReviewService(
            now_provider=lambda: FINALIZED_AT,
            outcome_loader=counted_loader,
        )
        async with self.sessions[0]() as db:
            await service.build(db, TRADE_DATE, finalized=True)
        async with self.sessions[1]() as db:
            updated = await service.update_manual_execution(
                db,
                TRADE_DATE,
                {str(candidate_id): {"executed": True, "quantity": 100}},
            )
            expected = {
                "signal": copy.deepcopy(updated.signal_review_json),
                "manual": copy.deepcopy(updated.manual_execution_json),
                "outcome": copy.deepcopy(updated.outcome_snapshot_json),
                "quality": copy.deepcopy(updated.data_quality_json),
                "finalized_at": updated.finalized_at,
            }
        async with self.sessions[0]() as db:
            fact = await db.scalar(select(MarketReviewStockDaily))
            fact.close_price = 20.0
            fact.change_pct = 100.0
            await db.commit()
        later_service = TradingPlaybookReviewService(
            now_provider=lambda: CN_TZ.localize(
                datetime(2026, 7, 14, 16, 0)
            ),
            outcome_loader=counted_loader,
        )

        async with self.sessions[1]() as db:
            repeated = (
                await later_service.build(db, TRADE_DATE, finalized=True)
            )[0]

        self.assertEqual(load_calls, 1)
        self.assertEqual(repeated.signal_review_json, expected["signal"])
        self.assertEqual(repeated.manual_execution_json, expected["manual"])
        self.assertEqual(repeated.outcome_snapshot_json, expected["outcome"])
        self.assertEqual(repeated.data_quality_json, expected["quality"])
        self.assertEqual(repeated.finalized_at, expected["finalized_at"])

    async def test_two_engines_build_once_and_restart_reuses_the_same_row(self):
        await self._add_plan()
        await self._add_outcome()
        services = [
            TradingPlaybookReviewService(now_provider=lambda: FINALIZED_AT),
            TradingPlaybookReviewService(now_provider=lambda: FINALIZED_AT),
        ]

        async def build(index):
            async with self.sessions[index]() as db:
                return await services[index].build(db, TRADE_DATE, finalized=False)

        first, second = await asyncio.gather(build(0), build(1))
        async with self.sessions[0]() as db:
            count = await db.scalar(select(func.count(TradingExecutionReview.id)))
            stored = await db.scalar(select(TradingExecutionReview))
        self.assertEqual(count, 1)
        self.assertEqual(first[0].id, stored.id)
        self.assertEqual(second[0].id, stored.id)

        restarted = TradingPlaybookReviewService(
            now_provider=lambda: CN_TZ.localize(datetime(2026, 7, 14, 15, 40)),
        )
        async with self.sessions[1]() as db:
            rows = await restarted.build(db, TRADE_DATE, finalized=False)
        self.assertEqual(rows[0].id, stored.id)

    def test_postgresql_review_insert_uses_conflict_safe_unique_key(self):
        statement = TradingPlaybookReviewService.review_insert_statement(
            "postgresql",
            trade_date=TRADE_DATE,
            plan_version_id=7,
            generated_at=datetime(2026, 7, 14, 15, 10),
        )

        sql = str(
            statement.compile(
                dialect=postgresql.dialect(),
                compile_kwargs={"literal_binds": False},
            )
        )
        self.assertIn("ON CONFLICT (trade_date, plan_version_id) DO NOTHING", sql)

    def test_postgresql_review_update_locks_the_row_without_json_equality(self):
        statement = TradingPlaybookReviewService.review_select_statement(
            7,
            for_update=True,
        )

        sql = str(statement.compile(dialect=postgresql.dialect()))
        self.assertIn("FOR UPDATE", sql)
        self.assertNotIn("manual_execution_json =", sql)

    def test_event_query_is_postgresql_safe_and_filters_json_in_python(self):
        statement = TradingPlaybookReviewService.event_select_statement(
            7,
            [11, 12],
        )

        sql = str(statement.compile(dialect=postgresql.dialect()))
        self.assertNotIn("->", sql)
        self.assertNotIn("json_extract", sql.lower())
        self.assertIn("trading_alert_events.candidate_id IN", sql)

    def test_nonfinite_close_fact_is_removed_and_marks_quality_degraded(self):
        outcome, quality = TradingPlaybookReviewService._outcome_payload(
            TRADE_DATE,
            ["000001"],
            [
                {
                    "trade_date": TRADE_DATE,
                    "stock_code": "000001",
                    "close_price": float("nan"),
                    "pre_close": 10.0,
                    "change_pct": float("inf"),
                    "data_quality_flag": "ok",
                }
            ],
            finalized=True,
        )

        self.assertIsNone(outcome["000001"]["close_price"])
        self.assertIsNone(outcome["000001"]["change_pct"])
        self.assertEqual(quality["status"], "degraded")
        self.assertEqual(quality["degraded_stock_codes"], ["000001"])

    def test_outcome_freshness_uses_china_date_for_naive_and_aware_timestamps(self):
        base = {
            "trade_date": TRADE_DATE,
            "stock_code": "000001",
            "close_price": 10.5,
            "pre_close": 10.0,
            "change_pct": 5.0,
            "open_count": 0,
            "data_quality_flag": "ok",
        }
        cases = [
            (datetime(2026, 7, 14, 15, 20), "ready", "fresh"),
            (
                datetime(2026, 7, 14, 7, 20, tzinfo=timezone.utc),
                "ready",
                "fresh",
            ),
            (
                datetime(2026, 7, 14, 16, 20, tzinfo=timezone.utc),
                "degraded",
                "stale",
            ),
            (None, "degraded", "unknown"),
            ("not-a-time", "degraded", "unknown"),
        ]

        for updated_at, expected_status, expected_freshness in cases:
            with self.subTest(updated_at=updated_at):
                row = dict(base, updated_at=updated_at)
                outcome, quality = TradingPlaybookReviewService._outcome_payload(
                    TRADE_DATE,
                    ["000001"],
                    [row],
                    finalized=True,
                )
                self.assertEqual(quality["status"], expected_status)
                self.assertEqual(
                    outcome["000001"]["freshness"],
                    expected_freshness,
                )

        _, preliminary_quality = TradingPlaybookReviewService._outcome_payload(
            TRADE_DATE,
            ["000001"],
            [dict(base, updated_at=None)],
            finalized=False,
        )
        self.assertEqual(preliminary_quality["status"], "degraded")


if __name__ == "__main__":
    unittest.main()
