import asyncio
import json
import unittest
from datetime import date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from sqlalchemy import event, func, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.models.market_review import MarketReviewStockDaily
from app.models.trading_playbook import (
    TradingAlertEvent,
    TradingExecutionReview,
    TradingPlanCandidate,
    TradingPlanVersion,
)
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
            {"planned": 3, "executed": 0, "unplanned": 0},
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
                "999": {"executed": True, "execution_price": 9.0},
            },
        )

        self.assertEqual(result["triggered_executed"], ["000001"])
        self.assertEqual(result["not_triggered"], ["000002"])
        self.assertEqual(
            result["plan_compliance"],
            {"planned": 2, "executed": 1, "unplanned": 1},
        )
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
                },
                "signal_outcomes": [],
            },
        )

    def test_signal_outcome_keeps_delivery_and_acknowledgement_audit(self):
        result = TradingPlaybookReviewService().summarize(
            [{"id": 1, "stock_code": "000001", "status": "triggered"}],
            [
                {
                    "candidate_id": 1,
                    "event_type": "entry_triggered",
                    "triggered_at": datetime(2026, 7, 14, 10, 0),
                    "acknowledged_at": datetime(2026, 7, 14, 10, 1),
                    "channel_status_json": {"in_app": "sent"},
                }
            ],
            {"1": {"executed": False}},
        )

        audit = result["signal_outcomes"][0]["events"][0]
        self.assertEqual(audit["event_type"], "entry_triggered")
        self.assertEqual(audit["triggered_at"], "2026-07-14T10:00:00")
        self.assertEqual(audit["acknowledged_at"], "2026-07-14T10:01:00")
        self.assertEqual(audit["channel_status"], {"in_app": "sent"})


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
                status="triggered",
            )
            db.add(candidate)
            await db.flush()
            db.add(
                TradingAlertEvent(
                    plan_version_id=plan.id,
                    candidate_id=candidate.id,
                    event_type="entry_triggered",
                    severity="info",
                    dedup_key=f"entry-{plan.id}-{candidate.id}",
                    triggered_at=datetime(2026, 7, 14, 10, 0),
                    market_snapshot_json={"price": 10.0},
                    message="entry",
                    channel_status_json={"in_app": "sent"},
                )
            )
            await db.commit()
            return plan.id, candidate.id

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


if __name__ == "__main__":
    unittest.main()
