import asyncio
import json
import unittest
from datetime import date, datetime, timedelta
from unittest.mock import patch
from zoneinfo import ZoneInfo

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool

from app.api.v1 import trading_playbook as trading_playbook_api
from app.api.v1.trading_playbook import (
    get_trading_playbook_now,
    get_trading_playbook_orchestrator,
    get_trading_playbook_review_service,
    router,
)
from app.database import Base, get_db
from app.models.trading_playbook import (
    TradingAlertEvent,
    TradingModeRule,
    TradingPlanCandidate,
    TradingPlanVersion,
    TradingPlaybookSettings,
)
from app.services.trading_playbook.runtime import trading_playbook_runtime
from app.services.trading_playbook import plan_service as plan_service_module
from app.services.trading_playbook import serialization as serialization_module
from app.services.trading_playbook.errors import (
    InvalidRequestError,
    InvalidTransitionError,
    PlaybookNotFoundError,
    UpstreamUnavailableError,
)


CN = ZoneInfo("Asia/Shanghai")
FIXED_NOW = datetime(2026, 7, 10, 15, 30, tzinfo=CN)


class _FakeOrchestrator:
    def __init__(self):
        self.calls = []

    async def build_stage(self, db, source_trade_date, stage, as_of):
        self.calls.append((db, source_trade_date, stage, as_of))
        target_trade_date = source_trade_date + timedelta(days=1)
        return {
            "id": 901,
            "source_trade_date": source_trade_date.isoformat(),
            "target_trade_date": target_trade_date.isoformat(),
            "stage": stage,
            "status": "draft",
            "generated_at": as_of.isoformat(),
            "risk_settings_json": {
                "trial": 10.0,
                "confirmed": 30.0,
                "hard_stop": 5.0,
                "max_candidates": 1,
            },
            "candidates": [
                {
                    "stock_code": "000001",
                    "stock_name": "平安银行",
                    "action_trade_date": target_trade_date.isoformat(),
                    "primary_mode_key": "a_mode",
                    "rank": 1,
                    "position_reference": 10.0,
                    "risk_level": "trial",
                    "entry_trigger_json": {"reference_price": 10.0},
                    "invalidation_json": {"price_lte": 9.5},
                    "exit_trigger_json": {"change_pct_lte": -5.0},
                }
            ],
        }


class _FakeReviewService:
    def __init__(self):
        self.calls = []

    async def update_manual_execution(self, db, trade_date, executions):
        self.calls.append((db, trade_date, executions))
        return {
            "id": 501,
            "trade_date": trade_date.isoformat(),
            "plan_version_id": 1,
            "signal_review_json": {},
            "manual_execution_json": executions,
            "plan_compliance_json": {},
            "outcome_snapshot_json": {},
            "data_quality_json": {},
            "generated_at": FIXED_NOW,
            "finalized_at": None,
        }


class TradingPlaybookApiTests(unittest.TestCase):
    def setUp(self):
        trading_playbook_runtime.reset()
        self.engine = create_async_engine(
            "sqlite+aiosqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        self.Session = async_sessionmaker(self.engine, expire_on_commit=False)
        asyncio.run(self._seed())

        self.orchestrator = _FakeOrchestrator()
        self.review_service = _FakeReviewService()
        app = FastAPI()
        app.include_router(router, prefix="/trading-playbook")

        async def override_db():
            async with self.Session() as db:
                yield db

        app.dependency_overrides[get_db] = override_db
        app.dependency_overrides[get_trading_playbook_orchestrator] = (
            lambda: self.orchestrator
        )
        app.dependency_overrides[get_trading_playbook_review_service] = (
            lambda: self.review_service
        )
        app.dependency_overrides[get_trading_playbook_now] = lambda: FIXED_NOW
        self.app = app
        self.client = TestClient(app)

    async def _seed(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        async with self.Session() as db:
            enabled_rule = TradingModeRule(
                mode_key="z_mode",
                version=2,
                name="Z",
                family="outbreak",
                style="board_flow",
                window="outbreak",
                automation_level="assisted",
                description="rule-z",
                prerequisites_json={"priority": 2},
                candidate_filters_json=[{"field": "x"}],
                entry_trigger_json={"label": "entry"},
                invalidation_json={"label": "invalid"},
                exit_trigger_json={"label": "exit"},
                risk_guidance_json={"risk": "trial"},
                source_refs_json=[{"source_key": "s1", "excerpt": "e1"}],
                enabled=True,
                content_hash="a" * 64,
            )
            first_rule = TradingModeRule(
                mode_key="a_mode",
                version=1,
                name="A",
                family="outbreak",
                style="board_flow",
                window="outbreak",
                automation_level="automatic",
                description="rule-a",
                prerequisites_json={"priority": 1},
                candidate_filters_json=[],
                entry_trigger_json={},
                invalidation_json={},
                exit_trigger_json={},
                risk_guidance_json={},
                source_refs_json="malformed-but-preserved",
                enabled=True,
                content_hash="b" * 64,
            )
            disabled_rule = TradingModeRule(
                mode_key="disabled",
                version=1,
                name="D",
                family="decline",
                style="chaos_retreat",
                window="decline",
                automation_level="manual_only",
                description="disabled",
                prerequisites_json={},
                candidate_filters_json=[],
                entry_trigger_json={},
                invalidation_json={},
                exit_trigger_json={},
                risk_guidance_json={},
                source_refs_json=[],
                enabled=False,
                content_hash="c" * 64,
            )
            db.add_all([enabled_rule, first_rule, disabled_rule])
            plan = TradingPlanVersion(
                source_trade_date=date(2026, 7, 10),
                target_trade_date=date(2026, 7, 13),
                stage="after_close",
                version_no=1,
                status="draft",
                risk_settings_json={
                    "trial": 10.0,
                    "confirmed": 30.0,
                    "hard_stop": 5.0,
                    "max_candidates": 3,
                },
                input_hash="seed",
                generated_at=datetime(2026, 7, 10, 15, 30),
            )
            db.add(plan)
            await db.flush()
            db.add(
                TradingPlanCandidate(
                    plan_version_id=plan.id,
                    stock_code="000001",
                    stock_name="平安银行",
                    action_trade_date=date(2026, 7, 13),
                    theme_name="金融",
                    primary_mode_key="a_mode",
                    role="leader",
                    rank=1,
                    entry_trigger_json={"reference_price": 10},
                    invalidation_json={"price_lte": 9.5},
                    exit_trigger_json={"label": "按计划退出"},
                    risk_level="trial",
                    position_reference=10,
                    status="waiting",
                )
            )
            db.add_all(
                [
                    TradingAlertEvent(
                        plan_version_id=plan.id,
                        event_type="watch",
                        severity="info",
                        dedup_key="old",
                        triggered_at=datetime(2026, 7, 10, 14, 40),
                        message="old",
                    ),
                    TradingAlertEvent(
                        plan_version_id=plan.id,
                        event_type="confirmation_required",
                        severity="warning",
                        dedup_key="new",
                        triggered_at=datetime(2026, 7, 10, 15, 0),
                        message="new",
                    ),
                ]
            )
            await db.commit()

    def tearDown(self):
        self.client.close()
        asyncio.run(self.engine.dispose())
        trading_playbook_runtime.reset()

    def test_rules_are_enabled_stably_sorted_and_auditable(self):
        response = self.client.get("/trading-playbook/rules")
        self.assertEqual(response.status_code, 200, response.text)
        items = response.json()["items"]
        self.assertEqual([item["mode_key"] for item in items], ["z_mode", "a_mode"])
        self.assertEqual(items[1]["source_refs_json"], "malformed-but-preserved")
        for key in (
            "version",
            "content_hash",
            "prerequisites_json",
            "candidate_filters_json",
            "entry_trigger_json",
            "invalidation_json",
            "exit_trigger_json",
            "risk_guidance_json",
            "source_refs_json",
        ):
            self.assertIn(key, items[1])

    def test_rules_put_unrepresentable_priority_after_valid_priorities(self):
        async def add_rule():
            async with self.Session() as db:
                db.add(
                    TradingModeRule(
                        mode_key="overflow_priority",
                        version=1,
                        name="overflow",
                        family="outbreak",
                        style="board_flow",
                        window="outbreak",
                        automation_level="manual_only",
                        description="historical malformed priority",
                        prerequisites_json={"priority": 10**400},
                        candidate_filters_json=[],
                        entry_trigger_json={},
                        invalidation_json={},
                        exit_trigger_json={},
                        risk_guidance_json={},
                        source_refs_json=[],
                        enabled=True,
                        content_hash="d" * 64,
                    )
                )
                await db.commit()

        asyncio.run(add_rule())
        response = self.client.get("/trading-playbook/rules")
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(
            [item["mode_key"] for item in response.json()["items"]],
            ["z_mode", "a_mode", "overflow_priority"],
        )

    def test_plan_list_detail_revision_and_confirmation_are_serialized(self):
        listed = self.client.get(
            "/trading-playbook/plans", params={"trade_date": "2026-07-13"}
        )
        self.assertEqual(listed.status_code, 200, listed.text)
        plan = listed.json()["items"][0]
        self.assertEqual(len(plan["candidates"]), 1)
        self.assertTrue(plan["generated_at"].endswith("+08:00"))

        detail = self.client.get(f"/trading-playbook/plans/{plan['id']}")
        self.assertEqual(detail.status_code, 200, detail.text)
        revised = self.client.put(
            f"/trading-playbook/plans/{plan['id']}",
            json={
                "change_note": "调整观察条件",
                "candidate_overrides": [
                    {
                        "candidate_id": plan["candidates"][0]["id"],
                        "entry_trigger": {"label": "回封再看"},
                    }
                ],
            },
        )
        self.assertEqual(revised.status_code, 200, revised.text)
        revised_payload = revised.json()
        self.assertEqual(revised_payload["parent_plan_version_id"], plan["id"])
        self.assertEqual(revised_payload["candidates"][0]["entry_trigger_json"]["label"], "回封再看")
        unchanged = self.client.get(f"/trading-playbook/plans/{plan['id']}").json()
        self.assertNotIn("label", unchanged["candidates"][0]["entry_trigger_json"])

        confirmed = self.client.post(
            f"/trading-playbook/plans/{revised_payload['id']}/confirm",
            json={"confirmed_by": "  local-user  "},
        )
        self.assertEqual(confirmed.status_code, 200, confirmed.text)
        self.assertEqual(confirmed.json()["status"], "active")
        self.assertEqual(confirmed.json()["confirmed_by"], "local-user")
        self.assertTrue(confirmed.json()["confirmed_at"].endswith("+08:00"))

    def test_confirm_response_does_not_serialize_after_commit(self):
        original = trading_playbook_api._plan_service.serialize
        calls = 0

        async def fail_after_preflight(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls > 1:
                raise RuntimeError("post-commit confirm serialization secret")
            return await original(*args, **kwargs)

        with patch.object(
            trading_playbook_api._plan_service,
            "serialize",
            new=fail_after_preflight,
        ):
            response = self.client.post(
                "/trading-playbook/plans/1/confirm",
                json={"confirmed_by": "local-user"},
            )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["status"], "active")
        self.assertEqual(calls, 1)

    def test_cancel_response_does_not_serialize_after_commit(self):
        original = trading_playbook_api._plan_service.serialize
        calls = 0

        async def fail_after_preflight(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls > 1:
                raise RuntimeError("post-commit cancel serialization secret")
            return await original(*args, **kwargs)

        with patch.object(
            trading_playbook_api._plan_service,
            "serialize",
            new=fail_after_preflight,
        ):
            response = self.client.post("/trading-playbook/plans/1/cancel")
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["status"], "expired")
        self.assertEqual(calls, 1)

    def test_revision_response_does_not_serialize_after_commit(self):
        original = trading_playbook_api._plan_service.serialize
        calls = 0

        async def fail_after_preflight(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls > 1:
                raise RuntimeError("post-commit revision serialization secret")
            return await original(*args, **kwargs)

        with patch.object(
            trading_playbook_api._plan_service,
            "serialize",
            new=fail_after_preflight,
        ):
            response = self.client.put(
                "/trading-playbook/plans/1",
                json={"change_note": "idempotent revision"},
            )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["parent_plan_version_id"], 1)
        self.assertEqual(calls, 1)

    def test_same_revision_request_reuses_existing_child(self):
        request = {"change_note": "same normalized revision"}
        first = self.client.put("/trading-playbook/plans/1", json=request)
        second = self.client.put("/trading-playbook/plans/1", json=request)
        self.assertEqual(first.status_code, 200, first.text)
        self.assertEqual(second.status_code, 200, second.text)
        self.assertEqual(first.json()["id"], second.json()["id"])

        async def count_children():
            async with self.Session() as db:
                return await db.scalar(
                    text(
                        "SELECT count(*) FROM trading_plan_versions "
                        "WHERE parent_plan_version_id=1"
                    )
                )

        self.assertEqual(asyncio.run(count_children()), 1)

    def test_missing_plan_and_invalid_transitions_have_distinct_statuses(self):
        self.assertEqual(
            self.client.get("/trading-playbook/plans/99999").status_code, 404
        )
        self.assertEqual(
            self.client.put(
                "/trading-playbook/plans/99999", json={"change_note": "x"}
            ).status_code,
            404,
        )
        self.assertEqual(
            self.client.post(
                "/trading-playbook/plans/99999/confirm",
                json={"confirmed_by": "user"},
            ).status_code,
            404,
        )

        plan_id = self.client.get(
            "/trading-playbook/plans", params={"trade_date": "2026-07-13"}
        ).json()["items"][0]["id"]
        self.assertEqual(
            self.client.post(f"/trading-playbook/plans/{plan_id}/cancel").status_code,
            200,
        )
        self.assertEqual(
            self.client.post(f"/trading-playbook/plans/{plan_id}/cancel").status_code,
            409,
        )
        self.assertEqual(
            self.client.post(
                f"/trading-playbook/plans/{plan_id}/confirm",
                json={"confirmed_by": "user"},
            ).status_code,
            409,
        )

    def test_cancel_upstream_failure_is_fixed_503_without_internal_detail(self):
        async def fail(*_args, **_kwargs):
            raise UpstreamUnavailableError("cancel provider secret")

        with patch.object(
            trading_playbook_api._plan_service,
            "cancel",
            new=fail,
        ), TestClient(self.app, raise_server_exceptions=False) as client:
            response = client.post("/trading-playbook/plans/1/cancel")
        self.assertEqual(response.status_code, 503, response.text)
        self.assertEqual(
            response.json()["detail"],
            "Trading playbook service is unavailable",
        )
        self.assertNotIn("secret", response.text)

    def test_generate_uses_injected_orchestrator_and_aware_clock(self):
        payload = {"source_trade_date": "2026-07-10", "stage": "after_close"}
        first = self.client.post("/trading-playbook/plans/generate", json=payload)
        second = self.client.post("/trading-playbook/plans/generate", json=payload)
        self.assertEqual(first.status_code, 200, first.text)
        self.assertEqual(first.json()["id"], second.json()["id"])
        _, source_date, stage, as_of = self.orchestrator.calls[0]
        self.assertEqual(source_date, date(2026, 7, 10))
        self.assertEqual(stage, "after_close")
        self.assertIsNotNone(as_of.utcoffset())
        self.assertEqual(as_of.utcoffset().total_seconds(), 8 * 3600)

    def test_generate_serializes_an_orm_result_without_leaking_it(self):
        async def return_plan(db, *_args, **_kwargs):
            return await db.get(TradingPlanVersion, 1)

        self.orchestrator.build_stage = return_plan
        response = self.client.post(
            "/trading-playbook/plans/generate",
            json={"source_trade_date": "2026-07-10", "stage": "after_close"},
        )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["id"], 1)
        self.assertEqual(len(response.json()["candidates"]), 1)
        self.assertNotIn("_sa_instance_state", response.json())

    def test_plan_responses_normalize_nonfinite_audit_history(self):
        async def corrupt_audit_history():
            async with self.Session() as db:
                plan = await db.get(TradingPlanVersion, 1)
                plan.market_state_json = {
                    "breadth_score": float("nan"),
                    "extremes": [float("inf"), float("-inf")],
                }
                plan.mode_radar_json = [
                    {"mode_key": "a_mode", "score": float("nan")}
                ]
                candidate = await db.get(TradingPlanCandidate, 1)
                candidate.recognition_json = {"audit_score": float("inf")}
                await db.commit()

        asyncio.run(corrupt_audit_history())
        response = self.client.get("/trading-playbook/plans/1")
        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        self.assertEqual(payload["market_state_json"]["breadth_score"], "NaN")
        self.assertEqual(
            payload["market_state_json"]["extremes"],
            ["Infinity", "-Infinity"],
        )
        self.assertEqual(payload["mode_radar_json"][0]["score"], "NaN")
        self.assertEqual(
            payload["candidates"][0]["recognition_json"]["audit_score"],
            "Infinity",
        )

    def test_every_plan_endpoint_rejects_nonfinite_strong_history_with_fixed_503(self):
        async def add_bad_plan(index: int):
            target = date(2026, 7, 20 + index)
            async with self.Session() as db:
                plan = TradingPlanVersion(
                    source_trade_date=date(2026, 7, 10),
                    target_trade_date=target,
                    stage="after_close",
                    version_no=1,
                    status="draft",
                    risk_settings_json={
                        "trial": 10.0,
                        "confirmed": 30.0,
                        "hard_stop": 5.0,
                        "max_candidates": 3,
                    },
                    input_hash=f"bad-plan-{index}",
                    generated_at=datetime(2026, 7, 10, 15, 30),
                )
                db.add(plan)
                await db.flush()
                db.add(
                    TradingPlanCandidate(
                        plan_version_id=plan.id,
                        stock_code=f"00{index:04d}",
                        stock_name=f"bad-{index}",
                        action_trade_date=target,
                        theme_name="history",
                        primary_mode_key="a_mode",
                        role="leader",
                        rank=1,
                        entry_trigger_json={"reference_price": 10.0},
                        invalidation_json={"price_lte": 9.5},
                        exit_trigger_json={"label": "exit"},
                        risk_level="trial",
                        position_reference=float("inf"),
                        status="waiting",
                    )
                )
                await db.commit()
                return plan.id, target

        plans = [asyncio.run(add_bad_plan(index)) for index in range(1, 7)]
        list_id, list_date = plans[0]
        requests = [
            lambda: self.client.get(
                "/trading-playbook/plans",
                params={"trade_date": list_date.isoformat()},
            ),
            lambda: self.client.get(f"/trading-playbook/plans/{plans[1][0]}"),
            lambda: self.client.put(
                f"/trading-playbook/plans/{plans[3][0]}",
                json={"change_note": "must reject unsafe history"},
            ),
            lambda: self.client.post(
                f"/trading-playbook/plans/{plans[4][0]}/confirm",
                json={"confirmed_by": "local-user"},
            ),
            lambda: self.client.post(
                f"/trading-playbook/plans/{plans[5][0]}/cancel"
            ),
        ]

        async def return_bad_plan(db, *_args, **_kwargs):
            return await db.get(TradingPlanVersion, plans[2][0])

        self.orchestrator.build_stage = return_bad_plan
        requests.insert(
            2,
            lambda: self.client.post(
                "/trading-playbook/plans/generate",
                json={
                    "source_trade_date": "2026-07-10",
                    "stage": "after_close",
                },
            ),
        )
        for request in requests:
            with self.subTest(request=request):
                response = request()
                self.assertEqual(response.status_code, 503, response.text)
                self.assertEqual(
                    response.json()["detail"],
                    "Trading playbook service is unavailable",
                )
                self.assertNotIn("inf", response.text.lower())

        async def statuses_and_children():
            async with self.Session() as db:
                statuses = [
                    (await db.get(TradingPlanVersion, plan_id)).status
                    for plan_id, _target in plans
                ]
                count = await db.scalar(
                    text(
                        "SELECT count(*) FROM trading_plan_versions "
                        "WHERE parent_plan_version_id IS NOT NULL"
                    )
                )
                return statuses, count

        statuses, child_count = asyncio.run(statuses_and_children())
        self.assertEqual(statuses, ["draft"] * 6)
        self.assertEqual(child_count, 0)

    def test_plan_response_rejects_nonfinite_risk_setting(self):
        async def corrupt_risk_history():
            async with self.Session() as db:
                plan = await db.get(TradingPlanVersion, 1)
                plan.risk_settings_json = {"hard_stop": float("nan")}
                await db.commit()

        asyncio.run(corrupt_risk_history())
        response = self.client.get("/trading-playbook/plans/1")
        self.assertEqual(response.status_code, 503, response.text)
        self.assertEqual(
            response.json()["detail"],
            "Trading playbook service is unavailable",
        )

    def test_plan_detail_rejects_out_of_domain_strong_history(self):
        cases = [
            ("missing-risk-fields", {"risk": {"hard_stop": 5.0}}),
            (
                "position-above-limit",
                {"candidate": {"position_reference": 101.0}},
            ),
            (
                "position-below-limit",
                {"candidate": {"position_reference": -1.0}},
            ),
            (
                "formal-risk-level-is-not-actionable",
                {"candidate": {"risk_level": "watch"}},
            ),
            (
                "position-does-not-match-risk-level",
                {"candidate": {"position_reference": 100.0}},
            ),
            (
                "missing-materialized-hard-stop",
                {"candidate": {"invalidation_json": {"label": "missing"}}},
            ),
            (
                "mismatched-materialized-hard-stop",
                {"candidate": {"invalidation_json": {"price_lte": 9.4}}},
            ),
            (
                "blank-stock-name",
                {"candidate": {"stock_name": "   "}},
            ),
            (
                "malformed-stock-code",
                {"candidate": {"stock_code": "1"}},
            ),
            (
                "blank-primary-mode",
                {"candidate": {"primary_mode_key": "   "}},
            ),
            (
                "trial-below-limit",
                {
                    "risk": {
                        "trial": -1.0,
                        "confirmed": 30.0,
                        "hard_stop": 5.0,
                        "max_candidates": 3,
                    }
                },
            ),
            (
                "trial-above-confirmed",
                {
                    "risk": {
                        "trial": 40.0,
                        "confirmed": 30.0,
                        "hard_stop": 5.0,
                        "max_candidates": 3,
                    }
                },
            ),
            (
                "hard-stop-above-limit",
                {
                    "risk": {
                        "trial": 10.0,
                        "confirmed": 30.0,
                        "hard_stop": 21.0,
                        "max_candidates": 3,
                    }
                },
            ),
            (
                "hard-stop-zero",
                {
                    "risk": {
                        "trial": 10.0,
                        "confirmed": 30.0,
                        "hard_stop": 0.0,
                        "max_candidates": 3,
                    }
                },
            ),
            (
                "max-candidates-above-limit",
                {
                    "risk": {
                        "trial": 10.0,
                        "confirmed": 30.0,
                        "hard_stop": 5.0,
                        "max_candidates": 4,
                    }
                },
            ),
            (
                "max-candidates-zero",
                {
                    "risk": {
                        "trial": 10.0,
                        "confirmed": 30.0,
                        "hard_stop": 5.0,
                        "max_candidates": 0,
                    }
                },
            ),
            ("candidate-count-above-max", {"max_candidates": 1, "extra": True}),
            ("duplicate-stock", {"duplicate": True}),
            (
                "nonpositive-reference-price",
                {"candidate": {"entry_trigger_json": {"reference_price": 0}}},
            ),
            (
                "missing-reference-price",
                {"candidate": {"entry_trigger_json": {"sealed": True}}},
            ),
            (
                "nonpositive-price-threshold",
                {"candidate": {"invalidation_json": {"price_lte": -1}}},
            ),
            (
                "percentage-out-of-range",
                {"candidate": {"exit_trigger_json": {"change_pct_lte": -101}}},
            ),
            ("action-date-outside-plan", {"outside_action_date": True}),
            ("source-after-target", {"source_after_target": True}),
        ]

        async def add_bad_plan(index: int, case: dict):
            target = date(2026, 8, 1) + timedelta(days=index)
            risk = case.get(
                "risk",
                {
                    "trial": 10.0,
                    "confirmed": 30.0,
                    "hard_stop": 5.0,
                    "max_candidates": case.get("max_candidates", 3),
                },
            )
            async with self.Session() as db:
                plan = TradingPlanVersion(
                    source_trade_date=(
                        target + timedelta(days=1)
                        if case.get("source_after_target")
                        else target - timedelta(days=1)
                    ),
                    target_trade_date=target,
                    stage="after_close",
                    version_no=1,
                    status="draft",
                    risk_settings_json=risk,
                    input_hash=f"domain-invalid-{index}",
                    generated_at=datetime(2026, 7, 10, 15, 30),
                )
                db.add(plan)
                await db.flush()
                candidate_values = {
                    "plan_version_id": plan.id,
                    "stock_code": f"10{index:04d}",
                    "stock_name": f"bad-domain-{index}",
                    "action_trade_date": (
                        target + timedelta(days=2)
                        if case.get("outside_action_date")
                        else target
                    ),
                    "theme_name": "history",
                    "primary_mode_key": "a_mode",
                    "role": "leader",
                    "rank": 1,
                    "entry_trigger_json": {"reference_price": 10.0},
                    "invalidation_json": {"price_lte": 9.5},
                    "exit_trigger_json": {"change_pct_lte": -5.0},
                    "risk_level": "trial",
                    "position_reference": 10.0,
                    "status": "waiting",
                }
                candidate_values.update(case.get("candidate", {}))
                db.add(TradingPlanCandidate(**candidate_values))
                if case.get("extra") or case.get("duplicate"):
                    db.add(
                        TradingPlanCandidate(
                            **{
                                **candidate_values,
                                "stock_code": (
                                    candidate_values["stock_code"]
                                    if case.get("duplicate")
                                    else f"20{index:04d}"
                                ),
                                "stock_name": f"bad-domain-extra-{index}",
                                "primary_mode_key": "z_mode",
                                "rank": 2,
                            }
                        )
                    )
                await db.commit()
                return plan.id

        for index, (name, case) in enumerate(cases):
            plan_id = asyncio.run(add_bad_plan(index, case))
            with self.subTest(case=name):
                response = self.client.get(f"/trading-playbook/plans/{plan_id}")
                self.assertEqual(response.status_code, 503, response.text)
                self.assertEqual(
                    response.json()["detail"],
                    "Trading playbook service is unavailable",
                )

    def test_confirm_and_revise_reject_inconsistent_formal_candidates_without_writes(self):
        cases = [
            ("position", {"position_reference": 100.0}),
            ("missing-stop", {"invalidation_json": {"label": "missing"}}),
            ("wrong-stop", {"invalidation_json": {"price_lte": 9.4}}),
            ("blank-identity", {"stock_name": "   "}),
        ]

        async def add_bad_plan(index: int, candidate_changes: dict):
            target = date(2026, 9, 1) + timedelta(days=index)
            async with self.Session() as db:
                plan = TradingPlanVersion(
                    source_trade_date=target - timedelta(days=1),
                    target_trade_date=target,
                    stage="after_close",
                    version_no=1,
                    status="draft",
                    risk_settings_json={
                        "trial": 10.0,
                        "confirmed": 30.0,
                        "hard_stop": 5.0,
                        "max_candidates": 3,
                    },
                    input_hash=f"formal-risk-invalid-{index}",
                    generated_at=datetime(2026, 7, 10, 15, 30),
                )
                db.add(plan)
                await db.flush()
                values = {
                    "plan_version_id": plan.id,
                    "stock_code": f"30{index:04d}",
                    "stock_name": f"bad-formal-{index}",
                    "action_trade_date": target,
                    "theme_name": "history",
                    "primary_mode_key": "a_mode",
                    "role": "leader",
                    "rank": 1,
                    "entry_trigger_json": {"reference_price": 10.0},
                    "invalidation_json": {"price_lte": 9.5},
                    "exit_trigger_json": {"change_pct_lte": -5.0},
                    "risk_level": "trial",
                    "position_reference": 10.0,
                    "status": "waiting",
                }
                values.update(candidate_changes)
                db.add(TradingPlanCandidate(**values))
                await db.commit()
                return plan.id

        async def state(plan_id: int):
            async with self.Session() as db:
                plan = await db.get(TradingPlanVersion, plan_id)
                children = await db.scalar(
                    text(
                        "SELECT count(*) FROM trading_plan_versions "
                        "WHERE parent_plan_version_id=:parent_id"
                    ),
                    {"parent_id": plan_id},
                )
                return plan.status, children

        for index, (name, candidate_changes) in enumerate(cases, start=1):
            plan_id = asyncio.run(add_bad_plan(index, candidate_changes))
            with self.subTest(case=name, operation="confirm"):
                confirmed = self.client.post(
                    f"/trading-playbook/plans/{plan_id}/confirm",
                    json={"confirmed_by": "local-user"},
                )
                self.assertEqual(confirmed.status_code, 503, confirmed.text)
            with self.subTest(case=name, operation="revise"):
                revised = self.client.put(
                    f"/trading-playbook/plans/{plan_id}",
                    json={"change_note": "must reject inconsistent history"},
                )
                self.assertEqual(revised.status_code, 503, revised.text)
            self.assertEqual(asyncio.run(state(plan_id)), ("draft", 0))

    def test_generate_and_review_are_503_until_production_dependencies_exist(self):
        app = FastAPI()
        app.include_router(router, prefix="/trading-playbook")

        async def override_db():
            async with self.Session() as db:
                yield db

        app.dependency_overrides[get_db] = override_db
        app.dependency_overrides[get_trading_playbook_now] = lambda: FIXED_NOW
        with TestClient(app) as client:
            generated = client.post(
                "/trading-playbook/plans/generate",
                json={
                    "source_trade_date": "2026-07-10",
                    "stage": "after_close",
                },
            )
            reviewed = client.put(
                "/trading-playbook/reviews/2026-07-10",
                json={"executions": {}},
            )
        self.assertEqual(generated.status_code, 503)
        self.assertEqual(reviewed.status_code, 503)
        self.assertEqual(
            generated.json()["detail"],
            "Trading playbook service is unavailable",
        )
        self.assertEqual(
            reviewed.json()["detail"],
            "Trading playbook service is unavailable",
        )

    def test_all_endpoints_map_unexpected_failures_to_fixed_503(self):
        async def fail(*_args, **_kwargs):
            raise RuntimeError("unexpected provider secret")

        cases = [
            (
                "rules",
                "get",
                "/trading-playbook/rules",
                None,
                lambda: patch.object(AsyncSession, "scalars", new=fail),
            ),
            (
                "generate",
                "post",
                "/trading-playbook/plans/generate",
                {"source_trade_date": "2026-07-10", "stage": "after_close"},
                lambda: patch.object(
                    self.orchestrator,
                    "build_stage",
                    new=fail,
                ),
            ),
            (
                "plans",
                "get",
                "/trading-playbook/plans?trade_date=2026-07-13",
                None,
                lambda: patch.object(AsyncSession, "scalars", new=fail),
            ),
            (
                "plan-detail",
                "get",
                "/trading-playbook/plans/1",
                None,
                lambda: patch.object(
                    trading_playbook_api._plan_service,
                    "serialize",
                    new=fail,
                ),
            ),
            (
                "revise",
                "put",
                "/trading-playbook/plans/1",
                {"change_note": "failure contract"},
                lambda: patch.object(
                    trading_playbook_api._plan_service,
                    "serialize",
                    new=fail,
                ),
            ),
            (
                "confirm",
                "post",
                "/trading-playbook/plans/1/confirm",
                {"confirmed_by": "contract-test"},
                lambda: patch.object(
                    trading_playbook_api._plan_service,
                    "serialize",
                    new=fail,
                ),
            ),
            (
                "cancel",
                "post",
                "/trading-playbook/plans/1/cancel",
                None,
                lambda: patch.object(
                    trading_playbook_api._plan_service,
                    "serialize",
                    new=fail,
                ),
            ),
            (
                "alerts",
                "get",
                "/trading-playbook/alerts",
                None,
                lambda: patch.object(AsyncSession, "scalars", new=fail),
            ),
            (
                "alert-ack",
                "post",
                "/trading-playbook/alerts/1/ack",
                None,
                lambda: patch.object(AsyncSession, "get", new=fail),
            ),
            (
                "review",
                "put",
                "/trading-playbook/reviews/2026-07-10",
                {"executions": {}},
                lambda: patch.object(
                    self.review_service,
                    "update_manual_execution",
                    new=fail,
                ),
            ),
            (
                "settings-get",
                "get",
                "/trading-playbook/settings",
                None,
                lambda: patch.object(
                    trading_playbook_api,
                    "_settings_row",
                    new=fail,
                ),
            ),
            (
                "settings-put",
                "put",
                "/trading-playbook/settings",
                {"enabled": False},
                lambda: patch.object(
                    trading_playbook_api._plan_service,
                    "update_settings",
                    new=fail,
                ),
            ),
        ]

        with TestClient(self.app, raise_server_exceptions=False) as client:
            for name, method, path, body, patcher in cases:
                with self.subTest(endpoint=name), patcher():
                    response = client.request(method, path, json=body)
                    self.assertEqual(response.status_code, 503, response.text)
                    self.assertEqual(
                        response.json()["detail"],
                        "Trading playbook service is unavailable",
                    )
                    self.assertNotIn("secret", response.text)

    def test_real_application_resolves_registered_shared_runtime(self):
        from app.main import app as production_app

        trading_playbook_runtime.install_orchestrator(self.orchestrator)
        client = TestClient(production_app)
        try:
            response = client.post(
                "/api/v1/trading-playbook/plans/generate",
                json={
                    "source_trade_date": date.today().isoformat(),
                    "stage": "after_close",
                },
            )
        finally:
            client.close()
            trading_playbook_runtime.reset()
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["id"], 901)

    def test_generate_maps_window_error_to_422_and_source_failure_to_503(self):
        async def bad_window(*_args, **_kwargs):
            raise InvalidRequestError("outside stage window secret")

        self.orchestrator.build_stage = bad_window
        response = self.client.post(
            "/trading-playbook/plans/generate",
            json={"source_trade_date": "2026-07-10", "stage": "after_close"},
        )
        self.assertEqual(response.status_code, 422)
        self.assertEqual(
            response.json()["detail"],
            "Invalid trading playbook request",
        )

        async def invalid_snapshot(*_args, **_kwargs):
            raise ValueError("candidate 000001 missing 19 internal rules")

        self.orchestrator.build_stage = invalid_snapshot
        response = self.client.post(
            "/trading-playbook/plans/generate",
            json={"source_trade_date": "2026-07-10", "stage": "after_close"},
        )
        self.assertEqual(response.status_code, 503)
        self.assertEqual(
            response.json()["detail"],
            "Trading playbook service is unavailable",
        )
        self.assertNotIn("000001", response.text)

        async def failed_source(*_args, **_kwargs):
            raise ConnectionError("market feed unavailable")

        self.orchestrator.build_stage = failed_source
        response = self.client.post(
            "/trading-playbook/plans/generate",
            json={"source_trade_date": "2026-07-10", "stage": "after_close"},
        )
        self.assertEqual(response.status_code, 503)

        async def failed_runtime_source(*_args, **_kwargs):
            raise RuntimeError("market provider failed")

        self.orchestrator.build_stage = failed_runtime_source
        response = self.client.post(
            "/trading-playbook/plans/generate",
            json={"source_trade_date": "2026-07-10", "stage": "after_close"},
        )
        self.assertEqual(response.status_code, 503)

    def test_generate_trusts_only_explicit_validated_plan_payload_marker(self):
        marker_type = getattr(
            serialization_module,
            "ValidatedPlanPayload",
            None,
        )
        self.assertIsNotNone(marker_type)

        async def validated_result(db, source_trade_date, stage, as_of):
            raw = await _FakeOrchestrator().build_stage(
                db,
                source_trade_date,
                stage,
                as_of,
            )
            return marker_type(raw)

        self.orchestrator.build_stage = validated_result
        with patch.object(
            trading_playbook_api,
            "_normalize_plan_payload",
            side_effect=RuntimeError("must not normalize a trusted payload twice"),
        ):
            response = self.client.post(
                "/trading-playbook/plans/generate",
                json={
                    "source_trade_date": "2026-07-10",
                    "stage": "after_close",
                },
            )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["id"], 901)

    def test_generate_still_validates_arbitrary_mapping_results(self):
        async def unsafe_mapping(db, source_trade_date, stage, as_of):
            payload = await _FakeOrchestrator().build_stage(
                db,
                source_trade_date,
                stage,
                as_of,
            )
            payload["candidates"][0]["position_reference"] = 100.0
            return payload

        self.orchestrator.build_stage = unsafe_mapping
        response = self.client.post(
            "/trading-playbook/plans/generate",
            json={"source_trade_date": "2026-07-10", "stage": "after_close"},
        )
        self.assertEqual(response.status_code, 503, response.text)
        self.assertEqual(
            response.json()["detail"],
            "Trading playbook service is unavailable",
        )

    def test_request_schemas_reject_extra_fields_bad_locators_and_nonfinite_numbers(self):
        invalid_requests = [
            (
                "/trading-playbook/plans/generate",
                {"source_trade_date": "2026-07-10", "stage": "after_close", "extra": 1},
            ),
            (
                "/trading-playbook/plans/1/confirm",
                {"confirmed_by": "user", "extra": 1},
            ),
            (
                "/trading-playbook/plans/1",
                {
                    "change_note": "x",
                    "candidate_overrides": [
                        {
                            "candidate_id": 1,
                            "stock_code": "000001",
                            "primary_mode_key": "a_mode",
                            "manual_note": "x",
                        }
                    ],
                },
            ),
        ]
        for path, payload in invalid_requests:
            method = (
                self.client.post
                if path.endswith("generate") or path.endswith("confirm")
                else self.client.put
            )
            with self.subTest(path=path):
                self.assertEqual(method(path, json=payload).status_code, 422)

        response = self.client.put(
            "/trading-playbook/settings",
            content='{"hard_stop_pct": NaN}',
            headers={"content-type": "application/json"},
        )
        self.assertEqual(response.status_code, 422)

    def test_revision_distinguishes_invalid_request_from_state_conflict(self):
        plan = self.client.get(
            "/trading-playbook/plans", params={"trade_date": "2026-07-13"}
        ).json()["items"][0]
        invalid = self.client.put(
            f"/trading-playbook/plans/{plan['id']}",
            json={
                "change_note": "尝试覆盖刚性止损",
                "candidate_overrides": [
                    {
                        "candidate_id": plan["candidates"][0]["id"],
                        "invalidation": {"price_lte": 9.0},
                    }
                ],
            },
        )
        self.assertEqual(invalid.status_code, 422, invalid.text)
        self.assertEqual(
            invalid.json()["detail"],
            "Invalid trading playbook request",
        )

        contradictory = self.client.put(
            f"/trading-playbook/plans/{plan['id']}",
            json={
                "change_note": "矛盾的价格区间",
                "candidate_overrides": [
                    {
                        "candidate_id": plan["candidates"][0]["id"],
                        "entry_trigger": {
                            "price_gte": 12.0,
                            "price_lte": 11.0,
                        },
                    }
                ],
            },
        )
        self.assertEqual(contradictory.status_code, 422, contradictory.text)
        self.assertEqual(
            contradictory.json()["detail"],
            "Invalid trading playbook request",
        )

        cancelled = self.client.post(
            f"/trading-playbook/plans/{plan['id']}/cancel"
        )
        self.assertEqual(cancelled.status_code, 200, cancelled.text)
        conflict = self.client.put(
            f"/trading-playbook/plans/{plan['id']}",
            json={"change_note": "过期后不可修改"},
        )
        self.assertEqual(conflict.status_code, 409, conflict.text)
        self.assertEqual(
            conflict.json()["detail"],
            "Trading plan state conflict",
        )

    def test_alerts_have_stable_pagination_filter_and_idempotent_ack(self):
        response = self.client.get(
            "/trading-playbook/alerts", params={"limit": 1, "offset": 0}
        )
        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        self.assertEqual(payload["items"][0]["dedup_key"], "new")
        self.assertTrue(payload["items"][0]["triggered_at"].endswith("+08:00"))
        alert_id = payload["items"][0]["id"]

        first = self.client.post(f"/trading-playbook/alerts/{alert_id}/ack")
        second = self.client.post(f"/trading-playbook/alerts/{alert_id}/ack")
        self.assertEqual(first.status_code, 200, first.text)
        self.assertEqual(first.json()["acknowledged_at"], second.json()["acknowledged_at"])
        unread = self.client.get(
            "/trading-playbook/alerts", params={"unread_only": True}
        ).json()["items"]
        self.assertNotIn(alert_id, [item["id"] for item in unread])
        self.assertEqual(
            self.client.post("/trading-playbook/alerts/99999/ack").status_code, 404
        )
        self.assertEqual(
            self.client.get("/trading-playbook/alerts", params={"limit": 101}).status_code,
            422,
        )

    def test_settings_are_singleton_patch_only_and_wechat_is_disabled(self):
        initial = self.client.get("/trading-playbook/settings")
        self.assertEqual(initial.status_code, 200, initial.text)
        self.assertEqual(initial.json()["id"], 1)
        original_confirmed = initial.json()["confirmed_position_pct"]

        updated = self.client.put(
            "/trading-playbook/settings", json={"trial_position_pct": 8.5}
        )
        self.assertEqual(updated.status_code, 200, updated.text)
        self.assertEqual(updated.json()["trial_position_pct"], 8.5)
        self.assertEqual(updated.json()["confirmed_position_pct"], original_confirmed)
        self.assertNotIn("channel_config_json", updated.json())

        invalid_payloads = [
            {"wechat_enabled": True},
            {"trial_position_pct": 31, "confirmed_position_pct": 30},
            {"hard_stop_pct": 21},
            {"max_action_candidates": 4},
            {"channel_config_json": {"secret": "leak"}},
        ]
        for payload in invalid_payloads:
            with self.subTest(payload=payload):
                self.assertEqual(
                    self.client.put("/trading-playbook/settings", json=payload).status_code,
                    422,
                )

    def test_settings_get_persistently_disables_legacy_wechat_flag(self):
        async def set_legacy_flag():
            async with self.Session() as db:
                await db.execute(text("PRAGMA ignore_check_constraints=ON"))
                row = await db.get(TradingPlaybookSettings, 1)
                if row is None:
                    row = TradingPlaybookSettings(id=1, wechat_enabled=True)
                    db.add(row)
                else:
                    row.wechat_enabled = True
                await db.commit()
                await db.execute(text("PRAGMA ignore_check_constraints=OFF"))

        async def read_flag():
            async with self.Session() as db:
                row = await db.get(TradingPlaybookSettings, 1)
                return row.wechat_enabled

        asyncio.run(set_legacy_flag())
        response = self.client.get("/trading-playbook/settings")
        self.assertEqual(response.status_code, 200, response.text)
        self.assertFalse(response.json()["wechat_enabled"])
        self.assertFalse(asyncio.run(read_flag()))

    def test_settings_put_serialization_failure_rolls_back_update(self):
        async def seed_settings():
            async with self.Session() as db:
                db.add(
                    TradingPlaybookSettings(
                        id=1,
                        trial_position_pct=10.0,
                        confirmed_position_pct=30.0,
                        wechat_enabled=False,
                    )
                )
                await db.commit()

        async def read_positions():
            async with self.Session() as db:
                row = await db.get(TradingPlaybookSettings, 1)
                return row.trial_position_pct, row.confirmed_position_pct

        asyncio.run(seed_settings())
        with patch.object(
            plan_service_module,
            "normalize_settings_payload",
            side_effect=RuntimeError("settings serializer failed"),
            create=True,
        ):
            response = self.client.put(
                "/trading-playbook/settings",
                json={"trial_position_pct": 20.0},
            )
        self.assertEqual(response.status_code, 503, response.text)
        self.assertEqual(asyncio.run(read_positions()), (10.0, 30.0))

    def test_settings_get_creation_serialization_failure_does_not_commit(self):
        async def settings_count():
            async with self.Session() as db:
                return await db.scalar(
                    text("SELECT count(*) FROM trading_playbook_settings")
                )

        with patch.object(
            trading_playbook_api,
            "_serialize_settings",
            side_effect=RuntimeError("settings serializer failed"),
        ):
            response = self.client.get("/trading-playbook/settings")
        self.assertEqual(response.status_code, 503, response.text)
        self.assertEqual(asyncio.run(settings_count()), 0)

    def test_settings_get_repair_serialization_failure_does_not_commit(self):
        async def seed_legacy_settings():
            async with self.Session() as db:
                await db.execute(text("PRAGMA ignore_check_constraints=ON"))
                db.add(TradingPlaybookSettings(id=1, wechat_enabled=True))
                await db.commit()
                await db.execute(text("PRAGMA ignore_check_constraints=OFF"))

        async def read_flag():
            async with self.Session() as db:
                row = await db.get(TradingPlaybookSettings, 1)
                return row.wechat_enabled

        asyncio.run(seed_legacy_settings())
        with patch.object(
            plan_service_module,
            "normalize_settings_payload",
            side_effect=RuntimeError("settings serializer failed"),
            create=True,
        ):
            response = self.client.get("/trading-playbook/settings")
        self.assertEqual(response.status_code, 503, response.text)
        self.assertTrue(asyncio.run(read_flag()))

    def test_settings_response_snapshot_matches_committed_database_fields(self):
        response = self.client.put(
            "/trading-playbook/settings",
            json={
                "trial_position_pct": 20.0,
                "confirmed_position_pct": 30.0,
                "hard_stop_pct": 6.0,
                "max_action_candidates": 2,
            },
        )
        self.assertEqual(response.status_code, 200, response.text)

        async def read_settings():
            async with self.Session() as db:
                row = await db.get(TradingPlaybookSettings, 1)
                return {
                    "id": row.id,
                    "enabled": bool(row.enabled),
                    "trial_position_pct": row.trial_position_pct,
                    "confirmed_position_pct": row.confirmed_position_pct,
                    "hard_stop_pct": row.hard_stop_pct,
                    "max_action_candidates": row.max_action_candidates,
                    "in_app_enabled": bool(row.in_app_enabled),
                    "wechat_enabled": bool(row.wechat_enabled),
                    "updated_at": row.updated_at.replace(tzinfo=CN).isoformat(),
                }

        self.assertEqual(response.json(), asyncio.run(read_settings()))

    def test_settings_upstream_failure_is_fixed_503_without_internal_detail(self):
        async def fail(*_args, **_kwargs):
            raise UpstreamUnavailableError("settings provider secret")

        with patch.object(
            trading_playbook_api._plan_service,
            "update_settings",
            new=fail,
        ), TestClient(self.app, raise_server_exceptions=False) as client:
            response = client.put(
                "/trading-playbook/settings",
                json={"enabled": True},
            )
        self.assertEqual(response.status_code, 503, response.text)
        self.assertEqual(
            response.json()["detail"],
            "Trading playbook service is unavailable",
        )
        self.assertNotIn("secret", response.text)

    def test_review_payload_is_strict_and_forwarded_without_business_logic(self):
        response = self.client.put(
            "/trading-playbook/reviews/2026-07-10",
            json={
                "executions": {
                    "1": {
                        "executed": False,
                        "executed_at": "2026-07-10T09:31:00+08:00",
                        "manual_note": "计划内未执行",
                    }
                }
            },
        )
        self.assertEqual(response.status_code, 200, response.text)
        _, trade_date, executions = self.review_service.calls[0]
        self.assertEqual(trade_date, date(2026, 7, 10))
        self.assertEqual(
            executions,
            {
                "1": {
                    "executed": False,
                    "executed_at": datetime(2026, 7, 10, 9, 31, tzinfo=CN),
                    "manual_note": "计划内未执行",
                }
            },
        )
        self.assertTrue(response.json()["generated_at"].endswith("+08:00"))

        invalid = self.client.put(
            "/trading-playbook/reviews/2026-07-10",
            json={"executions": {"1": {"executed": False, "unknown": 1}}},
        )
        self.assertEqual(invalid.status_code, 422)

    def test_review_invalid_request_is_fixed_422_without_internal_detail(self):
        async def fail(*_args, **_kwargs):
            raise InvalidRequestError("execution 7 contains secret internals")

        self.review_service.update_manual_execution = fail
        response = self.client.put(
            "/trading-playbook/reviews/2026-07-10",
            json={"executions": {}},
        )
        self.assertEqual(response.status_code, 422, response.text)
        self.assertEqual(
            response.json()["detail"],
            "Invalid trading playbook request",
        )
        self.assertNotIn("secret", response.text)

    def test_review_missing_resource_is_fixed_404_without_internal_detail(self):
        async def fail(*_args, **_kwargs):
            raise PlaybookNotFoundError("plan 991 secret lookup")

        self.review_service.update_manual_execution = fail
        response = self.client.put(
            "/trading-playbook/reviews/2026-07-10",
            json={"executions": {}},
        )
        self.assertEqual(response.status_code, 404, response.text)
        self.assertEqual(response.json()["detail"], "Trading plan not found")
        self.assertNotIn("secret", response.text)

    def test_review_invalid_transition_is_fixed_409_without_internal_detail(self):
        async def fail(*_args, **_kwargs):
            raise InvalidTransitionError("active version 42 secret state")

        self.review_service.update_manual_execution = fail
        response = self.client.put(
            "/trading-playbook/reviews/2026-07-10",
            json={"executions": {}},
        )
        self.assertEqual(response.status_code, 409, response.text)
        self.assertEqual(
            response.json()["detail"],
            "Trading plan state conflict",
        )
        self.assertNotIn("secret", response.text)

    def test_review_upstream_failure_is_fixed_503_without_internal_detail(self):
        async def fail(*_args, **_kwargs):
            raise UpstreamUnavailableError("provider secret endpoint")

        self.review_service.update_manual_execution = fail
        with TestClient(self.app, raise_server_exceptions=False) as client:
            response = client.put(
                "/trading-playbook/reviews/2026-07-10",
                json={"executions": {}},
            )
        self.assertEqual(response.status_code, 503, response.text)
        self.assertEqual(
            response.json()["detail"],
            "Trading playbook service is unavailable",
        )
        self.assertNotIn("secret", response.text)

    def test_review_unclassified_value_error_is_safe_503(self):
        async def fail(*_args, **_kwargs):
            raise ValueError("unexpected secret implementation invariant")

        self.review_service.update_manual_execution = fail
        response = self.client.put(
            "/trading-playbook/reviews/2026-07-10",
            json={"executions": {}},
        )
        self.assertEqual(response.status_code, 503, response.text)
        self.assertEqual(
            response.json()["detail"],
            "Trading playbook service is unavailable",
        )
        self.assertNotIn("secret", response.text)

    def test_router_is_mounted_once_under_api_v1_prefix(self):
        from app.api.v1 import api_router

        operations = [
            (route.path, method)
            for route in api_router.routes
            if route.path.startswith("/trading-playbook")
            for method in route.methods
        ]
        self.assertEqual(len(operations), 12)
        self.assertEqual(len(set(operations)), 12)


if __name__ == "__main__":
    unittest.main()
