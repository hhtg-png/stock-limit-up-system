"""Contract tests for one complete trading-playbook stage pipeline."""

from __future__ import annotations

import copy
import hashlib
import unittest
from dataclasses import replace
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock
from zoneinfo import ZoneInfo

import app.models  # noqa: F401
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.database import Base
from app.models.stock import Stock
from app.models.trading_playbook import TradingPlanVersion
from app.services.trading_playbook.domain import (
    CandidateSnapshot,
    DataQuality,
    MarketSnapshot,
    ModeEvaluation,
)
from app.services.trading_playbook.orchestrator import (
    TradingPlaybookOrchestrator,
    build_default_orchestrator,
)
from app.services.trading_playbook.market_data import (
    TradingPlaybookMarketDataProvider,
)
from app.services.trading_playbook.market_state import MarketStateAnalyzer
from app.services.trading_playbook.mode_features import ModeFeatureBuilder
from app.services.trading_playbook.mode_matcher import ModeMatcher
from app.services.trading_playbook.plan_service import TradingPlanService
from app.services.trading_playbook.rule_catalog import RuleCatalog


CN_TZ = ZoneInfo("Asia/Shanghai")
SOURCE_DATE = date(2026, 7, 10)
TARGET_DATE = date(2026, 7, 13)
AS_OF = datetime(2026, 7, 10, 14, 40, tzinfo=CN_TZ)


def _candidate(code: str, *, marker: str = "raw") -> CandidateSnapshot:
    return CandidateSnapshot(
        stock_code=code,
        stock_name=f"股票{code}",
        theme_name="AI",
        features={"marker": marker},
        evidence=[{"source": "fixture", "as_of": AS_OF, "quality": "ready"}],
    )


def _snapshot(
    *,
    source: date = SOURCE_DATE,
    target: date = TARGET_DATE,
    stage: str = "preclose",
    as_of: datetime = AS_OF,
    candidates: list[CandidateSnapshot] | None = None,
) -> MarketSnapshot:
    return MarketSnapshot(
        source_trade_date=source,
        target_trade_date=target,
        stage=stage,
        as_of=as_of,
        market_features={"limit_up_count": 20},
        candidates=list(candidates or []),
        theme_rankings=[],
        quality=DataQuality("ready", as_of, "fixture"),
    )


def _rule_rows() -> list[dict]:
    return [
        {
            "mode_key": f"mode_{index:02d}",
            "version": 1,
            "content_hash": hashlib.sha256(
                f"mode_{index:02d}".encode("utf-8")
            ).hexdigest(),
        }
        for index in range(19)
    ]


def _evaluation(code: str, key: str = "mode_00") -> ModeEvaluation:
    return ModeEvaluation(
        mode_key=key,
        stock_code=code,
        status="matched",
        score=90.0,
        role="leader",
        risk_level="trial",
        entry_trigger={"label": "观察"},
        invalidation={"label": "失效"},
        exit_trigger={"label": "退出"},
        evidence=[{"source": "fixture", "as_of": AS_OF, "quality": "ready"}],
        rule_version=1,
        rule_hash=hashlib.sha256(key.encode("utf-8")).hexdigest(),
    )


class _EchoMarketData:
    def __init__(self, *, candidates=None):
        self.calls = []
        self.candidates = candidates or []

    async def build_market_snapshot(self, **kwargs):
        self.calls.append(kwargs)
        return _snapshot(
            source=kwargs["source_trade_date"],
            target=kwargs["target_trade_date"],
            stage=kwargs["stage"],
            as_of=kwargs["as_of"],
            candidates=copy.deepcopy(self.candidates),
        )


class _CopyAnalyzer:
    def __init__(self):
        self.calls = []

    def enrich_snapshot(self, snapshot):
        self.calls.append(snapshot)
        result = copy.deepcopy(snapshot)
        result.market_features["style"] = "dual_active"
        result.market_features["window"] = "outbreak"
        return result


class _FeatureBuilder:
    def __init__(self):
        self.seen = []

    def build(self, snapshot, candidate):
        self.seen.append((snapshot, candidate, copy.deepcopy(candidate.features)))
        return {"built_for": candidate.stock_code}


class _Matcher:
    def __init__(self):
        self.seen = []

    def evaluate(self, market_features, candidate):
        self.seen.append((copy.deepcopy(market_features), copy.deepcopy(candidate)))
        return [
            _evaluation(candidate.stock_code, row["mode_key"])
            for row in _rule_rows()
        ]

    def rule_snapshot(self):
        return _rule_rows()


class _DroppingEvaluationMatcher:
    def __init__(self, delegate, *, stock_code: str, mode_key: str):
        self.delegate = delegate
        self.stock_code = stock_code
        self.mode_key = mode_key

    def evaluate(self, market_features, candidate):
        rows = self.delegate.evaluate(market_features, candidate)
        if candidate.stock_code != self.stock_code:
            return rows
        return [row for row in rows if row.mode_key != self.mode_key]

    def rule_snapshot(self):
        return self.delegate.rule_snapshot()


class _TamperingEvaluationMatcher:
    def __init__(self, delegate, *, stock_code: str, mutation: str):
        self.delegate = delegate
        self.stock_code = stock_code
        self.mutation = mutation

    def evaluate(self, market_features, candidate):
        rows = self.delegate.evaluate(market_features, candidate)
        if candidate.stock_code != self.stock_code:
            return rows
        if self.mutation == "extra":
            return rows + [
                replace(
                    rows[0],
                    mode_key="unexpected_mode",
                    rule_hash=hashlib.sha256(b"unexpected_mode").hexdigest(),
                )
            ]
        if self.mutation == "duplicate":
            return rows + [rows[0]]
        raise AssertionError(f"unknown mutation: {self.mutation}")

    def rule_snapshot(self):
        return self.delegate.rule_snapshot()


class _PlanService:
    def __init__(self):
        self.calls = []

    async def generate(self, db, snapshot, evaluations, stock_names, rule_snapshot):
        self.calls.append(
            (db, snapshot, list(evaluations), dict(stock_names), list(rule_snapshot))
        )
        return {"stage": snapshot.stage, "candidates": []}


class TradingPlaybookOrchestratorTests(unittest.IsolatedAsyncioTestCase):
    async def test_preclose_runs_real_interfaces_and_shares_built_features(self):
        raw_candidates = [_candidate("000002"), _candidate("000001")]
        market_data = _EchoMarketData(candidates=raw_candidates)
        analyzer = _CopyAnalyzer()
        feature_builder = _FeatureBuilder()
        matcher = _Matcher()
        plan_service = _PlanService()
        resolver_calls = []

        def resolver(value):
            resolver_calls.append(value)
            return TARGET_DATE

        orchestrator = TradingPlaybookOrchestrator(
            market_data=market_data,
            analyzer=analyzer,
            feature_builder=feature_builder,
            matcher=matcher,
            plan_service=plan_service,
            next_trade_date=resolver,
        )
        db = AsyncMock()

        result = await orchestrator.build_stage(
            db, SOURCE_DATE, "preclose", AS_OF, degraded=True
        )

        self.assertEqual(result["stage"], "preclose")
        self.assertEqual(resolver_calls, [SOURCE_DATE])
        self.assertEqual(
            market_data.calls,
            [
                {
                    "db": db,
                    "source_trade_date": SOURCE_DATE,
                    "target_trade_date": TARGET_DATE,
                    "stage": "preclose",
                    "as_of": AS_OF,
                    "force_degraded": True,
                }
            ],
        )
        self.assertEqual(len(plan_service.calls), 1)
        _, planned, evaluations, names, rules = plan_service.calls[0]
        self.assertEqual([row.stock_code for row in planned.candidates], ["000001", "000002"])
        self.assertEqual(len(evaluations), 38)
        self.assertEqual(
            {row.stock_code for row in evaluations}, {"000001", "000002"}
        )
        self.assertEqual(names, {"000001": "股票000001", "000002": "股票000002"})
        self.assertEqual(len(rules), 19)
        for candidate in planned.candidates:
            self.assertEqual(candidate.features["marker"], "raw")
            self.assertEqual(candidate.features["built_for"], candidate.stock_code)
        for _, candidate in matcher.seen:
            self.assertEqual(candidate.features["built_for"], candidate.stock_code)
        self.assertEqual([row.features for row in raw_candidates], [{"marker": "raw"}] * 2)
        db.commit.assert_not_awaited()
        db.rollback.assert_not_awaited()

    async def test_stage_date_semantics_are_exact_and_calendar_is_not_guessed(self):
        cases = (
            ("preclose", SOURCE_DATE, AS_OF, TARGET_DATE, 1),
            (
                "after_close",
                SOURCE_DATE,
                datetime(2026, 7, 10, 15, 30, tzinfo=CN_TZ),
                TARGET_DATE,
                1,
            ),
            (
                "overnight",
                TARGET_DATE,
                datetime(2026, 7, 13, 8, 50, tzinfo=CN_TZ),
                TARGET_DATE,
                0,
            ),
            (
                "auction",
                TARGET_DATE,
                datetime(2026, 7, 13, 9, 26, tzinfo=CN_TZ),
                TARGET_DATE,
                0,
            ),
        )
        for stage, source, as_of, expected_target, expected_resolver_calls in cases:
            with self.subTest(stage=stage):
                market_data = _EchoMarketData()
                calls = []

                def resolver(value):
                    calls.append(value)
                    return TARGET_DATE

                orchestrator = TradingPlaybookOrchestrator(
                    market_data=market_data,
                    analyzer=_CopyAnalyzer(),
                    feature_builder=_FeatureBuilder(),
                    matcher=_Matcher(),
                    plan_service=_PlanService(),
                    next_trade_date=resolver,
                )
                await orchestrator.build_stage(object(), source, stage, as_of)
                call = market_data.calls[0]
                self.assertEqual(call["source_trade_date"], source)
                self.assertEqual(call["target_trade_date"], expected_target)
                self.assertIs(call["as_of"], as_of)
                self.assertEqual(len(calls), expected_resolver_calls)

    async def test_invalid_inputs_and_calendar_results_stop_before_provider(self):
        invalid_inputs = (
            (SOURCE_DATE, "bad-stage", AS_OF, False),
            (datetime(2026, 7, 10, tzinfo=CN_TZ), "preclose", AS_OF, False),
            (SOURCE_DATE, "preclose", AS_OF.replace(tzinfo=None), False),
            (SOURCE_DATE, "preclose", AS_OF, 1),
        )
        for source, stage, as_of, degraded in invalid_inputs:
            with self.subTest(source=source, stage=stage, as_of=as_of):
                provider = AsyncMock()
                orchestrator = TradingPlaybookOrchestrator(
                    market_data=provider,
                    analyzer=_CopyAnalyzer(),
                    feature_builder=_FeatureBuilder(),
                    matcher=_Matcher(),
                    plan_service=_PlanService(),
                    next_trade_date=lambda value: TARGET_DATE,
                )
                with self.assertRaises((TypeError, ValueError)):
                    await orchestrator.build_stage(
                        object(), source, stage, as_of, degraded=degraded
                    )
                provider.build_market_snapshot.assert_not_awaited()

        for resolved in (None, [], SOURCE_DATE, date(2026, 7, 9), AS_OF):
            with self.subTest(resolved=resolved):
                provider = AsyncMock()
                orchestrator = TradingPlaybookOrchestrator(
                    market_data=provider,
                    analyzer=_CopyAnalyzer(),
                    feature_builder=_FeatureBuilder(),
                    matcher=_Matcher(),
                    plan_service=_PlanService(),
                    next_trade_date=lambda value, result=resolved: result,
                )
                with self.assertRaises((TypeError, ValueError)):
                    await orchestrator.build_stage(
                        object(), SOURCE_DATE, "preclose", AS_OF
                    )
                provider.build_market_snapshot.assert_not_awaited()

        provider = AsyncMock()

        def failed_calendar(_value):
            raise RuntimeError("calendar unavailable")

        orchestrator = TradingPlaybookOrchestrator(
            market_data=provider,
            analyzer=_CopyAnalyzer(),
            feature_builder=_FeatureBuilder(),
            matcher=_Matcher(),
            plan_service=_PlanService(),
            next_trade_date=failed_calendar,
        )
        with self.assertRaisesRegex(RuntimeError, "calendar unavailable"):
            await orchestrator.build_stage(
                object(), SOURCE_DATE, "preclose", AS_OF
            )
        provider.build_market_snapshot.assert_not_awaited()

    async def test_provider_snapshot_identity_mismatch_is_never_persisted(self):
        wrong_snapshots = (
            object(),
            _snapshot(source=date(2026, 7, 9)),
            _snapshot(target=date(2026, 7, 14)),
            _snapshot(stage="after_close"),
            _snapshot(as_of=datetime(2026, 7, 10, 14, 41, tzinfo=CN_TZ)),
        )
        for wrong in wrong_snapshots:
            with self.subTest(wrong=wrong):
                provider = AsyncMock()
                provider.build_market_snapshot.return_value = wrong
                analyzer = MagicMock()
                plan = AsyncMock()
                orchestrator = TradingPlaybookOrchestrator(
                    market_data=provider,
                    analyzer=analyzer,
                    feature_builder=_FeatureBuilder(),
                    matcher=_Matcher(),
                    plan_service=plan,
                    next_trade_date=lambda value: TARGET_DATE,
                )
                with self.assertRaises((TypeError, ValueError)):
                    await orchestrator.build_stage(
                        object(), SOURCE_DATE, "preclose", AS_OF
                    )
                analyzer.enrich_snapshot.assert_not_called()
                plan.generate.assert_not_awaited()

    async def test_layer_failures_do_not_persist_or_mutate_provider_snapshot(self):
        failed_provider = AsyncMock()
        failed_provider.build_market_snapshot.side_effect = RuntimeError(
            "provider failed"
        )
        failed_plan = AsyncMock()
        orchestrator = TradingPlaybookOrchestrator(
            market_data=failed_provider,
            analyzer=_CopyAnalyzer(),
            feature_builder=_FeatureBuilder(),
            matcher=_Matcher(),
            plan_service=failed_plan,
            next_trade_date=lambda value: TARGET_DATE,
        )
        with self.assertRaisesRegex(RuntimeError, "provider failed"):
            await orchestrator.build_stage(
                object(), SOURCE_DATE, "preclose", AS_OF
            )
        failed_plan.generate.assert_not_awaited()

        raw = _snapshot(candidates=[_candidate("000001")])
        provider = AsyncMock()
        provider.build_market_snapshot.return_value = raw
        plan = AsyncMock()

        class MutatingFailedAnalyzer:
            def enrich_snapshot(self, snapshot):
                snapshot.market_features["polluted"] = True
                raise RuntimeError("analysis failed")

        orchestrator = TradingPlaybookOrchestrator(
            market_data=provider,
            analyzer=MutatingFailedAnalyzer(),
            feature_builder=_FeatureBuilder(),
            matcher=_Matcher(),
            plan_service=plan,
            next_trade_date=lambda value: TARGET_DATE,
        )
        with self.assertRaisesRegex(RuntimeError, "analysis failed"):
            await orchestrator.build_stage(object(), SOURCE_DATE, "preclose", AS_OF)
        self.assertNotIn("polluted", raw.market_features)
        plan.generate.assert_not_awaited()

        class MutatingFailedBuilder:
            def build(self, snapshot, candidate):
                snapshot.market_features["polluted"] = True
                candidate.features["polluted"] = True
                raise RuntimeError("feature failed")

        orchestrator = TradingPlaybookOrchestrator(
            market_data=provider,
            analyzer=_CopyAnalyzer(),
            feature_builder=MutatingFailedBuilder(),
            matcher=_Matcher(),
            plan_service=plan,
            next_trade_date=lambda value: TARGET_DATE,
        )
        with self.assertRaisesRegex(RuntimeError, "feature failed"):
            await orchestrator.build_stage(object(), SOURCE_DATE, "preclose", AS_OF)
        self.assertNotIn("polluted", raw.market_features)
        self.assertNotIn("polluted", raw.candidates[0].features)
        plan.generate.assert_not_awaited()

        matcher = MagicMock()
        matcher.evaluate.side_effect = RuntimeError("matcher failed")
        matcher.rule_snapshot.return_value = _rule_rows()
        orchestrator = TradingPlaybookOrchestrator(
            market_data=provider,
            analyzer=_CopyAnalyzer(),
            feature_builder=_FeatureBuilder(),
            matcher=matcher,
            plan_service=plan,
            next_trade_date=lambda value: TARGET_DATE,
        )
        with self.assertRaisesRegex(RuntimeError, "matcher failed"):
            await orchestrator.build_stage(object(), SOURCE_DATE, "preclose", AS_OF)
        plan.generate.assert_not_awaited()

    async def test_duplicate_candidates_and_incomplete_rule_snapshot_are_rejected(self):
        duplicate_provider = _EchoMarketData(
            candidates=[_candidate("000001"), _candidate("000001")]
        )
        feature_builder = _FeatureBuilder()
        plan = _PlanService()
        orchestrator = TradingPlaybookOrchestrator(
            market_data=duplicate_provider,
            analyzer=_CopyAnalyzer(),
            feature_builder=feature_builder,
            matcher=_Matcher(),
            plan_service=plan,
            next_trade_date=lambda value: TARGET_DATE,
        )
        with self.assertRaisesRegex(ValueError, "duplicate"):
            await orchestrator.build_stage(object(), SOURCE_DATE, "preclose", AS_OF)
        self.assertEqual(feature_builder.seen, [])
        self.assertEqual(plan.calls, [])

        matcher = _Matcher()
        matcher.rule_snapshot = lambda: _rule_rows()[:-1]
        orchestrator = TradingPlaybookOrchestrator(
            market_data=_EchoMarketData(candidates=[_candidate("000001")]),
            analyzer=_CopyAnalyzer(),
            feature_builder=_FeatureBuilder(),
            matcher=matcher,
            plan_service=plan,
            next_trade_date=lambda value: TARGET_DATE,
        )
        with self.assertRaisesRegex(ValueError, "19|complete"):
            await orchestrator.build_stage(object(), SOURCE_DATE, "preclose", AS_OF)
        self.assertEqual(plan.calls, [])

    async def test_each_candidate_must_have_exact_catalog_mode_coverage(self):
        catalog = RuleCatalog(
            Path("app/data/trading_playbook_rules_v1.json")
        ).load()
        real_matcher = ModeMatcher(
            catalog["rules"], catalog_version=catalog["catalog_version"]
        )
        cases = (
            (
                _DroppingEvaluationMatcher(
                    real_matcher,
                    stock_code="000001",
                    mode_key=catalog["rules"][0]["mode_key"],
                ),
                "missing",
            ),
            (
                _TamperingEvaluationMatcher(
                    real_matcher, stock_code="000001", mutation="extra"
                ),
                "extra.*unexpected_mode",
            ),
            (
                _TamperingEvaluationMatcher(
                    real_matcher, stock_code="000001", mutation="duplicate"
                ),
                "duplicate",
            ),
        )
        for matcher, expected_error in cases:
            with self.subTest(expected_error=expected_error):
                plan = _PlanService()
                orchestrator = TradingPlaybookOrchestrator(
                    market_data=_EchoMarketData(
                        candidates=[_candidate("000001")]
                    ),
                    analyzer=_CopyAnalyzer(),
                    feature_builder=ModeFeatureBuilder(),
                    matcher=matcher,
                    plan_service=plan,
                    next_trade_date=lambda value: TARGET_DATE,
                )
                with self.assertRaisesRegex(ValueError, expected_error):
                    await orchestrator.build_stage(
                        object(), SOURCE_DATE, "preclose", AS_OF
                    )
                self.assertEqual(plan.calls, [])

    async def test_second_candidate_mode_gap_stops_the_whole_plan(self):
        catalog = RuleCatalog(
            Path("app/data/trading_playbook_rules_v1.json")
        ).load()
        missing_mode = catalog["rules"][0]["mode_key"]
        matcher = _DroppingEvaluationMatcher(
            ModeMatcher(
                catalog["rules"], catalog_version=catalog["catalog_version"]
            ),
            stock_code="000002",
            mode_key=missing_mode,
        )
        plan = _PlanService()
        orchestrator = TradingPlaybookOrchestrator(
            market_data=_EchoMarketData(
                candidates=[_candidate("000001"), _candidate("000002")]
            ),
            analyzer=_CopyAnalyzer(),
            feature_builder=ModeFeatureBuilder(),
            matcher=matcher,
            plan_service=plan,
            next_trade_date=lambda value: TARGET_DATE,
        )

        with self.assertRaisesRegex(
            ValueError, rf"000002.*missing.*{missing_mode}"
        ):
            await orchestrator.build_stage(
                object(), SOURCE_DATE, "preclose", AS_OF
            )

        self.assertEqual(plan.calls, [])

    def test_composition_root_uses_one_real_catalog_and_explicit_data_dependencies(self):
        quote_api = object()
        kline_loader = object()
        realtime_loader = object()
        context_loader = object()
        resolver = lambda value: TARGET_DATE

        orchestrator = build_default_orchestrator(
            quote_api=quote_api,
            kline_loader=kline_loader,
            realtime_limit_up_loader=realtime_loader,
            full_market_context_loader=context_loader,
            next_trade_date=resolver,
        )

        self.assertIsInstance(orchestrator, TradingPlaybookOrchestrator)
        self.assertIsInstance(
            orchestrator.market_data, TradingPlaybookMarketDataProvider
        )
        self.assertIs(orchestrator.market_data.quote_api, quote_api)
        self.assertIs(orchestrator.market_data.kline_loader, kline_loader)
        self.assertIs(
            orchestrator.market_data.realtime_limit_up_loader, realtime_loader
        )
        self.assertIs(
            orchestrator.market_data.full_market_context_loader, context_loader
        )
        self.assertIsInstance(orchestrator.analyzer, MarketStateAnalyzer)
        self.assertIsInstance(orchestrator.feature_builder, ModeFeatureBuilder)
        self.assertIsInstance(orchestrator.matcher, ModeMatcher)
        self.assertEqual(len(orchestrator.matcher.rule_snapshot()), 19)
        self.assertIsInstance(orchestrator.plan_service, TradingPlanService)
        self.assertIs(orchestrator.next_trade_date, resolver)


class _FixtureQuoteAPI:
    def __init__(self, payloads):
        self.payloads = payloads

    async def get_quotes_batch(self, codes):
        return {
            code: copy.deepcopy(self.payloads[code])
            for code in codes
            if code in self.payloads
        }


class TradingPlaybookOrchestratorIntegrationTests(
    unittest.IsolatedAsyncioTestCase
):
    async def asyncSetUp(self):
        self.engine = create_async_engine(
            "sqlite+aiosqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        self.Session = async_sessionmaker(self.engine, expire_on_commit=False)
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)

    async def asyncTearDown(self):
        await self.engine.dispose()

    async def test_real_task3_to_task6_pipeline_persists_one_bounded_plan(self):
        codes = ("300001", "000002")
        payloads = {
            code: {
                "code": code,
                "name": f"Mode {code}",
                "price": "10.5" if code == "300001" else "10.2",
                "pre_close": "10",
                "open": "10.1",
                "amount": "2000" if code == "300001" else "1000",
                "turnover_rate": "3.2",
                "bid1_price": "10.5" if code == "300001" else "10.2",
                "bid1_volume": "88",
                "limit_up": "11",
                "datetime": AS_OF,
            }
            for code in codes
        }

        async def realtime_loader(_trade_date):
            return [
                {
                    "stock_code": code,
                    "theme_name": "AI",
                    "_collected_at": AS_OF - timedelta(minutes=1),
                    "first_limit_up_time": (
                        "09:31:00" if code == "300001" else "09:35:00"
                    ),
                    "continuous_limit_up_days": 2 if code == "300001" else 1,
                    "seal_amount": 500 if code == "300001" else 300,
                    "is_final_sealed": True,
                    "open_count": 2 if code == "300001" else 1,
                    "float_market_value": (
                        2_000_000 if code == "300001" else 1_000_000
                    ),
                }
                for code in codes
            ]

        async def kline_loader(*_args, **_kwargs):
            return [
                {
                    "date": SOURCE_DATE - timedelta(days=offset),
                    "close": close,
                }
                for offset, close in zip(
                    range(6, 0, -1), (8.0, 8.2, 8.4, 8.6, 8.8, 9.2)
                )
            ]

        context = {
            "limit_up_count": 20,
            "limit_up_count_prev": 10,
            "trend_new_high_count": 5,
            "trend_new_high_count_prev": 5,
            "limit_down_count": 1,
            "max_board_height": 3,
            "seal_rate": 80,
            "negative_feedback": False,
            "divergence_days": 0,
            "sell_pressure_falling": False,
            "breadth_recovered": False,
            "prior_window": "",
            "sell_pressure_rising": False,
        }

        async def context_loader(trade_date, _stage, captured_at):
            return {
                "scope": "full_market",
                "trade_date": trade_date,
                "captured_at": captured_at - timedelta(minutes=1),
                **context,
                "field_quality": {key: "ready" for key in context},
            }

        catalog = RuleCatalog(
            Path("app/data/trading_playbook_rules_v1.json")
        ).load()
        orchestrator = TradingPlaybookOrchestrator(
            market_data=TradingPlaybookMarketDataProvider(
                quote_api=_FixtureQuoteAPI(payloads),
                kline_loader=kline_loader,
                realtime_limit_up_loader=realtime_loader,
                full_market_context_loader=context_loader,
            ),
            analyzer=MarketStateAnalyzer(),
            feature_builder=ModeFeatureBuilder(),
            matcher=ModeMatcher(
                catalog["rules"], catalog_version=catalog["catalog_version"]
            ),
            plan_service=TradingPlanService(),
            next_trade_date=lambda value: TARGET_DATE,
        )

        async with self.Session() as db:
            db.add_all(
                [
                    Stock(
                        stock_code=code,
                        stock_name=f"Mode {code}",
                        market="SZ",
                        is_st=0,
                        circulating_shares=100_000,
                    )
                    for code in codes
                ]
            )
            await db.commit()
            result = await orchestrator.build_stage(
                db, SOURCE_DATE, "preclose", AS_OF
            )
            persisted = await db.scalar(
                select(func.count()).select_from(TradingPlanVersion)
            )

        self.assertEqual(result["stage"], "preclose")
        self.assertEqual(persisted, 1)
        self.assertEqual(len(result["mode_radar_json"]), 38)
        matched = {
            (row["stock_code"], row["mode_key"]): row
            for row in result["mode_radar_json"]
        }
        self.assertEqual(
            matched[("300001", "new_theme_high_volatility")]["status"],
            "matched",
        )
        self.assertGreaterEqual(len(result["candidates"]), 1)
        self.assertLessEqual(len(result["candidates"]), 3)

    async def test_candidate_missing_one_catalog_mode_is_rejected_before_plan_write(self):
        catalog = RuleCatalog(
            Path("app/data/trading_playbook_rules_v1.json")
        ).load()
        missing_mode = catalog["rules"][0]["mode_key"]
        matcher = _DroppingEvaluationMatcher(
            ModeMatcher(
                catalog["rules"], catalog_version=catalog["catalog_version"]
            ),
            stock_code="000001",
            mode_key=missing_mode,
        )
        orchestrator = TradingPlaybookOrchestrator(
            market_data=_EchoMarketData(candidates=[_candidate("000001")]),
            analyzer=_CopyAnalyzer(),
            feature_builder=ModeFeatureBuilder(),
            matcher=matcher,
            plan_service=TradingPlanService(),
            next_trade_date=lambda value: TARGET_DATE,
        )

        async with self.Session() as db:
            with self.assertRaisesRegex(
                ValueError, rf"missing.*{missing_mode}"
            ):
                await orchestrator.build_stage(
                    db, SOURCE_DATE, "preclose", AS_OF
                )
            persisted = await db.scalar(
                select(func.count()).select_from(TradingPlanVersion)
            )

        self.assertEqual(persisted, 0)

    async def test_empty_candidate_snapshot_persists_zero_radar_with_full_rules(self):
        catalog = RuleCatalog(
            Path("app/data/trading_playbook_rules_v1.json")
        ).load()
        orchestrator = TradingPlaybookOrchestrator(
            market_data=_EchoMarketData(),
            analyzer=_CopyAnalyzer(),
            feature_builder=ModeFeatureBuilder(),
            matcher=ModeMatcher(
                catalog["rules"], catalog_version=catalog["catalog_version"]
            ),
            plan_service=TradingPlanService(),
            next_trade_date=lambda value: TARGET_DATE,
        )

        async with self.Session() as db:
            result = await orchestrator.build_stage(
                db, SOURCE_DATE, "preclose", AS_OF
            )
            persisted = await db.scalar(
                select(func.count()).select_from(TradingPlanVersion)
            )

        self.assertEqual(persisted, 1)
        self.assertEqual(result["mode_radar_json"], [])
        self.assertEqual(len(result["rule_snapshot_json"]), 19)

    async def test_first_non_preclose_stage_propagates_missing_lineage(self):
        catalog = RuleCatalog(
            Path("app/data/trading_playbook_rules_v1.json")
        ).load()
        as_of = datetime(2026, 7, 10, 15, 30, tzinfo=CN_TZ)
        orchestrator = TradingPlaybookOrchestrator(
            market_data=_EchoMarketData(),
            analyzer=_CopyAnalyzer(),
            feature_builder=_FeatureBuilder(),
            matcher=ModeMatcher(
                catalog["rules"], catalog_version=catalog["catalog_version"]
            ),
            plan_service=TradingPlanService(),
            next_trade_date=lambda value: TARGET_DATE,
        )

        async with self.Session() as db:
            with self.assertRaisesRegex(
                ValueError, "predecessor|previous stage|retry"
            ):
                await orchestrator.build_stage(
                    db, SOURCE_DATE, "after_close", as_of
                )
            persisted = await db.scalar(
                select(func.count()).select_from(TradingPlanVersion)
            )

        self.assertEqual(persisted, 0)


if __name__ == "__main__":
    unittest.main()
