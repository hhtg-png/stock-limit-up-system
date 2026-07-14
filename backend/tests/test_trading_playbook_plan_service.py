import asyncio
import copy
import hashlib
import math
import tempfile
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.database import Base, ensure_sqlite_schema_compat
from app.models.trading_playbook import (
    TradingPlanCandidate,
    TradingPlanVersion,
    TradingPlaybookSettings,
)
from app.models.stock import Stock
from app.services.trading_playbook.domain import (
    CandidateSnapshot,
    DataQuality,
    MarketSnapshot,
    ModeEvaluation,
)
from app.services.trading_playbook.plan_service import TradingPlanService
from app.services.trading_playbook.market_data import (
    TradingPlaybookMarketDataProvider,
)
from app.services.trading_playbook.market_state import MarketStateAnalyzer
from app.services.trading_playbook import serialization as serialization_module
from app.services.trading_playbook.errors import (
    InvalidRequestError,
    InvalidTransitionError,
)


SOURCE_DATE = date(2026, 7, 10)
TARGET_DATE = date(2026, 7, 13)
AS_OF = datetime(2026, 7, 10, 14, 40)


def _rule_hash(mode_key: str) -> str:
    return hashlib.sha256(mode_key.encode("utf-8")).hexdigest()


def _rule_snapshot(*mode_keys: str) -> list[dict]:
    return [
        {
            "mode_key": mode_key,
            "version": 1,
            "content_hash": _rule_hash(mode_key),
            "source_refs": [{"source_key": "test", "excerpt": mode_key}],
        }
        for mode_key in reversed(sorted(set(mode_keys)))
    ]


def _evaluation(
    mode_key: str,
    stock_code: str,
    *,
    status: str = "matched",
    score: float = 100,
    risk_level: str = "confirmed",
    reference_price=10.0,
    action_scope: str = "target",
) -> ModeEvaluation:
    return ModeEvaluation(
        mode_key=mode_key,
        stock_code=stock_code,
        status=status,
        score=score,
        role="leader",
        risk_level=risk_level,
        entry_trigger={
            "label": "进入",
            "reference_price": reference_price,
            "sealed": True,
        },
        invalidation={"label": "逻辑失效", "price_lte": 8.88},
        exit_trigger={"label": "退出", "change_pct_lte": -5.0},
        evidence=[
            {
                "source": "test",
                "quality": "ready",
                "captured_at": AS_OF,
            }
        ],
        rule_version=1,
        rule_hash=_rule_hash(mode_key),
        action_scope=action_scope,
    )


def _snapshot(
    *,
    stage: str = "preclose",
    quality_status: str = "ready",
    stale: bool = False,
    warnings: list[str] | None = None,
    quality_as_of: datetime = AS_OF,
    forced_degraded: bool = False,
    degradation_reason: str | None = None,
) -> MarketSnapshot:
    candidates = [
        CandidateSnapshot(
            stock_code=f"00000{i}",
            stock_name=f"快照股票{i}",
            theme_name="机器人" if i % 2 else "芯片",
            features={
                "recognition_rank": i,
                "recognition_score": 6 - i,
                "recognition_evidence": {"fastest": {"rank": i}},
            },
            evidence=[{"source": "snapshot", "captured_at": AS_OF}],
        )
        for i in range(1, 6)
    ]
    return MarketSnapshot(
        source_trade_date=SOURCE_DATE,
        target_trade_date=TARGET_DATE,
        stage=stage,
        as_of=AS_OF,
        market_features={
            "style": "board_flow",
            "window": "outbreak",
            "quality": quality_status,
        },
        candidates=candidates,
        theme_rankings=[
            {"theme_name": "机器人", "rank": 1, "score": 57},
            {"theme_name": "芯片", "rank": 2, "score": 18},
        ],
        quality=DataQuality(
            quality_status,
            quality_as_of,
            "test",
            stale=stale,
            warnings=warnings or [],
            forced_degraded=forced_degraded,
            degradation_reason=degradation_reason,
        ),
    )


class TradingPlaybookPlanServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine(
            "sqlite+aiosqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        self.Session = async_sessionmaker(self.engine, expire_on_commit=False)
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        self.service = TradingPlanService()

    async def asyncTearDown(self):
        await self.engine.dispose()

    async def _generate(
        self,
        db,
        evaluations,
        *,
        snapshot=None,
        rule_snapshot=None,
    ):
        snapshot = snapshot or _snapshot()
        return await self.service.generate(
            db,
            snapshot,
            evaluations,
            stock_names={f"00000{i}": f"股票{i}" for i in range(1, 6)},
            rule_snapshot=rule_snapshot
            if rule_snapshot is not None
            else _rule_snapshot(*(row.mode_key for row in evaluations)),
        )

    async def test_structured_forced_degradation_survives_many_ordinary_warnings(self):
        warnings = [f"ordinary warning {index}" for index in range(51)]
        snapshot = _snapshot(
            stage="after_close",
            quality_status="degraded",
            warnings=warnings,
            forced_degraded=True,
            degradation_reason="after_close_barrier_timeout",
        )

        async with self.Session() as db:
            await self._generate(
                db,
                [_evaluation("leader", "000001")],
                snapshot=_snapshot(stage="preclose"),
            )
            payload = await self._generate(
                db,
                [_evaluation("leader", "000001")],
                snapshot=snapshot,
            )

        quality = payload["data_quality_json"]
        self.assertTrue(quality["forced_degraded"])
        self.assertEqual(
            quality["degradation_reason"],
            "after_close_barrier_timeout",
        )
        self.assertEqual(quality["warnings"], warnings)

    async def test_forced_marker_survives_actual_market_data_analyzer_and_persistence(self):
        class OneQuoteAPI:
            async def get_quotes_batch(self, codes):
                if "000001" not in codes:
                    return {}
                return {
                    "000001": {
                        "code": "000001",
                        "name": "Pipeline Candidate",
                        "price": 10,
                        "pre_close": 9.5,
                        "open": 9.8,
                        "amount": 1000,
                        "turnover_rate": 2,
                        "bid1_price": 10,
                        "bid1_volume": 100,
                        "limit_up": 10.45,
                        "datetime": "20260710144000",
                    }
                }

        async def kline_loader(*_args, **_kwargs):
            return [
                {
                    "date": SOURCE_DATE - timedelta(days=6 - index),
                    "available_at": AS_OF - timedelta(days=6 - index),
                    "close": close,
                }
                for index, close in enumerate(
                    (8.0, 8.2, 8.4, 8.6, 8.8, 9.2)
                )
            ]

        async with self.Session() as db:
            db.add_all(
                [
                    Stock(
                        stock_code=f"{index:06d}",
                        stock_name=f"Pipeline {index}",
                        market="SZ",
                        is_st=0,
                    )
                    for index in range(1, 53)
                ]
            )
            await db.commit()
            raw_snapshot = await TradingPlaybookMarketDataProvider(
                quote_api=OneQuoteAPI(),
                kline_loader=kline_loader,
                realtime_limit_up_loader=lambda _date: asyncio.sleep(
                    0, result=[]
                ),
            ).build_market_snapshot(
                db=db,
                source_trade_date=SOURCE_DATE,
                target_trade_date=SOURCE_DATE,
                stage="preclose",
                as_of=AS_OF,
                force_degraded=True,
                force_degraded_reason="after_close_barrier_timeout",
            )
            self.assertGreater(len(raw_snapshot.quality.warnings), 51)

            analyzed_snapshot = MarketStateAnalyzer().enrich_snapshot(
                raw_snapshot
            )
            payload = await self._generate(
                db,
                [_evaluation("leader", "000001")],
                snapshot=analyzed_snapshot,
            )

        self.assertLessEqual(len(analyzed_snapshot.quality.warnings), 50)
        self.assertTrue(payload["data_quality_json"]["forced_degraded"])
        self.assertEqual(
            payload["data_quality_json"]["degradation_reason"],
            "after_close_barrier_timeout",
        )

    async def test_generate_limits_unique_action_candidates_and_preserves_radar(self):
        evaluations = [
            _evaluation("leader", "000001", score=100),
            _evaluation(
                "support",
                "000001",
                score=90,
                risk_level="trial",
            ),
            _evaluation("tail", "000002", score=99, action_scope="tail"),
            _evaluation("third", "000003", score=98, risk_level="trial"),
            _evaluation("fourth", "000004", score=97),
            _evaluation("waiting", "000005", status="waiting", risk_level="watch"),
            _evaluation(
                "manual",
                "000005",
                status="manual_review",
                risk_level="watch",
            ),
            _evaluation(
                "failed",
                "000005",
                status="not_matched",
                risk_level="avoid",
            ),
        ]
        original_snapshot = copy.deepcopy(_snapshot())
        original_evaluations = copy.deepcopy(evaluations)
        rules = _rule_snapshot(*(row.mode_key for row in evaluations))
        original_rules = copy.deepcopy(rules)

        async with self.Session() as db:
            plan = await self._generate(
                db,
                evaluations,
                snapshot=original_snapshot,
                rule_snapshot=rules,
            )

        self.assertEqual(len(plan["candidates"]), 3)
        self.assertEqual(len({row["stock_code"] for row in plan["candidates"]}), 3)
        self.assertEqual(
            [row["stock_code"] for row in plan["candidates"]],
            ["000001", "000002", "000003"],
        )
        self.assertEqual(len(plan["mode_radar"]), len(evaluations))
        self.assertEqual(
            {row["status"] for row in plan["mode_radar"]},
            {"matched", "waiting", "manual_review", "not_matched"},
        )
        first, tail, third = plan["candidates"]
        self.assertEqual(first["supporting_mode_keys_json"], ["support"])
        self.assertEqual(first["position_reference"], 30.0)
        self.assertEqual(third["position_reference"], 10.0)
        self.assertEqual(first["invalidation_json"]["label"], "逻辑失效")
        self.assertEqual(first["invalidation_json"]["price_lte"], 9.5)
        self.assertEqual(tail["action_trade_date"], SOURCE_DATE.isoformat())
        self.assertEqual(first["action_trade_date"], TARGET_DATE.isoformat())
        self.assertEqual(plan["risk_settings"]["max_candidates"], 3)
        self.assertEqual(len(plan["rule_snapshot"]), len(rules))
        self.assertEqual(original_snapshot, _snapshot())
        self.assertEqual(evaluations, original_evaluations)
        self.assertEqual(rules, original_rules)

        async with self.Session() as db:
            settings_count = await db.scalar(
                select(func.count()).select_from(TradingPlaybookSettings)
            )
            self.assertEqual(settings_count, 1)

    async def test_generate_does_not_serialize_after_successful_commit(self):
        async with self.Session() as db:
            with patch.object(
                self.service,
                "serialize",
                AsyncMock(
                    side_effect=RuntimeError(
                        "post-commit generation serialization"
                    )
                ),
            ) as serialize:
                plan = await self._generate(
                    db,
                    [_evaluation("leader", "000001")],
                )

        self.assertEqual(plan["status"], "draft")
        serialize.assert_not_awaited()

    async def test_generate_is_idempotent_and_hashes_every_effective_input(self):
        evaluation = _evaluation("leader", "000001")
        rules = _rule_snapshot("leader")
        async with self.Session() as db:
            first = await self._generate(db, [evaluation], rule_snapshot=rules)
            same = await self.service.generate(
                db,
                _snapshot(),
                [copy.deepcopy(evaluation)],
                stock_names={"000001": "股票1"},
                rule_snapshot=list(reversed(copy.deepcopy(rules))),
            )
            self.assertEqual(same["id"], first["id"])

            changed_snapshot = _snapshot()
            changed_snapshot.market_features["limit_up_count"] = 88
            snapshot_plan = await self._generate(
                db,
                [evaluation],
                snapshot=changed_snapshot,
                rule_snapshot=rules,
            )

            changed_evaluation = copy.deepcopy(evaluation)
            object.__setattr__(changed_evaluation, "score", 101)
            radar_plan = await self._generate(
                db,
                [changed_evaluation],
                rule_snapshot=rules,
            )

            changed_rules = copy.deepcopy(rules)
            changed_rules[0]["description"] = "完整规则快照的变化"
            rule_plan = await self._generate(
                db,
                [evaluation],
                rule_snapshot=changed_rules,
            )

            settings_row = await db.get(TradingPlaybookSettings, 1)
            settings_row.hard_stop_pct = 6
            await db.commit()
            risk_plan = await self._generate(
                db,
                [evaluation],
                rule_snapshot=rules,
            )

            ids = {
                first["id"],
                snapshot_plan["id"],
                radar_plan["id"],
                rule_plan["id"],
                risk_plan["id"],
            }
            self.assertEqual(len(ids), 5)
            versions = (
                await db.scalars(
                    select(TradingPlanVersion).order_by(
                        TradingPlanVersion.version_no
                    )
                )
            ).all()
            self.assertEqual([row.version_no for row in versions], [1, 2, 3, 4, 5])
            self.assertEqual(len({row.input_hash for row in versions}), 5)

    async def test_automatic_versions_form_same_stage_and_cross_stage_parent_chain(self):
        evaluation = _evaluation("leader", "000001")
        async with self.Session() as db:
            preclose_v1 = await self._generate(db, [evaluation])
            changed_preclose = _snapshot()
            changed_preclose.market_features["revision"] = 2
            preclose_v2 = await self._generate(
                db,
                [evaluation],
                snapshot=changed_preclose,
            )
            after_close = await self._generate(
                db,
                [evaluation],
                snapshot=_snapshot(stage="after_close"),
            )
            overnight = await self._generate(
                db,
                [evaluation],
                snapshot=_snapshot(stage="overnight"),
            )
            auction = await self._generate(
                db,
                [evaluation],
                snapshot=_snapshot(stage="auction"),
            )
            same = await self._generate(db, [evaluation])

        self.assertIsNone(preclose_v1["parent_plan_version_id"])
        self.assertEqual(
            preclose_v2["parent_plan_version_id"],
            preclose_v1["id"],
        )
        self.assertEqual(
            after_close["parent_plan_version_id"],
            preclose_v2["id"],
        )
        self.assertEqual(
            overnight["parent_plan_version_id"],
            after_close["id"],
        )
        self.assertEqual(auction["parent_plan_version_id"], overnight["id"])
        self.assertEqual(same["id"], preclose_v1["id"])

    async def test_change_summary_compares_the_same_selected_parent(self):
        async with self.Session() as db:
            await self._generate(
                db,
                [_evaluation("preclose-old", "000001")],
            )
            after_close_v1 = await self._generate(
                db,
                [_evaluation("after-old", "000001")],
                snapshot=_snapshot(stage="after_close"),
            )
            later_preclose = _snapshot()
            later_preclose.market_features["revision"] = "later-preclose"
            await self._generate(
                db,
                [_evaluation("pre-new", "000002")],
                snapshot=later_preclose,
            )
            after_close_v2_snapshot = _snapshot(stage="after_close")
            after_close_v2_snapshot.market_features["revision"] = 2
            after_close_v2 = await self._generate(
                db,
                [_evaluation("after-new", "000003")],
                snapshot=after_close_v2_snapshot,
            )

        self.assertEqual(
            after_close_v2["parent_plan_version_id"],
            after_close_v1["id"],
        )
        summary = after_close_v2["change_summary_json"]
        self.assertEqual(
            summary["previous_plan_version_id"],
            after_close_v1["id"],
        )
        self.assertEqual(
            summary["added_matches"],
            [{"stock_code": "000003", "mode_key": "after-new"}],
        )
        self.assertEqual(
            summary["removed_matches"],
            [{"stock_code": "000001", "mode_key": "after-old"}],
        )

    async def test_first_non_preclose_stage_requires_immediate_predecessor(self):
        async with self.Session() as db:
            for offset, stage in enumerate(
                ("after_close", "overnight", "auction"),
                start=1,
            ):
                snapshot = _snapshot(stage=stage)
                snapshot.target_trade_date = TARGET_DATE + timedelta(days=offset)
                snapshot.market_features["missing_predecessor_case"] = stage
                with self.subTest(stage=stage):
                    with self.assertRaisesRegex(
                        ValueError,
                        "predecessor|previous stage|retry",
                    ):
                        await self._generate(
                            db,
                            [_evaluation(f"missing-{stage}", "000001")],
                            snapshot=snapshot,
                        )
                    await db.rollback()

            count = await db.scalar(
                select(func.count()).select_from(TradingPlanVersion)
            )

        self.assertEqual(count, 0)

    async def test_same_target_generation_serializes_across_stages(self):
        class BlockingSettingsService(TradingPlanService):
            def __init__(self):
                super().__init__()
                self.entered = 0
                self.first_entered = asyncio.Event()
                self.second_entered = asyncio.Event()
                self.release_first = asyncio.Event()

            async def _get_or_create_settings(self, db):
                self.entered += 1
                if self.entered == 1:
                    self.first_entered.set()
                    await self.release_first.wait()
                else:
                    self.second_entered.set()
                return await super()._get_or_create_settings(db)

        service = BlockingSettingsService()
        preclose_evaluation = _evaluation("preclose", "000001")
        after_close_evaluation = _evaluation("after-close", "000001")
        async with self.Session() as first_db, self.Session() as second_db:
            preclose_task = asyncio.create_task(
                service.generate(
                    first_db,
                    _snapshot(),
                    [preclose_evaluation],
                    {"000001": "股票1"},
                    _rule_snapshot("preclose"),
                )
            )
            await service.first_entered.wait()
            after_close_task = asyncio.create_task(
                service.generate(
                    second_db,
                    _snapshot(stage="after_close"),
                    [after_close_evaluation],
                    {"000001": "股票1"},
                    _rule_snapshot("after-close"),
                )
            )
            try:
                await asyncio.wait_for(
                    service.second_entered.wait(),
                    timeout=0.05,
                )
                serialized = False
            except asyncio.TimeoutError:
                serialized = True
            finally:
                service.release_first.set()
            preclose, after_close = await asyncio.gather(
                preclose_task,
                after_close_task,
            )

        self.assertTrue(serialized)
        self.assertEqual(
            after_close["parent_plan_version_id"],
            preclose["id"],
        )

    async def test_rule_snapshot_order_and_dict_order_have_a_stable_hash(self):
        evaluations = [
            _evaluation("alpha", "000001"),
            _evaluation("beta", "000002", score=99),
        ]
        first_rules = _rule_snapshot("alpha", "beta")
        second_rules = [
            {key: value for key, value in reversed(list(row.items()))}
            for row in reversed(copy.deepcopy(first_rules))
        ]

        async with self.Session() as db:
            first = await self._generate(
                db,
                evaluations,
                rule_snapshot=first_rules,
            )
            second = await self._generate(
                db,
                list(reversed(evaluations)),
                rule_snapshot=second_rules,
            )

        self.assertEqual(first["id"], second["id"])
        self.assertEqual(first["input_hash"], second["input_hash"])
        self.assertEqual(
            [row["mode_key"] for row in first["rule_snapshot"]],
            ["alpha", "beta"],
        )

    async def test_invalid_prices_remain_in_radar_but_never_become_candidates(self):
        evaluations = [
            _evaluation("missing", "000001", reference_price=None),
            _evaluation("negative", "000002", reference_price=-1),
            _evaluation("text", "000003", reference_price="10"),
            _evaluation(
                "waiting",
                "000004",
                status="waiting",
                risk_level="watch",
            ),
        ]
        async with self.Session() as db:
            plan = await self._generate(db, evaluations)

        self.assertEqual(plan["candidates"], [])
        self.assertEqual(len(plan["mode_radar"]), 4)

    async def test_rejects_non_json_and_incomplete_rule_evidence_without_writes(self):
        invalid_cases = []

        nan_snapshot = _snapshot()
        nan_snapshot.market_features["bad"] = math.nan
        invalid_cases.append((nan_snapshot, [_evaluation("leader", "000001")], _rule_snapshot("leader")))

        bad_evaluation = _evaluation("leader", "000001")
        object.__setattr__(bad_evaluation, "evidence", [{"value": object()}])
        invalid_cases.append((_snapshot(), [bad_evaluation], _rule_snapshot("leader")))

        invalid_cases.extend(
            [
                (
                    _snapshot(),
                    [_evaluation("leader", "000001")],
                    [{"mode_key": "leader", "version": 1}],
                ),
                (
                    _snapshot(),
                    [_evaluation("leader", "000001")],
                    [
                        {
                            "mode_key": "leader",
                            "version": 1,
                            "content_hash": "not-a-sha256",
                        }
                    ],
                ),
            ]
        )

        async with self.Session() as db:
            for snapshot, evaluations, rules in invalid_cases:
                with self.subTest(rules=rules):
                    with self.assertRaises(ValueError):
                        await self._generate(
                            db,
                            evaluations,
                            snapshot=snapshot,
                            rule_snapshot=rules,
                        )
                    await db.rollback()

            count = await db.scalar(
                select(func.count()).select_from(TradingPlanVersion)
            )
            self.assertEqual(count, 0)

    async def test_unrelated_aggregate_degradation_does_not_block_confirmed(self):
        snapshot = _snapshot(
            quality_status="degraded",
            warnings=["theme member coverage is partial for an unrelated theme"],
        )
        async with self.Session() as db:
            plan = await self._generate(
                db,
                [_evaluation("leader", "000001", risk_level="confirmed")],
                snapshot=snapshot,
            )

        self.assertEqual(len(plan["candidates"]), 1)
        self.assertEqual(plan["candidates"][0]["risk_level"], "confirmed")

    async def test_relevant_stale_future_and_missing_evidence_block_confirmed(self):
        cases = [
            [
                {"source": "provider", "stale": True},
            ],
            [
                {
                    "source": "mode_requirement",
                    "feature": "candidate.flag",
                    "result": "matched",
                    "captured_at": AS_OF + timedelta(seconds=1),
                },
                {"source": "mode_risk", "quality": "ready"},
            ],
            [
                {
                    "source": "mode_requirement",
                    "feature": "candidate.flag",
                    "result": "missing",
                },
                {"source": "mode_risk", "quality": "ready"},
            ],
            [
                {
                    "source": "provider",
                    "required": True,
                    "quality": "invalid",
                }
            ],
            [
                {
                    "source": "provider",
                    "relevant": True,
                    "captured_at": AS_OF,
                }
            ],
        ]
        async with self.Session() as db:
            for index, evidence in enumerate(cases):
                evaluation = _evaluation(
                    f"confirmed-{index}",
                    "000001",
                    risk_level="confirmed",
                )
                object.__setattr__(evaluation, "evidence", evidence)
                snapshot = _snapshot()
                snapshot.market_features["evidence_case"] = index
                plan = await self._generate(
                    db,
                    [evaluation],
                    snapshot=snapshot,
                )
                self.assertEqual(plan["candidates"], [])
                self.assertEqual(len(plan["mode_radar"]), 1)

    async def test_structured_relevant_evidence_ignores_unrelated_provider_warning(self):
        async with self.Session() as db:
            cases = [
                ("ready-false", "ready", False),
                ("computed-zero", "computed", 0),
                ("ok-string", "ok", "valid"),
                ("ready-list", "ready", ["valid"]),
                ("computed-mapping", "computed", {"valid": True}),
            ]
            for case, quality, value in cases:
                evaluation = _evaluation(
                    f"leader-{case}",
                    "000001",
                    risk_level="confirmed",
                )
                object.__setattr__(
                    evaluation,
                    "evidence",
                    [
                        {
                            "source": "provider",
                            "quality": "degraded",
                            "warning": "unrelated aggregate coverage",
                            "relevant": False,
                        },
                        {
                            "source": "mode_requirement",
                            "feature": "candidate.flag",
                            "result": "matched",
                            "captured_at": AS_OF,
                        },
                        {"source": "mode_risk", "quality": "ready"},
                    ],
                )
                snapshot = _snapshot()
                snapshot.market_features["candidate_quality_case"] = case
                snapshot.candidates[0].features["flag"] = value
                snapshot.candidates[0].features["_feature_quality"] = {
                    "flag": quality
                }
                plan = await self._generate(db, [evaluation], snapshot=snapshot)

                with self.subTest(case=case):
                    self.assertEqual(len(plan["candidates"]), 1)
                    self.assertEqual(
                        plan["candidates"][0]["risk_level"], "confirmed"
                    )

    async def test_required_candidate_field_and_quality_are_mandatory_for_confirmed(self):
        cases = [
            ("absent-value", {"_feature_quality": {"flag": "ready"}}),
            (
                "missing-value",
                {"flag": None, "_feature_quality": {"flag": "ready"}},
            ),
            (
                "empty-string",
                {"flag": "", "_feature_quality": {"flag": "ready"}},
            ),
            (
                "blank-string",
                {"flag": " \t", "_feature_quality": {"flag": "ready"}},
            ),
            (
                "unknown",
                {"flag": "unknown", "_feature_quality": {"flag": "ready"}},
            ),
            (
                "unknown-variant",
                {"flag": "  UnKnOwN  ", "_feature_quality": {"flag": "ready"}},
            ),
            ("missing-quality", {"flag": True}),
            (
                "quality-missing",
                {"flag": True, "_feature_quality": {"flag": "missing"}},
            ),
            (
                "quality-invalid",
                {"flag": True, "_feature_quality": {"flag": "invalid"}},
            ),
        ]
        async with self.Session() as db:
            for index, (case, features) in enumerate(cases):
                evaluation = _evaluation(
                    f"required-candidate-field-{index}",
                    "000001",
                    risk_level="confirmed",
                )
                object.__setattr__(
                    evaluation,
                    "evidence",
                    [
                        {
                            "source": "mode_requirement",
                            "feature": "candidate.flag",
                            "result": "matched",
                        },
                        {"source": "mode_risk", "quality": "ready"},
                    ],
                )
                snapshot = _snapshot()
                snapshot.market_features["candidate_field_case"] = case
                snapshot.candidates[0].features.update(features)
                plan = await self._generate(db, [evaluation], snapshot=snapshot)

                with self.subTest(case=case):
                    self.assertEqual(plan["candidates"], [])
                    self.assertEqual(len(plan["mode_radar"]), 1)

    async def test_required_candidate_feature_quality_blocks_only_confirmed(self):
        snapshot = _snapshot()
        snapshot.candidates[0].features["flag"] = True
        snapshot.candidates[0].features["_feature_quality"] = {"flag": "stale"}
        evaluations = []
        for mode, code, risk, score in (
            ("confirmed", "000001", "confirmed", 100),
            ("trial", "000002", "trial", 99),
        ):
            row = _evaluation(mode, code, risk_level=risk, score=score)
            object.__setattr__(
                row,
                "evidence",
                [
                    {
                        "source": "mode_requirement",
                        "feature": "candidate.flag",
                        "result": "matched",
                    },
                    {"source": "mode_risk", "quality": "ready"},
                ],
            )
            evaluations.append(row)

        async with self.Session() as db:
            plan = await self._generate(db, evaluations, snapshot=snapshot)

        self.assertEqual(
            [(row["stock_code"], row["risk_level"]) for row in plan["candidates"]],
            [("000002", "trial")],
        )

    async def test_global_time_unsafety_blocks_confirmed_but_not_trial(self):
        evaluations = [
            _evaluation("confirmed", "000001", risk_level="confirmed"),
            _evaluation("trial", "000002", score=99, risk_level="trial"),
        ]
        snapshots = [
            _snapshot(stale=True),
            _snapshot(quality_as_of=AS_OF + timedelta(seconds=1)),
        ]
        invalid_point_in_time = _snapshot()
        invalid_point_in_time.market_features["_point_in_time_valid"] = False
        snapshots.append(invalid_point_in_time)

        async with self.Session() as db:
            for index, snapshot in enumerate(snapshots):
                snapshot.market_features["case"] = index
                plan = await self._generate(
                    db,
                    evaluations,
                    snapshot=snapshot,
                )
                self.assertEqual(
                    [row["stock_code"] for row in plan["candidates"]],
                    ["000002"],
                )
                self.assertEqual(plan["candidates"][0]["risk_level"], "trial")

    async def test_revise_creates_child_and_only_changes_controlled_candidate_fields(self):
        evaluations = [
            _evaluation("leader", "000001"),
            _evaluation("second", "000002", score=99, risk_level="trial"),
        ]
        async with self.Session() as db:
            generated = await self._generate(db, evaluations)
            parent_id = generated["id"]
            parent_before = await self.service.serialize(db, parent_id)
            first = parent_before["candidates"][0]
            changes = {
                "change_note": "人工调整触发位",
                "candidate_overrides": [
                    {
                        "candidate_id": first["id"],
                        "action_trade_date": SOURCE_DATE.isoformat(),
                        "entry_trigger": {
                            "label": "人工突破",
                            "reference_price": 12.0,
                            "price_gte": 12.2,
                        },
                        "invalidation": {"label": "人工止损"},
                        "exit_trigger": {
                            "label": "人工退出",
                            "change_pct_lte": -3.0,
                        },
                        "manual_note": "只按书面条件执行",
                    }
                ],
            }
            original_changes = copy.deepcopy(changes)

            child = await self.service.revise(db, parent_id, changes)
            child_payload = await self.service.serialize(db, child["id"])
            parent_after = await self.service.serialize(db, parent_id)

        self.assertEqual(changes, original_changes)
        self.assertEqual(parent_before, parent_after)
        self.assertEqual(child["parent_plan_version_id"], parent_id)
        self.assertEqual(child["version_no"], 2)
        self.assertEqual(child["status"], "draft")
        self.assertIsNone(child["confirmed_at"])
        self.assertIsNone(child["confirmed_by"])
        self.assertEqual(len(child_payload["candidates"]), 2)
        revised = child_payload["candidates"][0]
        untouched = child_payload["candidates"][1]
        self.assertNotEqual(revised["id"], first["id"])
        self.assertEqual(revised["action_trade_date"], SOURCE_DATE.isoformat())
        self.assertEqual(revised["entry_trigger_json"]["reference_price"], 12.0)
        self.assertEqual(revised["invalidation_json"]["label"], "人工止损")
        self.assertEqual(revised["invalidation_json"]["price_lte"], 11.4)
        self.assertEqual(
            revised["manual_overrides_json"]["manual_note"],
            "只按书面条件执行",
        )
        self.assertEqual(
            revised["manual_overrides_json"]["invalidation_json"]["price_lte"],
            11.4,
        )
        self.assertEqual(
            untouched["entry_trigger_json"],
            parent_before["candidates"][1]["entry_trigger_json"],
        )
        self.assertEqual(child_payload["change_summary_json"]["manual"], True)
        self.assertEqual(
            child_payload["change_summary_json"]["change_note"],
            "人工调整触发位",
        )

    async def test_revise_accepts_stock_and_mode_as_a_unique_locator(self):
        async with self.Session() as db:
            generated = await self._generate(
                db,
                [_evaluation("leader", "000001")],
            )
            child = await self.service.revise(
                db,
                generated["id"],
                {
                    "change_note": "按代码和模式定位",
                    "candidate_overrides": [
                        {
                            "stock_code": "000001",
                            "primary_mode_key": "leader",
                            "manual_note": "已复核",
                        }
                    ],
                },
            )
            payload = await self.service.serialize(db, child["id"])

        self.assertEqual(
            payload["candidates"][0]["manual_overrides_json"]["manual_note"],
            "已复核",
        )

    async def test_revise_rejects_unknown_duplicate_and_missing_overrides(self):
        async with self.Session() as db:
            generated = await self._generate(
                db,
                [_evaluation("leader", "000001")],
            )
            candidate = generated["candidates"][0]
            bad_changes = [
                {"change_note": "x", "unknown": True},
                {
                    "change_note": "x",
                    "candidate_overrides": [
                        {"candidate_id": candidate["id"], "stock_name": "污染"}
                    ],
                },
                {
                    "change_note": "x",
                    "candidate_overrides": [{"manual_note": "没有定位符"}],
                },
                {
                    "change_note": "x",
                    "candidate_overrides": [
                        {
                            "candidate_id": candidate["id"],
                            "stock_code": "000001",
                            "primary_mode_key": "leader",
                        }
                    ],
                },
                {
                    "change_note": "x",
                    "candidate_overrides": [
                        {"candidate_id": 999999, "manual_note": "不存在"}
                    ],
                },
                {
                    "change_note": "x",
                    "candidate_overrides": [
                        {"candidate_id": candidate["id"], "manual_note": "a"},
                        {"candidate_id": candidate["id"], "manual_note": "b"},
                    ],
                },
                {
                    "change_note": "x",
                    "candidate_overrides": [
                        {
                            "candidate_id": candidate["id"],
                            "action_trade_date": "2026-07-14",
                        }
                    ],
                },
                {
                    "change_note": "x",
                    "candidate_overrides": [
                        {
                            "candidate_id": candidate["id"],
                            "entry_trigger": {"label": "x", "arbitrary": 1},
                        }
                    ],
                },
            ]

            for changes in bad_changes:
                with self.subTest(changes=changes):
                    with self.assertRaises(ValueError):
                        await self.service.revise(db, generated["id"], changes)
                    await db.rollback()

            plan_count = await db.scalar(
                select(func.count()).select_from(TradingPlanVersion)
            )
            candidate_count = await db.scalar(
                select(func.count()).select_from(TradingPlanCandidate)
            )

        self.assertEqual(plan_count, 1)
        self.assertEqual(candidate_count, 1)

    async def test_revise_rejects_manual_hard_stop_override_without_child(self):
        async with self.Session() as db:
            generated = await self._generate(
                db,
                [_evaluation("leader", "000001")],
            )
            candidate_id = generated["candidates"][0]["id"]
            with self.assertRaisesRegex(ValueError, "hard stop|price_lte"):
                await self.service.revise(
                    db,
                    generated["id"],
                    {
                        "change_note": "试图覆盖刚性止损",
                        "candidate_overrides": [
                            {
                                "candidate_id": candidate_id,
                                "invalidation": {
                                    "label": "人工止损",
                                    "price_lte": 1.0,
                                },
                            }
                        ],
                    },
                )
            await db.rollback()
            plan_count = await db.scalar(
                select(func.count()).select_from(TradingPlanVersion)
            )

        self.assertEqual(plan_count, 1)

    async def test_revise_rejects_unsafe_or_contradictory_trigger_overrides(self):
        evaluation = _evaluation("leader", "000001")
        object.__setattr__(
            evaluation,
            "entry_trigger",
            {
                "label": "突破进入",
                "reference_price": 10.0,
                "price_gte": 12.0,
            },
        )
        bad_overrides = [
            ("positive-exit", "exit_trigger", {"change_pct_lte": 50}),
            ("out-of-range-exit", "exit_trigger", {"change_pct_lte": -500}),
            ("negative-entry", "entry_trigger", {"change_pct_gte": -1}),
            ("out-of-range-entry", "entry_trigger", {"change_pct_gte": 101}),
            (
                "out-of-range-invalidation",
                "invalidation",
                {"change_pct_gte": -101},
            ),
            ("merged-price-contradiction", "entry_trigger", {"price_lte": 11}),
            (
                "merged-change-contradiction",
                "exit_trigger",
                {"change_pct_gte": -3},
            ),
        ]
        async with self.Session() as db:
            generated = await self._generate(db, [evaluation])
            candidate_id = generated["candidates"][0]["id"]
            for case, trigger_name, trigger in bad_overrides:
                with self.subTest(case=case):
                    with self.assertRaises(ValueError):
                        await self.service.revise(
                            db,
                            generated["id"],
                            {
                                "change_note": case,
                                "candidate_overrides": [
                                    {
                                        "candidate_id": candidate_id,
                                        trigger_name: trigger,
                                    }
                                ],
                            },
                        )
                    await db.rollback()

            plan_count = await db.scalar(
                select(func.count()).select_from(TradingPlanVersion)
            )

        self.assertEqual(plan_count, 1)

    async def test_revise_rejects_post_reprice_invalidation_contradiction_without_child(self):
        async with self.Session() as db:
            generated = await self._generate(
                db,
                [_evaluation("leader", "000001")],
            )
            parent_before = await self.service.serialize(db, generated["id"])
            candidate_id = generated["candidates"][0]["id"]

            with self.assertRaisesRegex(ValueError, "contradictory invalidation"):
                await self.service.revise(
                    db,
                    generated["id"],
                    {
                        "change_note": "重算后止损矛盾",
                        "candidate_overrides": [
                            {
                                "candidate_id": candidate_id,
                                "entry_trigger": {"reference_price": 5},
                                "invalidation": {"price_gte": 9},
                            }
                        ],
                    },
                )

            parent_after = await self.service.serialize(db, generated["id"])
            plan_count = await db.scalar(
                select(func.count()).select_from(TradingPlanVersion)
            )

        self.assertEqual(plan_count, 1)
        self.assertEqual(parent_after, parent_before)

    async def test_revise_allows_final_stop_boundaries_and_reference_only_change(self):
        cases = [
            ("equal", {"reference_price": 10}, {"price_gte": 9.5}, 9.5),
            ("below", {"reference_price": 10}, {"price_gte": 9}, 9.5),
            ("reference-only", {"reference_price": 5}, None, 4.75),
        ]
        async with self.Session() as db:
            generated = await self._generate(
                db,
                [_evaluation("leader", "000001")],
            )
            candidate_id = generated["candidates"][0]["id"]
            for case, entry, invalidation, expected_stop in cases:
                override = {
                    "candidate_id": candidate_id,
                    "entry_trigger": entry,
                }
                if invalidation is not None:
                    override["invalidation"] = invalidation
                child = await self.service.revise(
                    db,
                    generated["id"],
                    {
                        "change_note": case,
                        "candidate_overrides": [override],
                    },
                )
                payload = await self.service.serialize(db, child["id"])
                revised = payload["candidates"][0]

                with self.subTest(case=case):
                    self.assertEqual(
                        revised["invalidation_json"]["price_lte"],
                        expected_stop,
                    )
                    self.assertEqual(
                        revised["manual_overrides_json"]["invalidation_json"][
                            "price_lte"
                        ],
                        expected_stop,
                    )

    async def test_confirm_activates_target_and_supersedes_old_target_active(self):
        other_target = TARGET_DATE + timedelta(days=1)
        async with self.Session() as db:
            old_active = TradingPlanVersion(
                source_trade_date=SOURCE_DATE,
                target_trade_date=TARGET_DATE,
                stage="preclose",
                version_no=1,
                status="active",
                data_quality_json={"status": "ready"},
                input_hash="old",
            )
            target = TradingPlanVersion(
                source_trade_date=SOURCE_DATE,
                target_trade_date=TARGET_DATE,
                stage="after_close",
                version_no=1,
                status="draft",
                data_quality_json={"status": "ready"},
                input_hash="target",
            )
            other_active = TradingPlanVersion(
                source_trade_date=TARGET_DATE,
                target_trade_date=other_target,
                stage="preclose",
                version_no=1,
                status="active",
                data_quality_json={"status": "ready"},
                input_hash="other",
            )
            db.add_all([old_active, target, other_active])
            await db.commit()
            old_active_id = old_active.id
            target_id = target.id
            other_active_id = other_active.id

            activated = await self.service.confirm(db, target_id, "local-user")

            self.assertEqual(activated["status"], "active")
            self.assertEqual(activated["confirmed_by"], "local-user")
            self.assertIsNotNone(activated["confirmed_at"])
            self.assertEqual(
                datetime.fromisoformat(activated["confirmed_at"]).utcoffset(),
                timedelta(hours=8),
            )
            self.assertEqual(old_active.status, "superseded")
            self.assertEqual(other_active.status, "active")

            with self.assertRaises(ValueError):
                await self.service.confirm(db, target_id, "local-user")
            with self.assertRaises(ValueError):
                await self.service.confirm(db, old_active_id, "local-user")
            with self.assertRaises(ValueError):
                await self.service.confirm(db, other_active_id, "")

    async def test_confirm_rejects_non_ready_or_stale_plan_quality(self):
        cases = [
            ("degraded", _snapshot(quality_status="degraded")),
            ("stale", _snapshot(stale=True)),
        ]
        async with self.Session() as db:
            for case, snapshot in cases:
                with self.subTest(case=case):
                    generated = await self._generate(
                        db,
                        [_evaluation(f"leader-{case}", "000001")],
                        snapshot=snapshot,
                    )
                    with self.assertRaisesRegex(
                        InvalidTransitionError,
                        "data quality",
                    ):
                        await self.service.confirm(
                            db,
                            generated["id"],
                            "local-user",
                        )
                    await db.rollback()

    async def test_confirm_rolls_back_all_status_changes_when_commit_fails(self):
        async with self.Session() as db:
            old_active = TradingPlanVersion(
                source_trade_date=SOURCE_DATE,
                target_trade_date=TARGET_DATE,
                stage="preclose",
                version_no=1,
                status="active",
                data_quality_json={"status": "ready"},
                input_hash="old",
            )
            target = TradingPlanVersion(
                source_trade_date=SOURCE_DATE,
                target_trade_date=TARGET_DATE,
                stage="after_close",
                version_no=1,
                status="draft",
                data_quality_json={"status": "ready"},
                input_hash="target",
            )
            db.add_all([old_active, target])
            await db.commit()
            old_active_id = old_active.id
            target_id = target.id
            original_rollback = db.rollback
            with patch.object(
                db,
                "commit",
                AsyncMock(side_effect=RuntimeError("commit failed")),
            ), patch.object(
                db,
                "rollback",
                AsyncMock(wraps=original_rollback),
            ) as rollback:
                with self.assertRaisesRegex(RuntimeError, "commit failed"):
                    await self.service.confirm(db, target.id, "local-user")
                rollback.assert_awaited_once()

        async with self.Session() as db:
            persisted_old = await db.get(TradingPlanVersion, old_active_id)
            persisted_target = await db.get(TradingPlanVersion, target_id)
            self.assertEqual(persisted_old.status, "active")
            self.assertEqual(persisted_target.status, "draft")
            self.assertIsNone(persisted_target.confirmed_at)

    async def test_confirm_does_not_refresh_after_successful_commit(self):
        async with self.Session() as db:
            generated = await self._generate(
                db,
                [_evaluation("leader", "000001")],
            )
            with patch.object(
                db,
                "refresh",
                AsyncMock(side_effect=RuntimeError("post-commit refresh secret")),
            ):
                result = await self.service.confirm(
                    db,
                    generated["id"],
                    "local-user",
                )

        self.assertEqual(result["status"], "active")
        self.assertEqual(result["confirmed_by"], "local-user")

    async def test_cancel_does_not_refresh_after_successful_commit(self):
        async with self.Session() as db:
            generated = await self._generate(
                db,
                [_evaluation("leader", "000001")],
            )
            with patch.object(
                db,
                "refresh",
                AsyncMock(side_effect=RuntimeError("post-commit refresh secret")),
            ):
                result = await self.service.cancel(db, generated["id"])

        self.assertEqual(result["status"], "expired")

    async def test_serialize_restores_china_offset_after_sqlite_round_trip(self):
        async with self.Session() as db:
            generated = await self._generate(
                db,
                [_evaluation("leader", "000001")],
            )
            await self.service.confirm(db, generated["id"], "local-user")
            plan_id = generated["id"]

        async with self.Session() as db:
            payload = await self.service.serialize(db, plan_id)

        self.assertTrue(payload["generated_at"].endswith("+08:00"))
        self.assertTrue(payload["confirmed_at"].endswith("+08:00"))

    async def test_concurrent_confirmations_leave_only_one_target_active(self):
        async with self.Session() as db:
            first = TradingPlanVersion(
                source_trade_date=SOURCE_DATE,
                target_trade_date=TARGET_DATE,
                stage="preclose",
                version_no=1,
                status="draft",
                data_quality_json={"status": "ready"},
                input_hash="first",
            )
            second = TradingPlanVersion(
                source_trade_date=SOURCE_DATE,
                target_trade_date=TARGET_DATE,
                stage="after_close",
                version_no=1,
                status="draft",
                data_quality_json={"status": "ready"},
                input_hash="second",
            )
            db.add_all([first, second])
            await db.commit()
            first_id, second_id = first.id, second.id

        async def confirm(plan_id, user):
            async with self.Session() as session:
                return await self.service.confirm(session, plan_id, user)

        results = await asyncio.gather(
            confirm(first_id, "first-user"),
            confirm(second_id, "second-user"),
            return_exceptions=True,
        )

        async with self.Session() as db:
            plans = (
                await db.scalars(
                    select(TradingPlanVersion).where(
                        TradingPlanVersion.target_trade_date == TARGET_DATE
                    )
                )
            ).all()
        self.assertEqual(sum(row.status == "active" for row in plans), 1)
        self.assertEqual(sum(row.status == "draft" for row in plans), 1)
        self.assertEqual(
            sum(isinstance(result, InvalidTransitionError) for result in results),
            1,
        )

    async def test_file_sqlite_different_drafts_cannot_replace_same_observed_active(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as directory:
            database_path = Path(directory) / "confirm-drafts.db"
            url = f"sqlite+aiosqlite:///{database_path.as_posix()}"
            first_engine = create_async_engine(url, connect_args={"timeout": 5})
            second_engine = create_async_engine(url, connect_args={"timeout": 5})
            FirstSession = async_sessionmaker(first_engine, expire_on_commit=False)
            SecondSession = async_sessionmaker(second_engine, expire_on_commit=False)
            try:
                async with first_engine.begin() as connection:
                    await connection.run_sync(Base.metadata.create_all)
                async with FirstSession() as setup:
                    old_active = TradingPlanVersion(
                        source_trade_date=SOURCE_DATE,
                        target_trade_date=TARGET_DATE,
                        stage="preclose",
                        version_no=1,
                        status="active",
                        data_quality_json={"status": "ready"},
                        input_hash="old-active",
                    )
                    first_draft = TradingPlanVersion(
                        source_trade_date=SOURCE_DATE,
                        target_trade_date=TARGET_DATE,
                        stage="after_close",
                        version_no=1,
                        status="draft",
                        data_quality_json={"status": "ready"},
                        input_hash="first-draft",
                    )
                    second_draft = TradingPlanVersion(
                        source_trade_date=SOURCE_DATE,
                        target_trade_date=TARGET_DATE,
                        stage="overnight",
                        version_no=1,
                        status="draft",
                        data_quality_json={"status": "ready"},
                        input_hash="second-draft",
                    )
                    setup.add_all([old_active, first_draft, second_draft])
                    await setup.commit()
                    old_id = old_active.id
                    first_id = first_draft.id
                    second_id = second_draft.id

                async def confirm(session_factory, plan_id, user):
                    async with session_factory() as session:
                        self.assertEqual(
                            (await session.get(TradingPlanVersion, plan_id)).status,
                            "draft",
                        )
                        return await TradingPlanService().confirm(
                            session,
                            plan_id,
                            user,
                        )

                results = await asyncio.gather(
                    confirm(FirstSession, first_id, "worker-one"),
                    confirm(SecondSession, second_id, "worker-two"),
                    return_exceptions=True,
                )
                self.assertEqual(
                    sum(
                        isinstance(result, dict)
                        for result in results
                    ),
                    1,
                )
                self.assertEqual(
                    sum(
                        isinstance(result, InvalidTransitionError)
                        for result in results
                    ),
                    1,
                )

                async with FirstSession() as verify:
                    winner_id = next(
                        result["id"]
                        for result in results
                        if isinstance(result, dict)
                    )
                    loser_id = second_id if winner_id == first_id else first_id
                    self.assertEqual(
                        (await verify.get(TradingPlanVersion, old_id)).status,
                        "superseded",
                    )
                    self.assertEqual(
                        (await verify.get(TradingPlanVersion, winner_id)).status,
                        "active",
                    )
                    self.assertEqual(
                        (await verify.get(TradingPlanVersion, loser_id)).status,
                        "draft",
                    )
            finally:
                await first_engine.dispose()
                await second_engine.dispose()

    async def test_file_sqlite_confirm_cancel_cas_has_one_winner_in_both_orders(self):
        async def run_order(cancel_first: bool):
            with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as directory:
                database_path = Path(directory) / "confirm-cancel.db"
                url = f"sqlite+aiosqlite:///{database_path.as_posix()}"
                first_engine = create_async_engine(url, connect_args={"timeout": 5})
                second_engine = create_async_engine(url, connect_args={"timeout": 5})
                FirstSession = async_sessionmaker(
                    first_engine,
                    expire_on_commit=False,
                )
                SecondSession = async_sessionmaker(
                    second_engine,
                    expire_on_commit=False,
                )
                try:
                    async with first_engine.begin() as connection:
                        await connection.run_sync(Base.metadata.create_all)
                    async with FirstSession() as setup:
                        existing = TradingPlanVersion(
                            source_trade_date=SOURCE_DATE,
                            target_trade_date=TARGET_DATE,
                            stage="preclose",
                            version_no=1,
                            status="active",
                            data_quality_json={"status": "ready"},
                            input_hash="existing-active",
                        )
                        selected = TradingPlanVersion(
                            source_trade_date=SOURCE_DATE,
                            target_trade_date=TARGET_DATE,
                            stage="after_close",
                            version_no=1,
                            status="draft",
                            data_quality_json={"status": "ready"},
                            input_hash="selected-draft",
                        )
                        setup.add_all([existing, selected])
                        await setup.commit()
                        existing_id, selected_id = existing.id, selected.id

                    async with FirstSession() as first, SecondSession() as second:
                        stale_first = await first.get(
                            TradingPlanVersion,
                            selected_id,
                        )
                        stale_second = await second.get(
                            TradingPlanVersion,
                            selected_id,
                        )
                        self.assertEqual(stale_first.status, "draft")
                        self.assertEqual(stale_second.status, "draft")
                        if cancel_first:
                            winner = await TradingPlanService().cancel(
                                first,
                                selected_id,
                            )
                            with self.assertRaises(InvalidTransitionError):
                                await TradingPlanService().confirm(
                                    second,
                                    selected_id,
                                    "stale-confirm",
                                )
                            self.assertEqual(winner["status"], "expired")
                        else:
                            winner = await TradingPlanService().confirm(
                                first,
                                selected_id,
                                "winning-confirm",
                            )
                            with self.assertRaises(InvalidTransitionError):
                                await TradingPlanService().cancel(
                                    second,
                                    selected_id,
                                )
                            self.assertEqual(winner["status"], "active")

                    async with FirstSession() as verify:
                        existing = await verify.get(
                            TradingPlanVersion,
                            existing_id,
                        )
                        selected = await verify.get(
                            TradingPlanVersion,
                            selected_id,
                        )
                        if cancel_first:
                            self.assertEqual(existing.status, "active")
                            self.assertEqual(selected.status, "expired")
                        else:
                            self.assertEqual(existing.status, "superseded")
                            self.assertEqual(selected.status, "active")
                finally:
                    await first_engine.dispose()
                    await second_engine.dispose()

        await run_order(cancel_first=True)
        await run_order(cancel_first=False)

    async def test_database_unique_index_protects_active_target_without_process_lock(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as directory:
            database_path = Path(directory) / "plans.db"
            url = f"sqlite+aiosqlite:///{database_path.as_posix()}"
            first_engine = create_async_engine(url, connect_args={"timeout": 5})
            second_engine = create_async_engine(url, connect_args={"timeout": 5})
            FirstSession = async_sessionmaker(first_engine, expire_on_commit=False)
            SecondSession = async_sessionmaker(second_engine, expire_on_commit=False)
            try:
                async with first_engine.begin() as connection:
                    await connection.run_sync(Base.metadata.create_all)
                async with FirstSession() as db:
                    first = TradingPlanVersion(
                        source_trade_date=SOURCE_DATE,
                        target_trade_date=TARGET_DATE,
                        stage="preclose",
                        version_no=1,
                        status="draft",
                        data_quality_json={"status": "ready"},
                        input_hash="first-engine",
                    )
                    second = TradingPlanVersion(
                        source_trade_date=SOURCE_DATE,
                        target_trade_date=TARGET_DATE,
                        stage="after_close",
                        version_no=1,
                        status="draft",
                        data_quality_json={"status": "ready"},
                        input_hash="second-engine",
                    )
                    db.add_all([first, second])
                    await db.commit()
                    first_id, second_id = first.id, second.id

                async def confirm(session_factory, plan_id, user):
                    async with session_factory() as db:
                        return await TradingPlanService().confirm(db, plan_id, user)

                results = await asyncio.gather(
                    confirm(FirstSession, first_id, "worker-one"),
                    confirm(SecondSession, second_id, "worker-two"),
                    return_exceptions=True,
                )
                async with FirstSession() as db:
                    plans = (
                        await db.scalars(
                            select(TradingPlanVersion).where(
                                TradingPlanVersion.target_trade_date == TARGET_DATE
                            )
                        )
                    ).all()
                self.assertEqual(sum(row.status == "active" for row in plans), 1)
                self.assertEqual(sum(row.status == "draft" for row in plans), 1)
                self.assertEqual(
                    sum(
                        isinstance(result, dict)
                        for result in results
                    ),
                    1,
                )
                self.assertEqual(
                    sum(
                        isinstance(result, InvalidTransitionError)
                        for result in results
                    ),
                    1,
                )
            finally:
                await first_engine.dispose()
                await second_engine.dispose()

    async def test_same_revision_is_idempotent_across_independent_engines(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as directory:
            database_path = Path(directory) / "revision-idempotency.db"
            url = f"sqlite+aiosqlite:///{database_path.as_posix()}"
            first_engine = create_async_engine(url, connect_args={"timeout": 5})
            second_engine = create_async_engine(url, connect_args={"timeout": 5})
            FirstSession = async_sessionmaker(first_engine, expire_on_commit=False)
            SecondSession = async_sessionmaker(second_engine, expire_on_commit=False)
            first_allocated = asyncio.Event()
            release_first = asyncio.Event()
            second_allocated = asyncio.Event()

            class PausingService(TradingPlanService):
                async def _next_version_no(self, db, target_trade_date, stage):
                    version_no = await super()._next_version_no(
                        db,
                        target_trade_date,
                        stage,
                    )
                    first_allocated.set()
                    await asyncio.wait_for(release_first.wait(), timeout=5)
                    return version_no

            class ObservingService(TradingPlanService):
                async def _next_version_no(self, db, target_trade_date, stage):
                    version_no = await super()._next_version_no(
                        db,
                        target_trade_date,
                        stage,
                    )
                    second_allocated.set()
                    return version_no

            try:
                async with first_engine.begin() as connection:
                    await connection.run_sync(Base.metadata.create_all)
                async with FirstSession() as setup:
                    parent = await TradingPlanService().generate(
                        setup,
                        _snapshot(),
                        [_evaluation("leader", "000001")],
                        stock_names={"000001": "股票1"},
                        rule_snapshot=_rule_snapshot("leader"),
                    )
                parent_id = parent["id"]
                changes = {
                    "change_note": "同一跨进程修订",
                    "candidate_overrides": [
                        {
                            "candidate_id": parent["candidates"][0]["id"],
                            "manual_note": "只创建一次",
                        }
                    ],
                }

                async def revise(service, session_factory):
                    async with session_factory() as session:
                        return await service.revise(session, parent_id, changes)

                first_task = asyncio.create_task(
                    revise(PausingService(), FirstSession)
                )
                await asyncio.wait_for(first_allocated.wait(), timeout=5)
                second_task = asyncio.create_task(
                    revise(ObservingService(), SecondSession)
                )
                try:
                    waiter = asyncio.create_task(second_allocated.wait())
                    reached, _pending = await asyncio.wait(
                        {waiter},
                        timeout=0.2,
                    )
                    self.assertFalse(
                        reached,
                        "second engine allocated before the first transaction committed",
                    )
                finally:
                    release_first.set()
                first, second = await asyncio.gather(first_task, second_task)

                self.assertEqual(first["id"], second["id"])
                async with FirstSession() as verify:
                    children = (
                        await verify.scalars(
                            select(TradingPlanVersion).where(
                                TradingPlanVersion.parent_plan_version_id
                                == parent_id
                            )
                        )
                    ).all()
                self.assertEqual(len(children), 1)
                self.assertEqual(children[0].input_hash, first["input_hash"])
            finally:
                await first_engine.dispose()
                await second_engine.dispose()

    async def test_reused_revision_commits_lock_before_returning_detached_payload(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as directory:
            database_path = Path(directory) / "revision-lock-release.db"
            url = f"sqlite+aiosqlite:///{database_path.as_posix()}"
            first_engine = create_async_engine(url, connect_args={"timeout": 1})
            second_engine = create_async_engine(url, connect_args={"timeout": 1})
            FirstSession = async_sessionmaker(first_engine, expire_on_commit=False)
            SecondSession = async_sessionmaker(second_engine, expire_on_commit=False)
            try:
                async with first_engine.begin() as connection:
                    await connection.run_sync(Base.metadata.create_all)
                async with FirstSession() as setup:
                    parent = await TradingPlanService().generate(
                        setup,
                        _snapshot(),
                        [_evaluation("leader", "000001")],
                        stock_names={"000001": "股票1"},
                        rule_snapshot=_rule_snapshot("leader"),
                    )
                parent_id = parent["id"]
                first_changes = {"change_note": "first child"}
                async with FirstSession() as setup:
                    child = await TradingPlanService().revise(
                        setup,
                        parent_id,
                        first_changes,
                    )
                    parent_row = await setup.get(TradingPlanVersion, parent_id)
                    parent_audit = (
                        parent_row.status,
                        parent_row.input_hash,
                        parent_row.generated_at,
                        parent_row.confirmed_at,
                        parent_row.confirmed_by,
                    )
                    await setup.rollback()

                async with FirstSession() as held_open:
                    reused = await TradingPlanService().revise(
                        held_open,
                        parent_id,
                        first_changes,
                    )
                    self.assertFalse(held_open.in_transaction())
                    self.assertEqual(reused["id"], child["id"])
                    self.assertEqual(reused["status"], child["status"])

                    async def create_different_revision():
                        async with SecondSession() as second:
                            return await TradingPlanService().revise(
                                second,
                                parent_id,
                                {"change_note": "different child"},
                            )

                    different = await asyncio.wait_for(
                        create_different_revision(),
                        timeout=2,
                    )
                    self.assertNotEqual(different["id"], child["id"])

                async with SecondSession() as verify:
                    parent_row = await verify.get(TradingPlanVersion, parent_id)
                    self.assertEqual(
                        (
                            parent_row.status,
                            parent_row.input_hash,
                            parent_row.generated_at,
                            parent_row.confirmed_at,
                            parent_row.confirmed_by,
                        ),
                        parent_audit,
                    )
            finally:
                await first_engine.dispose()
                await second_engine.dispose()

    async def test_reused_revision_commit_failure_rolls_back_without_returning(self):
        async with self.Session() as setup:
            parent = await self.service.generate(
                setup,
                _snapshot(),
                [_evaluation("leader", "000001")],
                stock_names={"000001": "股票1"},
                rule_snapshot=_rule_snapshot("leader"),
            )
            changes = {"change_note": "existing revision"}
            child = await self.service.revise(setup, parent["id"], changes)

        async with self.Session() as db:
            rollback = AsyncMock(wraps=db.rollback)
            with patch.object(
                db,
                "commit",
                new=AsyncMock(side_effect=RuntimeError("commit failed")),
            ), patch.object(db, "rollback", new=rollback):
                with self.assertRaisesRegex(RuntimeError, "commit failed"):
                    await self.service.revise(db, parent["id"], changes)
            rollback.assert_awaited_once()

        async with self.Session() as verify:
            children = (
                await verify.scalars(
                    select(TradingPlanVersion).where(
                        TradingPlanVersion.parent_plan_version_id == parent["id"]
                    )
                )
            ).all()
            self.assertEqual([row.id for row in children], [child["id"]])

    async def test_revision_integrity_recovery_retries_through_claimed_commit_path(self):
        async with self.Session() as setup:
            parent = await self.service.generate(
                setup,
                _snapshot(),
                [_evaluation("leader", "000001")],
                stock_names={"000001": "股票1"},
                rule_snapshot=_rule_snapshot("leader"),
            )
            changes = {"change_note": "concurrent winning revision"}
            child = await self.service.revise(setup, parent["id"], changes)

        find_calls = 0

        async with self.Session() as db:
            async def find_after_retry(
                session,
                parent_plan_id,
                input_hash,
            ):
                nonlocal find_calls
                find_calls += 1
                if find_calls == 1:
                    return None
                return await session.get(TradingPlanVersion, child["id"])

            forced_conflict = IntegrityError(
                "forced concurrent insert",
                {},
                RuntimeError("duplicate revision"),
            )
            with patch.object(
                self.service,
                "_find_revision_by_hash",
                new=find_after_retry,
            ), patch.object(
                db,
                "flush",
                new=AsyncMock(side_effect=forced_conflict),
            ):
                recovered = await self.service.revise(
                    db,
                    parent["id"],
                    changes,
                )

            self.assertEqual(find_calls, 2)
            self.assertEqual(recovered["id"], child["id"])
            self.assertFalse(db.in_transaction())

    async def test_generate_returns_explicit_validated_payload_marker(self):
        marker_type = getattr(
            serialization_module,
            "ValidatedPlanPayload",
            None,
        )
        self.assertIsNotNone(marker_type)
        async with self.Session() as db:
            payload = await self.service.generate(
                db,
                _snapshot(),
                [_evaluation("leader", "000001")],
                stock_names={"000001": "股票1"},
                rule_snapshot=_rule_snapshot("leader"),
            )
        self.assertIsInstance(payload, marker_type)

    async def test_zero_trial_position_remains_a_valid_formal_candidate(self):
        async with self.Session() as db:
            db.add(
                TradingPlaybookSettings(
                    id=1,
                    trial_position_pct=0.0,
                    confirmed_position_pct=30.0,
                    hard_stop_pct=5.0,
                    max_action_candidates=3,
                )
            )
            await db.commit()
            payload = await self.service.generate(
                db,
                _snapshot(),
                [_evaluation("leader", "000001", risk_level="trial")],
                stock_names={"000001": "股票1"},
                rule_snapshot=_rule_snapshot("leader"),
            )

        self.assertEqual(payload["candidates"][0]["position_reference"], 0.0)
        self.assertEqual(
            payload["candidates"][0]["invalidation_json"]["price_lte"],
            9.5,
        )

    async def test_sqlite_compat_migration_repairs_duplicate_active_before_index(self):
        async with self.engine.begin() as connection:
            await connection.exec_driver_sql(
                "DROP INDEX IF EXISTS uq_trading_plan_one_active_target"
            )
            for stage, version in (("preclose", 1), ("after_close", 1)):
                await connection.exec_driver_sql(
                    "INSERT INTO trading_plan_versions "
                    "(source_trade_date,target_trade_date,stage,version_no,status,input_hash,"
                    "market_state_json,theme_ranking_json,mode_radar_json,rule_snapshot_json,"
                    "risk_settings_json,data_quality_json,change_summary_json,generated_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        SOURCE_DATE.isoformat(),
                        TARGET_DATE.isoformat(),
                        stage,
                        version,
                        "active",
                        f"legacy-{stage}",
                        "{}",
                        "[]",
                        "[]",
                        "[]",
                        "{}",
                        "{}",
                        "{}",
                        AS_OF.isoformat(),
                    ),
                )
            await connection.run_sync(ensure_sqlite_schema_compat)
            indexes = (
                await connection.exec_driver_sql(
                    "PRAGMA index_list(trading_plan_versions)"
                )
            ).all()
            statuses = (
                await connection.exec_driver_sql(
                    "SELECT status FROM trading_plan_versions "
                    "WHERE target_trade_date=? ORDER BY id",
                    (TARGET_DATE.isoformat(),),
                )
            ).all()

        self.assertIn(
            "uq_trading_plan_one_active_target",
            {row[1] for row in indexes},
        )
        self.assertEqual([row[0] for row in statuses], ["superseded", "active"])

    async def test_database_rejects_invalid_persisted_settings(self):
        invalid_settings = [
            {"max_action_candidates": 4},
            {"max_action_candidates": 0},
            {"trial_position_pct": -1},
            {"confirmed_position_pct": 101},
            {"trial_position_pct": 40, "confirmed_position_pct": 30},
            {"hard_stop_pct": 0},
            {"hard_stop_pct": 21},
        ]
        for index, overrides in enumerate(invalid_settings):
            async with self.Session() as db:
                row = await db.get(TradingPlaybookSettings, 1)
                if row is None:
                    row = TradingPlaybookSettings(id=1)
                    db.add(row)
                for key, value in overrides.items():
                    setattr(row, key, value)
                with self.subTest(index=index, overrides=overrides):
                    with self.assertRaises(IntegrityError):
                        await db.commit()
                await db.rollback()

    async def test_settings_can_raise_both_position_limits_atomically(self):
        async with self.Session() as db:
            db.add(
                TradingPlaybookSettings(
                    id=1,
                    trial_position_pct=10,
                    confirmed_position_pct=30,
                )
            )
            await db.commit()

            row = await self.service.update_settings(
                db,
                {
                    "trial_position_pct": 50.0,
                    "confirmed_position_pct": 60.0,
                },
                AS_OF.replace(tzinfo=ZoneInfo("Asia/Shanghai")),
            )

        self.assertEqual(row["trial_position_pct"], 50.0)
        self.assertEqual(row["confirmed_position_pct"], 60.0)

    async def test_update_settings_does_not_query_after_successful_commit(self):
        async with self.Session() as db:
            db.add(
                TradingPlaybookSettings(
                    id=1,
                    trial_position_pct=10,
                    confirmed_position_pct=30,
                    hard_stop_pct=5,
                    max_action_candidates=3,
                    wechat_enabled=False,
                )
            )
            await db.commit()
            original_scalar = db.scalar
            original_commit = db.commit
            committed = False

            async def guarded_scalar(*args, **kwargs):
                if committed:
                    raise RuntimeError("post-commit settings query")
                return await original_scalar(*args, **kwargs)

            async def tracked_commit():
                nonlocal committed
                await original_commit()
                committed = True

            with patch.object(db, "scalar", new=guarded_scalar), patch.object(
                db,
                "commit",
                new=tracked_commit,
            ):
                row = await self.service.update_settings(
                    db,
                    {"trial_position_pct": 20.0},
                    AS_OF.replace(tzinfo=ZoneInfo("Asia/Shanghai")),
                )

        self.assertEqual(row["trial_position_pct"], 20.0)

    async def test_settings_can_lower_both_position_limits_atomically(self):
        async with self.Session() as db:
            db.add(
                TradingPlaybookSettings(
                    id=1,
                    trial_position_pct=50,
                    confirmed_position_pct=60,
                )
            )
            await db.commit()

            row = await self.service.update_settings(
                db,
                {
                    "trial_position_pct": 10.0,
                    "confirmed_position_pct": 20.0,
                },
                AS_OF.replace(tzinfo=ZoneInfo("Asia/Shanghai")),
            )

        self.assertEqual(row["trial_position_pct"], 10.0)
        self.assertEqual(row["confirmed_position_pct"], 20.0)

    async def test_file_sqlite_concurrent_settings_updates_preserve_position_order(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as directory:
            database_path = Path(directory) / "settings.db"
            url = f"sqlite+aiosqlite:///{database_path.as_posix()}"
            first_engine = create_async_engine(url, connect_args={"timeout": 5})
            second_engine = create_async_engine(url, connect_args={"timeout": 5})
            FirstSession = async_sessionmaker(first_engine, expire_on_commit=False)
            SecondSession = async_sessionmaker(second_engine, expire_on_commit=False)
            try:
                async with first_engine.begin() as connection:
                    await connection.run_sync(Base.metadata.create_all)
                async with FirstSession() as db:
                    db.add(
                        TradingPlaybookSettings(
                            id=1,
                            trial_position_pct=10,
                            confirmed_position_pct=30,
                        )
                    )
                    await db.commit()

                async def update(session_factory, changes):
                    async with session_factory() as db:
                        return await TradingPlanService().update_settings(
                            db,
                            changes,
                            AS_OF.replace(tzinfo=ZoneInfo("Asia/Shanghai")),
                        )

                results = await asyncio.gather(
                    update(FirstSession, {"trial_position_pct": 25.0}),
                    update(SecondSession, {"confirmed_position_pct": 20.0}),
                    return_exceptions=True,
                )
                self.assertEqual(
                    sum(isinstance(result, InvalidRequestError) for result in results),
                    1,
                )
                async with FirstSession() as db:
                    row = await db.get(TradingPlaybookSettings, 1)
                    self.assertLessEqual(
                        row.trial_position_pct,
                        row.confirmed_position_pct,
                    )
            finally:
                await first_engine.dispose()
                await second_engine.dispose()

    async def test_file_sqlite_concurrent_two_field_settings_remain_atomic_pairs(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as directory:
            database_path = Path(directory) / "settings-pairs.db"
            url = f"sqlite+aiosqlite:///{database_path.as_posix()}"
            first_engine = create_async_engine(url, connect_args={"timeout": 5})
            second_engine = create_async_engine(url, connect_args={"timeout": 5})
            FirstSession = async_sessionmaker(first_engine, expire_on_commit=False)
            SecondSession = async_sessionmaker(second_engine, expire_on_commit=False)
            try:
                async with first_engine.begin() as connection:
                    await connection.run_sync(Base.metadata.create_all)
                async with FirstSession() as db:
                    db.add(
                        TradingPlaybookSettings(
                            id=1,
                            trial_position_pct=10,
                            confirmed_position_pct=30,
                        )
                    )
                    await db.commit()

                async def update_pair(session_factory, trial, confirmed):
                    async with session_factory() as db:
                        return await TradingPlanService().update_settings(
                            db,
                            {
                                "trial_position_pct": trial,
                                "confirmed_position_pct": confirmed,
                            },
                            AS_OF.replace(tzinfo=ZoneInfo("Asia/Shanghai")),
                        )

                results = await asyncio.gather(
                    update_pair(FirstSession, 40.0, 60.0),
                    update_pair(SecondSession, 15.0, 25.0),
                    return_exceptions=True,
                )
                self.assertFalse(
                    [result for result in results if isinstance(result, Exception)]
                )
                async with FirstSession() as db:
                    row = await db.get(TradingPlaybookSettings, 1)
                    self.assertIn(
                        (row.trial_position_pct, row.confirmed_position_pct),
                        {(40.0, 60.0), (15.0, 25.0)},
                    )
            finally:
                await first_engine.dispose()
                await second_engine.dispose()

    async def test_version_number_conflict_rolls_back_and_retries_cleanly(self):
        async with self.Session() as db:
            db.add(TradingPlaybookSettings(id=1))
            db.add(
                TradingPlanVersion(
                    source_trade_date=SOURCE_DATE,
                    target_trade_date=TARGET_DATE,
                    stage="preclose",
                    version_no=1,
                    status="draft",
                    input_hash="different-input",
                )
            )
            await db.commit()

            with patch.object(
                self.service,
                "_next_version_no",
                AsyncMock(side_effect=[1, 2]),
            ):
                plan = await self._generate(
                    db,
                    [_evaluation("leader", "000001")],
                )

            self.assertEqual(plan["version_no"], 2)
            versions = (
                await db.scalars(
                    select(TradingPlanVersion).order_by(
                        TradingPlanVersion.version_no
                    )
                )
            ).all()
            self.assertEqual([row.version_no for row in versions], [1, 2])

    async def test_settings_insert_conflict_retries_without_an_unbound_hash(self):
        settings_row = TradingPlaybookSettings(
            id=1,
            trial_position_pct=10,
            confirmed_position_pct=30,
            hard_stop_pct=5,
            max_action_candidates=3,
        )
        conflict = IntegrityError("insert settings", {}, RuntimeError("race"))
        async with self.Session() as db:
            with patch.object(
                self.service,
                "_get_or_create_settings",
                AsyncMock(side_effect=[conflict, (settings_row, False)]),
            ):
                plan = await self._generate(
                    db,
                    [_evaluation("leader", "000001")],
                )

        self.assertEqual(plan["version_no"], 1)

    async def test_mixed_timezone_quality_timestamp_is_treated_as_time_unsafe(self):
        aware_quality_time = AS_OF.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
        snapshot = _snapshot(quality_as_of=aware_quality_time)
        evaluations = [
            _evaluation("confirmed", "000001", risk_level="confirmed"),
            _evaluation("trial", "000002", score=99, risk_level="trial"),
        ]
        async with self.Session() as db:
            plan = await self._generate(db, evaluations, snapshot=snapshot)

        self.assertEqual(
            [row["stock_code"] for row in plan["candidates"]],
            ["000002"],
        )

    async def test_non_string_rule_keys_and_malformed_theme_rows_are_rejected(self):
        bad_rule = _rule_snapshot("leader")
        bad_rule[0][1] = "not-json-object-key"
        bad_theme = _snapshot()
        bad_theme.theme_rankings.append({"theme_name": "坏数据", "rank": "first"})
        async with self.Session() as db:
            with self.assertRaises(ValueError):
                await self._generate(
                    db,
                    [_evaluation("leader", "000001")],
                    rule_snapshot=bad_rule,
                )
            await db.rollback()
            with self.assertRaises(ValueError):
                await self._generate(
                    db,
                    [_evaluation("leader", "000001")],
                    snapshot=bad_theme,
                )

    async def test_revision_hash_includes_actual_override_values(self):
        async with self.Session() as db:
            generated = await self._generate(
                db,
                [_evaluation("leader", "000001")],
            )
            parent = await db.get(TradingPlanVersion, generated["id"])
            candidates = await self.service._load_candidates(db, parent.id)
            first = generated["candidates"][0]
            first_changes = self.service._normalize_revision_changes(
                parent,
                candidates,
                {
                    "change_note": "同一说明",
                    "candidate_overrides": [
                        {"candidate_id": first["id"], "manual_note": "甲"}
                    ],
                },
            )
            second_changes = self.service._normalize_revision_changes(
                parent,
                candidates,
                {
                    "change_note": "同一说明",
                    "candidate_overrides": [
                        {"candidate_id": first["id"], "manual_note": "乙"}
                    ],
                },
            )

        first_child = self.service._clone_plan_for_revision(parent, first_changes, 2)
        second_child = self.service._clone_plan_for_revision(parent, second_changes, 2)
        self.assertNotEqual(first_child.input_hash, second_child.input_hash)


class TradingPlanLockManagerTests(unittest.TestCase):
    def test_waiters_share_one_lock_and_entry_is_cleaned_after_last_user(self):
        manager = TradingPlanService()._lock_manager

        async def exercise():
            key = ("lineage", TARGET_DATE)
            holder_entered = asyncio.Event()
            release_holder = asyncio.Event()
            waiter_entered = asyncio.Event()
            release_waiter = asyncio.Event()
            third_entered = asyncio.Event()
            locks = []

            async def holder():
                async with manager.hold(key) as lock:
                    locks.append(lock)
                    holder_entered.set()
                    await release_holder.wait()

            async def waiter():
                async with manager.hold(key) as lock:
                    locks.append(lock)
                    waiter_entered.set()
                    await release_waiter.wait()

            async def third():
                async with manager.hold(key) as lock:
                    locks.append(lock)
                    third_entered.set()

            holder_task = asyncio.create_task(holder())
            await holder_entered.wait()
            waiter_task = asyncio.create_task(waiter())
            await asyncio.sleep(0)
            release_holder.set()
            await waiter_entered.wait()
            third_task = asyncio.create_task(third())
            await asyncio.sleep(0)
            self.assertFalse(third_entered.is_set())
            release_waiter.set()
            await asyncio.gather(holder_task, waiter_task, third_task)

            self.assertIs(locks[0], locks[1])
            self.assertIs(locks[1], locks[2])
            self.assertEqual(manager.entry_count, 0)

        asyncio.run(exercise())

    def test_sequential_event_loops_do_not_reuse_an_asyncio_lock(self):
        manager = TradingPlanService()._lock_manager

        async def acquire_once():
            async with manager.hold(("lineage", TARGET_DATE)) as lock:
                return lock

        first = asyncio.run(acquire_once())
        second = asyncio.run(acquire_once())

        self.assertIsNot(first, second)
        self.assertEqual(manager.entry_count, 0)
