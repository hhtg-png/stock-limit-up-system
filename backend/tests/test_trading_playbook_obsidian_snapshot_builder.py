import hashlib
import json
import unittest
from copy import deepcopy
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.database import Base
from app.models import (
    TradingAlertEvent,
    TradingExecutionReview,
    TradingModeRule,
    TradingPlanCandidate,
    TradingPlanVersion,
    TradingRuleSource,
)
from app.services.trading_playbook.obsidian_snapshot_builder import (
    TradingPlaybookObsidianSnapshotBuilder,
)
from app.services.trading_playbook.obsidian_types import canonical_json_bytes
from app.services.trading_playbook.rule_catalog import (
    RuleCatalog,
    canonical_rule_content_hash,
    canonical_rule_source_refs,
)


CATALOG_PATH = (
    Path(__file__).resolve().parents[1]
    / "app"
    / "data"
    / "trading_playbook_rules_v2.json"
)


def _json_copy(value):
    return json.loads(json.dumps(value, ensure_ascii=False))


class TradingPlaybookObsidianSnapshotBuilderTests(
    unittest.IsolatedAsyncioTestCase
):
    async def asyncSetUp(self):
        self.engine = create_async_engine(
            "sqlite+aiosqlite:///:memory:",
            future=True,
            poolclass=StaticPool,
        )
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        self.session_factory = async_sessionmaker(
            self.engine,
            expire_on_commit=False,
        )
        self.builder = TradingPlaybookObsidianSnapshotBuilder(
            self.session_factory
        )
        self.catalog = RuleCatalog(CATALOG_PATH).load()
        self.declared_source_hashes = {
            source["content_hash"] for source in self.catalog["sources"]
        }
        self.rule_source_hashes = {
            source_ref["source_content_hash"]
            for rule in self.catalog["rules"]
            for source_ref in rule["source_refs"]
        }
        await self._seed_rules()
        await self._seed_plans()

    async def asyncTearDown(self):
        await self.engine.dispose()

    async def _seed_rules(self):
        created_at = datetime(2026, 7, 1, 10, 20, 30, 456789)
        sources = [
            TradingRuleSource(
                id=index,
                source_key=source["source_key"],
                source_path=source["source_path"],
                source_title=source["source_title"],
                content_hash=source["content_hash"],
                ingested_at=created_at,
                status="ready",
            )
            for index, source in enumerate(self.catalog["sources"], start=1)
        ]
        rows = []
        for index, rule in enumerate(self.catalog["rules"], start=1):
            rows.append(
                TradingModeRule(
                    id=index,
                    mode_key=rule["mode_key"],
                    version=2,
                    name=rule["name"],
                    family=rule["family"],
                    style=rule["style"],
                    window=rule["window"],
                    automation_level=rule["automation_level"],
                    description=f"catalog description {index}",
                    prerequisites_json={
                        "requirements": _json_copy(rule["requirements"]),
                        "priority": rule["priority"],
                        "role": rule["role"],
                    },
                    candidate_filters_json=[{"field": "rank", "lte": 3}],
                    entry_trigger_json=_json_copy(rule["entry"]),
                    invalidation_json=_json_copy(rule["invalidation"]),
                    exit_trigger_json=_json_copy(rule["exit"]),
                    risk_guidance_json={"hard_stop_pct": 5},
                    source_refs_json=canonical_rule_source_refs(rule),
                    enabled=True,
                    content_hash=canonical_rule_content_hash(rule),
                    created_at=created_at,
                )
            )

        historical = self.catalog["rules"][0]
        rows.extend(
            [
                TradingModeRule(
                    id=100,
                    mode_key=historical["mode_key"],
                    version=1,
                    name="historical",
                    family="historical",
                    style="historical",
                    window="historical",
                    automation_level="manual",
                    description="ignored old version",
                    prerequisites_json={},
                    candidate_filters_json=[],
                    entry_trigger_json={},
                    invalidation_json={},
                    exit_trigger_json={},
                    risk_guidance_json={},
                    source_refs_json=canonical_rule_source_refs(historical),
                    enabled=True,
                    content_hash="1" * 64,
                    created_at=created_at,
                ),
                TradingModeRule(
                    id=101,
                    mode_key="disabled_only",
                    version=2,
                    name="disabled",
                    family="disabled",
                    style="disabled",
                    window="disabled",
                    automation_level="manual",
                    description="ignored disabled rule",
                    prerequisites_json={},
                    candidate_filters_json=[],
                    entry_trigger_json={},
                    invalidation_json={},
                    exit_trigger_json={},
                    risk_guidance_json={},
                    source_refs_json=canonical_rule_source_refs(historical),
                    enabled=False,
                    content_hash="2" * 64,
                    created_at=created_at,
                ),
            ]
        )
        async with self.session_factory() as session:
            session.add_all(sources)
            session.add_all(rows)
            await session.commit()
        self.source_rows = sources
        self.rule_rows = rows

    def _plan_row(self, plan_id, stage, version_no, confirmed):
        catalog_by_mode = {
            rule["mode_key"]: rule for rule in self.catalog["rules"]
        }

        def snapshot_row(mode_key):
            snapshot_rule = catalog_by_mode[mode_key]
            source_refs = canonical_rule_source_refs(snapshot_rule)
            return {
                "mode_key": mode_key,
                "version": 2,
                "content_hash": canonical_rule_content_hash(snapshot_rule),
                "source_hashes": [
                    {
                        "source_key": source_key,
                        "content_hash": content_hash,
                    }
                    for source_key, content_hash in sorted(
                        {
                            ref["source_key"]: ref["source_content_hash"]
                            for ref in source_refs
                        }.items()
                    )
                ],
                "source_refs": _json_copy(source_refs),
            }

        snapshot_rows = [
            snapshot_row(mode_key)
            for mode_key in (
                "leader_turn_two",
                "trend_core_pullback",
                "first_mover_leader",
            )
        ]
        snapshot_by_mode = {
            row["mode_key"]: row for row in snapshot_rows
        }
        source_hashes_by_key = {
            source["source_key"]: source["content_hash"]
            for source in self.catalog["sources"]
        }
        market_state = {
            "cycle": "divergence",
            "breadth": {"limit_up": 42, "limit_down": 3},
        }
        plan = TradingPlanVersion(
            id=plan_id,
            source_trade_date=date(2026, 7, 14),
            target_trade_date=date(2026, 7, 16),
            stage=stage,
            version_no=version_no,
            parent_plan_version_id=plan_id - 1 if plan_id > 201 else None,
            status="confirmed" if confirmed else "draft",
            market_state_json=market_state,
            theme_ranking_json=[
                {"theme_name": "机器人", "rank": 1, "score": 91.5}
            ],
            mode_radar_json=[
                {
                    "stock_code": "600001",
                    "mode_key": "trend_core_pullback",
                    "rule_version": 2,
                    "rule_hash": snapshot_by_mode["trend_core_pullback"][
                        "content_hash"
                    ],
                },
                {
                    "stock_code": "600002",
                    "mode_key": "leader_turn_two",
                    "rule_version": 2,
                    "rule_hash": snapshot_by_mode["leader_turn_two"][
                        "content_hash"
                    ],
                },
                {
                    "stock_code": "600002",
                    "mode_key": "first_mover_leader",
                    "rule_version": 2,
                    "rule_hash": snapshot_by_mode["first_mover_leader"][
                        "content_hash"
                    ],
                },
            ],
            rule_snapshot_json=snapshot_rows,
            risk_settings_json={
                "trial": 10,
                "confirmed": 30,
                "hard_stop": 5,
                "max_candidates": 3,
                "source_refs": [
                    {
                        "source_key": "03-loss-qa",
                        "excerpt": "候选不超过三只，开仓和退出条件必须预先写清，并执行刚性止损",
                        "source_content_hash": source_hashes_by_key[
                            "03-loss-qa"
                        ],
                    },
                    {
                        "source_key": "04-trading-plan",
                        "excerpt": "交易前形成书面计划，盘后区分信号、执行与结果",
                        "source_content_hash": source_hashes_by_key[
                            "04-trading-plan"
                        ],
                    },
                ],
            },
            data_quality_json={"complete": True, "warnings": []},
            change_summary_json={
                "reason": f"{stage} refresh",
                "changed_fields": ["mode_radar"],
            },
            input_hash=f"{plan_id:064x}",
            generated_at=datetime(2026, 7, 15, 18, 5, 6, 123456),
            confirmed_at=(
                datetime(2026, 7, 15, 18, 30, 45, 654321)
                if confirmed
                else None
            ),
            confirmed_by="reviewer" if confirmed else None,
        )
        return plan, market_state

    def _candidate_rows(self, plan_id):
        shared_overrides = {"entry_trigger": {"price": 12.34}}
        rows = [
            TradingPlanCandidate(
                id=plan_id * 10 + 2,
                plan_version_id=plan_id,
                stock_code="600002",
                stock_name="候选乙",
                action_trade_date=date(2026, 7, 16),
                theme_name="机器人",
                primary_mode_key="leader_turn_two",
                supporting_mode_keys_json=["first_mover_leader"],
                role="leader",
                rank=2,
                recognition_json={"score": 88.5},
                entry_trigger_json={"reference_price": 12.34, "price_gte": 12.3},
                invalidation_json={"price_lte": 11.72},
                exit_trigger_json={"price_gte": 13.5},
                risk_level="confirmed",
                position_reference=30.0,
                evidence_json=[{"kind": "limit_up", "days": 2}],
                manual_overrides_json=shared_overrides,
                status="waiting",
            ),
            TradingPlanCandidate(
                id=plan_id * 10 + 1,
                plan_version_id=plan_id,
                stock_code="600001",
                stock_name="候选甲",
                action_trade_date=date(2026, 7, 14),
                theme_name="算力",
                primary_mode_key="trend_core_pullback",
                supporting_mode_keys_json=[],
                role="trend_core",
                rank=1,
                recognition_json={"score": 86},
                entry_trigger_json={"reference_price": 20.0, "price_gte": 20.0},
                invalidation_json={"price_lte": 19.0},
                exit_trigger_json={"price_gte": 22.0},
                risk_level="trial",
                position_reference=10.0,
                evidence_json=[{"kind": "trend", "days": 5}],
                manual_overrides_json={},
                status="triggered",
            ),
        ]
        return rows, shared_overrides

    async def _seed_plans(self):
        plans = []
        candidates = []
        self.plan_market_states = {}
        self.candidate_override_fixtures = {}
        for offset, stage in enumerate(
            ("preclose", "after_close", "overnight", "auction"),
            start=1,
        ):
            plan_id = 200 + offset
            plan, market_state = self._plan_row(
                plan_id,
                stage,
                version_no=offset,
                confirmed=offset % 2 == 0,
            )
            plan_candidates, override_fixture = self._candidate_rows(plan_id)
            plans.append(plan)
            candidates.extend(plan_candidates)
            self.plan_market_states[plan_id] = market_state
            self.candidate_override_fixtures[plan_id] = override_fixture
        async with self.session_factory() as session:
            session.add_all(plans)
            session.add_all(candidates)
            await session.commit()
        self.plan_rows = {plan.id: plan for plan in plans}
        self.candidate_rows = {
            candidate.id: candidate for candidate in candidates
        }

    async def _assert_plan_field_rejected(
        self,
        field,
        value,
        message,
        *,
        plan_id=202,
    ):
        async with self.session_factory() as session:
            plan = await session.get(TradingPlanVersion, plan_id)
            original = deepcopy(getattr(plan, field))
            setattr(plan, field, deepcopy(value))
            await session.commit()
        try:
            with self.assertRaisesRegex(ValueError, message):
                await self.builder.build_plan_artifact(plan_id)
        finally:
            async with self.session_factory() as session:
                plan = await session.get(TradingPlanVersion, plan_id)
                setattr(plan, field, original)
                await session.commit()

    async def _assert_candidate_field_rejected(
        self,
        field,
        value,
        message,
        *,
        candidate_id=2021,
    ):
        async with self.session_factory() as session:
            candidate = await session.get(TradingPlanCandidate, candidate_id)
            original = deepcopy(getattr(candidate, field))
            setattr(candidate, field, deepcopy(value))
            await session.commit()
        try:
            with self.assertRaisesRegex(ValueError, message):
                await self.builder.build_plan_artifact(202)
        finally:
            async with self.session_factory() as session:
                candidate = await session.get(TradingPlanCandidate, candidate_id)
                setattr(candidate, field, original)
                await session.commit()

    async def test_rule_builder_exports_actual_v2_catalog_deterministically(self):
        with patch(
            "builtins.open",
            side_effect=AssertionError("builder must not read Notes or files"),
        ):
            artifacts = await self.builder.build_rule_artifacts()

        self.assertIsInstance(artifacts, tuple)
        self.assertEqual(len(artifacts), 19)
        mode_keys = [artifact.payload_json()["mode_key"] for artifact in artifacts]
        self.assertEqual(mode_keys, sorted(mode_keys))
        self.assertEqual(len(mode_keys), len(set(mode_keys)))
        self.assertNotIn("disabled_only", mode_keys)
        self.assertTrue(all(artifact.trade_date == date(2026, 7, 1) for artifact in artifacts))
        self.assertEqual(
            {artifact.snapshot_key for artifact in artifacts},
            {f"rule:v2:{mode_key}" for mode_key in mode_keys},
        )
        self.assertEqual(
            {artifact.target_path for artifact in artifacts},
            {
                f"30_TradingPlaybook/Modes/Auto/v2/{mode_key}.md"
                for mode_key in mode_keys
            },
        )
        exported_source_hashes = {
            source_ref["source_content_hash"]
            for artifact in artifacts
            for source_ref in artifact.payload_json()["source_refs"]
        }
        self.assertEqual(exported_source_hashes, self.rule_source_hashes)
        self.assertEqual(len(self.declared_source_hashes), 8)
        self.assertEqual(len(self.rule_source_hashes), 7)
        declared_by_key = {
            source["source_key"]: source["content_hash"]
            for source in self.catalog["sources"]
        }
        self.assertEqual(
            self.declared_source_hashes - self.rule_source_hashes,
            {declared_by_key["03-loss-qa"]},
        )

        first = artifacts[0]
        stored = next(
            row for row in self.rule_rows if row.mode_key == mode_keys[0] and row.version == 2
        )
        payload = first.payload_json()
        self.assertEqual(
            set(payload),
            {
                "type",
                "catalog_version",
                "rule_id",
                "mode_key",
                "rule_version",
                "name",
                "family",
                "style",
                "window",
                "automation_level",
                "description",
                "prerequisites",
                "candidate_filters",
                "entry_trigger",
                "invalidation",
                "exit_trigger",
                "risk_guidance",
                "source_refs",
                "content_hash",
                "enabled",
                "created_at",
                "manual_required",
                "auto_execute",
            },
        )
        self.assertEqual(
            payload,
            {
                "type": "trading_mode_rule",
                "catalog_version": "v2",
                "rule_id": stored.id,
                "mode_key": stored.mode_key,
                "rule_version": stored.version,
                "name": stored.name,
                "family": stored.family,
                "style": stored.style,
                "window": stored.window,
                "automation_level": stored.automation_level,
                "description": stored.description,
                "prerequisites": stored.prerequisites_json,
                "candidate_filters": stored.candidate_filters_json,
                "entry_trigger": stored.entry_trigger_json,
                "invalidation": stored.invalidation_json,
                "exit_trigger": stored.exit_trigger_json,
                "risk_guidance": stored.risk_guidance_json,
                "source_refs": canonical_rule_source_refs(
                    {"source_refs": stored.source_refs_json}
                ),
                "content_hash": stored.content_hash,
                "enabled": True,
                "created_at": "2026-07-01T02:20:30.456789Z",
                "manual_required": True,
                "auto_execute": False,
            },
        )
        self.assertEqual(first.entity_type, "rule")
        self.assertEqual(first.entity_id, stored.id)
        self.assertEqual(first.phase, "catalog")
        self.assertTrue(first.immutable)

    async def test_rule_builder_rejects_noncanonical_catalog_versions(self):
        for catalog_version in ("v0", "v02", "V2", "2", "v-1", "", None):
            with self.subTest(catalog_version=catalog_version):
                with self.assertRaisesRegex(ValueError, "catalog_version"):
                    await self.builder.build_rule_artifacts(
                        catalog_version  # type: ignore[arg-type]
                    )

    async def test_rule_builder_fails_closed_on_corrupt_rule_identity_and_hashes(self):
        valid_ref = _json_copy(self.catalog["rules"][0]["source_refs"])
        corrupt_rows = [
            TradingModeRule(
                id=301,
                mode_key="../unsafe",
                version=3,
                name="unsafe",
                family="test",
                style="test",
                window="test",
                automation_level="manual",
                description="",
                prerequisites_json={},
                candidate_filters_json=[],
                entry_trigger_json={},
                invalidation_json={},
                exit_trigger_json={},
                risk_guidance_json={},
                source_refs_json=valid_ref,
                enabled=True,
                content_hash="a" * 64,
                created_at=datetime(2026, 7, 1, 9),
            ),
            TradingModeRule(
                id=302,
                mode_key="bad_content_hash",
                version=4,
                name="bad hash",
                family="test",
                style="test",
                window="test",
                automation_level="manual",
                description="",
                prerequisites_json={},
                candidate_filters_json=[],
                entry_trigger_json={},
                invalidation_json={},
                exit_trigger_json={},
                risk_guidance_json={},
                source_refs_json=valid_ref,
                enabled=True,
                content_hash="not-sha256",
                created_at=datetime(2026, 7, 1, 9),
            ),
            TradingModeRule(
                id=303,
                mode_key="bad_source_ref",
                version=5,
                name="bad ref",
                family="test",
                style="test",
                window="test",
                automation_level="manual",
                description="",
                prerequisites_json={},
                candidate_filters_json=[],
                entry_trigger_json={},
                invalidation_json={},
                exit_trigger_json={},
                risk_guidance_json={},
                source_refs_json=[
                    {
                        "source_key": "04-trading-plan",
                        "excerpt": "evidence",
                        "source_content_hash": "not-sha256",
                    }
                ],
                enabled=True,
                content_hash="b" * 64,
                created_at=datetime(2026, 7, 1, 9),
            ),
        ]
        async with self.session_factory() as session:
            session.add_all(corrupt_rows)
            await session.commit()

        for version, message in (
            ("v3", "mode_key"),
            ("v4", "content_hash"),
            ("v5", "source_content_hash"),
        ):
            with self.subTest(version=version):
                with self.assertRaisesRegex(ValueError, message):
                    await self.builder.build_rule_artifacts(version)

    async def test_rule_builder_recomputes_valid_looking_content_hash(self):
        async with self.session_factory() as session:
            stored = await session.get(TradingModeRule, 1)
            self.assertIsNotNone(stored)
            stored.content_hash = "f" * 64
            await session.commit()

        with self.assertRaisesRegex(ValueError, "content_hash does not match"):
            await self.builder.build_rule_artifacts("v2")

    async def test_rule_builder_requires_exact_ready_persisted_sources(self):
        source_ref = self.catalog["rules"][0]["source_refs"][0]
        async with self.session_factory() as session:
            await session.execute(
                delete(TradingRuleSource).where(
                    TradingRuleSource.source_key == source_ref["source_key"],
                    TradingRuleSource.content_hash
                    == source_ref["source_content_hash"],
                )
            )
            session.add(
                TradingRuleSource(
                    source_key=source_ref["source_key"],
                    source_path="wrong-version.txt",
                    source_title="wrong version",
                    content_hash="f" * 64,
                    ingested_at=datetime(2026, 7, 1, 11),
                    status="ready",
                )
            )
            await session.commit()

        with self.assertRaisesRegex(ValueError, "persisted ready source"):
            await self.builder.build_rule_artifacts("v2")

    async def test_plan_builder_maps_all_stages_and_preserves_distinct_dates(self):
        expected_stages = ("preclose", "after_close", "overnight", "auction")
        for offset, stage in enumerate(expected_stages, start=1):
            plan_id = 200 + offset
            with self.subTest(stage=stage):
                artifact = await self.builder.build_plan_artifact(plan_id)
                payload = artifact.payload_json()

                self.assertEqual(
                    artifact.target_path,
                    f"30_TradingPlaybook/Daily/Auto/2026/2026-07-16/{stage}-v{offset}.md",
                )
                self.assertEqual(artifact.snapshot_key, f"plan:{plan_id}")
                self.assertEqual(artifact.trade_date, date(2026, 7, 16))
                self.assertEqual(artifact.entity_type, "plan")
                self.assertEqual(artifact.entity_id, plan_id)
                self.assertEqual(artifact.phase, stage)
                self.assertTrue(artifact.immutable)
                self.assertEqual(payload["source_trade_date"], "2026-07-14")
                self.assertEqual(payload["target_trade_date"], "2026-07-16")
                self.assertEqual(
                    [row["action_trade_date"] for row in payload["candidates"]],
                    ["2026-07-14", "2026-07-16"],
                )
                self.assertEqual(
                    [row["candidate_id"] for row in payload["candidates"]],
                    [plan_id * 10 + 1, plan_id * 10 + 2],
                )
                self.assertLessEqual(len(payload["candidates"]), 3)
                self.assertEqual(payload["manual_required"], True)
                self.assertEqual(payload["auto_execute"], False)
                self.assertEqual(
                    payload["generated_at"],
                    "2026-07-15T10:05:06.123456Z",
                )
                if offset % 2 == 0:
                    self.assertEqual(
                        payload["confirmed_at"],
                        "2026-07-15T10:30:45.654321Z",
                    )
                    self.assertEqual(payload["confirmed_by"], "reviewer")
                else:
                    self.assertIsNone(payload["confirmed_at"])
                    self.assertIsNone(payload["confirmed_by"])

    async def test_plan_payload_uses_only_explicit_plan_and_candidate_fields(self):
        artifact = await self.builder.build_plan_artifact(202)
        payload = artifact.payload_json()
        self.assertEqual(
            set(payload),
            {
                "type",
                "plan_version_id",
                "version_no",
                "stage",
                "status",
                "source_trade_date",
                "target_trade_date",
                "parent_plan_version_id",
                "market_state",
                "theme_ranking",
                "mode_radar",
                "rule_snapshot",
                "data_quality",
                "risk_settings",
                "change_summary",
                "input_hash",
                "generated_at",
                "confirmed_at",
                "confirmed_by",
                "candidates",
                "manual_required",
                "auto_execute",
            },
        )
        candidate = payload["candidates"][0]
        self.assertEqual(
            set(candidate),
            {
                "candidate_id",
                "plan_version_id",
                "stock_code",
                "stock_name",
                "action_trade_date",
                "theme_name",
                "primary_mode_key",
                "supporting_mode_keys",
                "role",
                "rank",
                "recognition",
                "entry_trigger",
                "invalidation",
                "exit_trigger",
                "risk_level",
                "position_reference",
                "evidence",
                "manual_overrides",
                "status",
            },
        )
        self.assertEqual(payload["plan_version_id"], 202)
        self.assertEqual(payload["parent_plan_version_id"], 201)
        self.assertEqual(payload["version_no"], 2)
        self.assertEqual(payload["change_summary"]["reason"], "after_close refresh")
        self.assertEqual(
            payload["rule_snapshot"][0]["source_refs"][0][
                "source_content_hash"
            ],
            next(
                rule
                for rule in self.catalog["rules"]
                if rule["mode_key"] == "leader_turn_two"
            )["source_refs"][0]["source_content_hash"],
        )
        self.assertEqual(
            payload["candidates"][1]["manual_overrides"],
            {"entry_trigger": {"price": 12.34}},
        )
        self.assertEqual(
            [
                source_ref["source_key"]
                for source_ref in payload["risk_settings"]["source_refs"]
            ],
            ["03-loss-qa", "04-trading-plan"],
        )
        self.assertEqual(
            {
                source_ref["source_content_hash"]
                for source_ref in payload["risk_settings"]["source_refs"]
            },
            {
                source["content_hash"]
                for source in self.catalog["sources"]
                if source["source_key"] in {"03-loss-qa", "04-trading-plan"}
            },
        )

    async def test_plan_artifact_is_deterministic_and_owns_orm_json(self):
        first = await self.builder.build_plan_artifact(202)
        second = await self.builder.build_plan_artifact(202)
        expected_bytes = canonical_json_bytes(first.payload)  # type: ignore[arg-type]

        self.assertEqual(first.payload_json(), second.payload_json())
        self.assertEqual(first.source_hash, second.source_hash)
        self.assertEqual(
            canonical_json_bytes(second.payload),  # type: ignore[arg-type]
            expected_bytes,
        )
        self.assertEqual(
            first.source_hash,
            hashlib.sha256(expected_bytes).hexdigest(),
        )

        self.plan_market_states[202]["cycle"] = "caller-mutated"
        self.plan_rows[202].parent_plan_version_id = None
        self.plan_rows[202].version_no = 99
        self.plan_rows[202].change_summary_json["reason"] = "caller-mutated"
        self.plan_rows[202].rule_snapshot_json[0]["content_hash"] = "f" * 64
        self.candidate_override_fixtures[202]["entry_trigger"]["price"] = 99
        candidate = self.candidate_rows[2022]
        candidate.manual_overrides_json["entry_trigger"]["price"] = 88

        self.assertEqual(first.payload_json(), second.payload_json())
        self.assertEqual(first.source_hash, second.source_hash)
        self.assertEqual(first.payload_json()["parent_plan_version_id"], 201)
        self.assertEqual(first.payload_json()["version_no"], 2)
        self.assertEqual(
            first.payload_json()["change_summary"]["reason"],
            "after_close refresh",
        )
        with self.assertRaises(TypeError):
            first.payload["status"] = "mutated"  # type: ignore[index]

    async def test_plan_builder_rejects_missing_nonpositive_and_overflow(self):
        with self.assertRaisesRegex(ValueError, "plan_version_id"):
            await self.builder.build_plan_artifact(0)
        with self.assertRaisesRegex(ValueError, "plan_version_id"):
            await self.builder.build_plan_artifact(True)  # type: ignore[arg-type]
        with self.assertRaisesRegex(LookupError, "999999"):
            await self.builder.build_plan_artifact(999999)

        extra_rows = []
        for index in range(3, 5):
            extra_rows.append(
                TradingPlanCandidate(
                    id=2010 + index,
                    plan_version_id=201,
                    stock_code=f"60000{index}",
                    stock_name=f"overflow {index}",
                    action_trade_date=date(2026, 7, 16),
                    theme_name="overflow",
                    primary_mode_key=f"overflow_mode_{index}",
                    supporting_mode_keys_json=[],
                    role="overflow",
                    rank=index,
                    recognition_json={},
                    entry_trigger_json={},
                    invalidation_json={},
                    exit_trigger_json={},
                    risk_level="high",
                    position_reference=0,
                    evidence_json=[],
                    manual_overrides_json={},
                    status="waiting",
                )
            )
        async with self.session_factory() as session:
            session.add_all(extra_rows)
            await session.commit()

        with self.assertRaisesRegex(ValueError, "more than 3 candidates"):
            await self.builder.build_plan_artifact(201)

    async def test_plan_builder_rejects_invalid_stage_and_unsafe_path_version(self):
        invalid_stage, _ = self._plan_row(401, "../preclose", 1, False)
        invalid_version, _ = self._plan_row(402, "preclose", 0, False)
        async with self.session_factory() as session:
            session.add_all([invalid_stage, invalid_version])
            await session.commit()

        with self.assertRaisesRegex(ValueError, "stage"):
            await self.builder.build_plan_artifact(401)
        with self.assertRaisesRegex(ValueError, "version_no"):
            await self.builder.build_plan_artifact(402)

    async def test_plan_builder_rejects_corrupt_plan_scalars_and_json_roots(self):
        for field, value, message in (
            ("status", "publishing", "status"),
            ("input_hash", "f" * 63, "input_hash"),
            ("market_state_json", None, "market_state"),
            ("theme_ranking_json", {}, "theme_ranking"),
            ("mode_radar_json", {}, "mode_radar"),
            ("rule_snapshot_json", None, "rule_snapshot"),
            ("risk_settings_json", [], "risk_settings"),
            ("data_quality_json", [], "data_quality"),
            ("change_summary_json", [], "change_summary"),
        ):
            with self.subTest(field=field):
                await self._assert_plan_field_rejected(
                    field,
                    value,
                    message,
                )

        await self._assert_plan_field_rejected(
            "source_trade_date",
            date(2026, 7, 17),
            "trade-date order",
        )

    async def test_plan_builder_rejects_corrupt_candidate_semantics_and_roots(self):
        for field, value, message, candidate_id in (
            ("rank", 0, "rank", 2021),
            ("rank", 1, "ranks are not unique", 2022),
            ("stock_code", "../bad", "stock code", 2021),
            ("primary_mode_key", "../bad", "primary mode", 2021),
            ("action_trade_date", date(2026, 7, 15), "action trade date", 2021),
            ("supporting_mode_keys_json", None, "supporting_mode_keys", 2021),
            (
                "supporting_mode_keys_json",
                ["../bad"],
                "supporting_mode_keys",
                2021,
            ),
            ("recognition_json", [], "recognition", 2021),
            ("entry_trigger_json", None, "entry trigger", 2021),
            (
                "entry_trigger_json",
                {"price_gte": 20.0},
                "reference price is missing",
                2021,
            ),
            ("invalidation_json", [], "invalidation", 2021),
            (
                "invalidation_json",
                {"price_lte": 18.5},
                "hard stop does not match",
                2021,
            ),
            ("exit_trigger_json", [], "exit trigger", 2021),
            (
                "exit_trigger_json",
                {"change_pct_lte": 1},
                "exit percentage is unsafe",
                2021,
            ),
            ("evidence_json", {}, "evidence", 2021),
            ("manual_overrides_json", [], "manual_overrides", 2021),
            ("risk_level", "medium", "risk level", 2021),
            ("position_reference", -1, "position", 2021),
            ("position_reference", 30, "does not match", 2021),
            ("status", "observing", "candidate status", 2021),
        ):
            with self.subTest(field=field, value=value):
                await self._assert_candidate_field_rejected(
                    field,
                    value,
                    message,
                    candidate_id=candidate_id,
                )

        with self.assertRaisesRegex(ValueError, "ownership"):
            self.builder._validate_plan_data(
                self.plan_rows[202],
                [self.candidate_rows[2011]],
            )

    async def test_plan_builder_requires_candidate_modes_in_same_stock_radar(self):
        await self._assert_candidate_field_rejected(
            "primary_mode_key",
            "new_theme_high_volatility",
            "mode_radar",
            candidate_id=2021,
        )
        await self._assert_candidate_field_rejected(
            "supporting_mode_keys_json",
            ["leader_turn_two"],
            "mode_radar",
            candidate_id=2021,
        )

        duplicate_radar = deepcopy(self.plan_rows[202].mode_radar_json)
        duplicate_radar.append(deepcopy(duplicate_radar[0]))
        await self._assert_plan_field_rejected(
            "mode_radar_json",
            duplicate_radar,
            "duplicate",
        )

    async def test_plan_builder_validates_rule_snapshot_and_radar_provenance(self):
        fabricated_snapshot = deepcopy(
            self.plan_rows[202].rule_snapshot_json
        )
        fabricated_snapshot[0]["content_hash"] = "f" * 64
        await self._assert_plan_field_rejected(
            "rule_snapshot_json",
            fabricated_snapshot,
            "persisted rule",
        )

        wrong_rule = self.catalog["rules"][0]
        wrong_sources_snapshot = deepcopy(
            self.plan_rows[202].rule_snapshot_json
        )
        wrong_sources_snapshot[0]["source_refs"] = canonical_rule_source_refs(
            wrong_rule
        )
        wrong_sources_snapshot[0]["source_hashes"] = [
            {
                "source_key": source_key,
                "content_hash": content_hash,
            }
            for source_key, content_hash in sorted(
                {
                    ref["source_key"]: ref["source_content_hash"]
                    for ref in canonical_rule_source_refs(wrong_rule)
                }.items()
            )
        ]
        await self._assert_plan_field_rejected(
            "rule_snapshot_json",
            wrong_sources_snapshot,
            "source_refs",
        )

        fabricated_radar = deepcopy(self.plan_rows[202].mode_radar_json)
        fabricated_radar[0]["rule_hash"] = "e" * 64
        await self._assert_plan_field_rejected(
            "mode_radar_json",
            fabricated_radar,
            "mode_radar.*rule_snapshot",
        )

    async def test_plan_builder_validates_risk_setting_source_provenance(self):
        fabricated_risk = deepcopy(self.plan_rows[202].risk_settings_json)
        fabricated_risk["source_refs"][0]["source_content_hash"] = "f" * 64
        await self._assert_plan_field_rejected(
            "risk_settings_json",
            fabricated_risk,
            "risk_settings.*persisted ready source",
        )

        source_row = next(
            row for row in self.source_rows if row.source_key == "03-loss-qa"
        )
        async with self.session_factory() as session:
            source = await session.get(TradingRuleSource, source_row.id)
            source.status = "missing"
            await session.commit()
        try:
            with self.assertRaisesRegex(
                ValueError,
                "risk_settings.*persisted ready source",
            ):
                await self.builder.build_plan_artifact(202)
        finally:
            async with self.session_factory() as session:
                source = await session.get(TradingRuleSource, source_row.id)
                source.status = "ready"
                await session.commit()

    @staticmethod
    def _review_row(
        review_id=501,
        *,
        plan_version_id=202,
        finalized_at=None,
    ):
        return TradingExecutionReview(
            id=review_id,
            trade_date=date(2026, 7, 16),
            plan_version_id=plan_version_id,
            signal_review_json={
                "candidates": {"600001": {"triggered": True}},
                "alert_audit": {"delivered": 1, "acknowledged": 0},
            },
            manual_execution_json={
                "600001": {"executed": True, "note": "initial"}
            },
            plan_compliance_json={"status": "disciplined"},
            outcome_snapshot_json={"600001": {"close": 21.2}},
            data_quality_json={"status": "ready", "warnings": []},
            generated_at=datetime(2026, 7, 16, 15, 10, 1, 123456),
            finalized_at=finalized_at,
        )

    async def _make_plan_review_relevant(self, plan_id, *, status="active"):
        async with self.session_factory() as session:
            plan = await session.get(TradingPlanVersion, plan_id)
            plan.status = status
            if status == "expired" and plan.confirmed_at is None:
                plan.confirmed_at = datetime(2026, 7, 15, 18, 30)
                plan.confirmed_by = "reviewer"
            await session.commit()

    async def test_review_builder_freezes_initial_and_final_same_row_independently(self):
        await self._make_plan_review_relevant(202)
        async with self.session_factory() as session:
            session.add(self._review_row())
            await session.commit()

        initial = await self.builder.build_review_artifact(
            501,
            phase="initial_review",
        )
        initial_payload = initial.payload_json()
        initial_hash = initial.source_hash

        async with self.session_factory() as session:
            review = await session.get(TradingExecutionReview, 501)
            review.signal_review_json = {
                "candidates": {"600001": {"triggered": False}},
                "alert_audit": {"delivered": 1, "acknowledged": 1},
            }
            review.manual_execution_json = {
                "600001": {"executed": False, "note": "final correction"}
            }
            review.outcome_snapshot_json = {"600001": {"close": 19.8}}
            review.finalized_at = datetime(2026, 7, 16, 15, 30, 2, 654321)
            await session.commit()

        final = await self.builder.build_review_artifact(
            501,
            phase="final_review",
        )

        self.assertEqual(initial.snapshot_key, "review:501:initial")
        self.assertEqual(final.snapshot_key, "review:501:final")
        self.assertEqual(
            initial.target_path,
            "30_TradingPlaybook/Reviews/Auto/2026/2026-07-16/initial-review-202.md",
        )
        self.assertEqual(
            final.target_path,
            "30_TradingPlaybook/Reviews/Auto/2026/2026-07-16/final-review-202.md",
        )
        self.assertTrue(initial.immutable)
        self.assertTrue(final.immutable)
        self.assertNotEqual(initial.source_hash, final.source_hash)
        self.assertEqual(initial.source_hash, initial_hash)
        self.assertEqual(initial.payload_json(), initial_payload)
        self.assertEqual(
            initial.payload_json()["manual_execution"]["600001"]["note"],
            "initial",
        )
        self.assertEqual(
            final.payload_json()["manual_execution"]["600001"]["note"],
            "final correction",
        )
        self.assertIsNone(initial.payload_json()["finalized_at"])
        self.assertEqual(
            final.payload_json()["finalized_at"],
            "2026-07-16T07:30:02.654321Z",
        )
        self.assertEqual(initial.entity_type, "review")
        self.assertEqual(final.phase, "final_review")

    async def test_review_builder_rejects_invalid_identity_phase_state_and_json(self):
        with self.assertRaisesRegex(ValueError, "review_id"):
            await self.builder.build_review_artifact(0, phase="initial_review")
        with self.assertRaisesRegex(LookupError, "999999"):
            await self.builder.build_review_artifact(
                999999,
                phase="initial_review",
            )

        await self._make_plan_review_relevant(202)
        async with self.session_factory() as session:
            session.add(self._review_row())
            await session.commit()
        with self.assertRaisesRegex(ValueError, "phase"):
            await self.builder.build_review_artifact(501, phase="after_close")
        with self.assertRaisesRegex(ValueError, "finalized_at"):
            await self.builder.build_review_artifact(501, phase="final_review")

        async with self.session_factory() as session:
            review = await session.get(TradingExecutionReview, 501)
            review.signal_review_json = []
            await session.commit()
        with self.assertRaisesRegex(ValueError, "signal_review"):
            await self.builder.build_review_artifact(
                501,
                phase="initial_review",
            )

    async def test_review_builder_requires_review_service_plan_relevance(self):
        # confirmed/draft are not historical review targets until activated;
        # this protects against exporting unrelated plan rows.
        async with self.session_factory() as session:
            session.add(self._review_row(511, plan_version_id=202))
            await session.commit()
        with self.assertRaisesRegex(ValueError, "review-relevant status"):
            await self.builder.build_review_artifact(
                511,
                phase="initial_review",
            )

        await self._make_plan_review_relevant(202, status="active")
        async with self.session_factory() as session:
            cross_date = self._review_row(512, plan_version_id=202)
            cross_date.trade_date = date(2026, 7, 15)
            session.add(cross_date)
            await session.commit()
        with self.assertRaisesRegex(ValueError, "review trade date"):
            await self.builder.build_review_artifact(
                512,
                phase="initial_review",
            )

        # Superseded plans and expired confirmed plans remain valid historical
        # review targets when the date matches target or candidate action date.
        await self._make_plan_review_relevant(201, status="superseded")
        async with self.session_factory() as session:
            target_match = self._review_row(513, plan_version_id=201)
            action_match = self._review_row(514, plan_version_id=201)
            action_match.trade_date = date(2026, 7, 14)
            session.add_all([target_match, action_match])
            await session.commit()
        target_artifact = await self.builder.build_review_artifact(
            513,
            phase="initial_review",
        )
        action_artifact = await self.builder.build_review_artifact(
            514,
            phase="initial_review",
        )
        self.assertEqual(target_artifact.trade_date, date(2026, 7, 16))
        self.assertEqual(action_artifact.trade_date, date(2026, 7, 14))

        await self._make_plan_review_relevant(203, status="expired")
        async with self.session_factory() as session:
            expired = self._review_row(515, plan_version_id=203)
            session.add(expired)
            await session.commit()
        artifact = await self.builder.build_review_artifact(
            515,
            phase="initial_review",
        )
        self.assertEqual(artifact.entity_id, 515)

    async def test_alerts_builder_exports_cn_timeline_states_and_excludes_wechat(self):
        plan_market = {
            "source_trade_date": "2026-07-14",
            "target_trade_date": "2026-07-16",
            "stage": "after_close",
            "status": "confirmed",
            "trade_date": "2026-07-16",
            "api_key": "TOP-SECRET-API-KEY",
            "nested": {"password": "never-export-this"},
        }
        action_market = {
            "trade_date": "2026-07-16",
            "stock_code": "600001",
            "mode_key": "trend_core_pullback",
            "condition_version": "condition-v1",
            "occurrence_no": 1,
            "quote": {
                "code": "600001",
                "name": "候选甲",
                "price": 20.1,
                "change_pct": 3.2,
                "sealed": False,
                "open_count": 1,
                "datetime": "2026-07-16T09:04:00+08:00",
                "captured_at": "2026-07-16T09:04:00+08:00",
                "webhook": "https://private.invalid/token",
                "nested": {"secret": "quote-secret"},
            },
            "output": "raw-provider-output",
        }
        alert_specs = (
            (
                601,
                "plan_ready",
                "info",
                datetime(2026, 7, 16, 9, 0),
                None,
                {"status": "delivered", "attempts": 1, "delivered_at": "2026-07-16T09:00:01+08:00", "receipt": {"output": "private"}},
                None,
                plan_market,
            ),
            (
                602,
                "confirmation_required",
                "warning",
                datetime(2026, 7, 16, 9, 1),
                2021,
                {"status": "pending", "attempts": 0},
                None,
                plan_market,
            ),
            (
                603,
                "confirmation_required",
                "warning",
                datetime(2026, 7, 16, 9, 2),
                2022,
                {"status": "delivered", "attempts": 1, "delivered_at": "2026-07-16T09:02:01+08:00"},
                datetime(2026, 7, 16, 9, 3),
                plan_market,
            ),
            (
                604,
                "invalidated",
                "warning",
                datetime(2026, 7, 16, 9, 4),
                2021,
                {"status": "failed", "attempts": 2, "error": "Bearer SECRET-TOKEN", "failed_at": "2026-07-16T09:04:01+08:00"},
                None,
                action_market,
            ),
            (
                605,
                "confirmation_required",
                "warning",
                datetime(2026, 7, 16, 9, 5),
                None,
                {"status": "sending", "attempts": 1, "sending_at": "2026-07-16T09:05:01+08:00", "owner": "secret-owner", "idempotency_key": "private-token"},
                None,
                plan_market,
            ),
            (
                606,
                "plan_ready",
                "info",
                datetime(2026, 7, 16, 9, 6),
                None,
                {"status": "skipped", "attempts": 1, "reason": "password=private", "skipped_at": "2026-07-16T09:06:01+08:00"},
                None,
                plan_market,
            ),
        )
        async with self.session_factory() as session:
            for event_id, event_type, severity, triggered_at, candidate_id, in_app, acknowledged_at, market_facts in alert_specs:
                session.add(
                    TradingAlertEvent(
                        id=event_id,
                        plan_version_id=202,
                        candidate_id=candidate_id,
                        event_type=event_type,
                        severity=severity,
                        dedup_key=f"event:{event_id}",
                        triggered_at=triggered_at,
                        market_snapshot_json=deepcopy(market_facts),
                        message=f"alert {event_id}",
                        channel_status_json={
                            "in_app": in_app,
                            "wechat": {
                                "status": "delivered",
                                "label": "微信发送",
                                "secret": "must-not-export",
                                "config": {"webhook": "private"},
                                "output": "private response",
                            },
                        },
                        acknowledged_at=acknowledged_at,
                    )
                )
            # Same UTC-looking clock value on another CN date must not leak in.
            session.add(
                TradingAlertEvent(
                    id=609,
                    plan_version_id=202,
                    event_type="plan_ready",
                    severity="info",
                    dedup_key="event:609",
                    triggered_at=datetime(2026, 7, 15, 23, 59, 59),
                    market_snapshot_json={},
                    message="previous day",
                    channel_status_json={"in_app": {"status": "delivered", "attempts": 1}},
                )
            )
            await session.commit()

        artifact = await self.builder.build_alerts_artifact(date(2026, 7, 16))
        payload = artifact.payload_json()
        self.assertEqual(artifact.snapshot_key, "alerts:2026-07-16")
        self.assertEqual(
            artifact.target_path,
            "30_TradingPlaybook/Alerts/Auto/2026/2026-07-16.md",
        )
        self.assertFalse(artifact.immutable)
        self.assertEqual(
            [row["alert_id"] for row in payload["timeline"]],
            [601, 602, 603, 604, 605, 606],
        )
        self.assertEqual(
            [row["in_app_status"]["status"] for row in payload["timeline"]],
            ["delivered", "pending", "delivered", "failed", "sending", "skipped"],
        )
        self.assertEqual(
            [row["timeline_state"] for row in payload["timeline"]],
            [
                "delivered",
                "pending_confirmation",
                "confirmed",
                "failed",
                "pending_confirmation",
                "failed",
            ],
        )
        self.assertEqual(
            payload["timeline"][0]["triggered_at"],
            "2026-07-16T01:00:00Z",
        )
        self.assertEqual(payload["timeline"][2]["candidate_id"], 2022)
        self.assertEqual(
            payload["timeline"][2]["acknowledged_at"],
            "2026-07-16T01:03:00Z",
        )
        self.assertEqual(
            set(payload["timeline"][0]["market_facts"]),
            {
                "source_trade_date",
                "target_trade_date",
                "stage",
                "status",
                "trade_date",
            },
        )
        self.assertEqual(
            set(payload["timeline"][3]["market_facts"]),
            {
                "trade_date",
                "stock_code",
                "mode_key",
                "condition_version",
                "occurrence_no",
                "quote",
            },
        )
        self.assertEqual(
            set(payload["timeline"][3]["market_facts"]["quote"]),
            {
                "code",
                "name",
                "price",
                "change_pct",
                "sealed",
                "open_count",
                "datetime",
                "captured_at",
            },
        )
        encoded = canonical_json_bytes(payload).decode("utf-8")
        for forbidden in (
            "wechat",
            "微信",
            "webhook",
            "token",
            "secret",
            "password",
            "api_key",
            "bearer",
            "receipt",
            "config",
            "output",
            "owner",
            "idempotency",
            "private",
        ):
            self.assertNotIn(forbidden, encoded.lower())

    async def test_alerts_builder_rejects_invalid_date_and_corrupt_roots_or_links(self):
        with self.assertRaisesRegex(ValueError, "trade_date"):
            await self.builder.build_alerts_artifact(
                datetime(2026, 7, 16),  # type: ignore[arg-type]
            )
        async with self.session_factory() as session:
            session.add(
                TradingAlertEvent(
                    id=610,
                    plan_version_id=202,
                    candidate_id=2021,
                    event_type="plan_ready",
                    severity="info",
                    dedup_key="event:610",
                    triggered_at=datetime(2026, 7, 16, 10),
                    market_snapshot_json=[],
                    message="corrupt",
                    channel_status_json={"in_app": {"status": "delivered"}},
                )
            )
            await session.commit()
        with self.assertRaisesRegex(ValueError, "market_snapshot"):
            await self.builder.build_alerts_artifact(date(2026, 7, 16))

        corrupt_timestamp = TradingAlertEvent(
            id=611,
            plan_version_id=202,
            event_type="plan_ready",
            severity="info",
            dedup_key="event:611",
            triggered_at="not-a-datetime",
            market_snapshot_json={},
            message="corrupt timestamp",
            channel_status_json={
                "in_app": {"status": "delivered", "attempts": 1}
            },
        )
        with self.assertRaisesRegex(ValueError, "triggered_at"):
            self.builder._alert_payload(corrupt_timestamp)

    async def test_daily_index_lists_all_versions_and_distinguishes_three_dates(self):
        async with self.session_factory() as session:
            active = await session.get(TradingPlanVersion, 204)
            active.status = "active"
            await session.commit()

        artifact = await self.builder.build_daily_index_artifact(
            date(2026, 7, 16)
        )
        payload = artifact.payload_json()
        self.assertEqual(artifact.snapshot_key, "daily-index:2026-07-16")
        self.assertEqual(
            artifact.target_path,
            "30_TradingPlaybook/Daily/Auto/2026/2026-07-16/index.md",
        )
        self.assertFalse(artifact.immutable)
        self.assertEqual(
            {row["plan_version_id"] for row in payload["plan_versions"]},
            {201, 202, 203, 204},
        )
        self.assertEqual(payload["current_effective_plan_version_id"], 204)
        effective = [row for row in payload["plan_versions"] if row["current_effective"]]
        self.assertEqual([row["plan_version_id"] for row in effective], [204])
        for row in payload["plan_versions"]:
            self.assertEqual(row["source_trade_date"], "2026-07-14")
            self.assertEqual(row["target_trade_date"], "2026-07-16")
            self.assertEqual(
                [item["action_trade_date"] for item in row["candidates"]],
                ["2026-07-14", "2026-07-16"],
            )
            self.assertEqual(row["generated_at"], "2026-07-15T10:05:06.123456Z")
        self.assertEqual(
            [item["time_cn"] for item in payload["stage_schedule"]],
            ["14:40", "15:10", "15:30", "08:50", "09:26"],
        )
        self.assertEqual(
            payload["stage_schedule"][2]["phases"],
            ["after_close", "final_review"],
        )

    async def test_daily_index_current_effective_uses_status_and_newest_precedence(self):
        async with self.session_factory() as session:
            rows = {
                plan_id: await session.get(TradingPlanVersion, plan_id)
                for plan_id in (201, 202, 203, 204)
            }
            rows[201].status = "draft"
            rows[202].status = "confirmed"
            rows[203].status = "draft"
            rows[204].status = "draft"
            await session.commit()

        confirmed_over_newer_draft = (
            await self.builder.build_daily_index_artifact(date(2026, 7, 16))
        ).payload_json()
        self.assertEqual(
            confirmed_over_newer_draft["current_effective_plan_version_id"],
            202,
        )
        self.assertEqual(
            [
                row["plan_version_id"]
                for row in confirmed_over_newer_draft["plan_versions"]
                if row["current_effective"]
            ],
            [202],
        )

        async with self.session_factory() as session:
            for plan_id in (201, 202, 203, 204):
                row = await session.get(TradingPlanVersion, plan_id)
                row.status = "draft"
            await session.commit()
        draft_only = (
            await self.builder.build_daily_index_artifact(date(2026, 7, 16))
        ).payload_json()
        self.assertEqual(draft_only["current_effective_plan_version_id"], 204)

        async with self.session_factory() as session:
            for index, plan_id in enumerate((201, 202, 203, 204)):
                row = await session.get(TradingPlanVersion, plan_id)
                row.status = "superseded" if index % 2 == 0 else "expired"
            await session.commit()
        historical_only = (
            await self.builder.build_daily_index_artifact(date(2026, 7, 16))
        ).payload_json()
        self.assertIsNone(
            historical_only["current_effective_plan_version_id"]
        )
        self.assertEqual(len(historical_only["plan_versions"]), 4)
        self.assertFalse(
            any(
                row["current_effective"]
                for row in historical_only["plan_versions"]
            )
        )

        active_one, _ = self._plan_row(801, "preclose", 1, True)
        active_two, _ = self._plan_row(802, "after_close", 2, True)
        active_one.status = "active"
        active_two.status = "active"
        with self.assertRaisesRegex(ValueError, "multiple active"):
            self.builder._current_effective_plan_id(
                [active_one, active_two],
                trade_date=date(2026, 7, 16),
            )

    async def test_daily_index_rejects_invalid_date_and_more_than_three_candidates(self):
        with self.assertRaisesRegex(ValueError, "trade_date"):
            await self.builder.build_daily_index_artifact(
                datetime(2026, 7, 16),  # type: ignore[arg-type]
            )
        async with self.session_factory() as session:
            for index in (3, 4):
                session.add(
                    TradingPlanCandidate(
                        id=2010 + index,
                        plan_version_id=201,
                        stock_code=f"60000{index}",
                        stock_name=f"overflow {index}",
                        action_trade_date=date(2026, 7, 16),
                        theme_name="overflow",
                        primary_mode_key=f"overflow_mode_{index}",
                        supporting_mode_keys_json=[],
                        role="overflow",
                        rank=index,
                        recognition_json={},
                        entry_trigger_json={},
                        invalidation_json={},
                        exit_trigger_json={},
                        risk_level="high",
                        position_reference=0,
                        evidence_json=[],
                        manual_overrides_json={},
                        status="waiting",
                    )
                )
            await session.commit()
        with self.assertRaisesRegex(ValueError, "more than 3 candidates"):
            await self.builder.build_daily_index_artifact(date(2026, 7, 16))

    async def test_dashboard_has_only_auto_navigation_queries_and_notes_link(self):
        artifact = await self.builder.build_dashboard_artifact(date(2026, 7, 16))
        payload = artifact.payload_json()
        self.assertEqual(artifact.snapshot_key, "dashboard:trading-playbook")
        self.assertEqual(artifact.target_path, "Dashboards/交易预案.md")
        self.assertEqual(artifact.entity_type, "dashboard")
        self.assertFalse(artifact.immutable)
        self.assertEqual(
            payload["navigation"]["notes"],
            "[[30_TradingPlaybook/Notes/2026/2026-07-16]]",
        )
        self.assertTrue(payload["dataview_queries"])
        for query in payload["dataview_queries"]:
            self.assertIn("Auto", query)
            self.assertNotIn("Notes", query)
        self.assertEqual(
            set(payload),
            {
                "type",
                "trade_date",
                "navigation",
                "dataview_queries",
                "manual_required",
                "auto_execute",
            },
        )

        with self.assertRaisesRegex(ValueError, "trade_date"):
            await self.builder.build_dashboard_artifact(
                datetime(2026, 7, 16),  # type: ignore[arg-type]
            )

    async def test_stage_builder_sorts_deduplicates_and_sets_mutability(self):
        await self._make_plan_review_relevant(201, status="superseded")
        await self._make_plan_review_relevant(202, status="active")
        async with self.session_factory() as session:
            session.add_all(
                [
                    self._review_row(501, plan_version_id=201),
                    self._review_row(502, plan_version_id=202),
                ]
            )
            await session.commit()

        plan_batch = await self.builder.build_stage_artifacts(
            trade_date=date(2026, 7, 14),
            phase="preclose",
            plan_version_ids=[201, 201],
        )
        self.assertEqual(
            [artifact.snapshot_key for artifact in plan_batch],
            [
                "plan:201",
                "alerts:2026-07-14",
                "daily-index:2026-07-14",
                "dashboard:trading-playbook",
            ],
        )

        review_batch = await self.builder.build_stage_artifacts(
            trade_date=date(2026, 7, 16),
            phase="initial_review",
            review_ids=[502, 501, 502],
        )
        self.assertEqual(
            [artifact.snapshot_key for artifact in review_batch],
            [
                "review:501:initial",
                "review:502:initial",
                "alerts:2026-07-16",
                "daily-index:2026-07-16",
                "dashboard:trading-playbook",
            ],
        )
        self.assertEqual(
            [artifact.immutable for artifact in review_batch],
            [True, True, False, False, False],
        )
        all_artifacts = (*plan_batch, *review_batch)
        self.assertFalse(
            any(artifact.entity_type == "notes" for artifact in all_artifacts)
        )
        self.assertFalse(
            any("/Notes/" in artifact.target_path for artifact in all_artifacts)
        )

        with self.assertRaisesRegex(ValueError, "phase"):
            await self.builder.build_stage_artifacts(
                trade_date=date(2026, 7, 16),
                phase="invalid",
            )
        with self.assertRaisesRegex(ValueError, "plan_version_ids"):
            await self.builder.build_stage_artifacts(
                trade_date=date(2026, 7, 16),
                phase="preclose",
                plan_version_ids=[0],
            )
        with self.assertRaisesRegex(ValueError, "review_ids"):
            await self.builder.build_stage_artifacts(
                trade_date=date(2026, 7, 16),
                phase="initial_review",
                review_ids=[True],  # type: ignore[list-item]
            )

        with_rules = await self.builder.build_stage_artifacts(
            trade_date=date(2026, 7, 16),
            phase="preclose",
            include_rules=True,
        )
        self.assertEqual(len(with_rules), 22)
        self.assertTrue(
            all(artifact.entity_type == "rule" for artifact in with_rules[:19])
        )
        self.assertEqual(
            [artifact.entity_type for artifact in with_rules[-3:]],
            ["alerts", "daily_index", "dashboard"],
        )

    async def test_stage_builder_rejects_cross_phase_and_cross_date_entities(self):
        await self._make_plan_review_relevant(202, status="active")
        async with self.session_factory() as session:
            session.add(self._review_row(521, plan_version_id=202))
            await session.commit()

        for phase in ("catalog", "reconcile", "initial_review", "final_review"):
            with self.subTest(plan_phase=phase):
                with self.assertRaisesRegex(ValueError, "plan_version_ids.*phase"):
                    await self.builder.build_stage_artifacts(
                        trade_date=date(2026, 7, 14),
                        phase=phase,
                        plan_version_ids=[201],
                    )
        for phase in (
            "catalog",
            "reconcile",
            "preclose",
            "after_close",
            "overnight",
            "auction",
        ):
            with self.subTest(review_phase=phase):
                with self.assertRaisesRegex(ValueError, "review_ids.*phase"):
                    await self.builder.build_stage_artifacts(
                        trade_date=date(2026, 7, 16),
                        phase=phase,
                        review_ids=[521],
                    )

        with self.assertRaisesRegex(ValueError, "stage.*batch phase"):
            await self.builder.build_stage_artifacts(
                trade_date=date(2026, 7, 14),
                phase="preclose",
                plan_version_ids=[202],
            )
        with self.assertRaisesRegex(ValueError, "source_trade_date.*batch"):
            await self.builder.build_stage_artifacts(
                trade_date=date(2026, 7, 15),
                phase="preclose",
                plan_version_ids=[201],
            )
        with self.assertRaisesRegex(ValueError, "target_trade_date.*batch"):
            await self.builder.build_stage_artifacts(
                trade_date=date(2026, 7, 15),
                phase="overnight",
                plan_version_ids=[203],
            )
        with self.assertRaisesRegex(ValueError, "review trade_date.*batch"):
            await self.builder.build_stage_artifacts(
                trade_date=date(2026, 7, 15),
                phase="initial_review",
                review_ids=[521],
            )

    async def test_review_and_index_reject_corrupt_database_timestamps(self):
        corrupt_review = self._review_row()
        corrupt_review.generated_at = "not-a-datetime"
        plan, _ = self._plan_row(202, "after_close", 2, True)
        plan.status = "active"
        candidates, _ = self._candidate_rows(202)
        with self.assertRaisesRegex(ValueError, "generated_at"):
            self.builder._validate_review_data(
                corrupt_review,
                plan,
                candidates,
                phase="initial_review",
            )

        corrupt_plan, _ = self._plan_row(701, "preclose", 1, False)
        corrupt_plan.generated_at = "not-a-datetime"
        candidates, _ = self._candidate_rows(701)
        with self.assertRaisesRegex(ValueError, "generated_at"):
            self.builder._daily_plan_payload(
                corrupt_plan,
                candidates,
                current_effective_id=None,
            )


if __name__ == "__main__":
    unittest.main()
