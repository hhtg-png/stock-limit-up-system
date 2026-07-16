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
        snapshot_rule = next(
            rule
            for rule in self.catalog["rules"]
            if rule["mode_key"] == "leader_turn_two"
        )
        rule_hash = canonical_rule_content_hash(snapshot_rule)
        source_refs = canonical_rule_source_refs(snapshot_rule)
        source_hashes = [
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
        ]
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
                    "mode_key": "leader_turn_two",
                    "rule_version": 2,
                    "rule_hash": rule_hash,
                }
            ],
            rule_snapshot_json=[
                {
                    "mode_key": "leader_turn_two",
                    "version": 2,
                    "content_hash": rule_hash,
                    "source_hashes": _json_copy(source_hashes),
                    "source_refs": _json_copy(source_refs),
                }
            ],
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


if __name__ == "__main__":
    unittest.main()
