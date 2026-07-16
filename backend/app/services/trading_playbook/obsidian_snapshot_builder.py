"""Build immutable Obsidian artifacts from trading-playbook database rows."""

from __future__ import annotations

import re
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from datetime import date, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import TradingModeRule, TradingPlanCandidate, TradingPlanVersion
from app.services.trading_playbook.obsidian_types import (
    ObsidianArtifact,
    database_datetime_to_cn,
)
from app.services.trading_playbook.rule_catalog import canonical_rule_source_refs


_CATALOG_VERSION = re.compile(r"v([1-9][0-9]*)\Z")
_MODE_KEY = re.compile(r"[a-z][a-z0-9]*(?:_[a-z0-9]+)*\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_PLAN_STAGES = ("preclose", "after_close", "overnight", "auction")
_MAX_DATABASE_INTEGER = (1 << 63) - 1


SessionFactory = Callable[[], AbstractAsyncContextManager[AsyncSession]]


def _positive_integer(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def _database_date(value: object, field_name: str) -> date:
    if type(value) is not date:
        raise ValueError(f"{field_name} must be a date")
    return value


def _database_datetime(value: object, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"{field_name} must be a datetime")
    return value


class TradingPlaybookObsidianSnapshotBuilder:
    """Read persisted rules and plans into deeply owned snapshot artifacts."""

    def __init__(self, session_factory: SessionFactory) -> None:
        self._session_factory = session_factory

    async def build_rule_artifacts(
        self,
        catalog_version: str = "v2",
    ) -> tuple[ObsidianArtifact, ...]:
        match = (
            _CATALOG_VERSION.fullmatch(catalog_version)
            if isinstance(catalog_version, str)
            else None
        )
        if match is None:
            raise ValueError(
                "catalog_version must be canonical vN with positive integer N"
            )
        version = int(match.group(1))
        if version > _MAX_DATABASE_INTEGER:
            raise ValueError("catalog_version is outside the database integer range")

        async with self._session_factory() as db:
            rules = (
                await db.scalars(
                    select(TradingModeRule)
                    .where(
                        TradingModeRule.version == version,
                        TradingModeRule.enabled.is_(True),
                    )
                    .order_by(TradingModeRule.mode_key, TradingModeRule.id)
                )
            ).all()
            return tuple(
                self._build_rule_artifact(rule, catalog_version)
                for rule in rules
            )

    async def build_plan_artifact(
        self,
        plan_version_id: int,
    ) -> ObsidianArtifact:
        plan_version_id = _positive_integer(
            plan_version_id,
            "plan_version_id",
        )
        if plan_version_id > _MAX_DATABASE_INTEGER:
            raise ValueError("plan_version_id is outside the database integer range")

        async with self._session_factory() as db:
            plan = await db.get(TradingPlanVersion, plan_version_id)
            if plan is None:
                raise LookupError(
                    f"trading plan version {plan_version_id} was not found"
                )
            candidates = (
                await db.scalars(
                    select(TradingPlanCandidate)
                    .where(
                        TradingPlanCandidate.plan_version_id == plan_version_id
                    )
                    .order_by(
                        TradingPlanCandidate.rank,
                        TradingPlanCandidate.id,
                    )
                )
            ).all()
            if len(candidates) > 3:
                raise ValueError(
                    f"trading plan version {plan_version_id} has more than 3 candidates"
                )
            return self._build_plan_artifact(plan, candidates)

    @staticmethod
    def _build_rule_artifact(
        rule: TradingModeRule,
        catalog_version: str,
    ) -> ObsidianArtifact:
        mode_key = rule.mode_key
        if not isinstance(mode_key, str) or _MODE_KEY.fullmatch(mode_key) is None:
            raise ValueError(f"rule mode_key is not a safe lower-case identifier: {mode_key!r}")
        rule_id = _positive_integer(rule.id, "rule id")
        rule_version = _positive_integer(rule.version, "rule version")
        if (
            not isinstance(rule.content_hash, str)
            or _SHA256.fullmatch(rule.content_hash) is None
        ):
            raise ValueError(f"rule {mode_key} content_hash must be sha256")
        created_at = _database_datetime(rule.created_at, "rule created_at")
        try:
            source_refs = canonical_rule_source_refs(
                {"source_refs": rule.source_refs_json}
            )
            payload = {
                "type": "trading_mode_rule",
                "catalog_version": catalog_version,
                "rule_id": rule_id,
                "mode_key": mode_key,
                "rule_version": rule_version,
                "name": rule.name,
                "family": rule.family,
                "style": rule.style,
                "window": rule.window,
                "automation_level": rule.automation_level,
                "description": rule.description,
                "prerequisites": rule.prerequisites_json,
                "candidate_filters": rule.candidate_filters_json,
                "entry_trigger": rule.entry_trigger_json,
                "invalidation": rule.invalidation_json,
                "exit_trigger": rule.exit_trigger_json,
                "risk_guidance": rule.risk_guidance_json,
                "source_refs": source_refs,
                "content_hash": rule.content_hash,
                "enabled": rule.enabled,
                "created_at": database_datetime_to_cn(created_at),
                "manual_required": True,
                "auto_execute": False,
            }
            return ObsidianArtifact(
                snapshot_key=f"rule:{catalog_version}:{mode_key}",
                trade_date=created_at.date(),
                entity_type="rule",
                entity_id=rule_id,
                phase="catalog",
                target_path=(
                    "30_TradingPlaybook/Modes/Auto/"
                    f"{catalog_version}/{mode_key}.md"
                ),
                immutable=True,
                payload=payload,
            )
        except (TypeError, ValueError) as exc:
            raise ValueError(f"corrupt rule data for {mode_key}: {exc}") from exc

    @staticmethod
    def _build_plan_artifact(
        plan: TradingPlanVersion,
        candidates: list[TradingPlanCandidate],
    ) -> ObsidianArtifact:
        plan_id = _positive_integer(plan.id, "plan id")
        version_no = _positive_integer(plan.version_no, "version_no")
        if plan.stage not in _PLAN_STAGES:
            raise ValueError(f"plan stage must be one of {_PLAN_STAGES}")
        source_trade_date = _database_date(
            plan.source_trade_date,
            "source_trade_date",
        )
        target_trade_date = _database_date(
            plan.target_trade_date,
            "target_trade_date",
        )
        generated_at = _database_datetime(plan.generated_at, "generated_at")
        if plan.confirmed_at is not None:
            confirmed_at = _database_datetime(plan.confirmed_at, "confirmed_at")
        else:
            confirmed_at = None

        candidate_payloads = []
        for candidate in candidates:
            candidate_payloads.append(
                {
                    "candidate_id": _positive_integer(
                        candidate.id,
                        "candidate id",
                    ),
                    "plan_version_id": candidate.plan_version_id,
                    "stock_code": candidate.stock_code,
                    "stock_name": candidate.stock_name,
                    "action_trade_date": _database_date(
                        candidate.action_trade_date,
                        "action_trade_date",
                    ),
                    "theme_name": candidate.theme_name,
                    "primary_mode_key": candidate.primary_mode_key,
                    "supporting_mode_keys": candidate.supporting_mode_keys_json,
                    "role": candidate.role,
                    "rank": candidate.rank,
                    "recognition": candidate.recognition_json,
                    "entry_trigger": candidate.entry_trigger_json,
                    "invalidation": candidate.invalidation_json,
                    "exit_trigger": candidate.exit_trigger_json,
                    "risk_level": candidate.risk_level,
                    "position_reference": candidate.position_reference,
                    "evidence": candidate.evidence_json,
                    "manual_overrides": candidate.manual_overrides_json,
                    "status": candidate.status,
                }
            )

        payload = {
            "type": "trading_plan_version",
            "plan_version_id": plan_id,
            "version_no": version_no,
            "stage": plan.stage,
            "status": plan.status,
            "source_trade_date": source_trade_date,
            "target_trade_date": target_trade_date,
            "parent_plan_version_id": plan.parent_plan_version_id,
            "market_state": plan.market_state_json,
            "theme_ranking": plan.theme_ranking_json,
            "mode_radar": plan.mode_radar_json,
            "rule_snapshot": plan.rule_snapshot_json,
            "data_quality": plan.data_quality_json,
            "risk_settings": plan.risk_settings_json,
            "change_summary": plan.change_summary_json,
            "input_hash": plan.input_hash,
            "generated_at": database_datetime_to_cn(generated_at),
            "confirmed_at": database_datetime_to_cn(confirmed_at),
            "confirmed_by": plan.confirmed_by,
            "candidates": candidate_payloads,
            "manual_required": True,
            "auto_execute": False,
        }
        try:
            return ObsidianArtifact(
                snapshot_key=f"plan:{plan_id}",
                trade_date=target_trade_date,
                entity_type="plan",
                entity_id=plan_id,
                phase=plan.stage,
                target_path=(
                    "30_TradingPlaybook/Daily/Auto/"
                    f"{target_trade_date.year}/{target_trade_date.isoformat()}/"
                    f"{plan.stage}-v{version_no}.md"
                ),
                immutable=True,
                payload=payload,
            )
        except (TypeError, ValueError) as exc:
            raise ValueError(f"corrupt plan data for {plan_id}: {exc}") from exc


__all__ = ["TradingPlaybookObsidianSnapshotBuilder"]
