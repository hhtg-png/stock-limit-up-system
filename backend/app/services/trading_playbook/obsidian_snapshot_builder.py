"""Build immutable Obsidian artifacts from trading-playbook database rows."""

from __future__ import annotations

import re
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from datetime import date, datetime

from sqlalchemy import select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    TradingModeRule,
    TradingPlanCandidate,
    TradingPlanVersion,
    TradingRuleSource,
)
from app.services.trading_playbook.errors import UnsafePlanDataError
from app.services.trading_playbook.obsidian_types import (
    ObsidianArtifact,
    database_datetime_to_cn,
)
from app.services.trading_playbook.rule_catalog import (
    canonical_rule_content_hash,
    canonical_rule_source_refs,
)
from app.services.trading_playbook.serialization import normalize_plan_payload


_CATALOG_VERSION = re.compile(r"v([1-9][0-9]*)\Z")
_MODE_KEY = re.compile(r"[a-z][a-z0-9]*(?:_[a-z0-9]+)*\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_STOCK_CODE = re.compile(r"[0-9]{6}\Z")
_PLAN_STAGES = ("preclose", "after_close", "overnight", "auction")
_CANDIDATE_STATUSES = {"waiting", "triggered", "invalidated", "exit"}
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
            prepared_rules = []
            required_sources: set[tuple[str, str]] = set()
            for rule in rules:
                source_refs = self._validate_rule_provenance(rule)
                prepared_rules.append((rule, source_refs))
                required_sources.update(
                    (
                        source_ref["source_key"],
                        source_ref["source_content_hash"],
                    )
                    for source_ref in source_refs
                )
            ready_sources: set[tuple[str, str]] = set()
            if required_sources:
                source_rows = (
                    await db.execute(
                        select(
                            TradingRuleSource.source_key,
                            TradingRuleSource.content_hash,
                        ).where(
                            tuple_(
                                TradingRuleSource.source_key,
                                TradingRuleSource.content_hash,
                            ).in_(sorted(required_sources)),
                            TradingRuleSource.status == "ready",
                        )
                    )
                ).all()
                ready_sources = set(source_rows)
            missing_sources = required_sources - ready_sources
            if missing_sources:
                source_key, content_hash = sorted(missing_sources)[0]
                raise ValueError(
                    "rule source_ref has no exact persisted ready source: "
                    f"{source_key}@{content_hash}"
                )
            return tuple(
                self._build_rule_artifact(rule, catalog_version, source_refs)
                for rule, source_refs in prepared_rules
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
            self._validate_plan_data(plan, candidates)
            await self._validate_plan_rule_provenance(db, plan, candidates)
            return self._build_plan_artifact(plan, candidates)

    @staticmethod
    def _require_json_root(
        value: object,
        expected_type: type[dict] | type[list],
        field_name: str,
    ) -> None:
        if type(value) is not expected_type:
            expected_name = "object" if expected_type is dict else "array"
            raise ValueError(
                f"plan {field_name} must be a JSON {expected_name}"
            )

    @classmethod
    def _validate_plan_data(
        cls,
        plan: TradingPlanVersion,
        candidates: list[TradingPlanCandidate],
    ) -> None:
        plan_id = _positive_integer(plan.id, "plan id")
        _positive_integer(plan.version_no, "version_no")
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
        confirmed_at = (
            _database_datetime(plan.confirmed_at, "confirmed_at")
            if plan.confirmed_at is not None
            else None
        )
        if plan.parent_plan_version_id is not None:
            parent_id = _positive_integer(
                plan.parent_plan_version_id,
                "parent_plan_version_id",
            )
            if parent_id == plan_id:
                raise ValueError(
                    "parent_plan_version_id cannot reference the plan itself"
                )
        if (
            not isinstance(plan.input_hash, str)
            or _SHA256.fullmatch(plan.input_hash) is None
        ):
            raise ValueError("plan input_hash must be sha256")

        for field_name, value, expected_type in (
            ("market_state", plan.market_state_json, dict),
            ("theme_ranking", plan.theme_ranking_json, list),
            ("mode_radar", plan.mode_radar_json, list),
            ("rule_snapshot", plan.rule_snapshot_json, list),
            ("risk_settings", plan.risk_settings_json, dict),
            ("data_quality", plan.data_quality_json, dict),
            ("change_summary", plan.change_summary_json, dict),
        ):
            cls._require_json_root(value, expected_type, field_name)

        validation_candidates = []
        for candidate in candidates:
            candidate_id = _positive_integer(candidate.id, "candidate id")
            owner_id = _positive_integer(
                candidate.plan_version_id,
                f"candidate {candidate_id} plan_version_id",
            )
            if owner_id != plan_id:
                raise ValueError(
                    f"candidate {candidate_id} ownership does not match plan {plan_id}"
                )
            mode_key = candidate.primary_mode_key
            if (
                not isinstance(mode_key, str)
                or _MODE_KEY.fullmatch(mode_key) is None
            ):
                raise ValueError(
                    f"candidate {candidate_id} primary mode is malformed"
                )
            supporting_modes = candidate.supporting_mode_keys_json
            cls._require_json_root(
                supporting_modes,
                list,
                "candidate supporting_mode_keys",
            )
            seen_supporting: set[str] = set()
            for supporting_mode in supporting_modes:
                if (
                    not isinstance(supporting_mode, str)
                    or _MODE_KEY.fullmatch(supporting_mode) is None
                    or supporting_mode == mode_key
                    or supporting_mode in seen_supporting
                ):
                    raise ValueError(
                        f"candidate {candidate_id} supporting_mode_keys are malformed"
                    )
                seen_supporting.add(supporting_mode)
            for field_name, value, expected_type in (
                ("candidate recognition", candidate.recognition_json, dict),
                ("candidate entry trigger", candidate.entry_trigger_json, dict),
                ("candidate invalidation", candidate.invalidation_json, dict),
                ("candidate exit trigger", candidate.exit_trigger_json, dict),
                ("candidate evidence", candidate.evidence_json, list),
                ("candidate manual_overrides", candidate.manual_overrides_json, dict),
            ):
                cls._require_json_root(value, expected_type, field_name)
            if candidate.status not in _CANDIDATE_STATUSES:
                raise ValueError(
                    f"candidate {candidate_id} candidate status is malformed"
                )
            validation_candidates.append(
                {
                    "stock_code": candidate.stock_code,
                    "stock_name": candidate.stock_name,
                    "primary_mode_key": mode_key,
                    "rank": candidate.rank,
                    "action_trade_date": _database_date(
                        candidate.action_trade_date,
                        "action_trade_date",
                    ),
                    "position_reference": candidate.position_reference,
                    "risk_level": candidate.risk_level,
                    "entry_trigger_json": candidate.entry_trigger_json,
                    "invalidation_json": candidate.invalidation_json,
                    "exit_trigger_json": candidate.exit_trigger_json,
                }
            )

        try:
            normalize_plan_payload(
                {
                    "source_trade_date": source_trade_date,
                    "target_trade_date": target_trade_date,
                    "stage": plan.stage,
                    "status": plan.status,
                    "generated_at": database_datetime_to_cn(generated_at),
                    "confirmed_at": database_datetime_to_cn(confirmed_at),
                    "risk_settings_json": plan.risk_settings_json,
                    "candidates": validation_candidates,
                }
            )
        except UnsafePlanDataError as exc:
            raise ValueError(f"corrupt plan data for {plan_id}: {exc}") from exc

    @classmethod
    async def _validate_plan_rule_provenance(
        cls,
        db: AsyncSession,
        plan: TradingPlanVersion,
        candidates: list[TradingPlanCandidate],
    ) -> None:
        plan_id = plan.id
        snapshot_rows = plan.rule_snapshot_json
        if not snapshot_rows:
            raise ValueError(f"plan {plan_id} rule_snapshot must not be empty")
        snapshot_by_mode: dict[str, dict[str, object]] = {}
        identities: set[tuple[str, int, str]] = set()
        for index, row in enumerate(snapshot_rows):
            if type(row) is not dict:
                raise ValueError(
                    f"plan {plan_id} rule_snapshot[{index}] must be a JSON object"
                )
            mode_key = row.get("mode_key")
            version = row.get("version")
            content_hash = row.get("content_hash")
            if not isinstance(mode_key, str) or _MODE_KEY.fullmatch(mode_key) is None:
                raise ValueError(
                    f"plan {plan_id} rule_snapshot mode_key is malformed"
                )
            version = _positive_integer(
                version,
                f"plan {plan_id} rule_snapshot version",
            )
            if (
                not isinstance(content_hash, str)
                or _SHA256.fullmatch(content_hash) is None
            ):
                raise ValueError(
                    f"plan {plan_id} rule_snapshot content_hash must be sha256"
                )
            if mode_key in snapshot_by_mode:
                raise ValueError(
                    f"plan {plan_id} rule_snapshot has duplicate mode {mode_key}"
                )
            try:
                source_refs = canonical_rule_source_refs(row)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"plan {plan_id} rule_snapshot source_refs are malformed: {exc}"
                ) from exc
            if row.get("source_refs") != source_refs:
                raise ValueError(
                    f"plan {plan_id} rule_snapshot source_refs are not canonical"
                )
            expected_source_hashes: dict[str, str] = {}
            for source_ref in source_refs:
                source_key = source_ref["source_key"]
                source_hash = source_ref["source_content_hash"]
                previous = expected_source_hashes.setdefault(
                    source_key,
                    source_hash,
                )
                if previous != source_hash:
                    raise ValueError(
                        f"plan {plan_id} rule_snapshot source hashes conflict"
                    )
            canonical_source_hashes = [
                {"source_key": source_key, "content_hash": source_hash}
                for source_key, source_hash in sorted(
                    expected_source_hashes.items()
                )
            ]
            if row.get("source_hashes") != canonical_source_hashes:
                raise ValueError(
                    f"plan {plan_id} rule_snapshot source_hashes do not match source_refs"
                )
            normalized_row = {
                "mode_key": mode_key,
                "version": version,
                "content_hash": content_hash,
                "source_refs": source_refs,
                "source_hashes": canonical_source_hashes,
            }
            snapshot_by_mode[mode_key] = normalized_row
            identities.add((mode_key, version, content_hash))

        persisted_rules = (
            await db.scalars(
                select(TradingModeRule).where(
                    tuple_(
                        TradingModeRule.mode_key,
                        TradingModeRule.version,
                        TradingModeRule.content_hash,
                    ).in_(sorted(identities))
                )
            )
        ).all()
        persisted_by_identity = {
            (rule.mode_key, rule.version, rule.content_hash): rule
            for rule in persisted_rules
        }
        missing_identities = identities - set(persisted_by_identity)
        if missing_identities:
            mode_key, version, content_hash = sorted(missing_identities)[0]
            raise ValueError(
                f"plan {plan_id} rule_snapshot has no exact persisted rule: "
                f"{mode_key}@v{version}:{content_hash}"
            )

        required_sources: set[tuple[str, str]] = set()
        for identity, persisted_rule in persisted_by_identity.items():
            persisted_refs = cls._validate_rule_provenance(persisted_rule)
            snapshot_row = snapshot_by_mode[identity[0]]
            if snapshot_row["source_refs"] != persisted_refs:
                raise ValueError(
                    f"plan {plan_id} rule_snapshot source_refs do not match persisted rule"
                )
            required_sources.update(
                (ref["source_key"], ref["source_content_hash"])
                for ref in persisted_refs
            )
        try:
            risk_source_refs = canonical_rule_source_refs(
                {
                    "source_refs": plan.risk_settings_json.get(
                        "source_refs"
                    )
                }
            )
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"plan {plan_id} risk_settings source_refs are malformed: {exc}"
            ) from exc
        if plan.risk_settings_json.get("source_refs") != risk_source_refs:
            raise ValueError(
                f"plan {plan_id} risk_settings source_refs are not canonical"
            )
        risk_hash_by_key: dict[str, str] = {}
        for risk_source_ref in risk_source_refs:
            source_key = risk_source_ref["source_key"]
            source_hash = risk_source_ref["source_content_hash"]
            previous = risk_hash_by_key.setdefault(source_key, source_hash)
            if previous != source_hash:
                raise ValueError(
                    f"plan {plan_id} risk_settings source hashes conflict"
                )
        risk_source_pairs = set(risk_hash_by_key.items())
        required_sources.update(risk_source_pairs)
        ready_sources = set(
            (
                await db.execute(
                    select(
                        TradingRuleSource.source_key,
                        TradingRuleSource.content_hash,
                    ).where(
                        tuple_(
                            TradingRuleSource.source_key,
                            TradingRuleSource.content_hash,
                        ).in_(sorted(required_sources)),
                        TradingRuleSource.status == "ready",
                    )
                )
            ).all()
        )
        if risk_source_pairs - ready_sources:
            raise ValueError(
                f"plan {plan_id} risk_settings has no exact persisted ready source"
            )
        if required_sources - ready_sources:
            raise ValueError(
                f"plan {plan_id} rule_snapshot references a missing persisted ready source"
            )

        radar_identities: set[tuple[str, str]] = set()
        radar_modes_by_stock: dict[str, set[str]] = {}
        for index, radar_row in enumerate(plan.mode_radar_json):
            if type(radar_row) is not dict:
                raise ValueError(
                    f"plan {plan_id} mode_radar[{index}] must be a JSON object"
                )
            mode_key = radar_row.get("mode_key")
            stock_code = radar_row.get("stock_code")
            rule_version = radar_row.get("rule_version")
            rule_hash = radar_row.get("rule_hash")
            if (
                not isinstance(stock_code, str)
                or _STOCK_CODE.fullmatch(stock_code) is None
            ):
                raise ValueError(
                    f"plan {plan_id} mode_radar stock_code is malformed"
                )
            if (
                not isinstance(mode_key, str)
                or _MODE_KEY.fullmatch(mode_key) is None
            ):
                raise ValueError(
                    f"plan {plan_id} mode_radar mode_key is malformed"
                )
            rule_version = _positive_integer(
                rule_version,
                f"plan {plan_id} mode_radar rule_version",
            )
            if (
                not isinstance(rule_hash, str)
                or _SHA256.fullmatch(rule_hash) is None
            ):
                raise ValueError(
                    f"plan {plan_id} mode_radar rule_hash must be sha256"
                )
            snapshot_row = snapshot_by_mode.get(mode_key)
            if (
                snapshot_row is None
                or snapshot_row["version"] != rule_version
                or snapshot_row["content_hash"] != rule_hash
            ):
                raise ValueError(
                    f"plan {plan_id} mode_radar does not match rule_snapshot"
                )
            radar_identity = (stock_code, mode_key)
            if radar_identity in radar_identities:
                raise ValueError(
                    f"plan {plan_id} mode_radar has duplicate stock/mode identity"
                )
            radar_identities.add(radar_identity)
            radar_modes_by_stock.setdefault(stock_code, set()).add(mode_key)

        for candidate in candidates:
            candidate_modes = {
                candidate.primary_mode_key,
                *candidate.supporting_mode_keys_json,
            }
            missing_modes = candidate_modes - radar_modes_by_stock.get(
                candidate.stock_code,
                set(),
            )
            if missing_modes:
                raise ValueError(
                    f"candidate {candidate.id} mode_radar is missing same-stock "
                    f"modes: {', '.join(sorted(missing_modes))}"
                )

    @staticmethod
    def _validate_rule_provenance(
        rule: TradingModeRule,
    ) -> list[dict[str, str]]:
        mode_key = rule.mode_key
        if not isinstance(mode_key, str) or _MODE_KEY.fullmatch(mode_key) is None:
            raise ValueError(
                "rule mode_key is not a safe lower-case identifier: "
                f"{mode_key!r}"
            )
        _positive_integer(rule.id, "rule id")
        _positive_integer(rule.version, "rule version")
        if (
            not isinstance(rule.content_hash, str)
            or _SHA256.fullmatch(rule.content_hash) is None
        ):
            raise ValueError(f"rule {mode_key} content_hash must be sha256")
        try:
            source_refs = canonical_rule_source_refs(
                {"source_refs": rule.source_refs_json}
            )
            prerequisites = rule.prerequisites_json
            if type(prerequisites) is not dict:
                raise ValueError("prerequisites must be a JSON object")
            required_prerequisites = {"requirements", "priority", "role"}
            if set(prerequisites) != required_prerequisites:
                raise ValueError(
                    "prerequisites keys must be exactly requirements, priority, and role"
                )
            for field_name, value, expected_type in (
                ("requirements", prerequisites["requirements"], list),
                ("candidate_filters", rule.candidate_filters_json, list),
                ("entry_trigger", rule.entry_trigger_json, dict),
                ("invalidation", rule.invalidation_json, dict),
                ("exit_trigger", rule.exit_trigger_json, dict),
                ("risk_guidance", rule.risk_guidance_json, dict),
            ):
                if type(value) is not expected_type:
                    expected_name = (
                        "object" if expected_type is dict else "array"
                    )
                    raise ValueError(
                        f"{field_name} must be a JSON {expected_name}"
                    )
            persisted_rule = {
                "mode_key": mode_key,
                "name": rule.name,
                "family": rule.family,
                "style": rule.style,
                "window": rule.window,
                "automation_level": rule.automation_level,
                "priority": prerequisites["priority"],
                "role": prerequisites["role"],
                "requirements": prerequisites["requirements"],
                "entry": rule.entry_trigger_json,
                "invalidation": rule.invalidation_json,
                "exit": rule.exit_trigger_json,
                "source_refs": source_refs,
            }
            expected_hash = canonical_rule_content_hash(persisted_rule)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"corrupt rule data for {mode_key}: {exc}") from exc
        if rule.content_hash != expected_hash:
            raise ValueError(
                f"rule {mode_key} content_hash does not match persisted content"
            )
        return source_refs

    @staticmethod
    def _build_rule_artifact(
        rule: TradingModeRule,
        catalog_version: str,
        source_refs: list[dict[str, str]],
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
