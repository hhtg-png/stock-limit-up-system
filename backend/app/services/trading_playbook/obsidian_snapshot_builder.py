"""Build immutable Obsidian artifacts from trading-playbook database rows."""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from contextlib import AbstractAsyncContextManager
from datetime import date, datetime, time, timedelta

from sqlalchemy import select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    TradingAlertEvent,
    TradingExecutionReview,
    TradingModeRule,
    TradingPlanCandidate,
    TradingPlanVersion,
    TradingRuleSource,
)
from app.services.trading_playbook.errors import UnsafePlanDataError
from app.services.trading_playbook.obsidian_types import (
    OBSIDIAN_PHASES,
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
_REVIEW_PHASES = {"initial_review", "final_review"}
_IN_APP_CHANNEL_STATUSES = {
    "pending",
    "sending",
    "delivered",
    "skipped",
    "uncertain",
    "failed",
}
_ALERT_EVENT_SEVERITIES = {
    "plan_ready": "info",
    "confirmation_required": "warning",
    "review_ready": "info",
    "invalidated": "warning",
    "exit_triggered": "warning",
    "entry_triggered": "action",
}
_IN_APP_EXPORT_FIELDS = (
    "status",
    "attempts",
    "accepted",
    "skipped_at",
    "sending_at",
    "channel_started_at",
    "recovered_at",
    "delivered_at",
    "uncertain_at",
    "failed_at",
)
_IN_APP_TIMESTAMP_FIELDS = frozenset(
    field_name
    for field_name in _IN_APP_EXPORT_FIELDS
    if field_name.endswith("_at")
)
_PLAN_ALERT_MARKET_FIELDS = (
    "source_trade_date",
    "target_trade_date",
    "stage",
    "status",
    "trade_date",
)
_ACTION_ALERT_MARKET_FIELDS = (
    "trade_date",
    "stock_code",
    "mode_key",
    "condition_version",
    "occurrence_no",
)
_ACTION_QUOTE_FIELDS = (
    "code",
    "name",
    "price",
    "change_pct",
    "sealed",
    "open_count",
    "datetime",
    "captured_at",
)
_PLAN_ALERT_EVENT_TYPES = frozenset(
    {"plan_ready", "confirmation_required", "review_ready"}
)
_ACTION_ALERT_EVENT_TYPES = frozenset(
    {"entry_triggered", "invalidated", "exit_triggered"}
)
_STAGE_SCHEDULE = (
    {
        "phases": ("preclose",),
        "time_cn": "14:40",
        "label": "提前预案",
    },
    {
        "phases": ("initial_review",),
        "time_cn": "15:10",
        "label": "初步复盘",
    },
    {
        "phases": ("after_close", "final_review"),
        "time_cn": "15:30",
        "label": "正式预案与最终复盘",
    },
    {
        "phases": ("overnight",),
        "time_cn": "08:50",
        "label": "隔夜刷新",
    },
    {
        "phases": ("auction",),
        "time_cn": "09:26",
        "label": "竞价最终版本",
    },
)
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

    async def build_review_artifact(
        self,
        review_id: int,
        *,
        phase: str,
    ) -> ObsidianArtifact:
        review_id = _positive_integer(review_id, "review_id")
        if review_id > _MAX_DATABASE_INTEGER:
            raise ValueError("review_id is outside the database integer range")
        if phase not in _REVIEW_PHASES:
            raise ValueError(
                "review phase must be initial_review or final_review"
            )

        async with self._session_factory() as db:
            review = await db.get(TradingExecutionReview, review_id)
            if review is None:
                raise LookupError(f"trading execution review {review_id} was not found")
            plan_id = _positive_integer(
                review.plan_version_id,
                f"review {review_id} plan_version_id",
            )
            plan = await db.get(TradingPlanVersion, plan_id)
            if plan is None:
                raise ValueError(
                    f"review {review_id} references missing plan {plan_id}"
                )
            candidates = list(
                (
                    await db.scalars(
                        select(TradingPlanCandidate)
                        .where(TradingPlanCandidate.plan_version_id == plan_id)
                        .order_by(
                            TradingPlanCandidate.rank,
                            TradingPlanCandidate.id,
                        )
                    )
                ).all()
            )
            if len(candidates) > 3:
                raise ValueError(
                    f"trading plan version {plan_id} has more than 3 candidates"
                )
            self._validate_plan_data(plan, candidates)
            self._validate_review_data(
                review,
                plan,
                candidates,
                phase=phase,
            )
            return self._build_review_artifact(review, plan, phase=phase)

    async def build_alerts_artifact(
        self,
        trade_date: date,
    ) -> ObsidianArtifact:
        trade_date = _database_date(trade_date, "trade_date")
        day_start = datetime.combine(trade_date, time.min)
        day_end = day_start + timedelta(days=1)
        async with self._session_factory() as db:
            events = list(
                (
                    await db.scalars(
                        select(TradingAlertEvent)
                        .where(
                            TradingAlertEvent.triggered_at >= day_start,
                            TradingAlertEvent.triggered_at < day_end,
                        )
                        .order_by(
                            TradingAlertEvent.triggered_at,
                            TradingAlertEvent.id,
                        )
                    )
                ).all()
            )
            plan_ids = {
                _positive_integer(event.plan_version_id, "alert plan_version_id")
                for event in events
            }
            candidate_ids = {
                _positive_integer(event.candidate_id, "alert candidate_id")
                for event in events
                if event.candidate_id is not None
            }
            persisted_plan_ids: set[int] = set()
            if plan_ids:
                persisted_plan_ids = set(
                    await db.scalars(
                        select(TradingPlanVersion.id).where(
                            TradingPlanVersion.id.in_(sorted(plan_ids))
                        )
                    )
                )
            if plan_ids - persisted_plan_ids:
                missing = min(plan_ids - persisted_plan_ids)
                raise ValueError(f"alert references missing plan {missing}")
            candidate_owners: dict[int, int] = {}
            if candidate_ids:
                candidate_owners = dict(
                    (
                        await db.execute(
                            select(
                                TradingPlanCandidate.id,
                                TradingPlanCandidate.plan_version_id,
                            ).where(
                                TradingPlanCandidate.id.in_(
                                    sorted(candidate_ids)
                                )
                            )
                        )
                    ).all()
                )
            for event in events:
                if event.candidate_id is None:
                    continue
                owner_id = candidate_owners.get(event.candidate_id)
                if owner_id is None:
                    raise ValueError(
                        f"alert {event.id} references missing candidate {event.candidate_id}"
                    )
                if owner_id != event.plan_version_id:
                    raise ValueError(
                        f"alert {event.id} candidate does not belong to its plan"
                    )
            timeline = [self._alert_payload(event) for event in events]

        return ObsidianArtifact(
            snapshot_key=f"alerts:{trade_date.isoformat()}",
            trade_date=trade_date,
            entity_type="alerts",
            entity_id=None,
            phase="reconcile",
            target_path=(
                "30_TradingPlaybook/Alerts/Auto/"
                f"{trade_date.year}/{trade_date.isoformat()}.md"
            ),
            immutable=False,
            payload={
                "type": "trading_alert_timeline",
                "trade_date": trade_date,
                "timeline": timeline,
                "manual_required": True,
                "auto_execute": False,
            },
        )

    async def build_daily_index_artifact(
        self,
        trade_date: date,
    ) -> ObsidianArtifact:
        trade_date = _database_date(trade_date, "trade_date")
        async with self._session_factory() as db:
            plans = list(
                (
                    await db.scalars(
                        select(TradingPlanVersion)
                        .where(
                            TradingPlanVersion.target_trade_date == trade_date
                        )
                        .order_by(
                            TradingPlanVersion.generated_at,
                            TradingPlanVersion.version_no,
                            TradingPlanVersion.id,
                        )
                    )
                ).all()
            )
            plan_ids = [
                _positive_integer(plan.id, "plan id") for plan in plans
            ]
            candidates_by_plan: dict[int, list[TradingPlanCandidate]] = {
                plan_id: [] for plan_id in plan_ids
            }
            if plan_ids:
                candidates = list(
                    (
                        await db.scalars(
                            select(TradingPlanCandidate)
                            .where(
                                TradingPlanCandidate.plan_version_id.in_(
                                    plan_ids
                                )
                            )
                            .order_by(
                                TradingPlanCandidate.plan_version_id,
                                TradingPlanCandidate.rank,
                                TradingPlanCandidate.id,
                            )
                        )
                    ).all()
                )
                for candidate in candidates:
                    candidates_by_plan[candidate.plan_version_id].append(
                        candidate
                    )

            for plan in plans:
                candidates = candidates_by_plan[plan.id]
                if len(candidates) > 3:
                    raise ValueError(
                        f"trading plan version {plan.id} has more than 3 candidates"
                    )
                self._validate_plan_data(plan, candidates)

            current_effective_id = self._current_effective_plan_id(
                plans,
                trade_date=trade_date,
            )
            plan_payloads: list[dict[str, object]] = []
            for plan in plans:
                plan_payloads.append(
                    self._daily_plan_payload(
                        plan,
                        candidates_by_plan[plan.id],
                        current_effective_id=current_effective_id,
                    )
                )

        return ObsidianArtifact(
            snapshot_key=f"daily-index:{trade_date.isoformat()}",
            trade_date=trade_date,
            entity_type="daily_index",
            entity_id=None,
            phase="reconcile",
            target_path=(
                "30_TradingPlaybook/Daily/Auto/"
                f"{trade_date.year}/{trade_date.isoformat()}/index.md"
            ),
            immutable=False,
            payload={
                "type": "trading_daily_index",
                "trade_date": trade_date,
                "current_effective_plan_version_id": current_effective_id,
                "plan_versions": plan_payloads,
                "stage_schedule": _STAGE_SCHEDULE,
                "manual_required": True,
                "auto_execute": False,
            },
        )

    async def build_dashboard_artifact(
        self,
        trade_date: date,
    ) -> ObsidianArtifact:
        trade_date = _database_date(trade_date, "trade_date")
        iso_date = trade_date.isoformat()
        year = trade_date.year
        return ObsidianArtifact(
            snapshot_key="dashboard:trading-playbook",
            trade_date=trade_date,
            entity_type="dashboard",
            entity_id=None,
            phase="reconcile",
            target_path="Dashboards/交易预案.md",
            immutable=False,
            payload={
                "type": "trading_playbook_dashboard",
                "trade_date": trade_date,
                "navigation": {
                    "daily_index": (
                        "[[30_TradingPlaybook/Daily/Auto/"
                        f"{year}/{iso_date}/index]]"
                    ),
                    "alerts": (
                        "[[30_TradingPlaybook/Alerts/Auto/"
                        f"{year}/{iso_date}]]"
                    ),
                    "notes": (
                        "[[30_TradingPlaybook/Notes/"
                        f"{year}/{iso_date}]]"
                    ),
                },
                "dataview_queries": [
                    'TABLE stage, status, source_trade_date, target_trade_date FROM "30_TradingPlaybook/Daily/Auto" SORT generated_at DESC',
                    'TABLE event_type, severity, triggered_at FROM "30_TradingPlaybook/Alerts/Auto" SORT triggered_at DESC',
                    'TABLE phase, plan_version_id, finalized_at FROM "30_TradingPlaybook/Reviews/Auto" SORT date DESC',
                ],
                "manual_required": True,
                "auto_execute": False,
            },
        )

    async def build_stage_artifacts(
        self,
        *,
        trade_date: date,
        phase: str,
        plan_version_ids: Sequence[int] = (),
        review_ids: Sequence[int] = (),
        include_rules: bool = False,
    ) -> tuple[ObsidianArtifact, ...]:
        trade_date = _database_date(trade_date, "trade_date")
        if phase not in OBSIDIAN_PHASES:
            raise ValueError(f"phase must be one of {OBSIDIAN_PHASES}")
        if type(include_rules) is not bool:
            raise ValueError("include_rules must be a boolean")
        plan_ids = self._normalized_ids(
            plan_version_ids,
            field_name="plan_version_ids",
        )
        normalized_review_ids = self._normalized_ids(
            review_ids,
            field_name="review_ids",
        )
        if plan_ids and phase not in _PLAN_STAGES:
            raise ValueError(
                f"plan_version_ids are not allowed for batch phase {phase}"
            )
        if normalized_review_ids and phase not in _REVIEW_PHASES:
            raise ValueError(
                f"review_ids are not allowed for batch phase {phase}"
            )

        artifacts: list[ObsidianArtifact] = []
        if include_rules:
            artifacts.extend(await self.build_rule_artifacts("v2"))
        for plan_version_id in plan_ids:
            artifact = await self.build_plan_artifact(plan_version_id)
            payload = artifact.payload_json()
            if artifact.phase != phase or payload["stage"] != phase:
                raise ValueError(
                    f"plan {plan_version_id} stage does not match batch phase {phase}"
                )
            if phase in {"preclose", "after_close"}:
                if payload["source_trade_date"] != trade_date.isoformat():
                    raise ValueError(
                        f"plan {plan_version_id} source_trade_date does not match batch trade_date"
                    )
            elif payload["target_trade_date"] != trade_date.isoformat():
                raise ValueError(
                    f"plan {plan_version_id} target_trade_date does not match batch trade_date"
                )
            artifacts.append(artifact)
        for review_id in normalized_review_ids:
            artifact = await self.build_review_artifact(
                review_id,
                phase=phase,
            )
            if artifact.trade_date != trade_date:
                raise ValueError(
                    f"review trade_date for {review_id} does not match batch trade_date"
                )
            artifacts.append(artifact)
        artifacts.append(await self.build_alerts_artifact(trade_date))
        artifacts.append(await self.build_daily_index_artifact(trade_date))
        artifacts.append(await self.build_dashboard_artifact(trade_date))
        return tuple(artifacts)

    @staticmethod
    def _normalized_ids(
        values: Sequence[int],
        *,
        field_name: str,
    ) -> tuple[int, ...]:
        if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
            raise ValueError(f"{field_name} must be a sequence of positive integers")
        normalized: set[int] = set()
        for value in values:
            normalized_value = _positive_integer(value, field_name)
            if normalized_value > _MAX_DATABASE_INTEGER:
                raise ValueError(
                    f"{field_name} contains an id outside the database integer range"
                )
            normalized.add(normalized_value)
        return tuple(sorted(normalized))

    @classmethod
    def _validate_review_data(
        cls,
        review: TradingExecutionReview,
        plan: TradingPlanVersion,
        candidates: list[TradingPlanCandidate],
        *,
        phase: str,
    ) -> None:
        review_id = _positive_integer(review.id, "review id")
        plan_id = _positive_integer(
            review.plan_version_id,
            f"review {review_id} plan_version_id",
        )
        if plan_id != _positive_integer(plan.id, "review plan id"):
            raise ValueError(
                f"review {review_id} plan identity does not match loaded plan"
            )
        if not (
            plan.status in {"active", "superseded"}
            or (plan.status == "expired" and plan.confirmed_at is not None)
        ):
            raise ValueError(
                f"review {review_id} plan is not in a review-relevant status"
            )
        review_trade_date = _database_date(
            review.trade_date,
            "review trade_date",
        )
        target_trade_date = _database_date(
            plan.target_trade_date,
            "review plan target_trade_date",
        )
        action_trade_dates = {
            _database_date(
                candidate.action_trade_date,
                f"review candidate {candidate.id} action_trade_date",
            )
            for candidate in candidates
        }
        if (
            review_trade_date != target_trade_date
            and review_trade_date not in action_trade_dates
        ):
            raise ValueError(
                f"review {review_id} review trade date is not relevant to plan {plan_id}"
            )
        generated_at = _database_datetime(
            review.generated_at,
            f"review {review_id} generated_at",
        )
        finalized_at = (
            _database_datetime(
                review.finalized_at,
                f"review {review_id} finalized_at",
            )
            if review.finalized_at is not None
            else None
        )
        if phase == "initial_review" and finalized_at is not None:
            raise ValueError(
                f"review {review_id} initial_review cannot use a finalized row"
            )
        if phase == "final_review" and finalized_at is None:
            raise ValueError(
                f"review {review_id} final_review requires finalized_at"
            )
        if (
            finalized_at is not None
            and database_datetime_to_cn(finalized_at)
            < database_datetime_to_cn(generated_at)
        ):
            raise ValueError(
                f"review {review_id} finalized_at cannot precede generated_at"
            )
        for field_name, value in (
            ("signal_review", review.signal_review_json),
            ("manual_execution", review.manual_execution_json),
            ("plan_compliance", review.plan_compliance_json),
            ("outcome_snapshot", review.outcome_snapshot_json),
            ("data_quality", review.data_quality_json),
        ):
            if type(value) is not dict:
                raise ValueError(
                    f"review {review_id} {field_name} must be a JSON object"
                )

    @staticmethod
    def _build_review_artifact(
        review: TradingExecutionReview,
        plan: TradingPlanVersion,
        *,
        phase: str,
    ) -> ObsidianArtifact:
        review_id = _positive_integer(review.id, "review id")
        plan_id = _positive_integer(review.plan_version_id, "review plan_version_id")
        trade_date = _database_date(review.trade_date, "review trade_date")
        generated_at = _database_datetime(
            review.generated_at,
            "review generated_at",
        )
        finalized_at = (
            _database_datetime(review.finalized_at, "review finalized_at")
            if review.finalized_at is not None
            else None
        )
        kind = "initial" if phase == "initial_review" else "final"
        return ObsidianArtifact(
            snapshot_key=f"review:{review_id}:{kind}",
            trade_date=trade_date,
            entity_type="review",
            entity_id=review_id,
            phase=phase,
            target_path=(
                "30_TradingPlaybook/Reviews/Auto/"
                f"{trade_date.year}/{trade_date.isoformat()}/"
                f"{kind}-review-{plan_id}.md"
            ),
            immutable=True,
            payload={
                "type": "trading_execution_review",
                "review_id": review_id,
                "phase": phase,
                "trade_date": trade_date,
                "plan_version_id": plan_id,
                "plan_version": {
                    "version_no": _positive_integer(
                        plan.version_no,
                        "review plan version_no",
                    ),
                    "stage": plan.stage,
                    "status": plan.status,
                    "source_trade_date": _database_date(
                        plan.source_trade_date,
                        "review plan source_trade_date",
                    ),
                    "target_trade_date": _database_date(
                        plan.target_trade_date,
                        "review plan target_trade_date",
                    ),
                },
                "signal_review": review.signal_review_json,
                "manual_execution": review.manual_execution_json,
                "plan_compliance": review.plan_compliance_json,
                "outcome_snapshot": review.outcome_snapshot_json,
                "data_quality": review.data_quality_json,
                "generated_at": database_datetime_to_cn(generated_at),
                "finalized_at": database_datetime_to_cn(finalized_at),
                "manual_required": True,
                "auto_execute": False,
            },
        )

    @classmethod
    def _alert_payload(cls, event: TradingAlertEvent) -> dict[str, object]:
        alert_id = _positive_integer(event.id, "alert id")
        plan_id = _positive_integer(
            event.plan_version_id,
            f"alert {alert_id} plan_version_id",
        )
        candidate_id = (
            _positive_integer(
                event.candidate_id,
                f"alert {alert_id} candidate_id",
            )
            if event.candidate_id is not None
            else None
        )
        for field_name, value in (
            ("event_type", event.event_type),
            ("severity", event.severity),
            ("message", event.message),
        ):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"alert {alert_id} {field_name} must be nonempty")
        expected_severity = _ALERT_EVENT_SEVERITIES.get(event.event_type)
        if expected_severity is None:
            raise ValueError(f"alert {alert_id} event_type is unsupported")
        if event.severity != expected_severity:
            raise ValueError(
                f"alert {alert_id} severity does not match event_type"
            )
        triggered_at = _database_datetime(
            event.triggered_at,
            f"alert {alert_id} triggered_at",
        )
        acknowledged_at = (
            _database_datetime(
                event.acknowledged_at,
                f"alert {alert_id} acknowledged_at",
            )
            if event.acknowledged_at is not None
            else None
        )
        if (
            acknowledged_at is not None
            and database_datetime_to_cn(acknowledged_at)
            < database_datetime_to_cn(triggered_at)
        ):
            raise ValueError(
                f"alert {alert_id} acknowledged_at cannot precede triggered_at"
            )
        if type(event.market_snapshot_json) is not dict:
            raise ValueError(
                f"alert {alert_id} market_snapshot must be a JSON object"
            )
        safe_market_facts = cls._safe_alert_market_facts(
            event.event_type,
            event.market_snapshot_json,
            alert_id=alert_id,
        )
        if type(event.channel_status_json) is not dict:
            raise ValueError(
                f"alert {alert_id} channel_status must be a JSON object"
            )
        in_app = event.channel_status_json.get("in_app")
        if type(in_app) is not dict:
            raise ValueError(
                f"alert {alert_id} in_app channel status must be a JSON object"
            )
        channel_status = in_app.get("status")
        if channel_status not in _IN_APP_CHANNEL_STATUSES:
            raise ValueError(
                f"alert {alert_id} in_app channel status is malformed"
            )
        attempts = in_app.get("attempts")
        if attempts is not None and (
            isinstance(attempts, bool)
            or not isinstance(attempts, int)
            or attempts < 0
        ):
            raise ValueError(f"alert {alert_id} in_app attempts is malformed")
        safe_status: dict[str, object] = {}
        for field_name in _IN_APP_EXPORT_FIELDS:
            if field_name not in in_app:
                continue
            value = in_app[field_name]
            if field_name == "status":
                safe_status[field_name] = channel_status
                continue
            if field_name == "attempts":
                if attempts is not None:
                    safe_status[field_name] = attempts
                continue
            if field_name == "accepted":
                if type(value) is not bool:
                    raise ValueError(
                        f"alert {alert_id} in_app accepted is malformed"
                    )
                safe_status[field_name] = value
                continue
            if field_name in _IN_APP_TIMESTAMP_FIELDS:
                if not isinstance(value, str) or not value.strip():
                    raise ValueError(
                        f"alert {alert_id} in_app {field_name} is malformed"
                    )
                try:
                    parsed = datetime.fromisoformat(
                        value.replace("Z", "+00:00")
                    )
                except ValueError as exc:
                    raise ValueError(
                        f"alert {alert_id} in_app {field_name} is malformed"
                    ) from exc
                if parsed.tzinfo is None or parsed.utcoffset() is None:
                    raise ValueError(
                        f"alert {alert_id} in_app {field_name} must be timezone-aware"
                    )
                safe_status[field_name] = value
                continue
            if not isinstance(value, (str, bool, int)):
                raise ValueError(
                    f"alert {alert_id} in_app {field_name} is malformed"
                )
            safe_status[field_name] = value

        if acknowledged_at is not None:
            timeline_state = "confirmed"
        elif channel_status in {"uncertain", "failed", "skipped"}:
            timeline_state = "failed"
        elif (
            event.event_type == "confirmation_required"
            or channel_status in {"pending", "sending"}
        ):
            timeline_state = "pending_confirmation"
        elif channel_status == "delivered":
            timeline_state = "delivered"
        else:
            raise ValueError(
                f"alert {alert_id} cannot map in_app status to timeline state"
            )
        payload = {
            "alert_id": alert_id,
            "event_type": event.event_type,
            "severity": event.severity,
            "timeline_state": timeline_state,
            "triggered_at": database_datetime_to_cn(triggered_at),
            "plan_version_id": plan_id,
            "candidate_id": candidate_id,
            "message": event.message,
            "market_facts": safe_market_facts,
            "in_app_status": safe_status,
            "acknowledged_at": database_datetime_to_cn(acknowledged_at),
        }
        return payload

    @staticmethod
    def _safe_alert_market_facts(
        event_type: str,
        market_snapshot: dict[str, object],
        *,
        alert_id: int,
    ) -> dict[str, object]:
        if event_type in _PLAN_ALERT_EVENT_TYPES:
            field_names = _PLAN_ALERT_MARKET_FIELDS
        elif event_type in _ACTION_ALERT_EVENT_TYPES:
            field_names = _ACTION_ALERT_MARKET_FIELDS
        else:
            raise ValueError(f"alert {alert_id} event_type is unsupported")

        result: dict[str, object] = {}
        for field_name in field_names:
            if field_name not in market_snapshot:
                continue
            value = market_snapshot[field_name]
            if value is None or isinstance(value, (dict, list, tuple, set, bytes)):
                raise ValueError(
                    f"alert {alert_id} market fact {field_name} must be scalar"
                )
            result[field_name] = value

        if event_type in _ACTION_ALERT_EVENT_TYPES and "quote" in market_snapshot:
            quote = market_snapshot["quote"]
            if type(quote) is not dict:
                raise ValueError(
                    f"alert {alert_id} market quote must be a JSON object"
                )
            safe_quote: dict[str, object] = {}
            for field_name in _ACTION_QUOTE_FIELDS:
                if field_name not in quote:
                    continue
                value = quote[field_name]
                if value is None or isinstance(
                    value,
                    (dict, list, tuple, set, bytes),
                ):
                    raise ValueError(
                        f"alert {alert_id} quote fact {field_name} must be scalar"
                    )
                safe_quote[field_name] = value
            result["quote"] = safe_quote
        return result

    @staticmethod
    def _current_effective_plan_id(
        plans: Sequence[TradingPlanVersion],
        *,
        trade_date: date,
    ) -> int | None:
        active_plans = [plan for plan in plans if plan.status == "active"]
        if len(active_plans) > 1:
            raise ValueError(
                f"trade date {trade_date} has multiple active plan versions"
            )
        status_precedence = {"active": 3, "confirmed": 2, "draft": 1}
        eligible = [
            plan for plan in plans if plan.status in status_precedence
        ]
        if not eligible:
            return None

        def precedence_key(
            plan: TradingPlanVersion,
        ) -> tuple[int, int, datetime, int]:
            generated_at = _database_datetime(
                plan.generated_at,
                f"plan {plan.id} generated_at",
            )
            return (
                status_precedence[plan.status],
                _positive_integer(plan.version_no, "version_no"),
                database_datetime_to_cn(generated_at),
                _positive_integer(plan.id, "plan id"),
            )

        return max(eligible, key=precedence_key).id

    @staticmethod
    def _daily_plan_payload(
        plan: TradingPlanVersion,
        candidates: list[TradingPlanCandidate],
        *,
        current_effective_id: int | None,
    ) -> dict[str, object]:
        plan_id = _positive_integer(plan.id, "plan id")
        generated_at = _database_datetime(plan.generated_at, "plan generated_at")
        confirmed_at = (
            _database_datetime(plan.confirmed_at, "plan confirmed_at")
            if plan.confirmed_at is not None
            else None
        )
        return {
            "plan_version_id": plan_id,
            "version_no": _positive_integer(plan.version_no, "version_no"),
            "stage": plan.stage,
            "status": plan.status,
            "source_trade_date": _database_date(
                plan.source_trade_date,
                "source_trade_date",
            ),
            "target_trade_date": _database_date(
                plan.target_trade_date,
                "target_trade_date",
            ),
            "generated_at": database_datetime_to_cn(generated_at),
            "confirmed_at": database_datetime_to_cn(confirmed_at),
            "current_effective": plan_id == current_effective_id,
            "candidates": [
                {
                    "candidate_id": _positive_integer(
                        candidate.id,
                        "candidate id",
                    ),
                    "rank": _positive_integer(
                        candidate.rank,
                        "candidate rank",
                    ),
                    "stock_code": candidate.stock_code,
                    "stock_name": candidate.stock_name,
                    "action_trade_date": _database_date(
                        candidate.action_trade_date,
                        "action_trade_date",
                    ),
                }
                for candidate in candidates
            ],
        }

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
