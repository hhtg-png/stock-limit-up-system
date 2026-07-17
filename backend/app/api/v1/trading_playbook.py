"""REST API for the standalone, manually confirmed trading playbook."""

from __future__ import annotations

import asyncio
import copy
import math
import re
from collections.abc import Mapping
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import defer

from app.database import get_db
from app.models.trading_playbook import (
    TradingAlertEvent,
    TradingExecutionReview,
    TradingModeRule,
    TradingPlanVersion,
    TradingPlaybookSettings,
)
from app.schemas.trading_playbook import (
    ManualExecutionUpdate,
    PlanConfirmRequest,
    PlanGenerateRequest,
    PlanRevisionRequest,
    TradingPlaybookObsidianExportRequest,
    TradingPlaybookObsidianExportResponse,
    TradingPlaybookObsidianStatusResponse,
    TradingPlaybookSettingsUpdate,
)
from app.services.trading_playbook.errors import (
    InvalidRequestError,
    InvalidTransitionError,
    PlaybookNotFoundError,
    UnsafePlanDataError,
    UpstreamUnavailableError,
)
from app.services.trading_playbook.obsidian_types import (
    OBSIDIAN_PHASES,
    ObsidianSyncBatchResult,
    TRADING_PLAYBOOK_ALLOWED_ROOTS,
    contains_absolute_path_fragment,
)
from app.services.trading_playbook.plan_service import TradingPlanService
from app.services.trading_playbook.runtime import trading_playbook_runtime
from app.services.trading_playbook.serialization import (
    ValidatedPlanPayload,
    ValidatedSettingsPayload,
    json_value as _json_value,
    normalize_plan_payload as _normalize_plan_payload,
    normalize_settings_payload as _normalize_settings_payload,
)


router = APIRouter()
CN_TZ = ZoneInfo("Asia/Shanghai")
_plan_service = TradingPlanService()
INVALID_REQUEST_DETAIL = "Invalid trading playbook request"
STATE_CONFLICT_DETAIL = "Trading plan state conflict"
NOT_FOUND_DETAIL = "Trading plan not found"
SERVICE_UNAVAILABLE_DETAIL = "Trading playbook service is unavailable"
_OBSIDIAN_DASHBOARD_PATH = "Dashboards/交易预案.md"


def _service_unavailable() -> HTTPException:
    return HTTPException(
        status_code=503,
        detail=SERVICE_UNAVAILABLE_DETAIL,
    )


async def _rollback_quietly(db: AsyncSession) -> None:
    try:
        await db.rollback()
    except Exception:
        # Preserve the public error contract even when the broken connection
        # cannot complete its cleanup. Session close performs the final reset.
        pass


def get_trading_playbook_now() -> datetime:
    """Injectable Beijing clock shared by all API-side state changes."""
    return datetime.now(CN_TZ)


def get_trading_playbook_orchestrator(request: Request):
    """Return the one app-owned pipeline; Task 9 wires it during startup."""
    try:
        orchestrator = getattr(
            request.app.state,
            "trading_playbook_orchestrator",
            None,
        )
        if orchestrator is None:
            orchestrator = trading_playbook_runtime.get_orchestrator()
    except Exception as exc:
        raise _service_unavailable() from exc
    if orchestrator is None:
        raise HTTPException(
            status_code=503,
            detail=SERVICE_UNAVAILABLE_DETAIL,
        )
    return orchestrator


def get_trading_playbook_review_service(request: Request):
    """Return the app-owned review service once Task 11 installs it."""
    try:
        service = getattr(
            request.app.state,
            "trading_playbook_review_service",
            None,
        )
        if service is None:
            service = trading_playbook_runtime.get_review_service()
    except Exception as exc:
        raise _service_unavailable() from exc
    if service is None:
        raise HTTPException(
            status_code=503,
            detail=SERVICE_UNAVAILABLE_DETAIL,
        )
    return service


def get_personal_wechat_channel(request: Request):
    """Return the app-owned personal WeChat delivery channel."""
    try:
        channel = getattr(
            request.app.state,
            "trading_playbook_wxpusher_channel",
            None,
        )
    except Exception as exc:
        raise _service_unavailable() from exc
    if channel is None or not callable(getattr(channel, "status", None)):
        raise _service_unavailable()
    return channel


def get_trading_playbook_obsidian_sync(request: Request):
    """Return only the application-owned Obsidian coordinator."""

    try:
        coordinator = getattr(
            request.app.state,
            "trading_playbook_obsidian_sync",
            None,
        )
    except Exception as exc:
        raise _service_unavailable() from exc
    if coordinator is None or any(
        not callable(getattr(coordinator, method, None))
        for method in ("get_status", "export_trade_date")
    ):
        raise _service_unavailable()
    return coordinator


def _obsidian_relative_path(value: object) -> str:
    if type(value) is not str or not value or "\\" in value:
        raise ValueError("Obsidian response path is unsafe")
    if value.startswith("/") or re.match(r"^[A-Za-z]:", value):
        raise ValueError("Obsidian response path is unsafe")
    parts = value.split("/")
    if any(
        not part
        or part in {".", ".."}
        or any(ord(character) < 32 for character in part)
        for part in parts
    ):
        raise ValueError("Obsidian response path is unsafe")
    normalized = "/".join(parts)
    if not any(
        normalized == root or normalized.startswith(f"{root}/")
        for root in TRADING_PLAYBOOK_ALLOWED_ROOTS
    ):
        raise ValueError("Obsidian response path is outside its allowlist")
    return normalized


def _public_json(value: object, *, depth: int = 0) -> Any:
    if depth > 8:
        raise ValueError("Obsidian response JSON is too deeply nested")
    if value is None or type(value) in {bool, int}:
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError("Obsidian response JSON must be finite")
        return value
    if type(value) is str:
        safe = "".join(
            " " if character in "\r\n\t" else character
            for character in value
            if ord(character) >= 32
        )[:2000]
        return (
            "redacted"
            if contains_absolute_path_fragment(safe)
            else safe
        )
    if isinstance(value, Mapping):
        if len(value) > 64:
            raise ValueError("Obsidian response JSON has too many items")
        result: dict[str, Any] = {}
        for key, item in value.items():
            if (
                type(key) is not str
                or not key
                or len(key) > 128
                or any(ord(character) < 32 for character in key)
                or contains_absolute_path_fragment(key)
            ):
                raise ValueError("Obsidian response JSON key is unsafe")
            result[key] = _public_json(item, depth=depth + 1)
        return result
    if type(value) in {list, tuple}:
        if len(value) > 64:
            raise ValueError("Obsidian response JSON has too many items")
        return [_public_json(item, depth=depth + 1) for item in value]
    raise ValueError("Obsidian response JSON contains an unsupported value")


def _serialize_obsidian_status(
    value: object,
) -> TradingPlaybookObsidianStatusResponse:
    if not isinstance(value, Mapping):
        raise ValueError("Obsidian status must be a mapping")
    payload = dict(value)
    recent = payload.get("recent_files")
    if type(recent) is not list:
        raise ValueError("Obsidian recent files must be a list")
    payload["recent_files"] = [
        _obsidian_relative_path(item) for item in recent
    ]
    if payload.get("dashboard_path") != _OBSIDIAN_DASHBOARD_PATH:
        raise ValueError("Obsidian dashboard path is unsafe")
    last_error = payload.get("last_error")
    if last_error is not None:
        if type(last_error) is not str or contains_absolute_path_fragment(
            last_error
        ):
            raise ValueError("Obsidian status error is unsafe")
    return TradingPlaybookObsidianStatusResponse.model_validate(
        payload,
        strict=True,
    )


def _serialize_obsidian_export(
    value: object,
) -> TradingPlaybookObsidianExportResponse:
    if isinstance(value, ObsidianSyncBatchResult):
        payload = {
            "trade_date": value.trade_date,
            "phase": value.phase,
            "written_files": value.written_files,
            "skipped_files": value.skipped_files,
            "pending_files": value.pending_files,
            "failed_files": value.failed_files,
            "git_status": value.git_status_json(),
        }
    elif isinstance(value, Mapping):
        required = {
            "trade_date",
            "phase",
            "written_files",
            "skipped_files",
            "pending_files",
            "failed_files",
            "git_status",
        }
        if set(value) != required:
            raise ValueError("Obsidian export result has an invalid structure")
        payload = dict(value)
    else:
        raise ValueError("Obsidian export result has an invalid type")
    if type(payload["trade_date"]) is not date:
        raise ValueError("Obsidian export trade date is invalid")
    if payload["phase"] not in OBSIDIAN_PHASES:
        raise ValueError("Obsidian export phase is invalid")
    serialized: dict[str, Any] = {
        "trade_date": payload["trade_date"],
        "phase": payload["phase"],
    }
    for field in (
        "written_files",
        "skipped_files",
        "pending_files",
        "failed_files",
    ):
        files = payload[field]
        if type(files) not in {list, tuple}:
            raise ValueError("Obsidian export files must be a list or tuple")
        serialized[field] = [
            _obsidian_relative_path(item) for item in files
        ]
    git_status = _public_json(payload["git_status"])
    if type(git_status) is not dict:
        raise ValueError("Obsidian Git status must be an object")
    serialized["git_status"] = git_status
    failed_count = len(serialized["failed_files"])
    pending_count = len(serialized["pending_files"])
    if failed_count and pending_count:
        error_summary = f"{failed_count} failed, {pending_count} pending"
    elif failed_count:
        error_summary = f"{failed_count} failed"
    elif pending_count:
        error_summary = f"{pending_count} pending"
    else:
        error_summary = None
    serialized["error_summary"] = error_summary
    return TradingPlaybookObsidianExportResponse.model_validate(
        serialized,
        strict=True,
    )


@router.get(
    "/obsidian/status",
    response_model=TradingPlaybookObsidianStatusResponse,
    summary="查询交易预案 Obsidian 同步状态",
)
async def get_obsidian_status(
    coordinator: Any = Depends(get_trading_playbook_obsidian_sync),
):
    try:
        return _serialize_obsidian_status(await coordinator.get_status())
    except HTTPException:
        raise
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        raise _service_unavailable() from exc


@router.post(
    "/obsidian/export",
    response_model=TradingPlaybookObsidianExportResponse,
    summary="手动导出交易预案到 Obsidian",
)
async def export_obsidian_trade_date(
    request: TradingPlaybookObsidianExportRequest,
    coordinator: Any = Depends(get_trading_playbook_obsidian_sync),
):
    try:
        result = await coordinator.export_trade_date(
            request.trade_date,
            include_rules=request.include_rules,
            force=request.force,
        )
        return _serialize_obsidian_export(result)
    except HTTPException:
        raise
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        raise _service_unavailable() from exc


def _china_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        value = value.replace(tzinfo=CN_TZ)
    else:
        value = value.astimezone(CN_TZ)
    return value.isoformat()


async def _serialize_plan_response(
    db: AsyncSession,
    plan_or_id: TradingPlanVersion | int,
) -> dict[str, Any] | None:
    try:
        payload = await _plan_service.serialize(db, plan_or_id)
        if payload is None:
            return None
        if isinstance(payload, ValidatedPlanPayload):
            return payload
        return _normalize_plan_payload(payload)
    except HTTPException:
        raise
    except Exception as exc:
        raise _service_unavailable() from exc


def _priority(rule: TradingModeRule) -> tuple[int, float]:
    prerequisites = rule.prerequisites_json
    if not isinstance(prerequisites, Mapping):
        return (1, 0)
    value = prerequisites.get("priority")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return (1, 0)
    try:
        priority = float(value)
    except (OverflowError, TypeError, ValueError):
        return (1, 0)
    if not math.isfinite(priority):
        return (1, 0)
    return (0, -priority)


def _serialize_rule(rule: TradingModeRule) -> dict[str, Any]:
    return {
        "id": rule.id,
        "mode_key": rule.mode_key,
        "version": rule.version,
        "name": rule.name,
        "family": rule.family,
        "style": rule.style,
        "window": rule.window,
        "automation_level": rule.automation_level,
        "description": rule.description,
        "prerequisites_json": _json_value(rule.prerequisites_json),
        "candidate_filters_json": _json_value(rule.candidate_filters_json),
        "entry_trigger_json": _json_value(rule.entry_trigger_json),
        "invalidation_json": _json_value(rule.invalidation_json),
        "exit_trigger_json": _json_value(rule.exit_trigger_json),
        "risk_guidance_json": _json_value(rule.risk_guidance_json),
        "source_refs_json": _json_value(rule.source_refs_json),
        "enabled": bool(rule.enabled),
        "content_hash": rule.content_hash,
        "created_at": _china_iso(rule.created_at),
    }


def _serialize_alert(alert: TradingAlertEvent) -> dict[str, Any]:
    return {
        "id": alert.id,
        "plan_version_id": alert.plan_version_id,
        "candidate_id": alert.candidate_id,
        "event_type": alert.event_type,
        "severity": alert.severity,
        "dedup_key": alert.dedup_key,
        "triggered_at": _china_iso(alert.triggered_at),
        "market_snapshot_json": _json_value(alert.market_snapshot_json or {}),
        "message": alert.message,
        "channel_status_json": _json_value(alert.channel_status_json or {}),
        "acknowledged_at": _china_iso(alert.acknowledged_at),
    }


def _serialize_settings(row: Any) -> ValidatedSettingsPayload:
    if isinstance(row, ValidatedSettingsPayload):
        return row
    return _normalize_settings_payload(row)


def _serialize_review(review: Any) -> dict[str, Any]:
    if isinstance(review, Mapping):
        return _json_value(copy.deepcopy(dict(review)))
    fields = (
        "id",
        "trade_date",
        "plan_version_id",
        "signal_review_json",
        "manual_execution_json",
        "plan_compliance_json",
        "outcome_snapshot_json",
        "data_quality_json",
        "generated_at",
        "finalized_at",
    )
    return _json_value({field: getattr(review, field) for field in fields})


async def _settings_row(
    db: AsyncSession,
) -> TradingPlaybookSettings:
    return await _plan_service.get_settings(db)


@router.get("/rules", summary="查询交易模式规则")
async def list_rules(db: AsyncSession = Depends(get_db)):
    try:
        rows = list(
            (
                await db.scalars(
                    select(TradingModeRule).where(
                        TradingModeRule.enabled.is_(True)
                    )
                )
            ).all()
        )
        latest_by_mode: dict[str, TradingModeRule] = {}
        for row in rows:
            mode_key = str(row.mode_key or "")
            previous = latest_by_mode.get(mode_key)
            if previous is None or (row.version, row.id) > (
                previous.version,
                previous.id,
            ):
                latest_by_mode[mode_key] = row
        rows = list(latest_by_mode.values())
        rows.sort(
            key=lambda row: (
                str(row.family or ""),
                _priority(row),
                str(row.mode_key or ""),
                row.version,
                row.id,
            )
        )
        return {"items": [_serialize_rule(row) for row in rows]}
    except HTTPException:
        raise
    except Exception as exc:
        raise _service_unavailable() from exc


@router.post("/plans/generate", summary="补跑指定预案阶段")
async def generate_plan(
    request: PlanGenerateRequest,
    db: AsyncSession = Depends(get_db),
    orchestrator: Any = Depends(get_trading_playbook_orchestrator),
    now: datetime = Depends(get_trading_playbook_now),
):
    if now.tzinfo is None or now.utcoffset() is None:
        raise _service_unavailable()
    try:
        result = await orchestrator.build_stage(
            db,
            request.source_trade_date,
            request.stage,
            now.astimezone(CN_TZ),
        )
        if isinstance(result, TradingPlanVersion):
            return await _serialize_plan_response(db, result)
        if isinstance(result, ValidatedPlanPayload):
            return result
        if isinstance(result, Mapping):
            return _normalize_plan_payload(result)
        raise _service_unavailable()
    except HTTPException:
        await _rollback_quietly(db)
        raise
    except InvalidRequestError as exc:
        await _rollback_quietly(db)
        raise HTTPException(
            status_code=422,
            detail=INVALID_REQUEST_DETAIL,
        ) from exc
    except PlaybookNotFoundError as exc:
        await _rollback_quietly(db)
        raise HTTPException(status_code=404, detail=NOT_FOUND_DETAIL) from exc
    except InvalidTransitionError as exc:
        await _rollback_quietly(db)
        raise HTTPException(
            status_code=409,
            detail=STATE_CONFLICT_DETAIL,
        ) from exc
    except IntegrityError as exc:
        await _rollback_quietly(db)
        raise HTTPException(status_code=409, detail="Plan generation conflict") from exc
    except (
        UnsafePlanDataError,
        UpstreamUnavailableError,
        ValueError,
        RuntimeError,
        ConnectionError,
        TimeoutError,
        OSError,
    ) as exc:
        await _rollback_quietly(db)
        raise _service_unavailable() from exc
    except Exception as exc:
        await _rollback_quietly(db)
        raise _service_unavailable() from exc


@router.get("/plans", summary="查询交易日全部预案版本")
async def list_plans(
    trade_date: date = Query(...),
    db: AsyncSession = Depends(get_db),
):
    try:
        plans = list(
            (
                await db.scalars(
                    select(TradingPlanVersion)
                    .where(TradingPlanVersion.target_trade_date == trade_date)
                    .options(
                        defer(
                            TradingPlanVersion.mode_radar_json,
                            raiseload=True,
                        )
                    )
                    .order_by(
                        TradingPlanVersion.generated_at.desc(),
                        TradingPlanVersion.id.desc(),
                    )
                )
            ).all()
        )
        return {
            "items": [
                _serialize_plan_list_item(
                    await _serialize_plan_response(db, plan)
                )
                for plan in plans
            ]
        }
    except HTTPException:
        await _rollback_quietly(db)
        raise
    except Exception as exc:
        raise _service_unavailable() from exc


@router.get("/plans/latest-target-date", summary="查询最新预案目标交易日")
async def get_latest_plan_target_date(db: AsyncSession = Depends(get_db)):
    try:
        target_trade_date = await db.scalar(
            select(TradingPlanVersion.target_trade_date)
            .order_by(
                TradingPlanVersion.target_trade_date.desc(),
                TradingPlanVersion.generated_at.desc(),
                TradingPlanVersion.id.desc(),
            )
            .limit(1)
        )
        return {
            "target_trade_date": (
                target_trade_date.isoformat()
                if isinstance(target_trade_date, date)
                else None
            )
        }
    except Exception as exc:
        raise _service_unavailable() from exc


@router.get("/plans/{plan_id}", summary="查询预案详情")
async def get_plan(plan_id: int, db: AsyncSession = Depends(get_db)):
    try:
        payload = await _serialize_plan_response(db, plan_id)
        if payload is None:
            raise HTTPException(status_code=404, detail=NOT_FOUND_DETAIL)
        return payload
    except HTTPException:
        raise
    except Exception as exc:
        raise _service_unavailable() from exc


@router.put("/plans/{plan_id}", summary="创建人工修订版本")
async def revise_plan(
    plan_id: int,
    request: PlanRevisionRequest,
    db: AsyncSession = Depends(get_db),
):
    changes = request.model_dump(mode="python", exclude_unset=True)
    try:
        await _serialize_plan_response(db, plan_id)
        child = await _plan_service.revise(db, plan_id, changes)
        return child
    except HTTPException:
        await _rollback_quietly(db)
        raise
    except PlaybookNotFoundError as exc:
        await _rollback_quietly(db)
        raise HTTPException(status_code=404, detail=NOT_FOUND_DETAIL) from exc
    except InvalidRequestError as exc:
        await _rollback_quietly(db)
        raise HTTPException(
            status_code=422,
            detail=INVALID_REQUEST_DETAIL,
        ) from exc
    except InvalidTransitionError as exc:
        await _rollback_quietly(db)
        raise HTTPException(
            status_code=409,
            detail=STATE_CONFLICT_DETAIL,
        ) from exc
    except IntegrityError as exc:
        await _rollback_quietly(db)
        raise HTTPException(status_code=409, detail="Plan revision conflict") from exc
    except Exception as exc:
        await _rollback_quietly(db)
        raise _service_unavailable() from exc


@router.post("/plans/{plan_id}/confirm", summary="确认并激活预案")
async def confirm_plan(
    plan_id: int,
    request: PlanConfirmRequest,
    db: AsyncSession = Depends(get_db),
):
    try:
        await _serialize_plan_response(db, plan_id)
        plan = await _plan_service.confirm(db, plan_id, request.confirmed_by)
        return plan
    except HTTPException:
        await _rollback_quietly(db)
        raise
    except PlaybookNotFoundError as exc:
        await _rollback_quietly(db)
        raise HTTPException(status_code=404, detail=NOT_FOUND_DETAIL) from exc
    except InvalidRequestError as exc:
        await _rollback_quietly(db)
        raise HTTPException(
            status_code=422,
            detail=INVALID_REQUEST_DETAIL,
        ) from exc
    except InvalidTransitionError as exc:
        await _rollback_quietly(db)
        raise HTTPException(
            status_code=409,
            detail=STATE_CONFLICT_DETAIL,
        ) from exc
    except IntegrityError as exc:
        await _rollback_quietly(db)
        raise HTTPException(status_code=409, detail="Plan confirmation conflict") from exc
    except Exception as exc:
        await _rollback_quietly(db)
        raise _service_unavailable() from exc


@router.post("/plans/{plan_id}/cancel", summary="取消预案")
async def cancel_plan(plan_id: int, db: AsyncSession = Depends(get_db)):
    try:
        await _serialize_plan_response(db, plan_id)
        plan = await _plan_service.cancel(db, plan_id)
        return plan
    except HTTPException:
        await _rollback_quietly(db)
        raise
    except PlaybookNotFoundError as exc:
        await _rollback_quietly(db)
        raise HTTPException(status_code=404, detail=NOT_FOUND_DETAIL) from exc
    except InvalidTransitionError as exc:
        await _rollback_quietly(db)
        raise HTTPException(
            status_code=409,
            detail=STATE_CONFLICT_DETAIL,
        ) from exc
    except UpstreamUnavailableError as exc:
        await _rollback_quietly(db)
        raise _service_unavailable() from exc
    except IntegrityError as exc:
        await _rollback_quietly(db)
        raise HTTPException(status_code=409, detail="Plan cancellation conflict") from exc
    except SQLAlchemyError as exc:
        await _rollback_quietly(db)
        raise _service_unavailable() from exc
    except Exception as exc:
        await _rollback_quietly(db)
        raise _service_unavailable() from exc


@router.get("/alerts", summary="查询独立交易预案提醒")
async def list_alerts(
    unread_only: bool = Query(False),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    try:
        statement = select(TradingAlertEvent)
        if unread_only:
            statement = statement.where(
                TradingAlertEvent.acknowledged_at.is_(None)
            )
        statement = statement.order_by(
            TradingAlertEvent.triggered_at.desc(),
            TradingAlertEvent.id.desc(),
        ).offset(offset).limit(limit)
        rows = list((await db.scalars(statement)).all())
        return {
            "items": [_serialize_alert(row) for row in rows],
            "limit": limit,
            "offset": offset,
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise _service_unavailable() from exc


@router.post("/alerts/{alert_id}/ack", summary="确认交易预案提醒已读")
async def acknowledge_alert(
    alert_id: int,
    db: AsyncSession = Depends(get_db),
    now: datetime = Depends(get_trading_playbook_now),
):
    try:
        alert = await db.get(TradingAlertEvent, alert_id)
        if alert is None:
            raise HTTPException(
                status_code=404,
                detail="Trading alert not found",
            )
        if alert.acknowledged_at is not None:
            return _serialize_alert(alert)
        if now.tzinfo is None or now.utcoffset() is None:
            raise _service_unavailable()
        alert.acknowledged_at = now.astimezone(CN_TZ)
        await db.commit()
        return _serialize_alert(alert)
    except HTTPException:
        await _rollback_quietly(db)
        raise
    except IntegrityError as exc:
        await _rollback_quietly(db)
        raise HTTPException(status_code=409, detail="Alert acknowledgement conflict") from exc
    except SQLAlchemyError as exc:
        await _rollback_quietly(db)
        raise _service_unavailable() from exc
    except Exception as exc:
        await _rollback_quietly(db)
        raise _service_unavailable() from exc


@router.put("/reviews/{trade_date}", summary="记录人工执行情况")
async def update_manual_execution(
    trade_date: date,
    request: ManualExecutionUpdate,
    plan_id: int | None = Query(None, gt=0),
    db: AsyncSession = Depends(get_db),
    review_service: Any = Depends(get_trading_playbook_review_service),
):
    executions = {
        key: value.model_dump(mode="python", exclude_unset=True)
        for key, value in request.executions.items()
    }
    unplanned_executions = [
        value.model_dump(mode="python", exclude_unset=True)
        for value in request.unplanned_executions
    ]
    try:
        review = await review_service.update_manual_execution(
            db,
            trade_date,
            executions,
            unplanned_executions=unplanned_executions,
            plan_version_id=plan_id,
        )
        return _serialize_review(review)
    except InvalidRequestError as exc:
        await _rollback_quietly(db)
        raise HTTPException(
            status_code=422,
            detail=INVALID_REQUEST_DETAIL,
        ) from exc
    except PlaybookNotFoundError as exc:
        await _rollback_quietly(db)
        raise HTTPException(status_code=404, detail=NOT_FOUND_DETAIL) from exc
    except InvalidTransitionError as exc:
        await _rollback_quietly(db)
        raise HTTPException(
            status_code=409,
            detail=STATE_CONFLICT_DETAIL,
        ) from exc
    except UpstreamUnavailableError as exc:
        await _rollback_quietly(db)
        raise _service_unavailable() from exc
    except ValueError as exc:
        await _rollback_quietly(db)
        raise _service_unavailable() from exc
    except IntegrityError as exc:
        await _rollback_quietly(db)
        raise HTTPException(status_code=409, detail="Review update conflict") from exc
    except SQLAlchemyError as exc:
        await _rollback_quietly(db)
        raise _service_unavailable() from exc
    except Exception as exc:
        await _rollback_quietly(db)
        raise _service_unavailable() from exc


@router.get("/reviews", summary="查询交易执行复盘")
async def list_reviews(
    trade_date: str = Query(..., pattern=r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$"),
    plan_id: int | None = Query(None, gt=0),
    db: AsyncSession = Depends(get_db),
):
    try:
        try:
            parsed_trade_date = date.fromisoformat(trade_date)
        except ValueError as exc:
            raise HTTPException(
                status_code=422,
                detail="trade_date must be a canonical ISO date",
            ) from exc
        if trade_date != parsed_trade_date.isoformat():
            raise HTTPException(
                status_code=422,
                detail="trade_date must be a canonical ISO date",
            )
        statement = select(TradingExecutionReview).where(
            TradingExecutionReview.trade_date == parsed_trade_date
        )
        if plan_id is not None:
            statement = statement.where(
                TradingExecutionReview.plan_version_id == plan_id
            )
        rows = list(
            (
                await db.scalars(
                    statement.order_by(
                        TradingExecutionReview.plan_version_id,
                        TradingExecutionReview.id,
                    )
                )
            ).all()
        )
        return {"items": [_serialize_review(row) for row in rows]}
    except HTTPException:
        await _rollback_quietly(db)
        raise
    except SQLAlchemyError as exc:
        await _rollback_quietly(db)
        raise _service_unavailable() from exc
    except Exception as exc:
        await _rollback_quietly(db)
        raise _service_unavailable() from exc


@router.get("/settings", summary="查询交易预案设置")
async def get_settings(
    db: AsyncSession = Depends(get_db),
    now: datetime = Depends(get_trading_playbook_now),
):
    try:
        row = await _settings_row(db)
        if now.tzinfo is None or now.utcoffset() is None:
            raise _service_unavailable()
        if row.wechat_enabled is not False:
            payload = await _plan_service.update_settings(
                db,
                {"wechat_enabled": False},
                now,
            )
        else:
            payload = _serialize_settings(row)
            await db.commit()
        return payload
    except HTTPException:
        await _rollback_quietly(db)
        raise
    except IntegrityError as exc:
        await _rollback_quietly(db)
        try:
            row = await db.get(TradingPlaybookSettings, 1)
        except Exception as read_exc:
            raise _service_unavailable() from read_exc
        if row is None:
            raise HTTPException(
                status_code=409,
                detail="Settings creation conflict",
            ) from exc
        try:
            return _serialize_settings(row)
        except Exception as serialize_exc:
            raise _service_unavailable() from serialize_exc
    except SQLAlchemyError as exc:
        await _rollback_quietly(db)
        raise _service_unavailable() from exc
    except UpstreamUnavailableError as exc:
        await _rollback_quietly(db)
        raise _service_unavailable() from exc
    except Exception as exc:
        await _rollback_quietly(db)
        raise _service_unavailable() from exc


@router.get(
    "/notifications/personal-wechat/status",
    summary="查询个人微信提醒配置状态",
)
async def get_personal_wechat_status(
    channel=Depends(get_personal_wechat_channel),
):
    try:
        payload = channel.status()
        if not isinstance(payload, Mapping):
            raise TypeError("personal WeChat status must be a mapping")
        return dict(payload)
    except HTTPException:
        raise
    except Exception as exc:
        raise _service_unavailable() from exc


def _serialize_plan_list_item(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return only fields consumed by the timeline and selected-plan view."""
    item = {
        key: value
        for key, value in payload.items()
        if key
        not in {
            "market_state",
            "theme_rankings",
            "mode_radar",
            "rule_snapshot",
            "risk_settings",
            "data_quality",
        }
    }
    market_state = payload.get("market_state_json")
    if isinstance(market_state, Mapping):
        item["market_state_json"] = {
            key: market_state[key]
            for key in ("style", "window")
            if key in market_state
        }
    else:
        item["market_state_json"] = {}
    item["theme_ranking_json"] = []
    item["rule_snapshot_json"] = []
    return item


@router.put("/settings", summary="修改交易预案设置")
async def update_settings(
    request: TradingPlaybookSettingsUpdate,
    db: AsyncSession = Depends(get_db),
    now: datetime = Depends(get_trading_playbook_now),
):
    try:
        patch = request.model_dump(mode="python", exclude_unset=True)
        row = await _plan_service.update_settings(db, patch, now)
        return _serialize_settings(row)
    except HTTPException:
        await _rollback_quietly(db)
        raise
    except InvalidRequestError as exc:
        await _rollback_quietly(db)
        raise HTTPException(
            status_code=422,
            detail=INVALID_REQUEST_DETAIL,
        ) from exc
    except UpstreamUnavailableError as exc:
        await _rollback_quietly(db)
        raise _service_unavailable() from exc
    except IntegrityError as exc:
        await _rollback_quietly(db)
        raise HTTPException(status_code=409, detail="Settings update conflict") from exc
    except SQLAlchemyError as exc:
        await _rollback_quietly(db)
        raise _service_unavailable() from exc
    except Exception as exc:
        await _rollback_quietly(db)
        raise _service_unavailable() from exc


__all__ = [
    "get_trading_playbook_now",
    "get_trading_playbook_orchestrator",
    "get_trading_playbook_review_service",
    "router",
]
