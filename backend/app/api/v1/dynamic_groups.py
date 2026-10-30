"""Dynamic-group management and database-only archive read endpoints."""
from datetime import date
from typing import Literal

from fastapi import APIRouter, Query

from app.schemas.dynamic_group import DynamicGroupDefinition, DynamicGroupUpdate
from app.schemas.dynamic_group_interpreter import DynamicGroupInterpretRequest, DynamicGroupInterpretResponse
from app.services.dynamic_group_interpreter_service import dynamic_group_interpreter_service
from app.services.dynamic_group_service import dynamic_group_service

router = APIRouter()


@router.get("/fields")
async def fields():
    from app.services.dynamic_group_rules import FIELD_DEFINITIONS
    return {"fields": FIELD_DEFINITIONS, "templates": [
        {"name": "强势股持续跟踪", "membership_mode": "retain", "match_mode": "all",
         "rules": [{"field": "change_pct", "op": "gte", "value": 5}]},
        {"name": "排除ST的涨停股", "membership_mode": "dynamic", "match_mode": "all",
         "rules": [{"field": "is_st", "op": "eq", "value": False},
                   {"field": "today_touched_limit_up", "op": "eq", "value": True}]},
    ]}


@router.get("")
@router.get("/", include_in_schema=False)
async def groups(q: str = Query("", max_length=100), folder: str | None = Query(None, max_length=100),
                 state: Literal["all", "active", "paused", "archived"] = "all",
                 sort: Literal["pinned", "updated", "name"] = "pinned",
                 offset: int = Query(0, ge=0), limit: int = Query(50, ge=1, le=100)):
    return await dynamic_group_service.list_groups(q=q, folder=folder, state=state, sort=sort, offset=offset, limit=limit)


@router.post("")
@router.post("/", include_in_schema=False)
async def create(payload: DynamicGroupDefinition):
    result = await dynamic_group_service.create_group(payload.model_dump())
    dynamic_group_service.request_refresh(result["id"])
    return result


@router.post("/preview")
async def preview(payload: DynamicGroupDefinition):
    return await dynamic_group_service.preview(payload.model_dump())


@router.post("/interpret", response_model=DynamicGroupInterpretResponse)
async def interpret(payload: DynamicGroupInterpretRequest):
    return await dynamic_group_interpreter_service.interpret(payload.prompt, payload.current_definition)


@router.get("/{group_id}")
async def group(group_id: int):
    return await dynamic_group_service.get_group(group_id)


@router.put("/{group_id}")
async def update(group_id: int, payload: DynamicGroupUpdate):
    result = await dynamic_group_service.update_group(group_id, payload.model_dump(exclude_unset=True))
    dynamic_group_service.request_refresh(group_id)
    return result


@router.delete("/{group_id}")
async def archive(group_id: int):
    return await dynamic_group_service.archive_group(group_id)


@router.post("/{group_id}/refresh")
async def refresh(group_id: int):
    await dynamic_group_service.get_group(group_id)
    dynamic_group_service.request_refresh(group_id)
    return {"status": "queued", "group_id": group_id}


@router.get("/{group_id}/current")
async def current(group_id: int, q: str = Query("", max_length=100),
                  offset: int = Query(0, ge=0), limit: int = Query(50, ge=1, le=200)):
    return await dynamic_group_service.get_current(group_id, q=q, offset=offset, limit=limit)


@router.get("/{group_id}/dates")
async def dates(group_id: int):
    return await dynamic_group_service.get_dates(group_id)


@router.get("/{group_id}/selections")
async def selections(group_id: int, trade_date: date | None = None,
                     offset: int = Query(0, ge=0), limit: int = Query(50, ge=1, le=100)):
    return await dynamic_group_service.list_selections(group_id, trade_date=trade_date, offset=offset, limit=limit)


@router.get("/{group_id}/selections/{selection_id}")
async def selection(group_id: int, selection_id: int, q: str = Query("", max_length=100),
                    offset: int = Query(0, ge=0), limit: int = Query(50, ge=1, le=200)):
    return await dynamic_group_service.get_selection(group_id, selection_id, q=q, offset=offset, limit=limit)


@router.get("/{group_id}/trend")
async def trend(group_id: int, selection_id: int | None = None,
                window_days: int = Query(250, ge=0, le=1095)):
    return await dynamic_group_service.get_trend(group_id, selection_id, window_days)


@router.get("/{group_id}/intraday")
async def intraday(group_id: int, trade_date: date | None = None):
    return await dynamic_group_service.get_intraday(group_id, trade_date)


@router.delete("/{group_id}/members/{stock_code}")
async def remove_member(group_id: int, stock_code: str):
    return await dynamic_group_service.remove_member(group_id, stock_code)
