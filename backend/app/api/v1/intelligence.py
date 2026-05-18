"""Knowledge intelligence API."""
from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.intelligence import DailyInfoDigest, JiegeModeSignal
from app.services.intelligence_service import intelligence_service
from app.utils.time_utils import today_cn

router = APIRouter()


@router.get("/daily-info", summary="获取每日资讯")
async def get_daily_info(
    trade_date: Optional[date] = Query(None, description="交易日期，默认今天"),
    db: AsyncSession = Depends(get_db),
):
    target_date = trade_date or today_cn()
    existing = await _get_daily_digest(db, target_date)
    needs_model_refresh = existing is not None and intelligence_service.daily_digest_needs_model_refresh(existing)
    if existing is not None and not needs_model_refresh:
        return intelligence_service.serialize_daily_digest(existing, cache_hit=True)
    return await intelligence_service.build_daily_info(
        db,
        target_date,
        allow_latest_fallback=True,
        force=needs_model_refresh,
    )


@router.post("/daily-info/sync", summary="同步每日资讯知识库")
async def sync_daily_info(db: AsyncSession = Depends(get_db)):
    return await intelligence_service.sync_all(db, force_daily=True)


@router.get("/jiege-mode", summary="获取杰哥交易模式")
async def get_jiege_mode(
    trade_date: Optional[date] = Query(None, description="交易日期，默认今天"),
    db: AsyncSession = Depends(get_db),
):
    target_date = trade_date or today_cn()
    existing = await _get_jiege_signal(db, target_date)
    if existing is not None:
        return intelligence_service.serialize_jiege_signal(existing, cache_hit=True)
    return await intelligence_service.build_jiege_mode(db, target_date, allow_latest_fallback=True)


@router.post("/jiege-mode/rebuild", summary="重算杰哥交易模式")
async def rebuild_jiege_mode(
    trade_date: Optional[date] = Query(None, description="交易日期，默认今天"),
    db: AsyncSession = Depends(get_db),
):
    target_date = trade_date or today_cn()
    return await intelligence_service.build_jiege_mode(db, target_date, allow_latest_fallback=True, force=True)


@router.get("/sources", summary="获取知识库同步源")
async def get_sources():
    return {
        "sources": [
            {"key": source.key, "name": source.name, "kind": source.kind, "share_id": source.share_id}
            for source in intelligence_service.sources.values()
        ]
    }


async def _get_daily_digest(db: AsyncSession, trade_date: date) -> Optional[DailyInfoDigest]:
    result = await db.execute(select(DailyInfoDigest).where(DailyInfoDigest.trade_date == trade_date))
    return result.scalar_one_or_none()


async def _get_jiege_signal(db: AsyncSession, trade_date: date) -> Optional[JiegeModeSignal]:
    result = await db.execute(select(JiegeModeSignal).where(JiegeModeSignal.trade_date == trade_date))
    return result.scalar_one_or_none()
