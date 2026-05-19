"""Knowledge intelligence API."""
from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.intelligence import DailyInfoDigest, JiegeModeSignal
from app.services.intelligence_service import intelligence_service
from app.utils.time_utils import today_cn

router = APIRouter()


@router.get("/daily-info/history", summary="获取每日资讯历史")
async def get_daily_info_history(
    limit: int = Query(30, ge=1, le=100, description="返回条数"),
    db: AsyncSession = Depends(get_db),
):
    return {"items": await intelligence_service.list_daily_digests(db, limit=limit)}


@router.get("/daily-info/search", summary="搜索每日资讯摘要和原文")
async def search_daily_info(
    keyword: str = Query("", description="关键词，匹配摘要、标题、简介和已缓存原文"),
    limit: int = Query(50, ge=1, le=100, description="返回条数"),
    db: AsyncSession = Depends(get_db),
):
    return {"items": await intelligence_service.search_daily_digests(db, keyword=keyword, limit=limit)}


@router.get("/daily-info", summary="获取每日资讯")
async def get_daily_info(
    trade_date: Optional[date] = Query(None, description="交易日期，默认今天"),
    db: AsyncSession = Depends(get_db),
):
    target_date = trade_date or today_cn()
    existing = await _get_daily_digest(db, target_date)
    if existing is not None:
        return await intelligence_service.serialize_daily_digest_with_sources(db, existing, cache_hit=True)
    return await intelligence_service.build_daily_info(
        db,
        target_date,
        allow_latest_fallback=True,
    )


@router.get("/documents/{document_id}", summary="获取知识库原文")
async def get_document_source(document_id: int, db: AsyncSession = Depends(get_db)):
    document = await intelligence_service.get_document_source(db, document_id)
    if document is None:
        raise HTTPException(status_code=404, detail="Document not found")
    return document


@router.post("/daily-info/sync", summary="同步每日资讯知识库")
async def sync_daily_info(
    force: bool = Query(False, description="是否强制重算每日摘要；默认只在知识库内容更新后调用模型"),
    db: AsyncSession = Depends(get_db),
):
    return await intelligence_service.sync_all(db, force_daily=force)


@router.get("/jiege-mode", summary="获取杰哥交易模式")
async def get_jiege_mode(
    trade_date: Optional[date] = Query(None, description="交易日期，默认今天"),
    db: AsyncSession = Depends(get_db),
):
    target_date = trade_date or today_cn()
    existing = await _get_jiege_signal(db, target_date)
    if existing is not None and _jiege_signal_has_yesterday_prediction(existing):
        return intelligence_service.serialize_jiege_signal(existing, cache_hit=True)
    if existing is not None:
        return await intelligence_service.ensure_jiege_yesterday_prediction(db, existing)
    return await intelligence_service.build_jiege_mode(
        db,
        target_date,
        allow_latest_fallback=True,
    )


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


def _jiege_signal_has_yesterday_prediction(signal: JiegeModeSignal) -> bool:
    return isinstance(signal.signal_json, dict) and "yesterday_prediction" in signal.signal_json
