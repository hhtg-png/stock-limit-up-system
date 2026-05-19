from datetime import date
from typing import Dict, Literal, Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.services.daily_analysis_service import daily_analysis_service


router = APIRouter()
DailyAnalysisSession = Literal["after_close", "intraday"]


class DailyAnalysisOverrideRequest(BaseModel):
    overrides: Dict[str, Optional[str]] = Field(default_factory=dict, description="列名到人工覆盖内容")


class DailyAnalysisBackfillRequest(BaseModel):
    month: Optional[str] = Field(None, description="月份，格式 YYYY-MM")


@router.get("", summary="获取每日分析月表")
async def get_daily_analysis_month(
    month: Optional[str] = Query(None, description="月份，格式 YYYY-MM"),
    session: DailyAnalysisSession = Query("after_close", description="分析版本：after_close=盘后，intraday=盘中"),
    db: AsyncSession = Depends(get_db),
):
    if month is None:
        month = date.today().strftime("%Y-%m")
    return await daily_analysis_service.get_month(db, month, session=session)


@router.post("/backfill", summary="回填每日分析")
async def backfill_daily_analysis(
    payload: DailyAnalysisBackfillRequest,
    db: AsyncSession = Depends(get_db),
):
    return await daily_analysis_service.backfill(db, payload.month)


@router.post("/{trade_date}/rebuild", summary="重算单日每日分析")
async def rebuild_daily_analysis(
    trade_date: date,
    session: DailyAnalysisSession = Query("after_close", description="分析版本：after_close=盘后，intraday=盘中"),
    db: AsyncSession = Depends(get_db),
):
    return await daily_analysis_service.rebuild_for_date(db, trade_date, session=session)


@router.patch("/{trade_date}/overrides", summary="更新每日分析人工覆盖")
async def update_daily_analysis_overrides(
    trade_date: date,
    payload: DailyAnalysisOverrideRequest,
    session: DailyAnalysisSession = Query("after_close", description="分析版本：after_close=盘后，intraday=盘中"),
    db: AsyncSession = Depends(get_db),
):
    return await daily_analysis_service.update_overrides(db, trade_date, payload.overrides, session=session)
