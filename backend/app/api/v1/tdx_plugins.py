"""Tongdaxin embedded watch plugin API."""
from datetime import date
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.services.tdx_plugin_service import tdx_plugin_service

router = APIRouter()


@router.get("/limit-up-live", summary="通达信插件：涨停播报")
async def get_limit_up_live(
    trade_date: Optional[date] = Query(None, description="交易日期，默认今天"),
):
    return await tdx_plugin_service.get_limit_up_live(trade_date)


@router.get("/stock-move/{stock_code}", summary="通达信插件：股票异动解析联动")
async def get_stock_move(
    stock_code: str,
    trade_date: Optional[date] = Query(None, description="交易日期，默认今天"),
):
    return await tdx_plugin_service.get_stock_move(stock_code, trade_date, source_scope="mixed")


@router.get("/plate-strength", summary="通达信插件：实时板块强度")
async def get_plate_strength(
    trade_date: Optional[date] = Query(None, description="交易日期，默认今天"),
):
    return await tdx_plugin_service.get_plate_strength(trade_date)


@router.get("/news", summary="通达信插件：聚合快讯")
async def get_news(
    limit: int = Query(80, ge=1, le=200, description="返回条数"),
    db: AsyncSession = Depends(get_db),
):
    return await tdx_plugin_service.get_news(db, limit=limit)


@router.get("/ths-move/{stock_code}", summary="通达信插件：异动解析（同花顺版）")
async def get_ths_move(
    stock_code: str,
    trade_date: Optional[date] = Query(None, description="交易日期，默认今天"),
):
    return await tdx_plugin_service.get_stock_move(stock_code, trade_date, source_scope="ths")


@router.post("/calibration/compare", summary="通达信插件：黑盒对照差异报告")
async def compare_calibration_samples(
    payload: Dict[str, Any] = Body(...),
):
    return tdx_plugin_service.compare_samples(
        target_items=payload.get("target_items") or [],
        ours_items=payload.get("ours_items") or [],
        key_field=payload.get("key_field") or "stock_code",
    )
