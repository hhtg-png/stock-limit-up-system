"""
市场复盘API
"""
from datetime import date

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.market_review import MarketReviewDailyMetric, MarketReviewStockDaily
from app.schemas.market_review import (
    MarketReviewDailyData,
    MarketReviewDailyMetricRow,
    MarketReviewDailyResponse,
    MarketReviewDetailResponse,
    MarketReviewLadderItem,
    MarketReviewLadderResponse,
    MarketReviewStockItem,
)

router = APIRouter()


@router.get("/daily", response_model=MarketReviewDailyResponse, summary="获取复盘日级指标")
async def get_market_review_daily(
    start_date: date | None = Query(None, description="开始日期"),
    end_date: date | None = Query(None, description="结束日期"),
    db: AsyncSession = Depends(get_db),
):
    """获取市场复盘日级指标列表。"""
    query = select(MarketReviewDailyMetric)

    if start_date:
        query = query.where(MarketReviewDailyMetric.trade_date >= start_date)
    if end_date:
        query = query.where(MarketReviewDailyMetric.trade_date <= end_date)

    result = await db.execute(query.order_by(MarketReviewDailyMetric.trade_date.asc()))
    rows = [MarketReviewDailyMetricRow.model_validate(row) for row in result.scalars().all()]

    return MarketReviewDailyResponse(
        data=MarketReviewDailyData(
            series=[row.trade_date for row in rows],
            rows=rows,
        )
    )


@router.get("/detail", response_model=MarketReviewDetailResponse, summary="获取复盘个股明细")
async def get_market_review_detail(
    trade_date: date = Query(..., description="交易日期"),
    db: AsyncSession = Depends(get_db),
):
    """获取指定交易日的市场复盘个股明细。"""
    result = await db.execute(
        select(MarketReviewStockDaily)
        .where(MarketReviewStockDaily.trade_date == trade_date)
        .order_by(
            MarketReviewStockDaily.today_continuous_days.desc(),
            MarketReviewStockDaily.amount.desc(),
        )
    )
    stocks = [MarketReviewStockItem.model_validate(row) for row in result.scalars().all()]

    return MarketReviewDetailResponse(
        trade_date=trade_date,
        stocks=stocks,
    )


@router.get("/ladder", response_model=MarketReviewLadderResponse, summary="获取复盘连板梯队")
async def get_market_review_ladder(
    trade_date: date = Query(..., description="交易日期"),
    db: AsyncSession = Depends(get_db),
):
    """获取指定交易日的市场复盘连板梯队。"""
    result = await db.execute(
        select(MarketReviewStockDaily)
        .where(
            MarketReviewStockDaily.trade_date == trade_date,
            MarketReviewStockDaily.today_touched_limit_up.is_(True),
            MarketReviewStockDaily.today_continuous_days >= 2,
        )
        .order_by(
            MarketReviewStockDaily.today_continuous_days.desc(),
            MarketReviewStockDaily.amount.desc(),
        )
    )

    ladders: list[MarketReviewLadderItem] = []
    current_ladder: MarketReviewLadderItem | None = None

    for row in result.scalars().all():
        stock = MarketReviewStockItem.model_validate(row)
        if current_ladder is None or current_ladder.continuous_days != stock.today_continuous_days:
            current_ladder = MarketReviewLadderItem(
                continuous_days=stock.today_continuous_days,
                count=0,
                stocks=[],
            )
            ladders.append(current_ladder)

        current_ladder.stocks.append(stock)
        current_ladder.count = len(current_ladder.stocks)

    return MarketReviewLadderResponse(
        trade_date=trade_date,
        ladders=ladders,
    )
