"""
市场复盘API
"""
from datetime import date

from fastapi import APIRouter, Depends, Query
from sqlalchemy import desc, select
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


async def _resolve_review_trade_date(
    db: AsyncSession,
    requested_date: date,
) -> tuple[date, bool]:
    """按复盘明细表解析实际可用的交易日期。"""
    result = await db.execute(
        select(MarketReviewStockDaily.trade_date)
        .where(MarketReviewStockDaily.trade_date <= requested_date)
        .order_by(desc(MarketReviewStockDaily.trade_date))
        .limit(1)
    )
    resolved_date = result.scalar_one_or_none()
    if resolved_date is None:
        return requested_date, False
    return resolved_date, resolved_date != requested_date


async def _resolve_daily_metric_range(
    db: AsyncSession,
    start_date: date | None,
    end_date: date | None,
) -> tuple[date | None, date | None, date | None, bool]:
    """把日线查询锚定到最新已有复盘指标，避免今日未生成时返回空图。"""
    latest_query = (
        select(MarketReviewDailyMetric.trade_date)
        .order_by(desc(MarketReviewDailyMetric.trade_date))
        .limit(1)
    )
    if end_date is not None:
        latest_query = latest_query.where(MarketReviewDailyMetric.trade_date <= end_date)

    result = await db.execute(latest_query)
    latest_trade_date = result.scalar_one_or_none()
    if latest_trade_date is None:
        return start_date, end_date, None, False

    resolved_end_date = end_date
    if resolved_end_date is None or resolved_end_date > latest_trade_date:
        resolved_end_date = latest_trade_date

    resolved_start_date = start_date
    if resolved_start_date is not None and resolved_start_date > resolved_end_date:
        resolved_start_date = resolved_end_date

    is_fallback = resolved_start_date != start_date or resolved_end_date != end_date
    return resolved_start_date, resolved_end_date, latest_trade_date, is_fallback


@router.get("/daily", response_model=MarketReviewDailyResponse, summary="获取复盘日级指标")
async def get_market_review_daily(
    start_date: date | None = Query(None, description="开始日期"),
    end_date: date | None = Query(None, description="结束日期"),
    db: AsyncSession = Depends(get_db),
):
    """获取市场复盘日级指标列表。"""
    resolved_start_date, resolved_end_date, latest_trade_date, is_fallback = await _resolve_daily_metric_range(
        db,
        start_date,
        end_date,
    )
    query = select(MarketReviewDailyMetric)

    if resolved_start_date:
        query = query.where(MarketReviewDailyMetric.trade_date >= resolved_start_date)
    if resolved_end_date:
        query = query.where(MarketReviewDailyMetric.trade_date <= resolved_end_date)

    result = await db.execute(query.order_by(MarketReviewDailyMetric.trade_date.asc()))
    rows = [MarketReviewDailyMetricRow.model_validate(row) for row in result.scalars().all()]

    return MarketReviewDailyResponse(
        data=MarketReviewDailyData(
            series=[row.trade_date for row in rows],
            rows=rows,
        ),
        requested_start_date=start_date,
        requested_end_date=end_date,
        start_date=rows[0].trade_date if rows else resolved_start_date,
        end_date=rows[-1].trade_date if rows else resolved_end_date,
        latest_trade_date=latest_trade_date,
        is_fallback=is_fallback,
    )


@router.get("/detail", response_model=MarketReviewDetailResponse, summary="获取复盘个股明细")
async def get_market_review_detail(
    trade_date: date = Query(..., description="交易日期"),
    db: AsyncSession = Depends(get_db),
):
    """获取指定交易日的市场复盘个股明细。"""
    resolved_date, is_fallback = await _resolve_review_trade_date(db, trade_date)
    result = await db.execute(
        select(MarketReviewStockDaily)
        .where(MarketReviewStockDaily.trade_date == resolved_date)
        .order_by(
            MarketReviewStockDaily.today_continuous_days.desc(),
            MarketReviewStockDaily.amount.desc(),
        )
    )
    stocks = [MarketReviewStockItem.model_validate(row) for row in result.scalars().all()]

    return MarketReviewDetailResponse(
        trade_date=resolved_date,
        is_fallback=is_fallback,
        stocks=stocks,
    )


@router.get("/ladder", response_model=MarketReviewLadderResponse, summary="获取复盘连板梯队")
async def get_market_review_ladder(
    trade_date: date = Query(..., description="交易日期"),
    db: AsyncSession = Depends(get_db),
):
    """获取指定交易日的市场复盘连板梯队。"""
    resolved_date, is_fallback = await _resolve_review_trade_date(db, trade_date)
    result = await db.execute(
        select(MarketReviewStockDaily)
        .where(
            MarketReviewStockDaily.trade_date == resolved_date,
            MarketReviewStockDaily.today_touched_limit_up.is_(True),
            MarketReviewStockDaily.today_continuous_days >= 2,
        )
        .order_by(
            MarketReviewStockDaily.today_continuous_days.desc(),
            MarketReviewStockDaily.today_sealed_close.desc(),
            MarketReviewStockDaily.first_limit_time.asc(),
            MarketReviewStockDaily.stock_code.asc(),
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
        trade_date=resolved_date,
        is_fallback=is_fallback,
        ladders=ladders,
    )
