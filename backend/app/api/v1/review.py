"""
市场复盘API
"""
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

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
    MarketReviewIntradayResponse,
    MarketReviewLadderItem,
    MarketReviewLadderResponse,
    MarketReviewStockItem,
)
from app.services.market_review_metrics_service import market_review_metrics_service

router = APIRouter()
CN_TZ = timezone(timedelta(hours=8))


async def _collect_intraday_source(trade_date: date) -> dict[str, Any]:
    from app.services.market_review_source_service import market_review_source_service

    return await market_review_source_service.collect_for_date(trade_date)


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


def _format_board_height_label(
    stock_rows: list[MarketReviewStockDaily | dict[str, Any]],
    height: int,
    board_types: set[str] | None = None,
) -> str | None:
    if height <= 0:
        return None

    names = [
        f"{_get_stock_value(row, 'stock_name')}{height}"
        for row in stock_rows
        if _get_stock_int(row, "today_continuous_days") == height
        and (board_types is None or _get_stock_value(row, "board_type") in board_types)
    ]
    return "\n".join(names) if names else None


def _get_stock_value(row: MarketReviewStockDaily | dict[str, Any], key: str, default: Any = None) -> Any:
    if isinstance(row, dict):
        return row.get(key, default)
    return getattr(row, key, default)


def _get_stock_int(row: MarketReviewStockDaily | dict[str, Any], key: str) -> int:
    try:
        return int(_get_stock_value(row, key, 0) or 0)
    except (TypeError, ValueError):
        return 0


def _get_stock_float(row: MarketReviewStockDaily | dict[str, Any], key: str) -> float | None:
    value = _get_stock_value(row, key)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _build_stock_item_from_source(row: dict[str, Any]) -> MarketReviewStockItem:
    change_pct = _get_stock_float(row, "change_pct")
    return MarketReviewStockItem(
        stock_code=str(row.get("stock_code") or ""),
        stock_name=str(row.get("stock_name") or row.get("stock_code") or ""),
        today_continuous_days=_get_stock_int(row, "today_continuous_days"),
        today_sealed_close=bool(row.get("today_sealed_close")),
        today_opened_close=bool(row.get("today_opened_close") or row.get("today_broken")),
        change_pct=change_pct,
        amount=float(row.get("amount") or 0.0),
        limit_up_reason=row.get("limit_up_reason") or None,
    )


def _build_intraday_detail(trade_date: date, stock_rows: list[dict[str, Any]]) -> MarketReviewDetailResponse:
    stocks = [_build_stock_item_from_source(row) for row in stock_rows if row.get("stock_code")]
    stocks.sort(
        key=lambda stock: (
            -stock.today_continuous_days,
            -stock.amount,
            stock.stock_code,
        )
    )
    return MarketReviewDetailResponse(
        trade_date=trade_date,
        is_fallback=False,
        stocks=stocks,
    )


def _build_intraday_ladder(trade_date: date, stock_rows: list[dict[str, Any]]) -> MarketReviewLadderResponse:
    stocks = [
        _build_stock_item_from_source(row)
        for row in stock_rows
        if row.get("today_touched_limit_up") and _get_stock_int(row, "today_continuous_days") >= 2
    ]
    stocks.sort(
        key=lambda stock: (
            -stock.today_continuous_days,
            not stock.today_sealed_close,
            stock.stock_code,
        )
    )

    ladders: list[MarketReviewLadderItem] = []
    current_ladder: MarketReviewLadderItem | None = None
    for stock in stocks:
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
        is_fallback=False,
        ladders=ladders,
    )


def _should_collect_live_intraday(trade_date: date) -> bool:
    now = datetime.now(CN_TZ)
    return trade_date == now.date() and time(9, 15) <= now.time() <= time(15, 30)


async def _build_intraday_snapshot(trade_date: date) -> MarketReviewIntradayResponse:
    normalized_data = await _collect_intraday_source(trade_date)
    stock_rows = normalized_data.get("stock_rows") or []
    metric = market_review_metrics_service.aggregate_daily_metrics(
        trade_date,
        stock_rows,
        int(normalized_data.get("limit_down_count") or 0),
        float(normalized_data.get("market_turnover") or 0.0),
        int(normalized_data.get("up_count_ex_st") or 0),
        int(normalized_data.get("down_count_ex_st") or 0),
    )
    metric_row = MarketReviewDailyMetricRow.model_validate(metric)
    labels = {
        "max_board_label": _format_board_height_label(stock_rows, metric_row.max_board_height),
        "second_board_label": _format_board_height_label(stock_rows, metric_row.second_board_height),
        "gem_board_label": _format_board_height_label(
            stock_rows,
            metric_row.gem_board_height,
            {"gem", "star"},
        ),
    }
    metric_row = metric_row.model_copy(update=labels)

    detail = _build_intraday_detail(metric_row.trade_date, stock_rows)
    ladder = _build_intraday_ladder(metric_row.trade_date, stock_rows)

    return MarketReviewIntradayResponse(
        data=MarketReviewDailyData(
            series=[metric_row.trade_date],
            rows=[metric_row],
        ),
        requested_start_date=trade_date,
        requested_end_date=trade_date,
        start_date=metric_row.trade_date,
        end_date=metric_row.trade_date,
        latest_trade_date=metric_row.trade_date,
        is_fallback=False,
        snapshot_time=datetime.now(CN_TZ),
        detail=detail,
        ladder=ladder,
    )


async def _build_detail_response_from_db(
    db: AsyncSession,
    resolved_date: date,
    is_fallback: bool,
) -> MarketReviewDetailResponse:
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


async def _build_ladder_response_from_db(
    db: AsyncSession,
    resolved_date: date,
    is_fallback: bool,
) -> MarketReviewLadderResponse:
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


async def _build_stored_intraday_snapshot(
    db: AsyncSession,
    requested_date: date,
) -> MarketReviewIntradayResponse | None:
    result = await db.execute(
        select(MarketReviewDailyMetric)
        .where(MarketReviewDailyMetric.trade_date == requested_date)
        .limit(1)
    )
    metric_row = result.scalar_one_or_none()
    is_fallback = False

    if metric_row is None:
        result = await db.execute(
            select(MarketReviewDailyMetric)
            .where(MarketReviewDailyMetric.trade_date <= requested_date)
            .order_by(desc(MarketReviewDailyMetric.trade_date))
            .limit(1)
        )
        metric_row = result.scalar_one_or_none()
        is_fallback = metric_row is not None

    if metric_row is None:
        return None

    rows = await _attach_board_height_labels(db, [metric_row])
    daily_row = rows[0] if rows else MarketReviewDailyMetricRow.model_validate(metric_row)
    detail = await _build_detail_response_from_db(db, daily_row.trade_date, is_fallback)
    ladder = await _build_ladder_response_from_db(db, daily_row.trade_date, is_fallback)

    return MarketReviewIntradayResponse(
        data=MarketReviewDailyData(
            series=[daily_row.trade_date],
            rows=[daily_row],
        ),
        requested_start_date=requested_date,
        requested_end_date=requested_date,
        start_date=daily_row.trade_date,
        end_date=daily_row.trade_date,
        latest_trade_date=daily_row.trade_date,
        is_fallback=is_fallback,
        snapshot_time=datetime.now(CN_TZ),
        detail=detail,
        ladder=ladder,
    )


async def _attach_board_height_labels(
    db: AsyncSession,
    metric_rows: list[MarketReviewDailyMetric],
) -> list[MarketReviewDailyMetricRow]:
    if not metric_rows:
        return []

    trade_dates = [row.trade_date for row in metric_rows]
    result = await db.execute(
        select(MarketReviewStockDaily)
        .where(
            MarketReviewStockDaily.trade_date.in_(trade_dates),
            MarketReviewStockDaily.today_touched_limit_up.is_(True),
            MarketReviewStockDaily.today_continuous_days > 0,
        )
        .order_by(
            MarketReviewStockDaily.trade_date.asc(),
            MarketReviewStockDaily.today_continuous_days.desc(),
            MarketReviewStockDaily.today_sealed_close.desc(),
            MarketReviewStockDaily.first_limit_time.asc(),
            MarketReviewStockDaily.stock_code.asc(),
        )
    )

    stocks_by_date: dict[date, list[MarketReviewStockDaily]] = {}
    for stock_row in result.scalars().all():
        stocks_by_date.setdefault(stock_row.trade_date, []).append(stock_row)

    rows: list[MarketReviewDailyMetricRow] = []
    for metric_row in metric_rows:
        stock_rows = stocks_by_date.get(metric_row.trade_date, [])
        labels = {
            "max_board_label": _format_board_height_label(stock_rows, metric_row.max_board_height),
            "second_board_label": _format_board_height_label(stock_rows, metric_row.second_board_height),
            "gem_board_label": _format_board_height_label(
                stock_rows,
                metric_row.gem_board_height,
                {"gem", "star"},
            ),
        }
        rows.append(MarketReviewDailyMetricRow.model_validate(metric_row).model_copy(update=labels))

    return rows


@router.get("/intraday", response_model=MarketReviewIntradayResponse, summary="获取盘中复盘快照")
async def get_market_review_intraday(
    trade_date: date | None = Query(None, description="交易日期，默认今天"),
    db: AsyncSession = Depends(get_db),
):
    """获取盘中实时复盘快照。"""
    target_date = trade_date or datetime.now(CN_TZ).date()

    if _should_collect_live_intraday(target_date):
        try:
            return await _build_intraday_snapshot(target_date)
        except Exception:
            stored_snapshot = await _build_stored_intraday_snapshot(db, target_date)
            if stored_snapshot is not None:
                return stored_snapshot
            raise

    stored_snapshot = await _build_stored_intraday_snapshot(db, target_date)
    if stored_snapshot is not None:
        return stored_snapshot

    return await _build_intraday_snapshot(target_date)


@router.get("/daily", response_model=MarketReviewDailyResponse, summary="获取复盘日级指标")
async def get_market_review_daily(
    start_date: date | None = Query(None, description="开始日期"),
    end_date: date | None = Query(None, description="结束日期"),
    days: int | None = Query(None, ge=1, le=250, description="最近N个复盘交易日"),
    db: AsyncSession = Depends(get_db),
):
    """获取市场复盘日级指标列表。"""
    resolved_start_date, resolved_end_date, latest_trade_date, is_fallback = await _resolve_daily_metric_range(
        db,
        start_date,
        end_date,
    )

    if days is not None:
        query = select(MarketReviewDailyMetric)
        if resolved_end_date:
            query = query.where(MarketReviewDailyMetric.trade_date <= resolved_end_date)
        result = await db.execute(
            query.order_by(MarketReviewDailyMetric.trade_date.desc()).limit(days)
        )
        rows = await _attach_board_height_labels(db, list(reversed(result.scalars().all())))
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

    query = select(MarketReviewDailyMetric)
    if resolved_start_date:
        query = query.where(MarketReviewDailyMetric.trade_date >= resolved_start_date)
    if resolved_end_date:
        query = query.where(MarketReviewDailyMetric.trade_date <= resolved_end_date)

    result = await db.execute(query.order_by(MarketReviewDailyMetric.trade_date.asc()))
    rows = await _attach_board_height_labels(db, list(result.scalars().all()))

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
    return await _build_detail_response_from_db(db, resolved_date, is_fallback)


@router.get("/ladder", response_model=MarketReviewLadderResponse, summary="获取复盘连板梯队")
async def get_market_review_ladder(
    trade_date: date = Query(..., description="交易日期"),
    db: AsyncSession = Depends(get_db),
):
    """获取指定交易日的市场复盘连板梯队。"""
    resolved_date, is_fallback = await _resolve_review_trade_date(db, trade_date)
    return await _build_ladder_response_from_db(db, resolved_date, is_fallback)
