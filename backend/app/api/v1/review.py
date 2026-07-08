"""
市场复盘API
"""
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Iterable

from fastapi import APIRouter, Depends, Query
from sqlalchemy import desc, distinct, select
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
_TRADING_DAY_CACHE: dict[date, bool] = {}


async def _collect_intraday_source(trade_date: date) -> dict[str, Any]:
    from app.services.market_review_source_service import market_review_source_service

    return await market_review_source_service.collect_for_date(trade_date)


async def _resolve_review_trade_date(
    db: AsyncSession,
    requested_date: date,
) -> tuple[date, bool]:
    """按复盘明细表解析实际可用的交易日期。"""
    result = await db.execute(
        select(distinct(MarketReviewStockDaily.trade_date))
        .where(MarketReviewStockDaily.trade_date <= requested_date)
        .order_by(desc(MarketReviewStockDaily.trade_date))
    )
    candidate_dates = result.scalars().all()
    trading_dates = set(_filter_cn_trading_dates(candidate_dates))
    resolved_date = next((candidate for candidate in candidate_dates if candidate in trading_dates), None)
    if resolved_date is None:
        return requested_date, False
    return resolved_date, resolved_date != requested_date


async def _resolve_latest_metric_trade_date(
    db: AsyncSession,
    end_date: date | None,
) -> date | None:
    query = select(distinct(MarketReviewDailyMetric.trade_date)).order_by(desc(MarketReviewDailyMetric.trade_date))
    if end_date is not None:
        query = query.where(MarketReviewDailyMetric.trade_date <= end_date)

    result = await db.execute(query)
    candidate_dates = result.scalars().all()
    trading_dates = set(_filter_cn_trading_dates(candidate_dates))
    return next((candidate for candidate in candidate_dates if candidate in trading_dates), None)


async def _resolve_daily_metric_range(
    db: AsyncSession,
    start_date: date | None,
    end_date: date | None,
) -> tuple[date | None, date | None, date | None, bool]:
    """把日线查询锚定到最新已有复盘指标，避免今日未生成时返回空图。"""
    latest_trade_date = await _resolve_latest_metric_trade_date(db, end_date)
    if latest_trade_date is None:
        return start_date, end_date, None, False

    resolved_end_date = end_date
    if resolved_end_date is None or resolved_end_date > latest_trade_date:
        resolved_end_date = latest_trade_date

    resolved_start_date = start_date
    if resolved_start_date is not None and resolved_start_date > resolved_end_date:
        resolved_start_date = resolved_end_date

    is_fallback = (
        (start_date is not None and resolved_start_date != start_date)
        or (end_date is not None and resolved_end_date != end_date)
    )
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
        and bool(_get_stock_value(row, "today_sealed_close"))
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


def _build_stock_item(row: MarketReviewStockDaily | dict[str, Any]) -> MarketReviewStockItem:
    if isinstance(row, dict):
        return _build_stock_item_from_source(row)
    return MarketReviewStockItem.model_validate(row)


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


def _is_ladder_display_row(row: MarketReviewStockDaily | dict[str, Any]) -> bool:
    return (
        bool(_get_stock_value(row, "today_touched_limit_up"))
        and bool(_get_stock_value(row, "today_sealed_close"))
        and _get_stock_int(row, "today_continuous_days") >= 2
    )


def _is_ladder_cohort_touch_row(row: MarketReviewStockDaily | dict[str, Any]) -> bool:
    return bool(_get_stock_value(row, "today_touched_limit_up")) and _get_stock_int(row, "today_continuous_days") >= 2


def _is_same_ladder_cohort_row(row: MarketReviewStockDaily | dict[str, Any], continuous_days: int) -> bool:
    if continuous_days <= 1:
        return False

    previous_days = continuous_days - 1
    yesterday_days = _get_stock_int(row, "yesterday_continuous_days")
    if previous_days == 1 and bool(_get_stock_value(row, "yesterday_limit_up")) and yesterday_days in (0, 1):
        return True
    if yesterday_days == previous_days:
        return True

    return (
        _is_ladder_cohort_touch_row(row)
        and _get_stock_int(row, "today_continuous_days") == continuous_days
        and yesterday_days == 0
    )


def _get_ladder_cohort_sort_key(row: MarketReviewStockDaily | dict[str, Any]) -> tuple[float, int, int, str]:
    change_pct = _get_stock_float(row, "change_pct")
    change_sort = -change_pct if change_pct is not None else float("inf")
    return (
        change_sort,
        -_get_stock_int(row, "today_continuous_days"),
        0 if bool(_get_stock_value(row, "today_sealed_close")) else 1,
        str(_get_stock_value(row, "stock_code", "")),
    )


def _apply_ladder_cohort_metrics(
    ladders: list[MarketReviewLadderItem],
    stock_rows: list[MarketReviewStockDaily | dict[str, Any]],
) -> None:
    for ladder in ladders:
        cohort_rows = [
            row
            for row in stock_rows
            if _is_same_ladder_cohort_row(row, ladder.continuous_days)
        ]
        if not cohort_rows:
            cohort_rows = ladder.stocks

        changes = [
            change
            for change in (_get_stock_float(row, "change_pct") for row in cohort_rows)
            if change is not None
        ]
        cohort_count = len(cohort_rows)
        cohort_sealed_count = sum(1 for row in cohort_rows if bool(_get_stock_value(row, "today_sealed_close")))
        cohort_opened_count = sum(
            1
            for row in cohort_rows
            if bool(_get_stock_value(row, "today_opened_close") or _get_stock_value(row, "today_broken"))
        )

        ladder.cohort_count = cohort_count
        ladder.cohort_sealed_count = cohort_sealed_count
        ladder.cohort_opened_count = cohort_opened_count
        ladder.cohort_seal_rate = round(cohort_sealed_count * 100 / cohort_count, 2) if cohort_count else 0.0
        ladder.cohort_avg_change = round(sum(changes) / len(changes), 2) if changes else None
        ladder.cohort_stocks = [
            _build_stock_item(row)
            for row in sorted(cohort_rows, key=_get_ladder_cohort_sort_key)
        ]


def _build_ladder_response_from_rows(
    trade_date: date,
    is_fallback: bool,
    stock_rows: list[MarketReviewStockDaily | dict[str, Any]],
) -> MarketReviewLadderResponse:
    ladder_rows = [row for row in stock_rows if _is_ladder_display_row(row)]
    ladder_rows.sort(
        key=lambda row: (
            -_get_stock_int(row, "today_continuous_days"),
            not bool(_get_stock_value(row, "today_sealed_close")),
            str(_get_stock_value(row, "stock_code", "")),
        )
    )

    ladders: list[MarketReviewLadderItem] = []
    current_ladder: MarketReviewLadderItem | None = None
    for row in ladder_rows:
        stock = _build_stock_item(row)
        if current_ladder is None or current_ladder.continuous_days != stock.today_continuous_days:
            current_ladder = MarketReviewLadderItem(
                continuous_days=stock.today_continuous_days,
                count=0,
                stocks=[],
            )
            ladders.append(current_ladder)

        current_ladder.stocks.append(stock)
        current_ladder.count = len(current_ladder.stocks)

    _apply_ladder_cohort_metrics(ladders, stock_rows)

    return MarketReviewLadderResponse(
        trade_date=trade_date,
        is_fallback=is_fallback,
        ladders=ladders,
    )


def _build_intraday_ladder(trade_date: date, stock_rows: list[dict[str, Any]]) -> MarketReviewLadderResponse:
    return _build_ladder_response_from_rows(trade_date, False, stock_rows)


def _normalize_cn_trade_calendar_date(raw_value: Any) -> date | None:
    if isinstance(raw_value, datetime):
        return raw_value.date()
    if isinstance(raw_value, date):
        return raw_value
    if isinstance(raw_value, str):
        return datetime.strptime(raw_value, "%Y-%m-%d").date()
    if hasattr(raw_value, "date"):
        return raw_value.date()
    return None


def _load_cn_trading_dates(start_date: date, end_date: date) -> set[date]:
    import akshare as ak

    calendar_df = ak.tool_trade_date_hist_sina()
    if "trade_date" not in calendar_df:
        return set()

    trading_dates: set[date] = set()
    for raw_value in calendar_df["trade_date"].tolist():
        trade_date = _normalize_cn_trade_calendar_date(raw_value)
        if trade_date is not None and start_date <= trade_date <= end_date:
            trading_dates.add(trade_date)
    return trading_dates


def _cache_cn_trading_dates(start_date: date, end_date: date, trading_dates: set[date]) -> None:
    current = start_date
    while current <= end_date:
        _TRADING_DAY_CACHE[current] = current.weekday() < 5 and current in trading_dates
        current += timedelta(days=1)


def _filter_cn_trading_dates(dates: Iterable[date]) -> list[date]:
    candidate_dates = list(dates)
    if not candidate_dates:
        return []

    unique_dates = sorted(set(candidate_dates))
    missing_weekdays = [
        trade_date
        for trade_date in unique_dates
        if trade_date.weekday() < 5 and trade_date not in _TRADING_DAY_CACHE
    ]
    for trade_date in unique_dates:
        if trade_date.weekday() >= 5:
            _TRADING_DAY_CACHE[trade_date] = False

    if missing_weekdays:
        start_date = missing_weekdays[0]
        end_date = missing_weekdays[-1]
        try:
            _cache_cn_trading_dates(start_date, end_date, _load_cn_trading_dates(start_date, end_date))
        except Exception:
            for trade_date in missing_weekdays:
                _TRADING_DAY_CACHE[trade_date] = True

    return [trade_date for trade_date in candidate_dates if _TRADING_DAY_CACHE.get(trade_date, False)]


def _filter_trading_metric_rows(rows: Iterable[MarketReviewDailyMetric]) -> list[MarketReviewDailyMetric]:
    metric_rows = list(rows)
    trading_dates = set(_filter_cn_trading_dates(row.trade_date for row in metric_rows))
    return [row for row in metric_rows if row.trade_date in trading_dates]


def _is_cn_trading_day(trade_date: date) -> bool:
    return bool(_filter_cn_trading_dates([trade_date]))


def _should_collect_live_intraday(trade_date: date) -> bool:
    now = datetime.now(CN_TZ)
    if trade_date != now.date() or not _is_cn_trading_day(trade_date):
        return False
    return time(9, 15) <= now.time() <= time(15, 0)


async def _build_intraday_snapshot(trade_date: date, is_live: bool = True) -> MarketReviewIntradayResponse:
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
        is_live=is_live,
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
        .where(MarketReviewStockDaily.trade_date == resolved_date)
        .order_by(
            MarketReviewStockDaily.today_continuous_days.desc(),
            MarketReviewStockDaily.today_sealed_close.desc(),
            MarketReviewStockDaily.first_limit_time.asc(),
            MarketReviewStockDaily.stock_code.asc(),
        )
    )

    return _build_ladder_response_from_rows(
        trade_date=resolved_date,
        is_fallback=is_fallback,
        stock_rows=list(result.scalars().all()),
    )


async def _build_stored_intraday_snapshot(
    db: AsyncSession,
    requested_date: date,
) -> MarketReviewIntradayResponse | None:
    resolved_date = await _resolve_latest_metric_trade_date(db, requested_date)
    if resolved_date is None:
        return None

    result = await db.execute(
        select(MarketReviewDailyMetric)
        .where(MarketReviewDailyMetric.trade_date == resolved_date)
        .limit(1)
    )
    metric_row = result.scalar_one_or_none()
    if metric_row is None:
        return None

    is_fallback = metric_row.trade_date != requested_date
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
        is_live=False,
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
            return await _build_intraday_snapshot(target_date, is_live=True)
        except Exception:
            stored_snapshot = await _build_stored_intraday_snapshot(db, target_date)
            if stored_snapshot is not None:
                return stored_snapshot
            raise

    stored_snapshot = await _build_stored_intraday_snapshot(db, target_date)
    if stored_snapshot is not None:
        return stored_snapshot

    return await _build_intraday_snapshot(target_date, is_live=False)


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
            query.order_by(MarketReviewDailyMetric.trade_date.desc())
        )
        metric_rows = _filter_trading_metric_rows(result.scalars().all())
        rows = await _attach_board_height_labels(db, list(reversed(metric_rows[:days])))
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
    rows = await _attach_board_height_labels(db, _filter_trading_metric_rows(result.scalars().all()))

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
