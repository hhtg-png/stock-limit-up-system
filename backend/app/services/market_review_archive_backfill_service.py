"""Build explicit historical samples without changing formal reviews or source rows."""

import asyncio
import math
import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models.limit_up import LimitUpRecord
from app.models.market_review import (
    MarketReviewArchiveSample,
    MarketReviewDailyMetric,
    MarketReviewLimitUpEvent,
    MarketReviewStockDaily,
)
from app.models.stock import Stock
from app.utils.time_utils import CN_TZ, today_cn


SOURCE_STATUS = "monitor_archive"
TURNOVER_SOURCE_STATUS = "exchange_turnover_archive"
_PRICE_FIELDS = {"close_price", "pre_close"}
_NONNEGATIVE_FIELDS = {"amount", "turnover_rate"}
_QUOTE_FIELDS = _PRICE_FIELDS | _NONNEGATIVE_FIELDS | {"change_pct"}


@dataclass(frozen=True)
class ArchiveBackfillDay:
    trade_date: date
    status: str
    stock_count: int = 0


class _MonitorSourceRow(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    trade_date: str
    stock_code: str = Field(pattern=r"^[0-9]{6}$")
    stock_name: str = Field(min_length=1, max_length=50)
    market: Literal["SH", "SZ", "BJ"]
    first_limit_up_time: str | None
    final_seal_time: str | None
    continuous_limit_up_days: int | None = Field(ge=0)
    open_count: int | None = Field(ge=0)
    is_final_sealed: bool | int | None
    current_status: Literal["unknown", "sealed", "resealed", "opened", "broken"] | None
    amount: float | None = Field(ge=0, allow_inf_nan=False)
    turnover_rate: float | None = Field(ge=0, allow_inf_nan=False)
    close_price: float | None = Field(ge=0, allow_inf_nan=False)
    limit_up_reason: str | None
    data_source: str | None


class _MonitorSourceFile(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    source_table: Literal["limit_up_records JOIN stocks"]
    amount_unit: Literal["CNY_10000"]
    trade_dates: list[str] = Field(min_length=1)
    rows: list[_MonitorSourceRow] = Field(min_length=1)


def _number(value: object) -> float | None:
    if type(value) not in (float, int):
        return None
    try:
        number = float(value)
    except OverflowError:
        return None
    return number if math.isfinite(number) else None


def _positive(value: object) -> float | None:
    number = _number(value)
    return number if number is not None and number > 0 else None


def _nonnegative(value: object) -> float | None:
    number = _number(value)
    return number if number is not None and number >= 0 else None


def _validate_evidence(
    dates: set[date],
    turnover_by_date: Mapping[date, float],
    quotes_by_date: Mapping[date, Mapping[str, Mapping[str, float | None]]],
) -> tuple[dict[date, float], dict[date, dict[str, dict[str, float]]]]:
    turnover: dict[date, float] = {}
    quotes: dict[date, dict[str, dict[str, float]]] = {}
    for day, value in turnover_by_date.items():
        amount = _positive(value)
        if type(day) is not date or day not in dates or amount is None:
            raise ValueError(f"Invalid exchange turnover for {day!s}: expected a requested trading day and positive amount")
        turnover[day] = amount
    for day, stocks in quotes_by_date.items():
        if type(day) is not date or day not in dates or not isinstance(stocks, Mapping):
            raise ValueError(f"Invalid quote evidence date or payload: {day!s}")
        quotes[day] = {}
        for stock_code, fields in stocks.items():
            if (
                not isinstance(stock_code, str)
                or not re.fullmatch(r"[0-9]{6}", stock_code)
                or not isinstance(fields, Mapping)
                or not set(fields).issubset(_QUOTE_FIELDS)
            ):
                raise ValueError(f"Invalid quote evidence fields for {day!s}/{stock_code!s}")
            selected = {}
            for name, value in fields.items():
                if value is None:
                    continue
                number = _number(value)
                if (
                    number is None
                    or (name in _PRICE_FIELDS and number <= 0)
                    or (name in _NONNEGATIVE_FIELDS and number < 0)
                ):
                    raise ValueError(f"Invalid quote value for {day!s}/{stock_code}/{name}")
                selected[name] = number
            quotes[day][stock_code] = selected
    return turnover, quotes


def _same_session_time(value: datetime | None, day: date) -> str | None:
    if value is None:
        return None
    local = value.astimezone(CN_TZ) if value.tzinfo is not None else value
    return local.time().isoformat(timespec="seconds") if local.date() == day else None


def _monitor_source_time(raw: str | None, day: date) -> datetime | None:
    if raw is None:
        return None
    if len(raw) < 19 or raw[10] not in {"T", " "}:
        raise ValueError(f"Invalid monitor source timestamp for {day}")
    value = datetime.fromisoformat(raw)
    local = value.astimezone(CN_TZ) if value.tzinfo is not None else value
    if local.date() != day or not time(9, 15) <= local.time() < time(15, 1):
        raise ValueError(f"Monitor source timestamp is outside the session for {day}")
    return value


def _validate_monitor_source(
    raw: Mapping[str, object] | None, sessions: set[date],
) -> dict[date, list[tuple[LimitUpRecord, Stock]]]:
    if raw is None:
        return {}
    source = _MonitorSourceFile.model_validate(raw)
    day_by_text = {day.isoformat(): day for day in sessions}
    declared = set(source.trade_dates)
    if len(declared) != len(source.trade_dates) or not declared.issubset(day_by_text):
        raise ValueError("Monitor source requires unique requested trading dates")
    found: dict[date, dict[str, tuple[LimitUpRecord, Stock]]] = {}
    for row in source.rows:
        if row.trade_date not in declared:
            raise ValueError(f"Monitor source row has undeclared trading date: {row.trade_date}")
        day = day_by_text[row.trade_date]
        rows = found.setdefault(day, {})
        if row.stock_code in rows or row.is_final_sealed not in (None, 0, 1):
            raise ValueError(f"Duplicate stock or invalid seal status for {day}/{row.stock_code}")
        first_time = _monitor_source_time(row.first_limit_up_time, day)
        final_time = _monitor_source_time(row.final_seal_time, day)
        first_clock = _same_session_time(first_time, day)
        final_clock = _same_session_time(final_time, day)
        if first_clock and final_clock and first_clock > final_clock:
            raise ValueError(f"Monitor source first seal follows final seal for {day}/{row.stock_code}")
        record = LimitUpRecord(
            trade_date=day,
            first_limit_up_time=first_time,
            final_seal_time=final_time,
            continuous_limit_up_days=row.continuous_limit_up_days,
            open_count=row.open_count,
            is_final_sealed=None if row.is_final_sealed is None else bool(row.is_final_sealed),
            current_status=row.current_status,
            amount=row.amount,
            turnover_rate=row.turnover_rate,
            close_price=row.close_price,
            limit_up_reason=row.limit_up_reason,
        )
        stock = Stock(stock_code=row.stock_code, stock_name=row.stock_name, market=row.market)
        rows[row.stock_code] = (record, stock)
    if len(found) != len(declared):
        raise ValueError("Monitor source must include stock rows for every declared trading date")
    return {day: [rows[code] for code in sorted(rows)] for day, rows in found.items()}


def _board_type(code: str) -> str:
    if code.startswith(("300", "301")):
        return "gem"
    if code.startswith(("688", "689")):
        return "star"
    if code.startswith(("4", "8", "92")):
        return "bj"
    return "main"


def _sample_stock(record: LimitUpRecord, stock: Stock, quote: Mapping[str, float]) -> dict:
    sealed = record.is_final_sealed
    if sealed is None:
        if record.current_status in {"sealed", "resealed"}:
            sealed = True
        elif record.current_status in {"opened", "broken"}:
            sealed = False
    return {
        "trade_date": record.trade_date.isoformat(),
        "stock_code": stock.stock_code,
        "stock_name": stock.stock_name,
        "board_type": _board_type(stock.stock_code),
        "is_st": None,  # Current stock metadata cannot establish its historical ST status.
        "today_touched_limit_up": True,
        "today_sealed_close": sealed is True,
        "today_opened_close": sealed is False,
        "today_broken": sealed is False,
        "today_continuous_days": max(record.continuous_limit_up_days or 0, 0),
        "yesterday_limit_up": None,
        "yesterday_continuous_days": None,
        "first_limit_time": _same_session_time(record.first_limit_up_time, record.trade_date),
        "final_seal_time": _same_session_time(record.final_seal_time, record.trade_date),
        "open_count": record.open_count if record.open_count is not None and record.open_count >= 0 else None,
        "amount": _positive(record.amount),  # Source zeros commonly denote a missing amount.
        "turnover_rate": _positive(record.turnover_rate),
        "close_price": _positive(record.close_price),
        "pre_close": None,
        "change_pct": None,
        "limit_up_reason": record.limit_up_reason or None,
        "data_quality_flag": SOURCE_STATUS,
        **quote,
    }


def _sum_amounts(rows: list[dict]) -> float | None:
    if any(row["amount"] is None for row in rows):
        return None
    return _nonnegative(sum(row["amount"] for row in rows))


def _sample_metrics(day: date, rows: list[dict], turnover: float | None) -> dict:
    sealed = [row for row in rows if row["today_sealed_close"]]
    broken = [row for row in rows if row["today_opened_close"]]
    status_known = bool(rows) and len(sealed) + len(broken) == len(rows)
    boards_known = status_known and all(row["today_continuous_days"] > 0 for row in sealed)
    heights = sorted({row["today_continuous_days"] for row in sealed}, reverse=True)
    gem_heights = [row["today_continuous_days"] for row in sealed if row["board_type"] in {"gem", "star"}]
    max_height = (heights[0] if heights else 0) if boards_known else None
    second_height = (heights[1] if len(heights) > 1 else 0) if boards_known else None
    gem_height = max(gem_heights, default=0) if boards_known else None

    def label(height: int | None, allowed: set[str] | None = None) -> str | None:
        if not height:
            return None
        return "\n".join(
            f"{row['stock_name']}{height}"
            for row in sealed
            if row["today_continuous_days"] == height and (allowed is None or row["board_type"] in allowed)
        ) or None

    return {
        "trade_date": day.isoformat(),
        "source_status": SOURCE_STATUS if rows else TURNOVER_SOURCE_STATUS,
        "limit_up_count": len(rows) if rows else None,
        "limit_down_count": None,
        "continuous_count": sum(row["today_continuous_days"] >= 2 for row in sealed) if boards_known else None,
        "max_board_height": max_height,
        "second_board_height": second_height,
        "gem_board_height": gem_height,
        "max_board_label": label(max_height),
        "second_board_label": label(second_height),
        "gem_board_label": label(gem_height, {"gem", "star"}),
        "first_to_second_rate": None,
        "continuous_promotion_rate": None,
        "seal_rate": round(len(sealed) * 100 / len(rows), 2) if status_known else None,
        "yesterday_limit_up_avg_change": None,
        "yesterday_continuous_avg_change": None,
        "market_turnover": turnover,
        "up_count_ex_st": None,
        "down_count_ex_st": None,
        "limit_up_amount": _sum_amounts(rows) if rows else None,
        "broken_amount": _sum_amounts(broken) if status_known else None,
    }


async def _process_day(
    db: AsyncSession,
    day: date,
    *,
    execute: bool,
    turnover: float | None,
    quotes: Mapping[str, Mapping[str, float]],
    monitor_source: list[tuple[LimitUpRecord, Stock]],
    include_turnover_only: bool,
) -> ArchiveBackfillDay:
    if db.get_bind().dialect.name != "sqlite":
        raise ValueError("Historical monitor backfill only supports SQLite")
    if execute:
        # Serialize the existence checks and insert with formal review writers.
        await db.execute(text("BEGIN IMMEDIATE"))
    for model, status in (
        (MarketReviewDailyMetric, "has_review"),
        (MarketReviewStockDaily, "incomplete_review"),
        (MarketReviewLimitUpEvent, "incomplete_review"),
        (MarketReviewArchiveSample, "has_sample"),
    ):
        if await db.scalar(select(model.id).where(model.trade_date == day).limit(1)) is not None:
            return ArchiveBackfillDay(day, status)
    source = (await db.execute(
        select(LimitUpRecord, Stock)
        .join(Stock, Stock.id == LimitUpRecord.stock_id)
        .where(LimitUpRecord.trade_date == day)
        .order_by(Stock.stock_code)
    )).tuples().all()
    if not source:
        source = monitor_source
    if not source and not (include_turnover_only and turnover is not None):
        return ArchiveBackfillDay(day, "no_source")
    rows = [_sample_stock(record, stock, quotes.get(stock.stock_code, {})) for record, stock in source]
    if execute:
        db.add(MarketReviewArchiveSample(
            trade_date=day,
            metric_json=_sample_metrics(day, rows, turnover),
            stocks_json=rows,
        ))
        await db.commit()
    return ArchiveBackfillDay(day, "inserted" if execute else "would_insert", len(rows))


async def backfill_market_review_archive(
    start_date: date,
    end_date: date,
    *,
    session_factory: async_sessionmaker[AsyncSession],
    calendar_loader: Callable[[date, date], Iterable[date]],
    current_date: date | None = None,
    execute: bool = False,
    turnover_by_date: Mapping[date, float] | None = None,
    quotes_by_date: Mapping[date, Mapping[str, Mapping[str, float | None]]] | None = None,
    monitor_source: Mapping[str, object] | None = None,
    include_turnover_only: bool = False,
) -> list[ArchiveBackfillDay]:
    today = current_date or today_cn()
    if start_date > end_date or end_date >= today:
        raise ValueError("Backfill requires an ordered range ending before the current China date")
    sessions = {
        day for day in await asyncio.to_thread(calendar_loader, start_date, end_date)
        if type(day) is date and start_date <= day <= end_date and day.weekday() < 5
    }
    turnover, quotes = _validate_evidence(sessions, turnover_by_date or {}, quotes_by_date or {})
    monitor_rows = _validate_monitor_source(monitor_source, sessions)
    results = []
    for day in sorted(sessions):
        async with session_factory() as db:
            results.append(await _process_day(
                db, day, execute=execute, turnover=turnover.get(day), quotes=quotes.get(day, {}),
                monitor_source=monitor_rows.get(day, []), include_turnover_only=include_turnover_only,
            ))
    return results
