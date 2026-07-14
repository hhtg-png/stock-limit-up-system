"""Production adapters for the single application-owned playbook pipeline."""

from __future__ import annotations

from datetime import date, datetime
from functools import partial
from typing import Any, Callable, Optional

from sqlalchemy import desc, select

from app.data_collectors.tencent_api import tencent_api
from app.models.market_review import MarketReviewDailyMetric
from app.services.realtime_limit_up_service import realtime_limit_up_service
from app.utils.time_utils import CN_TZ

from .orchestrator import (
    TradingPlaybookOrchestrator,
    build_default_orchestrator,
)
from .market_data import _FULL_MARKET_CONTEXT_FIELDS


async def load_production_kline(
    stock_code: str,
    market: str,
    period: str,
    limit: int,
    *,
    stock_name: Optional[str] = None,
) -> list[dict[str, Any]]:
    """Reuse the application's established EastMoney/Sina K-line chain."""
    # Imported lazily so production composition cannot create an API import
    # cycle while app.main is mounting the v1 router.
    from app.api.v1.market import _fetch_kline_from_em

    return await _fetch_kline_from_em(
        stock_code,
        market,
        period,
        limit,
        stock_name=stock_name,
    )


async def load_production_realtime_limit_up(
    trade_date: date,
) -> list[dict[str, Any]]:
    """Read the existing bounded real-time limit-up pool."""
    return await realtime_limit_up_service.get_fast_limit_up_pool(trade_date)


def _china_datetime(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=CN_TZ)
    return value.astimezone(CN_TZ)


async def load_production_full_market_context(
    trade_date: date,
    stage: str,
    as_of: datetime,
    *,
    session_factory: Optional[Callable[[], Any]] = None,
) -> dict[str, Any]:
    """Load only market-review rows that existed at the requested point in time."""
    del stage  # The persisted daily facts have one point-in-time contract.
    local_as_of = _china_datetime(as_of)
    database_as_of = local_as_of.replace(tzinfo=None)
    if session_factory is None:
        from app.database import async_session_maker

        session_factory = async_session_maker

    async with session_factory() as db:
        result = await db.execute(
            select(MarketReviewDailyMetric)
            .where(
                MarketReviewDailyMetric.trade_date <= trade_date,
                MarketReviewDailyMetric.created_at <= database_as_of,
                MarketReviewDailyMetric.updated_at <= database_as_of,
            )
            .order_by(desc(MarketReviewDailyMetric.trade_date))
            .limit(2)
        )
        rows = list(result.scalars().all())

    current = next(
        (row for row in rows if row.trade_date == trade_date),
        None,
    )
    if current is None:
        raise LookupError(
            f"market review facts unavailable for {trade_date.isoformat()}"
        )
    previous = next(
        (row for row in rows if row.trade_date < trade_date),
        None,
    )
    source_status = str(getattr(current, "source_status", "") or "").lower()
    captured_at = _china_datetime(current.updated_at)
    if captured_at > local_as_of:
        raise LookupError(
            f"market review facts are newer than as_of for {trade_date.isoformat()}"
        )
    accepted = source_status == "primary"
    fields: dict[str, Any] = {}
    field_quality = {
        key: "missing" for key in _FULL_MARKET_CONTEXT_FIELDS
    }
    if accepted:
        for key in _FULL_MARKET_CONTEXT_FIELDS:
            if key == "limit_up_count_prev":
                value = (
                    getattr(previous, "limit_up_count", None)
                    if previous is not None
                    and str(getattr(previous, "source_status", "") or "").lower()
                    == "primary"
                    else None
                )
            else:
                value = getattr(current, key, None)
            if value is not None:
                fields[key] = value
                field_quality[key] = "ready"
    complete = all(value == "ready" for value in field_quality.values())
    return {
        "scope": "full_market",
        "trade_date": trade_date,
        "as_of": captured_at,
        "quality": "ready" if accepted and complete else "degraded",
        "stale": False,
        "field_quality": field_quality,
        **fields,
    }


def build_production_trading_playbook_orchestrator(
    *,
    next_trade_date: Callable[[date], date],
    session_factory: Optional[Callable[[], Any]] = None,
) -> TradingPlaybookOrchestrator:
    """Build the sole production playbook pipeline from real project sources."""
    full_market_loader = partial(
        load_production_full_market_context,
        session_factory=session_factory,
    )
    return build_default_orchestrator(
        quote_api=tencent_api,
        kline_loader=load_production_kline,
        realtime_limit_up_loader=load_production_realtime_limit_up,
        full_market_context_loader=full_market_loader,
        next_trade_date=next_trade_date,
    )


__all__ = [
    "build_production_trading_playbook_orchestrator",
    "load_production_full_market_context",
    "load_production_kline",
    "load_production_realtime_limit_up",
]
