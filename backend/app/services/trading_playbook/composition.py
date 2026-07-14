"""Production adapters for the single application-owned playbook pipeline."""

from __future__ import annotations

from datetime import date, datetime
from functools import partial
from typing import Any, Callable, Optional

from app.data_collectors.tencent_api import tencent_api
from app.services.realtime_limit_up_service import realtime_limit_up_service

from .context_service import ProductionMarketContextService
from .orchestrator import (
    TradingPlaybookOrchestrator,
    build_default_orchestrator,
)


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


async def load_production_full_market_context(
    trade_date: date,
    stage: str,
    as_of: datetime,
    *,
    session_factory: Optional[Callable[[], Any]] = None,
) -> dict[str, Any]:
    """Load stage-aware persisted facts with field-level provenance."""
    if session_factory is None:
        from app.database import async_session_maker

        session_factory = async_session_maker
    return await ProductionMarketContextService(session_factory).load(
        trade_date,
        stage,
        as_of,
    )


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
