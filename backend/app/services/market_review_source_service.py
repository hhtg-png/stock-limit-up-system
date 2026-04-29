from __future__ import annotations

from datetime import date
from typing import Any, Dict


class MarketReviewSourceService:
    """Placeholder source adapter that returns a stable normalized shape."""

    async def collect_for_date(self, trade_date: date) -> Dict[str, Any]:
        return {
            "trade_date": trade_date,
            "stock_rows": [],
            "event_rows": [],
            "limit_down_count": 0,
            "market_turnover": 0.0,
            "up_count_ex_st": 0,
            "down_count_ex_st": 0,
            "source_status": "placeholder",
        }


market_review_source_service = MarketReviewSourceService()
