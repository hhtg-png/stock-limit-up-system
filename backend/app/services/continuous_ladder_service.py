from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Dict, Iterable, List, Optional


class ContinuousLadderService:
    """Build continuous ladder payloads from realtime limit-up data."""

    _STATUS_SORT_ORDER = {
        "sealed": 0,
        "opened": 1,
        "broken": 2,
    }

    def build_realtime_ladder(self, realtime_items: Iterable[Dict], min_days: int = 2) -> List[Dict]:
        ladder_map: Dict[int, List[Dict]] = defaultdict(list)

        for item in realtime_items:
            continuous_days = int(item.get("continuous_limit_up_days") or 1)
            if continuous_days < min_days:
                continue

            is_sealed = self._is_sealed(item)
            ladder_map[continuous_days].append(
                {
                    "stock_code": item.get("stock_code", ""),
                    "stock_name": item.get("stock_name", ""),
                    "first_limit_up_time": self._format_time(item.get("first_limit_up_time")),
                    "final_seal_time": self._format_time(item.get("final_seal_time")),
                    "reason": item.get("limit_up_reason", ""),
                    "is_sealed": is_sealed,
                    "open_count": int(item.get("open_count") or 0),
                    "change_pct": self._to_float(item.get("change_pct")),
                    "bid1_volume": self._to_float(item.get("bid1_volume")),
                    "turnover_rate": self._to_float(item.get("turnover_rate")),
                    "real_turnover_rate": self._calculate_real_turnover_rate(item),
                }
            )

        ladder_list: List[Dict] = []
        for continuous_days in sorted(ladder_map.keys(), reverse=True):
            stocks = ladder_map[continuous_days]
            stocks.sort(
                key=lambda stock: (
                    0 if stock["is_sealed"] else 1,
                    stock.get("first_limit_up_time") or "23:59:59",
                    stock.get("stock_code") or "",
                )
            )
            ladder_list.append(
                {
                    "continuous_days": continuous_days,
                    "count": len(stocks),
                    "stocks": stocks,
                }
            )

        return ladder_list

    def build_yesterday_ladder(
        self,
        yesterday_pool: Iterable[Dict],
        realtime_items: Iterable[Dict],
        min_days: int = 2,
    ) -> List[Dict]:
        realtime_status_map = {
            item.get("stock_code", ""): self._normalize_status(item)
            for item in realtime_items
            if item.get("stock_code")
        }

        ladder_map: Dict[int, List[Dict]] = defaultdict(list)
        for item in yesterday_pool:
            yesterday_days = int(item.get("ylbc") or 1)
            if yesterday_days < min_days:
                continue

            stock_code = item.get("c", "")
            today_status = realtime_status_map.get(stock_code, "broken")
            ladder_map[yesterday_days].append(
                {
                    "stock_code": stock_code,
                    "stock_name": item.get("n", ""),
                    "yesterday_days": yesterday_days,
                    "today_status": today_status,
                    "today_change_pct": self._to_float(item.get("zdp")),
                }
            )

        ladder_list: List[Dict] = []
        for continuous_days in sorted(ladder_map.keys(), reverse=True):
            stocks = ladder_map[continuous_days]
            stocks.sort(
                key=lambda stock: (
                    self._STATUS_SORT_ORDER.get(stock["today_status"], 99),
                    -(stock["today_change_pct"] or 0.0),
                    stock["stock_code"],
                )
            )

            ladder_list.append(
                {
                    "continuous_days": continuous_days,
                    "count": len(stocks),
                    "sealed_count": sum(1 for stock in stocks if stock["today_status"] == "sealed"),
                    "opened_count": sum(1 for stock in stocks if stock["today_status"] == "opened"),
                    "broken_count": sum(1 for stock in stocks if stock["today_status"] == "broken"),
                    "stocks": stocks,
                }
            )

        return ladder_list

    def _calculate_real_turnover_rate(self, item: Dict) -> Optional[float]:
        amount = self._to_float(item.get("amount"))
        tradable_market_value = self._to_float(item.get("tradable_market_value"))
        if amount is None or tradable_market_value is None or tradable_market_value <= 0:
            return None
        return round((amount / tradable_market_value) * 100, 2)

    def _normalize_status(self, item: Dict) -> str:
        status = item.get("current_status")
        if status in self._STATUS_SORT_ORDER:
            return status
        return "sealed" if self._is_sealed(item) else "opened"

    def _is_sealed(self, item: Dict) -> bool:
        if "is_sealed" in item:
            return bool(item.get("is_sealed"))
        if "is_final_sealed" in item:
            return bool(item.get("is_final_sealed"))
        return item.get("current_status") == "sealed"

    def _format_time(self, value: object) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.strftime("%H:%M:%S")
        if isinstance(value, str):
            return value
        return None

    def _to_float(self, value: object) -> Optional[float]:
        if value in (None, ""):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None


continuous_ladder_service = ContinuousLadderService()
