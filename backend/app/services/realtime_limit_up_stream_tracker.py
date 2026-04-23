"""
Realtime list snapshot/delta tracker for WebSocket sync.
"""
from __future__ import annotations

import copy
from datetime import date, datetime
from typing import Any, Dict, List, Optional


class RealtimeLimitUpStreamTracker:
    """Track realtime list state and emit snapshot/delta messages."""

    def __init__(self):
        self._trade_date: Optional[date] = None
        self._items_by_code: Dict[str, Dict[str, Any]] = {}

    def sync(self, realtime_data: List[Dict[str, Any]], trade_date: date) -> Optional[Dict[str, Any]]:
        """Build a snapshot for first sync, then deltas for subsequent changes."""
        items_by_code = self._normalize_items(realtime_data, trade_date)

        if self._trade_date != trade_date or not self._items_by_code:
            self._trade_date = trade_date
            self._items_by_code = items_by_code
            return self._build_snapshot(trade_date, items_by_code)

        previous_items = self._items_by_code
        upsert = [
            copy.deepcopy(item)
            for code, item in items_by_code.items()
            if previous_items.get(code) != item
        ]
        upsert.sort(key=lambda item: item.get("stock_code", ""))
        removed = sorted(code for code in previous_items.keys() if code not in items_by_code)

        self._trade_date = trade_date
        self._items_by_code = items_by_code

        if not upsert and not removed:
            return None

        return {
            "type": "limit_up_delta",
            "data": {
                "trade_date": trade_date.isoformat(),
                "upsert": upsert,
                "remove": removed,
            },
        }

    def get_cached_snapshot(self, trade_date: date) -> Optional[Dict[str, Any]]:
        """Return cached snapshot for the current trade date."""
        if self._trade_date != trade_date or not self._items_by_code:
            return None

        return self._build_snapshot(trade_date, self._items_by_code)

    def _build_snapshot(self, trade_date: date, items_by_code: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        return {
            "type": "limit_up_snapshot",
            "data": {
                "trade_date": trade_date.isoformat(),
                "items": [copy.deepcopy(item) for item in items_by_code.values()],
            },
        }

    def _normalize_items(
        self,
        realtime_data: List[Dict[str, Any]],
        trade_date: date,
    ) -> Dict[str, Dict[str, Any]]:
        items_by_code: Dict[str, Dict[str, Any]] = {}

        for item in realtime_data:
            stock_code = item.get("stock_code", "")
            if not stock_code:
                continue

            normalized = {
                key: self._serialize_value(value)
                for key, value in item.items()
            }
            normalized["trade_date"] = trade_date.isoformat()
            items_by_code[stock_code] = normalized

        return items_by_code

    def _serialize_value(self, value: Any) -> Any:
        if isinstance(value, datetime):
            return value.strftime("%H:%M:%S")
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, list):
            return [self._serialize_value(item) for item in value]
        if isinstance(value, dict):
            return {
                key: self._serialize_value(item)
                for key, item in value.items()
            }
        return value
