"""
实时涨停播报去重跟踪器

职责：
- 首次接入时只建立基线，不回放当日全部涨停
- 之后仅对新增进入涨停池的股票生成播报事件
- 交易日切换时自动重置状态
"""
from __future__ import annotations

from datetime import date
from typing import Dict, List, Optional, Set


class RealtimeLimitUpAlertTracker:
    """跟踪实时涨停池中的新增股票，避免首次连接时全量播报"""

    def __init__(self):
        self._trade_date: Optional[date] = None
        self._seen_codes: Set[str] = set()
        self._is_primed: bool = False

    def collect_new_alerts(self, records: List[Dict], trade_date: Optional[date] = None) -> List[Dict]:
        if trade_date is None:
            trade_date = date.today()

        if self._trade_date != trade_date:
            self._trade_date = trade_date
            self._seen_codes = set()
            self._is_primed = False

        current_codes = {
            record.get("stock_code", "")
            for record in records
            if record.get("stock_code")
        }

        if not self._is_primed:
            self._seen_codes = current_codes
            self._is_primed = True
            return []

        alerts: List[Dict] = []
        for record in records:
            stock_code = record.get("stock_code", "")
            if not stock_code or stock_code in self._seen_codes:
                continue

            first_limit_up_time = record.get("first_limit_up_time")
            alerts.append(
                {
                    "stock_code": stock_code,
                    "stock_name": record.get("stock_name", ""),
                    "time": first_limit_up_time.strftime("%H:%M:%S")
                    if hasattr(first_limit_up_time, "strftime")
                    else (first_limit_up_time or ""),
                    "reason": record.get("limit_up_reason"),
                    "continuous_days": record.get("continuous_limit_up_days", 1),
                }
            )

        self._seen_codes.update(current_codes)
        return alerts
