from __future__ import annotations

from datetime import date
from typing import Dict, List


class MarketReviewMetricsService:
    """Pure aggregation service for end-of-day market review metrics."""

    def aggregate_daily_metrics(
        self,
        trade_date: date,
        stock_rows: List[Dict],
        limit_down_count: int,
        market_turnover: float,
        up_count_ex_st: int,
        down_count_ex_st: int,
    ) -> Dict:
        touched_rows = [row for row in stock_rows if row.get("today_touched_limit_up")]
        sealed_rows = [row for row in touched_rows if row.get("today_sealed_close")]
        opened_rows = [row for row in touched_rows if row.get("today_opened_close")]

        ladder_days = sorted(
            [
                self._to_int(row.get("today_continuous_days"))
                for row in touched_rows
                if self._to_int(row.get("today_continuous_days")) > 1
            ],
            reverse=True,
        )

        yesterday_first_board = [
            row
            for row in stock_rows
            if row.get("yesterday_limit_up") and self._to_int(row.get("yesterday_continuous_days")) == 1
        ]
        yesterday_continuous = [
            row for row in stock_rows if self._to_int(row.get("yesterday_continuous_days")) >= 2
        ]

        promoted_first_board = [
            row for row in yesterday_first_board if self._to_int(row.get("today_continuous_days")) >= 2
        ]
        promoted_continuous = [
            row
            for row in yesterday_continuous
            if self._to_int(row.get("today_continuous_days")) > self._to_int(row.get("yesterday_continuous_days"))
        ]

        gem_days = [
            self._to_int(row.get("today_continuous_days"))
            for row in touched_rows
            if row.get("board_type") in {"gem", "star"}
        ]

        def avg(values: List[float]) -> float:
            return round(sum(values) / len(values), 2) if values else 0.0

        yesterday_limit_up_changes = [
            float(row.get("change_pct") or 0)
            for row in stock_rows
            if row.get("yesterday_limit_up")
        ]
        yesterday_continuous_changes = [
            float(row.get("change_pct") or 0)
            for row in stock_rows
            if self._to_int(row.get("yesterday_continuous_days")) >= 2
        ]

        return {
            "trade_date": trade_date,
            "limit_up_count": len(touched_rows),
            "limit_down_count": self._to_int(limit_down_count),
            "continuous_count": len(
                [row for row in touched_rows if self._to_int(row.get("today_continuous_days")) >= 2]
            ),
            "max_board_height": ladder_days[0] if ladder_days else 0,
            "second_board_height": ladder_days[1] if len(ladder_days) > 1 else 0,
            "gem_board_height": max(gem_days) if gem_days else 0,
            "first_to_second_rate": round(len(promoted_first_board) * 100 / len(yesterday_first_board), 2)
            if yesterday_first_board
            else 0.0,
            "continuous_promotion_rate": round(len(promoted_continuous) * 100 / len(yesterday_continuous), 2)
            if yesterday_continuous
            else 0.0,
            "seal_rate": round(len(sealed_rows) * 100 / len(touched_rows), 2) if touched_rows else 0.0,
            "yesterday_limit_up_avg_change": avg(yesterday_limit_up_changes),
            "yesterday_continuous_avg_change": avg(yesterday_continuous_changes),
            "market_turnover": float(market_turnover or 0),
            "up_count_ex_st": self._to_int(up_count_ex_st),
            "down_count_ex_st": self._to_int(down_count_ex_st),
            "limit_up_amount": round(sum(float(row.get("amount") or 0) for row in touched_rows), 2),
            "broken_amount": round(sum(float(row.get("amount") or 0) for row in opened_rows), 2),
        }

    def _to_int(self, value: object) -> int:
        if value in (None, ""):
            return 0
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0


market_review_metrics_service = MarketReviewMetricsService()
