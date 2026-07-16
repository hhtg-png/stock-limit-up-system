"""Stage-aware persisted evidence for production playbook snapshots."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime
from typing import Any, Callable, Optional

from sqlalchemy import desc, select

from app.models.market_review import DailyAnalysisRecord, MarketReviewDailyMetric
from app.models.trading_playbook import TradingPlanVersion
from app.utils.time_utils import CN_TZ


FULL_MARKET_CONTEXT_FIELDS = (
    "limit_up_count",
    "limit_up_count_prev",
    "trend_new_high_count",
    "trend_new_high_count_prev",
    "limit_down_count",
    "max_board_height",
    "seal_rate",
    "negative_feedback",
    "divergence_days",
    "sell_pressure_falling",
    "breadth_recovered",
    "prior_window",
    "sell_pressure_rising",
)


class ProductionMarketContextService:
    """Load only persisted fields that existed at the requested point in time."""

    def __init__(self, session_factory: Callable[[], Any]) -> None:
        self._session_factory = session_factory

    @staticmethod
    def _china_datetime(value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            return CN_TZ.localize(value)
        return value.astimezone(CN_TZ)

    async def load(
        self,
        trade_date: date,
        stage: str,
        as_of: datetime,
    ) -> dict[str, Any]:
        local_as_of = self._china_datetime(as_of)
        database_as_of = local_as_of.replace(tzinfo=None)
        async with self._session_factory() as db:
            metrics = list(
                (
                    await db.execute(
                        select(MarketReviewDailyMetric)
                        .where(
                            MarketReviewDailyMetric.trade_date <= trade_date,
                            MarketReviewDailyMetric.created_at <= database_as_of,
                            MarketReviewDailyMetric.updated_at <= database_as_of,
                            MarketReviewDailyMetric.source_status == "primary",
                        )
                        .order_by(desc(MarketReviewDailyMetric.trade_date))
                        .limit(3)
                    )
                )
                .scalars()
                .all()
            )
            current, previous = self._metric_pair(metrics, trade_date, stage)
            evidence_date = (
                current.trade_date
                if current is not None
                else previous.trade_date
                if previous is not None
                else trade_date
            )
            analysis = await self._daily_analysis(
                db,
                evidence_date,
                database_as_of,
            )
            prior_plan = await self._prior_plan(
                db,
                self._prior_plan_trade_date(current, previous, stage),
                database_as_of,
            )

        values: dict[str, Any] = {}
        quality = {key: "missing" for key in FULL_MARKET_CONTEXT_FIELDS}
        provenance: dict[str, dict[str, Any]] = {}

        def publish(key: str, value: Any, source: str, value_date: date) -> None:
            if value is None:
                return
            values[key] = value
            quality[key] = "ready"
            provenance[key] = {"source": source, "trade_date": value_date}

        if current is not None:
            for key, attr in (
                ("limit_up_count", "limit_up_count"),
                ("limit_down_count", "limit_down_count"),
                ("max_board_height", "max_board_height"),
                ("seal_rate", "seal_rate"),
            ):
                publish(
                    key,
                    getattr(current, attr, None),
                    "market_review_daily_metric",
                    current.trade_date,
                )
        if previous is not None:
            publish(
                "limit_up_count_prev",
                previous.limit_up_count,
                "market_review_daily_metric",
                previous.trade_date,
            )
        if current is not None and previous is not None:
            current_pressure = current.limit_down_count
            previous_pressure = previous.limit_down_count
            publish(
                "sell_pressure_falling",
                current_pressure < previous_pressure,
                "market_review_daily_metric_compare",
                current.trade_date,
            )
            publish(
                "sell_pressure_rising",
                current_pressure > previous_pressure,
                "market_review_daily_metric_compare",
                current.trade_date,
            )
            current_breadth = current.up_count_ex_st > current.down_count_ex_st
            previous_breadth = previous.up_count_ex_st > previous.down_count_ex_st
            publish(
                "breadth_recovered",
                current_breadth and not previous_breadth,
                "market_review_daily_metric_compare",
                current.trade_date,
            )

        if analysis is not None:
            cell = (analysis.auto_result or {}).get("负反馈")
            if isinstance(cell, Mapping) and isinstance(cell.get("items"), list):
                publish(
                    "negative_feedback",
                    bool(cell["items"]),
                    "daily_analysis_negative_feedback",
                    analysis.trade_date,
                )

        if prior_plan is not None:
            state = prior_plan.market_state_json or {}
            candidate_window = state.get("window")
            if isinstance(candidate_window, str) and candidate_window != "unknown":
                prior_window = candidate_window.strip()
                previous_divergence = state.get("divergence_days")
                if isinstance(previous_divergence, int) and previous_divergence >= 0:
                    divergence_days = (
                        previous_divergence + 1
                        if candidate_window
                        in {
                            "first_divergence",
                            "divergence_exhaustion",
                            "second_divergence",
                        }
                        else 0
                    )
                    publish(
                        "prior_window",
                        prior_window,
                        "trading_plan_market_state",
                        prior_plan.source_trade_date,
                    )
                    publish(
                        "divergence_days",
                        divergence_days,
                        "trading_plan_market_state",
                        prior_plan.source_trade_date,
                    )

        captured_candidates = [
            self._china_datetime(metric.updated_at)
            for metric in (current, previous)
            if metric is not None
        ]
        if analysis is not None:
            captured_candidates.append(self._china_datetime(analysis.generated_at))
        if prior_plan is not None:
            captured_candidates.append(self._china_datetime(prior_plan.generated_at))
        captured_at = max(captured_candidates) if captured_candidates else local_as_of
        complete = all(value in {"ready", "computed"} for value in quality.values())
        comparison_baseline = {}
        if previous is not None:
            comparison_baseline = {
                "trade_date": previous.trade_date,
                "limit_down_count": previous.limit_down_count,
                "up_count_ex_st": previous.up_count_ex_st,
                "down_count_ex_st": previous.down_count_ex_st,
            }
        return {
            "scope": "full_market",
            "trade_date": trade_date,
            "evidence_trade_date": evidence_date,
            "as_of": captured_at,
            "quality": "ready" if complete else "degraded",
            "stale": False,
            "field_quality": quality,
            "field_provenance": provenance,
            "comparison_baseline": comparison_baseline,
            **values,
        }

    @staticmethod
    def _metric_pair(metrics, trade_date: date, stage: str):
        if stage in {"overnight", "auction"}:
            eligible = [row for row in metrics if row.trade_date < trade_date]
            return (
                eligible[0] if eligible else None,
                eligible[1] if len(eligible) > 1 else None,
            )
        current = next(
            (row for row in metrics if row.trade_date == trade_date),
            None,
        )
        previous = next(
            (row for row in metrics if row.trade_date < trade_date),
            None,
        )
        return current, previous

    @staticmethod
    def _prior_plan_trade_date(current, previous, stage: str) -> Optional[date]:
        """Resolve plan lineage independently from metric comparison roles.

        Before the current session opens, ``current`` is already the most
        recent completed trading session and therefore owns the prior plan.
        During pre-close/after-close builds, ``current`` is today's metric and
        the prior plan belongs to ``previous``.
        """
        if stage in {"overnight", "auction"}:
            return current.trade_date if current is not None else None
        return previous.trade_date if previous is not None else None

    @staticmethod
    async def _daily_analysis(db, evidence_date: date, database_as_of: datetime):
        result = await db.execute(
            select(DailyAnalysisRecord)
            .where(
                DailyAnalysisRecord.trade_date == evidence_date,
                DailyAnalysisRecord.generated_at <= database_as_of,
                DailyAnalysisRecord.data_status == "ready",
            )
            .order_by(desc(DailyAnalysisRecord.generated_at))
            .limit(1)
        )
        return result.scalar_one_or_none()

    @staticmethod
    async def _prior_plan(
        db,
        prior_trade_date: Optional[date],
        database_as_of: datetime,
    ):
        if prior_trade_date is None:
            return None
        result = await db.execute(
            select(TradingPlanVersion)
            .where(
                TradingPlanVersion.source_trade_date == prior_trade_date,
                TradingPlanVersion.stage == "after_close",
                TradingPlanVersion.generated_at <= database_as_of,
                TradingPlanVersion.status.in_(("active", "confirmed", "draft")),
            )
            .order_by(
                desc(TradingPlanVersion.version_no),
                desc(TradingPlanVersion.generated_at),
                desc(TradingPlanVersion.id),
            )
            .limit(1)
        )
        return result.scalar_one_or_none()
