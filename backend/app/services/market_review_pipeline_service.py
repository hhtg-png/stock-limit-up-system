from __future__ import annotations

from datetime import date
from typing import Any, Dict, Iterable, Optional

from sqlalchemy import delete
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import async_session_maker
from app.models.market_review import (
    MarketReviewDailyMetric,
    MarketReviewLimitUpEvent,
    MarketReviewStockDaily,
)
from app.services.market_review_metrics_service import (
    MarketReviewMetricsService,
    market_review_metrics_service,
)
from app.services.market_review_source_service import (
    MarketReviewSourceService,
    market_review_source_service,
)


class MarketReviewPipelineService:
    """Builds and persists market review payloads from normalized source rows."""

    def __init__(
        self,
        metrics_service: Optional[MarketReviewMetricsService] = None,
        source_service: Optional[MarketReviewSourceService] = None,
        session_factory=async_session_maker,
    ) -> None:
        self.metrics_service = metrics_service or market_review_metrics_service
        self.source_service = source_service or market_review_source_service
        self.session_factory = session_factory

    async def build_payload_for_date(
        self,
        trade_date: date,
        normalized: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        normalized_data = normalized or await self.source_service.collect_for_date(trade_date)
        is_authoritative = self._resolve_authoritative_flag(normalized_data)
        stock_rows = [
            {**row, "trade_date": trade_date}
            for row in (normalized_data.get("stock_rows") or [])
        ]
        event_rows = [
            {**row, "trade_date": trade_date}
            for row in (normalized_data.get("event_rows") or [])
        ]

        metric_row = self.metrics_service.aggregate_daily_metrics(
            trade_date=trade_date,
            stock_rows=stock_rows,
            limit_down_count=normalized_data.get("limit_down_count", 0),
            market_turnover=normalized_data.get("market_turnover", 0.0),
            up_count_ex_st=normalized_data.get("up_count_ex_st", 0),
            down_count_ex_st=normalized_data.get("down_count_ex_st", 0),
        )
        metric_row["trade_date"] = trade_date
        metric_row["source_status"] = normalized_data.get("source_status", "unknown")

        return {
            "trade_date": trade_date,
            "is_authoritative": is_authoritative,
            "metric_row": metric_row,
            "stock_rows": stock_rows,
            "event_rows": event_rows,
            "source_status": metric_row["source_status"],
        }

    async def persist_payload(self, db: AsyncSession, payload: Dict[str, Any]) -> Dict[str, int]:
        self._ensure_authoritative_payload(payload)
        metric_row = dict(payload.get("metric_row") or {})
        trade_date = self._resolve_trade_date(payload, metric_row)
        metric_row["trade_date"] = trade_date
        stock_rows = self._canonicalize_trade_date(
            payload.get("stock_rows") or [],
            trade_date=trade_date,
        )
        event_rows = self._canonicalize_trade_date(
            payload.get("event_rows") or [],
            trade_date=trade_date,
        )

        if metric_row:
            await self._upsert_metric_row(db, metric_row)
        await self._replace_trade_date_rows(db, trade_date, stock_rows, event_rows)

        return {
            "metric_rows": 1 if metric_row else 0,
            "stock_rows": len(stock_rows),
            "event_rows": len(event_rows),
        }

    async def run_for_date(
        self,
        trade_date: date,
        calc_version: int = 1,
        normalized: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        payload = await self.build_payload_for_date(trade_date, normalized=normalized)
        self._ensure_authoritative_payload(payload)
        payload["metric_row"]["calc_version"] = calc_version

        async with self.session_factory() as session:
            await self.persist_payload(session, payload)
            await session.commit()

        return payload

    async def _upsert_metric_row(self, db: AsyncSession, metric_row: Dict[str, Any]) -> None:
        values = self._filter_model_columns(MarketReviewDailyMetric, metric_row)
        stmt = sqlite_insert(MarketReviewDailyMetric).values(**values)
        update_values = {
            key: stmt.excluded[key]
            for key in values
            if key not in {"id", "created_at", "trade_date"}
        }
        await db.execute(
            stmt.on_conflict_do_update(
                index_elements=["trade_date"],
                set_=update_values,
            )
        )

    async def _upsert_stock_rows(self, db: AsyncSession, stock_rows: Iterable[Dict[str, Any]]) -> None:
        for row in stock_rows:
            values = self._filter_model_columns(MarketReviewStockDaily, row)
            stmt = sqlite_insert(MarketReviewStockDaily).values(**values)
            update_values = {
                key: stmt.excluded[key]
                for key in values
                if key not in {"id", "created_at", "trade_date", "stock_code"}
            }
            await db.execute(
                stmt.on_conflict_do_update(
                    index_elements=["trade_date", "stock_code"],
                    set_=update_values,
                )
            )

    async def _upsert_event_rows(self, db: AsyncSession, event_rows: Iterable[Dict[str, Any]]) -> None:
        for row in event_rows:
            values = self._filter_model_columns(MarketReviewLimitUpEvent, row)
            stmt = sqlite_insert(MarketReviewLimitUpEvent).values(**values)
            update_values = {
                key: stmt.excluded[key]
                for key in values
                if key not in {"id", "created_at", "trade_date", "stock_code", "event_type", "event_seq"}
            }
            await db.execute(
                stmt.on_conflict_do_update(
                    index_elements=["trade_date", "stock_code", "event_type", "event_seq"],
                    set_=update_values,
                )
            )

    async def _replace_trade_date_rows(
        self,
        db: AsyncSession,
        trade_date: date,
        stock_rows: Iterable[Dict[str, Any]],
        event_rows: Iterable[Dict[str, Any]],
    ) -> None:
        await db.execute(
            delete(MarketReviewStockDaily).where(MarketReviewStockDaily.trade_date == trade_date)
        )
        await db.execute(
            delete(MarketReviewLimitUpEvent).where(MarketReviewLimitUpEvent.trade_date == trade_date)
        )
        if stock_rows:
            await self._upsert_stock_rows(db, stock_rows)
        if event_rows:
            await self._upsert_event_rows(db, event_rows)

    def _resolve_authoritative_flag(
        self,
        normalized_data: Dict[str, Any],
    ) -> bool:
        if "is_authoritative" in normalized_data:
            return normalized_data["is_authoritative"] is True
        return False

    def _ensure_authoritative_payload(self, payload: Dict[str, Any]) -> None:
        if payload.get("is_authoritative"):
            return
        trade_date = payload.get("trade_date")
        source_status = payload.get("source_status") or (payload.get("metric_row") or {}).get("source_status")
        raise RuntimeError(
            f"Market review payload for {trade_date} is not authoritative; "
            f"refusing to persist source_status={source_status!r}"
        )

    def _resolve_trade_date(
        self,
        payload: Dict[str, Any],
        metric_row: Dict[str, Any],
    ) -> date:
        for candidate in (payload.get("trade_date"), metric_row.get("trade_date")):
            if isinstance(candidate, date):
                return candidate
        raise RuntimeError("Authoritative market review payload is missing trade_date")

    def _canonicalize_trade_date(
        self,
        rows: Iterable[Dict[str, Any]],
        trade_date: date,
    ) -> list[Dict[str, Any]]:
        return [
            {
                **dict(row),
                "trade_date": trade_date,
            }
            for row in rows
        ]

    def _filter_model_columns(self, model, row: Dict[str, Any]) -> Dict[str, Any]:
        allowed_columns = set(model.__table__.columns.keys())
        return {key: value for key, value in row.items() if key in allowed_columns}


market_review_pipeline_service = MarketReviewPipelineService()
