from __future__ import annotations

from datetime import date, datetime, time
from typing import Any, Dict, Iterable, Optional

from sqlalchemy import delete, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import async_session_maker
from app.models.limit_up import LimitUpRecord
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
        limit_sync_result = await self._sync_limit_up_records_from_review(db, trade_date, stock_rows)

        return {
            "metric_rows": 1 if metric_row else 0,
            "stock_rows": len(stock_rows),
            "event_rows": len(event_rows),
            **limit_sync_result,
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
        # SQLAlchemy's ``onupdate`` hook is not applied by SQLite's explicit
        # ``ON CONFLICT DO UPDATE`` statement. Keep this capture time fresh so
        # the after-close barrier can distinguish the 15:05 rebuild from the
        # 14:50 intraday snapshot.
        values["updated_at"] = datetime.now()
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

    async def _sync_limit_up_records_from_review(
        self,
        db: AsyncSession,
        trade_date: date,
        stock_rows: Iterable[Dict[str, Any]],
    ) -> Dict[str, int]:
        touched_rows = [
            row
            for row in stock_rows
            if row.get("today_touched_limit_up") and row.get("stock_id")
        ]
        touched_stock_ids = {int(row["stock_id"]) for row in touched_rows}

        existing_records = (
            await db.execute(
                select(LimitUpRecord).where(LimitUpRecord.trade_date == trade_date)
            )
        ).scalars().all()
        existing_by_stock_id = {
            int(record.stock_id): record
            for record in existing_records
            if record.stock_id is not None
        }

        upsert_count = 0
        for row in touched_rows:
            stock_id = int(row["stock_id"])
            record = existing_by_stock_id.get(stock_id)
            if record is None:
                record = LimitUpRecord(stock_id=stock_id, trade_date=trade_date)
                db.add(record)
                existing_by_stock_id[stock_id] = record

            self._apply_review_row_to_limit_up_record(record, row, trade_date)
            upsert_count += 1

        deleted_count = 0
        for record in existing_records:
            if record.stock_id not in touched_stock_ids:
                await db.delete(record)
                deleted_count += 1

        return {
            "limit_up_records": upsert_count,
            "limit_up_deleted": deleted_count,
        }

    def _apply_review_row_to_limit_up_record(
        self,
        record: LimitUpRecord,
        row: Dict[str, Any],
        trade_date: date,
    ) -> None:
        sealed = bool(row.get("today_sealed_close"))
        continuous_days = self._to_int(row.get("today_continuous_days"))
        record.continuous_limit_up_days = continuous_days if sealed and continuous_days > 0 else 1
        record.is_final_sealed = sealed
        record.current_status = "sealed" if sealed else "opened"
        record.first_limit_up_time = self._combine_date_time(trade_date, row.get("first_limit_time"))
        record.final_seal_time = (
            self._combine_date_time(trade_date, row.get("final_seal_time"))
            if sealed
            else None
        )
        record.open_count = self._to_int(row.get("open_count"))
        record.limit_up_reason = row.get("limit_up_reason") or record.limit_up_reason
        record.reason_category = row.get("reason_category") or record.reason_category
        record.close_price = self._to_float(row.get("close_price")) or record.close_price
        record.limit_up_price = self._resolve_limit_up_price(row, record.limit_up_price)
        record.turnover_rate = self._to_float(row.get("turnover_rate"))
        record.amount = self._to_float(row.get("amount")) or 0.0
        record.seal_amount = self._resolve_seal_amount(row, record.seal_amount, sealed)
        record.data_source = "MARKET_REVIEW"
        record.is_validated = True
        record.updated_at = datetime.now()

    def _resolve_limit_up_price(self, row: Dict[str, Any], existing_price: Optional[float]) -> Optional[float]:
        for value in (
            row.get("limit_up_price"),
            row.get("close_price") if row.get("today_sealed_close") else None,
        ):
            resolved = self._to_float(value)
            if resolved is not None and resolved > 0:
                return resolved

        pre_close = self._to_float(row.get("pre_close"))
        if pre_close is not None and pre_close > 0:
            return round(pre_close * (1 + self._limit_ratio(row)), 2)

        existing = self._to_float(existing_price)
        if existing is not None and existing > 0:
            return existing
        return None

    def _resolve_seal_amount(
        self,
        row: Dict[str, Any],
        existing_amount: Optional[float],
        sealed: bool,
    ) -> float:
        if not sealed:
            return 0.0

        source_amount = self._to_float(row.get("seal_amount"))
        if source_amount is not None and source_amount > 0:
            return source_amount

        existing = self._to_float(existing_amount)
        if existing is not None and existing > 0:
            return existing
        return 0.0

    def _limit_ratio(self, row: Dict[str, Any]) -> float:
        stock_code = str(row.get("stock_code") or "")
        board_type = str(row.get("board_type") or "main")
        if row.get("is_st"):
            return 0.05
        if board_type == "bj" or stock_code.startswith("8"):
            return 0.30
        if board_type in {"gem", "star"}:
            return 0.20
        return 0.10

    def _combine_date_time(self, trade_date: date, value: Any) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, time):
            return datetime.combine(trade_date, value)
        return None

    def _resolve_authoritative_flag(
        self,
        normalized_data: Dict[str, Any],
    ) -> bool:
        if "is_authoritative" in normalized_data:
            return normalized_data["is_authoritative"] is True
        return False

    def _ensure_authoritative_payload(self, payload: Dict[str, Any]) -> None:
        if payload.get("is_authoritative") is True:
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

    def _to_int(self, value: Any) -> int:
        if value in (None, ""):
            return 0
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    def _to_float(self, value: Any) -> Optional[float]:
        if value in (None, ""):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None


market_review_pipeline_service = MarketReviewPipelineService()
