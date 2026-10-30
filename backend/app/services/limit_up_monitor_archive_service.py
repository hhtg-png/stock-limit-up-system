"""Durable closed-session monitor data; live sessions always bypass this cache."""
from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import date, time, timedelta

from loguru import logger
from sqlalchemy import and_, func, select
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import async_session_maker
from app.models.limit_up import LimitUpMonitorArchive, LimitUpRecord
from app.models.market_review import MarketReviewDailyMetric, MarketReviewStockDaily
from app.models.stock import Stock
from app.schemas.limit_up import LimitUpRealtime
from app.services.limit_up_context_service import limit_up_context_service
from app.utils.time_utils import now_cn


def _positive_int_or_default(value, default=1):
    try:
        parsed = int(value)
        return parsed if parsed > 0 else default
    except (TypeError, ValueError):
        return default


def _format_hms(value):
    if value is None:
        return None
    return value.strftime("%H:%M:%S") if hasattr(value, "strftime") else str(value)


def _is_one_word_limit_up(first_time, open_count, sealed):
    first_time = _format_hms(first_time)
    return bool(sealed) and int(open_count or 0) == 0 and bool(first_time and first_time <= "09:25:59")


class LimitUpMonitorArchiveService:
    SCHEMA_VERSION = 1

    def __init__(self, *, session_factory=async_session_maker, context_service=limit_up_context_service,
                 now_provider=now_cn, retry_seconds=30):
        self.session_factory = session_factory
        self.context_service = context_service
        self.now_provider = now_provider
        self.retry_seconds = max(float(retry_seconds), 0)
        self._tasks = {}

    def is_closed_session(self, day: date) -> bool:
        now = self.now_provider()
        return day < now.date() or (day == now.date() and now.time().replace(tzinfo=None) >= time(15, 5))

    async def ensure_for_date(self, trade_date: date, force: bool = False) -> LimitUpMonitorArchive | None:
        """Read or build the exact date, without performing date fallback.

        Every caller shares the same build task. Cancelling an HTTP request
        does not cancel persistence for other requests or the scheduler.
        """
        if not self.is_closed_session(trade_date):
            return None
        return await asyncio.shield(self._start_build(trade_date, force=force))

    async def get_for_display(self, trade_date: date) -> LimitUpMonitorArchive | None:
        """Show valid partial rows immediately while their shared retry runs."""
        if not self.is_closed_session(trade_date):
            return None
        task = self._start_build(trade_date)
        async with self.session_factory() as db:
            archive = await self._get_archive(db, trade_date)
            if archive is not None and archive.status == "partial" and archive.schema_version == self.SCHEMA_VERSION:
                if trade_date == self.now_provider().date() and not await self._has_final_review(db, trade_date):
                    return None
                signature = await self._source_signature(db, trade_date, archive.previous_trade_date)
                if archive.source_signature == signature:
                    return archive
        archive = await asyncio.shield(task)
        if archive is not None and archive.status == "partial":
            if archive.schema_version != self.SCHEMA_VERSION:
                return None
            async with self.session_factory() as db:
                signature = await self._source_signature(db, trade_date, archive.previous_trade_date)
            if archive.source_signature != signature:
                return None
        return archive

    def _start_build(self, trade_date: date, *, force: bool = False):
        task = self._tasks.get(trade_date)
        if task is None:
            task = asyncio.create_task(self._ensure_for_date(trade_date, force=force))
            self._tasks[trade_date] = task

            def finished(done):
                if self._tasks.get(trade_date) is done:
                    self._tasks.pop(trade_date, None)
                if not done.cancelled():
                    done.exception()

            task.add_done_callback(finished)
        return task

    async def _ensure_for_date(self, day: date, *, force: bool):
        existing = None
        try:
            async with self.session_factory() as db:
                existing = await self._get_archive(db, day)
                if day == self.now_provider().date() and not await self._has_final_review(db, day):
                    return None
                previous = existing.previous_trade_date if existing is not None else None
                previous = previous or await self.context_service._previous_session(day)
                signature = await self._source_signature(db, day, previous)
                now = self.now_provider().replace(tzinfo=None)
                if existing is not None and existing.schema_version == self.SCHEMA_VERSION:
                    if not force and existing.source_signature == signature:
                        if existing.status == "ready":
                            return existing
                        if existing.status == "partial" and existing.updated_at + timedelta(seconds=self.retry_seconds) > now:
                            return existing
                items = await load_database_limit_up_items(db, day)
            if not items:
                return existing
            if force or (existing is not None and existing.source_signature != signature):
                self.context_service.invalidate_sessions(day, previous, [item.stock_code for item in items])
            enriched, complete = await self.context_service.enrich_for_archive(day, [item.model_dump() for item in items])
            validated = [LimitUpRealtime.model_validate(row) for row in enriched]
            if len(validated) != len(items) or {row.stock_code for row in validated} != {row.stock_code for row in items}:
                raise ValueError("Monitor enrichment changed the session's stock coverage")
            if any(row.trade_date != day or row.context_date != day for row in validated):
                raise ValueError("Monitor enrichment returned a different market session")
            if complete and (previous is None or any(row.previous_trade_date != previous for row in validated)):
                raise ValueError("Monitor enrichment returned an unverified previous session")
            if existing is not None and existing.status == "ready" and not complete:
                return existing
            payload = {"trade_date": day.isoformat(), "data": [row.model_dump(mode="json") for row in validated]}
            async with self.session_factory() as db:
                if signature != await self._source_signature(db, day, previous):
                    logger.info("Monitor sources changed while building {}; retrying on the next request", day)
                    return existing
                values = dict(trade_date=day, previous_trade_date=previous, payload_json=payload,
                              schema_version=self.SCHEMA_VERSION, status="ready" if complete else "partial",
                              total_count=len(validated), source_signature=signature, archived_at=now, updated_at=now)
                dialect = db.get_bind().dialect.name
                insert = postgresql_insert if dialect == "postgresql" else sqlite_insert
                statement = insert(LimitUpMonitorArchive).values(**values)
                statement = statement.on_conflict_do_update(
                    index_elements=["trade_date"],
                    set_={key: value for key, value in values.items() if key != "trade_date"},
                    # A second process may have completed the date while this
                    # process was waiting on a failed provider.
                    where=None if complete else LimitUpMonitorArchive.status != "ready",
                )
                await db.execute(statement)
                await db.commit()
                return await self._get_archive(db, day)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("Closed-session monitor archive unavailable for {}: {}", day, exc)
            return existing

    @staticmethod
    async def _get_archive(db, day):
        return (await db.execute(select(LimitUpMonitorArchive).where(
            LimitUpMonitorArchive.trade_date == day,
        ))).scalar_one_or_none()

    @staticmethod
    async def _has_final_review(db, day):
        metric = (await db.execute(select(MarketReviewDailyMetric).where(
            MarketReviewDailyMetric.trade_date == day,
        ))).scalar_one_or_none()
        if metric is None or metric.calc_version < 1 or metric.source_status not in {"primary", "partial"}:
            return False
        if metric.updated_at is None or metric.updated_at.date() != day or metric.updated_at.time() < time(15, 5):
            return False
        count = await db.scalar(select(func.count()).select_from(MarketReviewStockDaily).where(
            MarketReviewStockDaily.trade_date == day, MarketReviewStockDaily.today_touched_limit_up.is_(True),
        ))
        return count == metric.limit_up_count

    async def _source_signature(self, db, day, previous):
        days = [value for value in (previous, day) if value is not None]
        revision = {"schema": self.SCHEMA_VERSION, "days": [value.isoformat() for value in days]}
        for model in (LimitUpRecord, MarketReviewStockDaily, MarketReviewDailyMetric):
            rows = (await db.execute(select(model.trade_date, func.count(), func.max(model.updated_at)).where(
                model.trade_date.in_(days),
            ).group_by(model.trade_date).order_by(model.trade_date))).all()
            revision[model.__tablename__] = [[str(value) for value in row] for row in rows]
        archive = getattr(self.context_service, "archive_service", None)
        root = getattr(archive, "root_dir", None)
        if root is not None:
            def fingerprints():
                result = []
                for value in days:
                    try:
                        stat = (root / f"{value.isoformat()}.json").stat()
                        result.append([value.isoformat(), stat.st_mtime_ns, stat.st_size])
                    except FileNotFoundError:
                        result.append([value.isoformat(), None, None])
                return result
            revision["opening_auctions"] = await asyncio.to_thread(fingerprints)
        return hashlib.sha256(json.dumps(revision, sort_keys=True).encode()).hexdigest()


def _record_to_realtime(
    record: LimitUpRecord,
    stock: Stock,
    continuous_days_override: int | None = None,
) -> LimitUpRealtime:
    is_final_sealed = getattr(record, "is_final_sealed", True)
    if is_final_sealed is None:
        is_final_sealed = True
    current_status = getattr(record, "current_status", None)
    if not current_status or current_status == "unknown":
        current_status = "sealed" if is_final_sealed else "opened"
    open_count = _positive_int_or_default(getattr(record, "open_count", 0), default=0)
    first_limit_up_time = _format_hms(record.first_limit_up_time)
    final_seal_time = _format_hms(getattr(record, "final_seal_time", None))

    return LimitUpRealtime(
        stock_code=stock.stock_code,
        stock_name=stock.stock_name,
        trade_date=record.trade_date,
        first_limit_up_time=first_limit_up_time,
        final_seal_time=final_seal_time,
        limit_up_reason=getattr(record, "limit_up_reason", None),
        reason_category=getattr(record, "reason_category", None),
        continuous_limit_up_days=_positive_int_or_default(
            continuous_days_override
            if continuous_days_override is not None
            else getattr(record, "continuous_limit_up_days", None)
        ),
        open_count=open_count,
        is_sealed=is_final_sealed,
        is_one_word=_is_one_word_limit_up(first_limit_up_time, open_count, is_final_sealed),
        current_status=current_status,
        seal_amount=getattr(record, "seal_amount", None),
        seal_volume=getattr(record, "seal_volume", None),
        limit_up_price=getattr(record, "limit_up_price", None) or 0,
        current_price=(
            getattr(record, "close_price", None)
            or getattr(record, "limit_up_price", None)
            or 0
        ),
        turnover_rate=getattr(record, "turnover_rate", None),
        amount=getattr(record, "amount", None),
        tradable_market_value=None,
        market=stock.market,
        industry=stock.industry,
    )


async def _calculate_strict_database_continuous_days(
    db: AsyncSession,
    actual_date: date,
    rows,
    lookback_days: int = 30,
) -> dict[int, int]:
    stock_ids = [getattr(record, "stock_id", None) for record, _ in rows]
    stock_ids = [stock_id for stock_id in stock_ids if stock_id is not None]
    if not stock_ids:
        return {}

    date_result = await db.execute(
        select(LimitUpRecord.trade_date)
        .where(LimitUpRecord.trade_date <= actual_date)
        .group_by(LimitUpRecord.trade_date)
        .order_by(LimitUpRecord.trade_date.desc())
        .limit(lookback_days)
    )
    trade_dates = [row[0] for row in date_result.all()]
    if not trade_dates:
        return {}

    streak_result = await db.execute(
        select(
            LimitUpRecord.stock_id,
            LimitUpRecord.trade_date,
            LimitUpRecord.is_final_sealed,
        )
        .where(and_(
            LimitUpRecord.stock_id.in_(stock_ids),
            LimitUpRecord.trade_date.in_(trade_dates),
        ))
    )
    sealed_dates_by_stock: dict[int, set] = {}
    for stock_id, trade_day, is_final_sealed in streak_result.all():
        if is_final_sealed:
            sealed_dates_by_stock.setdefault(stock_id, set()).add(trade_day)

    strict_days: dict[int, int] = {}
    for record, _ in rows:
        stock_id = getattr(record, "stock_id", None)
        if stock_id is None:
            continue
        if not getattr(record, "is_final_sealed", True):
            strict_days[stock_id] = 1
            continue

        sealed_dates = sealed_dates_by_stock.get(stock_id, set())
        streak = 0
        for trade_day in trade_dates:
            if trade_day in sealed_dates:
                streak += 1
            else:
                break
        strict_days[stock_id] = max(streak, 1)

    return strict_days


async def _get_market_review_continuous_days(
    db: AsyncSession,
    actual_date: date,
    rows,
) -> dict[int, int]:
    stock_ids = [getattr(record, "stock_id", None) for record, _ in rows]
    stock_ids = [stock_id for stock_id in stock_ids if stock_id is not None]
    if not stock_ids:
        return {}

    result = await db.execute(
        select(
            MarketReviewStockDaily.stock_id,
            MarketReviewStockDaily.today_sealed_close,
            MarketReviewStockDaily.today_continuous_days,
        )
        .where(and_(
            MarketReviewStockDaily.stock_id.in_(stock_ids),
            MarketReviewStockDaily.trade_date == actual_date,
            MarketReviewStockDaily.today_touched_limit_up.is_(True),
        ))
    )

    review_days: dict[int, int] = {}
    for stock_id, today_sealed_close, today_continuous_days in result.all():
        if today_sealed_close:
            review_days[stock_id] = _positive_int_or_default(today_continuous_days)
        else:
            review_days[stock_id] = 1
    return review_days


async def load_database_limit_up_items(db, actual_date: date) -> list[LimitUpRealtime]:
    rows = (await db.execute(select(LimitUpRecord, Stock).join(Stock, LimitUpRecord.stock_id == Stock.id).where(
        LimitUpRecord.trade_date == actual_date,
    ))).all()
    review_days = await _get_market_review_continuous_days(db, actual_date, rows)
    strict_rows = [(record, stock) for record, stock in rows if getattr(record, "stock_id", None) not in review_days]
    strict_days = await _calculate_strict_database_continuous_days(db, actual_date, strict_rows)
    return [_record_to_realtime(record, stock, continuous_days_override=review_days.get(getattr(record, "stock_id", None))
                               or strict_days.get(getattr(record, "stock_id", None))) for record, stock in rows]


limit_up_monitor_archive_service = LimitUpMonitorArchiveService()
