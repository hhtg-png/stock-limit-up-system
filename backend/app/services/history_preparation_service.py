"""Build small, reusable daily database results outside interactive requests."""

from __future__ import annotations

import asyncio
from datetime import date, time, timedelta

from loguru import logger
from sqlalchemy import case, func, select, union

from app.database import async_session_maker
from app.models.limit_up import LimitUpRecord
from app.models.market_data import DailyStatistics
from app.models.market_review import MarketReviewDailyMetric, MarketReviewStockDaily
from app.utils.time_utils import CN_TZ, now_cn


class HistoryPreparationService:
    def __init__(self, *, session_factory=async_session_maker, clock=now_cn,
                 monitor_service=None, report_service=None, calendar_loader=None):
        self.session_factory = session_factory
        self.clock = clock
        self.monitor_service = monitor_service
        self.report_service = report_service
        self.calendar_loader = calendar_loader
        self._calendar_lock = asyncio.Lock()
        self._calendar_day: date | None = None
        self._calendar_dates: set[date] = set()
        self._calendar_retry_at = None
        self._batch_lock = asyncio.Lock()
        self._last_attempt: dict[date, int] = {}
        self._attempt_sequence = 0

    def _now(self):
        value = self.clock()
        return value.astimezone(CN_TZ) if value.tzinfo else value.replace(tzinfo=CN_TZ)

    def _closed(self, day):
        now = self._now()
        return day < now.date() or day == now.date() and now.time().replace(tzinfo=None) >= time(15, 5)

    async def _trading_dates(self) -> set[date]:
        now = self._now()
        today = now.date()
        if self._calendar_day == today and (self._calendar_retry_at is None or now < self._calendar_retry_at):
            return self._calendar_dates
        async with self._calendar_lock:
            if self._calendar_day == today and (self._calendar_retry_at is None or now < self._calendar_retry_at):
                return self._calendar_dates

            def load():
                if self.calendar_loader is not None:
                    return self.calendar_loader()
                from app.data_collectors.scheduler import _get_cn_trading_dates
                return _get_cn_trading_dates(date(1990, 1, 1), today)

            try:
                dates = set(await asyncio.wait_for(asyncio.to_thread(load), timeout=5))
                if not dates:
                    raise ValueError("Empty trading calendar")
                self._calendar_dates = dates
                self._calendar_retry_at = None
            except Exception as exc:
                # Legacy raw records and final review facts both contain some
                # holiday rows. Skip generation until the calendar is verified.
                self._calendar_dates = set()
                self._calendar_retry_at = now + timedelta(minutes=5)
                logger.warning("History preparation calendar unavailable; retrying in 5 minutes: {}", exc)
            self._calendar_day = today
            return self._calendar_dates

    async def _is_trading_day(self, day: date) -> bool:
        return day in await self._trading_dates()

    async def refresh_statistics(self, trade_date: date) -> bool:
        if not self._closed(trade_date) or not await self._is_trading_day(trade_date):
            return False
        record = LimitUpRecord
        async with self.session_factory() as db:
            if trade_date == self._now().date():
                metric = await db.scalar(select(MarketReviewDailyMetric).where(
                    MarketReviewDailyMetric.trade_date == trade_date))
                if (metric is None or metric.calc_version < 1
                        or metric.source_status not in {"primary", "partial"}
                        or metric.updated_at is None or metric.updated_at.date() != trade_date
                        or metric.updated_at.time() < time(15, 5)):
                    return False
                touched = await db.scalar(select(func.count()).select_from(MarketReviewStockDaily).where(
                    MarketReviewStockDaily.trade_date == trade_date,
                    MarketReviewStockDaily.today_touched_limit_up.is_(True)))
                if touched != metric.limit_up_count:
                    return False
            row = (await db.execute(select(
                func.count(record.id),
                *(func.sum(case((condition, 1), else_=0)) for condition in (
                    record.continuous_limit_up_days == 1,
                    record.continuous_limit_up_days == 2,
                    record.continuous_limit_up_days == 3,
                    record.continuous_limit_up_days >= 4,
                    record.open_count > 0,
                )),
            ).where(record.trade_date == trade_date))).one()
            total = int(row[0] or 0)
            if not total:
                return False
            values = dict(zip(("total_limit_up", "new_limit_up", "continuous_2", "continuous_3",
                               "continuous_4_plus", "break_count"), (int(value or 0) for value in row)))
            values.update(trade_date=trade_date,
                          break_rate=round(values["break_count"] / total * 100, 2),
                          updated_at=self._now().replace(tzinfo=None))
            if db.bind.dialect.name == "postgresql":
                from sqlalchemy.dialects.postgresql import insert
            else:
                from sqlalchemy.dialects.sqlite import insert
            statement = insert(DailyStatistics).values(**values)
            await db.execute(statement.on_conflict_do_update(
                index_elements=["trade_date"],
                set_={key: getattr(statement.excluded, key) for key in values if key != "trade_date"},
            ))
            await db.commit()
        return True

    async def available_dates(self) -> list[date]:
        async with self.session_factory() as db:
            dates = list(await db.scalars(union(
                select(LimitUpRecord.trade_date),
                select(MarketReviewDailyMetric.trade_date).where(MarketReviewDailyMetric.calc_version >= 1),
            )))
        trading_dates = await self._trading_dates()
        return sorted((day for day in dates if day in trading_dates and self._closed(day)), reverse=True)

    async def prepare_for_date(self, trade_date: date, *, force=False) -> dict:
        result = {"trade_date": trade_date.isoformat(),
                  "skipped": not self._closed(trade_date) or not await self._is_trading_day(trade_date),
                  "statistics_ready": False, "monitor_ready": False, "report_ready": False}
        if result["skipped"]:
            return result
        try:
            result["statistics_ready"] = await self.refresh_statistics(trade_date)
        except Exception as exc:
            logger.warning("Daily statistics preparation failed for {}: {}", trade_date, exc)

        monitor = self.monitor_service
        if monitor is None:
            from app.services.limit_up_monitor_archive_service import limit_up_monitor_archive_service
            monitor = limit_up_monitor_archive_service
        report = self.report_service
        if report is None:
            from app.services.broken_board_performance_service import broken_board_performance_service
            report = broken_board_performance_service

        async def prepare(name, operation):
            try:
                value = await asyncio.wait_for(operation, timeout=120)
                result[name] = (getattr(value, "status", None) == "ready" if name == "monitor_ready"
                                else isinstance(value, dict) and value.get("data_status") == "ready")
            except Exception as exc:
                logger.warning("{} preparation failed for {}: {}", name, trade_date, exc)

        await asyncio.gather(
            prepare("monitor_ready", monitor.ensure_for_date(trade_date, force=force)),
            prepare("report_ready", report.build_after_close(trade_date, force=force)),
        )
        return result

    async def prepare_pending(self, *, limit=2) -> dict:
        if self._batch_lock.locked():
            return {"skipped": True, "dates": [], "results": []}
        async with self._batch_lock:
            dates = await self.available_dates()
            # Revisit completed dates too: both archive services validate source
            # revisions before returning a cheap database hit. Old ready rows
            # must not conceal repaired facts. Least recently attempted dates
            # progress even when the newest sessions repeatedly lack a source.
            pending = sorted(dates, key=lambda day: (self._last_attempt.get(day, 0), -day.toordinal()))
            results = []
            for day in pending[:max(0, limit)]:
                self._attempt_sequence += 1
                self._last_attempt[day] = self._attempt_sequence
                result = await self.prepare_for_date(day)
                results.append(result)
            return {"skipped": False, "dates": [row["trade_date"] for row in results], "results": results}

    async def prepare_latest(self) -> dict | None:
        if self._batch_lock.locked():
            return None
        async with self._batch_lock:
            dates = await self.available_dates()
            return await self.prepare_for_date(dates[0]) if dates else None


history_preparation_service = HistoryPreparationService()
