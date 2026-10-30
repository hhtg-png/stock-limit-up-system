import unittest
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.database import Base
from app.models import DailyStatistics, LimitUpRecord, Stock, LimitUpMonitorArchive, BrokenBoardDailyArchive
from app.services.history_preparation_service import HistoryPreparationService
from app.utils.time_utils import CN_TZ


class HistoryPreparationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        self.addAsyncCleanup(self.engine.dispose)
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        self.clock = lambda: datetime(2026, 10, 30, 10, 30, tzinfo=CN_TZ)
        self.monitor = SimpleNamespace(ensure_for_date=AsyncMock(return_value=SimpleNamespace(status="ready")))
        self.report = SimpleNamespace(build_after_close=AsyncMock(return_value={"data_status": "ready"}))
        self.service = HistoryPreparationService(session_factory=self.sessions, clock=self.clock,
            monitor_service=self.monitor, report_service=self.report)

    async def seed(self, day, boards=(1, 2, 3, 4), opens=(0, 1, 0, 2)):
        async with self.sessions() as db:
            for index, (board, opened) in enumerate(zip(boards, opens)):
                code = f"{index:06d}"
                stock = await db.scalar(select(Stock).where(Stock.stock_code == code))
                if stock is None:
                    stock = Stock(stock_code=code, stock_name=f"stock-{index}", market="SZ")
                    db.add(stock)
                    await db.flush()
                db.add(LimitUpRecord(stock_id=stock.id, trade_date=day,
                    continuous_limit_up_days=board, open_count=opened, is_final_sealed=True))
            await db.commit()

    async def test_statistics_are_correct_and_repeated_build_updates_one_row(self):
        day = date(2026, 10, 29)
        await self.seed(day)
        self.assertTrue(await self.service.refresh_statistics(day))
        self.assertTrue(await self.service.refresh_statistics(day))
        async with self.sessions() as db:
            rows = list((await db.scalars(select(DailyStatistics))).all())
            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual((row.total_limit_up, row.new_limit_up, row.continuous_2,
                row.continuous_3, row.continuous_4_plus, row.break_count, row.break_rate),
                (4, 1, 1, 1, 1, 2, 50.0))
            record = await db.scalar(select(LimitUpRecord).where(LimitUpRecord.trade_date == day))
            record.open_count = 1
            await db.commit()
        await self.service.refresh_statistics(day)
        async with self.sessions() as db:
            rows = list((await db.scalars(select(DailyStatistics))).all())
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0].break_rate, 75.0)

    async def test_empty_and_current_intraday_dates_never_create_final_statistics(self):
        self.assertFalse(await self.service.refresh_statistics(date(2026, 10, 28)))
        await self.seed(date(2026, 10, 30))
        self.assertFalse(await self.service.refresh_statistics(date(2026, 10, 30)))
        result = await self.service.prepare_for_date(date(2026, 10, 30))
        self.assertTrue(result["skipped"])
        self.monitor.ensure_for_date.assert_not_awaited()
        self.report.build_after_close.assert_not_awaited()
        async with self.sessions() as db:
            self.assertEqual(list(await db.scalars(select(DailyStatistics))), [])

    async def test_one_archive_failure_does_not_block_other_archive_or_statistics(self):
        day = date(2026, 10, 29)
        await self.seed(day)
        self.monitor.ensure_for_date.side_effect = RuntimeError("upstream unavailable")
        result = await self.service.prepare_for_date(day)
        self.assertFalse(result["monitor_ready"])
        self.assertTrue(result["report_ready"])
        self.assertTrue(result["statistics_ready"])
        self.report.build_after_close.assert_awaited_once_with(day, force=False)

    async def test_pending_backfill_is_bounded_recent_first_and_failed_dates_do_not_starve_older_days(self):
        days = [date(2026, 10, number) for number in (26, 27, 28, 29, 30)]
        for day in days:
            await self.seed(day)
        self.monitor.ensure_for_date.side_effect = RuntimeError("temporarily missing")
        first = await self.service.prepare_pending(limit=2)
        second = await self.service.prepare_pending(limit=2)
        self.assertEqual(first["dates"], ["2026-10-29", "2026-10-28"])
        self.assertEqual(second["dates"], ["2026-10-27", "2026-10-26"])
        self.assertNotIn("2026-10-30", first["dates"] + second["dates"])

    async def test_same_scheduler_batch_cannot_run_overlapping(self):
        import asyncio
        await self.seed(date(2026, 10, 29))
        entered, release = asyncio.Event(), asyncio.Event()
        async def blocked(*args, **kwargs):
            entered.set()
            await release.wait()
            return SimpleNamespace(status="ready")
        self.monitor.ensure_for_date.side_effect = blocked
        task = asyncio.create_task(self.service.prepare_pending(limit=1))
        await entered.wait()
        duplicate = await self.service.prepare_pending(limit=1)
        self.assertTrue(duplicate["skipped"])
        release.set()
        await task
        self.monitor.ensure_for_date.assert_awaited_once()

    async def test_existing_ready_rows_are_revalidated_after_source_repairs(self):
        day = date(2026, 10, 29)
        await self.seed(day)
        async with self.sessions() as db:
            db.add(DailyStatistics(trade_date=day, total_limit_up=999))
            db.add(LimitUpMonitorArchive(trade_date=day, status="ready", schema_version=1,
                source_signature="obsolete", payload_json={"data": []}))
            db.add(BrokenBoardDailyArchive(trade_date=day, previous_trade_date=date(2026,10,28),
                source_fingerprint="obsolete", calc_version=1, point_json={"data_status":"ready"}))
            await db.commit()
        self.assertEqual((await self.service.prepare_pending(limit=2))["dates"], [day.isoformat()])
        self.monitor.ensure_for_date.assert_awaited_once_with(day, force=False)
        self.report.build_after_close.assert_awaited_once_with(day, force=False)
        async with self.sessions() as db:
            self.assertEqual((await db.scalar(select(DailyStatistics))).total_limit_up, 4)

    async def test_real_five_minute_ticks_do_not_starve_dates_behind_repeated_failures(self):
        from datetime import timedelta
        now = [self.clock()]
        self.service.clock = lambda: now[0]
        days = [date(2026, 10, number) for number in range(20,30)]
        for day in days:
            await self.seed(day)
        self.monitor.ensure_for_date.return_value = SimpleNamespace(status="partial")
        self.report.build_after_close.return_value = None
        seen = []
        for _ in range(5):
            seen.extend((await self.service.prepare_pending(limit=2))["dates"])
            now[0] += timedelta(minutes=5)
        self.assertEqual(seen, [day.isoformat() for day in reversed(days)])

    async def test_today_after_close_statistics_require_final_market_review(self):
        from app.models import MarketReviewDailyMetric, MarketReviewStockDaily
        self.service.clock = lambda: datetime(2026,10,30,16,0,tzinfo=CN_TZ)
        day = date(2026,10,30)
        await self.seed(day)
        self.assertFalse(await self.service.refresh_statistics(day))
        async with self.sessions() as db:
            db.add(MarketReviewDailyMetric(trade_date=day, calc_version=1, limit_up_count=4,
                source_status="primary", updated_at=datetime(2026,10,30,15,6)))
            for stock in await db.scalars(select(Stock)):
                db.add(MarketReviewStockDaily(trade_date=day, stock_id=stock.id,
                    stock_code=stock.stock_code, stock_name=stock.stock_name, today_touched_limit_up=True))
            await db.commit()
        self.assertTrue(await self.service.refresh_statistics(day))


if __name__ == "__main__":
    unittest.main()
