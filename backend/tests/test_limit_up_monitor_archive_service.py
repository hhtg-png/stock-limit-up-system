import asyncio
import importlib.util
import tempfile
import unittest
from datetime import date, datetime, time
from pathlib import Path
from unittest.mock import AsyncMock, patch

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.database import Base
from app.models.limit_up import LimitUpRecord
from app.models.market_review import MarketReviewDailyMetric, MarketReviewStockDaily
from app.models.stock import Stock
from app.services.limit_up_context_service import LimitUpContextService
from app.services.opening_auction_archive_service import OpeningAuctionArchiveService

if importlib.util.find_spec("app.services.limit_up_monitor_archive_service"):
    from app.services.limit_up_monitor_archive_service import LimitUpMonitorArchiveService
else:
    LimitUpMonitorArchiveService = None


DAY, PREVIOUS, NEXT = date(2026, 10, 23), date(2026, 10, 22), date(2026, 10, 26)


class LimitUpMonitorArchiveTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.assertIsNotNone(LimitUpMonitorArchiveService, "durable monitor archives are missing")
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.engine = create_async_engine("sqlite+aiosqlite:///" + str(Path(self.directory.name) / "test.db"))
        self.addAsyncCleanup(self.engine.dispose)
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        self.now = datetime(2026, 10, 26, 10)
        self.auctions = OpeningAuctionArchiveService(root_dir=Path(self.directory.name) / "auctions")
        await self.auctions.save_percentages(PREVIOUS, {"000001": 0, "000002": -2})
        await self.auctions.save_percentages(DAY, {"000001": 5, "000002": 0})
        self.history = AsyncMock(return_value={})
        self.pool = AsyncMock(return_value=[])
        self.context = self.new_context()
        self.service = self.new_service()
        self.addAsyncCleanup(self.stop_builds)
        async with self.sessions() as db:
            db.add_all([Stock(id=i, stock_code=f"{i:06}", stock_name=f"stock {i}", market="SZ") for i in (1, 2)])
            for day, codes in ((PREVIOUS, (1,)), (DAY, (1, 2))):
                db.add(MarketReviewDailyMetric(trade_date=day, calc_version=1, source_status="primary", limit_up_count=len(codes),
                                              updated_at=datetime.combine(day, time(15, 6))))
                for code in codes:
                    db.add(LimitUpRecord(stock_id=code, trade_date=day, first_limit_up_time=datetime.combine(day, time(9, 31)),
                                        final_seal_time=datetime.combine(day, time(14, code)), is_final_sealed=True,
                                        close_price=11, limit_up_price=11, seal_amount=100 * code, reason_category="机器人"))
                    db.add(MarketReviewStockDaily(stock_id=code, stock_code=f"{code:06}", stock_name=f"stock {code}",
                                                 trade_date=day, today_touched_limit_up=True, today_sealed_close=True,
                                                 today_continuous_days=2 if code == 1 else 1,
                                                 first_limit_time=time(9, 31), final_seal_time=time(14, code)))
            await db.commit()

    async def stop_builds(self):
        tasks = list(self.service._tasks.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    def new_context(self):
        return LimitUpContextService(calendar_loader=lambda *args: [PREVIOUS, DAY, NEXT], session_factory=self.sessions,
                                     archive_service=self.auctions, history_fetcher=self.history, pool_fetcher=self.pool,
                                     today_provider=lambda: self.now.date(), now_provider=lambda: self.now)

    def new_service(self, context=None):
        return LimitUpMonitorArchiveService(session_factory=self.sessions, context_service=context or self.context,
                                           now_provider=lambda: self.now, retry_seconds=30)

    async def test_final_enriched_rows_survive_service_restart_without_provider_requests(self):
        archive = await self.service.ensure_for_date(DAY)
        self.assertEqual(archive.status, "ready")
        self.assertEqual(archive.total_count, 2)
        rows = archive.payload_json["data"]
        self.assertEqual(rows[0]["today_open_change_pct"], 5)
        self.assertEqual(rows[0]["yesterday_open_change_pct"], 0)
        self.assertEqual(rows[0]["yesterday_final_seal_time"], "14:01:00")
        self.assertIsNone(rows[1]["yesterday_first_limit_up_time"])
        self.assertIsNone(rows[1]["yesterday_final_seal_time"])
        self.pool.assert_not_awaited()
        self.history.assert_not_awaited()
        fresh_context = self.new_context()
        with patch.object(fresh_context, "enrich_for_archive", AsyncMock(side_effect=AssertionError("archive must survive restart"))) as enrich:
            reloaded = await self.new_service(fresh_context).ensure_for_date(DAY)
        self.assertEqual(reloaded.payload_json, archive.payload_json)
        enrich.assert_not_awaited()

    async def test_concurrent_builds_share_one_task_and_caller_cancellation_keeps_build_alive(self):
        entered, release = asyncio.Event(), asyncio.Event()
        original = self.context.enrich_for_archive

        async def held(*args, **kwargs):
            entered.set()
            await release.wait()
            return await original(*args, **kwargs)

        with patch.object(self.context, "enrich_for_archive", AsyncMock(side_effect=held)) as enrich:
            cancelled = asyncio.create_task(self.service.ensure_for_date(DAY))
            await asyncio.wait_for(entered.wait(), 1)
            others = [asyncio.create_task(self.service.ensure_for_date(DAY)) for _ in range(4)]
            cancelled.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await cancelled
            release.set()
            archives = await asyncio.gather(*others)
        self.assertTrue(all(row.status == "ready" for row in archives))
        self.assertEqual(len({row.id for row in archives}), 1)
        enrich.assert_awaited_once()

    async def test_failure_and_partial_rebuild_preserve_existing_ready_payload(self):
        original = await self.service.ensure_for_date(DAY)
        with patch.object(self.context, "enrich_for_archive", AsyncMock(side_effect=RuntimeError("provider down"))):
            failed = await self.service.ensure_for_date(DAY, force=True)
        self.assertEqual(failed.payload_json, original.payload_json)
        partial_rows = [dict(row, today_open_change_pct=None) for row in original.payload_json["data"]]
        with patch.object(self.context, "enrich_for_archive", AsyncMock(return_value=(partial_rows, False))):
            partial = await self.service.ensure_for_date(DAY, force=True)
        self.assertEqual(partial.status, "ready")
        self.assertEqual(partial.payload_json, original.payload_json)

    async def test_empty_dates_do_not_create_or_overwrite_archives(self):
        self.assertIsNone(await self.service.ensure_for_date(date(2026, 10, 21)))
        original = await self.service.ensure_for_date(DAY)
        async with self.sessions() as db:
            from sqlalchemy import delete
            await db.execute(delete(LimitUpRecord).where(LimitUpRecord.trade_date == DAY))
            await db.commit()
        preserved = await self.service.ensure_for_date(DAY, force=True)
        self.assertEqual(preserved.payload_json, original.payload_json)

    async def test_partial_snapshot_retries_after_archive_import_and_keeps_valid_nulls(self):
        (self.auctions.root_dir / f"{DAY}.json").unlink()
        partial = await self.service.ensure_for_date(DAY)
        self.assertEqual(partial.status, "partial")
        self.assertIsNone(partial.payload_json["data"][0]["today_open_change_pct"])
        await self.auctions.save_percentages(DAY, {"000001": 7, "000002": 0})
        recovered = await self.service.ensure_for_date(DAY)
        self.assertEqual(recovered.status, "ready")
        self.assertEqual(recovered.payload_json["data"][0]["today_open_change_pct"], 7)
        self.assertIsNone(recovered.payload_json["data"][1]["yesterday_final_seal_time"])

    async def test_partial_retry_after_restart_is_bounded_but_not_frozen(self):
        (self.auctions.root_dir / f"{DAY}.json").unlink()
        partial = await self.service.ensure_for_date(DAY)
        fresh_context = self.new_context()
        fresh = self.new_service(fresh_context)
        with patch.object(fresh_context, "enrich_for_archive", AsyncMock(wraps=fresh_context.enrich_for_archive)) as enrich:
            self.assertEqual((await fresh.ensure_for_date(DAY)).status, "partial")
            enrich.assert_not_awaited()
            self.now = datetime(2026, 10, 26, 10, 1)
            await fresh.ensure_for_date(DAY)
            enrich.assert_awaited_once()

    async def test_valid_partial_api_returns_before_slow_refresh_and_background_finishes(self):
        from app.api.v1 import limit_up
        (self.auctions.root_dir / f"{DAY}.json").unlink()
        stored = await self.service.ensure_for_date(DAY)
        self.assertEqual(stored.status, "partial")
        self.now = datetime(2026, 10, 26, 10, 1)
        fresh_context = self.new_context()
        self.service = self.new_service(fresh_context)
        self.history.return_value = {DAY: {"open": 11, "pre_close": 10}}
        entered, release = asyncio.Event(), asyncio.Event()
        original = fresh_context.enrich_for_archive

        async def slow(*args, **kwargs):
            entered.set()
            await release.wait()
            return await original(*args, **kwargs)

        with patch.object(limit_up, "limit_up_monitor_archive_service", self.service), \
             patch.object(limit_up, "_MONITOR_ARCHIVE_WAIT_SECONDS", 1), \
             patch.object(fresh_context, "enrich_for_archive", AsyncMock(side_effect=slow)) as enrich:
            try:
                async with self.sessions() as db:
                    response = await asyncio.wait_for(limit_up._get_database_limit_up_response(
                        db, DAY, None, None, None, "seal_amount", "desc"), timeout=0.4)
                self.assertEqual([row.stock_code for row in response.data], ["000002", "000001"])
                self.assertIsNone(response.data[0].today_open_change_pct)
                await asyncio.wait_for(entered.wait(), 1)
                self.assertIn(DAY, self.service._tasks)
            except asyncio.TimeoutError:
                self.fail("A valid partial snapshot must return without waiting for its slow provider refresh")
            finally:
                release.set()
                completed = await self.service.ensure_for_date(DAY)
            self.assertEqual(completed.status, "ready")
            self.assertEqual(completed.payload_json["data"][0]["today_open_change_pct"], 10)
            enrich.assert_awaited_once()

    async def test_changed_source_does_not_return_obsolete_partial_while_rebuilding(self):
        from app.api.v1 import limit_up
        (self.auctions.root_dir / f"{DAY}.json").unlink()
        await self.service.ensure_for_date(DAY)
        async with self.sessions() as db:
            await db.execute(update(LimitUpRecord).where(LimitUpRecord.trade_date == DAY).values(
                seal_amount=999, updated_at=datetime(2026, 10, 26, 11)))
            await db.commit()
        self.now = datetime(2026, 10, 26, 10, 1)
        fresh_context = self.new_context()
        self.service = self.new_service(fresh_context)
        self.history.return_value = {DAY: {"open": 11, "pre_close": 10}}
        entered, release = asyncio.Event(), asyncio.Event()
        original = fresh_context.enrich_for_archive

        async def slow(*args, **kwargs):
            entered.set()
            await release.wait()
            return await original(*args, **kwargs)

        with patch.object(limit_up, "limit_up_monitor_archive_service", self.service), \
             patch.object(fresh_context, "enrich_for_archive", AsyncMock(side_effect=slow)):
            request = asyncio.create_task(limit_up._get_closed_monitor_archive(DAY))
            try:
                await asyncio.wait_for(entered.wait(), 1)
                self.assertFalse(request.done(), "A changed source must not serve the old partial snapshot")
            finally:
                release.set()
            archive = await request
        self.assertEqual(archive.status, "ready")
        self.assertEqual(archive.payload_json["data"][0]["seal_amount"], 999)

    async def test_changed_source_and_provider_failure_fall_back_to_latest_database_rows(self):
        from app.api.v1 import limit_up
        from app.services.limit_up_context_service import limit_up_context_service
        (self.auctions.root_dir / f"{DAY}.json").unlink()
        stored = await self.service.ensure_for_date(DAY)
        self.assertEqual(stored.status, "partial")
        self.assertEqual(stored.payload_json["data"][0]["seal_amount"], 100)
        async with self.sessions() as db:
            await db.execute(update(LimitUpRecord).where(LimitUpRecord.trade_date == DAY).values(
                seal_amount=999, updated_at=datetime(2026, 10, 26, 11)))
            await db.commit()
        with patch.object(self.context, "enrich_for_archive", AsyncMock(side_effect=RuntimeError("provider down"))), \
             patch.object(limit_up, "limit_up_monitor_archive_service", self.service), \
             patch.object(limit_up_context_service, "enrich", AsyncMock(side_effect=lambda day, rows: rows)):
            self.assertIsNone(await self.service.get_for_display(DAY))
            async with self.sessions() as db:
                response = await limit_up._get_database_limit_up_response(db, DAY, None, None, None, "time", None)
        self.assertTrue(all(row.seal_amount == 999 for row in response.data))

    async def test_both_session_and_previous_session_repairs_invalidate_cached_rows(self):
        original = await self.service.ensure_for_date(DAY)
        async with self.sessions() as db:
            await db.execute(update(LimitUpRecord).where(LimitUpRecord.trade_date == PREVIOUS).values(
                final_seal_time=datetime.combine(PREVIOUS, time(14, 59)), updated_at=datetime(2026, 10, 26, 11)))
            await db.execute(update(LimitUpRecord).where(LimitUpRecord.trade_date == DAY, LimitUpRecord.stock_id == 1).values(
                seal_amount=999, updated_at=datetime(2026, 10, 26, 11)))
            await db.commit()
        repaired = await self.service.ensure_for_date(DAY)
        self.assertNotEqual(repaired.source_signature, original.source_signature)
        self.assertEqual(repaired.payload_json["data"][0]["seal_amount"], 999)
        self.assertEqual(repaired.payload_json["data"][0]["yesterday_final_seal_time"], "14:59:00")

    async def test_intraday_and_unfinalized_current_day_never_use_or_write_archive(self):
        archive = await self.service.ensure_for_date(DAY)
        self.now = datetime(2026, 10, 23, 10)
        self.assertIsNone(await self.service.ensure_for_date(DAY))
        self.now = datetime(2026, 10, 23, 16)
        async with self.sessions() as db:
            await db.execute(update(MarketReviewDailyMetric).where(MarketReviewDailyMetric.trade_date == DAY).values(calc_version=0))
            await db.commit()
        self.assertIsNone(await self.service.ensure_for_date(DAY, force=True))
        async with self.sessions() as db:
            await db.execute(update(MarketReviewDailyMetric).where(MarketReviewDailyMetric.trade_date == DAY).values(
                calc_version=1, updated_at=datetime.combine(DAY, time(14, 50))))
            await db.commit()
        self.assertIsNone(await self.service.ensure_for_date(DAY, force=True))
        async with self.sessions() as db:
            await db.execute(update(MarketReviewDailyMetric).where(MarketReviewDailyMetric.trade_date == DAY).values(
                calc_version=1, updated_at=datetime.combine(DAY, time(15, 6))))
            await db.commit()
        self.assertEqual((await self.service.ensure_for_date(DAY)).status, "ready")

    async def test_historical_api_uses_actual_date_archive_then_applies_original_filters_and_sort(self):
        from app.api.v1 import limit_up
        archive = await self.service.ensure_for_date(DAY)
        requested = date(2026, 10, 25)
        with patch.object(limit_up, "limit_up_monitor_archive_service", self.service), \
             patch.object(limit_up, "today_cn", return_value=NEXT), \
             patch.object(limit_up.realtime_limit_up_service, "get_realtime_limit_up_list", AsyncMock(side_effect=AssertionError("history must not use realtime"))):
            async with self.sessions() as db:
                response = await limit_up.get_realtime_limit_up(
                    trade_date=requested, continuous_days=None, reason_category="机器人", status="sealed",
                    sort_by="seal_amount", sort_order="desc", min_price=10, max_price=12,
                    continuous_days_exact=1, db=db)
        self.assertEqual(response.trade_date, DAY)
        self.assertTrue(response.is_fallback)
        self.assertEqual([row.stock_code for row in response.data], ["000002"])
        self.assertEqual(archive.total_count, 2)

    async def test_current_after_close_api_reads_final_archive_and_intraday_keeps_live_data(self):
        from app.api.v1 import limit_up
        archive = await self.service.ensure_for_date(DAY)
        raw = dict(stock_code="000003", stock_name="live", limit_up_price=15, current_price=15, market="SZ")
        with patch.object(limit_up, "limit_up_monitor_archive_service", self.service), \
             patch.object(limit_up, "today_cn", return_value=DAY), \
             patch.object(limit_up, "is_a_share_trading_day", AsyncMock(return_value=True)), \
             patch.object(limit_up.realtime_limit_up_service, "get_realtime_limit_up_list", AsyncMock(return_value=[raw])) as live:
            self.now = datetime(2026, 10, 23, 16)
            async with self.sessions() as db:
                after_close = await limit_up.get_realtime_limit_up(trade_date=DAY, continuous_days=None,
                    reason_category=None, sort_by="seal_amount", sort_order="desc", db=db)
            self.assertEqual([row.stock_code for row in after_close.data], ["000002", "000001"])
            live.assert_not_awaited()
            self.now = datetime(2026, 10, 23, 10)
            with patch.object(self.service, "ensure_for_date", AsyncMock(side_effect=AssertionError("intraday must bypass archive"))):
                async with self.sessions() as db:
                    intraday = await limit_up.get_realtime_limit_up(trade_date=DAY, continuous_days=None,
                        reason_category=None, sort_by="time", db=db)
            self.assertEqual([row.stock_code for row in intraday.data], ["000003"])
            live.assert_awaited_once()

    async def test_slow_first_build_continues_in_background_after_bounded_api_wait(self):
        from app.api.v1 import limit_up
        from app.services.limit_up_context_service import limit_up_context_service
        entered, release = asyncio.Event(), asyncio.Event()
        original = self.context.enrich_for_archive

        async def held(*args, **kwargs):
            entered.set()
            await release.wait()
            return await original(*args, **kwargs)

        with patch.object(limit_up, "limit_up_monitor_archive_service", self.service), \
             patch.object(limit_up, "_MONITOR_ARCHIVE_WAIT_SECONDS", 0.02, create=True), \
             patch.object(limit_up_context_service, "enrich", AsyncMock(side_effect=lambda day, rows: rows)), \
             patch.object(self.context, "enrich_for_archive", AsyncMock(side_effect=held)) as enrich:
            async with self.sessions() as db:
                result = await asyncio.wait_for(limit_up._get_database_limit_up_response(
                    db, DAY, None, None, None, "time", None), timeout=0.3)
            self.assertEqual(len(result.data), 2)
            await asyncio.wait_for(entered.wait(), 1)
            self.assertIn(DAY, self.service._tasks)
            release.set()
            self.assertEqual((await self.service.ensure_for_date(DAY)).status, "ready")
            enrich.assert_awaited_once()

    async def test_archive_batches_finish_all_openings_beyond_live_budget_with_bounded_concurrency(self):
        running, maximum = 0, 0

        async def history(code, start, end):
            nonlocal running, maximum
            running += 1
            maximum = max(maximum, running)
            try:
                await asyncio.sleep(0.02)
                return {PREVIOUS: {"open": 10, "pre_close": 10}, DAY: {"open": 11, "pre_close": 10}}
            finally:
                running -= 1

        context = LimitUpContextService(calendar_loader=lambda *args: [PREVIOUS, DAY, NEXT],
            session_factory=self.sessions, archive_service=None, history_fetcher=history, pool_fetcher=self.pool,
            today_provider=lambda: self.now.date(), now_provider=lambda: self.now, budget_seconds=0.01)
        rows, complete = await context.enrich_for_archive(DAY, [{"stock_code": f"{code:06}"} for code in range(1, 15)])
        self.assertTrue(complete)
        self.assertTrue(all(row["today_open_change_pct"] == 10 and row["yesterday_open_change_pct"] == 0 for row in rows))
        self.assertLessEqual(maximum, 6)
        self.assertEqual(running, 0)
        self.pool.assert_not_awaited()

    async def test_incomplete_previous_review_does_not_turn_unknown_seals_into_known_nulls(self):
        async with self.sessions() as db:
            await db.execute(update(MarketReviewDailyMetric).where(MarketReviewDailyMetric.trade_date == PREVIOUS).values(calc_version=0))
            await db.commit()
        archive = await self.service.ensure_for_date(DAY)
        self.assertEqual(archive.status, "partial")
        self.pool.assert_awaited_once_with(PREVIOUS)


if __name__ == "__main__":
    unittest.main()
