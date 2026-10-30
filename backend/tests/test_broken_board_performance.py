import unittest
import asyncio
from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
import app.models
from app.database import Base
from app.models.stock import Stock
from app.models.market_review import MarketReviewDailyMetric, MarketReviewStockDaily
from app.services.broken_board_performance_service import BrokenBoardPerformanceService


class BrokenBoardPerformanceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        self.previous, self.today = date(2026, 9, 10), date(2026, 9, 11)
        async with self.sessions() as db:
            db.add(MarketReviewDailyMetric(trade_date=self.previous, calc_version=1))
            for i, (board, sealed) in enumerate([(3, False), (2, False), (1, False), (2, True)], 1):
                db.add(Stock(id=i, stock_code=f"60000{i}", stock_name=f"Stock{i}", market="SH"))
                db.add(MarketReviewStockDaily(trade_date=self.previous, stock_id=i,
                    stock_code=f"60000{i}", stock_name=f"Stock{i}", yesterday_continuous_days=board,
                    today_sealed_close=sealed, today_touched_limit_up=sealed))
            await db.commit()
        self.quotes = AsyncMock(return_value={
            "600001": {"change_pct": 5, "datetime": "20260911143000"},
            "600002": {"change_pct": -3, "datetime": "20260911143000"},
        })
        self.history = AsyncMock(return_value={self.today: 2.0})
        self.service = BrokenBoardPerformanceService(session_factory=self.sessions,
            suspension_fetcher=AsyncMock(return_value=set()), quote_fetcher=self.quotes, history_fetcher=self.history, fallback_fetcher=AsyncMock(return_value={}),
            calendar_loader=lambda: [self.previous, self.today], today_provider=lambda: self.today)

    async def asyncTearDown(self):
        await self.engine.dispose()

    async def test_selects_pre_previous_continuous_then_broken_and_returns_each_stock(self):
        result = await self.service.get_performance(1, self.today)
        point = result["points"][0]
        self.assertEqual(point["average_change"], 1.0)
        self.assertEqual(point["sample_count"], 2)
        self.assertEqual([s["stock_code"] for s in point["stocks"]], ["600001", "600002"])
        self.assertEqual([s["previous_board"] for s in point["stocks"]], [3, 2])
        self.quotes.assert_awaited_once_with(["600001", "600002"])
        self.history.assert_not_awaited()

    async def test_stale_quote_is_missing_not_zero_or_previous_day(self):
        self.quotes.return_value["600002"]["datetime"] = "20260910150000"
        point = (await self.service.get_performance(1, self.today))["points"][0]
        self.assertIsNone(point["average_change"])
        self.assertEqual(point["priced_count"], 1)
        self.assertIsNone(point["stocks"][1]["change_pct"])

    async def test_historical_points_use_requested_day_prices_not_current_quotes(self):
        self.service.today_provider = lambda: date(2026, 9, 14)
        point = (await self.service.get_performance(1, self.today))["points"][0]
        self.assertEqual(point["average_change"], 2)
        self.quotes.assert_not_awaited()
        self.assertEqual(self.history.await_count, 2)

    async def test_complete_history_survives_restart_without_fetching_prices(self):
        self.service.today_provider = lambda: date(2026, 9, 14)
        point = (await self.service.get_performance(1, self.today))["points"][0]
        self.assertEqual(point["average_change"], 2)
        restarted = BrokenBoardPerformanceService(
            session_factory=self.sessions, calendar_loader=lambda: [self.previous, self.today],
            today_provider=lambda: date(2026, 9, 14),
            history_fetcher=AsyncMock(side_effect=AssertionError("archived history fetched again")),
            fallback_fetcher=AsyncMock(side_effect=AssertionError("archived fallback fetched again")),
            quote_fetcher=AsyncMock(side_effect=AssertionError("historical quotes fetched")),
            suspension_fetcher=AsyncMock(side_effect=AssertionError("archived suspension fetched again")),
        )
        archived = (await restarted.get_performance(1, self.today))["points"][0]
        self.assertEqual(archived["average_change"], 2)
        self.assertEqual(archived["priced_count"], 2)
        self.assertEqual(archived["stocks"][0]["change_pct"], 2)

    async def test_concurrent_identical_ranges_share_history_preparation(self):
        self.service.today_provider = lambda: date(2026, 9, 14)
        release = asyncio.Event()
        calls = []
        async def slow_prices(code, end_date):
            calls.append(code)
            await release.wait()
            return {self.today: 2}
        self.service.history_fetcher = slow_prices
        requests = [asyncio.create_task(self.service.get_performance(1, self.today)) for _ in range(2)]
        await asyncio.sleep(0.05)
        release.set()
        results = await asyncio.gather(*requests)
        self.assertEqual([result["points"][0]["average_change"] for result in results], [2, 2])
        self.assertEqual(sorted(calls), ["600001", "600002"])

    async def test_concurrent_primary_history_for_overlapping_ranges_fetches_once(self):
        import tempfile
        from unittest.mock import Mock, patch
        release = asyncio.Event()
        calls = []
        async def fetch(*args, **kwargs):
            calls.append(kwargs["params"]["param"])
            await release.wait()
            return Mock(status_code=200, json=lambda: {"data": {"sh600001": {"qfqday": [
                ["2026-09-10", "10", "10"], ["2026-09-11", "10", "11"]]}}})
        client = AsyncMock()
        client.get.side_effect = fetch
        client.__aenter__.return_value = client
        with tempfile.TemporaryDirectory() as directory, patch(
            "app.services.broken_board_performance_service.httpx.AsyncClient", return_value=client
        ):
            service = BrokenBoardPerformanceService(cache_dir=directory)
            requests = [asyncio.create_task(service._fetch_history("600001", self.today)) for _ in range(2)]
            await asyncio.sleep(0.02)
            release.set()
            results = await asyncio.gather(*requests)
        self.assertEqual([result[self.today] for result in results], [10, 10])
        self.assertEqual(len(calls), 1)

    async def test_concurrent_fallback_history_fetches_once(self):
        import tempfile
        release = asyncio.Event()
        calls = []
        async def fetch(code, end_date):
            calls.append(code)
            await release.wait()
            return {self.today: 3}
        with tempfile.TemporaryDirectory() as directory:
            service = BrokenBoardPerformanceService(cache_dir=directory)
            service._fetch_eastmoney_history = fetch
            requests = [asyncio.create_task(service._fetch_fallback_history("600001", self.today)) for _ in range(2)]
            await asyncio.sleep(0.02)
            release.set()
            results = await asyncio.gather(*requests)
        self.assertEqual([result[self.today] for result in results], [3, 3])
        self.assertEqual(calls, ["600001"])

    async def test_rebuilt_source_invalidates_archived_cohort(self):
        self.service.today_provider = lambda: date(2026, 9, 14)
        await self.service.get_performance(1, self.today)
        async with self.sessions() as db:
            from sqlalchemy import select
            source = (await db.execute(select(MarketReviewStockDaily).where(
                MarketReviewStockDaily.stock_code == "600002"
            ))).scalar_one()
            source.today_sealed_close = True
            await db.commit()
        self.history.return_value = {self.today: 4}
        point = (await self.service.get_performance(1, self.today))["points"][0]
        self.assertEqual(point["sample_count"], 1)
        self.assertEqual(point["average_change"], 4)

    async def test_incomplete_history_is_retried_until_complete(self):
        self.service.today_provider = lambda: date(2026, 9, 14)
        self.history.return_value = {}
        point = (await self.service.get_performance(1, self.today))["points"][0]
        self.assertEqual(point["data_status"], "unavailable")
        self.history.return_value = {self.today: 3}
        point = (await self.service.get_performance(1, self.today))["points"][0]
        self.assertEqual(point["data_status"], "ready")
        self.assertEqual(point["average_change"], 3)

    async def test_mixed_range_reuses_complete_point_and_only_prices_missing_day(self):
        self.service.today_provider = lambda: date(2026, 9, 15)
        monday = date(2026, 9, 14)
        async with self.sessions() as db:
            db.add(MarketReviewDailyMetric(trade_date=self.today, calc_version=1))
            db.add(MarketReviewStockDaily(trade_date=self.today, stock_id=1,
                stock_code="600001", stock_name="Stock1", yesterday_continuous_days=2,
                today_sealed_close=False))
            await db.commit()
        await self.service.get_performance(1, self.today)
        self.service._calendar_cache = [self.previous, self.today, monday]
        async def missing_day_prices(code, end_date):
            if code != "600001" or end_date != monday:
                raise AssertionError("a completed point should need no prices")
            return {monday: 7}
        self.service.history_fetcher = missing_day_prices
        points = (await self.service.get_performance(2, monday))["points"]
        self.assertEqual([point["average_change"] for point in points], [2, 7])

    async def test_weekend_range_uses_friday_and_thursday_cohort(self):
        self.service.today_provider = lambda: date(2026, 9, 14)
        point = (await self.service.get_performance(1, date(2026, 9, 13)))["points"][0]
        self.assertEqual(point["trade_date"], "2026-09-11")
        self.assertEqual(point["sample_count"], 2)
        self.assertEqual(point["average_change"], 2)

    async def _seed_today_final_review(self, calc_version=1, captured_at=None):
        async with self.sessions() as db:
            db.add(MarketReviewDailyMetric(trade_date=self.today, calc_version=calc_version,
                updated_at=captured_at or datetime(2026, 9, 11, 15, 6)))
            await db.commit()
        self.service.now_provider = lambda: datetime(2026, 9, 11, 15, 35,
            tzinfo=timezone(timedelta(hours=8)))
        for quote in self.quotes.return_value.values():
            quote["datetime"] = "20260911150000"

    async def test_same_day_intraday_quote_cannot_be_archived_as_final(self):
        await self._seed_today_final_review()
        self.quotes.return_value["600001"]["datetime"] = "20260911143000"
        point = await self.service.build_after_close(self.today)
        self.assertEqual(point["data_status"], "unavailable")
        self.assertIsNone(point["average_change"])
        self.assertFalse(await self.service.has_after_close_data(self.today))
        self.quotes.return_value["600001"]["datetime"] = "20260911150000"
        point = await self.service.build_after_close(self.today)
        self.assertEqual(point["average_change"], 1)
        self.assertTrue(await self.service.has_after_close_data(self.today))

    async def test_missing_quote_time_cannot_overwrite_successful_final_archive(self):
        await self._seed_today_final_review()
        await self.service.build_after_close(self.today)
        self.quotes.return_value["600001"]["datetime"] = "20260911"
        self.quotes.return_value["600001"]["change_pct"] = 20
        point = await self.service.build_after_close(self.today, force=True)
        self.assertEqual(point["data_status"], "unavailable")
        point = (await self.service.get_performance(1, self.today))["points"][0]
        self.assertEqual(point["average_change"], 1)

    async def test_valuation_day_repair_invalidates_final_archive(self):
        await self._seed_today_final_review()
        await self.service.build_after_close(self.today)
        self.assertTrue(await self.service.has_after_close_data(self.today))
        async with self.sessions() as db:
            from sqlalchemy import select
            metric = (await db.execute(select(MarketReviewDailyMetric).where(
                MarketReviewDailyMetric.trade_date == self.today))).scalar_one()
            metric.calc_version = 2
            metric.updated_at = datetime(2026, 9, 11, 16)
            await db.commit()
        self.service.now_provider = lambda: datetime(2026, 9, 11, 16, 35)
        self.assertFalse(await self.service.has_after_close_data(self.today))
        for quote in self.quotes.return_value.values():
            quote["change_pct"] = 9
        point = await self.service.build_after_close(self.today)
        self.assertEqual(point["average_change"], 9)

    async def test_today_final_requires_after_close_review_and_matching_quote_date(self):
        await self._seed_today_final_review()
        self.quotes.return_value["600002"]["datetime"] = "20260910150000"
        point = await self.service.build_after_close(self.today)
        self.assertEqual(point["data_status"], "unavailable")
        self.assertFalse(await self.service.has_after_close_data(self.today))
        self.quotes.return_value["600002"]["datetime"] = "20260911150000"
        point = await self.service.build_after_close(self.today)
        self.assertEqual(point["average_change"], 1)
        self.assertTrue(await self.service.has_after_close_data(self.today))

    async def test_today_archive_is_bypassed_during_intraday(self):
        await self._seed_today_final_review()
        await self.service.build_after_close(self.today)
        self.quotes.return_value["600001"]["change_pct"] = 9
        self.quotes.return_value["600002"]["change_pct"] = 9
        self.service.now_provider = lambda: datetime(2026, 9, 11, 14, 59)
        self.assertIsNone(await self.service.build_after_close(self.today))
        self.assertFalse(await self.service.has_after_close_data(self.today))
        point = (await self.service.get_performance(1, self.today))["points"][0]
        self.assertEqual(point["average_change"], 9)

    async def test_after_close_archive_survives_failed_forced_rebuild(self):
        await self._seed_today_final_review()
        await self.service.build_after_close(self.today)
        self.quotes.side_effect = RuntimeError("provider down")
        point = await self.service.build_after_close(self.today, force=True)
        self.assertEqual(point["data_status"], "unavailable")
        self.assertTrue(await self.service.has_after_close_data(self.today))
        stored = (await self.service.get_performance(1, self.today))["points"][0]
        self.assertEqual(stored["average_change"], 1)

    async def test_intraday_capture_cannot_be_finalized_after_five(self):
        await self._seed_today_final_review(calc_version=0,
            captured_at=datetime(2026, 9, 11, 14, 50))
        self.assertIsNone(await self.service.build_after_close(self.today))
        self.assertFalse(await self.service.has_after_close_data(self.today))

    async def test_metric_repair_invalidates_archive_even_when_membership_is_unchanged(self):
        self.service.today_provider = lambda: date(2026, 9, 14)
        await self.service.build_after_close(self.today)
        self.assertTrue(await self.service.has_after_close_data(self.today))
        async with self.sessions() as db:
            from sqlalchemy import select
            metric = (await db.execute(select(MarketReviewDailyMetric).where(
                MarketReviewDailyMetric.trade_date == self.previous))).scalar_one()
            metric.calc_version = 2
            await db.commit()
        self.assertFalse(await self.service.has_after_close_data(self.today))
        self.history.return_value = {self.today: -4}
        point = await self.service.build_after_close(self.today)
        self.assertEqual(point["average_change"], -4)
        self.assertTrue(await self.service.has_after_close_data(self.today))

    async def test_unavailable_prior_source_is_never_archived_as_empty_cohort(self):
        self.service.today_provider = lambda: date(2026, 9, 14)
        async with self.sessions() as db:
            from sqlalchemy import select
            metric = (await db.execute(select(MarketReviewDailyMetric).where(
                MarketReviewDailyMetric.trade_date == self.previous))).scalar_one()
            metric.source_status = "unavailable"
            await db.commit()
        point = await self.service.build_after_close(self.today)
        self.assertEqual(point["data_status"], "unavailable")
        self.assertFalse(await self.service.has_after_close_data(self.today))

    async def test_missing_prior_review_is_unavailable_not_empty_cohort(self):
        async with self.sessions() as db:
            from sqlalchemy import delete
            await db.execute(delete(MarketReviewDailyMetric))
            await db.commit()
        point = (await self.service.get_performance(1, self.today))["points"][0]
        self.assertIsNone(point["average_change"])
        self.assertEqual(point["data_status"], "unavailable")
        self.quotes.assert_not_awaited()

    def test_history_parser_uses_adjacent_closes_and_keeps_missing_dates_missing(self):
        rows = [["2026-09-10", "10", "10"], ["2026-09-11", "10", "11"]]
        self.assertEqual(self.service.parse_history(rows), {self.today: 10.0})

    def test_invalid_previous_close_does_not_create_multiday_return(self):
        rows = [["2026-09-09", "10", "10"], ["2026-09-10", "10", ""], ["2026-09-11", "11", "11"]]
        self.assertNotIn(self.today, self.service.parse_history(rows))

    async def test_missing_history_is_filled_by_alternate_daily_source(self):
        self.service.today_provider = lambda: date(2026, 9, 14)
        self.history.return_value = {}
        self.service.fallback_fetcher = AsyncMock(return_value={self.today: -1.5})
        point = (await self.service.get_performance(1, self.today))["points"][0]
        self.assertEqual(point["average_change"], -1.5)
        self.assertEqual(point["priced_count"], 2)

    async def test_fallback_uses_sina_and_persists_history_across_instances(self):
        import tempfile
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as directory:
            service = BrokenBoardPerformanceService(cache_dir=directory)
            with patch.object(service, "_fetch_eastmoney_history", side_effect=RuntimeError("source down")), patch.object(
                service, "_fetch_sina_history", new_callable=AsyncMock, return_value={self.today: 3.5}
            ) as sina:
                result = await service._fetch_fallback_history("600001", self.today)
                self.assertEqual(result[self.today], 3.5)
                sina.assert_awaited_once()
            restarted = BrokenBoardPerformanceService(cache_dir=directory)
            with patch.object(restarted, "_fetch_eastmoney_history", side_effect=AssertionError("must use disk cache")):
                self.assertEqual((await restarted._fetch_fallback_history("600001", self.today))[self.today], 3.5)

    def test_sina_adjustment_removes_split_and_invalid_rows_break_chain(self):
        factors = [{"d": "1900-01-01", "f": "2"}, {"d": "2026-09-10", "f": "1"}]
        rows = [{"day": "2026-09-09", "close": "20"}, {"day": "2026-09-10", "close": "10"},
                {"day": "2026-09-11", "close": "11"}]
        parsed = self.service.parse_sina_adjusted_history(rows, factors, self.today)
        self.assertEqual(parsed[self.previous], 0)
        self.assertEqual(parsed[self.today], 10)
        rows[1]["close"] = "invalid"
        self.assertNotIn(self.today, self.service.parse_sina_adjusted_history(rows, factors, self.today))
        self.assertEqual(self.service.parse_sina_adjusted_history(rows, [], self.today), {})

    async def test_old_cache_survives_next_day_source_outage(self):
        import tempfile
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as directory:
            service = BrokenBoardPerformanceService(cache_dir=directory)
            service._save_disk_cache("fallback", "600001", self.previous, {self.previous: 3.5})
            with patch.object(service, "_fetch_eastmoney_history", side_effect=RuntimeError("down")), patch.object(
                service, "_fetch_sina_history", side_effect=RuntimeError("down")):
                result = await service._fetch_fallback_history("600001", self.today)
            self.assertEqual(result, {self.previous: 3.5})
            self.assertEqual(service._disk_ends[("fallback", "600001")], self.previous)

    async def test_confirmed_suspension_excluded_from_average_but_retained_in_details(self):
        self.service.suspension_fetcher = AsyncMock(return_value={"600002"})
        point = (await self.service.get_performance(1, self.today))["points"][0]
        self.assertEqual(point["average_change"], 5)
        self.assertEqual(point["data_status"], "ready")
        self.assertEqual(point["suspended_count"], 1)
        self.assertEqual(point["priced_count"], 1)
        self.assertEqual(point["stocks"][1]["quote_status"], "suspended")
        self.assertIsNone(point["stocks"][1]["change_pct"])

    async def test_unknown_missing_quote_remains_unavailable_when_suspension_lookup_fails(self):
        self.quotes.return_value.pop("600002")
        self.service.suspension_fetcher = AsyncMock(side_effect=RuntimeError("down"))
        point = (await self.service.get_performance(1, self.today))["points"][0]
        self.assertIsNone(point["average_change"])
        self.assertEqual(point["stocks"][1]["quote_status"], "unavailable")

    async def test_all_suspended_is_not_zero_percent(self):
        self.service.suspension_fetcher = AsyncMock(return_value={"600001", "600002"})
        point = (await self.service.get_performance(1, self.today))["points"][0]
        self.assertIsNone(point["average_change"])
        self.assertEqual(point["suspended_count"], 2)
        self.assertEqual(point["data_status"], "ready")
