import unittest
from datetime import date
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
            quote_fetcher=self.quotes, history_fetcher=self.history, fallback_fetcher=AsyncMock(return_value={}),
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
