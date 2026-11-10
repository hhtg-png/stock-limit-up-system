import threading
import unittest
from datetime import date
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1 import limit_up
from app.database import Base, get_db
from app.models.limit_up import LimitUpRecord
from app.models.stock import Stock


class LimitUpDatesApiTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        self.Session = async_sessionmaker(self.engine, expire_on_commit=False)
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        async with self.Session() as session:
            session.add_all([
                Stock(id=1, stock_code="000001", stock_name="甲", market="SZ"),
                Stock(id=2, stock_code="000002", stock_name="乙", market="SZ"),
            ])
            await session.commit()

        app = FastAPI()
        app.include_router(limit_up.router, prefix="/limit-up")

        async def override_get_db():
            async with self.Session() as session:
                yield session

        app.dependency_overrides[get_db] = override_get_db
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test")

    async def asyncTearDown(self):
        await self.client.aclose()
        await self.engine.dispose()

    async def seed(self, rows):
        async with self.Session() as session:
            session.add_all([LimitUpRecord(stock_id=stock_id, trade_date=day)
                             for stock_id, day in rows])
            await session.commit()

    async def request_dates(self, today, calendar):
        with patch.object(limit_up, "today_cn", return_value=today), \
             patch("app.data_collectors.scheduler._get_cn_trading_dates", side_effect=calendar), \
             patch.object(limit_up.realtime_limit_up_service, "get_realtime_limit_up_item", AsyncMock(return_value=None)):
            # A missing/misordered /dates route must fail locally, without live stock-detail requests.
            return await self.client.get("/limit-up/dates")

    async def test_returns_unique_ordered_sessions_excluding_holiday_and_future_records(self):
        await self.seed([
            (1, date(2026, 10, 9)), (2, date(2026, 10, 9)),
            (1, date(2026, 9, 30)), (1, date(2026, 10, 1)),
            (1, date(2026, 10, 13)),
        ])
        response = await self.request_dates(date(2026, 10, 12), lambda start, end: [
            date(2026, 9, 30), date(2026, 10, 9), date(2026, 10, 12), date(2026, 10, 13)])
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {
            "dates": ["2026-09-30", "2026-10-09", "2026-10-12"], "today": "2026-10-12"})

    async def test_includes_live_today_even_before_its_first_database_record(self):
        await self.seed([(1, date(2026, 11, 9))])
        response = await self.request_dates(date(2026, 11, 10), lambda start, end: [
            date(2026, 11, 9), date(2026, 11, 10)])
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {
            "dates": ["2026-11-09", "2026-11-10"], "today": "2026-11-10"})

    async def test_weekend_today_is_not_added_and_weekend_record_is_excluded(self):
        await self.seed([(1, date(2026, 11, 6)), (1, date(2026, 11, 7))])
        response = await self.request_dates(date(2026, 11, 8), lambda start, end: [date(2026, 11, 6)])
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"dates": ["2026-11-06"], "today": "2026-11-08"})

    async def test_weekday_holiday_today_is_not_added(self):
        await self.seed([(1, date(2026, 9, 30))])
        response = await self.request_dates(date(2026, 10, 1), lambda start, end: [date(2026, 9, 30)])
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"dates": ["2026-09-30"], "today": "2026-10-01"})

    async def test_calendar_failure_returns_503_instead_of_guessing_weekdays(self):
        await self.seed([(1, date(2026, 10, 1))])

        def unavailable(start, end):
            raise RuntimeError("provider temporarily unavailable")

        response = await self.request_dates(date(2026, 10, 12), unavailable)
        self.assertEqual(response.status_code, 503)
        self.assertNotIn("dates", response.json())

    async def test_empty_database_still_includes_verified_current_session(self):
        response = await self.request_dates(date(2026, 11, 10), lambda start, end: [date(2026, 11, 10)])
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"dates": ["2026-11-10"], "today": "2026-11-10"})

    async def test_calendar_provider_runs_off_the_request_event_loop(self):
        request_thread = threading.get_ident()
        provider_threads = []

        def calendar(start, end):
            provider_threads.append(threading.get_ident())
            return [date(2026, 11, 10)]

        response = await self.request_dates(date(2026, 11, 10), calendar)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["dates"], ["2026-11-10"])
        self.assertTrue(provider_threads)
        self.assertTrue(all(thread != request_thread for thread in provider_threads))


if __name__ == "__main__":
    unittest.main()
