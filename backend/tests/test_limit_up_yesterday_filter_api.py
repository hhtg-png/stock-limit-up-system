import unittest
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1 import limit_up
from app.database import Base, get_db
from app.models.limit_up import LimitUpRecord
from app.models.stock import Stock
from app.services.limit_up_context_service import limit_up_context_service


TODAY = date(2026, 11, 16)
FRIDAY = date(2026, 11, 13)
THURSDAY = date(2026, 11, 12)


class YesterdayBoardFilterApiTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        async with self.sessions() as db:
            db.add_all([Stock(id=i, stock_code=f"{i:06}", stock_name=f"股票{i}", market="SZ")
                        for i in range(1, 6)])
            for i, board in enumerate((2, 2, 3, 2, 2), 1):
                db.add(LimitUpRecord(
                    stock_id=i, trade_date=FRIDAY, continuous_limit_up_days=board,
                    first_limit_up_time=datetime(2026, 11, 13, 9, 30),
                    is_final_sealed=True, close_price=11, limit_up_price=11))
            # Historical display derives real streaks from sealed session rows.
            db.add(LimitUpRecord(
                stock_id=2, trade_date=THURSDAY, continuous_limit_up_days=1,
                first_limit_up_time=datetime(2026, 11, 12, 9, 30),
                is_final_sealed=True, close_price=10, limit_up_price=10))
            await db.commit()
        app = FastAPI()
        app.include_router(limit_up.router, prefix="/limit-up")

        async def override_db():
            async with self.sessions() as db:
                yield db

        app.dependency_overrides[get_db] = override_db
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")
        self.rows = [dict(
            stock_code=f"{i:06}", stock_name=f"股票{i}", market="SZ",
            continuous_limit_up_days=board, yesterday_continuous_days=previous,
            context_date=TODAY, previous_trade_date=FRIDAY, limit_up_price=11, current_price=11,
            is_sealed=True,
        ) for i, (board, previous) in enumerate(zip((2, 2, 3, 2, 2), (1, 2, 3, 0, None)), 1)]

    async def asyncTearDown(self):
        await self.client.aclose()
        await self.engine.dispose()

    async def live(self, **params):
        with patch.object(limit_up, "today_cn", return_value=TODAY), \
             patch.object(limit_up, "is_a_share_trading_day", AsyncMock(return_value=True)), \
             patch.object(limit_up.limit_up_monitor_archive_service, "is_closed_session", return_value=False), \
             patch.object(limit_up.realtime_limit_up_service, "get_realtime_limit_up_list", AsyncMock(return_value=self.rows)):
            response = await self.client.get("/limit-up/realtime", params=params)
        self.assertEqual(response.status_code, 200)
        return response.json()

    async def test_unfiltered_live_response_preserves_known_zero_and_unknown_yesterday_counts(self):
        payload = await self.live()
        self.assertEqual([row.get("yesterday_continuous_days", "missing") for row in payload["data"]],
                         [1, 2, 3, 0, None])

    async def test_yesterday_exact_and_today_exact_filters_are_an_intersection(self):
        payload = await self.live(continuous_days_exact=2, yesterday_continuous_days_exact=2)
        self.assertEqual([row["stock_code"] for row in payload["data"]], ["000002"])

    async def test_yesterday_minimum_filter_excludes_known_zero_and_unknown(self):
        payload = await self.live(yesterday_continuous_days=2)
        self.assertEqual([row["stock_code"] for row in payload["data"]], ["000002", "000003"])

    async def test_yesterday_exact_takes_priority_over_its_minimum(self):
        payload = await self.live(yesterday_continuous_days=3, yesterday_continuous_days_exact=1)
        self.assertEqual([row["stock_code"] for row in payload["data"]], ["000001"])

    async def test_yesterday_filter_excludes_unknown_instead_of_treating_it_as_first_board(self):
        payload = await self.live(yesterday_continuous_days_exact=1)
        self.assertEqual([row["stock_code"] for row in payload["data"]], ["000001"])

    async def test_historical_fallback_enriches_actual_session_before_yesterday_filtering(self):
        calls = []

        async def enrich(day, rows, quotes=None):
            calls.append(day)
            values = {"000001": 1, "000002": 2, "000003": 3, "000004": 0, "000005": None}
            return [{**row, "context_date": day, "previous_trade_date": THURSDAY,
                     "yesterday_continuous_days": values[row["stock_code"]]} for row in rows]

        with patch.object(limit_up, "today_cn", return_value=TODAY), \
             patch.object(limit_up, "_get_closed_monitor_archive", AsyncMock(return_value=None)), \
             patch.object(limit_up_context_service, "enrich", AsyncMock(side_effect=enrich)):
            response = await self.client.get("/limit-up/realtime", params={
                "trade_date": "2026-11-15", "continuous_days_exact": 2,
                "yesterday_continuous_days_exact": 2,
            })
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(calls, [FRIDAY])
        self.assertEqual(payload["trade_date"], "2026-11-13")
        self.assertTrue(payload["is_fallback"])
        self.assertEqual([row["stock_code"] for row in payload["data"]], ["000002"])
        self.assertEqual(payload["data"][0]["previous_trade_date"], "2026-11-12")
        self.assertEqual(payload["data"][0]["yesterday_continuous_days"], 2)

    async def test_old_historical_archive_is_enriched_before_applying_yesterday_filter(self):
        archive_rows = [{key: value for key, value in row.items() if key != "yesterday_continuous_days"}
                        for row in self.rows]
        for row in archive_rows:
            row.update(trade_date=FRIDAY, context_date=FRIDAY, previous_trade_date=THURSDAY)
        archive = SimpleNamespace(payload_json={"data": archive_rows})

        async def enrich_previous(day, rows):
            self.assertEqual(day, FRIDAY)
            return [{**row, "yesterday_continuous_days": 2 if row["stock_code"] == "000002" else None}
                    for row in rows]

        with patch.object(limit_up, "today_cn", return_value=TODAY), \
             patch.object(limit_up, "_get_closed_monitor_archive", AsyncMock(return_value=archive)), \
             patch.object(limit_up_context_service, "enrich_previous_boards", AsyncMock(side_effect=enrich_previous), create=True):
            response = await self.client.get("/limit-up/realtime", params={
                "trade_date": "2026-11-13", "yesterday_continuous_days_exact": 2,
            })
        self.assertEqual(response.status_code, 200)
        self.assertEqual([row["stock_code"] for row in response.json()["data"]], ["000002"])
        self.assertEqual(response.json()["data"][0]["yesterday_continuous_days"], 2)

    async def test_old_archive_unknown_yesterday_count_remains_null_without_a_filter(self):
        row = {key: value for key, value in self.rows[0].items() if key != "yesterday_continuous_days"}
        row.update(trade_date=FRIDAY, context_date=FRIDAY, previous_trade_date=THURSDAY)
        archive = SimpleNamespace(payload_json={"data": [row]})
        with patch.object(limit_up, "today_cn", return_value=TODAY), \
             patch.object(limit_up, "_get_closed_monitor_archive", AsyncMock(return_value=archive)), \
             patch.object(limit_up_context_service, "enrich_previous_boards", AsyncMock(side_effect=lambda day, rows: rows), create=True):
            response = await self.client.get("/limit-up/realtime", params={"trade_date": "2026-11-13"})
        self.assertEqual(response.status_code, 200)
        self.assertIn("yesterday_continuous_days", response.json()["data"][0])
        self.assertIsNone(response.json()["data"][0]["yesterday_continuous_days"])

    async def test_closed_current_session_archive_uses_both_board_filters(self):
        rows = [{key: value for key, value in row.items() if key != "yesterday_continuous_days"}
                for row in self.rows]
        for row in rows:
            row["trade_date"] = TODAY
        archive = SimpleNamespace(payload_json={"data": rows})

        async def enrich_previous(day, items):
            self.assertEqual(day, TODAY)
            return [{**row, "yesterday_continuous_days": 3 if row["stock_code"] == "000003" else None}
                    for row in items]

        with patch.object(limit_up, "today_cn", return_value=TODAY), \
             patch.object(limit_up, "is_a_share_trading_day", AsyncMock(return_value=True)), \
             patch.object(limit_up.limit_up_monitor_archive_service, "is_closed_session", return_value=True), \
             patch.object(limit_up, "_get_closed_monitor_archive", AsyncMock(return_value=archive)), \
             patch.object(limit_up_context_service, "enrich_previous_boards", AsyncMock(side_effect=enrich_previous), create=True):
            response = await self.client.get("/limit-up/realtime", params={
                "continuous_days_exact": 3, "yesterday_continuous_days_exact": 3,
            })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["data"][0].get("yesterday_continuous_days", "missing"), 3)
        self.assertEqual([row["stock_code"] for row in response.json()["data"]], ["000003"])


if __name__ == "__main__":
    unittest.main()
