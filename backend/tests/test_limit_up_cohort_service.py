import unittest
from datetime import date, datetime
from unittest.mock import AsyncMock, patch
from types import SimpleNamespace

import httpx
from fastapi import FastAPI
from app.database import get_db
from app.api.v1 import limit_up
from app.schemas.limit_up import LimitUpRealtime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.database import Base
from app.models.stock import Stock
from app.models.limit_up import LimitUpRecord
from app.models.market_review import MarketReviewDailyMetric, MarketReviewStockDaily
from app.services.limit_up_context_service import LimitUpContextService

try:
    from app.services.limit_up_cohort_service import LimitUpCohortService
except ImportError:
    LimitUpCohortService = None


DAY = date(2026, 11, 13)
PREVIOUS = date(2026, 11, 12)


class LimitUpCohortTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        self.addAsyncCleanup(self.engine.dispose)
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        async with self.sessions.begin() as db:
            db.add_all([Stock(id=i, stock_code=f"{i:06d}", stock_name=f"stock{i}", market="SZ") for i in range(1, 7)])
            for i, count in enumerate([2, 2, 2, 2, 3, 2], 1):
                db.add(LimitUpRecord(stock_id=i, trade_date=PREVIOUS,
                    is_final_sealed=i != 6, continuous_limit_up_days=count))
            for i, touched, sealed, change, price in (
                (1, True, True, 10.0, 11), (2, False, False, -9.18, 9.082),
                (3, True, False, -5.93, 9.407), (4, False, False, 0.0, 10),
            ):
                db.add(MarketReviewStockDaily(stock_id=i, stock_code=f"{i:06d}", stock_name=f"stock{i}",
                    trade_date=DAY, today_touched_limit_up=touched, today_sealed_close=sealed,
                    today_opened_close=touched and not sealed, today_continuous_days=3 if touched else 0,
                    change_pct=change, close_price=price, pre_close=10, amount=100))
            db.add(MarketReviewDailyMetric(trade_date=DAY, limit_up_count=2))
            db.add_all([
                LimitUpRecord(stock_id=1, trade_date=DAY, is_final_sealed=True, continuous_limit_up_days=3,
                              close_price=11, limit_up_price=11),
                LimitUpRecord(stock_id=3, trade_date=DAY, is_final_sealed=False, continuous_limit_up_days=1,
                              close_price=9.407, limit_up_price=9.407),
            ])

    def service(self, **overrides):
        self.assertIsNotNone(LimitUpCohortService)
        options = dict(
            context_service=LimitUpContextService(calendar_loader=lambda *args: [PREVIOUS, DAY],
                session_factory=self.sessions, archive_service=None, pool_fetcher=AsyncMock(return_value=[])),
            quote_fetcher=AsyncMock(return_value={}), history_fetcher=AsyncMock(return_value={}),
            today_provider=lambda: date(2026, 11, 16),
        )
        options.update(overrides)
        return LimitUpCohortService(**options)

    async def test_historical_scope_keeps_all_previous_boards_even_without_today_touch(self):
        service = self.service()
        current = [{"stock_code": "000001", "stock_name": "stock1", "is_sealed": True,
                    "continuous_limit_up_days": 3, "limit_up_price": 11, "current_price": 11},
                   {"stock_code": "000003", "stock_name": "stock3", "is_sealed": False,
                    "continuous_limit_up_days": 1, "limit_up_price": 9.407, "current_price": 11}]
        async with self.sessions() as db:
            rows = await service.get_rows(db, DAY, current, exact=2)
        by_code = {row["stock_code"]: row for row in rows}
        self.assertEqual(set(by_code), {"000001", "000002", "000003", "000004"})
        self.assertEqual(by_code["000002"]["change_pct"], -9.18)
        self.assertEqual(by_code["000003"]["change_pct"], -5.93)
        self.assertEqual(by_code["000004"]["change_pct"], 0)
        self.assertFalse(by_code["000002"]["today_touched_limit_up"])
        self.assertEqual(by_code["000002"]["current_status"], "untouched")
        self.assertEqual(by_code["000002"]["continuous_limit_up_days"], 0)
        self.assertTrue(by_code["000003"]["today_touched_limit_up"])
        self.assertEqual(by_code["000003"]["current_price"], 9.407)
        self.assertEqual(by_code["000003"]["limit_up_price"], 11)
        self.assertTrue(all(row["previous_trade_date"] == PREVIOUS for row in rows))
        service.quote_fetcher.assert_not_awaited()

    async def test_live_quote_high_detects_touch_even_when_missing_from_today_pool(self):
        quotes = {
            "000001": {"datetime": "20261113142206", "price": 10.1, "pre_close": 10,
                       "change_pct": 1, "high": 11, "limit_up": 11, "amount": 100},
            "000002": {"datetime": "20261113142206", "price": 9.08, "pre_close": 10,
                       "change_pct": -9.2, "high": 10, "limit_up": 11, "amount": 100},
            "000003": {"datetime": "20261112150000", "price": 11, "pre_close": 10,
                       "change_pct": 10, "high": 11, "limit_up": 11},
        }
        service = self.service(today_provider=lambda: DAY, quote_fetcher=AsyncMock(return_value=quotes))
        async with self.sessions() as db:
            rows = await service.get_rows(db, DAY, [], exact=2)
        by_code = {row["stock_code"]: row for row in rows}
        self.assertTrue(by_code["000001"]["today_touched_limit_up"])
        self.assertFalse(by_code["000001"]["is_sealed"])
        self.assertEqual(by_code["000001"]["change_pct"], 1)
        self.assertEqual(by_code["000001"]["continuous_limit_up_days"], 3)
        self.assertFalse(by_code["000002"]["today_touched_limit_up"])
        self.assertIsNone(by_code["000003"]["change_pct"])
        self.assertIsNone(by_code["000003"]["today_touched_limit_up"])
        self.assertIn("000004", by_code)
        self.assertIsNone(by_code["000004"]["change_pct"])
        service.quote_fetcher.assert_awaited_once()

    async def test_live_missing_high_is_unknown_and_current_seal_proves_touch(self):
        quotes = {
            "000001": {"datetime": "20261113140000", "price": 10, "pre_close": 10, "change_pct": None, "high": 0,
                       "limit_up": 11, "_missing_fields": ["high"]},
            "000002": {"datetime": "20261113140000", "price": 11, "change_pct": 10,
                       "high": 0, "limit_up": 11, "_missing_fields": ["high"]},
        }
        service = self.service(today_provider=lambda: DAY, quote_fetcher=AsyncMock(return_value=quotes))
        async with self.sessions() as db:
            rows = await service.get_rows(db, DAY, [], exact=2)
        by_code = {row["stock_code"]: row for row in rows}
        self.assertIsNone(by_code["000001"]["today_touched_limit_up"])
        self.assertEqual(by_code["000001"]["change_pct"], 0)
        self.assertTrue(by_code["000002"]["today_touched_limit_up"])
        self.assertTrue(by_code["000002"]["is_sealed"])
        self.assertEqual(by_code["000002"]["continuous_limit_up_days"], 3)

    async def test_live_pool_evidence_and_zero_openings_survive_partial_quotes(self):
        async def openings(day):
            return {"000001": 0.0}
        service = self.service(today_provider=lambda: DAY, quote_fetcher=AsyncMock(return_value={
            "000001": {"datetime": "20261113140000", "price": 10, "change_pct": 0,
                       "high": 10, "limit_up": 11},
        }))
        service.context_service.archive_service = SimpleNamespace(get_percentages=AsyncMock(side_effect=openings))
        current = [{"stock_code": "000001", "is_sealed": False, "continuous_limit_up_days": 3},
                   {"stock_code": "000002", "is_sealed": False, "change_pct": -4.5}]
        async with self.sessions() as db:
            rows = await service.get_rows(db, DAY, current, exact=2)
            await service.get_rows(db, DAY, current, exact=2)
        by_code = {row["stock_code"]: row for row in rows}
        self.assertTrue(by_code["000001"]["today_touched_limit_up"])
        self.assertEqual(by_code["000001"]["yesterday_open_change_pct"], 0)
        self.assertEqual(by_code["000001"]["today_open_change_pct"], 0)
        self.assertEqual(by_code["000002"]["change_pct"], -4.5)
        service.quote_fetcher.assert_awaited_once()

    async def test_api_historical_cohort_expands_missing_current_members_and_filters_touch(self):
        app = FastAPI()
        app.include_router(limit_up.router, prefix="/limit-up")
        async def database():
            async with self.sessions() as db:
                yield db
        app.dependency_overrides[get_db] = database
        service = self.service()
        with patch("app.services.limit_up_cohort_service.limit_up_cohort_service", service), \
             patch.object(limit_up, "today_cn", return_value=date(2026, 11, 16)), \
             patch.object(limit_up, "_get_closed_monitor_archive", AsyncMock(return_value=None)):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
                base = {"trade_date": "2026-11-15", "yesterday_continuous_days_exact": 2}
                payload = (await client.get("/limit-up/realtime", params=base)).json()
                self.assertEqual(payload["trade_date"], DAY.isoformat())
                self.assertTrue(payload["is_fallback"])
                self.assertEqual({row["stock_code"] for row in payload["data"]}, {"000001", "000002", "000003", "000004"})
                self.assertEqual(next(row for row in payload["data"] if row["stock_code"] == "000002")["change_pct"], -9.18)
                opened = (await client.get("/limit-up/realtime", params={**base, "status": "opened"})).json()
                self.assertEqual([row["stock_code"] for row in opened["data"]], ["000003"])
                untouched = (await client.get("/limit-up/realtime", params={**base, "status": "untouched"})).json()
                self.assertEqual({row["stock_code"] for row in untouched["data"]}, {"000002", "000004"})
                first = (await client.get("/limit-up/realtime", params={**base, "continuous_days_exact": 1})).json()
                self.assertEqual(first["data"], [])

    def test_legacy_touched_zero_board_still_counts_as_first_and_cohort_unknown_does_not(self):
        rows = [LimitUpRealtime(stock_code=f"{i:06d}", stock_name="one", trade_date=DAY,
                               market="SZ", current_price=10, limit_up_price=11,
                               continuous_limit_up_days=0, today_touched_limit_up=touched)
                for i, touched in enumerate([True, False, None], 1)]
        result = limit_up._filter_and_sort_limit_up_list(rows, None, None, None, "time", continuous_days_exact=1)
        self.assertEqual([row.stock_code for row in result], ["000001"])

    async def test_live_raw_missing_board_and_required_prices_still_serializes_the_cohort(self):
        service = self.service(today_provider=lambda: DAY)
        raw = [{"stock_code": "000001", "is_final_sealed": True,
                "continuous_limit_up_days": None, "limit_up_price": None, "current_price": None},
               {"stock_code": "000002", "is_final_sealed": False,
                "continuous_limit_up_days": float("inf"), "limit_up_price": float("nan"), "current_price": None},
               {"stock_code": "000003", "is_final_sealed": False, "continuous_limit_up_days": 0,
                "limit_up_price": None, "current_price": None}]
        async with self.sessions() as db:
            rows = await service.get_rows(db, DAY, raw, exact=2)
        validated = {row["stock_code"]: LimitUpRealtime.model_validate(row) for row in rows}
        self.assertEqual(validated["000001"].continuous_limit_up_days, 3)
        self.assertEqual(validated["000002"].continuous_limit_up_days, 3)
        self.assertEqual(validated["000003"].continuous_limit_up_days, 3)
        self.assertTrue(validated["000001"].today_touched_limit_up)
        self.assertEqual(validated["000001"].limit_up_price, 0)
        self.assertEqual(validated["000001"].current_price, 0)
        self.assertEqual(validated["000002"].limit_up_price, 0)
        self.assertIsNone(validated["000004"].today_touched_limit_up)
        self.assertEqual(validated["000004"].continuous_limit_up_days, 0)

    async def test_formal_prior_count_keeps_record_seal_times_when_formal_times_are_missing(self):
        async with self.sessions.begin() as db:
            stock = await db.get(Stock, 1)
            stock.industry = "retail"
            record = await db.scalar(select(LimitUpRecord).where(
                LimitUpRecord.stock_id == 1, LimitUpRecord.trade_date == PREVIOUS,
            ))
            record.first_limit_up_time = datetime(2026, 11, 12, 9, 31, 2)
            record.final_seal_time = datetime(2026, 11, 12, 14, 22, 31)
            db.add(MarketReviewStockDaily(stock_id=1, stock_code="000001", stock_name="stock1",
                trade_date=PREVIOUS, today_touched_limit_up=True, today_sealed_close=True,
                today_continuous_days=3, first_limit_time=None, final_seal_time=None))
        service = self.service()
        async with self.sessions() as db:
            rows = await service.get_rows(db, DAY, [{"stock_code": "000001",
                "yesterday_first_limit_up_time": None, "yesterday_final_seal_time": None}], exact=3)
        row = next(row for row in rows if row["stock_code"] == "000001")
        self.assertEqual(row["yesterday_continuous_days"], 3)
        self.assertEqual(row["yesterday_first_limit_up_time"], "09:31:02")
        self.assertEqual(row["yesterday_final_seal_time"], "14:22:31")
        self.assertEqual(row["industry"], "retail")

    async def test_missing_historical_quotes_keep_members_and_query_only_selected_date(self):
        selected = date(2026, 11, 16)
        history = AsyncMock(return_value={selected: -2.5, DAY: 10})
        context = LimitUpContextService(calendar_loader=lambda *args: [DAY, selected],
                                       session_factory=self.sessions, archive_service=None, pool_fetcher=AsyncMock(return_value=[]))
        # DAY contains one sealed 3-board fact, so minimum 3 has exactly one member.
        service = self.service(context_service=context, history_fetcher=history,
                               today_provider=lambda: date(2026, 11, 17))
        async with self.sessions() as db:
            rows = await service.get_rows(db, selected, [], minimum=3)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["stock_code"], "000001")
        self.assertEqual(rows[0]["change_pct"], -2.5)
        self.assertIsNone(rows[0]["today_touched_limit_up"])
        history.assert_awaited_once_with("000001", selected)


if __name__ == "__main__":
    unittest.main()
