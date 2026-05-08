import asyncio
import unittest
from datetime import date, datetime

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.api.v1.daily_analysis import router as daily_analysis_router
from app.database import Base, get_db
from app.models.limit_up import LimitUpRecord
from app.models.stock import Stock


class DailyAnalysisApiTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_async_engine(
            "sqlite+aiosqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        self.Session = async_sessionmaker(self.engine, expire_on_commit=False)
        asyncio.run(self._create_schema_and_seed())

        app = FastAPI()
        app.include_router(daily_analysis_router, prefix="/statistics/daily-analysis")

        async def override_get_db():
            async with self.Session() as session:
                yield session

        app.dependency_overrides[get_db] = override_get_db
        self.client = TestClient(app)

    def tearDown(self):
        asyncio.run(self.engine.dispose())

    async def _create_schema_and_seed(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        async with self.Session() as session:
            leader = Stock(
                stock_code="000001",
                stock_name="唯一高标",
                market="SZ",
                industry="人工智能",
                is_cy=0,
                is_kc=0,
            )
            trend_20cm = Stock(
                stock_code="300001",
                stock_name="趋势二十",
                market="SZ",
                industry="人工智能",
                is_cy=1,
                is_kc=0,
            )
            session.add_all([leader, trend_20cm])
            await session.flush()

            session.add_all(
                [
                    LimitUpRecord(
                        stock_id=leader.id,
                        trade_date=date(2026, 4, 23),
                        first_limit_up_time=datetime(2026, 4, 23, 9, 32, 0),
                        final_seal_time=datetime(2026, 4, 23, 9, 32, 0),
                        reason_category="人工智能",
                        limit_up_reason="AI",
                        continuous_limit_up_days=3,
                        open_count=0,
                        is_final_sealed=True,
                        open_price=10,
                        close_price=11,
                        limit_up_price=11,
                        amount=100000,
                        turnover_rate=7,
                    ),
                    LimitUpRecord(
                        stock_id=leader.id,
                        trade_date=date(2026, 4, 24),
                        first_limit_up_time=datetime(2026, 4, 25, 9, 25, 2),
                        final_seal_time=datetime(2026, 4, 25, 9, 25, 2),
                        reason_category="人工智能",
                        limit_up_reason="AI",
                        continuous_limit_up_days=4,
                        open_count=0,
                        is_final_sealed=True,
                        open_price=12.1,
                        close_price=12.1,
                        limit_up_price=12.1,
                        amount=120000,
                        turnover_rate=8,
                    ),
                    LimitUpRecord(
                        stock_id=trend_20cm.id,
                        trade_date=date(2026, 4, 24),
                        first_limit_up_time=datetime(2026, 4, 24, 10, 5, 0),
                        final_seal_time=None,
                        reason_category="人工智能",
                        limit_up_reason="机器人",
                        continuous_limit_up_days=1,
                        open_count=2,
                        is_final_sealed=False,
                        open_price=20,
                        close_price=22,
                        limit_up_price=24,
                        amplitude=24,
                        amount=30000,
                        turnover_rate=15,
                    ),
                ]
            )
            await session.commit()

    def test_backfill_query_override_and_rebuild_preserves_manual_override(self):
        backfill = self.client.post("/statistics/daily-analysis/backfill", json={"month": "2026-04"})
        self.assertEqual(backfill.status_code, 200)
        self.assertEqual(backfill.json()["built_count"], 2)

        month = self.client.get("/statistics/daily-analysis", params={"month": "2026-04"})
        self.assertEqual(month.status_code, 200)
        rows = month.json()["data"]
        self.assertEqual({row["trade_date"] for row in rows}, {"2026-04-23", "2026-04-24"})
        latest = next(row for row in rows if row["trade_date"] == "2026-04-24")
        self.assertEqual(latest["columns"]["连板唯一性"]["items"][0]["time"], "09:25:02")

        patched = self.client.patch(
            "/statistics/daily-analysis/2026-04-24/overrides",
            json={"overrides": {"辨识度": "人工确认：唯一高标"}},
        )
        self.assertEqual(patched.status_code, 200)
        self.assertEqual(patched.json()["columns"]["辨识度"]["content"], "人工确认：唯一高标")
        self.assertTrue(patched.json()["columns"]["辨识度"]["is_manual"])

        rebuilt = self.client.post("/statistics/daily-analysis/2026-04-24/rebuild")
        self.assertEqual(rebuilt.status_code, 200)
        self.assertEqual(rebuilt.json()["columns"]["辨识度"]["content"], "人工确认：唯一高标")
        self.assertTrue(rebuilt.json()["columns"]["辨识度"]["is_manual"])


if __name__ == "__main__":
    unittest.main()
