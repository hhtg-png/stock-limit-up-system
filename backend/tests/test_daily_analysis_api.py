import asyncio
import unittest
from datetime import date, datetime
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.api.v1.daily_analysis import router as daily_analysis_router
from app.database import Base, get_db
from app.models.limit_up import LimitUpRecord
from app.models.market_review import DailyAnalysisRecord, MarketReviewStockDaily
from app.models.stock import Stock
from app.services.daily_analysis_service import daily_analysis_service


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
            popular_limit_down = Stock(
                stock_code="002900",
                stock_name="人气跌停",
                market="SZ",
                industry="人工智能",
                is_cy=0,
                is_kc=0,
            )
            popular_slump = Stock(
                stock_code="002901",
                stock_name="人气大跌",
                market="SZ",
                industry="人工智能",
                is_cy=0,
                is_kc=0,
            )
            stale_leader = Stock(
                stock_code="600379",
                stock_name="宝光股份",
                market="SH",
                industry="电网设备",
                is_cy=0,
                is_kc=0,
            )
            review_leader = Stock(
                stock_code="002918",
                stock_name="蒙娜丽莎",
                market="SZ",
                industry="建筑陶瓷",
                is_cy=0,
                is_kc=0,
            )
            session.add_all([leader, trend_20cm, popular_limit_down, popular_slump, stale_leader, review_leader])
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
                    LimitUpRecord(
                        stock_id=trend_20cm.id,
                        trade_date=date(2026, 4, 25),
                        first_limit_up_time=datetime(2026, 4, 25, 10, 5, 0),
                        final_seal_time=datetime(2026, 4, 25, 10, 5, 0),
                        reason_category="人工智能",
                        limit_up_reason="机器人",
                        continuous_limit_up_days=1,
                        open_count=0,
                        is_final_sealed=True,
                        open_price=24,
                        close_price=28.8,
                        limit_up_price=28.8,
                        amount=30000,
                        turnover_rate=15,
                    ),
                    LimitUpRecord(
                        stock_id=popular_limit_down.id,
                        trade_date=date(2026, 4, 23),
                        first_limit_up_time=datetime(2026, 4, 23, 9, 40, 0),
                        final_seal_time=datetime(2026, 4, 23, 9, 40, 0),
                        reason_category="人工智能",
                        limit_up_reason="人形机器人",
                        continuous_limit_up_days=3,
                        open_count=0,
                        is_final_sealed=True,
                        open_price=9.1,
                        close_price=10,
                        limit_up_price=10,
                        amount=90000,
                        turnover_rate=8,
                    ),
                    LimitUpRecord(
                        stock_id=popular_slump.id,
                        trade_date=date(2026, 4, 23),
                        first_limit_up_time=datetime(2026, 4, 23, 9, 42, 0),
                        final_seal_time=datetime(2026, 4, 23, 9, 42, 0),
                        reason_category="人工智能",
                        limit_up_reason="人形机器人",
                        continuous_limit_up_days=3,
                        open_count=0,
                        is_final_sealed=True,
                        open_price=9.1,
                        close_price=10,
                        limit_up_price=10,
                        amount=85000,
                        turnover_rate=7,
                    ),
                    LimitUpRecord(
                        stock_id=stale_leader.id,
                        trade_date=date(2026, 4, 27),
                        first_limit_up_time=datetime(2026, 4, 27, 9, 38, 1),
                        final_seal_time=datetime(2026, 4, 27, 9, 38, 1),
                        reason_category="电网设备",
                        limit_up_reason="旧源高标",
                        continuous_limit_up_days=10,
                        open_count=3,
                        is_final_sealed=True,
                        open_price=18,
                        close_price=19.8,
                        limit_up_price=19.8,
                        amount=200000,
                        turnover_rate=20,
                    ),
                ]
            )
            session.add(
                DailyAnalysisRecord(
                    trade_date=date(2026, 4, 25),
                    month="2026-04",
                    auto_result={},
                    manual_overrides={},
                    calc_version=1,
                    data_status="ready",
                    generated_at=datetime(2026, 4, 25, 16, 0, 0),
                )
            )
            session.add_all(
                [
                    MarketReviewStockDaily(
                        stock_id=popular_limit_down.id,
                        trade_date=date(2026, 4, 24),
                        stock_code="002900",
                        stock_name="人气跌停",
                        board_type="main",
                        is_st=False,
                        yesterday_limit_up=True,
                        yesterday_continuous_days=3,
                        today_touched_limit_up=False,
                        today_sealed_close=False,
                        today_opened_close=False,
                        today_broken=False,
                        today_continuous_days=0,
                        close_price=9.05,
                        pre_close=10.0,
                        change_pct=-9.5,
                        amount=70000,
                        turnover_rate=11,
                        limit_up_reason=None,
                        data_quality_flag="ok",
                    ),
                    MarketReviewStockDaily(
                        stock_id=popular_slump.id,
                        trade_date=date(2026, 4, 24),
                        stock_code="002901",
                        stock_name="人气大跌",
                        board_type="main",
                        is_st=False,
                        yesterday_limit_up=True,
                        yesterday_continuous_days=3,
                        today_touched_limit_up=False,
                        today_sealed_close=False,
                        today_opened_close=False,
                        today_broken=False,
                        today_continuous_days=0,
                        close_price=9.3,
                        pre_close=10.0,
                        change_pct=-7.0,
                        amount=65000,
                        turnover_rate=10,
                        limit_up_reason=None,
                        data_quality_flag="ok",
                    ),
                    MarketReviewStockDaily(
                        stock_id=stale_leader.id,
                        trade_date=date(2026, 4, 27),
                        stock_code="600379",
                        stock_name="宝光股份",
                        board_type="main",
                        is_st=False,
                        yesterday_limit_up=True,
                        yesterday_continuous_days=9,
                        today_touched_limit_up=True,
                        today_sealed_close=False,
                        today_opened_close=True,
                        today_broken=True,
                        today_continuous_days=1,
                        first_limit_time=datetime(2026, 4, 27, 9, 38, 1).time(),
                        open_count=4,
                        close_price=18.66,
                        pre_close=18.63,
                        change_pct=0.16,
                        amount=225812,
                        turnover_rate=24.73,
                        limit_up_reason="电网设备",
                        data_quality_flag="ok",
                    ),
                    MarketReviewStockDaily(
                        stock_id=review_leader.id,
                        trade_date=date(2026, 4, 27),
                        stock_code="002918",
                        stock_name="蒙娜丽莎",
                        board_type="main",
                        is_st=False,
                        yesterday_limit_up=True,
                        yesterday_continuous_days=2,
                        today_touched_limit_up=True,
                        today_sealed_close=True,
                        today_opened_close=False,
                        today_broken=False,
                        today_continuous_days=3,
                        first_limit_time=datetime(2026, 4, 27, 9, 30, 39).time(),
                        final_seal_time=datetime(2026, 4, 27, 9, 30, 39).time(),
                        open_count=1,
                        close_price=15.73,
                        pre_close=14.3,
                        change_pct=10.0,
                        amount=44520,
                        turnover_rate=8.5,
                        limit_up_reason="建筑陶瓷",
                        data_quality_flag="ok",
                    ),
                ]
            )
            await session.commit()

    def test_backfill_query_override_and_rebuild_preserves_manual_override(self):
        trading_dates = {date(2026, 4, 23), date(2026, 4, 24)}
        with patch.object(daily_analysis_service, "_load_cn_trading_date_set", return_value=trading_dates):
            backfill = self.client.post("/statistics/daily-analysis/backfill", json={"month": "2026-04"})
            self.assertEqual(backfill.status_code, 200)
            self.assertEqual(backfill.json()["built_count"], 2)
            self.assertEqual(backfill.json()["removed_non_trading_count"], 1)

            month = self.client.get("/statistics/daily-analysis", params={"month": "2026-04"})
            self.assertEqual(month.status_code, 200)
            rows = month.json()["data"]
            self.assertEqual({row["trade_date"] for row in rows}, {"2026-04-23", "2026-04-24"})
            latest = next(row for row in rows if row["trade_date"] == "2026-04-24")
            self.assertEqual(latest["columns"]["连板唯一性"]["items"][0]["time"], "09:25:02")
            negative_codes = {
                item["stock_code"]
                for item in latest["columns"]["负反馈"]["items"]
            }
            self.assertIn("002900", negative_codes)
            self.assertNotIn("002901", negative_codes)

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

    def test_rebuild_prefers_market_review_candidates_over_stale_limit_up_records(self):
        trading_dates = {
            date(2026, 4, 23),
            date(2026, 4, 24),
            date(2026, 4, 25),
            date(2026, 4, 27),
        }
        with patch.object(daily_analysis_service, "_load_cn_trading_date_set", return_value=trading_dates):
            rebuilt = self.client.post("/statistics/daily-analysis/2026-04-27/rebuild")

        self.assertEqual(rebuilt.status_code, 200)
        unique_items = rebuilt.json()["columns"]["连板唯一性"]["items"]
        self.assertEqual(len(unique_items), 1)
        self.assertEqual(unique_items[0]["stock_code"], "002918")
        self.assertEqual(unique_items[0]["tags"], ["唯一", "3板"])
        self.assertNotEqual(unique_items[0]["stock_code"], "600379")

    def test_intraday_and_after_close_sessions_are_stored_separately(self):
        trading_dates = {date(2026, 4, 23), date(2026, 4, 24)}
        with patch.object(daily_analysis_service, "_load_cn_trading_date_set", return_value=trading_dates):
            intraday = self.client.post(
                "/statistics/daily-analysis/2026-04-24/rebuild",
                params={"session": "intraday"},
            )
            after_close = self.client.post(
                "/statistics/daily-analysis/2026-04-24/rebuild",
                params={"session": "after_close"},
            )

        self.assertEqual(intraday.status_code, 200)
        self.assertEqual(after_close.status_code, 200)
        self.assertEqual(intraday.json()["session"], "intraday")
        self.assertEqual(after_close.json()["session"], "after_close")

        patched_intraday = self.client.patch(
            "/statistics/daily-analysis/2026-04-24/overrides",
            params={"session": "intraday"},
            json={"overrides": {"辨识度": "盘中人工"}},
        )
        patched_after_close = self.client.patch(
            "/statistics/daily-analysis/2026-04-24/overrides",
            params={"session": "after_close"},
            json={"overrides": {"辨识度": "盘后人工"}},
        )
        self.assertEqual(patched_intraday.status_code, 200)
        self.assertEqual(patched_after_close.status_code, 200)
        self.assertEqual(patched_intraday.json()["columns"]["辨识度"]["content"], "盘中人工")
        self.assertEqual(patched_after_close.json()["columns"]["辨识度"]["content"], "盘后人工")

        intraday_month = self.client.get(
            "/statistics/daily-analysis",
            params={"month": "2026-04", "session": "intraday"},
        )
        after_close_month = self.client.get(
            "/statistics/daily-analysis",
            params={"month": "2026-04", "session": "after_close"},
        )
        intraday_row = next(row for row in intraday_month.json()["data"] if row["trade_date"] == "2026-04-24")
        after_close_row = next(row for row in after_close_month.json()["data"] if row["trade_date"] == "2026-04-24")
        self.assertEqual(intraday_row["columns"]["辨识度"]["content"], "盘中人工")
        self.assertEqual(after_close_row["columns"]["辨识度"]["content"], "盘后人工")


if __name__ == "__main__":
    unittest.main()
