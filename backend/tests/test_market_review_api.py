import asyncio
import importlib.util
import sys
import unittest
from datetime import date, time
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

import app.models  # noqa: F401
from app.database import Base, get_db
from app.models.market_review import MarketReviewDailyMetric, MarketReviewStockDaily
from app.models.stock import Stock


def _load_review_router():
    review_module_path = Path(__file__).resolve().parents[1] / "app" / "api" / "v1" / "review.py"
    spec = importlib.util.spec_from_file_location("isolated_market_review_router", review_module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module.router


class MarketReviewApiTests(unittest.TestCase):
    def test_review_router_import_does_not_execute_api_v1_package(self):
        before_modules = set(sys.modules)
        self.assertNotIn("app.api.v1", before_modules)

        _load_review_router()

        after_modules = set(sys.modules)
        self.assertNotIn("app.api.v1", after_modules - before_modules)

    def setUp(self):
        self.engine = create_async_engine(
            "sqlite+aiosqlite://",
            future=True,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        self.session_factory = async_sessionmaker(
            self.engine,
            expire_on_commit=False,
        )
        asyncio.run(self._create_schema())
        asyncio.run(self._seed_data())
        review_router = _load_review_router()

        self.app = FastAPI()
        self.app.include_router(review_router, prefix="/api/v1/statistics/review")

        async def override_get_db():
            async with self.session_factory() as session:
                yield session

        self.app.dependency_overrides[get_db] = override_get_db
        self.client = TestClient(self.app)

    def tearDown(self):
        self.client.close()
        self.app.dependency_overrides.clear()
        asyncio.run(self.engine.dispose())

    async def _create_schema(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def _seed_data(self):
        async with self.session_factory() as session:
            session.add_all(
                [
                    Stock(id=1, stock_code="600001", stock_name="Alpha", market="SH", is_st=0, is_kc=0, is_cy=0),
                    Stock(id=2, stock_code="600002", stock_name="Beta", market="SH", is_st=0, is_kc=0, is_cy=0),
                    Stock(id=3, stock_code="600003", stock_name="Gamma", market="SH", is_st=0, is_kc=0, is_cy=0),
                    Stock(id=4, stock_code="600004", stock_name="Delta", market="SH", is_st=0, is_kc=0, is_cy=0),
                    Stock(id=5, stock_code="600005", stock_name="Epsilon", market="SH", is_st=0, is_kc=0, is_cy=0),
                    Stock(id=6, stock_code="600006", stock_name="Zeta", market="SH", is_st=0, is_kc=0, is_cy=0),
                ]
            )
            session.add_all(
                [
                    MarketReviewDailyMetric(
                        trade_date=date(2026, 4, 26),
                        limit_up_count=3,
                        limit_down_count=1,
                        continuous_count=1,
                        max_board_height=2,
                        second_board_height=1,
                        gem_board_height=0,
                        first_to_second_rate=20.5,
                        continuous_promotion_rate=33.3,
                        seal_rate=70.0,
                        yesterday_limit_up_avg_change=1.5,
                        yesterday_continuous_avg_change=2.5,
                        market_turnover=1000.0,
                        up_count_ex_st=2100,
                        down_count_ex_st=900,
                        limit_up_amount=120.0,
                        broken_amount=35.0,
                    ),
                    MarketReviewDailyMetric(
                        trade_date=date(2026, 4, 27),
                        limit_up_count=5,
                        limit_down_count=2,
                        continuous_count=2,
                        max_board_height=3,
                        second_board_height=2,
                        gem_board_height=1,
                        first_to_second_rate=25.0,
                        continuous_promotion_rate=40.0,
                        seal_rate=72.5,
                        yesterday_limit_up_avg_change=1.8,
                        yesterday_continuous_avg_change=3.2,
                        market_turnover=1100.0,
                        up_count_ex_st=2200,
                        down_count_ex_st=880,
                        limit_up_amount=150.0,
                        broken_amount=45.0,
                    ),
                    MarketReviewDailyMetric(
                        trade_date=date(2026, 4, 28),
                        limit_up_count=8,
                        limit_down_count=1,
                        continuous_count=4,
                        max_board_height=4,
                        second_board_height=3,
                        gem_board_height=2,
                        first_to_second_rate=30.0,
                        continuous_promotion_rate=50.0,
                        seal_rate=78.8,
                        yesterday_limit_up_avg_change=2.1,
                        yesterday_continuous_avg_change=4.0,
                        market_turnover=1250.0,
                        up_count_ex_st=2300,
                        down_count_ex_st=820,
                        limit_up_amount=180.0,
                        broken_amount=30.0,
                    ),
                ]
            )
            session.add_all(
                [
                    self._stock_row(1, "600001", "Alpha", 4, True, True, False, 7.1, 200000.0, "AI", time(9, 31)),
                    self._stock_row(2, "600002", "Beta", 4, True, False, True, 8.8, 300000.0, "Robotics", time(9, 29)),
                    self._stock_row(3, "600003", "Gamma", 3, True, False, True, 6.5, 400000.0, "Chip", time(9, 45)),
                    self._stock_row(4, "600004", "Delta", 2, False, False, True, 4.2, 50000.0, "EV", time(10, 5)),
                    self._stock_row(5, "600005", "Epsilon", 1, True, True, False, 2.5, 600000.0, "Retail", time(9, 40)),
                    self._stock_row(6, "600006", "Zeta", 2, True, True, False, 5.0, 250000.0, "Finance", time(9, 35)),
                ]
            )
            await session.commit()

    def _stock_row(
        self,
        stock_id,
        stock_code,
        stock_name,
        today_continuous_days,
        today_touched_limit_up,
        today_sealed_close,
        today_opened_close,
        change_pct,
        amount,
        limit_up_reason,
        first_limit_time,
    ):
        return MarketReviewStockDaily(
            trade_date=date(2026, 4, 28),
            stock_id=stock_id,
            stock_code=stock_code,
            stock_name=stock_name,
            board_type="main",
            is_st=False,
            yesterday_limit_up=today_continuous_days > 1,
            yesterday_continuous_days=max(today_continuous_days - 1, 0),
            today_touched_limit_up=today_touched_limit_up,
            today_sealed_close=today_sealed_close,
            today_opened_close=today_opened_close,
            today_broken=today_opened_close,
            today_continuous_days=today_continuous_days,
            first_limit_time=first_limit_time,
            change_pct=change_pct,
            amount=amount,
            limit_up_reason=limit_up_reason,
        )

    def test_daily_endpoint_returns_filtered_rows_and_ascending_series(self):
        response = self.client.get(
            "/api/v1/statistics/review/daily",
            params={"start_date": "2026-04-27", "end_date": "2026-04-28"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["data"]["series"], ["2026-04-27", "2026-04-28"])

        rows = payload["data"]["rows"]
        self.assertEqual([row["trade_date"] for row in rows], ["2026-04-27", "2026-04-28"])
        self.assertEqual(rows[0]["limit_up_count"], 5)
        self.assertEqual(rows[1]["max_board_height"], 4)

        for field in (
            "trade_date",
            "limit_up_count",
            "limit_down_count",
            "continuous_count",
            "max_board_height",
            "second_board_height",
            "gem_board_height",
            "first_to_second_rate",
            "continuous_promotion_rate",
            "seal_rate",
            "yesterday_limit_up_avg_change",
            "yesterday_continuous_avg_change",
            "market_turnover",
            "up_count_ex_st",
            "down_count_ex_st",
            "limit_up_amount",
            "broken_amount",
        ):
            self.assertIn(field, rows[0])

    def test_detail_endpoint_sorts_by_continuous_days_then_amount_desc(self):
        response = self.client.get(
            "/api/v1/statistics/review/detail",
            params={"trade_date": "2026-04-28"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["trade_date"], "2026-04-28")
        self.assertEqual(payload["is_fallback"], False)
        self.assertEqual(
            [stock["stock_code"] for stock in payload["stocks"]],
            ["600002", "600001", "600003", "600006", "600004", "600005"],
        )
        self.assertEqual(payload["stocks"][0]["limit_up_reason"], "Robotics")
        self.assertEqual(payload["stocks"][0]["today_opened_close"], True)

    def test_detail_endpoint_falls_back_to_latest_available_review_date(self):
        response = self.client.get(
            "/api/v1/statistics/review/detail",
            params={"trade_date": "2026-05-02"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["trade_date"], "2026-04-28")
        self.assertEqual(payload["is_fallback"], True)
        self.assertEqual(payload["stocks"][0]["stock_code"], "600002")

    def test_ladder_endpoint_groups_descending_filters_and_orders_sealed_before_opened(self):
        response = self.client.get(
            "/api/v1/statistics/review/ladder",
            params={"trade_date": "2026-04-28"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["trade_date"], "2026-04-28")
        self.assertEqual(payload["is_fallback"], False)
        self.assertEqual(
            [ladder["continuous_days"] for ladder in payload["ladders"]],
            [4, 3, 2],
        )
        self.assertEqual(payload["ladders"][0]["count"], 2)
        self.assertEqual(
            [stock["stock_code"] for stock in payload["ladders"][0]["stocks"]],
            ["600001", "600002"],
        )
        ladder_stock_codes = {
            stock["stock_code"]
            for ladder in payload["ladders"]
            for stock in ladder["stocks"]
        }
        self.assertNotIn("600004", ladder_stock_codes)
        self.assertNotIn("600005", ladder_stock_codes)

    def test_ladder_endpoint_falls_back_to_latest_available_review_date(self):
        response = self.client.get(
            "/api/v1/statistics/review/ladder",
            params={"trade_date": "2026-05-02"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["trade_date"], "2026-04-28")
        self.assertEqual(payload["is_fallback"], True)
        self.assertEqual(payload["ladders"][0]["stocks"][0]["stock_code"], "600001")


if __name__ == "__main__":
    unittest.main()
