import asyncio
import importlib.util
import sys
import threading
import unittest
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

import app.models  # noqa: F401
from app.database import Base, get_db
from app.models.market_review import MarketReviewArchiveSample, MarketReviewDailyMetric, MarketReviewStockDaily
from app.models.stock import Stock


def _load_review_module():
    review_module_path = Path(__file__).resolve().parents[1] / "app" / "api" / "v1" / "review.py"
    spec = importlib.util.spec_from_file_location("isolated_market_review_router", review_module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _load_review_router():
    return _load_review_module().router


class MarketReviewApiTests(unittest.TestCase):
    def test_review_router_import_does_not_execute_api_v1_package(self):
        before_modules = set(sys.modules)

        _load_review_router()

        introduced_modules = set(sys.modules) - before_modules
        self.assertFalse(
            any(
                module_name == "app.api.v1" or module_name.startswith("app.api.v1.")
                for module_name in introduced_modules
            )
        )

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
        self.review_module = _load_review_module()
        self._mark_only_seed_dates_as_trading()

        self.app = FastAPI()
        self.app.include_router(self.review_module.router, prefix="/api/v1/statistics/review")

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
                    self._stock_row(
                        6,
                        "600006",
                        "Zeta",
                        2,
                        True,
                        True,
                        False,
                        5.0,
                        250000.0,
                        "Finance",
                        time(9, 35),
                        board_type="gem",
                    ),
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
        board_type="main",
    ):
        return MarketReviewStockDaily(
            trade_date=date(2026, 4, 28),
            stock_id=stock_id,
            stock_code=stock_code,
            stock_name=stock_name,
            board_type=board_type,
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

    async def _add_non_trading_review_rows(self):
        async with self.session_factory() as session:
            session.add(
                MarketReviewDailyMetric(
                    trade_date=date(2026, 4, 29),
                    limit_up_count=0,
                    limit_down_count=0,
                    continuous_count=0,
                    max_board_height=0,
                    second_board_height=0,
                    gem_board_height=0,
                    first_to_second_rate=0.0,
                    continuous_promotion_rate=0.0,
                    seal_rate=0.0,
                    yesterday_limit_up_avg_change=0.0,
                    yesterday_continuous_avg_change=0.0,
                    market_turnover=0.0,
                    up_count_ex_st=0,
                    down_count_ex_st=0,
                    limit_up_amount=0.0,
                    broken_amount=0.0,
                    source_status="placeholder",
                )
            )
            session.add(
                MarketReviewStockDaily(
                    trade_date=date(2026, 4, 29),
                    stock_id=6,
                    stock_code="600006",
                    stock_name="Zeta",
                    board_type="main",
                    is_st=False,
                    yesterday_limit_up=False,
                    yesterday_continuous_days=0,
                    today_touched_limit_up=True,
                    today_sealed_close=True,
                    today_opened_close=False,
                    today_broken=False,
                    today_continuous_days=1,
                    first_limit_time=time(9, 25),
                    change_pct=10.0,
                    amount=1.0,
                    limit_up_reason="Bad holiday row",
                )
            )
            await session.commit()

    def _mark_only_seed_dates_as_trading(self):
        self.review_module._TRADING_DAY_CACHE.clear()
        self.review_module._load_cn_trading_dates = Mock(
            return_value={date(2026, 4, 26), date(2026, 4, 27), date(2026, 4, 28)}
        )

    def test_cold_daily_calendar_lookup_keeps_the_event_loop_responsive(self):
        heartbeat = threading.Event()
        observed = []
        def load_calendar(start_date, end_date):
            observed.append(heartbeat.wait(timeout=0.5))
            return {date(2026, 4, 27), date(2026, 4, 28)}
        self.review_module._load_cn_trading_dates = load_calendar
        async def check():
            async def pulse():
                await asyncio.sleep(0.02)
                heartbeat.set()
            task = asyncio.create_task(pulse())
            async with self.session_factory() as db:
                response = await self.review_module.get_market_review_daily(
                    start_date=date(2026, 4, 27), end_date=date(2026, 4, 28), days=None, db=db)
            await task
            self.assertEqual(response.latest_trade_date, date(2026, 4, 28))
        asyncio.run(check())
        self.assertEqual(observed, [True])

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
        self.assertEqual(payload["start_date"], "2026-04-27")
        self.assertEqual(payload["end_date"], "2026-04-28")
        self.assertEqual(payload["latest_trade_date"], "2026-04-28")
        self.assertFalse(payload["is_fallback"])

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

    def test_daily_endpoint_includes_board_height_stock_labels(self):
        response = self.client.get(
            "/api/v1/statistics/review/daily",
            params={"start_date": "2026-04-28", "end_date": "2026-04-28"},
        )

        self.assertEqual(response.status_code, 200)
        row = response.json()["data"]["rows"][0]
        self.assertEqual(row["max_board_label"], "Alpha4")
        self.assertIsNone(row["second_board_label"])
        self.assertEqual(row["gem_board_label"], "Zeta2")

    def test_daily_endpoint_resolves_future_end_date_to_latest_available_metric(self):
        response = self.client.get(
            "/api/v1/statistics/review/daily",
            params={"start_date": "2026-04-27", "end_date": "2026-05-02"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["data"]["series"], ["2026-04-27", "2026-04-28"])
        self.assertEqual(payload["start_date"], "2026-04-27")
        self.assertEqual(payload["end_date"], "2026-04-28")
        self.assertEqual(payload["requested_start_date"], "2026-04-27")
        self.assertEqual(payload["requested_end_date"], "2026-05-02")
        self.assertEqual(payload["latest_trade_date"], "2026-04-28")
        self.assertTrue(payload["is_fallback"])

    def test_daily_endpoint_falls_back_to_latest_single_row_when_range_is_after_latest_metric(self):
        response = self.client.get(
            "/api/v1/statistics/review/daily",
            params={"start_date": "2026-05-02", "end_date": "2026-05-02"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["data"]["series"], ["2026-04-28"])
        self.assertEqual(payload["start_date"], "2026-04-28")
        self.assertEqual(payload["end_date"], "2026-04-28")
        self.assertEqual(payload["requested_start_date"], "2026-05-02")
        self.assertEqual(payload["requested_end_date"], "2026-05-02")
        self.assertEqual(payload["latest_trade_date"], "2026-04-28")
        self.assertTrue(payload["is_fallback"])

    def test_daily_endpoint_excludes_non_trading_metric_rows_from_all_series(self):
        asyncio.run(self._add_non_trading_review_rows())
        self._mark_only_seed_dates_as_trading()

        response = self.client.get(
            "/api/v1/statistics/review/daily",
            params={"start_date": "2026-04-27", "end_date": "2026-04-29"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["data"]["series"], ["2026-04-27", "2026-04-28"])
        self.assertEqual([row["trade_date"] for row in payload["data"]["rows"]], ["2026-04-27", "2026-04-28"])
        self.assertEqual(payload["end_date"], "2026-04-28")
        self.assertEqual(payload["latest_trade_date"], "2026-04-28")
        self.assertTrue(payload["is_fallback"])

    def test_daily_endpoint_days_returns_latest_available_trading_rows(self):
        response = self.client.get(
            "/api/v1/statistics/review/daily",
            params={"days": 2, "end_date": "2026-05-02"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["data"]["series"], ["2026-04-27", "2026-04-28"])
        self.assertEqual(payload["start_date"], "2026-04-27")
        self.assertEqual(payload["end_date"], "2026-04-28")
        self.assertEqual(payload["requested_end_date"], "2026-05-02")
        self.assertEqual(payload["latest_trade_date"], "2026-04-28")
        self.assertTrue(payload["is_fallback"])

    def test_daily_endpoint_days_skips_non_trading_metric_rows_before_limiting(self):
        asyncio.run(self._add_non_trading_review_rows())
        self._mark_only_seed_dates_as_trading()

        response = self.client.get(
            "/api/v1/statistics/review/daily",
            params={"days": 2, "end_date": "2026-04-29"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["data"]["series"], ["2026-04-27", "2026-04-28"])
        self.assertEqual(payload["end_date"], "2026-04-28")
        self.assertEqual(payload["latest_trade_date"], "2026-04-28")
        self.assertTrue(payload["is_fallback"])

    def test_daily_endpoint_days_without_requested_end_date_is_not_fallback(self):
        response = self.client.get(
            "/api/v1/statistics/review/daily",
            params={"days": 1},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["data"]["series"], ["2026-04-28"])
        self.assertEqual(payload["end_date"], "2026-04-28")
        self.assertIsNone(payload["requested_end_date"])
        self.assertEqual(payload["latest_trade_date"], "2026-04-28")
        self.assertFalse(payload["is_fallback"])

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

    def test_detail_endpoint_skips_non_trading_review_rows(self):
        asyncio.run(self._add_non_trading_review_rows())
        self._mark_only_seed_dates_as_trading()

        response = self.client.get(
            "/api/v1/statistics/review/detail",
            params={"trade_date": "2026-04-29"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["trade_date"], "2026-04-28")
        self.assertTrue(payload["is_fallback"])
        self.assertNotEqual(payload["stocks"][0]["limit_up_reason"], "Bad holiday row")

    def test_ladder_endpoint_groups_descending_and_excludes_opened_rows(self):
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
            [4, 2],
        )
        self.assertEqual(payload["ladders"][0]["count"], 1)
        self.assertEqual(
            [stock["stock_code"] for stock in payload["ladders"][0]["stocks"]],
            ["600001"],
        )
        ladder_stock_codes = {
            stock["stock_code"]
            for ladder in payload["ladders"]
            for stock in ladder["stocks"]
        }
        self.assertNotIn("600002", ladder_stock_codes)
        self.assertNotIn("600003", ladder_stock_codes)
        self.assertNotIn("600004", ladder_stock_codes)
        self.assertNotIn("600005", ladder_stock_codes)

    def test_ladder_endpoint_metrics_include_unpromoted_same_cohort(self):
        async def add_unpromoted_stock():
            async with self.session_factory() as session:
                session.add(
                    MarketReviewStockDaily(
                        trade_date=date(2026, 4, 28),
                        stock_id=7,
                        stock_code="600007",
                        stock_name="Eta",
                        board_type="main",
                        is_st=False,
                        yesterday_limit_up=True,
                        yesterday_continuous_days=3,
                        today_touched_limit_up=False,
                        today_sealed_close=False,
                        today_opened_close=False,
                        today_broken=False,
                        today_continuous_days=0,
                        change_pct=-2.0,
                        amount=100000.0,
                        limit_up_reason="AI",
                    )
                )
                await session.commit()

        asyncio.run(add_unpromoted_stock())

        response = self.client.get(
            "/api/v1/statistics/review/ladder",
            params={"trade_date": "2026-04-28"},
        )

        self.assertEqual(response.status_code, 200)
        ladder = response.json()["ladders"][0]
        self.assertEqual(ladder["continuous_days"], 4)
        self.assertEqual(ladder["count"], 1)
        self.assertEqual(ladder["cohort_count"], 3)
        self.assertEqual(ladder["cohort_sealed_count"], 1)
        self.assertEqual(ladder["cohort_opened_count"], 1)
        self.assertAlmostEqual(ladder["cohort_seal_rate"], 33.33)
        self.assertAlmostEqual(ladder["cohort_avg_change"], 4.63)
        self.assertEqual(
            [stock["stock_code"] for stock in ladder["cohort_stocks"]],
            ["600002", "600001", "600007"],
        )
        unpromoted = ladder["cohort_stocks"][2]
        self.assertEqual(unpromoted["stock_code"], "600007")
        self.assertEqual(unpromoted["today_continuous_days"], 0)
        self.assertAlmostEqual(unpromoted["change_pct"], -2.0)

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

    def test_intraday_endpoint_skips_stored_non_trading_snapshot(self):
        asyncio.run(self._add_non_trading_review_rows())
        self._mark_only_seed_dates_as_trading()
        self.review_module._collect_intraday_source = AsyncMock(
            side_effect=AssertionError("non-trading stored row should not be collected")
        )

        response = self.client.get(
            "/api/v1/statistics/review/intraday",
            params={"trade_date": "2026-04-29"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["data"]["series"], [])
        self.assertEqual(payload["end_date"], "2026-04-29")
        self.assertEqual(payload["detail"]["trade_date"], "2026-04-29")
        self.assertFalse(payload["is_fallback"])
        self.assertEqual(payload["data_status"], "unavailable")
        self.review_module._collect_intraday_source.assert_not_awaited()

    def test_live_intraday_detection_uses_trading_calendar_and_closes_at_market_end(self):
        class MarketOpenDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                return datetime(2026, 5, 6, 14, 59, tzinfo=timezone(timedelta(hours=8)))

        class MarketClosedDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                return datetime(2026, 5, 6, 15, 1, tzinfo=timezone(timedelta(hours=8)))

        original_datetime = self.review_module.datetime
        try:
            self.review_module._TRADING_DAY_CACHE.clear()
            self.review_module._load_cn_trading_dates = Mock(return_value={date(2026, 5, 6)})
            self.review_module.datetime = MarketOpenDatetime
            self.assertTrue(self.review_module._should_collect_live_intraday(date(2026, 5, 6)))

            self.review_module.datetime = MarketClosedDatetime
            self.assertFalse(self.review_module._should_collect_live_intraday(date(2026, 5, 6)))

            self.review_module._TRADING_DAY_CACHE.clear()
            self.review_module._load_cn_trading_dates = Mock(return_value=set())
            self.review_module.datetime = MarketOpenDatetime
            self.assertFalse(self.review_module._should_collect_live_intraday(date(2026, 5, 6)))
        finally:
            self.review_module.datetime = original_datetime
            self.review_module._TRADING_DAY_CACHE.clear()

    def test_live_failure_never_returns_stored_or_previous_day_snapshot(self):
        self.review_module._should_collect_live_intraday = Mock(return_value=True)
        self.review_module._collect_intraday_source = AsyncMock(side_effect=RuntimeError("source down"))
        for target in ("2026-04-28", "2026-04-30"):
            response = self.client.get("/api/v1/statistics/review/intraday", params={"trade_date": target})
            self.assertEqual(response.status_code, 200)
            payload = response.json()
            self.assertEqual(payload["data_status"], "unavailable")
            self.assertEqual(payload["data"]["rows"], [])
            self.assertEqual(payload["detail"]["trade_date"], target)
            self.assertEqual(payload["detail"]["stocks"], [])
            self.assertEqual(payload["ladder"]["ladders"], [])
            self.assertFalse(payload["is_fallback"])
            self.assertTrue(payload["is_live"])

    def test_live_non_authoritative_source_is_unavailable(self):
        self.review_module._should_collect_live_intraday = Mock(return_value=True)
        self.review_module._collect_intraday_source = AsyncMock(return_value={"is_authoritative": False, "stock_rows": []})
        response = self.client.get("/api/v1/statistics/review/intraday", params={"trade_date": "2026-04-30"})
        self.assertEqual(response.json()["data_status"], "unavailable")
        self.assertEqual(response.json()["data"]["rows"], [])

    def test_intraday_endpoint_returns_live_metric_detail_and_ladder_snapshot(self):
        collect_mock = AsyncMock(
            return_value={
                "is_authoritative": True,
                "source_status": "partial",
                "stock_rows": [
                    {
                        "stock_code": "600010",
                        "stock_name": "LiveAlpha",
                        "board_type": "main",
                        "today_touched_limit_up": True,
                        "today_sealed_close": True,
                        "today_opened_close": False,
                        "yesterday_limit_up": True,
                        "yesterday_continuous_days": 2,
                        "today_continuous_days": 3,
                        "change_pct": 10.01,
                        "first_limit_time": time(9, 29, 8),
                        "final_seal_time": time(9, 40, 9),
                        "open_count": 0,
                        "turnover_rate": 3.4,
                        "close_price": 11.0,
                        "pre_close": 10.0,
                        "amount": 120000.0,
                        "limit_up_reason": "AI",
                    },
                    {
                        "stock_code": "600011",
                        "stock_name": "LiveBeta",
                        "board_type": "main",
                        "today_touched_limit_up": True,
                        "today_sealed_close": False,
                        "today_opened_close": True,
                        "yesterday_limit_up": True,
                        "yesterday_continuous_days": 2,
                        "today_continuous_days": 3,
                        "change_pct": 7.2,
                        "amount": 220000.0,
                        "limit_up_reason": "AI",
                    },
                    {
                        "stock_code": "600012",
                        "stock_name": "LiveGamma",
                        "board_type": "gem",
                        "today_touched_limit_up": True,
                        "today_sealed_close": True,
                        "today_opened_close": False,
                        "yesterday_limit_up": True,
                        "yesterday_continuous_days": 1,
                        "today_continuous_days": 2,
                        "change_pct": 20.0,
                        "amount": 180000.0,
                        "limit_up_reason": "Chip",
                    },
                    {
                        "stock_code": "600013",
                        "stock_name": "LiveEta",
                        "board_type": "main",
                        "today_touched_limit_up": False,
                        "today_sealed_close": False,
                        "today_opened_close": False,
                        "yesterday_limit_up": True,
                        "yesterday_continuous_days": 2,
                        "today_continuous_days": 0,
                        "change_pct": -1.0,
                        "amount": 90000.0,
                        "limit_up_reason": "AI",
                    },
                ],
                "limit_down_count": 2,
                "market_turnover": 9800.0,
                "up_count_ex_st": 2600,
                "down_count_ex_st": 1200,
            }
        )
        aggregate_mock = Mock(
            return_value={
                "trade_date": date(2026, 5, 6),
                "limit_up_count": 3,
                "limit_down_count": 2,
                "continuous_count": 3,
                "max_board_height": 3,
                "second_board_height": 2,
                "gem_board_height": 2,
                "first_to_second_rate": 42.0,
                "continuous_promotion_rate": 55.0,
                "seal_rate": 66.67,
                "yesterday_limit_up_avg_change": 3.4,
                "yesterday_continuous_avg_change": 5.6,
                "market_turnover": 9800.0,
                "up_count_ex_st": 2600,
                "down_count_ex_st": 1200,
                "limit_up_amount": 520000.0,
                "broken_amount": 220000.0,
            }
        )

        self.review_module._collect_intraday_source = collect_mock
        self.review_module._should_collect_live_intraday = Mock(return_value=True)
        original_aggregate = None
        if hasattr(self.review_module, "market_review_metrics_service"):
            original_aggregate = self.review_module.market_review_metrics_service.aggregate_daily_metrics
            self.review_module.market_review_metrics_service.aggregate_daily_metrics = aggregate_mock

        try:
            response = self.client.get(
                "/api/v1/statistics/review/intraday",
                params={"trade_date": "2026-05-06"},
            )
        finally:
            if original_aggregate is not None:
                self.review_module.market_review_metrics_service.aggregate_daily_metrics = original_aggregate

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["is_intraday"])
        self.assertTrue(payload["is_live"])
        self.assertIsNotNone(payload["snapshot_time"])
        self.assertEqual(payload["data"]["series"], ["2026-05-06"])
        row = payload["data"]["rows"][0]
        self.assertEqual(payload.get("source_status"), "partial")
        self.assertEqual(row["source_status"], "partial")
        stock = next(stock for stock in payload["detail"]["stocks"] if stock["stock_code"] == "600010")
        self.assertEqual(stock["first_limit_time"], "09:29:08")
        self.assertEqual(stock["final_seal_time"], "09:40:09")
        self.assertEqual(stock["open_count"], 0)
        self.assertEqual(stock["yesterday_continuous_days"], 2)
        self.assertEqual(stock["turnover_rate"], 3.4)
        self.assertEqual(stock["close_price"], 11.0)
        self.assertEqual(stock["pre_close"], 10.0)
        self.assertEqual(row["max_board_label"], "LiveAlpha3")
        self.assertEqual(row["second_board_label"], "LiveGamma2")
        self.assertEqual(row["gem_board_label"], "LiveGamma2")
        self.assertEqual([stock["stock_code"] for stock in payload["detail"]["stocks"]], ["600011", "600010", "600012", "600013"])
        self.assertEqual([ladder["continuous_days"] for ladder in payload["ladder"]["ladders"]], [3, 2])
        self.assertEqual(payload["ladder"]["ladders"][0]["count"], 1)
        self.assertEqual(
            [stock["stock_code"] for stock in payload["ladder"]["ladders"][0]["stocks"]],
            ["600010"],
        )
        self.assertEqual(payload["ladder"]["ladders"][0]["cohort_count"], 3)
        self.assertAlmostEqual(payload["ladder"]["ladders"][0]["cohort_seal_rate"], 33.33)
        self.assertAlmostEqual(payload["ladder"]["ladders"][0]["cohort_avg_change"], 5.4)

    def test_intraday_endpoint_uses_stored_review_snapshot_when_available(self):
        self.review_module._collect_intraday_source = AsyncMock(
            side_effect=AssertionError("stored intraday snapshot should not collect live source")
        )

        response = self.client.get(
            "/api/v1/statistics/review/intraday",
            params={"trade_date": "2026-04-28"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["is_intraday"])
        self.assertFalse(payload["is_live"])
        self.assertFalse(payload["is_fallback"])
        self.assertEqual(payload["data"]["series"], ["2026-04-28"])
        self.assertEqual(payload["data"]["rows"][0]["max_board_label"], "Alpha4")
        self.assertEqual(payload.get("source_status"), "primary")
        self.assertEqual(payload["data"]["rows"][0]["source_status"], "primary")
        self.assertEqual(payload["detail"]["trade_date"], "2026-04-28")
        self.assertEqual(payload["ladder"]["trade_date"], "2026-04-28")
        stock = payload["detail"]["stocks"][0]
        self.assertEqual(stock["stock_code"], "600002")
        self.assertEqual(stock["first_limit_time"], "09:29:00")
        self.assertIsNone(stock["final_seal_time"])
        self.assertEqual(stock["open_count"], 0)
        self.assertEqual(stock["yesterday_continuous_days"], 3)
        self.assertEqual(payload["ladder"]["ladders"][0]["continuous_days"], 4)
        self.review_module._collect_intraday_source.assert_not_awaited()

    async def _seed_monitor_archive_sample(self, trade_date, *, payload_date=None):
        async with self.session_factory() as session:
            session.add(MarketReviewArchiveSample(
                trade_date=trade_date,
                metric_json={
                    "trade_date": (payload_date or trade_date).isoformat(),
                    "source_status": "monitor_archive",
                    "limit_up_count": 1,
                    "limit_down_count": None,
                    "continuous_count": 1,
                    "max_board_height": 2,
                    "second_board_height": 0,
                    "gem_board_height": 0,
                    "first_to_second_rate": None,
                    "continuous_promotion_rate": None,
                    "seal_rate": 100.0,
                    "yesterday_limit_up_avg_change": None,
                    "yesterday_continuous_avg_change": None,
                    "market_turnover": 9001.25,
                    "up_count_ex_st": None,
                    "down_count_ex_st": None,
                    "limit_up_amount": None,
                    "broken_amount": 0.0,
                },
                stocks_json=[{
                    "stock_code": "600001",
                    "stock_name": "Archive Alpha",
                    "trade_date": trade_date.isoformat(),
                    "today_touched_limit_up": True,
                    "today_continuous_days": 2,
                    "today_sealed_close": True,
                    "today_opened_close": False,
                    "first_limit_time": "09:30:00",
                    "yesterday_continuous_days": None,
                    "amount": None,
                    "data_quality_flag": "monitor_archive",
                }],
            ))
            await session.commit()
        self.review_module._TRADING_DAY_CACHE[trade_date] = True

    def test_intraday_exposes_verified_monitor_archive_data_with_unknown_fields(self):
        trade_date = date(2026, 4, 24)
        asyncio.run(self._seed_monitor_archive_sample(trade_date))
        response = self.client.get(
            "/api/v1/statistics/review/intraday",
            params={"trade_date": trade_date.isoformat()},
        )
        payload = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["data_status"], "ready")
        self.assertEqual(payload["source_status"], "monitor_archive")
        self.assertFalse(payload["is_intraday"])
        self.assertFalse(payload["is_live"])
        self.assertFalse(payload["is_fallback"])
        self.assertEqual(payload["detail"]["trade_date"], trade_date.isoformat())
        self.assertEqual(payload["ladder"]["trade_date"], trade_date.isoformat())
        row, = payload["data"]["rows"]
        self.assertEqual(row["source_status"], "monitor_archive")
        self.assertEqual(row["limit_up_count"], 1)
        self.assertEqual(row["market_turnover"], 9001.25)
        self.assertEqual(row["broken_amount"], 0.0)
        self.assertIsNone(row["limit_down_count"])
        self.assertIsNone(row["up_count_ex_st"])
        self.assertIsNone(row["continuous_promotion_rate"])
        self.assertIsNone(row["limit_up_amount"])
        stock, = payload["detail"]["stocks"]
        self.assertEqual(stock["first_limit_time"], "09:30:00")
        self.assertTrue(stock["today_touched_limit_up"])
        self.assertIsNone(stock["amount"])
        self.assertIsNone(stock["yesterday_continuous_days"])
        ladder, = payload["ladder"]["ladders"]
        self.assertEqual(ladder["count"], 1)
        self.assertIsNone(ladder["cohort_count"])
        self.assertIsNone(ladder["cohort_seal_rate"])
        self.assertEqual(ladder["cohort_stocks"], [])

    def test_monitor_archive_does_not_replace_a_formal_review_or_enter_trends(self):
        asyncio.run(self._seed_monitor_archive_sample(date(2026, 4, 24)))
        asyncio.run(self._seed_monitor_archive_sample(date(2026, 4, 28)))
        base = "/api/v1/statistics/review"
        snapshot = self.client.get(f"{base}/intraday", params={"trade_date": "2026-04-28"}).json()
        self.assertEqual(snapshot.get("source_status"), "primary")
        self.assertEqual(snapshot["data"]["rows"][0]["limit_up_count"], 8)
        trend = self.client.get(f"{base}/daily", params={"days": 7, "end_date": "2026-04-28"}).json()
        self.assertEqual(trend["data"]["series"], ["2026-04-27", "2026-04-28"])
        detail = self.client.get(f"{base}/detail", params={"trade_date": "2026-04-24"}).json()
        ladder = self.client.get(f"{base}/ladder", params={"trade_date": "2026-04-24"}).json()
        self.assertEqual(detail["stocks"], [])
        self.assertEqual(ladder["ladders"], [])

    def test_intraday_rejects_monitor_archive_with_mismatched_payload_date(self):
        trade_date = date(2026, 4, 24)
        asyncio.run(self._seed_monitor_archive_sample(trade_date, payload_date=date(2026, 4, 23)))
        response = self.client.get(
            "/api/v1/statistics/review/intraday",
            params={"trade_date": trade_date.isoformat()},
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["data_status"], "unavailable")
        self.assertEqual(response.json()["detail"]["stocks"], [])

    async def _seed_exchange_turnover_archive(
        self, trade_date, *, metric_overrides=None, stocks=None, trading_day=True,
    ):
        async with self.session_factory() as session:
            session.add(MarketReviewArchiveSample(
                trade_date=trade_date,
                metric_json={
                    "trade_date": trade_date.isoformat(),
                    "source_status": "exchange_turnover_archive",
                    "market_turnover": 23913.0,
                    **(metric_overrides or {}),
                },
                stocks_json=[] if stocks is None else stocks,
            ))
            await session.commit()
        self.review_module._TRADING_DAY_CACHE[trade_date] = trading_day

    def test_intraday_exposes_exchange_turnover_only_for_the_exact_session(self):
        trade_date = date(2026, 3, 5)
        asyncio.run(self._seed_exchange_turnover_archive(trade_date))
        base = "/api/v1/statistics/review"
        response = self.client.get(f"{base}/intraday", params={"trade_date": "2026-03-05"})
        payload = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["data_status"], "ready")
        self.assertEqual(payload["source_status"], "exchange_turnover_archive")
        self.assertEqual(payload["data"]["series"], ["2026-03-05"])
        self.assertFalse(payload["is_intraday"])
        self.assertFalse(payload["is_live"])
        self.assertFalse(payload["is_fallback"])
        self.assertEqual(payload["detail"], {
            "trade_date": "2026-03-05", "is_fallback": False, "stocks": [],
        })
        self.assertEqual(payload["ladder"], {
            "trade_date": "2026-03-05", "is_fallback": False, "ladders": [],
        })
        row, = payload["data"]["rows"]
        self.assertEqual(row.pop("trade_date"), "2026-03-05")
        self.assertEqual(row.pop("source_status"), "exchange_turnover_archive")
        self.assertEqual(row.pop("market_turnover"), 23913.0)
        self.assertTrue(row)
        self.assertTrue(all(value is None for value in row.values()))

        nearby = self.client.get(f"{base}/intraday", params={"trade_date": "2026-03-04"}).json()
        self.assertEqual(nearby["data_status"], "unavailable")
        self.assertEqual(nearby["data"]["rows"], [])

    def test_exchange_turnover_archive_does_not_replace_formal_review_or_enter_trends(self):
        asyncio.run(self._seed_exchange_turnover_archive(date(2026, 3, 5)))
        asyncio.run(self._seed_exchange_turnover_archive(date(2026, 4, 28)))
        base = "/api/v1/statistics/review"
        snapshot = self.client.get(f"{base}/intraday", params={"trade_date": "2026-04-28"}).json()
        self.assertEqual(snapshot["source_status"], "primary")
        self.assertEqual(snapshot["data"]["rows"][0]["limit_up_count"], 8)
        trend = self.client.get(f"{base}/daily", params={"days": 7, "end_date": "2026-04-28"}).json()
        self.assertEqual(trend["data"]["series"], ["2026-04-27", "2026-04-28"])

    def test_archive_dates_only_lists_readable_past_archive_sessions_newest_first(self):
        now = datetime.now(self.review_module.CN_TZ).date()
        asyncio.run(self._seed_monitor_archive_sample(date(2026, 4, 24)))
        asyncio.run(self._seed_exchange_turnover_archive(date(2026, 3, 5)))
        asyncio.run(self._seed_exchange_turnover_archive(date(2026, 3, 4)))
        asyncio.run(self._seed_exchange_turnover_archive(date(2026, 4, 6), trading_day=False))
        asyncio.run(self._seed_exchange_turnover_archive(now))
        asyncio.run(self._seed_exchange_turnover_archive(now + timedelta(days=1)))
        asyncio.run(self._seed_exchange_turnover_archive(
            date(2026, 3, 3), metric_overrides={"trade_date": "2026-03-02"},
        ))
        asyncio.run(self._seed_exchange_turnover_archive(
            date(2026, 3, 2), metric_overrides={"market_turnover": 0},
        ))
        response = self.client.get("/api/v1/statistics/review/archive-dates")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"dates": ["2026-04-24", "2026-03-05", "2026-03-04"]})

    def test_intraday_rejects_unverified_exchange_turnover_rows(self):
        cases = [
            (date(2026, 3, 2), {"market_turnover": float("inf")}, None),
            (date(2026, 3, 3), {"market_turnover": True}, None),
            (date(2026, 3, 4), {"market_turnover": -1}, None),
            (date(2026, 3, 5), {"limit_up_count": 0}, None),
            (date(2026, 3, 6), {}, [{"stock_code": "600001"}]),
            (datetime.now(self.review_module.CN_TZ).date() + timedelta(days=1), {}, None),
        ]
        for trade_date, metric_overrides, stocks in cases:
            with self.subTest(trade_date=trade_date):
                asyncio.run(self._seed_exchange_turnover_archive(
                    trade_date, metric_overrides=metric_overrides, stocks=stocks,
                ))
                response = self.client.get(
                    "/api/v1/statistics/review/intraday",
                    params={"trade_date": trade_date.isoformat()},
                )
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()["data_status"], "unavailable")
                self.assertEqual(response.json()["data"]["rows"], [])

    def test_intraday_rejects_live_inputs_declaring_a_different_trade_date(self):
        self.review_module._should_collect_live_intraday = Mock(return_value=True)
        self.review_module._collect_intraday_source = AsyncMock(return_value={
            "is_authoritative": True,
            "trade_date": date(2026, 5, 5),
            "stock_rows": [],
        })

        response = self.client.get(
            "/api/v1/statistics/review/intraday",
            params={"trade_date": "2026-05-06"},
        )
        payload = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["data_status"], "unavailable")
        self.assertEqual(payload["detail"]["trade_date"], "2026-05-06")
        self.assertEqual(payload["ladder"]["trade_date"], "2026-05-06")
        self.assertEqual(payload["data"]["rows"], [])

    def test_intraday_source_wrapper_uses_market_review_source_service_collect_for_date(self):
        service = SimpleNamespace(
            collect_for_date=AsyncMock(return_value={"stock_rows": []})
        )
        module_name = "app.services.market_review_source_service"
        original_module = sys.modules.get(module_name)
        sys.modules[module_name] = SimpleNamespace(market_review_source_service=service)
        try:
            result = asyncio.run(self.review_module._collect_intraday_source(date(2026, 5, 6)))
        finally:
            if original_module is None:
                sys.modules.pop(module_name, None)
            else:
                sys.modules[module_name] = original_module

        self.assertEqual(result, {"stock_rows": []})
        service.collect_for_date.assert_awaited_once_with(date(2026, 5, 6))


if __name__ == "__main__":
    unittest.main()
