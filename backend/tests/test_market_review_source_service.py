import unittest
from datetime import date, datetime

import pandas as pd
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

import app.models  # noqa: F401
from app.database import Base
from app.models.stock import Stock
from app.services.market_review_source_service import MarketReviewSourceService


class MarketReviewSourceServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:", future=True)
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        self.session_factory = async_sessionmaker(
            self.engine,
            expire_on_commit=False,
        )

        async with self.session_factory() as session:
            session.add(
                Stock(
                    stock_code="600001",
                    stock_name="Alpha",
                    market="SH",
                    is_st=0,
                    is_kc=0,
                    is_cy=0,
                )
            )
            await session.commit()

    async def asyncTearDown(self):
        await self.engine.dispose()

    async def test_collect_for_date_builds_authoritative_payload_from_market_sources(self):
        async def today_fetcher(trade_date):
            self.assertEqual(trade_date, date(2026, 4, 29))
            return [
                {
                    "stock_code": "600001",
                    "stock_name": "Alpha",
                    "continuous_limit_up_days": 2,
                    "is_sealed": False,
                    "first_limit_up_time": datetime(2026, 4, 29, 9, 31, 0),
                    "open_count": 1,
                    "current_price": 11.0,
                    "change_pct": 10.0,
                    "amount": 123456.0,
                    "turnover_rate": 12.3,
                    "tradable_market_value": 456789.0,
                    "limit_up_reason": "AI",
                    "data_source": "RT",
                }
            ]

        async def yesterday_pool_fetcher(trade_date):
            self.assertEqual(trade_date, date(2026, 4, 29))
            return [
                {
                    "c": "600001",
                    "n": "Alpha",
                    "ylbc": 1,
                    "zdp": 10.0,
                },
                {
                    "c": "600002",
                    "n": "Beta",
                    "ylbc": 2,
                    "zdp": -3.2,
                },
            ]

        async def quote_fetcher(codes):
            self.assertEqual(set(codes), {"600001", "600002"})
            return {
                "600001": {
                    "code": "600001",
                    "name": "Alpha",
                    "price": 11.0,
                    "pre_close": 10.0,
                    "change_pct": 10.0,
                    "amount": 123456.0,
                    "turnover_rate": 12.3,
                },
                "600002": {
                    "code": "600002",
                    "name": "Beta",
                    "price": 8.8,
                    "pre_close": 9.09,
                    "change_pct": -3.2,
                    "amount": 654321.0,
                    "turnover_rate": 5.6,
                },
            }

        async def market_stats_fetcher(trade_date):
            self.assertEqual(trade_date, date(2026, 4, 29))
            return {
                "limit_down_count": 4,
                "market_turnover": 9876.5,
                "up_count_ex_st": 3000,
                "down_count_ex_st": 1200,
            }

        service = MarketReviewSourceService(
            session_factory=self.session_factory,
            today_limit_up_fetcher=today_fetcher,
            yesterday_pool_fetcher=yesterday_pool_fetcher,
            quote_fetcher=quote_fetcher,
            market_stats_fetcher=market_stats_fetcher,
            current_date_provider=lambda: date(2026, 4, 29),
        )

        payload = await service.collect_for_date(date(2026, 4, 29))

        self.assertTrue(payload["is_authoritative"])
        self.assertEqual(payload["source_status"], "primary")
        self.assertEqual(payload["limit_down_count"], 4)
        self.assertAlmostEqual(payload["market_turnover"], 9876.5)
        self.assertEqual(payload["up_count_ex_st"], 3000)
        self.assertEqual(payload["down_count_ex_st"], 1200)

        rows_by_code = {row["stock_code"]: row for row in payload["stock_rows"]}
        self.assertEqual(set(rows_by_code), {"600001", "600002"})

        alpha = rows_by_code["600001"]
        self.assertTrue(alpha["yesterday_limit_up"])
        self.assertEqual(alpha["yesterday_continuous_days"], 1)
        self.assertTrue(alpha["today_touched_limit_up"])
        self.assertFalse(alpha["today_sealed_close"])
        self.assertTrue(alpha["today_opened_close"])
        self.assertTrue(alpha["today_broken"])
        self.assertEqual(alpha["today_continuous_days"], 2)
        self.assertAlmostEqual(alpha["change_pct"], 10.0)
        self.assertEqual(alpha["limit_up_reason"], "AI")
        self.assertIsNotNone(alpha["stock_id"])

        beta = rows_by_code["600002"]
        self.assertTrue(beta["yesterday_limit_up"])
        self.assertEqual(beta["yesterday_continuous_days"], 2)
        self.assertFalse(beta["today_touched_limit_up"])
        self.assertEqual(beta["today_continuous_days"], 0)
        self.assertAlmostEqual(beta["change_pct"], -3.2)
        self.assertAlmostEqual(beta["amount"], 654321.0)
        self.assertIsNotNone(beta["stock_id"])

        event_keys = {
            (row["stock_code"], row["event_type"])
            for row in payload["event_rows"]
        }
        self.assertIn(("600001", "first_seal"), event_keys)
        self.assertIn(("600001", "close_opened"), event_keys)

        async with self.session_factory() as session:
            stock_codes = (
                await session.execute(
                    select(Stock.stock_code).order_by(Stock.stock_code.asc())
                )
            ).scalars().all()

        self.assertEqual(stock_codes, ["600001", "600002"])

    async def test_collect_for_date_marks_301_codes_as_gem_board(self):
        async def today_fetcher(trade_date):
            self.assertEqual(trade_date, date(2026, 6, 15))
            return [
                {
                    "stock_code": "301176",
                    "stock_name": "逸豪新材",
                    "continuous_limit_up_days": 2,
                    "is_sealed": True,
                    "first_limit_up_time": datetime(2026, 6, 15, 9, 32, 18),
                    "current_price": 60.0,
                    "change_pct": 20.0,
                    "amount": 42969.0,
                    "limit_up_reason": "PCB铜箔",
                }
            ]

        async def empty_yesterday_pool(_trade_date):
            return []

        async def quote_fetcher(codes):
            self.assertEqual(set(codes), {"301176"})
            return {
                "301176": {
                    "code": "301176",
                    "name": "逸豪新材",
                    "price": 60.0,
                    "pre_close": 50.0,
                    "change_pct": 20.0,
                    "amount": 42969.0,
                }
            }

        async def market_stats_fetcher(_trade_date):
            return {
                "limit_down_count": 0,
                "market_turnover": 1000.0,
                "up_count_ex_st": 100,
                "down_count_ex_st": 100,
            }

        service = MarketReviewSourceService(
            session_factory=self.session_factory,
            today_limit_up_fetcher=today_fetcher,
            yesterday_pool_fetcher=empty_yesterday_pool,
            quote_fetcher=quote_fetcher,
            market_stats_fetcher=market_stats_fetcher,
            current_date_provider=lambda: date(2026, 6, 15),
        )

        payload = await service.collect_for_date(date(2026, 6, 15))
        row = payload["stock_rows"][0]

        self.assertEqual(row["stock_code"], "301176")
        self.assertEqual(row["board_type"], "gem")
        self.assertEqual(row["today_continuous_days"], 2)
        self.assertAlmostEqual(service._limit_ratio("301176", "逸豪新材"), 0.20)
        self.assertTrue(service._is_limit_down(-19.6, "301176", "逸豪新材"))
        self.assertFalse(service._is_limit_down(-10.0, "301176", "逸豪新材"))

        async with self.session_factory() as session:
            stock = (
                await session.execute(
                    select(Stock).where(Stock.stock_code == "301176")
                )
            ).scalar_one()

        self.assertEqual(stock.is_cy, 1)

    async def test_collect_for_date_excludes_delisting_period_stocks(self):
        async def today_fetcher(_trade_date):
            return [
                {
                    "stock_code": "920305",
                    "stock_name": "云创退",
                    "continuous_limit_up_days": 5,
                    "is_sealed": True,
                    "change_pct": 29.49,
                },
                {
                    "stock_code": "603580",
                    "stock_name": "艾艾精工",
                    "continuous_limit_up_days": 4,
                    "is_sealed": True,
                    "change_pct": 9.99,
                },
            ]

        async def yesterday_pool_fetcher(_trade_date):
            return [
                {"c": "920305", "n": "云创退", "ylbc": 4, "zdp": 29.49},
                {"c": "603580", "n": "艾艾精工", "ylbc": 3, "zdp": 9.99},
            ]

        async def quote_fetcher(codes):
            self.assertEqual(set(codes), {"920305", "603580"})
            return {
                "920305": {"name": "云创退", "price": 3.38, "change_pct": 29.49},
                "603580": {"name": "艾艾精工", "price": 28.52, "change_pct": 9.99},
            }

        async def market_stats_fetcher(_trade_date):
            return {
                "limit_down_count": 10,
                "market_turnover": 26710.2,
                "up_count_ex_st": 461,
                "down_count_ex_st": 4812,
            }

        service = MarketReviewSourceService(
            session_factory=self.session_factory,
            today_limit_up_fetcher=today_fetcher,
            yesterday_pool_fetcher=yesterday_pool_fetcher,
            quote_fetcher=quote_fetcher,
            market_stats_fetcher=market_stats_fetcher,
            current_date_provider=lambda: date(2026, 7, 17),
        )

        payload = await service.collect_for_date(date(2026, 7, 17))

        self.assertEqual(
            [row["stock_code"] for row in payload["stock_rows"]],
            ["603580"],
        )
        self.assertEqual(
            {row["stock_code"] for row in payload["event_rows"]},
            {"603580"},
        )
        async with self.session_factory() as session:
            delisting_stock = (
                await session.execute(
                    select(Stock).where(Stock.stock_code == "920305")
                )
            ).scalar_one_or_none()
        self.assertIsNone(delisting_stock)

    async def test_ensure_stock_ids_refreshes_existing_classification(self):
        async with self.session_factory() as session:
            session.add(
                Stock(
                    stock_code="300577",
                    stock_name="旧名称",
                    market="SH",
                    is_st=1,
                    is_kc=1,
                    is_cy=0,
                )
            )
            await session.commit()

        service = MarketReviewSourceService(
            session_factory=self.session_factory,
            current_date_provider=lambda: date(2026, 7, 17),
        )
        stock_ids = await service._ensure_stock_ids(
            {
                "300577": {
                    "stock_code": "300577",
                    "stock_name": "开润股份",
                    "market": "SZ",
                    "is_st": 0,
                    "is_kc": 0,
                    "is_cy": 1,
                }
            }
        )

        self.assertIn("300577", stock_ids)
        async with self.session_factory() as session:
            stock = (
                await session.execute(
                    select(Stock).where(Stock.stock_code == "300577")
                )
            ).scalar_one()
        self.assertEqual(stock.stock_name, "开润股份")
        self.assertEqual(stock.market, "SZ")
        self.assertEqual(stock.is_st, 0)
        self.assertEqual(stock.is_kc, 0)
        self.assertEqual(stock.is_cy, 1)

    async def test_collect_for_date_derives_opened_continuation_from_yesterday_pool(self):
        async def today_fetcher(trade_date):
            self.assertEqual(trade_date, date(2026, 6, 16))
            return [
                {
                    "stock_code": "600001",
                    "stock_name": "Alpha",
                    "continuous_limit_up_days": None,
                    "is_sealed": False,
                    "first_limit_up_time": datetime(2026, 6, 16, 9, 57, 0),
                    "open_count": 7,
                    "current_price": 11.0,
                    "change_pct": 6.0,
                    "amount": 123456.0,
                    "limit_up_reason": "化学制品",
                    "data_source": "EM",
                }
            ]

        async def yesterday_pool_fetcher(trade_date):
            self.assertEqual(trade_date, date(2026, 6, 16))
            return [
                {
                    "c": "600001",
                    "n": "Alpha",
                    "ylbc": 4,
                    "zdp": 10.0,
                }
            ]

        async def quote_fetcher(codes):
            self.assertEqual(set(codes), {"600001"})
            return {
                "600001": {
                    "code": "600001",
                    "name": "Alpha",
                    "price": 11.0,
                    "pre_close": 10.4,
                    "change_pct": 6.0,
                    "amount": 123456.0,
                }
            }

        async def market_stats_fetcher(_trade_date):
            return {
                "limit_down_count": 0,
                "market_turnover": 1000.0,
                "up_count_ex_st": 100,
                "down_count_ex_st": 100,
            }

        service = MarketReviewSourceService(
            session_factory=self.session_factory,
            today_limit_up_fetcher=today_fetcher,
            yesterday_pool_fetcher=yesterday_pool_fetcher,
            quote_fetcher=quote_fetcher,
            market_stats_fetcher=market_stats_fetcher,
            current_date_provider=lambda: date(2026, 6, 16),
        )

        payload = await service.collect_for_date(date(2026, 6, 16))
        row = payload["stock_rows"][0]

        self.assertTrue(row["today_opened_close"])
        self.assertEqual(row["yesterday_continuous_days"], 4)
        self.assertEqual(row["today_continuous_days"], 5)

    async def test_collect_for_date_preserves_limit_up_price_and_seal_amount_for_sync(self):
        async def today_fetcher(trade_date):
            self.assertEqual(trade_date, date(2026, 6, 18))
            return [
                {
                    "stock_code": "600001",
                    "stock_name": "Alpha",
                    "continuous_limit_up_days": 1,
                    "is_sealed": False,
                    "first_limit_up_time": datetime(2026, 6, 18, 9, 40, 0),
                    "open_count": 2,
                    "current_price": 10.66,
                    "limit_up_price": 11.0,
                    "seal_amount": 4321.0,
                    "change_pct": 6.6,
                    "amount": 123456.0,
                    "limit_up_reason": "算力",
                    "data_source": "EM",
                }
            ]

        async def empty_yesterday_pool(_trade_date):
            return []

        async def empty_quotes(_codes):
            return {}

        async def market_stats_fetcher(_trade_date):
            return {
                "limit_down_count": 0,
                "market_turnover": 1000.0,
                "up_count_ex_st": 100,
                "down_count_ex_st": 100,
            }

        service = MarketReviewSourceService(
            session_factory=self.session_factory,
            today_limit_up_fetcher=today_fetcher,
            yesterday_pool_fetcher=empty_yesterday_pool,
            quote_fetcher=empty_quotes,
            market_stats_fetcher=market_stats_fetcher,
            current_date_provider=lambda: date(2026, 6, 18),
        )

        payload = await service.collect_for_date(date(2026, 6, 18))
        row = payload["stock_rows"][0]

        self.assertAlmostEqual(row["limit_up_price"], 11.0)
        self.assertAlmostEqual(row["seal_amount"], 4321.0)

    async def test_historical_market_stats_are_loaded_when_daily_statistics_are_missing(self):
        called = {}

        class HistoricalStatsService(MarketReviewSourceService):
            def _fetch_historical_market_stats_sync(self, trade_date):
                called["trade_date"] = trade_date
                return {
                    "limit_down_count": 20,
                    "market_turnover": 26419.0,
                    "up_count_ex_st": 1994,
                    "down_count_ex_st": 3085,
                }

        service = HistoricalStatsService(
            session_factory=self.session_factory,
            current_date_provider=lambda: date(2026, 4, 29),
        )

        stats = await service._fetch_market_stats(date(2026, 4, 24))

        self.assertEqual(
            stats,
            {
                "limit_down_count": 20,
                "market_turnover": 26419.0,
                "up_count_ex_st": 1994,
                "down_count_ex_st": 3085,
            },
        )
        self.assertEqual(called["trade_date"], date(2026, 4, 24))

    def test_extract_exchange_market_turnover_normalizes_sse_and_szse_units(self):
        service = MarketReviewSourceService(
            session_factory=self.session_factory,
            current_date_provider=lambda: date(2026, 4, 29),
        )
        sse_df = pd.DataFrame(
            [
                {"单日情况": "成交金额", "股票": 11274.75},
            ]
        )
        szse_df = pd.DataFrame(
            [
                {"证券类别": "股票", "成交金额": 1465264000000.0},
            ]
        )

        turnover = service._extract_exchange_market_turnover(sse_df, szse_df)

        self.assertAlmostEqual(turnover, 25927.39)

    def test_parse_tencent_stock_history_stats_counts_moves_from_previous_close(self):
        service = MarketReviewSourceService(
            session_factory=self.session_factory,
            current_date_provider=lambda: date(2026, 4, 29),
        )
        rows = [
            ["2026-04-23", "9.80", "10.00", "10.10", "9.70", "1000"],
            ["2026-04-24", "10.00", "11.00", "11.00", "9.90", "2000"],
            ["2026-04-27", "11.00", "9.90", "11.10", "9.80", "3000"],
        ]

        stats = service._parse_tencent_stock_history_stats(
            stock_code="600001",
            stock_name="Alpha",
            rows=rows,
            count_start_date=date(2026, 4, 24),
        )

        self.assertEqual(
            stats,
            {
                date(2026, 4, 24): {
                    "limit_down_count": 0,
                    "up_count_ex_st": 1,
                    "down_count_ex_st": 0,
                },
                date(2026, 4, 27): {
                    "limit_down_count": 1,
                    "up_count_ex_st": 0,
                    "down_count_ex_st": 1,
                },
            },
        )

    async def test_collect_for_date_returns_placeholder_when_all_sources_are_empty(self):
        async def empty_fetcher(_trade_date):
            return []

        async def empty_quotes(_codes):
            return {}

        async def empty_market_stats(_trade_date):
            return {}

        service = MarketReviewSourceService(
            session_factory=self.session_factory,
            today_limit_up_fetcher=empty_fetcher,
            yesterday_pool_fetcher=empty_fetcher,
            quote_fetcher=empty_quotes,
            market_stats_fetcher=empty_market_stats,
            current_date_provider=lambda: date(2026, 4, 29),
        )

        payload = await service.collect_for_date(date(2026, 4, 28))

        self.assertFalse(payload["is_authoritative"])
        self.assertEqual(payload["source_status"], "placeholder")
        self.assertEqual(payload["stock_rows"], [])
        self.assertEqual(payload["event_rows"], [])

    async def test_collect_for_date_fails_safe_when_today_source_failed_but_yesterday_pool_is_non_empty(self):
        async def failed_today_fetcher(_trade_date):
            return {
                "items": [],
                "succeeded": False,
            }

        async def yesterday_pool_fetcher(_trade_date):
            return [
                {
                    "c": "600002",
                    "n": "Beta",
                    "ylbc": 2,
                    "zdp": -3.2,
                }
            ]

        async def empty_quotes(_codes):
            return {}

        async def empty_market_stats(_trade_date):
            return {}

        service = MarketReviewSourceService(
            session_factory=self.session_factory,
            today_limit_up_fetcher=failed_today_fetcher,
            yesterday_pool_fetcher=yesterday_pool_fetcher,
            quote_fetcher=empty_quotes,
            market_stats_fetcher=empty_market_stats,
            current_date_provider=lambda: date(2026, 4, 29),
        )

        payload = await service.collect_for_date(date(2026, 4, 28))

        self.assertFalse(payload["is_authoritative"])
        self.assertEqual(payload["source_status"], "placeholder")
        self.assertEqual(payload["stock_rows"], [])
        self.assertEqual(payload["event_rows"], [])

    async def test_collect_for_date_drops_yesterday_pool_sentinel_change_pct(self):
        async def empty_today_fetcher(_trade_date):
            return []

        async def yesterday_pool_fetcher(_trade_date):
            return [
                {
                    "c": "603272",
                    "n": "联翔股份",
                    "ylbc": 2,
                    "zdp": -100.0,
                    "p": 0,
                    "amount": 0,
                    "ltsz": 0.0,
                }
            ]

        async def empty_quotes(_codes):
            return {}

        async def market_stats_fetcher(_trade_date):
            return {
                "limit_down_count": 0,
                "market_turnover": 1000.0,
                "up_count_ex_st": 100,
                "down_count_ex_st": 100,
            }

        service = MarketReviewSourceService(
            session_factory=self.session_factory,
            today_limit_up_fetcher=empty_today_fetcher,
            yesterday_pool_fetcher=yesterday_pool_fetcher,
            quote_fetcher=empty_quotes,
            market_stats_fetcher=market_stats_fetcher,
            current_date_provider=lambda: date(2026, 4, 29),
        )

        payload = await service.collect_for_date(date(2026, 4, 28))

        self.assertTrue(payload["is_authoritative"])
        self.assertEqual(len(payload["stock_rows"]), 1)
        row = payload["stock_rows"][0]
        self.assertEqual(row["stock_code"], "603272")
        self.assertIsNone(row["change_pct"])

    async def test_bj_limit_helpers_use_thirty_percent_board_rules(self):
        service = MarketReviewSourceService(
            session_factory=self.session_factory,
            current_date_provider=lambda: date(2026, 4, 29),
        )

        self.assertAlmostEqual(service._limit_ratio("830001", "北交样本"), 0.30)
        self.assertTrue(service._is_limit_down(-29.6, "830001", "北交样本"))
        self.assertFalse(service._is_limit_down(-19.8, "830001", "北交样本"))
        self.assertEqual(service._detect_market("920305"), "BJ")
        self.assertEqual(service._detect_board_type("920305"), "bj")
        self.assertAlmostEqual(service._limit_ratio("920305", "北交样本"), 0.30)


if __name__ == "__main__":
    unittest.main()
