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

    async def test_bj_limit_helpers_use_thirty_percent_board_rules(self):
        service = MarketReviewSourceService(
            session_factory=self.session_factory,
            current_date_provider=lambda: date(2026, 4, 29),
        )

        self.assertAlmostEqual(service._limit_ratio("830001", "北交样本"), 0.30)
        self.assertTrue(service._is_limit_down(-29.6, "830001", "北交样本"))
        self.assertFalse(service._is_limit_down(-19.8, "830001", "北交样本"))


if __name__ == "__main__":
    unittest.main()
