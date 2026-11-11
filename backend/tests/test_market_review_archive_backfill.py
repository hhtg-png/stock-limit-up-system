import asyncio
import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1 import review
from app.database import Base
from app.models.limit_up import LimitUpRecord
from app.models.market_review import (
    MarketReviewArchiveSample,
    MarketReviewDailyMetric,
    MarketReviewStockDaily,
)
from app.models.stock import Stock
from app.services.market_review_archive_backfill_service import backfill_market_review_archive


class MarketReviewArchiveBackfillTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.engine = create_async_engine(
            f"sqlite+aiosqlite:///{Path(self.directory.name, 'backfill.db').as_posix()}"
        )
        self.addAsyncCleanup(self.engine.dispose)
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def seed(self, day: date):
        async with self.sessions.begin() as db:
            stocks = [
                Stock(stock_code="600001", stock_name="样本甲", market="SH"),
                Stock(stock_code="300001", stock_name="样本乙", market="SZ"),
            ]
            db.add_all(stocks)
            await db.flush()
            db.add_all([
                LimitUpRecord(
                    stock_id=stocks[0].id,
                    trade_date=day,
                    is_final_sealed=True,
                    current_status="sealed",
                    first_limit_up_time=datetime.combine(day, datetime.strptime("09:31:02", "%H:%M:%S").time()),
                    final_seal_time=datetime.combine(day, datetime.strptime("10:11:13", "%H:%M:%S").time()),
                    continuous_limit_up_days=3,
                    open_count=1,
                    amount=10000,
                    turnover_rate=5.27,
                    close_price=12.58,
                ),
                LimitUpRecord(
                    stock_id=stocks[1].id,
                    trade_date=day,
                    is_final_sealed=False,
                    current_status="opened",
                    continuous_limit_up_days=1,
                    open_count=2,
                    amount=0,
                    close_price=0,
                ),
            ])

    async def backfill(self, start, end, days, **kwargs):
        return await backfill_market_review_archive(
            start,
            end,
            session_factory=self.sessions,
            calendar_loader=lambda *_: days,
            current_date=date(2026, 4, 15),
            **kwargs,
        )

    async def samples(self):
        async with self.sessions() as db:
            return (await db.scalars(select(MarketReviewArchiveSample))).all()

    async def test_dry_run_is_read_only_and_execute_keeps_unknown_metrics_null(self):
        day = date(2026, 3, 6)
        await self.seed(day)
        dry_run = await self.backfill(day, day, [day], turnover_by_date={day: 22111.99})
        self.assertEqual([(result.status, result.stock_count) for result in dry_run], [("would_insert", 2)])
        self.assertEqual(await self.samples(), [])

        inserted = await self.backfill(day, day, [day], execute=True, turnover_by_date={day: 22111.99})
        self.assertEqual([result.status for result in inserted], ["inserted"])
        sample, = await self.samples()
        self.assertEqual(sample.trade_date, day)
        self.assertEqual(sample.metric_json["source_status"], "monitor_archive")
        self.assertEqual(sample.metric_json["market_turnover"], 22111.99)
        self.assertEqual(sample.metric_json["limit_up_count"], 2)
        self.assertEqual(sample.metric_json["max_board_height"], 3)
        self.assertEqual(sample.metric_json["seal_rate"], 50)
        for field in (
            "limit_down_count", "up_count_ex_st", "down_count_ex_st",
            "first_to_second_rate", "continuous_promotion_rate",
            "yesterday_limit_up_avg_change", "limit_up_amount", "broken_amount",
        ):
            self.assertIsNone(sample.metric_json[field], field)

        stocks = {stock["stock_code"]: stock for stock in sample.stocks_json}
        sealed = stocks["600001"]
        self.assertEqual(sealed["first_limit_time"], "09:31:02")
        self.assertEqual(sealed["final_seal_time"], "10:11:13")
        self.assertEqual(sealed["open_count"], 1)
        self.assertEqual(sealed["turnover_rate"], 5.27)
        self.assertEqual(sealed["close_price"], 12.58)
        self.assertEqual(sealed["amount"], 10000)
        self.assertIsNone(sealed["change_pct"])
        self.assertIsNone(sealed["yesterday_continuous_days"])
        self.assertEqual(sealed["data_quality_flag"], "monitor_archive")
        opened = stocks["300001"]
        self.assertTrue(opened["today_opened_close"])
        self.assertIsNone(opened["first_limit_time"])
        self.assertIsNone(opened["amount"])
        self.assertIsNone(opened["close_price"])

        async with self.sessions() as db:
            source = (await db.scalars(select(LimitUpRecord).where(LimitUpRecord.amount == 10000))).one()
            self.assertEqual(source.continuous_limit_up_days, 3)
            self.assertEqual(source.first_limit_up_time, datetime(2026, 3, 6, 9, 31, 2))
            self.assertEqual((await db.scalars(select(MarketReviewDailyMetric))).all(), [])
            self.assertEqual((await db.scalars(select(MarketReviewStockDaily))).all(), [])

    async def test_formal_and_isolated_review_rows_are_not_overwritten(self):
        formal_day, isolated_day = date(2026, 3, 6), date(2026, 3, 9)
        await self.seed(formal_day)
        async with self.sessions.begin() as db:
            stock_id = await db.scalar(select(Stock.id).where(Stock.stock_code == "600001"))
            db.add(MarketReviewDailyMetric(trade_date=formal_day, limit_up_count=99))
            db.add(MarketReviewStockDaily(
                trade_date=isolated_day, stock_id=stock_id,
                stock_code="600001", stock_name="保留", amount=4321,
            ))
            db.add(LimitUpRecord(stock_id=stock_id, trade_date=isolated_day))

        result = await self.backfill(formal_day, isolated_day, [formal_day, isolated_day], execute=True)
        self.assertEqual([item.status for item in result], ["has_review", "incomplete_review"])
        self.assertEqual(await self.samples(), [])
        async with self.sessions() as db:
            self.assertEqual((await db.scalars(select(MarketReviewDailyMetric))).one().limit_up_count, 99)
            self.assertEqual((await db.scalars(select(MarketReviewStockDaily))).one().amount, 4321)

    async def test_repeat_and_concurrent_execution_insert_one_sample(self):
        day = date(2026, 3, 6)
        await self.seed(day)
        reports = await asyncio.gather(*[
            self.backfill(day, day, [day], execute=True)
            for _ in range(2)
        ])
        self.assertEqual({report[0].status for report in reports}, {"inserted", "has_sample"})
        first, = await self.samples()
        repeated = await self.backfill(day, day, [day], execute=True, turnover_by_date={day: 22222})
        self.assertEqual(repeated[0].status, "has_sample")
        latest, = await self.samples()
        self.assertEqual(latest.generated_at, first.generated_at)
        self.assertIsNone(latest.metric_json["market_turnover"])

    async def test_calendar_filters_missing_days_weekends_and_holidays_and_rejects_today(self):
        holiday, weekend, session = date(2026, 4, 6), date(2026, 4, 5), date(2026, 4, 7)
        await self.seed(holiday)
        result = await self.backfill(weekend, session, [weekend, session], execute=True)
        self.assertEqual([(item.trade_date, item.status) for item in result], [(session, "no_source")])
        self.assertEqual(await self.samples(), [])
        today = date(2026, 4, 15)
        with self.assertRaises(ValueError):
            await self.backfill(today, today, [today], execute=True)

    async def test_invalid_turnover_fails_before_writing(self):
        day = date(2026, 3, 6)
        await self.seed(day)
        for invalid in (float("inf"), float("nan"), 0, -1, True, "22111.99"):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                await self.backfill(day, day, [day], execute=True, turnover_by_date={day: invalid})
        self.assertEqual(await self.samples(), [])

    async def test_quoted_fields_enrich_only_matched_records_for_that_session(self):
        day = date(2026, 3, 6)
        await self.seed(day)
        await self.backfill(day, day, [day], execute=True, quotes_by_date={
            day: {
                "300001": {
                    "close_price": 16.50, "pre_close": 16.50, "change_pct": 0,
                    "amount": 12345, "turnover_rate": 7.33,
                },
                "000001": {"close_price": 10, "amount": 1},
                "600001": {"amount": None},
            },
        })
        sample, = await self.samples()
        rows = {row["stock_code"]: row for row in sample.stocks_json}
        self.assertEqual(set(rows), {"600001", "300001"})
        self.assertEqual(rows["300001"]["change_pct"], 0.0)
        self.assertEqual(rows["300001"]["pre_close"], 16.50)
        self.assertEqual(rows["300001"]["close_price"], 16.50)
        self.assertEqual(rows["300001"]["turnover_rate"], 7.33)
        self.assertEqual(rows["600001"]["amount"], 10000)
        self.assertEqual(sample.metric_json["limit_up_amount"], 22345)
        self.assertEqual(sample.metric_json["broken_amount"], 12345)
        async with self.sessions() as db:
            opened = (await db.scalars(select(LimitUpRecord).where(LimitUpRecord.is_final_sealed.is_(False)))).one()
            self.assertEqual(opened.amount, 0)
            self.assertEqual(opened.close_price, 0)

    async def test_invalid_quote_and_non_trading_evidence_abort_before_writing(self):
        day = date(2026, 3, 6)
        await self.seed(day)
        for quotes in (
            {day: {"600001": {"close_price": 0}}},
            {day: {"600001": {"amount": -1}}},
            {day: {"600001": {"change_pct": float("nan")}}},
            {day: {"600001": {"turnover_rate": True}}},
            {day: {"invalid": {"close_price": 10}}},
            {date(2026, 3, 7): {"600001": {"close_price": 10}}},
        ):
            with self.subTest(quotes=quotes), self.assertRaises(ValueError):
                await self.backfill(day, date(2026, 3, 7), [day], execute=True, quotes_by_date=quotes)
        self.assertEqual(await self.samples(), [])

    async def test_generated_sample_is_available_from_the_exact_date_endpoint(self):
        day = date(2026, 3, 6)
        await self.seed(day)
        await self.backfill(day, day, [day], execute=True, turnover_by_date={day: 22111.99})
        api = FastAPI()
        api.include_router(review.router, prefix="/api/v1/statistics/review")

        async def session_dependency():
            async with self.sessions() as db:
                yield db

        api.dependency_overrides[review.get_db] = session_dependency
        with patch.object(review, "_should_collect_live_intraday", return_value=False), patch.object(
            review, "_is_cn_trading_day", side_effect=lambda requested: requested == day
        ):
            async with AsyncClient(transport=ASGITransport(app=api), base_url="http://test") as client:
                response = await client.get(f"/api/v1/statistics/review/intraday?trade_date={day.isoformat()}")

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["source_status"], "monitor_archive")
        self.assertEqual(body["detail"]["trade_date"], day.isoformat())
        self.assertEqual(body["data"]["rows"][0]["market_turnover"], 22111.99)
        self.assertEqual(len(body["detail"]["stocks"]), 2)
        self.assertIsNone(body["ladder"]["ladders"][0]["cohort_count"])
