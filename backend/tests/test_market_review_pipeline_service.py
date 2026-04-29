import unittest
from datetime import date, time

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

import app.models  # noqa: F401
from app.database import Base
from app.models.market_review import (
    MarketReviewDailyMetric,
    MarketReviewLimitUpEvent,
    MarketReviewStockDaily,
)
from app.models.stock import Stock
from app.services.market_review_pipeline_service import MarketReviewPipelineService


class StubSourceService:
    def __init__(self, normalized):
        self.normalized = normalized
        self.called_with = None

    async def collect_for_date(self, trade_date):
        self.called_with = trade_date
        return self.normalized


class MarketReviewPipelineServiceTests(unittest.IsolatedAsyncioTestCase):
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
                    id=1,
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

    def _normalized_input(self):
        return {
            "stock_rows": [
                {
                    "stock_id": 1,
                    "stock_code": "600001",
                    "stock_name": "Alpha",
                    "board_type": "main",
                    "is_st": False,
                    "yesterday_limit_up": True,
                    "yesterday_continuous_days": 1,
                    "today_touched_limit_up": True,
                    "today_sealed_close": True,
                    "today_opened_close": False,
                    "today_broken": False,
                    "today_continuous_days": 2,
                    "first_limit_time": time(9, 31),
                    "final_seal_time": time(14, 55),
                    "open_count": 1,
                    "close_price": 11.2,
                    "pre_close": 10.18,
                    "change_pct": 10.02,
                    "amount": 123456.78,
                    "turnover_rate": 12.34,
                    "tradable_market_value": 456789000.0,
                    "limit_up_reason": "AI",
                    "data_quality_flag": "ok",
                }
            ],
            "event_rows": [
                {
                    "stock_id": 1,
                    "stock_code": "600001",
                    "stock_name": "Alpha",
                    "event_type": "seal",
                    "event_time": time(9, 31),
                    "event_seq": 1,
                    "source_name": "stub",
                    "payload_json": {"note": "initial"},
                }
            ],
            "limit_down_count": 3,
            "market_turnover": 8888.5,
            "up_count_ex_st": 3000,
            "down_count_ex_st": 1200,
            "source_status": "stubbed",
        }

    async def test_build_payload_for_date_uses_supplied_normalized_input(self):
        normalized = self._normalized_input()
        service = MarketReviewPipelineService()

        payload = await service.build_payload_for_date(date(2026, 4, 28), normalized=normalized)

        self.assertEqual(payload["trade_date"], date(2026, 4, 28))
        self.assertIsNot(payload["stock_rows"], normalized["stock_rows"])
        self.assertIsNot(payload["event_rows"], normalized["event_rows"])
        self.assertIsNot(payload["stock_rows"][0], normalized["stock_rows"][0])
        self.assertIsNot(payload["event_rows"][0], normalized["event_rows"][0])
        self.assertEqual(payload["stock_rows"][0]["trade_date"], date(2026, 4, 28))
        self.assertEqual(payload["event_rows"][0]["trade_date"], date(2026, 4, 28))
        self.assertNotIn("trade_date", normalized["stock_rows"][0])
        self.assertNotIn("trade_date", normalized["event_rows"][0])
        self.assertEqual(payload["metric_row"]["trade_date"], date(2026, 4, 28))
        self.assertEqual(payload["metric_row"]["limit_up_count"], 1)
        self.assertEqual(payload["metric_row"]["limit_down_count"], 3)
        self.assertEqual(payload["metric_row"]["continuous_count"], 1)
        self.assertEqual(payload["metric_row"]["max_board_height"], 2)
        self.assertAlmostEqual(payload["metric_row"]["market_turnover"], 8888.5)
        self.assertEqual(payload["metric_row"]["up_count_ex_st"], 3000)
        self.assertEqual(payload["metric_row"]["down_count_ex_st"], 1200)
        self.assertEqual(payload["metric_row"]["source_status"], "stubbed")

    async def test_run_for_date_collects_from_source_service_when_normalized_not_supplied(self):
        normalized = self._normalized_input()
        source_service = StubSourceService(normalized)
        service = MarketReviewPipelineService(source_service=source_service)

        payload = await service.build_payload_for_date(date(2026, 4, 28))

        self.assertEqual(source_service.called_with, date(2026, 4, 28))
        self.assertEqual(payload["metric_row"]["source_status"], "stubbed")

    async def test_build_payload_for_date_reusing_normalized_input_does_not_leak_first_trade_date(self):
        normalized = self._normalized_input()
        service = MarketReviewPipelineService()

        first_payload = await service.build_payload_for_date(date(2026, 4, 28), normalized=normalized)
        second_payload = await service.build_payload_for_date(date(2026, 4, 29), normalized=normalized)

        self.assertEqual(first_payload["stock_rows"][0]["trade_date"], date(2026, 4, 28))
        self.assertEqual(first_payload["event_rows"][0]["trade_date"], date(2026, 4, 28))
        self.assertEqual(second_payload["stock_rows"][0]["trade_date"], date(2026, 4, 29))
        self.assertEqual(second_payload["event_rows"][0]["trade_date"], date(2026, 4, 29))
        self.assertNotIn("trade_date", normalized["stock_rows"][0])
        self.assertNotIn("trade_date", normalized["event_rows"][0])

    async def test_persist_payload_upserts_metric_stock_and_event_rows(self):
        service = MarketReviewPipelineService(session_factory=self.session_factory)
        first_payload = await service.build_payload_for_date(
            date(2026, 4, 28),
            normalized=self._normalized_input(),
        )

        await service.run_for_date(date(2026, 4, 28), calc_version=7, normalized=self._normalized_input())

        updated_normalized = self._normalized_input()
        updated_normalized["stock_rows"][0]["stock_name"] = "Alpha Updated"
        updated_normalized["stock_rows"][0]["today_opened_close"] = True
        updated_normalized["stock_rows"][0]["today_broken"] = True
        updated_normalized["stock_rows"][0]["amount"] = 223456.78
        updated_normalized["event_rows"][0]["payload_json"] = {"note": "updated"}
        updated_normalized["limit_down_count"] = 5
        updated_normalized["market_turnover"] = 9999.9
        updated_normalized["source_status"] = "refreshed"

        second_payload = await service.build_payload_for_date(
            date(2026, 4, 28),
            normalized=updated_normalized,
        )
        second_payload["metric_row"]["calc_version"] = 8

        async with self.session_factory() as session:
            await service.persist_payload(session, first_payload)
            await service.persist_payload(session, second_payload)
            await session.commit()

        async with self.session_factory() as session:
            metric_rows = (await session.execute(select(MarketReviewDailyMetric))).scalars().all()
            stock_rows = (await session.execute(select(MarketReviewStockDaily))).scalars().all()
            event_rows = (await session.execute(select(MarketReviewLimitUpEvent))).scalars().all()

        self.assertEqual(len(metric_rows), 1)
        self.assertEqual(len(stock_rows), 1)
        self.assertEqual(len(event_rows), 1)

        metric = metric_rows[0]
        stock_row = stock_rows[0]
        event_row = event_rows[0]

        self.assertEqual(metric.trade_date, date(2026, 4, 28))
        self.assertEqual(metric.limit_down_count, 5)
        self.assertAlmostEqual(metric.market_turnover, 9999.9)
        self.assertEqual(metric.calc_version, 8)
        self.assertEqual(metric.source_status, "refreshed")

        self.assertEqual(stock_row.stock_id, 1)
        self.assertEqual(stock_row.stock_code, "600001")
        self.assertEqual(stock_row.stock_name, "Alpha Updated")
        self.assertTrue(stock_row.today_opened_close)
        self.assertTrue(stock_row.today_broken)
        self.assertAlmostEqual(stock_row.amount, 223456.78)

        self.assertEqual(event_row.stock_id, 1)
        self.assertEqual(event_row.stock_code, "600001")
        self.assertEqual(event_row.event_type, "seal")
        self.assertEqual(event_row.event_seq, 1)
        self.assertEqual(event_row.payload_json, {"note": "updated"})

    async def test_persist_payload_upserts_heterogeneous_rows_and_leaves_commit_to_caller(self):
        service = MarketReviewPipelineService(session_factory=self.session_factory)
        first_payload = await service.build_payload_for_date(
            date(2026, 4, 28),
            normalized=self._normalized_input(),
        )

        updated_normalized = self._normalized_input()
        updated_normalized["stock_rows"][0].pop("today_opened_close")
        updated_normalized["stock_rows"][0]["today_broken"] = True
        updated_normalized["stock_rows"][0]["amount"] = 321000.0
        updated_normalized["stock_rows"][0]["turnover_rate"] = 18.88
        updated_normalized["event_rows"][0]["payload_json"] = {"note": "updated without commit"}
        second_payload = await service.build_payload_for_date(
            date(2026, 4, 28),
            normalized=updated_normalized,
        )

        async with self.session_factory() as session:
            await service.persist_payload(session, first_payload)
            self.assertTrue(session.in_transaction())
            await service.persist_payload(session, second_payload)
            self.assertTrue(session.in_transaction())
            await session.commit()

        async with self.session_factory() as session:
            stock_row = (
                await session.execute(
                    select(MarketReviewStockDaily).where(
                        MarketReviewStockDaily.trade_date == date(2026, 4, 28),
                        MarketReviewStockDaily.stock_code == "600001",
                    )
                )
            ).scalar_one()
            event_row = (
                await session.execute(
                    select(MarketReviewLimitUpEvent).where(
                        MarketReviewLimitUpEvent.trade_date == date(2026, 4, 28),
                        MarketReviewLimitUpEvent.stock_code == "600001",
                    )
                )
            ).scalar_one()

        self.assertFalse(stock_row.today_opened_close)
        self.assertTrue(stock_row.today_broken)
        self.assertAlmostEqual(stock_row.amount, 321000.0)
        self.assertAlmostEqual(stock_row.turnover_rate, 18.88)
        self.assertEqual(event_row.payload_json, {"note": "updated without commit"})


if __name__ == "__main__":
    unittest.main()
