import unittest
from datetime import date, datetime, time, timedelta
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.database import Base
from app.models.market_review import (
    DailyAnalysisRecord,
    MarketReviewDailyMetric,
    MarketReviewStockDaily,
)
from app.models.stock import Stock
from app.models.trading_playbook import TradingRuleSource
from app.services.realtime_limit_up_service import RealtimeLimitUpSnapshot
from app.services.trading_playbook.composition import (
    build_production_trading_playbook_orchestrator,
)
from app.services.trading_playbook.rule_catalog import RuleCatalog
from app.utils.time_utils import CN_TZ


class FakeQuoteAPI:
    def __init__(self, payloads):
        self.payloads = payloads

    async def get_quotes_batch(self, codes):
        return {
            code: dict(self.payloads[code])
            for code in codes
            if code in self.payloads
        }


class TradingPlaybookProductionPipelineTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _cn(value):
        return CN_TZ.localize(value)

    async def asyncSetUp(self):
        self.engine = create_async_engine(
            "sqlite+aiosqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        self.Session = async_sessionmaker(self.engine, expire_on_commit=False)
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        catalog = RuleCatalog(
            Path(__file__).resolve().parents[1]
            / "app"
            / "data"
            / "trading_playbook_rules_v2.json"
        ).load()
        self.catalog_sources = {
            source["source_key"]: source for source in catalog["sources"]
        }
        await self._seed_real_facts()

    async def asyncTearDown(self):
        await self.engine.dispose()

    async def _seed_real_facts(self):
        async with self.Session() as db:
            stock_codes = (
                "300001",
                "000002",
                *(f"600{index:03d}" for index in range(50)),
            )
            stocks = [
                Stock(
                    stock_code=code,
                    stock_name=f"Mode {code}",
                    market="SZ",
                    is_st=0,
                    circulating_shares=100_000,
                )
                for code in stock_codes
            ]
            db.add_all(stocks)
            await db.flush()
            # Production reaches this state only after the verified rule import
            # command has checked transcript files and persisted their hashes.
            db.add_all(
                [
                    TradingRuleSource(
                        source_key=source["source_key"],
                        source_path=source["source_path"],
                        source_title=source["source_title"],
                        content_hash=source["content_hash"],
                        status="ready",
                    )
                    for source in self.catalog_sources.values()
                ]
            )
            db.add_all(
                [
                    MarketReviewDailyMetric(
                        trade_date=date(2026, 7, 9),
                        limit_up_count=10,
                        limit_down_count=3,
                        max_board_height=2,
                        seal_rate=70,
                        up_count_ex_st=1000,
                        down_count_ex_st=2000,
                        broken_amount=300,
                        source_status="primary",
                        created_at=datetime(2026, 7, 9, 15, 5),
                        updated_at=datetime(2026, 7, 9, 15, 5),
                    ),
                    MarketReviewDailyMetric(
                        trade_date=date(2026, 7, 10),
                        limit_up_count=20,
                        limit_down_count=1,
                        max_board_height=3,
                        seal_rate=80,
                        up_count_ex_st=2500,
                        down_count_ex_st=1000,
                        broken_amount=100,
                        source_status="primary",
                        created_at=datetime(2026, 7, 10, 15, 5),
                        updated_at=datetime(2026, 7, 10, 15, 5),
                    ),
                ]
            )
            for stock in stocks:
                db.add(
                    MarketReviewStockDaily(
                        trade_date=date(2026, 7, 9),
                        stock_id=stock.id,
                        stock_code=stock.stock_code,
                        stock_name=stock.stock_name,
                        today_touched_limit_up=False,
                        today_sealed_close=False,
                        today_continuous_days=0,
                        close_price=10,
                        pre_close=9.8,
                        change_pct=2,
                        amount=800,
                        turnover_rate=2,
                        tradable_market_value=1_000_000,
                        limit_up_reason="AI",
                        data_quality_flag="ok",
                        created_at=datetime(2026, 7, 9, 15, 5),
                        updated_at=datetime(2026, 7, 9, 15, 5),
                    )
                )
                db.add(
                    MarketReviewStockDaily(
                        trade_date=date(2026, 7, 10),
                        stock_id=stock.id,
                        stock_code=stock.stock_code,
                        stock_name=stock.stock_name,
                        today_touched_limit_up=True,
                        today_sealed_close=True,
                        today_continuous_days=(
                            2 if stock.stock_code == "300001" else 1
                        ),
                        first_limit_time=(
                            time(9, 31)
                            if stock.stock_code == "300001"
                            else time(9, 35)
                        ),
                        open_count=(
                            2 if stock.stock_code == "300001" else 1
                        ),
                        close_price=10.5,
                        pre_close=10,
                        change_pct=5,
                        amount=(
                            2000 if stock.stock_code == "300001" else 1000
                        ),
                        turnover_rate=3.2,
                        tradable_market_value=(
                            2_000_000
                            if stock.stock_code == "300001"
                            else 1_000_000
                        ),
                        limit_up_reason="AI",
                        data_quality_flag="ok",
                        created_at=datetime(2026, 7, 10, 15, 5),
                        updated_at=datetime(2026, 7, 10, 15, 5),
                    )
                )
            db.add(
                DailyAnalysisRecord(
                    trade_date=date(2026, 7, 10),
                    month="2026-07",
                    auto_result={"负反馈": {"items": []}},
                    manual_overrides={},
                    data_status="ready",
                    generated_at=datetime(2026, 7, 10, 15, 8),
                    created_at=datetime(2026, 7, 10, 15, 8),
                    updated_at=datetime(2026, 7, 10, 15, 8),
                )
            )
            await db.commit()

    async def _build_stage(self, stage):
        cases = {
            "preclose": (
                date(2026, 7, 10),
                self._cn(datetime(2026, 7, 10, 14, 40)),
                "20260710144000",
                date(2026, 7, 10),
            ),
            "after_close": (
                date(2026, 7, 10),
                self._cn(datetime(2026, 7, 10, 15, 30)),
                "20260710150000",
                date(2026, 7, 10),
            ),
            "overnight": (
                date(2026, 7, 13),
                self._cn(datetime(2026, 7, 13, 8, 50)),
                "20260710150000",
                date(2026, 7, 10),
            ),
            "auction": (
                date(2026, 7, 13),
                self._cn(datetime(2026, 7, 13, 9, 26)),
                "20260713092600",
                date(2026, 7, 13),
            ),
        }
        source_date, as_of, quote_time, evidence_date = cases[stage]
        payloads = {
            code: {
                "code": code,
                "name": f"Mode {code}",
                "price": "10.5" if code == "300001" else "10.2",
                "pre_close": "10",
                "open": "10.1",
                "amount": "2000" if code == "300001" else "1000",
                "turnover_rate": "3.2",
                "bid1_price": "10.5" if code == "300001" else "10.2",
                "bid1_volume": "88",
                "limit_up": "11",
                "datetime": quote_time,
            }
            for code in (
                "300001",
                "000002",
                *(f"600{index:03d}" for index in range(50)),
            )
        }

        async def realtime_loader(_trade_date):
            rows = [
                {
                    "stock_code": code,
                    "trade_date": evidence_date,
                    "theme_name": "AI",
                    "_collected_at": (
                        as_of - timedelta(minutes=1)
                        if evidence_date == source_date
                        else self._cn(datetime(2026, 7, 10, 15, 5))
                    ),
                    "first_limit_up_time": (
                        "09:31:00" if code == "300001" else "09:35:00"
                    ),
                    "continuous_limit_up_days": (
                        2 if code == "300001" else 1
                    ),
                    "seal_amount": 500 if code == "300001" else 300,
                    "is_final_sealed": True,
                    "open_count": 2 if code == "300001" else 1,
                    "float_market_value": (
                        2_000_000 if code == "300001" else 1_000_000
                    ),
                }
                for code in ("300001", "000002")
            ]
            rows.extend(
                {
                    "stock_code": f"600{index:03d}",
                    "trade_date": evidence_date,
                    "_collected_at": (
                        as_of - timedelta(minutes=1)
                        if evidence_date == source_date
                        else self._cn(datetime(2026, 7, 10, 15, 5))
                    ),
                    "continuous_limit_up_days": 1,
                    "is_final_sealed": True,
                }
                for index in range(18)
            )
            return RealtimeLimitUpSnapshot(
                items=rows,
                authoritative=True,
                complete=True,
                evidence_trade_date=evidence_date,
            )

        async def kline_loader(*_args, **_kwargs):
            return [
                {
                    "date": date(2026, 7, 3) + timedelta(days=offset),
                    "available_at": self._cn(
                        datetime(2026, 7, 3, 15, 0)
                    )
                    + timedelta(days=offset),
                    "close": close,
                }
                for offset, close in enumerate((8.0, 8.2, 8.4, 8.6, 8.8, 9.2))
            ]

        with patch(
            "app.services.trading_playbook.composition.tencent_api",
            FakeQuoteAPI(payloads),
        ), patch(
            "app.services.trading_playbook.composition.load_production_kline",
            kline_loader,
        ), patch(
            "app.services.trading_playbook.composition.load_production_realtime_limit_up",
            realtime_loader,
        ):
            orchestrator = build_production_trading_playbook_orchestrator(
                next_trade_date=lambda _value: date(2026, 7, 13),
                session_factory=self.Session,
            )
            async with self.Session() as db:
                return await orchestrator.build_stage(
                    db,
                    source_date,
                    stage,
                    as_of,
                )

    async def test_all_four_stages_use_real_evidence_for_actionable_plans(self):
        for stage in ("preclose", "after_close", "overnight", "auction"):
            with self.subTest(stage=stage):
                plan = await self._build_stage(stage)
                self.assertNotEqual(
                    plan["market_state_json"]["style"],
                    "unknown",
                )
                self.assertNotEqual(
                    plan["market_state_json"]["window"],
                    "unknown",
                )
                self.assertEqual(
                    plan["market_state_json"]["trend_evidence_source"],
                    "bounded_sample",
                )
                self.assertTrue(
                    any(
                        candidate["risk_level"] in {"trial", "confirmed"}
                        for candidate in plan["candidates"]
                    )
                )
                self.assertEqual(
                    {
                        ref["source_key"]: ref["source_content_hash"]
                        for ref in plan["risk_settings_json"]["source_refs"]
                    },
                    {
                        source_key: self.catalog_sources[source_key][
                            "content_hash"
                        ]
                        for source_key in ("03-loss-qa", "04-trading-plan")
                    },
                )

    async def test_missing_previous_facts_remain_degraded_without_candidates(self):
        async with self.Session() as db:
            await db.execute(
                delete(MarketReviewDailyMetric).where(
                    MarketReviewDailyMetric.trade_date == date(2026, 7, 9)
                )
            )
            await db.execute(
                delete(MarketReviewStockDaily).where(
                    MarketReviewStockDaily.trade_date == date(2026, 7, 9)
                )
            )
            await db.commit()

        plan = await self._build_stage("preclose")

        self.assertEqual(plan["data_quality_json"]["status"], "degraded")
        self.assertEqual(plan["market_state_json"]["style"], "unknown")
        self.assertEqual(plan["market_state_json"]["window"], "unknown")
        self.assertEqual(plan["candidates"], [])
