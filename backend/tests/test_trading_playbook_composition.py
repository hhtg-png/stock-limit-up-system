import unittest
from datetime import date, datetime
from unittest.mock import AsyncMock, patch

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.data_collectors.tencent_api import tencent_api
from app.models.market_review import DailyAnalysisRecord, MarketReviewDailyMetric
from app.models.trading_playbook import TradingPlanVersion
from app.utils.time_utils import CN_TZ


class TradingPlaybookProductionCompositionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        self.Session = async_sessionmaker(self.engine, expire_on_commit=False)
        async with self.engine.begin() as connection:
            await connection.run_sync(MarketReviewDailyMetric.__table__.create)
            await connection.run_sync(DailyAnalysisRecord.__table__.create)
            await connection.run_sync(TradingPlanVersion.__table__.create)

    async def asyncTearDown(self):
        await self.engine.dispose()

    @staticmethod
    def _metric(trade_date, *, status="primary", **overrides):
        values = {
            "limit_up_count": 55,
            "limit_down_count": 1,
            "max_board_height": 5,
            "seal_rate": 75.0,
            "up_count_ex_st": 3000,
            "down_count_ex_st": 1000,
            "source_status": status,
            "created_at": datetime.combine(trade_date, datetime.min.time()).replace(
                hour=15, minute=7
            ),
            "updated_at": datetime.combine(trade_date, datetime.min.time()).replace(
                hour=15, minute=7
            ),
        }
        values.update(overrides)
        return MarketReviewDailyMetric(trade_date=trade_date, **values)

    async def _seed_complete_persisted_context(self):
        async with self.Session() as db:
            db.add_all(
                [
                    self._metric(date(2026, 7, 13)),
                    self._metric(
                        date(2026, 7, 10),
                        limit_up_count=43,
                        limit_down_count=3,
                        up_count_ex_st=900,
                        down_count_ex_st=2100,
                    ),
                    DailyAnalysisRecord(
                        trade_date=date(2026, 7, 13),
                        month="2026-07",
                        auto_result={"负反馈": {"items": []}},
                        data_status="ready",
                        generated_at=datetime(2026, 7, 13, 15, 8),
                    ),
                    TradingPlanVersion(
                        source_trade_date=date(2026, 7, 10),
                        target_trade_date=date(2026, 7, 13),
                        stage="after_close",
                        version_no=1,
                        market_state_json={
                            "window": "first_divergence",
                            "divergence_days": 1,
                        },
                        input_hash="prior",
                        generated_at=datetime(2026, 7, 10, 15, 30),
                    ),
                ]
            )
            await db.commit()

    def test_factory_reuses_real_quote_and_installs_all_loader_adapters(self):
        from app.services.trading_playbook.composition import (
            build_production_trading_playbook_orchestrator,
        )

        next_day = date(2026, 7, 14)
        resolver = lambda _value: next_day
        orchestrator = build_production_trading_playbook_orchestrator(
            next_trade_date=resolver,
        )

        self.assertIs(orchestrator.market_data.quote_api, tencent_api)
        self.assertIs(orchestrator.next_trade_date, resolver)
        self.assertTrue(callable(orchestrator.market_data.kline_loader))
        self.assertTrue(callable(orchestrator.market_data.realtime_limit_up_loader))
        self.assertTrue(callable(orchestrator.market_data.full_market_context_loader))

    async def test_kline_adapter_reuses_existing_fetcher_without_changing_contract(self):
        from app.services.trading_playbook.composition import load_production_kline

        points = [{"date": date(2026, 7, 13), "close": 10.5}]
        with patch(
            "app.api.v1.market._fetch_kline_from_em",
            AsyncMock(return_value=points),
        ) as fetch:
            result = await load_production_kline(
                "000001", "SZ", "day", 60, stock_name="平安银行"
            )

        self.assertEqual(result, points)
        fetch.assert_awaited_once_with(
            "000001", "SZ", "day", 60, stock_name="平安银行"
        )

    async def test_adapter_derives_point_in_time_fields_from_real_orm_rows(self):
        from app.services.trading_playbook.composition import (
            load_production_full_market_context,
        )

        await self._seed_complete_persisted_context()
        result = await load_production_full_market_context(
            date(2026, 7, 13),
            "after_close",
            CN_TZ.localize(datetime(2026, 7, 13, 15, 30)),
            session_factory=self.Session,
        )

        self.assertEqual(result["scope"], "full_market")
        self.assertEqual(result["evidence_trade_date"], date(2026, 7, 13))
        self.assertEqual(result["limit_up_count"], 55)
        self.assertEqual(result["limit_up_count_prev"], 43)
        self.assertTrue(result["sell_pressure_falling"])
        self.assertFalse(result["sell_pressure_rising"])
        self.assertTrue(result["breadth_recovered"])
        self.assertFalse(result["negative_feedback"])
        self.assertEqual(result["prior_window"], "first_divergence")
        self.assertEqual(result["divergence_days"], 2)
        self.assertEqual(result["field_quality"]["trend_new_high_count"], "missing")
        self.assertNotIn("trend_new_high_count", result)
        self.assertEqual(result["quality"], "degraded")

    async def test_partial_metric_never_promotes_default_zeroes_to_ready(self):
        from app.services.trading_playbook.composition import (
            load_production_full_market_context,
        )

        async with self.Session() as db:
            db.add(self._metric(date(2026, 7, 13), status="partial"))
            await db.commit()

        result = await load_production_full_market_context(
            date(2026, 7, 13),
            "after_close",
            CN_TZ.localize(datetime(2026, 7, 13, 15, 30)),
            session_factory=self.Session,
        )

        self.assertEqual(result["quality"], "degraded")
        self.assertEqual(result["field_quality"]["limit_up_count"], "missing")
        self.assertNotIn("limit_up_count", result)
        self.assertEqual(result["prior_window"], "")
        self.assertEqual(result["divergence_days"], 0)

    async def test_partial_previous_metric_cannot_supply_previous_comparisons(self):
        from app.services.trading_playbook.composition import (
            load_production_full_market_context,
        )

        async with self.Session() as db:
            db.add_all(
                [
                    self._metric(date(2026, 7, 13)),
                    self._metric(date(2026, 7, 10), status="partial"),
                ]
            )
            await db.commit()

        result = await load_production_full_market_context(
            date(2026, 7, 13),
            "after_close",
            CN_TZ.localize(datetime(2026, 7, 13, 15, 30)),
            session_factory=self.Session,
        )

        for key in (
            "limit_up_count_prev",
            "sell_pressure_falling",
            "sell_pressure_rising",
            "breadth_recovered",
        ):
            self.assertEqual(result["field_quality"][key], "missing")
            self.assertNotIn(key, result)

    async def test_overnight_uses_latest_prior_trading_evidence(self):
        from app.services.trading_playbook.composition import (
            load_production_full_market_context,
        )

        async with self.Session() as db:
            db.add_all(
                [
                    self._metric(date(2026, 7, 10), limit_up_count=43),
                    self._metric(date(2026, 7, 9), limit_up_count=31),
                ]
            )
            await db.commit()

        result = await load_production_full_market_context(
            date(2026, 7, 13),
            "overnight",
            CN_TZ.localize(datetime(2026, 7, 13, 8, 50)),
            session_factory=self.Session,
        )

        self.assertEqual(result["trade_date"], date(2026, 7, 13))
        self.assertEqual(result["evidence_trade_date"], date(2026, 7, 10))
        self.assertEqual(result["limit_up_count"], 43)
        self.assertEqual(result["limit_up_count_prev"], 31)

    async def test_missing_source_stays_explicitly_degraded_without_fake_values(self):
        from app.services.trading_playbook.composition import (
            load_production_full_market_context,
        )
        from app.services.trading_playbook.context_service import (
            FULL_MARKET_CONTEXT_FIELDS,
        )

        result = await load_production_full_market_context(
            date(2026, 7, 13),
            "after_close",
            CN_TZ.localize(datetime(2026, 7, 13, 15, 30)),
            session_factory=self.Session,
        )

        self.assertEqual(result["quality"], "degraded")
        self.assertEqual(result["field_quality"]["limit_up_count"], "missing")
        for key in FULL_MARKET_CONTEXT_FIELDS:
            if key not in {"prior_window", "divergence_days"}:
                self.assertNotIn(key, result)


if __name__ == "__main__":
    unittest.main()
