import unittest
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from app.data_collectors.tencent_api import tencent_api
from app.utils.time_utils import CN_TZ


class AsyncSessionContext:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class ScalarRows:
    def __init__(self, rows):
        self.rows = rows

    def scalars(self):
        return self

    def all(self):
        return self.rows


class TradingPlaybookProductionCompositionTests(unittest.IsolatedAsyncioTestCase):
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
        self.assertTrue(
            callable(orchestrator.market_data.realtime_limit_up_loader)
        )
        self.assertTrue(
            callable(orchestrator.market_data.full_market_context_loader)
        )

    async def test_kline_adapter_reuses_existing_fetcher_without_changing_contract(self):
        from app.services.trading_playbook.composition import (
            load_production_kline,
        )

        points = [{"date": date(2026, 7, 13), "close": 10.5}]
        with patch(
            "app.api.v1.market._fetch_kline_from_em",
            AsyncMock(return_value=points),
        ) as fetch:
            result = await load_production_kline(
                "000001",
                "SZ",
                "day",
                60,
                stock_name="平安银行",
            )

        self.assertEqual(result, points)
        fetch.assert_awaited_once_with(
            "000001",
            "SZ",
            "day",
            60,
            stock_name="平安银行",
        )

    async def test_full_market_adapter_returns_only_point_in_time_db_facts(self):
        from app.services.trading_playbook.composition import (
            load_production_full_market_context,
        )

        previous = SimpleNamespace(
            trade_date=date(2026, 7, 10),
            limit_up_count=43,
            limit_down_count=2,
            max_board_height=4,
            seal_rate=68.0,
            source_status="primary",
            updated_at=datetime(2026, 7, 10, 15, 8),
        )
        current = SimpleNamespace(
            trade_date=date(2026, 7, 13),
            limit_up_count=55,
            limit_down_count=1,
            max_board_height=5,
            seal_rate=75.0,
            source_status="primary",
            updated_at=datetime(2026, 7, 13, 15, 7),
        )
        db = SimpleNamespace(execute=AsyncMock(return_value=ScalarRows([current, previous])))
        as_of = datetime(2026, 7, 13, 15, 30, tzinfo=CN_TZ)

        result = await load_production_full_market_context(
            date(2026, 7, 13),
            "after_close",
            as_of,
            session_factory=lambda: AsyncSessionContext(db),
        )

        self.assertEqual(result["scope"], "full_market")
        self.assertEqual(result["trade_date"], date(2026, 7, 13))
        self.assertEqual(result["limit_up_count"], 55)
        self.assertEqual(result["limit_up_count_prev"], 43)
        self.assertEqual(result["limit_down_count"], 1)
        self.assertEqual(result["max_board_height"], 5)
        self.assertEqual(result["seal_rate"], 75.0)
        self.assertEqual(
            result["field_quality"]["limit_up_count"],
            "ready",
        )
        self.assertEqual(result["as_of"].tzinfo, CN_TZ)
        db.execute.assert_awaited_once()

    async def test_full_market_adapter_fails_closed_when_source_date_is_missing(self):
        from app.services.trading_playbook.composition import (
            load_production_full_market_context,
        )

        db = SimpleNamespace(execute=AsyncMock(return_value=ScalarRows([])))
        with self.assertRaisesRegex(LookupError, "market review"):
            await load_production_full_market_context(
                date(2026, 7, 13),
                "after_close",
                datetime(2026, 7, 13, 15, 30, tzinfo=CN_TZ),
                session_factory=lambda: AsyncSessionContext(db),
            )


if __name__ == "__main__":
    unittest.main()
