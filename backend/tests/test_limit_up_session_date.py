import threading
import unittest
from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from app.api.v1 import limit_up
from app.services.limit_up_context_service import limit_up_context_service
from app.services.trading_playbook.calendar_service import TradingCalendarService
from app.utils import trade_date
from test_limit_up_detail_api import (
    FakeAllResult, FakeScalarResult, SequencedSession, make_record, make_stock,
)


FRIDAY = date(2026, 10, 23)
SUNDAY = date(2026, 10, 25)
MONDAY = date(2026, 10, 26)
CONTEXT = {"yesterday_first_limit_up_time": "09:31:02", "yesterday_final_seal_time": "14:22:31",
           "yesterday_open_change_pct": 0.0, "today_open_change_pct": -1.25}


class AShareTradingDayTests(unittest.IsolatedAsyncioTestCase):
    def helper(self):
        helper = getattr(trade_date, "is_a_share_trading_day", None)
        self.assertIsNotNone(helper, "Shared A-share session predicate is missing")
        return helper

    async def test_weekend_returns_false_without_loading_calendar(self):
        calendar = SimpleNamespace(ensure_date=AsyncMock(side_effect=AssertionError("weekend needs no calendar I/O")))
        with patch.object(trade_date, "_a_share_calendar", calendar, create=True):
            self.assertFalse(await self.helper()(SUNDAY))
        calendar.ensure_date.assert_not_awaited()

    async def test_weekday_calendar_is_authoritative_cached_and_loaded_off_loop(self):
        threads = []

        def loader(start, end):
            threads.append(threading.get_ident())
            return [FRIDAY, MONDAY]

        calendar = TradingCalendarService(loader=loader, today_provider=lambda: FRIDAY)
        with patch.object(trade_date, "_a_share_calendar", calendar, create=True):
            self.assertTrue(await self.helper()(FRIDAY))
            self.assertTrue(await self.helper()(FRIDAY))
        self.assertEqual(len(threads), 1)
        self.assertNotEqual(threads[0], threading.get_ident())

    async def test_weekday_holiday_returns_false_even_though_not_weekend(self):
        holiday = date(2026, 10, 1)
        calendar = TradingCalendarService(loader=lambda start, end: [date(2026, 10, 9)], today_provider=lambda: holiday)
        with patch.object(trade_date, "_a_share_calendar", calendar, create=True):
            self.assertFalse(await self.helper()(holiday))

    async def test_failed_calendar_is_conservatively_closed_and_does_not_retry_each_call(self):
        calls = []

        def failed(start, end):
            calls.append((start, end))
            raise RuntimeError("calendar source unavailable")

        calendar = TradingCalendarService(loader=failed, today_provider=lambda: MONDAY)
        with patch.object(trade_date, "_a_share_calendar", calendar, create=True):
            self.assertFalse(await self.helper()(MONDAY))
            self.assertFalse(await self.helper()(MONDAY))
        self.assertEqual(len(calls), 1)


class LimitUpSessionDateTests(unittest.IsolatedAsyncioTestCase):
    async def database_fallback(self, requested, actual, *, calendar=None):
        stock, record = make_stock(), make_record(actual)
        db = SequencedSession([FakeScalarResult(0), FakeScalarResult(actual),
                               FakeAllResult([(record, stock)]), FakeAllResult([(stock.id, True, 2)])])
        raw = dict(stock_code=stock.stock_code, stock_name=stock.stock_name, limit_up_price=42.5)

        async def enriched(day, rows, quotes=None):
            self.assertEqual(day, requested)
            self.assertEqual(rows[0]["trade_date"], actual)
            return [{**row, **CONTEXT} for row in rows]

        calendar = calendar or TradingCalendarService(loader=lambda start, end: [], today_provider=lambda: requested)
        with patch.object(trade_date, "_a_share_calendar", calendar, create=True), \
             patch.object(limit_up, "today_cn", return_value=requested), \
             patch.object(limit_up.realtime_limit_up_service, "get_realtime_limit_up_list", AsyncMock(return_value=[raw])) as live, \
             patch.object(limit_up_context_service, "enrich", AsyncMock(side_effect=enriched)) as enrich:
            result = await limit_up.get_realtime_limit_up(
                None, continuous_days=None, reason_category=None, sort_by="time", db=db)
        self.assertEqual(result.trade_date, actual)
        self.assertTrue(result.is_fallback)
        self.assertEqual(result.data[0].trade_date, actual)
        self.assertEqual({key: getattr(result.data[0], key) for key in CONTEXT}, CONTEXT)
        live.assert_not_awaited()
        enrich.assert_awaited_once()

    async def test_sunday_uses_friday_rows_with_sunday_comparison_anchor(self):
        await self.database_fallback(SUNDAY, FRIDAY)

    async def test_weekday_holiday_uses_last_database_session_without_live_pool(self):
        holiday, actual = date(2026, 10, 1), date(2026, 9, 30)
        calendar = TradingCalendarService(loader=lambda start, end: [date(2026, 10, 9)], today_provider=lambda: holiday)
        await self.database_fallback(holiday, actual, calendar=calendar)

    async def test_calendar_failure_uses_existing_database_fallback(self):
        def failed(start, end):
            raise RuntimeError("calendar source unavailable")

        calendar = TradingCalendarService(loader=failed, today_provider=lambda: MONDAY)
        await self.database_fallback(MONDAY, FRIDAY, calendar=calendar)

    async def test_verified_current_trading_day_keeps_live_pool(self):
        calendar = TradingCalendarService(loader=lambda start, end: [FRIDAY, MONDAY], today_provider=lambda: FRIDAY)
        raw = dict(stock_code="000001", stock_name="Alpha", limit_up_price=11, **CONTEXT)
        with patch.object(trade_date, "_a_share_calendar", calendar, create=True), \
             patch.object(limit_up, "today_cn", return_value=FRIDAY), \
             patch.object(limit_up.realtime_limit_up_service, "get_realtime_limit_up_list", AsyncMock(return_value=[raw])) as live, \
             patch.object(limit_up, "_get_database_limit_up_response", AsyncMock(side_effect=AssertionError("verified live day should stay live"))) as database:
            result = await limit_up.get_realtime_limit_up(
                None, continuous_days=None, reason_category=None, sort_by="time", db=None)
        self.assertEqual(result.trade_date, FRIDAY)
        self.assertFalse(result.is_fallback)
        self.assertEqual(result.data[0].today_open_change_pct, -1.25)
        live.assert_awaited_once_with(FRIDAY, include_monitor_context=True)
        database.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
