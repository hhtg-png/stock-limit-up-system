import asyncio
import time
import unittest
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from app.data_collectors.scheduler import TradingCalendarLookupError
from app.utils.time_utils import CN_TZ


class AsyncSessionContext:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class TradingPlaybookCalendarServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_scheduler_monitor_hot_path_reuses_one_range_refresh(self):
        from app.data_collectors.scheduler import DataScheduler
        from app.services.trading_playbook.calendar_service import (
            TradingCalendarService,
        )

        calls = 0

        def loader(start_date, end_date):
            nonlocal calls
            calls += 1
            return [date(2026, 7, 13), date(2026, 7, 14)]

        calendar = TradingCalendarService(
            loader=loader,
            today_provider=lambda: date(2026, 7, 13),
        )
        clock = [CN_TZ.localize(datetime(2026, 7, 13, 15, 35))]
        alert = SimpleNamespace(monitor=AsyncMock())
        scheduler = DataScheduler(
            trading_playbook_alert_service=alert,
            session_factory=lambda: AsyncSessionContext(MagicMock()),
            now_provider=lambda: clock[0],
            calendar_service=calendar,
        )
        scheduler._upgrade_forced_trading_playbook_after_close = AsyncMock()

        for second in (0, 1, 3):
            clock[0] = clock[0].replace(second=second)
            await scheduler._monitor_trading_playbook()

        self.assertEqual(calls, 1)
        self.assertEqual(alert.monitor.await_count, 3)
        self.assertEqual(
            scheduler._upgrade_forced_trading_playbook_after_close.await_count,
            3,
        )

    async def test_concurrent_refresh_is_singleflight_and_does_not_block_heartbeat(self):
        from app.services.trading_playbook.calendar_service import (
            TradingCalendarService,
        )

        calls = 0

        def blocking_loader(start_date, end_date):
            nonlocal calls
            calls += 1
            time.sleep(0.05)
            return [date(2026, 7, 13), date(2026, 7, 14)]

        ticks = 0

        async def heartbeat():
            nonlocal ticks
            deadline = asyncio.get_running_loop().time() + 0.04
            while asyncio.get_running_loop().time() < deadline:
                ticks += 1
                await asyncio.sleep(0.005)

        service = TradingCalendarService(
            loader=blocking_loader,
            today_provider=lambda: date(2026, 7, 13),
        )

        await asyncio.gather(
            *(service.ensure_date(date(2026, 7, 13)) for _ in range(5)),
            heartbeat(),
        )

        self.assertEqual(calls, 1)
        self.assertGreaterEqual(ticks, 2)
        self.assertTrue(service.is_trading_day(date(2026, 7, 13)))
        self.assertEqual(
            service.next_trade_date(date(2026, 7, 13)),
            date(2026, 7, 14),
        )

    async def test_refresh_failure_uses_last_good_and_throttles_retry(self):
        from app.services.trading_playbook.calendar_service import (
            TradingCalendarService,
        )

        calls = 0
        current_day = [date(2026, 7, 13)]
        clock = [0.0]

        def loader(start_date, end_date):
            nonlocal calls
            calls += 1
            if calls > 1:
                raise RuntimeError("calendar offline")
            return [date(2026, 7, 13), date(2026, 7, 14)]

        service = TradingCalendarService(
            loader=loader,
            retry_interval_seconds=60,
            today_provider=lambda: current_day[0],
            monotonic=lambda: clock[0],
        )
        await service.ensure_date(date(2026, 7, 13))
        current_day[0] = date(2026, 7, 14)

        await service.ensure_date(date(2026, 7, 13))
        await service.ensure_date(date(2026, 7, 13))

        self.assertEqual(calls, 2)
        self.assertTrue(service.is_trading_day(date(2026, 7, 13)))
        self.assertEqual(
            service.next_trade_date(date(2026, 7, 13)),
            date(2026, 7, 14),
        )

    async def test_no_cache_failure_raises_without_weekday_guess(self):
        from app.services.trading_playbook.calendar_service import (
            TradingCalendarService,
        )

        calls = 0

        def loader(start_date, end_date):
            nonlocal calls
            calls += 1
            raise RuntimeError("calendar offline")

        service = TradingCalendarService(
            loader=loader,
            retry_interval_seconds=60,
            today_provider=lambda: date(2026, 7, 13),
        )

        with self.assertRaises(TradingCalendarLookupError):
            await service.ensure_date(date(2026, 7, 13))
        with self.assertRaises(TradingCalendarLookupError):
            await service.ensure_date(date(2026, 7, 13))

        self.assertEqual(calls, 1)
        self.assertFalse(service.is_trading_day(date(2026, 7, 13)))

    async def test_empty_refresh_never_replaces_last_good_calendar(self):
        from app.services.trading_playbook.calendar_service import (
            TradingCalendarLookupError,
            TradingCalendarService,
        )

        calls = 0
        current_day = [date(2026, 7, 13)]

        def loader(_start, _end):
            nonlocal calls
            calls += 1
            if calls == 1:
                return [date(2026, 7, 13), date(2026, 7, 14)]
            return []

        service = TradingCalendarService(
            loader=loader,
            retry_interval_seconds=0,
            today_provider=lambda: current_day[0],
        )
        await service.ensure_date(date(2026, 7, 13))
        current_day[0] = date(2026, 7, 14)
        await service.ensure_date(date(2026, 7, 13))

        self.assertEqual(service.next_trade_date(date(2026, 7, 13)), date(2026, 7, 14))
        fresh = TradingCalendarService(
            loader=lambda _start, _end: [],
            today_provider=lambda: date(2026, 7, 13),
        )
        with self.assertRaises(TradingCalendarLookupError):
            await fresh.ensure_date(date(2026, 7, 13))

    async def test_timed_out_loader_is_singleflight_and_close_is_bounded(self):
        from app.services.trading_playbook.calendar_service import (
            TradingCalendarLookupError,
            TradingCalendarService,
        )

        calls = 0

        def loader(_start, _end):
            nonlocal calls
            calls += 1
            time.sleep(0.2)
            return [date(2026, 7, 13), date(2026, 7, 14)]

        service = TradingCalendarService(
            loader=loader,
            refresh_timeout_seconds=0.03,
            retry_interval_seconds=0,
            today_provider=lambda: date(2026, 7, 13),
        )
        for _ in range(2):
            with self.assertRaises(TradingCalendarLookupError):
                await service.ensure_date(date(2026, 7, 13))
        started = time.monotonic()
        await service.close()

        self.assertEqual(calls, 1)
        self.assertLess(time.monotonic() - started, 0.1)


if __name__ == "__main__":
    unittest.main()
