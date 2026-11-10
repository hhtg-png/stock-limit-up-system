import asyncio
import tempfile
import threading
import time
import unittest
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch

from app.services.limit_up_context_service import LimitUpContextService
from app.services.opening_auction_archive_service import OpeningAuctionArchiveService


DAY = date(2026, 10, 12)
PREVIOUS = date(2026, 10, 9)
TODAY = date(2026, 11, 10)


async def empty_pool(day):
    return []


class LimitUpContextPersistenceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.archive = OpeningAuctionArchiveService(root_dir=self.root, today_provider=lambda: TODAY)

    def service(self, history, **overrides):
        options = dict(
            calendar_loader=lambda start, end: [PREVIOUS, DAY],
            session_factory=None,
            archive_service=self.archive,
            history_fetcher=history,
            pool_fetcher=empty_pool,
            today_provider=lambda: TODAY,
            now_provider=lambda: datetime(2026, 11, 10, 11, 0),
        )
        options.update(overrides)
        return LimitUpContextService(**options)

    async def test_fetched_historical_zero_and_negative_openings_survive_restart_without_network(self):
        async def history(code, start, end):
            return {
                PREVIOUS: {"open": 10, "pre_close": 10},
                DAY: {"open": 9.75, "pre_close": 10},
            }

        rows = [{"stock_code": "605136"}]
        initial = await self.service(history).enrich(DAY, rows)
        self.assertEqual(initial[0]["yesterday_open_change_pct"], 0)
        self.assertEqual(initial[0]["today_open_change_pct"], -2.5)
        self.assertEqual(await self.archive.get_percentages(PREVIOUS), {"605136": 0.0})
        self.assertEqual(await self.archive.get_percentages(DAY), {"605136": -2.5})

        async def unavailable_history(code, start, end):
            self.fail("A restarted service must use its persisted historical openings")

        restarted_archive = OpeningAuctionArchiveService(root_dir=self.root, today_provider=lambda: TODAY)
        restarted = self.service(unavailable_history, archive_service=restarted_archive)
        refreshed = await restarted.enrich(DAY, rows)
        self.assertEqual(refreshed[0]["yesterday_open_change_pct"], 0)
        self.assertEqual(refreshed[0]["today_open_change_pct"], -2.5)

    async def test_missing_indicative_and_future_openings_are_not_persisted(self):
        async def history(code, start, end):
            return {
                PREVIOUS: {"open": 0, "pre_close": 10},
                DAY: {"open": 11, "pre_close": 10},
                date(2026, 10, 13): {"open": 11, "pre_close": 10},
            }

        before_auction = self.service(
            history, today_provider=lambda: DAY,
            now_provider=lambda: datetime(2026, 10, 12, 9, 24, 59),
        )
        result = await before_auction.enrich(DAY, [{"stock_code": "605136"}])
        self.assertIsNone(result[0]["today_open_change_pct"])
        self.assertIsNone(result[0]["yesterday_open_change_pct"])
        self.assertEqual(list(self.root.iterdir()), [])

        future = self.service(
            history, today_provider=lambda: PREVIOUS,
            now_provider=lambda: datetime(2026, 10, 9, 15, 30),
        )
        result = await future.enrich(DAY, [{"stock_code": "605136"}])
        self.assertIsNone(result[0]["today_open_change_pct"])
        self.assertEqual(list(self.root.iterdir()), [])

    async def test_finalized_openings_for_multiple_stocks_merge_without_exceeding_fetch_concurrency(self):
        active, maximum = 0, 0

        async def history(code, start, end):
            nonlocal active, maximum
            active += 1
            maximum = max(maximum, active)
            try:
                await asyncio.sleep(0.01)
                return {
                    PREVIOUS: {"open": 10, "pre_close": 10},
                    DAY: {"open": 11, "pre_close": 10},
                }
            finally:
                active -= 1

        codes = [f"{code:06}" for code in range(1, 16)]
        service = self.service(
            history, today_provider=lambda: DAY,
            now_provider=lambda: datetime(2026, 10, 12, 9, 25),
        )
        rows = await service.enrich(DAY, [{"stock_code": code} for code in codes])
        self.assertTrue(all(row["today_open_change_pct"] == 10 for row in rows))
        self.assertEqual(await self.archive.get_percentages(PREVIOUS), {code: 0.0 for code in codes})
        self.assertEqual(await self.archive.get_percentages(DAY), {code: 10.0 for code in codes})
        self.assertGreater(maximum, 1)
        self.assertLessEqual(maximum, 6)
        self.assertEqual(active, 0)

    async def test_save_failure_keeps_all_fetched_openings_in_memory(self):
        async def history(code, start, end):
            return {
                PREVIOUS: {"open": 10.5, "pre_close": 10},
                DAY: {"open": 9.75, "pre_close": 10},
            }

        def fail_save(*args, **kwargs):
            raise OSError("disk full")

        with patch.object(self.archive, "_save", side_effect=fail_save):
            result = await self.service(history).enrich(DAY, [{"stock_code": "605136"}])
        self.assertEqual(result[0]["yesterday_open_change_pct"], 5)
        self.assertEqual(result[0]["today_open_change_pct"], -2.5)
        self.assertEqual(list(self.root.iterdir()), [])

    async def test_archive_import_during_history_request_wins_in_memory_and_on_disk(self):
        entered, release = asyncio.Event(), asyncio.Event()

        async def history(code, start, end):
            entered.set()
            await release.wait()
            return {
                PREVIOUS: {"open": 11, "pre_close": 10},
                DAY: {"open": 11, "pre_close": 10},
            }

        service = self.service(history)
        pending = asyncio.create_task(service.enrich(DAY, [{"stock_code": "605136"}]))
        await asyncio.wait_for(entered.wait(), 0.5)
        await self.archive.save_percentages(PREVIOUS, {"605136": 0.0})
        await self.archive.save_percentages(DAY, {"605136": -2.5})
        release.set()
        result = await pending
        self.assertEqual(result[0]["yesterday_open_change_pct"], 0)
        self.assertEqual(result[0]["today_open_change_pct"], -2.5)
        self.assertEqual(await self.archive.get_percentages(PREVIOUS), {"605136": 0.0})
        self.assertEqual(await self.archive.get_percentages(DAY), {"605136": -2.5})

    async def test_live_quote_during_history_request_keeps_priority_when_history_finishes(self):
        entered, release = asyncio.Event(), asyncio.Event()

        async def history(code, start, end):
            entered.set()
            await release.wait()
            return {
                PREVIOUS: {"open": 11, "pre_close": 10},
                DAY: {"open": 11, "pre_close": 10},
            }

        # A captured previous session keeps the second request out of the held provider.
        await self.archive.save_percentages(PREVIOUS, {"605136": 0.0})
        service = self.service(
            history, today_provider=lambda: DAY,
            now_provider=lambda: datetime(2026, 10, 12, 10, 0),
        )
        pending = asyncio.create_task(service.enrich(DAY, [{"stock_code": "605136"}]))
        await asyncio.wait_for(entered.wait(), 0.5)
        quote = {"datetime": "20261012100000", "open": 9.75, "pre_close": 10}
        fresh = await service.enrich(DAY, [{"stock_code": "605136"}], {"605136": quote})
        self.assertEqual(fresh[0]["today_open_change_pct"], -2.5)
        release.set()
        result = await pending
        self.assertEqual(result[0]["today_open_change_pct"], -2.5)
        # The test archive intentionally treats DAY as historical, so record_quotes is inert.
        self.assertEqual(await self.archive.get_percentages(DAY), {})

    async def test_archive_import_between_provider_read_and_locked_save_is_preserved(self):
        entered, release = threading.Event(), threading.Event()
        original_save = self.archive._save

        async def history(code, start, end):
            return {
                PREVIOUS: {"open": 11, "pre_close": 10},
                DAY: {"open": 11, "pre_close": 10},
            }

        def held_save(day, values, *args, **kwargs):
            entered.set()
            if not release.wait(2):
                raise RuntimeError("test did not release provider save")
            return original_save(day, values, *args, **kwargs)

        importer = OpeningAuctionArchiveService(root_dir=self.root, today_provider=lambda: TODAY)
        with patch.object(self.archive, "_save", side_effect=held_save):
            pending = asyncio.create_task(self.service(history).enrich(DAY, [{"stock_code": "605136"}]))
            try:
                self.assertTrue(await asyncio.to_thread(entered.wait, 1))
                await importer.save_percentages(PREVIOUS, {"605136": 0.0})
                await importer.save_percentages(DAY, {"605136": -2.5})
            finally:
                release.set()
            result = await pending
        self.assertEqual(result[0]["yesterday_open_change_pct"], 0)
        self.assertEqual(result[0]["today_open_change_pct"], -2.5)
        self.assertEqual(await importer.get_percentages(PREVIOUS), {"605136": 0.0})
        self.assertEqual(await importer.get_percentages(DAY), {"605136": -2.5})

    async def test_slow_persistence_stays_inside_live_budget_without_losing_fetched_values(self):
        entered = asyncio.Event()

        async def history(code, start, end):
            return {
                PREVIOUS: {"open": 10, "pre_close": 10},
                DAY: {"open": 9.75, "pre_close": 10},
            }

        async def slow_save(day, values, **kwargs):
            entered.set()
            await asyncio.sleep(1)

        with patch.object(self.archive, "save_percentages", side_effect=slow_save):
            started = time.monotonic()
            result = await self.service(history, budget_seconds=0.05).enrich(DAY, [{"stock_code": "605136"}])
        self.assertLess(time.monotonic() - started, 0.3)
        self.assertTrue(entered.is_set())
        self.assertEqual(result[0]["yesterday_open_change_pct"], 0)
        self.assertEqual(result[0]["today_open_change_pct"], -2.5)


if __name__ == "__main__":
    unittest.main()
