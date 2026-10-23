import asyncio
import importlib.util
import json
import os
import tempfile
import threading
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

if importlib.util.find_spec("app.services.opening_auction_archive_service"):
    from app.services.opening_auction_archive_service import OpeningAuctionArchiveService
else:
    OpeningAuctionArchiveService = None


DAY = date(2026, 10, 23)
PREVIOUS = date(2026, 10, 22)


class OpeningAuctionArchiveTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.assertIsNotNone(OpeningAuctionArchiveService, "opening auction archive is not implemented")
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)

    def service(self, **kwargs):
        return OpeningAuctionArchiveService(root_dir=self.root, today_provider=lambda: DAY, **kwargs)

    async def test_persistence_reloads_and_merges_fresh_file_without_losing_zero_or_other_days(self):
        service = self.service()
        await service.save_percentages(DAY, {"000001": 0, "000002": 3.25})
        await self.service().save_percentages(DAY, {"000003": -1.08})
        await service.save_percentages(DAY, {
            "000004": "2.5", "000005": float("nan"), "000006": float("inf"),
            "000007": None, "000008": True,
        })
        await service.save_percentages(PREVIOUS, {"000001": -5})
        reloaded = await self.service().get_percentages(DAY)
        self.assertEqual(reloaded, {"000001": 0.0, "000002": 3.25, "000003": -1.08, "000004": 2.5})
        reloaded.clear()
        self.assertEqual(len(await service.get_percentages(DAY)), 4)
        self.assertEqual(await service.get_percentages(PREVIOUS), {"000001": -5.0})
        self.assertEqual(json.loads((self.root / "2026-10-23.json").read_text()), {
            "000001": 0.0, "000002": 3.25, "000003": -1.08, "000004": 2.5,
        })

    async def test_quotes_only_archive_final_same_day_open_with_valid_reference_prices(self):
        service = self.service()
        valid = {"datetime": "20261023092500", "open": 10, "pre_close": 10}
        quotes = {
            "000001": valid,
            "000002": {**valid, "open": 11},
            "000003": {**valid, "datetime": "20261022150000"},
            "000004": {**valid, "datetime": "20261023092459"},
            "000005": {**valid, "_missing_fields": ["open"]},
            "000006": {**valid, "open": 0},
            "000007": {**valid, "pre_close": -1},
            "000008": {**valid, "pre_close": float("inf")},
            "000009": {**valid, "open": float("nan")},
            "000010": {**valid, "datetime": "20261023092599"},
            "000011": {**valid, "_missing_fields": ["pre_close"]},
        }
        await service.record_quotes(DAY, quotes)
        result = await service.get_percentages(DAY)
        self.assertEqual(set(result), {"000001", "000002"})
        self.assertEqual(result["000001"], 0)
        self.assertAlmostEqual(result["000002"], 10)
        await service.record_quotes(PREVIOUS, {"000001": {**valid, "datetime": "20261022092500"}})
        self.assertFalse((self.root / "2026-10-22.json").exists())

    async def test_atomic_worker_thread_writes_skip_unchanged_and_preserve_file_on_failure(self):
        service = self.service()
        main_thread = threading.get_ident()
        observed_threads = []
        replace = os.replace

        def observe_replace(source, destination):
            observed_threads.append(threading.get_ident())
            replace(source, destination)

        with patch("app.services.opening_auction_archive_service.os.replace", side_effect=observe_replace):
            await service.save_percentages(DAY, {"000001": 0})
            await service.save_percentages(DAY, {"000001": 0, "000002": float("nan")})
        self.assertEqual(len(observed_threads), 1)
        self.assertNotEqual(observed_threads[0], main_thread)
        with patch("app.services.opening_auction_archive_service.os.replace", side_effect=OSError("disk failure")):
            with self.assertRaises(OSError):
                await service.save_percentages(DAY, {"000002": 3.25})
        self.assertEqual(await service.get_percentages(DAY), {"000001": 0})
        self.assertEqual([path.name for path in self.root.iterdir()], ["2026-10-23.json"])

    async def test_full_capture_is_nonblocking_deduplicated_batched_and_tolerates_suspended_quotes(self):
        codes = [f"{index:06d}" for index in range(321)]
        release, four_active = asyncio.Event(), asyncio.Event()
        calls, universe_calls = [], []
        active, maximum = 0, 0

        async def universe():
            universe_calls.append(True)
            return codes

        async def fetch(chunk):
            nonlocal active, maximum
            calls.append(chunk)
            active += 1
            maximum = max(maximum, active)
            if active == 4:
                four_active.set()
            await release.wait()
            active -= 1
            return {code: {
                "datetime": "20261022150000" if code == codes[-1] else "20261023092500",
                "open": 10, "pre_close": 10,
            } for code in chunk}

        service = self.service(universe_loader=universe, quote_fetcher=fetch)
        task = service.request_full_capture(DAY)
        self.assertIsNotNone(task)
        service.request_full_capture(DAY)
        await asyncio.wait_for(four_active.wait(), 1)
        self.assertFalse(task.done(), "request should return while network requests remain in flight")
        self.assertEqual(len(calls), 4)
        release.set()
        await task
        self.assertEqual(maximum, 4)
        self.assertEqual(sorted(map(len, calls)), [1, 80, 80, 80, 80])
        self.assertEqual(len(await service.get_percentages(DAY)), 320)
        self.assertIsNone(service.request_full_capture(DAY))
        self.assertIsNone(service.request_full_capture(PREVIOUS))
        self.assertEqual(len(universe_calls), 1)

    async def test_cancelled_caller_leaves_worker_serialized_without_delaying_cancellation(self):
        service = self.service()
        entered, release = threading.Event(), threading.Event()
        original_read = service._read
        first_read = [True]

        def blocked_read(day):
            existing = original_read(day)
            if first_read[0]:
                first_read[0] = False
                entered.set()
                release.wait(2)
            return existing

        with patch.object(service, "_read", side_effect=blocked_read):
            first = asyncio.create_task(service.save_percentages(DAY, {"000001": 0}))
            self.assertTrue(await asyncio.to_thread(entered.wait, 1))
            first.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(first, 0.1)
            second = asyncio.create_task(self.service().save_percentages(DAY, {"000002": 3.25}))
            try:
                await asyncio.sleep(0.02)
                self.assertFalse(second.done(), "a cancelled caller must not release a running merge's lock")
            finally:
                release.set()
                await second
        self.assertEqual(await service.get_percentages(DAY), {"000001": 0, "000002": 3.25})

    async def test_missing_stock_in_nonempty_response_retries_instead_of_finishing_capture(self):
        ticks, calls = [100.0], []

        async def universe():
            return ["000001", "000002"]

        async def fetch(chunk):
            calls.append(chunk)
            return {code: {"datetime": "20261023092500", "open": 10, "pre_close": 10}
                    for code in (chunk[:1] if len(calls) == 1 else chunk)}

        service = self.service(universe_loader=universe, quote_fetcher=fetch, clock=lambda: ticks[0])
        await service.request_full_capture(DAY)
        self.assertEqual(await service.get_percentages(DAY), {"000001": 0})
        ticks[0] = 131
        retry = service.request_full_capture(DAY)
        self.assertIsNotNone(retry, "omitted stocks must keep a partial capture retryable")
        await retry
        self.assertEqual(await service.get_percentages(DAY), {"000001": 0, "000002": 0})

    async def test_partial_capture_persists_good_batches_and_retries_after_short_cooldown(self):
        ticks, failing = [100.0], [True]
        codes = [f"{index:06d}" for index in range(81)]
        calls = []

        async def universe():
            return codes

        async def fetch(chunk):
            calls.append(chunk)
            if len(chunk) == 1 and failing[0]:
                raise OSError("temporary quote failure")
            return {code: {"datetime": "20261023100000", "open": 10, "pre_close": 10} for code in chunk}

        service = self.service(universe_loader=universe, quote_fetcher=fetch, clock=lambda: ticks[0], retry_delay=30)
        await service.request_full_capture(DAY)
        self.assertEqual(len(await service.get_percentages(DAY)), 80)
        self.assertIsNone(service.request_full_capture(DAY))
        self.assertEqual(len(calls), 2)
        ticks[0], failing[0] = 131.0, False
        await service.request_full_capture(DAY)
        self.assertEqual(len(await self.service().get_percentages(DAY)), 81)
        self.assertIsNone(service.request_full_capture(DAY))


if __name__ == "__main__":
    unittest.main()
