import asyncio
import unittest
from datetime import date
from unittest.mock import AsyncMock

from app.services.live_market_stats_service import LiveMarketStatsService


class SharedMarketSnapshotTests(unittest.IsolatedAsyncioTestCase):
    async def test_stats_and_groups_share_scan_and_do_not_leak_mutable_cache(self):
        fetch = AsyncMock(return_value={"600001": dict(name="测试", datetime="20261030110000",
            amount=10000, price=10, change_pct=1, limit_down=9)})
        service = LiveMarketStatsService(universe_loader=AsyncMock(return_value=["600001"]), quote_fetcher=fetch)
        snapshot, stats = await asyncio.gather(service.get_quotes_snapshot(date(2026, 10, 30)),
                                                service.get_stats(date(2026, 10, 30)))
        self.assertEqual(fetch.await_count, 1)
        self.assertEqual(stats["market_turnover"], 1)
        snapshot["quotes"]["600001"]["price"] = 99
        self.assertEqual((await service.get_quotes_snapshot(date(2026, 10, 30)))["quotes"]["600001"]["price"], 10)

    async def test_partial_snapshot_is_available_but_never_a_complete_market_stat(self):
        fetch = AsyncMock(return_value={"600001": dict(name="测试", datetime="20261030110000", price=10)})
        service = LiveMarketStatsService(universe_loader=AsyncMock(return_value=["600001", "000001"]), quote_fetcher=fetch)
        snapshot = await service.get_quotes_snapshot(date(2026, 10, 30))
        self.assertIn("600001", snapshot["quotes"])
        self.assertEqual(snapshot["missing_codes"], ["000001", "600001"])
        with self.assertRaises(ValueError):
            await service.get_stats(date(2026, 10, 30))
        self.assertEqual(fetch.await_count, 2)

    async def test_stalled_batches_are_bounded_and_completed_quotes_remain_usable(self):
        codes = [f"600{i:03}" for i in range(81)]
        async def fetch(chunk):
            if chunk[0] != codes[0]:
                await asyncio.Event().wait()
            return {code: dict(name="测试", datetime="20261030110000", amount=1,
                              price=10, change_pct=1, limit_down=9) for code in chunk}
        service = LiveMarketStatsService(universe_loader=AsyncMock(return_value=codes),
            quote_fetcher=fetch, scan_timeout=0.03)
        snapshot = await asyncio.wait_for(service.get_quotes_snapshot(date(2026, 10, 30)), 1)
        self.assertEqual(len(snapshot["quotes"]), 80)
        self.assertEqual(snapshot["missing_codes"], [codes[-1]])
