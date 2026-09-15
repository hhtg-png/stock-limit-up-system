import asyncio
import unittest
from datetime import date
from unittest.mock import AsyncMock, patch

from app.services.market_review_source_service import MarketReviewSourceService


class LiveMarketStatsTests(unittest.IsolatedAsyncioTestCase):
    async def test_current_stats_recover_when_sina_is_broken(self):
        service = MarketReviewSourceService(current_date_provider=lambda: date(2026, 9, 15))
        expected = dict(market_turnover=100, up_count_ex_st=10, down_count_ex_st=20, limit_down_count=0)
        with patch.object(service, '_load_daily_statistics', AsyncMock(return_value={'limit_down_count': 9})), \
             patch.object(service, '_fetch_live_market_stats_sync', side_effect=ValueError('HTML response')), \
             patch.object(service, '_fetch_tencent_market_stats', AsyncMock(return_value=expected), create=True):
            self.assertEqual(await service._fetch_market_stats(date(2026, 9, 15)), expected)

    async def test_full_universe_aggregation_and_refresh(self):
        from app.services.live_market_stats_service import LiveMarketStatsService
        now = [0.0]
        def quote(name, change, amount, price=10, limit_down=9):
            return dict(name=name, change_pct=change, amount=amount, price=price,
                        limit_down=limit_down, datetime='20260915110000')
        quotes = {'600001': quote('Alpha', 2, 10000),
                  '300001': quote('Beta', -20, 20000, 8, 8),
                  '000001': quote('ST Gamma', -5, 30000, 9.5, 9.5)}
        fetch = AsyncMock(side_effect=lambda codes: {c: quotes[c] for c in codes})
        universe = AsyncMock(return_value=[*quotes, '920001', '510300'])
        service = LiveMarketStatsService(universe_loader=universe, quote_fetcher=fetch, clock=lambda: now[0])
        result, duplicate = await asyncio.gather(service.get_stats(date(2026, 9, 15)), service.get_stats(date(2026, 9, 15)))
        self.assertEqual(result, dict(market_turnover=6, up_count_ex_st=1, down_count_ex_st=1, limit_down_count=2))
        self.assertEqual(result, duplicate)
        self.assertEqual(fetch.await_count, 1)
        quotes['600001']['amount'] = 20000
        now[0] = 31
        self.assertEqual((await service.get_stats(date(2026, 9, 15)))['market_turnover'], 7)
        self.assertEqual(fetch.await_count, 2)
        self.assertEqual(universe.await_count, 1)

    async def test_partial_or_yesterday_quotes_are_retried_not_counted_as_zero(self):
        from app.services.live_market_stats_service import LiveMarketStatsService
        fetch = AsyncMock(return_value={'600001': dict(datetime='20260914150000')})
        service = LiveMarketStatsService(universe_loader=AsyncMock(return_value=['600001']), quote_fetcher=fetch)
        with self.assertRaises(ValueError):
            await service.get_stats(date(2026, 9, 15))
        self.assertEqual(fetch.await_count, 2)

    async def test_cancelled_refresh_releases_lock_for_next_request(self):
        from app.services.live_market_stats_service import LiveMarketStatsService
        started = asyncio.Event()
        async def slow_fetch(codes):
            started.set()
            await asyncio.Event().wait()
        service = LiveMarketStatsService(universe_loader=AsyncMock(return_value=['600001']), quote_fetcher=slow_fetch)
        task = asyncio.create_task(service.get_stats(date(2026, 9, 15)))
        await started.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        service.quote_fetcher = AsyncMock(return_value={'600001': dict(
            name='Alpha', datetime='20260915110000', amount=10000,
            change_pct=1, price=10, limit_down=9)})
        result = await asyncio.wait_for(service.get_stats(date(2026, 9, 15)), timeout=1)
        self.assertEqual(result['market_turnover'], 1)

    async def test_primary_timeout_uses_legacy_source(self):
        service = MarketReviewSourceService(current_date_provider=lambda: date(2026, 9, 15))
        expected = dict(market_turnover=100, up_count_ex_st=10, down_count_ex_st=20, limit_down_count=1)
        with patch.object(service, '_load_daily_statistics', AsyncMock(return_value={})), \
             patch.object(service, '_fetch_tencent_market_stats', AsyncMock(side_effect=TimeoutError)), \
             patch.object(service, '_fetch_live_market_stats_sync', return_value=expected):
            self.assertEqual(await service._fetch_market_stats(date(2026, 9, 15)), expected)
