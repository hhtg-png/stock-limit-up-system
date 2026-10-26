import time
import unittest
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import app.services.realtime_limit_up_service as realtime_module
from app.crawlers.eastmoney_crawler import EastMoneyCrawler
from app.services.realtime_limit_up_service import RealtimeLimitUpService


MONDAY = date(2026, 10, 26)
FRIDAY = date(2026, 10, 23)
THURSDAY = date(2026, 10, 22)


def payload(qdate, code="000001"):
    return {"data": {"qdate": qdate, "tc": 1, "pool": [
        {"c": code, "n": "Alpha", "fbt": 92500, "lbt": 92500, "p": 11000},
    ]}}


class EastMoneyCrawlerDateTests(unittest.IsolatedAsyncioTestCase):
    async def test_future_requested_pool_is_rejected_before_turnover_enrichment(self):
        for sealed_day, opened_day in [(20261023, "20261026"), (20261026, "20261023")]:
            with self.subTest(sealed_day=sealed_day, opened_day=opened_day):
                crawler = EastMoneyCrawler()
                crawler.fetch_json = AsyncMock(side_effect=[payload(sealed_day), payload(opened_day, "000002")])
                crawler._enrich_real_turnover = AsyncMock()
                rows = await crawler.crawl(MONDAY)
                self.assertEqual(rows, [])
                crawler._enrich_real_turnover.assert_not_awaited()

    async def test_historical_pool_before_latest_qdate_and_current_pool_both_remain_available(self):
        for requested, source_day in [(THURSDAY, 20261023), (MONDAY, "20261026")]:
            with self.subTest(requested=requested):
                crawler = EastMoneyCrawler()
                crawler.fetch_json = AsyncMock(side_effect=[payload(source_day), payload(source_day, "000002")])
                crawler._enrich_real_turnover = AsyncMock()
                rows = await crawler.crawl(requested)
                self.assertEqual([row["stock_code"] for row in rows], ["000001", "000002"])
                crawler._enrich_real_turnover.assert_awaited_once()

    async def test_missing_qdate_keeps_legacy_payload_contract_but_invalid_qdate_is_rejected(self):
        crawler = EastMoneyCrawler()
        legacy = payload(20261026)
        legacy["data"].pop("qdate")
        crawler.fetch_json = AsyncMock(return_value=legacy)
        crawler._enrich_real_turnover = AsyncMock()
        self.assertEqual(len(await crawler.crawl(MONDAY)), 2)
        crawler.fetch_json = AsyncMock(side_effect=[payload("invalid-date"), legacy])
        crawler._enrich_real_turnover.reset_mock()
        self.assertEqual(await crawler.crawl(MONDAY), [])
        crawler._enrich_real_turnover.assert_not_awaited()


class RealtimePoolDateTests(unittest.IsolatedAsyncioTestCase):
    def client(self, sealed, opened):
        responses = []
        for body in [sealed, opened]:
            response = MagicMock()
            response.json.return_value = body
            responses.append(response)
        client = MagicMock()
        client.get = AsyncMock(side_effect=responses)
        context = MagicMock()
        context.__aenter__ = AsyncMock(return_value=client)
        context.__aexit__ = AsyncMock(return_value=None)
        return context

    async def test_stale_either_pool_clears_wrongly_dated_cache_before_parse_or_enrichment(self):
        for sealed_day, opened_day in [(20261023, "20261026"), (20261026, "20261023")]:
            with self.subTest(sealed_day=sealed_day, opened_day=opened_day):
                service = RealtimeLimitUpService()
                service._pool_cache[MONDAY] = [{"stock_code": "wrongly-dated"}]
                service._pool_cache_time[MONDAY] = time.time()
                service._enrich_board_metadata = AsyncMock()
                context = self.client(payload(sealed_day), payload(opened_day, "000002"))
                with patch.object(realtime_module.httpx, "AsyncClient", return_value=context), \
                     patch.object(realtime_module.em_crawler, "parse", wraps=realtime_module.em_crawler.parse) as parse:
                    rows = await service._refresh_pool_cache(MONDAY)
                self.assertEqual(rows, [])
                self.assertNotIn(MONDAY, service._pool_cache)
                self.assertNotIn(MONDAY, service._pool_cache_time)
                self.assertIn(MONDAY, service._pool_refresh_errors)
                parse.assert_not_called()
                service._enrich_board_metadata.assert_not_awaited()

    async def test_valid_historical_and_current_source_dates_keep_requested_session(self):
        for requested, source_day in [(THURSDAY, 20261023), (MONDAY, "20261026")]:
            with self.subTest(requested=requested):
                service = RealtimeLimitUpService()
                service._enrich_board_metadata = AsyncMock()
                context = self.client(payload(source_day), payload(source_day, "000002"))
                with patch.object(realtime_module.httpx, "AsyncClient", return_value=context):
                    rows = await service._refresh_pool_cache(requested)
                self.assertEqual(len(rows), 2)
                self.assertEqual([row["trade_date"] for row in rows], [requested, requested])
                self.assertNotIn(requested, service._pool_refresh_errors)
                service._enrich_board_metadata.assert_awaited_once()

    async def test_generic_upstream_failure_keeps_last_known_cache(self):
        service = RealtimeLimitUpService()
        cached = [{"stock_code": "000001", "trade_date": FRIDAY}]
        service._pool_cache[FRIDAY] = cached
        service._pool_cache_time[FRIDAY] = 123.0
        context = MagicMock()
        context.__aenter__ = AsyncMock(side_effect=RuntimeError("temporary upstream failure"))
        context.__aexit__ = AsyncMock(return_value=None)
        with patch.object(realtime_module.httpx, "AsyncClient", return_value=context):
            rows = await service._refresh_pool_cache(FRIDAY)
        self.assertEqual(rows, cached)
        self.assertEqual(service._pool_cache[FRIDAY], cached)
        self.assertEqual(service._pool_cache_time[FRIDAY], 123.0)
        self.assertIn(FRIDAY, service._pool_refresh_errors)


if __name__ == "__main__":
    unittest.main()
