import unittest
from datetime import date
from unittest.mock import AsyncMock, patch

from app.api.v1 import limit_up
from app.services.realtime_limit_up_service import RealtimeLimitUpService


CONTEXT = dict(yesterday_first_limit_up_time='09:31:02',
               yesterday_final_seal_time='14:22:31',
               yesterday_open_change_pct=0.0, today_open_change_pct=-1.25)


class MonitorContextIntegrationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        # Calendar routing has dedicated session-date tests; keep these on the live branch.
        calendar_patch = patch.object(limit_up, 'is_a_share_trading_day', AsyncMock(return_value=True))
        calendar_patch.start()
        self.addCleanup(calendar_patch.stop)

    async def test_preopen_stale_pool_falls_back_before_computing_yesterday(self):
        import httpx
        import app.services.realtime_limit_up_service as realtime_module
        from app.services.limit_up_context_service import limit_up_context_service
        from test_limit_up_detail_api import (
            make_stock, make_record, SequencedSession, FakeAllResult, FakeScalarResult,
        )
        from datetime import datetime

        monday, friday = date(2026, 10, 26), date(2026, 10, 23)
        stock = make_stock(stock_code='600838', stock_name='Shanghai Nine Hundred')
        record = make_record(friday, first_limit_up_time=datetime(2026, 10, 23, 9, 25),
                             final_seal_time=datetime(2026, 10, 23, 9, 25))
        db = SequencedSession([FakeScalarResult(0), FakeScalarResult(friday),
                               FakeAllResult([(record, stock)]), FakeAllResult([(stock.id, True, 2)])])

        def source(request):
            pool = [] if 'getTopicZBPool' in request.url.path else [{
                'c': '600838', 'n': 'Shanghai Nine Hundred', 'p': 11000,
                'fbt': 92500, 'lbt': 92500, 'lbc': 3, 'zttj': {'days': 3, 'ct': 3},
            }]
            return httpx.Response(200, json={'rc': 0, 'data': {'qdate': 20261023, 'pool': pool}})

        real_client = httpx.AsyncClient
        service = RealtimeLimitUpService()

        async def context(day, rows, quotes=None):
            self.assertEqual(day, monday)
            self.assertEqual(rows[0]['trade_date'], friday)
            return [{**row, 'context_date': monday, 'previous_trade_date': friday,
                     'yesterday_first_limit_up_time': '09:25:00',
                     'yesterday_final_seal_time': '09:25:00',
                     'yesterday_open_change_pct': 10, 'today_open_change_pct': None} for row in rows]

        with patch.object(limit_up, 'today_cn', return_value=monday), \
             patch.object(limit_up, 'realtime_limit_up_service', service), \
             patch.object(service, '_ensure_reason_refresh'), \
             patch.object(service, '_enrich_board_metadata', AsyncMock()), \
             patch.object(realtime_module.httpx, 'AsyncClient', side_effect=lambda **kwargs: real_client(transport=httpx.MockTransport(source))), \
             patch.object(realtime_module.tencent_api, 'get_quotes_batch', AsyncMock(return_value={})), \
             patch.object(realtime_module.tradable_market_value_service, 'get_float_share_map', AsyncMock(return_value={})), \
             patch.object(limit_up_context_service, 'enrich', AsyncMock(side_effect=context)) as enrich:
            result = await limit_up.get_realtime_limit_up(
                monday, continuous_days=None, reason_category=None, sort_by='time', db=db)

        self.assertEqual(result.trade_date, friday)
        self.assertTrue(result.is_fallback)
        self.assertEqual(result.data[0].final_seal_time, '09:25:00')
        self.assertEqual(result.data[0].yesterday_final_seal_time, '09:25:00')
        self.assertEqual(result.data[0].context_date, monday)
        self.assertEqual(result.data[0].previous_trade_date, friday)
        self.assertIsNone(result.data[0].today_open_change_pct)
        enrich.assert_awaited_once()
        self.assertEqual(enrich.await_args.args[0], monday)

    async def test_live_api_preserves_all_context_fields_in_response(self):
        day = date(2026, 10, 23)
        raw = dict(stock_code='600001', stock_name='Alpha', limit_up_price=11,
                   current_price=11, continuous_limit_up_days=2,
                   context_date=day, previous_trade_date=date(2026, 10, 22), **CONTEXT)
        with patch.object(limit_up, 'today_cn', return_value=day), patch.object(
            limit_up.realtime_limit_up_service, 'get_realtime_limit_up_list', AsyncMock(return_value=[raw])
        ):
            result = await limit_up.get_realtime_limit_up(
                day, continuous_days=None, reason_category=None, sort_by='time', db=None)
        payload = result.model_dump()['data'][0]
        self.assertEqual({key: payload.get(key) for key in CONTEXT}, CONTEXT)
        self.assertEqual(payload.get('context_date'), day)
        self.assertEqual(payload.get('previous_trade_date'), date(2026, 10, 22))

    async def test_rich_realtime_enrichment_receives_existing_quotes_and_stream_keeps_fields(self):
        from app.services.limit_up_context_service import limit_up_context_service
        from app.services.realtime_limit_up_stream_tracker import RealtimeLimitUpStreamTracker
        import app.services.realtime_limit_up_service as realtime_module
        day = date(2026, 10, 23)
        row = dict(stock_code='600001', stock_name='Alpha', limit_up_price=11)
        quotes = {'600001': dict(open=10, pre_close=10, datetime='20261023093000')}

        async def enriched(trade_date, rows, quotes=None):
            return [{**item, **CONTEXT} for item in rows]

        service = RealtimeLimitUpService()
        with patch.object(service, 'get_fast_limit_up_pool', AsyncMock(return_value=[row])), \
             patch.object(service, '_ensure_reason_refresh'), \
             patch.object(service, '_fetch_ths_reason_map', AsyncMock(return_value={})), \
             patch.object(realtime_module.tencent_api, 'get_quotes_batch', AsyncMock(return_value=quotes)), \
             patch.object(realtime_module.tradable_market_value_service, 'get_float_share_map', AsyncMock(return_value={})), \
             patch.object(limit_up_context_service, 'enrich', AsyncMock(side_effect=enriched)) as enrich:
            result = await service.get_realtime_limit_up_list(day, include_monitor_context=True)
        self.assertEqual(enrich.await_args.args[0], day)
        self.assertEqual(enrich.await_args.kwargs['quotes'], quotes)
        snapshot = RealtimeLimitUpStreamTracker().sync(result, day)
        payload = snapshot['data']['items'][0]
        self.assertEqual({key: payload.get(key) for key in CONTEXT}, CONTEXT)

    async def test_plain_quote_consumers_do_not_start_monitor_history_requests(self):
        from app.services.limit_up_context_service import limit_up_context_service
        import app.services.realtime_limit_up_service as realtime_module
        service = RealtimeLimitUpService()
        row = dict(stock_code='600001', stock_name='Alpha', limit_up_price=11)
        with patch.object(service, 'get_fast_limit_up_pool', AsyncMock(return_value=[row])), \
             patch.object(service, '_fetch_ths_reason_map', AsyncMock(return_value={})), \
             patch.object(realtime_module.tencent_api, 'get_quotes_batch', AsyncMock(return_value={})), \
             patch.object(realtime_module.tradable_market_value_service, 'get_float_share_map', AsyncMock(return_value={})), \
             patch.object(limit_up_context_service, 'enrich', AsyncMock()) as enrich:
            result = await service.get_realtime_limit_up_list(date(2026, 10, 23))
        enrich.assert_not_awaited()
        self.assertEqual(result[0]['stock_code'], '600001')

    async def test_database_fallback_preserves_actual_rows_with_requested_context_date(self):
        from app.services.limit_up_context_service import limit_up_context_service
        from test_limit_up_detail_api import make_stock, make_record, SequencedSession, FakeAllResult
        actual = date(2026, 10, 9)
        requested = date(2026, 10, 11)
        stock = make_stock()
        record = make_record(actual)
        db = SequencedSession([FakeAllResult([(record, stock)]), FakeAllResult([(stock.id, True, 2)])])

        async def enriched(trade_date, rows, quotes=None):
            self.assertEqual(trade_date, requested)
            self.assertEqual(rows[0]['trade_date'], actual)
            return [{**item, **CONTEXT, 'context_date': requested, 'previous_trade_date': actual} for item in rows]

        with patch.object(limit_up, 'get_trade_date_with_fallback', AsyncMock(return_value=(actual, True))), \
             patch.object(limit_up_context_service, 'enrich', AsyncMock(side_effect=enriched)) as enrich:
            result = await limit_up._get_database_limit_up_response(
                db, requested, None, None, None, 'time', None)
        enrich.assert_awaited_once()
        self.assertEqual(result.trade_date, actual)
        self.assertTrue(result.is_fallback)
        payload = result.model_dump()['data'][0]
        self.assertEqual({key: payload.get(key) for key in CONTEXT}, CONTEXT)
        self.assertEqual(payload.get('context_date'), requested)
        self.assertEqual(payload.get('previous_trade_date'), actual)
