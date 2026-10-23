import unittest
from datetime import date
from unittest.mock import AsyncMock, patch

from app.api.v1 import limit_up
from app.services.realtime_limit_up_service import RealtimeLimitUpService


CONTEXT = dict(yesterday_first_limit_up_time='09:31:02',
               yesterday_final_seal_time='14:22:31',
               yesterday_open_change_pct=0.0, today_open_change_pct=-1.25)


class MonitorContextIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_live_api_preserves_all_context_fields_in_response(self):
        day = date(2026, 10, 23)
        raw = dict(stock_code='600001', stock_name='Alpha', limit_up_price=11,
                   current_price=11, continuous_limit_up_days=2, **CONTEXT)
        with patch.object(limit_up, 'today_cn', return_value=day), patch.object(
            limit_up.realtime_limit_up_service, 'get_realtime_limit_up_list', AsyncMock(return_value=[raw])
        ):
            result = await limit_up.get_realtime_limit_up(
                day, continuous_days=None, reason_category=None, sort_by='time', db=None)
        payload = result.model_dump()['data'][0]
        self.assertEqual({key: payload.get(key) for key in CONTEXT}, CONTEXT)

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

    async def test_database_fallback_enriches_actual_date_not_requested_date(self):
        from app.services.limit_up_context_service import limit_up_context_service
        from test_limit_up_detail_api import make_stock, make_record, SequencedSession, FakeAllResult
        actual = date(2026, 10, 9)
        requested = date(2026, 10, 11)
        stock = make_stock()
        record = make_record(actual)
        db = SequencedSession([FakeAllResult([(record, stock)]), FakeAllResult([(stock.id, True, 2)])])

        async def enriched(trade_date, rows, quotes=None):
            self.assertEqual(trade_date, actual)
            self.assertEqual(rows[0]['trade_date'], actual)
            return [{**item, **CONTEXT} for item in rows]

        with patch.object(limit_up, 'get_trade_date_with_fallback', AsyncMock(return_value=(actual, True))), \
             patch.object(limit_up_context_service, 'enrich', AsyncMock(side_effect=enriched)) as enrich:
            result = await limit_up._get_database_limit_up_response(
                db, requested, None, None, None, 'time', None)
        enrich.assert_awaited_once()
        self.assertEqual(result.trade_date, actual)
        self.assertTrue(result.is_fallback)
        payload = result.model_dump()['data'][0]
        self.assertEqual({key: payload.get(key) for key in CONTEXT}, CONTEXT)
