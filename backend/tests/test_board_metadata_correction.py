import unittest
from datetime import date, datetime
from unittest.mock import AsyncMock, patch

from app.crawlers.tonghuashun_crawler import ths_crawler
from app.crawlers.eastmoney_crawler import em_crawler
from app.services.realtime_limit_up_service import RealtimeLimitUpService
from app.services.tdx_plugin_service import TdxPluginService
from app.api.v1.websocket import tdx_limit_up_status_label
from app.services.realtime_limit_up_alert_tracker import RealtimeLimitUpAlertTracker
from app.database import Base
from app.models.stock import Stock
from app.models.market_review import MarketReviewDailyMetric, MarketReviewStockDaily
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.pool import StaticPool


class BoardMetadataCorrectionTests(unittest.IsolatedAsyncioTestCase):
    def test_broken_pool_equal_window_is_not_todays_streak(self):
        rows = em_crawler.parse({'data': {'pool': [{'c': '603136',
            'n': '天目湖', 'zttj': {'days': 1, 'ct': 1}}]}}, is_sealed=False)
        self.assertIsNone(rows[0]['continuous_limit_up_days'])

    def test_broken_pool_explicit_zero_history_confirms_first_touch(self):
        rows = em_crawler.parse({'data': {'pool': [{'c': '603082',
            'n': '北自科技', 'zttj': {'days': 0, 'ct': 0}}]}}, is_sealed=False)
        self.assertEqual(rows[0]['continuous_limit_up_days'], 1)
    def test_touch_alert_preserves_interval_label(self):
        tracker = RealtimeLimitUpAlertTracker()
        day = date(2026, 9, 14)
        tracker.collect_new_alerts([], day)
        alerts = tracker.collect_new_alerts([{'stock_code': '000823',
            'continuous_limit_up_days': 3, 'board_label': '6天4板'}], day)
        self.assertEqual(alerts[0].get('board_label'), '6天4板')
    def test_ths_interval_count_is_not_a_continuous_streak(self):
        rows = ths_crawler.parse({'data': {'info': [
            {'code': '000823', 'name': '超声电子', 'high_days': '6天4板'},
            {'code': '000980', 'name': '众泰汽车', 'high_days': '5天4板'},
        ]}})
        self.assertEqual([r['continuous_limit_up_days'] for r in rows], [None, None])
        self.assertEqual(rows[0]['board_label'], '6天4板')

    def test_live_source_label_overrides_inflated_history(self):
        row = TdxPluginService()._build_limit_up_event({
            'stock_code': '000823', 'continuous_limit_up_days': 3,
            'board_label': '6天4板', 'is_final_sealed': True,
        }, date(2026, 9, 14), status_label='6天5板')
        self.assertEqual(row['board'], 3)
        self.assertEqual(row['target_status_label'], '6天4板')

    def test_live_first_board_is_not_replaced_with_arbitrary_history_window(self):
        row = TdxPluginService()._build_limit_up_event({
            'stock_code': '600172', 'continuous_limit_up_days': 1,
            'board_label': '首板', 'is_final_sealed': True,
        }, date(2026, 9, 14), status_label='15天3板')
        self.assertEqual(row['target_status_label'], '首板')

    def test_unknown_board_is_not_announced_as_first_board(self):
        self.assertEqual(tdx_limit_up_status_label(None), '涨停')
        row = TdxPluginService()._build_limit_up_event({
            'stock_code': '603136', 'continuous_limit_up_days': None,
            'is_final_sealed': True,
        }, date(2026, 9, 14))
        self.assertEqual(row['board'], 0)
        self.assertEqual(row['target_status_label'], '涨停')

    async def test_broken_pool_uses_previous_sealed_streak_including_weekend(self):
        service = RealtimeLimitUpService()
        rows = [{'stock_code': '603136', 'continuous_limit_up_days': None,
                 'board_label': '首板', 'is_final_sealed': False},
                {'stock_code': '000823', 'continuous_limit_up_days': 3,
                 'board_label': '6天4板', 'is_final_sealed': True},
                {'stock_code': '600172', 'continuous_limit_up_days': None,
                 'board_label': '首板', 'is_final_sealed': False}]
        with patch.object(service, '_load_previous_board_counts', AsyncMock(return_value={'603136': 1, '000823': 2, '600172': 0}), create=True):
            await service._enrich_board_metadata(rows, date(2026, 9, 14))
        self.assertEqual([r['continuous_limit_up_days'] for r in rows], [2, 3, 1])
        self.assertEqual(rows[1]['board_label'], '6天4板')

    async def test_sealed_to_open_does_not_lose_confirmed_board(self):
        service = RealtimeLimitUpService()
        day = date(2026, 9, 14)
        service._pool_cache[day] = [{'stock_code': '603136', 'continuous_limit_up_days': 2, 'board_label': '2板'}]
        rows = [{'stock_code': '603136', 'continuous_limit_up_days': None, 'board_label': '首板', 'is_final_sealed': False}]
        with patch.object(service, '_load_previous_board_counts', AsyncMock(return_value=None), create=True):
            await service._enrich_board_metadata(rows, day)
        self.assertEqual(rows[0]['continuous_limit_up_days'], 2)

    async def test_unavailable_history_remains_unknown(self):
        service = RealtimeLimitUpService()
        rows = [{'stock_code': '603136', 'continuous_limit_up_days': None, 'board_label': '首板'}]
        with patch.object(service, '_load_previous_board_counts', AsyncMock(return_value=None), create=True):
            await service._enrich_board_metadata(rows, date(2026, 9, 14))
        self.assertIsNone(rows[0]['continuous_limit_up_days'])
        self.assertEqual(rows[0]['board_label'], '')

    async def test_absent_history_cannot_prove_first_board(self):
        rows = [{'stock_code': '009999', 'continuous_limit_up_days': None}]
        service = RealtimeLimitUpService()
        with patch.object(service, '_load_previous_board_counts', AsyncMock(return_value={'603136': 1})):
            await service._enrich_board_metadata(rows, date(2026, 9, 14))
        self.assertIsNone(rows[0]['continuous_limit_up_days'])


class PreviousBoardReviewTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine('sqlite+aiosqlite:///:memory:', poolclass=StaticPool)
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        self.previous = date(2026, 9, 11)
        async with self.sessions() as db:
            db.add_all([Stock(id=1, stock_code='603136', stock_name='天目湖', market='SH'),
                        Stock(id=2, stock_code='000978', stock_name='桂林旅游', market='SZ')])
            db.add(MarketReviewDailyMetric(trade_date=self.previous, limit_up_count=2,
                source_status='primary', updated_at=datetime(2026, 9, 11, 16)))
            db.add_all([
                MarketReviewStockDaily(stock_id=1, stock_code='603136', stock_name='天目湖',
                    trade_date=self.previous, today_touched_limit_up=True,
                    today_sealed_close=True, today_continuous_days=1),
                MarketReviewStockDaily(stock_id=2, stock_code='000978', stock_name='桂林旅游',
                    trade_date=self.previous, today_touched_limit_up=True,
                    today_sealed_close=False, today_continuous_days=5),
            ])
            await db.commit()

    async def asyncTearDown(self):
        await self.engine.dispose()

    async def load(self, service):
        with patch('app.database.async_session_maker', self.sessions), patch(
            'app.data_collectors.scheduler._get_cn_trading_dates',
            return_value=[self.previous, date(2026, 9, 14)],
        ):
            return await service._load_previous_board_counts(date(2026, 9, 14))

    async def test_previous_complete_review_excludes_broken_close(self):
        self.assertEqual(await self.load(RealtimeLimitUpService()), {'603136': 1, '000978': 0})

    async def test_incomplete_review_cannot_prove_first_board(self):
        from sqlalchemy import update
        async with self.sessions() as db:
            await db.execute(update(MarketReviewDailyMetric).values(limit_up_count=3))
            await db.commit()
        self.assertIsNone(await self.load(RealtimeLimitUpService()))

    async def test_intraday_review_cannot_be_used_as_previous_close(self):
        from sqlalchemy import update
        async with self.sessions() as db:
            await db.execute(update(MarketReviewDailyMetric).values(updated_at=datetime(2026, 9, 11, 14)))
            await db.commit()
        self.assertIsNone(await self.load(RealtimeLimitUpService()))

    async def test_rich_history_path_keeps_unconfirmed_stock_unknown(self):
        service = TdxPluginService()
        raw = [{'stock_code': '009999', 'continuous_limit_up_days': None,
                'board_label': '', 'is_final_sealed': True}]
        async with self.sessions() as db:
            labels = await service._load_historical_status_labels(raw, date(2026, 9, 14), db)
        row = service._build_limit_up_event(raw[0], date(2026, 9, 14), status_label=labels['009999'])
        self.assertEqual(row['target_status_label'], '涨停')
