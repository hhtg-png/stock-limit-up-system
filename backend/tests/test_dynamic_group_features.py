import unittest
from datetime import date, datetime

from sqlalchemy import delete, event, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.database import Base
from app.models.market_review import MarketReviewDailyMetric, MarketReviewStockDaily


class HistoricalFeatureTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine('sqlite+aiosqlite:///:memory:')
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        self.days = [date(2026, 10, 28), date(2026, 10, 29), date(2026, 10, 30)]
        async with self.sessions() as session, session.begin():
            for day in self.days:
                session.add(MarketReviewDailyMetric(trade_date=day, limit_up_count=1, calc_version=1,
                    source_status='primary', updated_at=datetime.combine(day, datetime.min.time()).replace(hour=15, minute=5)))
                session.add(MarketReviewStockDaily(trade_date=day, stock_id=1, stock_code='600001',
                    stock_name='test', today_touched_limit_up=True, today_sealed_close=day != self.days[0],
                    today_continuous_days=2 if day == self.days[1] else 1, data_quality_flag='ok'))

    async def asyncTearDown(self):
        await self.engine.dispose()

    def service(self, now=datetime(2026, 10, 30, 10)):
        from app.services.dynamic_group_features_service import DynamicGroupFeatureService
        return DynamicGroupFeatureService(session_factory=self.sessions, now_provider=lambda: now,
            calendar_loader=lambda start, end: self.days)

    def snapshot(self, codes=('600001', '600002')):
        return dict(trade_date='2026-10-30', source_time='2026-10-30T10:00:00', status='ready', warnings=[],
            rows=[dict(stock_code=code) for code in codes], universe_codes=list(codes))

    def rules(self):
        return [{'field': field, 'op': 'gte', 'value': 0, 'days': 2} for field in (
            'recent_limit_up_count', 'recent_touched_limit_up_count', 'recent_max_continuous_days')]

    async def test_completed_sessions_exclude_today_and_compute_real_zero_for_verified_absence(self):
        result = await self.service().enrich_snapshot(self.snapshot(), [self.rules()])
        features = result['rows'][0]['historical_features']
        self.assertEqual({field: values['2'] for field, values in features.items()}, {
            'recent_limit_up_count': 1, 'recent_touched_limit_up_count': 2, 'recent_max_continuous_days': 2})
        self.assertTrue(all(values['2'] == 0 for values in result['rows'][1]['historical_features'].values()))
        self.assertEqual(result['historical_coverage']['2']['sessions'], ['2026-10-28', '2026-10-29'])

    async def test_missing_or_unverified_session_stays_unknown_instead_of_zero(self):
        async with self.sessions() as session, session.begin():
            metric = await session.get(MarketReviewDailyMetric, 1)
            metric.calc_version = 0
        result = await self.service().enrich_snapshot(self.snapshot(), [self.rules()])
        self.assertTrue(all(values['2'] is None for row in result['rows']
                            for values in row['historical_features'].values()))
        self.assertEqual(result['historical_coverage']['2']['status'], 'partial')
        self.assertTrue(result['warnings'])

    async def test_shared_batch_query_is_independent_of_stock_and_group_count(self):
        queries = []
        def capture(conn, cursor, statement, parameters, context, executemany):
            if statement.lstrip().upper().startswith('SELECT'):
                queries.append(statement)
        event.listen(self.engine.sync_engine, 'before_cursor_execute', capture)
        try:
            snapshot = self.snapshot(tuple(f'{600000+i:06}' for i in range(500)))
            result = await self.service().enrich_snapshot(snapshot, [self.rules()] * 100)
        finally:
            event.remove(self.engine.sync_engine, 'before_cursor_execute', capture)
        self.assertEqual(len(result['rows']), 500)
        self.assertLessEqual(len(queries), 3)

    async def test_after_close_includes_verified_today_and_ranking_only_window(self):
        snapshot = self.snapshot()
        snapshot['source_time'] = '2026-10-30T15:00:00'
        result = await self.service(now=datetime(2026, 10, 30, 16)).enrich_snapshot(snapshot, [{
            'rules': [], 'strategy': {'ranking': {'field': 'recent_limit_up_count', 'days': 1, 'direction': 'desc'}}}])
        self.assertEqual(result['rows'][0]['historical_features']['recent_limit_up_count']['1'], 1)
        self.assertEqual(result['historical_coverage']['1']['sessions'], ['2026-10-30'])

    async def test_real_database_history_runs_through_preview_membership_and_frozen_batch(self):
        from app.services.dynamic_group_service import DynamicGroupService
        snapshot = self.snapshot()
        for row in snapshot['rows']:
            row.update(current_price=10, change_pct=2, quote_time=snapshot['source_time'])
        class Source:
            def peek_snapshot(self):
                return snapshot
            def request_refresh(self):
                pass
        class History:
            def request_build(self, *args):
                pass
        service = DynamicGroupService(session_factory=self.sessions, source=Source(), history=History(),
            features=self.service(), now=lambda: datetime(2026, 10, 30, 10))
        try:
            definition = dict(name='近两日涨停', membership_mode='dynamic', rules=[
                {'all': [{'field': 'recent_limit_up_count', 'op': 'gte', 'value': 1, 'days': 2},
                         {'field': 'change_pct', 'op': 'gte', 'value': 0}]}],
                strategy=dict(prompt='近两日涨停最多', summary='按次数', assumptions=[], ranking={
                    'field': 'recent_limit_up_count', 'days': 2, 'direction': 'desc', 'limit': 1}))
            preview = await service.preview(definition)
            self.assertEqual([row['stock_code'] for row in preview['items']], ['600001'])
            group = await service.create_group(definition)
            await service.evaluate_snapshot(snapshot)
            current = await service.get_current(group['id'])
            self.assertEqual([row['stock_code'] for row in current['items']], ['600001'])
            batch = await service.get_selection(group['id'], current['selection_id'])
            self.assertEqual(batch['items'][0]['trigger_fields']['historical_features']['recent_limit_up_count']['2'], 1)
            async with self.sessions() as session, session.begin():
                metric = await session.get(MarketReviewDailyMetric, 1)
                metric.calc_version = 0
            # Quotes are unchanged, but repaired/unavailable facts have a new
            # signature and must update coverage without falsely removing stock.
            await service.evaluate_snapshot(snapshot)
            after = await service.get_current(group['id'])
            self.assertEqual(after['total'], 1)
            self.assertEqual(after['items'][0]['data_status'], 'unknown')
            self.assertEqual(after['status'], 'partial')
            self.assertEqual(await service.get_selection(group['id'], batch['id']), batch)
        finally:
            await service.close()

    async def test_incomplete_calendar_or_daily_fact_count_never_becomes_zero(self):
        # A "complete" metric whose touched facts are missing is not usable.
        async with self.sessions() as session, session.begin():
            metric = await session.get(MarketReviewDailyMetric, 1)
            metric.limit_up_count = 2
        result = await self.service().enrich_snapshot(self.snapshot(), [self.rules()])
        self.assertIsNone(result['rows'][1]['historical_features']['recent_limit_up_count']['2'])
        feature_service = self.service()
        feature_service.calendar_loader = lambda start, end: [self.days[1]]
        result = await feature_service.enrich_snapshot(self.snapshot(), [self.rules()])
        self.assertEqual(result['historical_coverage']['2']['covered_sessions'], 1)
        self.assertIsNone(result['rows'][1]['historical_features']['recent_limit_up_count']['2'])

    async def test_empty_fact_archive_is_unknown_even_when_metric_defaults_to_zero(self):
        async with self.sessions() as session, session.begin():
            await session.execute(delete(MarketReviewStockDaily))
            await session.execute(update(MarketReviewDailyMetric).values(limit_up_count=0))
        result = await self.service().enrich_snapshot(self.snapshot(), [self.rules()])
        self.assertEqual(result['historical_coverage']['2']['status'], 'partial')
        self.assertTrue(all(values['2'] is None for row in result['rows']
                            for values in row['historical_features'].values()))

    async def test_verified_non_event_facts_allow_a_real_zero_event_day(self):
        async with self.sessions() as session, session.begin():
            await session.execute(update(MarketReviewStockDaily).values(
                today_touched_limit_up=False, today_sealed_close=False, today_continuous_days=0))
            await session.execute(update(MarketReviewDailyMetric).values(limit_up_count=0))
        result = await self.service().enrich_snapshot(self.snapshot(), [self.rules()])
        self.assertEqual(result['historical_coverage']['2']['status'], 'ready')
        self.assertTrue(all(values['2'] == 0 for row in result['rows']
                            for values in row['historical_features'].values()))
