import unittest
from datetime import datetime

from sqlalchemy import create_engine, inspect, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.database import Base, ensure_sqlite_schema_compat
from app.models.dynamic_group import DynamicGroupSelectionItem
from app.services.dynamic_group_service import DynamicGroupService


class History:
    def request_build(self, *args, **kwargs):
        pass


class SmartServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine('sqlite+aiosqlite:///:memory:')
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        self.service = DynamicGroupService(session_factory=self.sessions, history=History(),
            now=lambda: datetime(2026, 10, 30, 10, 5))

    async def asyncTearDown(self):
        await self.service.close()
        await self.engine.dispose()

    def definition(self, **changes):
        return dict(name='smart', membership_mode='dynamic', rules=[{
            'field': 'change_pct', 'op': 'gte', 'value': 0}], strategy={
                'prompt': '按换手率取前二', 'summary': '换手率降序', 'assumptions': [],
                'ranking': {'field': 'turnover_rate', 'direction': 'desc', 'limit': 2}}, **changes)

    def snapshot(self, minute=0, rates=(10, 10, 5)):
        stamp = f'2026-10-30T10:{minute:02d}:00'
        codes = ['600001', '600002', '600003']
        return dict(trade_date='2026-10-30', previous_trade_date='2026-10-29', source_time=stamp,
            status='ready', warnings=[], universe_codes=codes,
            rows=[dict(stock_code=code, current_price=10, change_pct=2, turnover_rate=rate, quote_time=stamp)
                  for code, rate in zip(codes, rates)])

    async def test_top_n_executes_with_stable_ties_and_changes_dynamic_members(self):
        group = await self.service.create_group(self.definition())
        await self.service.evaluate_snapshot(self.snapshot())
        first = await self.service.get_current(group['id'])
        self.assertEqual([row['stock_code'] for row in first['items']], ['600001', '600002'])
        old = await self.service.get_selection(group['id'], first['selection_id'])
        self.assertEqual(old['items'][0]['trigger_fields']['turnover_rate'], 10)
        await self.service.evaluate_snapshot(self.snapshot(1, (3, 4, 9)))
        current = await self.service.get_current(group['id'])
        self.assertEqual([row['stock_code'] for row in current['items']], ['600003', '600002'])
        self.assertEqual(await self.service.get_selection(group['id'], first['selection_id']), old)

    async def test_unknown_ranking_or_missing_quotes_cannot_falsely_evict_old_member(self):
        group = await self.service.create_group(self.definition())
        await self.service.evaluate_snapshot(self.snapshot())
        await self.service.evaluate_snapshot(self.snapshot(1, (None, 1, 50)))
        current = await self.service.get_current(group['id'])
        self.assertEqual({row['stock_code'] for row in current['items']}, {'600001', '600002', '600003'})
        self.assertEqual(current['status'], 'partial')
        self.assertEqual(next(row for row in current['items'] if row['stock_code'] == '600001')['data_status'], 'unknown')
        missing = self.snapshot(2, (1, 2, 50))
        missing['rows'] = missing['rows'][1:]
        await self.service.evaluate_snapshot(missing)
        self.assertEqual((await self.service.get_current(group['id']))['total'], 3)

    async def test_preview_ranking_matches_actual_selection_and_exposes_coverage(self):
        snapshot = self.snapshot(rates=(2, 3, 9))
        class Source:
            def peek_snapshot(self):
                return snapshot
            def request_refresh(self):
                pass
        self.service._source = Source()
        preview = await self.service.preview(self.definition())
        self.assertEqual([row['stock_code'] for row in preview['items']], ['600003', '600002'])
        self.assertEqual(preview['matched_count'], 2)
        self.assertEqual(preview['ranking_coverage']['covered_count'], 3)

    async def test_metadata_only_edit_keeps_version_but_ranking_change_resets_and_archives_strategy(self):
        group = await self.service.create_group(self.definition())
        await self.service.evaluate_snapshot(self.snapshot())
        current = await self.service.get_current(group['id'])
        old = await self.service.get_selection(group['id'], current['selection_id'])
        metadata = {**group['strategy'], 'prompt': '换个说法'}
        edited = await self.service.update_group(group['id'], {'strategy': metadata})
        self.assertEqual(edited['version'], 1)
        self.assertEqual((await self.service.get_current(group['id']))['total'], 2)
        changed = {**metadata, 'ranking': {'field': 'turnover_rate', 'direction': 'asc', 'limit': 1}}
        self.assertEqual((await self.service.update_group(group['id'], {'strategy': changed}))['version'], 2)
        await self.service.evaluate_snapshot(self.snapshot(1))
        self.assertEqual([r['stock_code'] for r in (await self.service.get_current(group['id']))['items']], ['600003'])
        self.assertEqual(await self.service.get_selection(group['id'], current['selection_id']), old)

    async def test_nested_archive_keeps_all_relevant_trigger_fields(self):
        definition = self.definition()
        definition['rules'] = [{'any': [{'field': 'change_pct', 'op': 'gte', 'value': 0},
                                      {'field': 'turnover_rate', 'op': 'gte', 'value': 0}]}]
        group = await self.service.create_group(definition)
        await self.service.evaluate_snapshot(self.snapshot())
        current = await self.service.get_current(group['id'])
        frozen = await self.service.get_selection(group['id'], current['selection_id'])
        self.assertEqual(frozen['items'][0]['trigger_fields']['change_pct'], 2)
        self.assertEqual(frozen['items'][0]['trigger_fields']['turnover_rate'], 10)


class StrategyMigrationTests(unittest.TestCase):
    def test_existing_sqlite_database_receives_nullable_strategy_and_preserves_groups(self):
        engine = create_engine('sqlite:///:memory:')
        try:
            with engine.begin() as connection:
                Base.metadata.create_all(connection)
                columns = {column['name'] for column in inspect(connection).get_columns('dynamic_groups')}
                if 'strategy' in columns:
                    connection.exec_driver_sql('ALTER TABLE dynamic_groups DROP COLUMN strategy')
                ensure_sqlite_schema_compat(connection)
                ensure_sqlite_schema_compat(connection)
                columns = {column['name']: column for column in inspect(connection).get_columns('dynamic_groups')}
                self.assertIn('strategy', columns)
                self.assertTrue(columns['strategy']['nullable'])
        finally:
            engine.dispose()
