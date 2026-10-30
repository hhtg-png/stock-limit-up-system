import tempfile
import unittest
import sqlite3
from contextlib import closing
from datetime import date
from pathlib import Path

from sqlalchemy import event, func, select, text, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.database import Base
from app.models.trading_playbook import TradingPlaybookObsidianExport as Export
from app.services.trading_playbook.obsidian_snapshot_builder import TradingPlaybookObsidianSnapshotBuilder
from app.services.trading_playbook.obsidian_sync import TradingPlaybookObsidianSyncCoordinator
from test_trading_playbook_obsidian_sync import FIXED_NOW, TRADE_DATE, FakeBuilder, artifact, plan_row


class BoundedObsidianQueryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        path = Path(self.temporary.name) / "bounded.db"
        self.database_path = path
        self.engine = create_async_engine(f"sqlite+aiosqlite:///{path.as_posix()}")
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        self.coordinator = TradingPlaybookObsidianSyncCoordinator(
            session_factory=self.sessions, builder=object(), exporter=object(), writer=object(),
            clock=lambda: FIXED_NOW)
        self.statements = []
        event.listen(self.engine.sync_engine, "before_cursor_execute", self._capture)

    def _capture(self, connection, cursor, statement, parameters, context, executemany):
        if statement.lstrip().upper().startswith("SELECT") and "trading_playbook_obsidian_exports" in statement:
            self.statements.append((statement, parameters))

    async def asyncTearDown(self):
        await self.engine.dispose()
        self.temporary.cleanup()

    async def seed(self, statuses, *, snapshot_key=None, immutable=False, source_hashes=None):
        rows = [Export(snapshot_key=snapshot_key or f"test:{index}", snapshot_version=index + 1,
                       trade_date=TRADE_DATE, entity_type="daily_index", phase="reconcile",
                       target_path=f"path-{index % 2}", source_hash=(source_hashes or {}).get(index, str(index)),
                       snapshot_json={"payload": {"text": "historical snapshot"}}, immutable=immutable,
                       status="written", attempt_no=0, git_status_json=status)
                for index, status in enumerate(statuses)]
        async with self.sessions() as session:
            session.add_all(rows)
            await session.commit()
        self.statements.clear()
        return rows

    async def test_git_retry_pages_past_completed_rows_with_sql_limits_and_no_snapshot_projection(self):
        rows = await self.seed([{"state": "git_complete"}] * 1100 + [{"state": "git_pending"}])
        selected = await self.coordinator._load_git_intent_rows(row_ids=None, allowed_paths=None, limit=1)
        self.assertEqual([row.id for row in selected], [rows[-1].id])
        self.assertGreater(len(self.statements), 1)
        for sql, _ in self.statements:
            self.assertIn("LIMIT", sql.upper())
            self.assertNotIn("snapshot_json", sql)
        # These are the fields used after the query session is closed.
        self.assertEqual(self.coordinator._batch_context(selected), (TRADE_DATE, "reconcile"))
        self.assertEqual(selected[0].target_path, rows[-1].target_path)

    async def test_git_cursor_uses_composite_range_seek_for_many_rows_on_the_same_day(self):
        await self.seed([{"state": "git_complete"}] * 1100 + [{"state": "git_pending"}])
        async with self.engine.begin() as connection:
            await connection.exec_driver_sql(
                "CREATE INDEX test_git_seek ON trading_playbook_obsidian_exports(status, trade_date, id)")
        await self.coordinator._load_git_intent_rows(row_ids=None, allowed_paths=None, limit=1)
        sql, parameters = self.statements[1]
        self.assertIn("trade_date =", sql)
        self.assertIn(".id >", sql)
        async with self.engine.connect() as connection:
            plan = (await connection.exec_driver_sql("EXPLAIN QUERY PLAN " + sql, parameters)).all()
        details = " ".join(str(row[-1]) for row in plan)
        self.assertIn("USING INDEX test_git_seek", details)
        self.assertNotIn("TEMP B-TREE", details)
        self.assertIn("id>?", details)

    async def test_git_same_day_late_page_seeks_id_instead_of_rescanning_older_rows(self):
        values = [dict(snapshot_key=f"bulk:{index}", snapshot_version=1, trade_date=TRADE_DATE,
                       entity_type="daily_index", phase="reconcile", target_path="path",
                       source_hash=str(index), snapshot_json={}, immutable=False, status="written",
                       git_status_json={"state": "git_pending" if index == 5000 else "git_complete"})
                  for index in range(5001)]
        async with self.engine.begin() as connection:
            await connection.execute(Export.__table__.insert(), values)
            await connection.exec_driver_sql(
                "CREATE INDEX test_git_late_seek ON trading_playbook_obsidian_exports(status, trade_date, id)")
        selected = await self.coordinator._load_git_intent_rows(row_ids=None, allowed_paths=None, limit=1)
        self.assertEqual([row.id for row in selected], [5001])
        sql, parameters = next((sql, parameters) for sql, parameters in reversed(self.statements)
                               if ".id >" in sql)
        steps = [0]
        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.set_progress_handler(lambda: steps.__setitem__(0, steps[0] + 100), 100)
            connection.execute(sql, parameters).fetchall()
        self.assertLess(steps[0], 20000, "A late same-day page must seek past older IDs")

    async def test_git_pages_cross_dates_and_finish_when_nothing_needs_retry(self):
        rows = await self.seed([{"state": "git_complete"}] * 1001 + [{"state": "git_pending"}] * 2)
        async with self.sessions() as session:
            await session.execute(update(Export).where(Export.id.in_([row.id for row in rows[-2:]]))
                                  .values(trade_date=date(2026, 7, 17)))
            await session.commit()
        self.statements.clear()
        selected = await self.coordinator._load_git_intent_rows(row_ids=None, allowed_paths=None, limit=2)
        self.assertEqual([row.id for row in selected], [row.id for row in rows[-2:]])
        self.assertTrue(any("trade_date =" in sql and ".id >" in sql for sql, _ in self.statements))
        self.assertTrue(any("trade_date >" in sql for sql, _ in self.statements))
        async with self.sessions() as session:
            await session.execute(update(Export).values(git_status_json={"state": "git_complete"}))
            await session.commit()
        self.assertEqual(await self.coordinator._load_git_intent_rows(
            row_ids=None, allowed_paths=None, limit=2), ())

    async def test_git_retry_preserves_python_legacy_and_error_truthiness_including_raw_json(self):
        statuses = [None, [], "", 0, False, "legacy", {}, {"state": "git_complete"},
                    *({"state": state} for state in ("git_pending", "git_error", "write_in_progress", "write_failed")),
                    *({"error": value} for value in (None, False, 0, "", [], {}, "0", "\0", 1, [0], {"x": 0})),
                    {"state": "git_pending\0suffix"}]
        rows = await self.seed(statuses)
        expected = [row.id for row, status in zip(rows, statuses) if self.coordinator._git_needs_retry(status)]
        raw_values = ['{"state":"git_complete","state":"git_pending"}',
                      '{"st\\u0061te":"git_pending"}', 'invalid-json', None]
        extra = await self.seed([{}] * len(raw_values), snapshot_key="raw")
        async with self.sessions() as session:
            for row, raw in zip(extra, raw_values):
                await session.execute(text("UPDATE trading_playbook_obsidian_exports SET git_status_json=:raw WHERE id=:id"),
                                      {"raw": raw, "id": row.id})
            await session.commit()
        expected.extend(row.id for row in extra)
        selected = await self.coordinator._load_git_intent_rows(row_ids=None, allowed_paths=None, limit=100)
        self.assertEqual([row.id for row in selected], expected)

    async def test_git_filters_allowed_paths_and_ids_before_limiting_and_never_decodes_snapshot(self):
        rows = await self.seed([{"state": "git_pending"}] * 4)
        async with self.sessions() as session:
            await session.execute(text("UPDATE trading_playbook_obsidian_exports SET snapshot_json='invalid-json'"))
            await session.commit()
        self.statements.clear()
        selected = await self.coordinator._load_git_intent_rows(
            row_ids=[rows[0].id, rows[1].id, rows[3].id], allowed_paths=("path-1",), limit=1)
        self.assertEqual([row.id for row in selected], [rows[1].id])
        sql = self.statements[0][0]
        self.assertIn("target_path IN", sql)
        self.assertIn(".id IN", sql)
        self.assertIn("LIMIT", sql.upper())
        self.assertNotIn("snapshot_json", sql)

    async def test_mutable_enqueue_reads_latest_metadata_without_decoding_old_versions(self):
        current = artifact("new", snapshot_key="dashboard:trading-playbook")
        await self.seed([{"state": "git_complete"}] * 50, snapshot_key=current.snapshot_key)
        async with self.sessions() as session:
            await session.execute(text("UPDATE trading_playbook_obsidian_exports SET snapshot_json='invalid-json'"))
            await session.commit()
        self.statements.clear()
        created = (await self.coordinator.enqueue_artifacts([current]))[0]
        self.assertEqual(created.snapshot_version, 51)
        lookups = [sql for sql, _ in self.statements if ".snapshot_key =" in sql]
        self.assertTrue(lookups)
        self.assertTrue(all("LIMIT" in sql.upper() and "snapshot_json" not in sql for sql in lookups))
        reused = (await self.coordinator.enqueue_artifacts([current]))[0]
        self.assertEqual(reused.id, created.id)

    async def test_immutable_enqueue_reuses_older_hash_and_allocates_conflicts_from_latest_version(self):
        original = artifact("original", snapshot_key="plan:42", immutable=True)
        rows = await self.seed([{}] * 50, snapshot_key=original.snapshot_key, immutable=True,
                               source_hashes={0: original.source_hash})
        async with self.sessions() as session:
            await session.execute(text("UPDATE trading_playbook_obsidian_exports SET snapshot_json='invalid-json' WHERE id != :id"),
                                  {"id": rows[0].id})
            await session.commit()
        self.statements.clear()
        reused = (await self.coordinator.enqueue_artifacts([original]))[0]
        self.assertEqual(reused.id, rows[0].id)
        conflict = (await self.coordinator.enqueue_artifacts([artifact("conflict", snapshot_key=original.snapshot_key, immutable=True)]))[0]
        self.assertEqual(conflict.snapshot_version, 51)
        self.assertEqual(conflict.last_error, "immutable_snapshot_hash_conflict")
        for sql, _ in self.statements:
            if ".snapshot_key =" in sql:
                self.assertIn("LIMIT", sql.upper())

    async def test_reconcile_builds_global_dashboard_once_for_latest_day_without_version_growth(self):
        earlier = date(2026, 7, 15)
        async with self.sessions() as session:
            plan = plan_row(source_trade_date=earlier, target_trade_date=TRADE_DATE)
            session.add(plan)
            await session.commit()
        await self.coordinator.enqueue_artifacts([artifact("plan", snapshot_key=f"plan:{plan.id}",
                                                           immutable=True, entity_type="plan", entity_id=plan.id)])
        real_builder = TradingPlaybookObsidianSnapshotBuilder(self.sessions)
        by_date = {}
        for day in (earlier, TRADE_DATE):
            by_date[("alerts", day)] = artifact("alerts", snapshot_key=f"alerts:{day}", trade_date=day)
            by_date[("index", day)] = artifact("index", snapshot_key=f"index:{day}", trade_date=day)
            by_date[("dashboard", day)] = await real_builder.build_dashboard_artifact(day)
        builder = FakeBuilder(by_date=by_date)
        self.coordinator.builder = builder
        await self.coordinator.reconcile_committed_facts()
        await self.coordinator.reconcile_committed_facts()
        self.assertEqual([call for call in builder.calls if call[0] == "dashboard"],
                         [("dashboard", TRADE_DATE), ("dashboard", TRADE_DATE)])
        async with self.sessions() as session:
            count = await session.scalar(select(func.count()).select_from(Export).where(Export.snapshot_key == "dashboard:trading-playbook"))
        self.assertEqual(count, 1)


if __name__ == "__main__":
    unittest.main()
