import importlib
import unittest
from datetime import date
from unittest.mock import patch

from sqlalchemy.ext.asyncio import create_async_engine

from app import database
from app.models.trading_playbook import TradingPlaybookObsidianExport


INDEX = "ix_trading_playbook_obsidian_status_trade_id"


class ObsidianReconciliationIndexTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        self.addAsyncCleanup(self.engine.dispose)
        importlib.import_module("app.models")
        async with self.engine.begin() as connection:
            await connection.run_sync(database.Base.metadata.create_all)
            # Simulate a deployed database from before the new index existed.
            await connection.exec_driver_sql(f"DROP INDEX IF EXISTS {INDEX}")
            await connection.execute(TradingPlaybookObsidianExport.__table__.insert(), {
                "snapshot_key": "plan:1", "snapshot_version": 1,
                "trade_date": date(2026, 10, 13), "entity_type": "plan",
                "phase": "close", "target_path": "plans/example.md",
                "source_hash": "unchanged", "snapshot_json": {"fact": "keep"},
                "status": "written",
            })

    async def initialize(self):
        with patch.object(database, "engine", self.engine), patch.object(
            database.settings, "DATABASE_URL", "sqlite+aiosqlite:///:memory:"
        ):
            await database.init_db()

    def test_model_declares_status_date_id_index(self):
        indexes = {index.name: index for index in TradingPlaybookObsidianExport.__table__.indexes}
        self.assertIn(INDEX, indexes)
        self.assertEqual([column.name for column in indexes[INDEX].columns], ["status", "trade_date", "id"])

    async def test_existing_table_gets_index_and_repeated_startup_is_safe(self):
        await self.initialize()
        await self.initialize()
        async with self.engine.connect() as connection:
            indexes = (await connection.exec_driver_sql("PRAGMA index_list(trading_playbook_obsidian_exports)")).all()
            self.assertEqual(sum(row[1] == INDEX for row in indexes), 1)
            columns = (await connection.exec_driver_sql(f"PRAGMA index_info({INDEX})")).all()
            self.assertEqual([row[2] for row in columns], ["status", "trade_date", "id"])
            records = (await connection.exec_driver_sql(
                "SELECT snapshot_key, source_hash, snapshot_json, status FROM trading_playbook_obsidian_exports"
            )).all()
            self.assertEqual(records, [("plan:1", "unchanged", '{"fact": "keep"}', "written")])

    async def test_metadata_keyset_uses_index_without_temporary_sort(self):
        await self.initialize()
        async with self.engine.connect() as connection:
            for cursor in ("", " AND (trade_date, id) > ('2026-10-13', 167410)"):
                plan = (await connection.exec_driver_sql(
                    "EXPLAIN QUERY PLAN SELECT id, trade_date, snapshot_key, source_hash "
                    "FROM trading_playbook_obsidian_exports WHERE status='written'"
                    + cursor + " ORDER BY trade_date, id LIMIT 1000"
                )).all()
                detail = " ".join(row[3] for row in plan)
                self.assertIn(INDEX, detail)
                self.assertNotIn("TEMP B-TREE", detail.upper())


if __name__ == "__main__":
    unittest.main()
