import unittest
from unittest.mock import patch

from sqlalchemy import create_engine

from app import database


class DatabaseSqliteConfigTests(unittest.TestCase):
    def test_sqlite_engine_options_set_busy_timeout(self):
        options = database.build_engine_options("sqlite+aiosqlite:///./data/app.db")

        self.assertEqual(options["connect_args"]["timeout"], 30)

    def test_configure_sqlite_connection_enables_wal_and_busy_timeout(self):
        cursor = FakeCursor()
        connection = FakeConnection(cursor)

        database.configure_sqlite_connection(connection, None)

        self.assertEqual(cursor.commands, [
            "PRAGMA journal_mode=WAL",
            "PRAGMA busy_timeout=30000",
        ])
        self.assertTrue(cursor.closed)

    def test_sqlite_schema_compat_adds_market_review_stock_id_columns(self):
        engine = create_engine("sqlite:///:memory:", future=True)
        with engine.begin() as connection:
            connection.exec_driver_sql("CREATE TABLE market_review_stock_daily (id INTEGER PRIMARY KEY)")
            connection.exec_driver_sql("CREATE TABLE market_review_limitup_event (id INTEGER PRIMARY KEY)")
            connection.exec_driver_sql("CREATE TABLE daily_analysis_records (id INTEGER PRIMARY KEY)")

            database.ensure_sqlite_schema_compat(connection)

            stock_daily_columns = {
                row[1] for row in connection.exec_driver_sql("PRAGMA table_info(market_review_stock_daily)")
            }
            event_columns = {
                row[1] for row in connection.exec_driver_sql("PRAGMA table_info(market_review_limitup_event)")
            }
            daily_analysis_columns = {
                row[1] for row in connection.exec_driver_sql("PRAGMA table_info(daily_analysis_records)")
            }
            stock_daily_indexes = {
                row[1] for row in connection.exec_driver_sql("PRAGMA index_list(market_review_stock_daily)")
            }
            event_indexes = {
                row[1] for row in connection.exec_driver_sql("PRAGMA index_list(market_review_limitup_event)")
            }

        self.assertIn("stock_id", stock_daily_columns)
        self.assertIn("stock_id", event_columns)
        self.assertIn("intraday_auto_result", daily_analysis_columns)
        self.assertIn("intraday_manual_overrides", daily_analysis_columns)
        self.assertIn("intraday_calc_version", daily_analysis_columns)
        self.assertIn("intraday_data_status", daily_analysis_columns)
        self.assertIn("intraday_generated_at", daily_analysis_columns)
        self.assertIn("ix_market_review_stock_daily_stock_id", stock_daily_indexes)
        self.assertIn("ix_market_review_limitup_event_stock_id", event_indexes)


class DatabasePostgresqlCompatTests(unittest.IsolatedAsyncioTestCase):
    def test_postgresql_schema_compat_locks_repairs_and_indexes_existing_table(self):
        connection = FakePostgresqlConnection(table_name="trading_plan_versions")

        database.ensure_postgresql_schema_compat(connection)

        sql = [" ".join(statement.split()) for statement in connection.commands]
        self.assertIn("to_regclass('trading_plan_versions')", sql[0])
        self.assertEqual(
            sql[1],
            "LOCK TABLE trading_plan_versions IN ACCESS EXCLUSIVE MODE",
        )
        self.assertIn(
            "ROW_NUMBER() OVER (PARTITION BY target_trade_date "
            "ORDER BY generated_at DESC NULLS LAST, id DESC)",
            sql[2],
        )
        self.assertIn("SET status='superseded'", sql[2])
        self.assertIn("WHERE plan.id=ranked.id AND ranked.active_rank > 1", sql[2])
        self.assertEqual(
            sql[3],
            "CREATE UNIQUE INDEX IF NOT EXISTS "
            "uq_trading_plan_one_active_target "
            "ON trading_plan_versions (target_trade_date) "
            "WHERE status='active'",
        )

    async def test_init_db_runs_postgresql_compat_after_create_all(self):
        connection = FakeAsyncConnection()
        fake_engine = FakeAsyncEngine(connection)

        with patch.object(database, "engine", fake_engine), patch.object(
            database.settings,
            "DATABASE_URL",
            "postgresql+asyncpg://user:pass@localhost/plans",
        ):
            await database.init_db()

        self.assertEqual(connection.callbacks[0].__name__, "create_all")
        self.assertIs(
            connection.callbacks[1],
            database.ensure_postgresql_schema_compat,
        )


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


class FakeCursor:
    def __init__(self):
        self.commands = []
        self.closed = False

    def execute(self, command):
        self.commands.append(command)

    def close(self):
        self.closed = True


class FakeDriverResult:
    def __init__(self, value=None):
        self.value = value

    def scalar_one_or_none(self):
        return self.value


class FakePostgresqlConnection:
    def __init__(self, table_name=None):
        self.table_name = table_name
        self.commands = []

    def exec_driver_sql(self, command):
        self.commands.append(command)
        if "to_regclass" in command:
            return FakeDriverResult(self.table_name)
        return FakeDriverResult()


class FakeAsyncConnection:
    def __init__(self):
        self.callbacks = []

    async def run_sync(self, callback):
        self.callbacks.append(callback)


class FakeAsyncBegin:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class FakeAsyncEngine:
    def __init__(self, connection):
        self.connection = connection

    def begin(self):
        return FakeAsyncBegin(self.connection)


if __name__ == "__main__":
    unittest.main()
