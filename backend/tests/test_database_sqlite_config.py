import unittest

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

            database.ensure_sqlite_schema_compat(connection)

            stock_daily_columns = {
                row[1] for row in connection.exec_driver_sql("PRAGMA table_info(market_review_stock_daily)")
            }
            event_columns = {
                row[1] for row in connection.exec_driver_sql("PRAGMA table_info(market_review_limitup_event)")
            }
            stock_daily_indexes = {
                row[1] for row in connection.exec_driver_sql("PRAGMA index_list(market_review_stock_daily)")
            }
            event_indexes = {
                row[1] for row in connection.exec_driver_sql("PRAGMA index_list(market_review_limitup_event)")
            }

        self.assertIn("stock_id", stock_daily_columns)
        self.assertIn("stock_id", event_columns)
        self.assertIn("ix_market_review_stock_daily_stock_id", stock_daily_indexes)
        self.assertIn("ix_market_review_limitup_event_stock_id", event_indexes)


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


if __name__ == "__main__":
    unittest.main()
