import unittest

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
