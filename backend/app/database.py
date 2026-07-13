"""
数据库连接和会话管理
"""
from importlib import import_module

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import event
import os

from app.config import settings


def build_engine_options(database_url: str) -> dict:
    """Build SQLAlchemy engine options for the configured database."""
    if database_url.startswith("sqlite"):
        return {"connect_args": {"timeout": 30}}
    return {}


def configure_sqlite_connection(dbapi_connection, _connection_record) -> None:
    """Reduce transient SQLite lock failures during background sync writes."""
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA busy_timeout=30000")
    finally:
        cursor.close()


def ensure_sqlite_schema_compat(sync_connection) -> None:
    """Apply lightweight SQLite compatibility migrations for existing local DBs."""
    _add_sqlite_column_if_missing(
        sync_connection,
        "market_review_stock_daily",
        "stock_id",
        "INTEGER",
    )
    _add_sqlite_column_if_missing(
        sync_connection,
        "market_review_limitup_event",
        "stock_id",
        "INTEGER",
    )
    sync_connection.exec_driver_sql(
        "CREATE INDEX IF NOT EXISTS ix_market_review_stock_daily_stock_id "
        "ON market_review_stock_daily (stock_id)"
    )
    sync_connection.exec_driver_sql(
        "CREATE INDEX IF NOT EXISTS ix_market_review_limitup_event_stock_id "
        "ON market_review_limitup_event (stock_id)"
    )
    _add_sqlite_column_if_missing(
        sync_connection,
        "daily_analysis_records",
        "intraday_auto_result",
        "JSON DEFAULT '{}'",
    )
    _add_sqlite_column_if_missing(
        sync_connection,
        "daily_analysis_records",
        "intraday_manual_overrides",
        "JSON DEFAULT '{}'",
    )
    _add_sqlite_column_if_missing(
        sync_connection,
        "daily_analysis_records",
        "intraday_calc_version",
        "INTEGER DEFAULT 0",
    )
    _add_sqlite_column_if_missing(
        sync_connection,
        "daily_analysis_records",
        "intraday_data_status",
        "VARCHAR(20) DEFAULT 'empty'",
    )
    _add_sqlite_column_if_missing(
        sync_connection,
        "daily_analysis_records",
        "intraday_generated_at",
        "DATETIME",
    )
    _ensure_trading_plan_active_unique_index(sync_connection)


def _ensure_trading_plan_active_unique_index(sync_connection) -> None:
    table = sync_connection.exec_driver_sql(
        "SELECT 1 FROM sqlite_master "
        "WHERE type='table' AND name='trading_plan_versions'"
    ).first()
    if table is None:
        return
    sync_connection.exec_driver_sql(
        "UPDATE trading_plan_versions SET status='superseded' "
        "WHERE status='active' AND id NOT IN ("
        "SELECT MAX(id) FROM trading_plan_versions "
        "WHERE status='active' GROUP BY target_trade_date"
        ")"
    )
    sync_connection.exec_driver_sql(
        "CREATE UNIQUE INDEX IF NOT EXISTS "
        "uq_trading_plan_one_active_target "
        "ON trading_plan_versions (target_trade_date) "
        "WHERE status='active'"
    )


def _add_sqlite_column_if_missing(sync_connection, table_name: str, column_name: str, column_def: str) -> None:
    columns = {row[1] for row in sync_connection.exec_driver_sql(f"PRAGMA table_info({table_name})")}
    if columns and column_name not in columns:
        sync_connection.exec_driver_sql(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}")


# 确保数据目录存在
os.makedirs("./data", exist_ok=True)
os.makedirs("./logs", exist_ok=True)

# 创建异步引擎
engine = create_async_engine(
    settings.DATABASE_URL,
    echo=settings.DEBUG,
    future=True,
    **build_engine_options(settings.DATABASE_URL),
)

if settings.DATABASE_URL.startswith("sqlite"):
    event.listen(engine.sync_engine, "connect", configure_sqlite_connection)

# 创建异步会话工厂
async_session_maker = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False
)


class Base(DeclarativeBase):
    """ORM基类"""
    pass


async def get_db() -> AsyncSession:
    """获取数据库会话（依赖注入用）"""
    async with async_session_maker() as session:
        try:
            yield session
        finally:
            await session.close()


async def init_db():
    """初始化数据库（创建所有表）"""
    # Ensure all model modules are imported before metadata.create_all() runs.
    import_module("app.models")

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        if settings.DATABASE_URL.startswith("sqlite"):
            await conn.run_sync(ensure_sqlite_schema_compat)


async def close_db():
    """关闭数据库连接"""
    await engine.dispose()
