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


async def close_db():
    """关闭数据库连接"""
    await engine.dispose()
