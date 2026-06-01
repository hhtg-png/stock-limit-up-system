import unittest
from datetime import date, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.database import Base
from app.models.tdx_cache import TdxStockMoveCache
from app.scripts.import_tdx_stock_move_cache import import_seed_records


class TdxStockMoveSeedImportTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine(
            "sqlite+aiosqlite://",
            future=True,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        self.Session = async_sessionmaker(self.engine, expire_on_commit=False)
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def asyncTearDown(self):
        await self.engine.dispose()

    async def test_import_seed_records_upserts_valid_payloads_and_skips_empty(self):
        valid_payload = {
            "items": [
                {
                    "stock_code": "603677",
                    "stock_name": "奇精机械",
                    "trade_date": "2026-05-29",
                    "source_scope": "mixed",
                    "reasons": [{"title": "机器人+宁波国资", "content": "解析内容"}],
                }
            ],
            "updated_at": "2026-05-29T18:00:00",
            "source_status": {"seed": "ok"},
            "warnings": [],
        }
        records = [
            {
                "stock_code": "603677",
                "source_scope": "mixed",
                "trade_date": "2026-05-29",
                "stock_name": "奇精机械",
                "payload": valid_payload,
                "generated_at": "2026-05-29T18:00:00",
                "success": True,
            },
            {
                "stock_code": "000001",
                "source_scope": "mixed",
                "trade_date": "2026-05-29",
                "stock_name": "平安银行",
                "payload": {"items": []},
                "success": False,
                "error": "empty",
            },
        ]

        async with self.Session() as session:
            stats = await import_seed_records(records, session)
            result = await session.execute(select(TdxStockMoveCache))
            rows = result.scalars().all()

        self.assertEqual(stats["imported"], 1)
        self.assertEqual(stats["skipped"], 1)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].stock_code, "603677")
        self.assertEqual(rows[0].generated_at, datetime(2026, 5, 29, 18, 0, 0))
        self.assertEqual(rows[0].payload_json["items"][0]["reasons"][0]["title"], "机器人+宁波国资")

    async def test_import_seed_does_not_overwrite_newer_cache_with_older_seed(self):
        existing_payload = {
            "items": [{"stock_code": "603677", "stock_name": "奇精机械", "reasons": [{"title": "新缓存"}]}],
            "source_status": {"stock_move_cache": "fresh"},
            "warnings": [],
        }
        older_payload = {
            "items": [{"stock_code": "603677", "stock_name": "奇精机械", "reasons": [{"title": "旧种子", "content": "旧内容"}]}],
            "source_status": {"seed": "old"},
            "warnings": [],
        }

        async with self.Session() as session:
            session.add(
                TdxStockMoveCache(
                    stock_code="603677",
                    source_scope="mixed",
                    trade_date=date(2026, 5, 29),
                    stock_name="奇精机械",
                    payload_json=existing_payload,
                    source_status={"stock_move_cache": "fresh"},
                    warnings=[],
                    generated_at=datetime(2026, 5, 29, 18, 0, 0),
                )
            )
            await session.commit()

            stats = await import_seed_records(
                [
                    {
                        "stock_code": "603677",
                        "source_scope": "mixed",
                        "trade_date": "2026-05-29",
                        "stock_name": "奇精机械",
                        "payload": older_payload,
                        "generated_at": "2026-05-29T17:00:00",
                        "success": True,
                    }
                ],
                session,
            )
            result = await session.execute(select(TdxStockMoveCache))
            row = result.scalar_one()

        self.assertEqual(stats["kept_newer"], 1)
        self.assertEqual(row.payload_json["items"][0]["reasons"][0]["title"], "新缓存")

    async def test_import_seed_uses_cache_trade_date_over_movement_trade_date(self):
        payload = {
            "items": [
                {
                    "stock_code": "000002",
                    "stock_name": "万科A",
                    "trade_date": "2025-12-10",
                    "reasons": [{"title": "债券展期", "content": "解析内容"}],
                }
            ],
            "source_status": {"seed": "ok"},
            "warnings": [],
        }

        async with self.Session() as session:
            stats = await import_seed_records(
                [
                    {
                        "stock_code": "000002",
                        "source_scope": "mixed",
                        "trade_date": "2025-12-10",
                        "cache_trade_date": "2026-05-29",
                        "movement_trade_date": "2025-12-10",
                        "stock_name": "万科A",
                        "payload": payload,
                        "generated_at": "2026-05-29T14:30:00",
                        "success": True,
                    }
                ],
                session,
            )
            result = await session.execute(select(TdxStockMoveCache))
            row = result.scalar_one()

        self.assertEqual(stats["imported"], 1)
        self.assertEqual(row.trade_date, date(2026, 5, 29))


if __name__ == "__main__":
    unittest.main()
