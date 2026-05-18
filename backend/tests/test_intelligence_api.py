import asyncio
import unittest
from datetime import date, datetime
from unittest.mock import AsyncMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.api.v1.intelligence import router as intelligence_router
from app.database import Base, get_db
from app.models.intelligence import DailyInfoDigest, JiegeModeSignal


class IntelligenceApiTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_async_engine(
            "sqlite+aiosqlite://",
            future=True,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        self.Session = async_sessionmaker(self.engine, expire_on_commit=False)
        asyncio.run(self._create_schema_and_seed())

        app = FastAPI()
        app.include_router(intelligence_router, prefix="/intelligence")

        async def override_get_db():
            async with self.Session() as session:
                yield session

        app.dependency_overrides[get_db] = override_get_db
        self.client = TestClient(app)

    def tearDown(self):
        self.client.close()
        asyncio.run(self.engine.dispose())

    async def _create_schema_and_seed(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        async with self.Session() as session:
            session.add(
                DailyInfoDigest(
                    trade_date=date(2026, 5, 18),
                    status="ready",
                    source_count=2,
                    summary_json={"overview": "市场修复", "source_titles": ["复盘.md"]},
                    content_hash="daily-hash",
                    model="deepseek-v4-pro",
                    generated_at=datetime(2026, 5, 18, 20, 30, 0),
                )
            )
            session.add(
                JiegeModeSignal(
                    trade_date=date(2026, 5, 18),
                    status="ready",
                    signal_json={"market_phase": {"label": "修复期"}, "prediction": {"candidates": []}},
                    content_hash="jiege-hash",
                    generated_at=datetime(2026, 5, 18, 20, 31, 0),
                )
            )
            await session.commit()

    def test_get_daily_info_returns_existing_digest(self):
        response = self.client.get("/intelligence/daily-info", params={"trade_date": "2026-05-18"})

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["trade_date"], "2026-05-18")
        self.assertEqual(payload["summary"]["overview"], "市场修复")
        self.assertEqual(payload["source_count"], 2)

    def test_post_daily_sync_calls_service(self):
        fake_service = AsyncMock()
        fake_service.sync_all.return_value = {"sources": {"daily": {"changed_documents": 1}}}

        with patch("app.api.v1.intelligence.intelligence_service", fake_service):
            response = self.client.post("/intelligence/daily-info/sync")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["sources"]["daily"]["changed_documents"], 1)
        fake_service.sync_all.assert_awaited_once()

    def test_get_jiege_mode_returns_existing_signal(self):
        response = self.client.get("/intelligence/jiege-mode", params={"trade_date": "2026-05-18"})

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["trade_date"], "2026-05-18")
        self.assertEqual(payload["data"]["market_phase"]["label"], "修复期")


if __name__ == "__main__":
    unittest.main()
