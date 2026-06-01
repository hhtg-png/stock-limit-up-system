import asyncio
import unittest
from datetime import date, datetime, time
from unittest.mock import AsyncMock, Mock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.api.v1 import intelligence as intelligence_api
from app.api.v1.intelligence import router as intelligence_router
from app.database import Base, get_db
from app.models.intelligence import DailyInfoDigest, DailyInfoDigestVersion, JiegeModeSignal, KnowledgeDocument
from app.models.market_review import MarketReviewDailyMetric, MarketReviewStockDaily
from app.models.stock import Stock


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
                DailyInfoDigestVersion(
                    trade_date=date(2026, 5, 18),
                    status="ready",
                    source_count=2,
                    summary_json={"overview": "盘中第一版", "source_titles": ["复盘.md"]},
                    content_hash="daily-hash-intraday",
                    model="deepseek-v4-pro",
                    generated_at=datetime(2026, 5, 18, 14, 50, 0),
                )
            )
            session.add(
                DailyInfoDigestVersion(
                    trade_date=date(2026, 5, 18),
                    status="ready",
                    source_count=2,
                    summary_json={"overview": "盘后第二版", "source_titles": ["复盘.md"]},
                    content_hash="daily-hash-close",
                    model="deepseek-v4-pro",
                    generated_at=datetime(2026, 5, 18, 20, 30, 0),
                )
            )
            session.add(
                DailyInfoDigest(
                    trade_date=date(2026, 5, 17),
                    status="ready",
                    source_count=1,
                    summary_json={"overview": "AI 主线扩散", "source_titles": ["AI资讯.md"]},
                    content_hash="daily-hash-old",
                    model="codex-local",
                    generated_at=datetime(2026, 5, 17, 20, 30, 0),
                )
            )
            session.add(
                KnowledgeDocument(
                    source_key="daily",
                    source_name="每日复盘更新",
                    share_id="daily",
                    media_id="daily-20260518",
                    title="复盘.md",
                    media_type=7,
                    media_type_name="MD",
                    md5_sum="md5-a",
                    update_time="1779119000000",
                    jump_url="https://example.test/review.md",
                    source_path="file_manager/review.md",
                    abstract="AI摘要: 市场修复，赛微电子订单验证。",
                    introduction="# 复盘",
                    content_text="# 复盘\n市场修复，AI主线较强，赛微电子(300456)公告订单验证。",
                    content_hash="doc-hash-a",
                    summary_json={"summary": "市场修复"},
                    summary_status="ready",
                    trade_date=date(2026, 5, 18),
                )
            )
            session.add(
                KnowledgeDocument(
                    source_key="daily",
                    source_name="每日复盘更新",
                    share_id="daily",
                    media_id="daily-20260517",
                    title="AI资讯.md",
                    media_type=7,
                    media_type_name="MD",
                    md5_sum="md5-b",
                    update_time="1779032600000",
                    jump_url="https://example.test/ai.md",
                    source_path="file_manager/ai.md",
                    abstract="AI摘要: AI主线扩散",
                    introduction="# AI资讯",
                    content_text="# AI资讯\n算力和机器人轮动。",
                    content_hash="doc-hash-b",
                    summary_json={"summary": "AI主线扩散"},
                    summary_status="ready",
                    trade_date=date(2026, 5, 17),
                )
            )
            session.add(
                JiegeModeSignal(
                    trade_date=date(2026, 5, 18),
                    status="ready",
                    signal_json={
                        "market_phase": {"label": "修复期"},
                        "prediction": {"candidates": []},
                        "yesterday_prediction": {
                            "source_date": None,
                            "target_date": "2026-05-18",
                            "candidates": [],
                            "risk_flags": [],
                            "market_phase": {"label": "暂无昨日复盘数据", "score": 0, "basis": []},
                            "notes": "测试缓存",
                        },
                    },
                    content_hash="jiege-hash",
                    generated_at=datetime(2026, 5, 18, 20, 31, 0),
                )
            )
            session.add_all(
                [
                    Stock(
                        id=1,
                        stock_code="000001",
                        stock_name="昨日龙头",
                        market="SZ",
                    ),
                    Stock(
                        id=2,
                        stock_code="000002",
                        stock_name="今日结果",
                        market="SZ",
                    ),
                    MarketReviewDailyMetric(
                        trade_date=date(2026, 5, 18),
                        limit_up_count=82,
                        limit_down_count=1,
                        max_board_height=5,
                        seal_rate=80,
                        up_count_ex_st=3300,
                        down_count_ex_st=1600,
                    ),
                    MarketReviewStockDaily(
                        trade_date=date(2026, 5, 18),
                        stock_id=1,
                        stock_code="000001",
                        stock_name="昨日龙头",
                        today_sealed_close=True,
                        today_continuous_days=5,
                        first_limit_time=time(9, 31),
                        amount=800000,
                        limit_up_reason="昨日主线",
                    ),
                    MarketReviewDailyMetric(
                        trade_date=date(2026, 5, 19),
                        limit_up_count=30,
                        limit_down_count=8,
                        max_board_height=2,
                        seal_rate=50,
                        up_count_ex_st=1200,
                        down_count_ex_st=3600,
                    ),
                    MarketReviewStockDaily(
                        trade_date=date(2026, 5, 19),
                        stock_id=2,
                        stock_code="000002",
                        stock_name="今日结果",
                        today_sealed_close=True,
                        today_continuous_days=2,
                        first_limit_time=time(10, 2),
                        amount=500000,
                        limit_up_reason="今日已发生",
                    ),
                ]
            )
            await session.commit()

    def test_get_daily_info_returns_existing_digest(self):
        response = self.client.get("/intelligence/daily-info", params={"trade_date": "2026-05-18"})

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["trade_date"], "2026-05-18")
        self.assertEqual(payload["summary"]["overview"], "市场修复")
        self.assertIsNone(payload["version_id"])
        self.assertEqual(payload["source_count"], 2)
        self.assertEqual(payload["sources"][0]["title"], "复盘.md")
        self.assertEqual(payload["sources"][0]["jump_url"], "https://example.test/review.md")

    def test_get_daily_info_backfills_stock_mentions_from_sources(self):
        response = self.client.get("/intelligence/daily-info", params={"trade_date": "2026-05-18"})

        self.assertEqual(response.status_code, 200)
        stocks = response.json()["summary"]["mentioned_stocks"]
        self.assertTrue(any(stock["name"] == "赛微电子" and stock["code"] == "300456" for stock in stocks))

    def test_get_daily_info_history_returns_latest_first(self):
        response = self.client.get("/intelligence/daily-info/history", params={"limit": 10})

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual([item["trade_date"] for item in payload["items"]], ["2026-05-18", "2026-05-17"])
        self.assertEqual([item["summary"]["overview"] for item in payload["items"]], ["市场修复", "AI 主线扩散"])
        self.assertEqual(payload["items"][0]["sources"][0]["title"], "复盘.md")

    def test_get_daily_info_can_return_hidden_same_day_version_by_id(self):
        async def get_intraday_version_id():
            async with self.Session() as session:
                result = await session.execute(
                    select(DailyInfoDigestVersion)
                    .where(DailyInfoDigestVersion.summary_json["overview"].as_string() == "盘中第一版")
                    .limit(1)
                )
                return result.scalar_one().id

        version_id = asyncio.run(get_intraday_version_id())

        response = self.client.get(
            "/intelligence/daily-info",
            params={"trade_date": "2026-05-18", "version_id": version_id},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["summary"]["overview"], "盘中第一版")

    def test_get_document_source_returns_original_content(self):
        response = self.client.get("/intelligence/documents/1")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["title"], "复盘.md")
        self.assertIn("市场修复", payload["content_text"])

    def test_search_daily_info_matches_digest_and_original_content(self):
        summary_response = self.client.get("/intelligence/daily-info/search", params={"keyword": "市场修复"})
        content_response = self.client.get("/intelligence/daily-info/search", params={"keyword": "机器人"})

        self.assertEqual(summary_response.status_code, 200)
        self.assertEqual(content_response.status_code, 200)
        self.assertEqual([item["trade_date"] for item in summary_response.json()["items"]], ["2026-05-18"])
        self.assertEqual([item["trade_date"] for item in content_response.json()["items"]], ["2026-05-17"])

    def test_get_daily_info_returns_cached_digest_without_model_refresh(self):
        async def mark_digest_as_missing_key():
            async with self.Session() as session:
                result = await session.execute(
                    select(DailyInfoDigest).where(DailyInfoDigest.trade_date == date(2026, 5, 18))
                )
                digest = result.scalar_one()
                digest.summary_json = {"overview": "旧兜底", "model_status": "missing_api_key"}
                version_result = await session.execute(
                    select(DailyInfoDigestVersion)
                    .where(DailyInfoDigestVersion.trade_date == date(2026, 5, 18))
                    .order_by(DailyInfoDigestVersion.generated_at.desc(), DailyInfoDigestVersion.id.desc())
                    .limit(1)
                )
                latest_version = version_result.scalar_one()
                latest_version.summary_json = {"overview": "旧兜底", "model_status": "missing_api_key"}
                await session.commit()

        asyncio.run(mark_digest_as_missing_key())
        refresh = AsyncMock()

        with patch.object(intelligence_api.intelligence_service.summary_client, "api_key", "configured"), patch.object(
            intelligence_api.intelligence_service,
            "refresh_daily_info_in_background",
            refresh,
        ):
            response = self.client.get("/intelligence/daily-info", params={"trade_date": "2026-05-18"})

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["summary"]["model_status"], "missing_api_key")
        self.assertTrue(payload["cache_hit"])
        refresh.assert_not_awaited()

    def test_post_daily_sync_queues_background_job(self):
        fake_service = Mock()
        fake_service.queue_background_sync.return_value = {
            "state": "queued",
            "queued": True,
            "reason": "manual",
        }

        with patch("app.api.v1.intelligence.intelligence_service", fake_service):
            response = self.client.post("/intelligence/daily-info/sync")

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["queued"])
        fake_service.queue_background_sync.assert_called_once_with(force_daily=False, reason="manual")

    def test_post_daily_sync_can_wait_for_force_rebuild(self):
        fake_service = AsyncMock()
        fake_service.sync_all.return_value = {"sources": {"daily": {"changed_documents": 0}}}

        with patch("app.api.v1.intelligence.intelligence_service", fake_service):
            response = self.client.post("/intelligence/daily-info/sync", params={"force": "true", "wait": "true"})

        self.assertEqual(response.status_code, 200)
        self.assertTrue(fake_service.sync_all.await_args.kwargs["force_daily"])

    def test_get_daily_sync_status_returns_current_job_state(self):
        fake_service = Mock()
        fake_service.get_sync_status.return_value = {
            "state": "running",
            "reason": "probe",
            "started_at": "2026-05-20T11:30:00+08:00",
        }

        with patch("app.api.v1.intelligence.intelligence_service", fake_service):
            response = self.client.get("/intelligence/daily-info/sync-status")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["state"], "running")

    def test_post_daily_probe_queues_sync_when_source_changed(self):
        fake_service = Mock()
        fake_service.probe_daily_source = AsyncMock(return_value={"changed": True, "reason": "new_document"})
        fake_service.queue_background_sync.return_value = {"state": "queued", "queued": True, "reason": "probe"}
        fake_service.get_sync_status.return_value = {"state": "queued", "reason": "probe"}

        with patch("app.api.v1.intelligence.intelligence_service", fake_service):
            response = self.client.post("/intelligence/daily-info/probe")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["probe"]["changed"])
        self.assertTrue(payload["queued"])
        fake_service.queue_background_sync.assert_called_once_with(force_daily=False, reason="probe")

    def test_get_jiege_mode_returns_existing_signal(self):
        response = self.client.get("/intelligence/jiege-mode", params={"trade_date": "2026-05-18"})

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["trade_date"], "2026-05-18")
        self.assertEqual(payload["data"]["market_phase"]["label"], "修复期")

    def test_get_jiege_mode_includes_yesterday_prediction_for_target_date(self):
        response = self.client.get("/intelligence/jiege-mode", params={"trade_date": "2026-05-19"})

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        yesterday_prediction = payload["data"]["yesterday_prediction"]
        self.assertEqual(yesterday_prediction["source_date"], "2026-05-18")
        self.assertEqual(yesterday_prediction["target_date"], "2026-05-19")
        self.assertEqual(yesterday_prediction["candidates"][0]["stock_code"], "000001")
        self.assertNotEqual(yesterday_prediction["candidates"][0]["stock_code"], "000002")

    def test_get_jiege_mode_enriches_legacy_cache_without_yesterday_prediction(self):
        async def seed_legacy_signal():
            async with self.Session() as session:
                session.add(
                    JiegeModeSignal(
                        trade_date=date(2026, 5, 19),
                        status="ready",
                        signal_json={"market_phase": {"label": "旧缓存"}, "prediction": {"candidates": []}},
                        content_hash="legacy-jiege-hash",
                        generated_at=datetime(2026, 5, 19, 9, 0, 0),
                    )
                )
                await session.commit()

        asyncio.run(seed_legacy_signal())

        response = self.client.get("/intelligence/jiege-mode", params={"trade_date": "2026-05-19"})

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertFalse(payload["cache_hit"])
        self.assertEqual(payload["data"]["market_phase"]["label"], "旧缓存")
        self.assertEqual(payload["data"]["yesterday_prediction"]["source_date"], "2026-05-18")


if __name__ == "__main__":
    unittest.main()
