import asyncio
import tempfile
import unittest
from datetime import date, datetime, time
from pathlib import Path
from types import SimpleNamespace

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.database import Base
from app.models.intelligence import DailyInfoDigest, JiegeModeSignal, KnowledgeDocument
from app.models.market_review import DailyAnalysisRecord, MarketReviewDailyMetric, MarketReviewStockDaily
from app.models.stock import Stock
from app.services.obsidian_knowledge_service import ObsidianKnowledgeService
from app.services.obsidian_vault_writer import ObsidianVaultWriter


class ObsidianKnowledgeServiceTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_async_engine(
            "sqlite+aiosqlite://",
            future=True,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        self.Session = async_sessionmaker(self.engine, expire_on_commit=False)
        asyncio.run(self._create_schema_and_seed())

    def tearDown(self):
        asyncio.run(self.engine.dispose())

    async def _create_schema_and_seed(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        async with self.Session() as session:
            session.add_all(
                [
                    DailyInfoDigest(
                        trade_date=date(2026, 5, 18),
                        status="ready",
                        source_count=2,
                        summary_json={
                            "overview": "AI主线扩散，机器人催化增强。",
                            "main_lines": ["人工智能", "机器人"],
                            "catalysts": ["Figure 直播", "订单验证"],
                            "risks": ["高位分歧"],
                            "plan": "观察核心标的承接。",
                            "mentioned_stocks": [
                                {
                                    "name": "赛微电子",
                                    "code": "300456",
                                    "sector": "机器人",
                                    "summary": "订单验证强化机器人链。",
                                    "reason": "公告订单验证。",
                                    "source_title": "复盘.md",
                                }
                            ],
                            "source_titles": ["复盘.md"],
                        },
                        content_hash="daily-hash-20260518",
                        model="deepseek-v4-pro",
                        generated_at=datetime(2026, 5, 18, 20, 30, 0),
                    ),
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
                        abstract="AI摘要: AI主线扩散。",
                        introduction="# 复盘",
                        content_text="# 复盘\nAI主线扩散，赛微电子(300456)订单验证。",
                        content_hash="doc-hash-a",
                        summary_json={"summary": "AI主线扩散", "themes": ["人工智能"], "catalysts": ["订单验证"]},
                        summary_status="ready",
                        trade_date=date(2026, 5, 18),
                    ),
                    Stock(id=1, stock_code="000001", stock_name="昨日龙头", market="SZ"),
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
                        limit_up_reason="机器人+AI",
                    ),
                    DailyAnalysisRecord(
                        trade_date=date(2026, 5, 18),
                        month="2026-05",
                        auto_result={
                            "辨识度": {
                                "items": [
                                    {
                                        "stock_code": "000001",
                                        "stock_name": "昨日龙头",
                                        "label": "昨日龙头(000001)",
                                        "tags": ["5板", "唯一"],
                                        "reason": "机器人+AI",
                                        "score": 72,
                                    }
                                ],
                                "content": "昨日龙头(000001)",
                            }
                        },
                        manual_overrides={},
                        calc_version=1,
                        data_status="ready",
                        generated_at=datetime(2026, 5, 18, 15, 10, 0),
                        intraday_auto_result={
                            "辨识度": {
                                "items": [
                                    {
                                        "stock_code": "000001",
                                        "stock_name": "昨日龙头",
                                        "label": "昨日龙头(000001)",
                                        "tags": ["盘中提醒", "5板"],
                                        "reason": "盘中承接强",
                                        "score": 68,
                                    }
                                ],
                                "content": "昨日龙头(000001)",
                            }
                        },
                        intraday_manual_overrides={},
                        intraday_calc_version=1,
                        intraday_data_status="ready",
                        intraday_generated_at=datetime(2026, 5, 18, 14, 50, 0),
                    ),
                    JiegeModeSignal(
                        trade_date=date(2026, 5, 18),
                        status="ready",
                        signal_json={
                            "market_phase": {"label": "进攻期", "score": 72, "basis": ["封板率较高"]},
                            "prediction": {
                                "candidates": [
                                    {
                                        "stock_code": "000001",
                                        "stock_name": "昨日龙头",
                                        "label": "昨日龙头(000001)",
                                        "tags": ["5板"],
                                        "reason": "机器人+AI",
                                        "score": 72,
                                    }
                                ],
                                "risk_flags": ["高位分歧需确认"],
                                "daily_analysis": {},
                            },
                            "review": {"sealed_count": 1, "opened_count": 0, "max_board_height": 5, "notes": "验证强势。"},
                        },
                        content_hash="jiege-hash",
                        generated_at=datetime(2026, 5, 18, 20, 35, 0),
                    ),
                ]
            )
            await session.commit()

    def _settings(self, vault_path):
        return SimpleNamespace(
            OBSIDIAN_ENABLED=True,
            OBSIDIAN_VAULT_PATH=str(vault_path),
            OBSIDIAN_AUTO_GIT_ENABLED=False,
            WEB_RESEARCH_ENABLED=True,
            WEB_RESEARCH_ALLOWLIST="https://example.test",
        )

    def test_status_reports_disabled_and_missing_vault_without_throwing(self):
        service = ObsidianKnowledgeService(
            settings=SimpleNamespace(
                OBSIDIAN_ENABLED=False,
                OBSIDIAN_VAULT_PATH="",
                OBSIDIAN_AUTO_GIT_ENABLED=False,
                WEB_RESEARCH_ENABLED=False,
                WEB_RESEARCH_ALLOWLIST="",
            )
        )

        status = service.get_status()

        self.assertFalse(status["enabled"])
        self.assertFalse(status["vault_configured"])
        self.assertFalse(status["vault_exists"])
        self.assertFalse(status["web_research_enabled"])

    def test_build_industry_trends_preserves_sources_and_evidence(self):
        async def run():
            service = ObsidianKnowledgeService(settings=self._settings("D:/tmp/nonexistent-vault"))
            async with self.Session() as session:
                return await service.build_industry_trends(session, limit=10)

        trends = asyncio.run(run())

        self.assertEqual(trends[0]["theme"], "人工智能")
        self.assertEqual(trends[0]["status"], "candidate")
        self.assertEqual(trends[0]["confidence"], "medium")
        self.assertIn("订单验证", trends[0]["catalysts"])
        self.assertEqual(trends[0]["sources"][0]["url"], "https://example.test/review.md")
        self.assertTrue(any(stock["code"] == "300456" for stock in trends[0]["stocks"]))

    def test_build_ultra_short_signals_marks_every_signal_manual_required(self):
        async def run():
            service = ObsidianKnowledgeService(settings=self._settings("D:/tmp/nonexistent-vault"))
            async with self.Session() as session:
                return await service.build_ultra_short_signals(session, date(2026, 5, 18))

        signals = asyncio.run(run())

        self.assertGreaterEqual(len(signals), 1)
        self.assertTrue(all(signal["manual_required"] is True for signal in signals))
        self.assertTrue(all("auto_execute" not in signal for signal in signals))
        self.assertEqual(signals[0]["trade_date"], "2026-05-18")
        self.assertIn(signals[0]["alert_type"], {"watchlist", "plan", "buy_candidate", "sell_candidate"})

    def test_service_reuses_an_injected_vault_writer(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            writer = ObsidianVaultWriter(
                enabled=True,
                vault_path=temp_dir,
                auto_git_enabled=False,
            )

            service = ObsidianKnowledgeService(settings=self._settings(temp_dir), writer=writer)

            self.assertIs(service.writer, writer)
            self.assertEqual(service._vault_path(), Path(temp_dir).resolve())

    def test_export_daily_knowledge_is_idempotent_and_writes_stable_markdown(self):
        async def run(vault_path):
            service = ObsidianKnowledgeService(settings=self._settings(vault_path))
            async with self.Session() as session:
                first = await service.export_daily_knowledge(session, date(2026, 5, 18))
                second = await service.export_daily_knowledge(session, date(2026, 5, 18))
                return first, second

        with tempfile.TemporaryDirectory() as temp_dir:
            vault = Path(temp_dir)
            first, second = asyncio.run(run(vault))
            daily_note = vault / "50_Daily" / "2026" / "2026-05-18.md"
            signal_note = vault / "60_Signals" / "2026-05-18" / "000001-辨识度.md"
            industry_dashboard = vault / "Dashboards" / "产业趋势.md"
            ultra_short_dashboard = vault / "Dashboards" / "超短线驾驶舱.md"

            self.assertEqual(first["written_files"], second["written_files"])
            self.assertTrue(daily_note.exists())
            self.assertTrue(signal_note.exists())
            self.assertTrue(industry_dashboard.exists())
            self.assertTrue(ultra_short_dashboard.exists())
            self.assertFalse((vault / "Notes").exists())
            self.assertEqual(daily_note.read_text(encoding="utf-8"), daily_note.read_text(encoding="utf-8"))

            content = daily_note.read_text(encoding="utf-8")
            self.assertIn("---\ntype: daily", content)
            self.assertIn("date: 2026-05-18", content)
            self.assertIn("source_hash: daily-hash-20260518", content)
            self.assertIn("## 产业趋势", content)
            self.assertIn("## 超短线", content)

            signal_content = signal_note.read_text(encoding="utf-8")
            self.assertIn("manual_required: true", signal_content)
            self.assertIn("sim_result: pending", signal_content)
            self.assertIn("reviewed_at:", signal_content)


if __name__ == "__main__":
    unittest.main()
