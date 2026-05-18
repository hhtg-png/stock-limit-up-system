import asyncio
import unittest
from datetime import date
from types import SimpleNamespace

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.database import Base
from app.models.intelligence import DailyInfoDigest, KnowledgeDocument
from app.services.intelligence_service import (
    DeepSeekSummaryClient,
    ImaKnowledgeSource,
    IntelligenceService,
)


class FakeImaClient:
    def __init__(self, pages, contents=None, reports=None):
        self.pages = pages
        self.contents = contents or {}
        self.reports = reports or {}
        self.share_calls = []
        self.markdown_calls = []
        self.report_calls = []

    async def get_share_page(self, share_id, *, cursor="", folder_id="", limit=20):
        self.share_calls.append((share_id, cursor, folder_id, limit))
        key = (share_id, folder_id, cursor)
        return self.pages[key]

    async def fetch_markdown(self, url):
        self.markdown_calls.append(url)
        return self.contents[url]

    async def fetch_report_markdown(self, source_path):
        self.report_calls.append(source_path)
        return self.reports[source_path]


class FakeSummaryClient:
    def __init__(self):
        self.document_calls = []
        self.daily_calls = []
        self.rule_calls = []

    async def summarize_document(self, document):
        self.document_calls.append(document.title)
        return {
            "summary": f"{document.title} summary",
            "themes": ["AI"],
            "catalysts": ["订单验证"],
            "risks": ["高位分歧"],
            "sectors": ["人工智能"],
        }

    async def summarize_daily_info(self, trade_date, documents):
        self.daily_calls.append((trade_date, [doc.title for doc in documents]))
        return {
            "overview": f"{trade_date.isoformat()} overview",
            "main_lines": ["人工智能"],
            "catalysts": ["订单验证"],
            "risks": ["高位分歧"],
            "plan": "观察承接",
        }

    async def build_jiege_rules(self, documents):
        self.rule_calls.append([doc.title for doc in documents])
        return [
            {
                "rule_key": "l1-market",
                "title": "L1 市场环境",
                "category": "L1",
                "summary": "市场环境决定仓位权限",
                "payload": {"signals": ["涨跌家数", "封板率"]},
            }
        ]


def md_item(**overrides):
    base = {
        "media_id": "markdown_1",
        "title": "2026-05-18-复盘.md",
        "media_type": 7,
        "media_type_info": {"name": "MD"},
        "md5_sum": "md5-a",
        "update_time": "1779119000000",
        "create_time": "1779118000000",
        "jump_url": "https://example.test/a.md",
        "source_path": "file_manager/a.md",
        "raw_file_url": "file_manager/a.md",
        "abstract": "AI摘要: 市场修复，AI主线较强。",
        "introduction": "# 2026-05-18 复盘",
        "folder_info": None,
        "parent_folder_id": "root",
    }
    base.update(overrides)
    return base


def pdf_item(**overrides):
    base = md_item(
        media_id="pdf_1",
        title="2025年公众号汇总.pdf",
        media_type=1,
        media_type_info={"name": "PDF"},
        md5_sum="pdf-md5",
        jump_url="",
        source_path="file_manager/a.pdf",
        raw_file_url="file_manager/a.pdf",
        abstract="AI摘要: PDF 摘要。",
        introduction="PDF 前言内容",
    )
    base.update(overrides)
    return base


class IntelligenceServiceTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_async_engine(
            "sqlite+aiosqlite://",
            future=True,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        self.Session = async_sessionmaker(self.engine, expire_on_commit=False)
        asyncio.run(self._create_schema())

    def tearDown(self):
        asyncio.run(self.engine.dispose())

    async def _create_schema(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    def test_sync_fetches_and_summarizes_new_markdown_once(self):
        pages = {
            ("daily", "", ""): {
                "code": 0,
                "knowledge_list": [md_item()],
                "is_end": True,
                "next_cursor": "",
                "version": "v1",
                "knowledge_base_info": {"basic_info": {"name": "每日复盘更新"}},
                "current_path": [{"name": "每日复盘更新", "folder_id": "root"}],
            }
        }
        ima = FakeImaClient(pages, contents={"https://example.test/a.md": "# 复盘\nAI 主线继续。"})
        summary = FakeSummaryClient()
        service = IntelligenceService(
            ima_client=ima,
            summary_client=summary,
            sources=[ImaKnowledgeSource("daily", "每日复盘更新", "daily", "daily")],
        )

        async def run():
            async with self.Session() as session:
                first = await service.sync_source(session, "daily")
                second = await service.sync_source(session, "daily")
                docs = (await session.execute(select(KnowledgeDocument))).scalars().all()
                return first, second, docs

        first, second, docs = asyncio.run(run())

        self.assertEqual(first["changed_documents"], 1)
        self.assertEqual(second["changed_documents"], 0)
        self.assertEqual(ima.markdown_calls, ["https://example.test/a.md"])
        self.assertEqual(summary.document_calls, ["2026-05-18-复盘.md"])
        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0].trade_date, date(2026, 5, 18))
        self.assertEqual(docs[0].summary_status, "ready")

    def test_sync_uses_pdf_summary_without_downloading_large_file(self):
        pages = {
            ("jiege", "", ""): {
                "code": 0,
                "knowledge_list": [pdf_item()],
                "is_end": True,
                "next_cursor": "",
                "version": "v1",
                "knowledge_base_info": {"basic_info": {"name": "杰哥学霸圈"}},
                "current_path": [{"name": "杰哥学霸圈", "folder_id": "root"}],
            }
        }
        ima = FakeImaClient(pages)
        summary = FakeSummaryClient()
        service = IntelligenceService(
            ima_client=ima,
            summary_client=summary,
            sources=[ImaKnowledgeSource("jiege", "杰哥学霸圈", "jiege", "jiege")],
        )

        async def run():
            async with self.Session() as session:
                result = await service.sync_source(session, "jiege")
                doc = (await session.execute(select(KnowledgeDocument))).scalar_one()
                return result, doc

        result, doc = asyncio.run(run())

        self.assertEqual(result["changed_documents"], 1)
        self.assertEqual(ima.markdown_calls, [])
        self.assertEqual(ima.report_calls, [])
        self.assertIn("PDF 摘要", doc.content_text)
        self.assertEqual(doc.summary_status, "ready")

    def test_daily_digest_reuses_cached_hash(self):
        service = IntelligenceService(
            ima_client=FakeImaClient({}),
            summary_client=FakeSummaryClient(),
            sources=[],
        )

        async def run():
            async with self.Session() as session:
                doc = KnowledgeDocument(
                    source_key="daily",
                    source_name="每日复盘更新",
                    share_id="daily",
                    media_id="markdown_1",
                    title="2026-05-18-复盘.md",
                    media_type=7,
                    media_type_name="MD",
                    md5_sum="a",
                    update_time="1779119000000",
                    abstract="AI摘要: 市场修复。",
                    content_text="# 复盘",
                    content_hash="hash-1",
                    summary_json={"summary": "市场修复", "themes": ["AI"]},
                    summary_status="ready",
                    trade_date=date(2026, 5, 18),
                )
                session.add(doc)
                await session.commit()
                first = await service.build_daily_info(session, date(2026, 5, 18))
                second = await service.build_daily_info(session, date(2026, 5, 18))
                digest = (await session.execute(select(DailyInfoDigest))).scalar_one()
                return first, second, digest

        first, second, digest = asyncio.run(run())

        self.assertEqual(first["status"], "ready")
        self.assertTrue(second["cache_hit"])
        self.assertEqual(service.summary_client.daily_calls, [(date(2026, 5, 18), ["2026-05-18-复盘.md"])])
        self.assertEqual(digest.source_count, 1)


class DeepSeekSummaryClientTests(unittest.TestCase):
    def test_missing_api_key_returns_fallback_without_http_call(self):
        client = DeepSeekSummaryClient(settings=SimpleNamespace(DEEPSEEK_API_KEY=None))
        document = SimpleNamespace(
            title="测试文档",
            abstract="AI摘要: 测试摘要。",
            content_text="正文",
            media_type_name="MD",
        )

        result = asyncio.run(client.summarize_document(document))

        self.assertEqual(result["summary"], "测试摘要。")
        self.assertEqual(result["model_status"], "missing_api_key")


if __name__ == "__main__":
    unittest.main()
