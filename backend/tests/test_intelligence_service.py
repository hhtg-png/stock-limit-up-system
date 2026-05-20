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
    _json_hash,
)
from app.utils.time_utils import today_cn


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
        self.api_key = None
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


class StockAnalysisSummaryClient(FakeSummaryClient):
    def __init__(self):
        super().__init__()
        self.api_key = "configured"

    async def summarize_document(self, document):
        self.document_calls.append(document.title)
        return {
            "summary": "物理AI与AI PCB油墨方向个股梳理。",
            "themes": ["物理AI", "AI PCB油墨"],
            "catalysts": ["Figure 24h直播", "日系龙头提价15-25%"],
            "risks": ["题材轮动较快"],
            "sectors": ["机器人", "PCB"],
            "stocks": [
                {
                    "name": "美格智能",
                    "code": "002881",
                    "sector": "物理AI/具身基建",
                    "summary": "高算力AI模组供应商，受益具身智能硬件底座需求。",
                    "reason": "孙宇晨预测物理AI主线，Figure 24h直播催化。",
                },
                {
                    "name": "容大感光",
                    "code": "300576",
                    "sector": "AI PCB油墨",
                    "summary": "PCB油墨核心标的，受益高端AI PCB材料国产替代。",
                    "reason": "日系龙头提价15-25%，供需紧平衡。",
                },
            ],
            "stock_analysis_status": "ready",
        }

    async def summarize_daily_info(self, trade_date, documents):
        self.daily_calls.append((trade_date, [doc.title for doc in documents]))
        return {
            "overview": "物理AI和AI PCB油墨是当日重点方向。",
            "main_lines": ["物理AI", "AI PCB油墨"],
            "catalysts": ["Figure 24h直播", "日系龙头提价15-25%"],
            "risks": ["题材轮动较快"],
            "plan": "观察核心标的承接和产业验证。",
            "mentioned_stocks": [
                stock
                for document in documents
                for stock in (document.summary_json or {}).get("stocks", [])
            ],
            "stock_analysis_status": "ready",
        }


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

    def test_sync_ignores_signed_jump_url_change_when_version_fields_are_same(self):
        first_item = md_item(jump_url="https://example.test/a.md?sign=old&t=1")
        second_item = md_item(jump_url="https://example.test/a.md?sign=new&t=2")
        pages = {
            ("daily", "", ""): {
                "code": 0,
                "knowledge_list": [first_item],
                "is_end": True,
                "next_cursor": "",
                "version": "v1",
                "knowledge_base_info": {"basic_info": {"name": "每日复盘更新"}},
                "current_path": [{"name": "每日复盘更新", "folder_id": "root"}],
            }
        }
        ima = FakeImaClient(
            pages,
            contents={
                "https://example.test/a.md?sign=old&t=1": "# 复盘\nAI 主线继续。",
                "https://example.test/a.md?sign=new&t=2": "# 复盘\nAI 主线继续。",
            },
        )
        summary = FakeSummaryClient()
        service = IntelligenceService(
            ima_client=ima,
            summary_client=summary,
            sources=[ImaKnowledgeSource("daily", "每日复盘更新", "daily", "daily")],
        )

        async def run():
            async with self.Session() as session:
                first = await service.sync_source(session, "daily")
                pages[("daily", "", "")]["knowledge_list"] = [second_item]
                second = await service.sync_source(session, "daily")
                doc = (await session.execute(select(KnowledgeDocument))).scalar_one()
                return first, second, doc

        first, second, doc = asyncio.run(run())

        self.assertEqual(first["changed_documents"], 1)
        self.assertEqual(second["changed_documents"], 0)
        self.assertEqual(ima.markdown_calls, ["https://example.test/a.md?sign=old&t=1"])
        self.assertEqual(summary.document_calls, ["2026-05-18-复盘.md"])
        self.assertEqual(doc.jump_url, "https://example.test/a.md?sign=new&t=2")

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

    def test_daily_digest_force_refreshes_document_summaries_after_key_is_configured(self):
        summary = FakeSummaryClient()
        summary.api_key = "configured"
        service = IntelligenceService(
            ima_client=FakeImaClient({}),
            summary_client=summary,
            sources=[],
        )

        async def run():
            async with self.Session() as session:
                session.add(KnowledgeDocument(
                    source_key="daily",
                    source_name="每日复盘更新",
                    share_id="daily",
                    media_id="markdown_1",
                    title="2026-05-18-复盘.md",
                    media_type=7,
                    media_type_name="MD",
                    md5_sum="a",
                    update_time="1779119000000",
                    abstract="AI摘要: 旧摘要。",
                    content_text="# 复盘",
                    content_hash="hash-1",
                    summary_json={"summary": "旧摘要", "model_status": "missing_api_key"},
                    summary_status="ready",
                    trade_date=date(2026, 5, 18),
                ))
                await session.commit()
                digest = await service.build_daily_info(
                    session,
                    date(2026, 5, 18),
                    force=True,
                    refresh_stale_documents=True,
                )
                refreshed_doc = (await session.execute(select(KnowledgeDocument))).scalar_one()
                return digest, refreshed_doc

        digest, refreshed_doc = asyncio.run(run())

        self.assertEqual(summary.document_calls, ["2026-05-18-复盘.md"])
        self.assertEqual(refreshed_doc.summary_json["summary"], "2026-05-18-复盘.md summary")
        self.assertEqual(digest["summary"]["overview"], "2026-05-18 overview")

    def test_daily_digest_cache_does_not_refresh_document_summaries_after_key_is_configured(self):
        summary = FakeSummaryClient()
        summary.api_key = "configured"
        service = IntelligenceService(
            ima_client=FakeImaClient({}),
            summary_client=summary,
            sources=[],
        )

        async def run():
            async with self.Session() as session:
                document = KnowledgeDocument(
                    source_key="daily",
                    source_name="每日复盘更新",
                    share_id="daily",
                    media_id="markdown_1",
                    title="2026-05-18-复盘.md",
                    media_type=7,
                    media_type_name="MD",
                    md5_sum="a",
                    update_time="1779119000000",
                    abstract="AI摘要: 旧摘要。",
                    content_text="# 复盘",
                    content_hash="hash-1",
                    summary_json={"summary": "旧摘要", "model_status": "missing_api_key"},
                    summary_status="ready",
                    trade_date=date(2026, 5, 18),
                )
                session.add(document)
                await session.flush()
                session.add(DailyInfoDigest(
                    trade_date=date(2026, 5, 18),
                    status="ready",
                    source_count=1,
                    content_hash=_json_hash([{
                        "id": document.id,
                        "hash": "hash-1",
                        "summary": {"summary": "旧摘要", "model_status": "missing_api_key"},
                    }]),
                    summary_json={"overview": "旧兜底", "model_status": "missing_api_key"},
                    model="deepseek-v4-pro",
                ))
                await session.commit()
                return await service.build_daily_info(session, date(2026, 5, 18))

        digest = asyncio.run(run())

        self.assertTrue(digest["cache_hit"])
        self.assertEqual(summary.document_calls, [])
        self.assertEqual(summary.daily_calls, [])

    def test_daily_digest_adds_stock_mentions_from_source_documents(self):
        service = IntelligenceService(
            ima_client=FakeImaClient({}),
            summary_client=FakeSummaryClient(),
            sources=[],
        )

        async def run():
            async with self.Session() as session:
                session.add(KnowledgeDocument(
                    source_key="daily",
                    source_name="每日复盘更新",
                    share_id="daily",
                    media_id="markdown_stocks",
                    title="2026-05-18-个股资讯.md",
                    media_type=7,
                    media_type_name="MD",
                    md5_sum="stock-md5",
                    update_time="1779119000000",
                    abstract="AI摘要: 赛微电子订单验证，英伟达目标价上调。",
                    content_text="赛微电子(300456)公告订单验证，英伟达目标价上调，机器人主线继续扩散。",
                    content_hash="stock-hash",
                    summary_json={"summary": "赛微电子订单验证，英伟达目标价上调。", "themes": ["机器人"]},
                    summary_status="ready",
                    trade_date=date(2026, 5, 18),
                ))
                await session.commit()
                return await service.build_daily_info(session, date(2026, 5, 18))

        digest = asyncio.run(run())
        stocks = digest["summary"]["mentioned_stocks"]

        self.assertIn({"name": "赛微电子", "code": "300456"}, [{ "name": item["name"], "code": item.get("code", "") } for item in stocks])
        self.assertTrue(any(item["name"] == "英伟达" for item in stocks))
        self.assertTrue(all(item.get("source_title") == "2026-05-18-个股资讯.md" for item in stocks))

    def test_stock_mentions_do_not_include_generic_event_phrases(self):
        service = IntelligenceService(
            ima_client=FakeImaClient({}),
            summary_client=FakeSummaryClient(),
            sources=[],
        )

        async def run():
            async with self.Session() as session:
                session.add(KnowledgeDocument(
                    source_key="daily",
                    source_name="每日复盘更新",
                    share_id="daily",
                    media_id="markdown_generic",
                    title="5月19日盘前纪要",
                    media_type=7,
                    media_type_name="MD",
                    md5_sum="generic-md5",
                    update_time="1779205400000",
                    abstract="建议结合最新公告验证，盘前纪要发布时间为07:10，SpaceX星舰V3周三首飞。",
                    content_text="建议结合最新公告验证，盘前纪要发布时间为07:10，SpaceX星舰V3周三首飞。",
                    content_hash="generic-hash",
                    summary_json={"summary": "建议结合最新公告验证，SpaceX星舰V3周三首飞。"},
                    summary_status="ready",
                    trade_date=date(2026, 5, 19),
                ))
                await session.commit()
                return await service.build_daily_info(session, date(2026, 5, 19))

        digest = asyncio.run(run())
        names = [item["name"] for item in digest["summary"]["mentioned_stocks"]]

        self.assertIn("SpaceX", names)
        self.assertNotIn("并注意结合最新", names)
        self.assertNotIn("结合最新", names)
        self.assertNotIn("盘前纪要", names)

    def test_deepseek_refreshes_old_document_summary_to_build_stock_analysis(self):
        summary = StockAnalysisSummaryClient()
        service = IntelligenceService(
            ima_client=FakeImaClient({}),
            summary_client=summary,
            sources=[],
        )

        async def run():
            async with self.Session() as session:
                session.add(KnowledgeDocument(
                    source_key="daily",
                    source_name="每日复盘更新",
                    share_id="daily",
                    media_id="markdown_deepseek_stocks",
                    title="今日板块深度整理_5月18日.md",
                    media_type=7,
                    media_type_name="MD",
                    md5_sum="stock-analysis-md5",
                    update_time="1779205400000",
                    abstract="核心标的表格。",
                    content_text="| 板块 | 核心标的 | 催化逻辑 |\n| 物理AI | 美格智能、容大感光 | Figure直播、PCB油墨提价 |",
                    content_hash="stock-analysis-hash",
                    summary_json={"summary": "旧摘要，尚无个股结构化总结。", "model_status": "ready"},
                    summary_status="ready",
                    trade_date=date(2026, 5, 19),
                ))
                await session.commit()
                digest = await service.build_daily_info(
                    session,
                    date(2026, 5, 19),
                    force=True,
                    refresh_stale_documents=True,
                )
                document = (await session.execute(select(KnowledgeDocument))).scalar_one()
                return digest, document

        digest, document = asyncio.run(run())
        stocks = digest["summary"]["mentioned_stocks"]
        by_name = {item["name"]: item for item in stocks}

        self.assertEqual(summary.document_calls, ["今日板块深度整理_5月18日.md"])
        self.assertEqual(document.summary_json["stock_analysis_status"], "ready")
        self.assertEqual(by_name["美格智能"]["sector"], "物理AI/具身基建")
        self.assertEqual(by_name["美格智能"]["summary"], "高算力AI模组供应商，受益具身智能硬件底座需求。")
        self.assertEqual(by_name["容大感光"]["reason"], "日系龙头提价15-25%，供需紧平衡。")

    def test_serialized_stock_mentions_filter_previous_generic_cache(self):
        service = IntelligenceService(
            ima_client=FakeImaClient({}),
            summary_client=FakeSummaryClient(),
            sources=[],
        )
        digest = SimpleNamespace(
            trade_date=date(2026, 5, 19),
            status="ready",
            source_count=1,
            summary_json={
                "overview": "资讯摘要",
                "mentioned_stocks": [
                    {"name": "结合最新", "code": "", "reason": "建议结合最新公告验证", "source_title": "盘前纪要"},
                    {"name": "SpaceX", "code": "", "reason": "SpaceX星舰V3首飞", "source_title": "晚间资讯"},
                ],
            },
            model="codex-local",
            generated_at=None,
        )

        payload = service.serialize_daily_digest(digest, sources=[])
        names = [item["name"] for item in payload["summary"]["mentioned_stocks"]]

        self.assertEqual(names, ["SpaceX"])

    def test_sync_all_rebuilds_changed_daily_history_dates(self):
        today = today_cn()
        old_date = date(2026, 5, 17)
        today_title = f"{today.isoformat()}-复盘.md"
        old_title = "2026-05-17-复盘.md"
        pages = {
            ("daily", "", ""): {
                "code": 0,
                "knowledge_list": [
                    md_item(media_id="today", title=today_title, update_time="1779291800000"),
                    md_item(media_id="old", title=old_title, update_time="1779032600000"),
                ],
                "is_end": True,
                "next_cursor": "",
                "version": "v1",
                "knowledge_base_info": {"basic_info": {"name": "每日复盘更新"}},
                "current_path": [{"name": "每日复盘更新", "folder_id": "root"}],
            }
        }
        ima = FakeImaClient(
            pages,
            contents={
                "https://example.test/a.md": "# 复盘\nAI 主线继续。",
            },
        )
        summary = FakeSummaryClient()
        service = IntelligenceService(
            ima_client=ima,
            summary_client=summary,
            sources=[ImaKnowledgeSource("daily", "每日复盘更新", "daily", "daily")],
        )

        async def run():
            async with self.Session() as session:
                result = await service.sync_all(session)
                digests = (await session.execute(select(DailyInfoDigest))).scalars().all()
                return result, digests

        result, digests = asyncio.run(run())

        self.assertEqual(result["sources"]["daily"]["changed_trade_dates"], [today.isoformat(), old_date.isoformat()])
        self.assertEqual(
            sorted(call[0] for call in summary.daily_calls),
            sorted([today, old_date]),
        )
        self.assertEqual({digest.trade_date for digest in digests}, {today, old_date})

    def test_daily_source_trade_date_uses_update_time_not_title_date(self):
        pages = {
            ("daily", "", ""): {
                "code": 0,
                "knowledge_list": [
                    md_item(
                        media_id="daily-0520-updated",
                        title="5月19日盘前纪要_AI深度整理.md",
                        update_time="1779235482990",
                    ),
                ],
                "is_end": True,
                "next_cursor": "",
                "version": "v1",
                "knowledge_base_info": {"basic_info": {"name": "每日复盘更新"}},
                "current_path": [{"name": "每日复盘更新", "folder_id": "root"}],
            }
        }
        ima = FakeImaClient(
            pages,
            contents={
                "https://example.test/a.md": "# 5月19日盘前纪要\nAI 主线继续。",
            },
        )
        service = IntelligenceService(
            ima_client=ima,
            summary_client=FakeSummaryClient(),
            sources=[ImaKnowledgeSource("daily", "每日复盘更新", "daily", "daily")],
        )

        async def run():
            async with self.Session() as session:
                result = await service.sync_source(session, "daily")
                doc = (
                    await session.execute(
                        select(KnowledgeDocument).where(KnowledgeDocument.media_id == "daily-0520-updated")
                    )
                ).scalar_one()
                return result, doc.trade_date

        result, trade_date = asyncio.run(run())

        self.assertEqual(result["changed_trade_dates"], ["2026-05-20"])
        self.assertEqual(trade_date, date(2026, 5, 20))

    def test_daily_source_reclassifies_cached_title_date_to_update_date(self):
        pages = {
            ("daily", "", ""): {
                "code": 0,
                "knowledge_list": [
                    md_item(
                        media_id="daily-0520-updated",
                        title="5月19日盘前纪要_AI深度整理.md",
                        update_time="1779235482990",
                    ),
                ],
                "is_end": True,
                "next_cursor": "",
                "version": "v1",
                "knowledge_base_info": {"basic_info": {"name": "每日复盘更新"}},
                "current_path": [{"name": "每日复盘更新", "folder_id": "root"}],
            }
        }
        ima = FakeImaClient(pages, contents={"https://example.test/a.md": "# 5月19日盘前纪要\nAI 主线继续。"})
        summary = FakeSummaryClient()
        service = IntelligenceService(
            ima_client=ima,
            summary_client=summary,
            sources=[ImaKnowledgeSource("daily", "每日复盘更新", "daily", "daily")],
        )

        async def seed_and_run():
            async with self.Session() as session:
                session.add(
                    KnowledgeDocument(
                        source_key="daily",
                        source_name="每日复盘更新",
                        share_id="daily",
                        media_id="daily-0520-updated",
                        title="5月19日盘前纪要_AI深度整理.md",
                        media_type=7,
                        media_type_name="MD",
                        md5_sum="md5-a",
                        update_time="1779235482990",
                        jump_url="https://example.test/a.md",
                        source_path="file_manager/a.md",
                        abstract="AI摘要: 市场修复，AI主线较强。",
                        introduction="# 2026-05-18 复盘",
                        content_text="# 5月19日盘前纪要\nAI 主线继续。",
                        content_hash="doc-hash-a",
                        summary_json={"summary": "旧摘要"},
                        summary_status="ready",
                        trade_date=date(2026, 5, 19),
                    )
                )
                await session.commit()

                result = await service.sync_source(session, "daily")
                doc = (
                    await session.execute(
                        select(KnowledgeDocument).where(KnowledgeDocument.media_id == "daily-0520-updated")
                    )
                ).scalar_one()
                return result, doc.trade_date

        result, trade_date = asyncio.run(seed_and_run())

        self.assertEqual(result["changed_documents"], 1)
        self.assertEqual(result["summarized_documents"], 0)
        self.assertEqual(result["changed_trade_dates"], ["2026-05-20", "2026-05-19"])
        self.assertEqual(trade_date, date(2026, 5, 20))
        self.assertEqual(summary.document_calls, [])


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
