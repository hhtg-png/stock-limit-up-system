import unittest
from datetime import date, datetime
from unittest.mock import AsyncMock, patch

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.api.v1 import limit_up
from app.database import Base
from app.models.limit_up import LimitUpClassificationArchive, LimitUpClassificationDigest
from app.models.limit_up import LimitUpRecord
from app.models.stock import Stock
from app.services.ths_move_analysis_source import ThsMoveAnalysis
from app.services.ths_limit_up_classification_service import ThsLimitUpClassificationService


class FakeRealtimeLimitUpService:
    def __init__(self, items):
        self.items = items
        self.requested_dates = []

    async def get_realtime_limit_up_list(self, trade_date):
        self.requested_dates.append(trade_date)
        return self.items


class FailingRealtimeLimitUpService:
    async def get_realtime_limit_up_list(self, trade_date):
        raise AssertionError("archived classification should avoid realtime reload")

    async def get_fast_limit_up_pool(self, trade_date, wait_for_refresh=False, max_cache_age=None):
        raise AssertionError("archived classification should avoid fast realtime pool")

    async def _fetch_ths_reason_map(self):
        raise AssertionError("archived classification should avoid THS reason fetch")


class FakeFastRealtimeLimitUpService:
    def __init__(self, items, reason_map):
        self.items = items
        self.reason_map = reason_map
        self.fast_dates = []
        self.heavy_calls = []

    async def get_fast_limit_up_pool(self, trade_date, wait_for_refresh=False, max_cache_age=None):
        self.fast_dates.append((trade_date, wait_for_refresh, max_cache_age))
        return [dict(item) for item in self.items]

    async def _fetch_ths_reason_map(self):
        return dict(self.reason_map)

    async def get_realtime_limit_up_list(self, trade_date):
        self.heavy_calls.append(trade_date)
        raise AssertionError("classification should not call heavy realtime enrichment")


class FakeAiClassificationClient:
    def __init__(self, classifications, *, api_key="configured", status="ready"):
        self.classifications = classifications
        self.api_key = api_key
        self.status = status
        self.calls = []
        self.model = "deepseek-test"
        self.last_stocks = []

    async def classify_limit_up_reasons(self, trade_date, stocks):
        self.calls.append((trade_date, [stock["stock_code"] for stock in stocks]))
        self.last_stocks = [dict(stock) for stock in stocks]
        return {
            "model": self.model,
            "model_status": self.status,
            "classifications": self.classifications,
        }


class FailingAiClassificationClient(FakeAiClassificationClient):
    async def classify_limit_up_reasons(self, trade_date, stocks):
        raise AssertionError("cached classification should avoid DeepSeek call")


class CountingAiClassificationClient(FakeAiClassificationClient):
    async def classify_limit_up_reasons(self, trade_date, stocks):
        self.calls.append((trade_date, [stock["stock_code"] for stock in stocks]))
        return await super().classify_limit_up_reasons(trade_date, stocks)


class FakeThsMoveService:
    def __init__(self, payloads):
        self.payloads = payloads
        self.calls = []

    async def get_stock_move(self, stock_code, trade_date, *, source_scope="mixed", db=None):
        self.calls.append((stock_code, trade_date, source_scope, db is not None))
        return self.payloads.get(stock_code, {"items": [], "source_status": {"stock_move": "empty"}})


class FakeThsMoveAnalysisSource:
    def __init__(self, analyses):
        self.analyses = analyses
        self.calls = []

    async def get_daily_analyses(self, trade_date, *, target_codes=None, force_refresh=False):
        self.calls.append((trade_date, tuple(target_codes or ()), force_refresh))
        if target_codes:
            targets = {str(code) for code in target_codes}
            return [item for item in self.analyses if item.stock_code in targets]
        return list(self.analyses)


class FakeRowsResult:
    def __init__(self, rows):
        self.rows = rows

    def all(self):
        return self.rows


class SequencedSession:
    def __init__(self, results):
        self.results = list(results)
        self.queries = []

    async def execute(self, query):
        self.queries.append(query)
        return self.results.pop(0)


def make_realtime_item(
    code,
    name,
    reason,
    *,
    first_time,
    final_time=None,
    sealed=True,
    board=1,
    open_count=0,
):
    return {
        "stock_code": code,
        "stock_name": name,
        "limit_up_reason": reason,
        "reason_category": "其他",
        "first_limit_up_time": first_time,
        "final_seal_time": final_time,
        "continuous_limit_up_days": board,
        "open_count": open_count,
        "is_sealed": sealed,
        "is_final_sealed": sealed,
        "current_status": "sealed" if sealed else "opened",
        "seal_amount": 12880.0,
        "turnover_rate": 7.2,
        "amount": 186000.0,
    }


class ThsLimitUpClassificationServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_uses_fast_pool_and_ths_reason_map_without_heavy_realtime_enrichment(self):
        trade_date = date(2026, 6, 16)
        realtime = FakeFastRealtimeLimitUpService(
            [
                make_realtime_item(
                    "600021",
                    "上海电力",
                    "",
                    first_time=datetime(2026, 6, 15, 9, 33, 0),
                )
            ],
            {"600021": "电力改革+虚拟电厂"},
        )
        service = ThsLimitUpClassificationService(realtime_service=realtime)

        payload = await service.get_classification(trade_date)

        self.assertEqual(realtime.fast_dates[0][0], trade_date)
        self.assertEqual(realtime.heavy_calls, [])
        self.assertEqual(payload["source_status"]["realtime_path"], "fast_pool_ths")
        self.assertEqual(payload["groups"][0]["stocks"][0]["limit_up_reason"], "电力改革+虚拟电厂")

    async def test_groups_realtime_items_by_strict_ths_reason_and_preserves_seal_times(self):
        trade_date = date(2026, 6, 16)
        service = ThsLimitUpClassificationService(
            realtime_service=FakeRealtimeLimitUpService(
                [
                    make_realtime_item(
                        "603000",
                        "人民网",
                        "AI电源+英伟达+股权激励",
                        first_time=datetime(2026, 6, 15, 9, 31, 20),
                        final_time=datetime(2026, 6, 15, 9, 45, 5),
                        board=2,
                    ),
                    make_realtime_item(
                        "002230",
                        "科大讯飞",
                        "AI电源+机器人",
                        first_time=datetime(2026, 6, 15, 10, 2, 1),
                        sealed=False,
                        open_count=1,
                    ),
                    make_realtime_item(
                        "300750",
                        "宁德时代",
                        "固态电池+储能",
                        first_time=datetime(2026, 6, 15, 9, 40, 0),
                    ),
                ]
            )
        )

        with patch(
            "app.services.tdx_plugin_service.tdx_plugin_service.get_limit_up_live",
            AsyncMock(side_effect=AssertionError("classification must not use TDX attribution")),
        ) as tdx_live:
            payload = await service.get_classification(trade_date)

        tdx_live.assert_not_called()
        self.assertEqual(payload["requested_date"], trade_date)
        self.assertEqual(payload["trade_date"], trade_date)
        self.assertFalse(payload["is_fallback"])
        self.assertEqual(payload["source_status"]["limit_up_pool"], "ok")
        self.assertEqual(payload["source_status"]["classification_scope"], "strict_ths")
        self.assertEqual(payload["total_count"], 3)

        ai_group = payload["groups"][0]
        self.assertEqual(ai_group["plate_name"], "AI电源")
        self.assertEqual(ai_group["count"], 2)
        self.assertEqual(ai_group["sealed_count"], 1)
        self.assertEqual(ai_group["opened_count"], 1)
        self.assertEqual(ai_group["earliest_first_limit_time"], "09:31:20")
        self.assertEqual(ai_group["latest_first_limit_time"], "10:02:01")
        self.assertEqual([stock["stock_code"] for stock in ai_group["stocks"]], ["603000", "002230"])
        self.assertEqual(ai_group["stocks"][0]["first_limit_up_time"], "09:31:20")
        self.assertEqual(ai_group["stocks"][0]["final_seal_time"], "09:45:05")
        self.assertEqual(ai_group["stocks"][0]["fine_themes"], ["AI电源", "英伟达"])
        self.assertEqual(ai_group["stocks"][1]["current_status"], "opened")
        self.assertEqual(ai_group["stocks"][1]["final_seal_time"], "")

        new_energy_group = payload["groups"][1]
        self.assertEqual(new_energy_group["plate_name"], "固态电池")
        self.assertEqual(new_energy_group["stocks"][0]["classified_plate"], "固态电池")

    async def test_classifies_by_fine_grained_ths_speculation_theme(self):
        service = ThsLimitUpClassificationService(realtime_service=FakeRealtimeLimitUpService([]))

        examples = {
            "PCB铜箔+复合铜箔+PET铜箔": "PCB铜箔",
            "AI电源+英伟达+H股发行": "AI电源",
            "AI算力PCB+存储芯片": "AI算力PCB",
            "高速覆铜板+环氧树脂+先进封装": "高速覆铜板",
            "智能电网+特高压": "智能电网",
            "定增审核通过+AI眼镜电池+机器人+固态电池": "AI眼镜电池",
        }

        for reason, expected in examples.items():
            with self.subTest(reason=reason):
                self.assertEqual(service.classify_reason(reason), expected)
                self.assertEqual(service.extract_fine_themes(reason)[0], expected)

    async def test_industry_background_performance_does_not_override_title_theme(self):
        service = ThsLimitUpClassificationService(realtime_service=FakeRealtimeLimitUpService([]))

        decision = service.classify_ths_article_analysis(
            title="涨停雷达：锆铪分离+锆系新材+半导体上游+固态电池 三祥新材触及涨停",
            summary=(
                "异动原因揭秘：行业原因：1、亿纬锂能预计2026年上半年净利润同比增长95%-110%，"
                "龙头业绩超预期打响中报预增行情。2、锂电行业供给侧出清。"
            ),
            evidence=(
                "行业原因：1、亿纬锂能预计2026年上半年净利润同比增长95%-110%，"
                "龙头业绩超预期打响中报预增行情。2、锂电行业供给侧出清。"
            ),
            fallback_reason="锆铪分离+锆系新材+半导体上游+固态电池",
        )

        self.assertEqual(decision["classified_plate"], "锆铪分离")
        self.assertEqual(decision["fine_theme"], "锆铪分离")
        self.assertIn("固态电池", decision["secondary_themes"])

    async def test_prefers_ths_move_interpretation_for_fine_theme_grouping(self):
        engine = create_async_engine(
            "sqlite+aiosqlite://",
            future=True,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Session = async_sessionmaker(engine, expire_on_commit=False)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        trade_date = date(2026, 6, 16)
        realtime = FakeRealtimeLimitUpService(
            [
                make_realtime_item(
                    "600001",
                    "铜箔科技",
                    "AI算力+PCB概念",
                    first_time=datetime(2026, 6, 16, 9, 32, 0),
                )
            ]
        )
        ths_move_service = FakeThsMoveService(
            {
                "600001": {
                    "items": [
                        {
                            "stock_code": "600001",
                            "reasons": [
                                {
                                    "source": "同花顺",
                                    "title": "PCB铜箔+AI电源",
                                    "content": "公司铜箔产品用于AI服务器电源和高速PCB材料方向。",
                                }
                            ],
                            "concepts": ["PCB铜箔", "AI电源"],
                        }
                    ],
                    "source_status": {"stock_move": "ok"},
                }
            }
        )
        service = ThsLimitUpClassificationService(
            realtime_service=realtime,
            ths_move_service=ths_move_service,
        )

        async with Session() as session:
            payload = await service.get_classification(trade_date, db=session)

        await engine.dispose()

        self.assertEqual(ths_move_service.calls, [("600001", trade_date, "ths", True)])
        self.assertEqual(payload["source_status"]["ths_move_classification"], "ok")
        self.assertEqual(payload["source_status"]["classification_granularity"], "ths_move_fine_theme")
        self.assertEqual(payload["groups"][0]["plate_name"], "PCB铜箔")
        stock = payload["groups"][0]["stocks"][0]
        self.assertEqual(stock["classified_plate"], "PCB铜箔")
        self.assertEqual(stock["rule_classified_plate"], "PCB铜箔")
        self.assertEqual(stock["fine_themes"][:2], ["PCB铜箔", "AI电源"])
        self.assertNotIn("AI算力", stock["fine_themes"])
        self.assertEqual(stock["classification_basis"], "ths_move")
        self.assertEqual(stock["ths_move_title"], "PCB铜箔+AI电源")
        self.assertIn("AI服务器电源", stock["ths_move_summary"])

    async def test_prefers_ths_article_analysis_and_event_priority_over_concept_words(self):
        trade_date = date(2026, 6, 16)
        analysis_source = FakeThsMoveAnalysisSource(
            [
                ThsMoveAnalysis(
                    stock_code="603335",
                    stock_name="迪生力",
                    trade_date=trade_date,
                    title="涨停雷达：并购重组+存储芯片+机器人 迪生力触及涨停",
                    summary="异动原因揭秘：公司拟收购广东全芯半导体30%股权，标的主营存储芯片封装测试。",
                    evidence="公司拟收购广东全芯半导体30%股权，标的主营存储芯片封装测试。",
                    article_url="http://yuanchuang.10jqka.com.cn/20260616/c677499000.shtml",
                    published_at="2026-06-16 10:17:00",
                )
            ]
        )
        ths_move_service = FakeThsMoveService(
            {
                "603335": {
                    "items": [
                        {
                            "stock_code": "603335",
                            "reasons": [
                                {
                                    "source": "同花顺",
                                    "title": "机器人+汽车零部件",
                                    "content": "旧fallback只看到概念词。",
                                }
                            ],
                        }
                    ]
                }
            }
        )
        service = ThsLimitUpClassificationService(
            realtime_service=FakeRealtimeLimitUpService(
                [
                    make_realtime_item(
                        "603335",
                        "迪生力",
                        "汽车零部件+机器人",
                        first_time=datetime(2026, 6, 16, 10, 17, 0),
                    )
                ]
            ),
            ths_analysis_source=analysis_source,
            ths_move_service=ths_move_service,
        )

        payload = await service.get_classification(trade_date)

        self.assertEqual(
            analysis_source.calls,
            [(trade_date, ("603335",), False)],
        )
        self.assertEqual(ths_move_service.calls, [])
        self.assertEqual(payload["source_status"]["ths_article_analysis"], "ok")
        self.assertEqual(payload["source_status"]["classification_granularity"], "ths_article_fine_theme")
        self.assertEqual(payload["groups"][0]["plate_name"], "并购重组")
        stock = payload["groups"][0]["stocks"][0]
        self.assertEqual(stock["classification_basis"], "ths_move_analysis")
        self.assertEqual(stock["classified_plate"], "并购重组")
        self.assertEqual(stock["primary_theme"], "并购重组")
        self.assertEqual(stock["fine_theme"], "收购半导体")
        self.assertIn("存储芯片", stock["secondary_themes"])
        self.assertIn("拟收购广东全芯半导体30%股权", stock["classification_evidence"])
        self.assertEqual(stock["ths_article_url"], "http://yuanchuang.10jqka.com.cn/20260616/c677499000.shtml")
        self.assertEqual(stock["ths_article_time"], "2026-06-16 10:17:00")
        self.assertNotEqual(stock["classified_plate"], "机器人")

    async def test_ths_move_error_falls_back_to_limit_up_reason_classification(self):
        class ErrorThsMoveService:
            async def get_stock_move(self, stock_code, trade_date, *, source_scope="mixed", db=None):
                raise RuntimeError("ths move unavailable")

        engine = create_async_engine(
            "sqlite+aiosqlite://",
            future=True,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Session = async_sessionmaker(engine, expire_on_commit=False)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        trade_date = date(2026, 6, 16)
        service = ThsLimitUpClassificationService(
            realtime_service=FakeRealtimeLimitUpService(
                [
                    make_realtime_item(
                        "600002",
                        "电源科技",
                        "AI电源+英伟达",
                        first_time=datetime(2026, 6, 16, 9, 35, 0),
                    )
                ]
            ),
            ths_move_service=ErrorThsMoveService(),
        )

        async with Session() as session:
            payload = await service.get_classification(trade_date, db=session)

        await engine.dispose()

        self.assertEqual(payload["source_status"]["ths_move_classification"], "error")
        self.assertEqual(payload["groups"][0]["plate_name"], "AI电源")
        stock = payload["groups"][0]["stocks"][0]
        self.assertEqual(stock["classification_basis"], "limit_up_reason")
        self.assertEqual(stock["ths_move_title"], "")

    async def test_forced_ai_receives_ths_move_interpretation_context(self):
        engine = create_async_engine(
            "sqlite+aiosqlite://",
            future=True,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Session = async_sessionmaker(engine, expire_on_commit=False)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        trade_date = date(2026, 6, 16)
        ai_client = FakeAiClassificationClient(
            [{"stock_code": "600003", "plate_name": "PCB铜箔", "confidence": 0.96}]
        )
        service = ThsLimitUpClassificationService(
            realtime_service=FakeRealtimeLimitUpService(
                [
                    make_realtime_item(
                        "600003",
                        "材料科技",
                        "AI算力+PCB概念",
                        first_time=datetime(2026, 6, 16, 9, 38, 0),
                    )
                ]
            ),
            ths_move_service=FakeThsMoveService(
                {
                    "600003": {
                        "items": [
                            {
                                "stock_code": "600003",
                                "reasons": [
                                    {
                                        "source": "同花顺",
                                        "title": "PCB铜箔+AI电源",
                                        "content": "同花顺异动解读指向PCB铜箔方向。",
                                    }
                                ],
                            }
                        ]
                    }
                }
            ),
            ai_classification_client=ai_client,
        )

        async with Session() as session:
            await service.get_classification(trade_date, db=session, force_ai=True)

        await engine.dispose()

        self.assertEqual(ai_client.last_stocks[0]["classification_basis"], "ths_move")
        self.assertEqual(ai_client.last_stocks[0]["ths_move_title"], "PCB铜箔+AI电源")
        self.assertEqual(ai_client.last_stocks[0]["rule_classified_plate"], "PCB铜箔")

    async def test_missing_ai_cache_uses_rule_classification_until_forced(self):
        engine = create_async_engine(
            "sqlite+aiosqlite://",
            future=True,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Session = async_sessionmaker(engine, expire_on_commit=False)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        trade_date = date(2026, 6, 16)
        realtime = FakeRealtimeLimitUpService(
            [
                make_realtime_item(
                    "600406",
                    "国电南瑞",
                    "智能电网+特高压",
                    first_time=datetime(2026, 6, 16, 9, 36, 0),
                )
            ]
        )
        ai_client = CountingAiClassificationClient(
            [{"stock_code": "600406", "plate_name": "电力设备", "confidence": 0.9}]
        )
        service = ThsLimitUpClassificationService(
            realtime_service=realtime,
            ai_classification_client=ai_client,
        )

        async with Session() as session:
            payload = await service.get_classification(trade_date, db=session)

        await engine.dispose()

        self.assertEqual(ai_client.calls, [])
        self.assertEqual(payload["classification_method"], "rule")
        self.assertEqual(payload["source_status"]["ai_classification"], "cache_miss")
        self.assertEqual(payload["groups"][0]["plate_name"], "智能电网")

    async def test_deepseek_cache_overrides_rule_plate_without_changing_ths_reason(self):
        engine = create_async_engine(
            "sqlite+aiosqlite://",
            future=True,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Session = async_sessionmaker(engine, expire_on_commit=False)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        trade_date = date(2026, 6, 16)
        reason = "智能电网+特高压"
        realtime = FakeRealtimeLimitUpService(
            [
                make_realtime_item(
                    "600406",
                    "国电南瑞",
                    reason,
                    first_time=datetime(2026, 6, 16, 9, 36, 0),
                )
            ]
        )
        ai_client = FakeAiClassificationClient(
            [
                {
                    "stock_code": "600406",
                    "plate_name": "电力设备",
                    "confidence": 0.93,
                    "reason_summary": "同花顺原因指向智能电网和特高压。",
                    "keywords": ["智能电网", "特高压"],
                }
            ]
        )
        service = ThsLimitUpClassificationService(
            realtime_service=realtime,
            ai_classification_client=ai_client,
        )

        async with Session() as session:
            first_payload = await service.get_classification(trade_date, db=session, force_ai=True)
            second_service = ThsLimitUpClassificationService(
                realtime_service=realtime,
                ai_classification_client=FailingAiClassificationClient([]),
            )
            second_payload = await second_service.get_classification(trade_date, db=session)
            cached = (await session.execute(select(LimitUpClassificationDigest))).scalars().all()

        await engine.dispose()

        self.assertEqual(ai_client.calls, [(trade_date, ["600406"])])
        self.assertEqual(len(cached), 1)
        self.assertEqual(first_payload["classification_method"], "ai")
        self.assertEqual(first_payload["source_status"]["ai_classification"], "ready")
        self.assertEqual(second_payload["source_status"]["ai_classification"], "cache_hit")
        self.assertEqual(first_payload["groups"][0]["plate_name"], "电力设备")
        stock = first_payload["groups"][0]["stocks"][0]
        self.assertEqual(stock["limit_up_reason"], reason)
        self.assertEqual(stock["classified_plate"], "电力设备")
        self.assertEqual(stock["rule_classified_plate"], "智能电网")
        self.assertEqual(stock["classification_method"], "ai")
        self.assertEqual(stock["ai_confidence"], 0.93)
        self.assertEqual(stock["ai_keywords"], ["智能电网", "特高压"])

        cached_stock = second_payload["groups"][0]["stocks"][0]
        self.assertEqual(cached_stock["classified_plate"], "电力设备")
        self.assertEqual(cached_stock["classification_method"], "ai")

    async def test_archive_daily_classification_persists_snapshot_and_historical_reads_use_archive(self):
        engine = create_async_engine(
            "sqlite+aiosqlite://",
            future=True,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Session = async_sessionmaker(engine, expire_on_commit=False)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        trade_date = date(2026, 6, 16)
        realtime = FakeRealtimeLimitUpService(
            [
                make_realtime_item(
                    "600001",
                    "铜箔科技",
                    "PCB铜箔+AI电源",
                    first_time=datetime(2026, 6, 16, 9, 32, 0),
                )
            ]
        )
        service = ThsLimitUpClassificationService(realtime_service=realtime)

        async with Session() as session:
            archive = await service.archive_daily_classification(trade_date, db=session)
            archived_rows = (await session.execute(select(LimitUpClassificationArchive))).scalars().all()

        self.assertEqual(len(archived_rows), 1)
        self.assertEqual(archive.trade_date, trade_date)
        self.assertEqual(archive.status, "ready")
        self.assertEqual(archive.total_count, 1)
        self.assertEqual(archive.payload_json["groups"][0]["plate_name"], "PCB铜箔")

        archived_service = ThsLimitUpClassificationService(
            realtime_service=FailingRealtimeLimitUpService(),
            ai_classification_client=FailingAiClassificationClient([]),
        )
        async with Session() as session:
            payload = await archived_service.get_classification(trade_date, db=session)

        await engine.dispose()

        self.assertEqual(payload["source_status"]["classification_archive"], "hit")
        self.assertEqual(payload["groups"][0]["plate_name"], "PCB铜箔")
        self.assertEqual(payload["groups"][0]["stocks"][0]["stock_code"], "600001")
        self.assertEqual(realtime.requested_dates, [trade_date])

    async def test_non_trading_date_reads_latest_archive_without_realtime_reload(self):
        engine = create_async_engine(
            "sqlite+aiosqlite://",
            future=True,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Session = async_sessionmaker(engine, expire_on_commit=False)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        async with Session() as session:
            session.add(
                LimitUpClassificationArchive(
                    trade_date=date(2026, 6, 12),
                    payload_json={
                        "requested_date": "2026-06-12",
                        "trade_date": "2026-06-12",
                        "is_fallback": False,
                        "updated_at": "2026-06-12T15:06:00",
                        "source_status": {"classification_archive": "written"},
                        "classification_method": "rule",
                        "total_count": 1,
                        "groups": [
                            {
                                "plate_name": "PCB铜箔",
                                "count": 1,
                                "sealed_count": 1,
                                "opened_count": 0,
                                "earliest_first_limit_time": "09:32:00",
                                "latest_first_limit_time": "09:32:00",
                                "stocks": [{"stock_code": "600001", "stock_name": "铜箔科技"}],
                            }
                        ],
                    },
                    total_count=1,
                    group_count=1,
                    content_hash="archive-hash",
                    source_status={"classification_archive": "written"},
                )
            )
            await session.commit()

        service = ThsLimitUpClassificationService(realtime_service=FailingRealtimeLimitUpService())
        async with Session() as session:
            payload = await service.get_classification(date(2026, 6, 13), db=session)

        await engine.dispose()

        self.assertEqual(payload["requested_date"], "2026-06-13")
        self.assertEqual(payload["trade_date"], "2026-06-12")
        self.assertTrue(payload["is_fallback"])
        self.assertEqual(payload["source_status"]["classification_archive"], "hit")
        self.assertEqual(payload["groups"][0]["plate_name"], "PCB铜箔")

    async def test_current_date_ignores_previous_archive_and_uses_realtime(self):
        engine = create_async_engine(
            "sqlite+aiosqlite://",
            future=True,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Session = async_sessionmaker(engine, expire_on_commit=False)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        async with Session() as session:
            session.add(
                LimitUpClassificationArchive(
                    trade_date=date(2026, 6, 15),
                    payload_json={
                        "requested_date": "2026-06-15",
                        "trade_date": "2026-06-15",
                        "is_fallback": False,
                        "updated_at": "2026-06-15T15:06:00",
                        "source_status": {"classification_archive": "written"},
                        "classification_method": "rule",
                        "total_count": 1,
                        "groups": [
                            {
                                "plate_name": "昨日归档",
                                "count": 1,
                                "sealed_count": 1,
                                "opened_count": 0,
                                "earliest_first_limit_time": "09:32:00",
                                "latest_first_limit_time": "09:32:00",
                                "stocks": [{"stock_code": "600015", "stock_name": "昨日股票"}],
                            }
                        ],
                    },
                    total_count=1,
                    group_count=1,
                    content_hash="previous-archive-hash",
                    source_status={"classification_archive": "written"},
                )
            )
            await session.commit()

        trade_date = date(2026, 6, 16)
        realtime = FakeRealtimeLimitUpService(
            [
                make_realtime_item(
                    "600016",
                    "今日股票",
                    "AI电源+PCB铜箔",
                    first_time=datetime(2026, 6, 16, 9, 35, 0),
                )
            ]
        )
        service = ThsLimitUpClassificationService(realtime_service=realtime)

        async with Session() as session:
            payload = await service.get_classification(trade_date, db=session)

        await engine.dispose()

        self.assertEqual(realtime.requested_dates, [trade_date])
        self.assertNotEqual(payload["source_status"].get("classification_archive"), "hit")
        self.assertEqual(payload["trade_date"], trade_date)
        self.assertEqual(payload["groups"][0]["stocks"][0]["stock_code"], "600016")

    async def test_historical_unarchived_date_uses_db_without_current_ths_article_analysis(self):
        engine = create_async_engine(
            "sqlite+aiosqlite://",
            future=True,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Session = async_sessionmaker(engine, expire_on_commit=False)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        async with Session() as session:
            stock = Stock(stock_code="600001", stock_name="铜箔科技", market="SH")
            session.add(stock)
            await session.flush()
            session.add(
                LimitUpRecord(
                    stock_id=stock.id,
                    trade_date=date(2026, 6, 12),
                    first_limit_up_time=datetime(2026, 6, 12, 9, 32, 0),
                    final_seal_time=datetime(2026, 6, 12, 9, 32, 0),
                    limit_up_reason="PCB铜箔+AI电源",
                    continuous_limit_up_days=1,
                    open_count=0,
                    is_final_sealed=True,
                    current_status="sealed",
                    seal_amount=10000.0,
                    turnover_rate=5.5,
                    amount=180000.0,
                )
            )
            await session.commit()

        analysis_source = FakeThsMoveAnalysisSource(
            [
                ThsMoveAnalysis(
                    stock_code="600001",
                    stock_name="铜箔科技",
                    trade_date=date(2026, 6, 16),
                    title="涨停雷达：今日错误题材 铜箔科技触及涨停",
                    summary="异动原因揭秘：今日原因不应该用于历史日期。",
                    evidence="今日原因不应该用于历史日期。",
                )
            ]
        )
        ths_move_service = FakeThsMoveService(
            {
                "600001": {
                    "items": [
                        {
                            "title": "今日错误异动",
                            "summary": "这条同花顺异动补充不应该用于历史日期。",
                        }
                    ],
                    "source_status": {"stock_move": "ok"},
                }
            }
        )
        service = ThsLimitUpClassificationService(
            realtime_service=FailingRealtimeLimitUpService(),
            ths_analysis_source=analysis_source,
            ths_move_service=ths_move_service,
        )

        async with Session() as session:
            payload = await service.get_classification(date(2026, 6, 12), db=session)

        await engine.dispose()

        self.assertEqual(analysis_source.calls, [])
        self.assertEqual(ths_move_service.calls, [])
        self.assertEqual(payload["source_status"]["realtime_path"], "skipped_historical")
        self.assertEqual(payload["source_status"]["ths_article_analysis"], "skipped_historical")
        self.assertEqual(payload["source_status"]["ths_move_classification"], "skipped_historical")
        self.assertEqual(payload["groups"][0]["plate_name"], "PCB铜箔")
        stock_payload = payload["groups"][0]["stocks"][0]
        self.assertEqual(stock_payload["classification_basis"], "limit_up_reason")
        self.assertEqual(stock_payload["ths_move_title"], "")

    async def test_falls_back_to_database_records_when_realtime_pool_is_empty(self):
        requested_date = date(2026, 6, 16)
        service = ThsLimitUpClassificationService(
            realtime_service=FakeRealtimeLimitUpService([])
        )
        db = SequencedSession(
            [
                FakeRowsResult(
                    [
                        (
                            "603019",
                            "中科曙光",
                            date(2026, 6, 14),
                            datetime(2026, 6, 14, 9, 35, 0),
                            datetime(2026, 6, 14, 10, 8, 30),
                            "AI算力+服务器",
                            3,
                            0,
                            True,
                            "sealed",
                            9200.0,
                            6.5,
                            208000.0,
                        )
                    ]
                )
            ]
        )

        payload = await service.get_classification(requested_date, db=db)

        self.assertEqual(payload["requested_date"], requested_date)
        self.assertEqual(payload["trade_date"], date(2026, 6, 14))
        self.assertTrue(payload["is_fallback"])
        self.assertEqual(payload["source_status"]["limit_up_pool"], "empty")
        self.assertEqual(payload["source_status"]["limit_up_db"], "ok")
        self.assertEqual(payload["groups"][0]["plate_name"], "AI算力")
        self.assertEqual(payload["groups"][0]["stocks"][0]["first_limit_up_time"], "09:35:00")
        self.assertEqual(payload["groups"][0]["stocks"][0]["final_seal_time"], "10:08:30")

        query_text = str(db.queries[0])
        self.assertIn("limit_up_records.trade_date <= :trade_date_1", query_text)


class ThsLimitUpClassificationApiTests(unittest.IsolatedAsyncioTestCase):
    async def test_classification_route_is_registered_before_stock_code_route(self):
        paths = [route.path for route in limit_up.router.routes]

        self.assertIn("/classification", paths)
        self.assertLess(paths.index("/classification"), paths.index("/{stock_code}"))

    async def test_force_ai_route_schedules_background_refresh_without_waiting(self):
        trade_date = date(2026, 6, 16)

        class FakeBackgroundTasks:
            def __init__(self):
                self.tasks = []

            def add_task(self, fn, *args, **kwargs):
                self.tasks.append((fn, args, kwargs))

        class FakeClassificationService:
            def __init__(self):
                self.calls = []

            async def get_classification(self, requested_date, *, db=None, force_ai=False, use_archive=True):
                self.calls.append((requested_date, force_ai, use_archive))
                if force_ai:
                    raise AssertionError("HTTP route must not wait for AI classification")
                return {
                    "requested_date": requested_date,
                    "trade_date": requested_date,
                    "is_fallback": False,
                    "updated_at": "2026-06-16T11:30:00",
                    "source_status": {"ai_classification": "cache_miss"},
                    "classification_method": "rule",
                    "total_count": 0,
                    "groups": [],
                }

            async def rebuild_ai_classification_cache(self, requested_date):
                return requested_date

        fake_background = FakeBackgroundTasks()
        fake_service = FakeClassificationService()
        with patch.object(limit_up, "ths_limit_up_classification_service", fake_service):
            payload = await limit_up.get_limit_up_classification(
                background_tasks=fake_background,
                trade_date=trade_date,
                force_ai=True,
                db=object(),
            )

        self.assertEqual(fake_service.calls, [(trade_date, False, False)])
        self.assertEqual(payload["source_status"]["ai_classification"], "refresh_scheduled")
        self.assertEqual(len(fake_background.tasks), 1)
        task_fn, task_args, task_kwargs = fake_background.tasks[0]
        self.assertEqual(task_fn, fake_service.rebuild_ai_classification_cache)
        self.assertEqual(task_args, (trade_date,))
        self.assertEqual(task_kwargs, {})


if __name__ == "__main__":
    unittest.main()
