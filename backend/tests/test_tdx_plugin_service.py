import asyncio
import unittest
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.database import Base
from app.services.tdx_attribution_sources import PublicStockAttribution
from app.services.tdx_external_sources import ExternalStockMove
from app.models.tdx_cache import TdxStockMoveCache
from app.services.tdx_plugin_service import TdxPluginService


def make_limit_up_item(
    code,
    name,
    reason_category,
    *,
    sealed=True,
    status=None,
    board=1,
    open_count=0,
    first_time=None,
    final_time=None,
):
    return {
        "stock_code": code,
        "stock_name": name,
        "reason_category": reason_category,
        "limit_up_reason": f"{reason_category}催化",
        "is_sealed": sealed,
        "is_final_sealed": sealed,
        "current_status": status or ("sealed" if sealed else "opened"),
        "continuous_limit_up_days": board,
        "open_count": open_count,
        "first_limit_up_time": first_time or datetime(2026, 5, 28, 9, 35, 0),
        "final_seal_time": final_time or datetime(2026, 5, 28, 10, 12, 0),
        "seal_amount": 50000000,
        "amount": 800000000,
        "turnover_rate": 12.3,
        "industry": "计算机",
    }


class FakeRowsResult:
    def __init__(self, rows):
        self.rows = rows

    def all(self):
        return self.rows


class FakeScalarResult:
    def __init__(self, value):
        self.value = value

    def scalar_one_or_none(self):
        return self.value


class SequencedSession:
    def __init__(self, results):
        self.results = list(results)

    async def execute(self, _query):
        return self.results.pop(0)


class FakeExternalMoveProvider:
    def __init__(self, stock_move=None, review_moves=None):
        self.stock_move = stock_move
        self.review_moves = review_moves or []
        self.stock_move_calls = 0

    async def get_stock_move(self, stock_code, trade_date=None):
        self.stock_move_calls += 1
        return self.stock_move

    async def get_review_moves(self, trade_date):
        return self.review_moves


class FakeAttributionProvider:
    def __init__(self, attributions=None):
        self.attributions = attributions or {}
        self.requested_codes = None

    async def get_attributions(self, codes):
        self.requested_codes = list(codes)
        return {
            code: self.attributions[code]
            for code in self.requested_codes
            if code in self.attributions
        }


class FakeNewsProvider:
    def __init__(self, items=None, status=None, warnings=None):
        self.items = items or []
        self.status = status or {}
        self.warnings = warnings or []
        self.called_limit = None

    async def get_latest_news(self, limit=80):
        self.called_limit = limit
        return self.items, self.status, self.warnings


class ExplodingKnowledgeSession:
    async def execute(self, _query):
        raise AssertionError("TDX聚合快讯不应该读取项目每日资讯知识库")


class TdxPluginServiceTests(unittest.IsolatedAsyncioTestCase):
    def test_normalize_code_pads_short_tdx_stock_code(self):
        service = TdxPluginService()

        self.assertEqual(service._normalize_code("00090"), "000090")
        self.assertEqual(service._normalize_code("90"), "000090")
        self.assertEqual(service._normalize_code("CODE_000090"), "000090")

    async def test_limit_up_live_normalizes_events_and_response_shape(self):
        service = TdxPluginService()
        trade_date = date(2026, 5, 28)

        with patch.object(
            service.realtime_limit_up_service,
            "get_realtime_limit_up_list",
            AsyncMock(
                return_value=[
                    make_limit_up_item("001259", "利仁科技", "家电", board=7),
                    make_limit_up_item(
                        "002421",
                        "达实智能",
                        "AI应用",
                        sealed=False,
                        status="opened",
                        open_count=1,
                    ),
                    make_limit_up_item(
                        "603115",
                        "海星股份",
                        "机器人",
                        sealed=True,
                        status="resealed",
                        open_count=2,
                    ),
                ]
            ),
        ):
            payload = await service.get_limit_up_live(trade_date)

        self.assertEqual(payload["updated_at"][:10], "2026-05-28")
        self.assertFalse(payload["is_cache"])
        self.assertEqual(payload["source_status"]["limit_up_pool"], "ok")
        self.assertEqual(payload["items"][0]["stock_code"], "001259")
        self.assertEqual(payload["items"][0]["event_type"], "limit_up_sealed")
        self.assertEqual(payload["items"][0]["event_label"], "封死涨停")
        self.assertEqual(payload["items"][0]["target_status_label"], "7天7板")
        self.assertEqual(payload["items"][0]["target_plate"], "家电催化")
        self.assertEqual(payload["items"][0]["target_seal_amount"], "5000万")
        self.assertIn("plate_filters", payload)
        self.assertEqual(payload["plate_filters"][0]["name"], "家电催化")
        self.assertEqual(payload["items"][1]["event_label"], "涨停打开")
        self.assertEqual(payload["items"][2]["event_label"], "涨停回封")

    async def test_limit_up_live_sorts_latest_first_like_target_plugin(self):
        service = TdxPluginService()
        trade_date = date(2026, 5, 28)

        with patch.object(
            service.realtime_limit_up_service,
            "get_realtime_limit_up_list",
            AsyncMock(
                return_value=[
                    make_limit_up_item(
                        "001259",
                        "利仁科技",
                        "家电",
                        first_time=datetime(2026, 5, 28, 9, 35, 0),
                    ),
                    make_limit_up_item(
                        "600589",
                        "大位科技",
                        "算力租赁+AIDC",
                        first_time=datetime(2026, 5, 28, 14, 55, 37),
                    ),
                ]
            ),
        ):
            payload = await service.get_limit_up_live(trade_date)

        self.assertEqual([item["stock_code"] for item in payload["items"]], ["600589", "001259"])

    async def test_limit_up_live_status_uses_fast_pool_and_skips_slow_attribution(self):
        attribution_provider = FakeAttributionProvider({
            "605177": PublicStockAttribution(
                stock_code="605177",
                stock_name="东亚药业",
                reason_title="医药(原料药)",
                plate="医药",
                concepts=["原料药", "医药"],
                source_name="复盘网/同花顺F10",
            )
        })
        service = TdxPluginService(
            attribution_provider=attribution_provider,
            enable_external_sources=True,
        )
        trade_date = date(2026, 5, 29)

        with patch.object(
            service.realtime_limit_up_service,
            "get_fast_limit_up_pool",
            AsyncMock(return_value=[make_limit_up_item("605177", "东亚药业", "化学制药", board=1)]),
        ) as fast_pool, patch.object(
            service.realtime_limit_up_service,
            "get_realtime_limit_up_list",
            AsyncMock(),
        ) as rich_list:
            payload = await service.get_limit_up_live_status(trade_date)

        fast_pool.assert_awaited_once()
        self.assertFalse(fast_pool.await_args.kwargs["wait_for_refresh"])
        rich_list.assert_not_called()
        self.assertEqual(attribution_provider.requested_codes, None)
        self.assertEqual(payload["source_status"]["limit_up_status"], "ok")
        self.assertEqual(payload["source_status"]["public_attribution"], "skipped")
        self.assertEqual(payload["items"][0]["stock_code"], "605177")
        self.assertEqual(payload["items"][0]["reason"], "化学制药催化")
        self.assertNotEqual(payload["items"][0]["target_reason_summary"], "医药+原料药")

    async def test_limit_up_live_defaults_to_today_not_latest_database_date(self):
        class FixedDate(date):
            @classmethod
            def today(cls):
                return cls(2026, 6, 3)

        service = TdxPluginService()
        db = SequencedSession([FakeScalarResult(date(2026, 6, 2))])

        with patch("app.services.tdx_plugin_service.date", FixedDate), patch.object(
            service.realtime_limit_up_service,
            "get_realtime_limit_up_list",
            AsyncMock(return_value=[]),
        ) as live_pool, patch.object(
            service,
            "_load_limit_up_records_from_db",
            AsyncMock(return_value=[]),
        ) as load_records:
            payload = await service.get_limit_up_live(db=db)

        self.assertEqual(payload["updated_at"][:10], "2026-06-03")
        live_pool.assert_awaited_once_with(date(2026, 6, 3))
        load_records.assert_awaited_once()
        self.assertEqual(load_records.await_args.args[0], date(2026, 6, 3))
        self.assertIn("2026-06-03 暂无涨停播报数据", payload["warnings"])

    async def test_limit_up_live_uses_first_seal_time_for_target_first_seal_column(self):
        service = TdxPluginService()
        trade_date = date(2026, 5, 28)

        with patch.object(
            service.realtime_limit_up_service,
            "get_realtime_limit_up_list",
            AsyncMock(
                return_value=[
                    make_limit_up_item(
                        "002056",
                        "横店东磁",
                        "AI算力+磁性材料+光伏",
                        first_time=datetime(2026, 5, 28, 14, 21, 45),
                        final_time=datetime(2026, 5, 28, 14, 56, 57),
                        open_count=7,
                    ),
                    make_limit_up_item(
                        "603989",
                        "艾华集团",
                        "MLPC+铝电解电容+AI服务器+新能源",
                        first_time=datetime(2026, 5, 28, 14, 55, 37),
                        final_time=datetime(2026, 5, 28, 14, 55, 55),
                    ),
                ]
            ),
        ):
            payload = await service.get_limit_up_live(trade_date)

        self.assertEqual([item["stock_code"] for item in payload["items"]], ["603989", "002056"])
        self.assertEqual(payload["items"][1]["event_time"], "14:21:45")

    async def test_limit_up_live_prefers_historical_interval_board_label(self):
        service = TdxPluginService()
        trade_date = date(2026, 5, 28)
        db = SequencedSession([
            FakeRowsResult([
                ("603989", date(2026, 5, 18)),
                ("603989", date(2026, 5, 20)),
                ("603989", date(2026, 5, 22)),
                ("603989", date(2026, 5, 27)),
                ("603989", date(2026, 5, 28)),
            ]),
            FakeRowsResult([
                (date(2026, 5, 18),),
                (date(2026, 5, 19),),
                (date(2026, 5, 20),),
                (date(2026, 5, 21),),
                (date(2026, 5, 22),),
                (date(2026, 5, 25),),
                (date(2026, 5, 26),),
                (date(2026, 5, 27),),
                (date(2026, 5, 28),),
            ]),
        ])

        with patch.object(
            service.realtime_limit_up_service,
            "get_realtime_limit_up_list",
            AsyncMock(return_value=[make_limit_up_item("603989", "艾华集团", "电阻电容+数据中心", board=2)]),
        ):
            payload = await service.get_limit_up_live(trade_date, db=db)

        self.assertEqual(payload["items"][0]["target_status_label"], "9天5板")

    def test_target_plate_maps_reasons_to_target_like_major_theme(self):
        service = TdxPluginService()

        self.assertEqual(service._target_plate("电阻电容+数据中心"), "元器件")
        self.assertEqual(service._target_plate("光模块+PCB铜箔"), "通信")
        self.assertEqual(service._target_plate("算力租赁+AIDC+东数西算"), "算力")
        self.assertEqual(service._target_plate("商业航天+玻璃微纤维+半导体洁净"), "商业航天")
        self.assertEqual(service._target_plate("BOPP新能源膜+薄膜电容器+固态电池"), "锂电池")
        self.assertEqual(service._target_plate("压缩空气储能+中石协交流+国企改革"), "储能")
        self.assertEqual(service._target_plate("首次回购+算力+AI手机"), "端侧AI")
        self.assertEqual(service._target_plate("折叠屏+AI眼镜"), "消费电子")
        self.assertEqual(service._target_plate("薄膜电容器+PET铜箔+国企"), "元器件")
        self.assertEqual(service._target_plate("铜箔+冷板组件+固态电池+铜加工龙头"), "通信")
        self.assertEqual(service._target_plate("电子布纺织机+纺织机器人+固态电池+高端纺机"), "通信")
        self.assertEqual(service._target_plate("培育钻石+CVD金刚石+热电联产"), "金刚石概念")
        self.assertEqual(service._target_plate("红外光学+先进封装+卫星太阳能电池+一季报增长"), "芯片")
        self.assertEqual(service._target_plate("特高压+电容膜+国企改革"), "智能电网")
        self.assertEqual(service._target_plate("固态电池+半导体设备+海外拓展"), "芯片")
        self.assertEqual(service._target_plate("MLCC+商业航天+光通信"), "元器件")
        self.assertEqual(service._target_plate("磷化铟涨价+光纤级锗产品+半导体材料"), "芯片")
        self.assertEqual(service._target_plate("铂抗癌药+铂族回收+云南国资"), "有色金属")
        self.assertEqual(service._target_plate("元件"), "元器件")
        self.assertEqual(service._target_plate("绿电+承诺不减持+浙江国资"), "电力")
        self.assertEqual(service._target_plate("世界杯+IP文创+AI应用"), "世界杯概念")

    def test_target_reason_summary_formats_like_target_board_column(self):
        service = TdxPluginService()

        self.assertEqual(service._target_reason_summary("字节算力+算力租赁+数据中心"), "算力(算力租赁)")
        self.assertEqual(service._target_reason_summary("电阻电容+数据中心（MLPC）+算力+充电桩"), "电阻电容+数据中心")
        self.assertEqual(service._target_reason_summary("HVDC+机器人+算电协同+光伏组件+稀土永磁+锂电池"), "稀土永磁+元器件")
        self.assertEqual(service._target_reason_summary("金刚石散热+供暖+热电联产+能源资产代币化"), "金刚石概念")
        self.assertEqual(service._target_reason_summary("AI算力+磁性材料+光伏"), "元器件+光伏")
        self.assertEqual(service._target_reason_summary("商业航天+玻璃微纤维+半导体洁净"), "商业航天+半导体")
        self.assertEqual(service._target_reason_summary("BOPP新能源膜+薄膜电容器+固态电池"), "锂电池+固态电池")
        self.assertEqual(service._target_reason_summary("首次回购+算力+AI手机"), "端侧AI+AI手机")

    async def test_plate_strength_groups_limit_up_items_into_ranked_board(self):
        service = TdxPluginService()

        with patch.object(
            service.realtime_limit_up_service,
            "get_realtime_limit_up_list",
            AsyncMock(
                return_value=[
                    make_limit_up_item("001259", "利仁科技", "家电", board=7),
                    make_limit_up_item("603311", "金海高科", "家电", board=3),
                    make_limit_up_item("002421", "达实智能", "AI应用", board=3, sealed=False),
                ]
            ),
        ):
            payload = await service.get_plate_strength(date(2026, 5, 28))

        self.assertEqual(payload["items"][0]["plate_name"], "家电")
        self.assertEqual(payload["items"][0]["limit_up_count"], 2)
        self.assertEqual(payload["items"][0]["sealed_count"], 2)
        self.assertEqual(payload["items"][0]["max_board"], 7)
        self.assertEqual(payload["items"][0]["core_stocks"][0]["stock_name"], "利仁科技")
        self.assertGreater(payload["items"][0]["strength_score"], payload["items"][1]["strength_score"])

    async def test_stock_move_combines_limit_up_reason_and_metadata(self):
        service = TdxPluginService()

        with patch.object(
            service.realtime_limit_up_service,
            "get_realtime_limit_up_item",
            AsyncMock(return_value=make_limit_up_item("001259", "利仁科技", "家电", board=7)),
        ):
            payload = await service.get_stock_move("001259", date(2026, 5, 28), source_scope="mixed")

        self.assertEqual(payload["items"][0]["stock_code"], "001259")
        self.assertEqual(payload["items"][0]["stock_name"], "利仁科技")
        self.assertEqual(payload["items"][0]["source_scope"], "mixed")
        self.assertEqual(payload["items"][0]["latest_limit_up"]["board"], 7)
        self.assertEqual(payload["items"][0]["reasons"][0]["title"], "家电催化")
        self.assertIn("家电催化", payload["items"][0]["reasons"][0]["content"])
        self.assertEqual(payload["source_status"]["stock_move"], "ok")

    async def test_stock_move_prefers_public_move_source_for_target_like_reason_text(self):
        service = TdxPluginService(
            external_move_provider=FakeExternalMoveProvider(
                stock_move=ExternalStockMove(
                    stock_code="600589",
                    stock_name="大位科技",
                    trade_date=date(2026, 5, 28),
                    title="字节算力+算力租赁+数据中心",
                    content="1、字节资本开支扩建数据中心。\n2、公司提供租赁服务。\n3、森华易腾提供IDC服务。",
                    source_name="芦苇复盘",
                )
            ),
            enable_external_sources=True,
        )

        with patch.object(
            service.realtime_limit_up_service,
            "get_realtime_limit_up_item",
            AsyncMock(return_value=make_limit_up_item("600589", "大位科技", "算力租赁+AIDC", board=1)),
        ):
            payload = await service.get_stock_move("600589", date(2026, 5, 28), source_scope="mixed")

        self.assertEqual(payload["source_status"]["lwwhy_move"], "ok")
        self.assertIn("芦苇复盘", payload["items"][0]["sources"])
        self.assertEqual(payload["items"][0]["reasons"][0]["title"], "字节算力+算力租赁+数据中心")
        self.assertIn("森华易腾", payload["items"][0]["reasons"][0]["content"])

    async def test_stock_move_reuses_payload_cache_for_arbitrary_tdx_stock(self):
        provider = FakeExternalMoveProvider(
            stock_move=ExternalStockMove(
                stock_code="603677",
                stock_name="奇精机械",
                trade_date=date(2026, 5, 29),
                title="机器人+宁波国资+家电零部件+冷锻工艺",
                content="1、公司机器人零部件获得定点。",
                source_name="芦苇复盘",
            )
        )
        service = TdxPluginService(
            external_move_provider=provider,
            enable_external_sources=True,
        )

        with patch.object(
            service.realtime_limit_up_service,
            "get_realtime_limit_up_item",
            AsyncMock(return_value=None),
        ) as mocked_get_item:
            first = await service.get_stock_move("603677", date(2026, 5, 29), source_scope="mixed")
            provider.stock_move = ExternalStockMove(
                stock_code="603677",
                stock_name="奇精机械",
                trade_date=date(2026, 5, 29),
                title="不应该二次阻塞刷新",
                content="二次请求应直接使用缓存。",
                source_name="芦苇复盘",
            )
            second = await service.get_stock_move("603677", date(2026, 5, 29), source_scope="mixed")

        self.assertFalse(first["is_cache"])
        self.assertTrue(second["is_cache"])
        self.assertEqual(provider.stock_move_calls, 1)
        mocked_get_item.assert_awaited_once_with("603677", date(2026, 5, 29))
        self.assertEqual(second["source_status"]["stock_move_cache"], "hit")
        self.assertEqual(second["items"][0]["reasons"][0]["title"], "机器人+宁波国资+家电零部件+冷锻工艺")

    async def test_stock_move_does_not_block_external_result_on_slow_live_metadata(self):
        provider = FakeExternalMoveProvider(
            stock_move=ExternalStockMove(
                stock_code="002576",
                stock_name="通达动力",
                trade_date=date(2025, 8, 20),
                title="机器人+核心客户比亚迪+驱动电机铁芯+新能源汽车",
                content="1、公司生产的伺服电机铁芯可适用于机器人领域。",
                source_name="打板客/同花顺F10",
            )
        )
        service = TdxPluginService(
            external_move_provider=provider,
            enable_external_sources=True,
            stock_move_live_timeout=0.01,
        )

        async def slow_live_item(*args, **kwargs):
            await asyncio.sleep(1)
            return make_limit_up_item("002576", "通达动力", "机器人", board=1)

        with patch.object(service.realtime_limit_up_service, "get_realtime_limit_up_item", slow_live_item):
            payload = await service.get_stock_move("002576", date(2026, 5, 30), source_scope="mixed")

        self.assertIsNone(payload["items"][0]["latest_limit_up"])
        self.assertEqual(payload["items"][0]["reasons"][0]["title"], "机器人+核心客户比亚迪+驱动电机铁芯+新能源汽车")
        self.assertEqual(payload["source_status"]["stock_move_live"], "timeout")

    async def test_limit_up_live_keeps_review_source_as_supplement_without_overriding_live_reason(self):
        service = TdxPluginService(
            external_move_provider=FakeExternalMoveProvider(
                review_moves=[
                    ExternalStockMove(
                        stock_code="603989",
                        stock_name="艾华集团",
                        trade_date=date(2026, 5, 28),
                        title="电阻电容+数据中心（MLPC）+算力+充电桩",
                        content="1、AI服务器供电。",
                        board_label="9天5板",
                        source_name="芦苇复盘",
                    )
                ]
            ),
            enable_external_sources=True,
        )

        with patch.object(
            service.realtime_limit_up_service,
            "get_realtime_limit_up_list",
            AsyncMock(return_value=[make_limit_up_item("603989", "艾华集团", "元器件", board=2)]),
        ):
            payload = await service.get_limit_up_live(date(2026, 5, 28))

        self.assertEqual(payload["source_status"]["lwwhy_review"], "ok")
        self.assertEqual(payload["items"][0]["target_status_label"], "2天2板")
        self.assertEqual(payload["items"][0]["reason"], "元器件催化")
        self.assertIn("芦苇复盘", payload["items"][0]["sources"])

    async def test_limit_up_live_uses_fupan_and_ths_attribution_for_target_board_text(self):
        service = TdxPluginService(
            attribution_provider=FakeAttributionProvider({
                "605177": PublicStockAttribution(
                    stock_code="605177",
                    stock_name="东亚药业",
                    reason_title="医药(原料药)",
                    plate="医药",
                    concepts=["原料药", "医药"],
                    source_name="复盘网/同花顺F10",
                )
            }),
            enable_external_sources=True,
        )
        trade_date = date(2026, 5, 29)

        with patch.object(
            service.realtime_limit_up_service,
            "get_realtime_limit_up_list",
            AsyncMock(return_value=[make_limit_up_item("605177", "东亚药业", "化学制药", board=1)]),
        ):
            payload = await service.get_limit_up_live(trade_date)

        self.assertEqual(payload["source_status"]["public_attribution"], "ok")
        self.assertEqual(payload["items"][0]["reason"], "医药(原料药)")
        self.assertEqual(payload["items"][0]["target_plate"], "医药")
        self.assertEqual(payload["items"][0]["target_reason_summary"], "医药+原料药")
        self.assertIn("复盘网/同花顺F10", payload["items"][0]["sources"])

    async def test_limit_up_live_does_not_let_review_source_override_target_status_label(self):
        service = TdxPluginService(
            external_move_provider=FakeExternalMoveProvider(
                review_moves=[
                    ExternalStockMove(
                        stock_code="605177",
                        stock_name="东亚药业",
                        trade_date=date(2026, 5, 29),
                        title="医药(原料药)",
                        content="公开涨停原因",
                        board_label="2天2板",
                        source_name="芦苇复盘",
                    )
                ]
            ),
            enable_external_sources=True,
        )
        trade_date = date(2026, 5, 29)

        with patch.object(
            service.realtime_limit_up_service,
            "get_realtime_limit_up_list",
            AsyncMock(return_value=[make_limit_up_item("605177", "东亚药业", "化学制药", board=1)]),
        ):
            payload = await service.get_limit_up_live(trade_date)

        self.assertEqual(payload["items"][0]["target_status_label"], "首板")

    async def test_limit_up_live_falls_back_to_database_records_when_realtime_pool_empty(self):
        service = TdxPluginService()
        trade_date = date(2026, 5, 28)
        db = SequencedSession([
            FakeRowsResult([
                (
                    "001259",
                    "利仁科技",
                    "家电",
                    datetime(2026, 5, 28, 9, 35, 0),
                    "家电+小家电",
                    "家电",
                    7,
                    0,
                    True,
                    "sealed",
                    datetime(2026, 5, 28, 9, 35, 0),
                    12345.0,
                    800000.0,
                    12.3,
                    "DB",
                )
            ]),
            FakeRowsResult([("001259", trade_date)]),
            FakeRowsResult([(trade_date,)]),
        ])

        with patch.object(
            service.realtime_limit_up_service,
            "get_realtime_limit_up_list",
            AsyncMock(return_value=[]),
        ):
            payload = await service.get_limit_up_live(trade_date, db=db)

        self.assertTrue(payload["is_cache"])
        self.assertEqual(payload["source_status"]["limit_up_pool"], "empty")
        self.assertEqual(payload["source_status"]["limit_up_db"], "ok")
        self.assertEqual(payload["items"][0]["stock_code"], "001259")
        self.assertEqual(payload["items"][0]["stock_name"], "利仁科技")
        self.assertIn("数据库兜底", payload["warnings"][0])

    async def test_stock_move_defaults_to_latest_available_trade_date(self):
        service = TdxPluginService()
        db = SequencedSession([FakeScalarResult(date(2026, 5, 28))])

        with patch.object(
            service.realtime_limit_up_service,
            "get_realtime_limit_up_item",
            AsyncMock(return_value=make_limit_up_item("600589", "大位科技", "算力租赁+AIDC", board=1)),
        ) as mocked_get_item:
            payload = await service.get_stock_move("600589", db=db)

        mocked_get_item.assert_awaited_once_with("600589", date(2026, 5, 28))
        self.assertEqual(payload["items"][0]["trade_date"], "2026-05-28")

    async def test_stock_move_pads_short_code_before_lookup(self):
        service = TdxPluginService()

        with patch.object(
            service.realtime_limit_up_service,
            "get_realtime_limit_up_item",
            AsyncMock(return_value=make_limit_up_item("000090", "天健集团", "深圳国资+房地产", board=1)),
        ) as mocked_get_item:
            payload = await service.get_stock_move("00090", date(2026, 5, 28), source_scope="mixed")

        mocked_get_item.assert_awaited_once_with("000090", date(2026, 5, 28))
        self.assertEqual(payload["items"][0]["stock_code"], "000090")
        self.assertEqual(payload["items"][0]["stock_name"], "天健集团")

    async def test_ths_move_marks_ths_only_scope(self):
        service = TdxPluginService()

        with patch.object(
            service.realtime_limit_up_service,
            "get_realtime_limit_up_item",
            AsyncMock(return_value=make_limit_up_item("001259", "利仁科技", "家电", board=7)),
        ):
            payload = await service.get_stock_move("001259", date(2026, 5, 28), source_scope="ths")

        self.assertEqual(payload["items"][0]["source_scope"], "ths")
        self.assertEqual(payload["items"][0]["sources"], ["同花顺"])

    def test_news_item_formats_update_time_as_timeline_clock(self):
        service = TdxPluginService()
        item = service._build_news_item(SimpleNamespace(
            id=1,
            update_time="1779900042635",
            created_at=None,
            title="测试快讯",
            source_name="同花顺",
            abstract="内容",
            introduction="",
            content_text="",
            jump_url=None,
        ))

        self.assertRegex(item["time"], r"^\d{2}:\d{2}:\d{2}$")

    async def test_news_uses_public_market_news_provider_instead_of_knowledge_documents(self):
        provider = FakeNewsProvider(
            items=[
                {
                    "news_id": "ths-1",
                    "time": "10:08:09",
                    "source": "同花顺",
                    "title": "A股异动",
                    "content": "市场消息",
                    "importance": 74,
                    "related_stocks": ["000090"],
                    "related_plates": ["房地产"],
                    "jump_url": "https://news.10jqka.com.cn/",
                }
            ],
            status={"ths": "ok", "cls": "empty", "jygs": "ok"},
        )
        service = TdxPluginService(news_provider=provider)

        payload = await service.get_news(ExplodingKnowledgeSession(), limit=10)

        self.assertEqual(provider.called_limit, 10)
        self.assertEqual(payload["items"][0]["source"], "同花顺")
        self.assertNotIn("knowledge_news", payload["source_status"])
        self.assertEqual(payload["source_status"]["ths"], "ok")

    def test_compare_samples_reports_missing_extra_field_and_order_differences(self):
        service = TdxPluginService()

        payload = service.compare_samples(
            target_items=[
                {"stock_code": "001259", "stock_name": "利仁科技", "event_label": "封死涨停"},
                {"stock_code": "002421", "stock_name": "达实智能", "event_label": "涨停打开"},
            ],
            ours_items=[
                {"stock_code": "002421", "stock_name": "达实智能", "event_label": "封死涨停"},
                {"stock_code": "603311", "stock_name": "金海高科", "event_label": "封死涨停"},
            ],
            key_field="stock_code",
        )

        self.assertEqual(payload["summary"]["target_count"], 2)
        self.assertEqual(payload["summary"]["ours_count"], 2)
        self.assertEqual(payload["missing_items"][0]["stock_code"], "001259")
        self.assertEqual(payload["extra_items"][0]["stock_code"], "603311")
        self.assertEqual(payload["field_diffs"][0]["field"], "event_label")
        self.assertEqual(payload["order_diffs"][0]["key"], "002421")


class TdxStockMovePersistentCacheTests(unittest.IsolatedAsyncioTestCase):
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

    async def test_stock_move_reads_persistent_cache_before_external_sources(self):
        cached_payload = {
            "items": [
                {
                    "stock_code": "603677",
                    "stock_name": "奇精机械",
                    "trade_date": "2026-05-29",
                    "source_scope": "mixed",
                    "reasons": [{"title": "机器人+宁波国资", "content": "本地种子缓存"}],
                    "sources": ["seed"],
                }
            ],
            "updated_at": "2026-05-29T18:00:00",
            "source_status": {"seed": "ok"},
            "is_cache": False,
            "warnings": [],
        }
        provider = FakeExternalMoveProvider(
            stock_move=ExternalStockMove(
                stock_code="603677",
                stock_name="奇精机械",
                trade_date=date(2026, 5, 29),
                title="不应该调用外部源",
                content="不应该出现",
            )
        )
        service = TdxPluginService(external_move_provider=provider, enable_external_sources=True)

        async with self.Session() as session:
            session.add(
                TdxStockMoveCache(
                    stock_code="603677",
                    source_scope="mixed",
                    trade_date=date(2026, 5, 29),
                    stock_name="奇精机械",
                    payload_json=cached_payload,
                    source_status={"seed": "ok"},
                    warnings=[],
                    generated_at=datetime(2026, 5, 29, 18, 0, 0),
                )
            )
            await session.commit()

            with patch.object(
                service.realtime_limit_up_service,
                "get_realtime_limit_up_item",
                AsyncMock(),
            ) as mocked_get_item:
                payload = await service.get_stock_move("603677", date(2026, 5, 29), source_scope="mixed", db=session)

        self.assertTrue(payload["is_cache"])
        self.assertEqual(payload["source_status"]["stock_move_cache"], "persistent_hit")
        self.assertEqual(payload["items"][0]["reasons"][0]["title"], "机器人+宁波国资")
        self.assertEqual(provider.stock_move_calls, 0)
        mocked_get_item.assert_not_called()

    async def test_stock_move_persists_successful_payload_for_later_clicks(self):
        service = TdxPluginService(
            external_move_provider=FakeExternalMoveProvider(
                stock_move=ExternalStockMove(
                    stock_code="002576",
                    stock_name="通达动力",
                    trade_date=date(2026, 5, 30),
                    title="机器人+驱动电机铁芯",
                    content="公司电机铁芯可用于机器人。",
                    source_name="打板客/同花顺F10",
                )
            ),
            enable_external_sources=True,
        )

        async with self.Session() as session:
            with patch.object(
                service.realtime_limit_up_service,
                "get_realtime_limit_up_item",
                AsyncMock(return_value=None),
            ):
                payload = await service.get_stock_move("002576", date(2026, 5, 30), source_scope="mixed", db=session)

            result = await session.execute(
                select(TdxStockMoveCache).where(
                    TdxStockMoveCache.stock_code == "002576",
                    TdxStockMoveCache.source_scope == "mixed",
                    TdxStockMoveCache.trade_date == date(2026, 5, 30),
                )
            )
            cached = result.scalar_one_or_none()

        self.assertIsNotNone(cached)
        self.assertEqual(cached.payload_json["items"][0]["reasons"][0]["title"], "机器人+驱动电机铁芯")
        self.assertFalse(payload["is_cache"])


if __name__ == "__main__":
    unittest.main()
