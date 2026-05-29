import unittest
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from app.services.tdx_external_sources import ExternalStockMove
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

    async def get_stock_move(self, stock_code, trade_date=None):
        return self.stock_move

    async def get_review_moves(self, trade_date):
        return self.review_moves


class TdxPluginServiceTests(unittest.IsolatedAsyncioTestCase):
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

    async def test_limit_up_live_uses_public_review_source_for_board_and_reason(self):
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
        self.assertEqual(payload["items"][0]["target_status_label"], "9天5板")
        self.assertEqual(payload["items"][0]["reason"], "电阻电容+数据中心（MLPC）+算力+充电桩")
        self.assertIn("芦苇复盘", payload["items"][0]["sources"])

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


if __name__ == "__main__":
    unittest.main()
