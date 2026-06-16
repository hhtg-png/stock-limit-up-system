import unittest
from datetime import date, datetime
from unittest.mock import AsyncMock, patch

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.api.v1 import limit_up
from app.database import Base
from app.models.limit_up import LimitUpClassificationDigest
from app.services.ths_limit_up_classification_service import ThsLimitUpClassificationService


class FakeRealtimeLimitUpService:
    def __init__(self, items):
        self.items = items
        self.requested_dates = []

    async def get_realtime_limit_up_list(self, trade_date):
        self.requested_dates.append(trade_date)
        return self.items


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

    async def classify_limit_up_reasons(self, trade_date, stocks):
        self.calls.append((trade_date, [stock["stock_code"] for stock in stocks]))
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
        trade_date = date(2026, 6, 15)
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
        trade_date = date(2026, 6, 15)
        service = ThsLimitUpClassificationService(
            realtime_service=FakeRealtimeLimitUpService(
                [
                    make_realtime_item(
                        "603000",
                        "人民网",
                        "AI大模型+数据要素",
                        first_time=datetime(2026, 6, 15, 9, 31, 20),
                        final_time=datetime(2026, 6, 15, 9, 45, 5),
                        board=2,
                    ),
                    make_realtime_item(
                        "002230",
                        "科大讯飞",
                        "人工智能+机器人",
                        first_time=datetime(2026, 6, 15, 10, 2, 1),
                        sealed=False,
                        open_count=1,
                    ),
                    make_realtime_item(
                        "300750",
                        "宁德时代",
                        "锂电池+储能",
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
        self.assertEqual(ai_group["plate_name"], "人工智能")
        self.assertEqual(ai_group["count"], 2)
        self.assertEqual(ai_group["sealed_count"], 1)
        self.assertEqual(ai_group["opened_count"], 1)
        self.assertEqual(ai_group["earliest_first_limit_time"], "09:31:20")
        self.assertEqual(ai_group["latest_first_limit_time"], "10:02:01")
        self.assertEqual([stock["stock_code"] for stock in ai_group["stocks"]], ["603000", "002230"])
        self.assertEqual(ai_group["stocks"][0]["first_limit_up_time"], "09:31:20")
        self.assertEqual(ai_group["stocks"][0]["final_seal_time"], "09:45:05")
        self.assertEqual(ai_group["stocks"][1]["current_status"], "opened")
        self.assertEqual(ai_group["stocks"][1]["final_seal_time"], "")

        new_energy_group = payload["groups"][1]
        self.assertEqual(new_energy_group["plate_name"], "新能源")
        self.assertEqual(new_energy_group["stocks"][0]["classified_plate"], "新能源")

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

        trade_date = date(2026, 6, 15)
        realtime = FakeRealtimeLimitUpService(
            [
                make_realtime_item(
                    "600406",
                    "国电南瑞",
                    "智能电网+特高压",
                    first_time=datetime(2026, 6, 15, 9, 36, 0),
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
        self.assertEqual(payload["groups"][0]["plate_name"], "人工智能")

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

        trade_date = date(2026, 6, 15)
        reason = "智能电网+特高压"
        realtime = FakeRealtimeLimitUpService(
            [
                make_realtime_item(
                    "600406",
                    "国电南瑞",
                    reason,
                    first_time=datetime(2026, 6, 15, 9, 36, 0),
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
        self.assertEqual(stock["rule_classified_plate"], "人工智能")
        self.assertEqual(stock["classification_method"], "ai")
        self.assertEqual(stock["ai_confidence"], 0.93)
        self.assertEqual(stock["ai_keywords"], ["智能电网", "特高压"])

        cached_stock = second_payload["groups"][0]["stocks"][0]
        self.assertEqual(cached_stock["classified_plate"], "电力设备")
        self.assertEqual(cached_stock["classification_method"], "ai")

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
        self.assertEqual(payload["groups"][0]["plate_name"], "人工智能")
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

            async def get_classification(self, requested_date, *, db=None, force_ai=False):
                self.calls.append((requested_date, force_ai))
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

        self.assertEqual(fake_service.calls, [(trade_date, False)])
        self.assertEqual(payload["source_status"]["ai_classification"], "refresh_scheduled")
        self.assertEqual(len(fake_background.tasks), 1)
        task_fn, task_args, task_kwargs = fake_background.tasks[0]
        self.assertEqual(task_fn, fake_service.rebuild_ai_classification_cache)
        self.assertEqual(task_args, (trade_date,))
        self.assertEqual(task_kwargs, {})


if __name__ == "__main__":
    unittest.main()
