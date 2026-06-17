import unittest
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from app.api.v1 import limit_up
from app.services.data_init_service import DataInitService


class FakeScalarResult:
    def __init__(self, value):
        self.value = value

    def scalar(self):
        return self.value

    def scalar_one_or_none(self):
        return self.value


class FakeAllResult:
    def __init__(self, rows):
        self.rows = rows

    def all(self):
        return self.rows


class FakeScalars:
    def __init__(self, values):
        self.values = values

    def all(self):
        return self.values


class FakeScalarsResult:
    def __init__(self, values):
        self.values = values

    def scalars(self):
        return FakeScalars(self.values)


class SequencedSession:
    def __init__(self, results):
        self.results = list(results)
        self.queries = []
        self.commit_count = 0

    async def execute(self, query):
        self.queries.append(query)
        return self.results.pop(0)

    async def commit(self):
        self.commit_count += 1


class FakeAsyncSessionContext:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, exc_type, exc, tb):
        return False


def make_stock():
    return SimpleNamespace(
        id=1,
        stock_code="002466",
        stock_name="天齐锂业",
        market="SZ",
        industry="有色金属",
    )


def make_record(
    trade_date,
    continuous_limit_up_days=1,
    seal_amount=12345.0,
    current_status="sealed",
):
    return SimpleNamespace(
        id=10,
        stock_id=1,
        trade_date=trade_date,
        first_limit_up_time=datetime(2026, 5, 8, 10, 12, 30),
        final_seal_time=datetime(2026, 5, 8, 14, 30, 0),
        limit_up_reason="锂电池",
        reason_category="题材",
        continuous_limit_up_days=continuous_limit_up_days,
        open_count=0,
        is_final_sealed=True,
        current_status=current_status,
        seal_amount=seal_amount,
        seal_volume=None,
        limit_up_price=42.5,
        close_price=42.5,
        turnover_rate=8.6,
        amount=98765.0,
        data_source="TEST",
    )


class LimitUpDetailApiTests(unittest.IsolatedAsyncioTestCase):
    def test_data_init_existing_record_update_persists_seal_fields(self):
        service = DataInitService()
        record = make_record(
            date(2026, 6, 17),
            continuous_limit_up_days=1,
            seal_amount=0,
            current_status="unknown",
        )

        service._update_existing_limit_up_record(
            record,
            {
                "seal_amount": 8888.0,
                "limit_up_price": 12.3,
                "amount": 99999.0,
                "turnover_rate": 4.2,
                "is_final_sealed": True,
                "open_count": 0,
                "continuous_limit_up_days": 2,
            },
            turnover_rate=4.2,
        )

        self.assertEqual(record.seal_amount, 8888.0)
        self.assertEqual(record.continuous_limit_up_days, 2)
        self.assertEqual(record.current_status, "sealed")

    async def test_refresh_limit_up_data_updates_seal_amount_without_marking_resealed_opened(self):
        trade_date = date(2026, 6, 17)
        stock = make_stock()
        record = make_record(
            trade_date,
            continuous_limit_up_days=2,
            seal_amount=0,
            current_status="opened",
        )
        db = SequencedSession([FakeAllResult([(record, stock)])])
        final_time = datetime(2026, 6, 17, 9, 25, 0)

        with patch(
            "app.api.v1.limit_up.async_session_maker",
            return_value=FakeAsyncSessionContext(db),
        ), patch(
            "app.crawlers.kaipanla_crawler.kpl_crawler.crawl",
            AsyncMock(return_value=[
                {
                    "stock_code": "002466",
                    "seal_amount": 8888.0,
                    "is_final_sealed": True,
                    "open_count": 1,
                    "final_seal_time": final_time,
                    "data_source": "KPL",
                }
            ]),
        ), patch(
            "app.crawlers.kaipanla_crawler.kpl_crawler.close_client",
            AsyncMock(),
        ), patch(
            "app.crawlers.tonghuashun_crawler.ths_crawler.crawl",
            AsyncMock(return_value=[]),
        ), patch(
            "app.crawlers.tonghuashun_crawler.ths_crawler.close_client",
            AsyncMock(),
        ):
            await limit_up._refresh_limit_up_data(trade_date)

        self.assertEqual(record.seal_amount, 8888.0)
        self.assertEqual(record.open_count, 1)
        self.assertEqual(record.current_status, "sealed")
        self.assertEqual(record.final_seal_time, final_time)
        self.assertEqual(db.commit_count, 1)

    async def test_get_realtime_limit_up_defaults_unknown_continuous_days_to_first_board(self):
        trade_date = date(2026, 6, 16)

        with patch("app.api.v1.limit_up.today_cn", return_value=trade_date), patch.object(
            limit_up.realtime_limit_up_service,
            "get_realtime_limit_up_list",
            AsyncMock(
                return_value=[
                    {
                        "stock_code": "603335",
                        "stock_name": "迪生力",
                        "first_limit_up_time": datetime(2026, 6, 16, 10, 12, 30),
                        "final_seal_time": None,
                        "limit_up_reason": "汽车零部件",
                        "reason_category": "汽车",
                        "continuous_limit_up_days": None,
                        "open_count": 1,
                        "is_final_sealed": False,
                        "limit_up_price": 6.72,
                        "current_price": 6.31,
                    }
                ]
            ),
        ):
            response = await limit_up.get_realtime_limit_up(
                trade_date,
                continuous_days=None,
                reason_category=None,
                sort_by="time",
                db=None,
            )

        self.assertEqual(response.data[0].continuous_limit_up_days, 1)

    async def test_get_realtime_limit_up_reads_historical_date_from_database_first(self):
        today = date(2026, 6, 18)
        requested_date = date(2026, 6, 16)
        previous_date = date(2026, 6, 15)
        stock = make_stock()
        record = make_record(
            requested_date,
            continuous_limit_up_days=2,
            seal_amount=5000,
            current_status="unknown",
        )
        db = SequencedSession([
            FakeScalarResult(1),
            FakeAllResult([(record, stock)]),
            FakeAllResult([(requested_date,), (previous_date,)]),
            FakeAllResult([
                (stock.id, requested_date, True),
                (stock.id, previous_date, True),
            ]),
        ])

        with patch("app.api.v1.limit_up.today_cn", return_value=today), patch.object(
            limit_up.realtime_limit_up_service,
            "get_realtime_limit_up_list",
            AsyncMock(side_effect=AssertionError("historical date should read database first")),
        ):
            response = await limit_up.get_realtime_limit_up(
                requested_date,
                continuous_days=2,
                reason_category=None,
                sort_by="seal_amount",
                db=db,
            )

        self.assertEqual(response.trade_date, requested_date)
        self.assertFalse(response.is_fallback)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0].stock_code, "002466")
        self.assertEqual(response.data[0].continuous_limit_up_days, 2)
        self.assertEqual(response.data[0].current_status, "sealed")

    async def test_get_realtime_limit_up_recomputes_polluted_historical_board_count(self):
        today = date(2026, 6, 18)
        requested_date = date(2026, 6, 15)
        previous_date = date(2026, 6, 12)
        stock = make_stock()
        record = make_record(
            requested_date,
            continuous_limit_up_days=8,
            seal_amount=0,
            current_status="sealed",
        )
        db = SequencedSession([
            FakeScalarResult(1),
            FakeAllResult([(record, stock)]),
            FakeAllResult([(requested_date,), (previous_date,)]),
            FakeAllResult([(stock.id, requested_date, True)]),
        ])

        with patch("app.api.v1.limit_up.today_cn", return_value=today), patch.object(
            limit_up.realtime_limit_up_service,
            "get_realtime_limit_up_list",
            AsyncMock(side_effect=AssertionError("historical date should read database first")),
        ):
            response = await limit_up.get_realtime_limit_up(
                requested_date,
                continuous_days=None,
                reason_category=None,
                sort_by="time",
                db=db,
            )

        self.assertEqual(response.data[0].continuous_limit_up_days, 1)

    async def test_get_limit_up_detail_falls_back_to_latest_available_record_date(self):
        requested_date = date(2026, 5, 12)
        fallback_date = date(2026, 5, 8)
        db = SequencedSession([
            FakeScalarResult(make_stock()),
            FakeScalarResult(make_record(fallback_date)),
            FakeScalarsResult([]),
        ])

        with patch.object(
            limit_up.realtime_limit_up_service,
            "get_realtime_limit_up_item",
            AsyncMock(return_value=None),
        ):
            response = await limit_up.get_limit_up_detail("002466", requested_date, db)

        self.assertEqual(response.trade_date, fallback_date)
        self.assertEqual(response.stock_code, "002466")

        record_query = db.queries[1]
        self.assertIn("limit_up_records.trade_date <= :trade_date_1", str(record_query))
        self.assertEqual(record_query.compile().params["trade_date_1"], requested_date)

    async def test_get_limit_up_detail_uses_requested_date_when_record_exists_on_requested_date(self):
        requested_date = date(2026, 5, 8)
        db = SequencedSession([
            FakeScalarResult(make_stock()),
            FakeScalarResult(make_record(requested_date)),
            FakeScalarsResult([]),
        ])

        with patch.object(
            limit_up.realtime_limit_up_service,
            "get_realtime_limit_up_item",
            AsyncMock(return_value=None),
        ):
            response = await limit_up.get_limit_up_detail("002466", requested_date, db)

        self.assertEqual(response.trade_date, requested_date)
        self.assertEqual(db.queries[1].compile().params["trade_date_1"], requested_date)


if __name__ == "__main__":
    unittest.main()
