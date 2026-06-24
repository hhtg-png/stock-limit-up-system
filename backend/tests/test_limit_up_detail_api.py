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


def make_stock(
    stock_id=1,
    stock_code="002466",
    stock_name="天齐锂业",
    market="SZ",
    industry="有色金属",
):
    return SimpleNamespace(
        id=stock_id,
        stock_code=stock_code,
        stock_name=stock_name,
        market=market,
        industry=industry,
    )


def make_record(
    trade_date,
    record_id=10,
    stock_id=1,
    continuous_limit_up_days=1,
    seal_amount=12345.0,
    current_status="sealed",
    first_limit_up_time=datetime(2026, 5, 8, 10, 12, 30),
    final_seal_time=datetime(2026, 5, 8, 14, 30, 0),
    open_count=0,
    limit_up_price=42.5,
    close_price=None,
):
    if close_price is None:
        close_price = limit_up_price

    return SimpleNamespace(
        id=record_id,
        stock_id=stock_id,
        trade_date=trade_date,
        first_limit_up_time=first_limit_up_time,
        final_seal_time=final_seal_time,
        limit_up_reason="锂电池",
        reason_category="题材",
        continuous_limit_up_days=continuous_limit_up_days,
        open_count=open_count,
        is_final_sealed=True,
        current_status=current_status,
        seal_amount=seal_amount,
        seal_volume=None,
        limit_up_price=limit_up_price,
        close_price=close_price,
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
            FakeAllResult([(stock.id, True, 2)]),
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

    async def test_get_realtime_limit_up_uses_market_review_continuous_days_when_available(self):
        today = date(2026, 6, 18)
        requested_date = date(2026, 5, 13)
        stock = make_stock()
        record = make_record(
            requested_date,
            continuous_limit_up_days=5,
            seal_amount=5000,
            current_status="sealed",
        )
        db = SequencedSession([
            FakeScalarResult(1),
            FakeAllResult([(record, stock)]),
            FakeAllResult([(stock.id, True, 6)]),
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

        self.assertEqual(response.data[0].continuous_limit_up_days, 6)
        self.assertEqual(len(db.queries), 3)

    async def test_get_realtime_limit_up_keeps_one_word_second_board_with_empty_seal_amount(self):
        today = date(2026, 6, 18)
        requested_date = date(2026, 6, 17)
        stock = make_stock()
        record = make_record(
            requested_date,
            continuous_limit_up_days=2,
            seal_amount=0,
            current_status="sealed",
            first_limit_up_time=datetime(2026, 6, 17, 9, 25, 3),
            final_seal_time=datetime(2026, 6, 17, 9, 25, 3),
            open_count=0,
        )
        db = SequencedSession([
            FakeScalarResult(1),
            FakeAllResult([(record, stock)]),
            FakeAllResult([(stock.id, True, 2)]),
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
                status="sealed",
                sort_by="seal_amount",
                sort_order="desc",
                db=db,
            )

        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0].continuous_limit_up_days, 2)
        self.assertTrue(response.data[0].is_one_word)
        self.assertEqual(response.data[0].seal_amount, 0)

    async def test_get_realtime_limit_up_treats_market_review_broken_row_as_first_board(self):
        today = date(2026, 6, 18)
        requested_date = date(2026, 5, 14)
        stock = make_stock()
        record = make_record(
            requested_date,
            continuous_limit_up_days=6,
            seal_amount=0,
            current_status="opened",
        )
        record.is_final_sealed = False
        record.final_seal_time = None
        db = SequencedSession([
            FakeScalarResult(1),
            FakeAllResult([(record, stock)]),
            FakeAllResult([(stock.id, False, 6)]),
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
                sort_by="time",
                db=db,
            )

        self.assertEqual(response.data, [])

    async def test_get_realtime_limit_up_filters_first_board_and_price_range_from_database(self):
        today = date(2026, 6, 18)
        requested_date = date(2026, 6, 17)
        low_first_stock = make_stock(1, "001001", "低价首板")
        mid_second_stock = make_stock(2, "001002", "中价二板")
        high_first_stock = make_stock(3, "001003", "高价首板")
        rows = [
            (
                make_record(
                    requested_date,
                    record_id=1,
                    stock_id=1,
                    continuous_limit_up_days=1,
                    limit_up_price=18.8,
                ),
                low_first_stock,
            ),
            (
                make_record(
                    requested_date,
                    record_id=2,
                    stock_id=2,
                    continuous_limit_up_days=2,
                    limit_up_price=18.9,
                ),
                mid_second_stock,
            ),
            (
                make_record(
                    requested_date,
                    record_id=3,
                    stock_id=3,
                    continuous_limit_up_days=1,
                    limit_up_price=120.0,
                ),
                high_first_stock,
            ),
        ]
        db = SequencedSession([
            FakeScalarResult(1),
            FakeAllResult(rows),
            FakeAllResult([(1, True, 1), (2, True, 2), (3, True, 1)]),
        ])

        with patch("app.api.v1.limit_up.today_cn", return_value=today), patch.object(
            limit_up.realtime_limit_up_service,
            "get_realtime_limit_up_list",
            AsyncMock(side_effect=AssertionError("historical date should read database first")),
        ):
            response = await limit_up.get_realtime_limit_up(
                requested_date,
                continuous_days=None,
                continuous_days_exact=1,
                reason_category=None,
                status=None,
                min_price=1,
                max_price=20,
                sort_by="time",
                sort_order="asc",
                db=db,
            )

        self.assertEqual([item.stock_code for item in response.data], ["001001"])

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
            FakeAllResult([]),
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
