import unittest
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from app.api.v1 import limit_up


class FakeScalarResult:
    def __init__(self, value):
        self.value = value

    def scalar_one_or_none(self):
        return self.value


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

    async def execute(self, query):
        self.queries.append(query)
        return self.results.pop(0)


def make_stock():
    return SimpleNamespace(
        id=1,
        stock_code="002466",
        stock_name="天齐锂业",
        market="SZ",
        industry="有色金属",
    )


def make_record(trade_date):
    return SimpleNamespace(
        id=10,
        stock_id=1,
        trade_date=trade_date,
        first_limit_up_time=datetime(2026, 5, 8, 10, 12, 30),
        final_seal_time=datetime(2026, 5, 8, 14, 30, 0),
        limit_up_reason="锂电池",
        reason_category="题材",
        continuous_limit_up_days=1,
        open_count=0,
        is_final_sealed=True,
        current_status="sealed",
        seal_amount=12345.0,
        limit_up_price=42.5,
        turnover_rate=8.6,
        amount=98765.0,
        data_source="TEST",
    )


class LimitUpDetailApiTests(unittest.IsolatedAsyncioTestCase):
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
