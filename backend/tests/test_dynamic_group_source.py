import unittest
from datetime import date, datetime
from unittest.mock import AsyncMock

from app.services.dynamic_group_source_service import DynamicGroupSourceService


class DynamicGroupSourceTests(unittest.IsolatedAsyncioTestCase):
    def service(self, quotes, context=None, now=None, calendar=None):
        market = type("Market", (), {})()
        market.get_quotes_snapshot = AsyncMock(return_value={"codes": ["600001", "000001"],
            "quotes": quotes, "missing_codes": []})
        return DynamicGroupSourceService(market_service=market,
            now_provider=lambda: now or datetime(2026, 10, 26, 10, 30),
            calendar_loader=AsyncMock(return_value=calendar or [date(2026, 10, 22), date(2026, 10, 23), date(2026, 10, 26)]),
            context_loader=AsyncMock(return_value=context or {"previous": {}, "previous_complete": False,
                "today": {}, "today_complete": False}),
            topic_records={"600001": {"industry": "测试行业", "concept": ["机器人"]}})

    def quote(self, stamp="20261026102959", **extra):
        return dict(name="测试", price=10, pre_close=9, open=9.45, change_pct=11.11,
                    amount=35000, turnover_rate=6, circulating_value=25, limit_up=9.9,
                    bid1_volume=0, datetime=stamp, **extra)

    async def test_previous_trading_day_units_opening_and_context(self):
        context = {"previous": {"600001": {"is_final_sealed": True, "continuous_limit_up_days": 2,
            "first_limit_up_time": "09:40:00", "final_seal_time": "10:52:37"}},
            "previous_complete": True, "today": {}, "today_complete": True,
            "previous_openings": {"600001": 1.2}}
        snapshot = await self.service({"600001": self.quote()}, context).get_snapshot()
        self.assertEqual(snapshot["previous_trade_date"], "2026-10-23")
        row = snapshot["rows"][0]
        self.assertAlmostEqual(row["today_open_change_pct"], 5)
        self.assertEqual(row["amount_yi"], 3.5)
        self.assertEqual(row["market_value_yi"], 25)
        self.assertEqual(row["yesterday_final_seal_time"], "10:52:37")
        self.assertEqual(row["yesterday_continuous_days"], 2)
        self.assertEqual(row["yesterday_open_change_pct"], 1.2)
        self.assertEqual(row["concepts"], ["机器人"])
        self.assertIs(row["today_touched_limit_up"], False)

    async def test_stale_future_cross_day_and_missing_fields_are_not_valid_prices(self):
        for stamp in ("20261023150000", "20261026103100", "20261026093000", "bad"):
            with self.subTest(stamp=stamp):
                snapshot = await self.service({"600001": self.quote(stamp)}).get_snapshot()
                self.assertEqual(snapshot["rows"], [])
                self.assertEqual(snapshot["status"], "partial")
        row = (await self.service({"600001": self.quote(_missing_fields=["open", "turnover_rate"])}).get_snapshot())["rows"][0]
        self.assertIsNone(row["today_open_change_pct"])
        self.assertIsNone(row["turnover_rate"])
        self.assertIsNone(row["yesterday_limit_up"])

    async def test_pre_auction_and_holiday_fallback_use_actual_quote_day(self):
        before = self.service({"600001": self.quote("20261026092000")}, now=datetime(2026, 10, 26, 9, 20))
        row = (await before.get_snapshot())["rows"][0]
        self.assertIsNone(row["today_open_change_pct"])
        weekend = self.service({"600001": self.quote("20261023150000")},
            now=datetime(2026, 10, 25, 12), calendar=[date(2026, 10, 22), date(2026, 10, 23)])
        snapshot = await weekend.get_snapshot()
        self.assertEqual(snapshot["trade_date"], "2026-10-23")
        self.assertEqual(snapshot["previous_trade_date"], "2026-10-22")

    async def test_known_no_event_is_different_from_unavailable_source(self):
        context = {"previous": {}, "previous_complete": True, "today": {}, "today_complete": True}
        row = (await self.service({"600001": self.quote()}, context).get_snapshot())["rows"][0]
        self.assertIs(row["yesterday_limit_up"], False)
        self.assertEqual(row["yesterday_continuous_days"], 0)
        self.assertIn("yesterday_final_seal_time", row["_known_absent_fields"])

    async def test_calendar_failure_does_not_invent_previous_day(self):
        service = self.service({"600001": self.quote()})
        service.calendar_loader = AsyncMock(side_effect=ValueError("calendar offline"))
        with self.assertRaises(ValueError):
            await service.get_snapshot()

    async def test_scan_completion_clock_is_used_for_quotes_received_later_in_a_large_scan(self):
        instant = [datetime(2026, 10, 26, 10, 30)]
        service = self.service({})
        service.now_provider = lambda: instant[0]
        async def scan(day):
            instant[0] = datetime(2026, 10, 26, 10, 30, 20)
            return {"codes": ["600001"], "quotes": {"600001": self.quote("20261026103019")}}
        service.market_service.get_quotes_snapshot = scan
        self.assertEqual(len((await service.get_snapshot())["rows"]), 1)
