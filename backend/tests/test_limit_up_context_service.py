import asyncio
import copy
import importlib.util
import threading
import time
import unittest
from datetime import date, datetime, time as wall_time

import httpx
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.database import Base
from app.models.limit_up import LimitUpRecord
from app.models.market_review import MarketReviewStockDaily
from app.models.stock import Stock

if importlib.util.find_spec("app.services.limit_up_context_service"):
    from app.services.limit_up_context_service import LimitUpContextService
else:
    LimitUpContextService = None


DAY = date(2026, 10, 12)
PREVIOUS = date(2026, 10, 9)
FIELDS = {
    "yesterday_first_limit_up_time", "yesterday_final_seal_time",
    "yesterday_open_change_pct", "today_open_change_pct",
}


async def empty_history(code, start, end):
    return {}


async def empty_pool(trade_date):
    return []


class LimitUpContextTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.assertIsNotNone(LimitUpContextService, "context enrichment service is not implemented")

    def service(self, **kwargs):
        defaults = dict(
            calendar_loader=lambda start, end: [PREVIOUS, DAY],
            session_factory=None,
            history_fetcher=empty_history,
            pool_fetcher=empty_pool,
            today_provider=lambda: DAY,
            now_provider=lambda: datetime(2026, 10, 12, 10, 0),
        )
        defaults.update(kwargs)
        return LimitUpContextService(**defaults)

    async def test_monday_uses_friday_and_does_not_mutate_original_rows(self):
        async def history(code, start, end):
            self.assertEqual((code, start, end), ("000001", PREVIOUS, DAY))
            return {PREVIOUS: {"open": 10.5, "pre_close": 10}, DAY: {"open": 10, "pre_close": 10}}

        rows = [{"stock_code": "000001", "change_pct": 9.99, "meta": {"source": "existing"}}]
        original = copy.deepcopy(rows)
        result = await self.service(history_fetcher=history).enrich(DAY, rows)
        self.assertEqual(rows, original)
        self.assertIsNot(result[0], rows[0])
        self.assertEqual(set(result[0]) - set(rows[0]), FIELDS)
        self.assertEqual(result[0]["change_pct"], 9.99)
        self.assertAlmostEqual(result[0]["yesterday_open_change_pct"], 5.0)
        self.assertEqual(result[0]["today_open_change_pct"], 0.0)

    async def test_holiday_calendar_and_missing_calendar_never_guess_weekdays(self):
        calls = []

        async def history(code, start, end):
            calls.append(start)
            return {}

        holiday = self.service(calendar_loader=lambda start, end: [date(2026, 9, 30), date(2026, 10, 9)], history_fetcher=history)
        await holiday.enrich(date(2026, 10, 9), [{"stock_code": "000001"}])
        self.assertEqual(calls, [date(2026, 9, 30)])
        result = await self.service(calendar_loader=lambda start, end: []).enrich(DAY, [{"stock_code": "000001"}])
        self.assertIsNone(result[0]["yesterday_open_change_pct"])
        self.assertIsNone(result[0]["yesterday_first_limit_up_time"])

    async def test_live_quote_date_is_checked_and_real_zero_is_kept(self):
        quotes = {
            "000001": {"datetime": "20261012092500", "open": 10, "pre_close": 10},
            "000002": {"datetime": "20261009150000", "open": 11, "pre_close": 10},
            "000003": {"open": 11, "pre_close": 10},
            "000004": {"datetime": "20261012092459", "open": 11, "pre_close": 10},
        }
        result = await self.service().enrich(DAY, [{"stock_code": code} for code in quotes], quotes)
        self.assertEqual(result[0]["today_open_change_pct"], 0.0)
        for row in result[1:]:
            self.assertIsNone(row["today_open_change_pct"])

    async def test_historical_day_never_uses_current_or_backdated_quotes(self):
        async def history(code, start, end):
            return {DAY: {"open": 9, "pre_close": 10}}

        result = await self.service(today_provider=lambda: date(2026, 10, 23), history_fetcher=history).enrich(
            DAY, [{"stock_code": "000001"}], {"000001": {"datetime": "20261012093000", "open": 11, "pre_close": 10}}
        )
        self.assertAlmostEqual(result[0]["today_open_change_pct"], -10.0)

    async def test_indicative_open_from_history_is_not_frozen_before_auction_finishes(self):
        now = [datetime(2026, 10, 12, 9, 24)]

        async def history(code, start, end):
            return {DAY: {"open": 11, "pre_close": 10}}

        service = self.service(history_fetcher=history, now_provider=lambda: now[0])
        rows = [{"stock_code": "000001"}]
        self.assertIsNone((await service.enrich(DAY, rows))[0]["today_open_change_pct"])
        now[0] = datetime(2026, 10, 12, 9, 25)
        self.assertAlmostEqual((await service.enrich(DAY, rows))[0]["today_open_change_pct"], 10)

    async def test_invalid_prices_are_missing_and_history_recovers_bad_quotes(self):
        async def history(code, start, end):
            values = {
                "000001": {"open": 0, "pre_close": 10},
                "000002": {"open": 10, "pre_close": 0},
                "000003": {"open": float("inf"), "pre_close": 10},
                "000004": {"open": 10, "pre_close": float("nan")},
                "000005": {"open": 12, "pre_close": 10},
            }
            return {DAY: values[code]}

        quotes = {"000005": {"datetime": "20261012100000", "open": 11, "pre_close": 10, "_missing_fields": ["open"]}}
        result = await self.service(history_fetcher=history).enrich(DAY, [{"stock_code": f"{i:06d}"} for i in range(1, 6)], quotes)
        self.assertTrue(all(row["today_open_change_pct"] is None for row in result[:4]))
        self.assertAlmostEqual(result[4]["today_open_change_pct"], 20.0)

    async def test_success_cache_is_isolated_by_session_and_missing_values_retry(self):
        ticks = [100.0]
        calls = []

        async def history(code, start, end):
            calls.append((code, start, end))
            return {PREVIOUS: {"open": 10, "pre_close": 10}} if len(calls) == 1 else {DAY: {"open": 11, "pre_close": 10}}

        service = self.service(history_fetcher=history, clock=lambda: ticks[0], missing_ttl=10)
        rows = [{"stock_code": "000001"}]
        self.assertIsNone((await service.enrich(DAY, rows))[0]["today_open_change_pct"])
        await service.enrich(DAY, rows)
        self.assertEqual(len(calls), 1)
        ticks[0] += 11
        result = await service.enrich(DAY, rows)
        self.assertAlmostEqual(result[0]["today_open_change_pct"], 10)
        self.assertEqual(result[0]["yesterday_open_change_pct"], 0)
        self.assertEqual(len(calls), 2)

    async def test_calendar_runs_off_event_loop_and_is_cached(self):
        main_thread = threading.get_ident()
        observed = []

        def calendar(start, end):
            observed.append(threading.get_ident())
            return [PREVIOUS, DAY]

        service = self.service(calendar_loader=calendar)
        await service.enrich(DAY, [{"stock_code": "000001"}])
        await service.enrich(DAY, [{"stock_code": "000002"}])
        self.assertEqual(len(observed), 1)
        self.assertNotEqual(observed[0], main_thread)

    async def test_slow_calendar_is_bounded_and_live_opening_survives(self):
        def calendar(start, end):
            time.sleep(0.15)
            return [PREVIOUS, DAY]

        started = time.monotonic()
        result = await self.service(calendar_loader=calendar, request_timeout=0.02).enrich(
            DAY, [{"stock_code": "000001"}], {"000001": {"datetime": "20261012100000", "open": 10, "pre_close": 10}}
        )
        self.assertLess(time.monotonic() - started, 0.12)
        self.assertEqual(result[0]["today_open_change_pct"], 0)
        self.assertIsNone(result[0]["yesterday_open_change_pct"])

    async def test_cached_previous_session_does_not_leak_into_next_session_open(self):
        next_day = date(2026, 10, 13)

        async def history(code, start, end):
            return {DAY: {"open": 11, "pre_close": 10}} if end == DAY else {next_day: {"open": 9, "pre_close": 10}}

        service = self.service(calendar_loader=lambda *args: [PREVIOUS, DAY, next_day], history_fetcher=history,
                               today_provider=lambda: date(2026, 10, 23))
        rows = [{"stock_code": "000001"}]
        await service.enrich(DAY, rows)
        result = await service.enrich(next_day, rows)
        self.assertAlmostEqual(result[0]["yesterday_open_change_pct"], 10)
        self.assertAlmostEqual(result[0]["today_open_change_pct"], -10)

    async def test_overall_budget_returns_partial_data_and_limits_concurrency(self):
        running = 0
        maximum = 0

        async def slow_history(code, start, end):
            nonlocal running, maximum
            running += 1
            maximum = max(maximum, running)
            try:
                await asyncio.sleep(2)
            finally:
                running -= 1
            return {}

        rows = [{"stock_code": f"{i:06d}"} for i in range(20)]
        service = self.service(history_fetcher=slow_history, budget_seconds=0.08, request_timeout=1)
        started = time.monotonic()
        result = await service.enrich(DAY, rows, {"000000": {"datetime": "20261012100000", "open": 11, "pre_close": 10}})
        self.assertLess(time.monotonic() - started, 0.3)
        self.assertLessEqual(maximum, 6)
        self.assertEqual(running, 0)
        self.assertAlmostEqual(result[0]["today_open_change_pct"], 10)
        self.assertEqual(len(result), 20)

    async def test_provider_failure_is_retryable_without_losing_live_percent(self):
        async def fail(*args):
            raise RuntimeError("source unavailable")

        result = await self.service(history_fetcher=fail, pool_fetcher=fail, calendar_loader=lambda *args: (_ for _ in ()).throw(RuntimeError("calendar unavailable"))).enrich(
            DAY, [{"stock_code": "000001"}], {"000001": {"datetime": "20261012100000", "open": 10, "pre_close": 10}}
        )
        self.assertEqual(result[0]["today_open_change_pct"], 0)
        self.assertIsNone(result[0]["yesterday_first_limit_up_time"])

    async def test_pool_can_finish_both_sealed_and_broken_requests_within_overall_budget(self):
        async def pool(day):
            await asyncio.sleep(0.06)
            return [{"stock_code": "000001", "first_limit_up_time": "09:31:02", "is_final_sealed": False}]

        result = await self.service(pool_fetcher=pool, request_timeout=0.04, budget_seconds=0.3).enrich(DAY, [{"stock_code": "000001"}])
        self.assertEqual(result[0]["yesterday_first_limit_up_time"], "09:31:02")
        self.assertIsNone(result[0]["yesterday_final_seal_time"])

    async def test_unadjusted_candle_uses_same_day_change_amount_for_ex_right_reference(self):
        async def respond(request):
            self.assertEqual(request.url.params["fqt"], "0")
            self.assertEqual(request.url.params["fields2"], "f51,f52,f53,f60")
            return httpx.Response(200, json={"data": {"code": "000001", "klines": ["2026-10-09,10,10.5,0.5", "2026-10-12,5.25,5.5,0.5"]}})

        client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
        self.addAsyncCleanup(client.aclose)
        result = await self.service(history_fetcher=None, http_client=client).enrich(DAY, [{"stock_code": "000001"}])
        self.assertAlmostEqual(result[0]["yesterday_open_change_pct"], 0)
        self.assertAlmostEqual(result[0]["today_open_change_pct"], 5)

    async def test_tencent_fallback_pairs_open_and_previous_close_in_same_qfq_series(self):
        async def respond(request):
            if "eastmoney" in request.url.host:
                raise httpx.ConnectError("primary unavailable", request=request)
            return httpx.Response(200, json={"data": {"sz000001": {"qfqday": [
                ["2026-10-08", "4.8", "5.0", "5.1", "4.7", "100"],
                ["2026-10-09", "5.0", "5.0", "5.1", "4.9", "100"],
                ["2026-10-12", "5.25", "5.5", "5.6", "5.2", "100"],
            ]}}})

        client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
        self.addAsyncCleanup(client.aclose)
        result = await self.service(history_fetcher=None, http_client=client).enrich(DAY, [{"stock_code": "000001"}])
        self.assertAlmostEqual(result[0]["yesterday_open_change_pct"], 0)
        self.assertAlmostEqual(result[0]["today_open_change_pct"], 5)

    async def test_tencent_public_api_supplies_history_when_web_frontend_is_unavailable(self):
        async def respond(request):
            if "eastmoney" in request.url.host:
                raise httpx.ConnectError("primary unavailable", request=request)
            if request.url.host != "proxy.finance.qq.com":
                return httpx.Response(501, text="Web frontend unavailable")
            self.assertEqual(request.url.path, "/ifzqgtimg/appstock/app/fqkline/get")
            self.assertEqual(request.headers["Referer"], "https://gu.qq.com/")
            return httpx.Response(200, json={"data": {"sz000001": {"qfqday": [
                ["2026-10-08", "10", "10", "10", "10", "100"],
                ["2026-10-09", "10", "10", "10", "10", "100"],
                ["2026-10-12", "10.5", "11", "11", "10.5", "100"],
            ]}}})

        client = httpx.AsyncClient(transport=httpx.MockTransport(respond), headers={"Referer": "https://quote.eastmoney.com/"})
        self.addAsyncCleanup(client.aclose)
        result = await self.service(history_fetcher=None, http_client=client).enrich(DAY, [{"stock_code": "000001"}])
        self.assertEqual(result[0]["yesterday_open_change_pct"], 0)
        self.assertEqual(result[0]["today_open_change_pct"], 5)

    async def test_tencent_raw_candles_are_not_mixed_across_ex_right_days(self):
        async def respond(request):
            if "eastmoney" in request.url.host:
                return httpx.Response(200, json={"data": None})
            return httpx.Response(200, json={"data": {"sz000001": {"day": [
                ["2026-10-09", "10", "10", "10.1", "9.9", "100"],
                ["2026-10-12", "5.25", "5.5", "5.6", "5.2", "100"],
            ]}}})

        client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
        self.addAsyncCleanup(client.aclose)
        result = await self.service(history_fetcher=None, http_client=client).enrich(DAY, [{"stock_code": "000001"}])
        self.assertIsNone(result[0]["today_open_change_pct"])

    async def test_db_prior_seals_merge_review_facts_and_pool_gaps(self):
        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        self.addAsyncCleanup(engine.dispose)
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        async with sessions() as session:
            session.add_all([Stock(id=1, stock_code="000001", stock_name="one", market="SZ"), Stock(id=2, stock_code="000002", stock_name="two", market="SZ"), Stock(id=3, stock_code="000003", stock_name="three", market="SZ")])
            session.add(LimitUpRecord(stock_id=1, trade_date=PREVIOUS, first_limit_up_time=None, final_seal_time=None, is_final_sealed=False))
            session.add(LimitUpRecord(stock_id=3, trade_date=PREVIOUS, first_limit_up_time=None, final_seal_time=None, is_final_sealed=True))
            session.add(MarketReviewStockDaily(stock_id=1, stock_code="000001", stock_name="one", trade_date=PREVIOUS, today_touched_limit_up=True, today_sealed_close=True, first_limit_time=wall_time(9, 31, 2), final_seal_time=wall_time(14, 2, 4)))
            session.add(MarketReviewStockDaily(stock_id=2, stock_code="000002", stock_name="two", trade_date=PREVIOUS, today_touched_limit_up=True, today_sealed_close=True, first_limit_time=wall_time(10, 1, 3), final_seal_time=wall_time(14, 2, 4)))
            await session.commit()

        async def pool(trade_date):
            self.assertEqual(trade_date, PREVIOUS)
            return [{"stock_code": "000003", "first_limit_up_time": "09:25:00", "final_seal_time": "09:25:00", "is_final_sealed": True}]

        result = await self.service(session_factory=sessions, pool_fetcher=pool).enrich(DAY, [{"stock_code": f"{i:06d}"} for i in range(1, 5)])
        self.assertEqual(result[0]["yesterday_first_limit_up_time"], "09:31:02")
        self.assertIsNone(result[0]["yesterday_final_seal_time"])
        self.assertEqual(result[1]["yesterday_first_limit_up_time"], "10:01:03")
        self.assertEqual(result[1]["yesterday_final_seal_time"], "14:02:04")
        self.assertEqual(result[2]["yesterday_final_seal_time"], "09:25:00")
        self.assertIsNone(result[3]["yesterday_first_limit_up_time"])


if __name__ == "__main__":
    unittest.main()
