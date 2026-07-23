import asyncio
import importlib.util
import math
import unittest
from dataclasses import FrozenInstanceError, fields
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

import app.models  # noqa: F401
from app.data_collectors.tencent_api import TencentStockAPI
from app.database import Base
from app.models.market_review import MarketReviewStockDaily
from app.models.stock import Stock
from app.models.trading_playbook import TradingPlanCandidate, TradingPlanVersion


def _tencent_response_line(code: str) -> str:
    fields = [""] * 50
    fields[0] = "51"
    fields[1] = f"Stock {code}"
    fields[2] = code
    fields[3] = "10.2"
    fields[4] = "10"
    fields[5] = "10.1"
    fields[30] = "20260713093003"
    fields[32] = "2"
    fields[37] = "1200"
    fields[38] = "3.2"
    fields[47] = "11"
    return f'v_test="{"~".join(fields)}"'


class _FakeTencentResponse:
    def __init__(self, text: str):
        self.status_code = 200
        self.text = text


class _FakeTencentClient:
    def __init__(self):
        self.calls = []

    async def get(self, url: str):
        symbols = url.split("q=", 1)[1].split(",") if "q=" in url else []
        self.calls.append(symbols)
        lines = [_tencent_response_line(symbol[-6:]) for symbol in symbols]
        return _FakeTencentResponse(";".join(lines) + ";")


def _quote_payload(
    code: str,
    price: float,
    captured_at,
    *,
    pre_close: float = 10.0,
    name: str = "Test Stock",
):
    return {
        "code": code,
        "name": name,
        "price": str(price),
        "pre_close": str(pre_close),
        "open": "10.1",
        "amount": "1200.5",
        "turnover_rate": "3.2",
        "bid1_price": str(price),
        "bid1_volume": "88",
        "limit_up": "11",
        "datetime": captured_at,
    }


class _FakeQuoteAPI:
    def __init__(self, payload=None, failing_codes=None, delay=0):
        self.payload = payload or {}
        self.failing_codes = set(failing_codes or [])
        self.delay = delay
        self.calls = []
        self.active = 0
        self.max_active = 0

    async def get_quotes_batch(self, codes):
        self.calls.append(list(codes))
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            if self.delay:
                await asyncio.sleep(self.delay)
            if self.failing_codes.intersection(codes):
                raise RuntimeError("upstream chunk failed")
            return {
                code: dict(self.payload[code])
                for code in codes
                if code in self.payload
            }
        finally:
            self.active -= 1


class TencentStockAPIMarketDataTests(unittest.IsolatedAsyncioTestCase):
    def test_format_code_supports_beijing_without_changing_shanghai_or_shenzhen(self):
        api = TencentStockAPI()

        self.assertEqual(api._format_code("920001"), "bj920001")
        self.assertEqual(api._format_code("430001"), "bj430001")
        self.assertEqual(api._format_code("830001"), "bj830001")
        self.assertEqual(api._format_code("600000"), "sh600000")
        self.assertEqual(api._format_code("000001"), "sz000001")

    async def test_get_quotes_batch_splits_161_codes_into_at_most_80_symbols(self):
        api = TencentStockAPI()
        client = _FakeTencentClient()
        api.client = client
        codes = [f"{index:06d}" for index in range(161)]

        quotes = await api.get_quotes_batch(codes)

        self.assertEqual(len(client.calls), 3)
        self.assertEqual([len(call) for call in client.calls], [80, 80, 1])
        self.assertLessEqual(max(map(len, client.calls)), 80)
        self.assertEqual(set(quotes), set(codes))

    async def test_get_quotes_batch_empty_input_does_not_call_upstream(self):
        api = TencentStockAPI()
        client = _FakeTencentClient()
        api.client = client

        self.assertEqual(await api.get_quotes_batch([]), {})
        self.assertEqual(client.calls, [])

    def test_parser_marks_blank_and_malformed_numeric_fields_not_valid_zero(self):
        fields = [""] * 50
        fields[1] = "Parser Quality"
        fields[2] = "000001"
        fields[3] = "10"
        fields[4] = "10"
        fields[9] = "bad-book-price"
        fields[10] = "0"
        fields[30] = "20260713093000"
        fields[32] = "0"
        fields[37] = ""
        fields[38] = "bad-turnover"

        parsed = TencentStockAPI()._parse_response(
            f'v_test="{"~".join(fields)}";'
        )

        self.assertEqual(parsed["amount"], 0)
        self.assertEqual(parsed["turnover_rate"], 0)
        self.assertEqual(parsed["bid1_price"], 0)
        self.assertEqual(parsed["bid1_volume"], 0.0)
        self.assertEqual(parsed["change_pct"], 0.0)
        self.assertTrue(
            {"amount", "turnover_rate", "bid1_price"}.issubset(
                parsed["_missing_fields"]
            )
        )
        self.assertNotIn("bid1_volume", parsed["_missing_fields"])
        self.assertNotIn("change_pct", parsed["_missing_fields"])


class TradingPlaybookDomainContractTests(unittest.TestCase):
    def test_domain_module_exists(self):
        self.assertIsNotNone(
            importlib.util.find_spec("app.services.trading_playbook.domain")
        )

    def test_domain_snapshot_dataclasses_match_contract(self):
        from app.services.trading_playbook import domain

        expected_names = {
            "DataQuality",
            "QuotePoint",
            "QuoteSnapshot",
            "CandidateSnapshot",
            "MarketSnapshot",
            "ModeEvaluation",
        }
        self.assertTrue(all(hasattr(domain, name) for name in expected_names))

        quality = domain.DataQuality(
            status="ready",
            as_of=datetime(2026, 7, 13, 9, 30),
            source="tencent",
        )
        self.assertEqual(
            [field.name for field in fields(quality)],
            [
                "status",
                "as_of",
                "source",
                "stale",
                "warnings",
                "forced_degraded",
                "degradation_reason",
            ],
        )
        self.assertEqual(quality.warnings, [])
        self.assertFalse(quality.forced_degraded)
        self.assertIsNone(quality.degradation_reason)
        with self.assertRaises(FrozenInstanceError):
            quality.status = "degraded"

        candidate = domain.CandidateSnapshot(
            stock_code="000001",
            stock_name="Ping An",
            theme_name="finance",
            features={},
        )
        candidate.features["rank"] = 1
        self.assertEqual(candidate.features["rank"], 1)

        evaluation = domain.ModeEvaluation(
            mode_key="leader",
            stock_code="000001",
            status="waiting",
            score=1.0,
            role="leader",
            risk_level="medium",
            entry_trigger={},
            invalidation={},
            exit_trigger={},
            evidence=[],
        )
        self.assertEqual(evaluation.rule_version, 1)
        self.assertEqual(evaluation.rule_hash, "")
        self.assertEqual(evaluation.action_scope, "target")
        with self.assertRaises(FrozenInstanceError):
            evaluation.score = 2.0


class TradingPlaybookMarketDataProviderBoundaryTests(unittest.TestCase):
    def test_market_data_provider_module_exists(self):
        self.assertIsNotNone(
            importlib.util.find_spec("app.services.trading_playbook.market_data")
        )

    def test_provider_supports_canonical_quote_api_constructor(self):
        from app.services.trading_playbook import market_data

        self.assertTrue(
            hasattr(market_data, "TradingPlaybookMarketDataProvider")
        )
        provider = market_data.TradingPlaybookMarketDataProvider(
            quote_api=object(),
            batch_size=120,
            max_concurrency=100,
        )
        self.assertIs(provider.quote_api, provider.quote_client)
        self.assertLessEqual(provider.batch_size, 80)
        self.assertLessEqual(provider.max_concurrency, 16)

    def test_realtime_crawler_aliases_are_strict_and_ignore_open_count(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        normalized = TradingPlaybookMarketDataProvider._normalize_realtime_row(
            {
                "is_final_sealed": True,
                "open_count": 3,
                "sealed": False,
                "float_market_value": 2_000_000,
                "provenance": "crawler",
            }
        )

        self.assertTrue(normalized["sealed"])
        self.assertFalse(normalized["broken"])
        self.assertEqual(normalized["tradable_market_value"], 2_000_000)
        self.assertEqual(normalized["open_count"], 3)
        self.assertEqual(normalized["provenance"], "crawler")
        for invalid_value in (True, -1, math.nan, math.inf):
            with self.subTest(float_market_value=invalid_value):
                invalid = TradingPlaybookMarketDataProvider._normalize_realtime_row(
                    {
                        "is_final_sealed": 1,
                        "open_count": 3,
                        "float_market_value": invalid_value,
                    }
                )
                self.assertNotIn("sealed", invalid)
                self.assertNotIn("broken", invalid)
                self.assertNotIn("tradable_market_value", invalid)


class TradingPlaybookQuoteSnapshotTests(unittest.IsolatedAsyncioTestCase):
    async def test_previous_price_cache_calculates_two_percent_speed_and_ready(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        trade_date = date(2026, 7, 13)
        first_as_of = datetime(2026, 7, 13, 9, 30, 0)
        api = _FakeQuoteAPI(
            {"000001": _quote_payload("000001", 10, "20260713093000")}
        )
        provider = TradingPlaybookMarketDataProvider(quote_api=api)

        first = await provider.quote_snapshot(
            ["sz000001", "000001"], trade_date, first_as_of
        )
        self.assertEqual(first.quotes["000001"].speed_pct, 0.0)
        self.assertEqual(api.calls, [["000001"]])

        api.payload["000001"] = _quote_payload(
            "000001", 10.2, "20260713093003"
        )
        second = await provider.quote_snapshot(
            ["000001"], trade_date, first_as_of + timedelta(seconds=3)
        )

        self.assertEqual(second.quotes["000001"].speed_pct, 2.0)
        self.assertEqual(second.quotes["000001"].change_pct, 2.0)
        self.assertEqual(second.quality.status, "ready")

    async def test_reverse_time_quote_is_missing_and_does_not_replace_newer_cache(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        trade_date = date(2026, 7, 13)
        api = _FakeQuoteAPI(
            {"000001": _quote_payload("000001", 10, "20260713093010")}
        )
        provider = TradingPlaybookMarketDataProvider(quote_api=api)
        await provider.quote_snapshot(
            ["000001"],
            trade_date,
            datetime(2026, 7, 13, 9, 30, 10),
        )

        api.payload["000001"] = _quote_payload(
            "000001", 9, "20260713093005"
        )
        reverse = await provider.quote_snapshot(
            ["000001"],
            trade_date,
            datetime(2026, 7, 13, 9, 30, 10),
        )
        self.assertTrue(math.isnan(reverse.quotes["000001"].speed_pct))

        api.payload["000001"] = _quote_payload(
            "000001", 11, "20260713093013"
        )
        newer = await provider.quote_snapshot(
            ["000001"],
            trade_date,
            datetime(2026, 7, 13, 9, 30, 13),
        )
        self.assertEqual(newer.quotes["000001"].speed_pct, 10.0)

    async def test_duplicate_time_quote_speed_is_missing(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        captured_at = datetime(2026, 7, 13, 9, 30, 10)
        api = _FakeQuoteAPI(
            {"000001": _quote_payload("000001", 10, "20260713093010")}
        )
        provider = TradingPlaybookMarketDataProvider(quote_api=api)
        await provider.quote_snapshot(["000001"], captured_at.date(), captured_at)

        api.payload["000001"] = _quote_payload(
            "000001", 11, "20260713093010"
        )
        duplicate = await provider.quote_snapshot(
            ["000001"],
            captured_at.date(),
            captured_at,
        )

        self.assertTrue(math.isnan(duplicate.quotes["000001"].speed_pct))

    async def test_duplicate_unchanged_quote_reuses_computed_speed(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        trade_date = date(2026, 7, 13)
        api = _FakeQuoteAPI(
            {"000001": _quote_payload("000001", 10, "20260713092400")}
        )
        provider = TradingPlaybookMarketDataProvider(quote_api=api)
        await provider.quote_snapshot(
            ["000001"],
            trade_date,
            datetime(2026, 7, 13, 9, 24),
        )

        api.payload["000001"] = _quote_payload(
            "000001", 11, "20260713092500"
        )
        computed = await provider.quote_snapshot(
            ["000001"],
            trade_date,
            datetime(2026, 7, 13, 9, 25),
        )
        repeated = await provider.quote_snapshot(
            ["000001"],
            trade_date,
            datetime(2026, 7, 13, 9, 26),
        )

        self.assertEqual(computed.quotes["000001"].speed_pct, 10.0)
        self.assertEqual(repeated.quotes["000001"].speed_pct, 10.0)
        self.assertEqual(repeated.quality.status, "ready")
        self.assertFalse(
            any("speed_pct" in warning for warning in repeated.quality.warnings)
        )

    async def test_quote_speed_over_sixty_seconds_is_missing(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        trade_date = date(2026, 7, 13)
        api = _FakeQuoteAPI(
            {"000001": _quote_payload("000001", 10, "20260713093000")}
        )
        provider = TradingPlaybookMarketDataProvider(quote_api=api)
        await provider.quote_snapshot(
            ["000001"],
            trade_date,
            datetime(2026, 7, 13, 9, 30),
        )

        api.payload["000001"] = _quote_payload(
            "000001", 11, "20260713093101"
        )
        stale_pair = await provider.quote_snapshot(
            ["000001"],
            trade_date,
            datetime(2026, 7, 13, 9, 31, 1),
        )

        self.assertTrue(math.isnan(stale_pair.quotes["000001"].speed_pct))

    async def test_after_close_speed_is_not_applicable_not_missing(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        trade_date = date(2026, 7, 13)
        as_of = datetime(2026, 7, 13, 15, 30)
        provider = TradingPlaybookMarketDataProvider(
            quote_api=_FakeQuoteAPI(
                {
                    "000001": _quote_payload(
                        "000001",
                        10,
                        "20260713150000",
                    )
                }
            )
        )

        snapshot, field_quality = await provider._quote_snapshot_with_quality(
            ["000001"],
            trade_date,
            as_of,
            stage="after_close",
        )

        self.assertTrue(math.isnan(snapshot.quotes["000001"].speed_pct))
        self.assertEqual(
            field_quality["000001"]["speed_pct"],
            "not_applicable",
        )
        self.assertEqual(snapshot.quality.status, "ready")
        self.assertFalse(
            any("speed_pct" in warning for warning in snapshot.quality.warnings)
        )

    async def test_overnight_speed_is_not_applicable_not_missing(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        evidence_trade_date = date(2026, 7, 13)
        trade_date = date(2026, 7, 14)
        as_of = datetime(2026, 7, 14, 8, 50)
        provider = TradingPlaybookMarketDataProvider(
            quote_api=_FakeQuoteAPI(
                {
                    "000001": _quote_payload(
                        "000001",
                        10,
                        "20260713150000",
                    )
                }
            )
        )

        snapshot, field_quality = await provider._quote_snapshot_with_quality(
            ["000001"],
            trade_date,
            as_of,
            stage="overnight",
            evidence_trade_date=evidence_trade_date,
        )

        self.assertTrue(math.isnan(snapshot.quotes["000001"].speed_pct))
        self.assertEqual(
            field_quality["000001"]["speed_pct"],
            "not_applicable",
        )
        self.assertEqual(snapshot.quality.status, "ready")
        self.assertFalse(
            any("speed_pct" in warning for warning in snapshot.quality.warnings)
        )

    async def test_cross_china_trading_date_quote_speed_is_missing(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        api = _FakeQuoteAPI(
            {"000001": _quote_payload("000001", 10, "20260713093000")}
        )
        provider = TradingPlaybookMarketDataProvider(quote_api=api)
        await provider.quote_snapshot(
            ["000001"],
            date(2026, 7, 13),
            datetime(2026, 7, 13, 9, 30),
        )

        api.payload["000001"] = _quote_payload(
            "000001", 11, "20260714093003"
        )
        next_day = await provider.quote_snapshot(
            ["000001"],
            date(2026, 7, 14),
            datetime(2026, 7, 14, 9, 30, 3),
        )

        self.assertTrue(math.isnan(next_day.quotes["000001"].speed_pct))

    async def test_quote_coverage_at_90_percent_is_ready_below_is_degraded(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        trade_date = date(2026, 7, 13)
        as_of = datetime(2026, 7, 13, 9, 30)
        codes = [f"{index:06d}" for index in range(10)]
        payload = {
            code: _quote_payload(code, 10, "20260713093000")
            for code in codes[:9]
        }
        ready = await TradingPlaybookMarketDataProvider(
            quote_api=_FakeQuoteAPI(payload)
        ).quote_snapshot(codes, trade_date, as_of)
        self.assertEqual(ready.quality.status, "ready")
        self.assertTrue(any(codes[-1] in warning for warning in ready.quality.warnings))

        degraded = await TradingPlaybookMarketDataProvider(
            quote_api=_FakeQuoteAPI(
                {code: payload[code] for code in codes[:8]}
            )
        ).quote_snapshot(codes, trade_date, as_of)
        self.assertEqual(degraded.quality.status, "degraded")
        self.assertTrue(any("missing quote" in warning for warning in degraded.quality.warnings))

    async def test_chunk_exception_preserves_successes_and_is_degraded(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        codes = ["000001", "000002", "000003", "000004"]
        api = _FakeQuoteAPI(
            {
                code: _quote_payload(code, 10, "20260713093000")
                for code in codes
            },
            failing_codes={"000003"},
            delay=0.01,
        )
        provider = TradingPlaybookMarketDataProvider(
            quote_api=api,
            batch_size=2,
            max_concurrency=2,
        )

        snapshot = await provider.quote_snapshot(
            codes,
            date(2026, 7, 13),
            datetime(2026, 7, 13, 9, 30),
        )

        self.assertEqual(set(snapshot.quotes), {"000001", "000002"})
        self.assertEqual(snapshot.quality.status, "degraded")
        self.assertFalse(snapshot.quality.forced_degraded)
        self.assertTrue(any("chunk" in warning for warning in snapshot.quality.warnings))
        self.assertEqual(len(api.calls), 2)
        self.assertLessEqual(api.max_active, 2)

    async def test_concurrent_quote_calls_share_provider_limit_without_network_lock(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        as_of = datetime(2026, 7, 13, 9, 30)
        codes = ["000001", "000002", "000003", "000004"]
        api = _FakeQuoteAPI(
            {
                code: _quote_payload(code, 10, "20260713093000")
                for code in codes
            },
            delay=0.03,
        )
        provider = TradingPlaybookMarketDataProvider(
            quote_api=api,
            batch_size=1,
            max_concurrency=2,
        )

        await asyncio.gather(
            provider.quote_snapshot(codes[:2], as_of.date(), as_of),
            provider.quote_snapshot(codes[2:], as_of.date(), as_of),
        )

        self.assertEqual(api.max_active, 2)

    async def test_concurrent_quote_race_never_replaces_newer_cache(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        class RacingQuoteAPI:
            def __init__(self):
                self.call_count = 0
                self.active = 0
                self.max_active = 0

            async def get_quotes_batch(self, codes):
                call_index = self.call_count
                self.call_count += 1
                self.active += 1
                self.max_active = max(self.max_active, self.active)
                try:
                    if call_index == 0:
                        await asyncio.sleep(0.04)
                        payload = _quote_payload(
                            "000001", 10, "20260713093000"
                        )
                    elif call_index == 1:
                        await asyncio.sleep(0.005)
                        payload = _quote_payload(
                            "000001", 11, "20260713093003"
                        )
                    else:
                        payload = _quote_payload(
                            "000001", 12.1, "20260713093006"
                        )
                    return {"000001": payload}
                finally:
                    self.active -= 1

        api = RacingQuoteAPI()
        provider = TradingPlaybookMarketDataProvider(
            quote_api=api,
            max_concurrency=2,
        )
        as_of = datetime(2026, 7, 13, 9, 30, 3)

        await asyncio.gather(
            provider.quote_snapshot(["000001"], as_of.date(), as_of),
            provider.quote_snapshot(["000001"], as_of.date(), as_of),
        )
        latest = await provider.quote_snapshot(
            ["000001"],
            as_of.date(),
            as_of + timedelta(seconds=3),
        )

        self.assertEqual(api.max_active, 2)
        self.assertEqual(latest.quotes["000001"].speed_pct, 10.0)

    async def test_invalid_timestamp_falls_back_and_old_timestamp_marks_stale(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        as_of = datetime(2026, 7, 13, 9, 30, 30)
        api = _FakeQuoteAPI(
            {
                "000001": _quote_payload("000001", 10, "not-a-timestamp"),
                "000002": _quote_payload("000002", 10, "20260713093000"),
            }
        )

        snapshot = await TradingPlaybookMarketDataProvider(
            quote_api=api
        ).quote_snapshot(["000001", "000002"], as_of.date(), as_of)

        self.assertEqual(snapshot.quotes["000001"].captured_at, as_of)
        self.assertTrue(snapshot.quality.stale)
        self.assertTrue(
            any(
                "000001" in warning and "timestamp" in warning
                for warning in snapshot.quality.warnings
            )
        )
        self.assertTrue(
            any(
                warning.startswith("incomplete quote freshness coverage")
                for warning in snapshot.quality.warnings
            )
        )

    async def test_small_stale_quote_minority_is_isolated_to_field_quality(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        as_of = datetime(2026, 7, 13, 9, 30, 30)
        codes = [f"{index:06d}" for index in range(10)]
        payload = {
            code: _quote_payload(code, 10, "20260713093030")
            for code in codes
        }
        payload[codes[-1]] = _quote_payload(
            codes[-1],
            10,
            "20260713093000",
        )

        snapshot, field_quality = await TradingPlaybookMarketDataProvider(
            quote_api=_FakeQuoteAPI(payload)
        )._quote_snapshot_with_quality(codes, as_of.date(), as_of)

        self.assertFalse(snapshot.quality.stale)
        self.assertEqual(
            field_quality[codes[-1]]["_baseline_freshness"],
            "stale",
        )
        self.assertFalse(
            any(
                warning.startswith("incomplete quote freshness coverage")
                for warning in snapshot.quality.warnings
            )
        )

    async def test_naive_as_of_is_china_local_when_quote_timestamp_is_aware(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        as_of = datetime(2026, 7, 13, 9, 30)
        captured_at = datetime(2026, 7, 13, 1, 30, tzinfo=timezone.utc)
        snapshot = await TradingPlaybookMarketDataProvider(
            quote_api=_FakeQuoteAPI(
                {
                    "000001": _quote_payload(
                        "000001",
                        10,
                        captured_at,
                    )
                }
            )
        ).quote_snapshot(["000001"], as_of.date(), as_of)

        self.assertFalse(snapshot.quality.stale)

    async def test_future_quote_is_rejected_and_isolated_at_90_percent_coverage(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        as_of = datetime(2026, 7, 13, 9, 30)
        codes = [f"{index:06d}" for index in range(10)]
        payload = {
            code: _quote_payload(code, 10, "20260713093000")
            for code in codes
        }
        payload[codes[-1]] = _quote_payload(
            codes[-1],
            10,
            datetime(2026, 7, 13, 1, 30, 1, tzinfo=timezone.utc),
        )

        snapshot, field_quality = await TradingPlaybookMarketDataProvider(
            quote_api=_FakeQuoteAPI(payload)
        )._quote_snapshot_with_quality(codes, as_of.date(), as_of)

        self.assertNotIn(codes[-1], snapshot.quotes)
        self.assertEqual(snapshot.quality.status, "ready")
        self.assertEqual(field_quality[codes[-1]]["timestamp"], "invalid")
        self.assertIn(
            "future quote",
            field_quality[codes[-1]]["_rejection_reason"],
        )
        self.assertEqual(snapshot.quality.warnings, [])

    async def test_missing_numeric_fields_are_unavailable_not_fake_zero(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        as_of = datetime(2026, 7, 13, 9, 30)
        payload = _quote_payload("000001", 10, "20260713093000")
        for key in ("pre_close", "amount", "bid1_volume"):
            payload.pop(key)

        snapshot = await TradingPlaybookMarketDataProvider(
            quote_api=_FakeQuoteAPI({"000001": payload})
        ).quote_snapshot(["000001"], as_of.date(), as_of)

        quote = snapshot.quotes["000001"]
        self.assertTrue(math.isnan(quote.pre_close))
        self.assertTrue(math.isnan(quote.change_pct))
        self.assertTrue(math.isnan(quote.amount))
        self.assertTrue(math.isnan(quote.bid1_volume))

    async def test_parser_missing_metadata_overrides_compatibility_zero(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        as_of = datetime(2026, 7, 13, 9, 30)
        payload = _quote_payload("000001", 10, "20260713093000")
        payload.update(
            {
                "amount": 0,
                "turnover_rate": 0,
                "bid1_price": 0,
                "bid1_volume": 0,
                "_missing_fields": [
                    "amount",
                    "turnover_rate",
                    "bid1_price",
                ],
            }
        )

        snapshot = await TradingPlaybookMarketDataProvider(
            quote_api=_FakeQuoteAPI({"000001": payload})
        ).quote_snapshot(["000001"], as_of.date(), as_of)

        quote = snapshot.quotes["000001"]
        self.assertTrue(math.isnan(quote.amount))
        self.assertTrue(math.isnan(quote.turnover_rate))
        self.assertTrue(math.isnan(quote.bid1_price))
        self.assertEqual(quote.bid1_volume, 0.0)

    async def test_empty_quote_snapshot_is_ready_without_upstream_call(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        api = _FakeQuoteAPI()
        as_of = datetime(2026, 7, 13, 9, 30)
        snapshot = await TradingPlaybookMarketDataProvider(
            quote_api=api
        ).quote_snapshot([], as_of.date(), as_of)

        self.assertEqual(snapshot.quotes, {})
        self.assertEqual(snapshot.quality.status, "ready")
        self.assertEqual(api.calls, [])


class TradingPlaybookKlineFeatureTests(unittest.IsolatedAsyncioTestCase):
    async def test_kline_features_use_exact_loader_call_and_detect_new_high(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        calls = []

        async def loader(stock_code, market, period, limit, *, stock_name):
            calls.append((stock_code, market, period, limit, stock_name))
            return [
                {"close": close}
                for close in [10, 12, 11.8, 11.9, 12, 12.5]
            ]

        provider = TradingPlaybookMarketDataProvider(
            quote_api=_FakeQuoteAPI(),
            kline_loader=loader,
        )

        features = await provider.kline_features("830001", "BJ", "Test BJ")

        self.assertEqual(calls, [("830001", "BJ", "day", 60, "Test BJ")])
        self.assertEqual(
            features,
            {
                "n_day_high": True,
                "prior_n_day_high": False,
                "consolidation_days": 4,
                "trend_established": True,
                "kline_quality": "ready",
            },
        )

    async def test_kline_features_never_invent_ready_data_when_missing_or_failed(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        async def short_loader(*args, **kwargs):
            return [{"close": value} for value in [10, 10.1, 10.2]]

        async def failed_loader(*args, **kwargs):
            raise RuntimeError("kline unavailable")

        async def malformed_loader(*args, **kwargs):
            return [{"close": "bad"}] * 6

        async def nan_loader(*args, **kwargs):
            return [
                {"close": value}
                for value in [10, 10.1, 10.2, 10.3, 10.4, float("nan")]
            ]

        async def infinite_loader(*args, **kwargs):
            return [
                {"close": value}
                for value in [10, 10.1, 10.2, 10.3, 10.4, float("inf")]
            ]

        async def negative_loader(*args, **kwargs):
            return [
                {"close": value}
                for value in [10, 10.1, 10.2, 10.3, 10.4, -1]
            ]

        expected = {
            "n_day_high": False,
            "prior_n_day_high": False,
            "consolidation_days": 0,
            "trend_established": False,
            "kline_quality": "missing",
        }
        for loader in (
            None,
            short_loader,
            failed_loader,
            malformed_loader,
            nan_loader,
            infinite_loader,
            negative_loader,
        ):
            with self.subTest(loader=loader):
                provider = TradingPlaybookMarketDataProvider(
                    quote_api=_FakeQuoteAPI(),
                    kline_loader=loader,
                )
                self.assertEqual(
                    await provider.kline_features("000001", "SZ", "Test"),
                    expected,
                )

    async def test_partial_malformed_kline_is_missing_even_with_six_valid_closes(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        class ExplodingClose:
            def __float__(self):
                raise RuntimeError("unexpected close conversion failure")

        valid_points = [
            {"close": close}
            for close in [10, 10.1, 10.2, 10.3, 10.4, 10.5]
        ]
        malformed_points = (
            object(),
            {"close": "bad"},
            {"close": float("nan")},
            {"close": float("inf")},
            {"close": 0},
            {"close": -1},
            {"close": ExplodingClose()},
        )
        expected = {
            "n_day_high": False,
            "prior_n_day_high": False,
            "consolidation_days": 0,
            "trend_established": False,
            "kline_quality": "missing",
        }

        for malformed_point in malformed_points:
            async def loader(*args, **kwargs):
                return [*valid_points, malformed_point]

            with self.subTest(malformed_point=malformed_point):
                provider = TradingPlaybookMarketDataProvider(
                    quote_api=_FakeQuoteAPI(),
                    kline_loader=loader,
                )
                self.assertEqual(
                    await provider.kline_features("000001", "SZ", "Test"),
                    expected,
                )

    async def test_kline_explicitly_incomplete_points_can_be_skipped(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        async def loader(*args, **kwargs):
            return [
                {"close": close}
                for close in [10, 10.1, 10.2, 10.3, 10.4, 10.5]
            ] + [{}, {"close": None}]

        features = await TradingPlaybookMarketDataProvider(
            quote_api=_FakeQuoteAPI(),
            kline_loader=loader,
        ).kline_features("000001", "SZ", "Test")

        self.assertEqual(features["kline_quality"], "ready")

    async def test_concurrent_kline_calls_share_provider_concurrency_limit(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        active = 0
        max_active = 0

        async def loader(*args, **kwargs):
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            try:
                await asyncio.sleep(0.02)
                return [
                    {"close": close}
                    for close in [10, 10.1, 10.2, 10.3, 10.4, 10.5]
                ]
            finally:
                active -= 1

        provider = TradingPlaybookMarketDataProvider(
            quote_api=_FakeQuoteAPI(),
            kline_loader=loader,
            max_concurrency=2,
        )
        await asyncio.gather(
            *(
                provider.kline_features(code, "SZ", f"Stock {code}")
                for code in ("000001", "000002", "000003", "000004")
            )
        )

        self.assertEqual(max_active, 2)

    async def test_date_string_kline_is_available_only_at_china_close(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        source_date = date(2026, 7, 13)

        async def loader(*args, **kwargs):
            return [
                {
                    "date": (source_date - timedelta(days=offset)).isoformat(),
                    "close": 10,
                }
                for offset in range(5, -1, -1)
            ]

        result = await TradingPlaybookMarketDataProvider(
            quote_api=_FakeQuoteAPI(),
            kline_loader=loader,
        )._kline_features_as_of(
            "000001",
            "SZ",
            "Date String Kline",
            source_date,
            datetime(2026, 7, 13, 9, 30),
        )

        self.assertEqual(result.features["kline_quality"], "missing")
        self.assertEqual(result.available_at, datetime(2026, 7, 12, 15))

    async def test_kline_bar_date_and_availability_are_bounded_independently(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        source_date = date(2026, 7, 12)
        target_date = date(2026, 7, 13)

        async def loader(*args, **kwargs):
            return [
                {
                    "date": source_date - timedelta(days=offset),
                    "close": 10,
                }
                for offset in range(5, 0, -1)
            ] + [
                {
                    "date": source_date,
                    "available_at": datetime(2026, 7, 13, 0, 15),
                    "close": 10,
                },
                {
                    "date": target_date,
                    "available_at": datetime(2026, 7, 12, 15),
                    "close": 20,
                },
                {
                    "date": datetime(
                        2026,
                        7,
                        12,
                        16,
                        30,
                        tzinfo=timezone.utc,
                    ),
                    "available_at": datetime(2026, 7, 13, 0, 20),
                    "close": 20,
                },
            ]

        result = await TradingPlaybookMarketDataProvider(
            quote_api=_FakeQuoteAPI(),
            kline_loader=loader,
        )._kline_features_as_of(
            "000001",
            "SZ",
            "Independent Kline Bounds",
            source_date,
            datetime(2026, 7, 13, 0, 30),
        )

        self.assertEqual(result.features["kline_quality"], "ready")
        self.assertFalse(result.features["n_day_high"])
        self.assertEqual(result.available_at, datetime(2026, 7, 13, 0, 15))


class TradingPlaybookMarketSnapshotTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine(
            "sqlite+aiosqlite:///:memory:",
            future=True,
        )
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        self.session_factory = async_sessionmaker(
            self.engine,
            expire_on_commit=False,
        )

    async def asyncTearDown(self):
        await self.engine.dispose()

    def test_bounded_kline_sample_never_populates_full_market_trend_fields(self):
        from app.services.trading_playbook.domain import DataQuality, QuoteSnapshot
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
            _KlineBuildResult,
        )

        trade_date = date(2026, 7, 13)
        as_of = datetime(2026, 7, 13, 15, 30)
        scope_codes = [f"{index:06d}" for index in range(100)]
        ready = {
            "n_day_high": True,
            "prior_n_day_high": False,
            "consolidation_days": 0,
            "trend_established": True,
            "kline_quality": "ready",
        }
        kline_by_code = {
            code: _KlineBuildResult(
                ready,
                as_of,
                evidence_trade_date=trade_date,
            )
            for code in scope_codes[:2]
        }
        values, evidence, _warning = (
            TradingPlaybookMarketDataProvider._complete_market_context(
                {},
                [],
                source_trade_date=trade_date,
                stage="after_close",
                as_of=as_of,
                universe_codes=scope_codes,
                quote_snapshot=QuoteSnapshot(
                    trade_date=trade_date,
                    quotes={},
                    quality=DataQuality(
                        status="degraded",
                        as_of=as_of,
                        source="test",
                    ),
                ),
                quote_field_quality={},
                realtime_rows=[],
                realtime_complete=False,
                realtime_evidence_date=None,
                kline_scope_codes=scope_codes,
                kline_by_code=kline_by_code,
                review_history_by_code={},
            )
        )

        self.assertNotIn("trend_new_high_count", values)
        self.assertNotIn("trend_new_high_count_prev", values)
        self.assertEqual(values["trend_new_high_sample_count"], 2)
        self.assertEqual(values["trend_new_high_sample_count_prev"], 0)
        self.assertEqual(values["trend_sample_size"], 100)
        self.assertEqual(values["trend_sample_ready_coverage"], 0.02)
        self.assertEqual(values["trend_scope"], "bounded_candidate_union")
        self.assertEqual(
            evidence[-1]["field_provenance"]["trend_new_high_sample_count"][
                "source"
            ],
            "bounded_sample",
        )

    def test_valid_bounded_trend_sample_does_not_block_market_context(self):
        from app.services.trading_playbook.context_service import (
            FULL_MARKET_CONTEXT_FIELDS,
        )
        from app.services.trading_playbook.domain import DataQuality, QuoteSnapshot
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
            _KlineBuildResult,
        )

        trade_date = date(2026, 7, 13)
        as_of = datetime(2026, 7, 13, 15, 30)
        context = {
            "limit_up_count": 30,
            "limit_up_count_prev": 25,
            "limit_down_count": 1,
            "max_board_height": 3,
            "seal_rate": 70,
            "negative_feedback": False,
            "divergence_days": 1,
            "sell_pressure_falling": False,
            "breadth_recovered": False,
            "prior_window": "",
            "sell_pressure_rising": False,
        }
        scope_codes = [f"{index:06d}" for index in range(20)]
        ready_kline = {
            "n_day_high": True,
            "prior_n_day_high": True,
            "kline_quality": "ready",
        }

        values, evidence, warning = (
            TradingPlaybookMarketDataProvider._complete_market_context(
                context,
                [
                    {
                        "field_quality": {
                            key: "ready" if key in context else "missing"
                            for key in FULL_MARKET_CONTEXT_FIELDS
                        }
                    }
                ],
                source_trade_date=trade_date,
                stage="after_close",
                as_of=as_of,
                universe_codes=[],
                quote_snapshot=QuoteSnapshot(
                    trade_date=trade_date,
                    quotes={},
                    quality=DataQuality("ready", as_of, "test"),
                ),
                quote_field_quality={},
                realtime_rows=[],
                realtime_complete=False,
                realtime_evidence_date=None,
                kline_scope_codes=scope_codes,
                kline_by_code={
                    code: _KlineBuildResult(
                        ready_kline,
                        as_of,
                        evidence_trade_date=trade_date,
                    )
                    for code in scope_codes
                },
                review_history_by_code={},
            )
        )

        self.assertIsNone(warning)
        self.assertEqual(evidence[0]["quality"], "ready")
        self.assertNotIn("trend_new_high_count", values)
        self.assertNotIn("trend_new_high_count_prev", values)
        self.assertEqual(
            evidence[0]["field_quality"]["trend_new_high_count"],
            "missing",
        )
        self.assertEqual(
            evidence[0]["field_quality"]["trend_new_high_count_prev"],
            "missing",
        )

    def test_realtime_board_labels_complete_missing_continuous_days(self):
        from app.services.trading_playbook.context_service import (
            FULL_MARKET_CONTEXT_FIELDS,
        )
        from app.services.trading_playbook.domain import DataQuality, QuoteSnapshot
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        trade_date = date(2026, 7, 13)
        as_of = datetime(2026, 7, 13, 14, 40)
        context = {
            "limit_up_count": 3,
            "limit_up_count_prev": 2,
            "trend_new_high_count": 20,
            "trend_new_high_count_prev": 18,
            "limit_down_count": 0,
            "seal_rate": 80,
            "negative_feedback": False,
            "divergence_days": 0,
            "sell_pressure_falling": False,
            "breadth_recovered": False,
            "prior_window": "",
            "sell_pressure_rising": False,
        }

        values, evidence, warning = (
            TradingPlaybookMarketDataProvider._complete_market_context(
                context,
                [
                    {
                        "field_quality": {
                            key: "ready" if key in context else "missing"
                            for key in FULL_MARKET_CONTEXT_FIELDS
                        }
                    }
                ],
                source_trade_date=trade_date,
                stage="preclose",
                as_of=as_of,
                universe_codes=[],
                quote_snapshot=QuoteSnapshot(
                    trade_date=trade_date,
                    quotes={},
                    quality=DataQuality("ready", as_of, "test"),
                ),
                quote_field_quality={},
                realtime_rows=[
                    {"continuous_limit_up_days": 3},
                    {"continuous_limit_up_days": None, "board_label": "首板"},
                    {
                        "continuous_limit_up_days": None,
                        "board_label": "3天2板",
                    },
                ],
                realtime_complete=True,
                realtime_evidence_date=trade_date,
                kline_scope_codes=[],
                kline_by_code={},
                review_history_by_code={},
            )
        )

        self.assertIsNone(warning)
        self.assertEqual(values["max_board_height"], 3)
        self.assertEqual(
            evidence[0]["field_quality"]["max_board_height"],
            "computed",
        )

    def test_invalid_bounded_trend_samples_still_block_market_context(self):
        from app.services.trading_playbook.context_service import (
            FULL_MARKET_CONTEXT_FIELDS,
        )
        from app.services.trading_playbook.domain import DataQuality, QuoteSnapshot
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        trade_date = date(2026, 7, 13)
        as_of = datetime(2026, 7, 13, 15, 30)
        complete_context = {
            "limit_up_count": 30,
            "limit_up_count_prev": 25,
            "limit_down_count": 1,
            "max_board_height": 3,
            "seal_rate": 70,
            "negative_feedback": False,
            "divergence_days": 1,
            "sell_pressure_falling": False,
            "breadth_recovered": False,
            "prior_window": "",
            "sell_pressure_rising": False,
        }
        common_sample = {
            "trend_new_high_sample_count": 10,
            "trend_new_high_sample_count_prev": 5,
            "trend_sample_size": 20,
            "trend_sample_ready_coverage": 0.8,
            "trend_scope": "bounded_candidate_union",
        }
        cases = {
            "low_sample": {"trend_sample_size": 19},
            "low_coverage": {"trend_sample_ready_coverage": 0.79},
            "count_exceeds_sample": {"trend_new_high_sample_count": 21},
        }

        for name, invalid_sample in cases.items():
            with self.subTest(case=name):
                _values, evidence, warning = (
                    TradingPlaybookMarketDataProvider._complete_market_context(
                        {
                            **complete_context,
                            **common_sample,
                            **invalid_sample,
                        },
                        [
                            {
                                "field_quality": {
                                    key: (
                                        "ready"
                                        if key in complete_context
                                        else "missing"
                                    )
                                    for key in FULL_MARKET_CONTEXT_FIELDS
                                }
                            }
                        ],
                        source_trade_date=trade_date,
                        stage="after_close",
                        as_of=as_of,
                        universe_codes=[],
                        quote_snapshot=QuoteSnapshot(
                            trade_date=trade_date,
                            quotes={},
                            quality=DataQuality("ready", as_of, "test"),
                        ),
                        quote_field_quality={},
                        realtime_rows=[],
                        realtime_complete=False,
                        realtime_evidence_date=None,
                        kline_scope_codes=[],
                        kline_by_code={},
                        review_history_by_code={},
                    )
                )

                self.assertEqual(evidence[0]["quality"], "degraded")
                self.assertEqual(
                    warning,
                    "incomplete full-market context: "
                    "trend_new_high_count,trend_new_high_count_prev",
                )

    def test_realtime_empty_pool_requires_explicit_authoritative_snapshot(self):
        from app.services.trading_playbook.domain import DataQuality, QuoteSnapshot
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        trade_date = date(2026, 7, 13)
        as_of = datetime(2026, 7, 13, 15, 30)

        def complete(authoritative: bool):
            return TradingPlaybookMarketDataProvider._complete_market_context(
                {},
                [],
                source_trade_date=trade_date,
                stage="after_close",
                as_of=as_of,
                universe_codes=[],
                quote_snapshot=QuoteSnapshot(
                    trade_date=trade_date,
                    quotes={},
                    quality=DataQuality("ready", as_of, "test"),
                ),
                quote_field_quality={},
                realtime_rows=[],
                realtime_complete=authoritative,
                realtime_evidence_date=trade_date if authoritative else None,
                kline_scope_codes=[],
                kline_by_code={},
                review_history_by_code={},
            )[0]

        unavailable = complete(False)
        authoritative_empty = complete(True)

        self.assertNotIn("limit_up_count", unavailable)
        self.assertNotIn("max_board_height", unavailable)
        self.assertEqual(authoritative_empty["limit_up_count"], 0)
        self.assertEqual(authoritative_empty["max_board_height"], 0)

    async def test_auction_quote_age_is_strictly_bounded(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        trade_date = date(2026, 7, 14)
        as_of = datetime(2026, 7, 14, 9, 26)

        self.assertFalse(
            TradingPlaybookMarketDataProvider._quote_baseline_ready(
                stage="auction",
                trade_date=trade_date,
                evidence_trade_date=trade_date,
                captured_at=datetime(2026, 7, 14, 9, 15),
                as_of=as_of,
            )
        )
        self.assertTrue(
            TradingPlaybookMarketDataProvider._quote_baseline_ready(
                stage="auction",
                trade_date=trade_date,
                evidence_trade_date=trade_date,
                captured_at=datetime(2026, 7, 14, 9, 25),
                as_of=as_of,
            )
        )
        self.assertFalse(
            TradingPlaybookMarketDataProvider._quote_baseline_ready(
                stage="auction",
                trade_date=trade_date,
                evidence_trade_date=trade_date,
                captured_at=datetime(2026, 7, 14, 9, 27),
                as_of=as_of,
            )
        )

    async def test_kline_result_records_actual_latest_bar_trade_date(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        async def loader(*args, **kwargs):
            return [
                {
                    "date": date(2026, 7, 10) - timedelta(days=offset),
                    "close": 10 + index,
                }
                for index, offset in enumerate(range(5, -1, -1))
            ]

        result = await TradingPlaybookMarketDataProvider(
            quote_api=_FakeQuoteAPI(),
            kline_loader=loader,
        )._kline_features_as_of(
            "000001",
            "SZ",
            "Weekend provenance",
            date(2026, 7, 13),
            datetime(2026, 7, 13, 8, 50),
        )

        self.assertEqual(result.evidence_trade_date, date(2026, 7, 10))

    async def test_overnight_evidence_keeps_friday_quote_and_bar_dates(self):
        from app.services.realtime_limit_up_service import RealtimeLimitUpSnapshot
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        monday = date(2026, 7, 13)
        friday = date(2026, 7, 10)
        as_of = datetime(2026, 7, 13, 8, 50)
        async with self.session_factory() as db:
            db.add(
                Stock(
                    stock_code="000001",
                    stock_name="Weekend evidence",
                    market="SZ",
                    is_st=0,
                )
            )
            await db.commit()

            async def kline_loader(*args, **kwargs):
                return [
                    {
                        "date": friday - timedelta(days=offset),
                        "close": 10 + index,
                    }
                    for index, offset in enumerate(range(5, -1, -1))
                ]

            async def context_loader(trade_date, stage, captured_at):
                return {
                    "scope": "full_market",
                    "trade_date": monday,
                    "evidence_trade_date": friday,
                    "as_of": datetime(2026, 7, 10, 15, 30),
                    "quality": "degraded",
                    "field_quality": {},
                }

            requested_pool_dates = []

            async def realtime_loader(trade_date):
                requested_pool_dates.append(trade_date)
                return RealtimeLimitUpSnapshot(
                    items=[],
                    authoritative=True,
                    complete=True,
                    evidence_trade_date=trade_date,
                )

            provider = TradingPlaybookMarketDataProvider(
                quote_api=_FakeQuoteAPI(
                    {
                        "000001": _quote_payload(
                            "000001",
                            10.5,
                            "20260710150100",
                        )
                    }
                ),
                kline_loader=kline_loader,
                realtime_limit_up_loader=realtime_loader,
                full_market_context_loader=context_loader,
            )
            prepared_realtime = await provider.prepare_realtime_snapshot(
                monday,
                stage="overnight",
                as_of=as_of,
            )
            snapshot = await provider.build_market_snapshot(
                db=db,
                source_trade_date=monday,
                target_trade_date=monday,
                stage="overnight",
                as_of=as_of,
                prepared_realtime_snapshot=prepared_realtime,
            )

        candidate = snapshot.candidates[0]
        quote_evidence = next(
            item for item in candidate.evidence if item["source"] == "tencent"
        )
        kline_evidence = next(
            item for item in candidate.evidence if item["source"] == "kline"
        )
        self.assertEqual(requested_pool_dates, [friday])
        self.assertNotIn(
            "mismatched realtime snapshot evidence date",
            snapshot.quality.warnings,
        )
        self.assertEqual(quote_evidence["evidence_trade_date"], friday)
        self.assertEqual(kline_evidence["evidence_trade_date"], friday)
        self.assertNotIn("speed_pct", candidate.features)

    async def test_full_market_context_loader_populates_point_in_time_features(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        source_date = date(2026, 7, 10)
        target_date = date(2026, 7, 13)
        as_of = datetime(2026, 7, 10, 14, 40)
        expected = {
            "limit_up_count": 20,
            "limit_up_count_prev": 10,
            "trend_new_high_count": 8,
            "trend_new_high_count_prev": 7,
            "limit_down_count": 1,
            "max_board_height": 3,
            "seal_rate": 80,
            "negative_feedback": False,
            "divergence_days": 0,
            "sell_pressure_falling": False,
            "breadth_recovered": False,
            "prior_window": "",
            "sell_pressure_rising": False,
        }
        calls = []
        expected_quality = {key: "ready" for key in expected}
        expected_quality["divergence_days"] = "computed"

        async def full_market_context_loader(trade_date, stage, captured_at):
            calls.append((trade_date, stage, captured_at))
            return {
                "scope": "full_market",
                "trade_date": trade_date,
                "as_of": captured_at - timedelta(minutes=1),
                **expected,
                "field_quality": expected_quality,
            }

        async with self.session_factory() as db:
            snapshot = await TradingPlaybookMarketDataProvider(
                quote_api=_FakeQuoteAPI(),
                realtime_limit_up_loader=lambda trade_date: asyncio.sleep(
                    0,
                    result=[],
                ),
                full_market_context_loader=full_market_context_loader,
            ).build_market_snapshot(
                db=db,
                source_trade_date=source_date,
                target_trade_date=target_date,
                stage="preclose",
                as_of=as_of,
            )

        self.assertEqual(calls, [(source_date, "preclose", as_of)])
        for key, value in expected.items():
            self.assertEqual(snapshot.market_features[key], value)
            self.assertEqual(
                snapshot.market_features["_feature_quality"][key],
                expected_quality[key],
            )
        evidence = snapshot.market_features["_evidence"]
        self.assertEqual(evidence[0]["source"], "full_market_context")
        self.assertEqual(evidence[0]["scope"], "full_market")

    async def test_real_provider_chain_matches_new_theme_high_volatility(self):
        import copy
        import json
        from pathlib import Path

        from app.services.trading_playbook.domain import CandidateSnapshot
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )
        from app.services.trading_playbook.market_state import MarketStateAnalyzer
        from app.services.trading_playbook.mode_features import ModeFeatureBuilder
        from app.services.trading_playbook.mode_matcher import ModeMatcher

        source_date = date(2026, 7, 10)
        target_date = date(2026, 7, 13)
        as_of = datetime(2026, 7, 10, 14, 40)
        codes = ("300001", "000002")
        payloads = {
            code: {
                **_quote_payload(
                    code,
                    10.5 if code == "300001" else 10.2,
                    "20260710144000",
                ),
                "amount": "2000" if code == "300001" else "1000",
            }
            for code in codes
        }

        async def realtime_loader(trade_date):
            return [
                {
                    "stock_code": code,
                    "theme_name": "AI",
                    "_collected_at": datetime(2026, 7, 10, 14, 39),
                    "first_limit_up_time": (
                        "09:31:00" if code == "300001" else "09:35:00"
                    ),
                    "continuous_limit_up_days": 2 if code == "300001" else 1,
                    "seal_amount": 500 if code == "300001" else 300,
                    "is_final_sealed": True,
                    "open_count": 2 if code == "300001" else 1,
                    "float_market_value": (
                        2_000_000 if code == "300001" else 1_000_000
                    ),
                }
                for code in codes
            ]

        async def kline_loader(*args, **kwargs):
            return [
                {
                    "date": source_date - timedelta(days=offset),
                    "close": close,
                }
                for offset, close in zip(
                    range(6, 0, -1),
                    (8.0, 8.2, 8.4, 8.6, 8.8, 9.2),
                )
            ]

        context = {
            "limit_up_count": 20,
            "limit_up_count_prev": 10,
            "trend_new_high_count": 5,
            "trend_new_high_count_prev": 5,
            "limit_down_count": 1,
            "max_board_height": 3,
            "seal_rate": 80,
            "negative_feedback": False,
            "divergence_days": 0,
            "sell_pressure_falling": False,
            "breadth_recovered": False,
            "prior_window": "",
            "sell_pressure_rising": False,
        }

        async def context_loader(trade_date, stage, captured_at):
            return {
                "scope": "full_market",
                "trade_date": trade_date,
                "captured_at": captured_at - timedelta(minutes=1),
                **context,
                "field_quality": {key: "ready" for key in context},
            }

        async with self.session_factory() as db:
            db.add_all(
                [
                    Stock(
                        stock_code=code,
                        stock_name=f"Mode {code}",
                        market="SZ",
                        is_st=0,
                        circulating_shares=100_000,
                    )
                    for code in codes
                ]
            )
            await db.commit()
            raw = await TradingPlaybookMarketDataProvider(
                quote_api=_FakeQuoteAPI(payloads),
                kline_loader=kline_loader,
                realtime_limit_up_loader=realtime_loader,
                full_market_context_loader=context_loader,
            ).build_market_snapshot(
                db=db,
                source_trade_date=source_date,
                target_trade_date=target_date,
                stage="preclose",
                as_of=as_of,
            )

        enriched = MarketStateAnalyzer().enrich_snapshot(raw)
        analyzed = next(
            item for item in enriched.candidates if item.stock_code == "300001"
        )
        built = ModeFeatureBuilder().build(enriched, analyzed)
        match_candidate = CandidateSnapshot(
            stock_code=analyzed.stock_code,
            stock_name=analyzed.stock_name,
            theme_name=analyzed.theme_name,
            features=built,
            evidence=copy.deepcopy(analyzed.evidence),
        )
        catalog = json.loads(
            Path("app/data/trading_playbook_rules_v2.json").read_text(
                encoding="utf-8"
            )
        )
        matches = {
            row.mode_key: row
            for row in ModeMatcher(
                catalog["rules"],
                catalog_version=catalog["catalog_version"],
            ).evaluate(enriched.market_features, match_candidate)
        }

        self.assertEqual(enriched.market_features["style"], "dual_active")
        self.assertEqual(enriched.market_features["window"], "outbreak")
        self.assertEqual(
            enriched.market_features["_feature_quality"],
            {**{key: "ready" for key in context}, "style": "ready", "window": "ready"},
        )
        self.assertEqual(analyzed.features["recognition_quality"], "ready")
        self.assertEqual(analyzed.features["theme_quality"], "ready")
        self.assertEqual(analyzed.features["theme_rank"], 1)
        theme = enriched.theme_rankings[0]
        self.assertEqual(theme["sealed_count"], 2)
        self.assertEqual(theme["broken_count"], 0)
        self.assertEqual(analyzed.features["tradable_market_value"], 2_000_000)
        self.assertEqual(
            analyzed.features["_feature_quality"]["tradable_market_value"],
            "ready",
        )
        realtime_fact = analyzed.features["realtime_limit_up_fact"]
        self.assertTrue(realtime_fact["is_final_sealed"])
        self.assertEqual(realtime_fact["open_count"], 2)
        self.assertEqual(realtime_fact["float_market_value"], 2_000_000)
        self.assertTrue(realtime_fact["sealed"])
        self.assertFalse(realtime_fact["broken"])
        self.assertTrue(built["_current_sealed"])
        self.assertEqual(built["_current_sealed_quality"], "ready")
        self.assertTrue(built["high_volatility"])
        self.assertEqual(
            (matches["new_theme_high_volatility"].status,
             matches["new_theme_high_volatility"].risk_level),
            ("matched", "trial"),
        )

    async def test_bad_full_market_context_never_becomes_a_known_state(self):
        import copy
        import json
        from pathlib import Path

        from app.services.trading_playbook.domain import CandidateSnapshot
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )
        from app.services.trading_playbook.market_state import MarketStateAnalyzer
        from app.services.trading_playbook.mode_features import ModeFeatureBuilder
        from app.services.trading_playbook.mode_matcher import ModeMatcher

        source_date = date(2026, 7, 10)
        target_date = date(2026, 7, 13)
        as_of = datetime(2026, 7, 10, 14, 40)
        values = {
            "limit_up_count": 20,
            "limit_up_count_prev": 10,
            "trend_new_high_count": 5,
            "trend_new_high_count_prev": 5,
            "limit_down_count": 1,
            "max_board_height": 3,
            "seal_rate": 80,
            "negative_feedback": False,
            "divergence_days": 0,
            "sell_pressure_falling": False,
            "breadth_recovered": False,
            "prior_window": "",
            "sell_pressure_rising": False,
        }
        variants = {
            "wrong_scope": {"scope": "candidate_subset", "as_of": as_of},
            "future": {
                "scope": "full_market",
                "as_of": as_of + timedelta(seconds=1),
            },
            "stale": {
                "scope": "full_market",
                "as_of": as_of - timedelta(days=1),
            },
            "missing_trade_date": {
                "scope": "full_market",
                "as_of": as_of,
                "_omit_trade_date": True,
            },
            "invalid_trade_date": {
                "scope": "full_market",
                "as_of": as_of,
                "trade_date": "not-a-date",
            },
            "mismatched_trade_date": {
                "scope": "full_market",
                "as_of": as_of,
                "trade_date": source_date - timedelta(days=1),
            },
            "invalid_ready_market_field": {
                "scope": "full_market",
                "as_of": as_of,
                "limit_up_count": True,
                "_invalid_field": "limit_up_count",
            },
        }
        catalog = json.loads(
            Path("app/data/trading_playbook_rules_v2.json").read_text(
                encoding="utf-8"
            )
        )
        target_rule = next(
            rule
            for rule in catalog["rules"]
            if rule["mode_key"] == "new_theme_high_volatility"
        )
        payload = _quote_payload("300001", 10.5, "20260710144000")

        async def realtime_loader(trade_date):
            return [{
                "stock_code": "300001",
                "theme_name": "AI",
                "_collected_at": datetime(2026, 7, 10, 14, 39),
                "first_limit_up_time": "09:31:00",
                "continuous_limit_up_days": 2,
                "seal_amount": 500,
                "sealed": True,
                "broken": False,
                "tradable_market_value": 2_000_000,
            }]

        async def kline_loader(*args, **kwargs):
            return [
                {
                    "date": source_date - timedelta(days=offset),
                    "close": close,
                }
                for offset, close in zip(
                    range(6, 0, -1),
                    (8.0, 8.2, 8.4, 8.6, 8.8, 9.2),
                )
            ]

        def assert_waiting(snapshot):
            enriched = MarketStateAnalyzer().enrich_snapshot(snapshot)
            self.assertEqual(enriched.market_features["style"], "unknown")
            self.assertEqual(enriched.market_features["window"], "unknown")
            self.assertEqual(
                enriched.market_features["_feature_quality"]["style"],
                "missing",
            )
            analyzed = enriched.candidates[0]
            built = ModeFeatureBuilder().build(enriched, analyzed)
            candidate = CandidateSnapshot(
                stock_code=analyzed.stock_code,
                stock_name=analyzed.stock_name,
                theme_name=analyzed.theme_name,
                features=built,
                evidence=copy.deepcopy(analyzed.evidence),
            )
            match = ModeMatcher(
                [target_rule],
                catalog_version=catalog["catalog_version"],
            ).evaluate(enriched.market_features, candidate)[0]
            self.assertEqual((match.status, match.risk_level), ("waiting", "watch"))

        async with self.session_factory() as db:
            db.add(
                Stock(
                    stock_code="300001",
                    stock_name="Bad Context Candidate",
                    market="SZ",
                    is_st=0,
                    circulating_shares=100_000,
                )
            )
            await db.commit()
            for name, header in variants.items():
                async def loader(trade_date, stage, captured_at, header=header):
                    result = {
                        "trade_date": trade_date,
                        **values,
                        "field_quality": {key: "ready" for key in values},
                    }
                    result.update(
                        {
                            key: value
                            for key, value in header.items()
                            if not key.startswith("_")
                        }
                    )
                    if header.get("_omit_trade_date"):
                        result.pop("trade_date")
                    return result

                snapshot = await TradingPlaybookMarketDataProvider(
                    quote_api=_FakeQuoteAPI({"300001": payload}),
                    kline_loader=kline_loader,
                    realtime_limit_up_loader=realtime_loader,
                    full_market_context_loader=loader,
                ).build_market_snapshot(
                    db=db,
                    source_trade_date=source_date,
                    target_trade_date=target_date,
                    stage="preclose",
                    as_of=as_of,
                )
                with self.subTest(name=name):
                    assert_waiting(snapshot)
                    invalid_field = header.get("_invalid_field")
                    if invalid_field:
                        self.assertEqual(
                            snapshot.market_features["_feature_quality"][invalid_field],
                            "computed",
                        )
                        self.assertEqual(
                            snapshot.market_features["_evidence"][0][
                                "field_provenance"
                            ][invalid_field]["source"],
                            "realtime_limit_up_pool",
                        )
                        self.assertTrue(
                            any(
                                invalid_field in warning
                                for warning in snapshot.quality.warnings
                            )
                        )
            missing = await TradingPlaybookMarketDataProvider(
                quote_api=_FakeQuoteAPI({"300001": payload}),
                kline_loader=kline_loader,
                realtime_limit_up_loader=realtime_loader,
            ).build_market_snapshot(
                db=db,
                source_trade_date=source_date,
                target_trade_date=target_date,
                stage="preclose",
                as_of=as_of,
            )
        assert_waiting(missing)

    async def test_full_market_context_rejects_ready_invalid_field_values(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        trade_date = date(2026, 7, 10)
        as_of = datetime(2026, 7, 10, 14, 40)
        base = {
            "limit_up_count": 20,
            "limit_up_count_prev": 20,
            "trend_new_high_count": 5,
            "trend_new_high_count_prev": 5,
            "limit_down_count": 1,
            "max_board_height": 3,
            "seal_rate": 70,
            "negative_feedback": False,
            "divergence_days": 0,
            "sell_pressure_falling": False,
            "breadth_recovered": False,
            "prior_window": "",
            "sell_pressure_rising": False,
        }
        cases = (
            ("limit_up_count", True),
            ("limit_up_count_prev", False),
            ("trend_new_high_count", 20.5),
            ("trend_new_high_count_prev", 0.5),
            ("limit_down_count", -1),
            ("max_board_height", math.nan),
            ("divergence_days", math.inf),
            ("seal_rate", True),
            ("seal_rate", -0.1),
            ("seal_rate", 100.1),
            ("negative_feedback", 1),
            ("sell_pressure_falling", "true"),
            ("breadth_recovered", 0),
            ("sell_pressure_rising", "false"),
            ("prior_window", "not-a-window"),
        )

        for field, invalid_value in cases:
            values = {**base, field: invalid_value}

            async def loader(requested_date, stage, captured_at, values=values):
                return {
                    "scope": "full_market",
                    "trade_date": requested_date,
                    "as_of": captured_at,
                    **values,
                    "field_quality": {key: "ready" for key in values},
                }

            provider = TradingPlaybookMarketDataProvider(
                quote_api=_FakeQuoteAPI(),
                full_market_context_loader=loader,
            )
            normalized, evidence, warning = await provider._load_full_market_context(
                trade_date,
                "preclose",
                as_of,
            )

            with self.subTest(field=field, value=invalid_value):
                self.assertNotIn(field, normalized)
                self.assertEqual(evidence, [])
                self.assertIn(field, warning)

    async def test_full_market_context_accepts_integer_and_zero_boundaries(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        trade_date = date(2026, 7, 10)
        as_of = datetime(2026, 7, 10, 14, 40)
        for seal_rate in (0, 100):
            values = {
                "limit_up_count": 0,
                "limit_up_count_prev": 0,
                "trend_new_high_count": 0,
                "trend_new_high_count_prev": 0,
                "limit_down_count": 0,
                "max_board_height": 0,
                "seal_rate": seal_rate,
                "negative_feedback": False,
                "divergence_days": 0,
                "sell_pressure_falling": False,
                "breadth_recovered": False,
                "prior_window": "",
                "sell_pressure_rising": False,
            }

            async def loader(
                requested_date,
                stage,
                captured_at,
                values=values,
            ):
                return {
                    "scope": "full_market",
                    "trade_date": requested_date,
                    "as_of": captured_at,
                    **values,
                    "field_quality": {key: "ready" for key in values},
                }

            normalized, evidence, warning = (
                await TradingPlaybookMarketDataProvider(
                    quote_api=_FakeQuoteAPI(),
                    full_market_context_loader=loader,
                )._load_full_market_context(trade_date, "preclose", as_of)
            )

            with self.subTest(seal_rate=seal_rate):
                self.assertEqual(normalized, values)
                self.assertTrue(
                    all(
                        value == "ready"
                        for value in evidence[0]["field_quality"].values()
                    )
                )
                self.assertIsNone(warning)

    async def test_candidate_conflicts_and_missing_theme_facts_stay_unknown(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        source_date = date(2026, 7, 10)
        as_of = datetime(2026, 7, 10, 14, 40)
        async with self.session_factory() as db:
            stock = Stock(
                stock_code="300001",
                stock_name="Conflict Candidate",
                market="SZ",
                is_st=0,
                circulating_shares=100_000,
            )
            db.add(stock)
            await db.flush()
            db.add(
                MarketReviewStockDaily(
                    trade_date=source_date,
                    stock_id=stock.id,
                    stock_code=stock.stock_code,
                    stock_name=stock.stock_name,
                    first_limit_time=time(9, 35),
                    today_continuous_days=3,
                    tradable_market_value=3_000_000,
                    limit_up_reason="AI",
                    created_at=as_of - timedelta(minutes=2),
                    updated_at=as_of - timedelta(minutes=2),
                )
            )
            await db.commit()

            async def realtime_loader(trade_date):
                return [{
                    "stock_code": "300001",
                    "theme_name": "AI",
                    "_collected_at": as_of - timedelta(minutes=1),
                    "first_limit_up_time": "09:31:00",
                    "continuous_limit_up_days": 2,
                    "tradable_market_value": 2_000_000,
                    "is_final_sealed": "true",
                    "open_count": 3,
                }]

            snapshot = await TradingPlaybookMarketDataProvider(
                quote_api=_FakeQuoteAPI(
                    {"300001": _quote_payload("300001", 10.5, "20260710144000")}
                ),
                realtime_limit_up_loader=realtime_loader,
            ).build_market_snapshot(
                db=db,
                source_trade_date=source_date,
                target_trade_date=source_date,
                stage="preclose",
                as_of=as_of,
            )

        candidate = snapshot.candidates[0]
        realtime_fact = candidate.features["realtime_limit_up_fact"]
        self.assertEqual(realtime_fact["is_final_sealed"], "true")
        self.assertEqual(realtime_fact["open_count"], 3)
        self.assertNotIn("sealed", realtime_fact)
        self.assertNotIn("broken", realtime_fact)
        for key in (
            "first_limit_seconds",
            "board_height",
            "tradable_market_value",
        ):
            self.assertNotIn(key, candidate.features)
            self.assertEqual(candidate.features["_feature_quality"][key], "missing")
        theme = snapshot.theme_rankings[0]
        self.assertEqual(theme["limit_up_count"], 1)
        self.assertEqual(theme["field_quality"]["limit_up_count"], "ready")
        self.assertNotIn("new_high_count", theme)
        self.assertEqual(theme["field_quality"]["new_high_count"], "missing")
        self.assertNotIn("sealed_count", theme)
        self.assertNotIn("broken_count", theme)
        self.assertEqual(theme["quality"], "degraded")

    def test_complete_realtime_pool_certifies_absent_theme_counts_as_zero(self):
        from app.services.trading_playbook.domain import CandidateSnapshot
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        candidate = CandidateSnapshot(
            stock_code="000001",
            stock_name="Historical candidate",
            theme_name="Historical theme",
            features={},
        )

        incomplete = TradingPlaybookMarketDataProvider._theme_rankings(
            [candidate],
            realtime_complete=False,
        )[0]
        complete = TradingPlaybookMarketDataProvider._theme_rankings(
            [candidate],
            realtime_complete=True,
        )[0]

        self.assertEqual(incomplete["quality"], "degraded")
        self.assertEqual(complete["quality"], "ready")
        for field in (
            "limit_up_count",
            "new_high_count",
            "sealed_count",
            "broken_count",
            "middle_army_strength",
        ):
            self.assertEqual(complete[field], 0)
            self.assertEqual(complete["field_quality"][field], "ready")

    async def test_bounded_union_quotes_full_market_and_loads_klines_only_for_candidates(
        self,
    ):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        source_date = date(2026, 7, 10)
        target_date = date(2026, 7, 13)
        as_of = datetime(2026, 7, 13, 9, 30)
        regular_codes = [f"{index:06d}" for index in range(206)]
        eligible_codes = regular_codes + ["830001"]
        quote_retired_code = "600003"

        async with self.session_factory() as db:
            stocks = [
                Stock(
                    stock_code=code,
                    stock_name=f"Name {code}",
                    market="BJ" if code == "830001" else "SZ",
                    is_st=0,
                )
                for code in eligible_codes
            ]
            stocks.append(
                Stock(
                    stock_code="600001",
                    stock_name="*st excluded",
                    market="SH",
                    is_st=0,
                )
            )
            stocks.extend(
                [
                    Stock(
                        stock_code="600002",
                        stock_name="退市测试",
                        market="SH",
                        is_st=0,
                    ),
                    Stock(
                        stock_code=quote_retired_code,
                        stock_name="Former Name",
                        market="SH",
                        is_st=0,
                    ),
                ]
            )
            db.add_all(stocks)
            await db.flush()
            stock_by_code = {stock.stock_code: stock for stock in stocks}

            available_at = datetime(2026, 7, 10, 16)
            for offset in range(9):
                db.add(
                    MarketReviewStockDaily(
                        trade_date=source_date - timedelta(days=offset),
                        stock_id=stock_by_code["000200"].id,
                        stock_code="000200",
                        stock_name="Review Candidate",
                        today_touched_limit_up=True,
                        limit_up_reason="AI review",
                        created_at=available_at,
                        updated_at=available_at,
                    )
                )
            db.add_all(
                [
                    MarketReviewStockDaily(
                        trade_date=source_date - timedelta(days=10),
                        stock_id=stock_by_code["000200"].id,
                        stock_code="000200",
                        stock_name="Tenth Available Review Date",
                        today_touched_limit_up=True,
                        limit_up_reason="AI review",
                        created_at=available_at,
                        updated_at=available_at,
                    ),
                    MarketReviewStockDaily(
                        trade_date=source_date - timedelta(days=11),
                        stock_id=stock_by_code["000201"].id,
                        stock_code="000201",
                        stock_name="Eleventh Review Date",
                        today_touched_limit_up=True,
                        limit_up_reason="too old",
                        created_at=available_at,
                        updated_at=available_at,
                    ),
                    MarketReviewStockDaily(
                        trade_date=source_date,
                        stock_id=stock_by_code["000204"].id,
                        stock_code="000204",
                        stock_name="Updated After Snapshot",
                        today_touched_limit_up=True,
                        limit_up_reason="late update theme",
                        created_at=available_at,
                        updated_at=as_of + timedelta(minutes=1),
                    ),
                    MarketReviewStockDaily(
                        trade_date=source_date - timedelta(days=9),
                        stock_id=stock_by_code["000205"].id,
                        stock_code="000205",
                        stock_name="Created After Snapshot",
                        today_touched_limit_up=True,
                        limit_up_reason="late created theme",
                        created_at=as_of + timedelta(minutes=1),
                        updated_at=as_of + timedelta(minutes=1),
                    ),
                    MarketReviewStockDaily(
                        trade_date=target_date,
                        stock_id=stock_by_code["000201"].id,
                        stock_code="000201",
                        stock_name="Future Review",
                        today_touched_limit_up=True,
                        limit_up_reason="future",
                        created_at=available_at,
                        updated_at=available_at,
                    ),
                ]
            )

            current_plan = TradingPlanVersion(
                source_trade_date=source_date,
                target_trade_date=target_date,
                stage="close",
                version_no=1,
                input_hash="current",
                generated_at=datetime(2026, 7, 10, 16),
            )
            future_plan = TradingPlanVersion(
                source_trade_date=target_date,
                target_trade_date=target_date + timedelta(days=1),
                stage="close",
                version_no=1,
                input_hash="future",
            )
            late_plan = TradingPlanVersion(
                source_trade_date=source_date,
                target_trade_date=target_date,
                stage="close",
                version_no=2,
                input_hash="late",
                generated_at=as_of + timedelta(hours=1),
            )
            db.add_all([current_plan, future_plan, late_plan])
            await db.flush()
            db.add_all(
                [
                    TradingPlanCandidate(
                        plan_version_id=current_plan.id,
                        stock_code="000202",
                        stock_name="Current Plan Candidate",
                        action_trade_date=target_date,
                        theme_name="plan theme",
                        primary_mode_key="leader",
                        role="leader",
                        rank=1,
                        risk_level="medium",
                    ),
                    TradingPlanCandidate(
                        plan_version_id=future_plan.id,
                        stock_code="000203",
                        stock_name="Future Plan Candidate",
                        action_trade_date=target_date + timedelta(days=1),
                        theme_name="future theme",
                        primary_mode_key="leader",
                        role="leader",
                        rank=1,
                        risk_level="medium",
                    ),
                    TradingPlanCandidate(
                        plan_version_id=late_plan.id,
                        stock_code="000204",
                        stock_name="Late Plan Candidate",
                        action_trade_date=target_date,
                        theme_name="late theme",
                        primary_mode_key="leader",
                        role="leader",
                        rank=1,
                        risk_level="medium",
                    ),
                ]
            )
            await db.commit()

            payload = {
                code: _quote_payload(code, 10, "20260713093000")
                for code in [*eligible_codes, quote_retired_code]
            }
            payload[quote_retired_code]["name"] = "测试退"
            payload["000001"]["change_pct"] = 5
            payload["000002"]["change_pct"] = 5
            payload["000003"]["change_pct"] = 6
            quote_api = _FakeQuoteAPI(payload)
            kline_calls = []

            async def kline_loader(code, market, period, limit, *, stock_name):
                kline_calls.append(code)
                if code == "000202":
                    raise RuntimeError("candidate kline unavailable")
                return [
                    {
                        "date": source_date - timedelta(days=5 - index),
                        "close": close,
                    }
                    for index, close in enumerate(
                        [10, 10.1, 10.2, 10.2, 10.3, 10.4]
                    )
                ]

            async def realtime_loader(trade_date):
                return [
                    {
                        "stock_code": "830001",
                        "stock_name": "BJ Candidate",
                        "reason_category": "robotics",
                        "updated_at": as_of,
                    }
                ]

            provider = TradingPlaybookMarketDataProvider(
                quote_api=quote_api,
                kline_loader=kline_loader,
                batch_size=50,
                max_concurrency=3,
                realtime_limit_up_loader=realtime_loader,
            )
            await provider.quote_snapshot(
                eligible_codes,
                target_date,
                as_of,
            )
            quote_api.calls.clear()
            snapshot = await provider.build_market_snapshot(
                db=db,
                source_trade_date=source_date,
                target_trade_date=target_date,
                stage="close",
                as_of=as_of,
            )

        requested_quotes = {
            code for call in quote_api.calls for code in call
        }
        self.assertEqual(
            requested_quotes,
            set(eligible_codes) | {quote_retired_code},
        )
        self.assertNotIn("600001", requested_quotes)
        self.assertNotIn("600002", requested_quotes)
        self.assertEqual(
            set(kline_calls),
            set(regular_codes[:200]) | {"000200", "000202", "830001"},
        )
        candidates = {item.stock_code: item for item in snapshot.candidates}
        self.assertNotIn(quote_retired_code, candidates)
        self.assertEqual(
            snapshot.market_features["quote_requested_count"],
            len(eligible_codes),
        )
        self.assertNotIn("000201", candidates)
        self.assertNotIn("000203", candidates)
        self.assertNotIn("000204", candidates)
        self.assertNotIn("000205", candidates)
        self.assertEqual(candidates["000003"].features["change_rank"], 1)
        self.assertEqual(candidates["000001"].features["change_rank"], 2)
        self.assertEqual(candidates["000002"].features["change_rank"], 3)
        self.assertNotIn("speed_rank", candidates["000000"].features)
        self.assertEqual(snapshot.market_features["full_market_speed_ranks"], {})
        self.assertEqual(candidates["000202"].features["price"], 10.0)
        self.assertEqual(candidates["000202"].features["kline_quality"], "missing")
        self.assertTrue(
            any(
                evidence["source"] == "kline"
                and evidence["quality"] == "missing"
                for evidence in candidates["000202"].evidence
            )
        )
        self.assertEqual(candidates["830001"].theme_name, "robotics")
        self.assertEqual(snapshot.market_features["quote_requested_count"], 207)
        self.assertEqual(snapshot.market_features["quote_returned_count"], 207)

    async def test_kline_stage_deadline_cancels_batch_and_preserves_required_candidates(
        self,
    ):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        trade_date = date(2026, 7, 13)
        as_of = datetime(2026, 7, 13, 14, 40)
        ranked_codes = [f"{index:06d}" for index in range(200)]
        realtime_code = "000200"
        prior_plan_code = "000201"
        review_only_code = "000202"
        all_codes = ranked_codes + [
            realtime_code,
            prior_plan_code,
            review_only_code,
        ]
        active_loads = set()

        async with self.session_factory() as db:
            stocks = [
                Stock(
                    stock_code=code,
                    stock_name=f"Budget {code}",
                    market="SZ",
                    is_st=0,
                )
                for code in all_codes
            ]
            db.add_all(stocks)
            prior_plan = TradingPlanVersion(
                source_trade_date=trade_date,
                target_trade_date=trade_date,
                stage="preclose",
                version_no=1,
                status="draft",
                input_hash="prior-budget-plan",
                generated_at=as_of - timedelta(minutes=1),
            )
            db.add(prior_plan)
            await db.flush()
            stocks_by_code = {stock.stock_code: stock for stock in stocks}
            db.add(
                TradingPlanCandidate(
                    plan_version_id=prior_plan.id,
                    stock_code=prior_plan_code,
                    stock_name="Prior Plan Required",
                    action_trade_date=trade_date,
                    primary_mode_key="leader",
                    role="leader",
                    rank=1,
                    risk_level="trial",
                )
            )
            db.add(
                MarketReviewStockDaily(
                    trade_date=trade_date,
                    stock_id=stocks_by_code[review_only_code].id,
                    stock_code=review_only_code,
                    stock_name="Review History Required",
                    today_touched_limit_up=True,
                    limit_up_reason="review-only theme",
                    created_at=as_of - timedelta(minutes=1),
                    updated_at=as_of - timedelta(minutes=1),
                )
            )
            await db.commit()

            payload = {
                code: {
                    **_quote_payload(code, 10, "20260713144000"),
                    "change_pct": 5 if code in ranked_codes else 0,
                }
                for code in all_codes
            }

            async def slow_kline(code, *args, **kwargs):
                active_loads.add(code)
                try:
                    if code in {realtime_code, prior_plan_code}:
                        return [
                            {
                                "date": trade_date - timedelta(days=6 - index),
                                "close": close,
                            }
                            for index, close in enumerate(
                                [10, 10.1, 10.2, 10.2, 10.3, 10.4]
                            )
                        ]
                    await asyncio.sleep(1)
                    return []
                finally:
                    active_loads.discard(code)

            async def realtime_loader(_trade_date):
                return [
                    {
                        "stock_code": realtime_code,
                        "stock_name": "Realtime Required",
                        "reason_category": "robotics",
                        "updated_at": as_of - timedelta(seconds=1),
                    }
                ]

            provider = TradingPlaybookMarketDataProvider(
                quote_api=_FakeQuoteAPI(payload),
                kline_loader=slow_kline,
                max_concurrency=16,
                realtime_limit_up_loader=realtime_loader,
                kline_stage_timeout_seconds=0.05,
            )
            loop = asyncio.get_running_loop()
            started_at = loop.time()
            snapshot = await provider.build_market_snapshot(
                db=db,
                source_trade_date=trade_date,
                target_trade_date=trade_date,
                stage="preclose",
                as_of=as_of,
            )
            elapsed = loop.time() - started_at

        candidates = {row.stock_code: row for row in snapshot.candidates}
        self.assertLess(elapsed, 0.3)
        self.assertIn(realtime_code, candidates)
        self.assertIn(prior_plan_code, candidates)
        self.assertIn(review_only_code, candidates)
        self.assertEqual(
            candidates[realtime_code].features["kline_quality"],
            "ready",
        )
        self.assertEqual(
            candidates[prior_plan_code].features["kline_quality"],
            "ready",
        )
        kline_evidence = next(
            item
            for item in candidates[review_only_code].evidence
            if item["source"] == "kline"
        )
        self.assertEqual(kline_evidence["quality"], "missing")
        self.assertEqual(kline_evidence["reason"], "kline stage deadline exceeded")
        self.assertNotIn(
            f"missing kline features for {review_only_code}",
            snapshot.quality.warnings,
        )
        self.assertEqual(active_loads, set())

    async def test_kline_stage_cleanup_is_bounded_when_loader_suppresses_cancel(
        self,
    ):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        trade_date = date(2026, 7, 13)
        as_of = datetime(2026, 7, 13, 14, 40)
        codes = [f"{index:06d}" for index in range(8)]
        release = asyncio.Event()
        active = set()
        max_active = 0

        async with self.session_factory() as db:
            db.add_all(
                [
                    Stock(
                        stock_code=code,
                        stock_name=f"Stubborn {code}",
                        market="SZ",
                        is_st=0,
                    )
                    for code in codes
                ]
            )
            await db.commit()

            async def stubborn_kline(code, *args, **kwargs):
                nonlocal max_active
                active.add(code)
                max_active = max(max_active, len(active))
                try:
                    try:
                        await asyncio.Event().wait()
                    except asyncio.CancelledError:
                        await release.wait()
                    return []
                finally:
                    active.discard(code)

            provider = TradingPlaybookMarketDataProvider(
                quote_api=_FakeQuoteAPI(
                    {
                        code: {
                            **_quote_payload(code, 10, "20260713144000"),
                            "change_pct": 5,
                        }
                        for code in codes
                    }
                ),
                kline_loader=stubborn_kline,
                max_concurrency=2,
                realtime_limit_up_loader=lambda _date: asyncio.sleep(
                    0, result=[]
                ),
                kline_stage_timeout_seconds=0.02,
                kline_cancel_grace_seconds=0.02,
            )
            loop = asyncio.get_running_loop()
            started_at = loop.time()
            try:
                snapshot = await asyncio.wait_for(
                    provider.build_market_snapshot(
                        db=db,
                        source_trade_date=trade_date,
                        target_trade_date=trade_date,
                        stage="preclose",
                        as_of=as_of,
                    ),
                    timeout=0.3,
                )
            finally:
                self.assertLessEqual(len(provider._orphan_kline_tasks), 2)
                release.set()
            elapsed = loop.time() - started_at
            await asyncio.sleep(0.05)
            await provider.aclose()

        self.assertLess(elapsed, 0.2)
        self.assertLessEqual(max_active, 2)
        self.assertEqual(active, set())
        self.assertEqual(provider._orphan_kline_tasks, set())
        for candidate in snapshot.candidates:
            evidence = next(
                row for row in candidate.evidence if row["source"] == "kline"
            )
            self.assertEqual(evidence["quality"], "missing")
            self.assertEqual(
                evidence["reason"],
                "kline stage deadline exceeded",
            )

    async def test_auction_window_adds_facts_and_theme_rank_without_fake_zeros(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        source_date = date(2026, 7, 10)
        target_date = date(2026, 7, 13)
        as_of = datetime(2026, 7, 13, 9, 25)
        codes = ["000001", "000002", "000003"]
        async with self.session_factory() as db:
            db.add_all(
                [
                    Stock(
                        stock_code=code,
                        stock_name=f"Auction {code}",
                        market="SZ",
                        is_st=0,
                    )
                    for code in codes
                ]
            )
            await db.commit()

            payload = {
                "000001": _quote_payload("000001", 10.5, "20260713092400"),
                "000002": _quote_payload("000002", 10.2, "20260713092430"),
                "000003": _quote_payload("000003", 10.1, "20260713093000"),
            }

            async def kline_loader(*args, **kwargs):
                return [
                    {
                        "date": source_date - timedelta(days=5 - index),
                        "close": close,
                    }
                    for index, close in enumerate(
                        [10, 10.1, 10.2, 10.2, 10.3, 10.4]
                    )
                ]

            async def realtime_loader(trade_date):
                return [
                    {
                        "stock_code": code,
                        "stock_name": f"Auction {code}",
                        "reason_category": "AI",
                        "updated_at": as_of - timedelta(seconds=1),
                    }
                    for code in codes
                ]

            snapshot = await TradingPlaybookMarketDataProvider(
                quote_api=_FakeQuoteAPI(payload),
                kline_loader=kline_loader,
                realtime_limit_up_loader=realtime_loader,
            ).build_market_snapshot(
                db=db,
                source_trade_date=source_date,
                target_trade_date=target_date,
                stage="auction",
                as_of=as_of,
            )

        candidates = {item.stock_code: item for item in snapshot.candidates}
        first = candidates["000001"].features
        second = candidates["000002"].features
        invalid = candidates["000003"]
        self.assertEqual(first["auction_change_pct"], 5.0)
        self.assertEqual(first["auction_amount"], 1200.5)
        self.assertEqual(first["bid1_volume"], 88.0)
        self.assertEqual(first["auction_theme_rank"], 1)
        self.assertEqual(second["auction_theme_rank"], 2)
        first_auction_evidence = next(
            evidence
            for evidence in candidates["000001"].evidence
            if evidence["source"] == "auction"
        )
        self.assertEqual(
            first_auction_evidence["field_quality"],
            {
                "auction_amount": "ready",
                "auction_bid1_volume": "ready",
                "auction_change_pct": "computed",
                "auction_quality": "ready",
                "auction_theme_rank": "computed",
            },
        )
        self.assertEqual(invalid.features["auction_quality"], "missing")
        self.assertNotIn("auction_change_pct", invalid.features)
        self.assertNotIn("auction_amount", invalid.features)
        self.assertNotIn("auction_theme_rank", invalid.features)
        self.assertNotIn(
            "000003",
            snapshot.market_features["full_market_change_ranks"],
        )
        self.assertTrue(
            any(
                warning.startswith("future quote coverage gap")
                for warning in snapshot.quality.warnings
            )
        )
        self.assertTrue(
            any(
                evidence["source"] == "auction"
                and evidence["quality"] == "missing"
                for evidence in invalid.evidence
            )
        )
        self.assertTrue(
            any(
                evidence["source"] == "tencent"
                and "future quote" in evidence.get("warning", "")
                for evidence in invalid.evidence
            )
        )

    async def test_auction_omits_missing_metrics_but_preserves_valid_zero(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        source_date = date(2026, 7, 10)
        target_date = date(2026, 7, 13)
        as_of = datetime(2026, 7, 13, 9, 25)
        codes = [
            "000010",
            "000011",
            "000012",
            "000013",
            "000014",
            "000015",
        ]
        async with self.session_factory() as db:
            db.add_all(
                [
                    Stock(
                        stock_code=code,
                        stock_name=f"Auction Name {code}",
                        market="SZ",
                        is_st=0,
                    )
                    for code in codes
                ]
            )
            await db.commit()

            missing = _quote_payload("000010", 10, "20260713092400")
            for key in ("pre_close", "amount", "bid1_volume"):
                missing.pop(key)

            valid_zero = _quote_payload("000011", 10, "20260713092400")
            valid_zero.pop("pre_close")
            valid_zero["change_pct"] = 0
            valid_zero["amount"] = 0
            valid_zero["bid1_volume"] = 0

            partial = _quote_payload("000012", 10.5, "20260713092400")
            partial["amount"] = "bad"
            partial["bid1_volume"] = float("nan")

            invalid_price = _quote_payload("000013", 10, "20260713092400")
            invalid_price["price"] = "bad"

            raw_change = _quote_payload("000014", 10.2, "20260713092400")
            raw_change.pop("pre_close")
            raw_change["change_pct"] = 2

            overflow_change = _quote_payload(
                "000015",
                1e308,
                "20260713092400",
                pre_close=1e-308,
            )

            async def kline_loader(*args, **kwargs):
                return [
                    {
                        "date": source_date - timedelta(days=5 - index),
                        "close": close,
                    }
                    for index, close in enumerate(
                        [10, 10.1, 10.2, 10.2, 10.3, 10.4]
                    )
                ]

            async def realtime_loader(trade_date):
                return [
                    {
                        "stock_code": code,
                        "stock_name": f"Auction Name {code}",
                        "reason_category": "AI",
                        "updated_at": as_of - timedelta(seconds=1),
                    }
                    for code in codes
                ]

            snapshot = await TradingPlaybookMarketDataProvider(
                quote_api=_FakeQuoteAPI(
                    {
                        "000010": missing,
                        "000011": valid_zero,
                        "000012": partial,
                        "000013": invalid_price,
                        "000014": raw_change,
                        "000015": overflow_change,
                    }
                ),
                kline_loader=kline_loader,
                realtime_limit_up_loader=realtime_loader,
            ).build_market_snapshot(
                db=db,
                source_trade_date=source_date,
                target_trade_date=target_date,
                stage="auction",
                as_of=as_of,
            )

        candidates = {item.stock_code: item for item in snapshot.candidates}
        missing_features = candidates["000010"].features
        self.assertEqual(missing_features["auction_quality"], "missing")
        self.assertNotIn("auction_change_pct", missing_features)
        self.assertNotIn("auction_amount", missing_features)
        self.assertNotIn("auction_bid1_volume", missing_features)
        self.assertNotIn("auction_theme_rank", missing_features)

        zero_features = candidates["000011"].features
        self.assertEqual(zero_features["auction_quality"], "ready")
        self.assertEqual(zero_features["auction_change_pct"], 0.0)
        self.assertEqual(zero_features["auction_amount"], 0.0)
        self.assertEqual(zero_features["auction_bid1_volume"], 0.0)
        self.assertEqual(zero_features["auction_theme_rank"], 3)

        partial_features = candidates["000012"].features
        self.assertEqual(partial_features["auction_quality"], "degraded")
        self.assertEqual(partial_features["auction_change_pct"], 5.0)
        self.assertNotIn("auction_amount", partial_features)
        self.assertNotIn("auction_bid1_volume", partial_features)
        self.assertEqual(partial_features["auction_theme_rank"], 1)

        invalid_price_features = candidates["000013"].features
        self.assertEqual(invalid_price_features["auction_quality"], "missing")
        self.assertNotIn("price", invalid_price_features)
        self.assertNotIn("auction_change_pct", invalid_price_features)
        self.assertNotIn(
            "000013",
            snapshot.market_features["full_market_change_ranks"],
        )

        raw_change_features = candidates["000014"].features
        self.assertEqual(raw_change_features["auction_quality"], "ready")
        self.assertEqual(raw_change_features["auction_change_pct"], 2.0)
        self.assertNotIn("pre_close", raw_change_features)
        self.assertEqual(raw_change_features["auction_theme_rank"], 2)

        overflow_features = candidates["000015"].features
        self.assertEqual(overflow_features["auction_quality"], "degraded")
        self.assertNotIn("auction_change_pct", overflow_features)
        self.assertNotIn("auction_theme_rank", overflow_features)
        self.assertNotIn(
            "000015",
            snapshot.market_features["full_market_change_ranks"],
        )

    async def test_cached_invalid_speed_input_is_missing_never_baseline(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        trade_date = date(2026, 7, 13)
        as_of = datetime(2026, 7, 13, 15)
        async with self.session_factory() as db:
            db.add(
                Stock(
                    stock_code="000001",
                    stock_name="Cached Invalid Speed",
                    market="SZ",
                    is_st=0,
                )
            )
            await db.commit()

            async def kline_loader(*args, **kwargs):
                return [
                    {
                        "date": trade_date - timedelta(days=6 - index),
                        "close": close,
                    }
                    for index, close in enumerate(
                        [10, 10.1, 10.2, 10.2, 10.3, 10.4]
                    )
                ]

            for previous_price in (float("nan"), float("inf"), 0.0):
                with self.subTest(previous_price=previous_price):
                    provider = TradingPlaybookMarketDataProvider(
                        quote_api=_FakeQuoteAPI(
                            {
                                "000001": _quote_payload(
                                    "000001",
                                    10,
                                    "20260713150000",
                                )
                            }
                        ),
                        kline_loader=kline_loader,
                        realtime_limit_up_loader=lambda trade_date: (
                            asyncio.sleep(0, result=[])
                        ),
                    )
                    provider._previous_prices["000001"] = previous_price

                    snapshot = await provider.build_market_snapshot(
                        db=db,
                        source_trade_date=trade_date,
                        target_trade_date=trade_date,
                        stage="close",
                        as_of=as_of,
                    )

                    candidate = snapshot.candidates[0]
                    rank_evidence = next(
                        evidence
                        for evidence in candidate.evidence
                        if evidence["source"] == "full_market_quote_rank"
                    )
                    quote_evidence = next(
                        evidence
                        for evidence in candidate.evidence
                        if evidence["source"] == "tencent"
                    )
                    market_rank_evidence = snapshot.market_features[
                        "full_market_rank_evidence"
                    ][0]
                    self.assertEqual(
                        snapshot.market_features["full_market_speed_ranks"],
                        {},
                    )
                    self.assertNotIn("speed_rank", candidate.features)
                    self.assertNotIn("speed_pct", candidate.features)
                    self.assertEqual(
                        candidate.features["speed_quality"],
                        "missing",
                    )
                    self.assertEqual(rank_evidence["speed_quality"], "missing")
                    self.assertEqual(
                        rank_evidence["field_quality"],
                        {"change_rank": "ready"},
                    )
                    self.assertEqual(
                        quote_evidence["field_quality"]["price"],
                        "ready",
                    )
                    self.assertEqual(
                        quote_evidence["field_quality"]["captured_at"],
                        "ready",
                    )
                    self.assertEqual(
                        quote_evidence["field_quality"]["speed_pct"],
                        "missing",
                    )
                    self.assertEqual(
                        market_rank_evidence["speed_quality"],
                        "missing",
                    )

    async def test_review_evidence_uses_row_availability_before_snapshot(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        trade_date = date(2026, 7, 13)
        as_of = datetime(2026, 7, 13, 9, 30)
        same_day_available_at = datetime(2026, 7, 13, 9, 20)
        historical_available_at = datetime(2026, 7, 13, 8, 45)
        async with self.session_factory() as db:
            stocks = [
                Stock(
                    stock_code=code,
                    stock_name=f"Review Evidence {code}",
                    market="SZ",
                    is_st=0,
                )
                for code in ("000001", "000002")
            ]
            db.add_all(stocks)
            await db.flush()
            stock_by_code = {stock.stock_code: stock for stock in stocks}
            db.add_all(
                [
                    MarketReviewStockDaily(
                        trade_date=trade_date,
                        stock_id=stock_by_code["000001"].id,
                        stock_code="000001",
                        stock_name="Same-day Review",
                        limit_up_reason="same-day theme",
                        created_at=datetime(2026, 7, 13, 9, 10),
                        updated_at=same_day_available_at,
                    ),
                    MarketReviewStockDaily(
                        trade_date=trade_date - timedelta(days=1),
                        stock_id=stock_by_code["000002"].id,
                        stock_code="000002",
                        stock_name="Historical Review",
                        limit_up_reason="historical theme",
                        created_at=datetime(2026, 7, 12, 16),
                        updated_at=historical_available_at,
                    ),
                ]
            )
            await db.commit()

            async def kline_loader(*args, **kwargs):
                return [
                    {
                        "date": trade_date - timedelta(days=5 - index),
                        "close": close,
                    }
                    for index, close in enumerate(
                        [10, 10.1, 10.2, 10.2, 10.3, 10.4]
                    )
                ]

            snapshot = await TradingPlaybookMarketDataProvider(
                quote_api=_FakeQuoteAPI(
                    {
                        code: _quote_payload(code, 10, "20260713093000")
                        for code in ("000001", "000002")
                    }
                ),
                kline_loader=kline_loader,
                realtime_limit_up_loader=lambda trade_date: asyncio.sleep(
                    0,
                    result=[],
                ),
            ).build_market_snapshot(
                db=db,
                source_trade_date=trade_date,
                target_trade_date=trade_date,
                stage="close",
                as_of=as_of,
            )

        candidates = {item.stock_code: item for item in snapshot.candidates}
        expected_availability = {
            "000001": same_day_available_at,
            "000002": historical_available_at,
        }
        for code, candidate in candidates.items():
            review_evidence = next(
                evidence
                for evidence in candidate.evidence
                if evidence["source"] == "market_review_stock_daily"
            )
            self.assertEqual(
                review_evidence["as_of"],
                expected_availability[code],
            )
            self.assertTrue(
                all(
                    evidence["as_of"] <= snapshot.as_of
                    for evidence in candidate.evidence
                )
            )

    async def test_aware_utc_as_of_uses_china_date_and_filters_future_kline(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        source_date = date(2026, 7, 12)
        target_date = date(2026, 7, 13)
        as_of = datetime(2026, 7, 12, 16, 30, tzinfo=timezone.utc)
        realtime_dates = []
        async with self.session_factory() as db:
            db.add(
                Stock(
                    stock_code="000001",
                    stock_name="UTC Provenance",
                    market="SZ",
                    is_st=0,
                )
            )
            await db.commit()

            async def kline_loader(*args, **kwargs):
                return [
                    {"date": source_date - timedelta(days=offset), "close": 10}
                    for offset in range(5, -1, -1)
                ] + [{"date": target_date, "close": 20}]

            async def realtime_loader(trade_date):
                realtime_dates.append(trade_date)
                return []

            snapshot = await TradingPlaybookMarketDataProvider(
                quote_api=_FakeQuoteAPI(
                    {"000001": _quote_payload("000001", 10, as_of)}
                ),
                kline_loader=kline_loader,
                realtime_limit_up_loader=realtime_loader,
            ).build_market_snapshot(
                db=db,
                source_trade_date=source_date,
                target_trade_date=target_date,
                stage="close",
                as_of=as_of,
            )

        candidate = snapshot.candidates[0]
        kline_evidence = next(
            evidence
            for evidence in candidate.evidence
            if evidence["source"] == "kline"
        )
        self.assertEqual(realtime_dates, [target_date])
        self.assertFalse(candidate.features["n_day_high"])
        self.assertEqual(
            kline_evidence["as_of"],
            datetime(
                2026,
                7,
                12,
                15,
                tzinfo=ZoneInfo("Asia/Shanghai"),
            ),
        )
        self.assertLessEqual(kline_evidence["as_of"], snapshot.as_of)

    async def test_historical_kline_without_time_provenance_is_missing(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        trade_date = date(2026, 7, 10)
        as_of = datetime(2026, 7, 13, 9, 30)
        async with self.session_factory() as db:
            db.add(
                Stock(
                    stock_code="000001",
                    stock_name="Unproven Kline",
                    market="SZ",
                    is_st=0,
                )
            )
            await db.commit()

            async def kline_loader(*args, **kwargs):
                return [
                    {"close": close}
                    for close in [10, 10.1, 10.2, 10.3, 10.4, 10.5]
                ]

            snapshot = await TradingPlaybookMarketDataProvider(
                quote_api=_FakeQuoteAPI(
                    {
                        "000001": _quote_payload(
                            "000001",
                            10,
                            "20260713093000",
                        )
                    }
                ),
                kline_loader=kline_loader,
                realtime_limit_up_loader=lambda trade_date: asyncio.sleep(
                    0,
                    result=[],
                ),
            ).build_market_snapshot(
                db=db,
                source_trade_date=trade_date,
                target_trade_date=trade_date,
                stage="close",
                as_of=as_of,
            )

        candidate = snapshot.candidates[0]
        kline_evidence = next(
            evidence
            for evidence in candidate.evidence
            if evidence["source"] == "kline"
        )
        self.assertEqual(candidate.features["kline_quality"], "missing")
        self.assertNotIn("n_day_high", candidate.features)
        self.assertNotIn("consolidation_days", candidate.features)
        self.assertNotIn("trend_established", candidate.features)
        self.assertIsNone(kline_evidence["as_of"])

    async def test_realtime_rows_require_actual_provenance_and_never_look_ahead(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        trade_date = date(2026, 7, 10)
        as_of = datetime(2026, 7, 13, 9, 30)
        codes = ("000001", "000002", "000003", "000004")
        accepted_at = datetime(2026, 7, 10, 15)
        async with self.session_factory() as db:
            db.add_all(
                [
                    Stock(
                        stock_code=code,
                        stock_name=f"Realtime {code}",
                        market="SZ",
                        is_st=0,
                    )
                    for code in codes
                ]
            )
            await db.commit()

            async def kline_loader(*args, **kwargs):
                return [
                    {
                        "date": trade_date - timedelta(days=offset),
                        "close": 10,
                    }
                    for offset in range(5, -1, -1)
                ]

            async def realtime_loader(requested_date):
                return [
                    {
                        "stock_code": "000001",
                        "reason_category": "future",
                        "_collected_at": as_of + timedelta(seconds=1),
                    },
                    {
                        "stock_code": "000002",
                        "reason_category": "event times only",
                        "first_limit_up_time": datetime(2026, 7, 10, 9, 31),
                        "final_seal_time": datetime(2026, 7, 10, 10, 5),
                        "datetime": datetime(2026, 7, 10, 10, 5),
                        "timestamp": datetime(2026, 7, 10, 10, 5),
                        "quote_time": datetime(2026, 7, 10, 10, 5),
                        "time": "10:05:00",
                    },
                    {
                        "stock_code": "000003",
                        "reason_category": "accepted",
                        "_collected_at": accepted_at,
                        "seal_amount": math.nan,
                        "nested_quality": {
                            "valid_ratio": 1.0,
                            "invalid_ratio": math.inf,
                        },
                    },
                    {
                        "stock_code": "000004",
                        "reason_category": "date only",
                        "updated_at": trade_date.isoformat(),
                    },
                ]

            snapshot = await TradingPlaybookMarketDataProvider(
                quote_api=_FakeQuoteAPI(
                    {
                        code: _quote_payload(code, 10, "20260713093000")
                        for code in codes
                    }
                ),
                kline_loader=kline_loader,
                realtime_limit_up_loader=realtime_loader,
            ).build_market_snapshot(
                db=db,
                source_trade_date=trade_date,
                target_trade_date=trade_date,
                stage="close",
                as_of=as_of,
            )

        candidates = {item.stock_code: item for item in snapshot.candidates}
        self.assertEqual(snapshot.quality.status, "degraded")
        self.assertTrue(
            any(
                "future realtime row for 000001" in warning
                for warning in snapshot.quality.warnings
            )
        )
        for code in ("000001", "000002", "000004"):
            self.assertNotIn(
                "realtime_limit_up_fact",
                candidates[code].features,
            )
            realtime_evidence = next(
                evidence
                for evidence in candidates[code].evidence
                if evidence["source"] == "realtime_limit_up_pool"
            )
            self.assertEqual(realtime_evidence["quality"], "degraded")
            self.assertTrue(realtime_evidence["candidate_discovery_only"])
        accepted = candidates["000003"]
        realtime_evidence = next(
            evidence
            for evidence in accepted.evidence
            if evidence["source"] == "realtime_limit_up_pool"
        )
        self.assertEqual(accepted.theme_name, "accepted")
        self.assertEqual(realtime_evidence["as_of"], accepted_at)
        realtime_fact = accepted.features["realtime_limit_up_fact"]
        self.assertNotIn("seal_amount", realtime_fact)
        self.assertEqual(
            realtime_fact["nested_quality"],
            {"valid_ratio": 1.0},
        )

    async def test_prepared_realtime_snapshot_is_used_without_refetch(self):
        from app.services.realtime_limit_up_service import RealtimeLimitUpSnapshot
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        trade_date = date(2026, 7, 14)
        as_of = datetime(2026, 7, 14, 9, 30)
        code = "000001"
        live_calls = []

        async def live_loader(requested_date):
            live_calls.append(requested_date)
            return RealtimeLimitUpSnapshot(
                items=[],
                authoritative=True,
                complete=True,
                evidence_trade_date=requested_date,
            )

        async def kline_loader(*args, **kwargs):
            return [
                {
                    "date": trade_date - timedelta(days=6 - index),
                    "close": close,
                }
                for index, close in enumerate([10, 10.1, 10.2, 10.3, 10.4, 10.5])
            ]

        prepared = RealtimeLimitUpSnapshot(
            items=[
                {
                    "stock_code": code,
                    "reason_category": "robotics",
                    "_collected_at": as_of - timedelta(seconds=1),
                }
            ],
            authoritative=True,
            complete=True,
            evidence_trade_date=trade_date,
        )
        async with self.session_factory() as db:
            db.add(
                Stock(
                    stock_code=code,
                    stock_name="Prepared Snapshot",
                    market="SZ",
                    is_st=0,
                )
            )
            await db.commit()
            snapshot = await TradingPlaybookMarketDataProvider(
                quote_api=_FakeQuoteAPI(
                    {code: _quote_payload(code, 10.5, "20260714093000")}
                ),
                kline_loader=kline_loader,
                realtime_limit_up_loader=live_loader,
            ).build_market_snapshot(
                db=db,
                source_trade_date=trade_date,
                target_trade_date=trade_date,
                stage="close",
                as_of=as_of,
                prepared_realtime_snapshot=prepared,
            )

        self.assertEqual(live_calls, [])
        self.assertFalse(
            any(
                "future realtime row" in warning
                for warning in snapshot.quality.warnings
            )
        )
        candidate = next(row for row in snapshot.candidates if row.stock_code == code)
        self.assertEqual(
            candidate.features["realtime_limit_up_fact"]["reason_category"],
            "robotics",
        )

    async def test_failed_refresh_with_30_minute_cache_is_discovery_only(self):
        import copy
        import json

        from app.services.realtime_limit_up_service import RealtimeLimitUpService
        from app.services.trading_playbook.domain import CandidateSnapshot
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )
        from app.services.trading_playbook.market_state import MarketStateAnalyzer
        from app.services.trading_playbook.mode_features import ModeFeatureBuilder
        from app.services.trading_playbook.mode_matcher import ModeMatcher

        trade_date = date(2026, 7, 14)
        as_of = datetime(2026, 7, 14, 10, 0)
        code = "000099"
        service = RealtimeLimitUpService()
        service._pool_cache[trade_date] = [
            {
                "stock_code": code,
                "theme_name": "stale-theme-must-not-match",
                "_collected_at": as_of - timedelta(minutes=30),
                "is_final_sealed": True,
                "open_count": 2,
            }
        ]
        service._pool_cache_time[trade_date] = __import__("time").time() - 1800

        class FailingClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return None

            async def get(self, *_args, **_kwargs):
                raise RuntimeError("refresh unavailable")

        with patch(
            "app.services.realtime_limit_up_service.httpx.AsyncClient",
            return_value=FailingClient(),
        ):
            stale_snapshot = await service.get_fast_limit_up_snapshot(trade_date)

        self.assertFalse(stale_snapshot.authoritative)
        self.assertFalse(stale_snapshot.complete)
        self.assertIsNone(stale_snapshot.evidence_trade_date)
        self.assertEqual(len(stale_snapshot.items), 1)

        async def kline_loader(*_args, **_kwargs):
            return [
                {
                    "date": trade_date - timedelta(days=offset),
                    "close": 10,
                }
                for offset in range(5, -1, -1)
            ]

        async with self.session_factory() as db:
            db.add(
                Stock(
                    stock_code=code,
                    stock_name="Cache Candidate",
                    market="SZ",
                    is_st=0,
                )
            )
            await db.commit()
            raw = await TradingPlaybookMarketDataProvider(
                quote_api=_FakeQuoteAPI(),
                kline_loader=kline_loader,
                realtime_limit_up_loader=lambda _trade_date: asyncio.sleep(
                    0, result=stale_snapshot
                ),
            ).build_market_snapshot(
                db=db,
                source_trade_date=trade_date,
                target_trade_date=trade_date,
                stage="preclose",
                as_of=as_of,
            )

        candidate = next(item for item in raw.candidates if item.stock_code == code)
        self.assertNotIn("realtime_limit_up_fact", candidate.features)
        realtime_evidence = next(
            item
            for item in candidate.evidence
            if item["source"] == "realtime_limit_up_pool"
        )
        self.assertIn(realtime_evidence["quality"], {"degraded", "missing"})
        self.assertNotEqual(candidate.theme_name, "stale-theme-must-not-match")
        self.assertNotIn("limit_up_count", raw.market_features)
        self.assertEqual(
            raw.market_features["_feature_quality"]["limit_up_count"],
            "missing",
        )

        enriched = MarketStateAnalyzer().enrich_snapshot(raw)
        analyzed = next(
            item for item in enriched.candidates if item.stock_code == code
        )
        built = ModeFeatureBuilder().build(enriched, analyzed)
        match_candidate = CandidateSnapshot(
            stock_code=analyzed.stock_code,
            stock_name=analyzed.stock_name,
            theme_name=analyzed.theme_name,
            features=built,
            evidence=copy.deepcopy(analyzed.evidence),
        )
        catalog = json.loads(
            Path("app/data/trading_playbook_rules_v2.json").read_text(
                encoding="utf-8"
            )
        )
        evaluations = ModeMatcher(
            catalog["rules"],
            catalog_version=catalog["catalog_version"],
        ).evaluate(enriched.market_features, match_candidate)
        self.assertFalse(any(row.risk_level == "trial" for row in evaluations))

    async def test_stale_and_fallback_quotes_never_enter_ready_ranks(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        source_date = date(2026, 7, 12)
        target_date = date(2026, 7, 13)
        as_of = datetime(2026, 7, 13, 9, 30, 30)
        payloads = {
            "000001": _quote_payload("000001", 10.3, "20260713093025"),
            "000002": _quote_payload("000002", 10.5, "20260713093000"),
            "000003": _quote_payload("000003", 10.4, "bad-timestamp"),
        }
        async with self.session_factory() as db:
            db.add_all(
                [
                    Stock(
                        stock_code=code,
                        stock_name=f"Rank Freshness {code}",
                        market="SZ",
                        is_st=0,
                    )
                    for code in payloads
                ]
            )
            await db.commit()

            async def kline_loader(*args, **kwargs):
                return [
                    {
                        "date": source_date - timedelta(days=offset),
                        "close": 10,
                    }
                    for offset in range(5, -1, -1)
                ]

            async def realtime_loader(requested_date):
                return [
                    {
                        "stock_code": code,
                        "reason_category": f"theme {code}",
                        "updated_at": as_of - timedelta(seconds=1),
                    }
                    for code in ("000002", "000003")
                ]

            snapshot = await TradingPlaybookMarketDataProvider(
                quote_api=_FakeQuoteAPI(payloads),
                kline_loader=kline_loader,
                realtime_limit_up_loader=realtime_loader,
            ).build_market_snapshot(
                db=db,
                source_trade_date=source_date,
                target_trade_date=target_date,
                stage="close",
                as_of=as_of,
            )

        self.assertEqual(
            snapshot.market_features["full_market_change_ranks"],
            {"000001": 1},
        )
        candidates = {item.stock_code: item for item in snapshot.candidates}
        expected_quote_quality = {"000002": "stale", "000003": "degraded"}
        for code, quality in expected_quote_quality.items():
            candidate = candidates[code]
            self.assertNotIn("change_rank", candidate.features)
            self.assertNotIn("speed_rank", candidate.features)
            quote_evidence = next(
                evidence
                for evidence in candidate.evidence
                if evidence["source"] == "tencent"
            )
            self.assertEqual(quote_evidence["quality"], quality)
            self.assertFalse(
                any(
                    evidence["source"] == "full_market_quote_rank"
                    for evidence in candidate.evidence
                )
            )

        market_rank_evidence = {
            item["stock_code"]: item
            for item in snapshot.market_features["full_market_rank_evidence"]
        }
        self.assertEqual(
            market_rank_evidence["000002"]["change_quality"],
            "stale",
        )
        self.assertEqual(
            market_rank_evidence["000003"]["change_quality"],
            "degraded",
        )
        fresh_rank_evidence = next(
            evidence
            for evidence in candidates["000001"].evidence
            if evidence["source"] == "full_market_quote_rank"
        )
        self.assertEqual(fresh_rank_evidence["quality"], "ready")
        self.assertEqual(
            fresh_rank_evidence["as_of"],
            datetime(2026, 7, 13, 9, 30, 25),
        )

    async def test_current_plan_resolution_is_cross_stage_status_aware_and_bounded(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        source_date = date(2026, 7, 10)
        target_date = date(2026, 7, 13)
        as_of = datetime(2026, 7, 13, 9, 25)
        codes = [f"{index:06d}" for index in range(1, 11)]
        async with self.session_factory() as db:
            # Exercise the provider's defense against legacy/corrupt duplicate
            # active rows; current schemas prevent this state with a partial
            # unique index.
            await db.execute(
                text("DROP INDEX IF EXISTS uq_trading_plan_one_active_target")
            )
            db.add_all(
                [
                    Stock(
                        stock_code=code,
                        stock_name=f"Plan Resolution {code}",
                        market="SZ",
                        is_st=0,
                    )
                    for code in codes
                ]
            )
            await db.flush()

            def plan(
                target,
                stage,
                version_no,
                status,
                generated_at,
                suffix,
            ):
                return TradingPlanVersion(
                    source_trade_date=source_date - timedelta(days=1),
                    target_trade_date=target,
                    stage=stage,
                    version_no=version_no,
                    status=status,
                    input_hash=f"plan-{suffix}",
                    generated_at=generated_at,
                )

            target_versions = [
                plan(target_date, "after_close", 1, "draft", as_of, "old"),
                plan(target_date, "overnight", 5, "draft", as_of, "draft"),
                plan(target_date, "preclose", 3, "confirmed", as_of, "confirmed"),
                plan(target_date, "after_close", 2, "active", as_of, "active"),
                plan(target_date, "overnight", 6, "superseded", as_of, "superseded"),
                plan(target_date, "preclose", 7, "expired", as_of, "expired"),
                plan(
                    target_date,
                    "overnight",
                    8,
                    "active",
                    as_of + timedelta(seconds=1),
                    "late",
                ),
            ]
            source_versions = [
                plan(source_date, "after_close", 1, "draft", as_of, "source-old"),
                plan(source_date, "overnight", 2, "draft", as_of, "source-new"),
            ]
            versions = target_versions + source_versions
            db.add_all(versions)
            await db.flush()

            candidate_specs = [
                (target_versions[0], "000001", target_date),
                (target_versions[1], "000002", target_date),
                (target_versions[2], "000003", target_date),
                (target_versions[3], "000004", target_date),
                (target_versions[4], "000005", target_date),
                (target_versions[5], "000006", target_date),
                (source_versions[0], "000007", source_date),
                (source_versions[1], "000008", source_date),
                (target_versions[6], "000009", target_date),
                (target_versions[3], "000010", source_date + timedelta(days=1)),
            ]
            db.add_all(
                [
                    TradingPlanCandidate(
                        plan_version_id=version.id,
                        stock_code=code,
                        stock_name=f"Plan Candidate {code}",
                        action_trade_date=action_date,
                        theme_name=f"theme {code}",
                        primary_mode_key="leader",
                        role="leader",
                        rank=1,
                        risk_level="medium",
                    )
                    for version, code, action_date in candidate_specs
                ]
            )
            await db.commit()

            async def kline_loader(*args, **kwargs):
                return [
                    {
                        "date": source_date - timedelta(days=offset),
                        "close": 10,
                    }
                    for offset in range(5, -1, -1)
                ]

            snapshot = await TradingPlaybookMarketDataProvider(
                quote_api=_FakeQuoteAPI(
                    {
                        code: _quote_payload(code, 10, "bad-timestamp")
                        for code in codes
                    }
                ),
                kline_loader=kline_loader,
                realtime_limit_up_loader=lambda trade_date: asyncio.sleep(
                    0,
                    result=[],
                ),
            ).build_market_snapshot(
                db=db,
                source_trade_date=source_date,
                target_trade_date=target_date,
                stage="auction",
                as_of=as_of,
            )

        candidates = {item.stock_code: item for item in snapshot.candidates}
        self.assertEqual(set(candidates), {"000004", "000008"})
        self.assertEqual(
            candidates["000004"].features["plan_candidate_fact"][
                "plan_version_id"
            ],
            target_versions[3].id,
        )
        self.assertEqual(
            candidates["000008"].features["plan_candidate_fact"][
                "plan_version_id"
            ],
            source_versions[1].id,
        )

    async def test_selected_plan_version_does_not_depend_on_having_candidates(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        source_date = date(2026, 7, 10)
        target_date = date(2026, 7, 13)
        as_of = datetime(2026, 7, 13, 9, 25)
        async with self.session_factory() as db:
            db.add(
                Stock(
                    stock_code="000001",
                    stock_name="Unselected Draft Candidate",
                    market="SZ",
                    is_st=0,
                )
            )
            draft = TradingPlanVersion(
                source_trade_date=source_date,
                target_trade_date=target_date,
                stage="overnight",
                version_no=9,
                status="draft",
                input_hash="draft-with-candidate",
                generated_at=as_of,
            )
            active = TradingPlanVersion(
                source_trade_date=source_date,
                target_trade_date=target_date,
                stage="after_close",
                version_no=1,
                status="active",
                input_hash="active-without-candidate",
                generated_at=as_of,
            )
            db.add_all([draft, active])
            await db.flush()
            db.add(
                TradingPlanCandidate(
                    plan_version_id=draft.id,
                    stock_code="000001",
                    stock_name="Unselected Draft Candidate",
                    action_trade_date=target_date,
                    primary_mode_key="leader",
                    role="leader",
                    rank=1,
                    risk_level="medium",
                )
            )
            await db.commit()

            snapshot = await TradingPlaybookMarketDataProvider(
                quote_api=_FakeQuoteAPI(
                    {
                        "000001": _quote_payload(
                            "000001",
                            10,
                            "bad-timestamp",
                        )
                    }
                ),
                realtime_limit_up_loader=lambda trade_date: asyncio.sleep(
                    0,
                    result=[],
                ),
            ).build_market_snapshot(
                db=db,
                source_trade_date=source_date,
                target_trade_date=target_date,
                stage="auction",
                as_of=as_of,
            )

        self.assertEqual(snapshot.candidates, [])

    async def test_plan_only_theme_supports_ready_same_theme_auction_rank(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        source_date = date(2026, 7, 10)
        target_date = date(2026, 7, 13)
        as_of = datetime(2026, 7, 13, 9, 25)
        codes = ("000001", "000002")
        async with self.session_factory() as db:
            db.add_all(
                [
                    Stock(
                        stock_code=code,
                        stock_name=f"Plan Theme {code}",
                        market="SZ",
                        is_st=0,
                    )
                    for code in codes
                ]
            )
            plan_version = TradingPlanVersion(
                source_trade_date=source_date,
                target_trade_date=target_date,
                stage="overnight",
                version_no=1,
                status="active",
                input_hash="plan-theme",
                generated_at=datetime(2026, 7, 10, 16),
            )
            db.add(plan_version)
            await db.flush()
            db.add_all(
                [
                    TradingPlanCandidate(
                        plan_version_id=plan_version.id,
                        stock_code=code,
                        stock_name=f"Plan Theme {code}",
                        action_trade_date=target_date,
                        theme_name="Plan Robotics",
                        primary_mode_key="leader",
                        role="leader",
                        rank=rank,
                        risk_level="medium",
                    )
                    for rank, code in enumerate(codes, start=1)
                ]
            )
            await db.commit()

            async def kline_loader(*args, **kwargs):
                return [
                    {
                        "date": source_date - timedelta(days=offset),
                        "close": 10,
                    }
                    for offset in range(5, -1, -1)
                ]

            snapshot = await TradingPlaybookMarketDataProvider(
                quote_api=_FakeQuoteAPI(
                    {
                        "000001": _quote_payload(
                            "000001", 10.5, "20260713092400"
                        ),
                        "000002": _quote_payload(
                            "000002", 10.2, "20260713092400"
                        ),
                    }
                ),
                kline_loader=kline_loader,
                realtime_limit_up_loader=lambda trade_date: asyncio.sleep(
                    0,
                    result=[],
                ),
            ).build_market_snapshot(
                db=db,
                source_trade_date=source_date,
                target_trade_date=target_date,
                stage="auction",
                as_of=as_of,
            )

        candidates = {item.stock_code: item for item in snapshot.candidates}
        self.assertEqual(candidates["000001"].theme_name, "Plan Robotics")
        self.assertEqual(candidates["000002"].theme_name, "Plan Robotics")
        self.assertEqual(candidates["000001"].features["auction_quality"], "ready")
        self.assertEqual(candidates["000002"].features["auction_quality"], "ready")
        self.assertEqual(candidates["000001"].features["auction_theme_rank"], 1)
        self.assertEqual(candidates["000002"].features["auction_theme_rank"], 2)
        self.assertFalse(
            any("missing auction theme" in warning for warning in snapshot.quality.warnings)
        )

    async def test_force_degraded_overrides_otherwise_ready_snapshot(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        trade_date = date(2026, 7, 13)
        as_of = datetime(2026, 7, 13, 15)
        async with self.session_factory() as db:
            db.add(
                Stock(
                    stock_code="000001",
                    stock_name="Ready Name",
                    market="SZ",
                    is_st=0,
                )
            )
            await db.commit()

            async def kline_loader(*args, **kwargs):
                return [
                    {"close": close}
                    for close in [10, 10.1, 10.2, 10.2, 10.3, 10.4]
                ]

            snapshot = await TradingPlaybookMarketDataProvider(
                quote_api=_FakeQuoteAPI(
                    {
                        "000001": _quote_payload(
                            "000001", 10, "20260713150000"
                        )
                    }
                ),
                kline_loader=kline_loader,
                realtime_limit_up_loader=lambda trade_date: asyncio.sleep(
                    0, result=[]
                ),
            ).build_market_snapshot(
                db=db,
                source_trade_date=trade_date,
                target_trade_date=trade_date,
                stage="close",
                as_of=as_of,
                force_degraded=True,
            )

        self.assertEqual(snapshot.quality.status, "degraded")
        self.assertTrue(snapshot.quality.forced_degraded)
        self.assertEqual(snapshot.market_features["full_market_speed_ranks"], {})
        self.assertNotIn("speed_rank", snapshot.candidates[0].features)
        self.assertNotIn("speed_pct", snapshot.candidates[0].features)
        self.assertTrue(
            any("force_degraded" in warning for warning in snapshot.quality.warnings)
        )


if __name__ == "__main__":
    unittest.main()
