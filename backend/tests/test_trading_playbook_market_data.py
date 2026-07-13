import asyncio
import importlib.util
import unittest
from dataclasses import FrozenInstanceError, fields
from datetime import date, datetime, timedelta, timezone

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
            ["status", "as_of", "source", "stale", "warnings"],
        )
        self.assertEqual(quality.warnings, [])
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
        self.assertTrue(any("chunk" in warning for warning in snapshot.quality.warnings))
        self.assertEqual(len(api.calls), 2)
        self.assertLessEqual(api.max_active, 2)

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
        self.assertTrue(any("stale quote" in warning for warning in snapshot.quality.warnings))

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

        expected = {
            "n_day_high": False,
            "consolidation_days": 0,
            "trend_established": False,
            "kline_quality": "missing",
        }
        for loader in (None, short_loader, failed_loader, malformed_loader):
            with self.subTest(loader=loader):
                provider = TradingPlaybookMarketDataProvider(
                    quote_api=_FakeQuoteAPI(),
                    kline_loader=loader,
                )
                self.assertEqual(
                    await provider.kline_features("000001", "SZ", "Test"),
                    expected,
                )


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

    async def test_bounded_union_quotes_full_market_and_loads_klines_only_for_candidates(self):
        from app.services.trading_playbook.market_data import (
            TradingPlaybookMarketDataProvider,
        )

        source_date = date(2026, 7, 10)
        target_date = date(2026, 7, 13)
        as_of = datetime(2026, 7, 13, 9, 30)
        regular_codes = [f"{index:06d}" for index in range(206)]
        eligible_codes = regular_codes + ["830001"]

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
            db.add_all(stocks)
            await db.flush()
            stock_by_code = {stock.stock_code: stock for stock in stocks}

            for offset in range(10):
                db.add(
                    MarketReviewStockDaily(
                        trade_date=source_date - timedelta(days=offset),
                        stock_id=stock_by_code["000200"].id,
                        stock_code="000200",
                        stock_name="Review Candidate",
                        today_touched_limit_up=True,
                        limit_up_reason="AI review",
                    )
                )
            db.add_all(
                [
                    MarketReviewStockDaily(
                        trade_date=source_date - timedelta(days=10),
                        stock_id=stock_by_code["000201"].id,
                        stock_code="000201",
                        stock_name="Eleventh Review Date",
                        today_touched_limit_up=True,
                        limit_up_reason="too old",
                    ),
                    MarketReviewStockDaily(
                        trade_date=target_date,
                        stock_id=stock_by_code["000201"].id,
                        stock_code="000201",
                        stock_name="Future Review",
                        today_touched_limit_up=True,
                        limit_up_reason="future",
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
                for code in eligible_codes
            }
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
                    {"close": close}
                    for close in [10, 10.1, 10.2, 10.2, 10.3, 10.4]
                ]

            async def realtime_loader(trade_date):
                return [
                    {
                        "stock_code": "830001",
                        "stock_name": "BJ Candidate",
                        "reason_category": "robotics",
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
                as_of - timedelta(seconds=3),
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
        self.assertEqual(requested_quotes, set(eligible_codes))
        self.assertNotIn("600001", requested_quotes)
        self.assertEqual(
            set(kline_calls),
            set(regular_codes[:200]) | {"000200", "000202", "830001"},
        )
        candidates = {item.stock_code: item for item in snapshot.candidates}
        self.assertNotIn("000201", candidates)
        self.assertNotIn("000203", candidates)
        self.assertNotIn("000204", candidates)
        self.assertEqual(candidates["000003"].features["change_rank"], 1)
        self.assertEqual(candidates["000001"].features["change_rank"], 2)
        self.assertEqual(candidates["000002"].features["change_rank"], 3)
        self.assertEqual(candidates["000000"].features["speed_rank"], 1)
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
                "000001": _quote_payload("000001", 10.5, "20260713092000"),
                "000002": _quote_payload("000002", 10.2, "20260713092100"),
                "000003": _quote_payload("000003", 10.1, "20260713093000"),
            }

            async def kline_loader(*args, **kwargs):
                return [
                    {"close": close}
                    for close in [10, 10.1, 10.2, 10.2, 10.3, 10.4]
                ]

            async def realtime_loader(trade_date):
                return [
                    {
                        "stock_code": code,
                        "stock_name": f"Auction {code}",
                        "reason_category": "AI",
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
        self.assertEqual(invalid.features["auction_quality"], "missing")
        self.assertNotIn("auction_change_pct", invalid.features)
        self.assertNotIn("auction_amount", invalid.features)
        self.assertNotIn("auction_theme_rank", invalid.features)
        self.assertTrue(
            any(
                evidence["source"] == "auction"
                and evidence["quality"] == "missing"
                for evidence in invalid.evidence
            )
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
        self.assertEqual(snapshot.market_features["full_market_speed_ranks"], {})
        self.assertNotIn("speed_rank", snapshot.candidates[0].features)
        self.assertTrue(
            any("force_degraded" in warning for warning in snapshot.quality.warnings)
        )


if __name__ == "__main__":
    unittest.main()
