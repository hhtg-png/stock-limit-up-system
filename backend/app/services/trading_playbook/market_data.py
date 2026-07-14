"""Point-in-time market data collection for trading playbook snapshots."""

import asyncio
import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from sqlalchemy import desc, select

from app.config import settings
from app.models.market_review import MarketReviewStockDaily
from app.models.stock import Stock
from app.models.trading_playbook import TradingPlanCandidate, TradingPlanVersion
from app.services.realtime_limit_up_service import RealtimeLimitUpSnapshot
from app.services.trading_playbook.domain import (
    CandidateSnapshot,
    DataQuality,
    MarketSnapshot,
    QuotePoint,
    QuoteSnapshot,
)
from app.services.trading_playbook.context_service import (
    FULL_MARKET_CONTEXT_FIELDS,
)
from app.utils.market_data_sanitizer import normalize_change_pct

QuoteFieldQuality = Dict[str, Dict[str, str]]
_NONFINITE = object()
_FULL_MARKET_CONTEXT_FIELDS = FULL_MARKET_CONTEXT_FIELDS
_FULL_MARKET_PRIOR_WINDOWS = {
    "",
    "outbreak",
    "first_divergence",
    "divergence_exhaustion",
    "divergence_to_consensus",
    "stronger_confirmation",
    "second_divergence",
    "stage_three",
    "decline",
}


@dataclass(frozen=True)
class _QuoteCacheRecord:
    price: float
    captured_at: datetime


@dataclass(frozen=True)
class _KlineBuildResult:
    features: Dict[str, Any]
    available_at: Optional[datetime]
    reason: Optional[str] = None
    evidence_trade_date: Optional[date] = None


class TradingPlaybookMarketDataProvider:
    """Collect normalized quote and K-line facts through injectable clients."""

    MAX_QUOTE_BATCH_SIZE = 80
    MAX_CONCURRENCY = 16
    STALE_AFTER_SECONDS = 10
    SPEED_MAX_INTERVAL_SECONDS = 60

    def __init__(
        self,
        quote_api: Any,
        kline_loader: Optional[Callable[..., Any]] = None,
        batch_size: int = 80,
        max_concurrency: int = 4,
        realtime_limit_up_loader: Optional[Callable[..., Any]] = None,
        full_market_context_loader: Optional[Callable[..., Any]] = None,
        kline_stage_timeout_seconds: Optional[float] = None,
        kline_cancel_grace_seconds: Optional[float] = None,
    ):
        self.quote_api = quote_api
        self.quote_client = quote_api
        self.kline_loader = kline_loader
        self.batch_size = min(max(int(batch_size), 1), self.MAX_QUOTE_BATCH_SIZE)
        self.max_concurrency = min(
            max(int(max_concurrency), 1),
            self.MAX_CONCURRENCY,
        )
        self.realtime_limit_up_loader = realtime_limit_up_loader
        self.full_market_context_loader = full_market_context_loader
        timeout = (
            settings.TRADING_PLAYBOOK_KLINE_STAGE_TIMEOUT_SECONDS
            if kline_stage_timeout_seconds is None
            else kline_stage_timeout_seconds
        )
        self.kline_stage_timeout_seconds = max(float(timeout), 0.0)
        cancel_grace = (
            settings.TRADING_PLAYBOOK_KLINE_CANCEL_GRACE_SECONDS
            if kline_cancel_grace_seconds is None
            else kline_cancel_grace_seconds
        )
        self.kline_cancel_grace_seconds = max(float(cancel_grace), 0.0)
        self._previous_prices: Dict[str, _QuoteCacheRecord] = {}
        self._quote_state_lock = asyncio.Lock()
        self._quote_semaphore = asyncio.Semaphore(self.max_concurrency)
        self._kline_semaphore = asyncio.Semaphore(self.max_concurrency)
        self._orphan_kline_tasks: set[asyncio.Task] = set()

    def _track_orphan_kline_task(self, task: asyncio.Task) -> None:
        if task in self._orphan_kline_tasks:
            return
        self._orphan_kline_tasks.add(task)

        def done(completed: asyncio.Task) -> None:
            self._orphan_kline_tasks.discard(completed)
            self._consume_task_result(completed)

        task.add_done_callback(done)

    async def aclose(self) -> None:
        """Bounded drain for cancellation-suppressing K-line workers."""
        tasks = list(self._orphan_kline_tasks)
        if not tasks:
            return
        for task in tasks:
            if not task.done():
                task.cancel()
        _done, pending = await asyncio.wait(
            tasks,
            timeout=self.kline_cancel_grace_seconds,
        )
        for task in pending:
            self._track_orphan_kline_task(task)

    async def quote_snapshot(
        self,
        stock_codes: List[str],
        trade_date: date,
        as_of: datetime,
    ) -> QuoteSnapshot:
        """Fetch a normalized quote snapshot.

        The first observation for a code uses ``0.0`` as a speed-delta baseline;
        it is not evidence that the market itself had zero speed.
        """
        snapshot, _field_quality = await self._quote_snapshot_with_quality(
            stock_codes,
            trade_date,
            as_of,
        )
        return snapshot

    async def _quote_snapshot_with_quality(
        self,
        stock_codes: List[str],
        trade_date: date,
        as_of: datetime,
        *,
        stage: Optional[str] = None,
        evidence_trade_date: Optional[date] = None,
    ) -> Tuple[QuoteSnapshot, QuoteFieldQuality]:
        return await self._collect_quote_snapshot(
            stock_codes,
            trade_date,
            as_of,
            stage=stage,
            evidence_trade_date=evidence_trade_date,
        )

    async def _collect_quote_snapshot(
        self,
        stock_codes: List[str],
        trade_date: date,
        as_of: datetime,
        *,
        stage: Optional[str] = None,
        evidence_trade_date: Optional[date] = None,
    ) -> Tuple[QuoteSnapshot, QuoteFieldQuality]:
        requested_codes = sorted(
            {
                normalized
                for code in stock_codes
                if (normalized := self._normalize_code(code))
            }
        )
        if not requested_codes:
            return (
                QuoteSnapshot(
                    trade_date=trade_date,
                    quotes={},
                    quality=DataQuality(
                        status="ready",
                        as_of=as_of,
                        source="tencent",
                    ),
                ),
                {},
            )

        chunks = [
            requested_codes[start:start + self.batch_size]
            for start in range(0, len(requested_codes), self.batch_size)
        ]

        async def fetch_chunk(chunk: List[str]) -> Tuple[Dict[str, Dict], Optional[str]]:
            async with self._quote_semaphore:
                try:
                    response = await self.quote_api.get_quotes_batch(chunk)
                    if not isinstance(response, dict):
                        raise TypeError("quote API returned a non-dictionary payload")
                    return response, None
                except Exception as exc:
                    return {}, f"quote chunk failed ({','.join(chunk)}): {exc}"

        chunk_results = await asyncio.gather(
            *(fetch_chunk(chunk) for chunk in chunks)
        )
        raw_quotes: Dict[str, Dict] = {}
        warnings: List[str] = []
        chunk_failed = False
        for response, warning in chunk_results:
            if warning:
                warnings.append(warning)
                chunk_failed = True
            for response_code, raw_quote in response.items():
                if not isinstance(raw_quote, dict):
                    warnings.append(f"invalid quote payload for {response_code}")
                    continue
                code = self._normalize_code(
                    self._pick(raw_quote, "code", "stock_code", "symbol")
                    or response_code
                )
                if code in requested_codes:
                    raw_quotes[code] = raw_quote

        quotes: Dict[str, QuotePoint] = {}
        field_quality: QuoteFieldQuality = {}
        stale_codes = []
        future_quote_found = False
        invalid_price_found = False
        for code in requested_codes:
            raw_quote = raw_quotes.get(code)
            if raw_quote is None:
                warnings.append(f"missing quote for {code}")
                continue

            captured_at, valid_timestamp = self._parse_quote_datetime(
                self._pick(
                    raw_quote,
                    "datetime",
                    "timestamp",
                    "captured_at",
                    "quote_time",
                    "time",
                )
            )
            quality: Dict[str, str] = {}
            if not valid_timestamp:
                captured_at = as_of
                quality["timestamp"] = "fallback"
                warnings.append(
                    f"invalid quote timestamp for {code}; used as_of fallback"
                )
            else:
                age_seconds = self._age_seconds(as_of, captured_at)
                if age_seconds < 0:
                    future_quote_found = True
                    warnings.append(
                        f"future quote for {code} at {captured_at.isoformat()}"
                    )
                    continue
                quality["timestamp"] = "ready"
                baseline_ready = self._quote_baseline_ready(
                    stage=stage,
                    trade_date=trade_date,
                    evidence_trade_date=evidence_trade_date,
                    captured_at=captured_at,
                    as_of=as_of,
                )
                quality["_baseline_freshness"] = (
                    "ready" if baseline_ready else "stale"
                )
                if not baseline_ready:
                    stale_codes.append(code)
                    warnings.append(
                        f"stale quote for {code} at {captured_at.isoformat()}"
                    )

            price = self._optional_float(
                self._quote_source_value(
                    raw_quote,
                    "price",
                    "current_price",
                    "last_price",
                )
            )
            if price is None or price <= 0:
                invalid_price_found = True
                warnings.append(f"missing or invalid quote price for {code}")
                continue
            quality["price"] = "ready"

            pre_close = self._optional_float(
                self._quote_source_value(
                    raw_quote,
                    "pre_close",
                    "previous_close",
                    "preclose",
                    "last_close",
                )
            )
            if pre_close is None or pre_close <= 0:
                pre_close = None
                quality["pre_close"] = "missing"
            else:
                quality["pre_close"] = "ready"

            amount = self._optional_float(
                self._quote_source_value(
                    raw_quote,
                    "amount",
                    "turnover",
                    "trade_amount",
                )
            )
            if amount is None or amount < 0:
                amount = None
                quality["amount"] = "missing"
            else:
                quality["amount"] = "ready"

            open_price = self._optional_float(
                self._quote_source_value(raw_quote, "open", "open_price")
            )
            quality["open_price"] = (
                "ready" if open_price is not None and open_price >= 0 else "missing"
            )
            turnover_rate = self._optional_float(
                self._quote_source_value(
                    raw_quote,
                    "turnover_rate",
                    "turnover_pct",
                )
            )
            quality["turnover_rate"] = (
                "ready"
                if turnover_rate is not None and turnover_rate >= 0
                else "missing"
            )
            bid1_price = self._optional_float(
                self._quote_source_value(
                    raw_quote,
                    "bid1_price",
                    "bid_price_1",
                )
            )
            quality["bid1_price"] = (
                "ready"
                if bid1_price is not None and bid1_price >= 0
                else "missing"
            )
            bid1_volume = self._optional_float(
                self._quote_source_value(
                    raw_quote,
                    "bid1_volume",
                    "bid_volume_1",
                )
            )
            quality["bid1_volume"] = (
                "ready"
                if bid1_volume is not None and bid1_volume >= 0
                else "missing"
            )
            limit_up = self._optional_float(
                self._quote_source_value(
                    raw_quote,
                    "limit_up",
                    "limit_up_price",
                )
            )
            quality["limit_up"] = (
                "ready" if limit_up is not None and limit_up > 0 else "missing"
            )
            raw_change_pct = self._quote_source_value(
                raw_quote,
                "change_pct",
                "change_percent",
                "percent",
            )
            change_pct = normalize_change_pct(
                raw_change_pct,
                price=price,
                amount=amount,
            )
            if change_pct is not None:
                quality["change_pct"] = "ready"
            elif pre_close is not None:
                computed_change = (price / pre_close - 1) * 100
                if math.isfinite(computed_change):
                    change_pct = round(computed_change, 4)
                    quality["change_pct"] = "computed"
                else:
                    change_pct = math.nan
                    quality["change_pct"] = "missing"
            else:
                change_pct = math.nan
                quality["change_pct"] = "missing"

            speed_timestamp_ready = (
                quality["timestamp"] == "ready"
                and self._speed_timestamp_ready(captured_at, as_of, trade_date)
            )
            speed_pct, quality["speed_pct"] = await self._speed_and_cache(
                code,
                price,
                captured_at,
                timestamp_ready=speed_timestamp_ready,
            )
            quotes[code] = QuotePoint(
                stock_code=code,
                stock_name=str(
                    self._pick(raw_quote, "name", "stock_name", "security_name")
                    or ""
                ),
                price=price,
                pre_close=pre_close if pre_close is not None else math.nan,
                open_price=open_price if open_price is not None else math.nan,
                change_pct=change_pct,
                speed_pct=speed_pct,
                amount=amount if amount is not None else math.nan,
                turnover_rate=(
                    turnover_rate if turnover_rate is not None else math.nan
                ),
                bid1_price=bid1_price if bid1_price is not None else math.nan,
                bid1_volume=(
                    bid1_volume if bid1_volume is not None else math.nan
                ),
                limit_up=limit_up if limit_up is not None else math.nan,
                captured_at=captured_at,
            )
            field_quality[code] = quality
            missing_fields = sorted(
                field
                for field, status in quality.items()
                if status == "missing"
            )
            if missing_fields:
                warnings.append(
                    f"missing quote fields for {code}: {','.join(missing_fields)}"
                )

        coverage = len(quotes) / len(requested_codes)
        status = (
            "degraded"
            if (
                coverage < 0.9
                or chunk_failed
                or future_quote_found
                or invalid_price_found
            )
            else "ready"
        )
        return (
            QuoteSnapshot(
                trade_date=trade_date,
                quotes=quotes,
                quality=DataQuality(
                    status=status,
                    as_of=as_of,
                    source="tencent",
                    stale=bool(stale_codes),
                    warnings=warnings,
                ),
            ),
            field_quality,
        )

    @classmethod
    def _quote_baseline_ready(
        cls,
        *,
        stage: Optional[str],
        trade_date: date,
        evidence_trade_date: Optional[date],
        captured_at: datetime,
        as_of: datetime,
    ) -> bool:
        local_captured = cls._china_datetime(captured_at)
        local_as_of = cls._china_datetime(as_of)
        if local_captured > local_as_of:
            return False
        captured_time = local_captured.time().replace(tzinfo=None)
        if stage == "after_close":
            return (
                local_captured.date() == trade_date
                and captured_time >= time(15, 0)
            )
        if stage == "overnight":
            return (
                evidence_trade_date is not None
                and local_captured.date() == evidence_trade_date
                and captured_time >= time(15, 0)
            )
        if stage == "auction":
            return (
                cls._is_auction_timestamp(local_captured, trade_date)
                and cls._age_seconds(local_as_of, local_captured)
                <= settings.TRADING_PLAYBOOK_AUCTION_QUOTE_MAX_AGE_SECONDS
            )
        return cls._age_seconds(local_as_of, local_captured) <= cls.STALE_AFTER_SECONDS

    @classmethod
    def _speed_timestamp_ready(
        cls,
        captured_at: datetime,
        as_of: datetime,
        trade_date: date,
    ) -> bool:
        local_captured = cls._china_datetime(captured_at)
        local_as_of = cls._china_datetime(as_of)
        return (
            local_captured.date() == trade_date
            and 0 <= cls._age_seconds(local_as_of, local_captured)
            <= cls.SPEED_MAX_INTERVAL_SECONDS
        )

    async def kline_features(
        self,
        stock_code: str,
        market: str,
        stock_name: str,
    ) -> Dict[str, Any]:
        missing = {
            "n_day_high": False,
            "prior_n_day_high": False,
            "consolidation_days": 0,
            "trend_established": False,
            "kline_quality": "missing",
        }
        if self.kline_loader is None:
            return missing
        try:
            async with self._kline_semaphore:
                points = await self.kline_loader(
                    stock_code,
                    market,
                    "day",
                    60,
                    stock_name=stock_name,
                )
            return self._calculate_kline_features(points)
        except Exception:
            return missing

    @staticmethod
    def _consume_task_result(task: asyncio.Task) -> None:
        try:
            task.exception()
        except BaseException:
            pass

    @staticmethod
    def _calculate_kline_features(points: Any) -> Dict[str, Any]:
        missing = {
            "n_day_high": False,
            "prior_n_day_high": False,
            "consolidation_days": 0,
            "trend_established": False,
            "kline_quality": "missing",
        }
        try:
            closes = []
            for point in points:
                if not isinstance(point, Mapping):
                    return missing
                raw_close = point.get("close")
                # A missing/None close is an incomplete point, not bad evidence.
                if raw_close is None:
                    continue
                try:
                    close = float(raw_close)
                except Exception:
                    return missing
                if not math.isfinite(close) or close <= 0:
                    return missing
                closes.append(close)
            if len(closes) < 6:
                return missing
            prior_high = max(closes[:-1])
            recent = closes[-5:-1]
            band = (max(recent) - min(recent)) / max(min(recent), 0.01)
            return {
                "n_day_high": closes[-1] > prior_high,
                "prior_n_day_high": closes[-2] > max(closes[:-2]),
                "consolidation_days": 4 if band <= 0.08 else 0,
                "trend_established": closes[-1] > sum(closes[-6:-1]) / 5,
                "kline_quality": "ready",
            }
        except Exception:
            return missing

    async def _kline_features_as_of(
        self,
        stock_code: str,
        market: str,
        stock_name: str,
        source_trade_date: date,
        as_of: datetime,
    ) -> _KlineBuildResult:
        missing = self._calculate_kline_features([])
        if self.kline_loader is None:
            return _KlineBuildResult(missing, None, "kline loader unavailable")
        try:
            async with self._kline_semaphore:
                points = await self.kline_loader(
                    stock_code,
                    market,
                    "day",
                    60,
                    stock_name=stock_name,
                )
        except Exception as exc:
            return _KlineBuildResult(missing, None, f"kline load failed: {exc}")

        local_as_of = self._china_datetime(as_of)
        accepted = []
        accepted_times = []
        accepted_dates = []
        try:
            for point in points:
                if not isinstance(point, Mapping):
                    return _KlineBuildResult(
                        missing,
                        None,
                        "invalid kline point",
                    )
                bar_date = self._kline_trade_date(point)
                if bar_date is None:
                    return _KlineBuildResult(
                        missing,
                        None,
                        "kline point missing usable bar date",
                    )
                available_at = self._kline_available_at(point)
                if available_at is None:
                    return _KlineBuildResult(
                        missing,
                        None,
                        "kline point missing usable provenance",
                    )
                if (
                    bar_date > source_trade_date
                    or available_at > local_as_of
                ):
                    continue
                accepted.append(point)
                accepted_times.append(available_at)
                accepted_dates.append(bar_date)
        except Exception as exc:
            return _KlineBuildResult(
                missing,
                None,
                f"invalid kline provenance: {exc}",
            )

        features = self._calculate_kline_features(accepted)
        available_at = max(accepted_times) if accepted_times else None
        reason = None
        if features["kline_quality"] != "ready":
            reason = "insufficient point-in-time kline observations"
        return _KlineBuildResult(
            features,
            self._evidence_datetime(available_at, as_of),
            reason,
            max(accepted_dates) if accepted_dates else None,
        )

    async def build_market_snapshot(
        self,
        db: Any,
        source_trade_date: date,
        target_trade_date: date,
        stage: str,
        as_of: datetime,
        force_degraded: bool = False,
        force_degraded_reason: Optional[str] = None,
    ) -> MarketSnapshot:
        local_as_of = self._china_datetime(as_of)
        database_as_of = local_as_of.replace(tzinfo=None)
        market_context, market_context_evidence, context_warning = (
            await self._load_full_market_context(
                source_trade_date,
                stage,
                as_of,
            )
        )
        evidence_trade_date = source_trade_date
        if market_context_evidence:
            parsed_evidence_date = self._parse_date_value(
                market_context_evidence[0].get("evidence_trade_date")
            )
            if parsed_evidence_date is not None:
                evidence_trade_date = parsed_evidence_date
        stock_result = await db.execute(select(Stock).order_by(Stock.stock_code))
        eligible_stocks = [
            stock
            for stock in stock_result.scalars().all()
            if not self._is_st_stock(stock)
        ]
        stock_by_code = {
            self._normalize_code(stock.stock_code): stock
            for stock in eligible_stocks
        }
        universe_codes = sorted(stock_by_code)
        quote_snapshot, quote_field_quality = await self._quote_snapshot_with_quality(
            universe_codes,
            min(target_trade_date, local_as_of.date()),
            as_of,
            stage=stage,
            evidence_trade_date=evidence_trade_date,
        )

        change_order = sorted(
            (
                quote
                for quote in quote_snapshot.quotes.values()
                if (
                    quote_field_quality.get(quote.stock_code, {}).get(
                        "change_pct"
                    )
                    in {"ready", "computed"}
                    and self._rank_timestamp_quality(
                        quote,
                        as_of,
                        quote_field_quality.get(quote.stock_code, {}),
                    )
                    == "ready"
                )
            ),
            key=lambda quote: (-quote.change_pct, quote.stock_code),
        )
        speed_order = sorted(
            (
                quote
                for quote in quote_snapshot.quotes.values()
                if (
                    quote_field_quality.get(quote.stock_code, {}).get(
                        "speed_pct"
                    )
                    == "ready"
                    and self._rank_timestamp_quality(
                        quote,
                        as_of,
                        quote_field_quality.get(quote.stock_code, {}),
                    )
                    == "ready"
                )
            ),
            key=lambda quote: (-quote.speed_pct, quote.stock_code),
        )
        change_ranks = {
            quote.stock_code: rank
            for rank, quote in enumerate(change_order, start=1)
        }
        speed_ranks = {
            quote.stock_code: rank
            for rank, quote in enumerate(speed_order, start=1)
        }
        quote_candidate_codes = {
            quote.stock_code for quote in change_order[:200]
        } | {
            quote.stock_code for quote in speed_order[:200]
        }
        candidate_codes = set(quote_candidate_codes)
        realtime_candidate_codes = set()
        plan_candidate_codes = set()
        warnings = list(quote_snapshot.quality.warnings)
        if context_warning:
            warnings.append(context_warning)

        realtime_rows: List[Dict[str, Any]] = []
        realtime_snapshot = RealtimeLimitUpSnapshot(
            items=[],
            authoritative=False,
            complete=False,
            evidence_trade_date=None,
            warning="realtime limit-up pool unavailable",
        )
        pool_trade_date = (
            evidence_trade_date
            if stage == "overnight"
            else min(target_trade_date, local_as_of.date())
        )
        expected_realtime_date = (
            evidence_trade_date if stage == "overnight" else pool_trade_date
        )
        try:
            realtime_snapshot = await self._load_realtime_limit_up(pool_trade_date)
            realtime_rows = list(realtime_snapshot.items)
            if realtime_snapshot.warning:
                warnings.append(realtime_snapshot.warning)
        except Exception as exc:
            warnings.append(f"realtime limit-up pool failed: {exc}")
        realtime_by_code: Dict[str, Dict[str, Any]] = {}
        realtime_provenance_by_code: Dict[str, datetime] = {}
        realtime_discovery_by_code: Dict[str, Dict[str, Any]] = {}
        realtime_context_rows: List[Dict[str, Any]] = []
        realtime_context_complete = (
            realtime_snapshot.authoritative
            and realtime_snapshot.complete
            and realtime_snapshot.evidence_trade_date == expected_realtime_date
        )
        realtime_snapshot_publishable = realtime_context_complete
        if (
            realtime_snapshot.evidence_trade_date is not None
            and realtime_snapshot.evidence_trade_date != expected_realtime_date
        ):
            warnings.append("mismatched realtime snapshot evidence date")
        for row in realtime_rows or []:
            if not isinstance(row, dict):
                warnings.append("invalid realtime limit-up row")
                realtime_context_complete = False
                continue
            code = self._normalize_code(
                self._pick(row, "stock_code", "code", "symbol")
            )
            if code in stock_by_code:
                # A non-authoritative snapshot may still broaden the bounded
                # candidate union.  No other values from that row are allowed
                # to enter matching until the complete snapshot contract and
                # row provenance have both passed below.
                candidate_codes.add(code)
                realtime_candidate_codes.add(code)
                realtime_discovery_by_code[code] = {
                    "as_of": None,
                    "evidence_trade_date": (
                        realtime_snapshot.evidence_trade_date
                    ),
                    "quality": "degraded",
                    "warning": (
                        realtime_snapshot.warning
                        or "realtime candidate row is not publishable"
                    ),
                }
            available_at = self._realtime_available_at(row, pool_trade_date)
            if available_at is None:
                warnings.append(
                    f"realtime row missing usable provenance for {code or 'unknown'}"
                )
                realtime_context_complete = False
                continue
            if code in realtime_discovery_by_code:
                realtime_discovery_by_code[code]["as_of"] = (
                    self._evidence_datetime(available_at, as_of)
                )
            if available_at > local_as_of:
                warnings.append(f"future realtime row for {code or 'unknown'}")
                realtime_context_complete = False
                continue
            row_trade_date = self._parse_date_value(
                self._pick(row, "trade_date", "date")
            )
            if row_trade_date is None:
                row_trade_date = available_at.date()
            if row_trade_date != expected_realtime_date:
                warnings.append(
                    f"mismatched realtime row date for {code or 'unknown'}"
                )
                realtime_context_complete = False
                continue
            normalized_realtime = self._normalize_realtime_row(row)
            realtime_context_rows.append(normalized_realtime)
            if code in stock_by_code and realtime_snapshot_publishable:
                realtime_by_code[code] = normalized_realtime
                realtime_provenance_by_code[code] = self._evidence_datetime(
                    available_at,
                    as_of,
                )

        review_dates_result = await db.execute(
            select(MarketReviewStockDaily.trade_date)
            .where(
                MarketReviewStockDaily.trade_date <= source_trade_date,
                MarketReviewStockDaily.created_at <= database_as_of,
                MarketReviewStockDaily.updated_at <= database_as_of,
            )
            .distinct()
            .order_by(desc(MarketReviewStockDaily.trade_date))
            .limit(10)
        )
        review_dates = list(review_dates_result.scalars().all())
        review_rows: List[MarketReviewStockDaily] = []
        if review_dates:
            review_result = await db.execute(
                select(MarketReviewStockDaily)
                .where(
                    MarketReviewStockDaily.trade_date.in_(review_dates),
                    MarketReviewStockDaily.created_at <= database_as_of,
                    MarketReviewStockDaily.updated_at <= database_as_of,
                )
                .order_by(
                    desc(MarketReviewStockDaily.trade_date),
                    MarketReviewStockDaily.stock_code,
                )
            )
            review_rows = list(review_result.scalars().all())
        review_history_by_code: Dict[str, List[MarketReviewStockDaily]] = {}
        for row in review_rows:
            code = self._normalize_code(row.stock_code)
            if code in stock_by_code:
                review_history_by_code.setdefault(code, []).append(row)
                candidate_codes.add(code)

        relevant_plan_dates = {source_trade_date, target_trade_date}
        plan_version_result = await db.execute(
            select(TradingPlanVersion)
            .where(
                TradingPlanVersion.source_trade_date <= source_trade_date,
                TradingPlanVersion.target_trade_date.in_(relevant_plan_dates),
                TradingPlanVersion.generated_at <= database_as_of,
                TradingPlanVersion.status.in_(("active", "confirmed", "draft")),
            )
            .order_by(
                desc(TradingPlanVersion.target_trade_date),
                desc(TradingPlanVersion.version_no),
                desc(TradingPlanVersion.generated_at),
            )
        )
        eligible_plan_versions = list(plan_version_result.scalars().all())
        versions_by_target: Dict[date, Dict[int, TradingPlanVersion]] = {}
        for plan_version in eligible_plan_versions:
            versions_by_target.setdefault(
                plan_version.target_trade_date,
                {},
            )[plan_version.id] = plan_version
        status_precedence = {"draft": 1, "confirmed": 2, "active": 3}
        selected_version_ids = {
            max(
                versions.values(),
                key=lambda version: (
                    status_precedence.get(version.status, 0),
                    version.version_no,
                    version.generated_at,
                    version.id,
                ),
            ).id
            for versions in versions_by_target.values()
        }
        selected_versions = {
            version.id: version
            for version in eligible_plan_versions
            if version.id in selected_version_ids
        }
        plan_rows: List[Tuple[TradingPlanCandidate, TradingPlanVersion]] = []
        if selected_version_ids:
            plan_candidate_result = await db.execute(
                select(TradingPlanCandidate)
                .where(
                    TradingPlanCandidate.plan_version_id.in_(
                        selected_version_ids
                    ),
                    TradingPlanCandidate.action_trade_date.in_(
                        relevant_plan_dates
                    ),
                    TradingPlanCandidate.action_trade_date
                    <= target_trade_date,
                )
                .order_by(
                    desc(TradingPlanCandidate.action_trade_date),
                    TradingPlanCandidate.rank,
                    TradingPlanCandidate.stock_code,
                )
            )
            plan_rows = [
                (candidate, selected_versions[candidate.plan_version_id])
                for candidate in plan_candidate_result.scalars().all()
            ]
        plan_by_code: Dict[str, Tuple[TradingPlanCandidate, TradingPlanVersion]] = {}
        for candidate, plan_version in plan_rows:
            code = self._normalize_code(candidate.stock_code)
            if code in stock_by_code:
                plan_by_code.setdefault(code, (candidate, plan_version))
                candidate_codes.add(code)
                plan_candidate_codes.add(code)

        candidate_codes.intersection_update(stock_by_code)
        required_codes = (
            realtime_candidate_codes | plan_candidate_codes
        ) & candidate_codes
        quote_priority_codes = quote_candidate_codes & candidate_codes
        review_only_codes = candidate_codes - required_codes - quote_priority_codes
        kline_load_order = (
            sorted(required_codes)
            + sorted(quote_priority_codes - required_codes)
            + sorted(review_only_codes)
        )
        ordered_candidate_codes = sorted(candidate_codes)
        async def load_kline(code: str) -> Tuple[str, _KlineBuildResult]:
            stock = stock_by_code[code]
            return code, await self._kline_features_as_of(
                code,
                self._infer_market(code, stock.market),
                stock.stock_name,
                source_trade_date,
                as_of,
            )

        kline_queue: asyncio.Queue[str] = asyncio.Queue()
        for code in kline_load_order:
            kline_queue.put_nowait(code)
        stop_kline_workers = asyncio.Event()
        completed_klines: Dict[str, _KlineBuildResult] = {}

        async def kline_worker() -> None:
            while not stop_kline_workers.is_set():
                try:
                    code = kline_queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    result_code, result = await load_kline(code)
                    if not stop_kline_workers.is_set():
                        completed_klines[result_code] = result
                finally:
                    kline_queue.task_done()

        workers = [
            asyncio.create_task(kline_worker())
            for _ in range(min(self.max_concurrency, len(kline_load_order)))
        ]

        async def stop_workers() -> None:
            stop_kline_workers.set()
            for worker in workers:
                if not worker.done():
                    worker.cancel()
            if not workers:
                return
            _done, stubborn = await asyncio.wait(
                workers,
                timeout=self.kline_cancel_grace_seconds,
            )
            for worker in stubborn:
                self._track_orphan_kline_task(worker)

        try:
            if workers:
                _done, pending = await asyncio.wait(
                    workers,
                    timeout=self.kline_stage_timeout_seconds,
                )
                if pending:
                    on_time_klines = dict(completed_klines)
                    await stop_workers()
                else:
                    on_time_klines = dict(completed_klines)
            else:
                on_time_klines = {}
        except BaseException:
            await stop_workers()
            raise

        missing_kline = self._calculate_kline_features([])
        kline_by_code: Dict[str, _KlineBuildResult] = {
            code: _KlineBuildResult(
                dict(missing_kline),
                None,
                "kline stage deadline exceeded",
            )
            for code in kline_load_order
            if code not in on_time_klines
        }
        kline_by_code.update(on_time_klines)
        market_context, market_context_evidence, completion_warning = (
            self._complete_market_context(
                market_context,
                market_context_evidence,
                source_trade_date=source_trade_date,
                stage=stage,
                as_of=as_of,
                universe_codes=universe_codes,
                quote_snapshot=quote_snapshot,
                quote_field_quality=quote_field_quality,
                realtime_rows=realtime_context_rows,
                realtime_complete=realtime_context_complete,
                realtime_evidence_date=realtime_snapshot.evidence_trade_date,
                kline_scope_codes=kline_load_order,
                kline_by_code=kline_by_code,
                review_history_by_code=review_history_by_code,
            )
        )
        if completion_warning:
            warnings.append(completion_warning)

        candidates: List[CandidateSnapshot] = []
        for code in ordered_candidate_codes:
            stock = stock_by_code[code]
            quote = quote_snapshot.quotes.get(code)
            features: Dict[str, Any] = {}
            evidence: List[Dict[str, Any]] = []
            if quote is not None:
                quality = quote_field_quality.get(code, {})
                quote_features = self._quote_features(quote, quality)
                features.update(quote_features)
                quote_evidence_field_quality = {
                    "price": quality.get("price", "missing"),
                    "captured_at": quality.get("timestamp", "missing"),
                    "speed_pct": quality.get("speed_pct", "missing"),
                    "speed_quality": "ready",
                    **{
                        key: quality.get(key, "missing")
                        for key in (
                            "pre_close",
                            "open_price",
                            "change_pct",
                            "amount",
                            "turnover_rate",
                            "bid1_price",
                            "bid1_volume",
                            "limit_up",
                        )
                    },
                }
                evidence.append(
                    {
                        "source": "tencent",
                        "as_of": quote.captured_at,
                        "evidence_trade_date": self._china_datetime(
                            quote.captured_at
                        ).date(),
                        "quality": self._quote_evidence_quality(
                            quote,
                            as_of,
                            quality,
                        ),
                        "field_quality": quote_evidence_field_quality,
                    }
                )
            else:
                evidence.append(
                    {
                        "source": "tencent",
                        "as_of": as_of,
                        "quality": "missing",
                        "warning": f"missing quote for {code}",
                    }
                )
            if code in change_ranks:
                features["change_rank"] = change_ranks[code]
            if code in speed_ranks:
                features["speed_rank"] = speed_ranks[code]
            if code in change_ranks or code in speed_ranks:
                rank_evidence = {
                    "source": "full_market_quote_rank",
                    "as_of": quote.captured_at,
                    "evidence_trade_date": self._china_datetime(
                        quote.captured_at
                    ).date(),
                    "quality": self._rank_timestamp_quality(
                        quote,
                        as_of,
                        quote_field_quality.get(code, {}),
                    ),
                }
                if code in change_ranks:
                    rank_evidence["change_rank"] = change_ranks[code]
                if code in speed_ranks:
                    rank_evidence["speed_rank"] = speed_ranks[code]
                else:
                    rank_evidence["speed_quality"] = quote_field_quality.get(
                        code,
                        {},
                    ).get("speed_pct", "missing")
                rank_evidence["field_quality"] = {
                    key: rank_evidence["quality"]
                    for key in ("change_rank", "speed_rank")
                    if key in rank_evidence
                }
                evidence.append(rank_evidence)

            kline_result = kline_by_code[code]
            kline = kline_result.features
            kline_quality = kline["kline_quality"]
            if kline_quality == "ready":
                features.update(kline)
            else:
                features["kline_quality"] = kline_quality
            kline_evidence = {
                "source": "kline",
                "as_of": kline_result.available_at,
                "evidence_trade_date": kline_result.evidence_trade_date,
                "quality": kline_quality,
            }
            if kline_quality != "ready":
                warning = f"missing kline features for {code}"
                warnings.append(warning)
                kline_evidence["warning"] = warning
                if kline_result.reason:
                    kline_evidence["reason"] = kline_result.reason
            evidence.append(kline_evidence)

            realtime_row = realtime_by_code.get(code)
            if realtime_row is not None:
                features["realtime_limit_up_fact"] = self._sanitize_fact(
                    realtime_row
                )
                evidence.append(
                    {
                        "source": "realtime_limit_up_pool",
                        "as_of": realtime_provenance_by_code[code],
                        "evidence_trade_date": realtime_snapshot.evidence_trade_date,
                        "quality": "ready",
                    }
                )
            elif code in realtime_discovery_by_code:
                discovery = realtime_discovery_by_code[code]
                evidence.append(
                    {
                        "source": "realtime_limit_up_pool",
                        "as_of": discovery["as_of"],
                        "evidence_trade_date": discovery[
                            "evidence_trade_date"
                        ],
                        "quality": discovery["quality"],
                        "warning": discovery["warning"],
                        "candidate_discovery_only": True,
                    }
                )

            review_history = review_history_by_code.get(code, [])
            if review_history:
                review_facts = [
                    self._review_fact(row) for row in review_history
                ]
                latest_review = review_facts[0]
                features["review_history"] = review_facts
                features.update(
                    {
                        f"review_{key}": value
                        for key, value in latest_review.items()
                    }
                )
                latest_row = review_history[0]
                evidence.append(
                    {
                        "source": "market_review_stock_daily",
                        "as_of": max(
                            self._evidence_datetime(
                                self._china_datetime(latest_row.created_at),
                                as_of,
                            ),
                            self._evidence_datetime(
                                self._china_datetime(latest_row.updated_at),
                                as_of,
                            ),
                        ),
                        "quality": latest_row.data_quality_flag or "ok",
                    }
                )

            plan_context = plan_by_code.get(code)
            if plan_context is not None:
                plan_candidate, plan_version = plan_context
                features["plan_candidate_fact"] = {
                    "plan_version_id": plan_version.id,
                    "source_trade_date": plan_version.source_trade_date,
                    "target_trade_date": plan_version.target_trade_date,
                    "action_trade_date": plan_candidate.action_trade_date,
                    "primary_mode_key": plan_candidate.primary_mode_key,
                    "theme_name": plan_candidate.theme_name,
                    "role": plan_candidate.role,
                    "rank": plan_candidate.rank,
                    "status": plan_candidate.status,
                }
                evidence.append(
                    {
                        "source": "trading_plan_candidate",
                        "as_of": self._evidence_datetime(
                            self._china_datetime(plan_version.generated_at),
                            as_of,
                        ),
                        "quality": "ready",
                    }
                )

            theme_name = self._theme_name(
                realtime_row,
                review_history[0] if review_history else None,
                plan_context[0] if plan_context is not None else None,
            )
            self._add_candidate_mode_inputs(
                features,
                evidence,
                stock=stock,
                realtime_row=realtime_row,
                review_row=review_history[0] if review_history else None,
                quote=quote,
                quote_quality=quote_field_quality.get(code, {}),
                as_of=as_of,
            )
            candidates.append(
                CandidateSnapshot(
                    stock_code=code,
                    stock_name=stock.stock_name,
                    theme_name=theme_name,
                    features=features,
                    evidence=evidence,
                )
            )

        self._add_theme_amount_ranks(candidates, as_of)
        if stage == "auction":
            self._add_auction_features(
                candidates,
                quote_snapshot,
                quote_field_quality,
                target_trade_date,
                warnings,
            )

        rank_evidence = []
        for quote in sorted(
            quote_snapshot.quotes.values(),
            key=lambda item: item.stock_code,
        ):
            item = {"stock_code": quote.stock_code}
            timestamp_quality = self._rank_timestamp_quality(
                quote,
                as_of,
                quote_field_quality.get(quote.stock_code, {}),
            )
            if quote.stock_code in change_ranks:
                item["change_rank"] = change_ranks[quote.stock_code]
            else:
                item["change_quality"] = (
                    timestamp_quality
                    if timestamp_quality != "ready"
                    else quote_field_quality.get(quote.stock_code, {}).get(
                        "change_pct",
                        "missing",
                    )
                )
            if quote.stock_code in speed_ranks:
                item["speed_rank"] = speed_ranks[quote.stock_code]
            else:
                item["speed_quality"] = (
                    timestamp_quality
                    if timestamp_quality != "ready"
                    else quote_field_quality.get(
                        quote.stock_code,
                        {},
                    ).get("speed_pct", "missing")
                )
            rank_evidence.append(item)
        market_features = {
            "quote_requested_count": len(universe_codes),
            "quote_returned_count": len(quote_snapshot.quotes),
            "quote_coverage_ratio": round(
                len(quote_snapshot.quotes) / len(universe_codes),
                4,
            )
            if universe_codes
            else 1.0,
            "candidate_count": len(candidates),
            "full_market_change_ranks": change_ranks,
            "full_market_speed_ranks": speed_ranks,
            "full_market_rank_evidence": rank_evidence,
            **market_context,
            "_feature_quality": {
                key: (
                    market_context_evidence[0]["field_quality"].get(
                        key,
                        "missing",
                    )
                    if market_context_evidence
                    else "missing"
                )
                for key in _FULL_MARKET_CONTEXT_FIELDS
            },
            "_evidence": market_context_evidence,
        }
        if force_degraded:
            warnings.append("force_degraded requested")
        overall_degraded = (
            force_degraded
            or quote_snapshot.quality.status != "ready"
            or quote_snapshot.quality.stale
            or bool(warnings)
        )
        return MarketSnapshot(
            source_trade_date=source_trade_date,
            target_trade_date=target_trade_date,
            stage=stage,
            as_of=as_of,
            market_features=market_features,
            candidates=candidates,
            theme_rankings=self._theme_rankings(candidates),
            quality=DataQuality(
                status="degraded" if overall_degraded else "ready",
                as_of=as_of,
                source="trading_playbook_market_data",
                stale=quote_snapshot.quality.stale,
                warnings=warnings,
                forced_degraded=force_degraded,
                degradation_reason=(
                    force_degraded_reason if force_degraded else None
                ),
            ),
        )

    @classmethod
    def _complete_market_context(
        cls,
        context: Mapping[str, Any],
        evidence: List[Dict[str, Any]],
        *,
        source_trade_date: date,
        stage: str,
        as_of: datetime,
        universe_codes: List[str],
        quote_snapshot: QuoteSnapshot,
        quote_field_quality: QuoteFieldQuality,
        realtime_rows: List[Dict[str, Any]],
        realtime_complete: bool,
        realtime_evidence_date: Optional[date],
        kline_scope_codes: List[str],
        kline_by_code: Mapping[str, _KlineBuildResult],
        review_history_by_code: Mapping[str, List[MarketReviewStockDaily]],
    ) -> Tuple[Dict[str, Any], List[Dict[str, Any]], Optional[str]]:
        values = dict(context)
        field_quality = {
            key: "missing" for key in _FULL_MARKET_CONTEXT_FIELDS
        }
        field_provenance: Dict[str, Dict[str, Any]] = {}
        comparison_baseline: Mapping[str, Any] = {}
        if evidence:
            declared = evidence[0].get("field_quality")
            if isinstance(declared, Mapping):
                field_quality.update(
                    {
                        key: str(declared.get(key) or "missing")
                        for key in _FULL_MARKET_CONTEXT_FIELDS
                    }
                )
            declared_provenance = evidence[0].get("field_provenance")
            if isinstance(declared_provenance, Mapping):
                field_provenance.update(
                    {
                        str(key): dict(value)
                        for key, value in declared_provenance.items()
                        if isinstance(value, Mapping)
                    }
                )
            baseline = evidence[0].get("comparison_baseline")
            if isinstance(baseline, Mapping):
                comparison_baseline = baseline

        computed_fields: Dict[str, Dict[str, Any]] = {}

        def publish(
            key: str,
            value: Any,
            *,
            source: str,
            evidence_date: date,
            coverage: Optional[float] = None,
        ) -> None:
            if field_quality.get(key) in {"ready", "computed"} or value is None:
                return
            values[key] = value
            field_quality[key] = "computed"
            provenance = {"source": source, "trade_date": evidence_date}
            if coverage is not None:
                provenance["coverage"] = coverage
            field_provenance[key] = provenance
            computed_fields[key] = provenance

        if realtime_complete and realtime_evidence_date is not None:
            realtime_date = realtime_evidence_date
            publish(
                "limit_up_count",
                len(realtime_rows),
                source="realtime_limit_up_pool",
                evidence_date=realtime_date,
                coverage=1.0,
            )
            board_heights = []
            seal_flags = []
            broken_flags = []
            for row in realtime_rows:
                raw_height = cls._pick(
                    row,
                    "continuous_limit_up_days",
                    "today_continuous_days",
                    "board_height",
                )
                height = cls._optional_float(raw_height)
                if (
                    height is not None
                    and height >= 0
                    and float(height).is_integer()
                ):
                    board_heights.append(int(height))
                sealed = cls._fact_flag(row, "sealed", "today_sealed_close")
                broken = cls._fact_flag(row, "broken", "today_broken")
                seal_flags.append(sealed)
                broken_flags.append(broken)
            if len(board_heights) == len(realtime_rows):
                publish(
                    "max_board_height",
                    max(board_heights, default=0),
                    source="realtime_limit_up_pool",
                    evidence_date=realtime_date,
                    coverage=1.0,
                )
            if realtime_rows and all(
                isinstance(value, bool)
                for value in [*seal_flags, *broken_flags]
            ):
                attempts = sum(
                    bool(sealed) or bool(broken)
                    for sealed, broken in zip(seal_flags, broken_flags)
                )
                if attempts:
                    publish(
                        "seal_rate",
                        round(100 * sum(seal_flags) / attempts, 4),
                        source="realtime_limit_up_pool",
                        evidence_date=realtime_date,
                        coverage=1.0,
                    )

        quote_ready_codes = [
            code
            for code in universe_codes
            if code in quote_snapshot.quotes
            and quote_field_quality.get(code, {}).get("_baseline_freshness")
            == "ready"
            and quote_field_quality.get(code, {}).get("change_pct")
            in {"ready", "computed"}
        ]
        quote_coverage = (
            len(quote_ready_codes) / len(universe_codes)
            if universe_codes
            else 0.0
        )
        quote_complete = bool(universe_codes) and quote_coverage >= 0.9
        if quote_complete:
            quote_evidence_date = max(
                cls._china_datetime(
                    quote_snapshot.quotes[code].captured_at
                ).date()
                for code in quote_ready_codes
            )
            limit_down_count = sum(
                cls._is_limit_down_quote(
                    code,
                    quote_snapshot.quotes[code].change_pct,
                )
                for code in quote_ready_codes
            )
            publish(
                "limit_down_count",
                limit_down_count,
                source="full_market_quote",
                evidence_date=quote_evidence_date,
                coverage=quote_coverage,
            )
            prior_limit_down = cls._optional_float(
                comparison_baseline.get("limit_down_count")
            )
            if prior_limit_down is not None:
                publish(
                    "sell_pressure_falling",
                    limit_down_count < prior_limit_down,
                    source="full_market_quote_vs_daily_metric",
                    evidence_date=quote_evidence_date,
                    coverage=quote_coverage,
                )
                publish(
                    "sell_pressure_rising",
                    limit_down_count > prior_limit_down,
                    source="full_market_quote_vs_daily_metric",
                    evidence_date=quote_evidence_date,
                    coverage=quote_coverage,
                )
            prior_up = cls._optional_float(
                comparison_baseline.get("up_count_ex_st")
            )
            prior_down = cls._optional_float(
                comparison_baseline.get("down_count_ex_st")
            )
            if prior_up is not None and prior_down is not None:
                current_up = sum(
                    quote_snapshot.quotes[code].change_pct > 0
                    for code in quote_ready_codes
                )
                current_down = sum(
                    quote_snapshot.quotes[code].change_pct < 0
                    for code in quote_ready_codes
                )
                publish(
                    "breadth_recovered",
                    current_up > current_down and prior_up <= prior_down,
                    source="full_market_quote_vs_daily_metric",
                    evidence_date=quote_evidence_date,
                    coverage=quote_coverage,
                )

        ready_klines = [
            kline_by_code[code]
            for code in kline_scope_codes
            if code in kline_by_code
            if kline_by_code[code].features.get("kline_quality") == "ready"
        ]
        # The bounded union is useful evidence only under an explicit sample
        # contract. It must never masquerade as a full-market aggregate.
        sample_fields: Dict[str, Dict[str, Any]] = {}
        kline_coverage = (
            len(ready_klines) / len(kline_scope_codes)
            if kline_scope_codes
            else 0.0
        )
        if kline_scope_codes:
            sample_evidence_date = max(
                (
                    row.evidence_trade_date
                    for row in ready_klines
                    if row.evidence_trade_date is not None
                ),
                default=None,
            )

            def publish_sample(key: str, value: Any) -> None:
                values[key] = value
                provenance = {
                    "source": "bounded_sample",
                    "scope": "bounded_candidate_union",
                    "trade_date": sample_evidence_date,
                    "coverage": round(kline_coverage, 4),
                }
                sample_fields[key] = provenance

            publish_sample(
                "trend_new_high_sample_count",
                sum(bool(row.features.get("n_day_high")) for row in ready_klines),
            )
            publish_sample(
                "trend_new_high_sample_count_prev",
                sum(
                    bool(row.features.get("prior_n_day_high"))
                    for row in ready_klines
                ),
            )
            publish_sample("trend_sample_size", len(kline_scope_codes))
            publish_sample(
                "trend_sample_ready_coverage",
                round(kline_coverage, 4),
            )
            publish_sample("trend_scope", "bounded_candidate_union")

        if field_quality.get("negative_feedback") == "missing" and quote_complete:
            prior_rows = {}
            for code in universe_codes:
                rows = [
                    row
                    for row in review_history_by_code.get(code, [])
                    if row.trade_date < source_trade_date
                ]
                if rows:
                    prior_rows[code] = rows[0]
            if len(prior_rows) == len(universe_codes):
                popular_codes = [
                    code
                    for code, row in prior_rows.items()
                    if row.today_continuous_days >= 2
                ]
                publish(
                    "negative_feedback",
                    any(
                        cls._is_limit_down_quote(
                            code,
                            quote_snapshot.quotes[code].change_pct,
                        )
                        for code in popular_codes
                    ),
                    source="daily_analysis_popular_to_quote_limit_down",
                    evidence_date=quote_evidence_date,
                    coverage=quote_coverage,
                )

        complete = all(
            field_quality[key] in {"ready", "computed"}
            for key in _FULL_MARKET_CONTEXT_FIELDS
        )
        aggregate = dict(evidence[0]) if evidence else {
            "source": "full_market_context",
            "scope": "full_market",
            "trade_date": source_trade_date,
            "evidence_trade_date": source_trade_date,
            "as_of": as_of,
        }
        aggregate["quality"] = "ready" if complete else "degraded"
        aggregate["field_quality"] = field_quality
        aggregate["field_provenance"] = field_provenance
        merged_evidence = [aggregate]
        if computed_fields:
            merged_evidence.append(
                {
                    "source": "computed_market_context",
                    "as_of": as_of,
                    "quality": "ready" if complete else "degraded",
                    "field_quality": {
                        key: field_quality[key] for key in computed_fields
                    },
                    "field_provenance": computed_fields,
                }
            )
        if sample_fields:
            merged_evidence.append(
                {
                    "source": "bounded_sample",
                    "scope": "bounded_candidate_union",
                    "as_of": as_of,
                    "quality": (
                        "ready" if kline_coverage >= 0.8 else "degraded"
                    ),
                    "field_quality": {
                        key: "computed" for key in sample_fields
                    },
                    "field_provenance": sample_fields,
                }
            )
        missing = [
            key
            for key in _FULL_MARKET_CONTEXT_FIELDS
            if field_quality[key] not in {"ready", "computed"}
        ]
        warning = (
            "incomplete full-market context: " + ",".join(missing)
            if missing
            else None
        )
        return values, merged_evidence, warning

    @staticmethod
    def _is_limit_down_quote(stock_code: str, change_pct: float) -> bool:
        if stock_code.startswith(("300", "301", "688")):
            return change_pct <= -19.5
        if stock_code.startswith(("8", "920")):
            return change_pct <= -29.5
        return change_pct <= -9.5

    async def _load_full_market_context(
        self,
        trade_date: date,
        stage: str,
        as_of: datetime,
    ) -> Tuple[Dict[str, Any], List[Dict[str, Any]], Optional[str]]:
        if self.full_market_context_loader is None:
            return {}, [], None
        try:
            payload = await self.full_market_context_loader(
                trade_date,
                stage,
                as_of,
            )
        except Exception as exc:
            return {}, [], f"full-market context failed: {exc}"
        if not isinstance(payload, Mapping):
            return {}, [], "invalid full-market context payload"
        if payload.get("scope") != "full_market":
            return {}, [], "invalid full-market context scope"
        if "trade_date" not in payload:
            return {}, [], "full-market context missing trade date"
        payload_trade_date = self._parse_date_value(payload.get("trade_date"))
        if payload_trade_date is None:
            return {}, [], "invalid full-market context trade date"
        if payload_trade_date != trade_date:
            return {}, [], "mismatched full-market context trade date"
        evidence_trade_date = self._parse_date_value(
            payload.get("evidence_trade_date")
        )
        if evidence_trade_date is None:
            evidence_trade_date = trade_date
        if evidence_trade_date > trade_date:
            return {}, [], "future full-market context evidence date"
        captured_at = self._parse_temporal_value(payload.get("as_of"))
        if captured_at is None:
            captured_at = self._parse_temporal_value(payload.get("captured_at"))
        local_as_of = self._china_datetime(as_of)
        if captured_at is None:
            return {}, [], "full-market context missing provenance"
        if captured_at > local_as_of:
            return {}, [], "future full-market context"
        if (
            stage in {"preclose", "after_close"}
            and evidence_trade_date == trade_date
            and self._china_datetime(captured_at).date() != trade_date
        ):
            return {}, [], "stale full-market context"
        if (
            payload.get("stale") is True
            or str(payload.get("quality") or "").strip().lower() == "stale"
        ):
            return {}, [], "stale full-market context"

        declared_quality = payload.get("field_quality")
        if not isinstance(declared_quality, Mapping):
            declared_quality = {}
        if any(
            isinstance(value, str)
            and value.strip().lower() in {"stale", "invalid"}
            for value in declared_quality.values()
        ):
            return {}, [], "stale or invalid full-market context field"
        normalized: Dict[str, Any] = {}
        accepted_quality: Dict[str, str] = {}
        invalid_fields = []
        count_fields = {
            "limit_up_count",
            "limit_up_count_prev",
            "trend_new_high_count",
            "trend_new_high_count_prev",
            "limit_down_count",
            "max_board_height",
            "divergence_days",
        }
        flag_fields = {
            "negative_feedback",
            "sell_pressure_falling",
            "breadth_recovered",
            "sell_pressure_rising",
        }
        for key in _FULL_MARKET_CONTEXT_FIELDS:
            if declared_quality.get(key) not in {"ready", "computed"}:
                continue
            value = payload.get(key)
            valid = True
            if key in count_fields:
                if isinstance(value, bool):
                    valid = False
                else:
                    value = self._optional_float(value)
                    valid = (
                        value is not None
                        and value >= 0
                        and value.is_integer()
                    )
                if valid:
                    value = int(value)
            elif key == "seal_rate":
                if isinstance(value, bool):
                    valid = False
                else:
                    value = self._optional_float(value)
                    valid = value is not None and 0 <= value <= 100
            elif key in flag_fields:
                valid = isinstance(value, bool)
            elif key == "prior_window":
                valid = isinstance(value, str)
                if valid:
                    value = value.strip()
                    valid = value in _FULL_MARKET_PRIOR_WINDOWS
            if not valid:
                accepted_quality[key] = "invalid"
                invalid_fields.append(key)
                continue
            normalized[key] = value
            accepted_quality[key] = str(declared_quality[key])
        payload_quality = str(payload.get("quality") or "").strip().lower()
        complete = len(normalized) == len(_FULL_MARKET_CONTEXT_FIELDS)
        explicitly_degraded = bool(payload_quality) and payload_quality != "ready"
        evidence = [{
            "source": "full_market_context",
            "scope": "full_market",
            "trade_date": trade_date,
            "evidence_trade_date": evidence_trade_date,
            "as_of": self._evidence_datetime(captured_at, as_of),
            "quality": (
                "ready"
                if complete and not explicitly_degraded
                else "degraded"
            ),
            "field_quality": {
                key: accepted_quality.get(key, "missing")
                for key in _FULL_MARKET_CONTEXT_FIELDS
            },
            "field_provenance": self._sanitize_fact(
                payload.get("field_provenance")
                if isinstance(payload.get("field_provenance"), Mapping)
                else {}
            ),
            "comparison_baseline": self._sanitize_fact(
                payload.get("comparison_baseline")
                if isinstance(payload.get("comparison_baseline"), Mapping)
                else {}
            ),
        }]
        warnings = []
        if invalid_fields:
            return (
                {},
                [],
                "invalid full-market context fields: "
                + ",".join(sorted(invalid_fields)),
            )
        warning = "; ".join(warnings) if warnings else None
        return normalized, evidence, warning

    async def _load_realtime_limit_up(
        self,
        trade_date: date,
    ) -> RealtimeLimitUpSnapshot:
        if self.realtime_limit_up_loader is not None:
            loaded = await self.realtime_limit_up_loader(trade_date)
            if isinstance(loaded, RealtimeLimitUpSnapshot):
                return loaded
            if isinstance(loaded, list):
                # Legacy non-empty injection remains usable for candidates,
                # but a plain empty list is not authoritative evidence of 0.
                complete = bool(loaded)
                return RealtimeLimitUpSnapshot(
                    items=loaded,
                    authoritative=complete,
                    complete=complete,
                    evidence_trade_date=trade_date if complete else None,
                    warning=(
                        None
                        if complete
                        else "unstructured empty realtime limit-up pool"
                    ),
                )
            raise TypeError("invalid realtime limit-up snapshot")
        from app.services.realtime_limit_up_service import realtime_limit_up_service

        return await realtime_limit_up_service.get_fast_limit_up_snapshot(trade_date)

    @classmethod
    def _normalize_realtime_row(
        cls,
        row: Mapping[str, Any],
    ) -> Dict[str, Any]:
        normalized = dict(row)
        is_final_sealed = row.get("is_final_sealed")
        if isinstance(is_final_sealed, bool):
            normalized["sealed"] = is_final_sealed
            normalized["broken"] = not is_final_sealed
        float_market_value = row.get("float_market_value")
        if not isinstance(float_market_value, bool):
            market_value = cls._optional_float(float_market_value)
            if market_value is not None and market_value >= 0:
                normalized["tradable_market_value"] = market_value
        return normalized

    def _quote_evidence_quality(
        self,
        quote: QuotePoint,
        as_of: datetime,
        field_quality: Dict[str, str],
    ) -> str:
        if field_quality.get("timestamp") != "ready":
            return "degraded"
        if field_quality.get("_baseline_freshness") != "ready":
            return "stale"
        if any(
            value == "missing"
            for key, value in field_quality.items()
            if not key.startswith("_")
        ):
            return "degraded"
        return "ready"

    def _rank_timestamp_quality(
        self,
        quote: QuotePoint,
        as_of: datetime,
        field_quality: Dict[str, str],
    ) -> str:
        if field_quality.get("timestamp") != "ready":
            return "degraded"
        if field_quality.get("_baseline_freshness") != "ready":
            return "stale"
        return "ready"

    @staticmethod
    def _quote_features(
        quote: QuotePoint,
        field_quality: Dict[str, str],
    ) -> Dict[str, Any]:
        features: Dict[str, Any] = {
            "price": quote.price,
            "speed_quality": field_quality.get("speed_pct", "missing"),
            "captured_at": quote.captured_at,
        }
        if field_quality.get("speed_pct") == "ready":
            features["speed_pct"] = quote.speed_pct
        optional_fields = (
            "pre_close",
            "open_price",
            "change_pct",
            "amount",
            "turnover_rate",
            "bid1_price",
            "bid1_volume",
            "limit_up",
        )
        for field in optional_fields:
            if field_quality.get(field) in {"ready", "computed"}:
                features[field] = getattr(quote, field)
        return features

    def _add_auction_features(
        self,
        candidates: List[CandidateSnapshot],
        quote_snapshot: QuoteSnapshot,
        quote_field_quality: QuoteFieldQuality,
        target_trade_date: date,
        warnings: List[str],
    ) -> None:
        valid_by_theme: Dict[str, List[CandidateSnapshot]] = {}
        for candidate in candidates:
            quote = quote_snapshot.quotes.get(candidate.stock_code)
            field_quality = quote_field_quality.get(candidate.stock_code, {})
            valid_timestamp = (
                quote is not None
                and field_quality.get("timestamp") == "ready"
                and field_quality.get("_baseline_freshness") == "ready"
                and self._is_auction_timestamp(
                    quote.captured_at,
                    target_trade_date,
                )
            )
            if not valid_timestamp:
                candidate.features["auction_quality"] = "missing"
                warning = (
                    f"missing auction timestamp for {candidate.stock_code}"
                )
                warnings.append(warning)
                candidate.evidence.append(
                    {
                        "source": "auction",
                        "as_of": quote.captured_at if quote else quote_snapshot.quality.as_of,
                        "quality": "missing",
                        "field_quality": {"auction_quality": "ready"},
                        "warning": warning,
                    }
                )
                continue

            metric_fields = (
                ("auction_change_pct", "change_pct", quote.change_pct),
                ("auction_amount", "amount", quote.amount),
                ("auction_bid1_volume", "bid1_volume", quote.bid1_volume),
            )
            missing_metrics = []
            available_metrics = 0
            for feature_name, source_field, value in metric_fields:
                if field_quality.get(source_field) in {"ready", "computed"}:
                    candidate.features[feature_name] = value
                    available_metrics += 1
                else:
                    missing_metrics.append(source_field)

            auction_quality = (
                "ready"
                if not missing_metrics
                else "degraded"
                if available_metrics
                else "missing"
            )
            warning_parts = []
            if missing_metrics:
                warning_parts.append(
                    f"missing auction metrics: {','.join(missing_metrics)}"
                )
            if not candidate.theme_name:
                if auction_quality == "ready":
                    auction_quality = "degraded"
                warning_parts.append("missing auction theme")

            candidate.features["auction_quality"] = auction_quality
            evidence = {
                "source": "auction",
                "as_of": quote.captured_at,
                "quality": auction_quality,
                "field_quality": {
                    "auction_quality": "ready",
                    **{
                        feature_name: field_quality.get(source_field, "missing")
                        for feature_name, source_field, _ in metric_fields
                        if feature_name in candidate.features
                    },
                },
            }
            if warning_parts:
                warning = f"{candidate.stock_code}: {'; '.join(warning_parts)}"
                warnings.append(warning)
                evidence["warning"] = warning
            candidate.evidence.append(evidence)

            if (
                candidate.theme_name
                and "auction_change_pct" in candidate.features
            ):
                valid_by_theme.setdefault(candidate.theme_name, []).append(candidate)

        for theme_candidates in valid_by_theme.values():
            ordered = sorted(
                theme_candidates,
                key=lambda candidate: (
                    -candidate.features["auction_change_pct"],
                    candidate.stock_code,
                ),
            )
            for rank, candidate in enumerate(ordered, start=1):
                candidate.features["auction_theme_rank"] = rank
                for evidence in reversed(candidate.evidence):
                    if evidence.get("source") != "auction":
                        continue
                    evidence.setdefault("field_quality", {})[
                        "auction_theme_rank"
                    ] = "computed"
                    break

    @staticmethod
    def _is_auction_timestamp(
        captured_at: datetime,
        target_trade_date: date,
    ) -> bool:
        local = captured_at
        if captured_at.tzinfo is not None:
            local = captured_at.astimezone(ZoneInfo("Asia/Shanghai"))
        return (
            local.date() == target_trade_date
            and time(9, 15) <= local.time().replace(tzinfo=None) < time(9, 27)
        )

    @classmethod
    def _kline_available_at(
        cls,
        point: Mapping[str, Any],
    ) -> Optional[datetime]:
        for key in (
            "available_at",
            "captured_at",
            "updated_at",
            "collected_at",
            "datetime",
            "timestamp",
            "date",
            "trade_date",
        ):
            if key not in point or point[key] in (None, ""):
                continue
            parsed = cls._parse_temporal_value(
                point[key],
                date_only_at=time(15, 0),
            )
            if parsed is not None:
                return parsed
        return None

    @classmethod
    def _kline_trade_date(
        cls,
        point: Mapping[str, Any],
    ) -> Optional[date]:
        for key in ("date", "trade_date"):
            parsed_date = cls._parse_date_value(point.get(key))
            if parsed_date is not None:
                return parsed_date
        for key in ("datetime", "timestamp"):
            if key not in point or point[key] in (None, ""):
                continue
            parsed = cls._parse_temporal_value(
                point[key],
                date_only_at=time(15, 0),
            )
            if parsed is not None:
                return parsed.date()
        return None

    @classmethod
    def _realtime_available_at(
        cls,
        row: Mapping[str, Any],
        requested_date: date,
    ) -> Optional[datetime]:
        row_date = requested_date
        for date_key in ("trade_date", "date"):
            parsed_date = cls._parse_date_value(row.get(date_key))
            if parsed_date is not None:
                row_date = parsed_date
                break

        candidates = []
        for key in (
            "available_at",
            "captured_at",
            "updated_at",
            "_collected_at",
            "collected_at",
            "collection_time",
            "generated_at",
        ):
            if key not in row or row[key] in (None, ""):
                continue
            parsed = cls._parse_temporal_value(
                row[key],
                default_date=row_date,
            )
            if parsed is not None:
                candidates.append(parsed)
        return max(candidates) if candidates else None

    @classmethod
    def _parse_temporal_value(
        cls,
        value: Any,
        *,
        default_date: Optional[date] = None,
        date_only_at: Optional[time] = None,
    ) -> Optional[datetime]:
        if isinstance(value, datetime):
            return cls._china_datetime(value)
        if isinstance(value, date):
            if date_only_at is None:
                return None
            return cls._china_datetime(datetime.combine(value, date_only_at))
        if value in (None, ""):
            return None

        raw = str(value).strip()
        for pattern in (
            "%Y%m%d%H%M%S",
            "%Y-%m-%d %H:%M:%S",
            "%Y/%m/%d %H:%M:%S",
        ):
            try:
                return cls._china_datetime(datetime.strptime(raw, pattern))
            except ValueError:
                pass
        parsed_date = cls._parse_date_value(raw)
        if parsed_date is not None:
            if date_only_at is None:
                return None
            return cls._china_datetime(datetime.combine(parsed_date, date_only_at))
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return cls._china_datetime(parsed)
        except ValueError:
            pass
        if default_date is not None:
            for pattern in ("%H:%M:%S", "%H%M%S", "%H:%M"):
                try:
                    parsed_time = datetime.strptime(raw, pattern).time()
                    return cls._china_datetime(
                        datetime.combine(default_date, parsed_time)
                    )
                except ValueError:
                    pass
        if raw.isdigit() and len(raw) in {10, 13}:
            try:
                seconds = int(raw) / (1000 if len(raw) == 13 else 1)
                return datetime.fromtimestamp(
                    seconds,
                    tz=timezone.utc,
                ).astimezone(ZoneInfo("Asia/Shanghai"))
            except (OverflowError, OSError, ValueError):
                return None
        return None

    @classmethod
    def _parse_date_value(cls, value: Any) -> Optional[date]:
        if isinstance(value, datetime):
            return cls._china_datetime(value).date()
        if isinstance(value, date):
            return value
        if value in (None, ""):
            return None
        raw = str(value).strip()
        for pattern in ("%Y-%m-%d", "%Y%m%d", "%Y/%m/%d"):
            try:
                return datetime.strptime(raw, pattern).date()
            except ValueError:
                pass
        return None

    @staticmethod
    def _evidence_datetime(
        value: Optional[datetime],
        as_of: datetime,
    ) -> Optional[datetime]:
        if value is None:
            return None
        local = TradingPlaybookMarketDataProvider._china_datetime(value)
        return local if as_of.tzinfo is not None else local.replace(tzinfo=None)

    @staticmethod
    def _review_fact(row: MarketReviewStockDaily) -> Dict[str, Any]:
        return {
            "trade_date": row.trade_date,
            "yesterday_limit_up": row.yesterday_limit_up,
            "yesterday_continuous_days": row.yesterday_continuous_days,
            "today_touched_limit_up": row.today_touched_limit_up,
            "today_sealed_close": row.today_sealed_close,
            "today_broken": row.today_broken,
            "today_continuous_days": row.today_continuous_days,
            "first_limit_time": (
                row.first_limit_time.isoformat() if row.first_limit_time else None
            ),
            "final_seal_time": (
                row.final_seal_time.isoformat() if row.final_seal_time else None
            ),
            "change_pct": row.change_pct,
            "amount": row.amount,
            "turnover_rate": row.turnover_rate,
            "tradable_market_value": row.tradable_market_value,
            "limit_up_reason": row.limit_up_reason or "",
            "data_quality_flag": row.data_quality_flag,
        }

    @staticmethod
    def _theme_name(
        realtime_row: Optional[Dict[str, Any]],
        review_row: Optional[MarketReviewStockDaily],
        plan_candidate: Optional[TradingPlanCandidate] = None,
    ) -> str:
        if realtime_row:
            for key in ("theme_name", "reason_category", "limit_up_reason"):
                if realtime_row.get(key):
                    return str(realtime_row[key])
        if review_row is not None and review_row.limit_up_reason:
            return str(review_row.limit_up_reason)
        if plan_candidate is not None and plan_candidate.theme_name:
            return str(plan_candidate.theme_name)
        return ""

    @classmethod
    def _theme_rankings(
        cls,
        candidates: List[CandidateSnapshot],
    ) -> List[Dict[str, Any]]:
        grouped: Dict[str, List[CandidateSnapshot]] = {}
        for candidate in candidates:
            if candidate.theme_name:
                grouped.setdefault(candidate.theme_name, []).append(candidate)
        rankings = []
        for theme_name, members in grouped.items():
            realtime_members = [
                member
                for member in members
                if isinstance(
                    member.features.get("realtime_limit_up_fact"),
                    Mapping,
                )
            ]
            field_quality = {
                key: "missing"
                for key in (
                    "limit_up_count",
                    "new_high_count",
                    "sealed_count",
                    "broken_count",
                    "middle_army_strength",
                )
            }
            row: Dict[str, Any] = {
                "theme_name": theme_name,
                "candidate_count": len(members),
                "stock_codes": sorted(member.stock_code for member in members),
            }
            if realtime_members:
                row["limit_up_count"] = len(realtime_members)
                field_quality["limit_up_count"] = "ready"

                high_flags = []
                sealed_flags = []
                broken_flags = []
                amounts = []
                for member in realtime_members:
                    high_flags.append(
                        member.features.get("n_day_high")
                        if member.features.get("kline_quality") == "ready"
                        and isinstance(member.features.get("n_day_high"), bool)
                        else None
                    )
                    fact = member.features["realtime_limit_up_fact"]
                    sealed_flags.append(
                        cls._fact_flag(fact, "sealed", "today_sealed_close")
                    )
                    broken_flags.append(
                        cls._fact_flag(fact, "broken", "today_broken")
                    )
                    amount = cls._optional_float(member.features.get("amount"))
                    amount_ready = any(
                        evidence.get("source") == "tencent"
                        and isinstance(evidence.get("field_quality"), Mapping)
                        and evidence["field_quality"].get("amount")
                        in {"ready", "computed"}
                        and evidence.get("quality") != "stale"
                        and evidence.get("stale") is not True
                        for evidence in member.evidence
                    )
                    amounts.append(
                        amount if amount_ready and amount is not None else None
                    )

                for field, values, predicate in (
                    ("new_high_count", high_flags, bool),
                    ("sealed_count", sealed_flags, bool),
                    ("broken_count", broken_flags, bool),
                ):
                    if all(isinstance(value, bool) for value in values):
                        row[field] = sum(predicate(value) for value in values)
                        field_quality[field] = "ready"
                if all(value is not None for value in amounts):
                    row["middle_army_strength"] = sum(
                        value > 0 for value in amounts
                    )
                    field_quality["middle_army_strength"] = "ready"

            row["field_quality"] = field_quality
            row["quality"] = (
                "ready"
                if all(value == "ready" for value in field_quality.values())
                else "degraded"
            )
            source_fields = {
                "realtime_limit_up_pool": (
                    "limit_up_count",
                    "sealed_count",
                    "broken_count",
                ),
                "kline": ("new_high_count",),
                "tencent": ("middle_army_strength",),
            }
            row["evidence"] = []
            for source, fields in source_fields.items():
                observed = [
                    evidence.get("as_of")
                    for member in realtime_members
                    for evidence in member.evidence
                    if evidence.get("source") == source
                    and isinstance(evidence.get("as_of"), datetime)
                ]
                row["evidence"].append(
                    {
                        "source": source,
                        "as_of": max(observed) if observed else None,
                        "quality": (
                            "ready"
                            if all(field_quality[field] == "ready" for field in fields)
                            else "degraded"
                        ),
                        "field_quality": {
                            field: field_quality[field] for field in fields
                        },
                        "stock_codes": sorted(
                            member.stock_code for member in realtime_members
                        ),
                    }
                )
            rankings.append(row)
        return sorted(
            rankings,
            key=lambda item: (-item["candidate_count"], item["theme_name"]),
        )

    @staticmethod
    def _fact_flag(fact: Mapping[str, Any], *keys: str) -> Optional[bool]:
        for key in keys:
            if key in fact:
                return fact[key] if isinstance(fact[key], bool) else None
        return None

    @classmethod
    def _add_candidate_mode_inputs(
        cls,
        features: Dict[str, Any],
        evidence: List[Dict[str, Any]],
        *,
        stock: Stock,
        realtime_row: Optional[Mapping[str, Any]],
        review_row: Optional[MarketReviewStockDaily],
        quote: Optional[QuotePoint],
        quote_quality: Mapping[str, str],
        as_of: datetime,
    ) -> None:
        keys = (
            "first_limit_seconds",
            "board_height",
            "seal_strength",
            "resilience",
            "influence",
            "tradable_market_value",
            "theme_amount_rank",
        )
        feature_quality = features.get("_feature_quality")
        feature_quality = (
            dict(feature_quality)
            if isinstance(feature_quality, Mapping)
            else {}
        )
        feature_quality.update({key: "missing" for key in keys})
        normalized: Dict[str, Any] = {}
        conflicts = set()

        if realtime_row is not None:
            first_limit = cls._first_limit_seconds(
                realtime_row.get("first_limit_up_time")
                if "first_limit_up_time" in realtime_row
                else realtime_row.get("first_limit_time")
            )
            board_height = cls._optional_float(
                realtime_row.get("continuous_limit_up_days")
                if "continuous_limit_up_days" in realtime_row
                else realtime_row.get("today_continuous_days")
            )
            seal_strength = cls._optional_float(realtime_row.get("seal_amount"))
            market_value = cls._optional_float(
                realtime_row.get("tradable_market_value")
            )
            if first_limit is not None:
                normalized["first_limit_seconds"] = first_limit
            if board_height is not None and board_height >= 0:
                normalized["board_height"] = (
                    int(board_height) if board_height.is_integer() else board_height
                )
            if seal_strength is not None and seal_strength >= 0:
                normalized["seal_strength"] = seal_strength
            if market_value is not None and market_value >= 0:
                normalized["tradable_market_value"] = market_value

        if review_row is not None:
            review_first = cls._first_limit_seconds(review_row.first_limit_time)
            review_height = cls._optional_float(review_row.today_continuous_days)
            review_value = cls._optional_float(review_row.tradable_market_value)
            for key, value in (
                ("first_limit_seconds", review_first),
                ("board_height", review_height),
                ("tradable_market_value", review_value),
            ):
                if value is None or value < 0:
                    continue
                if key in normalized and normalized[key] != value:
                    normalized.pop(key)
                    conflicts.add(key)
                    continue
                if key not in conflicts:
                    normalized[key] = (
                        int(value)
                        if isinstance(value, float) and value.is_integer()
                        else value
                    )

        quote_is_fresh = (
            quote is not None
            and quote_quality.get("timestamp") == "ready"
            and cls._age_seconds(as_of, quote.captured_at)
            <= cls.STALE_AFTER_SECONDS
        )
        if quote_is_fresh and quote_quality.get("change_pct") in {
            "ready",
            "computed",
        }:
            resilience = cls._optional_float(features.get("change_pct"))
            if resilience is not None:
                normalized["resilience"] = resilience
        if quote_is_fresh and quote_quality.get("amount") in {
            "ready",
            "computed",
        }:
            influence = cls._optional_float(features.get("amount"))
            if influence is not None and influence >= 0:
                normalized["influence"] = influence
        if (
            "tradable_market_value" not in normalized
            and "tradable_market_value" not in conflicts
            and quote_is_fresh
        ):
            shares = cls._optional_float(stock.circulating_shares)
            price = cls._optional_float(features.get("price"))
            if shares is not None and shares > 0 and price is not None and price > 0:
                normalized["tradable_market_value"] = shares * price

        for key, value in normalized.items():
            features[key] = value
            feature_quality[key] = "ready"
        features["_feature_quality"] = feature_quality
        if normalized:
            evidence.append(
                {
                    "source": "computed",
                    "as_of": as_of,
                    "quality": "ready",
                    "fields": sorted(normalized),
                    "field_quality": {
                        key: "ready" for key in normalized
                    },
                }
            )

    @classmethod
    def _add_theme_amount_ranks(
        cls,
        candidates: List[CandidateSnapshot],
        as_of: datetime,
    ) -> None:
        grouped: Dict[str, List[CandidateSnapshot]] = {}
        for candidate in candidates:
            if candidate.theme_name:
                grouped.setdefault(candidate.theme_name, []).append(candidate)
        for members in grouped.values():
            amounts = []
            for member in members:
                quality = member.features.get("_feature_quality", {})
                amount = cls._optional_float(member.features.get("amount"))
                if (
                    not isinstance(quality, Mapping)
                    or quality.get("influence") != "ready"
                ):
                    amounts = []
                    break
                if amount is None or amount < 0:
                    amounts = []
                    break
                amounts.append((member, amount))
            if not amounts:
                continue
            previous = None
            rank = 0
            for member, amount in sorted(
                amounts,
                key=lambda item: (-item[1], item[0].stock_code),
            ):
                if amount != previous:
                    rank += 1
                    previous = amount
                member.features["theme_amount_rank"] = rank
                member.features["_feature_quality"]["theme_amount_rank"] = "ready"
                member.evidence.append(
                    {
                        "source": "computed",
                        "as_of": as_of,
                        "quality": "ready",
                        "fields": ["theme_amount_rank"],
                        "field_quality": {"theme_amount_rank": "ready"},
                    }
                )

    @staticmethod
    def _first_limit_seconds(value: Any) -> Optional[int]:
        if isinstance(value, datetime):
            value = value.time()
        if isinstance(value, time):
            seconds = value.hour * 3600 + value.minute * 60 + value.second
            return seconds if 33900 <= seconds <= 54000 else None
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            number = float(value)
            if math.isfinite(number) and 33900 <= number <= 54000:
                return int(number)
            return None
        if not isinstance(value, str):
            return None
        raw = value.strip()
        for pattern in ("%H:%M:%S", "%H:%M", "%H%M%S"):
            try:
                parsed = datetime.strptime(raw, pattern).time()
                seconds = parsed.hour * 3600 + parsed.minute * 60 + parsed.second
                return seconds if 33900 <= seconds <= 54000 else None
            except ValueError:
                continue
        return None

    @staticmethod
    def _is_st_stock(stock: Stock) -> bool:
        is_st = stock.is_st
        flagged = (
            is_st.strip().lower() in {"1", "true", "yes"}
            if isinstance(is_st, str)
            else bool(is_st)
        )
        return flagged or "ST" in (stock.stock_name or "").upper()

    @staticmethod
    def _infer_market(stock_code: str, market: Any) -> str:
        normalized_market = str(market or "").upper()
        if normalized_market in {"SH", "SZ", "BJ"}:
            return normalized_market
        if normalized_market == "BSE" or stock_code.startswith(("4", "8", "92")):
            return "BJ"
        if stock_code.startswith(("5", "6", "9")):
            return "SH"
        return "SZ"

    @staticmethod
    def _normalize_code(value: Any) -> str:
        if value is None:
            return ""
        code = str(value).strip().lower()
        if "." in code:
            left, right = code.split(".", 1)
            code = left if left.isdigit() else right
        if code.startswith(("sh", "sz", "bj")):
            code = code[2:]
        return code.zfill(6) if code.isdigit() and len(code) <= 6 else code

    @staticmethod
    def _pick(payload: Dict[str, Any], *names: str) -> Any:
        for name in names:
            if name in payload and payload[name] not in (None, ""):
                return payload[name]
        return None

    @classmethod
    def _quote_source_value(
        cls,
        payload: Dict[str, Any],
        canonical_name: str,
        *aliases: str,
    ) -> Any:
        missing = payload.get("_missing_fields")
        if isinstance(missing, (list, tuple, set, frozenset)):
            missing_names = {str(name) for name in missing}
            if canonical_name in missing_names:
                return None
        return cls._pick(payload, canonical_name, *aliases)

    @staticmethod
    def _optional_float(value: Any) -> Optional[float]:
        try:
            number = float(value)
            return number if math.isfinite(number) else None
        except (OverflowError, TypeError, ValueError):
            return None

    @classmethod
    def _sanitize_fact(cls, value: Any) -> Any:
        if isinstance(value, float) and not math.isfinite(value):
            return _NONFINITE
        if isinstance(value, Mapping):
            cleaned = {}
            for key, item in value.items():
                sanitized = cls._sanitize_fact(item)
                if sanitized is not _NONFINITE:
                    cleaned[key] = sanitized
            return cleaned
        if isinstance(value, list):
            cleaned = [cls._sanitize_fact(item) for item in value]
            return [item for item in cleaned if item is not _NONFINITE]
        if isinstance(value, tuple):
            cleaned = (cls._sanitize_fact(item) for item in value)
            return tuple(item for item in cleaned if item is not _NONFINITE)
        return value

    @staticmethod
    def _parse_quote_datetime(value: Any) -> Tuple[Optional[datetime], bool]:
        if isinstance(value, datetime):
            return value, True
        if value in (None, ""):
            return None, False
        raw = str(value).strip()
        for pattern in (
            "%Y%m%d%H%M%S",
            "%Y-%m-%d %H:%M:%S",
            "%Y/%m/%d %H:%M:%S",
        ):
            try:
                return datetime.strptime(raw, pattern), True
            except ValueError:
                pass
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00")), True
        except ValueError:
            return None, False

    @staticmethod
    def _age_seconds(as_of: datetime, captured_at: datetime) -> float:
        left = as_of
        right = captured_at
        china_tz = ZoneInfo("Asia/Shanghai")
        if left.tzinfo is None:
            left = left.replace(tzinfo=china_tz)
        if right.tzinfo is None:
            right = right.replace(tzinfo=china_tz)
        return (left - right).total_seconds()

    async def _speed_and_cache(
        self,
        code: str,
        price: float,
        captured_at: datetime,
        *,
        timestamp_ready: bool,
    ) -> Tuple[float, str]:
        async with self._quote_state_lock:
            first_observation = code not in self._previous_prices
            previous = self._previous_prices.get(code)
            if first_observation:
                speed_pct = 0.0
                quality = "baseline"
            elif (
                not isinstance(previous, _QuoteCacheRecord)
                or not math.isfinite(previous.price)
                or previous.price <= 0
                or not self._can_compare_speed(
                    previous.captured_at,
                    captured_at,
                )
            ):
                speed_pct = math.nan
                quality = "missing"
            else:
                computed_speed = (price / previous.price - 1) * 100
                if math.isfinite(computed_speed):
                    speed_pct = round(computed_speed, 4)
                    quality = "ready"
                else:
                    speed_pct = math.nan
                    quality = "missing"

            if timestamp_ready and (
                not isinstance(previous, _QuoteCacheRecord)
                or self._age_seconds(captured_at, previous.captured_at) > 0
            ):
                self._previous_prices[code] = _QuoteCacheRecord(
                    price=price,
                    captured_at=captured_at,
                )
            return speed_pct, quality

    @classmethod
    def _can_compare_speed(
        cls,
        previous_at: datetime,
        current_at: datetime,
    ) -> bool:
        previous = cls._china_datetime(previous_at)
        current = cls._china_datetime(current_at)
        interval = (current - previous).total_seconds()
        return (
            previous.date() == current.date()
            and cls._trading_session(previous) is not None
            and cls._trading_session(previous) == cls._trading_session(current)
            and 0 < interval <= cls.SPEED_MAX_INTERVAL_SECONDS
        )

    @staticmethod
    def _china_datetime(value: datetime) -> datetime:
        china_tz = ZoneInfo("Asia/Shanghai")
        if value.tzinfo is None:
            return value.replace(tzinfo=china_tz)
        return value.astimezone(china_tz)

    @staticmethod
    def _trading_session(value: datetime) -> Optional[str]:
        local_time = value.time().replace(tzinfo=None)
        if time(9, 15) <= local_time < time(9, 27):
            return "auction"
        if time(9, 30) <= local_time <= time(11, 30):
            return "morning"
        if time(13, 0) <= local_time <= time(15, 0):
            return "afternoon"
        return None
