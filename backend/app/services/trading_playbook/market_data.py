"""Point-in-time market data collection for trading playbook snapshots."""

import asyncio
import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from sqlalchemy import desc, select

from app.models.market_review import MarketReviewStockDaily
from app.models.stock import Stock
from app.models.trading_playbook import TradingPlanCandidate, TradingPlanVersion
from app.services.trading_playbook.domain import (
    CandidateSnapshot,
    DataQuality,
    MarketSnapshot,
    QuotePoint,
    QuoteSnapshot,
)
from app.utils.market_data_sanitizer import normalize_change_pct

QuoteFieldQuality = Dict[str, Dict[str, str]]
_NONFINITE = object()


@dataclass(frozen=True)
class _QuoteCacheRecord:
    price: float
    captured_at: datetime


@dataclass(frozen=True)
class _KlineBuildResult:
    features: Dict[str, Any]
    available_at: Optional[datetime]
    reason: Optional[str] = None


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
        self._previous_prices: Dict[str, _QuoteCacheRecord] = {}
        self._quote_state_lock = asyncio.Lock()
        self._quote_semaphore = asyncio.Semaphore(self.max_concurrency)
        self._kline_semaphore = asyncio.Semaphore(self.max_concurrency)

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
    ) -> Tuple[QuoteSnapshot, QuoteFieldQuality]:
        return await self._collect_quote_snapshot(
            stock_codes,
            trade_date,
            as_of,
        )

    async def _collect_quote_snapshot(
        self,
        stock_codes: List[str],
        trade_date: date,
        as_of: datetime,
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
                if age_seconds > self.STALE_AFTER_SECONDS:
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

            speed_pct, quality["speed_pct"] = await self._speed_and_cache(
                code,
                price,
                captured_at,
                timestamp_ready=quality["timestamp"] == "ready",
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

    async def kline_features(
        self,
        stock_code: str,
        market: str,
        stock_name: str,
    ) -> Dict[str, Any]:
        missing = {
            "n_day_high": False,
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
    def _calculate_kline_features(points: Any) -> Dict[str, Any]:
        missing = {
            "n_day_high": False,
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
        )

    async def build_market_snapshot(
        self,
        db: Any,
        source_trade_date: date,
        target_trade_date: date,
        stage: str,
        as_of: datetime,
        force_degraded: bool = False,
    ) -> MarketSnapshot:
        local_as_of = self._china_datetime(as_of)
        database_as_of = local_as_of.replace(tzinfo=None)
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
        candidate_codes = {
            quote.stock_code for quote in change_order[:200]
        } | {
            quote.stock_code for quote in speed_order[:200]
        }
        warnings = list(quote_snapshot.quality.warnings)

        realtime_rows: List[Dict[str, Any]] = []
        pool_trade_date = min(target_trade_date, local_as_of.date())
        try:
            realtime_rows = await self._load_realtime_limit_up(pool_trade_date)
        except Exception as exc:
            warnings.append(f"realtime limit-up pool failed: {exc}")
        realtime_by_code: Dict[str, Dict[str, Any]] = {}
        realtime_provenance_by_code: Dict[str, datetime] = {}
        for row in realtime_rows or []:
            if not isinstance(row, dict):
                warnings.append("invalid realtime limit-up row")
                continue
            code = self._normalize_code(
                self._pick(row, "stock_code", "code", "symbol")
            )
            available_at = self._realtime_available_at(row, pool_trade_date)
            if available_at is None:
                warnings.append(
                    f"realtime row missing usable provenance for {code or 'unknown'}"
                )
                continue
            if available_at > local_as_of:
                warnings.append(f"future realtime row for {code or 'unknown'}")
                continue
            if code in stock_by_code:
                realtime_by_code[code] = row
                realtime_provenance_by_code[code] = self._evidence_datetime(
                    available_at,
                    as_of,
                )
                candidate_codes.add(code)

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

        candidate_codes.intersection_update(stock_by_code)
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

        kline_pairs = await asyncio.gather(
            *(load_kline(code) for code in ordered_candidate_codes)
        )
        kline_by_code = dict(kline_pairs)

        candidates: List[CandidateSnapshot] = []
        for code in ordered_candidate_codes:
            stock = stock_by_code[code]
            quote = quote_snapshot.quotes.get(code)
            features: Dict[str, Any] = {}
            evidence: List[Dict[str, Any]] = []
            if quote is not None:
                quality = quote_field_quality.get(code, {})
                features.update(self._quote_features(quote, quality))
                evidence.append(
                    {
                        "source": "tencent",
                        "as_of": quote.captured_at,
                        "quality": self._quote_evidence_quality(
                            quote,
                            as_of,
                            quality,
                        ),
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
                        "quality": "ready",
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
            candidates.append(
                CandidateSnapshot(
                    stock_code=code,
                    stock_name=stock.stock_name,
                    theme_name=theme_name,
                    features=features,
                    evidence=evidence,
                )
            )

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
            ),
        )

    async def _load_realtime_limit_up(
        self,
        trade_date: date,
    ) -> List[Dict[str, Any]]:
        if self.realtime_limit_up_loader is not None:
            return await self.realtime_limit_up_loader(trade_date)
        from app.services.realtime_limit_up_service import realtime_limit_up_service

        return await realtime_limit_up_service.get_fast_limit_up_pool(trade_date)

    def _quote_evidence_quality(
        self,
        quote: QuotePoint,
        as_of: datetime,
        field_quality: Dict[str, str],
    ) -> str:
        if field_quality.get("timestamp") != "ready":
            return "degraded"
        if self._age_seconds(as_of, quote.captured_at) > self.STALE_AFTER_SECONDS:
            return "stale"
        if "missing" in field_quality.values():
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
        if self._age_seconds(as_of, quote.captured_at) > self.STALE_AFTER_SECONDS:
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
            "datetime",
            "timestamp",
            "quote_time",
            "time",
            "final_seal_time",
            "first_limit_up_time",
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
            "first_limit_time": row.first_limit_time,
            "final_seal_time": row.final_seal_time,
            "change_pct": row.change_pct,
            "amount": row.amount,
            "turnover_rate": row.turnover_rate,
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

    @staticmethod
    def _theme_rankings(
        candidates: List[CandidateSnapshot],
    ) -> List[Dict[str, Any]]:
        grouped: Dict[str, List[CandidateSnapshot]] = {}
        for candidate in candidates:
            if candidate.theme_name:
                grouped.setdefault(candidate.theme_name, []).append(candidate)
        rankings = [
            {
                "theme_name": theme_name,
                "candidate_count": len(members),
                "stock_codes": sorted(member.stock_code for member in members),
            }
            for theme_name, members in grouped.items()
        ]
        return sorted(
            rankings,
            key=lambda item: (-item["candidate_count"], item["theme_name"]),
        )

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
