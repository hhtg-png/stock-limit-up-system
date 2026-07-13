"""Compose and run one deterministic trading-playbook plan stage."""

from __future__ import annotations

import copy
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any

from .domain import MarketSnapshot, ModeEvaluation
from .market_data import TradingPlaybookMarketDataProvider
from .market_state import MarketStateAnalyzer
from .mode_features import ModeFeatureBuilder
from .mode_matcher import ModeMatcher
from .plan_service import TradingPlanService
from .rule_catalog import RuleCatalog


class TradingPlaybookOrchestrator:
    """Run collection, analysis, matching, and one immutable plan write."""

    VALID_STAGES = frozenset(
        {"preclose", "after_close", "overnight", "auction"}
    )
    _NEXT_DAY_STAGES = frozenset({"preclose", "after_close"})
    RULE_COUNT = 19

    def __init__(
        self,
        *,
        market_data: Any,
        analyzer: Any,
        feature_builder: Any,
        matcher: Any,
        plan_service: Any,
        next_trade_date: Callable[[date], date],
    ) -> None:
        self.market_data = market_data
        self.analyzer = analyzer
        self.feature_builder = feature_builder
        self.matcher = matcher
        self.plan_service = plan_service
        self.next_trade_date = next_trade_date

    async def build_stage(
        self,
        db: Any,
        source_trade_date: date,
        stage: str,
        as_of: datetime,
        degraded: bool = False,
    ) -> Any:
        self._validate_request(source_trade_date, stage, as_of, degraded)
        target_trade_date = self._target_trade_date(source_trade_date, stage)

        raw_snapshot = await self.market_data.build_market_snapshot(
            db=db,
            source_trade_date=source_trade_date,
            target_trade_date=target_trade_date,
            stage=stage,
            as_of=as_of,
            force_degraded=degraded,
        )
        self._validate_snapshot_identity(
            raw_snapshot,
            source_trade_date=source_trade_date,
            target_trade_date=target_trade_date,
            stage=stage,
            as_of=as_of,
            layer="market data provider",
        )

        # A partially failing analyzer must not mutate the point-in-time provider
        # result that callers may retain for diagnostics.
        analyzed_snapshot = self.analyzer.enrich_snapshot(
            copy.deepcopy(raw_snapshot)
        )
        self._validate_snapshot_identity(
            analyzed_snapshot,
            source_trade_date=source_trade_date,
            target_trade_date=target_trade_date,
            stage=stage,
            as_of=as_of,
            layer="market state analyzer",
        )

        snapshot = copy.deepcopy(analyzed_snapshot)
        snapshot.candidates = sorted(
            snapshot.candidates,
            key=lambda candidate: self._candidate_code(candidate),
        )
        self._validate_unique_candidates(snapshot)

        # Every build receives the same pre-build snapshot. A builder cannot leak
        # mutations from one stock into another stock's facts.
        feature_base = copy.deepcopy(snapshot)
        for candidate in snapshot.candidates:
            isolated_snapshot = copy.deepcopy(feature_base)
            isolated_candidate = next(
                row
                for row in isolated_snapshot.candidates
                if self._candidate_code(row) == candidate.stock_code
            )
            built = self.feature_builder.build(
                isolated_snapshot,
                isolated_candidate,
            )
            if not isinstance(built, Mapping):
                raise TypeError("mode feature builder must return a mapping")
            merged = copy.deepcopy(candidate.features)
            merged.update(copy.deepcopy(dict(built)))
            candidate.features = merged

        evaluations = []
        stock_names = {}
        for candidate in snapshot.candidates:
            code = self._candidate_code(candidate)
            stock_names[code] = str(candidate.stock_name or "").strip()
            rows = self.matcher.evaluate(snapshot.market_features, candidate)
            try:
                candidate_evaluations = list(rows)
            except TypeError as exc:
                raise TypeError("mode matcher must return an iterable") from exc
            for evaluation in candidate_evaluations:
                if not isinstance(evaluation, ModeEvaluation):
                    raise TypeError(
                        "mode matcher must return ModeEvaluation rows"
                    )
                if evaluation.stock_code != code:
                    raise ValueError(
                        "mode evaluation stock_code must match its candidate"
                    )
                evaluations.append(evaluation)

        evaluations.sort(key=self._evaluation_order)
        rule_snapshot = self._complete_rule_snapshot(
            self.matcher.rule_snapshot()
        )
        self._validate_candidate_mode_coverage(
            snapshot,
            evaluations,
            rule_snapshot,
        )
        return await self.plan_service.generate(
            db,
            snapshot,
            evaluations,
            stock_names,
            rule_snapshot=rule_snapshot,
        )

    @classmethod
    def _validate_request(
        cls,
        source_trade_date: date,
        stage: str,
        as_of: datetime,
        degraded: bool,
    ) -> None:
        if type(source_trade_date) is not date:
            raise TypeError("source_trade_date must be a date")
        if stage not in cls.VALID_STAGES:
            raise ValueError(f"unsupported stage: {stage}")
        if not isinstance(as_of, datetime):
            raise TypeError("as_of must be a datetime")
        if as_of.tzinfo is None or as_of.utcoffset() is None:
            raise ValueError("as_of must be timezone-aware")
        if type(degraded) is not bool:
            raise TypeError("degraded must be a boolean")

    def _target_trade_date(self, source_trade_date: date, stage: str) -> date:
        if stage not in self._NEXT_DAY_STAGES:
            return source_trade_date
        target = self.next_trade_date(source_trade_date)
        if type(target) is not date:
            raise TypeError("next_trade_date must return a date")
        if target <= source_trade_date:
            raise ValueError("next_trade_date must be later than source date")
        return target

    @staticmethod
    def _validate_snapshot_identity(
        snapshot: Any,
        *,
        source_trade_date: date,
        target_trade_date: date,
        stage: str,
        as_of: datetime,
        layer: str,
    ) -> None:
        if not isinstance(snapshot, MarketSnapshot):
            raise TypeError(f"{layer} must return a MarketSnapshot")
        expected = (
            source_trade_date,
            target_trade_date,
            stage,
            as_of,
        )
        actual = (
            snapshot.source_trade_date,
            snapshot.target_trade_date,
            snapshot.stage,
            snapshot.as_of,
        )
        if actual != expected:
            raise ValueError(f"{layer} returned mismatched snapshot identity")

    @classmethod
    def _validate_unique_candidates(cls, snapshot: MarketSnapshot) -> None:
        seen = set()
        for candidate in snapshot.candidates:
            code = cls._candidate_code(candidate)
            if code in seen:
                raise ValueError(f"duplicate candidate stock_code: {code}")
            seen.add(code)

    @staticmethod
    def _candidate_code(candidate: Any) -> str:
        code = getattr(candidate, "stock_code", None)
        if not isinstance(code, str) or not code.strip():
            raise ValueError("candidate stock_code is required")
        if code != code.strip():
            raise ValueError("candidate stock_code must not contain whitespace")
        return code

    @staticmethod
    def _evaluation_order(evaluation: ModeEvaluation) -> tuple[Any, ...]:
        return (
            evaluation.stock_code,
            evaluation.mode_key,
            evaluation.status,
            evaluation.risk_level,
            evaluation.action_scope,
        )

    @classmethod
    def _validate_candidate_mode_coverage(
        cls,
        snapshot: MarketSnapshot,
        evaluations: Sequence[ModeEvaluation],
        rule_snapshot: Sequence[Mapping[str, Any]],
    ) -> None:
        expected = {row["mode_key"] for row in rule_snapshot}
        by_candidate: dict[str, dict[str, int]] = {
            cls._candidate_code(candidate): {}
            for candidate in snapshot.candidates
        }
        for evaluation in evaluations:
            counts = by_candidate[evaluation.stock_code]
            counts[evaluation.mode_key] = counts.get(evaluation.mode_key, 0) + 1

        for stock_code in sorted(by_candidate):
            counts = by_candidate[stock_code]
            actual = set(counts)
            missing = sorted(expected - actual)
            extra = sorted(actual - expected)
            duplicate = sorted(
                mode_key for mode_key, count in counts.items() if count > 1
            )
            if missing or extra or duplicate:
                raise ValueError(
                    f"candidate {stock_code} mode coverage mismatch: "
                    f"missing={missing}, extra={extra}, duplicate={duplicate}"
                )

    @classmethod
    def _complete_rule_snapshot(cls, rows: Any) -> list[dict[str, Any]]:
        if not isinstance(rows, Sequence) or isinstance(
            rows, (str, bytes, bytearray)
        ):
            raise TypeError("complete rule snapshot must be a sequence")
        if len(rows) != cls.RULE_COUNT:
            raise ValueError("complete rule snapshot must contain 19 rules")
        normalized = []
        seen = set()
        for row in rows:
            if not isinstance(row, Mapping):
                raise TypeError("rule snapshot rows must be mappings")
            mode_key = row.get("mode_key")
            if not isinstance(mode_key, str) or not mode_key.strip():
                raise ValueError("rule snapshot mode_key is required")
            if mode_key in seen:
                raise ValueError(f"duplicate rule snapshot mode_key: {mode_key}")
            seen.add(mode_key)
            normalized.append(copy.deepcopy(dict(row)))
        normalized.sort(key=lambda row: row["mode_key"])
        return normalized


def build_default_orchestrator(
    *,
    quote_api: Any,
    kline_loader: Callable[..., Any],
    realtime_limit_up_loader: Callable[..., Any],
    full_market_context_loader: Callable[..., Any],
    next_trade_date: Callable[[date], date],
) -> TradingPlaybookOrchestrator:
    """Build the sole production pipeline from explicit market dependencies."""
    catalog_path = (
        Path(__file__).resolve().parents[2]
        / "data"
        / "trading_playbook_rules_v1.json"
    )
    catalog = RuleCatalog(catalog_path).load()
    market_data = TradingPlaybookMarketDataProvider(
        quote_api=quote_api,
        kline_loader=kline_loader,
        realtime_limit_up_loader=realtime_limit_up_loader,
        full_market_context_loader=full_market_context_loader,
    )
    return TradingPlaybookOrchestrator(
        market_data=market_data,
        analyzer=MarketStateAnalyzer(),
        feature_builder=ModeFeatureBuilder(),
        matcher=ModeMatcher(
            catalog["rules"],
            catalog_version=catalog["catalog_version"],
        ),
        plan_service=TradingPlanService(),
        next_trade_date=next_trade_date,
    )
