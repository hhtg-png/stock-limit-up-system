"""Conservative point-in-time features for transcript-derived trading modes."""

from __future__ import annotations

import copy
import math
from dataclasses import dataclass
from datetime import date, datetime
from statistics import median
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

from .domain import CandidateSnapshot, MarketSnapshot


FEATURE_KEYS = {
    "high_volatility",
    "high_position",
    "same_level_turnover",
    "middle_army",
    "started_before_theme",
    "unique_survivor",
    "turn_confirmed",
    "stronger_confirmed",
    "confirmed_leader",
    "acceleration_to_divergence",
    "low_position_new_start",
    "supplement",
    "first_bearish",
    "trend_established",
    "pullback",
    "consolidation_rebreak",
    "linkage_confirmed",
    "trend_turn_two",
    "middle_army_linkage",
    "divergence_days",
    "resilience_rank",
    "theme_alive",
    "theme_dead",
    "snake_setup",
    "right_reversal",
    "external_switch",
    "theme_rank",
    "recognition_rank",
    "tail_action_eligible",
    "reference_price",
    "planned_pullback_price",
    "planned_breakout_price",
    "exit_change_pct_floor",
}


def _tri_or(*values: Optional[bool]) -> Optional[bool]:
    """Kleene OR: true wins, unknown survives unless everything is false."""
    normalized = [value if isinstance(value, bool) else None for value in values]
    if any(value is True for value in normalized):
        return True
    if normalized and all(value is False for value in normalized):
        return False
    return None


def _tri_and(*values: Optional[bool]) -> Optional[bool]:
    """Kleene AND: false wins, unknown survives unless everything is true."""
    normalized = [value if isinstance(value, bool) else None for value in values]
    if any(value is False for value in normalized):
        return False
    if normalized and all(value is True for value in normalized):
        return True
    return None


@dataclass(frozen=True)
class _PeerSet:
    ready: Tuple[CandidateSnapshot, ...]
    unknown_count: int = 0


_EVIDENCE_SOURCES = {
    "quote": {"tencent", "quote", "market_quote"},
    "realtime": {"realtime_limit_up_pool"},
    "review": {"market_review_stock_daily"},
    "kline": {"kline", "validated_support", "support", "technical_support"},
    "plan": {"trading_plan_candidate"},
    "computed": {
        "computed",
        "ranker",
        "market_state",
        "full_market_quote_rank",
        "auction",
        "theme_ranker",
        "recognition_ranker",
    },
}

_QUOTE_FACTS = {
    "price",
    "captured_at",
    "pre_close",
    "open_price",
    "change_pct",
    "amount",
    "turnover_rate",
    "bid1_price",
    "bid1_volume",
    "limit_up",
    "speed_pct",
    "speed_quality",
}
_KLINE_FACTS = {
    "kline_quality",
    "n_day_high",
    "consolidation_days",
    "trend_established",
    "validated_support",
    "support_price",
    "five_day_low",
    "five_day_low_price",
    "prior_n_day_high",
    "n_day_high_price",
    "prior_high",
}
_RECOGNITION_FACTS = {
    "recognition_rank",
    "resilience_rank",
    "influence_rank",
    "fastest_rank",
    "highest_rank",
    "hardest_rank",
    "recognition_score",
    "recognition_evidence",
}


def _finite_number(
    value: Any,
    *,
    minimum: Optional[float] = None,
    maximum: Optional[float] = None,
    integer: bool = False,
) -> Optional[float]:
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(number):
        return None
    if minimum is not None and number < minimum:
        return None
    if maximum is not None and number > maximum:
        return None
    if integer and not number.is_integer():
        return None
    return number


def _strict_flag(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1"}:
            return True
        if normalized in {"false", "0"}:
            return False
    return None


def _parse_datetime(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    if value in (None, ""):
        return None
    raw = str(value).strip()
    for pattern in (
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y%m%d%H%M%S",
    ):
        try:
            return datetime.strptime(raw, pattern)
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def _comparable_datetime(value: datetime, reference: datetime) -> datetime:
    china = ZoneInfo("Asia/Shanghai")
    local_value = (
        value.replace(tzinfo=china)
        if value.tzinfo is None
        else value.astimezone(china)
    )
    if reference.tzinfo is None:
        return local_value.replace(tzinfo=None)
    return local_value.astimezone(reference.tzinfo)


def _first_number(
    source: Mapping[str, Any],
    names: Sequence[str],
    **bounds: Any,
) -> Optional[float]:
    for name in names:
        if name in source:
            value = _finite_number(source.get(name), **bounds)
            if value is not None:
                return value
    return None


def _first_flag(source: Mapping[str, Any], names: Sequence[str]) -> Optional[bool]:
    values = [
        _strict_flag(source.get(name)) for name in names if name in source
    ]
    return _tri_or(*values)


def _fact_sources(source: Mapping[str, Any]) -> Tuple[Mapping[str, Any], ...]:
    nested = []
    for key in ("realtime_limit_up_fact", "latest_review_fact"):
        value = source.get(key)
        if isinstance(value, Mapping):
            nested.append(value)
    return (source, *nested)


def _fact_flag(source: Mapping[str, Any], names: Sequence[str]) -> Optional[bool]:
    return _tri_or(
        *(
            _first_flag(facts, names)
            for facts in _fact_sources(source)
            if any(name in facts for name in names)
        )
    )


def _seconds_since_midnight(value: Any) -> Optional[int]:
    number = _finite_number(value, minimum=0, maximum=86399, integer=True)
    if number is not None:
        return int(number)
    if not isinstance(value, str):
        return None
    raw = value.strip()
    for pattern in ("%H:%M:%S", "%H:%M"):
        try:
            parsed = datetime.strptime(raw, pattern).time()
            return parsed.hour * 3600 + parsed.minute * 60 + parsed.second
        except ValueError:
            pass
    return None


class ModeFeatureBuilder:
    """Build mode prerequisites without converting absent evidence to false facts."""

    def __init__(
        self,
        *,
        hard_stop_pct: float = 5.0,
        quote_fresh_seconds: int = 120,
        market_tick: float = 0.01,
    ) -> None:
        stop = _finite_number(hard_stop_pct, minimum=0.01, maximum=99)
        tick = _finite_number(market_tick, minimum=0.0001)
        freshness = _finite_number(
            quote_fresh_seconds,
            minimum=1,
            maximum=3600,
            integer=True,
        )
        if stop is None or tick is None or freshness is None:
            raise ValueError("invalid mode feature settings")
        self.hard_stop_pct = stop
        self.quote_fresh_seconds = int(freshness)
        self.market_tick = tick

    def build(
        self,
        snapshot: MarketSnapshot,
        candidate: CandidateSnapshot,
    ) -> Dict[str, Any]:
        source_quality = self._evidence_index(snapshot, candidate)
        raw = self._trusted_facts(snapshot, candidate, source_quality)
        result: Dict[str, Any] = {key: None for key in FEATURE_KEYS}
        result["planned_pullback_quality"] = "missing"
        quality: Dict[str, str] = {}

        point_in_time_valid = self._point_in_time_valid(
            snapshot,
            raw,
            source_quality,
        )
        trust_quality = raw.get("_trust_quality")
        trust_states = (
            trust_quality.values()
            if isinstance(trust_quality, Mapping)
            else ()
        )
        candidate_quality = (
            "degraded"
            if any(
                state in {"degraded", "invalid"}
                for state in (*source_quality.values(), *trust_states)
            )
            else "ready"
        )
        result.update(
            {
                "_snapshot_quality_status": snapshot.quality.status,
                "_snapshot_stale": bool(snapshot.quality.stale),
                "_point_in_time_valid": point_in_time_valid,
                "_stage": snapshot.stage,
                "_source_quality": source_quality,
                "_candidate_quality_status": candidate_quality,
            }
        )

        if point_in_time_valid:
            reference_price = self._reference_price(snapshot, candidate, raw)
            result["reference_price"] = reference_price
            result.update(
                self._planned_prices(
                    snapshot,
                    candidate,
                    raw,
                    reference_price,
                )
            )
            result["high_volatility"] = self._high_volatility(
                candidate.stock_code,
                raw,
            )
            result["theme_rank"] = self._rank(raw.get("theme_rank"))
            result["recognition_rank"] = self._rank(
                raw.get("recognition_rank")
            )
            result["resilience_rank"] = self._rank(
                raw.get("resilience_rank")
            )
            result["divergence_days"] = self._days(
                raw.get(
                    "divergence_days",
                    snapshot.market_features.get("divergence_days"),
                )
            )

            trusted_candidate = CandidateSnapshot(
                stock_code=candidate.stock_code,
                stock_name=candidate.stock_name,
                theme_name=candidate.theme_name,
                features=raw,
                evidence=copy.deepcopy(candidate.evidence),
            )
            theme_peers = self._theme_peers(snapshot, trusted_candidate)
            result["high_position"] = self._high_position(
                trusted_candidate,
                theme_peers,
            )
            result["same_level_turnover"] = self._same_level_turnover(
                trusted_candidate,
                theme_peers,
            )
            result["middle_army"] = self._middle_army(
                trusted_candidate,
                theme_peers,
            )
            result["started_before_theme"] = self._started_before_theme(
                snapshot,
                trusted_candidate,
            )
            result["unique_survivor"] = self._unique_survivor(
                trusted_candidate,
                theme_peers,
                result["recognition_rank"],
            )

            prior_state = self._prior_state(raw)
            result["turn_confirmed"] = self._turn_confirmed(
                raw,
                prior_state,
            )
            result["stronger_confirmed"] = self._stronger_confirmed(
                raw,
                prior_state,
                reference_price,
                result["recognition_rank"],
            )
            result["confirmed_leader"] = self._confirmed_leader(
                raw,
                prior_state,
                result["stronger_confirmed"],
                result["recognition_rank"],
            )
            result["acceleration_to_divergence"] = (
                self._acceleration_to_divergence(
                    raw,
                    result["confirmed_leader"],
                )
            )
            result["low_position_new_start"] = self._low_position_new_start(
                raw,
                self._board_height(raw),
            )
            result["supplement"] = self._supplement(
                snapshot,
                raw,
                result["low_position_new_start"],
            )
            result["first_bearish"] = self._first_bearish(
                raw,
                result["confirmed_leader"],
            )

            kline_ready = raw.get("kline_quality") == "ready"
            result["trend_established"] = (
                _strict_flag(raw.get("trend_established"))
                if kline_ready
                else None
            )
            result["pullback"] = self._pullback(
                raw,
                reference_price,
                result["planned_pullback_price"],
                result["planned_pullback_quality"],
                result["trend_established"],
            )
            result["linkage_confirmed"] = self._linkage_confirmed(
                snapshot,
                trusted_candidate,
                raw,
            )
            result["middle_army_linkage"] = self._middle_army_linkage(
                raw,
                theme_peers,
            )
            result["consolidation_rebreak"] = self._consolidation_rebreak(
                raw,
                result["trend_established"],
            )
            result["trend_turn_two"] = self._all_flags(
                result["trend_established"],
                self._consolidation_in_range(raw),
                _strict_flag(raw.get("n_day_high")) if kline_ready else None,
                result["linkage_confirmed"],
            )

            result["theme_alive"] = self._theme_alive(theme_peers)
            result["theme_dead"] = self._theme_dead(
                trusted_candidate,
                raw,
                theme_peers,
            )
            result["snake_setup"] = self._all_flags(
                result["theme_alive"],
                _first_flag(raw, ("snake_pattern", "snake_setup_confirmed")),
            )
            result["right_reversal"] = self._all_flags(
                result["theme_dead"],
                _first_flag(raw, ("right_side_breakout", "right_reversal_confirmed")),
            )
            result["external_switch"] = self._external_switch(
                snapshot,
                trusted_candidate,
                raw,
                result["theme_rank"],
            )
            result["tail_action_eligible"] = self._tail_action_eligible(
                snapshot,
                trusted_candidate,
                raw,
            )

        for key in FEATURE_KEYS:
            quality[key] = self._feature_quality(key, raw, result.get(key))
        quality["planned_pullback_price"] = result["planned_pullback_quality"]
        if not point_in_time_valid:
            for key in FEATURE_KEYS:
                result[key] = None
                quality[key] = "missing"
        result["_feature_quality"] = quality
        return result

    @classmethod
    def _evidence_index(
        cls,
        snapshot: MarketSnapshot,
        candidate: CandidateSnapshot,
    ) -> Dict[str, str]:
        return {
            family: cls._evidence_state(
                snapshot,
                candidate.evidence,
                sources,
                accept_ok=family == "review",
            )
            for family, sources in _EVIDENCE_SOURCES.items()
        }

    @staticmethod
    def _evidence_state(
        snapshot: MarketSnapshot,
        evidence_rows: Iterable[Mapping[str, Any]],
        sources: set[str],
        *,
        accept_ok: bool = False,
    ) -> str:
        matched = []
        for item in evidence_rows:
            if not isinstance(item, Mapping):
                continue
            if str(item.get("source") or "").strip() in sources:
                matched.append(item)
        if not matched:
            return "missing"

        accepted = {"ready", "computed"}
        if accept_ok:
            accepted.add("ok")
        saw_ready = False
        saw_degraded = False
        for item in matched:
            observed_at = _parse_datetime(item.get("as_of"))
            if observed_at is None:
                saw_degraded = True
                continue
            if _comparable_datetime(observed_at, snapshot.as_of) > snapshot.as_of:
                return "invalid"
            if (
                _strict_flag(item.get("stale")) is True
                or item.get("quality") not in accepted
            ):
                saw_degraded = True
            else:
                saw_ready = True
        if saw_degraded:
            return "degraded"
        return "ready" if saw_ready else "degraded"

    @classmethod
    def _trusted_facts(
        cls,
        snapshot: MarketSnapshot,
        candidate: CandidateSnapshot,
        source_quality: Optional[Mapping[str, str]] = None,
    ) -> Dict[str, Any]:
        del snapshot  # evidence times were normalized into source_quality
        raw = copy.deepcopy(candidate.features)
        states = dict(source_quality or {})
        explicit_quality = raw.get("_feature_quality")
        explicit_quality = (
            explicit_quality if isinstance(explicit_quality, Mapping) else {}
        )
        trusted: Dict[str, Any] = {}
        trust_quality: Dict[str, str] = {}

        for key, value in raw.items():
            if key == "_feature_quality":
                continue
            family = cls._feature_family(key)
            if family == "recognition":
                state = cls._normalized_quality(raw.get("recognition_quality"))
            elif family == "theme":
                state = cls._normalized_quality(raw.get("theme_quality"))
            elif family is not None:
                state = states.get(family, "missing")
            else:
                state = cls._normalized_quality(explicit_quality.get(key))
                if state == "missing":
                    state = states.get("computed", "missing")

            trust_quality[key] = state
            if state == "ready":
                trusted[key] = value

        for metadata in ("recognition_quality", "theme_quality"):
            if metadata in raw:
                trusted[metadata] = raw[metadata]
        trusted["_trust_quality"] = trust_quality
        return trusted

    @staticmethod
    def _feature_family(key: str) -> Optional[str]:
        if key == "realtime_limit_up_fact":
            return "realtime"
        if key in {"review_history", "latest_review_fact"} or key.startswith(
            "review_"
        ):
            return "review"
        if key == "plan_candidate_fact":
            return "plan"
        if key in _KLINE_FACTS:
            return "kline"
        if key in _QUOTE_FACTS:
            return "quote"
        if key in _RECOGNITION_FACTS:
            return "recognition"
        if key in {"theme_rank", "theme_score", "theme_evidence"}:
            return "theme"
        return None

    @staticmethod
    def _normalized_quality(value: Any) -> str:
        return "ready" if value in {"ready", "computed"} else (
            "missing" if value in {None, "", "missing"} else "degraded"
        )

    @classmethod
    def _theme_peers(
        cls,
        snapshot: MarketSnapshot,
        candidate: CandidateSnapshot,
    ) -> _PeerSet:
        theme = str(candidate.theme_name or "").strip()
        peers = (
            tuple(
                peer
                for peer in snapshot.candidates
                if peer.stock_code == candidate.stock_code
            )
            if not theme
            else tuple(
                peer
                for peer in snapshot.candidates
                if str(peer.theme_name or "").strip() == theme
            )
        )
        ready = []
        unknown_count = 0
        for peer in peers:
            source_quality = cls._evidence_index(snapshot, peer)
            features = cls._trusted_facts(snapshot, peer, source_quality)
            if any(
                not key.startswith("_")
                and key not in {"recognition_quality", "theme_quality"}
                for key in features
            ):
                ready.append(
                    CandidateSnapshot(
                        stock_code=peer.stock_code,
                        stock_name=peer.stock_name,
                        theme_name=peer.theme_name,
                        features=features,
                        evidence=copy.deepcopy(peer.evidence),
                    )
                )
            else:
                unknown_count += 1
        return _PeerSet(ready=tuple(ready), unknown_count=unknown_count)

    @staticmethod
    def _has_ready_evidence(
        snapshot: MarketSnapshot,
        evidence_rows: Iterable[Mapping[str, Any]],
        *,
        allowed_sources: Optional[set[str]] = None,
        ready_qualities: Optional[set[str]] = None,
    ) -> bool:
        accepted_qualities = ready_qualities or {"ready", "computed"}
        ready = False
        for item in evidence_rows:
            if not isinstance(item, Mapping):
                continue
            source = str(item.get("source") or "").strip()
            if allowed_sources is not None and source not in allowed_sources:
                continue
            observed_at = _parse_datetime(item.get("as_of"))
            if observed_at is None:
                continue
            if _comparable_datetime(observed_at, snapshot.as_of) > snapshot.as_of:
                return False
            if (
                _strict_flag(item.get("stale")) is True
                or item.get("quality") not in accepted_qualities
            ):
                continue
            ready = True
        return ready

    @staticmethod
    def _point_in_time_valid(
        snapshot: MarketSnapshot,
        raw: Mapping[str, Any],
        source_quality: Mapping[str, str],
    ) -> bool:
        times = [_parse_datetime(raw.get("captured_at"))]
        for observed_at in times:
            if observed_at is None:
                continue
            if _comparable_datetime(observed_at, snapshot.as_of) > snapshot.as_of:
                return False
        quality_as_of = _parse_datetime(snapshot.quality.as_of)
        if quality_as_of is not None and (
            _comparable_datetime(quality_as_of, snapshot.as_of) > snapshot.as_of
        ):
            return False
        if source_quality.get("quote") == "invalid":
            return False
        return True

    @classmethod
    def _reference_price(
        cls,
        snapshot: MarketSnapshot,
        candidate: CandidateSnapshot,
        raw: Mapping[str, Any],
    ) -> Optional[float]:
        price = _finite_number(raw.get("price"), minimum=0.0001)
        captured_at = _parse_datetime(raw.get("captured_at"))
        if price is None or captured_at is None:
            return None
        if _comparable_datetime(captured_at, snapshot.as_of) > snapshot.as_of:
            return None
        if not cls._has_ready_evidence(
            snapshot,
            candidate.evidence,
            allowed_sources={"tencent", "quote", "market_quote"},
        ):
            return None
        return price

    def _planned_prices(
        self,
        snapshot: MarketSnapshot,
        candidate: CandidateSnapshot,
        raw: Mapping[str, Any],
        reference_price: Optional[float],
    ) -> Dict[str, Any]:
        if reference_price is None:
            return {
                "planned_pullback_price": None,
                "planned_pullback_quality": "missing",
                "planned_breakout_price": None,
                "hard_stop_price": None,
                "exit_change_pct_floor": None,
            }
        support_sources = {
            "kline",
            "validated_support",
            "support",
            "technical_support",
        }
        support_evidence_ready = self._has_ready_evidence(
            snapshot,
            candidate.evidence,
            allowed_sources=support_sources,
        )
        supports = (
            [
                _finite_number(
                    raw.get(key),
                    minimum=0.0001,
                    maximum=reference_price,
                )
                for key in (
                    "validated_support",
                    "support_price",
                    "five_day_low",
                    "five_day_low_price",
                )
            ]
            if support_evidence_ready
            else []
        )
        valid_supports = [value for value in supports if value is not None]
        pullback = max(valid_supports) if valid_supports else (
            reference_price * (1 - self.hard_stop_pct / 100)
        )
        prior_high = _first_number(
            raw,
            ("prior_n_day_high", "n_day_high_price", "prior_high"),
            minimum=0.0001,
        )
        breakout_base = prior_high if prior_high is not None else reference_price
        raw_exit = _finite_number(
            raw.get("exit_change_pct_floor"),
            minimum=-100,
            maximum=0,
        )
        return {
            "planned_pullback_price": round(pullback, 2),
            "planned_pullback_quality": "ready"
            if valid_supports
            else "fallback",
            "planned_breakout_price": round(
                breakout_base + self.market_tick,
                2,
            ),
            "hard_stop_price": round(
                reference_price * (1 - self.hard_stop_pct / 100),
                2,
            ),
            "exit_change_pct_floor": max(-5.0, raw_exit)
            if raw_exit is not None
            else -5.0,
        }

    @staticmethod
    def _high_volatility(stock_code: str, raw: Mapping[str, Any]) -> Optional[bool]:
        if not str(stock_code).startswith(("300", "301", "688", "8", "4", "92")):
            return False
        present = []
        for key in ("speed_rank", "change_rank"):
            if key in raw:
                present.append(ModeFeatureBuilder._rank(raw.get(key)))
        if any(rank is not None and rank <= 20 for rank in present):
            return True
        if len(present) == 2 and all(rank is not None for rank in present):
            return False
        return None

    @staticmethod
    def _rank(value: Any) -> Optional[int]:
        number = _finite_number(value, minimum=1, integer=True)
        return int(number) if number is not None else None

    @staticmethod
    def _days(value: Any) -> Optional[int]:
        number = _finite_number(value, minimum=0, maximum=1000, integer=True)
        return int(number) if number is not None else None

    @staticmethod
    def _board_height(raw: Mapping[str, Any]) -> Optional[int]:
        number = _first_number(
            raw,
            (
                "board_height",
                "review_today_continuous_days",
                "today_continuous_days",
                "continuous_days",
            ),
            minimum=0,
            maximum=100,
            integer=True,
        )
        if number is not None:
            return int(number)
        realtime = raw.get("realtime_limit_up_fact")
        if isinstance(realtime, Mapping):
            number = _first_number(
                realtime,
                ("board_height", "continuous_days", "consecutive_days"),
                minimum=0,
                maximum=100,
                integer=True,
            )
        return int(number) if number is not None else None

    @staticmethod
    def _sealed(raw: Mapping[str, Any]) -> Optional[bool]:
        return _fact_flag(
            raw,
            (
                "sealed",
                "review_today_sealed_close",
                "today_sealed_close",
                "is_sealed",
            ),
        )

    @classmethod
    def _high_position(
        cls,
        candidate: CandidateSnapshot,
        peers: _PeerSet,
    ) -> Optional[bool]:
        if not any(
            peer.stock_code == candidate.stock_code for peer in peers.ready
        ):
            return None
        candidate_height = cls._board_height(candidate.features)
        if candidate_height is not None and candidate_height < 2:
            return False
        heights = [cls._board_height(peer.features) for peer in peers.ready]
        if candidate_height is None or any(value is None for value in heights):
            return None
        if any(value > candidate_height for value in heights):
            return False
        if peers.unknown_count:
            return None
        return True

    @classmethod
    def _same_level_turnover(
        cls,
        candidate: CandidateSnapshot,
        peers: _PeerSet,
    ) -> Optional[bool]:
        if not any(
            peer.stock_code == candidate.stock_code for peer in peers.ready
        ):
            return None
        peer_rows = peers.ready
        height = cls._board_height(candidate.features)
        heights = [cls._board_height(peer.features) for peer in peer_rows]
        if height is None or any(value is None for value in heights):
            return None
        maximum = max(heights)
        if height != maximum:
            return False
        if peers.unknown_count:
            return None
        if sum(value == height for value in heights) < 2:
            return False
        sealed = cls._sealed(candidate.features)
        turnover = _finite_number(
            candidate.features.get("turnover_rate"),
            minimum=0,
            maximum=100,
        )
        if sealed is False or turnover == 0:
            return False
        if sealed is None or turnover is None:
            return None
        return True

    @staticmethod
    def _amount(raw: Mapping[str, Any]) -> Optional[float]:
        return _first_number(raw, ("amount", "review_amount"), minimum=0)

    @classmethod
    def _middle_army(
        cls,
        candidate: CandidateSnapshot,
        peers: _PeerSet,
    ) -> Optional[bool]:
        if not any(
            peer.stock_code == candidate.stock_code for peer in peers.ready
        ):
            return None
        peer_rows = peers.ready
        amounts = [cls._amount(peer.features) for peer in peer_rows]
        candidate_amount = cls._amount(candidate.features)
        explicit_rank = cls._rank(candidate.features.get("theme_amount_rank"))
        if explicit_rank is not None and explicit_rank != 1:
            return False
        if explicit_rank is None:
            if candidate_amount is None or any(value is None for value in amounts):
                return None
            if candidate_amount < max(amounts):
                return False
        if peers.unknown_count:
            return None
        values = [
            _finite_number(
                peer.features.get("tradable_market_value"),
                minimum=0,
            )
            for peer in peer_rows
        ]
        candidate_value = _finite_number(
            candidate.features.get("tradable_market_value"),
            minimum=0,
        )
        if candidate_value is None or any(value is None for value in values):
            return None
        return candidate_value >= median(values)

    @staticmethod
    def _theme_row(
        snapshot: MarketSnapshot,
        candidate: CandidateSnapshot,
    ) -> Optional[Mapping[str, Any]]:
        theme_name = str(candidate.theme_name or "").strip()
        for row in snapshot.theme_rankings:
            if str(row.get("theme_name") or "").strip() == theme_name:
                return row if row.get("quality") == "ready" else None
        return None

    @classmethod
    def _started_before_theme(
        cls,
        snapshot: MarketSnapshot,
        candidate: CandidateSnapshot,
    ) -> Optional[bool]:
        explicit = _first_flag(
            candidate.features,
            ("started_before_theme_fact", "started_before_theme_confirmed"),
        )
        if explicit is not None:
            return explicit
        first_limit = None
        for key in (
            "first_limit_seconds",
            "review_first_limit_seconds",
            "review_first_limit_time",
            "first_limit_time",
        ):
            if key in candidate.features:
                first_limit = _seconds_since_midnight(candidate.features.get(key))
                if first_limit is not None:
                    break
        if first_limit is not None and not 33900 <= first_limit <= 54000:
            first_limit = None
        theme = cls._theme_row(snapshot, candidate)
        theme_start = _first_number(
            theme or {},
            ("outbreak_start_seconds", "theme_start_seconds"),
            minimum=33900,
            maximum=54000,
        )
        if first_limit is None or theme_start is None:
            return None
        return first_limit < theme_start

    @classmethod
    def _unique_survivor(
        cls,
        candidate: CandidateSnapshot,
        peers: _PeerSet,
        recognition_rank: Optional[int],
    ) -> Optional[bool]:
        if recognition_rank is not None and recognition_rank != 1:
            return False
        if recognition_rank is None:
            return None
        former_peers = []
        states: list[Optional[bool]] = [None] * peers.unknown_count
        for peer in peers.ready:
            if peer.stock_code == candidate.stock_code:
                continue
            former_state = cls._former_high_position_state(peer.features)
            if former_state is True:
                former_peers.append(peer)
            elif former_state is None:
                states.append(None)
        if not former_peers and not states:
            return None
        for peer in former_peers:
            broken = _first_flag(
                peer.features,
                ("review_today_broken", "today_broken", "broken"),
            )
            opened = _first_flag(peer.features, ("opened", "opened_board"))
            trend_broken = _first_flag(peer.features, ("trend_broken",))
            states.append(_tri_or(broken, opened, trend_broken))
        if any(value is False for value in states):
            return False
        if any(value is None for value in states):
            return None
        return True

    @classmethod
    def _former_high_position_state(
        cls,
        raw: Mapping[str, Any],
    ) -> Optional[bool]:
        explicit = _first_flag(raw, ("former_high_position",))
        yesterday_height = _first_number(
            raw,
            ("review_yesterday_continuous_days",),
            minimum=0,
            maximum=100,
            integer=True,
        )
        current_height = cls._board_height(raw)
        return _tri_or(
            explicit,
            yesterday_height >= 2 if yesterday_height is not None else None,
            current_height >= 2 if current_height is not None else None,
        )

    @staticmethod
    def _prior_state(raw: Mapping[str, Any]) -> Optional[str]:
        for key in ("prior_mode_state", "prior_state"):
            value = raw.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        plan = raw.get("plan_candidate_fact")
        if isinstance(plan, Mapping):
            role = str(plan.get("role") or "").strip()
            mode = str(plan.get("primary_mode_key") or "").strip()
            if role == "survivor" or mode == "unique_survivor_trial":
                return "survivor"
            if mode == "leader_turn_two":
                return "turn_confirmed"
            if (
                role == "confirmed_leader"
                or mode == "leader_stronger_confirmation"
            ):
                return "stronger_confirmed"
        return None

    @staticmethod
    def _turn_confirmed(
        raw: Mapping[str, Any],
        prior_state: Optional[str],
    ) -> Optional[bool]:
        if prior_state is not None and prior_state != "survivor":
            return False
        if prior_state is None:
            return None
        resealed = _tri_or(
            _fact_flag(raw, ("resealed", "is_resealed")),
            _fact_flag(raw, ("reversal_limit_up",)),
        )
        breakout = _fact_flag(raw, ("right_side_breakout",))
        influence_rank = ModeFeatureBuilder._rank(raw.get("influence_rank"))
        influence_ready = (
            influence_rank <= 3 if influence_rank is not None else None
        )
        influenced_breakout = _tri_and(breakout, influence_ready)
        return _tri_or(resealed, influenced_breakout)

    @staticmethod
    def _stronger_confirmed(
        raw: Mapping[str, Any],
        prior_state: Optional[str],
        price: Optional[float],
        recognition_rank: Optional[int],
    ) -> Optional[bool]:
        if prior_state is not None and prior_state != "turn_confirmed":
            return False
        if prior_state is None:
            return None
        pre_close = _finite_number(raw.get("pre_close"), minimum=0.0001)
        open_price = _finite_number(raw.get("open_price"), minimum=0.0001)
        open_strength = (
            open_price > pre_close
            if open_price is not None and pre_close is not None
            else None
        )
        current_strength = (
            price > pre_close
            if price is not None and pre_close is not None
            else None
        )
        strength = _tri_or(open_strength, current_strength)
        new_leader = _first_flag(raw, ("new_recognition_leader",))
        if new_leader is not None:
            no_new_leader = not new_leader
        elif recognition_rank is not None:
            no_new_leader = recognition_rank == 1
        else:
            no_new_leader = None
        return _tri_and(strength, no_new_leader)

    @staticmethod
    def _confirmed_leader(
        raw: Mapping[str, Any],
        prior_state: Optional[str],
        stronger: Optional[bool],
        recognition_rank: Optional[int],
    ) -> Optional[bool]:
        explicit = _first_flag(raw, ("confirmed_leader_fact",))
        if prior_state in {"stronger_confirmed", "confirmed_leader"}:
            if recognition_rank is None:
                prior_confirmation = None
            else:
                prior_confirmation = recognition_rank == 1
        elif prior_state is None:
            prior_confirmation = None
        else:
            prior_confirmation = False
        return _tri_or(explicit, stronger, prior_confirmation)

    @staticmethod
    def _acceleration_to_divergence(
        raw: Mapping[str, Any],
        confirmed_leader: Optional[bool],
    ) -> Optional[bool]:
        if confirmed_leader is False:
            return False
        if confirmed_leader is None:
            return None
        accelerated = _tri_or(
            _first_flag(raw, ("prior_accelerated",)),
            _first_flag(raw, ("accelerated",)),
        )
        divergence = _tri_or(
            _fact_flag(raw, ("opened", "opened_board")),
            _fact_flag(raw, ("review_today_broken", "today_broken")),
            _fact_flag(raw, ("high_turnover_divergence",)),
        )
        return _tri_and(accelerated, divergence)

    @staticmethod
    def _low_position_new_start(
        raw: Mapping[str, Any],
        board_height: Optional[int],
    ) -> Optional[bool]:
        if board_height is not None and board_height > 1:
            return False
        if board_height is None:
            return None
        new_start = _first_flag(raw, ("new_start", "low_position_start"))
        if new_start is not None:
            return new_start
        yesterday = _first_flag(raw, ("review_yesterday_limit_up",))
        if yesterday is None:
            yesterday_height = _first_number(
                raw,
                ("review_yesterday_continuous_days",),
                minimum=0,
                maximum=100,
                integer=True,
            )
            if yesterday_height is not None:
                yesterday = yesterday_height > 0
        sealed = ModeFeatureBuilder._sealed(raw)
        if yesterday is None or sealed is None:
            return None
        return not yesterday and sealed

    @staticmethod
    def _supplement(
        snapshot: MarketSnapshot,
        raw: Mapping[str, Any],
        low_position_new_start: Optional[bool],
    ) -> Optional[bool]:
        window = snapshot.market_features.get("window")
        if window not in {"first_divergence", "stage_three"}:
            return False if isinstance(window, str) else None
        after_leader = _first_flag(
            raw,
            ("started_after_leader", "supplement_at_turn"),
        )
        return ModeFeatureBuilder._all_flags(low_position_new_start, after_leader)

    @staticmethod
    def _first_bearish(
        raw: Mapping[str, Any],
        confirmed_leader: Optional[bool],
    ) -> Optional[bool]:
        if confirmed_leader is False:
            return False
        if confirmed_leader is None:
            return None
        explicit = _first_flag(raw, ("first_bearish_signal",))
        if explicit is not None:
            return explicit
        days = _finite_number(
            raw.get("bearish_days_since_confirm"),
            minimum=0,
            maximum=100,
            integer=True,
        )
        return days == 1 if days is not None else None

    @staticmethod
    def _pullback(
        raw: Mapping[str, Any],
        reference_price: Optional[float],
        support_price: Optional[float],
        support_quality: str,
        trend_established: Optional[bool],
    ) -> Optional[bool]:
        if trend_established is False:
            return False
        if trend_established is None:
            return None
        explicit = _first_flag(raw, ("pullback_confirmed",))
        if explicit is False:
            return False
        if support_quality != "ready":
            return None
        prior_high = _first_number(
            raw,
            ("prior_n_day_high", "n_day_high_price", "prior_high"),
            minimum=0.0001,
        )
        if reference_price is None or support_price is None or prior_high is None:
            return None
        computed = support_price <= reference_price < prior_high
        return _tri_and(explicit, computed) if explicit is not None else computed

    @classmethod
    def _linkage_confirmed(
        cls,
        snapshot: MarketSnapshot,
        candidate: CandidateSnapshot,
        raw: Mapping[str, Any],
    ) -> Optional[bool]:
        explicit = _first_flag(raw, ("linkage_confirmed", "theme_linkage"))
        if explicit is not None:
            return explicit
        theme = cls._theme_row(snapshot, candidate)
        if theme is None:
            return None
        new_highs = _finite_number(theme.get("new_high_count"), minimum=0)
        middle_strength = _finite_number(
            theme.get("middle_army_strength"),
            minimum=0,
        )
        if new_highs is None or middle_strength is None:
            return None
        return new_highs > 0 and middle_strength > 0

    @staticmethod
    def _middle_army_linkage(
        raw: Mapping[str, Any],
        peers: _PeerSet,
    ) -> Optional[bool]:
        explicit = _first_flag(raw, ("middle_army_linkage",))
        if explicit is not None:
            return explicit
        states: list[Optional[bool]] = [None] * peers.unknown_count
        for peer in peers.ready:
            middle = _first_flag(peer.features, ("middle_army_fact",))
            trend = _first_flag(
                peer.features,
                ("positive_trend", "trend_established"),
            )
            state = _tri_and(middle, trend)
            if state is True:
                return True
            states.append(state)
        return _tri_or(*states)

    @staticmethod
    def _consolidation_in_range(raw: Mapping[str, Any]) -> Optional[bool]:
        days = _finite_number(
            raw.get("consolidation_days"),
            minimum=0,
            maximum=100,
            integer=True,
        )
        if days is None:
            return None
        return 3 <= days <= 10

    @staticmethod
    def _consolidation_rebreak(
        raw: Mapping[str, Any],
        trend_established: Optional[bool],
    ) -> Optional[bool]:
        n_day_high = (
            _strict_flag(raw.get("n_day_high"))
            if raw.get("kline_quality") == "ready"
            else None
        )
        return ModeFeatureBuilder._all_flags(
            trend_established,
            ModeFeatureBuilder._consolidation_in_range(raw),
            n_day_high,
        )

    @staticmethod
    def _peer_alive_state(peer: CandidateSnapshot) -> Optional[bool]:
        sealed = ModeFeatureBuilder._sealed(peer.features)
        supplement = _first_flag(peer.features, ("supplement", "is_supplement"))
        middle = _first_flag(peer.features, ("middle_army", "middle_army_fact"))
        positive = _first_flag(
            peer.features,
            ("positive_trend", "trend_established", "n_day_high"),
        )
        return _tri_or(
            _tri_and(sealed, supplement),
            _tri_and(middle, positive),
        )

    @classmethod
    def _theme_alive(cls, peers: _PeerSet) -> Optional[bool]:
        states = [None] * peers.unknown_count
        states.extend(cls._peer_alive_state(peer) for peer in peers.ready)
        return _tri_or(*states)

    @classmethod
    def _theme_dead(
        cls,
        candidate: CandidateSnapshot,
        raw: Mapping[str, Any],
        peers: _PeerSet,
    ) -> Optional[bool]:
        peer_states: list[Optional[bool]] = [None] * peers.unknown_count
        for peer in peers.ready:
            if peer.stock_code == candidate.stock_code:
                continue
            sealed = cls._sealed(peer.features)
            new_high = _first_flag(peer.features, ("n_day_high", "new_high"))
            positive_peer = _tri_or(sealed, new_high)
            if positive_peer is True:
                return False
            peer_states.append(positive_peer)
        negative_days = _finite_number(
            raw.get("theme_breadth_negative_days"),
            minimum=0,
            maximum=100,
            integer=True,
        )
        if negative_days is not None and negative_days < 2:
            return False
        if negative_days is None:
            return None
        if peer_states and _tri_or(*peer_states) is None:
            return None
        return True

    @classmethod
    def _external_switch(
        cls,
        snapshot: MarketSnapshot,
        candidate: CandidateSnapshot,
        raw: Mapping[str, Any],
        theme_rank: Optional[int],
    ) -> Optional[bool]:
        external = _first_flag(raw, ("is_external_theme", "external_theme"))
        if external is False or (theme_rank is not None and theme_rank > 2):
            return False
        theme = cls._theme_row(snapshot, candidate)
        expanding = _first_flag(raw, ("theme_expanding",))
        if expanding is None and theme is not None:
            limit_up_count = _finite_number(theme.get("limit_up_count"), minimum=0)
            expanding = limit_up_count > 1 if limit_up_count is not None else None
        return cls._all_flags(
            external,
            theme_rank is not None and theme_rank <= 2 if theme_rank is not None else None,
            expanding,
        )

    def _tail_action_eligible(
        self,
        snapshot: MarketSnapshot,
        candidate: CandidateSnapshot,
        raw: Mapping[str, Any],
    ) -> Optional[bool]:
        if snapshot.stage != "preclose":
            return False
        if raw.get("automation_level") == "manual_only":
            return False
        entry = _first_flag(raw, ("tail_entry_satisfied",))
        invalidated = _first_flag(raw, ("tail_invalidation_satisfied",))
        if invalidated is True:
            return False
        captured_at = _parse_datetime(raw.get("captured_at"))
        quote_ready = self._has_ready_evidence(
            snapshot,
            candidate.evidence,
            allowed_sources={"tencent", "quote", "market_quote"},
        )
        if captured_at is None or not quote_ready:
            fresh = None
        else:
            age = (
                snapshot.as_of - _comparable_datetime(captured_at, snapshot.as_of)
            ).total_seconds()
            fresh = 0 <= age <= self.quote_fresh_seconds
        return self._all_flags(
            snapshot.quality.status == "ready" and not snapshot.quality.stale,
            fresh,
            entry,
            not invalidated if invalidated is not None else None,
        )

    @staticmethod
    def _all_flags(*values: Optional[bool]) -> Optional[bool]:
        return _tri_and(*values)

    @staticmethod
    def _feature_quality(
        key: str,
        raw: Mapping[str, Any],
        value: Any,
    ) -> str:
        trust_quality = raw.get("_trust_quality")
        source_state = (
            trust_quality.get(key)
            if isinstance(trust_quality, Mapping)
            else None
        )
        if value is None:
            return (
                "degraded"
                if source_state in {"degraded", "invalid"}
                else "missing"
            )
        if key in {"theme_rank"} and raw.get("theme_quality") not in {
            None,
            "ready",
        }:
            return "degraded"
        if key in {"recognition_rank", "resilience_rank"} and raw.get(
            "recognition_quality"
        ) not in {None, "ready"}:
            return "degraded"
        if key in {
            "trend_established",
            "pullback",
            "consolidation_rebreak",
            "trend_turn_two",
        } and raw.get("kline_quality") != "ready":
            return "missing"
        return "ready"


__all__ = ["FEATURE_KEYS", "ModeFeatureBuilder"]
