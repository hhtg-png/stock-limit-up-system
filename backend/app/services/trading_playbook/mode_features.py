"""Conservative point-in-time features for transcript-derived trading modes."""

from __future__ import annotations

import copy
import math
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
    for name in names:
        if name in source:
            value = _strict_flag(source.get(name))
            if value is not None:
                return value
    return None


def _fact_sources(source: Mapping[str, Any]) -> Tuple[Mapping[str, Any], ...]:
    nested = []
    for key in ("realtime_limit_up_fact", "latest_review_fact"):
        value = source.get(key)
        if isinstance(value, Mapping):
            nested.append(value)
    return (source, *nested)


def _fact_flag(source: Mapping[str, Any], names: Sequence[str]) -> Optional[bool]:
    for facts in _fact_sources(source):
        value = _first_flag(facts, names)
        if value is not None:
            return value
    return None


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
        raw = copy.deepcopy(candidate.features)
        result: Dict[str, Any] = {key: None for key in FEATURE_KEYS}
        quality: Dict[str, str] = {}

        point_in_time_valid = self._point_in_time_valid(snapshot, candidate, raw)
        result.update(
            {
                "_snapshot_quality_status": snapshot.quality.status,
                "_snapshot_stale": bool(snapshot.quality.stale),
                "_point_in_time_valid": point_in_time_valid,
                "_stage": snapshot.stage,
            }
        )

        if point_in_time_valid:
            reference_price = self._reference_price(snapshot, candidate, raw)
            result["reference_price"] = reference_price
            result.update(self._planned_prices(raw, reference_price))
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

            theme_peers = self._theme_peers(snapshot, candidate)
            result["high_position"] = self._high_position(
                candidate,
                theme_peers,
            )
            result["same_level_turnover"] = self._same_level_turnover(
                candidate,
                theme_peers,
            )
            result["middle_army"] = self._middle_army(
                candidate,
                theme_peers,
            )
            result["started_before_theme"] = self._started_before_theme(
                snapshot,
                candidate,
            )
            result["unique_survivor"] = self._unique_survivor(
                candidate,
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
                result["trend_established"],
            )
            result["linkage_confirmed"] = self._linkage_confirmed(
                snapshot,
                candidate,
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
            result["theme_dead"] = self._theme_dead(raw, theme_peers)
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
                candidate,
                raw,
                result["theme_rank"],
            )
            result["tail_action_eligible"] = self._tail_action_eligible(
                snapshot,
                candidate,
                raw,
            )

        for key in FEATURE_KEYS:
            quality[key] = self._feature_quality(key, raw, result.get(key))
        if not point_in_time_valid:
            for key in FEATURE_KEYS:
                result[key] = None
                quality[key] = "missing"
        result["_feature_quality"] = quality
        return result

    @staticmethod
    def _theme_peers(
        snapshot: MarketSnapshot,
        candidate: CandidateSnapshot,
    ) -> Tuple[CandidateSnapshot, ...]:
        theme = str(candidate.theme_name or "").strip()
        if not theme:
            return (candidate,)
        return tuple(
            peer
            for peer in snapshot.candidates
            if str(peer.theme_name or "").strip() == theme
        )

    @staticmethod
    def _point_in_time_valid(
        snapshot: MarketSnapshot,
        candidate: CandidateSnapshot,
        raw: Mapping[str, Any],
    ) -> bool:
        times = [_parse_datetime(raw.get("captured_at"))]
        times.extend(
            _parse_datetime(item.get("as_of"))
            for item in candidate.evidence
            if isinstance(item, Mapping)
        )
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
        return True

    @staticmethod
    def _reference_price(
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
        quote_evidence = [
            item
            for item in candidate.evidence
            if isinstance(item, Mapping)
            and item.get("source") in {"tencent", "quote", "market_quote"}
        ]
        if quote_evidence and not any(
            item.get("quality") in {"ready", "computed"}
            for item in quote_evidence
        ):
            return None
        return price

    def _planned_prices(
        self,
        raw: Mapping[str, Any],
        reference_price: Optional[float],
    ) -> Dict[str, Optional[float]]:
        if reference_price is None:
            return {
                "planned_pullback_price": None,
                "planned_breakout_price": None,
                "hard_stop_price": None,
                "exit_change_pct_floor": None,
            }
        supports = [
            _finite_number(raw.get(key), minimum=0.0001, maximum=reference_price)
            for key in (
                "validated_support",
                "support_price",
                "five_day_low",
                "five_day_low_price",
            )
        ]
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
        peers: Iterable[CandidateSnapshot],
    ) -> Optional[bool]:
        candidate_height = cls._board_height(candidate.features)
        if candidate_height is not None and candidate_height < 2:
            return False
        heights = [cls._board_height(peer.features) for peer in peers]
        if candidate_height is None or any(value is None for value in heights):
            return None
        return candidate_height >= 2 and candidate_height == max(heights)

    @classmethod
    def _same_level_turnover(
        cls,
        candidate: CandidateSnapshot,
        peers: Iterable[CandidateSnapshot],
    ) -> Optional[bool]:
        peer_rows = tuple(peers)
        height = cls._board_height(candidate.features)
        heights = [cls._board_height(peer.features) for peer in peer_rows]
        if height is None or any(value is None for value in heights):
            return None
        maximum = max(heights)
        if height != maximum or sum(value == height for value in heights) < 2:
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
        peers: Iterable[CandidateSnapshot],
    ) -> Optional[bool]:
        peer_rows = tuple(peers)
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
                return row
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
        peers: Iterable[CandidateSnapshot],
        recognition_rank: Optional[int],
    ) -> Optional[bool]:
        if recognition_rank is not None and recognition_rank != 1:
            return False
        if recognition_rank is None:
            return None
        former_peers = [
            peer
            for peer in peers
            if peer.stock_code != candidate.stock_code
            and (
                _first_flag(peer.features, ("former_high_position",)) is True
                or (
                    _first_number(
                        peer.features,
                        ("review_yesterday_continuous_days",),
                        minimum=2,
                        maximum=100,
                        integer=True,
                    )
                    is not None
                )
                or (cls._board_height(peer.features) or 0) >= 2
            )
        ]
        if not former_peers:
            return None
        states = []
        for peer in former_peers:
            broken = _first_flag(
                peer.features,
                ("review_today_broken", "today_broken", "broken"),
            )
            opened = _first_flag(peer.features, ("opened", "opened_board"))
            trend_broken = _first_flag(peer.features, ("trend_broken",))
            flags = (broken, opened, trend_broken)
            if any(value is True for value in flags):
                states.append(True)
            elif all(value is False for value in flags):
                states.append(False)
            else:
                states.append(None)
        if any(value is False for value in states):
            return False
        if any(value is None for value in states):
            return None
        return True

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
            if role == "confirmed_leader" or mode == "leader_stronger_confirmation":
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
        resealed = _fact_flag(
            raw,
            ("resealed", "is_resealed", "reversal_limit_up"),
        )
        breakout = _fact_flag(raw, ("right_side_breakout",))
        influence_rank = ModeFeatureBuilder._rank(raw.get("influence_rank"))
        influenced_breakout = (
            breakout and influence_rank is not None and influence_rank <= 3
            if breakout is not None
            else None
        )
        states = (resealed, influenced_breakout)
        if any(value is True for value in states):
            return True
        if all(value is False for value in states):
            return False
        return None

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
        if pre_close is None or (open_price is None and price is None):
            strength = None
        else:
            strength = any(
                value is not None and value > pre_close
                for value in (open_price, price)
            )
        new_leader = _first_flag(raw, ("new_recognition_leader",))
        if new_leader is not None:
            no_new_leader = not new_leader
        elif recognition_rank is not None:
            no_new_leader = recognition_rank == 1
        else:
            no_new_leader = None
        return ModeFeatureBuilder._all_flags(strength, no_new_leader)

    @staticmethod
    def _confirmed_leader(
        raw: Mapping[str, Any],
        prior_state: Optional[str],
        stronger: Optional[bool],
        recognition_rank: Optional[int],
    ) -> Optional[bool]:
        explicit = _first_flag(raw, ("confirmed_leader_fact",))
        if explicit is not None:
            return explicit
        if stronger is True:
            return True
        if prior_state in {"stronger_confirmed", "confirmed_leader"}:
            if recognition_rank is None:
                return None
            return recognition_rank == 1
        if stronger is False:
            return False
        return None

    @staticmethod
    def _acceleration_to_divergence(
        raw: Mapping[str, Any],
        confirmed_leader: Optional[bool],
    ) -> Optional[bool]:
        if confirmed_leader is False:
            return False
        if confirmed_leader is None:
            return None
        accelerated = _first_flag(raw, ("prior_accelerated", "accelerated"))
        divergence = _first_flag(
            raw,
            ("opened", "review_today_broken", "high_turnover_divergence"),
        )
        return ModeFeatureBuilder._all_flags(accelerated, divergence)

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
        trend_established: Optional[bool],
    ) -> Optional[bool]:
        if trend_established is False:
            return False
        if trend_established is None:
            return None
        explicit = _first_flag(raw, ("pullback_confirmed",))
        if explicit is not None:
            return explicit
        prior_high = _first_number(
            raw,
            ("prior_n_day_high", "n_day_high_price", "prior_high"),
            minimum=0.0001,
        )
        if reference_price is None or support_price is None or prior_high is None:
            return None
        return support_price <= reference_price < prior_high

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
        peers: Iterable[CandidateSnapshot],
    ) -> Optional[bool]:
        explicit = _first_flag(raw, ("middle_army_linkage",))
        if explicit is not None:
            return explicit
        states = []
        for peer in peers:
            middle = _first_flag(peer.features, ("middle_army_fact",))
            trend = _first_flag(
                peer.features,
                ("positive_trend", "trend_established"),
            )
            if middle is True and trend is True:
                return True
            if middle is False or trend is False:
                states.append(False)
            else:
                states.append(None)
        return False if states and all(value is False for value in states) else None

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
        if sealed is True and supplement is True:
            return True
        middle = _first_flag(peer.features, ("middle_army", "middle_army_fact"))
        positive = _first_flag(
            peer.features,
            ("positive_trend", "trend_established", "n_day_high"),
        )
        if middle is True and positive is True:
            return True
        supplement_branch = False if sealed is False or supplement is False else None
        middle_branch = False if middle is False or positive is False else None
        if supplement_branch is False and middle_branch is False:
            return False
        return None

    @classmethod
    def _theme_alive(cls, peers: Iterable[CandidateSnapshot]) -> Optional[bool]:
        states = [cls._peer_alive_state(peer) for peer in peers]
        if any(value is True for value in states):
            return True
        if states and all(value is False for value in states):
            return False
        return None

    @classmethod
    def _theme_dead(
        cls,
        raw: Mapping[str, Any],
        peers: Iterable[CandidateSnapshot],
    ) -> Optional[bool]:
        peer_states = []
        for peer in peers:
            sealed = cls._sealed(peer.features)
            new_high = _first_flag(peer.features, ("n_day_high", "new_high"))
            if sealed is True or new_high is True:
                return False
            if sealed is False and new_high is False:
                peer_states.append(False)
            else:
                peer_states.append(None)
        negative_days = _finite_number(
            raw.get("theme_breadth_negative_days"),
            minimum=0,
            maximum=100,
            integer=True,
        )
        if negative_days is not None and negative_days < 2:
            return False
        if negative_days is None or any(value is None for value in peer_states):
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
        quote_ready = any(
            isinstance(item, Mapping)
            and item.get("source") in {"tencent", "quote", "market_quote"}
            and item.get("quality") in {"ready", "computed"}
            for item in candidate.evidence
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
        if any(value is False for value in values):
            return False
        if all(value is True for value in values):
            return True
        return None

    @staticmethod
    def _feature_quality(
        key: str,
        raw: Mapping[str, Any],
        value: Any,
    ) -> str:
        if value is None:
            return "missing"
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
