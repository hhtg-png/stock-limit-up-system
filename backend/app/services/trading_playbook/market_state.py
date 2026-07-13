"""Deterministic market-state classification and relative ranking."""

from __future__ import annotations

import math
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple


def _finite_number(value: Any) -> Optional[float]:
    """Return a finite float without treating booleans as market evidence."""
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _number_or_zero(value: Any) -> float:
    number = _finite_number(value)
    return number if number is not None else 0.0


def _flag(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    number = _finite_number(value)
    return number is not None and number != 0


class MarketStateClassifier:
    """Classify the dominant market style and the ordered cycle window."""

    def classify(self, features: Mapping[str, Any]) -> Dict[str, Any]:
        limit_up_growth = self._growth(
            features,
            explicit_key="limit_up_growth",
            current_key="limit_up_count",
            previous_key="limit_up_count_prev",
        )
        trend_growth = self._growth(
            features,
            explicit_key="trend_growth",
            current_key="trend_new_high_count",
            previous_key="trend_new_high_count_prev",
        )

        limit_down_count = _finite_number(features.get("limit_down_count"))
        seal_rate = _finite_number(features.get("seal_rate"))
        max_board_height = _finite_number(features.get("max_board_height"))
        limit_up_count = _finite_number(features.get("limit_up_count"))
        trend_new_high_count = _finite_number(
            features.get("trend_new_high_count")
        )

        if (
            limit_down_count is not None
            and limit_down_count >= 10
        ) or (seal_rate is not None and seal_rate < 50):
            style = "chaos_retreat"
        elif (
            max_board_height is not None
            and max_board_height >= 4
            and limit_up_count is not None
            and limit_up_count >= 50
            and limit_up_growth > trend_growth
        ):
            style = "board_flow"
        elif (
            trend_new_high_count is not None
            and trend_new_high_count >= 20
            and trend_growth >= limit_up_growth
        ):
            style = "trend_main_wave"
        else:
            style = "dual_active"

        divergence_days = _finite_number(features.get("divergence_days"))
        prior_window = str(features.get("prior_window") or "")
        if _flag(features.get("negative_feedback")):
            window = "decline"
        elif (
            limit_up_growth >= 0.35
            and seal_rate is not None
            and seal_rate >= 65
        ):
            window = "outbreak"
        elif (
            divergence_days is not None
            and divergence_days >= 3
            and _flag(features.get("sell_pressure_falling"))
        ):
            window = "divergence_exhaustion"
        elif (
            prior_window in {"first_divergence", "divergence_exhaustion"}
            and _flag(features.get("breadth_recovered"))
        ):
            window = "divergence_to_consensus"
        elif (
            prior_window == "divergence_to_consensus"
            and _flag(features.get("breadth_recovered"))
        ):
            window = "stronger_confirmation"
        elif (
            prior_window == "stronger_confirmation"
            and _flag(features.get("sell_pressure_rising"))
        ):
            window = "second_divergence"
        else:
            window = "first_divergence"

        return {
            "style": style,
            "window": window,
            "limit_up_growth": limit_up_growth,
            "trend_growth": trend_growth,
        }

    @staticmethod
    def _growth(
        features: Mapping[str, Any],
        *,
        explicit_key: str,
        current_key: str,
        previous_key: str,
    ) -> float:
        explicit = _finite_number(features.get(explicit_key))
        if explicit is not None:
            return explicit

        current = _finite_number(features.get(current_key))
        previous = _finite_number(features.get(previous_key))
        if current is None or previous is None:
            return 0.0
        if previous == 0:
            if current > 0:
                return 1.0
            if current < 0:
                return -1.0
            return 0.0
        return (current - previous) / previous


class ThemeRanker:
    """Rank themes using the version-one transcript-derived score."""

    def rank(self, rows: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
        ranked: List[Dict[str, Any]] = []
        for source in rows:
            row = dict(source)
            row["theme_name"] = str(source.get("theme_name") or "")
            row["score"] = (
                _number_or_zero(source.get("limit_up_count")) * 5
                + _number_or_zero(source.get("new_high_count")) * 3
                + _number_or_zero(source.get("sealed_count")) * 2
                - _number_or_zero(source.get("broken_count")) * 3
                + _number_or_zero(source.get("middle_army_strength"))
            )
            ranked.append(row)

        ranked.sort(key=lambda item: (-item["score"], item["theme_name"]))
        for rank, row in enumerate(ranked, start=1):
            row["rank"] = rank
        return ranked


class RecognitionRanker:
    """Rank recognition relatively across five transcript-defined dimensions."""

    _DIMENSIONS: Tuple[Tuple[str, str, str, bool], ...] = (
        ("fastest", "first_limit_seconds", "fastest_rank", False),
        ("highest", "board_height", "highest_rank", True),
        ("hardest", "seal_strength", "hardest_rank", True),
        ("resilient", "resilience", "resilience_rank", True),
        ("influential", "influence", "influence_rank", True),
    )

    def rank(self, rows: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
        records = [dict(source) for source in rows]
        for record in records:
            record["stock_code"] = str(record.get("stock_code") or "")
            record["recognition_evidence"] = {}

        # Canonicalize before every relative comparison so caller order cannot
        # become an implicit tie-break.
        records.sort(key=lambda item: item["stock_code"])
        for dimension, field, rank_key, higher_is_better in self._DIMENSIONS:
            ordered = sorted(
                records,
                key=lambda item: self._dimension_sort_key(
                    item,
                    field=field,
                    higher_is_better=higher_is_better,
                ),
            )
            for rank, record in enumerate(ordered, start=1):
                record[rank_key] = rank
                record["recognition_evidence"][dimension] = {
                    "field": field,
                    "value": record.get(field),
                    "rank": rank,
                }

        for record in records:
            record["recognition_score"] = round(
                sum(
                    1.0 / record[rank_key]
                    for _, _, rank_key, _ in self._DIMENSIONS
                ),
                8,
            )

        records.sort(
            key=lambda item: (-item["recognition_score"], item["stock_code"])
        )
        for rank, record in enumerate(records, start=1):
            record["recognition_rank"] = rank
        return records

    @staticmethod
    def _dimension_sort_key(
        row: Mapping[str, Any],
        *,
        field: str,
        higher_is_better: bool,
    ) -> Tuple[bool, float, str]:
        value = _finite_number(row.get(field))
        missing = value is None
        ordered_value = 0.0 if missing else value
        if higher_is_better:
            ordered_value = -ordered_value
        return missing, ordered_value, str(row.get("stock_code") or "")


__all__ = ["MarketStateClassifier", "ThemeRanker", "RecognitionRanker"]
