"""Deterministic market-state classification and relative ranking."""

from __future__ import annotations

import copy
import math
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from .domain import CandidateSnapshot, DataQuality, MarketSnapshot


def _finite_number(value: Any) -> Optional[float]:
    """Return a finite float without treating booleans as market evidence."""
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _bounded_number(
    value: Any,
    *,
    minimum: Optional[float] = None,
    maximum: Optional[float] = None,
) -> Optional[float]:
    number = _finite_number(value)
    if number is None:
        return None
    if minimum is not None and number < minimum:
        return None
    if maximum is not None and number > maximum:
        return None
    return number


def _strict_flag(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true"}:
            return True
        if normalized in {"0", "false"}:
            return False
        return None
    number = _finite_number(value)
    if number == 1:
        return True
    if number == 0:
        return False
    return None


class MarketStateClassifier:
    """Classify the dominant market style and the ordered cycle window."""

    _PRIOR_WINDOWS = {
        "",
        "outbreak",
        "first_divergence",
        "divergence_exhaustion",
        "divergence_to_consensus",
        "stronger_confirmation",
        "second_divergence",
        "stage_three",
        "decline",
        "unknown",
    }

    def classify(self, features: Mapping[str, Any]) -> Dict[str, Any]:
        missing_fields = set()

        def read_number(
            key: str,
            *,
            maximum: Optional[float] = None,
        ) -> Optional[float]:
            value = _bounded_number(
                features.get(key),
                minimum=0,
                maximum=maximum,
            )
            if value is None:
                missing_fields.add(key)
            return value

        limit_up_count = read_number("limit_up_count")
        trend_new_high_count = read_number("trend_new_high_count")
        limit_up_growth = self._growth(
            features,
            explicit_key="limit_up_growth",
            current_key="limit_up_count",
            previous_key="limit_up_count_prev",
            missing_fields=missing_fields,
        )
        trend_growth = self._growth(
            features,
            explicit_key="trend_growth",
            current_key="trend_new_high_count",
            previous_key="trend_new_high_count_prev",
            missing_fields=missing_fields,
        )

        limit_down_count = read_number("limit_down_count")
        seal_rate = read_number("seal_rate", maximum=100)
        max_board_height = read_number("max_board_height")
        divergence_days = read_number("divergence_days")

        if (
            limit_down_count is not None
            and limit_down_count >= 10
        ) or (seal_rate is not None and seal_rate < 50):
            style = "chaos_retreat"
        elif any(
            value is None
            for value in (
                limit_down_count,
                seal_rate,
                max_board_height,
                limit_up_count,
                trend_new_high_count,
                limit_up_growth,
                trend_growth,
            )
        ):
            style = "unknown"
        elif (
            max_board_height >= 4
            and limit_up_count >= 50
            and limit_up_growth > trend_growth
        ):
            style = "board_flow"
        elif (
            trend_new_high_count >= 20
            and trend_growth >= limit_up_growth
        ):
            style = "trend_main_wave"
        else:
            style = "dual_active"

        negative_feedback = self._optional_flag(
            features,
            "negative_feedback",
            missing_fields,
        )
        sell_pressure_falling = self._optional_flag(
            features,
            "sell_pressure_falling",
            missing_fields,
        )
        breadth_recovered = self._optional_flag(
            features,
            "breadth_recovered",
            missing_fields,
        )
        sell_pressure_rising = self._optional_flag(
            features,
            "sell_pressure_rising",
            missing_fields,
        )
        prior_window = self._prior_window(features, missing_fields)

        if negative_feedback is True:
            window = "decline"
        elif negative_feedback is None:
            window = "unknown"
        elif limit_up_growth is None or seal_rate is None:
            window = "unknown"
        elif (
            limit_up_growth >= 0.35
            and seal_rate >= 65
        ):
            window = "outbreak"
        elif divergence_days is None:
            window = "unknown"
        elif (
            divergence_days >= 3
            and sell_pressure_falling is True
        ):
            window = "divergence_exhaustion"
        elif divergence_days >= 3 and sell_pressure_falling is None:
            window = "unknown"
        elif (
            prior_window in {"first_divergence", "divergence_exhaustion"}
            and breadth_recovered is True
        ):
            window = "divergence_to_consensus"
        elif (
            prior_window in {"first_divergence", "divergence_exhaustion"}
            and breadth_recovered is None
        ):
            window = "unknown"
        elif (
            prior_window == "divergence_to_consensus"
            and breadth_recovered is True
        ):
            window = "stronger_confirmation"
        elif (
            prior_window == "divergence_to_consensus"
            and breadth_recovered is None
        ):
            window = "unknown"
        elif (
            prior_window == "stronger_confirmation"
            and sell_pressure_rising is True
        ):
            window = "second_divergence"
        elif (
            prior_window in {None, "unknown"}
            or (
                prior_window == "stronger_confirmation"
                and sell_pressure_rising is None
            )
        ):
            window = "unknown"
        else:
            window = "first_divergence"

        return {
            "style": style,
            "window": window,
            "limit_up_growth": limit_up_growth,
            "trend_growth": trend_growth,
            "quality": "degraded" if missing_fields else "ready",
            "missing_fields": sorted(missing_fields),
            "_feature_quality": {
                "style": "ready" if style != "unknown" else "missing",
                "window": "ready" if window != "unknown" else "missing",
            },
        }

    @staticmethod
    def _growth(
        features: Mapping[str, Any],
        *,
        explicit_key: str,
        current_key: str,
        previous_key: str,
        missing_fields: set,
    ) -> Optional[float]:
        explicit = _finite_number(features.get(explicit_key))
        if explicit is not None:
            return explicit

        current = _bounded_number(features.get(current_key), minimum=0)
        previous = _bounded_number(features.get(previous_key), minimum=0)
        if current is None:
            missing_fields.add(current_key)
        if previous is None:
            missing_fields.add(previous_key)
        if current is None or previous is None or previous == 0:
            missing_fields.add(explicit_key)
            return None
        return (current - previous) / previous

    @staticmethod
    def _optional_flag(
        features: Mapping[str, Any],
        key: str,
        missing_fields: set,
    ) -> Optional[bool]:
        if key not in features:
            return False
        value = _strict_flag(features.get(key))
        if value is None:
            missing_fields.add(key)
        return value

    @classmethod
    def _prior_window(
        cls,
        features: Mapping[str, Any],
        missing_fields: set,
    ) -> Optional[str]:
        if "prior_window" not in features:
            return ""
        value = features.get("prior_window")
        if not isinstance(value, str):
            missing_fields.add("prior_window")
            return None
        normalized = value.strip()
        if normalized not in cls._PRIOR_WINDOWS:
            missing_fields.add("prior_window")
            return None
        if normalized == "unknown":
            missing_fields.add("prior_window")
        return normalized


class ThemeRanker:
    """Rank themes using the version-one transcript-derived score."""

    _FIELDS = (
        "limit_up_count",
        "new_high_count",
        "sealed_count",
        "broken_count",
        "middle_army_strength",
    )

    def rank(self, rows: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
        ranked: List[Dict[str, Any]] = []
        seen_names = set()
        for source in rows:
            row = dict(source)
            theme_name = str(source.get("theme_name") or "").strip()
            if theme_name in seen_names:
                raise ValueError(f"duplicate theme_name: {theme_name}")
            seen_names.add(theme_name)
            row["theme_name"] = theme_name

            evidence = {
                field: source.get(field)
                for field in self._FIELDS
            }
            values = {
                field: _bounded_number(source.get(field), minimum=0)
                for field in self._FIELDS
            }
            missing_fields = [
                field for field, value in values.items() if value is None
            ]
            if not theme_name:
                missing_fields.append("theme_name")
            missing_fields.sort()
            row["theme_evidence"] = evidence
            row["missing_fields"] = missing_fields
            row["quality"] = "degraded" if missing_fields else "ready"
            row["score"] = (
                values["limit_up_count"] * 5
                + values["new_high_count"] * 3
                + values["sealed_count"] * 2
                - values["broken_count"] * 3
                + values["middle_army_strength"]
            ) if not missing_fields else None
            ranked.append(row)

        ranked.sort(
            key=lambda item: (
                item["score"] is None,
                -(item["score"] or 0.0),
                item["theme_name"],
            )
        )
        rank = 0
        for row in ranked:
            if row["score"] is None:
                row["rank"] = None
            else:
                rank += 1
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
        records: List[Dict[str, Any]] = []
        seen_codes = set()
        for source in rows:
            record = dict(source)
            stock_code = str(source.get("stock_code") or "").strip()
            if stock_code in seen_codes:
                raise ValueError(f"duplicate stock_code: {stock_code}")
            seen_codes.add(stock_code)
            record["stock_code"] = stock_code
            normalized = {
                field: self._dimension_value(field, source.get(field))
                for _, field, _, _ in self._DIMENSIONS
            }
            missing_fields = [
                field for field, value in normalized.items() if value is None
            ]
            if not stock_code:
                missing_fields.append("stock_code")
            record["_normalized"] = normalized
            record["missing_fields"] = sorted(missing_fields)
            record["quality"] = "degraded" if missing_fields else "ready"
            record["recognition_evidence"] = {}
            for _, _, rank_key, _ in self._DIMENSIONS:
                record[rank_key] = None
            records.append(record)

        # Canonicalize before every relative comparison so caller order cannot
        # become an implicit tie-break.
        records.sort(key=lambda item: item["stock_code"])
        for dimension, field, rank_key, higher_is_better in self._DIMENSIONS:
            ordered = sorted(
                (
                    record
                    for record in records
                    if record["stock_code"]
                    and record["_normalized"][field] is not None
                ),
                key=lambda item: (
                    -item["_normalized"][field]
                    if higher_is_better
                    else item["_normalized"][field],
                    item["stock_code"],
                ),
            )
            rank = 0
            previous_value: Any = object()
            for record in ordered:
                value = record["_normalized"][field]
                if value != previous_value:
                    rank += 1
                    previous_value = value
                record[rank_key] = rank

            for record in records:
                record["recognition_evidence"][dimension] = {
                    "field": field,
                    "value": record.get(field),
                    "rank": record[rank_key],
                }

        for record in records:
            ranks = [
                record[rank_key]
                for _, _, rank_key, _ in self._DIMENSIONS
                if record[rank_key] is not None
            ]
            record["recognition_score"] = (
                round(sum(1.0 / rank for rank in ranks), 8)
                if record["stock_code"] and ranks
                else None
            )

        records.sort(
            key=lambda item: (
                item["recognition_score"] is None,
                -(item["recognition_score"] or 0.0),
                item["stock_code"],
            )
        )
        rank = 0
        previous_score: Any = object()
        for record in records:
            score = record["recognition_score"]
            if score is None:
                record["recognition_rank"] = None
            else:
                if score != previous_score:
                    rank += 1
                    previous_score = score
                record["recognition_rank"] = rank
            record.pop("_normalized")
        return records

    @staticmethod
    def _dimension_value(field: str, value: Any) -> Optional[float]:
        if field == "first_limit_seconds":
            return _bounded_number(value, minimum=33900, maximum=54000)
        return _bounded_number(value, minimum=0)


class MarketStateAnalyzer:
    """Enrich a Task 3 snapshot without mutating its point-in-time evidence."""

    _MAX_QUALITY_WARNINGS = 50

    _RECOGNITION_FIELDS = tuple(
        field for _, field, _, _ in RecognitionRanker._DIMENSIONS
    )
    _RECOGNITION_OUTPUT_FIELDS = (
        "fastest_rank",
        "highest_rank",
        "hardest_rank",
        "resilience_rank",
        "influence_rank",
        "recognition_score",
        "recognition_rank",
        "recognition_evidence",
    )

    def __init__(
        self,
        classifier: Optional[MarketStateClassifier] = None,
        theme_ranker: Optional[ThemeRanker] = None,
        recognition_ranker: Optional[RecognitionRanker] = None,
    ) -> None:
        self.classifier = classifier or MarketStateClassifier()
        self.theme_ranker = theme_ranker or ThemeRanker()
        self.recognition_ranker = recognition_ranker or RecognitionRanker()

    def enrich_snapshot(self, snapshot: MarketSnapshot) -> MarketSnapshot:
        """Return a new snapshot containing classified and ranked evidence."""
        market_features = copy.deepcopy(snapshot.market_features)
        market_state = self.classifier.classify(market_features)
        source_feature_quality = market_features.get("_feature_quality")
        source_feature_quality = (
            dict(source_feature_quality)
            if isinstance(source_feature_quality, Mapping)
            else {}
        )
        state_feature_quality = market_state.get("_feature_quality", {})
        market_state["_feature_quality"] = {
            **source_feature_quality,
            **dict(state_feature_quality),
        }
        market_features.update(market_state)
        analysis_degraded = market_state["quality"] != "ready"
        analysis_warnings = []
        if market_state["missing_fields"]:
            analysis_warnings.append(
                "market_state missing: "
                + ",".join(market_state["missing_fields"])
            )

        theme_rankings = self.theme_ranker.rank(
            copy.deepcopy(snapshot.theme_rankings)
        )
        for row in theme_rankings:
            if row["quality"] != "ready":
                analysis_degraded = True
                analysis_warnings.append(
                    f"theme {row['theme_name'] or '<missing>'} missing: "
                    + ",".join(row["missing_fields"])
                )
        theme_by_name = {
            row["theme_name"]: row
            for row in theme_rankings
            if row["theme_name"]
        }

        recognition_rows = self._rank_recognition_by_theme(snapshot.candidates)
        for row in recognition_rows:
            if row["quality"] != "ready":
                analysis_degraded = True
                analysis_warnings.append(
                    f"recognition {row['stock_code'] or '<missing>'} missing: "
                    + ",".join(row["missing_fields"])
                )
        recognition_by_code = {
            row["stock_code"]: row for row in recognition_rows
        }

        candidates = []
        for candidate in sorted(
            snapshot.candidates,
            key=lambda item: str(item.stock_code or "").strip(),
        ):
            features = copy.deepcopy(candidate.features)
            recognition = recognition_by_code[
                str(candidate.stock_code or "").strip()
            ]
            for key in self._RECOGNITION_OUTPUT_FIELDS:
                features[key] = copy.deepcopy(recognition[key])
            features["recognition_quality"] = recognition["quality"]
            features["recognition_missing_fields"] = list(
                recognition["missing_fields"]
            )

            theme = theme_by_name.get(str(candidate.theme_name or "").strip())
            if theme is None:
                analysis_degraded = True
                analysis_warnings.append(
                    f"candidate theme {candidate.stock_code or '<missing>'} "
                    "missing: theme_ranking"
                )
                features.update(
                    {
                        "theme_rank": None,
                        "theme_score": None,
                        "theme_evidence": {},
                        "theme_quality": "degraded",
                        "theme_missing_fields": ["theme_ranking"],
                    }
                )
            else:
                features.update(
                    {
                        "theme_rank": theme["rank"],
                        "theme_score": theme["score"],
                        "theme_evidence": copy.deepcopy(
                            theme["theme_evidence"]
                        ),
                        "theme_quality": theme["quality"],
                        "theme_missing_fields": list(theme["missing_fields"]),
                    }
                )

            candidates.append(
                CandidateSnapshot(
                    stock_code=candidate.stock_code,
                    stock_name=candidate.stock_name,
                    theme_name=candidate.theme_name,
                    features=features,
                    evidence=copy.deepcopy(candidate.evidence),
                )
            )

        quality = self._analysis_quality(
            snapshot.quality,
            analysis_degraded=analysis_degraded,
            analysis_warnings=analysis_warnings,
        )

        return MarketSnapshot(
            source_trade_date=snapshot.source_trade_date,
            target_trade_date=snapshot.target_trade_date,
            stage=snapshot.stage,
            as_of=snapshot.as_of,
            market_features=market_features,
            candidates=candidates,
            theme_rankings=theme_rankings,
            quality=quality,
        )

    def _rank_recognition_by_theme(
        self,
        candidates: Iterable[CandidateSnapshot],
    ) -> List[Dict[str, Any]]:
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        unranked = []
        seen_codes = set()
        for candidate in candidates:
            stock_code = str(candidate.stock_code or "").strip()
            if stock_code in seen_codes:
                raise ValueError(f"duplicate stock_code: {stock_code}")
            seen_codes.add(stock_code)
            row = {
                "stock_code": stock_code,
                **{
                    field: candidate.features.get(field)
                    for field in self._RECOGNITION_FIELDS
                },
            }
            theme_name = str(candidate.theme_name or "").strip()
            if theme_name:
                grouped.setdefault(theme_name, []).append(row)
            else:
                unranked.append(self._unranked_recognition(row))

        ranked = list(unranked)
        for theme_name in sorted(grouped):
            ranked.extend(self.recognition_ranker.rank(grouped[theme_name]))
        return sorted(ranked, key=lambda row: row["stock_code"])

    @staticmethod
    def _unranked_recognition(row: Mapping[str, Any]) -> Dict[str, Any]:
        result = dict(row)
        missing_fields = ["theme_name"]
        evidence = {}
        for dimension, field, rank_key, _ in RecognitionRanker._DIMENSIONS:
            if RecognitionRanker._dimension_value(field, row.get(field)) is None:
                missing_fields.append(field)
            result[rank_key] = None
            evidence[dimension] = {
                "field": field,
                "value": row.get(field),
                "rank": None,
            }
        result.update(
            {
                "recognition_evidence": evidence,
                "recognition_score": None,
                "recognition_rank": None,
                "quality": "degraded",
                "missing_fields": sorted(set(missing_fields)),
            }
        )
        return result

    @classmethod
    def _analysis_quality(
        cls,
        original: DataQuality,
        *,
        analysis_degraded: bool,
        analysis_warnings: Iterable[str],
    ) -> DataQuality:
        warnings = []
        for warning in [*original.warnings, *analysis_warnings]:
            normalized = str(warning).strip()
            if normalized and normalized not in warnings:
                warnings.append(normalized)
            if len(warnings) >= cls._MAX_QUALITY_WARNINGS:
                break

        if original.status == "missing":
            status = "missing"
        elif (
            original.status != "ready"
            or original.stale
            or analysis_degraded
        ):
            status = "degraded"
        else:
            status = "ready"
        return DataQuality(
            status=status,
            as_of=original.as_of,
            source=original.source,
            stale=original.stale,
            warnings=warnings,
        )


__all__ = [
    "MarketStateAnalyzer",
    "MarketStateClassifier",
    "ThemeRanker",
    "RecognitionRanker",
]
