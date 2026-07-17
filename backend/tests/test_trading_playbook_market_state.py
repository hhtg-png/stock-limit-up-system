import copy
import math
from datetime import date, datetime

import pytest

from app.services.trading_playbook import market_state as market_state_module
from app.services.trading_playbook.domain import (
    CandidateSnapshot,
    DataQuality,
    MarketSnapshot,
)
from app.services.trading_playbook.market_state import (
    MarketStateClassifier,
    RecognitionRanker,
    ThemeRanker,
)


class TestMarketStateClassifier:
    def test_bounded_trend_sample_requires_material_ready_coverage(self):
        classifier = MarketStateClassifier()
        common = {
            "limit_down_count": 1,
            "seal_rate": 70,
            "max_board_height": 3,
            "limit_up_count": 30,
            "limit_up_count_prev": 25,
        }

        tiny = classifier.classify(
            {
                **common,
                "trend_new_high_sample_count": 2,
                "trend_new_high_sample_count_prev": 1,
                "trend_sample_size": 2,
                "trend_sample_ready_coverage": 1.0,
                "trend_scope": "bounded_candidate_union",
            }
        )
        production_sized = classifier.classify(
            {
                **common,
                "trend_new_high_sample_count": 24,
                "trend_new_high_sample_count_prev": 12,
                "trend_sample_size": 52,
                "trend_sample_ready_coverage": 1.0,
                "trend_scope": "bounded_candidate_union",
            }
        )

        assert tiny["style"] == "unknown"
        assert tiny["trend_evidence_source"] is None
        assert production_sized["style"] == "trend_main_wave"
        assert production_sized["trend_evidence_source"] == "bounded_sample"

    def test_bounded_trend_sample_below_ready_coverage_is_unknown(self):
        result = MarketStateClassifier().classify(
            {
                "limit_down_count": 1,
                "seal_rate": 70,
                "max_board_height": 3,
                "limit_up_count": 30,
                "limit_up_count_prev": 25,
                "trend_new_high_sample_count": 24,
                "trend_new_high_sample_count_prev": 12,
                "trend_sample_size": 52,
                "trend_sample_ready_coverage": 0.79,
                "trend_scope": "bounded_candidate_union",
            }
        )

        assert result["style"] == "unknown"
        assert result["trend_evidence_source"] is None

    def test_style_and_window_publish_dependency_quality(self):
        complete = MarketStateClassifier().classify(
            {
                "limit_up_count": 82,
                "limit_up_count_prev": 42,
                "max_board_height": 6,
                "seal_rate": 79,
                "limit_down_count": 2,
                "trend_new_high_count": 8,
                "trend_new_high_count_prev": 7,
                "negative_feedback": False,
                "divergence_days": 0,
                "sell_pressure_falling": False,
                "breadth_recovered": False,
                "prior_window": "",
                "sell_pressure_rising": False,
            }
        )
        incomplete = MarketStateClassifier().classify({})

        assert complete["_feature_quality"] == {
            "style": "ready",
            "window": "ready",
        }
        assert incomplete["style"] == "unknown"
        assert incomplete["window"] == "unknown"
        assert incomplete["_feature_quality"] == {
            "style": "missing",
            "window": "missing",
        }

    def test_board_flow_and_outbreak_require_expansion(self):
        result = MarketStateClassifier().classify(
            {
                "limit_up_count": 82,
                "limit_up_count_prev": 42,
                "max_board_height": 6,
                "seal_rate": 79,
                "limit_down_count": 2,
                "trend_new_high_count": 8,
                "trend_new_high_count_prev": 7,
            }
        )

        assert result["style"] == "board_flow"
        assert result["window"] == "outbreak"
        assert result["limit_up_growth"] == 40 / 42
        assert result["trend_growth"] == 1 / 7

    def test_style_rules_are_ordered_and_cover_all_outcomes(self):
        classifier = MarketStateClassifier()

        chaos = classifier.classify(
            {
                "limit_down_count": 10,
                "seal_rate": 80,
                "max_board_height": 8,
                "limit_up_count": 90,
                "limit_up_count_prev": 30,
                "trend_new_high_count": 5,
                "trend_new_high_count_prev": 5,
            }
        )
        trend = classifier.classify(
            {
                "limit_down_count": 1,
                "seal_rate": 70,
                "max_board_height": 3,
                "limit_up_count": 30,
                "limit_up_count_prev": 25,
                "trend_new_high_count": 24,
                "trend_new_high_count_prev": 12,
            }
        )
        dual = classifier.classify(
            {
                "limit_down_count": 1,
                "seal_rate": 60,
                "max_board_height": 2,
                "limit_up_count": 20,
                "limit_up_count_prev": 19,
                "trend_new_high_count": 8,
                "trend_new_high_count_prev": 8,
            }
        )

        assert chaos["style"] == "chaos_retreat"
        assert trend["style"] == "trend_main_wave"
        assert dual["style"] == "dual_active"

    def test_window_rules_are_ordered_and_cover_the_transition_chain(self):
        classifier = MarketStateClassifier()
        base = {
            "limit_down_count": 1,
            "seal_rate": 70,
            "limit_up_count": 20,
            "limit_up_count_prev": 20,
            "divergence_days": 0,
        }

        assert classifier.classify(
            {
                **base,
                "negative_feedback": True,
                "limit_up_count": 40,
            }
        )["window"] == "decline"
        assert classifier.classify(
            {
                **base,
                "divergence_days": 3,
                "sell_pressure_falling": True,
            }
        )["window"] == "divergence_exhaustion"
        assert classifier.classify(
            {
                **base,
                "prior_window": "first_divergence",
                "breadth_recovered": True,
            }
        )["window"] == "divergence_to_consensus"
        assert classifier.classify(
            {
                **base,
                "prior_window": "divergence_to_consensus",
                "breadth_recovered": True,
            }
        )["window"] == "stronger_confirmation"
        assert classifier.classify(
            {
                **base,
                "prior_window": "stronger_confirmation",
                "sell_pressure_rising": True,
            }
        )["window"] == "second_divergence"
        assert classifier.classify(base)["window"] == "first_divergence"

    @pytest.mark.parametrize("prior_window", [None, "unknown"])
    def test_unknown_prior_window_cannot_default_to_first_divergence(
        self,
        prior_window,
    ):
        result = MarketStateClassifier().classify(
            {
                "limit_down_count": 1,
                "seal_rate": 70,
                "max_board_height": 2,
                "limit_up_count": 20,
                "limit_up_count_prev": 20,
                "trend_new_high_count": 8,
                "trend_new_high_count_prev": 8,
                "divergence_days": 0,
                "prior_window": prior_window,
            }
        )

        assert result["window"] == "unknown"
        assert result["quality"] == "degraded"
        assert "prior_window" in result["missing_fields"]

    def test_direct_window_signal_survives_unknown_prior_but_stays_degraded(self):
        result = MarketStateClassifier().classify(
            {
                "negative_feedback": True,
                "prior_window": "unknown",
            }
        )

        assert result["window"] == "decline"
        assert result["quality"] == "degraded"
        assert "prior_window" in result["missing_fields"]

    @pytest.mark.parametrize(
        ("current", "previous"),
        [(None, 10), (10, None), (0, 0), (1, 0), (-1, 10), (10, -1)],
    )
    def test_unavailable_growth_never_fabricates_a_comparable_value(
        self,
        current,
        previous,
    ):
        result = MarketStateClassifier().classify(
            {
                "limit_up_count": current,
                "limit_up_count_prev": previous,
                "trend_new_high_count": 8,
                "trend_new_high_count_prev": 7,
                "max_board_height": 6,
                "seal_rate": 79,
                "limit_down_count": 2,
            }
        )

        assert result["limit_up_growth"] is None
        assert result["style"] == "unknown"
        assert result["window"] == "unknown"
        assert result["quality"] == "degraded"
        assert "limit_up_growth" in result["missing_fields"]

    def test_explicit_finite_growth_can_replace_unavailable_count_history(self):
        result = MarketStateClassifier().classify(
            {
                "limit_up_count": 82,
                "limit_up_count_prev": 0,
                "limit_up_growth": 0.95,
                "max_board_height": 6,
                "seal_rate": 79,
                "limit_down_count": 2,
                "trend_new_high_count": 8,
                "trend_new_high_count_prev": 0,
                "trend_growth": 0.14,
            }
        )

        assert result["limit_up_growth"] == 0.95
        assert result["trend_growth"] == 0.14
        assert result["style"] == "board_flow"
        assert result["window"] == "outbreak"

    def test_direct_negative_rules_survive_other_missing_evidence(self):
        chaos = MarketStateClassifier().classify({"limit_down_count": 10})
        decline = MarketStateClassifier().classify({"negative_feedback": True})

        assert chaos["style"] == "chaos_retreat"
        assert chaos["window"] == "unknown"
        assert chaos["quality"] == "degraded"
        assert chaos["_feature_quality"]["style"] == "ready"
        assert chaos["_feature_quality"]["window"] == "missing"
        assert decline["style"] == "unknown"
        assert decline["window"] == "decline"
        assert decline["quality"] == "degraded"
        assert decline["_feature_quality"]["style"] == "missing"
        assert decline["_feature_quality"]["window"] == "ready"

    def test_missing_non_finite_and_zero_denominator_are_safe_and_deterministic(self):
        features = {
            "limit_up_count": 0,
            "limit_up_count_prev": 0,
            "trend_new_high_count": None,
            "trend_new_high_count_prev": math.nan,
            "seal_rate": math.inf,
            "limit_down_count": None,
            "divergence_days": "not-a-number",
        }
        original = copy.deepcopy(features)
        classifier = MarketStateClassifier()

        first = classifier.classify(features)
        second = classifier.classify(features)

        assert first == second
        assert first["style"] == "unknown"
        assert first["window"] == "unknown"
        assert first["limit_up_growth"] is None
        assert first["trend_growth"] is None
        assert first["quality"] == "degraded"
        assert first["missing_fields"] == sorted(first["missing_fields"])
        assert {
            "divergence_days",
            "limit_down_count",
            "limit_up_growth",
            "seal_rate",
            "trend_growth",
            "trend_new_high_count",
        }.issubset(first["missing_fields"])
        assert features.keys() == original.keys()
        for key, value in original.items():
            if isinstance(value, float) and math.isnan(value):
                assert math.isnan(features[key])
            else:
                assert features[key] == value

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("limit_down_count", -1),
            ("max_board_height", -1),
            ("seal_rate", -0.1),
            ("seal_rate", 100.1),
            ("divergence_days", -1),
        ],
    )
    def test_out_of_range_classifier_evidence_is_missing(self, field, value):
        features = {
            "limit_up_count": 20,
            "limit_up_count_prev": 10,
            "trend_new_high_count": 8,
            "trend_new_high_count_prev": 7,
            "max_board_height": 3,
            "seal_rate": 70,
            "limit_down_count": 1,
            "divergence_days": 0,
        }
        features[field] = value

        result = MarketStateClassifier().classify(features)

        assert result["quality"] == "degraded"
        assert field in result["missing_fields"]

    @pytest.mark.parametrize("value", [2, -1, "yes", "", object()])
    def test_ambiguous_boolean_values_do_not_trigger_negative_rules(self, value):
        result = MarketStateClassifier().classify(
            {
                "negative_feedback": value,
                "sell_pressure_falling": value,
                "breadth_recovered": value,
                "sell_pressure_rising": value,
            }
        )

        assert result["style"] == "unknown"
        assert result["window"] == "unknown"
        assert result["quality"] == "degraded"
        assert "negative_feedback" in result["missing_fields"]


class TestThemeRanker:
    def test_prefers_expanding_theme_and_uses_the_documented_formula(self):
        rows = ThemeRanker().rank(
            [
                {
                    "theme_name": "甲",
                    "limit_up_count": 6,
                    "new_high_count": 4,
                    "sealed_count": 5,
                    "broken_count": 1,
                    "middle_army_strength": 8,
                },
                {
                    "theme_name": "乙",
                    "limit_up_count": 3,
                    "new_high_count": 1,
                    "sealed_count": 2,
                    "broken_count": 2,
                    "middle_army_strength": 2,
                },
            ]
        )

        assert rows[0]["theme_name"] == "甲"
        assert rows[0]["score"] == 57
        assert rows[0]["rank"] == 1
        assert rows[1]["score"] == 18
        assert rows[1]["rank"] == 2

    def test_score_ties_are_broken_by_theme_name(self):
        zero_evidence = {
            "limit_up_count": 0,
            "new_high_count": 0,
            "sealed_count": 0,
            "broken_count": 0,
            "middle_army_strength": 0,
        }
        rows = ThemeRanker().rank(
            [
                {"theme_name": "Beta", **zero_evidence},
                {"theme_name": "Alpha", **zero_evidence},
            ]
        )

        assert [(row["theme_name"], row["rank"]) for row in rows] == [
            ("Alpha", 1),
            ("Beta", 2),
        ]

    def test_invalid_numeric_evidence_is_missing_without_mutating_input(self):
        source = [
            {
                "theme_name": "Beta",
                "limit_up_count": math.nan,
                "new_high_count": None,
                "sealed_count": math.inf,
                "broken_count": "bad",
                "middle_army_strength": -math.inf,
            },
            {"theme_name": "Alpha"},
        ]
        original = copy.deepcopy(source)
        ranker = ThemeRanker()

        first = ranker.rank(source)
        second = ranker.rank(source)

        assert first == second
        assert [row["theme_name"] for row in first] == ["Alpha", "Beta"]
        assert all(row["score"] is None and row["rank"] is None for row in first)
        assert all(row["quality"] == "degraded" for row in first)
        assert first[0]["missing_fields"] == [
            "broken_count",
            "limit_up_count",
            "middle_army_strength",
            "new_high_count",
            "sealed_count",
        ]
        assert source[0].keys() == original[0].keys()
        assert math.isnan(source[0]["limit_up_count"])
        assert source[1] == original[1]

    @pytest.mark.parametrize(
        "field",
        [
            "limit_up_count",
            "new_high_count",
            "sealed_count",
            "broken_count",
            "middle_army_strength",
        ],
    )
    def test_negative_theme_evidence_is_missing(self, field):
        evidence = {
            "theme_name": "Alpha",
            "limit_up_count": 1,
            "new_high_count": 1,
            "sealed_count": 1,
            "broken_count": 1,
            "middle_army_strength": 1,
        }
        evidence[field] = -1

        row = ThemeRanker().rank([evidence])[0]

        assert row["score"] is None
        assert row["rank"] is None
        assert row["missing_fields"] == [field]

    def test_duplicate_theme_names_are_rejected(self):
        evidence = {
            "limit_up_count": 1,
            "new_high_count": 1,
            "sealed_count": 1,
            "broken_count": 0,
            "middle_army_strength": 1,
        }

        with pytest.raises(ValueError, match="duplicate theme_name: Alpha"):
            ThemeRanker().rank(
                [
                    {"theme_name": "Alpha", **evidence},
                    {"theme_name": "Alpha", **evidence},
                ]
            )

    def test_empty_theme_name_is_degraded_and_unranked(self):
        row = ThemeRanker().rank(
            [
                {
                    "theme_name": "",
                    "limit_up_count": 1,
                    "new_high_count": 1,
                    "sealed_count": 1,
                    "broken_count": 0,
                    "middle_army_strength": 1,
                }
            ]
        )[0]

        assert row["score"] is None
        assert row["rank"] is None
        assert row["missing_fields"] == ["theme_name"]


class TestRecognitionRanker:
    def test_recognition_is_relative_and_returns_raw_rank_evidence(self):
        rows = RecognitionRanker().rank(
            [
                {
                    "stock_code": "000001",
                    "first_limit_seconds": 34260,
                    "board_height": 4,
                    "seal_strength": 9,
                    "resilience": 8,
                    "influence": 7,
                },
                {
                    "stock_code": "000002",
                    "first_limit_seconds": 36000,
                    "board_height": 2,
                    "seal_strength": 4,
                    "resilience": 3,
                    "influence": 2,
                },
            ]
        )

        leader = rows[0]
        assert leader["stock_code"] == "000001"
        assert leader["recognition_rank"] == 1
        assert leader["recognition_score"] == 5.0
        assert leader["fastest_rank"] == 1
        assert leader["highest_rank"] == 1
        assert leader["hardest_rank"] == 1
        assert leader["resilience_rank"] == 1
        assert leader["influence_rank"] == 1
        assert leader["recognition_evidence"] == {
            "fastest": {"field": "first_limit_seconds", "value": 34260, "rank": 1},
            "highest": {"field": "board_height", "value": 4, "rank": 1},
            "hardest": {"field": "seal_strength", "value": 9, "rank": 1},
            "resilient": {"field": "resilience", "value": 8, "rank": 1},
            "influential": {"field": "influence", "value": 7, "rank": 1},
        }

    def test_first_limit_seconds_is_ascending_and_other_dimensions_descending(self):
        rows = RecognitionRanker().rank(
            [
                {
                    "stock_code": "000002",
                    "first_limit_seconds": 34200,
                    "board_height": 1,
                    "seal_strength": 1,
                    "resilience": 1,
                    "influence": 1,
                },
                {
                    "stock_code": "000001",
                    "first_limit_seconds": 36000,
                    "board_height": 5,
                    "seal_strength": 5,
                    "resilience": 5,
                    "influence": 5,
                },
            ]
        )
        by_code = {row["stock_code"]: row for row in rows}

        assert by_code["000002"]["fastest_rank"] == 1
        assert by_code["000001"]["fastest_rank"] == 2
        assert by_code["000001"]["highest_rank"] == 1
        assert by_code["000001"]["hardest_rank"] == 1
        assert by_code["000001"]["resilience_rank"] == 1
        assert by_code["000001"]["influence_rank"] == 1
        assert rows[0]["stock_code"] == "000001"

    def test_equal_values_share_dense_ranks_and_stock_code_only_orders_output(self):
        common = {
            "first_limit_seconds": 34200,
            "board_height": 3,
            "seal_strength": 5,
            "resilience": 4,
            "influence": 2,
        }
        rows = RecognitionRanker().rank(
            [
                {"stock_code": "000002", **common},
                {"stock_code": "000001", **common},
            ]
        )

        assert [row["stock_code"] for row in rows] == ["000001", "000002"]
        assert rows[0]["recognition_rank"] == 1
        assert rows[0]["recognition_score"] == 5.0
        assert rows[1]["recognition_rank"] == 1
        assert rows[1]["recognition_score"] == 5.0
        for row in rows:
            assert all(
                row[key] == 1
                for key in (
                    "fastest_rank",
                    "highest_rank",
                    "hardest_rank",
                    "resilience_rank",
                    "influence_rank",
                )
            )

    def test_dimension_ranks_are_dense_after_a_tie(self):
        common = {
            "board_height": 1,
            "seal_strength": 1,
            "resilience": 1,
            "influence": 1,
        }
        rows = RecognitionRanker().rank(
            [
                {"stock_code": "000003", "first_limit_seconds": 35000, **common},
                {"stock_code": "000002", "first_limit_seconds": 34000, **common},
                {"stock_code": "000001", "first_limit_seconds": 34000, **common},
            ]
        )
        by_code = {row["stock_code"]: row for row in rows}

        assert by_code["000001"]["fastest_rank"] == 1
        assert by_code["000002"]["fastest_rank"] == 1
        assert by_code["000003"]["fastest_rank"] == 2

    def test_missing_and_non_finite_values_rank_last_without_mutation(self):
        source = [
            {
                "stock_code": "000003",
                "first_limit_seconds": None,
                "board_height": math.nan,
                "seal_strength": math.inf,
                "resilience": "bad",
                "influence": -math.inf,
            },
            {
                "stock_code": "000001",
                "first_limit_seconds": 33900,
                "board_height": 0,
                "seal_strength": 0,
                "resilience": 0,
                "influence": 0,
            },
            {
                "stock_code": "000002",
                "first_limit_seconds": None,
                "board_height": None,
                "seal_strength": None,
                "resilience": None,
                "influence": None,
            },
        ]
        original = copy.deepcopy(source)
        ranker = RecognitionRanker()

        first = ranker.rank(source)
        second = ranker.rank(source)

        assert first == second
        assert [row["stock_code"] for row in first] == ["000001", "000002", "000003"]
        valid = first[0]
        assert all(
            valid[key] == 1
            for key in (
                "fastest_rank",
                "highest_rank",
                "hardest_rank",
                "resilience_rank",
                "influence_rank",
            )
        )
        for row in first[1:]:
            assert row["recognition_rank"] is None
            assert row["recognition_score"] is None
            assert row["quality"] == "degraded"
            assert all(
                row[key] is None
                for key in (
                    "fastest_rank",
                    "highest_rank",
                    "hardest_rank",
                    "resilience_rank",
                    "influence_rank",
                )
            )
        assert source[0].keys() == original[0].keys()
        assert math.isnan(source[0]["board_height"])
        assert source[1:] == original[1:]

    def test_reversing_input_keeps_the_same_ranked_output(self):
        source = [
            {
                "stock_code": "000003",
                "first_limit_seconds": 35000,
                "board_height": 5,
                "seal_strength": 3,
                "resilience": 6,
                "influence": 2,
            },
            {
                "stock_code": "000001",
                "first_limit_seconds": 34000,
                "board_height": 2,
                "seal_strength": 5,
                "resilience": 1,
                "influence": 7,
            },
            {
                "stock_code": "000002",
                "first_limit_seconds": 36000,
                "board_height": 3,
                "seal_strength": 4,
                "resilience": 8,
                "influence": 1,
            },
        ]
        ranker = RecognitionRanker()

        assert ranker.rank(source) == ranker.rank(list(reversed(source)))

    def test_partial_evidence_omits_only_the_missing_dimensions_from_score(self):
        rows = RecognitionRanker().rank(
            [
                {
                    "stock_code": "000001",
                    "first_limit_seconds": 34000,
                    "board_height": None,
                    "seal_strength": 5,
                    "resilience": None,
                    "influence": 2,
                },
                {
                    "stock_code": "000002",
                    "first_limit_seconds": 35000,
                    "board_height": 2,
                    "seal_strength": 4,
                    "resilience": 3,
                    "influence": 1,
                },
            ]
        )
        by_code = {row["stock_code"]: row for row in rows}
        partial = by_code["000001"]

        assert partial["highest_rank"] is None
        assert partial["resilience_rank"] is None
        assert partial["recognition_score"] == 3.0
        assert partial["recognition_rank"] == 2
        assert partial["quality"] == "degraded"
        assert partial["missing_fields"] == ["board_height", "resilience"]

    @pytest.mark.parametrize(
        "field",
        [
            "first_limit_seconds",
            "board_height",
            "seal_strength",
            "resilience",
            "influence",
        ],
    )
    def test_negative_recognition_evidence_is_missing(self, field):
        evidence = {
            "stock_code": "000001",
            "first_limit_seconds": 34000,
            "board_height": 2,
            "seal_strength": 4,
            "resilience": 3,
            "influence": 1,
        }
        evidence[field] = -1

        row = RecognitionRanker().rank([evidence])[0]

        expected_rank_key = {
            "first_limit_seconds": "fastest_rank",
            "board_height": "highest_rank",
            "seal_strength": "hardest_rank",
            "resilience": "resilience_rank",
            "influence": "influence_rank",
        }[field]
        assert row[expected_rank_key] is None
        assert field in row["missing_fields"]

    def test_duplicate_stock_codes_are_rejected(self):
        evidence = {
            "first_limit_seconds": 34000,
            "board_height": 2,
            "seal_strength": 4,
            "resilience": 3,
            "influence": 1,
        }

        with pytest.raises(ValueError, match="duplicate stock_code: 000001"):
            RecognitionRanker().rank(
                [
                    {"stock_code": "000001", **evidence},
                    {"stock_code": "000001", **evidence},
                ]
            )

    def test_empty_stock_code_is_degraded_and_unranked(self):
        row = RecognitionRanker().rank(
            [
                {
                    "stock_code": "",
                    "first_limit_seconds": 34000,
                    "board_height": 2,
                    "seal_strength": 4,
                    "resilience": 3,
                    "influence": 1,
                }
            ]
        )[0]

        assert row["recognition_score"] is None
        assert row["recognition_rank"] is None
        assert row["missing_fields"] == ["stock_code"]

    @pytest.mark.parametrize("seconds", [33900, 54000])
    def test_first_limit_seconds_accepts_a_share_event_boundaries(self, seconds):
        row = RecognitionRanker().rank(
            [
                {
                    "stock_code": "000001",
                    "first_limit_seconds": seconds,
                    "board_height": 2,
                    "seal_strength": 4,
                    "resilience": 3,
                    "influence": 1,
                }
            ]
        )[0]

        assert row["fastest_rank"] == 1
        assert row["quality"] == "ready"

    @pytest.mark.parametrize("seconds", [-1, 0, 33899, 54001])
    def test_first_limit_seconds_outside_a_share_session_is_missing(self, seconds):
        row = RecognitionRanker().rank(
            [
                {
                    "stock_code": "000001",
                    "first_limit_seconds": seconds,
                    "board_height": 2,
                    "seal_strength": 4,
                    "resilience": 3,
                    "influence": 1,
                }
            ]
        )[0]

        assert row["fastest_rank"] is None
        assert row["recognition_score"] == 4.0
        assert row["quality"] == "degraded"
        assert "first_limit_seconds" in row["missing_fields"]


class TestMarketStateAnalyzer:
    @staticmethod
    def _snapshot(*, complete: bool = True) -> MarketSnapshot:
        as_of = datetime(2026, 7, 10, 15, 30)
        if complete:
            market_features = {
                "limit_up_count": 82,
                "limit_up_count_prev": 42,
                "max_board_height": 6,
                "seal_rate": 79,
                "limit_down_count": 2,
                "trend_new_high_count": 8,
                "trend_new_high_count_prev": 7,
                "divergence_days": 0,
            }
            theme_rankings = [
                {
                    "theme_name": "机器人",
                    "limit_up_count": 6,
                    "new_high_count": 4,
                    "sealed_count": 5,
                    "broken_count": 1,
                    "middle_army_strength": 8,
                },
                {
                    "theme_name": "芯片",
                    "limit_up_count": 3,
                    "new_high_count": 1,
                    "sealed_count": 2,
                    "broken_count": 2,
                    "middle_army_strength": 2,
                },
            ]
            candidates = [
                CandidateSnapshot(
                    stock_code="000001",
                    stock_name="样本甲",
                    theme_name="机器人",
                    features={
                        "first_limit_seconds": 34260,
                        "board_height": 4,
                        "seal_strength": 9,
                        "resilience": 8,
                        "influence": 7,
                    },
                    evidence=[{"source": "review", "value": 1}],
                ),
                CandidateSnapshot(
                    stock_code="000002",
                    stock_name="样本乙",
                    theme_name="芯片",
                    features={
                        "first_limit_seconds": 36000,
                        "board_height": 2,
                        "seal_strength": 4,
                        "resilience": 3,
                        "influence": 2,
                    },
                ),
            ]
        else:
            market_features = {}
            theme_rankings = [
                {
                    "theme_name": "机器人",
                    "candidate_count": 1,
                    "stock_codes": ["000001"],
                }
            ]
            candidates = [
                CandidateSnapshot(
                    stock_code="000001",
                    stock_name="样本甲",
                    theme_name="机器人",
                    features={},
                )
            ]
        return MarketSnapshot(
            source_trade_date=date(2026, 7, 10),
            target_trade_date=date(2026, 7, 13),
            stage="after_close",
            as_of=as_of,
            market_features=market_features,
            candidates=candidates,
            theme_rankings=theme_rankings,
            quality=DataQuality(
                status="ready",
                as_of=as_of,
                source="test",
            ),
        )

    def test_enrich_snapshot_is_a_non_mutating_task3_to_task4_bridge(self):
        snapshot = self._snapshot()
        original = copy.deepcopy(snapshot)

        enriched = market_state_module.MarketStateAnalyzer().enrich_snapshot(
            snapshot
        )

        assert enriched is not snapshot
        assert snapshot == original
        assert enriched.market_features["style"] == "board_flow"
        assert enriched.market_features["window"] == "outbreak"
        assert [row["theme_name"] for row in enriched.theme_rankings] == [
            "机器人",
            "芯片",
        ]
        assert enriched.theme_rankings[0]["score"] == 57
        assert enriched.theme_rankings[0]["rank"] == 1
        assert enriched.quality.status == "ready"
        by_code = {
            candidate.stock_code: candidate for candidate in enriched.candidates
        }
        leader = by_code["000001"].features
        assert leader["theme_rank"] == 1
        assert leader["theme_score"] == 57
        assert leader["theme_quality"] == "ready"
        assert leader["recognition_rank"] == 1
        assert leader["recognition_score"] == 5.0
        assert leader["recognition_quality"] == "ready"
        assert leader["recognition_evidence"]["fastest"]["value"] == 34260
        assert by_code["000002"].features["recognition_rank"] == 1
        assert enriched.candidates[0].evidence is not snapshot.candidates[0].evidence

    def test_enrich_snapshot_propagates_incomplete_evidence_without_zeroes(self):
        snapshot = self._snapshot(complete=False)

        enriched = market_state_module.MarketStateAnalyzer().enrich_snapshot(
            snapshot
        )

        assert enriched.market_features["style"] == "unknown"
        assert enriched.market_features["window"] == "unknown"
        theme = enriched.theme_rankings[0]
        assert theme["score"] is None
        assert theme["rank"] is None
        assert theme["quality"] == "degraded"
        assert "limit_up_count" in theme["missing_fields"]
        features = enriched.candidates[0].features
        assert features["theme_rank"] is None
        assert features["theme_score"] is None
        assert features["theme_quality"] == "degraded"
        assert features["recognition_rank"] is None
        assert features["recognition_score"] is None
        assert features["recognition_quality"] == "degraded"
        assert features["recognition_missing_fields"] == [
            "board_height",
            "first_limit_seconds",
            "influence",
            "resilience",
            "seal_strength",
        ]
        assert enriched.quality.status == "degraded"
        assert enriched.quality.as_of == snapshot.quality.as_of
        assert enriched.quality.source == snapshot.quality.source
        assert enriched.quality.stale is snapshot.quality.stale
        assert len(enriched.quality.warnings) == len(
            set(enriched.quality.warnings)
        )
        assert any(
            warning.startswith("market_state missing:")
            for warning in enriched.quality.warnings
        )
        assert not any(
            warning.startswith(("theme ", "recognition "))
            for warning in enriched.quality.warnings
        )

    def test_candidate_analysis_gaps_do_not_degrade_ready_market_snapshot(self):
        snapshot = self._snapshot()
        snapshot.theme_rankings[0].pop("middle_army_strength")
        snapshot.candidates[0].features.pop("board_height")

        enriched = market_state_module.MarketStateAnalyzer().enrich_snapshot(
            snapshot
        )

        assert enriched.quality.status == "ready"
        assert enriched.quality.warnings == []
        by_code = {
            candidate.stock_code: candidate for candidate in enriched.candidates
        }
        assert by_code["000001"].features["theme_quality"] == "degraded"
        assert by_code["000001"].features["recognition_quality"] == "degraded"

    def test_enrich_snapshot_preserves_missing_or_stale_quality_and_bounds_warnings(self):
        snapshot = self._snapshot()
        warnings = ["重复", "重复"] + [f"已有警告{i}" for i in range(100)]
        snapshot.quality = DataQuality(
            status="missing",
            as_of=snapshot.as_of,
            source="task3",
            stale=True,
            warnings=warnings,
        )

        enriched = market_state_module.MarketStateAnalyzer().enrich_snapshot(
            snapshot
        )

        assert enriched.quality is not snapshot.quality
        assert enriched.quality.status == "missing"
        assert enriched.quality.as_of == snapshot.quality.as_of
        assert enriched.quality.source == "task3"
        assert enriched.quality.stale is True
        assert len(enriched.quality.warnings) <= 50
        assert len(enriched.quality.warnings) == len(
            set(enriched.quality.warnings)
        )

    def test_enrich_snapshot_groups_recognition_by_normalized_theme_and_sorts_codes(self):
        snapshot = self._snapshot()
        snapshot.candidates[0].theme_name = " 机器人 "
        snapshot.candidates.reverse()

        enriched = market_state_module.MarketStateAnalyzer().enrich_snapshot(
            snapshot
        )

        assert [candidate.stock_code for candidate in enriched.candidates] == [
            "000001",
            "000002",
        ]
        assert all(
            candidate.features["recognition_rank"] == 1
            for candidate in enriched.candidates
        )

    def test_enrich_snapshot_does_not_rank_a_candidate_without_theme(self):
        snapshot = self._snapshot()
        snapshot.theme_rankings = []
        snapshot.candidates = [snapshot.candidates[0]]
        snapshot.candidates[0].theme_name = "   "

        enriched = market_state_module.MarketStateAnalyzer().enrich_snapshot(
            snapshot
        )

        features = enriched.candidates[0].features
        assert features["recognition_rank"] is None
        assert features["recognition_score"] is None
        assert features["recognition_quality"] == "degraded"
        assert features["recognition_missing_fields"] == ["theme_name"]
        assert all(
            features[key] is None
            for key in (
                "fastest_rank",
                "highest_rank",
                "hardest_rank",
                "resilience_rank",
                "influence_rank",
            )
        )
        assert enriched.quality.status == "ready"
        assert enriched.quality.warnings == []
