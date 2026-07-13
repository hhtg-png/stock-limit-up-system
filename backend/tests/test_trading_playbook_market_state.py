import copy
import math

from app.services.trading_playbook.market_state import (
    MarketStateClassifier,
    RecognitionRanker,
    ThemeRanker,
)


class TestMarketStateClassifier:
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
        assert first == {
            "style": "dual_active",
            "window": "first_divergence",
            "limit_up_growth": 0.0,
            "trend_growth": 0.0,
        }
        assert features.keys() == original.keys()
        for key, value in original.items():
            if isinstance(value, float) and math.isnan(value):
                assert math.isnan(features[key])
            else:
                assert features[key] == value


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
        rows = ThemeRanker().rank(
            [
                {"theme_name": "Beta", "limit_up_count": 1},
                {"theme_name": "Alpha", "limit_up_count": 1},
            ]
        )

        assert [(row["theme_name"], row["rank"]) for row in rows] == [
            ("Alpha", 1),
            ("Beta", 2),
        ]

    def test_invalid_numeric_evidence_is_neutral_without_mutating_input(self):
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
        assert [(row["theme_name"], row["score"]) for row in first] == [
            ("Alpha", 0.0),
            ("Beta", 0.0),
        ]
        assert source[0].keys() == original[0].keys()
        assert math.isnan(source[0]["limit_up_count"])
        assert source[1] == original[1]


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

    def test_every_tie_is_broken_by_stock_code(self):
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
        assert rows[1]["recognition_rank"] == 2
        assert rows[1]["recognition_score"] == 2.5

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
                "first_limit_seconds": 0,
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
