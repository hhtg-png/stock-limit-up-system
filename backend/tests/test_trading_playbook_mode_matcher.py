"""Contract tests for transcript-derived mode features and matching."""

from __future__ import annotations

import copy
import json
import math
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from app.services.trading_playbook.domain import (
    CandidateSnapshot,
    DataQuality,
    MarketSnapshot,
)
from app.services.trading_playbook.mode_features import (
    FEATURE_KEYS,
    ModeFeatureBuilder,
    _tri_and,
    _tri_or,
)
from app.services.trading_playbook.market_state import MarketStateAnalyzer
from app.services.trading_playbook.mode_matcher import ModeMatcher
from app.services.trading_playbook import rule_catalog as rule_catalog_module


CATALOG_PATH = Path("app/data/trading_playbook_rules_v1.json")
AS_OF = datetime(2026, 7, 10, 14, 40)


def _candidate(
    code: str = "000001",
    *,
    theme: str = "机器人",
    features: dict | None = None,
    evidence: list | None = None,
) -> CandidateSnapshot:
    normalized_features = copy.deepcopy(features or {})
    if "recognition_rank" in normalized_features:
        normalized_features.setdefault("recognition_quality", "ready")
    if "resilience_rank" in normalized_features:
        normalized_features.setdefault("recognition_quality", "ready")
    if "theme_rank" in normalized_features:
        normalized_features.setdefault("theme_quality", "ready")
    if evidence is None:
        evidence = _ready_all_evidence()
    evidence = copy.deepcopy(evidence)
    for row in evidence:
        if (
            row.get("source") == "computed"
            and "fields" not in row
            and "field_quality" not in row
        ):
            row["fields"] = sorted(
                key for key in normalized_features if key != "_feature_quality"
            )
    return CandidateSnapshot(
        stock_code=code,
        stock_name=f"样本{code}",
        theme_name=theme,
        features=normalized_features,
        evidence=copy.deepcopy(evidence),
    )


def _snapshot(
    candidate: CandidateSnapshot,
    *peers: CandidateSnapshot,
    stage: str = "preclose",
    market_features: dict | None = None,
    theme_rankings: list | None = None,
    quality: str = "ready",
    stale: bool = False,
) -> MarketSnapshot:
    return MarketSnapshot(
        source_trade_date=date(2026, 7, 10),
        target_trade_date=date(2026, 7, 13),
        stage=stage,
        as_of=AS_OF,
        market_features=copy.deepcopy(
            market_features
            or {
                "style": "dual_active",
                "window": "outbreak",
                "quality": "ready",
                "divergence_days": 3,
            }
        ),
        candidates=[candidate, *peers],
        theme_rankings=copy.deepcopy(theme_rankings or []),
        quality=DataQuality(
            status=quality,
            as_of=AS_OF,
            source="test",
            stale=stale,
        ),
    )


def _ready_quote_evidence(captured_at: datetime = AS_OF) -> list[dict]:
    return [{"source": "tencent", "as_of": captured_at, "quality": "ready"}]


def _ready_quote_and_kline_evidence(
    captured_at: datetime = AS_OF,
) -> list[dict]:
    return [
        *_ready_quote_evidence(captured_at),
        {"source": "kline", "as_of": captured_at, "quality": "ready"},
        {"source": "computed", "as_of": captured_at, "quality": "ready"},
    ]


def _ready_all_evidence(captured_at: datetime = AS_OF) -> list[dict]:
    return [
        *_ready_quote_evidence(captured_at),
        _source_evidence("computed", as_of=captured_at),
        _source_evidence(
            "market_review_stock_daily",
            quality="ok",
            as_of=captured_at,
        ),
        _source_evidence("kline", as_of=captured_at),
        _source_evidence("realtime_limit_up_pool", as_of=captured_at),
        _source_evidence("trading_plan_candidate", as_of=captured_at),
    ]


def _source_evidence(
    source: str,
    *,
    quality: str = "ready",
    as_of: datetime = AS_OF,
    stale: bool = False,
    fields: list[str] | None = None,
    field_quality: dict[str, str] | None = None,
) -> dict:
    row = {
        "source": source,
        "as_of": as_of,
        "quality": quality,
        "stale": stale,
    }
    if fields is not None:
        row["fields"] = fields
    if field_quality is not None:
        row["field_quality"] = field_quality
    return row


def test_tri_value_helpers_preserve_unknowns_without_masking_true_or_false():
    assert _tri_or(False, True) is True
    assert _tri_or(False, None) is None
    assert _tri_or(False, False) is False
    assert _tri_and(True, False, None) is False
    assert _tri_and(True, None) is None
    assert _tri_and(True, True) is True


class TestModeFeatureBuilder:
    def test_declares_the_complete_transcript_feature_contract(self):
        assert FEATURE_KEYS == {
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

    def test_builds_prices_and_high_volatility_from_point_in_time_evidence(self):
        candidate = _candidate(
            "300001",
            features={
                "price": 10,
                "captured_at": AS_OF,
                "speed_rank": 20,
                "change_rank": 21,
                "validated_support": 9.35,
                "five_day_low": 9.1,
                "prior_n_day_high": 10.5,
            },
            evidence=_ready_quote_and_kline_evidence(),
        )
        candidate.evidence.append(
            _source_evidence(
                "full_market_quote_rank",
                fields=["speed_rank", "change_rank"],
            )
        )

        result = ModeFeatureBuilder().build(_snapshot(candidate), candidate)

        assert result["high_volatility"] is True
        assert result["reference_price"] == 10.0
        assert result["planned_pullback_price"] == 9.35
        assert result["planned_breakout_price"] == 10.51
        assert result["hard_stop_price"] == 9.5
        assert result["exit_change_pct_floor"] == -5.0

    @pytest.mark.parametrize(
        "bad_value",
        [True, math.nan, math.inf, -1, "not-a-number"],
    )
    def test_invalid_numeric_evidence_never_becomes_a_false_or_zero_fact(
        self,
        bad_value,
    ):
        candidate = _candidate(
            "300001",
            features={
                "price": 10,
                "captured_at": AS_OF,
                "speed_rank": bad_value,
            },
            evidence=_ready_all_evidence(),
        )

        result = ModeFeatureBuilder().build(_snapshot(candidate), candidate)

        assert result["high_volatility"] is None

    def test_high_position_same_level_and_middle_army_are_theme_relative(self):
        leader = _candidate(
            features={
                "price": 10,
                "captured_at": AS_OF,
                "board_height": 3,
                "amount": 1000,
                "tradable_market_value": 500,
                "turnover_rate": 4,
                "sealed": True,
            },
            evidence=_ready_all_evidence(),
        )
        same_level = _candidate(
            "000002",
            features={
                "board_height": 3,
                "amount": 800,
                "tradable_market_value": 300,
                "turnover_rate": 3,
                "sealed": True,
            },
        )
        low = _candidate(
            "000003",
            features={
                "board_height": 1,
                "amount": 500,
                "tradable_market_value": 100,
                "turnover_rate": 2,
                "sealed": True,
            },
        )

        result = ModeFeatureBuilder().build(
            _snapshot(leader, same_level, low),
            leader,
        )

        assert result["high_position"] is True
        assert result["same_level_turnover"] is True
        assert result["middle_army"] is True

    def test_survivor_requires_rank_one_and_complete_peer_elimination(self):
        survivor = _candidate(
            features={
                "price": 10,
                "captured_at": AS_OF,
                "recognition_rank": 1,
                "former_high_position": True,
            },
            evidence=_ready_all_evidence(),
        )
        eliminated = _candidate(
            "000002",
            features={
                "former_high_position": True,
                "review_today_broken": True,
            },
        )
        incomplete = _candidate(
            "000003",
            features={"former_high_position": True},
        )
        builder = ModeFeatureBuilder()

        assert builder.build(
            _snapshot(survivor, eliminated), survivor
        )["unique_survivor"] is True
        assert builder.build(
            _snapshot(survivor, eliminated, incomplete), survivor
        )["unique_survivor"] is None
        incomplete.features.update(
            {
                "review_today_broken": False,
                "opened": False,
                "trend_broken": False,
                "_feature_quality": {
                    "opened": "computed",
                    "trend_broken": "computed",
                },
            }
        )
        assert builder.build(
            _snapshot(survivor, eliminated, incomplete), survivor
        )["unique_survivor"] is False

    def test_leader_state_transitions_require_prior_state_and_confirmation(self):
        candidate = _candidate(
            features={
                "price": 10.2,
                "pre_close": 10,
                "open_price": 10.1,
                "captured_at": AS_OF,
                "prior_mode_state": "survivor",
                "resealed": True,
                "influence_rank": 1,
                "recognition_rank": 1,
            },
            evidence=_ready_all_evidence(),
        )
        builder = ModeFeatureBuilder()

        result = builder.build(_snapshot(candidate), candidate)
        assert result["turn_confirmed"] is True

        candidate.features.update(
            {
                "prior_mode_state": "turn_confirmed",
                "new_recognition_leader": False,
            }
        )
        result = builder.build(_snapshot(candidate), candidate)
        assert result["stronger_confirmed"] is True
        assert result["confirmed_leader"] is True

        candidate.features.update({"open_price": 9.9, "price": 10.2})
        result = builder.build(_snapshot(candidate), candidate)
        assert result["stronger_confirmed"] is True

    def test_turn_and_divergence_alternatives_use_three_value_or(self):
        candidate = _candidate(
            features={
                "price": 10,
                "captured_at": AS_OF,
                "prior_mode_state": "survivor",
                "resealed": False,
                "reversal_limit_up": True,
                "right_side_breakout": False,
                "recognition_rank": 1,
            },
        )
        builder = ModeFeatureBuilder()

        assert builder.build(
            _snapshot(candidate), candidate
        )["turn_confirmed"] is True

        candidate.features.update(
            {
                "reversal_limit_up": False,
                "right_side_breakout": True,
            }
        )
        candidate.features.pop("influence_rank", None)
        assert builder.build(
            _snapshot(candidate), candidate
        )["turn_confirmed"] is None

        candidate.features.update(
            {
                "confirmed_leader_fact": True,
                "prior_accelerated": True,
                "opened": False,
                "review_today_broken": True,
                "_feature_quality": {
                    "confirmed_leader_fact": "computed",
                    "prior_accelerated": "computed",
                    "opened": "computed",
                },
            }
        )
        assert builder.build(
            _snapshot(candidate), candidate
        )["acceleration_to_divergence"] is True

    def test_stronger_confirmation_preserves_unknown_current_strength(self):
        candidate = _candidate(
            features={
                "pre_close": 10,
                "open_price": 9.9,
                "captured_at": AS_OF,
                "prior_mode_state": "turn_confirmed",
                "recognition_rank": 1,
                "new_recognition_leader": False,
            },
        )

        result = ModeFeatureBuilder().build(_snapshot(candidate), candidate)

        assert result["stronger_confirmed"] is None

    def test_nested_realtime_review_and_plan_facts_drive_conservative_states(self):
        candidate = _candidate(
            features={
                "price": 10.2,
                "captured_at": AS_OF,
                "review_yesterday_continuous_days": 0,
                "review_today_continuous_days": 1,
                "review_today_sealed_close": True,
                "review_first_limit_time": "09:35:00",
                "realtime_limit_up_fact": {"resealed": True},
                "plan_candidate_fact": {
                    "role": "survivor",
                    "primary_mode_key": "unique_survivor_trial",
                },
                "influence_rank": 1,
                "recognition_rank": 1,
                "started_after_leader": True,
            },
            evidence=_ready_all_evidence(),
        )
        snapshot = _snapshot(
            candidate,
            market_features={
                "style": "board_flow",
                "window": "first_divergence",
                "quality": "ready",
            },
            theme_rankings=[
                {
                    "theme_name": "机器人",
                    "quality": "ready",
                    "outbreak_start_seconds": 36000,
                }
            ],
        )

        result = ModeFeatureBuilder().build(snapshot, candidate)

        assert result["started_before_theme"] is True
        assert result["low_position_new_start"] is True
        assert result["supplement"] is True
        assert result["turn_confirmed"] is True

    def test_realtime_degraded_cannot_borrow_ready_quote_quality(self):
        candidate = _candidate(
            features={
                "price": 10,
                "captured_at": AS_OF,
                "recognition_rank": 1,
                "plan_candidate_fact": {
                    "role": "survivor",
                    "primary_mode_key": "unique_survivor_trial",
                },
                "realtime_limit_up_fact": {"resealed": True},
            },
            evidence=[
                _source_evidence("tencent"),
                _source_evidence("trading_plan_candidate"),
                _source_evidence(
                    "realtime_limit_up_pool",
                    quality="degraded",
                ),
            ],
        )
        snapshot = _snapshot(
            candidate,
            market_features={
                "style": "board_flow",
                "window": "divergence_to_consensus",
                "quality": "ready",
            },
        )
        built = ModeFeatureBuilder().build(snapshot, candidate)

        assert built["turn_confirmed"] is None
        assert built["_source_quality"]["quote"] == "ready"
        assert built["_source_quality"]["realtime"] == "degraded"
        assert built["_candidate_quality_status"] == "degraded"

        candidate.features.update(built)
        leader_rule = next(
            rule
            for rule in _catalog_payload()["rules"]
            if rule["mode_key"] == "leader_turn_two"
        )
        row = ModeMatcher([leader_rule]).evaluate(
            snapshot.market_features,
            candidate,
        )[0]
        assert (row.status, row.risk_level) == ("waiting", "watch")

    @pytest.mark.parametrize(
        "bad_realtime",
        [
            _source_evidence("realtime_limit_up_pool", quality="stale", stale=True),
            _source_evidence(
                "realtime_limit_up_pool",
                as_of=AS_OF + timedelta(minutes=1),
            ),
        ],
        ids=["stale", "future"],
    )
    def test_realtime_stale_or_future_exact_source_is_untrusted(self, bad_realtime):
        candidate = _candidate(
            features={
                "price": 10,
                "captured_at": AS_OF,
                "plan_candidate_fact": {"role": "survivor"},
                "realtime_limit_up_fact": {"reversal_limit_up": True},
            },
            evidence=[
                _source_evidence("tencent"),
                _source_evidence("trading_plan_candidate"),
                bad_realtime,
            ],
        )

        result = ModeFeatureBuilder().build(_snapshot(candidate), candidate)

        assert result["turn_confirmed"] is None

    @pytest.mark.parametrize(
        "bad_review",
        [
            _source_evidence(
                "market_review_stock_daily",
                quality="degraded",
            ),
            _source_evidence(
                "market_review_stock_daily",
                quality="stale",
                stale=True,
            ),
            _source_evidence(
                "market_review_stock_daily",
                as_of=AS_OF + timedelta(minutes=1),
            ),
        ],
        ids=["degraded", "stale", "future"],
    )
    def test_bad_review_cannot_borrow_peer_quote_or_computed_quality(
        self,
        bad_review,
    ):
        survivor = _candidate(
            features={
                "price": 10,
                "captured_at": AS_OF,
                "recognition_rank": 1,
            },
        )
        eliminated = _candidate(
            "000002",
            features={
                "former_high_position": True,
                "review_today_broken": True,
            },
            evidence=[
                _source_evidence("tencent"),
                _source_evidence("computed"),
                bad_review,
            ],
        )

        result = ModeFeatureBuilder().build(
            _snapshot(survivor, eliminated),
            survivor,
        )

        assert result["unique_survivor"] is None

    def test_degraded_kline_cannot_prove_trend_linkage_or_pullback(self):
        candidate = _candidate(
            features={
                "price": 9.6,
                "captured_at": AS_OF,
                "kline_quality": "ready",
                "trend_established": True,
                "n_day_high": True,
                "consolidation_days": 5,
                "five_day_low": 9.4,
                "prior_n_day_high": 10,
            },
            evidence=[
                _source_evidence("tencent"),
                _source_evidence("kline", quality="degraded"),
            ],
        )

        result = ModeFeatureBuilder().build(_snapshot(candidate), candidate)

        assert result["trend_established"] is None
        assert result["consolidation_rebreak"] is None
        assert result["pullback"] is None
        assert result["planned_pullback_quality"] == "fallback"

    def test_degraded_plan_cannot_supply_prior_mode_state(self):
        candidate = _candidate(
            features={
                "price": 10,
                "captured_at": AS_OF,
                "plan_candidate_fact": {"role": "survivor"},
                "realtime_limit_up_fact": {"resealed": True},
            },
            evidence=[
                _source_evidence("tencent"),
                _source_evidence(
                    "trading_plan_candidate",
                    quality="degraded",
                ),
                _source_evidence("realtime_limit_up_pool"),
            ],
        )

        result = ModeFeatureBuilder().build(_snapshot(candidate), candidate)

        assert result["turn_confirmed"] is None

    def test_degraded_quote_cannot_borrow_computed_quality_for_reference_price(self):
        candidate = _candidate(
            features={"price": 10, "captured_at": AS_OF},
            evidence=[
                _source_evidence("tencent", quality="degraded"),
                _source_evidence("computed"),
            ],
        )

        result = ModeFeatureBuilder().build(_snapshot(candidate), candidate)

        assert result["reference_price"] is None

    def test_task4_rank_requires_its_explicit_ready_quality(self):
        candidate = _candidate(
            features={
                "price": 10,
                "captured_at": AS_OF,
                "recognition_rank": 1,
                "recognition_quality": "degraded",
                "theme_rank": 1,
                "theme_quality": "degraded",
            },
        )

        result = ModeFeatureBuilder().build(_snapshot(candidate), candidate)

        assert result["recognition_rank"] is None
        assert result["theme_rank"] is None
        assert result["_candidate_quality_status"] == "degraded"

    def test_degraded_theme_row_cannot_prove_linkage_or_external_expansion(self):
        candidate = _candidate(
            features={
                "price": 10,
                "captured_at": AS_OF,
                "theme_rank": 1,
                "theme_quality": "ready",
                "is_external_theme": True,
            },
        )
        snapshot = _snapshot(
            candidate,
            theme_rankings=[
                {
                    "theme_name": "机器人",
                    "quality": "degraded",
                    "new_high_count": 3,
                    "middle_army_strength": 2,
                    "limit_up_count": 4,
                }
            ],
        )

        result = ModeFeatureBuilder().build(snapshot, candidate)

        assert result["linkage_confirmed"] is None
        assert result["external_switch"] is None

    def test_direct_derived_flags_require_feature_quality_or_computed_evidence(self):
        candidate = _candidate(
            features={
                "price": 10,
                "captured_at": AS_OF,
                "prior_mode_state": "survivor",
                "resealed": True,
            },
            evidence=[_source_evidence("tencent")],
        )

        untrusted = ModeFeatureBuilder().build(_snapshot(candidate), candidate)
        assert untrusted["turn_confirmed"] is None

        candidate.features["_feature_quality"] = {
            "prior_mode_state": "ready",
            "resealed": "computed",
        }
        trusted = ModeFeatureBuilder().build(_snapshot(candidate), candidate)
        assert trusted["turn_confirmed"] is True

    def test_field_quality_keeps_ready_quote_price_when_optional_quote_is_missing(self):
        candidate = _candidate(
            features={
                "price": 10,
                "captured_at": AS_OF,
                "amount": 1200,
            },
            evidence=[
                _source_evidence(
                    "tencent",
                    quality="degraded",
                    field_quality={
                        "price": "ready",
                        "captured_at": "ready",
                        "amount": "missing",
                    },
                )
            ],
        )

        result = ModeFeatureBuilder().build(_snapshot(candidate), candidate)

        assert result["reference_price"] == 10
        assert result["_source_quality"]["quote"] == "degraded"

    @pytest.mark.parametrize(
        "bad_quality",
        ["degraded", "stale", "future"],
    )
    def test_only_evidence_rows_covering_price_can_degrade_price(self, bad_quality):
        if bad_quality == "future":
            bad = _source_evidence(
                "tencent",
                as_of=AS_OF + timedelta(minutes=1),
                fields=["price"],
            )
        else:
            bad = _source_evidence(
                "tencent",
                quality=bad_quality,
                stale=bad_quality == "stale",
                fields=["price" if bad_quality == "degraded" else "amount"],
            )
        candidate = _candidate(
            features={"price": 10, "captured_at": AS_OF, "amount": 1200},
            evidence=[
                _source_evidence(
                    "tencent",
                    fields=["price", "captured_at"],
                ),
                bad,
            ],
        )

        result = ModeFeatureBuilder().build(_snapshot(candidate), candidate)

        expected = None if bad_quality in {"degraded", "future"} else 10
        assert result["reference_price"] == expected

    def test_full_market_rank_evidence_cannot_certify_unrelated_direct_facts(self):
        candidate = _candidate(
            features={
                "price": 10,
                "captured_at": AS_OF,
                "speed_rank": 1,
                "prior_mode_state": "survivor",
                "resealed": True,
                "snake_pattern": True,
                "tail_entry_satisfied": True,
            },
            evidence=[
                _source_evidence("tencent"),
                _source_evidence("full_market_quote_rank"),
            ],
        )

        result = ModeFeatureBuilder().build(_snapshot(candidate), candidate)

        assert result["high_volatility"] is False
        assert result["turn_confirmed"] is None
        assert result["snake_setup"] is None
        assert "prior_mode_state" not in result.get("_trusted_facts", {})

    def test_unknown_direct_fact_needs_exact_field_declaration(self):
        candidate = _candidate(
            features={
                "price": 10,
                "captured_at": AS_OF,
                "prior_mode_state": "survivor",
                "resealed": True,
            },
            evidence=[
                _source_evidence("tencent"),
                _source_evidence(
                    "computed",
                    fields=["prior_mode_state", "resealed"],
                ),
            ],
        )

        result = ModeFeatureBuilder().build(_snapshot(candidate), candidate)

        assert result["turn_confirmed"] is True

    def test_partial_recognition_quality_preserves_ready_dimension_rank_only(self):
        candidate = _candidate(
            features={
                "price": 10,
                "captured_at": AS_OF,
                "resilience_rank": 1,
                "recognition_rank": 1,
                "recognition_quality": "degraded",
                "recognition_evidence": {
                    "resilient": {
                        "field": "resilience",
                        "value": 9,
                        "rank": 1,
                    },
                    "fastest": {
                        "field": "first_limit_seconds",
                        "value": None,
                        "rank": None,
                    },
                },
            },
        )

        result = ModeFeatureBuilder().build(_snapshot(candidate), candidate)

        assert result["resilience_rank"] == 1
        assert result["recognition_rank"] is None

    def test_historical_review_today_is_prior_and_realtime_is_current(self):
        candidate = _candidate(
            features={
                "price": 10,
                "captured_at": AS_OF,
                "review_trade_date": date(2026, 7, 9),
                "review_today_continuous_days": 1,
                "review_today_sealed_close": True,
                "realtime_limit_up_fact": {
                    "trade_date": date(2026, 7, 10),
                    "board_height": 3,
                    "sealed": False,
                },
            },
            evidence=[
                _source_evidence("tencent"),
                _source_evidence("market_review_stock_daily", quality="ok"),
                _source_evidence("realtime_limit_up_pool"),
            ],
        )

        result = ModeFeatureBuilder().build(_snapshot(candidate), candidate)

        assert result["_current_board_height"] == 3
        assert result["_current_sealed"] is False
        assert result["low_position_new_start"] is False

    @pytest.mark.parametrize(
        ("raw_sealed", "expected", "expected_status"),
        [
            (True, True, "matched"),
            ("true", None, "waiting"),
        ],
    )
    def test_current_sealed_supports_only_boolean_crawler_schema(
        self,
        raw_sealed,
        expected,
        expected_status,
    ):
        candidate = _candidate(
            features={
                "price": 10,
                "captured_at": AS_OF,
                "realtime_limit_up_fact": {
                    "is_final_sealed": raw_sealed,
                    "open_count": 3,
                },
            },
            evidence=[
                _source_evidence("tencent"),
                _source_evidence("realtime_limit_up_pool"),
            ],
        )
        snapshot = _snapshot(candidate)
        built = ModeFeatureBuilder().build(snapshot, candidate)
        match_candidate = _matcher_candidate(**built)

        row = ModeMatcher([
            _rule(
                role="high_volatility",
                requirements=[{
                    "feature": "candidate._current_sealed",
                    "op": "eq",
                    "value": True,
                }],
            )
        ]).evaluate(
            snapshot.market_features,
            match_candidate,
        )[0]

        assert built["_current_sealed"] is expected
        assert row.status == expected_status

    def test_same_day_ready_review_realtime_conflict_makes_current_unknown(self):
        candidate = _candidate(
            features={
                "price": 10,
                "captured_at": AS_OF,
                "review_trade_date": date(2026, 7, 10),
                "review_today_continuous_days": 1,
                "review_today_sealed_close": True,
                "realtime_limit_up_fact": {
                    "trade_date": date(2026, 7, 10),
                    "board_height": 3,
                    "sealed": False,
                },
            },
            evidence=[
                _source_evidence("tencent"),
                _source_evidence("market_review_stock_daily", quality="ok"),
                _source_evidence("realtime_limit_up_pool"),
            ],
        )

        result = ModeFeatureBuilder().build(_snapshot(candidate), candidate)

        assert result["_current_board_height"] is None
        assert result["_current_sealed"] is None
        assert result["low_position_new_start"] is None
        assert result["_current_board_height_quality"] == "degraded"
        assert result["_current_sealed_quality"] == "degraded"
        assert result["_feature_quality"]["low_position_new_start"] == "degraded"

    def test_former_high_position_can_be_proven_by_yesterday_review_height(self):
        survivor = _candidate(
            features={
                "price": 10,
                "captured_at": AS_OF,
                "recognition_rank": 1,
            },
            evidence=_ready_all_evidence(),
        )
        eliminated = _candidate(
            "000002",
            features={
                "review_yesterday_continuous_days": 3,
                "review_today_broken": True,
            },
        )

        result = ModeFeatureBuilder().build(
            _snapshot(survivor, eliminated),
            survivor,
        )

        assert result["unique_survivor"] is True

    def test_peer_review_ok_quality_is_normalized_as_ready(self):
        survivor = _candidate(
            features={
                "price": 10,
                "captured_at": AS_OF,
                "recognition_rank": 1,
            },
        )
        eliminated = _candidate(
            "000002",
            features={
                "former_high_position": True,
                "review_today_broken": True,
            },
            evidence=[
                {
                    "source": "market_review_stock_daily",
                    "as_of": AS_OF,
                    "quality": "ok",
                },
                _source_evidence("computed"),
            ],
        )

        result = ModeFeatureBuilder().build(
            _snapshot(survivor, eliminated),
            survivor,
        )

        assert result["unique_survivor"] is True

    def test_future_or_unsourced_peer_cannot_prove_unique_survivor(self):
        survivor = _candidate(
            features={
                "price": 10,
                "captured_at": AS_OF,
                "recognition_rank": 1,
            },
        )
        eliminated = _candidate(
            "000002",
            features={
                "former_high_position": True,
                "review_today_broken": True,
            },
        )
        future = _candidate(
            "000003",
            features={
                "former_high_position": True,
                "review_today_broken": True,
            },
            evidence=_ready_quote_evidence(AS_OF + timedelta(minutes=1)),
        )
        unsourced = _candidate(
            "000004",
            features={
                "former_high_position": True,
                "review_today_broken": True,
            },
            evidence=[],
        )
        builder = ModeFeatureBuilder()

        assert builder.build(
            _snapshot(survivor, eliminated, future), survivor
        )["unique_survivor"] is None
        assert builder.build(
            _snapshot(survivor, eliminated, unsourced), survivor
        )["unique_survivor"] is None

    def test_trend_turn_two_requires_trend_consolidation_breakout_and_linkage(self):
        candidate = _candidate(
            features={
                "price": 10.6,
                "captured_at": AS_OF,
                "kline_quality": "ready",
                "trend_established": True,
                "consolidation_days": 5,
                "n_day_high": True,
                "linkage_confirmed": True,
                "middle_army_linkage": True,
            },
            evidence=_ready_all_evidence(),
        )

        result = ModeFeatureBuilder().build(_snapshot(candidate), candidate)

        assert result["consolidation_rebreak"] is True
        assert result["trend_turn_two"] is True

    def test_pullback_linkage_rotation_and_leader_risk_features_are_derived(self):
        candidate = _candidate(
            features={
                "price": 9.6,
                "captured_at": AS_OF,
                "validated_support": 9.4,
                "prior_n_day_high": 10.0,
                "kline_quality": "ready",
                "trend_established": True,
                "confirmed_leader_fact": True,
                "prior_accelerated": True,
                "review_today_broken": True,
                "first_bearish_signal": True,
                "is_external_theme": True,
                "theme_expanding": True,
                "theme_rank": 1,
            },
            evidence=_ready_all_evidence(),
        )
        snapshot = _snapshot(
            candidate,
            market_features={
                "style": "dual_active",
                "window": "stage_three",
                "quality": "ready",
            },
            theme_rankings=[
                {
                    "theme_name": "机器人",
                    "quality": "ready",
                    "new_high_count": 2,
                    "middle_army_strength": 3,
                    "limit_up_count": 4,
                }
            ],
        )

        result = ModeFeatureBuilder().build(snapshot, candidate)

        assert result["pullback"] is True
        assert result["linkage_confirmed"] is True
        assert result["acceleration_to_divergence"] is True
        assert result["first_bearish"] is True
        assert result["external_switch"] is True

    def test_fallback_pullback_price_is_not_support_evidence(self):
        candidate = _candidate(
            features={
                "price": 9.6,
                "captured_at": AS_OF,
                "prior_n_day_high": 10,
                "kline_quality": "ready",
                "trend_established": True,
            },
            evidence=_ready_quote_and_kline_evidence(),
        )
        builder = ModeFeatureBuilder()

        fallback = builder.build(_snapshot(candidate), candidate)
        assert fallback["planned_pullback_price"] == 9.12
        assert fallback["planned_pullback_quality"] == "fallback"
        assert fallback["pullback"] is None

        candidate.features["five_day_low"] = 9.4
        supported = builder.build(_snapshot(candidate), candidate)
        assert supported["planned_pullback_price"] == 9.4
        assert supported["planned_pullback_quality"] == "ready"
        assert supported["pullback"] is True

    @pytest.mark.parametrize(
        ("support", "expected_quality"),
        [(9.5, "invalid"), (9.4, "invalid"), (9.6, "ready")],
    )
    def test_planned_pullback_marks_support_at_or_below_stop_invalid(
        self,
        support,
        expected_quality,
    ):
        candidate = _candidate(
            features={
                "price": 10,
                "captured_at": AS_OF,
                "validated_support": support,
            },
            evidence=_ready_quote_and_kline_evidence(),
        )

        result = ModeFeatureBuilder().build(_snapshot(candidate), candidate)

        assert result["planned_pullback_price"] == support
        assert result["planned_pullback_quality"] == expected_quality

    def test_snake_and_right_reversal_wait_without_their_distinct_theme_evidence(self):
        candidate = _candidate(
            features={
                "price": 10,
                "captured_at": AS_OF,
                "snake_pattern": True,
                "right_side_breakout": True,
            },
            evidence=_ready_all_evidence(),
        )

        result = ModeFeatureBuilder().build(_snapshot(candidate), candidate)

        assert result["theme_alive"] is None
        assert result["theme_dead"] is None
        assert result["snake_setup"] is None
        assert result["right_reversal"] is None

    def test_theme_alive_and_dead_use_peer_breadth_not_isolated_shape(self):
        core = _candidate(
            features={
                "price": 10,
                "captured_at": AS_OF,
                "theme_breadth_negative_days": 2,
                "sealed": False,
                "n_day_high": False,
            },
            evidence=_ready_all_evidence(),
        )
        supplement = _candidate(
            "000002",
            features={
                "supplement": True,
                "sealed": True,
                "n_day_high": False,
            },
        )
        builder = ModeFeatureBuilder()

        alive = builder.build(_snapshot(core, supplement), core)
        assert alive["theme_alive"] is True
        assert alive["theme_dead"] is False

        supplement.features.update({"supplement": False, "sealed": False})
        dead = builder.build(_snapshot(core, supplement), core)
        assert dead["theme_alive"] is False
        assert dead["theme_dead"] is True

    def test_theme_dead_excludes_the_candidate_own_right_side_new_high(self):
        candidate = _candidate(
            features={
                "price": 10,
                "captured_at": AS_OF,
                "theme_breadth_negative_days": 2,
                "sealed": True,
                "n_day_high": True,
                "right_side_breakout": True,
            },
        )

        result = ModeFeatureBuilder().build(_snapshot(candidate), candidate)

        assert result["theme_dead"] is True
        assert result["right_reversal"] is True

    def test_tail_base_requires_only_preclose_fresh_quote_and_nonstale_snapshot(self):
        candidate = _candidate(
            features={
                "price": 10,
                "captured_at": AS_OF - timedelta(seconds=30),
                "automation_level": "manual_only",
            },
            evidence=_ready_all_evidence(AS_OF - timedelta(seconds=30)),
        )
        builder = ModeFeatureBuilder()

        assert builder.build(
            _snapshot(candidate, stage="preclose"), candidate
        )["tail_action_eligible"] is True
        assert builder.build(
            _snapshot(candidate, stage="auction"), candidate
        )["tail_action_eligible"] is False
        assert builder.build(
            _snapshot(candidate, stage="preclose", stale=True), candidate
        )["tail_action_eligible"] is False

    def test_tail_base_eligibility_can_defer_automation_level_to_the_rule_matcher(self):
        candidate = _candidate(
            features={
                "price": 10,
                "captured_at": AS_OF,
            },
            evidence=_ready_all_evidence(),
        )

        result = ModeFeatureBuilder().build(
            _snapshot(candidate, stage="preclose"),
            candidate,
        )

        assert result["tail_action_eligible"] is True

    def test_future_evidence_is_not_used_and_inputs_are_immutable(self):
        candidate = _candidate(
            "300001",
            features={
                "price": 10,
                "captured_at": AS_OF + timedelta(minutes=1),
                "speed_rank": 1,
            },
            evidence=_ready_quote_evidence(AS_OF + timedelta(minutes=1)),
        )
        snapshot = _snapshot(candidate)
        before_candidate = copy.deepcopy(candidate)
        before_snapshot = copy.deepcopy(snapshot)

        result = ModeFeatureBuilder().build(snapshot, candidate)

        assert result["reference_price"] is None
        assert result["high_volatility"] is None
        assert result["_point_in_time_valid"] is False
        assert candidate == before_candidate
        assert snapshot == before_snapshot

    def test_reference_price_requires_ready_non_stale_quote_evidence(self):
        naked = _candidate(
            features={"price": 10, "captured_at": AS_OF},
            evidence=[],
        )
        stale = _candidate(
            features={"price": 10, "captured_at": AS_OF},
            evidence=[
                {
                    "source": "tencent",
                    "as_of": AS_OF,
                    "quality": "stale",
                    "stale": True,
                }
            ],
        )
        builder = ModeFeatureBuilder()

        assert builder.build(_snapshot(naked), naked)["reference_price"] is None
        assert builder.build(_snapshot(stale), stale)["reference_price"] is None

    def test_aware_utc_future_evidence_is_compared_in_shanghai_time(self):
        captured_at = datetime(2026, 7, 10, 6, 41, tzinfo=timezone.utc)
        candidate = _candidate(
            features={"price": 10, "captured_at": captured_at},
            evidence=_ready_quote_evidence(captured_at),
        )

        result = ModeFeatureBuilder().build(_snapshot(candidate), candidate)

        assert result["_point_in_time_valid"] is False
        assert result["reference_price"] is None

    def test_future_known_nonquote_evidence_is_a_global_point_in_time_failure(self):
        candidate = _candidate(
            features={
                "price": 10,
                "captured_at": AS_OF,
                "realtime_limit_up_fact": {"sealed": True},
            },
            evidence=[
                _source_evidence("tencent"),
                _source_evidence(
                    "realtime_limit_up_pool",
                    as_of=AS_OF + timedelta(seconds=1),
                ),
            ],
        )

        result = ModeFeatureBuilder().build(_snapshot(candidate), candidate)

        assert result["_point_in_time_valid"] is False
        assert result["reference_price"] is None


def _rule(
    mode_key: str = "example",
    *,
    automation_level: str = "assisted",
    role: str = "survivor",
    requirements: list | None = None,
    window: str = "outbreak",
    style: str = "dual_active",
) -> dict:
    return {
        "mode_key": mode_key,
        "name": mode_key,
        "family": "test",
        "style": style,
        "window": window,
        "automation_level": automation_level,
        "priority": 100,
        "role": role,
        "requirements": requirements
        or [{"feature": "candidate.flag", "op": "eq", "value": True}],
        "entry": {"label": "进入"},
        "invalidation": {"label": "失效"},
        "exit": {"label": "退出"},
        "source_refs": [{"source_key": "test", "excerpt": "evidence"}],
    }


def _matcher_candidate(**overrides) -> CandidateSnapshot:
    features = {
        "flag": True,
        "reference_price": 10.0,
        "planned_pullback_price": 9.6,
        "planned_pullback_quality": "ready",
        "planned_breakout_price": 10.51,
        "hard_stop_price": 9.5,
        "exit_change_pct_floor": -5.0,
        "tail_action_eligible": False,
        "_snapshot_quality_status": "ready",
        "_snapshot_stale": False,
        "_point_in_time_valid": True,
        "_feature_quality": {},
    }
    features.update(overrides)
    return _candidate(features=features, evidence=[{"source": "test", "quality": "ready"}])


class TestModeMatcherContract:
    def test_distinguishes_missing_failed_manual_assisted_and_automatic(self):
        rules = [
            _rule("assisted"),
            _rule("automatic", automation_level="automatic"),
            _rule("manual", automation_level="manual_only"),
        ]
        market = {"style": "dual_active", "window": "outbreak", "quality": "ready"}
        matcher = ModeMatcher(rules)

        positive = {row.mode_key: row for row in matcher.evaluate(market, _matcher_candidate())}
        assert (positive["assisted"].status, positive["assisted"].risk_level) == (
            "matched",
            "trial",
        )
        assert (positive["automatic"].status, positive["automatic"].risk_level) == (
            "matched",
            "confirmed",
        )
        assert (positive["manual"].status, positive["manual"].risk_level) == (
            "manual_review",
            "watch",
        )

        waiting = matcher.evaluate(market, _matcher_candidate(flag=None))
        assert {row.status for row in waiting} == {"waiting"}
        failed = matcher.evaluate(market, _matcher_candidate(flag=False))
        assert {row.status for row in failed} == {"not_matched"}

    def test_unrelated_aggregate_degradation_does_not_block_but_stale_does(self):
        matcher = ModeMatcher([_rule(automation_level="automatic")])

        degraded = matcher.evaluate(
            {
                "style": "dual_active",
                "window": "outbreak",
                "quality": "degraded",
            },
            _matcher_candidate(
                _snapshot_quality_status="degraded",
                _candidate_quality_status="degraded",
                _feature_quality={"unused_kline_fact": "degraded"},
            ),
        )[0]
        stale = matcher.evaluate(
            {"style": "dual_active", "window": "outbreak", "quality": "ready"},
            _matcher_candidate(_snapshot_stale=True),
        )[0]

        assert (degraded.status, degraded.risk_level) == ("matched", "confirmed")
        assert (stale.status, stale.risk_level) == ("waiting", "watch")

    def test_implicit_style_and_window_use_their_own_field_quality(self):
        matcher = ModeMatcher([_rule(automation_level="automatic")])
        candidate = _matcher_candidate()
        market = {
            "style": "dual_active",
            "window": "outbreak",
            "quality": "degraded",
            "_feature_quality": {"style": "degraded", "window": "ready"},
        }

        assert matcher.evaluate(market, candidate)[0].status == "waiting"
        market["_feature_quality"] = {"style": "ready", "window": "ready"}
        assert matcher.evaluate(market, candidate)[0].status == "matched"

    @pytest.mark.parametrize("unknown", [None, "", "unknown", " UNKNOWN "])
    def test_unknown_implicit_market_state_waits_even_when_candidate_fails(
        self,
        unknown,
    ):
        matcher = ModeMatcher([_rule(automation_level="automatic")])
        market = {
            "style": unknown,
            "window": unknown,
            "_feature_quality": {"style": "ready", "window": "ready"},
        }

        row = matcher.evaluate(market, _matcher_candidate(flag=False))[0]

        assert (row.status, row.risk_level) == ("waiting", "watch")

    def test_unknown_market_state_makes_the_full_catalog_wait(self):
        market = {
            "style": "unknown",
            "window": "unknown",
            "_feature_quality": {"style": "missing", "window": "missing"},
        }
        catalog = _catalog_payload()

        rows = ModeMatcher(
            catalog["rules"],
            catalog_version=catalog["catalog_version"],
        ).evaluate(market, _matcher_candidate(flag=False))

        assert len(rows) == 19
        assert {row.status for row in rows} == {"waiting"}

    def test_known_implicit_market_mismatch_remains_not_matched(self):
        row = ModeMatcher([_rule(automation_level="automatic")]).evaluate(
            {
                "style": "board_flow",
                "window": "outbreak",
                "_feature_quality": {"style": "ready", "window": "ready"},
            },
            _matcher_candidate(),
        )[0]

        assert (row.status, row.risk_level) == ("not_matched", "avoid")


    def test_required_feature_quality_and_missing_trigger_price_wait(self):
        matcher = ModeMatcher([_rule()])
        market = {"style": "dual_active", "window": "outbreak", "quality": "ready"}

        degraded = matcher.evaluate(
            market,
            _matcher_candidate(_feature_quality={"flag": "degraded"}),
        )[0]
        no_price = matcher.evaluate(
            market,
            _matcher_candidate(reference_price=None),
        )[0]

        assert degraded.status == "waiting"
        assert no_price.status == "waiting"

    @pytest.mark.parametrize(
        ("role", "trigger_key"),
        [
            ("survivor", "price_lte"),
            ("leader", "sealed"),
            ("middle_army", "price_gte"),
        ],
    )
    def test_materializes_entry_invalidation_exit_and_hard_stop(self, role, trigger_key):
        row = ModeMatcher([_rule(role=role)]).evaluate(
            {"style": "dual_active", "window": "outbreak", "quality": "ready"},
            _matcher_candidate(),
        )[0]

        assert trigger_key in row.entry_trigger
        assert row.entry_trigger["reference_price"] == 10.0
        assert row.invalidation == {"label": "失效", "price_lte": 9.5}
        assert row.exit_trigger == {"label": "退出", "change_pct_lte": -5.0}
        assert any(item.get("hard_stop_price") == 9.5 for item in row.evidence)

    def test_only_ready_non_manual_preclose_matches_can_use_tail_scope(self):
        market = {"style": "dual_active", "window": "outbreak", "quality": "ready"}
        assisted = ModeMatcher([_rule()]).evaluate(
            market,
            _matcher_candidate(
                reference_price=9.6,
                tail_action_eligible=True,
            ),
        )[0]
        manual = ModeMatcher([_rule(automation_level="manual_only")]).evaluate(
            market,
            _matcher_candidate(
                reference_price=9.6,
                tail_action_eligible=True,
            ),
        )[0]

        assert assisted.action_scope == "tail"
        assert manual.action_scope == "target"

        auction = ModeMatcher([_rule()]).evaluate(
            market,
            _matcher_candidate(
                reference_price=9.6,
                tail_action_eligible=True,
                _stage="auction",
            ),
        )[0]
        assert auction.action_scope == "target"

    @pytest.mark.parametrize(
        "role",
        ["survivor", "resilient_core", "snake_arbitrage"],
    )
    @pytest.mark.parametrize(
        ("pullback", "quality"),
        [
            (9.5, "ready"),
            (9.4, "ready"),
            (10.01, "ready"),
            (9.6, "fallback"),
            (9.6, "invalid"),
        ],
    )
    def test_pullback_entry_requires_ready_price_strictly_above_stop(
        self,
        role,
        pullback,
        quality,
    ):
        row = ModeMatcher([_rule(role=role)]).evaluate(
            {"style": "dual_active", "window": "outbreak"},
            _matcher_candidate(
                reference_price=10,
                planned_pullback_price=pullback,
                planned_pullback_quality=quality,
                hard_stop_price=9.5,
            ),
        )[0]

        assert row.status == "waiting"
        assert "price_lte" not in row.entry_trigger

    @pytest.mark.parametrize(
        "role",
        ["survivor", "resilient_core", "snake_arbitrage"],
    )
    def test_pullback_entry_accepts_ready_price_between_stop_and_reference(self, role):
        row = ModeMatcher([_rule(role=role)]).evaluate(
            {"style": "dual_active", "window": "outbreak"},
            _matcher_candidate(
                reference_price=10,
                planned_pullback_price=9.6,
                planned_pullback_quality="ready",
                hard_stop_price=9.5,
            ),
        )[0]

        assert row.status == "matched"
        assert row.entry_trigger["price_lte"] == 9.6

    @pytest.mark.parametrize(
        ("role", "current_price", "sealed", "expected_scope"),
        [
            ("leader", 10.0, True, "tail"),
            ("leader", 10.0, False, "target"),
            ("survivor", 9.6, None, "tail"),
            ("survivor", 9.7, None, "target"),
            ("middle_army", 10.51, None, "tail"),
            ("middle_army", 10.50, None, "target"),
            ("survivor", 9.5, None, "target"),
        ],
    )
    def test_tail_scope_checks_each_materialized_rule_entry_and_stop(
        self,
        role,
        current_price,
        sealed,
        expected_scope,
    ):
        row = ModeMatcher([_rule(role=role)]).evaluate(
            {"style": "dual_active", "window": "outbreak"},
            _matcher_candidate(
                reference_price=current_price,
                planned_pullback_price=9.6,
                planned_breakout_price=10.51,
                hard_stop_price=9.5,
                tail_action_eligible=True,
                _current_sealed=sealed,
            ),
        )[0]

        assert row.action_scope == expected_scope

    def test_normalizes_rules_without_mutation_and_hashes_deterministically(self):
        original = _rule()
        reordered = {key: original[key] for key in reversed(original)}
        before = copy.deepcopy(original)

        first = ModeMatcher([original], catalog_version=7)
        second = ModeMatcher([reordered], catalog_version=7)

        assert original == before
        assert first.rule_snapshot() == second.rule_snapshot()
        assert first.rule_snapshot() == [
            {
                "mode_key": "example",
                "version": 7,
                "content_hash": first.rules[0]["content_hash"],
            }
        ]
        assert len(first.rules[0]["content_hash"]) == 64

    def test_uses_the_shared_catalog_hash_and_rejects_noncanonical_hashes(self):
        helper = getattr(
            rule_catalog_module,
            "canonical_rule_content_hash",
            None,
        )
        assert callable(helper)
        rule = _rule()
        expected = helper(rule)

        assert ModeMatcher([rule]).rules[0]["content_hash"] == expected
        assert ModeMatcher(
            [{**rule, "content_hash": expected}],
        ).rules[0]["content_hash"] == expected
        for bad_hash in (
            None,
            "",
            expected.upper(),
            "0" * 64,
            "g" * 64,
            "short",
        ):
            with pytest.raises(ValueError, match="content_hash"):
                ModeMatcher([{**rule, "content_hash": bad_hash}])

    @pytest.mark.parametrize(
        "mutate",
        [
            lambda rule: rule.update(requirements=[]),
            lambda rule: rule.update(requirements=["not-a-mapping"]),
            lambda rule: rule.update(
                requirements=[
                    {"feature": "other.flag", "op": "eq", "value": True}
                ]
            ),
            lambda rule: rule.update(
                requirements=[
                    {"feature": "candidate.", "op": "eq", "value": True}
                ]
            ),
            lambda rule: rule.update(
                requirements=[
                    {"feature": "candidate.flag", "op": "contains", "value": True}
                ]
            ),
            lambda rule: rule.update(
                requirements=[
                    {"feature": "candidate.flag", "op": "in", "value": []}
                ]
            ),
            lambda rule: rule.update(
                requirements=[
                    {"feature": "candidate.flag", "op": "in", "value": ""}
                ]
            ),
            lambda rule: rule.update(
                requirements=[
                    {"feature": "candidate.flag", "op": "eq", "value": []}
                ]
            ),
            lambda rule: rule.update(
                requirements=[
                    {"feature": "candidate.flag", "op": "gte", "value": "1"}
                ]
            ),
            lambda rule: rule.update(
                requirements=[
                    {"feature": "candidate.flag", "op": "in", "value": [math.nan]}
                ]
            ),
        ],
        ids=[
            "empty-requirements",
            "non-mapping-requirement",
            "unsupported-owner",
            "empty-feature-key",
            "unsupported-op",
            "empty-in-list",
            "empty-in-string",
            "malformed-eq-expected",
            "malformed-numeric-expected",
            "nonfinite-in-choice",
        ],
    )
    def test_rejects_malformed_automatic_rules_at_compile_time(self, mutate):
        rule = _rule(automation_level="automatic")
        mutate(rule)

        with pytest.raises(ValueError):
            ModeMatcher([rule])

    def test_rejects_duplicate_mode_keys(self):
        with pytest.raises(ValueError, match="duplicate mode_key"):
            ModeMatcher([_rule(), _rule()])

    def test_comma_windows_accept_either_value_and_reject_or_wait_other_values(self):
        matcher = ModeMatcher([_rule(window="outbreak,first_divergence")])
        candidate = _matcher_candidate()

        assert matcher.evaluate(
            {"style": "dual_active", "window": "first_divergence", "quality": "ready"},
            candidate,
        )[0].status == "matched"
        assert matcher.evaluate(
            {"style": "dual_active", "window": "decline", "quality": "ready"},
            candidate,
        )[0].status == "not_matched"
        assert matcher.evaluate(
            {"style": "dual_active", "quality": "ready"},
            candidate,
        )[0].status == "waiting"

    def test_evaluation_is_deterministic_sorted_and_does_not_mutate_inputs(self):
        rules = [_rule("low", role="leader"), _rule("high", role="leader")]
        rules[0]["priority"] = 1
        rules[1]["priority"] = 2
        candidate = _matcher_candidate()
        market = {"style": "dual_active", "window": "outbreak", "quality": "ready"}
        before_candidate = copy.deepcopy(candidate)
        before_market = copy.deepcopy(market)

        first = ModeMatcher(rules).evaluate(market, candidate)
        second = ModeMatcher(list(reversed(rules))).evaluate(market, candidate)

        assert [row.mode_key for row in first] == ["high", "low"]
        assert first == second
        assert candidate == before_candidate
        assert market == before_market

    @pytest.mark.parametrize(
        ("op", "actual", "expected", "status"),
        [
            ("eq", "x", "x", "matched"),
            ("in", "x", ["x", "y"], "matched"),
            ("in", "", ["x", "y"], "waiting"),
            ("in", True, [1, 2], "waiting"),
            ("lte", 2, 2, "matched"),
            ("gte", 2, 2, "matched"),
            ("lte", 3, 2, "not_matched"),
            ("gte", 1, 2, "not_matched"),
            ("gte", True, 1, "waiting"),
            ("lte", math.inf, 2, "waiting"),
        ],
    )
    def test_supported_operators_are_strict_and_numeric_safe(
        self,
        op,
        actual,
        expected,
        status,
    ):
        rule = _rule(
            requirements=[
                {"feature": "candidate.value", "op": op, "value": expected}
            ]
        )
        row = ModeMatcher([rule]).evaluate(
            {"style": "dual_active", "window": "outbreak", "quality": "ready"},
            _matcher_candidate(value=actual),
        )[0]
        assert row.status == status


class TestRealTailChain:
    @staticmethod
    def _evaluate(role: str, *, price: float, sealed: bool):
        raw = _candidate(
            features={
                "price": price,
                "captured_at": AS_OF,
                "realtime_limit_up_fact": {"sealed": sealed},
                "validated_support": 9.6,
                "prior_n_day_high": 10.5,
            },
            evidence=[
                _source_evidence("tencent", as_of=AS_OF),
                _source_evidence(
                    "realtime_limit_up_pool",
                    as_of=AS_OF,
                    fields=["realtime_limit_up_fact"],
                ),
                _source_evidence(
                    "kline",
                    as_of=AS_OF,
                    fields=["validated_support", "prior_n_day_high"],
                ),
            ],
        )
        snapshot = _snapshot(
            raw,
            market_features={
                "style": "dual_active",
                "window": "outbreak",
                "quality": "degraded",
            },
            quality="degraded",
        )
        enriched = MarketStateAnalyzer().enrich_snapshot(snapshot)
        analyzed = enriched.candidates[0]
        features = ModeFeatureBuilder().build(enriched, analyzed)
        candidate = CandidateSnapshot(
            stock_code=analyzed.stock_code,
            stock_name=analyzed.stock_name,
            theme_name=analyzed.theme_name,
            features={**features, "flag": True},
            evidence=copy.deepcopy(analyzed.evidence),
        )
        return ModeMatcher([_rule(role=role)]).evaluate(
            {
                "style": "dual_active",
                "window": "outbreak",
                "quality": "degraded",
            },
            candidate,
        )[0]

    def test_snapshot_analyzer_builder_matcher_tail_uses_current_sealed(self):
        assert self._evaluate("leader", price=10, sealed=True).action_scope == "tail"
        assert self._evaluate("leader", price=10, sealed=False).action_scope == "target"

    def test_snapshot_analyzer_builder_matcher_tail_uses_current_price(self):
        assert self._evaluate("survivor", price=9.6, sealed=False).action_scope == "tail"
        assert self._evaluate("survivor", price=9.7, sealed=False).action_scope == "target"


def _catalog_payload() -> dict:
    return json.loads(CATALOG_PATH.read_text(encoding="utf-8"))


def _satisfying_value(requirement: dict):
    return copy.deepcopy(requirement["value"])


def _failing_value(requirement: dict):
    expected = requirement["value"]
    op = requirement["op"]
    if op == "eq":
        if isinstance(expected, bool):
            return not expected
        if isinstance(expected, str):
            return f"not-{expected}"
        return expected + 1
    if op == "lte":
        return float(expected) + 100
    if op == "gte":
        return float(expected) - 1
    if op == "in":
        return "definitely-not-in-set"
    raise AssertionError(op)


def _catalog_case(rule: dict) -> tuple[dict, CandidateSnapshot]:
    market = {
        "style": rule["style"],
        "window": rule["window"].split(",")[0],
        "quality": "ready",
    }
    candidate = _matcher_candidate()
    for requirement in rule["requirements"]:
        owner, key = requirement["feature"].split(".", 1)
        if owner == "market":
            market[key] = _satisfying_value(requirement)
        else:
            candidate.features[key] = _satisfying_value(requirement)
    return market, candidate


class TestRealCatalogCoverage:
    @pytest.mark.parametrize(
        "rule",
        _catalog_payload()["rules"],
        ids=lambda rule: rule["mode_key"],
    )
    def test_each_transcript_rule_has_positive_missing_and_failed_paths(self, rule):
        market, candidate = _catalog_case(rule)
        matcher = ModeMatcher([rule], catalog_version=_catalog_payload()["catalog_version"])

        positive = matcher.evaluate(market, candidate)[0]
        expected_status = (
            "manual_review"
            if rule["automation_level"] == "manual_only"
            else "matched"
        )
        assert positive.status == expected_status
        assert positive.risk_level == {
            "manual_only": "watch",
            "assisted": "trial",
            "automatic": "confirmed",
        }[rule["automation_level"]]

        candidate_requirement = next(
            item
            for item in rule["requirements"]
            if item["feature"].startswith("candidate.")
        )
        key = candidate_requirement["feature"].split(".", 1)[1]
        missing_candidate = copy.deepcopy(candidate)
        missing_candidate.features[key] = None
        assert matcher.evaluate(market, missing_candidate)[0].status == "waiting"

        failed_candidate = copy.deepcopy(candidate)
        failed_candidate.features[key] = _failing_value(candidate_requirement)
        assert matcher.evaluate(market, failed_candidate)[0].status == "not_matched"

    def test_complete_catalog_snapshot_is_sorted_and_has_nineteen_unique_hashes(self):
        payload = _catalog_payload()
        matcher = ModeMatcher(payload["rules"], catalog_version=payload["catalog_version"])

        snapshot = matcher.rule_snapshot()

        assert len(snapshot) == 19
        assert [item["mode_key"] for item in snapshot] == sorted(
            item["mode_key"] for item in snapshot
        )
        assert len({item["mode_key"] for item in snapshot}) == 19
        assert len({item["content_hash"] for item in snapshot}) == 19
        assert all(item["version"] == 1 for item in snapshot)
        assert all(len(item["content_hash"]) == 64 for item in snapshot)
