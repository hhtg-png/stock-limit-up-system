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
from app.services.trading_playbook.mode_features import FEATURE_KEYS, ModeFeatureBuilder
from app.services.trading_playbook.mode_matcher import ModeMatcher


CATALOG_PATH = Path("app/data/trading_playbook_rules_v1.json")
AS_OF = datetime(2026, 7, 10, 14, 40)


def _candidate(
    code: str = "000001",
    *,
    theme: str = "机器人",
    features: dict | None = None,
    evidence: list | None = None,
) -> CandidateSnapshot:
    return CandidateSnapshot(
        stock_code=code,
        stock_name=f"样本{code}",
        theme_name=theme,
        features=copy.deepcopy(features or {}),
        evidence=copy.deepcopy(evidence or []),
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
            evidence=_ready_quote_evidence(),
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
            evidence=_ready_quote_evidence(),
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
            evidence=_ready_quote_evidence(),
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
            evidence=_ready_quote_evidence(),
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
            evidence=_ready_quote_evidence(),
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
            evidence=_ready_quote_evidence(),
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
                    "outbreak_start_seconds": 36000,
                }
            ],
        )

        result = ModeFeatureBuilder().build(snapshot, candidate)

        assert result["started_before_theme"] is True
        assert result["low_position_new_start"] is True
        assert result["supplement"] is True
        assert result["turn_confirmed"] is True

    def test_former_high_position_can_be_proven_by_yesterday_review_height(self):
        survivor = _candidate(
            features={
                "price": 10,
                "captured_at": AS_OF,
                "recognition_rank": 1,
            },
            evidence=_ready_quote_evidence(),
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
            evidence=_ready_quote_evidence(),
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
            evidence=_ready_quote_evidence(),
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

    def test_snake_and_right_reversal_wait_without_their_distinct_theme_evidence(self):
        candidate = _candidate(
            features={
                "price": 10,
                "captured_at": AS_OF,
                "snake_pattern": True,
                "right_side_breakout": True,
            },
            evidence=_ready_quote_evidence(),
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
            evidence=_ready_quote_evidence(),
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

    def test_tail_action_requires_preclose_fresh_entry_not_invalidated_and_non_manual(self):
        candidate = _candidate(
            features={
                "price": 10,
                "captured_at": AS_OF - timedelta(seconds=30),
                "tail_entry_satisfied": True,
                "tail_invalidation_satisfied": False,
                "automation_level": "assisted",
            },
            evidence=_ready_quote_evidence(AS_OF - timedelta(seconds=30)),
        )
        builder = ModeFeatureBuilder()

        assert builder.build(
            _snapshot(candidate, stage="preclose"), candidate
        )["tail_action_eligible"] is True
        assert builder.build(
            _snapshot(candidate, stage="auction"), candidate
        )["tail_action_eligible"] is False
        candidate.features["automation_level"] = "manual_only"
        assert builder.build(
            _snapshot(candidate, stage="preclose"), candidate
        )["tail_action_eligible"] is False

    def test_tail_base_eligibility_can_defer_automation_level_to_the_rule_matcher(self):
        candidate = _candidate(
            features={
                "price": 10,
                "captured_at": AS_OF,
                "tail_entry_satisfied": True,
                "tail_invalidation_satisfied": False,
            },
            evidence=_ready_quote_evidence(),
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

    def test_aware_utc_future_evidence_is_compared_in_shanghai_time(self):
        captured_at = datetime(2026, 7, 10, 6, 41, tzinfo=timezone.utc)
        candidate = _candidate(
            features={"price": 10, "captured_at": captured_at},
            evidence=_ready_quote_evidence(captured_at),
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
        "planned_pullback_price": 9.5,
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

    def test_degraded_or_stale_quality_never_produces_actionable_match(self):
        matcher = ModeMatcher([_rule(automation_level="automatic")])

        degraded = matcher.evaluate(
            {"style": "dual_active", "window": "outbreak", "quality": "degraded"},
            _matcher_candidate(),
        )[0]
        stale = matcher.evaluate(
            {"style": "dual_active", "window": "outbreak", "quality": "ready"},
            _matcher_candidate(_snapshot_stale=True),
        )[0]

        assert (degraded.status, degraded.risk_level) == ("waiting", "watch")
        assert (stale.status, stale.risk_level) == ("waiting", "watch")

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
            _matcher_candidate(tail_action_eligible=True),
        )[0]
        manual = ModeMatcher([_rule(automation_level="manual_only")]).evaluate(
            market,
            _matcher_candidate(tail_action_eligible=True),
        )[0]

        assert assisted.action_scope == "tail"
        assert manual.action_scope == "target"

        auction = ModeMatcher([_rule()]).evaluate(
            market,
            _matcher_candidate(tail_action_eligible=True, _stage="auction"),
        )[0]
        assert auction.action_scope == "target"

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
