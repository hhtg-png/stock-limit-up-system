import copy
import importlib
import io
import json
import math
import re
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from unittest.mock import patch

from app.services.trading_playbook.rule_catalog import (
    canonical_rule_content_hash,
)


BACKEND_ROOT = Path(__file__).resolve().parents[1]
REPLAY_MODULE = BACKEND_ROOT / "app" / "scripts" / "replay_trading_playbook.py"
SCENARIO_FIXTURE = (
    BACKEND_ROOT / "tests" / "fixtures" / "trading_playbook_scenarios.json"
)
CATALOG_PATH = BACKEND_ROOT / "app" / "data" / "trading_playbook_rules_v1.json"

EXPECTED_SCENARIOS = {
    "new_theme_high_volatility": (
        "outbreak",
        {"high_volatility": True, "theme_rank": 1},
        "matched",
    ),
    "new_theme_high_position": (
        "outbreak",
        {"high_position": True},
        "matched",
    ),
    "new_theme_same_level_turnover": (
        "outbreak",
        {"same_level_turnover": True},
        "matched",
    ),
    "big_middle_army_transition": (
        "first_divergence",
        {"middle_army": True, "theme_rank": 1},
        "matched",
    ),
    "first_mover_leader": (
        "first_divergence",
        {"started_before_theme": True, "recognition_rank": 1},
        "matched",
    ),
    "unique_survivor_trial": (
        "divergence_exhaustion",
        {"unique_survivor": True},
        "matched",
    ),
    "leader_turn_two": (
        "divergence_to_consensus",
        {"turn_confirmed": True, "recognition_rank": 1},
        "matched",
    ),
    "leader_stronger_confirmation": (
        "stronger_confirmation",
        {"stronger_confirmed": True},
        "matched",
    ),
    "leader_acceleration_to_divergence": (
        "second_divergence",
        {"confirmed_leader": True, "acceleration_to_divergence": True},
        "matched",
    ),
    "stage_three_high_low_switch": (
        "stage_three",
        {"low_position_new_start": True},
        "matched",
    ),
    "stage_transition_supplement": (
        "stage_three",
        {"supplement": True},
        "matched",
    ),
    "leader_first_bearish_rebound": (
        "stage_three",
        {"confirmed_leader": True, "first_bearish": True},
        "manual_review",
    ),
    "trend_core_pullback": (
        "first_divergence",
        {"trend_established": True, "resilience_rank": 1, "pullback": True},
        "matched",
    ),
    "trend_consolidation_rebreak": (
        "divergence_to_consensus",
        {"consolidation_rebreak": True, "linkage_confirmed": True},
        "matched",
    ),
    "trend_turn_two": (
        "divergence_to_consensus",
        {"trend_turn_two": True, "middle_army_linkage": True},
        "matched",
    ),
    "resilient_core_exhaustion": (
        "divergence_exhaustion",
        {"divergence_days": 3, "resilience_rank": 1},
        "matched",
    ),
    "alive_theme_snake_arbitrage": (
        "divergence_exhaustion",
        {"theme_alive": True, "snake_setup": True},
        "manual_review",
    ),
    "dead_pile_right_confirmation": (
        "divergence_to_consensus",
        {"theme_dead": True, "right_reversal": True},
        "manual_review",
    ),
    "external_high_low_switch": (
        "stage_three",
        {"external_switch": True},
        "manual_review",
    ),
}


class ReplayArtifactsTest(unittest.TestCase):
    def test_replay_artifacts_exist(self):
        self.assertTrue(REPLAY_MODULE.is_file())
        self.assertTrue(SCENARIO_FIXTURE.is_file())


class GoldenScenarioFixtureTest(unittest.TestCase):
    def test_fixture_has_one_source_backed_scenario_per_catalog_mode(self):
        catalog = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))
        payload = json.loads(SCENARIO_FIXTURE.read_text(encoding="utf-8"))
        self.assertIn("scenarios", payload)
        rules = {rule["mode_key"]: rule for rule in catalog["rules"]}
        scenarios = payload["scenarios"]
        mode_keys = [scenario["mode_key"] for scenario in scenarios]

        self.assertEqual(len(scenarios), 19)
        self.assertEqual(len(mode_keys), len(set(mode_keys)))
        self.assertEqual(set(mode_keys), set(rules))
        for scenario in scenarios:
            rule = rules[scenario["mode_key"]]
            self.assertEqual(scenario["source_refs"], rule["source_refs"])
            self.assertEqual(scenario["market_features"]["style"], rule["style"])
            self.assertEqual(
                scenario.get("rule_hash"),
                canonical_rule_content_hash(rule),
            )
            self.assertTrue(scenario["facts"])
            features = scenario["candidate"]["features"]
            for field in (
                "planned_pullback_price",
                "planned_breakout_price",
                "hard_stop_price",
            ):
                self.assertGreater(features[field], 0)

    def test_all_golden_scenarios_use_minimum_facts_and_real_matcher(self):
        payload = json.loads(SCENARIO_FIXTURE.read_text(encoding="utf-8"))
        replay_module = importlib.import_module(
            "app.scripts.replay_trading_playbook"
        )
        replay_scenario = getattr(replay_module, "replay_scenario", None)
        self.assertTrue(callable(replay_scenario))

        for scenario in payload["scenarios"]:
            window, candidate_minimum, expected = EXPECTED_SCENARIOS[
                scenario["mode_key"]
            ]
            self.assertEqual(scenario["market_features"]["window"], window)
            for key, value in candidate_minimum.items():
                self.assertEqual(scenario["candidate"]["features"][key], value)
            self.assertEqual(scenario["expected"], expected)
            self.assertEqual(
                replay_scenario(scenario, catalog_path=CATALOG_PATH),
                expected,
            )


class ReplayValidationTest(unittest.TestCase):
    def setUp(self):
        payload = json.loads(SCENARIO_FIXTURE.read_text(encoding="utf-8"))
        self.scenario = payload["scenarios"][0]
        self.replay_scenario = importlib.import_module(
            "app.scripts.replay_trading_playbook"
        ).replay_scenario

    def test_rejects_a_fact_captured_after_as_of(self):
        future_instants = (
            "2026-07-10T14:40:01+08:00",
            "2026-07-10T06:40:01+00:00",
        )
        for captured_at in future_instants:
            with self.subTest(captured_at=captured_at):
                scenario = copy.deepcopy(self.scenario)
                scenario["facts"][0]["captured_at"] = captured_at
                with self.assertRaisesRegex(ValueError, "^future fact$"):
                    self.replay_scenario(
                        scenario,
                        catalog_path=CATALOG_PATH,
                    )

    def test_rejects_scenario_without_facts(self):
        scenario = copy.deepcopy(self.scenario)
        scenario["facts"] = []

        with self.assertRaisesRegex(ValueError, "facts"):
            self.replay_scenario(scenario, catalog_path=CATALOG_PATH)

    def test_rejects_source_refs_not_derived_from_exact_rule(self):
        scenario = copy.deepcopy(self.scenario)
        scenario["source_refs"][0]["excerpt"] = "伪造的文字稿摘要"

        with self.assertRaisesRegex(ValueError, "source_refs"):
            self.replay_scenario(scenario, catalog_path=CATALOG_PATH)

    def test_rejects_fact_value_that_disagrees_with_declared_features(self):
        scenario = copy.deepcopy(self.scenario)
        scenario["facts"][0]["candidate_features"]["high_volatility"] = False

        with self.assertRaisesRegex(ValueError, "feature map mismatch"):
            self.replay_scenario(scenario, catalog_path=CATALOG_PATH)

    def test_rejects_scenario_when_required_feature_has_no_fact(self):
        scenario = copy.deepcopy(self.scenario)
        del scenario["facts"][0]["candidate_features"]["high_volatility"]

        with self.assertRaisesRegex(ValueError, "feature map mismatch"):
            self.replay_scenario(scenario, catalog_path=CATALOG_PATH)

    def test_rejects_fact_not_tied_to_catalog_sources(self):
        scenario = copy.deepcopy(self.scenario)
        scenario["facts"][0]["source_keys"] = ["invented-source"]

        with self.assertRaisesRegex(ValueError, "fact source_keys"):
            self.replay_scenario(scenario, catalog_path=CATALOG_PATH)

    def test_unfacted_matcher_inputs_cannot_change_actual_status(self):
        mutations = (
            ("reference_price", 0.0),
            ("_snapshot_stale", True),
            ("exit_change_pct_floor", -101.0),
        )
        for key, value in mutations:
            with self.subTest(key=key):
                scenario = copy.deepcopy(self.scenario)
                scenario["candidate"]["features"][key] = value
                with self.assertRaisesRegex(ValueError, "feature map mismatch"):
                    self.replay_scenario(
                        scenario,
                        catalog_path=CATALOG_PATH,
                    )

    def test_declared_and_fact_feature_maps_must_match_exactly(self):
        variants = []
        declared_more = copy.deepcopy(self.scenario)
        declared_more["market_features"]["unfacted"] = True
        variants.append(declared_more)
        declared_less = copy.deepcopy(self.scenario)
        del declared_less["market_features"]["quality"]
        variants.append(declared_less)
        fact_extra = copy.deepcopy(self.scenario)
        fact_extra["facts"][0]["market_features"]["invented"] = True
        variants.append(fact_extra)

        for index, scenario in enumerate(variants):
            with self.subTest(index=index):
                with self.assertRaisesRegex(ValueError, "feature map mismatch"):
                    self.replay_scenario(
                        scenario,
                        catalog_path=CATALOG_PATH,
                    )

    def test_boolean_and_numeric_feature_values_are_not_interchangeable(self):
        scenario = copy.deepcopy(self.scenario)
        scenario["facts"][0]["candidate_features"]["high_volatility"] = 1

        with self.assertRaisesRegex(ValueError, "feature map mismatch"):
            self.replay_scenario(scenario, catalog_path=CATALOG_PATH)

    def test_conflicting_facts_fail_closed(self):
        scenario = copy.deepcopy(self.scenario)
        conflicting = copy.deepcopy(scenario["facts"][0])
        conflicting["captured_at"] = "2026-07-10T14:39:31+08:00"
        conflicting["candidate_features"]["high_volatility"] = False
        scenario["facts"].append(conflicting)

        with self.assertRaisesRegex(ValueError, "conflicting facts"):
            self.replay_scenario(scenario, catalog_path=CATALOG_PATH)

    def test_rejects_invalid_or_timezone_naive_timestamps(self):
        mutations = (
            ("as_of", "not-an-iso-timestamp"),
            ("as_of", "2026-07-10T14:40:00"),
            ("fact", "not-an-iso-timestamp"),
            ("fact", "2026-07-10T14:39:30"),
        )
        for target, value in mutations:
            with self.subTest(target=target, value=value):
                scenario = copy.deepcopy(self.scenario)
                if target == "as_of":
                    scenario["as_of"] = value
                else:
                    scenario["facts"][0]["captured_at"] = value
                with self.assertRaises(ValueError):
                    self.replay_scenario(scenario, catalog_path=CATALOG_PATH)

    def test_rejects_unknown_mode(self):
        scenario = copy.deepcopy(self.scenario)
        scenario["mode_key"] = "unknown_mode"

        with self.assertRaisesRegex(ValueError, "unknown mode"):
            self.replay_scenario(scenario, catalog_path=CATALOG_PATH)

    def test_replay_rejects_same_version_catalog_content_drift(self):
        catalog = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))
        scenario = copy.deepcopy(self.scenario)
        original_rule = next(
            rule
            for rule in catalog["rules"]
            if rule["mode_key"] == scenario["mode_key"]
        )
        scenario["rule_hash"] = canonical_rule_content_hash(original_rule)
        catalog["rules"][0]["entry"]["label"] = "漂移但不改变匹配状态"

        with tempfile.TemporaryDirectory() as directory:
            catalog_path = Path(directory) / "drifted-catalog.json"
            catalog_path.write_text(
                json.dumps(catalog, ensure_ascii=False),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "rule_hash"):
                self.replay_scenario(
                    scenario,
                    catalog_path=catalog_path,
                )

    def test_all_planned_prices_must_be_positive_finite_real_numbers(self):
        fields = (
            "planned_pullback_price",
            "planned_breakout_price",
            "hard_stop_price",
        )
        invalid_values = (0, -1, math.nan, math.inf, True, "10.0")
        for field in fields:
            for value in invalid_values:
                with self.subTest(field=field, value=value):
                    scenario = copy.deepcopy(self.scenario)
                    scenario["candidate"]["features"][field] = value
                    scenario["facts"][0]["candidate_features"][field] = value
                    with self.assertRaisesRegex(
                        ValueError,
                        "positive finite price",
                    ):
                        self.replay_scenario(
                            scenario,
                            catalog_path=CATALOG_PATH,
                        )

    def test_replay_strictly_preflights_duplicate_catalog_keys(self):
        raw_catalog = CATALOG_PATH.read_text(encoding="utf-8")
        raw_catalog = raw_catalog.replace(
            '"priority": 100,',
            '"priority": 100, "priority": 100,',
            1,
        )

        with tempfile.TemporaryDirectory() as directory:
            catalog_path = Path(directory) / "duplicate-catalog.json"
            catalog_path.write_text(raw_catalog, encoding="utf-8")
            with self.assertRaisesRegex(
                ValueError,
                r"duplicate-catalog\.json.*duplicate key.*priority",
            ):
                self.replay_scenario(
                    copy.deepcopy(self.scenario),
                    catalog_path=catalog_path,
                )

    def test_direct_mapping_rejects_non_json_values_with_full_path(self):
        mode_context = r"scenario\[new_theme_high_volatility\]"
        cases = (
            (
                "nested-infinity",
                "candidate",
                "nested",
                {"bad": math.inf},
                rf"{mode_context}\.candidate\.features\.nested\.bad.*non-finite",
            ),
            (
                "nested-negative-infinity",
                "market",
                "nested",
                {"bad": -math.inf},
                rf"{mode_context}\.market_features\.nested\.bad.*non-finite",
            ),
            (
                "tuple",
                "candidate",
                "tuple_value",
                (1, 2),
                rf"{mode_context}\.candidate\.features\.tuple_value.*tuple",
            ),
            (
                "bytes",
                "candidate",
                "bytes_value",
                b"not-json",
                rf"{mode_context}\.candidate\.features\.bytes_value.*bytes",
            ),
            (
                "object",
                "candidate",
                "object_value",
                object(),
                rf"{mode_context}\.candidate\.features\.object_value.*object",
            ),
        )

        for label, owner, key, value, message in cases:
            with self.subTest(label=label):
                scenario = copy.deepcopy(self.scenario)
                if owner == "candidate":
                    scenario["candidate"]["features"][key] = value
                    scenario["facts"][0]["candidate_features"][key] = value
                else:
                    scenario["market_features"][key] = value
                    scenario["facts"][0]["market_features"][key] = value
                with self.assertRaisesRegex(ValueError, message):
                    self.replay_scenario(
                        scenario,
                        catalog_path=CATALOG_PATH,
                    )

        scenario = copy.deepcopy(self.scenario)
        scenario["market_features"][1] = "not-a-string-key"
        scenario["facts"][0]["market_features"][1] = "not-a-string-key"
        with self.assertRaisesRegex(
            ValueError,
            rf"{mode_context}\.market_features.*non-string key.*1",
        ):
            self.replay_scenario(scenario, catalog_path=CATALOG_PATH)

    def test_matcher_control_schema_rejects_unsafe_values(self):
        cases = (
            ("candidate", "_snapshot_stale", 1),
            ("candidate", "_snapshot_stale", math.nan),
            ("candidate", "_point_in_time_valid", 1),
            ("candidate", "_point_in_time_valid", math.nan),
            ("candidate", "_feature_quality", []),
            ("market", "_feature_quality", []),
            ("candidate", "_feature_quality", None),
            ("candidate", "_feature_quality", {"": "ready"}),
            ("candidate", "_feature_quality", {"flag": []}),
            ("candidate", "_feature_quality", {"flag": "unknown"}),
            ("candidate", "quality", []),
            ("market", "quality", []),
            ("candidate", "planned_pullback_quality", []),
            ("candidate", "_stage", "midday"),
            ("candidate", "_stage", []),
            ("candidate", "tail_action_eligible", 1),
        )
        mode = "new_theme_high_volatility"

        for owner, key, value in cases:
            with self.subTest(owner=owner, key=key, value=value):
                scenario = copy.deepcopy(self.scenario)
                if owner == "candidate":
                    scenario["candidate"]["features"][key] = value
                    scenario["facts"][0]["candidate_features"][key] = value
                    field_path = f"scenario[{mode}].candidate.features.{key}"
                else:
                    scenario["market_features"][key] = value
                    scenario["facts"][0]["market_features"][key] = value
                    field_path = f"scenario[{mode}].market_features.{key}"
                with self.assertRaisesRegex(
                    ValueError,
                    re.escape(field_path),
                ):
                    self.replay_scenario(
                        scenario,
                        catalog_path=CATALOG_PATH,
                    )

    def test_valid_matcher_controls_preserve_real_matcher_behavior(self):
        stale = copy.deepcopy(self.scenario)
        stale["candidate"]["features"]["_snapshot_stale"] = True
        stale["facts"][0]["candidate_features"]["_snapshot_stale"] = True
        self.assertEqual(
            self.replay_scenario(stale, catalog_path=CATALOG_PATH),
            "waiting",
        )

        point_invalid = copy.deepcopy(self.scenario)
        point_invalid["candidate"]["features"]["_point_in_time_valid"] = False
        point_invalid["facts"][0]["candidate_features"][
            "_point_in_time_valid"
        ] = False
        self.assertEqual(
            self.replay_scenario(point_invalid, catalog_path=CATALOG_PATH),
            "waiting",
        )

        ready = copy.deepcopy(self.scenario)
        ready["candidate"]["features"].update(
            {
                "_feature_quality": {"high_volatility": "ready"},
                "_stage": "auction",
                "tail_action_eligible": False,
            }
        )
        ready["facts"][0]["candidate_features"].update(
            {
                "_feature_quality": {"high_volatility": "ready"},
                "_stage": "auction",
                "tail_action_eligible": False,
            }
        )
        self.assertEqual(
            self.replay_scenario(ready, catalog_path=CATALOG_PATH),
            "matched",
        )

    def test_direct_scenario_shape_errors_include_mode_and_field_path(self):
        mode = "new_theme_high_volatility"
        cases = []
        cases.append((None, r"scenario\[unknown\].*object required"))
        cases.append(([], r"scenario\[unknown\].*object required"))
        cases.append(({}, r"scenario\[unknown\]\.mode_key"))

        missing_market = copy.deepcopy(self.scenario)
        del missing_market["market_features"]
        cases.append(
            (missing_market, rf"scenario\[{mode}\]\.market_features")
        )
        null_market = copy.deepcopy(self.scenario)
        null_market["market_features"] = None
        cases.append((null_market, rf"scenario\[{mode}\]\.market_features"))

        missing_candidate = copy.deepcopy(self.scenario)
        del missing_candidate["candidate"]
        cases.append((missing_candidate, rf"scenario\[{mode}\]\.candidate"))
        null_candidate = copy.deepcopy(self.scenario)
        null_candidate["candidate"] = None
        cases.append((null_candidate, rf"scenario\[{mode}\]\.candidate"))
        missing_features = copy.deepcopy(self.scenario)
        del missing_features["candidate"]["features"]
        cases.append(
            (
                missing_features,
                rf"scenario\[{mode}\]\.candidate\.features",
            )
        )
        blank_stock = copy.deepcopy(self.scenario)
        blank_stock["candidate"]["stock_code"] = ""
        cases.append(
            (blank_stock, rf"scenario\[{mode}\]\.candidate\.stock_code")
        )
        bad_fact = copy.deepcopy(self.scenario)
        bad_fact["facts"][0] = None
        cases.append((bad_fact, rf"scenario\[{mode}\]\.facts\[0\]"))
        bad_source_keys = copy.deepcopy(self.scenario)
        bad_source_keys["facts"][0]["source_keys"] = None
        cases.append(
            (
                bad_source_keys,
                rf"scenario\[{mode}\]\.facts\[0\]\.source_keys",
            )
        )
        bad_source_refs = copy.deepcopy(self.scenario)
        bad_source_refs["source_refs"] = None
        cases.append(
            (bad_source_refs, rf"scenario\[{mode}\]\.source_refs")
        )

        for index, (scenario, message) in enumerate(cases):
            with self.subTest(index=index):
                with self.assertRaisesRegex(ValueError, message):
                    self.replay_scenario(
                        scenario,
                        catalog_path=CATALOG_PATH,
                    )


class ScenarioLoaderTest(unittest.TestCase):
    def test_fixture_rejects_nonfinite_constants_and_nested_duplicate_keys(self):
        raw_fixture = SCENARIO_FIXTURE.read_text(encoding="utf-8")
        mutations = (
            (
                "NaN",
                raw_fixture.replace(
                    '"planned_pullback_price": 10.0',
                    '"planned_pullback_price": NaN',
                    1,
                ),
                r"fixture-NaN\.json.*constant.*NaN",
            ),
            (
                "Infinity",
                raw_fixture.replace(
                    '"planned_pullback_price": 10.0',
                    '"planned_pullback_price": Infinity',
                    1,
                ),
                r"fixture-Infinity\.json.*constant.*Infinity",
            ),
            (
                "negative-infinity",
                raw_fixture.replace(
                    '"planned_pullback_price": 10.0',
                    '"planned_pullback_price": -Infinity',
                    1,
                ),
                r"fixture-negative-infinity\.json.*constant.*-Infinity",
            ),
            (
                "duplicate",
                raw_fixture.replace(
                    '"market_features": {"style": "dual_active",',
                    '"market_features": {"style": "dual_active", '
                    '"style": "dual_active",',
                    1,
                ),
                r"fixture-duplicate\.json.*duplicate key.*style",
            ),
        )

        for label, content, message in mutations:
            with (
                self.subTest(label=label),
                tempfile.TemporaryDirectory() as directory,
            ):
                fixture_path = Path(directory) / f"fixture-{label}.json"
                fixture_path.write_text(content, encoding="utf-8")
                with self.assertRaisesRegex(ValueError, message):
                    importlib.import_module(
                        "app.scripts.replay_trading_playbook"
                    ).load_scenarios(
                        fixture_path=fixture_path,
                        catalog_path=CATALOG_PATH,
                    )

    def test_loader_strictly_preflights_nonfinite_catalog_constant(self):
        raw_catalog = CATALOG_PATH.read_text(encoding="utf-8").replace(
            '"priority": 100,',
            '"priority": NaN,',
            1,
        )

        with tempfile.TemporaryDirectory() as directory:
            catalog_path = Path(directory) / "constant-catalog.json"
            catalog_path.write_text(raw_catalog, encoding="utf-8")
            with self.assertRaisesRegex(
                ValueError,
                r"constant-catalog\.json.*constant.*NaN",
            ):
                importlib.import_module(
                    "app.scripts.replay_trading_playbook"
                ).load_scenarios(
                    fixture_path=SCENARIO_FIXTURE,
                    catalog_path=catalog_path,
                )

    def test_rejects_duplicate_mode_scenario(self):
        replay_module = importlib.import_module(
            "app.scripts.replay_trading_playbook"
        )
        load_scenarios = getattr(replay_module, "load_scenarios", None)
        self.assertTrue(callable(load_scenarios))
        payload = json.loads(SCENARIO_FIXTURE.read_text(encoding="utf-8"))
        payload["scenarios"][-1] = copy.deepcopy(payload["scenarios"][0])

        with tempfile.TemporaryDirectory() as directory:
            fixture_path = Path(directory) / "duplicate.json"
            fixture_path.write_text(
                json.dumps(payload, ensure_ascii=False),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "duplicate mode_key"):
                load_scenarios(
                    fixture_path=fixture_path,
                    catalog_path=CATALOG_PATH,
                )

    def test_rejects_fixture_for_different_catalog_version(self):
        replay_module = importlib.import_module(
            "app.scripts.replay_trading_playbook"
        )
        payload = json.loads(SCENARIO_FIXTURE.read_text(encoding="utf-8"))
        payload["catalog_version"] = 2

        with tempfile.TemporaryDirectory() as directory:
            fixture_path = Path(directory) / "wrong-version.json"
            fixture_path.write_text(
                json.dumps(payload, ensure_ascii=False),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "catalog_version"):
                replay_module.load_scenarios(
                    fixture_path=fixture_path,
                    catalog_path=CATALOG_PATH,
                )

    def test_rejects_missing_or_extra_catalog_mode(self):
        replay_module = importlib.import_module(
            "app.scripts.replay_trading_playbook"
        )
        original = json.loads(SCENARIO_FIXTURE.read_text(encoding="utf-8"))
        missing = copy.deepcopy(original)
        missing["scenarios"].pop()
        extra = copy.deepcopy(original)
        invented = copy.deepcopy(extra["scenarios"][0])
        invented["mode_key"] = "invented_mode"
        invented["scenario_id"] = "invented-mode-golden"
        extra["scenarios"].append(invented)

        for index, payload in enumerate((missing, extra)):
            with (
                self.subTest(index=index),
                tempfile.TemporaryDirectory() as directory,
            ):
                fixture_path = Path(directory) / "coverage.json"
                fixture_path.write_text(
                    json.dumps(payload, ensure_ascii=False),
                    encoding="utf-8",
                )
                with self.assertRaisesRegex(ValueError, "coverage mismatch"):
                    replay_module.load_scenarios(
                        fixture_path=fixture_path,
                        catalog_path=CATALOG_PATH,
                    )

    def test_loader_rejects_same_version_catalog_content_drift(self):
        replay_module = importlib.import_module(
            "app.scripts.replay_trading_playbook"
        )
        catalog = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))
        payload = json.loads(SCENARIO_FIXTURE.read_text(encoding="utf-8"))
        rules = {rule["mode_key"]: rule for rule in catalog["rules"]}
        for scenario in payload["scenarios"]:
            scenario["rule_hash"] = canonical_rule_content_hash(
                rules[scenario["mode_key"]]
            )
        catalog["rules"][0]["requirements"][0]["value"] = "changed-window"

        with tempfile.TemporaryDirectory() as directory:
            fixture_path = Path(directory) / "scenarios.json"
            catalog_path = Path(directory) / "drifted-catalog.json"
            fixture_path.write_text(
                json.dumps(payload, ensure_ascii=False),
                encoding="utf-8",
            )
            catalog_path.write_text(
                json.dumps(catalog, ensure_ascii=False),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "rule_hash"):
                replay_module.load_scenarios(
                    fixture_path=fixture_path,
                    catalog_path=catalog_path,
                )


class ReplayCliTest(unittest.TestCase):
    def run_cli(self, *arguments):
        return subprocess.run(
            [
                sys.executable,
                "-m",
                "app.scripts.replay_trading_playbook",
                *arguments,
            ],
            cwd=BACKEND_ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            check=False,
        )

    def test_historical_date_requires_explicit_no_notify_gate(self):
        result = self.run_cli("--date", "2000-01-01", "--stage", "preclose")

        self.assertEqual(result.returncode, 2)
        self.assertIn("historical replay requires --no-notify", result.stderr)
        self.assertNotIn("evaluated", result.stdout)

    def test_no_notify_replays_all_nineteen_scenarios(self):
        result = self.run_cli(
            "--date",
            "2026-07-10",
            "--stage",
            "preclose",
            "--no-notify",
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("19 evaluated", result.stdout)
        self.assertIn("no future facts", result.stdout)

    def test_requested_context_is_distinct_from_golden_fixture_facts(self):
        result = self.run_cli(
            "--date",
            "2099-12-31",
            "--stage",
            "auction",
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("golden_fixture_context", result.stdout)
        self.assertIn("requested_date=2099-12-31", result.stdout)
        self.assertIn("requested_stage=auction", result.stdout)
        self.assertIn(
            "fixture_as_of=2026-07-10T14:40:00+08:00",
            result.stdout,
        )
        self.assertIn("facts_rewritten=false", result.stdout)
        self.assertNotRegex(result.stdout, r"(?:^|[;\s])date=")
        self.assertNotRegex(result.stdout, r"(?:^|[;\s])stage=")
        self.assertNotIn("fixture_as_of=2099", result.stdout)
        self.assertNotIn("fixture_stage=auction", result.stdout)

    def test_mismatch_returns_nonzero_with_mode_details(self):
        replay_module = importlib.import_module(
            "app.scripts.replay_trading_playbook"
        )
        scenario = json.loads(
            SCENARIO_FIXTURE.read_text(encoding="utf-8")
        )["scenarios"][0]
        stderr = io.StringIO()
        with patch.object(
            replay_module,
            "load_scenarios",
            return_value=[scenario],
        ), patch.object(
            replay_module,
            "replay_scenario",
            return_value="not_matched",
        ), redirect_stderr(stderr):
            code = replay_module.main(
                [
                    "--date",
                    "2026-07-10",
                    "--stage",
                    "preclose",
                    "--no-notify",
                ]
            )

        self.assertEqual(code, 1)
        self.assertIn("replay mismatch", stderr.getvalue())
        self.assertIn("new_theme_high_volatility", stderr.getvalue())

    def test_invalid_date_stage_and_extra_argument_fail_strictly(self):
        commands = (
            ("--date", "2026-7-10", "--stage", "preclose"),
            ("--date", "20260710", "--stage", "preclose"),
            ("--date", "2026-07-10", "--stage", "midday"),
            (
                "--date",
                "2026-07-10",
                "--stage",
                "preclose",
                "--unexpected",
            ),
        )
        for command in commands:
            with self.subTest(command=command):
                result = self.run_cli(*command)
                self.assertEqual(result.returncode, 2)
                self.assertIn("usage:", result.stderr)

    def test_cli_failure_reports_scenario_index_mode_and_field_path(self):
        replay_module = importlib.import_module(
            "app.scripts.replay_trading_playbook"
        )
        scenario = json.loads(
            SCENARIO_FIXTURE.read_text(encoding="utf-8")
        )["scenarios"][0]
        del scenario["candidate"]
        stderr = io.StringIO()

        with patch.object(
            replay_module,
            "load_scenarios",
            return_value=[scenario],
        ), redirect_stderr(stderr):
            code = replay_module.main(
                ["--date", "2099-12-31", "--stage", "preclose"]
            )

        self.assertEqual(code, 1)
        self.assertIn(
            "scenario index=0 mode=new_theme_high_volatility",
            stderr.getvalue(),
        )
        self.assertIn(
            "scenario[new_theme_high_volatility].candidate",
            stderr.getvalue(),
        )
        self.assertNotIn("KeyError", stderr.getvalue())
