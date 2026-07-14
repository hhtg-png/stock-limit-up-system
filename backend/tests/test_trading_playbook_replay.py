import copy
import importlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from unittest.mock import patch


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

        with self.assertRaisesRegex(ValueError, "fact mismatch"):
            self.replay_scenario(scenario, catalog_path=CATALOG_PATH)

    def test_rejects_scenario_when_required_feature_has_no_fact(self):
        scenario = copy.deepcopy(self.scenario)
        del scenario["facts"][0]["candidate_features"]["high_volatility"]

        with self.assertRaisesRegex(ValueError, "unaudited feature"):
            self.replay_scenario(scenario, catalog_path=CATALOG_PATH)

    def test_rejects_fact_not_tied_to_catalog_sources(self):
        scenario = copy.deepcopy(self.scenario)
        scenario["facts"][0]["source_keys"] = ["invented-source"]

        with self.assertRaisesRegex(ValueError, "fact source_keys"):
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


class ScenarioLoaderTest(unittest.TestCase):
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
