from __future__ import annotations

import re
import unittest
from datetime import date, datetime, timezone

from app.services.trading_playbook.obsidian_exporter import (
    TradingPlaybookObsidianExporter,
)
from app.services.trading_playbook.obsidian_types import ObsidianArtifact


UTC_CREATED = datetime(2026, 7, 15, 6, 0, tzinfo=timezone.utc)
MODE_KEYS = tuple(f"mode_{number:02d}" for number in range(1, 20))


class TradingPlaybookObsidianExporterContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.exporter = TradingPlaybookObsidianExporter()
        self.generated_at = datetime(
            2026, 7, 15, 7, 10, 11, 120000, tzinfo=timezone.utc
        )

    @staticmethod
    def _rule_artifact(
        *,
        mode_key: str = "safe\n---\nstatus: hacked #: 2026-01-01",
        payload_type: str = "trading_mode_rule",
    ) -> ObsidianArtifact:
        target_mode = (
            mode_key
            if re.fullmatch(r"[a-z0-9][a-z0-9_-]*", mode_key)
            else "trend_core_pullback"
        )
        return ObsidianArtifact(
            snapshot_key=f"rule:v2:{mode_key}",
            trade_date=date(2026, 7, 15),
            entity_type="rule",
            entity_id=1,
            phase="catalog",
            target_path=(
                f"30_TradingPlaybook/Modes/Auto/v2/{target_mode}.md"
            ),
            immutable=True,
            payload={
                "type": payload_type,
                "catalog_version": "v2",
                "rule_id": 1,
                "mode_key": mode_key,
                "rule_version": 2,
                "name": "趋势 `核心` [[诱导链接]]",
                "family": "趋势",
                "style": "回踩",
                "window": "盘中",
                "automation_level": "manual_required",
                "description": "先观察\n```dataview\nFROM \"Notes\"\n```",
                "prerequisites": {
                    "priority": 1,
                    "role": "core",
                    "requirements": ["放量", "不追高"],
                },
                "candidate_filters": [],
                "entry_trigger": {"all": ["站稳均线"]},
                "invalidation": {"any": ["跌破支撑"]},
                "exit_trigger": {"any": ["趋势破坏"]},
                "risk_guidance": {"position": "试仓"},
                "source_refs": [
                    {
                        "source_key": "01-trend",
                        "source_content_hash": "a" * 64,
                        "quote": "短引文 #1: [[不应成为链接]]",
                    }
                ],
                "content_hash": "b" * 64,
                "enabled": True,
                "created_at": UTC_CREATED,
                "manual_required": True,
                "auto_execute": False,
            },
        )

    @staticmethod
    def _candidate(
        candidate_id: int,
        rank: int,
        primary_mode: str,
        supporting_modes: list[str],
    ) -> dict[str, object]:
        return {
            "candidate_id": candidate_id,
            "plan_version_id": 101,
            "stock_code": f"600{candidate_id:03d}",
            "stock_name": f"候选{rank} [[伪链接]]",
            "action_trade_date": date(2026, 7, 16),
            "theme_name": "机器人",
            "primary_mode_key": primary_mode,
            "supporting_mode_keys": supporting_modes,
            "role": "core",
            "rank": rank,
            "recognition": {"facts": ["量价齐升"]},
            "entry_trigger": {"kind": "breakout", "price": 12.3},
            "invalidation": {"kind": "price_below", "price": 11.6},
            "exit_trigger": {"kind": "trend_break"},
            "risk_level": "trial",
            "position_reference": 10,
            "evidence": [{"kind": "quote", "value": "盘面事实"}],
            "manual_overrides": {},
            "status": "waiting",
        }

    @classmethod
    def _plan_artifact(
        cls,
        *,
        plan_id: int = 101,
        stage: str = "preclose",
        version_no: int = 1,
        modes: tuple[str, ...] = MODE_KEYS,
    ) -> ObsidianArtifact:
        distributed = (list(modes[1:7]), list(modes[8:13]), list(modes[14:]))
        candidates = [
            cls._candidate(2, 2, modes[7], distributed[1]),
            cls._candidate(1, 1, modes[0], distributed[0]),
            cls._candidate(3, 3, modes[13], distributed[2]),
        ]
        for candidate in candidates:
            candidate["plan_version_id"] = plan_id
        return ObsidianArtifact(
            snapshot_key=f"plan:{plan_id}",
            trade_date=date(2026, 7, 16),
            entity_type="plan",
            entity_id=plan_id,
            phase=stage,
            target_path=(
                "30_TradingPlaybook/Daily/Auto/2026/2026-07-16/"
                f"{stage}-v{version_no}.md"
            ),
            immutable=True,
            payload={
                "type": "trading_plan_version",
                "plan_version_id": plan_id,
                "version_no": version_no,
                "stage": stage,
                "status": "confirmed",
                "source_trade_date": date(2026, 7, 15),
                "target_trade_date": date(2026, 7, 16),
                "parent_plan_version_id": plan_id - 1 if version_no > 1 else None,
                "market_state": {"cycle": "divergence"},
                "theme_ranking": [
                    {"rank": 2, "theme_name": "算力"},
                    {"rank": 1, "theme_name": "机器人"},
                ],
                "mode_radar": [
                    {"mode_key": key, "rule_version": 2} for key in modes
                ],
                "rule_snapshot": [
                    {
                        "mode_key": key,
                        "version": 2,
                        "content_hash": f"{index:064x}"[-64:],
                        "source_hashes": [
                            {
                                "source_key": "01-trend",
                                "content_hash": "a" * 64,
                            }
                        ],
                        "source_refs": [
                            {
                                "source_key": "01-trend",
                                "source_content_hash": "a" * 64,
                                "quote": "短引文",
                            }
                        ],
                    }
                    for index, key in enumerate(reversed(modes), start=1)
                ],
                "data_quality": {"complete": True, "warnings": []},
                "risk_settings": {
                    "trial": 10,
                    "confirmed": 30,
                    "hard_stop": 5,
                    "max_candidates": 3,
                },
                "change_summary": {
                    "added": ["600001"],
                    "removed": [],
                    "changed": ["risk_settings"],
                },
                "input_hash": "c" * 64,
                "generated_at": datetime(
                    2026, 7, 15, 6, 40, tzinfo=timezone.utc
                ),
                "confirmed_at": datetime(
                    2026, 7, 15, 6, 45, tzinfo=timezone.utc
                ),
                "confirmed_by": "operator [[伪链接]]",
                "candidates": candidates,
                "manual_required": True,
                "auto_execute": False,
            },
        )

    @staticmethod
    def _review_artifact(
        *, plan_id: int = 101, phase: str = "initial_review", stage: str = "preclose", version_no: int = 1
    ) -> ObsidianArtifact:
        kind = "initial" if phase == "initial_review" else "final"
        review_id = plan_id * 10 + (1 if kind == "initial" else 2)
        return ObsidianArtifact(
            snapshot_key=f"review:{review_id}:{kind}",
            trade_date=date(2026, 7, 16),
            entity_type="review",
            entity_id=review_id,
            phase=phase,
            target_path=(
                "30_TradingPlaybook/Reviews/Auto/2026/2026-07-16/"
                f"{kind}-review-{plan_id}.md"
            ),
            immutable=True,
            payload={
                "type": "trading_execution_review",
                "review_id": review_id,
                "phase": phase,
                "trade_date": date(2026, 7, 16),
                "plan_version_id": plan_id,
                "plan_version": {
                    "version_no": version_no,
                    "stage": stage,
                    "status": "confirmed",
                    "source_trade_date": date(2026, 7, 15),
                    "target_trade_date": date(2026, 7, 16),
                },
                "signal_review": {
                    "triggered": True,
                    "invalidated": False,
                    "executed": False,
                },
                "manual_execution": {
                    "planned": True,
                    "outside_plan": False,
                    "records": [],
                },
                "plan_compliance": {"grade": "A"},
                "outcome_snapshot": {"market_result": "观察，不推断账户盈亏"},
                "data_quality": {"complete": True},
                "generated_at": datetime(
                    2026, 7, 16, 7, 10, tzinfo=timezone.utc
                ),
                "finalized_at": datetime(
                    2026, 7, 16, 7, 30, tzinfo=timezone.utc
                ),
                "manual_required": True,
                "auto_execute": False,
            },
        )

    @staticmethod
    def _alerts_artifact() -> ObsidianArtifact:
        states = (
            (4, "failed", "failed"),
            (2, "delivered", "delivered"),
            (1, "pending_confirmation", "pending"),
            (3, "confirmed", "delivered"),
        )
        timeline = []
        for alert_id, timeline_state, channel_state in states:
            timeline.append(
                {
                    "alert_id": alert_id,
                    "event_type": "confirmation_required",
                    "severity": "warning",
                    "timeline_state": timeline_state,
                    "triggered_at": datetime(
                        2026, 7, 16, 1, alert_id, tzinfo=timezone.utc
                    ),
                    "plan_version_id": 101,
                    "candidate_id": 1,
                    "message": f"提醒 {alert_id} [[伪链接]]",
                    "market_facts": {"trade_date": "2026-07-16"},
                    "in_app_status": {
                        "status": channel_state,
                        "attempts": 1,
                        "secret": "must-not-render",
                        "api_token": "must-not-render",
                    },
                    "acknowledged_at": (
                        datetime(2026, 7, 16, 1, 5, tzinfo=timezone.utc)
                        if timeline_state == "confirmed"
                        else None
                    ),
                }
            )
        return ObsidianArtifact(
            snapshot_key="alerts:2026-07-16",
            trade_date=date(2026, 7, 16),
            entity_type="alerts",
            entity_id=None,
            phase="reconcile",
            target_path="30_TradingPlaybook/Alerts/Auto/2026/2026-07-16.md",
            immutable=False,
            payload={
                "type": "trading_alert_timeline",
                "trade_date": date(2026, 7, 16),
                "timeline": timeline,
                "manual_required": True,
                "auto_execute": False,
            },
        )

    @staticmethod
    def _daily_index_artifact(plans: list[dict[str, object]]) -> ObsidianArtifact:
        return ObsidianArtifact(
            snapshot_key="daily-index:2026-07-16",
            trade_date=date(2026, 7, 16),
            entity_type="daily_index",
            entity_id=None,
            phase="reconcile",
            target_path=(
                "30_TradingPlaybook/Daily/Auto/2026/2026-07-16/index.md"
            ),
            immutable=False,
            payload={
                "type": "trading_daily_index",
                "trade_date": date(2026, 7, 16),
                "current_effective_plan_version_id": 104,
                "plan_versions": list(reversed(plans)),
                "stage_schedule": [
                    {"phases": ["preclose"], "time_cn": "14:40", "label": "提前预案"},
                    {"phases": ["initial_review"], "time_cn": "15:10", "label": "初步复盘"},
                    {"phases": ["after_close", "final_review"], "time_cn": "15:30", "label": "正式预案与最终复盘"},
                    {"phases": ["overnight"], "time_cn": "08:50", "label": "隔夜刷新"},
                    {"phases": ["auction"], "time_cn": "09:26", "label": "竞价最终版本"},
                ],
                "manual_required": True,
                "auto_execute": False,
            },
        )

    @staticmethod
    def _dashboard_artifact() -> ObsidianArtifact:
        return ObsidianArtifact(
            snapshot_key="dashboard:trading-playbook",
            trade_date=date(2026, 7, 16),
            entity_type="dashboard",
            entity_id=None,
            phase="reconcile",
            target_path="Dashboards/交易预案.md",
            immutable=False,
            payload={
                "type": "trading_playbook_dashboard",
                "trade_date": date(2026, 7, 16),
                "navigation": {
                    "daily_index": "[[injected]]",
                    "alerts": "[[injected]]",
                    "notes": "[[injected]]",
                },
                "dataview_queries": [
                    'TABLE secret FROM "30_TradingPlaybook/Notes"'
                ],
                "manual_required": True,
                "auto_execute": False,
            },
        )

    def test_rule_frontmatter_is_ordered_escaped_and_deterministic(self) -> None:
        artifact = self._rule_artifact()

        first = self.exporter.render(artifact, generated_at=self.generated_at)
        second = self.exporter.render(artifact, generated_at=self.generated_at)

        self.assertEqual(first, second)
        self.assertTrue(first.endswith("\n"))
        expected_frontmatter = "\n".join(
            (
                "---",
                'type: "rule"',
                'date: "2026-07-15"',
                'source: "stock-limit-up-system"',
                f'source_hash: "{artifact.source_hash}"',
                'generated_at: "2026-07-15T07:10:11.120000Z"',
                'status: "enabled"',
                "manual_required: true",
                "auto_execute: false",
                'mode_key: "safe\\n---\\nstatus: hacked #: 2026-01-01"',
                'rule_version: 2',
                "---",
                "",
            )
        )
        self.assertTrue(first.startswith(expected_frontmatter))
        self.assertNotIn("[[诱导链接]]", first)
        self.assertNotIn("[[不应成为链接]]", first)
        self.assertEqual(first.count("```dataview"), 1)
        self.assertIn('FROM "30_TradingPlaybook/Daily/Auto"', first)
        self.assertNotIn('FROM "Notes"', first)

    def test_generated_at_must_be_timezone_aware(self) -> None:
        with self.assertRaisesRegex(ValueError, "generated_at must be timezone-aware"):
            self.exporter.render(
                self._rule_artifact(),
                generated_at=datetime(2026, 7, 15, 15, 10),
            )

    def test_unsupported_entity_and_payload_type_fail_closed(self) -> None:
        artifact = self._rule_artifact()
        object.__setattr__(artifact, "entity_type", "unknown")
        with self.assertRaisesRegex(
            ValueError, "Unsupported Obsidian entity type: unknown"
        ):
            self.exporter.render(artifact, generated_at=self.generated_at)

        with self.assertRaisesRegex(ValueError, "payload type"):
            self.exporter.render(
                self._rule_artifact(payload_type="wrong"),
                generated_at=self.generated_at,
            )

    def test_plan_page_contains_core_facts_candidates_provenance_and_links(self) -> None:
        rendered = self.exporter.render(
            self._plan_artifact(), generated_at=self.generated_at
        )

        for heading in (
            "## 版本事实",
            "## 市场状态",
            "## 题材排序",
            "## 风险设置",
            "## 候选",
            "## 版本变化",
            "## 规则与文字稿溯源",
            "## 导航",
        ):
            self.assertIn(heading, rendered)
        self.assertLess(rendered.index("候选1"), rendered.index("候选2"))
        self.assertLess(rendered.index("机器人"), rendered.index("算力"))
        self.assertIn("source_content_hash", rendered)
        self.assertIn("source_hashes", rendered)
        self.assertIn('mode_keys: ["mode_01","mode_02"', rendered)
        self.assertIn("[[30_TradingPlaybook/Daily/Auto/2026/2026-07-16/index", rendered)
        self.assertIn("[[30_TradingPlaybook/Modes/Auto/v2/mode_01", rendered)
        self.assertIn("[[30_TradingPlaybook/Alerts/Auto/2026/2026-07-16", rendered)
        self.assertIn("[[30_TradingPlaybook/Reviews/Auto/2026/2026-07-16/initial-review-101", rendered)
        self.assertIn("[[30_TradingPlaybook/Reviews/Auto/2026/2026-07-16/final-review-101", rendered)
        self.assertIn("[[30_TradingPlaybook/Notes/2026/2026-07-16", rendered)
        self.assertNotIn("[[伪链接]]", rendered)

    def test_review_page_contains_full_review_sections_and_plan_link(self) -> None:
        rendered = self.exporter.render(
            self._review_artifact(), generated_at=self.generated_at
        )

        for heading in (
            "## 关联计划",
            "## 信号复核",
            "## 人工执行记录",
            "## 计划纪律评价",
            "## 结果快照",
            "## 数据质量",
            "## 校正时间",
        ):
            self.assertIn(heading, rendered)
        self.assertIn(
            "[[30_TradingPlaybook/Daily/Auto/2026/2026-07-16/preclose-v1",
            rendered,
        )
        self.assertIn("不推断账户盈亏", rendered)

    def test_alerts_page_has_four_states_and_no_external_channel_or_secrets(self) -> None:
        rendered = self.exporter.render(
            self._alerts_artifact(), generated_at=self.generated_at
        )

        for state in ("pending_confirmation", "delivered", "confirmed", "failed"):
            self.assertIn(state, rendered)
        self.assertLess(rendered.index("提醒 1"), rendered.index("提醒 4"))
        self.assertIn("in_app_status", rendered)
        self.assertNotIn("微信发送", rendered)
        self.assertNotIn("secret", rendered.lower())
        self.assertNotIn("token", rendered.lower())
        self.assertNotIn("[[伪链接]]", rendered)

    def test_all_entity_frontmatters_share_the_ordered_safety_boundary(self) -> None:
        artifacts = (
            self._rule_artifact(mode_key="mode_01"),
            self._plan_artifact(),
            self._review_artifact(),
            self._alerts_artifact(),
            self._daily_index_artifact([]),
            self._dashboard_artifact(),
        )
        common_keys = (
            "type",
            "date",
            "source",
            "source_hash",
            "generated_at",
            "status",
            "manual_required",
            "auto_execute",
        )

        for artifact in artifacts:
            with self.subTest(entity_type=artifact.entity_type):
                first = self.exporter.render(
                    artifact, generated_at=self.generated_at
                )
                second = self.exporter.render(
                    artifact, generated_at=self.generated_at
                )
                frontmatter = first.split("---\n", 2)[1].splitlines()
                keys = tuple(line.split(":", 1)[0] for line in frontmatter)
                self.assertEqual(keys[: len(common_keys)], common_keys)
                self.assertIn("manual_required: true", frontmatter)
                self.assertIn("auto_execute: false", frontmatter)
                self.assertEqual(first, second)
                self.assertTrue(first.endswith("\n"))
                self.assertNotIn("datetime.datetime(", first)

    def test_daily_index_lists_versions_current_marker_and_all_five_times(self) -> None:
        plan_specs = (
            (101, "preclose", 1),
            (102, "after_close", 2),
            (103, "overnight", 3),
            (104, "auction", 4),
        )
        plans = [
            {
                "plan_version_id": plan_id,
                "version_no": version_no,
                "stage": stage,
                "status": "confirmed",
                "source_trade_date": date(2026, 7, 15),
                "target_trade_date": date(2026, 7, 16),
                "generated_at": UTC_CREATED,
                "confirmed_at": UTC_CREATED,
                "current_effective": plan_id == 104,
                "candidates": [],
            }
            for plan_id, stage, version_no in plan_specs
        ]
        rendered = self.exporter.render(
            self._daily_index_artifact(plans), generated_at=self.generated_at
        )

        for time_cn in ("14:40", "15:10", "15:30", "08:50", "09:26"):
            self.assertIn(time_cn, rendered)
        self.assertIn("after_close + final_review", rendered)
        self.assertIn("当前有效", rendered)
        for plan_id, stage, version_no in plan_specs:
            self.assertIn(f"{stage}-v{version_no}", rendered)
            self.assertIn(f"计划 #{plan_id}", rendered)

    def test_dashboard_uses_fixed_navigation_and_auto_only_dataview_sources(self) -> None:
        rendered = self.exporter.render(
            self._dashboard_artifact(), generated_at=self.generated_at
        )

        self.assertNotIn("[[injected]]", rendered)
        self.assertIn("[[30_TradingPlaybook/Daily/Auto/2026/2026-07-16/index", rendered)
        self.assertIn("[[30_TradingPlaybook/Alerts/Auto/2026/2026-07-16", rendered)
        self.assertIn("[[30_TradingPlaybook/Notes/2026/2026-07-16", rendered)
        from_targets = re.findall(r'^FROM "([^"]+)"$', rendered, flags=re.MULTILINE)
        self.assertEqual(
            from_targets,
            [
                "30_TradingPlaybook/Daily/Auto",
                "30_TradingPlaybook/Alerts/Auto",
                "30_TradingPlaybook/Reviews/Auto",
            ],
        )
        self.assertTrue(all("/Auto" in target for target in from_targets))
        self.assertNotIn('FROM "30_TradingPlaybook/Notes"', rendered)

    def test_nineteen_rules_and_five_stage_navigation_form_a_closed_link_graph(self) -> None:
        rules = [self._rule_artifact(mode_key=mode) for mode in MODE_KEYS]
        plan_specs = (
            (101, "preclose", 1),
            (102, "after_close", 2),
            (103, "overnight", 3),
            (104, "auction", 4),
        )
        plans = [
            self._plan_artifact(
                plan_id=plan_id,
                stage=stage,
                version_no=version_no,
            )
            for plan_id, stage, version_no in plan_specs
        ]
        reviews = [
            self._review_artifact(
                plan_id=plan_id,
                phase=phase,
                stage=stage,
                version_no=version_no,
            )
            for plan_id, stage, version_no in plan_specs
            for phase in ("initial_review", "final_review")
        ]
        daily_plans = [
            {
                "plan_version_id": plan_id,
                "version_no": version_no,
                "stage": stage,
                "status": "confirmed",
                "source_trade_date": date(2026, 7, 15),
                "target_trade_date": date(2026, 7, 16),
                "generated_at": UTC_CREATED,
                "confirmed_at": UTC_CREATED,
                "current_effective": plan_id == 104,
                "candidates": [],
            }
            for plan_id, stage, version_no in plan_specs
        ]
        artifacts = [
            *rules,
            *plans,
            *reviews,
            self._alerts_artifact(),
            self._daily_index_artifact(daily_plans),
            self._dashboard_artifact(),
        ]
        rendered = [
            self.exporter.render(artifact, generated_at=self.generated_at)
            for artifact in artifacts
        ]
        artifact_paths = {
            artifact.target_path.removesuffix(".md") for artifact in artifacts
        }
        links = {
            match.group(1)
            for page in rendered
            for match in re.finditer(r"\[\[([^\]|#]+)(?:\|[^\]]+)?\]\]", page)
        }
        auto_links = {link for link in links if "/Auto/" in link}
        notes_links = {
            link for link in links if link.startswith("30_TradingPlaybook/Notes/")
        }

        self.assertEqual(len(rules), 19)
        self.assertTrue(auto_links)
        self.assertEqual(auto_links - artifact_paths, set())
        self.assertEqual(
            notes_links,
            {"30_TradingPlaybook/Notes/2026/2026-07-16"},
        )
        self.assertTrue(notes_links.isdisjoint(artifact_paths))
        self.assertTrue(
            all(
                not path.startswith("30_TradingPlaybook/Notes/")
                for path in artifact_paths
            )
        )


if __name__ == "__main__":
    unittest.main()
