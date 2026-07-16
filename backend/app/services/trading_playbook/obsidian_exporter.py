"""Pure deterministic Markdown rendering for trading-playbook snapshots."""

from __future__ import annotations

import html
import json
import re
from datetime import date, datetime, timezone
from typing import Callable

from app.services.trading_playbook.obsidian_types import ObsidianArtifact
from app.services.trading_playbook.rule_catalog import canonical_rule_source_refs


_PAYLOAD_TYPES = {
    "rule": "trading_mode_rule",
    "plan": "trading_plan_version",
    "review": "trading_execution_review",
    "alerts": "trading_alert_timeline",
    "daily_index": "trading_daily_index",
    "dashboard": "trading_playbook_dashboard",
}

_SAFE_MODE_KEY = re.compile(r"[a-z][a-z0-9]*(?:_[a-z0-9]+)*")
_SAFE_STAGE = re.compile(r"[a-z][a-z0-9_]*")
_SAFE_CATALOG_VERSION = re.compile(r"v[1-9][0-9]*")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_EXPECTED_STAGE_SCHEDULE = (
    ("14:40", ("preclose",)),
    ("15:10", ("initial_review",)),
    ("15:30", ("after_close", "final_review")),
    ("08:50", ("overnight",)),
    ("09:26", ("auction",)),
)
_IN_APP_STATUS_FIELDS = (
    "status",
    "attempts",
    "accepted",
    "skipped_at",
    "sending_at",
    "channel_started_at",
    "recovered_at",
    "delivered_at",
    "uncertain_at",
    "failed_at",
)
_MARKET_FACT_FIELDS = (
    "source_trade_date",
    "target_trade_date",
    "stage",
    "status",
    "trade_date",
    "stock_code",
    "mode_key",
    "condition_version",
    "occurrence_no",
)
_QUOTE_FIELDS = (
    "code",
    "name",
    "price",
    "change_pct",
    "sealed",
    "open_count",
    "datetime",
    "captured_at",
)


class TradingPlaybookObsidianExporter:
    """Render a frozen artifact without I/O, database access, or wall-clock reads."""

    def render(self, artifact: ObsidianArtifact, *, generated_at: datetime) -> str:
        if generated_at.tzinfo is None or generated_at.utcoffset() is None:
            raise ValueError("generated_at must be timezone-aware")
        renderers: dict[str, Callable[[dict[str, object]], str]] = {
            "rule": self._render_rule,
            "plan": self._render_plan,
            "review": self._render_review,
            "alerts": self._render_alerts,
            "daily_index": self._render_daily_index,
            "dashboard": self._render_dashboard,
        }
        renderer = renderers.get(artifact.entity_type)
        if renderer is None:
            raise ValueError(
                f"Unsupported Obsidian entity type: {artifact.entity_type}"
            )

        payload = artifact.payload_json()
        expected_payload_type = _PAYLOAD_TYPES[artifact.entity_type]
        if payload.get("type") != expected_payload_type:
            raise ValueError(
                "Obsidian payload type does not match entity type "
                f"{artifact.entity_type}"
            )
        self._validate_manual_boundary(payload)
        frontmatter = self._frontmatter(
            artifact,
            payload,
            generated_at=generated_at,
        )
        body = renderer(payload)
        return f"---\n{frontmatter}---\n\n{body.rstrip()}\n"

    @staticmethod
    def _validate_manual_boundary(payload: dict[str, object]) -> None:
        if payload.get("manual_required") is not True:
            raise ValueError("manual_required must be true")
        if payload.get("auto_execute") is not False:
            raise ValueError("auto_execute must be false")

    def _frontmatter(
        self,
        artifact: ObsidianArtifact,
        payload: dict[str, object],
        *,
        generated_at: datetime,
    ) -> str:
        values: list[tuple[str, object]] = [
            ("type", artifact.entity_type),
            ("date", artifact.trade_date.isoformat()),
            ("source", "stock-limit-up-system"),
            ("source_hash", artifact.source_hash),
            ("generated_at", self._utc_timestamp(generated_at)),
            ("status", self._page_status(artifact.entity_type, payload)),
            ("manual_required", True),
            ("auto_execute", False),
        ]
        if artifact.entity_type == "rule":
            values.extend(
                (
                    ("mode_key", payload.get("mode_key")),
                    ("rule_version", payload.get("rule_version")),
                )
            )
        elif artifact.entity_type == "plan":
            candidates = [
                self._require_dict(value, "candidate")
                for value in self._require_list(
                    payload.get("candidates"), "candidates"
                )
            ]
            candidates.sort(
                key=lambda row: (
                    self._positive_int(row.get("rank"), "candidate rank"),
                    self._positive_int(
                        row.get("candidate_id"), "candidate_id"
                    ),
                )
            )
            mode_keys: set[str] = set()
            for candidate in candidates:
                mode_keys.add(
                    self._safe_identifier(
                        candidate.get("primary_mode_key"),
                        "primary_mode_key",
                        _SAFE_MODE_KEY,
                    )
                )
                mode_keys.update(
                    self._safe_identifier(
                        mode,
                        "supporting_mode_key",
                        _SAFE_MODE_KEY,
                    )
                    for mode in self._require_list(
                        candidate.get("supporting_mode_keys"),
                        "supporting_mode_keys",
                    )
                )
            theme_rows = [
                self._require_dict(value, "theme ranking row")
                for value in self._require_list(
                    payload.get("theme_ranking"), "theme_ranking"
                )
            ]
            theme_rows.sort(
                key=lambda row: (
                    self._positive_int(row.get("rank"), "theme rank"),
                    str(row.get("theme_name", "")),
                )
            )
            values.extend(
                (
                    ("plan_version_id", payload.get("plan_version_id")),
                    ("plan_version_no", payload.get("version_no")),
                    ("stage", payload.get("stage")),
                    ("source_trade_date", payload.get("source_trade_date")),
                    ("target_trade_date", payload.get("target_trade_date")),
                    (
                        "action_trade_date",
                        sorted(
                            {
                                str(candidate.get("action_trade_date"))
                                for candidate in candidates
                            }
                        ),
                    ),
                    ("mode_keys", sorted(mode_keys)),
                    (
                        "stocks",
                        [str(candidate.get("stock_code")) for candidate in candidates],
                    ),
                    (
                        "themes",
                        [str(row.get("theme_name")) for row in theme_rows],
                    ),
                    (
                        "risk_level",
                        sorted(
                            {str(candidate.get("risk_level")) for candidate in candidates}
                        ),
                    ),
                    ("data_quality", payload.get("data_quality")),
                )
            )
        elif artifact.entity_type == "review":
            values.extend(
                (
                    ("review_id", payload.get("review_id")),
                    ("plan_version_id", payload.get("plan_version_id")),
                    ("phase", payload.get("phase")),
                    (
                        "data_quality",
                        payload.get("data_quality"),
                    ),
                )
            )
        elif artifact.entity_type == "alerts":
            values.append(
                ("alert_count", len(self._require_list(payload.get("timeline"), "timeline")))
            )
        elif artifact.entity_type == "daily_index":
            values.append(
                (
                    "current_effective_plan_version_id",
                    payload.get("current_effective_plan_version_id"),
                )
            )
        return "\n".join(
            f"{key}: {self._yaml_scalar(value)}" for key, value in values
        ) + "\n"

    @staticmethod
    def _page_status(entity_type: str, payload: dict[str, object]) -> str:
        if entity_type == "rule":
            return "enabled" if payload.get("enabled") is True else "disabled"
        if entity_type == "plan":
            status = payload.get("status")
            if not isinstance(status, str) or not status:
                raise ValueError("plan status must be nonempty")
            return status
        if entity_type == "review":
            return "finalized" if payload.get("finalized_at") else "generated"
        return "current"

    @staticmethod
    def _utc_timestamp(value: datetime) -> str:
        return (
            value.astimezone(timezone.utc)
            .isoformat(timespec="microseconds")
            .replace("+00:00", "Z")
        )

    @staticmethod
    def _yaml_scalar(value: object) -> str:
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, int) and not isinstance(value, bool):
            return str(value)
        if value is None:
            return "null"
        if isinstance(value, str):
            return json.dumps(value, ensure_ascii=False)
        if isinstance(value, (list, dict)):
            return json.dumps(
                value,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        raise TypeError(f"unsupported frontmatter value: {type(value).__name__}")

    @staticmethod
    def _safe_text(value: object) -> str:
        if value is None:
            return "—"
        text = html.escape(str(value), quote=True)
        return (
            text.replace("[", "&#91;")
            .replace("]", "&#93;")
            .replace("`", "&#96;")
            .replace("\r\n", "<br>")
            .replace("\r", "<br>")
            .replace("\n", "<br>")
        )

    @classmethod
    def _safe_cell(cls, value: object) -> str:
        return cls._safe_text(value).replace("|", "&#124;")

    @classmethod
    def _wiki_link(cls, path: str, label: object) -> str:
        if "[[" in path or "]]" in path or "|" in path or "#" in path:
            raise ValueError("unsafe Obsidian Wiki link path")
        return f"[[{path}|{cls._safe_cell(label)}]]"

    @staticmethod
    def _positive_int(value: object, field_name: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{field_name} must be a positive integer")
        return value

    @staticmethod
    def _iso_date(value: object, field_name: str) -> str:
        if not isinstance(value, str):
            raise ValueError(f"{field_name} must be an ISO date")
        try:
            parsed = date.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(f"{field_name} must be an ISO date") from exc
        if parsed.isoformat() != value:
            raise ValueError(f"{field_name} must be an ISO date")
        return value

    @staticmethod
    def _safe_identifier(
        value: object,
        field_name: str,
        pattern: re.Pattern[str],
    ) -> str:
        if not isinstance(value, str) or pattern.fullmatch(value) is None:
            raise ValueError(f"{field_name} is not a safe identifier")
        return value

    @classmethod
    def _plan_path(cls, plan: dict[str, object]) -> str:
        target_date = cls._iso_date(
            plan.get("target_trade_date"), "target_trade_date"
        )
        stage = cls._safe_identifier(
            plan.get("stage"), "plan stage", _SAFE_STAGE
        )
        version_no = cls._positive_int(plan.get("version_no"), "version_no")
        return (
            "30_TradingPlaybook/Daily/Auto/"
            f"{target_date[:4]}/{target_date}/{stage}-v{version_no}"
        )

    @classmethod
    def _date_links(cls, trade_date: object) -> dict[str, str]:
        iso_date = cls._iso_date(trade_date, "trade_date")
        root = f"{iso_date[:4]}/{iso_date}"
        return {
            "index": f"30_TradingPlaybook/Daily/Auto/{root}/index",
            "alerts": f"30_TradingPlaybook/Alerts/Auto/{root}",
            "notes": f"30_TradingPlaybook/Notes/{root}",
        }

    @staticmethod
    def _json_block(value: object) -> str:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            indent=2,
        )
        escaped = html.escape(encoded, quote=False)
        escaped = (
            escaped.replace("[", "&#91;")
            .replace("]", "&#93;")
            .replace("`", "&#96;")
        )
        return f'<pre><code class="language-json">{escaped}</code></pre>'

    @staticmethod
    def _require_dict(value: object, field_name: str) -> dict[str, object]:
        if not isinstance(value, dict):
            raise ValueError(f"{field_name} must be an object")
        return value

    @staticmethod
    def _require_list(value: object, field_name: str) -> list[object]:
        if not isinstance(value, list):
            raise ValueError(f"{field_name} must be an array")
        return value

    def _render_rule(self, payload: dict[str, object]) -> str:
        catalog_version = self._safe_identifier(
            payload.get("catalog_version"),
            "catalog_version",
            _SAFE_CATALOG_VERSION,
        )
        rule_id = self._positive_int(payload.get("rule_id"), "rule_id")
        mode_key = self._safe_identifier(
            payload.get("mode_key"),
            "mode_key",
            _SAFE_MODE_KEY,
        )
        rule_version = self._positive_int(
            payload.get("rule_version"), "rule_version"
        )
        if catalog_version != f"v{rule_version}":
            raise ValueError("catalog_version must match rule_version")
        content_hash = payload.get("content_hash")
        if (
            not isinstance(content_hash, str)
            or _SHA256.fullmatch(content_hash) is None
        ):
            raise ValueError("content_hash must be sha256")
        prerequisites = self._require_dict(
            payload.get("prerequisites"), "rule prerequisites"
        )
        raw_source_refs = self._require_list(
            payload.get("source_refs"), "source_refs"
        )
        try:
            source_refs = canonical_rule_source_refs(
                {"source_refs": raw_source_refs}
            )
        except (TypeError, ValueError) as exc:
            raise ValueError(f"source_refs are malformed: {exc}") from exc
        if source_refs != raw_source_refs:
            raise ValueError("source_refs must be stored in canonical order")
        source_rows: list[str] = []
        for source_ref in source_refs:
            source_rows.extend(
                (
                    f"- source_key: {self._safe_text(source_ref.get('source_key'))}",
                    "  - source_content_hash: "
                    f"{self._safe_text(source_ref.get('source_content_hash'))}",
                    f"  - 短引用: {self._safe_text(source_ref.get('excerpt'))}",
                )
            )
        mode_literal = json.dumps(
            mode_key, ensure_ascii=False
        ).replace("`", "\\u0060")
        return "\n".join(
            (
                f"# 交易模式：{self._safe_text(payload.get('name'))}",
                "",
                f"- 规则 ID：{rule_id}",
                f"- 模式键：{mode_key}",
                f"- 规则目录：{catalog_version}",
                f"- 家族：{self._safe_text(payload.get('family'))}",
                f"- 风格：{self._safe_text(payload.get('style'))}",
                f"- 适用窗口：{self._safe_text(payload.get('window'))}",
                f"- 优先级：{self._safe_text(prerequisites.get('priority'))}",
                f"- 角色：{self._safe_text(prerequisites.get('role'))}",
                f"- 自动化等级：{self._safe_text(payload.get('automation_level'))}",
                "- 人工确认：是",
                "- 自动执行：否",
                "",
                "## 说明",
                "",
                self._safe_text(payload.get("description")),
                "",
                "## 识别条件",
                "",
                self._json_block(
                    {
                        "requirements": prerequisites.get("requirements", []),
                        "candidate_filters": payload.get("candidate_filters", []),
                    }
                ),
                "",
                "## 入场触发",
                "",
                self._json_block(payload.get("entry_trigger")),
                "",
                "## 失效条件",
                "",
                self._json_block(payload.get("invalidation")),
                "",
                "## 退出条件",
                "",
                self._json_block(payload.get("exit_trigger")),
                "",
                "## 风险纪律",
                "",
                self._json_block(payload.get("risk_guidance")),
                "",
                "## 文字稿证据",
                "",
                *(source_rows or ["- 无"]),
                "",
                "## 采用该模式的预案",
                "",
                "```dataview",
                "TABLE stage, status, source_trade_date, target_trade_date",
                'FROM "30_TradingPlaybook/Daily/Auto"',
                f"WHERE contains(mode_keys, {mode_literal})",
                "SORT generated_at DESC",
                "```",
            )
        )

    def _render_plan(self, payload: dict[str, object]) -> str:
        plan_id = self._positive_int(
            payload.get("plan_version_id"), "plan_version_id"
        )
        version_no = self._positive_int(payload.get("version_no"), "version_no")
        stage = self._safe_identifier(
            payload.get("stage"), "plan stage", _SAFE_STAGE
        )
        source_date = self._iso_date(
            payload.get("source_trade_date"), "source_trade_date"
        )
        target_date = self._iso_date(
            payload.get("target_trade_date"), "target_trade_date"
        )
        candidates = [
            self._require_dict(value, "candidate")
            for value in self._require_list(payload.get("candidates"), "candidates")
        ]
        candidates.sort(
            key=lambda row: (
                self._positive_int(row.get("rank"), "candidate rank"),
                self._positive_int(row.get("candidate_id"), "candidate_id"),
            )
        )
        themes = [
            self._require_dict(value, "theme ranking row")
            for value in self._require_list(
                payload.get("theme_ranking"), "theme_ranking"
            )
        ]
        themes.sort(
            key=lambda row: (
                self._positive_int(row.get("rank"), "theme rank"),
                str(row.get("theme_name", "")),
            )
        )
        rule_snapshot = [
            self._require_dict(value, "rule snapshot row")
            for value in self._require_list(
                payload.get("rule_snapshot"), "rule_snapshot"
            )
        ]
        rule_snapshot.sort(
            key=lambda row: (
                str(row.get("mode_key", "")),
                int(row.get("version", 0)),
                str(row.get("content_hash", "")),
            )
        )

        lines = [
            f"# 交易预案：{target_date} {self._safe_text(stage)} v{version_no}",
            "",
            "## 版本事实",
            "",
            f"- 计划版本：#{plan_id}",
            f"- 来源交易日：{source_date}",
            f"- 目标交易日：{target_date}",
            f"- 阶段：{self._safe_text(stage)}",
            f"- 版本号：{version_no}",
            f"- 父版本：{self._safe_text(payload.get('parent_plan_version_id'))}",
            f"- 状态：{self._safe_text(payload.get('status'))}",
            f"- 生成时间：{self._safe_text(payload.get('generated_at'))}",
            f"- 人工确认时间：{self._safe_text(payload.get('confirmed_at'))}",
            f"- 确认人：{self._safe_text(payload.get('confirmed_by'))}",
            "- 人工确认：是",
            "- 自动执行：否",
            "",
            "## 市场状态",
            "",
            self._json_block(payload.get("market_state")),
            "",
            "## 题材排序",
            "",
        ]
        if themes:
            lines.extend(("| 排名 | 题材 |", "| ---: | --- |"))
            lines.extend(
                f"| {theme['rank']} | {self._safe_cell(theme.get('theme_name'))} |"
                for theme in themes
            )
        else:
            lines.append("- 无")
        lines.extend(
            (
                "",
                "## 数据质量",
                "",
                self._json_block(payload.get("data_quality")),
                "",
                "## 风险设置",
                "",
                self._json_block(payload.get("risk_settings")),
                "",
                "## 候选",
                "",
            )
        )
        if not candidates:
            lines.append("- 无")
        review_dates = {target_date}
        for candidate in candidates:
            candidate_id = self._positive_int(
                candidate.get("candidate_id"), "candidate_id"
            )
            action_trade_date = self._iso_date(
                candidate.get("action_trade_date"),
                "candidate action_trade_date",
            )
            review_dates.add(action_trade_date)
            primary_mode = self._safe_identifier(
                candidate.get("primary_mode_key"),
                "primary_mode_key",
                _SAFE_MODE_KEY,
            )
            supporting_modes = [
                self._safe_identifier(
                    mode, "supporting_mode_key", _SAFE_MODE_KEY
                )
                for mode in self._require_list(
                    candidate.get("supporting_mode_keys"),
                    "supporting_mode_keys",
                )
            ]
            rule_versions = {
                self._safe_identifier(
                    row.get("mode_key"), "rule mode_key", _SAFE_MODE_KEY
                ): self._positive_int(row.get("version"), "rule version")
                for row in rule_snapshot
            }
            missing_modes = {primary_mode, *supporting_modes} - set(rule_versions)
            if missing_modes:
                raise ValueError(
                    "candidate modes are absent from rule_snapshot: "
                    + ", ".join(sorted(missing_modes))
                )
            primary_link = self._wiki_link(
                "30_TradingPlaybook/Modes/Auto/"
                f"v{rule_versions[primary_mode]}/{primary_mode}",
                primary_mode,
            )
            supporting_links = ", ".join(
                self._wiki_link(
                    "30_TradingPlaybook/Modes/Auto/"
                    f"v{rule_versions[mode]}/{mode}",
                    mode,
                )
                for mode in sorted(supporting_modes)
            ) or "—"
            lines.extend(
                (
                    f"### {candidate['rank']}. {self._safe_text(candidate.get('stock_name'))}（{self._safe_text(candidate.get('stock_code'))}）",
                    "",
                    f"- 候选 ID：{candidate_id}",
                    f"- 行动交易日：{action_trade_date}",
                    f"- 题材：{self._safe_text(candidate.get('theme_name'))}",
                    f"- 主模式：{primary_link}",
                    f"- 辅助模式：{supporting_links}",
                    f"- 角色：{self._safe_text(candidate.get('role'))}",
                    f"- 风险等级：{self._safe_text(candidate.get('risk_level'))}",
                    f"- 仓位参考：{self._safe_text(candidate.get('position_reference'))}",
                    f"- 状态：{self._safe_text(candidate.get('status'))}",
                    "",
                    "识别证据：",
                    "",
                    self._json_block(
                        {
                            "recognition": candidate.get("recognition"),
                            "evidence": candidate.get("evidence"),
                        }
                    ),
                    "",
                    "执行边界：",
                    "",
                    self._json_block(
                        {
                            "entry_trigger": candidate.get("entry_trigger"),
                            "invalidation": candidate.get("invalidation"),
                            "exit_trigger": candidate.get("exit_trigger"),
                            "manual_overrides": candidate.get("manual_overrides"),
                        }
                    ),
                    "",
                )
            )
        lines.extend(
            (
                "## 版本变化",
                "",
                self._json_block(payload.get("change_summary")),
                "",
                "## 规则与文字稿溯源",
                "",
                self._json_block(rule_snapshot),
                "",
                "## 导航",
                "",
            )
        )
        date_links = self._date_links(target_date)
        lines.extend(
            (
                f"- 日期索引：{self._wiki_link(date_links['index'], target_date)}",
                f"- 提醒时间线：{self._wiki_link(date_links['alerts'], '项目内提醒')}",
            )
        )
        for review_date in sorted(review_dates):
            review_root = (
                "30_TradingPlaybook/Reviews/Auto/"
                f"{review_date[:4]}/{review_date}"
            )
            lines.extend(
                (
                    "- 初步复盘："
                    + self._wiki_link(
                        f"{review_root}/initial-review-{plan_id}",
                        f"{review_date} 15:10 初步复盘",
                    ),
                    "- 最终复盘："
                    + self._wiki_link(
                        f"{review_root}/final-review-{plan_id}",
                        f"{review_date} 15:30 最终复盘",
                    ),
                )
            )
        lines.append(
            "- 个人手记："
            + self._wiki_link(
                date_links["notes"],
                target_date + " Notes",
            )
        )
        return "\n".join(lines)

    def _render_review(self, payload: dict[str, object]) -> str:
        review_id = self._positive_int(payload.get("review_id"), "review_id")
        plan_id = self._positive_int(
            payload.get("plan_version_id"), "plan_version_id"
        )
        phase = self._safe_identifier(
            payload.get("phase"), "review phase", _SAFE_STAGE
        )
        trade_date = self._iso_date(payload.get("trade_date"), "trade_date")
        plan = self._require_dict(payload.get("plan_version"), "plan_version")
        plan_path = self._plan_path(plan)
        return "\n".join(
            (
                f"# 交易复盘：{trade_date} {self._safe_text(phase)}",
                "",
                f"- 复盘 ID：{review_id}",
                f"- 阶段：{self._safe_text(phase)}",
                "- 人工确认：是",
                "- 自动执行：否",
                "",
                "## 关联计划",
                "",
                f"- {self._wiki_link(plan_path, '计划 #' + str(plan_id))}",
                "",
                "## 信号复核",
                "",
                self._json_block(payload.get("signal_review")),
                "",
                "## 人工执行记录",
                "",
                self._json_block(payload.get("manual_execution")),
                "",
                "## 计划纪律评价",
                "",
                self._json_block(payload.get("plan_compliance")),
                "",
                "## 结果快照",
                "",
                self._json_block(payload.get("outcome_snapshot")),
                "",
                "## 数据质量",
                "",
                self._json_block(payload.get("data_quality")),
                "",
                "## 校正时间",
                "",
                f"- 生成时间：{self._safe_text(payload.get('generated_at'))}",
                f"- 最终校正：{self._safe_text(payload.get('finalized_at'))}",
            )
        )

    def _render_alerts(self, payload: dict[str, object]) -> str:
        trade_date = self._iso_date(payload.get("trade_date"), "trade_date")
        timeline = [
            self._require_dict(value, "alert timeline row")
            for value in self._require_list(payload.get("timeline"), "timeline")
        ]
        timeline.sort(
            key=lambda row: (
                str(row.get("triggered_at", "")),
                self._positive_int(row.get("alert_id"), "alert_id"),
            )
        )
        lines = [
            f"# 项目内提醒时间线：{trade_date}",
            "",
            "本页只记录项目内提醒与人工确认审计，不是外部发送通道。",
            "",
        ]
        if not timeline:
            lines.append("- 当日无提醒")
        for row in timeline:
            alert_id = self._positive_int(row.get("alert_id"), "alert_id")
            raw_market_facts = self._require_dict(
                row.get("market_facts"), "market_facts"
            )
            market_facts = {
                field_name: raw_market_facts[field_name]
                for field_name in _MARKET_FACT_FIELDS
                if field_name in raw_market_facts
            }
            if "quote" in raw_market_facts:
                raw_quote = self._require_dict(
                    raw_market_facts["quote"], "market_facts quote"
                )
                market_facts["quote"] = {
                    field_name: raw_quote[field_name]
                    for field_name in _QUOTE_FIELDS
                    if field_name in raw_quote
                }
            raw_in_app_status = self._require_dict(
                row.get("in_app_status"), "in_app_status"
            )
            in_app_status = {
                field_name: raw_in_app_status[field_name]
                for field_name in _IN_APP_STATUS_FIELDS
                if field_name in raw_in_app_status
            }
            lines.extend(
                (
                    f"## {self._safe_text(row.get('triggered_at'))} · 提醒 {alert_id}",
                    "",
                    f"- 状态：{self._safe_text(row.get('timeline_state'))}",
                    f"- 事件类型：{self._safe_text(row.get('event_type'))}",
                    f"- 严重级别：{self._safe_text(row.get('severity'))}",
                    f"- 关联计划：#{self._safe_text(row.get('plan_version_id'))}",
                    f"- 关联候选：{self._safe_text(row.get('candidate_id'))}",
                    f"- 提醒消息：{self._safe_text(row.get('message'))}",
                    f"- 人工确认时间：{self._safe_text(row.get('acknowledged_at'))}",
                    "",
                    "市场事实：",
                    "",
                    self._json_block(market_facts),
                    "",
                    "in_app_status：",
                    "",
                    self._json_block(in_app_status),
                    "",
                )
            )
        return "\n".join(lines)

    def _render_daily_index(self, payload: dict[str, object]) -> str:
        trade_date = self._iso_date(payload.get("trade_date"), "trade_date")
        schedule = [
            self._require_dict(value, "stage schedule row")
            for value in self._require_list(
                payload.get("stage_schedule"), "stage_schedule"
            )
        ]
        actual_schedule: list[tuple[str, tuple[str, ...]]] = []
        for row in schedule:
            if set(row) != {"phases", "time_cn", "label"}:
                raise ValueError("stage_schedule rows have unexpected fields")
            time_cn = row.get("time_cn")
            phases = row.get("phases")
            label = row.get("label")
            if (
                not isinstance(time_cn, str)
                or not isinstance(phases, list)
                or not all(isinstance(phase, str) for phase in phases)
                or not isinstance(label, str)
                or not label.strip()
            ):
                raise ValueError("stage_schedule rows are malformed")
            actual_schedule.append((time_cn, tuple(phases)))
        if (
            len(actual_schedule) != len(_EXPECTED_STAGE_SCHEDULE)
            or len(set(actual_schedule)) != len(actual_schedule)
            or set(actual_schedule) != set(_EXPECTED_STAGE_SCHEDULE)
        ):
            raise ValueError(
                "stage_schedule must contain exactly the five prescribed stages"
            )
        schedule_order = {
            contract: index
            for index, contract in enumerate(_EXPECTED_STAGE_SCHEDULE)
        }
        schedule.sort(
            key=lambda row: schedule_order[
                (
                    str(row.get("time_cn")),
                    tuple(str(phase) for phase in row.get("phases", [])),
                )
            ]
        )
        plans = [
            self._require_dict(value, "plan version row")
            for value in self._require_list(
                payload.get("plan_versions"), "plan_versions"
            )
        ]
        plans.sort(
            key=lambda row: (
                self._positive_int(row.get("version_no"), "version_no"),
                self._positive_int(
                    row.get("plan_version_id"), "plan_version_id"
                ),
            )
        )
        lines = [
            f"# 每日交易预案索引：{trade_date}",
            "",
            "## 五阶段时间表（中国时间）",
            "",
            "| 时间 | 阶段 | 内容 |",
            "| --- | --- | --- |",
        ]
        for row in schedule:
            phases = [
                self._safe_identifier(phase, "schedule phase", _SAFE_STAGE)
                for phase in self._require_list(row.get("phases"), "phases")
            ]
            lines.append(
                f"| {self._safe_cell(row.get('time_cn'))} | "
                f"{self._safe_cell(' + '.join(phases))} | "
                f"{self._safe_cell(row.get('label'))} |"
            )
        lines.extend(("", "## 全部预案版本", ""))
        if not plans:
            lines.append("- 无")
        for plan in plans:
            plan_id = self._positive_int(
                plan.get("plan_version_id"), "plan_version_id"
            )
            marker = "（当前有效）" if plan.get("current_effective") is True else ""
            plan_path = self._plan_path(plan)
            lines.extend(
                (
                    "### "
                    + self._wiki_link(plan_path, f"计划 #{plan_id}")
                    + marker,
                    "",
                    f"- 来源交易日：{self._safe_text(plan.get('source_trade_date'))}",
                    f"- 目标交易日：{self._safe_text(plan.get('target_trade_date'))}",
                    f"- 阶段：{self._safe_text(plan.get('stage'))}",
                    f"- 状态：{self._safe_text(plan.get('status'))}",
                    f"- 生成时间：{self._safe_text(plan.get('generated_at'))}",
                    f"- 确认时间：{self._safe_text(plan.get('confirmed_at'))}",
                    "- 候选行动日：",
                )
            )
            candidates = [
                self._require_dict(value, "daily candidate")
                for value in self._require_list(
                    plan.get("candidates"), "daily candidates"
                )
            ]
            candidates.sort(
                key=lambda row: (
                    self._positive_int(row.get("rank"), "candidate rank"),
                    self._positive_int(row.get("candidate_id"), "candidate_id"),
                )
            )
            if candidates:
                lines.extend(
                    "  - "
                    + f"{self._safe_text(candidate.get('stock_code'))} "
                    + f"{self._safe_text(candidate.get('stock_name'))}："
                    + self._safe_text(candidate.get("action_trade_date"))
                    for candidate in candidates
                )
            else:
                lines.append("  - 无")
            lines.append("")
        date_links = self._date_links(trade_date)
        lines.extend(
            (
                "## 当日导航",
                "",
                f"- 提醒时间线：{self._wiki_link(date_links['alerts'], '项目内提醒')}",
                f"- 个人手记：{self._wiki_link(date_links['notes'], trade_date + ' Notes')}",
            )
        )
        return "\n".join(lines)

    def _render_dashboard(self, payload: dict[str, object]) -> str:
        trade_date = self._iso_date(payload.get("trade_date"), "trade_date")
        links = self._date_links(trade_date)
        return "\n".join(
            (
                "# 交易预案 Dashboard",
                "",
                "## 今日导航",
                "",
                f"- 日期索引：{self._wiki_link(links['index'], trade_date)}",
                f"- 提醒时间线：{self._wiki_link(links['alerts'], '项目内提醒')}",
                f"- 个人手记：{self._wiki_link(links['notes'], trade_date + ' Notes')}",
                "",
                "## 最近预案版本",
                "",
                "```dataview",
                "TABLE stage, status, source_trade_date, target_trade_date",
                'FROM "30_TradingPlaybook/Daily/Auto"',
                "SORT generated_at DESC",
                "```",
                "",
                "## 最近项目内提醒",
                "",
                "```dataview",
                "TABLE event_type, severity, triggered_at",
                'FROM "30_TradingPlaybook/Alerts/Auto"',
                "SORT triggered_at DESC",
                "```",
                "",
                "## 最近复盘",
                "",
                "```dataview",
                "TABLE phase, plan_version_id, finalized_at",
                'FROM "30_TradingPlaybook/Reviews/Auto"',
                "SORT date DESC",
                "```",
            )
        )


__all__ = ["TradingPlaybookObsidianExporter"]
