from __future__ import annotations

import hashlib
import re
from collections import OrderedDict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.intelligence import DailyInfoDigest, JiegeModeSignal, KnowledgeDocument
from app.models.market_review import DailyAnalysisRecord, MarketReviewDailyMetric
from app.services.obsidian_vault_writer import ObsidianVaultWriter
from app.utils.time_utils import today_cn


VAULT_DIRECTORIES = [
    "00_Inbox/Auto",
    "10_Industry",
    "40_UltraShort",
    "50_Daily",
    "60_Signals",
    "Dashboards",
]
STOCK_CODE_PATTERN = re.compile(r"([\u4e00-\u9fa5A-Za-z0-9·]{2,16})[（(]([0368]\d{5})[）)]")


class ObsidianKnowledgeService:
    """Export project intelligence into an external Obsidian vault."""

    def __init__(self, settings=settings, writer: Optional[ObsidianVaultWriter] = None):
        self.settings = settings
        self.writer = writer or ObsidianVaultWriter(
            enabled=bool(getattr(settings, "OBSIDIAN_ENABLED", False)),
            vault_path=str(getattr(settings, "OBSIDIAN_VAULT_PATH", "") or ""),
            auto_git_enabled=bool(getattr(settings, "OBSIDIAN_AUTO_GIT_ENABLED", False)),
        )

    def get_status(self) -> Dict[str, Any]:
        vault_path = self._vault_path()
        allowlist = self._web_allowlist()
        return {
            "enabled": self.writer.enabled,
            "vault_configured": vault_path is not None,
            "vault_exists": bool(vault_path and vault_path.exists()),
            "vault_path": str(vault_path) if vault_path else "",
            "auto_git_enabled": self.writer.auto_git_enabled,
            "web_research_enabled": bool(getattr(self.settings, "WEB_RESEARCH_ENABLED", False)),
            "web_research_allowlist": allowlist,
            "required_directories": VAULT_DIRECTORIES,
        }

    async def build_industry_trends(self, db: AsyncSession, *, limit: int = 30) -> List[Dict[str, Any]]:
        result = await db.execute(
            select(DailyInfoDigest)
            .order_by(DailyInfoDigest.trade_date.desc(), DailyInfoDigest.generated_at.desc(), DailyInfoDigest.id.desc())
            .limit(limit)
        )
        digests = list(result.scalars().all())
        trends: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()

        for digest in digests:
            documents = await self._daily_documents(db, digest.trade_date)
            summary = dict(digest.summary_json or {})
            themes = self._summary_themes(summary, documents)
            catalysts = self._string_list(summary.get("catalysts"))
            risks = self._string_list(summary.get("risks"))
            stocks = self._stock_list(summary.get("mentioned_stocks") or summary.get("stocks"))
            if not stocks:
                stocks = self._extract_stock_mentions(documents)
            sources = [self._source_payload(document) for document in documents]
            if not sources and summary.get("source_titles"):
                sources = [
                    {
                        "title": title,
                        "url": "",
                        "source_name": "每日资讯",
                        "trade_date": digest.trade_date.isoformat(),
                    }
                    for title in self._string_list(summary.get("source_titles"))
                ]

            for theme in themes:
                if theme not in trends:
                    trends[theme] = {
                        "theme": theme,
                        "status": "candidate",
                        "confidence": "medium",
                        "last_seen": digest.trade_date.isoformat(),
                        "catalysts": [],
                        "risks": [],
                        "stocks": [],
                        "sources": [],
                        "evidence": [],
                    }
                item = trends[theme]
                item["last_seen"] = max(item["last_seen"], digest.trade_date.isoformat())
                self._extend_unique(item["catalysts"], catalysts)
                self._extend_unique(item["risks"], risks)
                self._extend_unique_dicts(item["stocks"], stocks, key="code")
                self._extend_unique_dicts(item["sources"], sources, key="url")
                overview = str(summary.get("overview") or "").strip()
                if overview:
                    self._extend_unique(item["evidence"], [overview])

        return list(trends.values())

    async def build_ultra_short_signals(
        self,
        db: AsyncSession,
        trade_date: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        target_date = trade_date or today_cn()
        record = await self._daily_analysis_record(db, target_date)
        jiege_signal = await self._jiege_signal(db, target_date)
        metric = await self._market_metric(db, target_date)
        signals: List[Dict[str, Any]] = []

        if record is not None:
            analysis_result = (
                record.intraday_auto_result
                if record.intraday_data_status == "ready" and record.intraday_auto_result
                else record.auto_result
            ) or {}
            for setup, cell in analysis_result.items():
                for raw_item in self._cell_items(cell):
                    signals.append(
                        self._signal_payload(
                            raw_item,
                            trade_date=target_date,
                            setup=str(setup),
                            source="daily_analysis",
                            alert_type="watchlist",
                            metric=metric,
                        )
                    )

        payload = dict(jiege_signal.signal_json or {}) if jiege_signal is not None else {}
        prediction = payload.get("prediction") or {}
        for raw_item in prediction.get("candidates") or []:
            signals.append(
                self._signal_payload(
                    raw_item,
                    trade_date=target_date,
                    setup="杰哥模式",
                    source="jiege_mode",
                    alert_type="plan",
                    metric=metric,
                    risk_flags=prediction.get("risk_flags") or [],
                )
            )

        deduped: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
        for signal in signals:
            key = f"{signal.get('stock_code') or signal['label']}::{signal['setup']}::{signal['source']}"
            deduped[key] = signal
        return sorted(deduped.values(), key=lambda item: (-float(item.get("score") or 0), item["setup"], item["label"]))

    async def export_daily_knowledge(self, db: AsyncSession, trade_date: Optional[date] = None) -> Dict[str, Any]:
        target_date = trade_date or today_cn()
        vault = self._ensure_vault()
        if vault is None:
            return {
                "trade_date": target_date.isoformat(),
                "vault_path": "",
                "written_files": [],
                "skipped": True,
                "reason": "obsidian_disabled_or_unconfigured",
            }

        digest = await self._daily_digest(db, target_date)
        trends = await self.build_industry_trends(db, limit=30)
        today_trends = [item for item in trends if item["last_seen"] == target_date.isoformat()]
        signals = await self.build_ultra_short_signals(db, target_date)
        metric = await self._market_metric(db, target_date)

        written = []
        written.extend(self._write_dashboards(vault))
        for trend in today_trends:
            written.append(self._write_industry_note(vault, trend))
        for signal in signals:
            written.append(self._write_signal_note(vault, signal))
        written.append(self._write_ultra_short_index(vault, signals, target_date))
        written.append(self._write_daily_note(vault, target_date, digest, today_trends, signals, metric))

        unique_written = sorted({self._relative(vault, path) for path in written})
        git_result = self._maybe_git_commit(vault, target_date, unique_written)
        return {
            "trade_date": target_date.isoformat(),
            "vault_path": str(vault),
            "written_files": unique_written,
            "skipped": False,
            "git": git_result,
        }

    def _write_dashboards(self, vault: Path) -> List[Path]:
        industry_dashboard = vault / "Dashboards" / "产业趋势.md"
        ultra_dashboard = vault / "Dashboards" / "超短线驾驶舱.md"
        self._write_if_changed(
            industry_dashboard,
            "\n".join(
                [
                    "# 产业趋势",
                    "",
                    "```dataview",
                    'TABLE last_seen AS "最近出现", confidence AS "置信度", stocks AS "相关标的"',
                    'FROM "10_Industry"',
                    'WHERE type = "industry_trend"',
                    "SORT last_seen DESC",
                    "```",
                    "",
                ]
            ),
        )
        self._write_if_changed(
            ultra_dashboard,
            "\n".join(
                [
                    "# 超短线驾驶舱",
                    "",
                    "```dataview",
                    'TABLE date AS "日期", setup AS "模式", alert_type AS "提醒", sim_result AS "模拟结果"',
                    'FROM "60_Signals"',
                    "WHERE manual_required = true",
                    "SORT date DESC",
                    "```",
                    "",
                ]
            ),
        )
        return [industry_dashboard, ultra_dashboard]

    def _write_industry_note(self, vault: Path, trend: Dict[str, Any]) -> Path:
        path = vault / "10_Industry" / f"{self._slug(trend['theme'])}.md"
        stocks = [stock.get("code") or stock.get("name") for stock in trend.get("stocks") or []]
        sources = trend.get("sources") or []
        source_hash = self._hash_json({"theme": trend["theme"], "sources": sources, "evidence": trend.get("evidence")})
        content = [
            self._frontmatter(
                {
                    "type": "industry_trend",
                    "date": trend["last_seen"],
                    "source": "stock-limit-up-system",
                    "source_url": sources[0].get("url", "") if sources else "",
                    "source_hash": source_hash,
                    "stocks": stocks,
                    "themes": [trend["theme"]],
                    "confidence": trend["confidence"],
                    "status": trend["status"],
                    "last_seen": trend["last_seen"],
                }
            ),
            f"# {trend['theme']}",
            "",
            "## 催化",
            self._bullet_list(trend.get("catalysts") or ["暂无"]),
            "",
            "## 证据",
            self._bullet_list(trend.get("evidence") or ["暂无"]),
            "",
            "## 相关标的",
            self._bullet_list([self._stock_label(stock) for stock in trend.get("stocks") or []] or ["暂无"]),
            "",
            "## 风险与失效",
            self._bullet_list(trend.get("risks") or ["暂无"]),
            "",
            "## 来源",
            self._bullet_list([self._source_label(source) for source in sources] or ["暂无"]),
            "",
        ]
        self._write_if_changed(path, "\n".join(content))
        return path

    def _write_signal_note(self, vault: Path, signal: Dict[str, Any]) -> Path:
        trade_date = signal["trade_date"]
        stock_key = signal.get("stock_code") or self._slug(signal["label"])
        path = vault / "60_Signals" / trade_date / f"{self._slug(stock_key)}-{self._slug(signal['setup'])}.md"
        content = [
            self._frontmatter(
                {
                    "type": "ultra_short_signal",
                    "date": trade_date,
                    "source": signal["source"],
                    "source_url": "",
                    "source_hash": self._hash_json(signal),
                    "stocks": [signal.get("stock_code") or signal["label"]],
                    "themes": signal.get("tags") or [],
                    "confidence": "medium",
                    "status": "candidate",
                    "last_seen": trade_date,
                    "setup": signal["setup"],
                    "alert_type": signal["alert_type"],
                    "manual_required": True,
                    "sim_result": signal.get("sim_result") or "pending",
                    "reviewed_at": "",
                }
            ),
            f"# {signal['label']}",
            "",
            f"- 模式：{signal['setup']}",
            f"- 提醒：{signal['alert_type']}",
            f"- 分数：{signal.get('score', 0)}",
            f"- 原因：{signal.get('reason') or '暂无'}",
            "",
            "## 风险",
            self._bullet_list(signal.get("risk_flags") or ["人工确认承接和风险后再行动"]),
            "",
            "## 盘后模拟",
            f"- 结果：{signal.get('sim_result') or 'pending'}",
            "",
        ]
        self._write_if_changed(path, "\n".join(content))
        return path

    def _write_ultra_short_index(self, vault: Path, signals: List[Dict[str, Any]], trade_date: date) -> Path:
        path = vault / "40_UltraShort" / "规则索引.md"
        content = [
            self._frontmatter(
                {
                    "type": "ultra_short_rules",
                    "date": trade_date.isoformat(),
                    "source": "stock-limit-up-system",
                    "source_url": "",
                    "source_hash": self._hash_json(signals),
                    "stocks": [],
                    "themes": ["超短线"],
                    "confidence": "medium",
                    "status": "active",
                    "last_seen": trade_date.isoformat(),
                }
            ),
            "# 超短线规则索引",
            "",
            "## 今日候选",
            self._bullet_list([f"{item['setup']}：{item['label']}" for item in signals] or ["暂无"]),
            "",
        ]
        self._write_if_changed(path, "\n".join(content))
        return path

    def _write_daily_note(
        self,
        vault: Path,
        trade_date: date,
        digest: Optional[DailyInfoDigest],
        trends: List[Dict[str, Any]],
        signals: List[Dict[str, Any]],
        metric: Optional[MarketReviewDailyMetric],
    ) -> Path:
        path = vault / "50_Daily" / f"{trade_date.year}" / f"{trade_date.isoformat()}.md"
        summary = dict(digest.summary_json or {}) if digest is not None else {}
        stocks = [
            stock.get("code") or stock.get("name")
            for stock in self._stock_list(summary.get("mentioned_stocks") or summary.get("stocks"))
        ]
        themes = self._string_list(summary.get("main_lines"))
        source_hash = digest.content_hash if digest is not None else self._hash_json({"trade_date": trade_date.isoformat()})
        metric_lines = []
        if metric is not None:
            metric_lines = [
                f"- 涨停数：{metric.limit_up_count}",
                f"- 跌停数：{metric.limit_down_count}",
                f"- 最高连板：{metric.max_board_height}",
                f"- 封板率：{metric.seal_rate}",
            ]
        content = [
            self._frontmatter(
                {
                    "type": "daily",
                    "date": trade_date.isoformat(),
                    "source": "stock-limit-up-system",
                    "source_url": "",
                    "source_hash": source_hash,
                    "stocks": stocks,
                    "themes": themes,
                    "confidence": "medium",
                    "status": "generated",
                    "last_seen": trade_date.isoformat(),
                }
            ),
            f"# {trade_date.isoformat()} 每日复盘",
            "",
            "## 市场",
            self._bullet_list(metric_lines or ["暂无复盘指标"]),
            "",
            "## 产业趋势",
            self._bullet_list([f"[[{trend['theme']}]]：{'、'.join(trend.get('catalysts') or [])}" for trend in trends] or ["暂无"]),
            "",
            "## 超短线",
            self._bullet_list([f"{signal['setup']}：{signal['label']} ({signal['alert_type']})" for signal in signals] or ["暂无"]),
            "",
            "## 摘要",
            str(summary.get("overview") or "暂无"),
            "",
            "## 交易预案",
            str(summary.get("plan") or "暂无"),
            "",
            "## 风险",
            self._bullet_list(self._string_list(summary.get("risks")) or ["暂无"]),
            "",
        ]
        self._write_if_changed(path, "\n".join(content))
        return path

    async def _daily_digest(self, db: AsyncSession, trade_date: date) -> Optional[DailyInfoDigest]:
        result = await db.execute(select(DailyInfoDigest).where(DailyInfoDigest.trade_date == trade_date))
        return result.scalar_one_or_none()

    async def _daily_documents(self, db: AsyncSession, trade_date: date) -> List[KnowledgeDocument]:
        result = await db.execute(
            select(KnowledgeDocument)
            .where(KnowledgeDocument.source_key == "daily", KnowledgeDocument.trade_date == trade_date)
            .order_by(KnowledgeDocument.update_time.desc(), KnowledgeDocument.id.desc())
        )
        return list(result.scalars().all())

    async def _daily_analysis_record(self, db: AsyncSession, trade_date: date) -> Optional[DailyAnalysisRecord]:
        result = await db.execute(select(DailyAnalysisRecord).where(DailyAnalysisRecord.trade_date == trade_date))
        return result.scalar_one_or_none()

    async def _jiege_signal(self, db: AsyncSession, trade_date: date) -> Optional[JiegeModeSignal]:
        result = await db.execute(select(JiegeModeSignal).where(JiegeModeSignal.trade_date == trade_date))
        return result.scalar_one_or_none()

    async def _market_metric(self, db: AsyncSession, trade_date: date) -> Optional[MarketReviewDailyMetric]:
        result = await db.execute(select(MarketReviewDailyMetric).where(MarketReviewDailyMetric.trade_date == trade_date))
        return result.scalar_one_or_none()

    def _summary_themes(self, summary: Dict[str, Any], documents: Iterable[KnowledgeDocument]) -> List[str]:
        themes: List[str] = []
        self._extend_unique(themes, self._string_list(summary.get("main_lines")))
        for stock in self._stock_list(summary.get("mentioned_stocks") or summary.get("stocks")):
            if stock.get("sector"):
                self._extend_unique(themes, [stock["sector"]])
        for document in documents:
            doc_summary = document.summary_json or {}
            self._extend_unique(themes, self._string_list(doc_summary.get("themes") or doc_summary.get("sectors")))
        return themes or ["未分类"]

    def _signal_payload(
        self,
        raw_item: Dict[str, Any],
        *,
        trade_date: date,
        setup: str,
        source: str,
        alert_type: str,
        metric: Optional[MarketReviewDailyMetric],
        risk_flags: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        stock_code = str(raw_item.get("stock_code") or raw_item.get("code") or "")
        stock_name = str(raw_item.get("stock_name") or raw_item.get("name") or "")
        label = str(raw_item.get("label") or self._stock_label({"code": stock_code, "name": stock_name}) or setup)
        tags = [str(item) for item in raw_item.get("tags") or []]
        risk_items = list(risk_flags or [])
        if metric is not None:
            if metric.seal_rate < 55:
                risk_items.append("封板率偏低，提醒仅作观察")
            if metric.limit_down_count >= 10:
                risk_items.append("跌停家数偏高，注意负反馈扩散")
        return {
            "trade_date": trade_date.isoformat(),
            "setup": setup,
            "source": source,
            "alert_type": alert_type,
            "manual_required": True,
            "sim_result": "pending",
            "stock_code": stock_code,
            "stock_name": stock_name,
            "label": label,
            "tags": tags,
            "reason": str(raw_item.get("reason") or raw_item.get("content") or ""),
            "score": float(raw_item.get("score") or 0),
            "risk_flags": risk_items,
        }

    def _cell_items(self, cell: Any) -> List[Dict[str, Any]]:
        if isinstance(cell, dict) and isinstance(cell.get("items"), list):
            return [item for item in cell["items"] if isinstance(item, dict)]
        return []

    def _vault_path(self) -> Optional[Path]:
        return self.writer.configured_vault()

    def _ensure_vault(self) -> Optional[Path]:
        vault = self.writer.ensure_vault()
        if vault is None:
            return None
        for directory in VAULT_DIRECTORIES:
            self.writer.resolve_target(directory, allowed_roots=tuple(VAULT_DIRECTORIES)).mkdir(
                parents=True,
                exist_ok=True,
            )
        return vault

    def _web_allowlist(self) -> List[str]:
        raw = str(getattr(self.settings, "WEB_RESEARCH_ALLOWLIST", "") or "")
        return [item.strip() for item in raw.split(",") if item.strip()]

    def _write_if_changed(self, path: Path, content: str) -> None:
        vault = self.writer.configured_vault()
        if vault is None:
            raise ValueError("Obsidian Vault path is not configured")
        try:
            relative_path = path.resolve(strict=False).relative_to(vault).as_posix()
        except ValueError as exc:
            raise ValueError(f"Knowledge export path is outside the configured Vault: {path}") from exc
        self.writer.write_text(relative_path, content, allowed_roots=tuple(VAULT_DIRECTORIES))

    def _maybe_git_commit(self, vault: Path, trade_date: date, written_files: List[str]) -> Dict[str, Any]:
        return self.writer.commit_paths(
            written_files,
            allowed_roots=tuple(VAULT_DIRECTORIES),
            message=f"chore: sync knowledge {trade_date.isoformat()}",
        )

    def _frontmatter(self, values: Dict[str, Any]) -> str:
        lines = ["---"]
        for key, value in values.items():
            if isinstance(value, bool):
                lines.append(f"{key}: {'true' if value else 'false'}")
            elif isinstance(value, list):
                lines.append(f"{key}:")
                if value:
                    for item in value:
                        lines.append(f"  - {self._yaml_scalar(item)}")
            elif value is None:
                lines.append(f"{key}:")
            else:
                lines.append(f"{key}: {self._yaml_scalar(value)}")
        lines.append("---")
        return "\n".join(lines)

    def _yaml_scalar(self, value: Any) -> str:
        text = str(value)
        if not text:
            return '""'
        if re.search(r"[:#\[\]{}&,*!|>'\"%@`]", text):
            return '"' + text.replace('"', '\\"') + '"'
        return text

    def _bullet_list(self, items: Iterable[Any]) -> str:
        return "\n".join(f"- {str(item)}" for item in items)

    def _string_list(self, value: Any) -> List[str]:
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item or "").strip()]
        if value:
            return [str(value).strip()]
        return []

    def _stock_list(self, value: Any) -> List[Dict[str, Any]]:
        if not isinstance(value, list):
            return []
        return [item for item in value if isinstance(item, dict)]

    def _extract_stock_mentions(self, documents: Iterable[KnowledgeDocument]) -> List[Dict[str, Any]]:
        mentions: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for document in documents:
            text = "\n".join(
                [
                    document.title or "",
                    document.abstract or "",
                    document.introduction or "",
                    document.content_text or "",
                ]
            )
            for match in STOCK_CODE_PATTERN.finditer(text):
                code = match.group(2)
                if code in seen:
                    continue
                seen.add(code)
                mentions.append(
                    {
                        "name": match.group(1).strip(),
                        "code": code,
                        "sector": "",
                        "summary": "",
                        "reason": "",
                        "source_title": document.title,
                    }
                )
        return mentions

    def _extend_unique(self, target: List[str], values: Iterable[str]) -> None:
        seen = set(target)
        for value in values:
            text = str(value or "").strip()
            if text and text not in seen:
                target.append(text)
                seen.add(text)

    def _extend_unique_dicts(self, target: List[Dict[str, Any]], values: Iterable[Dict[str, Any]], *, key: str) -> None:
        seen = {str(item.get(key) or item.get("title") or item.get("name") or item) for item in target}
        for value in values:
            value_key = str(value.get(key) or value.get("title") or value.get("name") or value)
            if value_key and value_key not in seen:
                target.append(value)
                seen.add(value_key)

    def _source_payload(self, document: KnowledgeDocument) -> Dict[str, Any]:
        return {
            "title": document.title,
            "url": document.jump_url or document.source_path or "",
            "source_name": document.source_name,
            "trade_date": document.trade_date.isoformat() if document.trade_date else "",
        }

    def _source_label(self, source: Dict[str, Any]) -> str:
        title = source.get("title") or "来源"
        url = source.get("url") or ""
        return f"[{title}]({url})" if url else title

    def _stock_label(self, stock: Dict[str, Any]) -> str:
        name = str(stock.get("name") or stock.get("stock_name") or "").strip()
        code = str(stock.get("code") or stock.get("stock_code") or "").strip()
        if name and code:
            return f"{name}({code})"
        return name or code

    def _hash_json(self, value: Any) -> str:
        return hashlib.sha256(str(value).encode("utf-8", errors="ignore")).hexdigest()

    def _relative(self, vault: Path, path: Path) -> str:
        return path.relative_to(vault).as_posix()

    def _slug(self, value: Any) -> str:
        text = re.sub(r"\s+", "-", str(value or "").strip())
        text = re.sub(r'[\\/:*?"<>|]', "-", text)
        text = text.strip(".-")
        return text or "untitled"


obsidian_knowledge_service = ObsidianKnowledgeService()
