from __future__ import annotations

import hashlib
import asyncio
import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from types import SimpleNamespace
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import parse_qs, urlparse

import httpx
from sqlalchemy import String, cast, or_, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.intelligence import (
    DailyInfoDigest,
    JiegeModeSignal,
    JiegeTradingRule,
    KnowledgeDocument,
)
from app.models.market_review import DailyAnalysisRecord, MarketReviewDailyMetric, MarketReviewStockDaily
from app.services.daily_analysis_service import daily_analysis_service
from app.utils.logger import logger
from app.utils.time_utils import today_cn


@dataclass(frozen=True)
class ImaKnowledgeSource:
    key: str
    name: str
    share_id: str
    kind: str


DEFAULT_SOURCES = [
    ImaKnowledgeSource(
        key="daily",
        name="每日复盘更新",
        share_id=settings.IMA_DAILY_REVIEW_SHARE_ID,
        kind="daily",
    ),
    ImaKnowledgeSource(
        key="jiege",
        name="杰哥学霸圈",
        share_id=settings.IMA_JIEGE_SHARE_ID,
        kind="jiege",
    ),
]


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8", errors="ignore")).hexdigest()


def _json_hash(value: Any) -> str:
    return _sha256_text(json.dumps(value, ensure_ascii=False, sort_keys=True, default=str))


def _strip_ai_summary_prefix(value: str) -> str:
    return re.sub(r"^\s*AI摘要[:：]\s*", "", value or "").strip()


_STOCK_CODE_PATTERN = re.compile(r"([\u4e00-\u9fa5A-Za-z0-9·]{2,16})[（(]([0368]\d{5})[）)]")
_STOCK_EVENT_PATTERN = re.compile(
    r"([\u4e00-\u9fa5A-Za-z0-9·]{2,16})"
    r"(?:Q[1-4]|目标价|业绩|订单|公告|涨停|跌停|发布|突破|量产|中标|营收|首飞|发射|合作|回购|增持|减持)"
)
_ENGLISH_ENTITY_PATTERN = re.compile(r"\b(?:SpaceX|Figure|NVIDIA)\b", re.IGNORECASE)
_KNOWN_ENTITY_PREFIXES = [
    "长鑫存储",
    "寒武纪",
    "中际旭创",
    "新易盛",
    "工业富联",
    "赛微电子",
    "英伟达",
    "SpaceX",
    "Figure",
    "NVIDIA",
    "百度",
    "长鑫",
]
_GENERIC_STOCK_TERMS = {
    "人工智能",
    "机器人",
    "半导体",
    "商业航天",
    "光通信",
    "集成电路",
    "产业链",
    "新闻联播",
    "日系龙头",
    "核心AI新业务",
}


def _stock_document_text(document: Any) -> str:
    summary = getattr(document, "summary_json", None) or {}
    summary_text = json.dumps(summary, ensure_ascii=False, default=str) if summary else ""
    return "\n".join(
        str(value or "")
        for value in [
            getattr(document, "title", ""),
            getattr(document, "abstract", ""),
            getattr(document, "introduction", ""),
            getattr(document, "content_text", ""),
            summary_text,
        ]
    )


def _normalize_stock_name(raw_name: str) -> str:
    name = re.sub(r"\s+", "", str(raw_name or ""))
    name = name.strip(" #*`[]【】《》“”\"'，。；;、:：()（）")
    name = re.sub(r"^(?:关注|提及|观察|包括|以及|和|与|受益|标的|个股|公司|消息称|后续关注)", "", name)
    for prefix in sorted(_KNOWN_ENTITY_PREFIXES, key=len, reverse=True):
        if name.startswith(prefix):
            return prefix
    return name


def _mention_reason(text: str, name: str, start: int = 0) -> str:
    window = text[max(0, start - 80): start + 180]
    for segment in re.split(r"[。！？!?；;\n\r]", window):
        segment = re.sub(r"\s+", " ", segment).strip()
        if name and name in segment:
            return segment[:160]
    return re.sub(r"\s+", " ", window).strip()[:160]


def _is_valid_stock_name(name: str) -> bool:
    if len(name) < 2 or len(name) > 16:
        return False
    if name in _GENERIC_STOCK_TERMS:
        return False
    if re.fullmatch(r"(?:AI|A股|Q[1-4]|PCB|CPU|GPU|\d+)", name, flags=re.IGNORECASE):
        return False
    return True


def _is_known_stock_entity(name: str) -> bool:
    return _normalize_stock_name(name) in _KNOWN_ENTITY_PREFIXES


def _extract_stock_mentions_from_documents(documents: Iterable[Any], *, limit: int = 20) -> List[Dict[str, Any]]:
    mentions: List[Dict[str, Any]] = []
    seen: set[str] = set()

    def add(
        name: str,
        *,
        code: str = "",
        reason: str = "",
        source_title: str = "",
        require_known_without_code: bool = False,
    ) -> None:
        normalized = _normalize_stock_name(name)
        if not _is_valid_stock_name(normalized):
            return
        normalized_code = str(code or "").strip()
        if require_known_without_code and not normalized_code and not _is_known_stock_entity(normalized):
            return
        key = normalized_code or normalized.lower()
        if key in seen:
            return
        seen.add(key)
        payload = {"name": normalized, "code": normalized_code, "reason": reason[:160], "source_title": source_title}
        mentions.append(payload)

    for document in documents:
        source_title = str(getattr(document, "title", "") or "")
        summary = getattr(document, "summary_json", None) or {}
        for item in (summary.get("stocks") or summary.get("mentioned_stocks") or []):
            if isinstance(item, dict):
                add(
                    str(item.get("name") or ""),
                    code=str(item.get("code") or ""),
                    reason=str(item.get("reason") or ""),
                    source_title=str(item.get("source_title") or source_title),
                    require_known_without_code=True,
                )
            elif item:
                add(str(item), source_title=source_title, require_known_without_code=True)

        text = _stock_document_text(document)
        for match in _STOCK_CODE_PATTERN.finditer(text):
            name = _normalize_stock_name(match.group(1))
            add(name, code=match.group(2), reason=_mention_reason(text, name, match.start()), source_title=source_title)

        for match in _STOCK_EVENT_PATTERN.finditer(text):
            name = _normalize_stock_name(match.group(1))
            if name not in _KNOWN_ENTITY_PREFIXES:
                continue
            add(name, reason=_mention_reason(text, name, match.start()), source_title=source_title)

        for match in _ENGLISH_ENTITY_PATTERN.finditer(text):
            raw_name = match.group(0)
            name = "英伟达" if raw_name.lower() == "nvidia" else raw_name
            add(name, reason=_mention_reason(text, name, match.start()), source_title=source_title)

        if len(mentions) >= limit:
            break
    return mentions[:limit]


def _merge_stock_mentions(summary: Dict[str, Any], documents: Iterable[Any]) -> Dict[str, Any]:
    merged = dict(summary or {})
    model_mentions = merged.get("mentioned_stocks") or merged.get("stocks") or []
    pseudo_document = SimpleNamespace(title="", abstract="", introduction="", content_text="", summary_json={"stocks": model_mentions})
    combined = _extract_stock_mentions_from_documents([pseudo_document, *list(documents)])
    if combined:
        merged["mentioned_stocks"] = combined
    return merged


class ImaWikiClient:
    def __init__(self, timeout: Optional[float] = None):
        self.timeout = timeout or settings.CRAWLER_REQUEST_TIMEOUT
        self.share_api = "https://ima.qq.com/cgi-bin/knowledge_share_get/get_share_info"
        self.report_api = "https://ima.qq.com/cgi-bin/mission_report_manage/get_task_report"
        self.headers = {"User-Agent": settings.CRAWLER_USER_AGENT}

    async def get_share_page(
        self,
        share_id: str,
        *,
        cursor: str = "",
        folder_id: str = "",
        limit: int = 20,
    ) -> Dict[str, Any]:
        payload = {
            "shareId": share_id,
            "cursor": cursor,
            "limit": limit,
            "folderId": folder_id,
        }
        async with httpx.AsyncClient(timeout=self.timeout, headers=self.headers) as client:
            response = await client.post(self.share_api, json=payload)
            response.raise_for_status()
            return response.json()

    async def fetch_markdown(self, url: str) -> str:
        async with httpx.AsyncClient(timeout=self.timeout, headers=self.headers) as client:
            response = await client.get(url)
            response.raise_for_status()
            return response.text

    async def fetch_report_markdown(self, source_path: str) -> str:
        report_id = self._extract_report_id(source_path)
        if not report_id:
            return ""
        async with httpx.AsyncClient(timeout=self.timeout, headers=self.headers) as client:
            response = await client.post(self.report_api, json={"reportId": report_id})
            response.raise_for_status()
            data = response.json()
        if data.get("code") != 0:
            raise RuntimeError(data.get("msg") or f"ima report error: {data.get('code')}")
        return self._extract_report_text(data)

    def _extract_report_id(self, source_path: str) -> str:
        parsed = urlparse(source_path or "")
        return (parse_qs(parsed.query).get("reportId") or [""])[0]

    def _extract_report_text(self, data: Dict[str, Any]) -> str:
        blocks = ((data.get("format_answer") or {}).get("block_list") or [])
        texts: List[str] = []
        for raw_block in blocks:
            try:
                block = json.loads(raw_block) if isinstance(raw_block, str) else raw_block
            except json.JSONDecodeError:
                continue
            text = ((block.get("Data") or {}).get("Text") or "").strip()
            if text:
                texts.append(text)
        return "\n\n".join(texts)


class DeepSeekSummaryClient:
    def __init__(self, settings=settings):
        self.settings = settings
        self.api_key = getattr(settings, "DEEPSEEK_API_KEY", None)
        self.base_url = getattr(settings, "DEEPSEEK_BASE_URL", "https://api.deepseek.com").rstrip("/")
        self.model = getattr(settings, "DEEPSEEK_MODEL", "deepseek-v4-pro")
        self.timeout = getattr(settings, "DEEPSEEK_REQUEST_TIMEOUT", 60)

    async def summarize_document(self, document: KnowledgeDocument) -> Dict[str, Any]:
        fallback = self._fallback_document_summary(document, "missing_api_key" if not self.api_key else "fallback")
        if not self.api_key:
            return fallback

        prompt = {
            "title": document.title,
            "media_type": document.media_type_name,
            "abstract": document.abstract,
            "content": (document.content_text or "")[:18000],
        }
        try:
            return await self._request_json(
                "你是A股短线复盘助手。请从资料中提炼结构化摘要，只输出JSON。",
                (
                    "按以下键输出JSON：summary(100字内), themes(数组), catalysts(数组), "
                    "risks(数组), sectors(数组), stocks(数组，每项包含name,code,reason；只列资料明确提及对象), "
                    "trade_date_hint(字符串或空)。资料："
                    f"{json.dumps(prompt, ensure_ascii=False)}"
                ),
                fallback,
            )
        except Exception as exc:
            logger.warning(f"DeepSeek document summary failed: {exc}")
            fallback["model_status"] = "error"
            fallback["error"] = str(exc)
            return fallback

    async def summarize_daily_info(self, trade_date: date, documents: List[KnowledgeDocument]) -> Dict[str, Any]:
        fallback = self._fallback_daily_summary(trade_date, documents, "missing_api_key" if not self.api_key else "fallback")
        if not self.api_key:
            return fallback

        compact_docs = [
            {
                "title": doc.title,
                "type": doc.media_type_name,
                "summary": (doc.summary_json or {}).get("summary") or _strip_ai_summary_prefix(doc.abstract),
                "themes": (doc.summary_json or {}).get("themes") or [],
                "catalysts": (doc.summary_json or {}).get("catalysts") or [],
                "risks": (doc.summary_json or {}).get("risks") or [],
                "sectors": (doc.summary_json or {}).get("sectors") or [],
                "stocks": (doc.summary_json or {}).get("stocks") or (doc.summary_json or {}).get("mentioned_stocks") or [],
            }
            for doc in documents
        ]
        try:
            return await self._request_json(
                "你是A股每日资讯编辑。请聚合资料，输出严格JSON。",
                (
                    "按以下键输出JSON：overview, main_lines(数组), catalysts(数组), "
                    "risks(数组), plan, source_titles(数组), "
                    "mentioned_stocks(数组，每项包含name,code,reason,source_title；仅列资料明确提及对象)。"
                    "不要给买卖建议，只做资讯总结。"
                    f"日期：{trade_date.isoformat()}。资料：{json.dumps(compact_docs, ensure_ascii=False)}"
                ),
                fallback,
            )
        except Exception as exc:
            logger.warning(f"DeepSeek daily summary failed: {exc}")
            fallback["model_status"] = "error"
            fallback["error"] = str(exc)
            return fallback

    async def build_jiege_rules(self, documents: List[KnowledgeDocument]) -> List[Dict[str, Any]]:
        fallback = self._fallback_jiege_rules()
        if not self.api_key:
            return fallback

        compact_docs = [
            {
                "title": doc.title,
                "summary": (doc.summary_json or {}).get("summary") or _strip_ai_summary_prefix(doc.abstract),
                "content": (doc.content_text or "")[:12000],
            }
            for doc in documents[:6]
        ]
        try:
            payload = await self._request_json(
                "你是交易系统规则分析助手。请从资料中总结可执行但不构成投资建议的规则。",
                (
                    "输出JSON对象，包含rules数组。每个规则包含rule_key,title,category,summary,payload。"
                    "覆盖L1市场环境、L2催化剂、L3交易模式、L4风控、CKL、880005情绪周期。"
                    f"资料：{json.dumps(compact_docs, ensure_ascii=False)}"
                ),
                {"rules": fallback, "model_status": "fallback"},
            )
            rules = payload.get("rules") if isinstance(payload, dict) else None
            return rules if isinstance(rules, list) and rules else fallback
        except Exception as exc:
            logger.warning(f"DeepSeek Jiege rule build failed: {exc}")
            return fallback

    async def _request_json(self, system_prompt: str, user_prompt: str, fallback: Any) -> Any:
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                f"{self.base_url}/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": self.model,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    "temperature": 0.2,
                    "response_format": {"type": "json_object"},
                },
            )
            response.raise_for_status()
            data = response.json()
        content = (((data.get("choices") or [{}])[0].get("message") or {}).get("content") or "").strip()
        parsed = self._parse_json_content(content)
        if isinstance(parsed, dict):
            parsed.setdefault("model", self.model)
            parsed.setdefault("model_status", "ready")
            return parsed
        return fallback

    def _parse_json_content(self, content: str) -> Any:
        if not content:
            return None
        cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", content.strip(), flags=re.IGNORECASE)
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError:
            match = re.search(r"(\{.*\}|\[.*\])", cleaned, flags=re.DOTALL)
            if not match:
                return None
            try:
                return json.loads(match.group(1))
            except json.JSONDecodeError:
                return None

    def _fallback_document_summary(self, document: KnowledgeDocument, status: str) -> Dict[str, Any]:
        summary = _strip_ai_summary_prefix(document.abstract) or (document.introduction or document.content_text or "")[:160]
        return {
            "summary": summary,
            "themes": [],
            "catalysts": [],
            "risks": [],
            "sectors": [],
            "stocks": _extract_stock_mentions_from_documents([document]),
            "model": self.model,
            "model_status": status,
        }

    def _fallback_daily_summary(self, trade_date: date, documents: List[KnowledgeDocument], status: str) -> Dict[str, Any]:
        titles = [doc.title for doc in documents]
        summaries = [
            (doc.summary_json or {}).get("summary") or _strip_ai_summary_prefix(doc.abstract)
            for doc in documents[:6]
        ]
        return {
            "overview": "；".join(value for value in summaries if value)[:500],
            "main_lines": self._unique_from_docs(documents, "themes"),
            "catalysts": self._unique_from_docs(documents, "catalysts"),
            "risks": self._unique_from_docs(documents, "risks"),
            "plan": "基于已同步资料观察主线承接与风险变化。",
            "source_titles": titles,
            "mentioned_stocks": _extract_stock_mentions_from_documents(documents),
            "trade_date": trade_date.isoformat(),
            "model": self.model,
            "model_status": status,
        }

    def _unique_from_docs(self, documents: List[KnowledgeDocument], key: str) -> List[str]:
        values: List[str] = []
        for doc in documents:
            for item in (doc.summary_json or {}).get(key) or []:
                if item and item not in values:
                    values.append(str(item))
        return values[:8]

    def _fallback_jiege_rules(self) -> List[Dict[str, Any]]:
        return [
            {
                "rule_key": "l1-market",
                "title": "L1 市场环境",
                "category": "L1",
                "summary": "市场环境决定交易权限和仓位上限，重点观察上涨家数、量能、封板率、连板高度和亏钱效应。",
                "payload": {"signals": ["up_count_ex_st", "market_turnover", "seal_rate", "max_board_height"]},
            },
            {
                "rule_key": "l2-catalyst",
                "title": "L2 催化剂分层",
                "category": "L2",
                "summary": "产业验证、政策确认、订单业绩和供给侧断裂优先级高；情绪消息和小作文不能提高仓位权限。",
                "payload": {"tiers": ["产业验证", "政策确认", "订单业绩", "供给侧断裂", "情绪消息"]},
            },
            {
                "rule_key": "l3-patterns",
                "title": "L3 交易模式",
                "category": "L3",
                "summary": "关注容量首板、二波反包、趋势突破、炸板反包、20cm高度和板块主线扩散。",
                "payload": {"patterns": ["首板容量", "二波", "反包", "趋势", "20cm"]},
            },
            {
                "rule_key": "l4-risk",
                "title": "L4 风控否决",
                "category": "L4",
                "summary": "买点失效、止损位跌破、仓位越界、情绪化交易时，任何正向产业逻辑都不能覆盖退出纪律。",
                "payload": {"veto": ["买点失效", "止损不明", "仓位越界", "情绪失控"]},
            },
            {
                "rule_key": "ckl",
                "title": "CKL 认知评分",
                "category": "CKL",
                "summary": "CKL衡量认知准备度，但必须经过L4执行纪律校正，不能直接替代风控。",
                "payload": {"weights": ["深度", "偏差", "覆盖", "更新", "优势", "执行校正"]},
            },
        ]


class IntelligenceService:
    def __init__(
        self,
        *,
        ima_client: Optional[Any] = None,
        summary_client: Optional[Any] = None,
        sources: Optional[List[ImaKnowledgeSource]] = None,
    ):
        self.ima_client = ima_client or ImaWikiClient()
        self.summary_client = summary_client or DeepSeekSummaryClient()
        self.sources = {source.key: source for source in (sources if sources is not None else DEFAULT_SOURCES)}
        self._refreshing_daily_dates: set[date] = set()

    async def sync_all(self, db: AsyncSession, *, force_daily: bool = False) -> Dict[str, Any]:
        results = {}
        changed_daily_dates: set[date] = set()
        for source_key in self.sources:
            results[source_key] = await self.sync_source(db, source_key)
            if source_key == "daily":
                changed_daily_dates.update(
                    date.fromisoformat(value)
                    for value in results[source_key].get("changed_trade_dates", [])
                    if value
                )
        today = today_cn()
        dates_to_build = [today]
        dates_to_build.extend(sorted((item for item in changed_daily_dates if item != today), reverse=True))
        daily = None
        for target_date in dates_to_build:
            daily = await self.build_daily_info(
                db,
                target_date,
                allow_latest_fallback=target_date == today,
                force=force_daily or target_date in changed_daily_dates,
            )
        jiege = await self.build_jiege_mode(db, today, allow_latest_fallback=True)
        return {"sources": results, "daily_info": daily, "jiege_mode": jiege}

    async def sync_source(self, db: AsyncSession, source_key: str) -> Dict[str, Any]:
        source = self.sources[source_key]
        items = await self._collect_source_items(source)
        changed = 0
        summarized = 0
        changed_trade_dates: set[date] = set()
        for entry in items:
            is_changed, is_summarized, trade_date = await self._sync_source_entry(db, source, entry)
            if is_changed:
                changed += 1
                summarized += 1 if is_summarized else 0
                if source.key == "daily" and trade_date:
                    changed_trade_dates.add(trade_date)
        return {
            "source_key": source.key,
            "total_documents": len(items),
            "changed_documents": changed,
            "summarized_documents": summarized,
            "changed_trade_dates": [item.isoformat() for item in sorted(changed_trade_dates, reverse=True)],
        }

    async def _sync_source_entry(
        self,
        db: AsyncSession,
        source: ImaKnowledgeSource,
        entry: Dict[str, Any],
    ) -> tuple[bool, bool, Optional[date]]:
        for attempt in range(5):
            try:
                doc, is_changed = await self._upsert_document_from_entry(db, source, entry)
                await db.flush()
                snapshot = self._snapshot_document(doc)
                await db.commit()
                if not is_changed:
                    return False, False, snapshot.trade_date
                await self._summarize_document(snapshot)
                await self._write_document_summary_with_retry(
                    db,
                    snapshot.id,
                    snapshot.summary_json,
                    snapshot.summary_status,
                    snapshot.summary_error,
                )
                return True, snapshot.summary_status == "ready", snapshot.trade_date
            except OperationalError as exc:
                await db.rollback()
                if not self._is_database_locked(exc) or attempt == 4:
                    raise
                await asyncio.sleep(1.5 * (attempt + 1))
        raise RuntimeError("failed to sync intelligence source entry")

    async def build_daily_info(
        self,
        db: AsyncSession,
        trade_date: date,
        *,
        allow_latest_fallback: bool = False,
        force: bool = False,
    ) -> Dict[str, Any]:
        documents = await self._get_daily_documents(db, trade_date)
        actual_date = trade_date
        if not documents and allow_latest_fallback:
            actual_date = await self._latest_daily_trade_date(db) or trade_date
            documents = await self._get_daily_documents(db, actual_date)
        document_snapshots = [self._snapshot_document(doc) for doc in documents]
        await db.commit()
        await self._refresh_stale_document_summaries(db, document_snapshots)
        content_hash = _json_hash(
            [{"id": doc.id, "hash": doc.content_hash, "summary": doc.summary_json} for doc in document_snapshots]
        )
        with db.no_autoflush:
            existing = await self._get_daily_digest(db, actual_date)
        if existing and existing.content_hash == content_hash and not force:
            await db.commit()
            return self.serialize_daily_digest(existing, cache_hit=True, sources=document_snapshots)

        await db.commit()
        summary = await self.summary_client.summarize_daily_info(actual_date, document_snapshots)
        summary = _merge_stock_mentions(summary, document_snapshots)
        existing = await self._write_daily_digest_with_retry(
            db,
            actual_date,
            summary,
            status="ready" if documents else "empty",
            source_count=len(documents),
            content_hash=content_hash,
        )
        return self.serialize_daily_digest(existing, cache_hit=False, sources=document_snapshots)

    def daily_digest_needs_model_refresh(self, digest: DailyInfoDigest) -> bool:
        return self._has_api_key() and self._summary_from_missing_api_key(digest.summary_json)

    async def refresh_daily_info_in_background(self, trade_date: date) -> None:
        if trade_date in self._refreshing_daily_dates:
            return
        self._refreshing_daily_dates.add(trade_date)
        try:
            from app.database import async_session_maker

            logger.info(f"Background daily intelligence refresh started for {trade_date}")
            async with async_session_maker() as db:
                await self.build_daily_info(db, trade_date, allow_latest_fallback=True, force=True)
            logger.info(f"Background daily intelligence refresh finished for {trade_date}")
        except Exception as exc:
            logger.warning(f"Background daily intelligence refresh failed for {trade_date}: {exc}")
        finally:
            self._refreshing_daily_dates.discard(trade_date)

    def _snapshot_document(self, doc: KnowledgeDocument) -> SimpleNamespace:
        return SimpleNamespace(
            id=doc.id,
            source_name=doc.source_name,
            source_key=doc.source_key,
            title=doc.title,
            media_type_name=doc.media_type_name,
            abstract=doc.abstract,
            introduction=doc.introduction,
            content_text=doc.content_text,
            content_hash=doc.content_hash,
            trade_date=doc.trade_date,
            update_time=doc.update_time,
            jump_url=doc.jump_url,
            source_path=doc.source_path,
            summary_json=dict(doc.summary_json or {}),
            summary_status=doc.summary_status,
            summary_error=doc.summary_error or "",
        )

    async def _refresh_stale_document_summaries(self, db: AsyncSession, documents: List[SimpleNamespace]) -> None:
        if not self._has_api_key():
            return
        for doc in documents:
            if self._summary_from_missing_api_key(doc.summary_json):
                await self._summarize_document(doc)
                await self._write_document_summary_with_retry(
                    db,
                    doc.id,
                    doc.summary_json,
                    doc.summary_status,
                    doc.summary_error,
                )

    async def _write_document_summary_with_retry(
        self,
        db: AsyncSession,
        doc_id: int,
        summary_json: Dict[str, Any],
        summary_status: str,
        summary_error: str,
    ) -> None:
        summary_json = dict(summary_json or {})
        summary_error = summary_error or ""
        for attempt in range(5):
            try:
                target = await db.get(KnowledgeDocument, doc_id)
                if target is None:
                    return
                target.summary_json = summary_json
                target.summary_status = summary_status
                target.summary_error = summary_error
                target.updated_at = datetime.now()
                await db.commit()
                return
            except OperationalError as exc:
                await db.rollback()
                if not self._is_database_locked(exc) or attempt == 4:
                    raise
                await asyncio.sleep(1.5 * (attempt + 1))

    async def _write_daily_digest_with_retry(
        self,
        db: AsyncSession,
        trade_date: date,
        summary: Dict[str, Any],
        *,
        status: str,
        source_count: int,
        content_hash: str,
    ) -> DailyInfoDigest:
        for attempt in range(5):
            try:
                existing = await self._get_daily_digest(db, trade_date)
                now = datetime.now()
                if existing is None:
                    existing = DailyInfoDigest(trade_date=trade_date, created_at=now)
                    db.add(existing)
                existing.summary_json = summary
                existing.status = status
                existing.source_count = source_count
                existing.content_hash = content_hash
                existing.model = summary.get("model", getattr(self.summary_client, "model", ""))
                existing.generated_at = now
                existing.updated_at = now
                await db.commit()
                await db.refresh(existing)
                return existing
            except OperationalError as exc:
                await db.rollback()
                if not self._is_database_locked(exc) or attempt == 4:
                    raise
                await asyncio.sleep(1.5 * (attempt + 1))
        raise RuntimeError("failed to write daily intelligence digest")

    def _is_database_locked(self, exc: OperationalError) -> bool:
        return "database is locked" in str(exc).lower()

    def _has_api_key(self) -> bool:
        return bool(getattr(self.summary_client, "api_key", None))

    def _summary_from_missing_api_key(self, summary: Optional[Dict[str, Any]]) -> bool:
        return (summary or {}).get("model_status") == "missing_api_key"

    async def build_jiege_rules(self, db: AsyncSession, *, force: bool = False) -> List[Dict[str, Any]]:
        documents = await self._get_jiege_rule_documents(db)
        content_hash = _json_hash([{"id": doc.id, "hash": doc.content_hash} for doc in documents])
        if not force:
            existing = (await db.execute(select(JiegeTradingRule))).scalars().all()
            if existing and all(rule.content_hash == content_hash for rule in existing):
                return [self.serialize_rule(rule) for rule in existing]
        rules = await self.summary_client.build_jiege_rules(documents)
        for rule in rules:
            stmt = sqlite_insert(JiegeTradingRule).values(
                rule_key=str(rule.get("rule_key") or rule.get("title") or "rule"),
                title=str(rule.get("title") or rule.get("rule_key") or "规则"),
                category=str(rule.get("category") or ""),
                summary=str(rule.get("summary") or ""),
                payload_json=rule.get("payload") or {},
                source_media_id=",".join(doc.media_id for doc in documents[:4]),
                content_hash=content_hash,
                updated_at=datetime.now(),
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["rule_key"],
                set_={
                    "title": stmt.excluded.title,
                    "category": stmt.excluded.category,
                    "summary": stmt.excluded.summary,
                    "payload_json": stmt.excluded.payload_json,
                    "source_media_id": stmt.excluded.source_media_id,
                    "content_hash": stmt.excluded.content_hash,
                    "updated_at": datetime.now(),
                },
            )
            await db.execute(stmt)
        await db.commit()
        saved = (await db.execute(select(JiegeTradingRule).order_by(JiegeTradingRule.id))).scalars().all()
        return [self.serialize_rule(rule) for rule in saved]

    async def build_jiege_mode(
        self,
        db: AsyncSession,
        trade_date: date,
        *,
        allow_latest_fallback: bool = False,
        force: bool = False,
    ) -> Dict[str, Any]:
        rules = await self.build_jiege_rules(db, force=force)
        actual_date = trade_date
        metric = await self._get_metric(db, actual_date)
        stocks = await self._get_review_stocks(db, actual_date)
        if allow_latest_fallback and not metric and not stocks:
            actual_date = await self._latest_market_review_date(db) or trade_date
            metric = await self._get_metric(db, actual_date)
            stocks = await self._get_review_stocks(db, actual_date)
        payload = await self._build_jiege_signal_payload(db, actual_date, rules, metric, stocks)
        content_hash = _json_hash(payload)
        existing = await self._get_jiege_signal(db, actual_date)
        if existing and existing.content_hash == content_hash and not force:
            return self.serialize_jiege_signal(existing, cache_hit=True)
        now = datetime.now()
        if existing is None:
            existing = JiegeModeSignal(trade_date=actual_date)
            db.add(existing)
        existing.signal_json = payload
        existing.status = "ready"
        existing.content_hash = content_hash
        existing.generated_at = now
        existing.updated_at = now
        await db.commit()
        await db.refresh(existing)
        return self.serialize_jiege_signal(existing, cache_hit=False)

    async def _collect_source_items(self, source: ImaKnowledgeSource) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        visited_folders = {""}
        folders_to_scan = [""]
        while folders_to_scan:
            folder_id = folders_to_scan.pop(0)
            cursor = ""
            page_count = 0
            while True:
                page_count += 1
                if page_count > settings.IMA_SYNC_MAX_PAGES:
                    break
                data = await self.ima_client.get_share_page(
                    source.share_id,
                    cursor=cursor,
                    folder_id=folder_id,
                    limit=settings.IMA_SYNC_PAGE_SIZE,
                )
                if data.get("code") != 0:
                    raise RuntimeError(data.get("msg") or f"ima share error: {data.get('code')}")
                current_path = data.get("current_path") or []
                folder_path = " / ".join(path.get("name") or "" for path in current_path if path.get("name"))
                source_version = str(data.get("version") or "")
                for item in data.get("knowledge_list") or []:
                    normalized = dict(item)
                    normalized["_source_version"] = source_version
                    normalized["_folder_id"] = folder_id
                    normalized["_folder_path"] = folder_path
                    items.append(normalized)
                    folder_info = item.get("folder_info") or {}
                    next_folder_id = folder_info.get("folder_id")
                    if next_folder_id and next_folder_id not in visited_folders:
                        visited_folders.add(next_folder_id)
                        folders_to_scan.append(next_folder_id)
                if data.get("is_end") or not data.get("next_cursor"):
                    break
                cursor = data.get("next_cursor") or ""
        return items

    async def _upsert_document_from_entry(
        self,
        db: AsyncSession,
        source: ImaKnowledgeSource,
        entry: Dict[str, Any],
    ) -> tuple[KnowledgeDocument, bool]:
        media_id = str(entry.get("media_id") or entry.get("folder_info", {}).get("folder_id") or entry.get("title") or "")
        result = await db.execute(
            select(KnowledgeDocument).where(
                KnowledgeDocument.source_key == source.key,
                KnowledgeDocument.media_id == media_id,
            )
        )
        doc = result.scalar_one_or_none()
        is_new = doc is None
        if doc is None:
            doc = KnowledgeDocument(source_key=source.key, source_name=source.name, share_id=source.share_id, media_id=media_id)
            db.add(doc)

        metadata_changed = is_new or any(
            getattr(doc, attr) != str(entry.get(key) or "")
            for attr, key in (
                ("md5_sum", "md5_sum"),
                ("update_time", "update_time"),
                ("jump_url", "jump_url"),
                ("source_path", "source_path"),
                ("abstract", "abstract"),
                ("introduction", "introduction"),
            )
        )
        self._apply_entry_metadata(doc, source, entry, media_id)
        if metadata_changed:
            doc.content_text = await self._resolve_content_text(entry)
            doc.content_hash = self._build_document_hash(doc)
            doc.trade_date = self._infer_trade_date(doc)
            doc.summary_status = "pending"
            doc.summary_error = ""
            doc.updated_at = datetime.now()
        return doc, metadata_changed

    def _apply_entry_metadata(self, doc: KnowledgeDocument, source: ImaKnowledgeSource, entry: Dict[str, Any], media_id: str) -> None:
        media_type_info = entry.get("media_type_info") or {}
        doc.source_key = source.key
        doc.source_name = source.name
        doc.share_id = source.share_id
        doc.source_version = str(entry.get("_source_version") or "")
        doc.folder_id = str(entry.get("_folder_id") or entry.get("parent_folder_id") or "")
        doc.folder_path = str(entry.get("_folder_path") or "")
        doc.media_id = media_id
        doc.title = str(entry.get("title") or "")
        doc.media_type = int(entry.get("media_type") or 0)
        doc.media_type_name = str(media_type_info.get("name") or "")
        doc.md5_sum = str(entry.get("md5_sum") or "")
        doc.update_time = str(entry.get("update_time") or "")
        doc.create_time = str(entry.get("create_time") or "")
        doc.source_path = str(entry.get("source_path") or "")
        doc.jump_url = str(entry.get("jump_url") or "")
        doc.raw_file_url = str(entry.get("raw_file_url") or "")
        doc.abstract = str(entry.get("abstract") or "")
        doc.introduction = str(entry.get("introduction") or "")

    async def _resolve_content_text(self, entry: Dict[str, Any]) -> str:
        media_name = ((entry.get("media_type_info") or {}).get("name") or "").lower()
        jump_url = str(entry.get("jump_url") or "")
        source_path = str(entry.get("source_path") or "")
        abstract = str(entry.get("abstract") or "")
        introduction = str(entry.get("introduction") or "")
        if media_name == "md" and jump_url:
            return await self.ima_client.fetch_markdown(jump_url)
        if "ima" in media_name and "报告" in media_name and "reportId=" in source_path:
            return await self.ima_client.fetch_report_markdown(source_path)
        return "\n\n".join(value for value in [abstract, introduction] if value)

    async def _summarize_document(self, doc: KnowledgeDocument) -> None:
        try:
            doc.summary_json = await self.summary_client.summarize_document(doc)
            doc.summary_status = "ready"
        except Exception as exc:
            doc.summary_json = {}
            doc.summary_status = "error"
            doc.summary_error = str(exc)

    def _build_document_hash(self, doc: KnowledgeDocument) -> str:
        return _sha256_text(
            "\n".join([
                doc.title or "",
                doc.media_type_name or "",
                doc.md5_sum or "",
                doc.update_time or "",
                doc.abstract or "",
                doc.introduction or "",
                doc.content_text or "",
            ])
        )

    def _infer_trade_date(self, doc: KnowledgeDocument) -> Optional[date]:
        text = f"{doc.title} {doc.introduction[:200]}"
        patterns = [
            r"(20\d{2})[-/.年](\d{1,2})[-/.月](\d{1,2})",
            r"(20\d{2})(\d{2})(\d{2})",
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
                except ValueError:
                    pass
        short = re.search(r"(?<!\d)(\d{1,2})[./月](\d{1,2})(?:日)?", text)
        if short:
            year = self._year_from_timestamp(doc.update_time) or date.today().year
            try:
                return date(year, int(short.group(1)), int(short.group(2)))
            except ValueError:
                pass
        return self._date_from_timestamp(doc.update_time)

    def _year_from_timestamp(self, value: str) -> Optional[int]:
        parsed = self._date_from_timestamp(value)
        return parsed.year if parsed else None

    def _date_from_timestamp(self, value: str) -> Optional[date]:
        try:
            timestamp = int(value)
        except (TypeError, ValueError):
            return None
        if timestamp > 10_000_000_000:
            timestamp = timestamp // 1000
        try:
            return datetime.fromtimestamp(timestamp).date()
        except (OSError, ValueError):
            return None

    async def _get_daily_documents(self, db: AsyncSession, trade_date: date) -> List[KnowledgeDocument]:
        result = await db.execute(
            select(KnowledgeDocument)
            .where(KnowledgeDocument.source_key == "daily", KnowledgeDocument.trade_date == trade_date)
            .order_by(KnowledgeDocument.update_time.desc(), KnowledgeDocument.id.desc())
        )
        return list(result.scalars().all())

    async def _latest_daily_trade_date(self, db: AsyncSession) -> Optional[date]:
        result = await db.execute(
            select(KnowledgeDocument.trade_date)
            .where(KnowledgeDocument.source_key == "daily", KnowledgeDocument.trade_date.is_not(None))
            .order_by(KnowledgeDocument.trade_date.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def _get_daily_digest(self, db: AsyncSession, trade_date: date) -> Optional[DailyInfoDigest]:
        result = await db.execute(select(DailyInfoDigest).where(DailyInfoDigest.trade_date == trade_date))
        return result.scalar_one_or_none()

    async def list_daily_digests(self, db: AsyncSession, *, limit: int = 30) -> List[Dict[str, Any]]:
        result = await db.execute(
            select(DailyInfoDigest)
            .order_by(DailyInfoDigest.trade_date.desc(), DailyInfoDigest.generated_at.desc(), DailyInfoDigest.id.desc())
            .limit(limit)
        )
        items = []
        for digest in result.scalars().all():
            sources = await self._get_daily_documents(db, digest.trade_date)
            items.append(self.serialize_daily_digest(digest, cache_hit=True, sources=sources))
        return items

    async def search_daily_digests(self, db: AsyncSession, *, keyword: str, limit: int = 50) -> List[Dict[str, Any]]:
        keyword = (keyword or "").strip()
        if not keyword:
            return await self.list_daily_digests(db, limit=limit)

        pattern = f"%{keyword}%"
        document_result = await db.execute(
            select(KnowledgeDocument.trade_date)
            .where(
                KnowledgeDocument.source_key == "daily",
                KnowledgeDocument.trade_date.is_not(None),
                or_(
                    KnowledgeDocument.title.like(pattern),
                    KnowledgeDocument.abstract.like(pattern),
                    KnowledgeDocument.introduction.like(pattern),
                    KnowledgeDocument.content_text.like(pattern),
                    cast(KnowledgeDocument.summary_json, String).like(pattern),
                ),
            )
            .distinct()
        )
        matched_dates = {item for item in document_result.scalars().all() if item is not None}
        conditions = [cast(DailyInfoDigest.summary_json, String).like(pattern)]
        if matched_dates:
            conditions.append(DailyInfoDigest.trade_date.in_(matched_dates))

        result = await db.execute(
            select(DailyInfoDigest)
            .where(or_(*conditions))
            .order_by(DailyInfoDigest.trade_date.desc(), DailyInfoDigest.generated_at.desc(), DailyInfoDigest.id.desc())
            .limit(limit)
        )
        items = []
        for digest in result.scalars().all():
            sources = await self._get_daily_documents(db, digest.trade_date)
            items.append(self.serialize_daily_digest(digest, cache_hit=True, sources=sources))
        return items

    async def serialize_daily_digest_with_sources(
        self,
        db: AsyncSession,
        digest: DailyInfoDigest,
        *,
        cache_hit: bool = False,
    ) -> Dict[str, Any]:
        sources = await self._get_daily_documents(db, digest.trade_date)
        return self.serialize_daily_digest(digest, cache_hit=cache_hit, sources=sources)

    async def get_document_source(self, db: AsyncSession, document_id: int) -> Optional[Dict[str, Any]]:
        document = await db.get(KnowledgeDocument, document_id)
        if document is None:
            return None
        return self.serialize_document_source(document, include_content=True)

    async def _get_jiege_rule_documents(self, db: AsyncSession) -> List[KnowledgeDocument]:
        result = await db.execute(
            select(KnowledgeDocument)
            .where(KnowledgeDocument.source_key == "jiege")
            .order_by(KnowledgeDocument.media_type.desc(), KnowledgeDocument.update_time.desc())
            .limit(8)
        )
        return list(result.scalars().all())

    async def _get_metric(self, db: AsyncSession, trade_date: date) -> Optional[MarketReviewDailyMetric]:
        result = await db.execute(select(MarketReviewDailyMetric).where(MarketReviewDailyMetric.trade_date == trade_date))
        return result.scalar_one_or_none()

    async def _get_review_stocks(self, db: AsyncSession, trade_date: date) -> List[MarketReviewStockDaily]:
        result = await db.execute(
            select(MarketReviewStockDaily)
            .where(MarketReviewStockDaily.trade_date == trade_date)
            .order_by(MarketReviewStockDaily.today_continuous_days.desc(), MarketReviewStockDaily.amount.desc())
        )
        return list(result.scalars().all())

    async def _latest_market_review_date(self, db: AsyncSession) -> Optional[date]:
        result = await db.execute(
            select(MarketReviewDailyMetric.trade_date)
            .order_by(MarketReviewDailyMetric.trade_date.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def _get_jiege_signal(self, db: AsyncSession, trade_date: date) -> Optional[JiegeModeSignal]:
        result = await db.execute(select(JiegeModeSignal).where(JiegeModeSignal.trade_date == trade_date))
        return result.scalar_one_or_none()

    async def _build_jiege_signal_payload(
        self,
        db: AsyncSession,
        trade_date: date,
        rules: List[Dict[str, Any]],
        metric: Optional[MarketReviewDailyMetric],
        stocks: List[MarketReviewStockDaily],
    ) -> Dict[str, Any]:
        market_phase = self._classify_market_phase(metric)
        candidates = self._build_rule_candidates(stocks)
        risk_flags = self._build_risk_flags(metric, stocks)
        daily_analysis = await self._load_daily_analysis_cells(db, trade_date)
        return {
            "trade_date": trade_date.isoformat(),
            "market_phase": market_phase,
            "rules": rules,
            "prediction": {
                "candidates": candidates,
                "daily_analysis": daily_analysis,
                "risk_flags": risk_flags,
            },
            "review": {
                "sealed_count": sum(1 for stock in stocks if stock.today_sealed_close),
                "opened_count": sum(1 for stock in stocks if stock.today_opened_close or stock.today_broken),
                "max_board_height": metric.max_board_height if metric else 0,
                "notes": "基于项目复盘数据验证规则命中结果。",
            },
        }

    def _classify_market_phase(self, metric: Optional[MarketReviewDailyMetric]) -> Dict[str, Any]:
        if not metric:
            return {"label": "暂无复盘数据", "score": 0, "basis": []}
        score = 0
        basis = []
        if metric.seal_rate >= 75:
            score += 30
            basis.append("封板率较高")
        elif metric.seal_rate < 55:
            score -= 20
            basis.append("封板率偏低")
        if metric.limit_up_count >= 70:
            score += 25
            basis.append("涨停数量活跃")
        if metric.max_board_height >= 4:
            score += 20
            basis.append("连板高度打开")
        if metric.limit_down_count >= 10:
            score -= 25
            basis.append("跌停负反馈偏强")
        if metric.up_count_ex_st > metric.down_count_ex_st:
            score += 15
            basis.append("上涨家数占优")
        label = "进攻期" if score >= 50 else "修复期" if score >= 20 else "混沌/退潮期" if score < 0 else "观察期"
        return {"label": label, "score": score, "basis": basis}

    def _build_rule_candidates(self, stocks: List[MarketReviewStockDaily]) -> List[Dict[str, Any]]:
        candidates = []
        for stock in stocks[:12]:
            tags = []
            if stock.today_continuous_days >= 2:
                tags.append(f"{stock.today_continuous_days}板")
            if stock.board_type in {"gem", "star"} or stock.stock_code.startswith(("300", "301", "688")):
                tags.append("20cm")
            if stock.today_opened_close or stock.today_broken or stock.open_count > 0:
                tags.append("分歧")
            if stock.today_sealed_close and stock.today_continuous_days <= 1:
                tags.append("首板观察")
            if not tags:
                tags.append("观察")
            candidates.append({
                "stock_code": stock.stock_code,
                "stock_name": stock.stock_name,
                "label": f"{stock.stock_name}({stock.stock_code})",
                "tags": tags,
                "reason": stock.limit_up_reason or "",
                "score": self._candidate_score(stock),
            })
        candidates.sort(key=lambda item: (-item["score"], item["stock_code"]))
        return candidates[:8]

    def _candidate_score(self, stock: MarketReviewStockDaily) -> float:
        score = stock.today_continuous_days * 12
        if stock.today_sealed_close:
            score += 10
        if stock.first_limit_time and stock.first_limit_time.hour == 9 and stock.first_limit_time.minute <= 35:
            score += 6
        score += min((stock.amount or 0) / 100000, 10)
        score -= min(stock.open_count or 0, 6)
        return round(score, 2)

    def _build_risk_flags(self, metric: Optional[MarketReviewDailyMetric], stocks: List[MarketReviewStockDaily]) -> List[str]:
        flags = []
        if metric and metric.seal_rate < 55:
            flags.append("封板率低，L4降低开仓权限")
        if metric and metric.limit_down_count >= 10:
            flags.append("跌停家数高，注意负反馈扩散")
        if any(stock.today_opened_close or stock.today_broken for stock in stocks[:8]):
            flags.append("核心候选存在开板/炸板，需验证承接")
        return flags

    async def _load_daily_analysis_cells(self, db: AsyncSession, trade_date: date) -> Dict[str, Any]:
        result = await db.execute(select(DailyAnalysisRecord).where(DailyAnalysisRecord.trade_date == trade_date))
        record = result.scalar_one_or_none()
        if record is None:
            return {}
        return daily_analysis_service.serialize_record(record).get("columns", {})

    def serialize_daily_digest(
        self,
        digest: DailyInfoDigest,
        *,
        cache_hit: bool = False,
        sources: Optional[Iterable[Any]] = None,
    ) -> Dict[str, Any]:
        source_items = list(sources or [])
        summary = _merge_stock_mentions(dict(digest.summary_json or {}), source_items)
        return {
            "trade_date": digest.trade_date.isoformat(),
            "status": digest.status,
            "source_count": digest.source_count,
            "summary": summary,
            "model": digest.model,
            "generated_at": digest.generated_at.isoformat() if digest.generated_at else None,
            "cache_hit": cache_hit,
            "sources": [self.serialize_document_source(source) for source in source_items],
        }

    def serialize_document_source(self, document: Any, *, include_content: bool = False) -> Dict[str, Any]:
        payload = {
            "id": document.id,
            "title": document.title,
            "source_name": document.source_name,
            "source_key": document.source_key,
            "media_type_name": document.media_type_name,
            "trade_date": document.trade_date.isoformat() if document.trade_date else None,
            "update_time": document.update_time,
            "jump_url": document.jump_url,
            "source_path": document.source_path,
        }
        if include_content:
            payload.update({
                "abstract": document.abstract,
                "introduction": document.introduction,
                "content_text": document.content_text,
                "summary": document.summary_json or {},
            })
        return payload

    def serialize_rule(self, rule: JiegeTradingRule) -> Dict[str, Any]:
        return {
            "rule_key": rule.rule_key,
            "title": rule.title,
            "category": rule.category,
            "summary": rule.summary,
            "payload": rule.payload_json or {},
        }

    def serialize_jiege_signal(self, signal: JiegeModeSignal, *, cache_hit: bool = False) -> Dict[str, Any]:
        return {
            "trade_date": signal.trade_date.isoformat(),
            "status": signal.status,
            "data": signal.signal_json or {},
            "generated_at": signal.generated_at.isoformat() if signal.generated_at else None,
            "cache_hit": cache_hit,
        }


intelligence_service = IntelligenceService()
