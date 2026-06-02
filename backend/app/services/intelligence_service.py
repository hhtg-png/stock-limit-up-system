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
    DailyInfoDigestVersion,
    JiegeModeSignal,
    JiegeTradingRule,
    KnowledgeDocument,
)
from app.models.market_review import DailyAnalysisRecord, MarketReviewDailyMetric, MarketReviewStockDaily
from app.services.daily_analysis_service import daily_analysis_service
from app.utils.logger import logger
from app.utils.time_utils import CN_TZ, today_cn


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


def _normalize_stock_mentions(
    items: Iterable[Any],
    *,
    default_source_title: str = "",
    trusted: bool = False,
    limit: int = 30,
) -> List[Dict[str, Any]]:
    mentions: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for item in items or []:
        if isinstance(item, dict):
            raw_name = item.get("name") or item.get("stock_name") or ""
            code = str(item.get("code") or item.get("stock_code") or "").strip()
            source_title = str(item.get("source_title") or default_source_title)
            reason = str(item.get("reason") or item.get("catalyst") or "")
            summary = str(item.get("summary") or "")
            sector = str(item.get("sector") or item.get("theme") or "")
        else:
            raw_name = str(item or "")
            code = ""
            source_title = default_source_title
            reason = ""
            summary = ""
            sector = ""

        name = _normalize_stock_name(str(raw_name))
        if not _is_valid_stock_name(name):
            continue
        if not trusted and not code and not _is_known_stock_entity(name):
            continue
        key = code or name.lower()
        if key in seen:
            continue
        seen.add(key)
        payload = {
            "name": name,
            "code": code,
            "sector": sector,
            "summary": summary[:220],
            "reason": reason[:180],
            "source_title": source_title,
        }
        for optional_key in ("sentiment", "risk", "watch_points"):
            if isinstance(item, dict) and item.get(optional_key):
                payload[optional_key] = item[optional_key]
        mentions.append(payload)
        if len(mentions) >= limit:
            break
    return mentions


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
        text = _stock_document_text(document)
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
    document_items = list(documents)
    trusted_daily = merged.get("stock_analysis_status") == "ready"
    daily_mentions = _normalize_stock_mentions(
        merged.get("mentioned_stocks") or merged.get("stocks") or [],
        trusted=trusted_daily,
    )
    document_mentions: List[Dict[str, Any]] = []
    for document in document_items:
        doc_summary = getattr(document, "summary_json", None) or {}
        if doc_summary.get("stock_analysis_status") == "ready":
            document_mentions.extend(
                _normalize_stock_mentions(
                    doc_summary.get("stocks") or doc_summary.get("mentioned_stocks") or [],
                    default_source_title=str(getattr(document, "title", "") or ""),
                    trusted=True,
                )
            )

    if trusted_daily and daily_mentions:
        combined = _dedupe_stock_mentions([*daily_mentions, *document_mentions])
    elif document_mentions:
        combined = _dedupe_stock_mentions(document_mentions)
    else:
        pseudo_document = SimpleNamespace(
            title="",
            abstract="",
            introduction="",
            content_text="",
            summary_json={"stocks": daily_mentions},
        )
        combined = _extract_stock_mentions_from_documents([pseudo_document, *document_items])
    if combined:
        merged["mentioned_stocks"] = combined
    return merged


def _dedupe_stock_mentions(items: Iterable[Dict[str, Any]], *, limit: int = 30) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for item in items:
        key = str(item.get("code") or item.get("name") or "").lower()
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(item)
        if len(result) >= limit:
            break
    return result


def _summary_text_blocks(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        raw_blocks = [str(item or "") for item in value]
    else:
        raw_blocks = re.split(r"\n{2,}", str(value or ""))
    return [re.sub(r"\s+", " ", block).strip() for block in raw_blocks if str(block or "").strip()]


def _text_dedupe_key(value: str) -> str:
    return re.sub(r"[\s，。！？!?；;：:、,.()（）【】\[\]\"'“”‘’]+", "", value or "").lower()


def _similar_summary_text(left: str, right: str) -> bool:
    left_key = _text_dedupe_key(left)
    right_key = _text_dedupe_key(right)
    if not left_key or not right_key:
        return False
    if left_key in right_key or right_key in left_key:
        return True
    overlap = len(set(left_key) & set(right_key))
    return overlap / max(len(set(left_key)), len(set(right_key)), 1) >= 0.86


def _merge_summary_text(existing: Any, incoming: Any, *, limit: int = 6) -> str:
    result: List[str] = []
    for block in [*_summary_text_blocks(incoming), *_summary_text_blocks(existing)]:
        if any(_similar_summary_text(block, current) for current in result):
            continue
        result.append(block)
        if len(result) >= limit:
            break
    return "\n\n".join(result)


def _dedupe_summary_list(items: Iterable[Any], *, limit: int = 20) -> List[str]:
    result: List[str] = []
    for item in items:
        text = re.sub(r"\s+", " ", str(item or "")).strip()
        if not text:
            continue
        if any(_similar_summary_text(text, current) for current in result):
            continue
        result.append(text)
        if len(result) >= limit:
            break
    return result


def _summary_items(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return list(value)
    return [value]


def _stock_summary_items(value: Any) -> List[Dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if isinstance(value, dict):
        return [value]
    return []


def _merge_daily_summary(existing: Optional[Dict[str, Any]], incoming: Dict[str, Any]) -> Dict[str, Any]:
    if not existing:
        return dict(incoming or {})

    existing_payload = dict(existing or {})
    incoming_payload = dict(incoming or {})
    merged = {**existing_payload, **incoming_payload}

    for key in ("overview", "plan"):
        merged_text = _merge_summary_text(existing_payload.get(key), incoming_payload.get(key))
        if merged_text:
            merged[key] = merged_text

    for key in ("main_lines", "catalysts", "risks", "source_titles"):
        merged_items = _dedupe_summary_list([
            *_summary_items(incoming_payload.get(key)),
            *_summary_items(existing_payload.get(key)),
        ])
        if merged_items:
            merged[key] = merged_items

    for key in ("mentioned_stocks", "stocks"):
        combined = _dedupe_stock_mentions([
            *_stock_summary_items(incoming_payload.get(key)),
            *_stock_summary_items(existing_payload.get(key)),
        ])
        if combined:
            merged[key] = combined

    if (
        incoming_payload.get("stock_analysis_status") == "ready"
        or existing_payload.get("stock_analysis_status") == "ready"
    ):
        merged["stock_analysis_status"] = "ready"
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
            result = await self._request_json(
                "你是A股短线复盘助手。请从资料中提炼结构化摘要，只输出JSON。",
                (
                    "按以下键输出JSON：summary(100字内), themes(数组), catalysts(数组), risks(数组), sectors(数组), "
                    "stocks(数组，每项包含name,code,sector,summary,reason,source_title；"
                    "name为个股/港美股/明确公司名，summary用一句话说明文章中该个股的交易相关逻辑，"
                    "reason写催化或验证依据；只根据原文总结，不补充外部信息，不给买卖建议), "
                    "stock_analysis_status(固定为ready), trade_date_hint(字符串或空)。资料："
                    f"{json.dumps(prompt, ensure_ascii=False)}"
                ),
                fallback,
            )
            return self._finalize_document_summary(result, document)
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
            result = await self._request_json(
                "你是A股每日资讯编辑。请聚合资料，输出严格JSON。",
                (
                    "按以下键输出JSON：overview, main_lines(数组), catalysts(数组), "
                    "risks(数组), plan, source_titles(数组), "
                    "mentioned_stocks(数组，每项包含name,code,sector,summary,reason,source_title；"
                    "从各文档stocks聚合，summary保留个股逻辑，不要只截取原文), "
                    "stock_analysis_status(固定为ready)。"
                    "不要给买卖建议，只做资讯总结。"
                    f"日期：{trade_date.isoformat()}。资料：{json.dumps(compact_docs, ensure_ascii=False)}"
                ),
                fallback,
            )
            return self._finalize_daily_summary(result)
        except Exception as exc:
            logger.warning(f"DeepSeek daily summary failed: {exc}")
            fallback["model_status"] = "error"
            fallback["error"] = str(exc)
            return fallback

    def _finalize_document_summary(self, payload: Dict[str, Any], document: KnowledgeDocument) -> Dict[str, Any]:
        summary = dict(payload or {})
        summary["stocks"] = _normalize_stock_mentions(
            summary.get("stocks") or summary.get("mentioned_stocks") or [],
            default_source_title=document.title,
            trusted=summary.get("model_status") == "ready",
        )
        if summary.get("model_status") == "ready":
            summary["stock_analysis_status"] = "ready"
        elif "stock_analysis_status" not in summary:
            summary["stock_analysis_status"] = summary.get("model_status") or "fallback"
        return summary

    def _finalize_daily_summary(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        summary = dict(payload or {})
        if summary.get("model_status") == "ready":
            summary.setdefault("stock_analysis_status", "ready")
        summary["mentioned_stocks"] = _normalize_stock_mentions(
            summary.get("mentioned_stocks") or summary.get("stocks") or [],
            trusted=summary.get("stock_analysis_status") == "ready",
        )
        return summary

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
            "stock_analysis_status": status,
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
            "stock_analysis_status": status,
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
        self._background_sync_task: Optional[asyncio.Task] = None
        self._sync_status: Dict[str, Any] = {
            "state": "idle",
            "reason": "",
            "queued": False,
            "started_at": None,
            "finished_at": None,
            "error": "",
            "result": None,
        }

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
            should_rebuild_daily = force_daily or target_date in changed_daily_dates
            daily = await self.build_daily_info(
                db,
                target_date,
                allow_latest_fallback=target_date == today,
                force=should_rebuild_daily,
                refresh_stale_documents=force_daily or should_rebuild_daily,
            )
        jiege = await self.build_jiege_mode(db, today, allow_latest_fallback=True)
        obsidian = None
        try:
            from app.services.obsidian_knowledge_service import obsidian_knowledge_service

            obsidian = await obsidian_knowledge_service.export_daily_knowledge(db, today)
        except Exception as exc:
            logger.warning(f"Obsidian knowledge export skipped: {exc}")
            obsidian = {"skipped": True, "error": str(exc)}
        return {"sources": results, "daily_info": daily, "jiege_mode": jiege, "obsidian": obsidian}

    def get_sync_status(self) -> Dict[str, Any]:
        status = dict(self._sync_status)
        task = self._background_sync_task
        if task is not None and not task.done():
            status["state"] = "running"
            status["queued"] = True
        return status

    def queue_background_sync(self, *, force_daily: bool = False, reason: str = "manual") -> Dict[str, Any]:
        task = self._background_sync_task
        if task is not None and not task.done():
            status = self.get_sync_status()
            status["queued"] = False
            return status

        now = datetime.now(CN_TZ).isoformat()
        self._sync_status = {
            "state": "queued",
            "reason": reason,
            "queued": True,
            "started_at": now,
            "finished_at": None,
            "error": "",
            "result": None,
        }
        self._background_sync_task = asyncio.create_task(
            self._run_background_sync(force_daily=force_daily, reason=reason)
        )
        return self.get_sync_status()

    async def _run_background_sync(self, *, force_daily: bool, reason: str) -> None:
        started_at = datetime.now(CN_TZ).isoformat()
        self._sync_status.update(
            {
                "state": "running",
                "reason": reason,
                "queued": True,
                "started_at": started_at,
                "finished_at": None,
                "error": "",
                "result": None,
            }
        )
        try:
            from app.database import async_session_maker

            async with async_session_maker() as db:
                result = await self.sync_all(db, force_daily=force_daily)
            self._sync_status.update(
                {
                    "state": "completed",
                    "queued": False,
                    "finished_at": datetime.now(CN_TZ).isoformat(),
                    "error": "",
                    "result": result,
                }
            )
        except Exception as exc:
            logger.error(f"Background knowledge intelligence sync error: {exc}")
            self._sync_status.update(
                {
                    "state": "failed",
                    "queued": False,
                    "finished_at": datetime.now(CN_TZ).isoformat(),
                    "error": str(exc),
                    "result": None,
                }
            )

    async def probe_daily_source(self, db: AsyncSession) -> Dict[str, Any]:
        source = self.sources.get("daily")
        if source is None:
            return {
                "changed": False,
                "reason": "daily_source_disabled",
                "checked_documents": 0,
                "checked_at": datetime.now(CN_TZ).isoformat(),
            }

        entries = await self._collect_source_probe_items(source)
        checked = 0
        first_change: Optional[Dict[str, Any]] = None
        for entry in entries:
            checked += 1
            media_id = self._media_id_from_entry(entry)
            result = await db.execute(
                select(KnowledgeDocument).where(
                    KnowledgeDocument.source_key == source.key,
                    KnowledgeDocument.media_id == media_id,
                )
            )
            doc = result.scalar_one_or_none()
            if doc is None:
                change = {
                    "changed": True,
                    "reason": "new_document",
                    "media_id": media_id,
                    "title": str(entry.get("title") or ""),
                    "_is_folder": bool((entry.get("folder_info") or {}).get("folder_id")),
                }
                first_change = self._prefer_probe_change(first_change, change, entry)
                continue
            changed_field = self._changed_metadata_field(doc, entry)
            if changed_field:
                change = {
                    "changed": True,
                    "reason": "metadata_changed",
                    "field": changed_field,
                    "media_id": media_id,
                    "title": str(entry.get("title") or ""),
                    "_is_folder": bool((entry.get("folder_info") or {}).get("folder_id")),
                }
                first_change = self._prefer_probe_change(first_change, change, entry)
        if first_change is not None:
            first_change["checked_documents"] = checked
            first_change["checked_at"] = datetime.now(CN_TZ).isoformat()
            first_change.pop("_is_folder", None)
            return first_change
        return {
            "changed": False,
            "reason": "unchanged",
            "checked_documents": checked,
            "checked_at": datetime.now(CN_TZ).isoformat(),
        }

    async def sync_source(self, db: AsyncSession, source_key: str) -> Dict[str, Any]:
        source = self.sources[source_key]
        items = await self._collect_source_items(source)
        changed = 0
        summarized = 0
        changed_trade_dates: set[date] = set()
        for entry in items:
            is_changed, is_summarized, trade_dates = await self._sync_source_entry(db, source, entry)
            if is_changed:
                changed += 1
                summarized += 1 if is_summarized else 0
                if source.key == "daily":
                    changed_trade_dates.update(trade_dates)
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
    ) -> tuple[bool, bool, set[date]]:
        for attempt in range(5):
            try:
                doc, needs_summary, changed_trade_dates = await self._upsert_document_from_entry(db, source, entry)
                await db.flush()
                snapshot = self._snapshot_document(doc)
                await db.commit()
                if not needs_summary and not changed_trade_dates:
                    return False, False, set()
                if needs_summary:
                    await self._summarize_document(snapshot)
                    await self._write_document_summary_with_retry(
                        db,
                        snapshot.id,
                        snapshot.summary_json,
                        snapshot.summary_status,
                        snapshot.summary_error,
                    )
                return True, needs_summary and snapshot.summary_status == "ready", changed_trade_dates
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
        refresh_stale_documents: bool = False,
    ) -> Dict[str, Any]:
        documents = await self._get_daily_documents(db, trade_date)
        actual_date = trade_date
        if not documents and allow_latest_fallback:
            actual_date = await self._latest_daily_trade_date(db) or trade_date
            documents = await self._get_daily_documents(db, actual_date)
        document_snapshots = [self._snapshot_document(doc) for doc in documents]
        await db.commit()
        if refresh_stale_documents:
            await self._refresh_stale_document_summaries(db, document_snapshots)
        content_hash = _json_hash(
            [{"id": doc.id, "hash": doc.content_hash, "summary": doc.summary_json} for doc in document_snapshots]
        )
        with db.no_autoflush:
            existing = await self._get_daily_digest(db, actual_date)
        existing_summary = dict(existing.summary_json or {}) if existing is not None else None
        if existing and existing.content_hash == content_hash and not force:
            await db.commit()
            return self.serialize_daily_digest(existing, cache_hit=True, sources=document_snapshots)

        await db.commit()
        summary = await self.summary_client.summarize_daily_info(actual_date, document_snapshots)
        summary = _merge_stock_mentions(summary, document_snapshots)
        if existing_summary is not None:
            summary = _merge_daily_summary(existing_summary, summary)
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
        return self._has_api_key() and (
            self._summary_from_missing_api_key(digest.summary_json)
            or self._summary_missing_stock_analysis(digest.summary_json)
        )

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
            if self._document_summary_needs_model_refresh(doc):
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
    ) -> Any:
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
                await db.flush()
                version = DailyInfoDigestVersion(
                    digest_id=existing.id,
                    trade_date=trade_date,
                    summary_json=dict(summary or {}),
                    status=status,
                    source_count=source_count,
                    content_hash=content_hash,
                    model=existing.model,
                    generated_at=now,
                    created_at=now,
                )
                db.add(version)
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

    def _summary_missing_stock_analysis(self, summary: Optional[Dict[str, Any]]) -> bool:
        payload = summary or {}
        return payload.get("model_status") == "ready" and payload.get("stock_analysis_status") != "ready"

    def _document_summary_needs_model_refresh(self, doc: Any) -> bool:
        summary = getattr(doc, "summary_json", None) or {}
        return self._summary_from_missing_api_key(summary) or (
            getattr(doc, "source_key", "") == "daily" and self._summary_missing_stock_analysis(summary)
        )

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

    async def ensure_jiege_yesterday_prediction(
        self,
        db: AsyncSession,
        signal: JiegeModeSignal,
    ) -> Dict[str, Any]:
        payload = dict(signal.signal_json or {})
        if "yesterday_prediction" in payload:
            return self.serialize_jiege_signal(signal, cache_hit=True)

        payload["yesterday_prediction"] = await self._build_yesterday_prediction(db, signal.trade_date)
        signal.signal_json = payload
        signal.content_hash = _json_hash(payload)
        signal.updated_at = datetime.now()
        await db.commit()
        await db.refresh(signal)
        return self.serialize_jiege_signal(signal, cache_hit=False)

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

    async def _collect_source_probe_items(self, source: ImaKnowledgeSource) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        root = await self.ima_client.get_share_page(
            source.share_id,
            cursor="",
            folder_id="",
            limit=settings.INTELLIGENCE_PROBE_LIMIT,
        )
        if root.get("code") != 0:
            raise RuntimeError(root.get("msg") or f"ima share error: {root.get('code')}")
        root_version = str(root.get("version") or "")
        folders: List[str] = []
        for item in root.get("knowledge_list") or []:
            normalized = dict(item)
            normalized["_source_version"] = root_version
            normalized["_folder_id"] = ""
            normalized["_folder_path"] = ""
            items.append(normalized)
            folder_info = item.get("folder_info") or {}
            folder_id = folder_info.get("folder_id")
            if folder_id:
                folders.append(str(folder_id))

        for folder_id in folders:
            page = await self.ima_client.get_share_page(
                source.share_id,
                cursor="",
                folder_id=folder_id,
                limit=settings.INTELLIGENCE_PROBE_LIMIT,
            )
            if page.get("code") != 0:
                raise RuntimeError(page.get("msg") or f"ima share error: {page.get('code')}")
            current_path = page.get("current_path") or []
            folder_path = " / ".join(path.get("name") or "" for path in current_path if path.get("name"))
            source_version = str(page.get("version") or "")
            for item in page.get("knowledge_list") or []:
                normalized = dict(item)
                normalized["_source_version"] = source_version
                normalized["_folder_id"] = folder_id
                normalized["_folder_path"] = folder_path
                items.append(normalized)
        return items

    def _media_id_from_entry(self, entry: Dict[str, Any]) -> str:
        folder_info = entry.get("folder_info") or {}
        return str(entry.get("media_id") or folder_info.get("folder_id") or entry.get("title") or "")

    def _changed_metadata_field(self, doc: KnowledgeDocument, entry: Dict[str, Any]) -> str:
        checks = (
            ("source_version", "_source_version"),
            ("md5_sum", "md5_sum"),
            ("update_time", "update_time"),
            ("source_path", "source_path"),
            ("title", "title"),
            ("media_type", "media_type"),
        )
        for attr, key in checks:
            current = getattr(doc, attr)
            incoming: Any = entry.get(key)
            if attr == "media_type":
                incoming = int(incoming or 0)
            else:
                incoming = str(incoming or "")
            if current != incoming:
                return attr
        return ""

    def _prefer_probe_change(
        self,
        current: Optional[Dict[str, Any]],
        candidate: Dict[str, Any],
        entry: Dict[str, Any],
    ) -> Dict[str, Any]:
        if current is None:
            return candidate
        if not candidate.get("_is_folder") and current.get("_is_folder"):
            return candidate
        return current

    async def _upsert_document_from_entry(
        self,
        db: AsyncSession,
        source: ImaKnowledgeSource,
        entry: Dict[str, Any],
    ) -> tuple[KnowledgeDocument, bool, set[date]]:
        media_id = self._media_id_from_entry(entry)
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
        previous_trade_date = doc.trade_date

        metadata_changed = is_new or any(
            getattr(doc, attr) != str(entry.get(key) or "")
            for attr, key in (
                ("md5_sum", "md5_sum"),
                ("update_time", "update_time"),
                ("source_path", "source_path"),
                ("abstract", "abstract"),
                ("introduction", "introduction"),
            )
        )
        self._apply_entry_metadata(doc, source, entry, media_id)
        inferred_trade_date = self._infer_trade_date(doc)
        trade_date_changed = previous_trade_date != inferred_trade_date
        changed_trade_dates: set[date] = set()
        if metadata_changed:
            doc.content_text = await self._resolve_content_text(entry)
            doc.content_hash = self._build_document_hash(doc)
            doc.trade_date = inferred_trade_date
            doc.summary_status = "pending"
            doc.summary_error = ""
            doc.updated_at = datetime.now()
        elif trade_date_changed:
            doc.trade_date = inferred_trade_date
            doc.updated_at = datetime.now()

        if metadata_changed or trade_date_changed:
            if previous_trade_date:
                changed_trade_dates.add(previous_trade_date)
            if inferred_trade_date:
                changed_trade_dates.add(inferred_trade_date)
        return doc, metadata_changed, changed_trade_dates

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
        if getattr(doc, "source_key", "") == "daily":
            updated_date = self._date_from_timestamp(doc.update_time)
            if updated_date:
                return updated_date

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
            return datetime.fromtimestamp(timestamp, CN_TZ).date()
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
            .order_by(
                DailyInfoDigest.trade_date.desc(),
                DailyInfoDigest.generated_at.desc(),
                DailyInfoDigest.id.desc(),
            )
            .limit(limit)
        )
        return await self._serialize_daily_digest_records(db, list(result.scalars().all()))

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
        digest_conditions = [cast(DailyInfoDigest.summary_json, String).like(pattern)]
        if matched_dates:
            digest_conditions.append(DailyInfoDigest.trade_date.in_(matched_dates))

        result = await db.execute(
            select(DailyInfoDigest)
            .where(or_(*digest_conditions))
            .order_by(
                DailyInfoDigest.trade_date.desc(),
                DailyInfoDigest.generated_at.desc(),
                DailyInfoDigest.id.desc(),
            )
            .limit(limit)
        )
        return await self._serialize_daily_digest_records(db, list(result.scalars().all()))

    async def get_daily_digest_version(
        self,
        db: AsyncSession,
        version_id: int,
    ) -> Optional[DailyInfoDigestVersion]:
        result = await db.execute(
            select(DailyInfoDigestVersion).where(DailyInfoDigestVersion.id == version_id)
        )
        return result.scalar_one_or_none()

    async def get_latest_daily_digest_version(
        self,
        db: AsyncSession,
        trade_date: date,
    ) -> Optional[DailyInfoDigestVersion]:
        result = await db.execute(
            select(DailyInfoDigestVersion)
            .where(DailyInfoDigestVersion.trade_date == trade_date)
            .order_by(DailyInfoDigestVersion.generated_at.desc(), DailyInfoDigestVersion.id.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def serialize_daily_digest_with_sources(
        self,
        db: AsyncSession,
        digest: Any,
        *,
        cache_hit: bool = False,
    ) -> Dict[str, Any]:
        sources = await self._get_daily_documents(db, digest.trade_date)
        return self.serialize_daily_digest(digest, cache_hit=cache_hit, sources=sources)

    async def _serialize_daily_digest_records(
        self,
        db: AsyncSession,
        records: List[Any],
    ) -> List[Dict[str, Any]]:
        records.sort(key=self._daily_digest_order_key, reverse=True)
        items = []
        for digest in records:
            sources = await self._get_daily_documents(db, digest.trade_date)
            items.append(self.serialize_daily_digest(digest, cache_hit=True, sources=sources))
        return items

    def _daily_digest_order_key(self, digest: Any) -> tuple:
        return (
            digest.trade_date,
            digest.generated_at or datetime.min,
            digest.id or 0,
        )

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

    async def _previous_market_review_date(self, db: AsyncSession, trade_date: date) -> Optional[date]:
        result = await db.execute(
            select(MarketReviewDailyMetric.trade_date)
            .where(MarketReviewDailyMetric.trade_date < trade_date)
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
        yesterday_prediction = await self._build_yesterday_prediction(db, trade_date)
        return {
            "trade_date": trade_date.isoformat(),
            "market_phase": market_phase,
            "rules": rules,
            "prediction": {
                "candidates": candidates,
                "daily_analysis": daily_analysis,
                "risk_flags": risk_flags,
            },
            "yesterday_prediction": yesterday_prediction,
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

    async def _build_yesterday_prediction(self, db: AsyncSession, target_date: date) -> Dict[str, Any]:
        source_date = await self._previous_market_review_date(db, target_date)
        if source_date is None:
            return {
                "source_date": None,
                "target_date": target_date.isoformat(),
                "candidates": [],
                "risk_flags": [],
                "market_phase": {"label": "暂无昨日复盘数据", "score": 0, "basis": []},
                "notes": "未找到目标日前一复盘日，无法生成昨日预判。",
            }

        metric = await self._get_metric(db, source_date)
        stocks = await self._get_review_stocks(db, source_date)
        return {
            "source_date": source_date.isoformat(),
            "target_date": target_date.isoformat(),
            "candidates": self._build_rule_candidates(stocks),
            "risk_flags": self._build_risk_flags(metric, stocks),
            "market_phase": self._classify_market_phase(metric),
            "notes": f"基于 {source_date.isoformat()} 盘后复盘数据，生成 {target_date.isoformat()} 的盘前观察候选。",
        }

    async def _load_daily_analysis_cells(self, db: AsyncSession, trade_date: date) -> Dict[str, Any]:
        result = await db.execute(select(DailyAnalysisRecord).where(DailyAnalysisRecord.trade_date == trade_date))
        record = result.scalar_one_or_none()
        if record is None:
            return {}
        return daily_analysis_service.serialize_record(record).get("columns", {})

    def serialize_daily_digest(
        self,
        digest: Any,
        *,
        cache_hit: bool = False,
        sources: Optional[Iterable[Any]] = None,
    ) -> Dict[str, Any]:
        source_items = list(sources or [])
        summary = _merge_stock_mentions(dict(digest.summary_json or {}), source_items)
        is_version = isinstance(digest, DailyInfoDigestVersion)
        record_id = getattr(digest, "id", None)
        return {
            "id": record_id,
            "version_id": record_id if is_version else None,
            "digest_id": getattr(digest, "digest_id", record_id),
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
