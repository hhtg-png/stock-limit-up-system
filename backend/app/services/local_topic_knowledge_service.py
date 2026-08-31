"""Local stock topic knowledge used for deterministic theme mining."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

from loguru import logger


class LocalTopicKnowledgeService:
    """Load Codex-maintained local stock-to-topic candidates.

    This service is intentionally file-backed and deterministic. It must not
    call external APIs or model providers; DeepSeek token usage belongs only to
    the separate cached classification enhancement path.
    """

    DEFAULT_PATH = Path(__file__).resolve().parents[1] / "data" / "local_stock_topic_knowledge.json"
    TOPIC_LOOKUP_CACHE_MAX = 256

    def __init__(
        self,
        *,
        knowledge_path: Optional[Path] = None,
        records: Optional[Mapping[str, Any]] = None,
    ) -> None:
        self.knowledge_path = Path(knowledge_path) if knowledge_path else self.DEFAULT_PATH
        self._records_override = records
        self._records: Optional[Dict[str, Any]] = None
        self._topic_lookup_cache: Dict[str, List[Dict[str, Any]]] = {}

    def get_topics(self, stock_code: str, stock_name: Optional[str] = None) -> List[Dict[str, Any]]:
        code = self._normalize_code(stock_code)
        if not code:
            return []

        raw_record = self._load_records().get(code) or {}
        if not isinstance(raw_record, Mapping):
            return []

        topics = []
        for raw_topic in raw_record.get("topics") or []:
            topic = self._normalize_topic(raw_topic)
            if topic:
                topics.append(topic)
        return topics

    def find_stocks_by_topic(self, topic_name: str) -> List[Dict[str, Any]]:
        """Return the local A-share constituents matching a topic or industry name."""
        query = self._normalize_topic_name(topic_name)
        if not query:
            return []
        cached = self._topic_lookup_cache.get(query)
        if cached is not None:
            return [dict(item) for item in cached]

        matches: List[Dict[str, Any]] = []
        for stock_code, record in self._load_records().items():
            candidates = self._record_topic_names(record)
            match_reason = next(
                (candidate for candidate in candidates if self._topic_name_matches(query, candidate)),
                "",
            )
            if not match_reason:
                continue
            matches.append({
                "stock_code": stock_code,
                "stock_name": str(record.get("stock_name") or stock_code),
                "market": str(record.get("market") or ""),
                "match_reason": match_reason,
            })

        matches.sort(key=lambda item: item["stock_code"])
        self._topic_lookup_cache[query] = [dict(item) for item in matches]
        while len(self._topic_lookup_cache) > self.TOPIC_LOOKUP_CACHE_MAX:
            oldest_query = next(iter(self._topic_lookup_cache))
            self._topic_lookup_cache.pop(oldest_query, None)
        return matches

    @classmethod
    def _record_topic_names(cls, record: Mapping[str, Any]) -> List[str]:
        values: List[Any] = [record.get("industry")]
        concepts = record.get("concept") or []
        values.extend(concepts if isinstance(concepts, list) else [concepts])
        for raw_topic in record.get("topics") or []:
            topic = cls._normalize_topic(raw_topic)
            if not topic:
                continue
            values.append(topic.get("theme"))
            values.extend(topic.get("aliases") or [])

        names: List[str] = []
        for value in values:
            name = cls._normalize_topic_name(value)
            if name and name not in names:
                names.append(name)
        return names

    @staticmethod
    def _normalize_topic_name(value: Any) -> str:
        return str(value or "").strip().replace(" ", "")[:40]

    @staticmethod
    def _topic_name_matches(query: str, candidate: str) -> bool:
        if query == candidate:
            return True
        return min(len(query), len(candidate)) >= 3 and (query in candidate or candidate in query)

    def _load_records(self) -> Dict[str, Any]:
        if self._records is not None:
            return self._records
        if self._records_override is not None:
            self._records = self._normalize_records(self._records_override)
            return self._records
        try:
            with self.knowledge_path.open("r", encoding="utf-8") as file_obj:
                payload = json.load(file_obj)
        except FileNotFoundError:
            self._records = {}
        except Exception as exc:
            logger.warning(f"Local topic knowledge load failed: {exc}")
            self._records = {}
        else:
            self._records = self._normalize_records(payload)
        return self._records

    @classmethod
    def _normalize_records(cls, payload: Mapping[str, Any]) -> Dict[str, Any]:
        if not isinstance(payload, Mapping):
            return {}
        raw_records = payload.get("stocks") if "stocks" in payload else payload
        if not isinstance(raw_records, Mapping):
            return {}
        records: Dict[str, Any] = {}
        for raw_code, raw_record in raw_records.items():
            code = cls._normalize_code(raw_code)
            if code and isinstance(raw_record, Mapping):
                records[code] = raw_record
        return records

    @staticmethod
    def _normalize_code(value: Any) -> str:
        return str(value or "").strip()[:6]

    @classmethod
    def _normalize_topic(cls, raw_topic: Any) -> Dict[str, Any]:
        if isinstance(raw_topic, str):
            theme = raw_topic.strip()
            return {"theme": theme, "aliases": [], "confidence": 0.6, "evidence": ""} if theme else {}
        if not isinstance(raw_topic, Mapping):
            return {}
        theme = str(raw_topic.get("theme") or "").strip()
        if not theme:
            return {}
        aliases = cls._normalize_aliases(raw_topic.get("aliases"))
        confidence = cls._normalize_confidence(raw_topic.get("confidence"))
        return {
            "theme": theme[:40],
            "aliases": aliases,
            "source": str(raw_topic.get("source") or "local_codex")[:40],
            "confidence": confidence,
            "evidence": str(raw_topic.get("evidence") or "")[:160],
        }

    @staticmethod
    def _normalize_aliases(value: Any) -> List[str]:
        if isinstance(value, str):
            raw_items: Iterable[Any] = value.replace("，", ",").replace("、", ",").split(",")
        elif isinstance(value, list):
            raw_items = value
        else:
            raw_items = []
        aliases = []
        for raw in raw_items:
            alias = str(raw or "").strip()
            if alias and alias not in aliases:
                aliases.append(alias[:40])
        return aliases[:8]

    @staticmethod
    def _normalize_confidence(value: Any) -> float:
        try:
            number = float(value)
        except (TypeError, ValueError):
            return 0.6
        return max(0.0, min(1.0, number))
