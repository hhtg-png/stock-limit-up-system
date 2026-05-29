"""Public news source adapters for Tongdaxin embedded news plugin."""
from __future__ import annotations

import hashlib
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterable, List, Tuple
from urllib.parse import urlencode

import httpx


@dataclass
class MarketNewsItem:
    news_id: str
    source: str
    title: str
    content: str
    published_at: Any
    importance: int = 50
    related_stocks: List[str] = field(default_factory=list)
    related_plates: List[str] = field(default_factory=list)
    jump_url: str = ""


class PublicMarketNewsProvider:
    """Fetch the public feeds that match the target Tongdaxin news plugin sources."""

    THS_URL = "https://news.10jqka.com.cn/tapp/news/push/stock"
    STCN_URL = "https://www.stcn.com/article/list.html"
    CLS_URL = "https://api3.cls.cn/v1/roll/get_roll_list"
    JYGS_URL = "https://app.jiuyangongshe.com/jystock-app/api/v2/article/community"
    GLH_URL = "https://www.gelonghui.com/api/live-channels/all/lives"

    def __init__(self, *, timeout: float = 8.0, cache_ttl: int = 5):
        self.timeout = timeout
        self.cache_ttl = cache_ttl
        self._cache: Tuple[float, List[Dict[str, Any]], Dict[str, str], List[str]] | None = None

    async def get_latest_news(
        self,
        limit: int = 80,
        *,
        force_refresh: bool = False,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, str], List[str]]:
        cached = self.get_cached_news(limit)
        if cached and not force_refresh:
            return cached

        fetch_limit = max(limit, 80)
        source_status: Dict[str, str] = {}
        warnings: List[str] = []
        source_items: Dict[str, List[MarketNewsItem]] = {"stcn": [], "ths": [], "glh": [], "jygs": [], "cls": []}
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
        }

        async with httpx.AsyncClient(timeout=self.timeout, headers=headers, follow_redirects=True) as client:
            fetchers = {
                "stcn": self._fetch_stcn(client),
                "ths": self._fetch_ths(client),
                "glh": self._fetch_gelonghui(client),
                "jygs": self._fetch_jygs(client),
                "cls": self._fetch_cls(client),
            }
            for source, fetcher in fetchers.items():
                try:
                    source_items[source] = await fetcher
                    source_status[source] = "ok" if source_items[source] else "empty"
                except Exception as exc:
                    source_status[source] = "error"
                    warnings.append(f"{self._source_label(source)}快讯获取失败: {exc}")

        merged = self.merge_market_feeds(source_items, limit=fetch_limit)
        items = [self.to_plugin_item(item) for item in merged]
        self._cache = (time.time(), items, dict(source_status), list(warnings))
        return items[:limit], source_status, warnings

    def get_cached_news(self, limit: int) -> Tuple[List[Dict[str, Any]], Dict[str, str], List[str]] | None:
        cached = self._cache
        if cached and time.time() - cached[0] < self.cache_ttl:
            return cached[1][:limit], dict(cached[2]), list(cached[3])
        return None

    async def _fetch_ths(self, client: httpx.AsyncClient) -> List[MarketNewsItem]:
        response = await client.get(
            self.THS_URL,
            params={"page": 1, "tag": "", "track": "website"},
            headers={"Referer": "https://news.10jqka.com.cn/realtimenews.html"},
        )
        response.raise_for_status()
        return self.parse_ths_response(response.json())

    async def _fetch_stcn(self, client: httpx.AsyncClient) -> List[MarketNewsItem]:
        response = await client.get(
            self.STCN_URL,
            params={"type": "kx", "page": 1, "per-page": 30},
            headers={
                "Referer": "https://www.stcn.com/article/list/kx.html",
                "X-Requested-With": "XMLHttpRequest",
            },
        )
        response.raise_for_status()
        return self.parse_stcn_response(response.json())

    async def _fetch_cls(self, client: httpx.AsyncClient) -> List[MarketNewsItem]:
        params = {
            "app": "CailianpressWeb",
            "os": "web",
            "rn": "30",
            "sv": "8.4.6",
        }
        params["sign"] = self._cls_sign(params)
        response = await client.get(
            self.CLS_URL,
            params=params,
            headers={"Referer": "https://www.cls.cn/telegraph"},
        )
        response.raise_for_status()
        return self.parse_cls_response(response.json())

    async def _fetch_jygs(self, client: httpx.AsyncClient) -> List[MarketNewsItem]:
        timestamp = str(int(time.time() * 1000))
        response = await client.post(
            self.JYGS_URL,
            json={"type": 0, "category_id": "", "limit": 30, "start": 1, "order": 0, "back_garden": 0},
            headers={
                "Origin": "https://www.jiuyangongshe.com",
                "Referer": "https://www.jiuyangongshe.com/study_publish",
                "Content-Type": "application/json",
                "platform": "3",
                "timestamp": timestamp,
                "token": hashlib.md5(f"Uu0KfOB8iUP69d3c:{timestamp}".encode("utf-8")).hexdigest(),
                "X-Requested-With": "XMLHttpRequest",
            },
        )
        response.raise_for_status()
        return self.parse_jygs_response(response.json())

    async def _fetch_gelonghui(self, client: httpx.AsyncClient) -> List[MarketNewsItem]:
        response = await client.get(
            self.GLH_URL,
            headers={
                "Referer": "https://www.gelonghui.com/live/",
                "Accept": "application/json, text/plain, */*",
            },
        )
        response.raise_for_status()
        return self.parse_gelonghui_response(response.json())

    def parse_ths_response(self, payload: Dict[str, Any]) -> List[MarketNewsItem]:
        raw_items = (payload.get("data") or {}).get("list") or payload.get("list") or []
        items: List[MarketNewsItem] = []
        for raw in raw_items:
            title = self._clean_text(raw.get("title") or "")
            content = self._clean_text(raw.get("digest") or raw.get("short") or raw.get("content") or title)
            if not title and not content:
                continue
            related_stocks = self._extract_stock_codes(raw.get("stock") or [])
            related_plates = self._extract_names(raw.get("field") or raw.get("tagInfo") or [], key="name")
            published_at = raw.get("rtime") or raw.get("ctime") or raw.get("time") or 0
            source_id = raw.get("seq") or raw.get("id") or self._stable_id(title, content)
            items.append(MarketNewsItem(
                news_id=f"ths-{source_id}",
                source="同花顺",
                title=title or content[:36],
                content=content,
                published_at=published_at,
                importance=self._score_importance(title, content, raw.get("import")),
                related_stocks=related_stocks,
                related_plates=related_plates,
                jump_url=str(raw.get("url") or raw.get("shareUrl") or ""),
            ))
        return items

    def parse_cls_response(self, payload: Dict[str, Any]) -> List[MarketNewsItem]:
        raw_items = (payload.get("data") or {}).get("roll_data") or []
        items: List[MarketNewsItem] = []
        for raw in raw_items:
            title = self._clean_text(raw.get("title") or "")
            content = self._clean_text(raw.get("content") or raw.get("brief") or title)
            if not title and not content:
                continue
            source_id = raw.get("id") or raw.get("ctime") or self._stable_id(title, content)
            items.append(MarketNewsItem(
                news_id=f"cls-{source_id}",
                source="财联社",
                title=title or content[:36],
                content=content,
                published_at=raw.get("ctime") or raw.get("time") or 0,
                importance=self._score_importance(title, content, raw.get("level")),
                related_stocks=self._extract_stock_codes(raw.get("stocks") or raw.get("stock_list") or raw.get("codes") or []),
                related_plates=self._extract_names(raw.get("subjects") or raw.get("topics") or [], key="name"),
                jump_url=f"https://www.cls.cn/detail/{source_id}" if source_id else "",
            ))
        return items

    def parse_stcn_response(self, payload: Dict[str, Any]) -> List[MarketNewsItem]:
        raw_items = payload.get("data") or []
        items: List[MarketNewsItem] = []
        for raw in raw_items:
            if raw.get("isAdd"):
                continue
            title = self._clean_text(raw.get("title") or "")
            content = self._clean_text(raw.get("content") or raw.get("content_full") or title)
            if not title and not content:
                continue
            source_id = raw.get("id") or raw.get("pageTime") or self._stable_id(title, content)
            related_stocks, related_plates = self._extract_stcn_tags(raw.get("tags") or [])
            items.append(MarketNewsItem(
                news_id=f"stcn-{source_id}",
                source="时报快讯",
                title=title or content[:36],
                content=content,
                published_at=raw.get("time") or raw.get("show_time") or raw.get("pageTime") or 0,
                importance=max(72, self._score_importance(title, content, raw.get("isRed") or raw.get("red"))),
                related_stocks=related_stocks,
                related_plates=related_plates,
                jump_url=self._absolute_stcn_url(raw.get("share_url") or raw.get("web_url") or raw.get("url") or ""),
            ))
        return items

    def parse_jygs_response(self, payload: Dict[str, Any]) -> List[MarketNewsItem]:
        data = payload.get("data") or {}
        raw_items = data.get("result") or data.get("list") or []
        items: List[MarketNewsItem] = []
        for raw in raw_items:
            if str(raw.get("is_top") or raw.get("isTop") or raw.get("top") or "0") == "1":
                continue
            title = self._clean_text(raw.get("title") or "")
            content = self._clean_text(raw.get("content") or raw.get("subtitle") or title)
            if not title and not content:
                continue
            article_id = raw.get("article_id") or raw.get("id") or self._stable_id(title, content)
            items.append(MarketNewsItem(
                news_id=f"jygs-{article_id}",
                source="韭研公社",
                title=title or content[:36],
                content=content,
                published_at=raw.get("create_time") or raw.get("sync_time") or raw.get("new_interaction_time") or 0,
                importance=self._score_importance(title, content, raw.get("is_top")),
                related_stocks=self._extract_stock_codes(raw.get("stock_list") or []),
                related_plates=self._extract_names(raw.get("plate_list") or raw.get("field_list") or [], key="name"),
                jump_url=f"https://www.jiuyangongshe.com/a/{article_id}" if article_id else "",
            ))
        return items

    def parse_gelonghui_response(self, payload: Dict[str, Any]) -> List[MarketNewsItem]:
        raw_items = payload.get("result") or payload.get("data") or []
        items: List[MarketNewsItem] = []
        for raw in raw_items:
            title = self._clean_text(raw.get("title") or "")
            content = self._clean_text(raw.get("content") or raw.get("summary") or title)
            if not title:
                title = self._gelonghui_title_from_content(content)
            if not title and not content:
                continue
            source_id = raw.get("id") or raw.get("liveId") or self._stable_id(title, content)
            level = "B" if str(raw.get("level") or "0") not in {"", "0", "None"} else raw.get("level")
            items.append(MarketNewsItem(
                news_id=f"glh-{source_id}",
                source="格隆汇",
                title=title or content[:36],
                content=content,
                published_at=raw.get("createTime") or raw.get("createTimestamp") or raw.get("time") or 0,
                importance=self._score_importance(title, content, level),
                related_stocks=self._extract_stock_codes(raw.get("stockList") or raw.get("relatedStocks") or []),
                related_plates=self._extract_names(raw.get("subjects") or raw.get("relatedInfos") or [], key="name"),
                jump_url=str(raw.get("route") or raw.get("link") or f"https://www.gelonghui.com/live/{source_id}"),
            ))
        return items

    def merge_sources(self, *source_lists: Iterable[MarketNewsItem], limit: int = 80) -> List[MarketNewsItem]:
        deduped: Dict[str, MarketNewsItem] = {}
        for item in [item for source in source_lists for item in source]:
            key = self._dedupe_key(item.title, item.content)
            existing = deduped.get(key)
            if not existing or (
                self._timestamp(item.published_at),
                item.importance,
            ) > (
                self._timestamp(existing.published_at),
                existing.importance,
            ):
                deduped[key] = item
        return sorted(
            deduped.values(),
            key=lambda item: (self._timestamp(item.published_at), item.importance),
            reverse=True,
        )[:limit]

    def merge_market_feeds(self, source_items: Dict[str, List[MarketNewsItem]], limit: int = 80) -> List[MarketNewsItem]:
        primary_sources = [
            source_items.get("stcn", []),
            source_items.get("ths", []),
            source_items.get("glh", []),
            source_items.get("jygs", []),
        ]
        fallback_sources = [] if any(primary_sources) else [source_items.get("cls", [])[:10]]
        return self.merge_sources(*primary_sources, *fallback_sources, limit=limit)

    def to_plugin_item(self, item: MarketNewsItem) -> Dict[str, Any]:
        return {
            "news_id": item.news_id,
            "time": self._format_time(item.published_at),
            "source": item.source,
            "title": item.title,
            "content": item.content[:500],
            "importance": item.importance,
            "related_stocks": item.related_stocks,
            "related_plates": item.related_plates,
            "jump_url": item.jump_url,
        }

    @staticmethod
    def _cls_sign(params: Dict[str, Any]) -> str:
        query = urlencode(sorted((key, value) for key, value in params.items() if key != "sign"))
        return hashlib.md5(hashlib.sha1(query.encode("utf-8")).hexdigest().encode("utf-8")).hexdigest()

    @staticmethod
    def _source_label(source: str) -> str:
        return {
            "stcn": "时报快讯",
            "ths": "同花顺",
            "glh": "格隆汇",
            "cls": "财联社",
            "jygs": "韭研公社",
        }.get(source, source)

    @staticmethod
    def _clean_text(value: Any) -> str:
        text = re.sub(r"<[^>]+>", "", str(value or ""))
        text = text.replace("&nbsp;", " ").replace("\u3000", " ")
        return re.sub(r"\s+", " ", text).strip()

    @staticmethod
    def _extract_stock_codes(values: Any) -> List[str]:
        result: List[str] = []
        if isinstance(values, str):
            values = re.findall(r"[A-Za-z]{0,2}\d{5,6}", values)
        for value in values or []:
            raw_code = (
                value.get("stockCode") or value.get("stock_code") or value.get("code")
                if isinstance(value, dict)
                else value
            )
            raw_text = str(raw_code or "").upper()
            if raw_text.startswith("HK"):
                continue
            digits = "".join(ch for ch in raw_text if ch.isdigit())
            if not digits or len(digits) > 6:
                continue
            code = digits.zfill(6)[-6:]
            if code not in result:
                result.append(code)
        return result

    @classmethod
    def _extract_stcn_tags(cls, values: Any) -> Tuple[List[str], List[str]]:
        stocks: List[str] = []
        plates: List[str] = []
        rows = values if isinstance(values, list) else []
        for row in rows:
            tag_items = row if isinstance(row, list) else [row]
            for tag in tag_items:
                if not isinstance(tag, dict):
                    continue
                stock_code = tag.get("stock_code") or tag.get("stockCode")
                if stock_code:
                    for code in cls._extract_stock_codes([tag]):
                        if code not in stocks:
                            stocks.append(code)
                    continue
                name = cls._clean_text(tag.get("title") or tag.get("name") or "")
                if name and name not in plates:
                    plates.append(name)
        return stocks[:12], plates[:8]

    @staticmethod
    def _absolute_stcn_url(url: Any) -> str:
        text = str(url or "").strip()
        if not text:
            return ""
        if text.startswith("http://") or text.startswith("https://"):
            return text
        if not text.startswith("/"):
            text = f"/{text}"
        return f"https://www.stcn.com{text}"

    @classmethod
    def _gelonghui_title_from_content(cls, content: str) -> str:
        text = re.sub(r"^格隆汇\d+月\d+日[丨｜]\s*", "", cls._clean_text(content))
        first_sentence = re.split(r"[。；;]", text, maxsplit=1)[0].strip()
        return first_sentence[:48]

    @staticmethod
    def _extract_names(values: Any, *, key: str) -> List[str]:
        result: List[str] = []
        if isinstance(values, str):
            values = [{"name": part.strip()} for part in re.split(r"[,，、/]+", values) if part.strip()]
        for value in values or []:
            name = value.get(key) if isinstance(value, dict) else value
            text = str(name or "").strip()
            if text and text not in result and not re.fullmatch(r"\d{6}", text):
                result.append(text)
        return result[:8]

    @staticmethod
    def _score_importance(title: str, content: str, level: Any = None) -> int:
        text = f"{title} {content}"
        score = 50
        if str(level).upper() == "A" or str(level) == "3":
            score += 30
        elif str(level).upper() == "B" or str(level) == "2":
            score += 18
        elif str(level) == "1":
            score += 8
        for keyword in ["涨停", "异动", "公告", "并购", "重组", "订单", "业绩", "停牌", "复牌", "监管", "涨超"]:
            if keyword in text:
                score += 6
        return min(score, 100)

    @staticmethod
    def _stable_id(title: str, content: str) -> str:
        return hashlib.md5(f"{title}|{content}".encode("utf-8")).hexdigest()[:12]

    @classmethod
    def _dedupe_key(cls, title: str, content: str) -> str:
        text = cls._clean_text(title or content).lower()
        return re.sub(r"[^\w\u4e00-\u9fff]+", "", text)[:80]

    @classmethod
    def _timestamp(cls, value: Any) -> float:
        if isinstance(value, datetime):
            return value.timestamp()
        if isinstance(value, (int, float)):
            timestamp = float(value)
            return timestamp / 1000 if timestamp > 10_000_000_000 else timestamp
        text = str(value or "").strip()
        if not text:
            return 0
        if text.isdigit():
            return cls._timestamp(int(text))
        for candidate in [text, text.replace("/", "-"), text.replace("Z", "+00:00")]:
            try:
                return datetime.fromisoformat(candidate).timestamp()
            except ValueError:
                pass
        return 0

    @classmethod
    def _format_time(cls, value: Any) -> str:
        timestamp = cls._timestamp(value)
        if not timestamp:
            return ""
        return datetime.fromtimestamp(timestamp).strftime("%H:%M:%S")


public_market_news_provider = PublicMarketNewsProvider()
