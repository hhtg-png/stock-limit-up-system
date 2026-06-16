"""TongHuaShun original limit-up move analysis source."""
from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from datetime import date
from typing import Iterable, List, Optional

import httpx
from bs4 import BeautifulSoup


@dataclass(frozen=True)
class ThsMoveAnalysis:
    stock_code: str
    stock_name: str
    trade_date: date
    title: str
    summary: str
    evidence: str
    article_url: str = ""
    published_at: str = ""
    source_name: str = "同花顺异动观察"


class ThsMoveAnalysisSource:
    """Fetch and parse THS 涨停雷达 / 异动原因揭秘 entries."""

    LIST_URL = "https://yuanchuang.10jqka.com.cn/zhangting/"
    JSONP_URL = "https://comment.10jqka.com.cn/api/zhangting.php"

    def __init__(self, *, timeout: float = 6.0, cache_ttl: int = 240, max_pages: int = 10):
        self.timeout = timeout
        self.cache_ttl = cache_ttl
        self.max_pages = max(1, int(max_pages or 1))
        self._cache: dict[str, tuple[float, List[ThsMoveAnalysis]]] = {}

    async def get_daily_analyses(
        self,
        trade_date: date,
        *,
        target_codes: Optional[Iterable[str]] = None,
        force_refresh: bool = False,
    ) -> List[ThsMoveAnalysis]:
        cache_key = trade_date.isoformat()
        targets = {str(code).strip() for code in target_codes or [] if str(code or "").strip()}
        cached = self._cache.get(cache_key)
        if cached and not force_refresh and time.time() - cached[0] < self.cache_ttl:
            return self._filter_targets(cached[1], targets)

        items: List[ThsMoveAnalysis] = []
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/json,text/plain,*/*",
            "Referer": "https://yuanchuang.10jqka.com.cn/zhangting/",
        }
        async with httpx.AsyncClient(timeout=self.timeout, headers=headers, follow_redirects=True) as client:
            response = await client.get(self.LIST_URL)
            response.raise_for_status()
            items.extend(self.parse_list_html(self._decode_html_response(response), trade_date))

            if not self._has_all_targets(items, targets):
                for page in range(1, self.max_pages):
                    response = await client.get(
                        self.JSONP_URL,
                        params={"start": page * 10, "count": 10, "jsoncallback": "callback"},
                        headers={"Referer": self.LIST_URL},
                    )
                    response.raise_for_status()
                    page_items = self.parse_jsonp_response(response.text, trade_date)
                    if not page_items:
                        break
                    items.extend(page_items)
                    if self._has_all_targets(items, targets):
                        break

        deduped = self._dedupe(items)
        self._cache[cache_key] = (time.time(), deduped)
        return self._filter_targets(deduped, targets)

    @classmethod
    def parse_jsonp_response(cls, text: str, trade_date: date) -> List[ThsMoveAnalysis]:
        match = re.search(r"^[^(]*\((.*)\)\s*;?\s*$", text or "", flags=re.S)
        if not match:
            return []
        try:
            payload = json.loads(match.group(1))
        except json.JSONDecodeError:
            return []
        html = ((payload.get("data") or {}) if isinstance(payload, dict) else {}).get("html") or ""
        return cls.parse_list_html(str(html), trade_date)

    @classmethod
    def parse_list_html(cls, html: str, trade_date: date) -> List[ThsMoveAnalysis]:
        soup = BeautifulSoup(html or "", "html.parser")
        items: List[ThsMoveAnalysis] = []
        for node in soup.select(".item"):
            stock_links = cls._stock_links(node)
            if not stock_links:
                continue

            title = cls._extract_title(node)
            summary = cls._extract_summary(node)
            evidence = cls.extract_evidence(summary)
            article_url = cls._extract_article_url(node)
            published_at = cls._extract_published_at(node)
            if not title and not summary:
                continue

            for stock_code, stock_name in stock_links:
                items.append(
                    ThsMoveAnalysis(
                        stock_code=stock_code,
                        stock_name=stock_name,
                        trade_date=trade_date,
                        title=title,
                        summary=summary,
                        evidence=evidence,
                        article_url=article_url,
                        published_at=published_at,
                    )
                )
        return cls._dedupe(items)

    @classmethod
    def extract_evidence(cls, text: str) -> str:
        cleaned = cls._clean_text(text)
        if not cleaned:
            return ""
        markers = ("异动原因揭秘：", "异动原因揭秘:", "原因揭秘：", "原因揭秘:")
        for marker in markers:
            index = cleaned.find(marker)
            if index >= 0:
                cleaned = cleaned[index + len(marker):]
                break
        cleaned = re.split(r"\[?详细内容\]?|查看详情|展开全文", cleaned, maxsplit=1)[0]
        return cleaned.strip(" ：:。")[:280]

    @classmethod
    def _stock_links(cls, node) -> List[tuple[str, str]]:
        pairs: List[tuple[str, str]] = []
        for link in node.find_all("a", href=True):
            match = re.search(r"stockpage\.10jqka\.com\.cn/(\d{6})", str(link.get("href") or ""))
            if not match:
                continue
            stock_code = match.group(1)
            stock_name = cls._clean_text(link.get_text(" ", strip=True))
            stock_name = re.sub(r"[（(]\d{6}[）)]", "", stock_name).strip()
            if stock_code and stock_name and (stock_code, stock_name) not in pairs:
                pairs.append((stock_code, stock_name))
        return pairs

    @classmethod
    def _extract_title(cls, node) -> str:
        title_node = node.select_one(".title") or node.select_one("h2")
        if title_node:
            title_text = cls._clean_text(title_node.get_text(" ", strip=True))
            if title_text and title_text != "[详细内容]":
                return title_text
        link = node.select_one(".title a") or node.select_one("h2 a") or node.select_one("a.dlink")
        if link:
            title_text = cls._clean_text(link.get_text(" ", strip=True))
            return "" if title_text == "[详细内容]" else title_text
        return ""

    @classmethod
    def _extract_summary(cls, node) -> str:
        summary_node = (
            node.select_one(".arc-cont")
            or node.select_one(".intro")
            or node.select_one(".summary")
            or node.select_one(".desc")
            or node.select_one("p")
        )
        if summary_node:
            return cls._clean_text(summary_node.get_text(" ", strip=True))
        title = cls._extract_title(node)
        full_text = cls._clean_text(node.get_text(" ", strip=True))
        return full_text.replace(title, "", 1).strip() if title else full_text

    @classmethod
    def _extract_article_url(cls, node) -> str:
        link = node.select_one("a.dlink") or node.find("a", href=re.compile(r"yuanchuang\.10jqka\.com\.cn"))
        return str(link.get("href") or "").strip() if link else ""

    @staticmethod
    def _extract_published_at(node) -> str:
        text = node.get_text(" ", strip=True)
        match = re.search(r"20\d{2}-\d{2}-\d{2}\s+\d{2}:\d{2}(?::\d{2})?", text)
        if not match:
            match = re.search(r"20\d{2}-\d{2}-\d{2}\s+\d{2}\s*:\s*\d{2}(?:\s*:\s*\d{2})?", text)
        if match:
            return re.sub(r"\s*:\s*", ":", re.sub(r"\s+", " ", match.group(0))).strip()
        return ""

    @staticmethod
    def _clean_text(value: str) -> str:
        return re.sub(r"\s+", " ", str(value or "")).strip()

    @staticmethod
    def _decode_html_response(response: httpx.Response) -> str:
        content_type = response.headers.get("content-type", "").lower()
        if "utf-8" in content_type:
            return response.content.decode("utf-8", errors="replace")
        try:
            return response.content.decode("gb18030", errors="replace")
        except Exception:
            return response.text

    @classmethod
    def _dedupe(cls, items: Iterable[ThsMoveAnalysis]) -> List[ThsMoveAnalysis]:
        deduped: dict[str, ThsMoveAnalysis] = {}
        for item in items:
            if item.stock_code not in deduped:
                deduped[item.stock_code] = item
        return list(deduped.values())

    @staticmethod
    def _filter_targets(items: List[ThsMoveAnalysis], targets: set[str]) -> List[ThsMoveAnalysis]:
        if not targets:
            return list(items)
        return [item for item in items if item.stock_code in targets]

    @staticmethod
    def _has_all_targets(items: List[ThsMoveAnalysis], targets: set[str]) -> bool:
        if not targets:
            return False
        found = {item.stock_code for item in items}
        return targets.issubset(found)


ths_move_analysis_source = ThsMoveAnalysisSource()
