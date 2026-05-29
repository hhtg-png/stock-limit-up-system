"""Public source adapters for Tongdaxin plugin calibration data."""
from __future__ import annotations

import re
import time
from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Optional, Tuple

import httpx
from bs4 import BeautifulSoup


@dataclass
class ExternalStockMove:
    stock_code: str
    stock_name: str
    trade_date: Optional[date]
    title: str
    content: str
    board_label: str = ""
    plate: str = ""
    source_name: str = "芦苇复盘"
    source_url: str = ""


class LwwhyStockMoveProvider:
    """Fetch and parse publicly rendered lwwhy stock movement pages."""

    base_url = "https://www.lwwhy.com"

    def __init__(self, *, timeout: float = 8.0, cache_ttl: int = 300):
        self.timeout = timeout
        self.cache_ttl = cache_ttl
        self._review_cache: Dict[date, Tuple[float, List[ExternalStockMove]]] = {}
        self._stock_cache: Dict[str, Tuple[float, Optional[ExternalStockMove]]] = {}

    async def get_review_moves(self, trade_date: date) -> List[ExternalStockMove]:
        cached = self._review_cache.get(trade_date)
        if cached and time.time() - cached[0] < self.cache_ttl:
            return cached[1]

        url = f"{self.base_url}/review/action/{trade_date.isoformat()}"
        html = await self._fetch_text(url)
        moves = self.parse_review_action_html(html, trade_date)
        for move in moves:
            if not move.source_url:
                move.source_url = url
        self._review_cache[trade_date] = (time.time(), moves)
        return moves

    async def get_stock_move(self, stock_code: str, trade_date: Optional[date] = None) -> Optional[ExternalStockMove]:
        normalized_code = self.normalize_code(stock_code)
        if trade_date:
            moves = await self.get_review_moves(trade_date)
            for move in moves:
                if move.stock_code == normalized_code:
                    return move

        cached = self._stock_cache.get(normalized_code)
        if cached and time.time() - cached[0] < self.cache_ttl:
            return cached[1]

        url = f"{self.base_url}/stock/detail/{self.market_prefix(normalized_code)}{normalized_code}"
        html = await self._fetch_text(url)
        move = self.parse_stock_detail_html(html, normalized_code)
        if move:
            move.source_url = url
        self._stock_cache[normalized_code] = (time.time(), move)
        return move

    async def _fetch_text(self, url: str) -> str:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/125 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        async with httpx.AsyncClient(timeout=self.timeout, headers=headers, follow_redirects=True) as client:
            response = await client.get(url)
            response.raise_for_status()
            return response.text

    @staticmethod
    def parse_review_action_html(html: str, trade_date: date) -> List[ExternalStockMove]:
        soup = BeautifulSoup(html or "", "html.parser")
        moves: List[ExternalStockMove] = []
        seen_codes = set()

        for code_link in soup.find_all("a", string=re.compile(r"^\d{6}$")):
            href = str(code_link.get("href") or "")
            if "quote.eastmoney.com" not in href:
                continue

            card = LwwhyStockMoveProvider._find_parent_with_classes(code_link, {"p-2", "space-y-1"})
            if not card:
                continue

            code = LwwhyStockMoveProvider.normalize_code(code_link.get_text(strip=True))
            if not code or code in seen_codes:
                continue

            name_link = card.find("a", href=re.compile(r"/stock/detail/[A-Z]{2}\d{6}"))
            stock_name = name_link.get_text(strip=True) if name_link else code
            title_tag = LwwhyStockMoveProvider._find_review_title_tag(card)
            content_tag = LwwhyStockMoveProvider._find_review_content_tag(card)
            title = title_tag.get_text(" ", strip=True) if title_tag else ""
            content = (content_tag.get("title") or content_tag.get_text("\n", strip=True)) if content_tag else ""
            if not title and not content:
                continue

            badge = card.find("span", class_=lambda value: value and "badge" in str(value))
            board_label = badge.get_text(strip=True) if badge and "板" in badge.get_text() else ""
            source_url = ""
            if name_link and name_link.get("href"):
                source_url = LwwhyStockMoveProvider._absolute_url(str(name_link.get("href")))

            moves.append(ExternalStockMove(
                stock_code=code,
                stock_name=stock_name,
                trade_date=trade_date,
                title=title,
                content=LwwhyStockMoveProvider._normalize_content(content),
                board_label=board_label,
                source_url=source_url,
            ))
            seen_codes.add(code)

        return moves

    @staticmethod
    def parse_stock_detail_html(html: str, stock_code: str) -> Optional[ExternalStockMove]:
        soup = BeautifulSoup(html or "", "html.parser")
        marker = soup.find(string=lambda value: bool(value and value.strip() == "最新异动解析"))
        if not marker:
            return None

        container = None
        for parent in marker.parents:
            if parent.name == "div" and parent.find("p"):
                container = parent
                break
        if not container:
            return None

        text = container.get_text("\n", strip=True)
        parsed_date = None
        date_match = re.search(r"\((\d{4}-\d{2}-\d{2})\)", text)
        if date_match:
            parsed_date = date.fromisoformat(date_match.group(1))

        paragraphs = container.find_all("p")
        title_tag = next((p for p in paragraphs if "text-secondary" not in (p.get("class") or [])), None)
        content_tag = next((p for p in paragraphs if "text-secondary" in (p.get("class") or [])), None)
        title = title_tag.get_text(" ", strip=True) if title_tag else ""
        content = (content_tag.get("title") or content_tag.get_text("\n", strip=True)) if content_tag else ""
        if not title and not content:
            return None

        lines = [line.strip() for line in text.splitlines() if line.strip()]
        board_label = LwwhyStockMoveProvider._line_value_after(lines, "连板:")
        plate = LwwhyStockMoveProvider._line_value_after(lines, "板块:")

        return ExternalStockMove(
            stock_code=LwwhyStockMoveProvider.normalize_code(stock_code),
            stock_name=LwwhyStockMoveProvider.normalize_code(stock_code),
            trade_date=parsed_date,
            title=title,
            content=LwwhyStockMoveProvider._normalize_content(content),
            board_label=board_label,
            plate=plate,
        )

    @staticmethod
    def _find_review_title_tag(card):
        for tag in card.find_all("p", recursive=False):
            classes = set(tag.get("class") or [])
            if "text-sm" in classes and "text-secondary" not in classes:
                return tag
        return None

    @staticmethod
    def _find_review_content_tag(card):
        for tag in card.find_all("p", recursive=False):
            classes = set(tag.get("class") or [])
            if "text-secondary" in classes:
                return tag
        return None

    @staticmethod
    def _find_parent_with_classes(tag, required_classes):
        for parent in tag.parents:
            classes = set(parent.get("class") or [])
            if required_classes.issubset(classes):
                return parent
        return None

    @staticmethod
    def _line_value_after(lines: List[str], label: str) -> str:
        for index, line in enumerate(lines):
            if line == label and index + 1 < len(lines):
                return lines[index + 1]
        return ""

    @staticmethod
    def _normalize_content(content: str) -> str:
        return "\n".join(line.strip() for line in (content or "").splitlines() if line.strip())

    @staticmethod
    def _absolute_url(href: str) -> str:
        if href.startswith("http://") or href.startswith("https://"):
            return href
        return f"{LwwhyStockMoveProvider.base_url}{href if href.startswith('/') else '/' + href}"

    @staticmethod
    def normalize_code(stock_code: str) -> str:
        return "".join(ch for ch in str(stock_code or "") if ch.isdigit())[-6:]

    @staticmethod
    def market_prefix(stock_code: str) -> str:
        code = LwwhyStockMoveProvider.normalize_code(stock_code)
        if code.startswith(("6", "9")):
            return "SH"
        if code.startswith(("0", "2", "3")):
            return "SZ"
        return "BJ"


lwwhy_stock_move_provider = LwwhyStockMoveProvider()
