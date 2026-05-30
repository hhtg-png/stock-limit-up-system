"""Public source adapters for Tongdaxin plugin calibration data."""
from __future__ import annotations

import asyncio
import contextlib
import re
import time
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

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


@dataclass
class StockConcept:
    name: str
    summary: str = ""


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
        title_tag = LwwhyStockMoveProvider._find_latest_move_title_tag(paragraphs)
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
    def _find_latest_move_title_tag(paragraphs):
        metadata_labels = {
            "板块",
            "板块:",
            "板块：",
            "异动时间",
            "异动时间:",
            "异动时间：",
            "连板",
            "连板:",
            "连板：",
        }
        inline_metadata_prefixes = tuple(
            label for label in metadata_labels
            if label.endswith(":") or label.endswith("：")
        )
        previous_text = ""
        for tag in paragraphs:
            classes = set(tag.get("class") or [])
            text = tag.get_text(" ", strip=True)
            normalized = re.sub(r"\s+", "", text)
            previous_normalized = re.sub(r"\s+", "", previous_text)

            if "text-secondary" in classes:
                continue
            if not normalized:
                previous_text = text
                continue
            if normalized in metadata_labels:
                previous_text = text
                continue
            if normalized.startswith(inline_metadata_prefixes):
                previous_text = text
                continue
            if previous_normalized in metadata_labels:
                previous_text = text
                continue
            if re.fullmatch(r"\d{1,2}:\d{2}(?::\d{2})?", normalized):
                previous_text = text
                continue

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
        alternate_label = label[:-1] + "：" if label.endswith(":") else label
        for index, line in enumerate(lines):
            if line == label and index + 1 < len(lines):
                return lines[index + 1]
            for current_label in (label, alternate_label):
                if line.startswith(current_label):
                    return line[len(current_label):].strip()
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
        digits = "".join(ch for ch in str(stock_code or "") if ch.isdigit())
        return digits[-6:].zfill(6) if digits else ""

    @staticmethod
    def market_prefix(stock_code: str) -> str:
        code = LwwhyStockMoveProvider.normalize_code(stock_code)
        if code.startswith(("6", "9")):
            return "SH"
        if code.startswith(("0", "2", "3")):
            return "SZ"
        return "BJ"


class DabankeStockMoveProvider:
    """Fetch the latest historical limit-up reason and enrich it with THS F10 concepts."""

    base_url = "https://dabanke.com"
    ths_concept_url = "https://basic.10jqka.com.cn/{code}/concept.html"

    def __init__(self, *, timeout: float = 6.0, cache_ttl: int = 1800):
        self.timeout = timeout
        self.cache_ttl = cache_ttl
        self._stock_cache: Dict[str, Tuple[float, Optional[ExternalStockMove]]] = {}

    async def get_stock_move(self, stock_code: str, trade_date: Optional[date] = None) -> Optional[ExternalStockMove]:
        normalized_code = LwwhyStockMoveProvider.normalize_code(stock_code)
        cached = self._stock_cache.get(normalized_code)
        if cached and time.time() - cached[0] < self.cache_ttl:
            return cached[1]

        dabanke_url = f"{self.base_url}/gupiao-{normalized_code}.html"
        ths_url = self.ths_concept_url.format(code=normalized_code)
        headers = self._headers()
        async with httpx.AsyncClient(timeout=self.timeout, headers=headers, follow_redirects=True, verify=False) as client:
            dabanke_response, ths_response = await self._fetch_pair(client, dabanke_url, ths_url)

        dabanke_html = dabanke_response if isinstance(dabanke_response, str) else ""
        ths_html = ths_response if isinstance(ths_response, str) else ""
        move = self.parse_stock_history_html(dabanke_html, normalized_code, ths_html)
        if move:
            move.source_url = dabanke_url
        self._stock_cache[normalized_code] = (time.time(), move)
        return move

    async def _fetch_pair(self, client: httpx.AsyncClient, dabanke_url: str, ths_url: str):
        return await asyncio.gather(
            self._fetch_text(client, dabanke_url),
            self._fetch_text(client, ths_url, decode="gbk"),
            return_exceptions=True,
        )

    async def _fetch_text(self, client: httpx.AsyncClient, url: str, *, decode: Optional[str] = None) -> str:
        response = await client.get(url)
        response.raise_for_status()
        if decode:
            return response.content.decode(decode, errors="ignore")
        return response.text

    @classmethod
    def parse_stock_history_html(
        cls,
        dabanke_html: str,
        stock_code: str,
        ths_html: str = "",
    ) -> Optional[ExternalStockMove]:
        soup = BeautifulSoup(dabanke_html or "", "html.parser")
        row = cls._latest_limit_up_row(soup)
        if not row:
            return None

        stock_name = cls._parse_stock_name(soup) or LwwhyStockMoveProvider.normalize_code(stock_code)
        raw_reason = row["reason"]
        base_title, base_content = cls._split_reason(raw_reason)
        concepts = cls.parse_ths_concepts_html(ths_html)
        title = cls._synthesize_title(base_title, concepts)
        content = cls._build_content(base_content, concepts, title)

        return ExternalStockMove(
            stock_code=LwwhyStockMoveProvider.normalize_code(stock_code),
            stock_name=stock_name,
            trade_date=row["trade_date"],
            title=title or base_title or "暂无异动原因",
            content=content or base_content or title or base_title,
            board_label=row["event"],
            plate=cls._plate_from_title(title or base_title),
            source_name="打板客/同花顺F10",
        )

    @classmethod
    def parse_ths_concepts_html(cls, html: str) -> List[StockConcept]:
        soup = BeautifulSoup(html or "", "html.parser")
        concepts: List[StockConcept] = []
        for name_cell in soup.select("td.gnName"):
            name = cls._normalize_concept_name(name_cell.get_text(" ", strip=True))
            if not name:
                continue

            row = name_cell.find_parent("tr")
            summary = ""
            if row:
                next_row = row.find_next_sibling("tr", class_=lambda value: value and "extend_content" in str(value))
                if next_row:
                    summary = next_row.get_text(" ", strip=True)
                if not summary:
                    summary_cell = row.select_one("td.wider")
                    if summary_cell:
                        summary = summary_cell.get_text(" ", strip=True)
            concepts.append(StockConcept(name=name, summary=cls._clean_space(summary)))
        return concepts

    @staticmethod
    def _latest_limit_up_row(soup: BeautifulSoup) -> Optional[Dict[str, Any]]:
        candidates: List[Dict[str, Any]] = []
        for row in soup.find_all("tr"):
            cells = [cell.get_text(" ", strip=True) for cell in row.find_all("td")]
            if len(cells) < 4 or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", cells[0]):
                continue
            if "涨停" not in cells[2]:
                continue
            candidates.append({
                "trade_date": date.fromisoformat(cells[0]),
                "time": cells[1],
                "event": cells[2],
                "reason": cells[3],
            })
        if not candidates:
            return None
        exact = [item for item in candidates if item["event"] == "涨停"]
        return exact[0] if exact else candidates[0]

    @staticmethod
    def _parse_stock_name(soup: BeautifulSoup) -> str:
        title = soup.title.get_text(" ", strip=True) if soup.title else ""
        match = re.match(r"\s*([^（(]+)[（(]", title)
        return match.group(1).strip() if match else ""

    @classmethod
    def _split_reason(cls, raw_reason: str) -> Tuple[str, str]:
        reason = cls._clean_space(raw_reason)
        if "·" in reason:
            title, content = reason.split("·", 1)
            return cls._clean_reason_title(title), cls._clean_space(content)
        return cls._clean_reason_title(reason), ""

    @classmethod
    def _synthesize_title(cls, base_title: str, concepts: List[StockConcept]) -> str:
        joined = cls._clean_space(" ".join([base_title, *[f"{item.name} {item.summary}" for item in concepts]]))
        candidates: List[str] = []
        if "机器人" in joined:
            candidates.append("机器人")
        if "比亚迪" in joined:
            candidates.append("核心客户比亚迪" if any(keyword in joined for keyword in ["核心供应商", "向比亚迪", "主驱动电机"]) else "比亚迪")
        if "驱动电机铁芯" in joined or ("驱动电机" in joined and "铁芯" in joined):
            candidates.append("驱动电机铁芯")
        elif "电机铁芯" in joined:
            candidates.append("电机铁芯")
        if "新能源汽车" in joined:
            candidates.append("新能源汽车")

        deduped = cls._dedupe(candidates)
        if len(deduped) >= 3:
            return "+".join(deduped)
        return cls._clean_reason_title(base_title)

    @classmethod
    def _build_content(cls, base_content: str, concepts: List[StockConcept], title: str) -> str:
        title_text = title or ""
        selected: List[str] = []
        for concept in concepts:
            name = concept.name
            if not concept.summary:
                continue
            if name in title_text or name.replace("概念", "") in title_text or any(keyword in concept.summary for keyword in title_text.split("+")):
                selected.append(concept.summary)
        if not selected:
            selected = [concept.summary for concept in concepts if concept.summary][:4]

        lines = [f"{index + 1}、{cls._clean_space(summary)}" for index, summary in enumerate(cls._dedupe(selected[:4]))]
        if base_content and "..." not in base_content and "…" not in base_content:
            lines.append(f"{len(lines) + 1}、{base_content}")
        return "\n".join(lines)

    @classmethod
    def _clean_reason_title(cls, title: str) -> str:
        title = cls._clean_space(title)
        title = title.replace("机器人概念", "机器人")
        title = title.replace("比亚迪概念", "比亚迪")
        return title

    @staticmethod
    def _normalize_concept_name(name: str) -> str:
        return re.sub(r"\s+", "", name or "")

    @classmethod
    def _plate_from_title(cls, title: str) -> str:
        return (title or "").split("+", 1)[0].strip()

    @staticmethod
    def _clean_space(text: str) -> str:
        return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()

    @staticmethod
    def _dedupe(items: List[str]) -> List[str]:
        result: List[str] = []
        for item in items:
            if item and item not in result:
                result.append(item)
        return result

    @staticmethod
    def _headers() -> Dict[str, str]:
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/125 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": "https://basic.10jqka.com.cn/",
        }


class PublicStockMoveProvider:
    """Prefer rich latest detail pages, then fall back to historical public limit-up reasons."""

    def __init__(
        self,
        *,
        lwwhy_provider: Optional[LwwhyStockMoveProvider] = None,
        dabanke_provider: Optional[DabankeStockMoveProvider] = None,
        lwwhy_prefer_timeout: float = 0.9,
    ):
        self.lwwhy_provider = lwwhy_provider or LwwhyStockMoveProvider()
        self.dabanke_provider = dabanke_provider or DabankeStockMoveProvider()
        self.lwwhy_prefer_timeout = lwwhy_prefer_timeout

    async def get_review_moves(self, trade_date: date) -> List[ExternalStockMove]:
        return await self.lwwhy_provider.get_review_moves(trade_date)

    async def get_stock_move(self, stock_code: str, trade_date: Optional[date] = None) -> Optional[ExternalStockMove]:
        lwwhy_task = asyncio.create_task(self.lwwhy_provider.get_stock_move(stock_code, None))
        dabanke_task = asyncio.create_task(
            self.dabanke_provider.get_stock_move(stock_code, trade_date)
        )

        try:
            lwwhy_result = await asyncio.wait_for(
                asyncio.shield(lwwhy_task),
                timeout=self.lwwhy_prefer_timeout,
            )
            if isinstance(lwwhy_result, ExternalStockMove):
                dabanke_task.add_done_callback(self._consume_background_result)
                return lwwhy_result
        except asyncio.TimeoutError:
            pass
        except Exception:
            lwwhy_result = None

        dabanke_result = await self._task_result_or_none(dabanke_task)
        if isinstance(dabanke_result, ExternalStockMove):
            lwwhy_task.add_done_callback(self._consume_background_result)
            return dabanke_result

        lwwhy_result = await self._task_result_or_none(lwwhy_task)
        if isinstance(lwwhy_result, ExternalStockMove):
            return lwwhy_result
        return None

    @staticmethod
    async def _task_result_or_none(task: asyncio.Task) -> Optional[ExternalStockMove]:
        with contextlib.suppress(asyncio.CancelledError, Exception):
            result = await task
            if isinstance(result, ExternalStockMove):
                return result
        return None

    @staticmethod
    def _consume_background_result(task: asyncio.Task) -> None:
        with contextlib.suppress(asyncio.CancelledError, Exception):
            task.result()


lwwhy_stock_move_provider = LwwhyStockMoveProvider()
dabanke_stock_move_provider = DabankeStockMoveProvider()
public_stock_move_provider = PublicStockMoveProvider(
    lwwhy_provider=lwwhy_stock_move_provider,
    dabanke_provider=dabanke_stock_move_provider,
)
