"""Public attribution sources for target-like TDX plugin board labels."""
from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple

import httpx
from bs4 import BeautifulSoup


@dataclass(frozen=True)
class StockConcept:
    name: str
    summary: str = ""


@dataclass
class PublicStockAttribution:
    stock_code: str
    stock_name: str = ""
    reason_title: str = ""
    plate: str = ""
    concepts: List[str] = field(default_factory=list)
    source_name: str = "复盘网/同花顺F10"
    source_url: str = ""


class FupanwangThsAttributionProvider:
    """Combine public Fupanwang limit-up titles with THS F10 concept tags."""

    fupan_base_url = "https://{code}.fupanwang.com/"
    ths_base_url = "https://basic.10jqka.com.cn/{code}/concept.html"

    _GENERIC_CONCEPTS = {
        "一季报增长",
        "年报增长",
        "国企改革",
        "央企",
        "分红",
        "ST摘帽",
        "并购重组",
        "股权转让",
    }
    _MAJOR_THEMES = {
        "AI应用",
        "AI视频",
        "LED",
        "保健品",
        "创新药",
        "地产链",
        "电力",
        "电子布",
        "电阻电容",
        "房地产",
        "服装家纺",
        "工业母机",
        "光模块",
        "化工",
        "基础建设",
        "金刚石概念",
        "金融概念",
        "酒类",
        "锂矿",
        "零售",
        "酿酒",
        "汽车零部件",
        "氢氟酸",
        "商业航天",
        "水利",
        "通信",
        "文化传媒",
        "新型城镇化",
        "新型工业化",
        "医疗器械",
        "医药",
        "有色金属",
        "元器件",
        "智能电网",
    }
    _MAJOR_RULES: List[Tuple[str, List[str]]] = [
        ("通信", ["光模块", "光纤", "CPO", "通信", "PCB", "印制电路板", "覆铜板", "电子布", "高速连接"]),
        ("元器件", ["电阻", "电容", "MLCC", "元器件", "元件", "超级电容"]),
        ("电力", ["绿色电力", "绿电", "火电", "风电", "电力", "发电"]),
        ("地产链", ["地产链", "物业服务", "物业管理", "房屋检测", "城中村"]),
        ("房地产", ["房地产"]),
        ("医药", ["医药", "原料药", "仿制药", "病毒防治", "中药"]),
        ("创新药", ["创新药"]),
        ("医疗器械", ["医疗器械", "脑机接口"]),
        ("零售", ["零售", "新零售", "免税", "百货", "商场", "商业零售"]),
        ("酿酒", ["酿酒", "白酒"]),
        ("汽车零部件", ["汽车零部件", "汽车热管理", "比亚迪", "特斯拉"]),
        ("智能电网", ["智能电网", "变压器", "固态断路器", "固态变压器", "电气设备"]),
        ("基础建设", ["基础建设", "一带一路", "水利"]),
        ("水利", ["水利"]),
        ("储能", ["储能"]),
        ("锂矿", ["锂矿"]),
        ("芯片", ["芯片", "半导体设备", "半导体材料", "光刻胶"]),
        ("机器人概念", ["机器人"]),
        ("AI应用", ["AI营销", "AI应用", "网红经济"]),
        ("文化传媒", ["文化传媒", "短剧", "影视"]),
        ("金刚石概念", ["金刚石", "培育钻石", "CVD"]),
        ("有色金属", ["有色金属", "金属铝", "金属铜", "钼", "铜"]),
    ]
    _SECONDARY_PRIORITY: Dict[str, List[str]] = {
        "AI应用": ["AI营销", "AI视频", "短剧", "网红经济"],
        "地产链": ["房地产", "物业服务", "深圳国资", "洁净室", "香港牌照", "房屋检测"],
        "电力": ["绿色电力", "火电", "风电运营商", "信托概念", "环保"],
        "基础建设": ["一带一路", "水利", "中字头", "并购重组"],
        "零售": ["新零售", "预制菜", "免税", "海峡两岸", "黄金零售", "物业服务", "房地产"],
        "汽车零部件": ["比亚迪产业链", "特斯拉概念", "锂电池", "汽车热管理"],
        "创新药": ["医药"],
        "机器人概念": ["新型工业化", "汽车零部件", "机器视觉"],
        "通信": ["印制电路板", "光模块", "光纤概念", "覆铜板", "PCB铜箔", "HBM", "高速连接", "PCB设备", "电子布"],
        "元器件": ["电阻电容", "端侧AI", "负极", "超级电容"],
        "医药": ["原料药", "创新药", "仿制药", "病毒防治", "中药"],
        "智能电网": ["变压器", "固态断路器", "固态变压器", "电气设备"],
    }
    _CONCEPT_ALIASES = {
        "比亚迪概念": "比亚迪产业链",
        "新能源汽车": "汽车零部件",
        "PCB概念": "印制电路板",
        "共封装光学(CPO)": "光模块",
        "医疗器械概念": "医疗器械",
        "白酒概念": "白酒",
        "黄金概念": "黄金零售",
        "消费电子概念": "消费电子",
        "超级电容": "电阻电容",
        "数据中心(AIDC)": "算力",
        "绿色电力概念": "绿色电力",
    }

    def __init__(self, *, timeout: float = 5.0, cache_ttl: int = 900, max_concurrency: int = 8):
        self.timeout = timeout
        self.cache_ttl = cache_ttl
        self.max_concurrency = max_concurrency
        self._cache: Dict[str, Tuple[float, Optional[PublicStockAttribution]]] = {}

    async def get_attributions(self, codes: Iterable[str]) -> Dict[str, PublicStockAttribution]:
        unique_codes = []
        for code in codes:
            normalized = self.normalize_code(code)
            if normalized and normalized not in unique_codes:
                unique_codes.append(normalized)

        now = time.time()
        result: Dict[str, PublicStockAttribution] = {}
        missing: List[str] = []
        for code in unique_codes:
            cached = self._cache.get(code)
            if cached and now - cached[0] < self.cache_ttl:
                if cached[1]:
                    result[code] = cached[1]
                continue
            missing.append(code)

        if missing:
            semaphore = asyncio.Semaphore(self.max_concurrency)
            async with httpx.AsyncClient(timeout=self.timeout, headers=self._headers(), follow_redirects=True, verify=False) as client:
                fetched = await asyncio.gather(
                    *(self._fetch_one(client, semaphore, code) for code in missing),
                    return_exceptions=True,
                )
            for code, item in zip(missing, fetched):
                attribution = item if isinstance(item, PublicStockAttribution) else None
                self._cache[code] = (time.time(), attribution)
                if attribution:
                    result[code] = attribution

        return result

    async def _fetch_one(
        self,
        client: httpx.AsyncClient,
        semaphore: asyncio.Semaphore,
        code: str,
    ) -> Optional[PublicStockAttribution]:
        async with semaphore:
            fupan_html = ""
            ths_html = ""
            fupan_url = self.fupan_base_url.format(code=code)
            ths_url = self.ths_base_url.format(code=code)
            try:
                fupan_resp, ths_resp = await asyncio.gather(
                    client.get(fupan_url),
                    client.get(ths_url, headers={**self._headers(), "Referer": f"https://basic.10jqka.com.cn/{code}/"}),
                    return_exceptions=True,
                )
                if isinstance(fupan_resp, httpx.Response) and fupan_resp.status_code == 200:
                    fupan_html = fupan_resp.text
                if isinstance(ths_resp, httpx.Response) and ths_resp.status_code == 200:
                    ths_html = ths_resp.content.decode("gbk", errors="ignore")
            except Exception:
                return None

            reason_title = self.parse_fupanwang_reason(fupan_html)
            ths_concepts = self.parse_ths_concepts(ths_html)
            if not reason_title and not ths_concepts:
                return None

            plate, concepts = self.infer_plate_concepts(reason_title, ths_concepts)
            return PublicStockAttribution(
                stock_code=code,
                reason_title=reason_title,
                plate=plate,
                concepts=concepts,
                source_url=fupan_url if reason_title else ths_url,
            )

    @staticmethod
    def parse_fupanwang_reason(html: str) -> str:
        if not html:
            return ""
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text("\n", strip=True)
        match = re.search(r"涨停原因：\s*([^；\n|]+)", text)
        return match.group(1).strip() if match else ""

    @staticmethod
    def parse_ths_concepts(html: str) -> List[StockConcept]:
        if not html:
            return []
        soup = BeautifulSoup(html, "html.parser")
        concepts: List[StockConcept] = []
        for name_cell in soup.select("td.gnName"):
            name = name_cell.get_text(" ", strip=True)
            if not name:
                continue
            row = name_cell.find_parent("tr")
            summary = ""
            if row:
                summary_cell = row.select_one("td.wider")
                if summary_cell:
                    summary = summary_cell.get_text(" ", strip=True)
            concepts.append(StockConcept(name=name, summary=FupanwangThsAttributionProvider._clean_space(summary)))
        return concepts

    @classmethod
    def infer_plate_concepts(cls, reason_title: str, ths_concepts: List[StockConcept]) -> Tuple[str, List[str]]:
        title = (reason_title or "").strip()
        parenthesized = re.match(r"^([^()（）]+)[(（]([^()（）]+)[)）]$", title)
        if parenthesized:
            plate = parenthesized.group(1).strip()
            inner = cls._normalize_concept(parenthesized.group(2).strip())
            return plate, cls._dedupe([inner, plate])

        title_parts = [cls._normalize_concept(part) for part in cls._split_concepts(title)]
        plate = cls._choose_plate(title, ths_concepts)
        concept_candidates = cls._concept_candidates(title_parts, ths_concepts)
        secondary = cls._choose_secondary(plate, concept_candidates)
        concepts = cls._dedupe([secondary, *concept_candidates, plate])
        return plate, [concept for concept in concepts if concept]

    @classmethod
    def _choose_plate(cls, title: str, ths_concepts: List[StockConcept]) -> str:
        title_parts = [cls._normalize_concept(part) for part in cls._split_concepts(title)]
        for part in title_parts:
            if part in cls._MAJOR_THEMES:
                return cls._normalize_major(part)

        concept_names = [cls._normalize_concept(concept.name) for concept in ths_concepts]
        if not title_parts:
            first_name = concept_names[0] if concept_names else ""
            first_summary = ths_concepts[0].summary if ths_concepts else ""
            if first_name == "家用电器" and "机器人概念" in concept_names[:3] and "汽车零部件" in first_summary:
                return "机器人概念"
            first_major = cls._major_from_concept_name(first_name)
            if first_major:
                return first_major
            if "创新药" in concept_names:
                return "创新药"
            if any(name in {"电阻电容", "MLCC", "元器件"} for name in concept_names):
                return "元器件"
            if "医疗器械" in concept_names:
                return "医疗器械"

        text = title or " ".join(f"{item.name} {item.summary}" for item in ths_concepts[:5])
        lowered = text.lower()
        for theme, keywords in cls._MAJOR_RULES:
            if any(keyword.lower() in lowered for keyword in keywords):
                return theme

        for concept in ths_concepts:
            normalized = cls._normalize_concept(concept.name)
            if normalized in cls._MAJOR_THEMES:
                return cls._normalize_major(normalized)
        return title_parts[0] if title_parts else "其他"

    @classmethod
    def _major_from_concept_name(cls, concept_name: str) -> str:
        if not concept_name:
            return ""
        if concept_name in {"光纤概念", "光模块", "印制电路板", "覆铜板", "PCB铜箔", "高速连接", "通信", "F5G概念"}:
            return "通信"
        if concept_name in {"电阻电容", "元器件", "MLCC"}:
            return "元器件"
        if concept_name in {"房地产", "物业服务", "物业管理", "房屋检测", "租售同权"}:
            return "地产链"
        if concept_name in {"绿色电力", "火电", "风电", "核电", "电力"}:
            return "电力"
        if concept_name in {"医疗器械", "脑机接口"}:
            return "医疗器械"
        if concept_name in {"免税", "免税店"}:
            return "免税"
        if concept_name in {"预制菜", "新零售", "免税", "黄金零售", "商业零售"}:
            return "零售"
        if concept_name in {"比亚迪产业链", "汽车零部件", "特斯拉概念", "汽车热管理"}:
            return "汽车零部件"
        if concept_name in cls._MAJOR_THEMES:
            return cls._normalize_major(concept_name)
        return ""

    @classmethod
    def _concept_candidates(cls, title_parts: List[str], ths_concepts: List[StockConcept]) -> List[str]:
        candidates: List[str] = []
        for part in title_parts:
            if part not in cls._GENERIC_CONCEPTS:
                candidates.append(part)
        for concept in ths_concepts:
            normalized = cls._normalize_concept(concept.name)
            if normalized not in cls._GENERIC_CONCEPTS:
                candidates.append(normalized)
            summary = concept.summary or ""
            for phrase in [
                "汽车零部件",
                "电阻电容",
                "医药",
                "海南",
                "印制电路板",
                "光模块",
                "光纤概念",
                "比亚迪产业链",
                "预制菜",
                "新零售",
                "物业服务",
                "深圳国资",
                "火电",
                "绿色电力",
                "风电运营商",
                "食品饮料",
                "金属铝",
                "钢铁",
                "香港牌照",
                "黄金零售",
                "洁净室",
                "房屋检测",
            ]:
                if phrase in summary:
                    candidates.append(phrase)
            if "比亚迪" in summary:
                candidates.append("比亚迪产业链")
        if "创新药" in candidates and "医药" not in candidates:
            candidates.append("医药")
        return cls._dedupe(candidates)

    @classmethod
    def _choose_secondary(cls, plate: str, candidates: List[str]) -> str:
        priorities = cls._SECONDARY_PRIORITY.get(plate, [])
        for preferred in priorities:
            if preferred in candidates:
                return preferred
        for candidate in candidates:
            if candidate and candidate != plate:
                return candidate
        return ""

    @classmethod
    def _normalize_concept(cls, value: str) -> str:
        text = cls._clean_space(value).strip()
        text = cls._CONCEPT_ALIASES.get(text, text)
        if text.endswith("概念") and text in cls._CONCEPT_ALIASES:
            return cls._CONCEPT_ALIASES[text]
        return text

    @staticmethod
    def _normalize_major(value: str) -> str:
        if value == "电阻电容":
            return "元器件"
        return value

    @staticmethod
    def _split_concepts(text: str) -> List[str]:
        if not text:
            return []
        return [part.strip() for part in re.split(r"[+、，,/]+", text) if part.strip()]

    @staticmethod
    def _dedupe(values: Iterable[str]) -> List[str]:
        result: List[str] = []
        for value in values:
            if value and value not in result:
                result.append(value)
        return result

    @staticmethod
    def normalize_code(stock_code: str) -> str:
        digits = "".join(ch for ch in str(stock_code or "") if ch.isdigit())
        return digits[-6:].zfill(6) if digits else ""

    @staticmethod
    def _clean_space(value: str) -> str:
        return re.sub(r"\s+", "", value or "")

    @staticmethod
    def _headers() -> Dict[str, str]:
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/125 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }


public_attribution_provider = FupanwangThsAttributionProvider()
