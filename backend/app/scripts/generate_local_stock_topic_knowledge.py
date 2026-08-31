"""Generate the local stock topic knowledge base from local data only.

The generated JSON is used as a deterministic hidden-topic candidate source for
limit-up classification. This script does not call model providers or browse the
web. It reads local database rows and optional local seed cache files.
"""
from __future__ import annotations

import argparse
import asyncio
import gzip
import json
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional

from sqlalchemy import select

from app.database import async_session_maker
from app.models.limit_up import LimitUpRecord
from app.models.stock import Stock
from app.services.local_topic_knowledge_service import LocalTopicKnowledgeService
from app.services.ths_limit_up_classification_service import ThsLimitUpClassificationService


ROOT_DIR = Path(__file__).resolve().parents[3]
BACKEND_DIR = ROOT_DIR / "backend"
DEFAULT_OUTPUT_PATH = BACKEND_DIR / "app" / "data" / "local_stock_topic_knowledge.json"
DEFAULT_SEED_GLOB = str(BACKEND_DIR / "data" / "tdx_stock_move_seed_all_a_20260601*.jsonl.gz")

SHORT_VALID_THEMES = {"AI", "VR", "AR", "CPO", "PCB"}
REJECT_THEME_KEYWORDS = (
    "公司所属行业",
    "板块涨跌幅",
    "涨跌幅比例",
    "调整为",
    "风险博弈",
    "暂无异动",
    "股权激励",
    "定增",
    "审核通过",
    "递表",
    "一季报",
    "年报",
    "半年报",
    "业绩",
    "分红",
    "权益分派",
    "风险提示",
    "控股股东",
    "总裁受让",
    "市占率",
    "龙头",
)
REJECT_THEME_NAMES = {
    "其他",
    "人工智能",
    "半导体",
    "新能源",
    "通信",
    "数字经济",
    "重组",
    "国资",
    "央企",
    "国企",
    "中科院",
    "重大订单",
    "算力订单",
    "新能源订单",
    "资产处置",
}
MANUAL_TOPIC_SOURCES = {"local_codex", "manual", "local_manual"}


@dataclass
class TopicCandidate:
    theme: str
    source: str
    confidence: float
    evidence: str = ""
    aliases: List[str] = field(default_factory=list)
    hits: int = 1


@dataclass
class StockProfile:
    stock_code: str
    stock_name: str
    market: str = ""
    industry: str = ""
    concept: List[str] = field(default_factory=list)
    topics: Dict[str, TopicCandidate] = field(default_factory=dict)


def normalize_stock_code(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits[-6:].zfill(6) if digits else ""


def infer_market(stock_code: str) -> str:
    if stock_code.startswith(("6", "9")):
        return "SH"
    if stock_code.startswith(("0", "2", "3")):
        return "SZ"
    if stock_code.startswith(("4", "8")):
        return "BJ"
    return ""


def clean_text(value: Any, *, limit: int = 160) -> str:
    return " ".join(str(value or "").split())[:limit]


def normalize_concept_list(value: Any) -> List[str]:
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            parsed = value.replace("，", ",").replace("、", ",").split(",")
    else:
        parsed = value
    if isinstance(parsed, Mapping):
        raw_items: Iterable[Any] = parsed.values()
    elif isinstance(parsed, list):
        raw_items = parsed
    else:
        raw_items = []
    concepts = []
    for raw in raw_items:
        concept = clean_text(raw, limit=40)
        if concept and concept not in concepts:
            concepts.append(concept)
    return concepts[:12]


def is_valid_generated_theme(theme: str) -> bool:
    text = clean_text(theme, limit=80)
    if not text:
        return False
    if text not in SHORT_VALID_THEMES and len(text) < 2:
        return False
    if text.isdigit():
        return False
    if any(keyword in text for keyword in REJECT_THEME_KEYWORDS):
        return False
    return text not in REJECT_THEME_NAMES


def iter_jsonl_gz(path: Path) -> Iterator[Dict[str, Any]]:
    try:
        with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as file_obj:
            while True:
                try:
                    line = file_obj.readline()
                except EOFError:
                    break
                if not line:
                    break
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(payload, dict):
                    yield payload
    except FileNotFoundError:
        return
    except OSError:
        return


def add_or_merge_topic(
    profile: StockProfile,
    theme: str,
    *,
    source: str,
    confidence: float,
    evidence: str = "",
    aliases: Optional[List[str]] = None,
) -> None:
    normalized = ThsLimitUpClassificationService._normalize_fine_theme(theme) or clean_text(theme, limit=40)
    if not is_valid_generated_theme(normalized):
        return

    existing = profile.topics.get(normalized)
    if existing is None:
        profile.topics[normalized] = TopicCandidate(
            theme=normalized,
            source=source,
            confidence=max(0.0, min(1.0, confidence)),
            evidence=clean_text(evidence, limit=160),
            aliases=aliases or [],
            hits=1,
        )
        return

    existing.hits += 1
    existing.confidence = max(existing.confidence, max(0.0, min(1.0, confidence)))
    if evidence and not existing.evidence:
        existing.evidence = clean_text(evidence, limit=160)
    for alias in aliases or []:
        if alias and alias not in existing.aliases:
            existing.aliases.append(alias[:40])


def merge_manual_record(profile: StockProfile, raw_record: Mapping[str, Any]) -> None:
    if raw_record.get("stock_name"):
        profile.stock_name = clean_text(raw_record.get("stock_name"), limit=50)
    if raw_record.get("market") and not profile.market:
        profile.market = clean_text(raw_record.get("market"), limit=10)
    if raw_record.get("industry") and not profile.industry:
        profile.industry = clean_text(raw_record.get("industry"), limit=50)

    for raw_topic in raw_record.get("topics") or []:
        topic = LocalTopicKnowledgeService._normalize_topic(raw_topic)
        if not topic:
            continue
        if str(topic.get("source") or "") not in MANUAL_TOPIC_SOURCES:
            continue
        add_or_merge_topic(
            profile,
            topic["theme"],
            source=topic.get("source") or "local_codex",
            confidence=max(float(topic.get("confidence") or 0.0), 0.8),
            evidence=topic.get("evidence") or "",
            aliases=topic.get("aliases") or [],
        )


def has_manual_topics(raw_record: Mapping[str, Any]) -> bool:
    for raw_topic in raw_record.get("topics") or []:
        topic = LocalTopicKnowledgeService._normalize_topic(raw_topic)
        if topic and str(topic.get("source") or "") in MANUAL_TOPIC_SOURCES:
            return True
    return False


def apply_seed_payload(profiles: Dict[str, StockProfile], payload: Mapping[str, Any]) -> int:
    count = 0
    for item in ((payload.get("payload") or {}).get("items") or []):
        if not isinstance(item, Mapping):
            continue
        code = normalize_stock_code(item.get("stock_code"))
        if not code:
            continue
        name = clean_text(item.get("stock_name"), limit=50) or code
        profile = profiles.setdefault(
            code,
            StockProfile(stock_code=code, stock_name=name, market=infer_market(code)),
        )
        if not profile.stock_name or profile.stock_name == code:
            profile.stock_name = name
        if item.get("industry") and not profile.industry:
            profile.industry = clean_text(item.get("industry"), limit=50)

        concepts = normalize_concept_list(item.get("concepts"))
        for concept in concepts:
            if concept not in profile.concept:
                profile.concept.append(concept)
            add_or_merge_topic(
                profile,
                concept,
                source="local_seed_concept",
                confidence=0.62,
                evidence=f"本地seed概念：{concept}",
            )

        for reason in item.get("reasons") or []:
            if not isinstance(reason, Mapping):
                continue
            title = clean_text(reason.get("title"), limit=120)
            for theme in ThsLimitUpClassificationService.extract_fine_themes_from_texts([title], limit=5):
                add_or_merge_topic(
                    profile,
                    theme,
                    source="local_seed_reason",
                    confidence=0.66,
                    evidence=title,
                )
        count += 1
    return count


async def load_db_profiles() -> tuple[Dict[str, StockProfile], Counter[str]]:
    profiles: Dict[str, StockProfile] = {}
    stats: Counter[str] = Counter()
    async with async_session_maker() as session:
        stock_result = await session.execute(select(Stock).order_by(Stock.stock_code))
        for stock in stock_result.scalars().all():
            code = normalize_stock_code(stock.stock_code)
            if not code:
                continue
            concepts = normalize_concept_list(stock.concept)
            profile = profiles.setdefault(
                code,
                StockProfile(
                    stock_code=code,
                    stock_name=clean_text(stock.stock_name, limit=50) or code,
                    market=clean_text(stock.market, limit=10) or infer_market(code),
                    industry=clean_text(stock.industry, limit=50),
                    concept=concepts,
                ),
            )
            if profile.industry:
                add_or_merge_topic(
                    profile,
                    profile.industry,
                    source="local_db_industry",
                    confidence=0.58,
                    evidence=f"本地股票库行业：{profile.industry}",
                )
            for concept in concepts:
                add_or_merge_topic(
                    profile,
                    concept,
                    source="local_db_concept",
                    confidence=0.64,
                    evidence=f"本地股票库概念：{concept}",
                )
        stats["db_stock_count"] = len(profiles)

        reason_result = await session.execute(
            select(Stock.stock_code, LimitUpRecord.limit_up_reason, LimitUpRecord.reason_category)
            .join(Stock, LimitUpRecord.stock_id == Stock.id)
            .where(LimitUpRecord.limit_up_reason.isnot(None))
        )
        for code_raw, reason, category in reason_result.all():
            code = normalize_stock_code(code_raw)
            profile = profiles.get(code)
            if not profile:
                continue
            themes = ThsLimitUpClassificationService.extract_fine_themes(reason or "", limit=6)
            if not themes and category:
                themes = [clean_text(category, limit=40)]
            for theme in themes:
                add_or_merge_topic(
                    profile,
                    theme,
                    source="local_ths_limit_up_reason",
                    confidence=0.72,
                    evidence=f"历史同花顺涨停原因：{clean_text(reason, limit=120)}",
                )
                stats["db_reason_topic_hits"] += 1
    return profiles, stats


async def load_akshare_universe(profiles: Dict[str, StockProfile]) -> Counter[str]:
    """Merge current A-share code/name universe without deriving any topics."""
    stats: Counter[str] = Counter()

    def fetch_rows() -> List[tuple[str, str]]:
        import akshare as ak

        stock_df = ak.stock_info_a_code_name()
        rows = stock_df.to_dict("records")
        return [
            (normalize_stock_code(row.get("code")), clean_text(row.get("name"), limit=50))
            for row in rows
            if normalize_stock_code(row.get("code"))
        ]

    try:
        rows = await asyncio.to_thread(fetch_rows)
    except Exception as exc:
        stats["akshare_error_count"] = 1
        print(f"akshare universe load skipped: {exc}")
        return stats

    before = len(profiles)
    for code, name in rows:
        profile = profiles.setdefault(
            code,
            StockProfile(
                stock_code=code,
                stock_name=name or code,
                market=infer_market(code),
            ),
        )
        if name and (not profile.stock_name or profile.stock_name == code):
            profile.stock_name = name
        if not profile.market:
            profile.market = infer_market(code)

    stats["akshare_stock_count"] = len(rows)
    stats["akshare_new_stock_count"] = max(0, len(profiles) - before)
    return stats


def load_existing_records(path: Path) -> Dict[str, Any]:
    service = LocalTopicKnowledgeService(knowledge_path=path)
    return service._load_records()


def load_seed_profiles(profiles: Dict[str, StockProfile], seed_glob: str) -> Counter[str]:
    stats: Counter[str] = Counter()
    for path in sorted(Path().glob(seed_glob) if not Path(seed_glob).is_absolute() else Path(seed_glob).parent.glob(Path(seed_glob).name)):
        before = len(profiles)
        rows = 0
        for payload in iter_jsonl_gz(path):
            rows += apply_seed_payload(profiles, payload)
        stats["seed_files"] += 1
        stats["seed_rows"] += rows
        stats["seed_new_stock_count"] += max(0, len(profiles) - before)
    return stats


def topic_to_json(topic: TopicCandidate) -> Dict[str, Any]:
    payload = {
        "theme": topic.theme,
        "source": topic.source,
        "confidence": round(max(0.0, min(1.0, topic.confidence + min(topic.hits - 1, 3) * 0.03)), 2),
        "evidence": topic.evidence,
    }
    if topic.aliases:
        payload["aliases"] = topic.aliases[:8]
    if topic.hits > 1:
        payload["hits"] = topic.hits
    return payload


def build_payload(profiles: Dict[str, StockProfile], stats: Counter[str]) -> Dict[str, Any]:
    stock_payload: Dict[str, Any] = {}
    topic_stock_count = 0
    total_topics = 0
    for code, profile in sorted(profiles.items()):
        topics = sorted(profile.topics.values(), key=lambda item: (-item.confidence, item.theme))[:8]
        if topics:
            topic_stock_count += 1
            total_topics += len(topics)
        record: Dict[str, Any] = {
            "stock_name": profile.stock_name,
            "market": profile.market or infer_market(code),
            "topics": [topic_to_json(topic) for topic in topics],
        }
        if profile.industry:
            record["industry"] = profile.industry
        if profile.concept:
            record["concept"] = profile.concept[:12]
        stock_payload[code] = record

    return {
        "version": 2,
        "generated_by": "local_codex",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "description": "本地 Codex 维护的股票隐藏题材候选库。仅作为当日涨停队列题材挖掘候选，不直接覆盖同花顺涨停原因。",
        "universe": {
            "stock_count": len(stock_payload),
            "topic_stock_count": topic_stock_count,
            "empty_topic_stock_count": len(stock_payload) - topic_stock_count,
            "topic_count": total_topics,
            **{key: int(value) for key, value in sorted(stats.items())},
        },
        "stocks": stock_payload,
    }


async def generate_knowledge(
    *,
    output_path: Path = DEFAULT_OUTPUT_PATH,
    seed_glob: str = DEFAULT_SEED_GLOB,
    include_seed: bool = True,
    include_akshare: bool = True,
) -> Dict[str, Any]:
    profiles, stats = await load_db_profiles()

    existing = load_existing_records(output_path)
    for code, raw_record in existing.items():
        if not has_manual_topics(raw_record):
            continue
        profile = profiles.setdefault(
            code,
            StockProfile(stock_code=code, stock_name=str(raw_record.get("stock_name") or code), market=infer_market(code)),
        )
        merge_manual_record(profile, raw_record)
        stats["preserved_existing_stock_count"] += 1

    if include_seed:
        stats.update(load_seed_profiles(profiles, seed_glob))
    if include_akshare:
        stats.update(await load_akshare_universe(profiles))

    return build_payload(profiles, stats)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate local A-share stock topic knowledge JSON.")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH), help="Output JSON path.")
    parser.add_argument("--seed-glob", default=DEFAULT_SEED_GLOB, help="Local seed jsonl.gz glob for all-A stock universe.")
    parser.add_argument("--no-seed", action="store_true", help="Use only local database stocks and historical THS reasons.")
    parser.add_argument("--no-akshare", action="store_true", help="Do not use akshare to fill the current A-share code/name universe.")
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    output_path = Path(args.output)
    payload = await generate_knowledge(
        output_path=output_path,
        seed_glob=args.seed_glob,
        include_seed=not args.no_seed,
        include_akshare=not args.no_akshare,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    universe = payload["universe"]
    print(
        "generated local topic knowledge:",
        f"stocks={universe['stock_count']}",
        f"topic_stocks={universe['topic_stock_count']}",
        f"topics={universe['topic_count']}",
        f"output={output_path}",
    )


if __name__ == "__main__":
    asyncio.run(main())
