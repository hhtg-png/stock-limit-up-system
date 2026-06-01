"""Import Tongdaxin stock movement seed cache records."""
from __future__ import annotations

import argparse
import asyncio
import gzip
import json
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import async_session_maker, init_db
from app.models.tdx_cache import TdxStockMoveCache


def normalize_code(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits[-6:].zfill(6) if digits else ""


def parse_date(value: Any) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    text = str(value or "").strip()
    if text:
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            pass
    return datetime.now()


def payload_has_analysis(payload: dict) -> bool:
    for item in payload.get("items") or []:
        for reason in item.get("reasons") or []:
            title = str(reason.get("title") or "").strip()
            content = str(reason.get("content") or "").strip()
            if title and title != "暂无异动原因" and content:
                return True
    return False


def iter_seed_file(path: Path):
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


async def import_seed_records(records: Iterable[dict], session: AsyncSession) -> dict:
    stats = {"imported": 0, "updated": 0, "skipped": 0, "kept_newer": 0}

    for record in records:
        if record.get("success") is False:
            stats["skipped"] += 1
            continue

        payload = record.get("payload") or {}
        stock_code = normalize_code(record.get("stock_code") or payload.get("stock_code"))
        source_scope = str(record.get("source_scope") or "mixed")
        trade_date = parse_date(
            record.get("cache_trade_date")
            or record.get("target_trade_date")
            or record.get("trade_date")
        )
        generated_at = parse_datetime(record.get("generated_at") or payload.get("updated_at"))
        if not stock_code or trade_date is None or not payload_has_analysis(payload):
            stats["skipped"] += 1
            continue

        result = await session.execute(
            select(TdxStockMoveCache)
            .where(TdxStockMoveCache.stock_code == stock_code)
            .where(TdxStockMoveCache.source_scope == source_scope)
            .where(TdxStockMoveCache.trade_date == trade_date)
        )
        existing = result.scalar_one_or_none()
        if existing and existing.generated_at and existing.generated_at > generated_at:
            stats["kept_newer"] += 1
            continue

        stock_name = str(record.get("stock_name") or _payload_stock_name(payload) or stock_code)[:50]
        source_status = payload.get("source_status") or {}
        warnings = payload.get("warnings") or []
        if existing is None:
            session.add(
                TdxStockMoveCache(
                    stock_code=stock_code,
                    source_scope=source_scope,
                    trade_date=trade_date,
                    stock_name=stock_name,
                    payload_json=payload,
                    source_status=source_status,
                    warnings=warnings,
                    generated_at=generated_at,
                )
            )
            stats["imported"] += 1
        else:
            existing.stock_name = stock_name
            existing.payload_json = payload
            existing.source_status = source_status
            existing.warnings = warnings
            existing.generated_at = generated_at
            existing.updated_at = datetime.now()
            stats["updated"] += 1

    await session.commit()
    return stats


def _payload_stock_name(payload: dict) -> str:
    for item in payload.get("items") or []:
        name = str(item.get("stock_name") or "").strip()
        if name:
            return name
    return ""


async def import_seed_file(path: Path) -> dict:
    await init_db()
    async with async_session_maker() as session:
        return await import_seed_records(iter_seed_file(path), session)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True, type=Path, help="jsonl or jsonl.gz seed file")
    args = parser.parse_args()
    stats = asyncio.run(import_seed_file(args.file))
    print(json.dumps(stats, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
