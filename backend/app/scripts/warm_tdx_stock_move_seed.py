"""Build a local Tongdaxin stock movement seed cache file.

This script is intended to run locally first, then the generated jsonl.gz file
can be uploaded and imported on the online server.
"""
from __future__ import annotations

import argparse
import asyncio
import gzip
import json
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

from sqlalchemy import func, select

from app.database import async_session_maker, init_db
from app.models.limit_up import LimitUpRecord
from app.models.stock import Stock
from app.services.tdx_plugin_service import tdx_plugin_service


def parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return date.fromisoformat(value[:10])


async def load_stock_codes(limit: int | None = None) -> list[tuple[str, str]]:
    async with async_session_maker() as session:
        query = select(Stock.stock_code, Stock.stock_name).order_by(Stock.stock_code)
        if limit:
            query = query.limit(limit)
        result = await session.execute(query)
        return [(str(code), str(name or code)) for code, name in result.all()]


async def resolve_cache_trade_date(trade_date: date | None) -> date:
    if trade_date is not None:
        return trade_date

    async with async_session_maker() as session:
        result = await session.execute(select(func.max(LimitUpRecord.trade_date)))
        latest_trade_date = result.scalar_one_or_none()
    return latest_trade_date or date.today()


async def build_seed_record(stock_code: str, stock_name: str, trade_date: date | None, source_scope: str) -> dict:
    async with async_session_maker() as session:
        try:
            payload = await tdx_plugin_service.get_stock_move(
                stock_code,
                trade_date,
                source_scope=source_scope,
                db=session,
                force_refresh=True,
            )
            success = _payload_has_analysis(payload)
            cache_trade_date = trade_date or _payload_cache_trade_date(payload)
            movement_trade_date = _payload_trade_date(payload, cache_trade_date)
            return {
                "stock_code": stock_code,
                "stock_name": stock_name,
                "source_scope": source_scope,
                "trade_date": cache_trade_date.isoformat(),
                "cache_trade_date": cache_trade_date.isoformat(),
                "movement_trade_date": movement_trade_date,
                "generated_at": payload.get("updated_at") or datetime.now().isoformat(),
                "success": success,
                "payload": payload if success else {"items": []},
                "error": "" if success else "empty",
            }
        except Exception as exc:
            return {
                "stock_code": stock_code,
                "stock_name": stock_name,
                "source_scope": source_scope,
                "trade_date": trade_date.isoformat() if trade_date else date.today().isoformat(),
                "cache_trade_date": trade_date.isoformat() if trade_date else date.today().isoformat(),
                "movement_trade_date": "",
                "generated_at": datetime.now().isoformat(),
                "success": False,
                "payload": {"items": []},
                "error": str(exc),
            }


async def iter_seed_records(
    stocks: Iterable[tuple[str, str]],
    *,
    trade_date: date | None,
    source_scope: str,
    concurrency: int,
):
    semaphore = asyncio.Semaphore(max(1, concurrency))

    async def run_one(stock: tuple[str, str]):
        async with semaphore:
            return await build_seed_record(stock[0], stock[1], trade_date, source_scope)

    tasks = [asyncio.create_task(run_one(stock)) for stock in stocks]
    for task in asyncio.as_completed(tasks):
        yield await task


def _payload_has_analysis(payload: dict) -> bool:
    for item in payload.get("items") or []:
        for reason in item.get("reasons") or []:
            title = str(reason.get("title") or "").strip()
            content = str(reason.get("content") or "").strip()
            if title and title != "暂无异动原因" and content:
                return True
    return False


def _payload_trade_date(payload: dict, fallback: date | None) -> str:
    for item in payload.get("items") or []:
        value = str(item.get("trade_date") or "").strip()
        if value:
            return value[:10]
    return (fallback or date.today()).isoformat()


def _payload_cache_trade_date(payload: dict) -> date:
    value = str(payload.get("updated_at") or "").strip()
    if value:
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            pass
    return date.today()


async def warm_seed_file(
    output: Path,
    *,
    trade_date: date | None,
    source_scope: str,
    concurrency: int,
    limit: int | None,
) -> dict:
    await init_db()
    cache_trade_date = await resolve_cache_trade_date(trade_date)
    stocks = await load_stock_codes(limit)
    output.parent.mkdir(parents=True, exist_ok=True)
    stats = {
        "total": len(stocks),
        "success": 0,
        "failed": 0,
        "trade_date": cache_trade_date.isoformat(),
        "output": str(output),
    }
    with gzip.open(output, "wt", encoding="utf-8") as handle:
        async for record in iter_seed_records(
            stocks,
            trade_date=cache_trade_date,
            source_scope=source_scope,
            concurrency=concurrency,
        ):
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
            if record.get("success"):
                stats["success"] += 1
            else:
                stats["failed"] += 1
    return stats


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("data/tdx_stock_move_seed.jsonl.gz"))
    parser.add_argument("--trade-date", default=None)
    parser.add_argument("--source-scope", default="mixed", choices=["mixed", "ths"])
    parser.add_argument("--concurrency", type=int, default=3)
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()
    stats = asyncio.run(
        warm_seed_file(
            args.output,
            trade_date=parse_date(args.trade_date),
            source_scope=args.source_scope,
            concurrency=args.concurrency,
            limit=args.limit,
        )
    )
    print(json.dumps(stats, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
