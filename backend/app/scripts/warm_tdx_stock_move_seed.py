"""Build a local Tongdaxin stock movement seed cache file.

This script is intended to run locally first, then the generated jsonl.gz file
can be uploaded and imported on the online server.
"""
from __future__ import annotations

import argparse
import asyncio
import gzip
import json
import shutil
import subprocess
import time
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

from sqlalchemy import func, select

from app.database import async_session_maker, init_db
from app.models.limit_up import LimitUpRecord
from app.models.stock import Stock
from app.services.tdx_external_sources import (
    DabankeStockMoveProvider,
    ExternalStockMove,
    LwwhyStockMoveProvider,
)
from app.services.tdx_plugin_service import tdx_plugin_service


_seed_lwwhy_provider = LwwhyStockMoveProvider(
    timeout=2.5,
    cache_ttl=3600,
)
_seed_dabanke_provider = DabankeStockMoveProvider(
    timeout=2.0,
    cache_ttl=3600,
)
_SEED_LWWHY_PREFER_TIMEOUT = 0.8


def parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return date.fromisoformat(value[:10])


def normalize_stock_code(value: object) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits[-6:].zfill(6) if digits else ""


def merge_stock_code_lists(*stock_lists: Iterable[tuple[str, str]]) -> list[tuple[str, str]]:
    merged: list[tuple[str, str]] = []
    seen: set[str] = set()
    for stock_list in stock_lists:
        for raw_code, raw_name in stock_list:
            code = normalize_stock_code(raw_code)
            if not code or code in seen:
                continue
            name = str(raw_name or code).strip() or code
            merged.append((code, name))
            seen.add(code)
    return merged


async def load_local_stock_codes(limit: int | None = None) -> list[tuple[str, str]]:
    async with async_session_maker() as session:
        query = select(Stock.stock_code, Stock.stock_name).order_by(Stock.stock_code)
        if limit:
            query = query.limit(limit)
        result = await session.execute(query)
        return [(str(code), str(name or code)) for code, name in result.all()]


async def load_akshare_stock_codes() -> list[tuple[str, str]]:
    def fetch_codes() -> list[tuple[str, str]]:
        import akshare as ak

        stock_df = ak.stock_info_a_code_name()
        rows = stock_df.to_dict("records")
        return [
            (normalize_stock_code(row.get("code")), str(row.get("name") or "").strip())
            for row in rows
            if normalize_stock_code(row.get("code"))
        ]

    return await asyncio.to_thread(fetch_codes)


async def load_stock_codes(
    limit: int | None = None,
    universe: str = "merged",
    offset: int = 0,
) -> list[tuple[str, str]]:
    local_codes: list[tuple[str, str]] = []
    akshare_codes: list[tuple[str, str]] = []

    if universe in {"local", "merged"}:
        local_codes = await load_local_stock_codes()

    if universe in {"all-a", "merged"}:
        try:
            akshare_codes = await load_akshare_stock_codes()
        except Exception:
            if universe == "all-a":
                local_codes = await load_local_stock_codes()

    if universe == "local":
        stocks = merge_stock_code_lists(local_codes)
    elif universe == "all-a":
        stocks = merge_stock_code_lists(akshare_codes, local_codes)
    else:
        stocks = merge_stock_code_lists(akshare_codes, local_codes)

    stocks.sort(key=lambda item: item[0])
    sliced = stocks[max(0, offset):]
    return sliced[:limit] if limit else sliced


async def resolve_cache_trade_date(trade_date: date | None) -> date:
    if trade_date is not None:
        return trade_date

    async with async_session_maker() as session:
        result = await session.execute(select(func.max(LimitUpRecord.trade_date)))
        latest_trade_date = result.scalar_one_or_none()
    return latest_trade_date or date.today()


async def load_limit_up_item_map(trade_date: date) -> dict[str, dict]:
    try:
        items = await tdx_plugin_service.realtime_limit_up_service.get_realtime_limit_up_list(trade_date)
    except Exception:
        return {}
    return {
        normalize_stock_code(item.get("stock_code")): item
        for item in items
        if normalize_stock_code(item.get("stock_code"))
    }


async def load_seed_external_stock_move(stock_code: str, trade_date: date) -> ExternalStockMove | None:
    normalized_code = normalize_stock_code(stock_code)
    dabanke_result = await load_seed_dabanke_stock_move(normalized_code, trade_date)
    if isinstance(dabanke_result, ExternalStockMove):
        return dabanke_result

    try:
        return await asyncio.wait_for(
            _seed_lwwhy_provider.get_stock_move(normalized_code, None),
            timeout=_SEED_LWWHY_PREFER_TIMEOUT,
        )
    except (asyncio.TimeoutError, Exception):
        return None


async def load_seed_dabanke_stock_move(stock_code: str, trade_date: date) -> ExternalStockMove | None:
    normalized_code = normalize_stock_code(stock_code)
    cached = _seed_dabanke_provider._stock_cache.get(normalized_code)
    if cached and time.time() - cached[0] < _seed_dabanke_provider.cache_ttl:
        return cached[1]

    dabanke_url = f"{_seed_dabanke_provider.base_url}/gupiao-{normalized_code}.html"
    try:
        dabanke_html = await asyncio.to_thread(_fetch_seed_dabanke_html, dabanke_url)
        move = _seed_dabanke_provider.parse_stock_history_html(dabanke_html, normalized_code, "")
        if move:
            move.source_url = dabanke_url
        _seed_dabanke_provider._stock_cache[normalized_code] = (time.time(), move)
        return move
    except Exception:
        _seed_dabanke_provider._stock_cache[normalized_code] = (time.time(), None)
        return None


def _fetch_seed_dabanke_html(url: str) -> str:
    curl_path = shutil.which("curl.exe") or shutil.which("curl")
    if curl_path:
        command = [
            curl_path,
            "-L",
            "--max-time",
            "8",
            "-sS",
            "-A",
            _seed_dabanke_provider._headers()["User-Agent"],
            url,
        ]
        completed = subprocess.run(command, capture_output=True, timeout=10)
        if completed.returncode != 0:
            raise RuntimeError(completed.stderr.decode("utf-8", errors="ignore") or f"curl exited {completed.returncode}")
        return completed.stdout.decode("utf-8", errors="ignore")

    import requests

    response = requests.get(
        url,
        headers=_seed_dabanke_provider._headers(),
        timeout=(2.0, 6.0),
        verify=False,
        allow_redirects=True,
    )
    response.raise_for_status()
    return response.text


async def build_seed_record(
    stock_code: str,
    stock_name: str,
    trade_date: date | None,
    source_scope: str,
    limit_up_items: dict[str, dict] | None = None,
) -> dict:
    cache_trade_date = trade_date or date.today()
    normalized_code = normalize_stock_code(stock_code)

    try:
        warnings: list[str] = []
        source_status = {
            "stock_move": "ok",
            "stock_move_live": "preloaded" if limit_up_items is not None else "skipped",
        }
        external_move = (
            await load_seed_external_stock_move(normalized_code, cache_trade_date)
            if source_scope != "ths"
            else None
        )
        source_status["seed_external"] = "ok" if external_move else "empty"
        limit_up_item = (limit_up_items or {}).get(normalized_code)

        if not limit_up_item and not external_move:
            source_status["stock_move"] = "empty"
            warnings.append(f"{normalized_code} 暂无异动解析数据")
            payload = tdx_plugin_service._plugin_payload(
                [tdx_plugin_service._empty_stock_move(normalized_code, source_scope)],
                cache_trade_date,
                source_status,
                is_cache=False,
                warnings=warnings,
            )
        elif not limit_up_item and external_move:
            payload = tdx_plugin_service._plugin_payload(
                [
                    tdx_plugin_service._build_stock_move_from_external(
                        external_move,
                        normalized_code,
                        source_scope,
                        cache_trade_date,
                    )
                ],
                external_move.trade_date or cache_trade_date,
                source_status,
                is_cache=False,
                warnings=warnings,
            )
        else:
            item = tdx_plugin_service._build_stock_move_item(
                limit_up_item,
                normalized_code,
                source_scope,
                cache_trade_date,
                external_move=external_move,
            )
            payload = tdx_plugin_service._plugin_payload(
                [item],
                cache_trade_date,
                source_status,
                is_cache=False,
                warnings=warnings,
            )

        success = _payload_has_analysis(payload)
        movement_trade_date = _payload_trade_date(payload, cache_trade_date)
        return {
            "stock_code": normalized_code,
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
            "stock_code": normalized_code,
            "stock_name": stock_name,
            "source_scope": source_scope,
            "trade_date": cache_trade_date.isoformat(),
            "cache_trade_date": cache_trade_date.isoformat(),
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
    limit_up_items: dict[str, dict] | None = None,
):
    semaphore = asyncio.Semaphore(max(1, concurrency))

    async def run_one(stock: tuple[str, str]):
        async with semaphore:
            return await build_seed_record(stock[0], stock[1], trade_date, source_scope, limit_up_items)

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
    universe: str,
    offset: int,
) -> dict:
    await init_db()
    cache_trade_date = await resolve_cache_trade_date(trade_date)
    stocks = await load_stock_codes(limit, universe=universe, offset=offset)
    limit_up_items = await load_limit_up_item_map(cache_trade_date)
    output.parent.mkdir(parents=True, exist_ok=True)
    stats = {
        "total": len(stocks),
        "success": 0,
        "failed": 0,
        "trade_date": cache_trade_date.isoformat(),
        "universe": universe,
        "offset": offset,
        "limit_up_items": len(limit_up_items),
        "output": str(output),
    }
    with gzip.open(output, "wt", encoding="utf-8") as handle:
        processed = 0
        async for record in iter_seed_records(
            stocks,
            trade_date=cache_trade_date,
            source_scope=source_scope,
            concurrency=concurrency,
            limit_up_items=limit_up_items,
        ):
            processed += 1
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
            if record.get("success"):
                stats["success"] += 1
            else:
                stats["failed"] += 1
            if processed % 50 == 0:
                handle.flush()
                print(
                    json.dumps(
                        {
                            "processed": processed,
                            "success": stats["success"],
                            "failed": stats["failed"],
                            "total": stats["total"],
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    flush=True,
                )
        handle.flush()
    return stats


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("data/tdx_stock_move_seed.jsonl.gz"))
    parser.add_argument("--trade-date", default=None)
    parser.add_argument("--source-scope", default="mixed", choices=["mixed", "ths"])
    parser.add_argument("--concurrency", type=int, default=3)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument(
        "--universe",
        default="merged",
        choices=["merged", "all-a", "local"],
        help="stock universe: merged/all-a uses AkShare full A-share list, local uses the project stocks table",
    )
    args = parser.parse_args()
    stats = asyncio.run(
        warm_seed_file(
            args.output,
            trade_date=parse_date(args.trade_date),
            source_scope=args.source_scope,
            concurrency=args.concurrency,
            limit=args.limit,
            universe=args.universe,
            offset=args.offset,
        )
    )
    print(json.dumps(stats, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
