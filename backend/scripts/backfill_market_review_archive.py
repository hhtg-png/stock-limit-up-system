"""Recover historical review data; run from the backend directory.

Without --execute, SQLite is opened in read-only mode. Optional evidence files
contain {YYYY-MM-DD: turnover_in_yi} and {YYYY-MM-DD: {stock_code: quote_fields}}.
Quote amounts are in 万元 and the quote fields use the review API's names.
An exported monitor source is used when the database lacks records for the day.
"""

import argparse
import asyncio
import json
import sys
from dataclasses import asdict
from datetime import date
from pathlib import Path

from sqlalchemy.engine import URL, make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from app.config import settings
from app.services.market_review_archive_backfill_service import backfill_market_review_archive
from scripts.backfill_market_review import _get_cn_trading_dates


def _iso_date(value: str) -> date:
    day = date.fromisoformat(value)
    if day.isoformat() != value:
        raise ValueError(f"Expected YYYY-MM-DD, got {value!r}")
    return day


def _read_json_object(path: Path) -> dict:
    content = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(content, dict):
        raise ValueError(f"Expected a JSON object in {path}")
    return content


def _read_evidence(path: Path | None) -> dict:
    if path is None:
        return {}
    return {_iso_date(day): value for day, value in _read_json_object(path).items()}


def _database_url(configured_url: str, execute: bool) -> URL:
    url = make_url(configured_url)
    if url.get_backend_name() != "sqlite" or not url.database or url.database == ":memory:":
        raise ValueError("Backfill requires an existing SQLite database file")
    path = Path(url.database).resolve(strict=True)
    if not path.is_file():
        raise ValueError(f"Expected an existing SQLite database file: {path}")
    return URL.create(
        "sqlite+aiosqlite",
        database=path.as_uri(),
        query={"mode": "rw" if execute else "ro", "uri": "true"},
    )


async def _run(args: argparse.Namespace) -> None:
    turnover = _read_evidence(args.turnover_file)
    quotes = _read_evidence(args.quotes_file)
    monitor_source = _read_json_object(args.monitor_source_file) if args.monitor_source_file else None
    engine = create_async_engine(_database_url(settings.DATABASE_URL, args.execute), connect_args={"timeout": 30})
    try:
        rows = await backfill_market_review_archive(
            args.start,
            args.end,
            session_factory=async_sessionmaker(engine, expire_on_commit=False),
            calendar_loader=_get_cn_trading_dates,
            execute=args.execute,
            turnover_by_date=turnover,
            quotes_by_date=quotes,
            monitor_source=monitor_source,
            include_turnover_only=args.include_turnover_only,
        )
    finally:
        await engine.dispose()
    actionable = [row for row in rows if row.status in {"would_insert", "inserted"}]
    print(json.dumps({
        "mode": "execute" if args.execute else "dry-run",
        "trading_days_checked": len(rows),
        "recoverable_days": len(actionable),
        "recoverable_stocks": sum(row.stock_count for row in actionable),
        "days": [{**asdict(row), "trade_date": row.trade_date.isoformat()} for row in rows],
    }, ensure_ascii=False))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=_iso_date, required=True, help="first date (YYYY-MM-DD)")
    parser.add_argument("--end", type=_iso_date, required=True, help="last date (YYYY-MM-DD, before today)")
    parser.add_argument("--turnover-file", type=Path, help="verified exchange turnover by date (亿元)")
    parser.add_argument("--quotes-file", type=Path, help="verified unadjusted daily quote fields by date and stock")
    parser.add_argument("--monitor-source-file", type=Path, help="verified export of historical monitor rows")
    parser.add_argument("--include-turnover-only", action="store_true", help="archive exchange turnover when stock records are missing")
    parser.add_argument("--execute", action="store_true", help="insert missing archive rows after dry-run review")
    args = parser.parse_args()
    try:
        asyncio.run(_run(args))
    except (OSError, ValueError) as exc:
        parser.exit(status=1, message=f"{exc}\n")


if __name__ == "__main__":
    main()
