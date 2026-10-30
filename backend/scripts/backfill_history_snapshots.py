"""Run from backend: python -m scripts.backfill_history_snapshots --limit 30.

Only dates already represented in the business database are considered.
Existing complete results are reused; no historical records are deleted.
"""

import argparse
import asyncio
import json

from app.database import close_db, init_db
from app.services.history_preparation_service import history_preparation_service


async def backfill(limit: int):
    await init_db()
    try:
        dates = await history_preparation_service.available_dates()
        statistics_count = 0
        for day in dates:
            if await history_preparation_service.refresh_statistics(day):
                statistics_count += 1
        print(json.dumps({"statistics_dates": statistics_count, "available_dates": len(dates)}), flush=True)
        results = []
        for day in dates[:limit]:
            result = await history_preparation_service.prepare_for_date(day)
            results.append(result)
            print(json.dumps(result), flush=True)
        print(json.dumps({"prepared_dates": len(results),
                          "monitor_ready": sum(row["monitor_ready"] for row in results),
                          "report_ready": sum(row["report_ready"] for row in results)}), flush=True)
        return results
    finally:
        await close_db()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=30, help="Recent closed dates to prepare (1..250).")
    args = parser.parse_args()
    if not 1 <= args.limit <= 250:
        parser.error("--limit must be between 1 and 250")
    asyncio.run(backfill(args.limit))
