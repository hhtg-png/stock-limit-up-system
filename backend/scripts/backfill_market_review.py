"""
手动回补市场复盘数据
"""
import argparse
import asyncio
import sys
from datetime import datetime
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from app.services.market_review_pipeline_service import market_review_pipeline_service
from app.utils.time_utils import get_trading_dates


def parse_args():
    parser = argparse.ArgumentParser(description="Backfill market review data for a date range.")
    parser.add_argument("--start", required=True, help="Start date in YYYY-MM-DD format.")
    parser.add_argument("--end", required=True, help="End date in YYYY-MM-DD format.")
    parser.add_argument(
        "--calc-version",
        type=int,
        default=1,
        help="Calc version passed to market_review_pipeline_service.run_for_date().",
    )
    return parser.parse_args()


def parse_date(value: str):
    return datetime.strptime(value, "%Y-%m-%d").date()


async def backfill_market_review(start_date, end_date, calc_version: int):
    if end_date < start_date:
        raise ValueError("end date must be greater than or equal to start date")

    trading_dates = get_trading_dates(start_date, end_date)
    if not trading_dates:
        print("No trading dates found in the supplied range.")
        return

    print(
        f"Backfilling market review data from {start_date} to {end_date} "
        f"({len(trading_dates)} trading days), calc_version={calc_version}"
    )

    for trade_date in trading_dates:
        print(f"Processing {trade_date} ...")
        await market_review_pipeline_service.run_for_date(
            trade_date,
            calc_version=calc_version,
        )

    print("Backfill completed.")


def main():
    args = parse_args()
    start_date = parse_date(args.start)
    end_date = parse_date(args.end)
    asyncio.run(backfill_market_review(start_date, end_date, args.calc_version))


if __name__ == "__main__":
    main()
