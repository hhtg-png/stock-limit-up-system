"""Durable daily opening percentages, collected without blocking monitor requests."""
import asyncio
import json
import logging
import math
import os
import tempfile
import threading
import time
from datetime import date, datetime, time as wall_time
from pathlib import Path
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)
_ROOT_LOCKS, _ROOT_LOCKS_GUARD = {}, threading.Lock()


class OpeningAuctionArchiveService:
    def __init__(self, root_dir="data/opening-auctions", universe_loader=None,
                 quote_fetcher=None, today_provider=None, clock=time.monotonic, retry_delay=30):
        self.root_dir = Path(root_dir)
        with _ROOT_LOCKS_GUARD:
            self._write_lock = _ROOT_LOCKS.setdefault(self.root_dir.resolve(), threading.Lock())
        self.universe_loader, self.quote_fetcher = universe_loader, quote_fetcher
        self.today_provider = today_provider or (lambda: datetime.now(ZoneInfo("Asia/Shanghai")).date())
        self.clock, self.retry_delay = clock, retry_delay
        self._lock = asyncio.Lock()
        self._tasks, self._retry_at, self._captured = {}, {}, set()

    @staticmethod
    def _finite_values(values):
        result = {}
        for code, value in values.items():
            if not isinstance(code, str) or not code or isinstance(value, bool):
                continue
            try:
                number = float(value)
                if math.isfinite(number):
                    result[code] = number
            except (TypeError, ValueError, OverflowError):
                pass
        return result

    def _read(self, day):
        try:
            payload = json.loads((self.root_dir / f"{day.isoformat()}.json").read_text(encoding="utf-8"))
        except FileNotFoundError:
            return {}
        if not isinstance(payload, dict):
            raise ValueError("Invalid opening auction archive")
        return self._finite_values(payload)

    async def get_percentages(self, day: date) -> dict[str, float]:
        async with self._lock:
            try:
                return await asyncio.to_thread(self._read, day)
            except (OSError, ValueError):
                logger.exception("Unable to read opening auction archive for %s", day)
                return {}

    def _save(self, day, values):
        # A cancelled async caller leaves its worker running; lock the whole merge in that worker.
        with self._write_lock:
            previous = self._read(day)
            merged = {**previous, **values}
            if merged == previous:
                return
            self.root_dir.mkdir(parents=True, exist_ok=True)
            target = self.root_dir / f"{day.isoformat()}.json"
            descriptor, temporary = tempfile.mkstemp(prefix=f".{day.isoformat()}-", suffix=".tmp", dir=self.root_dir)
            try:
                with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                    json.dump(merged, stream, allow_nan=False, sort_keys=True)
                os.replace(temporary, target)
            finally:
                Path(temporary).unlink(missing_ok=True)

    async def save_percentages(self, day: date, values: dict[str, float]) -> None:
        valid = self._finite_values(values)
        if valid:
            async with self._lock:
                await asyncio.to_thread(self._save, day, valid)

    def _quote_percentages(self, day, quotes):
        values = {}
        if day != self.today_provider():
            return values
        for code, quote in quotes.items():
            if not isinstance(quote, dict) or set(quote.get("_missing_fields") or []) & {"open", "pre_close"}:
                continue
            try:
                stamp = datetime.strptime(str(quote.get("datetime") or ""), "%Y%m%d%H%M%S")
                opened, reference = float(quote["open"]), float(quote["pre_close"])
                if (stamp.date() == day and stamp.time() >= wall_time(9, 25)
                        and math.isfinite(opened) and math.isfinite(reference) and opened > 0 and reference > 0):
                    values[code] = (opened / reference - 1) * 100
            except (KeyError, TypeError, ValueError, OverflowError):
                continue
        return self._finite_values(values)

    async def record_quotes(self, day: date, quotes: dict) -> None:
        await self.save_percentages(day, self._quote_percentages(day, quotes))

    def request_full_capture(self, day: date):
        if day != self.today_provider() or day in self._captured or self.clock() < self._retry_at.get(day, 0):
            return None
        if day not in self._tasks:
            self._tasks[day] = asyncio.create_task(self._run_capture(day))
        return self._tasks[day]

    async def _run_capture(self, day):
        try:
            if await self._capture_today(day):
                self._captured.add(day)
        except Exception:
            logger.exception("Opening auction capture failed for %s", day)
        finally:
            self._retry_at[day] = self.clock() + self.retry_delay
            self._tasks.pop(day, None)

    async def _capture_today(self, day):
        loader, fetcher = self.universe_loader, self.quote_fetcher
        if loader is None:
            from app.services.live_market_stats_service import _load_universe
            loader = _load_universe
        if fetcher is None:
            from app.data_collectors.tencent_api import tencent_api
            fetcher = tencent_api.get_quotes_batch
        codes = list(dict.fromkeys(await loader()))
        semaphore = asyncio.Semaphore(4)

        async def fetch(chunk):
            async with semaphore:
                return await fetcher(chunk)

        chunks = [codes[i:i + 80] for i in range(0, len(codes), 80)]
        results = await asyncio.gather(*(fetch(chunk) for chunk in chunks), return_exceptions=True)
        quotes, failed = {}, False
        for chunk, result in zip(chunks, results):
            if isinstance(result, dict):
                quotes.update(result)
            if not isinstance(result, dict) or not set(chunk).issubset(result):
                failed = True
                logger.warning("Incomplete opening auction batch for %s: %s", day,
                               result if isinstance(result, Exception) else "missing stocks")
        values = self._quote_percentages(day, quotes)
        await self.save_percentages(day, values)
        return bool(values) and not failed


opening_auction_archive_service = OpeningAuctionArchiveService()
