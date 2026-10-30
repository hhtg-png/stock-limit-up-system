"""Durable fixed-basket history using whole Tencent newfqkline qfq revisions.

Reads never fetch. Bounded workers share per-stock snapshots. Refreshes rebuild
the entire adjusted series, retaining old revisions and their exact baselines.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import math
import re
from datetime import date, time, timedelta
from itertools import count
from uuid import uuid4

import httpx
from loguru import logger
from sqlalchemy import or_, select, update
from sqlalchemy.exc import IntegrityError

from app.database import async_session_maker
from app.models.dynamic_group_history import (
    DynamicGroupHistoryJob, DynamicGroupHistoryRevision,
    DynamicGroupStockHistoryCache, DynamicGroupStockHistoryRevision,
)
from app.utils.time_utils import now_cn


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     ensure_ascii=True).encode()).hexdigest()


def _calendar(start, end):
    from app.data_collectors.scheduler import _get_cn_trading_dates
    return _get_cn_trading_dates(start, end)


class DynamicGroupHistoryService:
    def __init__(self, *, session_factory=async_session_maker, history_fetcher=None,
                 http_client=None, now_provider=now_cn, concurrency=4,
                 workers=2, retry_delay=0.3, refresh_seconds=3600, calendar_loader=None):
        self.session_factory = session_factory
        self.now_provider = now_provider
        self.history_fetcher = history_fetcher or self._fetch_history
        self.calendar_loader = calendar_loader or _calendar
        self._calendar_cache = None
        self._calendar_lock = asyncio.Lock()
        self._client = http_client
        self._owns_client = http_client is None
        self._provider_limit = asyncio.Semaphore(max(1, int(concurrency)))
        self._worker_count = max(1, int(workers))
        self.retry_delay = max(0, float(retry_delay))
        self.refresh_seconds = max(1, float(refresh_seconds))
        self._queue = asyncio.PriorityQueue()
        self._queue_sequence = count()
        self._queued = set()
        self._workers = []
        self._register_tasks = set()
        self._stock_tasks = {}
        self._locks = {}
        self._register_lock = asyncio.Lock()
        self._closed = False

    def _now(self):
        return self.now_provider().replace(tzinfo=None)

    @staticmethod
    def _codes(items):
        codes = sorted({str(item.get("stock_code") or "") for item in items})
        if any(not re.fullmatch(r"\d{6}", code) for code in codes):
            raise ValueError("History requires six-digit stock codes")
        return codes

    def request_build(self, selection_id, items, refresh=False):
        """Schedule registration; return before external price work."""
        if self._closed:
            return
        codes = self._codes(items)
        task = asyncio.create_task(self._register_and_queue(int(selection_id), codes, refresh))
        self._register_tasks.add(task)
        task.add_done_callback(self._registered)

    def _registered(self, task):
        self._register_tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            logger.warning("Dynamic history registration failed: {}", task.exception())

    async def _register_and_queue(self, selection_id, codes, refresh):
        await self._register(selection_id, codes)
        self._enqueue(selection_id, refresh)

    async def _register(self, selection_id, codes):
        signature = _digest(codes)
        async with self._register_lock:
            async with self.session_factory() as session:
                job = await session.get(DynamicGroupHistoryJob, selection_id)
                if job is not None:
                    if job.signature != signature:
                        raise ValueError("Cannot replace a frozen selection basket")
                    return
                session.add(DynamicGroupHistoryJob(selection_id=selection_id, signature=signature,
                    stock_codes=codes, status="pending", attempts=0, next_run_at=self._now(),
                    updated_at=self._now()))
                try:
                    await session.commit()
                except IntegrityError:
                    await session.rollback()
                    job = await session.get(DynamicGroupHistoryJob, selection_id)
                    if job is None or job.signature != signature:
                        raise ValueError("Cannot replace a frozen selection basket")

    def _enqueue(self, selection_id, refresh=False, priority=0):
        if self._closed or selection_id in self._queued:
            return
        self._queued.add(selection_id)
        self._queue.put_nowait((priority, next(self._queue_sequence), selection_id, refresh))
        self._workers = [task for task in self._workers if not task.done()]
        while len(self._workers) < self._worker_count:
            self._workers.append(asyncio.create_task(self._worker()))

    async def _worker(self):
        while True:
            _, _, selection_id, refresh = await self._queue.get()
            try:
                async with self.session_factory() as session:
                    job = await session.get(DynamicGroupHistoryJob, selection_id)
                    codes = list(job.stock_codes) if job else None
                if codes is not None:
                    await self.ensure_series(selection_id,
                        [{"stock_code": code} for code in codes], refresh=refresh)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Dynamic selection history {} failed: {}", selection_id, exc)
            finally:
                self._queued.discard(selection_id)
                self._queue.task_done()

    async def wait_idle(self):
        """Drain currently queued work for controlled verification."""
        while self._register_tasks:
            await asyncio.gather(*list(self._register_tasks))
        await self._queue.join()

    async def tick(self, limit=16):
        """Recover durable work, fairly refreshing old batches before recent ones."""
        if self._closed:
            return
        # A process can stop after a selection commits but before request_build
        # registers its job. Repair from frozen archives, including archived groups.
        from app.models.dynamic_group import DynamicGroupSelection, DynamicGroupSelectionItem
        async with self.session_factory() as session:
            missing = list(await session.scalars(select(DynamicGroupSelection.id).where(
                ~select(DynamicGroupHistoryJob.selection_id).where(
                    DynamicGroupHistoryJob.selection_id == DynamicGroupSelection.id).exists())
                .order_by(DynamicGroupSelection.id).limit(10)))
            rows = (await session.scalars(select(DynamicGroupSelectionItem)
                .where(DynamicGroupSelectionItem.selection_id.in_(missing)))).all() if missing else []
            baskets = {selection_id: [] for selection_id in missing}
            for row in rows:
                baskets[row.selection_id].append({**row.payload, "stock_code": row.stock_code})
        for selection_id, items in baskets.items():
            await self._register(selection_id, self._codes(items))
        async with self.session_factory() as session:
            jobs = (await session.scalars(select(DynamicGroupHistoryJob)
                .where(DynamicGroupHistoryJob.next_run_at <= self._now())
                .order_by(DynamicGroupHistoryJob.last_attempt_at.asc(),
                          DynamicGroupHistoryJob.selection_id.asc())
                .limit(min(max(int(limit), 1), 100)))).all()
            ids = [job.selection_id for job in jobs]
        for selection_id in ids:
            self._enqueue(selection_id, refresh=False, priority=10)

    async def ensure_series(self, selection_id, items, refresh=False):
        """Persist valid history; failed refreshes retain successful revisions."""
        selection_id = int(selection_id)
        codes = self._codes(items)
        await self._register(selection_id, codes)
        lock = self._locks.setdefault(selection_id, asyncio.Lock())
        async with lock:
            owner = uuid4().hex
            async with self.session_factory() as session:
                job = await session.get(DynamicGroupHistoryJob, selection_id)
                previous = await session.get(DynamicGroupHistoryRevision, job.latest_revision_id) if job.latest_revision_id else None
                if not refresh and previous and job.next_run_at > self._now():
                    return await self.get_series(selection_id)
                claimed = await session.execute(update(DynamicGroupHistoryJob).where(
                    DynamicGroupHistoryJob.selection_id == selection_id,
                    or_(DynamicGroupHistoryJob.status != "building",
                        DynamicGroupHistoryJob.next_run_at <= self._now()))
                    .values(status="building", claim_owner=owner, last_attempt_at=self._now(),
                        updated_at=self._now(), attempts=DynamicGroupHistoryJob.attempts + 1,
                        next_run_at=self._now() + timedelta(minutes=5)))
                await session.commit()
                if claimed.rowcount != 1:
                    return await self.get_series(selection_id)
            try:
                revisions = []
                for offset in range(0, len(codes), 4):
                    revisions.extend(await asyncio.gather(*(
                        self._stock(code, refresh=refresh) for code in codes[offset:offset + 4])))
                    async with self.session_factory() as session:
                        renewed = await session.execute(update(DynamicGroupHistoryJob).where(
                            DynamicGroupHistoryJob.selection_id == selection_id,
                            DynamicGroupHistoryJob.claim_owner == owner)
                            .values(next_run_at=self._now() + timedelta(minutes=5)))
                        await session.commit()
                        if renewed.rowcount != 1:
                            return await self.get_series(selection_id)
                calendar = await self._calendar_dates(revisions) if revisions else []
                # A daily close is final only after the exchange session closes.
                # Calendar dates, rather than provider coverage, define the tail.
                today = self._now().date().isoformat()
                calendar = [day for day in calendar if day < today or
                    (day == today and self._now().time() >= time(15, 5))]
                await self._save(selection_id, self._compose(codes, revisions, previous, calendar), owner)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                async with self.session_factory() as session:
                    job = await session.get(DynamicGroupHistoryJob, selection_id)
                    if job.claim_owner != owner:
                        return await self.get_series(selection_id)
                    job.status = "stale" if job.latest_revision_id else "retrying"
                    job.last_error = str(exc)[:1000]
                    job.updated_at = self._now()
                    job.next_run_at = self._now() + timedelta(seconds=min(3600, 30 * 2 ** min(job.attempts, 6)))
                    await session.commit()
            return await self.get_series(selection_id)

    async def _calendar_dates(self, revisions):
        start = min(revision.requested_start for revision in revisions)
        end = self._now().date()
        async with self._calendar_lock:
            cached = self._calendar_cache
            if cached and cached[0] <= start and cached[1] >= end:
                return cached[2]
            days = await asyncio.wait_for(asyncio.to_thread(self.calendar_loader, start, end), timeout=15)
            normalized = sorted({str(day)[:10] for day in days})
            observed = {day for revision in revisions for day in revision.prices}
            if not normalized or not observed.issubset(set(normalized)):
                raise ValueError("交易日历不可验证完整历史行情日期；等待日历重试")
            self._calendar_cache = (start, end, normalized)
            return normalized

    async def _stock(self, code, *, refresh):
        task = self._stock_tasks.get(code)
        if task is None:
            task = asyncio.create_task(self._load_stock(code, refresh=refresh))
            self._stock_tasks[code] = task
        try:
            return await asyncio.shield(task)
        finally:
            if task.done() and self._stock_tasks.get(code) is task:
                self._stock_tasks.pop(code, None)

    async def _load_stock(self, code, *, refresh):
        async with self.session_factory() as session:
            cache = await session.get(DynamicGroupStockHistoryCache, code)
            old = await session.get(DynamicGroupStockHistoryRevision, cache.revision_id) if cache else None
            if old and not refresh and (self._now() - cache.refreshed_at).total_seconds() < self.refresh_seconds:
                return old
            start = old.requested_start if old else self._now().date() - timedelta(days=1096)
        payload = None
        for attempt in range(3):
            try:
                async with self._provider_limit:
                    raw = await asyncio.wait_for(self.history_fetcher(code, start, self._now().date()), timeout=25)
                payload = self._validate(raw, start, self._now().date())
                break
            except asyncio.CancelledError:
                raise
            except Exception:
                if attempt == 2:
                    raise
                await asyncio.sleep(self.retry_delay * (2 ** attempt))
        digest = _digest(payload)
        # Independent processes can cold-fetch a shared stock simultaneously.
        # Retry the whole transaction after a unique insert race; the provider
        # response remains valid and must not turn the selection into failure.
        for transaction_attempt in range(3):
            try:
                return await self._persist_stock(code, start, payload, digest)
            except IntegrityError:
                if transaction_attempt == 2:
                    raise
                await asyncio.sleep(0)

    async def _persist_stock(self, code, start, payload, digest):
        async with self.session_factory() as session:
            revision = await session.scalar(select(DynamicGroupStockHistoryRevision).where(
                DynamicGroupStockHistoryRevision.stock_code == code,
                DynamicGroupStockHistoryRevision.digest == digest))
            if revision is None:
                revision = DynamicGroupStockHistoryRevision(stock_code=code, digest=digest,
                    source=payload["source"], adjustment=payload["adjustment"],
                    prices=payload["prices"], requested_start=start, fetched_at=self._now())
                session.add(revision)
                await session.flush()
            cache = await session.get(DynamicGroupStockHistoryCache, code)
            if cache is None:
                session.add(DynamicGroupStockHistoryCache(stock_code=code, revision_id=revision.id,
                                                         refreshed_at=self._now()))
            else:
                cache.revision_id = revision.id
                cache.refreshed_at = self._now()
            await session.commit()
            return revision

    @staticmethod
    def _validate(raw, start, end):
        if (not isinstance(raw, dict) or raw.get("source") not in
                {"tencent_fqkline", "tencent_newfqkline"} or raw.get("adjustment") != "qfq"):
            raise ValueError("Expected one complete Tencent qfq price series; raw fallback forbidden")
        prices = {}
        for day, raw_price in (raw.get("prices") or {}).items():
            parsed = date.fromisoformat(str(day))
            if not start <= parsed <= end or parsed.weekday() > 4 or isinstance(raw_price, bool):
                raise ValueError("Invalid or future historical date/price")
            price = float(raw_price)
            if not math.isfinite(price) or price <= 0:
                raise ValueError("Invalid adjusted closing price")
            prices[parsed.isoformat()] = price
        if not prices:
            raise ValueError("Adjusted history is empty; retry required")
        return {"source": raw["source"], "adjustment": "qfq", "prices": dict(sorted(prices.items()))}

    @staticmethod
    def _compose(codes, revisions, previous, calendar):
        expected = len(codes)
        warnings = []
        if not expected:
            return {"base_date": None, "expected_count": 0, "points": [], "provenance": [],
                    "warnings": ["本批次没有成分股"], "status": "empty"}
        common = set(revisions[0].prices)
        dates = set(calendar)
        for revision in revisions:
            common.intersection_update(revision.prices)
        common.intersection_update(dates)
        if not common:
            raise ValueError("成分股没有共同有效的历史基准交易日")
        base = previous.base_date.isoformat() if previous and previous.base_date else min(common)
        if base not in common:
            raise ValueError("原历史基准日缺失，保留上一修订并等待完整复权序列")
        if len({min(revision.prices) for revision in revisions}) > 1:
            warnings.append("新股或历史覆盖差异缩短共同区间；起点采用全体成分共同有效交易日")
        provenance = [{"stock_code": revision.stock_code, "revision_id": revision.id,
            "source": revision.source, "adjustment": revision.adjustment,
            "digest": revision.digest, "base_price": revision.prices[base],
            "fetched_at": revision.fetched_at.isoformat()} for revision in revisions]
        points = []
        previous_value = None
        for day in sorted(day for day in dates if day >= base):
            covered = sum(day in revision.prices for revision in revisions)
            value = (round(1000 * sum(revision.prices[day] / revision.prices[base]
                                    for revision in revisions) / expected, 6)
                     if covered == expected else None)
            change = round((value / previous_value - 1) * 100, 6) if value is not None and previous_value else None
            points.append({"date": day, "value": value, "change_pct": change, "covered_count": covered})
            previous_value = value
        if previous:
            complete_dates = {point["date"] for point in points if point["value"] is not None}
            if any(point["value"] is not None and point["date"] not in complete_dates
                   for point in previous.points):
                raise ValueError("历史行情覆盖退步，保留上一完整修订并等待行情源重试")
        gaps = any(point["value"] is None for point in points)
        if gaps:
            warnings.append("缺失成分行情的交易日保留缺口；分母始终使用本批次全部成分")
        return {"base_date": base, "expected_count": expected, "points": points,
                "provenance": provenance, "warnings": warnings, "status": "partial" if gaps else "ready"}

    async def _save(self, selection_id, payload, owner):
        digest = _digest(payload)
        async with self.session_factory() as session:
            job = await session.get(DynamicGroupHistoryJob, selection_id)
            if job.claim_owner != owner:
                return
            revision = await session.scalar(select(DynamicGroupHistoryRevision).where(
                DynamicGroupHistoryRevision.selection_id == selection_id,
                DynamicGroupHistoryRevision.digest == digest))
            if revision is None:
                revision = DynamicGroupHistoryRevision(selection_id=selection_id, signature=job.signature,
                    digest=digest, base_date=date.fromisoformat(payload["base_date"]) if payload["base_date"] else None,
                    expected_count=payload["expected_count"], points=payload["points"],
                    provenance=payload["provenance"], warnings=payload["warnings"],
                    status=payload["status"], generated_at=self._now())
                session.add(revision)
                await session.flush()
            job.latest_revision_id = revision.id
            job.status = revision.status
            job.last_error = None
            job.attempts = 0
            job.updated_at = self._now()
            job.next_run_at = self._now() + timedelta(seconds=self.refresh_seconds)
            await session.commit()

    async def get_series(self, selection_id, window_days=250):
        """Read persisted output only. Cropping retains the original baseline."""
        async with self.session_factory() as session:
            job = await session.get(DynamicGroupHistoryJob, int(selection_id))
            revision = await session.get(DynamicGroupHistoryRevision, job.latest_revision_id) if job and job.latest_revision_id else None
            result = {"selection_id": int(selection_id), "status": job.status if job else "pending",
                "base_date": revision.base_date.isoformat() if revision and revision.base_date else None,
                "points": [], "expected_count": len(job.stock_codes) if job else 0,
                "covered_count": 0, "warnings": list(revision.warnings) if revision else [],
                "updated_at": job.updated_at.isoformat() if job else None,
                "series_type": "fixed_basket_history", "revision_id": revision.id if revision else None,
                "provenance": list(revision.provenance) if revision else []}
            if revision:
                points = list(revision.points)
                result["covered_count"] = points[-1]["covered_count"] if points else 0
                count = min(max(int(window_days), 0), 5000)
                result["points"] = points[-count:] if count else points
            if job and job.last_error:
                result["warnings"].append(job.last_error if re.search("[\u4e00-\u9fff]", job.last_error)
                    else "历史行情源暂不可用，后台将自动重试；已有历史继续保留")
            return result

    async def _fetch_history(self, code, start, end):
        prices = await self._fetch_chunk(code, start, end)
        chunk = prices
        # Tencent caps a response at 800 rows. Continue backwards for long-lived
        # archives, verifying a 45-day overlap before combining qfq responses.
        # This is one logical revision; incompatible adjustment bases never join.
        while len(chunk) == 800 and date.fromisoformat(min(prices)) > start:
            first = min(prices)
            chunk = await self._fetch_chunk(code, start,
                min(end, date.fromisoformat(first) + timedelta(days=45)))
            overlap = set(prices).intersection(chunk)
            if not overlap or any(float(prices[day]) != float(chunk[day]) for day in overlap):
                raise ValueError("复权分段重叠行情不一致，保留旧修订等待重试")
            if min(chunk) >= first:
                raise ValueError("行情源无法补齐原历史区间，保留旧修订等待重试")
            prices.update(chunk)
        if self._now().time() < time(15, 5):
            prices.pop(self._now().date().isoformat(), None)
        # newfqkline ignores start and returns up to 800 rows before end. Keep
        # raw lengths for pagination above, then filter the completed revision.
        prices = {day: price for day, price in prices.items()
                  if start.isoformat() <= day <= end.isoformat()}
        return {"source": "tencent_newfqkline", "adjustment": "qfq", "prices": prices}

    async def _fetch_chunk(self, code, start, end):
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=15, follow_redirects=True)
        symbol = ("sh" if code.startswith("6") else "bj" if code.startswith(("4", "8", "92")) else "sz") + code
        # No old cached prices enter this acquisition.
        # Values above 800 silently make Tencent return only its default 640.
        count = 800
        # Use this one endpoint for every chunk of a revision. The older web
        # endpoint is WAF-blocked on the server and uses different precision.
        response = await self._client.get("https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get",
            params={"param": f"{symbol},day,{start.isoformat()},{end.isoformat()},{count},qfq"},
            headers={"Referer": "https://gu.qq.com/"})
        response.raise_for_status()
        payload = response.json()
        if payload.get("code") != 0:
            raise ValueError("Tencent adjusted history returned an error")
        data = (payload.get("data") or {}).get(symbol)
        if not isinstance(data, dict) or not data.get("qfqday"):
            raise ValueError("Tencent response lacks the requested symbol/qfq series")
        if len(data["qfqday"]) > count:
            raise ValueError("Tencent adjusted response exceeds the requested page size")
        prices = {}
        for row in data["qfqday"]:
            if not isinstance(row, list) or len(row) < 3 or row[0] in prices:
                raise ValueError("Malformed or duplicate adjusted price row")
            if date.fromisoformat(row[0]) > end:
                raise ValueError("行情源未遵守历史截止日，保留旧修订等待重试")
            prices[row[0]] = row[2]
        return prices

    async def close(self):
        if self._closed:
            return
        self._closed = True
        tasks = list(self._register_tasks) + self._workers + list(self._stock_tasks.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if self._owns_client and self._client is not None:
            await self._client.aclose()
        self._workers.clear()
        self._stock_tasks.clear()


dynamic_group_history_service = DynamicGroupHistoryService()
