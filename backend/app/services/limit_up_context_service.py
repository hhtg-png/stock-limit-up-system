"""Optional previous-session seals and finalized opening prices for rich lists.

History is deliberately outside the alert path. Every source may fail without
changing the existing row, and one enrichment call has a fixed time budget.
"""
from __future__ import annotations

import asyncio
import math
import time
from collections import OrderedDict
from datetime import date, datetime, time as wall_time, timedelta

import httpx
from loguru import logger
from sqlalchemy import select

from app.database import async_session_maker
from app.models.limit_up import LimitUpRecord
from app.models.market_review import MarketReviewStockDaily
from app.models.stock import Stock
from app.services.opening_auction_archive_service import opening_auction_archive_service
from app.utils.time_utils import now_cn, today_cn


def _calendar(start: date, end: date):
    # The scheduler owns the authoritative exchange calendar. Import and fetch
    # in the worker thread too: neither can block the asyncio event loop.
    from app.data_collectors.scheduler import _get_cn_trading_dates

    return _get_cn_trading_dates(start, end)


async def _pool(trade_date: date):
    from app.crawlers.eastmoney_crawler import em_crawler

    return await em_crawler.crawl(trade_date)


class LimitUpContextService:
    """Cached, bounded enrichment with injectable external data boundaries.

    ``history_fetcher(code, start, end)`` returns
    ``{date: {"open": price, "pre_close": same_session_reference}}``.
    ``calendar_loader(start, end)`` is synchronous and always runs off-loop.
    Passing ``session_factory=None`` disables the optional database provider.
    """

    def __init__(
        self,
        *,
        calendar_loader=None,
        session_factory=async_session_maker,
        archive_service=opening_auction_archive_service,
        history_fetcher=None,
        pool_fetcher=None,
        http_client=None,
        today_provider=today_cn,
        now_provider=now_cn,
        clock=time.monotonic,
        budget_seconds=7.5,
        request_timeout=2.0,
        missing_ttl=30.0,
    ):
        self.calendar_loader = calendar_loader or _calendar
        self.session_factory = session_factory
        self.archive_service = archive_service
        self.history_fetcher = history_fetcher or self._fetch_history
        self.pool_fetcher = pool_fetcher or _pool
        self.today_provider = today_provider
        self.now_provider = now_provider
        self.clock = clock
        self.budget_seconds = min(max(float(budget_seconds), 0.01), 7.5)
        self.request_timeout = max(float(request_timeout), 0.01)
        self.missing_ttl = max(float(missing_ttl), 0.01)
        self._http_client = http_client
        self._daily_cache = OrderedDict()
        self._seal_cache = OrderedDict()
        self._calendar_cache = OrderedDict()
        self._pool_cache = OrderedDict()
        self._history_limit = asyncio.Semaphore(6)
        self._calendar_lock = asyncio.Lock()
        self._seal_lock = asyncio.Lock()
        self._em_retry_at = 0.0

    def _get(self, cache, key):
        cached = cache.get(key)
        if cached is None:
            return False, None
        expires, value = cached
        if expires <= self.clock():
            cache.pop(key, None)
            return False, None
        cache.move_to_end(key)
        return True, value

    def _put(self, cache, key, value, *, missing=False):
        cache[key] = (self.clock() + (self.missing_ttl if missing else 86400), value)
        cache.move_to_end(key)
        while len(cache) > 12000:
            cache.popitem(last=False)

    @staticmethod
    def _number(value):
        try:
            number = float(value)
            return number if math.isfinite(number) else None
        except (TypeError, ValueError, OverflowError):
            return None

    @classmethod
    def _opening_pct(cls, open_price, pre_close):
        opened, reference = cls._number(open_price), cls._number(pre_close)
        if opened is None or reference is None or opened <= 0 or reference <= 0:
            return None
        percentage = (opened / reference - 1) * 100
        return round(percentage, 4) if math.isfinite(percentage) else None

    @staticmethod
    def _time(value):
        if isinstance(value, (datetime, wall_time)):
            return value.strftime("%H:%M:%S")
        if isinstance(value, str):
            try:
                return wall_time.fromisoformat(value).strftime("%H:%M:%S")
            except ValueError:
                return None
        return None

    def _opening_finished(self, day):
        return day < self.today_provider() or (
            day == self.today_provider() and self.now_provider().time().replace(tzinfo=None) >= wall_time(9, 25)
        )

    def _quote_pct(self, day, quote):
        if day != self.today_provider() or not self._opening_finished(day) or not isinstance(quote, dict):
            return None
        if set(quote.get("_missing_fields") or []) & {"open", "pre_close"}:
            return None
        try:
            timestamp = str(quote.get("datetime") or "")
            stamp = datetime.strptime(timestamp, "%Y%m%d%H%M%S")
        except (TypeError, ValueError):
            return None
        if stamp.date() != day or stamp.time() < wall_time(9, 25):
            return None
        return self._opening_pct(quote.get("open"), quote.get("pre_close"))

    async def enrich(self, trade_date: date, rows: list[dict], quotes: dict | None = None) -> list[dict]:
        """Anchor comparisons to the requested date while preserving each row's data date."""
        result = [dict(row, context_date=trade_date, previous_trade_date=None,
                       yesterday_first_limit_up_time=None, yesterday_final_seal_time=None,
                       yesterday_open_change_pct=None, today_open_change_pct=None) for row in rows]
        codes = list(dict.fromkeys(str(row.get("stock_code") or "") for row in rows))
        codes = [code for code in codes if len(code) == 6 and code.isdigit()]
        if not codes:
            return result
        quotes = quotes or {}
        live = {}
        for code in codes:
            value = self._quote_pct(trade_date, quotes.get(code))
            if value is not None:
                live[code] = value
                self._put(self._daily_cache, (trade_date, code), value)

        previous = None

        async def populate():
            nonlocal previous
            previous = await self._previous_session(trade_date)
            await asyncio.gather(*(
                self._load_archived_openings(day, codes)
                for day in (previous, trade_date) if day is not None
            ))
            # Fresh current quotes take precedence over imported snapshots.
            for code, value in live.items():
                self._put(self._daily_cache, (trade_date, code), value)
            await asyncio.gather(
                self._load_daily(trade_date, previous, codes),
                self._load_seals(previous, codes) if previous else asyncio.sleep(0),
                self._record_current_openings(trade_date, {code: quotes[code] for code in live}),
            )

        try:
            await asyncio.wait_for(populate(), timeout=self.budget_seconds)
        except asyncio.TimeoutError:
            logger.debug("Opening context exceeded its time budget for {}", trade_date)
        except Exception as exc:
            logger.debug("Opening context unavailable for {}: {}", trade_date, exc)

        for row in result:
            code = str(row.get("stock_code") or "")
            row["previous_trade_date"] = previous
            if self._opening_finished(trade_date):
                row["today_open_change_pct"] = live.get(code, self._get(self._daily_cache, (trade_date, code))[1])
            if previous:
                row["yesterday_open_change_pct"] = self._get(self._daily_cache, (previous, code))[1]
                seal = self._get(self._seal_cache, (previous, code))[1]
                if seal:
                    row["yesterday_first_limit_up_time"], row["yesterday_final_seal_time"] = seal
        return result

    async def _load_archived_openings(self, day, codes):
        if self.archive_service is None or not self._opening_finished(day):
            return
        try:
            # Read every call so an imported file can replace a recent missing
            # provider result without waiting for the negative cache to expire.
            values = await self.archive_service.get_percentages(day)
            for code in codes:
                percentage = self._number(values.get(code))
                if percentage is not None:
                    self._put(self._daily_cache, (day, code), percentage)
        except Exception as exc:
            logger.debug("Archived opening context unavailable for {}: {}", day, exc)

    async def _record_current_openings(self, day, quotes):
        if self.archive_service is None or day != self.today_provider() or not self._opening_finished(day):
            return
        try:
            # This schedules the all-market job; the list never awaits it.
            self.archive_service.request_full_capture(day)
            if quotes:
                await self.archive_service.record_quotes(day, quotes)
        except Exception as exc:
            logger.debug("Opening snapshot capture unavailable for {}: {}", day, exc)

    async def _previous_session(self, day):
        async with self._calendar_lock:
            found, previous = self._get(self._calendar_cache, day)
            if found:
                return previous
            try:
                dates = await asyncio.wait_for(
                    asyncio.to_thread(self.calendar_loader, day - timedelta(days=60), day),
                    timeout=self.request_timeout,
                )
                # A weekend/holiday query still compares with its last known
                # exchange session; never subtract a guessed calendar day.
                previous = max((value for value in dates if value < day), default=None)
            except Exception as exc:
                logger.debug("Opening context calendar unavailable for {}: {}", day, exc)
                previous = None
            self._put(self._calendar_cache, day, previous, missing=previous is None)
            return previous

    async def _load_daily(self, day, previous, codes):
        days = [value for value in (previous, day) if value and self._opening_finished(value)]
        if not days:
            return

        async def one(code):
            async with self._history_limit:
                missing = [value for value in days if not self._get(self._daily_cache, (value, code))[0]]
                if not missing:
                    return
                try:
                    # A provider can try both sources; the overall enrichment
                    # deadline still bounds all queued codes together.
                    history = await asyncio.wait_for(
                        self.history_fetcher(code, previous or day, day),
                        timeout=self.request_timeout * 2 + 0.1,
                    )
                except Exception as exc:
                    logger.debug("Opening history unavailable for {}: {}", code, exc)
                    history = {}
                for value in missing:
                    # Another request may have loaded a new archive or quote
                    # while this provider request was in flight.
                    if self._get(self._daily_cache, (value, code))[1] is not None:
                        continue
                    prices = (history or {}).get(value) or {}
                    percentage = self._opening_pct(prices.get("open"), prices.get("pre_close"))
                    self._put(self._daily_cache, (value, code), percentage, missing=percentage is None)

        await asyncio.gather(*(one(code) for code in codes))

    async def _load_seals(self, day, codes):
        async with self._seal_lock:
            missing = [code for code in codes if not self._get(self._seal_cache, (day, code))[0]]
            if not missing:
                return
            values = {}
            if self.session_factory is not None:
                try:
                    values = await asyncio.wait_for(self._db_seals(day, missing), timeout=self.request_timeout)
                except Exception as exc:
                    logger.debug("Previous seals unavailable from database for {}: {}", day, exc)
            # Publish DB facts before the optional slower pool fallback finishes.
            for code, seal in values.items():
                partial = seal["touched"] and (not seal["first"] or (seal["sealed"] and not seal["final"]))
                self._put(self._seal_cache, (day, code), (seal["first"], seal["final"]), missing=partial)
            gaps = [code for code in missing if code not in values or (
                values[code]["touched"] and (not values[code]["first"] or (
                    values[code]["sealed"] and not values[code]["final"]
                ))
            )]
            if gaps:
                found, pool = self._get(self._pool_cache, day)
                if not found:
                    try:
                        # The crawler reads sealed and broken pools in sequence;
                        # the enclosing enrich deadline still caps their total.
                        rows = await asyncio.wait_for(self.pool_fetcher(day), timeout=self.request_timeout * 2 + 0.1)
                        pool = {str(row.get("stock_code")): row for row in rows or []}
                    except Exception as exc:
                        logger.debug("Previous seals unavailable from historical pool for {}: {}", day, exc)
                        pool = {}
                    self._put(self._pool_cache, day, pool, missing=not pool)
                for code in gaps:
                    item = (pool or {}).get(code) or {}
                    existing = values.get(code) or {}
                    can_final = existing.get("sealed", item.get("is_final_sealed") is not False)
                    first = existing.get("first") or self._time(item.get("first_limit_up_time"))
                    final = existing.get("final") or (
                        self._time(item.get("final_seal_time"))
                        if can_final and item.get("is_final_sealed") is not False else None
                    )
                    self._put(self._seal_cache, (day, code), (first, final), missing=not first or (can_final and not final))

    async def _db_seals(self, day, codes):
        values = {}
        async with self.session_factory() as session:
            records = (await session.execute(select(
                Stock.stock_code, LimitUpRecord.first_limit_up_time,
                LimitUpRecord.final_seal_time, LimitUpRecord.is_final_sealed,
            ).join(Stock, Stock.id == LimitUpRecord.stock_id).where(
                LimitUpRecord.trade_date == day, Stock.stock_code.in_(codes),
            ))).all()
            for code, first, final, sealed in records:
                values[code] = {"first": self._time(first), "final": self._time(final) if sealed else None,
                                "sealed": bool(sealed), "touched": True}
            facts = (await session.execute(select(
                MarketReviewStockDaily.stock_code, MarketReviewStockDaily.first_limit_time,
                MarketReviewStockDaily.final_seal_time, MarketReviewStockDaily.today_sealed_close,
                MarketReviewStockDaily.today_touched_limit_up,
            ).where(MarketReviewStockDaily.trade_date == day, MarketReviewStockDaily.stock_code.in_(codes)))).all()
            for code, first, final, sealed, touched in facts:
                if code not in values:
                    values[code] = {"first": self._time(first) if touched else None,
                                    "final": self._time(final) if touched and sealed else None,
                                    "sealed": bool(sealed), "touched": bool(touched)}
                elif touched:
                    values[code]["first"] = values[code]["first"] or self._time(first)
                    if values[code]["sealed"] and sealed:
                        values[code]["final"] = values[code]["final"] or self._time(final)
        return values

    async def _client(self):
        if self._http_client is None or self._http_client.is_closed:
            self._http_client = httpx.AsyncClient(
                timeout=self.request_timeout,
                limits=httpx.Limits(max_connections=6, max_keepalive_connections=6),
                headers={"User-Agent": "Mozilla/5.0", "Referer": "https://quote.eastmoney.com/"},
            )
        return self._http_client

    async def _fetch_history(self, code, start, end):
        client = await self._client()
        history = {}
        if self.clock() >= self._em_retry_at:
            try:
                response = await asyncio.wait_for(client.get(
                    "https://push2his.eastmoney.com/api/qt/stock/kline/get",
                    params={"secid": f"{1 if code.startswith('6') else 0}.{code}", "klt": "101", "fqt": "0",
                            "fields1": "f1,f2,f3", "fields2": "f51,f52,f53,f60",
                            "beg": start.strftime("%Y%m%d"), "end": end.strftime("%Y%m%d"), "lmt": "120"},
                ), timeout=self.request_timeout)
                response.raise_for_status()
                data = response.json().get("data") or {}
                if data.get("code", code) == code:
                    for line in data.get("klines") or []:
                        try:
                            raw_day, opened, closed, change = line.split(",")
                            day = date.fromisoformat(raw_day)
                            reference = self._number(closed) - self._number(change)
                            if start <= day <= end and self._opening_pct(opened, reference) is not None:
                                history[day] = {"open": opened, "pre_close": reference}
                        except (ValueError, TypeError, AttributeError):
                            continue
            except Exception as exc:
                self._em_retry_at = self.clock() + self.missing_ttl
                logger.debug("Unadjusted opening history unavailable for {}: {}", code, exc)
        if start in history and end in history:
            return history
        try:
            symbol = ("sh" if code.startswith("6") else "bj" if code.startswith(("4", "8", "92")) else "sz") + code
            response = await asyncio.wait_for(client.get(
                "https://proxy.finance.qq.com/ifzqgtimg/appstock/app/fqkline/get",
                params={"param": f"{symbol},day,{(start - timedelta(days=45)).isoformat()},{end.isoformat()},160,qfq"},
                headers={"Referer": "https://gu.qq.com/"},
            ), timeout=self.request_timeout)
            response.raise_for_status()
            data = (response.json().get("data") or {}).get(symbol) or {}
            # Pair only prices from this single qfq series. Raw ``day`` rows
            # would compare across corporate actions in incompatible bases.
            previous_close = None
            for row in sorted(data.get("qfqday") or [], key=lambda item: str(item[0])):
                try:
                    day = date.fromisoformat(row[0])
                    opened, closed = self._number(row[1]), self._number(row[2])
                except (ValueError, TypeError, IndexError):
                    previous_close = None
                    continue
                if closed is None or closed <= 0:
                    previous_close = None
                    continue
                if start <= day <= end and self._opening_pct(opened, previous_close) is not None:
                    history.setdefault(day, {"open": opened, "pre_close": previous_close})
                previous_close = closed
        except Exception as exc:
            logger.debug("Adjusted opening history unavailable for {}: {}", code, exc)
        return history


limit_up_context_service = LimitUpContextService()
