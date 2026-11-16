"""Previous sealed-session members valued on the monitor's actual session."""
from __future__ import annotations

import asyncio
import time
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP

from sqlalchemy import select
from loguru import logger

from app.data_collectors.tencent_api import tencent_api
from app.models.stock import Stock
from app.models.limit_up import LimitUpRecord, LimitUpMonitorArchive
from app.models.market_review import MarketReviewDailyMetric, MarketReviewStockDaily
from app.services.limit_up_context_service import limit_up_context_service
from app.services.broken_board_performance_service import broken_board_performance_service
from app.utils.time_utils import today_cn
from app.utils.market_data_sanitizer import normalize_change_pct


class LimitUpCohortService:
    def __init__(self, *, context_service=limit_up_context_service, quote_fetcher=None,
                 history_fetcher=None, today_provider=today_cn, budget_seconds=7.5):
        self.context_service = context_service
        self.quote_fetcher = quote_fetcher or tencent_api.get_quotes_batch
        self.history_fetcher = history_fetcher or broken_board_performance_service.fallback_fetcher
        self.today_provider = today_provider
        self.budget_seconds = budget_seconds
        self._quotes = {}
        self._quote_lock = asyncio.Lock()
        self._history_limit = asyncio.Semaphore(6)

    @staticmethod
    def _number(value):
        return limit_up_context_service._number(value)

    @classmethod
    def _limit_price(cls, fact):
        reference = cls._number(fact.pre_close)
        if reference is None or reference <= 0:
            return None
        rates = {"gem": Decimal("0.2"), "star": Decimal("0.2"), "bj": Decimal("0.3")}
        rate = rates.get(fact.board_type, Decimal("0.05") if fact.is_st else Decimal("0.1"))
        return float((Decimal(str(reference)) * (1 + rate)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

    async def _facts(self, db, day):
        rows = list((await db.scalars(select(MarketReviewStockDaily).where(
            MarketReviewStockDaily.trade_date == day,
        ))).all())
        metric = await db.scalar(select(MarketReviewDailyMetric).where(MarketReviewDailyMetric.trade_date == day))
        complete = bool(metric is not None and metric.calc_version >= 1
                        and metric.source_status in {"primary", "partial"}
                        and sum(bool(row.today_touched_limit_up) for row in rows) == metric.limit_up_count)
        return {row.stock_code: row for row in rows}, complete

    async def _previous(self, db, trade_date):
        archive = await db.scalar(select(LimitUpMonitorArchive).where(LimitUpMonitorArchive.trade_date == trade_date))
        previous = archive.previous_trade_date if archive else None
        if (isinstance(previous, date) and not isinstance(previous, datetime)
                and previous < trade_date and previous.weekday() < 5):
            return previous
        return await self.context_service._previous_session(trade_date)

    async def _members(self, db, previous):
        rows = (await db.execute(select(LimitUpRecord, Stock).join(Stock, Stock.id == LimitUpRecord.stock_id).where(
            LimitUpRecord.trade_date == previous, LimitUpRecord.is_final_sealed.is_(True),
        ))).all()
        members = {}
        for record, stock in rows:
            count = self.context_service._continuous_days(record.continuous_limit_up_days, record.is_final_sealed)
            if count is not None:
                members[stock.stock_code] = dict(stock_code=stock.stock_code, stock_name=stock.stock_name,
                    market=stock.market, industry=stock.industry, yesterday_continuous_days=count,
                    yesterday_first_limit_up_time=self.context_service._time(record.first_limit_up_time),
                    yesterday_final_seal_time=self.context_service._time(record.final_seal_time))
        facts, complete = await self._facts(db, previous)
        for code, fact in facts.items():
            if fact.today_sealed_close:
                count = self.context_service._continuous_days(fact.today_continuous_days, True)
                if count is not None:
                    existing = members.get(code, {})
                    members[code] = dict(existing, stock_code=code, stock_name=fact.stock_name,
                        market="SH" if code.startswith("6") else "BJ" if fact.board_type == "bj" else "SZ",
                        yesterday_continuous_days=count,
                        yesterday_first_limit_up_time=self.context_service._time(fact.first_limit_time) or existing.get("yesterday_first_limit_up_time"),
                        yesterday_final_seal_time=self.context_service._time(fact.final_seal_time) or existing.get("yesterday_final_seal_time"))
            else:
                members.pop(code, None)
        if not complete:
            found, pool = self.context_service._get(self.context_service._pool_cache, previous)
            if not found:
                try:
                    source = await asyncio.wait_for(self.context_service.pool_fetcher(previous), timeout=4.1)
                    pool = {str(item.get("stock_code")): item for item in source or []}
                except Exception as exc:
                    logger.debug("Previous monitor cohort pool unavailable for {}: {}", previous, exc)
                    pool = {}
                self.context_service._put(self.context_service._pool_cache, previous, pool, missing=not pool)
            for code, item in (pool or {}).items():
                if code in members or code in facts:
                    continue
                count = self.context_service._continuous_days(item.get("continuous_limit_up_days"), item.get("is_final_sealed"))
                if count:
                    members[code] = dict(stock_code=code, stock_name=item.get("stock_name") or code,
                        market="SH" if code.startswith("6") else "SZ", yesterday_continuous_days=count,
                        yesterday_first_limit_up_time=self.context_service._time(item.get("first_limit_up_time")),
                        yesterday_final_seal_time=self.context_service._time(item.get("final_seal_time")))
        return members

    async def _live_quotes(self, day, codes):
        key = (day, tuple(sorted(codes)))
        async with self._quote_lock:
            cached = self._quotes.get(key)
            if cached is not None and time.monotonic() - cached[0] < 3:
                return cached[1]
            try:
                quotes = await asyncio.wait_for(self.quote_fetcher(codes), timeout=self.budget_seconds)
            except Exception as exc:
                logger.debug("Monitor cohort quotes unavailable for {}: {}", day, exc)
                return {}
            valid = {}
            for code, quote in (quotes or {}).items():
                try:
                    stamp = datetime.strptime(str(quote.get("datetime") or ""), "%Y%m%d%H%M%S")
                except (ValueError, TypeError):
                    continue
                if stamp.date() == day:
                    valid[code] = quote
            self._quotes = {key: (time.monotonic(), valid)}
            return valid

    async def enrich_prices(self, db, trade_date, rows, *, cohort=False):
        result = [dict(row) for row in rows]
        if not result:
            return result
        if trade_date == self.today_provider():
            quotes = await self._live_quotes(trade_date, [row["stock_code"] for row in result])
            for row in result:
                quote = quotes.get(row["stock_code"])
                if not quote:
                    self._status(row, cohort)
                    continue
                missing = set(quote.get("_missing_fields") or [])
                value = None if "change_pct" in missing else quote.get("change_pct")
                row["change_pct"] = normalize_change_pct(value, price=quote.get("price"), amount=quote.get("amount"))
                if row["change_pct"] is None and not missing.intersection({"price", "pre_close"}):
                    price, reference = self._number(quote.get("price")), self._number(quote.get("pre_close"))
                    if price is not None and reference is not None and price > 0 and reference > 0:
                        row["change_pct"] = normalize_change_pct((price / reference - 1) * 100)
                for source, target in (("price", "current_price"), ("amount", "amount"), ("turnover_rate", "turnover_rate")):
                    value = self._number(quote.get(source))
                    if source not in missing and value is not None:
                        row[target] = value
                opening = self.context_service._quote_pct(trade_date, quote)
                if opening is not None:
                    row["today_open_change_pct"] = opening
                high, limit, price = (self._number(quote.get(field)) for field in ("high", "limit_up", "price"))
                if limit is not None and limit > 0:
                    row["limit_up_price"] = limit
                    if high is not None and high > 0 and "high" not in missing:
                        row["today_touched_limit_up"] = (
                            row.get("today_touched_limit_up") is True
                            or Decimal(str(high)) >= Decimal(str(limit))
                        )
                        if row["today_touched_limit_up"] and not row.get("continuous_limit_up_days"):
                            row["continuous_limit_up_days"] = (row.get("yesterday_continuous_days") or 0) + 1
                    if price is not None and "price" not in missing:
                        row["is_sealed"] = Decimal(str(price)) >= Decimal(str(limit))
                        if row["is_sealed"]:
                            row["today_touched_limit_up"] = True
                            row["continuous_limit_up_days"] = row.get("continuous_limit_up_days") or (row.get("yesterday_continuous_days") or 0) + 1
                self._status(row, cohort)
            return result

        facts, complete = await self._facts(db, trade_date)
        missing_rows = []
        for row in result:
            fact = facts.get(row["stock_code"])
            if fact:
                row["change_pct"] = normalize_change_pct(fact.change_pct, price=fact.close_price, amount=fact.amount)
                if row["change_pct"] is None and fact.close_price and fact.pre_close and fact.pre_close > 0:
                    row["change_pct"] = normalize_change_pct((fact.close_price / fact.pre_close - 1) * 100)
                if fact.close_price and fact.close_price > 0:
                    row["current_price"] = fact.close_price
                if cohort:
                    row["today_touched_limit_up"] = fact.today_touched_limit_up
                    row["is_sealed"] = fact.today_sealed_close
                    row["continuous_limit_up_days"] = fact.today_continuous_days if fact.today_touched_limit_up else 0
                    row["first_limit_up_time"] = self.context_service._time(fact.first_limit_time)
                    row["final_seal_time"] = self.context_service._time(fact.final_seal_time)
                    row["open_count"] = fact.open_count
                row["amount"] = fact.amount
                row["turnover_rate"] = fact.turnover_rate
                if cohort:
                    row["limit_up_reason"] = fact.limit_up_reason
                    row["limit_up_price"] = self._limit_price(fact) or row.get("limit_up_price", 0)
            elif complete and cohort:
                row["today_touched_limit_up"] = False
                row["is_sealed"] = False
            if row.get("change_pct") is None:
                missing_rows.append(row)
            self._status(row, cohort)

        async def load(row):
            async with self._history_limit:
                try:
                    history = await self.history_fetcher(row["stock_code"], trade_date)
                    row["change_pct"] = normalize_change_pct((history or {}).get(trade_date))
                except Exception as exc:
                    logger.debug("Historical monitor change unavailable for {}: {}", row["stock_code"], exc)
        try:
            await asyncio.wait_for(asyncio.gather(*(load(row) for row in missing_rows)), timeout=self.budget_seconds)
        except asyncio.TimeoutError:
            logger.debug("Historical monitor changes exceeded budget for {}", trade_date)
        return result

    @classmethod
    def _status(cls, row, cohort):
        if cohort:
            for field in ("limit_up_price", "current_price"):
                value = cls._number(row.get(field))
                row[field] = value if value is not None and value > 0 else 0
        touched = row.get("today_touched_limit_up")
        if touched is False:
            row.update(current_status="untouched", is_sealed=False, continuous_limit_up_days=0,
                       first_limit_up_time=None, final_seal_time=None, seal_amount=None, is_one_word=False)
        elif touched is True:
            row["current_status"] = "sealed" if row.get("is_sealed") else "opened"
            if cohort:
                count = limit_up_context_service._continuous_days(row.get("continuous_limit_up_days"), True)
                previous = limit_up_context_service._continuous_days(row.get("yesterday_continuous_days"), True)
                if count is None:
                    count = previous + 1 if previous is not None else 0
                row["continuous_limit_up_days"] = count
        elif cohort:
            row.update(current_status="unknown", is_sealed=False, continuous_limit_up_days=0)

    async def get_rows(self, db, trade_date, current_rows, *, minimum=None, exact=None):
        previous = await self._previous(db, trade_date)
        if previous is None:
            return []
        members = await self._members(db, previous)
        current = {row["stock_code"]: row for row in current_rows}
        rows = []
        for code, member in sorted(members.items()):
            count = member["yesterday_continuous_days"]
            if (exact is not None and count != exact) or (exact is None and minimum is not None and count < minimum):
                continue
            row = dict(trade_date=trade_date, context_date=trade_date, previous_trade_date=previous,
                limit_up_price=0, current_price=0, continuous_limit_up_days=0,
                is_sealed=False, today_touched_limit_up=None, current_status="unknown", change_pct=None,
                **member)
            if code in current:
                row.update(current[code])
                row["is_sealed"] = current[code].get("is_sealed", current[code].get("is_final_sealed", False))
                row["today_touched_limit_up"] = True
                for field in ("first_limit_up_time", "final_seal_time"):
                    row[field] = self.context_service._time(row.get(field))
            row.update(context_date=trade_date, previous_trade_date=previous, yesterday_continuous_days=count)
            for field in ("yesterday_first_limit_up_time", "yesterday_final_seal_time"):
                row[field] = member.get(field) or row.get(field)
            rows.append(row)
        archive = self.context_service.archive_service
        if archive is not None and rows:
            try:
                percentages = await asyncio.wait_for(asyncio.gather(
                    archive.get_percentages(previous), archive.get_percentages(trade_date),
                    return_exceptions=True,
                ), timeout=self.context_service.request_timeout)
                for values, field in zip(percentages, ("yesterday_open_change_pct", "today_open_change_pct")):
                    if not isinstance(values, dict):
                        continue
                    for row in rows:
                        value = self._number(values.get(row["stock_code"]))
                        if value is not None:
                            row[field] = value
            except asyncio.TimeoutError:
                logger.debug("Stored cohort openings exceeded budget for {}", trade_date)
        return await self.enrich_prices(db, trade_date, rows, cohort=True)


limit_up_cohort_service = LimitUpCohortService()
