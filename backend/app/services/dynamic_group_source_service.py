"""One coherent market snapshot for all dynamic groups, independent of the UI."""
from __future__ import annotations

import asyncio
import copy
import math
import logging
import time as monotonic_time
from datetime import date, datetime, time, timedelta

from sqlalchemy import func, select

from app.database import async_session_maker
from app.models.limit_up import LimitUpRecord
from app.models.market_review import MarketReviewDailyMetric, MarketReviewStockDaily
from app.models.stock import Stock
from app.services.live_market_stats_service import live_market_stats_service
from app.services.local_topic_knowledge_service import LocalTopicKnowledgeService
from app.utils.time_utils import CN_TZ, now_cn

logger = logging.getLogger(__name__)


def _number(value):
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
        return parsed if math.isfinite(parsed) else None
    except (ValueError, TypeError, OverflowError):
        return None


def _hms(value):
    if value is None:
        return None
    if isinstance(value, (datetime, time)):
        return value.strftime("%H:%M:%S")
    text = str(value).strip()
    if text.isdigit() and len(text) <= 6:
        text = text.zfill(6)
        text = f"{text[:2]}:{text[2:4]}:{text[4:]}"
    try:
        return time.fromisoformat(text).strftime("%H:%M:%S")
    except ValueError:
        return None


async def _calendar():
    # Keep scheduler imports lazy to avoid an import cycle during app startup.
    from app.data_collectors.scheduler import _get_cn_trading_dates
    today = now_cn().date()
    return await asyncio.to_thread(_get_cn_trading_dates, today - timedelta(days=90), today)


class DynamicGroupSourceService:
    def __init__(self, *, market_service=live_market_stats_service, now_provider=now_cn,
                 calendar_loader=_calendar, context_loader=None, topic_records=None,
                 session_factory=async_session_maker, clock=monotonic_time.monotonic):
        self.market_service, self.now_provider = market_service, now_provider
        self.calendar_loader, self.context_loader = calendar_loader, context_loader
        self.session_factory, self.clock = session_factory, clock
        self.topic_records = topic_records
        self._topics = LocalTopicKnowledgeService()
        self._lock = asyncio.Lock()
        self._snapshot = None
        self._snapshot_at = 0.0
        self._calendar_day = None
        self._calendar_dates = []
        self._calendar_task = None
        self._calendar_retry_at = 0.0
        self._context = None
        self._context_date = None
        self._context_at = 0.0
        self._refresh_task = None

    def _now(self):
        value = self.now_provider()
        return value.astimezone(CN_TZ).replace(tzinfo=None) if value.tzinfo else value

    async def sessions(self):
        now = self._now()
        if self._calendar_day != now.date():
            if self.clock() < self._calendar_retry_at:
                raise ValueError("交易日历正在恢复，请稍后刷新")
            if self._calendar_task is None or self._calendar_task.done():
                self._calendar_task = asyncio.create_task(self.calendar_loader())
            try:
                values = await asyncio.wait_for(asyncio.shield(self._calendar_task), 6)
                values = sorted({day for day in values if isinstance(day, date) and day <= now.date()})
                if len(values) < 2:
                    raise ValueError("交易日历缺少前一交易日")
                self._calendar_dates, self._calendar_day = values, now.date()
                self._calendar_retry_at = 0
            except Exception:
                self._calendar_retry_at = self.clock() + 60
                raise
        days = [day for day in self._calendar_dates if day <= now.date()]
        return days[-1], days[-2]

    def peek_snapshot(self):
        return copy.deepcopy(self._snapshot)

    def request_refresh(self):
        """Warm one shared snapshot for previews, even before the first group."""
        if self._refresh_task is None or self._refresh_task.done():
            self._refresh_task = asyncio.create_task(self.get_snapshot())
            self._refresh_task.add_done_callback(self._refresh_finished)
        return self._refresh_task

    @staticmethod
    def _refresh_finished(task):
        if not task.cancelled():
            error = task.exception()
            if error is not None:
                logger.warning("Dynamic group quote warmup failed: %s", error)

    async def get_snapshot(self):
        async with self._lock:
            now = self._now()
            if (self._snapshot is not None and self.clock() - self._snapshot_at < 25
                    and self._snapshot.get("observed_date") == now.date().isoformat()):
                return copy.deepcopy(self._snapshot)
            day, previous = await self.sessions()
            market, context = await asyncio.gather(
                self.market_service.get_quotes_snapshot(day), self._get_context(day, previous))
            # A full-market scan takes time; later batches must be compared to
            # the completion clock, not incorrectly rejected as future quotes.
            now = self._now()
            records = self.topic_records if self.topic_records is not None else self._topics._load_records()
            rows = []
            for code in market["codes"]:
                quote = market.get("quotes", {}).get(code)
                stamp = self._valid_stamp(quote, day, now)
                if stamp is None:
                    continue
                row = self._row(code, quote, stamp, day, previous, context, records.get(code, {}))
                if row["current_price"] is not None and row["current_price"] > 0:
                    rows.append(row)
            warnings = list(context.get("warnings") or [])
            missing = len(market["codes"]) - len(rows)
            if missing:
                warnings.append(f"{missing} 只股票行情待更新，已保留原有成员")
            if not context.get("previous_complete"):
                warnings.append("昨日涨停数据尚未完整核验，缺失项保留待更新")
            if not context.get("today_complete"):
                warnings.append("今日触板数据正在更新")
            warnings.append("行业 / 概念使用本地题材资料，资料更新于 2026-06-16，未覆盖项不参与匹配")
            snapshot = dict(trade_date=day.isoformat(), previous_trade_date=previous.isoformat(),
                source_time=max((row["quote_time"] for row in rows), default=None),
                observed_date=now.date().isoformat(), rows=rows, universe_codes=list(market["codes"]),
                status="ready" if not missing else "partial", warnings=warnings,
                limit_up_pool_complete=bool(context.get("today_complete")),
                previous_pool_complete=bool(context.get("previous_complete")))
            self._snapshot, self._snapshot_at = snapshot, self.clock()
            return copy.deepcopy(snapshot)

    @staticmethod
    def _valid_stamp(quote, day, now):
        if not isinstance(quote, dict):
            return None
        try:
            stamp = datetime.strptime(str(quote.get("datetime", "")), "%Y%m%d%H%M%S")
        except ValueError:
            return None
        if stamp.date() != day or stamp > now + timedelta(seconds=5):
            return None
        if day == now.date():
            current = now.time()
            if time(9, 15) <= current <= time(11, 30) or time(13) <= current <= time(15):
                if (now - stamp).total_seconds() > 180:
                    return None
            elif time(11, 30) < current < time(13):
                if stamp.time() < time(11, 27):
                    return None
            elif current > time(15) and stamp.time() < time(14, 57):
                return None
        elif stamp.time() < time(14, 57):
            return None
        return stamp

    def _row(self, code, quote, stamp, day, previous, context, metadata):
        missing = set(quote.get("_missing_fields") or [])
        def number(key):
            return None if key in missing else _number(quote.get(key))
        prior = context.get("previous", {}).get(code)
        today = context.get("today", {}).get(code)
        previous_complete = bool(context.get("previous_complete"))
        today_complete = bool(context.get("today_complete"))
        prior_sealed = (bool(prior.get("is_final_sealed")) if prior is not None
                        else False if previous_complete else None)
        price, reference, opened, upper = (number(key) for key in ("price", "pre_close", "open", "limit_up"))
        opened_pct = ((opened / reference - 1) * 100 if opened and reference and opened > 0 and reference > 0
                      and stamp.time() >= time(9, 25) else None)
        sealed = None
        if price and upper and upper > 0:
            if price < upper - 0.001:
                sealed = False
            elif number("bid1_volume") is not None:
                sealed = number("bid1_volume") > 0
        touched = True if today else False if today_complete else None
        high = number("high")
        if high and upper and upper > 0:
            touched = high >= upper - 0.001
        prior_days = (max(int(prior.get("continuous_limit_up_days") or 1), 1) if prior_sealed
                      else 0 if prior_sealed is False else None)
        if today and today.get("continuous_limit_up_days") is not None:
            boards = max(int(today["continuous_limit_up_days"]), 1)
        elif touched is False:
            boards = 0
        elif touched and prior_days is not None:
            boards = prior_days + 1
        else:
            boards = None
        known_absent = []
        if prior is None and previous_complete:
            known_absent.extend(["yesterday_first_limit_up_time", "yesterday_final_seal_time"])
        if touched is False:
            known_absent.extend(["first_limit_up_time", "final_seal_time"])
        concepts = list(dict.fromkeys(str(value) for value in (metadata.get("concept") or []) if value))
        for topic in metadata.get("topics") or []:
            label = topic.get("theme") if isinstance(topic, dict) else topic
            if label and label not in concepts:
                concepts.append(label)
        amount, market_value = number("amount"), number("circulating_value")
        name = str(quote.get("name") or metadata.get("stock_name") or code)
        return dict(stock_code=code, stock_name=name, market="SH" if code.startswith("6") else "SZ",
            board_type="star" if code.startswith("68") else "gem" if code.startswith("30") else "main",
            trade_date=day.isoformat(), context_date=day.isoformat(), previous_trade_date=previous.isoformat(),
            current_price=price, pre_close=reference, change_pct=number("change_pct"),
            today_open_change_pct=opened_pct, yesterday_open_change_pct=context.get("previous_openings", {}).get(code),
            turnover_rate=number("turnover_rate"), amount=amount,
            amount_yi=amount / 10000 if amount is not None else None,
            tradable_market_value=market_value * 10000 if market_value is not None else None,
            market_value_yi=market_value, is_st="ST" in name.upper(), industry=metadata.get("industry") or None,
            concepts=concepts or None, quote_time=stamp.isoformat(), is_sealed=sealed,
            limit_up_price=upper, today_touched_limit_up=touched, yesterday_limit_up=prior_sealed,
            continuous_limit_up_days=boards, yesterday_continuous_days=prior_days,
            yesterday_first_limit_up_time=_hms(prior.get("first_limit_up_time")) if prior else None,
            yesterday_final_seal_time=_hms(prior.get("final_seal_time")) if prior else None,
            first_limit_up_time=_hms(today.get("first_limit_up_time")) if today else None,
            final_seal_time=_hms(today.get("final_seal_time")) if today else None,
            reason_category=(today or {}).get("reason_category"), limit_up_reason=(today or {}).get("limit_up_reason"),
            _known_absent_fields=known_absent)

    async def _get_context(self, day, previous):
        if self._context_date == day and self.clock() - self._context_at < 25:
            return self._context
        value = (await self.context_loader(day, previous) if self.context_loader is not None
                 else await self._load_context(day, previous))
        self._context, self._context_date, self._context_at = value, day, self.clock()
        return value

    async def _load_context(self, day, previous):
        from app.services.opening_auction_archive_service import opening_auction_archive_service
        from app.services.realtime_limit_up_service import realtime_limit_up_service
        warnings = []

        async def pool():
            try:
                value = await asyncio.wait_for(realtime_limit_up_service.get_fast_limit_up_snapshot(day), 12)
                complete = bool(value.authoritative and value.complete and value.evidence_trade_date == day)
                return {row["stock_code"]: row for row in value.items if row.get("stock_code")}, complete
            except Exception:
                warnings.append("实时触板池尚未更新")
                return {}, False

        async def yesterday():
            async with self.session_factory() as db:
                records = (await db.execute(select(LimitUpRecord, Stock.stock_code).join(Stock)
                    .where(LimitUpRecord.trade_date == previous))).all()
                metric = await db.scalar(select(MarketReviewDailyMetric).where(MarketReviewDailyMetric.trade_date == previous))
                touched_count = await db.scalar(select(func.count()).select_from(MarketReviewStockDaily).where(
                    MarketReviewStockDaily.trade_date == previous, MarketReviewStockDaily.today_touched_limit_up.is_(True)))
                complete = bool(metric is not None and metric.calc_version >= 1 and metric.source_status in {"primary", "partial"}
                                and int(touched_count or 0) == metric.limit_up_count and len(records) >= metric.limit_up_count)
                rows = {code: dict(is_final_sealed=record.is_final_sealed,
                    continuous_limit_up_days=record.continuous_limit_up_days,
                    first_limit_up_time=_hms(record.first_limit_up_time), final_seal_time=_hms(record.final_seal_time))
                    for record, code in records}
                return rows, complete

        (today, today_complete), (prior, previous_complete), openings = await asyncio.gather(
            pool(), yesterday(), opening_auction_archive_service.get_percentages(previous))
        return dict(today=today, today_complete=today_complete, previous=prior, previous_complete=previous_complete,
                    previous_openings=openings, warnings=warnings)

    async def close(self):
        if self._refresh_task is not None and not self._refresh_task.done():
            self._refresh_task.cancel()
            await asyncio.gather(self._refresh_task, return_exceptions=True)
        task = self._calendar_task
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


dynamic_group_source_service = DynamicGroupSourceService()
