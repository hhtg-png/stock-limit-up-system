from __future__ import annotations

import asyncio
import math
from datetime import date, datetime
from zoneinfo import ZoneInfo

import httpx
from loguru import logger
from sqlalchemy import select, func

from app.database import async_session_maker
from app.data_collectors.tencent_api import tencent_api
from app.models.market_review import MarketReviewDailyMetric, MarketReviewStockDaily


def _today():
    return datetime.now(ZoneInfo("Asia/Shanghai")).date()


def _calendar():
    import akshare as ak
    return [value if isinstance(value, date) else date.fromisoformat(str(value))
            for value in ak.tool_trade_date_hist_sina()["trade_date"].tolist()]


class BrokenBoardPerformanceService:
    """Prior session's failed multi-board cohort, valued on each chart date."""

    def __init__(self, session_factory=async_session_maker, quote_fetcher=None,
                 history_fetcher=None, calendar_loader=None, today_provider=_today, fallback_fetcher=None):
        self.session_factory = session_factory
        self.quote_fetcher = quote_fetcher or tencent_api.get_quotes_batch
        self.history_fetcher = history_fetcher or self._fetch_history
        self.calendar_loader = calendar_loader or _calendar
        self.fallback_fetcher = fallback_fetcher or self._fetch_fallback_history
        self.today_provider = today_provider
        self._calendar_cache = None
        self._history_cache = {}
        self._cache_day = None
        self._history_limit = asyncio.Semaphore(8)

    @staticmethod
    def _number(value):
        try:
            value = float(value)
            return value if math.isfinite(value) else None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def parse_history(rows):
        result = {}
        previous = None
        for row in sorted(rows, key=lambda row: row[0]):
            try:
                day, close = date.fromisoformat(row[0]), float(row[2])
            except (ValueError, TypeError, IndexError):
                previous = None
                continue
            if not math.isfinite(close) or close <= 0:
                previous = None
                continue
            if previous is not None:
                result[day] = round((close / previous - 1) * 100, 2)
            previous = close
        return result

    async def _fetch_history(self, code, end_date):
        key = (code, end_date)
        if key in self._history_cache:
            return self._history_cache[key]
        async with self._history_limit:
            if key in self._history_cache:
                return self._history_cache[key]
            symbol = ("sh" if code.startswith("6") else "bj" if code.startswith(("4", "8", "92")) else "sz") + code
            async with httpx.AsyncClient(timeout=12) as client:
                response = await client.get("https://web.ifzq.gtimg.cn/appstock/app/fqkline/get", params={
                    "param": f"{symbol},day,,{end_date.isoformat()},640,qfq"
                })
                response.raise_for_status()
                data = (response.json().get("data") or {}).get(symbol) or {}
            history = self.parse_history(data.get("qfqday") or data.get("day") or [])
            if history:
                self._history_cache[key] = history
            return history

    async def _fetch_fallback_history(self, code, end_date):
        key = ("fallback", code, end_date)
        if key in self._history_cache:
            return self._history_cache[key]
        async with self._history_limit:
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.get("https://push2his.eastmoney.com/api/qt/stock/kline/get", params={
                    "secid": f"{1 if code.startswith('6') else 0}.{code}",
                    "fields1": "f1,f2,f3", "fields2": "f51,f59", "klt": "101", "fqt": "1",
                    "end": end_date.strftime("%Y%m%d"), "lmt": "640",
                })
                response.raise_for_status()
                result = {}
                for row in (response.json().get("data") or {}).get("klines") or []:
                    parts = row.split(",")
                    try:
                        value = self._number(parts[1])
                        if value is not None:
                            result[date.fromisoformat(parts[0])] = value
                    except (ValueError, IndexError):
                        continue
            if result:
                self._history_cache[key] = result
            return result

    async def get_performance(self, days: int, end_date: date):
        today = self.today_provider()
        if self._cache_day != today:
            self._history_cache.clear()
            self._calendar_cache = None
            self._cache_day = today
        if self._calendar_cache is None:
            self._calendar_cache = sorted(set(await asyncio.to_thread(self.calendar_loader)))
        dates = [day for day in self._calendar_cache if day <= min(today, end_date)]
        async with self.session_factory() as db:
            first_date = await db.scalar(select(func.min(MarketReviewDailyMetric.trade_date)))
        selected = [day for day in dates if first_date is None or day >= first_date][-days:]
        previous = {day: dates[index - 1] for index, day in enumerate(dates) if index > 0 and day in selected}
        async with self.session_factory() as db:
            metrics = {row.trade_date: row for row in (await db.execute(
                select(MarketReviewDailyMetric).where(MarketReviewDailyMetric.trade_date.in_(previous.values()))
            )).scalars()}
            rows = list((await db.execute(select(MarketReviewStockDaily).where(
                MarketReviewStockDaily.trade_date.in_(previous.values()),
                MarketReviewStockDaily.yesterday_continuous_days >= 2,
                MarketReviewStockDaily.today_sealed_close.is_(False),
            ).order_by(MarketReviewStockDaily.stock_code))).scalars())
        cohorts = {}
        for day in selected:
            prior = previous.get(day)
            metric = metrics.get(prior)
            if metric is not None and metric.calc_version >= 1:
                cohorts[day] = [row for row in rows if row.trade_date == prior]
        live_codes = sorted({row.stock_code for row in cohorts.get(today, [])})
        quotes = {}
        if live_codes:
            try:
                quotes = await self.quote_fetcher(live_codes)
            except Exception as exc:
                logger.warning("Broken-board live quotes unavailable: {}", exc)
        historical_codes = sorted({row.stock_code for day, cohort in cohorts.items() if day < today for row in cohort})
        historical_end = max((day for day in selected if day < today), default=end_date)
        results = await asyncio.gather(*(self.history_fetcher(code, historical_end) for code in historical_codes), return_exceptions=True)
        history = {code: value if isinstance(value, dict) else {} for code, value in zip(historical_codes, results)}
        missing_codes = sorted({row.stock_code for day, cohort in cohorts.items() if day < today
                                for row in cohort if day not in history.get(row.stock_code, {})})
        fallback_results = await asyncio.gather(
            *(self.fallback_fetcher(code, historical_end) for code in missing_codes), return_exceptions=True
        )
        for code, extra in zip(missing_codes, fallback_results):
            if isinstance(extra, dict):
                history[code] = {**extra, **history.get(code, {})}
        points = []
        for day in selected:
            stocks = []
            for row in cohorts.get(day, []):
                if day == today:
                    quote = quotes.get(row.stock_code) or {}
                    value = quote.get("change_pct") if str(quote.get("datetime", ""))[:8] == day.strftime("%Y%m%d") else None
                else:
                    value = history.get(row.stock_code, {}).get(day)
                stocks.append({"stock_code": row.stock_code, "stock_name": row.stock_name,
                               "previous_board": row.yesterday_continuous_days, "change_pct": self._number(value)})
            changes = [stock["change_pct"] for stock in stocks if stock["change_pct"] is not None]
            complete = day in cohorts and len(changes) == len(stocks)
            points.append({"trade_date": day.isoformat(), "average_change": round(sum(changes) / len(changes), 2) if complete and changes else None,
                           "sample_count": len(stocks), "priced_count": len(changes), "stocks": stocks,
                           "data_status": "ready" if complete else "unavailable"})
        return {"points": points}


broken_board_performance_service = BrokenBoardPerformanceService()
