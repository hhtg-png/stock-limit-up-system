from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo
from typing import Any, Awaitable, Callable, Dict, Iterable, Optional

import httpx
from loguru import logger
from sqlalchemy import select

from app.config import settings
from app.database import async_session_maker
from app.data_collectors.tencent_api import tencent_api
from app.models.market_data import DailyStatistics
from app.models.stock import Stock
from app.services.realtime_limit_up_service import realtime_limit_up_service
from app.crawlers.eastmoney_crawler import em_crawler
NormalizedFetcher = Callable[[date], Awaitable[Dict[str, Any]]]
ListFetcher = Callable[[date], Awaitable[list[Dict[str, Any]]]]
QuoteFetcher = Callable[[list[str]], Awaitable[Dict[str, Dict[str, Any]]]]


def _today_shanghai() -> date:
    return datetime.now(ZoneInfo("Asia/Shanghai")).date()


class MarketReviewSourceService:
    """Collects authoritative market-review inputs from existing market sources."""

    YESTERDAY_POOL_API = "https://push2ex.eastmoney.com/getYesterdayZTPool"
    EASTMONEY_STOCK_KLINE_API = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    HISTORICAL_BREADTH_WINDOW_DAYS = 60
    HISTORICAL_BREADTH_MAX_WORKERS = 16

    def __init__(
        self,
        session_factory=async_session_maker,
        today_limit_up_fetcher: Optional[ListFetcher] = None,
        yesterday_pool_fetcher: Optional[ListFetcher] = None,
        quote_fetcher: Optional[QuoteFetcher] = None,
        market_stats_fetcher: Optional[NormalizedFetcher] = None,
        current_date_provider: Callable[[], date] = _today_shanghai,
    ) -> None:
        self.session_factory = session_factory
        self.today_limit_up_fetcher = today_limit_up_fetcher or self._fetch_today_limit_up_rows
        self.yesterday_pool_fetcher = yesterday_pool_fetcher or self._fetch_yesterday_pool
        self.quote_fetcher = quote_fetcher or tencent_api.get_quotes_batch
        self.market_stats_fetcher = market_stats_fetcher or self._fetch_market_stats
        self.current_date_provider = current_date_provider
        self._historical_market_stats_cache: Dict[date, Dict[str, Any]] = {}

    async def collect_for_date(self, trade_date: date) -> Dict[str, Any]:
        today_result, yesterday_result, market_stats = await asyncio.gather(
            self.today_limit_up_fetcher(trade_date),
            self.yesterday_pool_fetcher(trade_date),
            self.market_stats_fetcher(trade_date),
        )
        today_rows, today_succeeded = self._unwrap_list_fetch_result(today_result)
        yesterday_pool, yesterday_succeeded = self._unwrap_list_fetch_result(yesterday_result)

        if not today_succeeded or not yesterday_succeeded:
            logger.warning(
                "Market review source incomplete for {}: today_succeeded={}, yesterday_succeeded={}",
                trade_date,
                today_succeeded,
                yesterday_succeeded,
            )
            return self._placeholder_payload(trade_date)

        union_codes = {
            item.get("stock_code", "")
            for item in today_rows
            if item.get("stock_code")
        }
        union_codes.update(
            item.get("c", "")
            for item in yesterday_pool
            if item.get("c")
        )
        union_codes.discard("")

        if not union_codes:
            return self._placeholder_payload(trade_date)

        quotes: Dict[str, Dict[str, Any]] = {}
        if trade_date == self.current_date_provider():
            try:
                quotes = await self.quote_fetcher(sorted(union_codes))
            except Exception as exc:
                logger.warning(f"Market review quote fetch failed for {trade_date}: {exc}")

        stock_meta = self._build_stock_meta(today_rows, yesterday_pool, quotes)
        stock_ids = await self._ensure_stock_ids(stock_meta)
        stock_rows = self._build_stock_rows(
            trade_date=trade_date,
            today_rows=today_rows,
            yesterday_pool=yesterday_pool,
            quotes=quotes,
            stock_ids=stock_ids,
        )
        event_rows = self._build_event_rows(
            trade_date=trade_date,
            today_rows=today_rows,
            stock_ids=stock_ids,
        )

        source_status = "primary" if self._market_stats_present(market_stats) else "partial"
        return {
            "trade_date": trade_date,
            "is_authoritative": True,
            "stock_rows": stock_rows,
            "event_rows": event_rows,
            "limit_down_count": int(market_stats.get("limit_down_count", 0) or 0),
            "market_turnover": float(market_stats.get("market_turnover", 0.0) or 0.0),
            "up_count_ex_st": int(market_stats.get("up_count_ex_st", 0) or 0),
            "down_count_ex_st": int(market_stats.get("down_count_ex_st", 0) or 0),
            "source_status": source_status,
        }

    async def _fetch_today_limit_up_rows(self, trade_date: date) -> list[Dict[str, Any]]:
        if trade_date == self.current_date_provider():
            try:
                realtime_rows = await realtime_limit_up_service.get_realtime_limit_up_list(trade_date)
                if realtime_rows:
                    return {
                        "items": realtime_rows,
                        "succeeded": True,
                    }
            except Exception as exc:
                logger.warning(f"Realtime limit-up source failed for {trade_date}: {exc}")

        try:
            history_rows = await em_crawler.crawl(trade_date)
            return {
                "items": history_rows or [],
                "succeeded": True,
            }
        except Exception as exc:
            logger.warning(f"EastMoney limit-up history source failed for {trade_date}: {exc}")
            return {
                "items": [],
                "succeeded": False,
            }

    async def _fetch_yesterday_pool(self, trade_date: date) -> list[Dict[str, Any]]:
        params = {
            "ut": "7eea3edcaed734bea9cbfc24409ed989",
            "dpt": "wz.ztzt",
            "Pageindex": "0",
            "pagesize": "5000",
            "sort": "zs:desc",
            "date": trade_date.strftime("%Y%m%d"),
        }
        headers = {
            "User-Agent": settings.CRAWLER_USER_AGENT,
            "Referer": "https://quote.eastmoney.com/",
        }

        try:
            async with httpx.AsyncClient(
                timeout=settings.CRAWLER_REQUEST_TIMEOUT,
                verify=False,
                headers=headers,
            ) as client:
                response = await client.get(self.YESTERDAY_POOL_API, params=params)
            response.raise_for_status()
            body = response.json()
            return {
                "items": list((body.get("data") or {}).get("pool") or []),
                "succeeded": True,
            }
        except Exception as exc:
            logger.warning(f"Yesterday limit-up pool fetch failed for {trade_date}: {exc}")
            return {
                "items": [],
                "succeeded": False,
            }

    async def _fetch_market_stats(self, trade_date: date) -> Dict[str, Any]:
        stored_stats = await self._load_daily_statistics(trade_date)
        if trade_date != self.current_date_provider():
            try:
                historical_stats = await asyncio.to_thread(
                    self._fetch_historical_market_stats_sync,
                    trade_date,
                )
            except Exception as exc:
                logger.warning(f"Historical market stats fetch failed for {trade_date}: {exc}")
                historical_stats = {}
            return self._merge_market_stats(historical_stats, stored_stats)

        try:
            live_stats = await asyncio.to_thread(self._fetch_live_market_stats_sync)
        except Exception as exc:
            logger.warning(f"Live market snapshot fetch failed for {trade_date}: {exc}")
            live_stats = {}

        if not live_stats:
            return stored_stats

        return self._merge_market_stats(live_stats, stored_stats)

    async def _load_daily_statistics(self, trade_date: date) -> Dict[str, Any]:
        async with self.session_factory() as session:
            result = await session.execute(
                select(DailyStatistics).where(DailyStatistics.trade_date == trade_date)
            )
            stats = result.scalar_one_or_none()

        if stats is None:
            return {}

        return {
            "limit_down_count": int(stats.limit_down_count or 0),
            "market_turnover": 0.0,
            "up_count_ex_st": int(stats.up_count or 0),
            "down_count_ex_st": int(stats.down_count or 0),
        }

    def _fetch_historical_market_stats_sync(self, trade_date: date) -> Dict[str, Any]:
        stats = dict(self._historical_market_stats_cache.get(trade_date) or {})

        try:
            exchange_turnover = self._fetch_exchange_market_turnover_sync(trade_date)
            if exchange_turnover:
                stats["market_turnover"] = exchange_turnover
        except Exception as exc:
            logger.warning(f"Exchange market turnover fetch failed for {trade_date}: {exc}")

        if not self._historical_breadth_present(stats):
            try:
                stats.update(self._fetch_historical_market_breadth_sync(trade_date))
            except Exception as exc:
                logger.warning(f"Historical market breadth fetch failed for {trade_date}: {exc}")

        if stats:
            cached = dict(self._historical_market_stats_cache.get(trade_date) or {})
            cached.update(stats)
            self._historical_market_stats_cache[trade_date] = cached
        return stats

    def _fetch_exchange_market_turnover_sync(self, trade_date: date) -> float:
        import akshare as ak

        trade_date_str = trade_date.strftime("%Y%m%d")
        sse_df = ak.stock_sse_deal_daily(date=trade_date_str)
        szse_df = ak.stock_szse_summary(date=trade_date_str)
        return self._extract_exchange_market_turnover(sse_df, szse_df)

    def _extract_exchange_market_turnover(self, sse_df: Any, szse_df: Any) -> float:
        sse_turnover = self._extract_numeric_from_frame(
            sse_df,
            row_column="单日情况",
            row_value="成交金额",
            value_column="股票",
        )
        szse_turnover_yuan = self._extract_numeric_from_frame(
            szse_df,
            row_column="证券类别",
            row_value="股票",
            value_column="成交金额",
        )
        szse_turnover = szse_turnover_yuan / 100000000 if szse_turnover_yuan else 0.0
        return round(float(sse_turnover or 0.0) + float(szse_turnover or 0.0), 2)

    def _extract_numeric_from_frame(
        self,
        frame: Any,
        row_column: str,
        row_value: str,
        value_column: str,
    ) -> float:
        if frame is None or getattr(frame, "empty", True):
            return 0.0
        if row_column not in frame or value_column not in frame:
            return 0.0

        matches = frame[frame[row_column].astype(str) == row_value]
        if matches.empty:
            return 0.0
        return self._to_float(matches.iloc[0][value_column]) or 0.0

    def _fetch_historical_market_breadth_sync(self, trade_date: date) -> Dict[str, Any]:
        if self._historical_breadth_present(self._historical_market_stats_cache.get(trade_date, {})):
            return self._historical_market_stats_cache[trade_date]

        import akshare as ak

        stock_df = ak.stock_info_a_code_name()
        if stock_df is None or stock_df.empty:
            return {}

        end_date = self._resolve_historical_breadth_end_date(trade_date)
        start_date = trade_date
        stock_rows = [
            {
                "code": self._normalize_code(row.get("code")),
                "name": str(row.get("name") or ""),
            }
            for row in stock_df.to_dict("records")
        ]
        stock_rows = [
            row
            for row in stock_rows
            if row["code"] and row["code"][0] in {"0", "3", "4", "6", "8", "9"}
        ]

        window_stats: Dict[date, Dict[str, Any]] = {}
        with ThreadPoolExecutor(max_workers=self.HISTORICAL_BREADTH_MAX_WORKERS) as executor:
            futures = [
                executor.submit(
                    self._fetch_one_stock_history_stats,
                    row["code"],
                    row["name"],
                    start_date,
                    end_date,
                )
                for row in stock_rows
            ]
            for future in as_completed(futures):
                for row_date, row_stats in future.result().items():
                    target = window_stats.setdefault(
                        row_date,
                        {
                            "limit_down_count": 0,
                            "up_count_ex_st": 0,
                            "down_count_ex_st": 0,
                        },
                    )
                    target["limit_down_count"] += row_stats.get("limit_down_count", 0)
                    target["up_count_ex_st"] += row_stats.get("up_count_ex_st", 0)
                    target["down_count_ex_st"] += row_stats.get("down_count_ex_st", 0)

        for row_date, row_stats in window_stats.items():
            cached = dict(self._historical_market_stats_cache.get(row_date) or {})
            cached.update(row_stats)
            self._historical_market_stats_cache[row_date] = cached

        return self._historical_market_stats_cache.get(trade_date, {})

    def _fetch_one_stock_history_stats(
        self,
        stock_code: str,
        stock_name: str,
        start_date: date,
        end_date: date,
    ) -> Dict[date, Dict[str, int]]:
        import requests

        params = {
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "ut": "7eea3edcaed734bea9cbfc24409ed989",
            "klt": "101",
            "fqt": "0",
            "secid": f"{self._eastmoney_market_code(stock_code)}.{stock_code}",
            "beg": start_date.strftime("%Y%m%d"),
            "end": end_date.strftime("%Y%m%d"),
        }
        try:
            session = requests.Session()
            session.trust_env = False
            response = session.get(
                self.EASTMONEY_STOCK_KLINE_API,
                params=params,
                headers={
                    "User-Agent": settings.CRAWLER_USER_AGENT,
                    "Referer": "https://quote.eastmoney.com/",
                },
                timeout=settings.CRAWLER_REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            klines = ((response.json().get("data") or {}).get("klines") or [])
        except Exception:
            return {}

        stats: Dict[date, Dict[str, int]] = {}
        is_st = self._is_st_name(stock_name)
        for raw_line in klines:
            parts = str(raw_line).split(",")
            if len(parts) < 9:
                continue
            try:
                row_date = datetime.strptime(parts[0], "%Y-%m-%d").date()
            except ValueError:
                continue
            change_pct = self._to_float(parts[8])
            if change_pct is None:
                continue
            row_stats = stats.setdefault(
                row_date,
                {
                    "limit_down_count": 0,
                    "up_count_ex_st": 0,
                    "down_count_ex_st": 0,
                },
            )
            if self._is_limit_down(change_pct, stock_code, stock_name):
                row_stats["limit_down_count"] += 1
            if not is_st and change_pct > 0:
                row_stats["up_count_ex_st"] += 1
            if not is_st and change_pct < 0:
                row_stats["down_count_ex_st"] += 1

        return stats

    def _resolve_historical_breadth_end_date(self, trade_date: date) -> date:
        current_date = self.current_date_provider()
        if current_date <= trade_date:
            return trade_date
        return min(
            current_date,
            trade_date + timedelta(days=self.HISTORICAL_BREADTH_WINDOW_DAYS),
        )

    def _fetch_live_market_stats_sync(self) -> Dict[str, Any]:
        import akshare as ak
        import pandas as pd

        snapshot = ak.stock_zh_a_spot()
        if snapshot is None or snapshot.empty:
            return {}

        names = snapshot.get("名称")
        changes = pd.to_numeric(snapshot.get("涨跌幅"), errors="coerce").fillna(0.0)
        amounts = pd.to_numeric(snapshot.get("成交额"), errors="coerce").fillna(0.0)
        codes = snapshot.get("代码")

        if names is None or codes is None:
            return {}

        non_st_mask = ~names.astype(str).str.contains("ST", na=False)
        limit_down_count = 0
        for raw_code, raw_name, raw_change in zip(codes.tolist(), names.tolist(), changes.tolist()):
            stock_code = self._normalize_code(raw_code)
            if not stock_code:
                continue
            if self._is_limit_down(self._to_float(raw_change), stock_code, str(raw_name or "")):
                limit_down_count += 1

        return {
            "limit_down_count": int(limit_down_count),
            "market_turnover": round(float(amounts.sum()) / 100000000, 2),
            "up_count_ex_st": int(((changes > 0) & non_st_mask).sum()),
            "down_count_ex_st": int(((changes < 0) & non_st_mask).sum()),
        }

    def _merge_market_stats(self, primary: Dict[str, Any], fallback: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "limit_down_count": int(
                primary.get("limit_down_count")
                or fallback.get("limit_down_count", 0)
                or 0
            ),
            "market_turnover": float(
                primary.get("market_turnover")
                or fallback.get("market_turnover", 0.0)
                or 0.0
            ),
            "up_count_ex_st": int(
                primary.get("up_count_ex_st")
                or fallback.get("up_count_ex_st", 0)
                or 0
            ),
            "down_count_ex_st": int(
                primary.get("down_count_ex_st")
                or fallback.get("down_count_ex_st", 0)
                or 0
            ),
        }

    def _historical_breadth_present(self, stats: Dict[str, Any]) -> bool:
        return any(
            stats.get(field) not in (None, 0, 0.0)
            for field in ("limit_down_count", "up_count_ex_st", "down_count_ex_st")
        )

    def _build_stock_meta(
        self,
        today_rows: Iterable[Dict[str, Any]],
        yesterday_pool: Iterable[Dict[str, Any]],
        quotes: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Dict[str, Any]]:
        meta: Dict[str, Dict[str, Any]] = {}

        for item in today_rows:
            stock_code = item.get("stock_code", "")
            if not stock_code:
                continue
            stock_name = item.get("stock_name", "") or quotes.get(stock_code, {}).get("name", "")
            meta[stock_code] = {
                "stock_code": stock_code,
                "stock_name": stock_name or stock_code,
                "market": self._detect_market(stock_code),
                "is_st": self._is_st_name(stock_name),
                "is_kc": int(stock_code.startswith("688")),
                "is_cy": int(stock_code.startswith("300")),
            }

        for item in yesterday_pool:
            stock_code = item.get("c", "")
            if not stock_code:
                continue
            stock_name = item.get("n", "") or quotes.get(stock_code, {}).get("name", "")
            meta.setdefault(
                stock_code,
                {
                    "stock_code": stock_code,
                    "stock_name": stock_name or stock_code,
                    "market": self._detect_market(stock_code),
                    "is_st": self._is_st_name(stock_name),
                    "is_kc": int(stock_code.startswith("688")),
                    "is_cy": int(stock_code.startswith("300")),
                },
            )

        for stock_code, quote in quotes.items():
            if not stock_code:
                continue
            meta.setdefault(
                stock_code,
                {
                    "stock_code": stock_code,
                    "stock_name": quote.get("name", "") or stock_code,
                    "market": self._detect_market(stock_code),
                    "is_st": self._is_st_name(quote.get("name", "")),
                    "is_kc": int(stock_code.startswith("688")),
                    "is_cy": int(stock_code.startswith("300")),
                },
            )

        return meta

    async def _ensure_stock_ids(self, stock_meta: Dict[str, Dict[str, Any]]) -> Dict[str, int]:
        if not stock_meta:
            return {}

        codes = list(stock_meta.keys())
        async with self.session_factory() as session:
            existing_rows = (
                await session.execute(
                    select(Stock).where(Stock.stock_code.in_(codes))
                )
            ).scalars().all()
            existing_map = {row.stock_code: row for row in existing_rows}

            missing_codes = [code for code in codes if code not in existing_map]
            if missing_codes:
                for code in missing_codes:
                    meta = stock_meta[code]
                    session.add(
                        Stock(
                            stock_code=code,
                            stock_name=meta["stock_name"],
                            market=meta["market"],
                            is_st=int(meta["is_st"]),
                            is_kc=int(meta["is_kc"]),
                            is_cy=int(meta["is_cy"]),
                        )
                    )
                await session.commit()

                existing_rows = (
                    await session.execute(
                        select(Stock).where(Stock.stock_code.in_(codes))
                    )
                ).scalars().all()
                existing_map = {row.stock_code: row for row in existing_rows}

        return {
            code: row.id
            for code, row in existing_map.items()
        }

    def _build_stock_rows(
        self,
        trade_date: date,
        today_rows: list[Dict[str, Any]],
        yesterday_pool: list[Dict[str, Any]],
        quotes: Dict[str, Dict[str, Any]],
        stock_ids: Dict[str, int],
    ) -> list[Dict[str, Any]]:
        today_map = {
            item.get("stock_code", ""): item
            for item in today_rows
            if item.get("stock_code")
        }
        yesterday_map = {
            item.get("c", ""): item
            for item in yesterday_pool
            if item.get("c")
        }

        stock_rows: list[Dict[str, Any]] = []
        for stock_code in sorted(set(today_map) | set(yesterday_map)):
            today_item = today_map.get(stock_code, {})
            yesterday_item = yesterday_map.get(stock_code, {})
            quote = quotes.get(stock_code, {})
            stock_name = (
                today_item.get("stock_name")
                or yesterday_item.get("n")
                or quote.get("name")
                or stock_code
            )
            today_continuous_days = self._to_int(
                today_item.get("continuous_limit_up_days")
                or today_item.get("today_continuous_days")
            )
            yesterday_continuous_days = self._to_int(yesterday_item.get("ylbc"))
            if yesterday_continuous_days == 0 and today_continuous_days > 1:
                yesterday_continuous_days = today_continuous_days - 1

            today_touched_limit_up = bool(today_item)
            today_sealed_close = self._is_sealed(today_item) if today_touched_limit_up else False
            close_price = self._resolve_close_price(today_item, quote, stock_code, stock_name)
            pre_close = self._resolve_pre_close(today_item, quote, stock_code, stock_name)
            change_pct = self._resolve_change_pct(today_item, yesterday_item, quote, close_price, pre_close)

            stock_rows.append(
                {
                    "trade_date": trade_date,
                    "stock_id": stock_ids.get(stock_code),
                    "stock_code": stock_code,
                    "stock_name": stock_name,
                    "board_type": self._detect_board_type(stock_code),
                    "is_st": self._is_st_name(stock_name),
                    "yesterday_limit_up": bool(yesterday_item) or yesterday_continuous_days > 0,
                    "yesterday_continuous_days": yesterday_continuous_days,
                    "today_touched_limit_up": today_touched_limit_up,
                    "today_sealed_close": today_sealed_close,
                    "today_opened_close": today_touched_limit_up and not today_sealed_close,
                    "today_broken": today_touched_limit_up and not today_sealed_close,
                    "today_continuous_days": today_continuous_days,
                    "first_limit_time": self._as_time(today_item.get("first_limit_up_time")),
                    "final_seal_time": self._as_time(today_item.get("final_seal_time")),
                    "open_count": self._to_int(today_item.get("open_count")),
                    "close_price": close_price,
                    "pre_close": pre_close,
                    "change_pct": change_pct,
                    "amount": self._resolve_amount(today_item, quote),
                    "turnover_rate": self._resolve_turnover_rate(today_item, quote),
                    "tradable_market_value": self._to_float(today_item.get("tradable_market_value")),
                    "limit_up_reason": today_item.get("limit_up_reason", "") or None,
                    "data_quality_flag": "ok",
                }
            )

        return stock_rows

    def _build_event_rows(
        self,
        trade_date: date,
        today_rows: list[Dict[str, Any]],
        stock_ids: Dict[str, int],
    ) -> list[Dict[str, Any]]:
        event_rows: list[Dict[str, Any]] = []
        for item in today_rows:
            stock_code = item.get("stock_code", "")
            if not stock_code or stock_code not in stock_ids:
                continue

            source_name = str(item.get("data_source") or "review")[:20]
            stock_name = item.get("stock_name", "") or stock_code
            first_limit_time = self._as_time(item.get("first_limit_up_time"))
            if first_limit_time is not None:
                event_rows.append(
                    {
                        "trade_date": trade_date,
                        "stock_id": stock_ids[stock_code],
                        "stock_code": stock_code,
                        "stock_name": stock_name,
                        "event_type": "first_seal",
                        "event_time": first_limit_time,
                        "event_seq": 1,
                        "source_name": source_name,
                        "payload_json": {
                            "continuous_limit_up_days": self._to_int(
                                item.get("continuous_limit_up_days")
                                or item.get("today_continuous_days")
                            ),
                            "open_count": self._to_int(item.get("open_count")),
                        },
                    }
                )

            event_rows.append(
                {
                    "trade_date": trade_date,
                    "stock_id": stock_ids[stock_code],
                    "stock_code": stock_code,
                    "stock_name": stock_name,
                    "event_type": "close_sealed" if self._is_sealed(item) else "close_opened",
                    "event_time": self._as_time(
                        item.get("final_seal_time")
                        or item.get("first_limit_up_time")
                    ),
                    "event_seq": 2,
                    "source_name": source_name,
                    "payload_json": {
                        "is_sealed": self._is_sealed(item),
                        "open_count": self._to_int(item.get("open_count")),
                    },
                }
            )

        return event_rows

    def _resolve_close_price(
        self,
        today_item: Dict[str, Any],
        quote: Dict[str, Any],
        stock_code: str,
        stock_name: str,
    ) -> Optional[float]:
        for value in (
            quote.get("price"),
            today_item.get("current_price"),
            today_item.get("close_price"),
        ):
            resolved = self._to_float(value)
            if resolved is not None and resolved > 0:
                return resolved

        if self._is_sealed(today_item):
            limit_up_price = self._to_float(today_item.get("limit_up_price"))
            if limit_up_price is not None and limit_up_price > 0:
                return limit_up_price

        return None

    def _resolve_pre_close(
        self,
        today_item: Dict[str, Any],
        quote: Dict[str, Any],
        stock_code: str,
        stock_name: str,
    ) -> Optional[float]:
        for value in (
            quote.get("pre_close"),
            today_item.get("pre_close"),
        ):
            resolved = self._to_float(value)
            if resolved is not None and resolved > 0:
                return resolved

        if not self._is_sealed(today_item):
            return None

        limit_up_price = self._to_float(today_item.get("limit_up_price"))
        if limit_up_price is None or limit_up_price <= 0:
            return None

        ratio = self._limit_ratio(stock_code, stock_name)
        return round(limit_up_price / (1 + ratio), 4)

    def _resolve_change_pct(
        self,
        today_item: Dict[str, Any],
        yesterday_item: Dict[str, Any],
        quote: Dict[str, Any],
        close_price: Optional[float],
        pre_close: Optional[float],
    ) -> Optional[float]:
        for value in (
            quote.get("change_pct"),
            today_item.get("change_pct"),
            yesterday_item.get("zdp"),
        ):
            resolved = self._to_float(value)
            if resolved is not None:
                return round(resolved, 2)

        if close_price is not None and pre_close not in (None, 0):
            return round((close_price - pre_close) / pre_close * 100, 2)
        return None

    def _resolve_amount(self, today_item: Dict[str, Any], quote: Dict[str, Any]) -> float:
        for value in (
            quote.get("amount"),
            today_item.get("amount"),
        ):
            resolved = self._to_float(value)
            if resolved is not None:
                return resolved
        return 0.0

    def _resolve_turnover_rate(self, today_item: Dict[str, Any], quote: Dict[str, Any]) -> Optional[float]:
        for value in (
            quote.get("turnover_rate"),
            today_item.get("turnover_rate"),
        ):
            resolved = self._to_float(value)
            if resolved is not None:
                return resolved
        return None

    def _market_stats_present(self, market_stats: Dict[str, Any]) -> bool:
        return any(
            market_stats.get(field) not in (None, 0, 0.0)
            for field in ("limit_down_count", "market_turnover", "up_count_ex_st", "down_count_ex_st")
        )

    def _unwrap_list_fetch_result(self, result: Any) -> tuple[list[Dict[str, Any]], bool]:
        if isinstance(result, dict) and "items" in result:
            items = result.get("items") or []
            succeeded = bool(result.get("succeeded", False))
            return list(items), succeeded
        return list(result or []), True

    def _placeholder_payload(self, trade_date: date) -> Dict[str, Any]:
        return {
            "trade_date": trade_date,
            "is_authoritative": False,
            "stock_rows": [],
            "event_rows": [],
            "limit_down_count": 0,
            "market_turnover": 0.0,
            "up_count_ex_st": 0,
            "down_count_ex_st": 0,
            "source_status": "placeholder",
        }

    def _detect_market(self, stock_code: str) -> str:
        if stock_code.startswith(("5", "6", "9")):
            return "SH"
        if stock_code.startswith("8"):
            return "BJ"
        return "SZ"

    def _detect_board_type(self, stock_code: str) -> str:
        if stock_code.startswith("688"):
            return "star"
        if stock_code.startswith("300"):
            return "gem"
        if stock_code.startswith("8"):
            return "bj"
        return "main"

    def _is_limit_down(self, change_pct: Optional[float], stock_code: str, stock_name: str) -> bool:
        if change_pct is None:
            return False
        if self._is_st_name(stock_name):
            return change_pct <= -4.8
        if stock_code.startswith("8"):
            return change_pct <= -29.5
        if stock_code.startswith(("300", "688")):
            return change_pct <= -19.5
        return change_pct <= -9.5

    def _limit_ratio(self, stock_code: str, stock_name: str) -> float:
        if self._is_st_name(stock_name):
            return 0.05
        if stock_code.startswith("8"):
            return 0.30
        if stock_code.startswith(("300", "688")):
            return 0.20
        return 0.10

    def _is_sealed(self, item: Dict[str, Any]) -> bool:
        if not item:
            return False
        if "is_sealed" in item:
            return bool(item.get("is_sealed"))
        if "is_final_sealed" in item:
            return bool(item.get("is_final_sealed"))
        return item.get("current_status") == "sealed"

    def _as_time(self, value: Any) -> Optional[time]:
        if value is None:
            return None
        if isinstance(value, time):
            return value
        if isinstance(value, datetime):
            return value.time()
        if isinstance(value, str):
            for fmt in ("%H:%M:%S", "%H%M%S", "%H:%M"):
                try:
                    parsed = datetime.strptime(value, fmt)
                    return parsed.time()
                except ValueError:
                    continue
        return None

    def _normalize_code(self, value: Any) -> str:
        code = str(value or "").strip().lower()
        if code.startswith(("sh", "sz", "bj")):
            code = code[2:]
        return code.zfill(6) if code.isdigit() else ""

    def _eastmoney_market_code(self, stock_code: str) -> int:
        return 1 if stock_code.startswith("6") else 0

    def _is_st_name(self, stock_name: Any) -> bool:
        name = str(stock_name or "")
        return "ST" in name.upper()

    def _to_int(self, value: Any) -> int:
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    def _to_float(self, value: Any) -> Optional[float]:
        if value in (None, ""):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None


market_review_source_service = MarketReviewSourceService()
