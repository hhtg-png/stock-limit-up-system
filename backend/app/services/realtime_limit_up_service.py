"""
高实时涨停数据服务

目标：
- 东方财富只负责提供涨停池/炸板池元数据
- 腾讯负责补充快照行情
- 同花顺负责补充涨停原因
- 热路径不再执行自由流通市值补算
"""
from __future__ import annotations

import asyncio
import copy
import time
from datetime import date, datetime
from typing import Dict, List, Optional

import httpx
from loguru import logger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.crawlers.eastmoney_crawler import em_crawler
from app.crawlers.tonghuashun_crawler import ths_crawler
from app.data_collectors.tencent_api import tencent_api
from app.services.tradable_market_value_service import tradable_market_value_service


class RealtimeLimitUpService:
    """高实时涨停数据聚合服务"""

    def __init__(self):
        self._pool_cache: Dict[date, List[Dict]] = {}
        self._pool_cache_time: Dict[date, float] = {}
        self._pool_refresh_tasks: Dict[date, asyncio.Task] = {}

        self._ths_reason_cache: Dict[str, str] = {}
        self._ths_reason_cache_time: float = 0.0
        self._ths_reason_refresh_task: Optional[asyncio.Task] = None

        self._POOL_CACHE_TTL = 3
        self._POOL_STALE_TTL = 15
        self._THS_CACHE_TTL = 300
        self._THS_STALE_TTL = 1800

    async def get_fast_limit_up_pool(self, trade_date: Optional[date] = None) -> List[Dict]:
        """获取快速涨停池数据，不做自由流通市值补算"""
        if trade_date is None:
            trade_date = date.today()

        now = time.time()
        cached = self._pool_cache.get(trade_date)
        cached_at = self._pool_cache_time.get(trade_date, 0.0)
        age = now - cached_at

        if cached and age < self._POOL_CACHE_TTL:
            return copy.deepcopy(cached)

        if cached and age < self._POOL_STALE_TTL:
            self._ensure_pool_refresh(trade_date)
            return copy.deepcopy(cached)

        data = await self._refresh_pool_cache(trade_date)
        return copy.deepcopy(data)

    async def get_realtime_limit_up_list(self, trade_date: Optional[date] = None) -> List[Dict]:
        """获取高实时涨停列表（EM 元数据 + THS 原因 + 腾讯行情）"""
        if trade_date is None:
            trade_date = date.today()

        raw_data, reason_map = await asyncio.gather(
            self.get_fast_limit_up_pool(trade_date),
            self._fetch_ths_reason_map(),
        )

        if not raw_data:
            return []

        self._enrich_reasons(raw_data, reason_map)

        codes = [item.get("stock_code", "") for item in raw_data if item.get("stock_code")]
        quotes: Dict[str, Dict] = {}
        float_share_map: Dict[str, float] = {}
        if codes:
            quote_task = tencent_api.get_quotes_batch(codes)
            float_share_task = tradable_market_value_service.get_float_share_map(trade_date, codes)
            try:
                quotes, float_share_map = await asyncio.gather(quote_task, float_share_task)
            except Exception as exc:
                logger.warning(f"实时行情或流通股本获取失败，回退到基础数据: {exc}")
                try:
                    quotes = await tencent_api.get_quotes_batch(codes)
                except Exception as quote_exc:
                    logger.warning(f"腾讯行情获取失败，回退到东方财富基础数据: {quote_exc}")

        return [
            self._merge_quote(
                item,
                quotes.get(item.get("stock_code", "")),
                float_share_map.get(item.get("stock_code", "")),
            )
            for item in raw_data
        ]

    async def get_realtime_limit_up_item(self, stock_code: str, trade_date: Optional[date] = None) -> Optional[Dict]:
        """获取单只股票的实时涨停详情基础数据"""
        data = await self.get_realtime_limit_up_list(trade_date)
        for item in data:
            if item.get("stock_code") == stock_code:
                return item
        return None

    async def get_monitored_stocks(self, db: AsyncSession) -> List[Dict]:
        """获取用于 L2 监控的当日涨停股票列表，优先使用实时涨停池"""
        from app.models.stock import Stock

        pool_data = await self.get_fast_limit_up_pool(date.today())
        if not pool_data:
            return []

        codes = [item.get("stock_code", "") for item in pool_data if item.get("stock_code")]
        if not codes:
            return []

        result = await db.execute(select(Stock).where(Stock.stock_code.in_(codes)))
        stocks = result.scalars().all()
        stock_map = {stock.stock_code: stock for stock in stocks}

        monitored: List[Dict] = []
        for item in pool_data:
            code = item.get("stock_code", "")
            name = item.get("stock_name", "")
            if not code:
                continue

            stock = stock_map.get(code)
            if stock is None:
                stock = Stock(
                    stock_code=code,
                    stock_name=name,
                    market=self._detect_market(code),
                    is_st="ST" in name or "*ST" in name,
                    is_kc=code.startswith("688"),
                    is_cy=code.startswith("300"),
                )
                db.add(stock)
                await db.flush()
                stock_map[code] = stock
            elif name and stock.stock_name != name:
                stock.stock_name = name

            monitored.append(
                {
                    "id": stock.id,
                    "stock_code": stock.stock_code,
                    "stock_name": stock.stock_name,
                    "market": stock.market,
                }
            )

        return monitored

    def _ensure_pool_refresh(self, trade_date: date):
        task = self._pool_refresh_tasks.get(trade_date)
        if task and not task.done():
            return

        task = asyncio.create_task(self._refresh_pool_cache(trade_date))
        task.add_done_callback(lambda _: self._pool_refresh_tasks.pop(trade_date, None))
        self._pool_refresh_tasks[trade_date] = task

    async def _refresh_pool_cache(self, trade_date: date) -> List[Dict]:
        date_str = trade_date.strftime("%Y%m%d")
        params = {
            "ut": "7eea3edcaed734bea9cbfc24409ed989",
            "dpt": "wz.ztzt",
            "Pageindex": 0,
            "pagesize": 200,
            "sort": "fbt:asc",
            "date": date_str,
            "_": int(time.time() * 1000),
        }

        headers = em_crawler.get_headers()
        headers.update(
            {
                "Referer": "https://quote.eastmoney.com/",
                "Host": "push2ex.eastmoney.com",
            }
        )

        try:
            async with httpx.AsyncClient(
                timeout=settings.CRAWLER_REQUEST_TIMEOUT,
                verify=False,
                headers=headers,
            ) as client:
                sealed_resp, opened_resp = await asyncio.gather(
                    client.get(em_crawler.LIMIT_UP_API, params=params),
                    client.get(em_crawler.BROKEN_API, params=params),
                )

            sealed_resp.raise_for_status()
            opened_resp.raise_for_status()

            sealed_data = em_crawler.parse(sealed_resp.json(), is_sealed=True)
            opened_data = em_crawler.parse(opened_resp.json(), is_sealed=False)
            merged = sealed_data + opened_data

            self._pool_cache[trade_date] = merged
            self._pool_cache_time[trade_date] = time.time()
            self._prune_pool_cache(current_date=trade_date)

            logger.info(
                f"快速涨停池刷新完成: {trade_date} 共 {len(merged)} 条 "
                f"(封板 {len(sealed_data)} / 炸板 {len(opened_data)})"
            )
            return merged
        except Exception as exc:
            logger.warning(f"快速涨停池刷新失败，trade_date={trade_date}: {exc}")
            return self._pool_cache.get(trade_date, [])

    async def _fetch_ths_reason_map(self) -> Dict[str, str]:
        """获取同花顺涨停原因，使用 stale-while-revalidate 缓存"""
        now = time.time()
        age = now - self._ths_reason_cache_time

        if self._ths_reason_cache and age < self._THS_CACHE_TTL:
            return dict(self._ths_reason_cache)

        if self._ths_reason_cache and age < self._THS_STALE_TTL:
            self._ensure_reason_refresh()
            return dict(self._ths_reason_cache)

        await self._refresh_reason_cache()
        return dict(self._ths_reason_cache)

    def _ensure_reason_refresh(self):
        task = self._ths_reason_refresh_task
        if task and not task.done():
            return

        task = asyncio.create_task(self._refresh_reason_cache())
        self._ths_reason_refresh_task = task

    async def _refresh_reason_cache(self):
        try:
            ths_data = await ths_crawler.crawl()
            reason_map = {}
            for item in ths_data:
                code = item.get("stock_code", "")
                reason = item.get("limit_up_reason", "")
                if code and reason:
                    reason_map[code] = reason

            if reason_map:
                self._ths_reason_cache = reason_map
                self._ths_reason_cache_time = time.time()
                logger.info(f"同花顺涨停原因缓存刷新成功: {len(reason_map)} 条")
        except Exception as exc:
            logger.warning(f"同花顺涨停原因刷新失败: {exc}")
        finally:
            self._ths_reason_refresh_task = None
            try:
                await ths_crawler.close_client()
            except Exception:
                pass

    def _merge_quote(
        self,
        item: Dict,
        quote: Optional[Dict],
        float_share: Optional[float] = None,
    ) -> Dict:
        merged = dict(item)
        stock_code = merged.get("stock_code", "")

        if quote:
            current_price = quote.get("price", 0) or merged.get("limit_up_price", 0)
            amount = quote.get("amount", 0) or merged.get("amount", 0)
            turnover_rate = quote.get("turnover_rate", 0) or merged.get("turnover_rate", 0)
        else:
            current_price = merged.get("limit_up_price", 0)
            amount = merged.get("amount", 0)
            turnover_rate = merged.get("turnover_rate", 0)

        is_sealed = bool(merged.get("is_final_sealed", True))
        merged["is_sealed"] = is_sealed
        merged["current_status"] = "sealed" if is_sealed else "opened"
        merged["current_price"] = current_price
        merged["amount"] = amount
        merged["turnover_rate"] = turnover_rate
        if float_share and current_price:
            merged["tradable_market_value"] = round(float_share * current_price, 2)
        else:
            merged["tradable_market_value"] = None
        merged["market"] = self._detect_market(stock_code)
        merged["industry"] = None
        return merged

    def _enrich_reasons(self, raw_data: List[Dict], reason_map: Dict[str, str]):
        if not reason_map:
            return

        for item in raw_data:
            code = item.get("stock_code", "")
            reason = reason_map.get(code)
            if reason:
                item["limit_up_reason"] = reason
                item["reason_category"] = self._classify_reason_simple(reason)

    def _classify_reason_simple(self, reason: str) -> str:
        if not reason:
            return "其他"

        category_keywords = {
            "新能源": ["新能源", "锂电", "光伏", "风电", "储能", "充电桩", "电池", "氢能"],
            "人工智能": ["AI", "人工智能", "算力", "大模型", "机器人", "智能", "DeepSeek"],
            "半导体": ["半导体", "芯片", "集成电路", "封装", "光刻", "晶圆", "存储"],
            "医药医疗": ["医药", "医疗", "生物", "疫苗", "创新药", "器械", "制药"],
            "军工": ["军工", "国防", "航空", "航天", "舰船", "武器"],
            "消费": ["消费", "白酒", "食品", "饮料", "零售", "电商", "酿酒"],
            "金融": ["金融", "银行", "保险", "证券", "券商"],
            "房地产": ["房地产", "地产", "房企", "物业"],
            "数字经济": ["数字经济", "数据", "云计算", "大数据", "信创", "软件"],
            "汽车": ["汽车", "整车", "零部件", "新能源车"],
        }
        for category, keywords in category_keywords.items():
            if any(keyword in reason for keyword in keywords):
                return category
        return "其他"

    def _detect_market(self, stock_code: str) -> str:
        if stock_code.startswith("6"):
            return "SH"
        return "SZ"

    def _prune_pool_cache(self, current_date: date):
        for cached_date in list(self._pool_cache.keys()):
            if cached_date != current_date:
                self._pool_cache.pop(cached_date, None)
                self._pool_cache_time.pop(cached_date, None)


realtime_limit_up_service = RealtimeLimitUpService()
