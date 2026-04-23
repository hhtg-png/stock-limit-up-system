"""
基于自由流通股口径计算实际流通值。

优先级：
1. Tushare daily_basic.free_share（官方自由流通股本，单位：万股）
2. 东方财富 F10 十大流通股东估算（剔除 >=5% 核心股东持股）

输出：
- get_float_share_map 返回自由流通股本，单位：万股
- 实际流通值由调用方使用 最新价(元) × 自由流通股本(万股) 计算，结果单位为万元
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Dict, Iterable, List, Optional

import httpx
from loguru import logger

from app.config import settings


class TradableMarketValueService:
    """按交易日缓存自由流通股本（万股）"""

    def __init__(self):
        self._cache: Dict[date, Dict[str, float]] = {}
        self._f10_cache: Dict[date, Dict[str, float]] = {}

    async def get_float_share_map(self, trade_date: date, stock_codes: Iterable[str]) -> Dict[str, float]:
        """获取指定股票的自由流通股本映射，单位：万股"""
        codes = {code for code in stock_codes if code}
        if not codes:
            return {}

        result: Dict[str, float] = {}

        cached = self._cache.get(trade_date)
        if cached is None and settings.TUSHARE_TOKEN:
            cached = await self._fetch_latest_tushare_free_share_map(trade_date)
            self._cache = {trade_date: cached}

        if cached:
            result.update({
                code: cached[code]
                for code in codes
                if code in cached
            })

        missing_codes = [code for code in codes if code not in result]
        if missing_codes:
            f10_cached = self._f10_cache.get(trade_date, {})
            missing_fetch_codes = [code for code in missing_codes if code not in f10_cached]
            if missing_fetch_codes:
                fetched = await self._fetch_f10_estimated_free_share_map(missing_fetch_codes)
                f10_cached = {**f10_cached, **fetched}
                self._f10_cache = {trade_date: f10_cached}

            result.update({
                code: f10_cached[code]
                for code in missing_codes
                if code in f10_cached
            })

        return result

    async def _fetch_latest_tushare_free_share_map(self, trade_date: date) -> Dict[str, float]:
        """获取最近可用交易日的 Tushare 自由流通股本，并补上当日解禁"""
        for offset in range(0, 8):
            candidate_date = trade_date - timedelta(days=offset)
            share_map = await self._fetch_trade_date_free_share_map(candidate_date)
            if not share_map:
                continue

            if candidate_date < trade_date:
                unlocked = await self._fetch_share_float_adjustment_map(trade_date)
                for code, float_share in unlocked.items():
                    share_map[code] = round(share_map.get(code, 0.0) + float_share, 4)

            return share_map

        return {}

    async def _fetch_trade_date_free_share_map(self, trade_date: date) -> Dict[str, float]:
        payload = {
            "api_name": "daily_basic",
            "token": settings.TUSHARE_TOKEN,
            "params": {
                "trade_date": trade_date.strftime("%Y%m%d"),
            },
            "fields": "ts_code,free_share",
        }
        return await self._post_tushare_map(payload, value_field_index=1, log_name="daily_basic.free_share")

    async def _fetch_share_float_adjustment_map(self, trade_date: date) -> Dict[str, float]:
        payload = {
            "api_name": "share_float",
            "token": settings.TUSHARE_TOKEN,
            "params": {
                "float_date": trade_date.strftime("%Y%m%d"),
            },
            "fields": "ts_code,float_share",
        }
        raw_map = await self._post_tushare_map(payload, value_field_index=1, log_name="share_float.float_share")
        return {
            code: round(value / 10000, 4)
            for code, value in raw_map.items()
        }

    async def _post_tushare_map(self, payload: dict, value_field_index: int, log_name: str) -> Dict[str, float]:
        try:
            async with httpx.AsyncClient(timeout=settings.CRAWLER_REQUEST_TIMEOUT) as client:
                response = await client.post(settings.TUSHARE_API_URL, json=payload)

            response.raise_for_status()
            body = response.json()

            if body.get("code") != 0:
                logger.warning(
                    f"Tushare {log_name} 请求失败: code={body.get('code')} msg={body.get('msg')}"
                )
                return {}

            items = (body.get("data") or {}).get("items") or []
            result: Dict[str, float] = {}
            for row in items:
                if len(row) <= value_field_index:
                    continue

                ts_code = row[0]
                field_value = row[value_field_index]
                if not ts_code or field_value in (None, ""):
                    continue

                stock_code = str(ts_code).split(".", 1)[0]
                try:
                    result[stock_code] = float(field_value)
                except (TypeError, ValueError):
                    continue

            logger.info(f"Tushare {log_name} 加载完成: {len(result)} 条")
            return result
        except Exception as exc:
            logger.warning(f"Tushare {log_name} 加载失败: {exc}")
            return {}

    async def _fetch_f10_estimated_free_share_map(self, stock_codes: Iterable[str]) -> Dict[str, float]:
        """从东方财富 F10 十大流通股东估算自由流通股本，单位：万股"""
        result: Dict[str, float] = {}
        headers = {
            "User-Agent": settings.CRAWLER_USER_AGENT,
            "Referer": "https://emweb.securities.eastmoney.com/",
        }

        async with httpx.AsyncClient(timeout=settings.CRAWLER_REQUEST_TIMEOUT, headers=headers) as client:
            for stock_code in stock_codes:
                code_fmt = self._to_em_code(stock_code)
                if not code_fmt:
                    continue

                try:
                    response = await client.get(
                        "https://emweb.securities.eastmoney.com/PC_HSF10/ShareholderResearch/PageAjax",
                        params={"code": code_fmt},
                    )
                    response.raise_for_status()
                    f10_data = response.json()
                    sdltgd = f10_data.get("sdltgd", [])
                    if not sdltgd:
                        continue

                    top1 = sdltgd[0]
                    top1_hold = top1.get("HOLD_NUM", 0) or 0
                    top1_ratio = self._to_float(top1.get("FREE_HOLDNUM_RATIO"))
                    if top1_hold <= 0 or top1_ratio <= 0:
                        continue

                    circulation_shares = top1_hold / (top1_ratio / 100)
                    major_holder_shares = 0.0
                    for holder in sdltgd:
                        ratio = self._to_float(holder.get("FREE_HOLDNUM_RATIO"))
                        if ratio >= 5.0:
                            major_holder_shares += float(holder.get("HOLD_NUM", 0) or 0)

                    free_shares = circulation_shares - major_holder_shares
                    if free_shares > 0:
                        result[stock_code] = round(free_shares / 10000, 4)
                except Exception as exc:
                    logger.debug(f"F10 自由流通股本估算失败 {stock_code}: {exc}")

        return result

    def _to_em_code(self, stock_code: str) -> str:
        if stock_code.startswith(("0", "3")):
            return f"SZ{stock_code}"
        if stock_code.startswith(("5", "6", "9")):
            return f"SH{stock_code}"
        if stock_code.startswith("8"):
            return f"BJ{stock_code}"
        return ""

    def _to_float(self, value: object) -> float:
        try:
            return float(value or 0)
        except (TypeError, ValueError):
            return 0.0


tradable_market_value_service = TradableMarketValueService()
