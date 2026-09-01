"""Tongdaxin black-theme watch plugin aggregation service."""
from __future__ import annotations

import copy
import contextlib
import asyncio
import logging
import math
import time as time_module
from collections import defaultdict
from datetime import date, datetime, time
from typing import Any, Dict, Iterable, List, Optional, Tuple

from sqlalchemy import desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.limit_up import LimitUpRecord
from app.models.stock import Stock
from app.models.tdx_cache import TdxStockMoveCache
from app.data_collectors.tencent_api import tencent_api
from app.services.local_topic_knowledge_service import LocalTopicKnowledgeService
from app.services.tdx_attribution_sources import (
    PublicStockAttribution,
    public_attribution_provider,
)
from app.services.tdx_external_sources import ExternalStockMove, public_stock_move_provider
from app.services.tdx_news_sources import public_market_news_provider
from app.services.realtime_limit_up_service import realtime_limit_up_service
from app.utils.market_data_sanitizer import normalize_change_pct


logger = logging.getLogger(__name__)


class TdxPluginService:
    """Build stable payloads for the Tongdaxin embedded plugin pages."""

    def __init__(
        self,
        *,
        external_move_provider=None,
        attribution_provider=None,
        enable_external_sources: bool = False,
        news_provider=None,
        stock_move_cache_ttl: int = 300,
        stock_move_cache_max: int = 500,
        stock_move_live_timeout: float = 0.9,
        plate_strength_history_cache_ttl: float = 60.0,
        topic_knowledge_service=None,
        quote_fetcher=None,
        plate_constituent_fetcher=None,
        plate_constituent_cache_ttl: float = 3600.0,
        plate_constituent_error_cache_ttl: float = 30.0,
        plate_constituent_cache_max: int = 128,
    ):
        self.realtime_limit_up_service = realtime_limit_up_service
        self.external_move_provider = external_move_provider
        self.attribution_provider = attribution_provider
        self.enable_external_sources = enable_external_sources
        self.news_provider = news_provider
        self.stock_move_cache_ttl = stock_move_cache_ttl
        self.stock_move_cache_max = stock_move_cache_max
        self.stock_move_live_timeout = stock_move_live_timeout
        self.plate_strength_history_cache_ttl = plate_strength_history_cache_ttl
        self.topic_knowledge_service = topic_knowledge_service or LocalTopicKnowledgeService()
        self.quote_fetcher = quote_fetcher or tencent_api.get_quotes_batch
        self.plate_constituent_fetcher = (
            plate_constituent_fetcher or self._fetch_eastmoney_plate_constituents
        )
        self.plate_constituent_cache_ttl = max(0.0, float(plate_constituent_cache_ttl))
        self.plate_constituent_error_cache_ttl = max(
            0.0,
            float(plate_constituent_error_cache_ttl),
        )
        self.plate_constituent_cache_max = max(1, int(plate_constituent_cache_max))
        self._stock_move_payload_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
        self._plate_strength_history_cache: Dict[
            Tuple[str, str, int], Tuple[float, List[Dict[str, Any]]]
        ] = {}
        self._plate_constituent_fallback_cache: Dict[
            str, Tuple[float, Dict[str, Any], Optional[str]]
        ] = {}

    async def get_limit_up_live(self, trade_date: Optional[date] = None, db: Optional[AsyncSession] = None) -> Dict[str, Any]:
        target_date = trade_date or date.today()
        warnings: List[str] = []
        source_status = {"limit_up_pool": "ok", "ths_reason": "ok", "tencent_quote": "ok"}
        is_cache = False

        try:
            raw_items = await self.realtime_limit_up_service.get_realtime_limit_up_list(target_date)
        except Exception as exc:
            raw_items = []
            source_status["limit_up_pool"] = "error"
            warnings.append(f"涨停池获取失败: {exc}")

        if not raw_items:
            if source_status.get("limit_up_pool") == "ok":
                source_status["limit_up_pool"] = "empty"
            try:
                raw_items = await self._load_limit_up_records_from_db(target_date, db)
            except Exception as exc:
                raw_items = []
                source_status["limit_up_db"] = "error"
                warnings.append(f"数据库涨停记录兜底失败: {exc}")
            else:
                if raw_items:
                    is_cache = True
                    source_status["limit_up_db"] = "ok"
                    warnings.append("实时涨停池暂无数据，已使用数据库兜底涨停记录")
                elif db is not None:
                    source_status["limit_up_db"] = "empty"

        external_moves = await self._load_external_review_moves(target_date, source_status, warnings)
        external_by_code = {move.stock_code: move for move in external_moves}
        public_attributions = await self._load_public_attributions(raw_items, source_status, warnings)
        history_labels = await self._load_historical_status_labels(raw_items, target_date, db)
        items = [
            self._build_limit_up_event(
                item,
                target_date,
                status_label=history_labels.get(item.get("stock_code", "")),
                external_move=external_by_code.get(item.get("stock_code", "")),
                public_attribution=public_attributions.get(item.get("stock_code", "")),
            )
            for item in raw_items
        ]
        items.sort(key=lambda item: (item.get("event_time") or "00:00:00", item.get("board", 0)), reverse=True)
        if not items and not warnings:
            warnings.append(f"{target_date.isoformat()} 暂无涨停播报数据")

        payload = self._plugin_payload(items, target_date, source_status, is_cache=is_cache, warnings=warnings)
        payload["plate_filters"] = self._build_plate_filters(items)
        return payload

    async def get_limit_up_live_status(self, trade_date: Optional[date] = None) -> Dict[str, Any]:
        """Return fast-changing limit-up status without slow attribution enrichment.

        This mirrors the target plugin split: rich list data is a slower snapshot,
        while seal/open/amount fields are refreshed through a lightweight path.
        """
        target_date = trade_date or date.today()
        warnings: List[str] = []
        source_status = {
            "limit_up_pool": "ok",
            "limit_up_status": "ok",
            "public_attribution": "skipped",
            "review_source": "skipped",
        }

        try:
            raw_items = await self.realtime_limit_up_service.get_fast_limit_up_pool(
                target_date,
                wait_for_refresh=False,
                max_cache_age=1,
            )
        except Exception as exc:
            raw_items = []
            source_status["limit_up_pool"] = "error"
            source_status["limit_up_status"] = "error"
            warnings.append(f"实时涨停状态获取失败: {exc}")

        if not raw_items and source_status["limit_up_pool"] == "ok":
            source_status["limit_up_pool"] = "empty"
            source_status["limit_up_status"] = "empty"
            warnings.append(f"{target_date.isoformat()} 暂无实时涨停状态数据")

        items = [
            self._build_limit_up_event(item, target_date)
            for item in raw_items
        ]
        items.sort(key=lambda item: (item.get("event_time") or "00:00:00", item.get("board", 0)), reverse=True)
        payload = self._plugin_payload(items, target_date, source_status, is_cache=False, warnings=warnings)
        payload["plate_filters"] = self._build_plate_filters(items)
        return payload

    async def get_stock_move(
        self,
        stock_code: str,
        trade_date: Optional[date] = None,
        *,
        source_scope: str = "mixed",
        db: Optional[AsyncSession] = None,
        force_refresh: bool = False,
    ) -> Dict[str, Any]:
        target_date = await self._resolve_trade_date(trade_date, db)
        normalized_code = self._normalize_code(stock_code)
        cache_key = self._stock_move_cache_key(normalized_code, source_scope, target_date)
        if not force_refresh:
            persistent_payload = await self._read_persistent_stock_move_cache(
                db,
                normalized_code,
                source_scope,
                target_date,
            )
            if persistent_payload:
                return persistent_payload

            cached_payload = self._read_stock_move_payload_cache(cache_key)
            if cached_payload:
                cached_payload["is_cache"] = True
                cached_payload.setdefault("source_status", {})["stock_move_cache"] = "hit"
                return cached_payload

        warnings: List[str] = []
        source_status = {"stock_move": "ok"}
        external_task = asyncio.create_task(
            self._load_external_stock_move(normalized_code, target_date, source_status, warnings)
            if source_scope != "ths"
            else self._empty_external_stock_move()
        )
        limit_task = asyncio.create_task(
            self._load_realtime_stock_move_item(normalized_code, target_date, source_status, warnings)
        )
        limit_task_started_at = time_module.monotonic()
        external_move = await external_task
        limit_up_item = await self._await_stock_move_live_item(
            limit_task,
            has_external_move=external_move is not None,
            source_status=source_status,
            started_at=limit_task_started_at,
        )

        if not limit_up_item and not external_move:
            source_status["stock_move"] = "empty"
            warnings.append(f"{normalized_code} 暂无异动解析数据")
            payload = self._plugin_payload(
                [self._empty_stock_move(normalized_code, source_scope)],
                target_date,
                source_status,
                is_cache=False,
                warnings=warnings,
            )
            self._store_stock_move_payload_cache(cache_key, payload)
            await self._store_persistent_stock_move_cache(db, normalized_code, source_scope, target_date, payload)
            return payload

        if not limit_up_item and external_move:
            item = self._build_stock_move_from_external(external_move, normalized_code, source_scope, target_date)
            payload = self._plugin_payload([item], external_move.trade_date or target_date, source_status, is_cache=False, warnings=warnings)
            self._store_stock_move_payload_cache(cache_key, payload)
            await self._store_persistent_stock_move_cache(db, normalized_code, source_scope, target_date, payload)
            return payload

        item = self._build_stock_move_item(limit_up_item, normalized_code, source_scope, target_date, external_move=external_move)
        payload = self._plugin_payload([item], target_date, source_status, is_cache=False, warnings=warnings)
        self._store_stock_move_payload_cache(cache_key, payload)
        await self._store_persistent_stock_move_cache(db, normalized_code, source_scope, target_date, payload)
        return payload

    async def refresh_stock_move_cache(
        self,
        stock_code: str,
        trade_date: date,
        *,
        db: AsyncSession,
        source_scope: str = "mixed",
    ) -> Dict[str, Any]:
        """Refresh a stock movement cache entry without serving stale cache first."""
        return await self.get_stock_move(
            stock_code,
            trade_date,
            source_scope=source_scope,
            db=db,
            force_refresh=True,
        )

    async def get_cached_stock_move_reason(
        self,
        stock_code: str,
        trade_date: Optional[date] = None,
        *,
        source_scope: str = "mixed",
        db: Optional[AsyncSession] = None,
    ) -> Optional[str]:
        """Return a cached stock-move reason without touching slow external sources."""
        target_date = trade_date or date.today()
        normalized_code = self._normalize_code(stock_code)
        cache_key = self._stock_move_cache_key(normalized_code, source_scope, target_date)

        cached_payload = self._read_stock_move_payload_cache(cache_key)
        cached_reason = self._stock_move_payload_reason_title(cached_payload)
        if cached_reason:
            return cached_reason

        persistent_payload = await self._read_persistent_stock_move_cache(
            db,
            normalized_code,
            source_scope,
            target_date,
        )
        return self._stock_move_payload_reason_title(persistent_payload)

    async def get_plate_strength(
        self,
        trade_date: Optional[date] = None,
        db: Optional[AsyncSession] = None,
        *,
        source: str = "kpl",
        window_days: int = 20,
    ) -> Dict[str, Any]:
        target_date = trade_date or date.today()
        source = "ths" if source == "ths" else "kpl"
        window_days = max(5, min(120, int(window_days or 20)))
        warnings: List[str] = []
        source_status = {
            "limit_up_pool": "ok",
            "plate_strength": "ok",
            "plate_history": "ok",
        }
        is_cache = False

        try:
            raw_items = await self.realtime_limit_up_service.get_fast_limit_up_pool(
                target_date,
                wait_for_refresh=False,
                max_cache_age=2,
            )
        except Exception as exc:
            raw_items = []
            source_status["limit_up_pool"] = "error"
            logger.warning("Failed to load realtime plate-strength pool", exc_info=exc)
            warnings.append("板块强度实时数据获取失败")

        if source == "ths":
            source_status["plate_source"] = "ths"
            reason_map: Dict[str, str] = {}
            try:
                reason_map = await self.realtime_limit_up_service._fetch_ths_reason_map()
            except Exception as exc:
                source_status["ths_reason"] = "error"
                logger.warning("Failed to load THS plate-strength reasons", exc_info=exc)
                warnings.append("同花顺题材原因获取失败")
            else:
                source_status["ths_reason"] = "ok" if reason_map else "empty"
            raw_items = [
                {
                    **item,
                    "limit_up_reason": reason_map.get(item.get("stock_code", ""))
                    or item.get("limit_up_reason")
                    or item.get("reason_category")
                    or "",
                }
                for item in raw_items
            ]
        else:
            source_status["plate_source"] = "eastmoney_kpl_compatible"
            warnings.append("开盘啦公开实时接口不可用，当前使用东方财富板块分类兼容口径")

        if not raw_items:
            if source_status["limit_up_pool"] == "ok":
                source_status["limit_up_pool"] = "empty"
            try:
                raw_items = await self._load_limit_up_records_from_db(target_date, db)
            except Exception as exc:
                source_status["limit_up_db"] = "error"
                logger.warning("Failed to load plate-strength DB fallback", exc_info=exc)
                warnings.append("数据库板块强度兜底失败")
            else:
                if raw_items:
                    is_cache = True
                    source_status["limit_up_db"] = "ok"
                    warnings.append("实时涨停池暂无数据，已使用数据库板块强度快照")
                elif db is not None:
                    source_status["limit_up_db"] = "empty"

        items = self._build_plate_strength_items(raw_items, source)
        if not items:
            source_status["plate_strength"] = "empty"
            if not any("暂无" in warning for warning in warnings):
                warnings.append(f"{target_date.isoformat()} 暂无实时板块题材强度数据")

        try:
            history = await self._get_cached_plate_strength_history(
                target_date,
                db,
                source=source,
                window_days=window_days,
            )
        except Exception as exc:
            history = []
            source_status["plate_history"] = "error"
            logger.warning("Failed to load plate-strength history", exc_info=exc)
            warnings.append("板块强度历史趋势获取失败")

        current_point = {
            "trade_date": target_date.isoformat(),
            "items": copy.deepcopy(items),
        }
        history = [point for point in history if point["trade_date"] != current_point["trade_date"]]
        if items:
            history.append(current_point)
        history = history[-window_days:]
        self._annotate_plate_strength_changes(history)
        if items:
            current_history = next(
                (point for point in history if point["trade_date"] == current_point["trade_date"]),
                None,
            )
            if current_history is not None:
                items = copy.deepcopy(current_history["items"])

        payload = self._plugin_payload(items, target_date, source_status, is_cache=is_cache, warnings=warnings)
        payload.update({
            "source": source,
            "window_days": window_days,
            "history": history,
            "summary": {
                "plate_count": len(items),
                "limit_up_count": sum(item["limit_up_count"] for item in items),
                "sealed_count": sum(item["sealed_count"] for item in items),
                "total_seal_amount": round(sum(item["total_seal_amount"] for item in items), 2),
            },
        })
        return payload

    async def get_plate_constituents(
        self,
        plate_name: str,
        trade_date: Optional[date] = None,
        *,
        source: str = "kpl",
    ) -> Dict[str, Any]:
        target_date = trade_date or date.today()
        normalized_plate = str(plate_name or "").strip()[:40]
        source = "ths" if source == "ths" else "kpl"
        warnings: List[str] = []
        source_status = {
            "topic_knowledge": "ok",
            "tencent_quote": "ok",
            "limit_up_pool": "ok",
        }
        constituent_source = "local_topic_knowledge"
        source_label = "同花顺" if source == "ths" else "开盘啦"
        source_note = f"本地题材映射；{source_label}仅用于板块命名与强度口径"

        try:
            candidates = self.topic_knowledge_service.find_stocks_by_topic(normalized_plate)
        except Exception as exc:
            logger.warning("Failed to resolve plate constituents", exc_info=exc)
            candidates = []
            source_status["topic_knowledge"] = "error"
            warnings.append("板块成分股匹配失败")
        else:
            if not candidates:
                source_status["topic_knowledge"] = "empty"

        if not candidates:
            try:
                fallback_payload = await self._get_cached_plate_constituent_fallback(normalized_plate)
            except Exception as exc:
                logger.warning("Failed to load Eastmoney plate constituents", exc_info=exc)
                fallback_payload = {}
                source_status["eastmoney_constituents"] = "error"
                warnings.append("板块成分源不可用：本地题材映射未命中，东方财富板块兜底失败")
            else:
                candidates = list((fallback_payload or {}).get("items") or [])
                source_status["eastmoney_constituents"] = "ok" if candidates else "empty"
                if candidates:
                    matched_plate = str(fallback_payload.get("matched_plate") or normalized_plate)
                    constituent_source = "eastmoney_board"
                    source_note = (
                        f"东方财富“{matched_plate}”板块；"
                        f"{source_label}仅用于板块命名与强度口径"
                    )
                else:
                    warnings.append(f"暂未匹配到“{normalized_plate}”的板块成分股")

        if not candidates:
            constituent_source = "unavailable"
            source_note = "本地题材映射与东方财富兼容板块均未匹配"

        try:
            limit_up_items = await self.realtime_limit_up_service.get_fast_limit_up_pool(
                target_date,
                wait_for_refresh=False,
                max_cache_age=2,
            )
        except Exception as exc:
            logger.warning("Failed to load intraday leader context", exc_info=exc)
            limit_up_items = []
            source_status["limit_up_pool"] = "error"
            warnings.append("日内龙头封板信息获取失败，已按实时涨幅排序")

        existing_codes = {
            self._normalize_code(item.get("stock_code", ""))
            for item in candidates
            if self._normalize_code(item.get("stock_code", ""))
        }
        supplemented_count = 0
        for limit_up_item in limit_up_items:
            stock_code = self._normalize_code(limit_up_item.get("stock_code", ""))
            if (
                not stock_code
                or stock_code in existing_codes
                or self._plate_strength_name(limit_up_item, source) != normalized_plate
            ):
                continue
            candidates.append({
                "stock_code": stock_code,
                "stock_name": limit_up_item.get("stock_name") or stock_code,
                "market": limit_up_item.get("market") or "",
                "match_reason": f"{source_label}日内涨停池补齐",
            })
            existing_codes.add(stock_code)
            supplemented_count += 1

        if supplemented_count:
            source_status["intraday_constituents"] = "supplemented"
            if constituent_source == "unavailable":
                constituent_source = "intraday_limit_up_pool"
                source_note = f"{source_label}日内涨停池匹配；仅包含当日涨停或炸板股票"
            else:
                source_note = f"{source_note}；日内涨停池补齐 {supplemented_count} 只"
        else:
            source_status["intraday_constituents"] = "unchanged"

        codes = list(dict.fromkeys(
            code
            for code in (self._normalize_code(item.get("stock_code", "")) for item in candidates)
            if code
        ))
        try:
            quotes = (await self.quote_fetcher(codes) if codes else {}) or {}
        except Exception as exc:
            logger.warning("Failed to load plate constituent quotes", exc_info=exc)
            quotes = {}
            source_status["tencent_quote"] = "error"
            warnings.append("板块成分股实时行情获取失败")
        else:
            quoted_count = sum(1 for code in codes if quotes.get(code))
            if not codes:
                source_status["tencent_quote"] = "skipped"
            elif quoted_count == 0:
                source_status["tencent_quote"] = "error"
                warnings.append(f"板块成分股实时行情未返回（0/{len(codes)}）")
            elif quoted_count < len(codes):
                source_status["tencent_quote"] = "partial"
                warnings.append(f"板块成分股实时行情仅返回 {quoted_count}/{len(codes)} 只")

        quoted_count = sum(1 for code in codes if quotes.get(code))
        limit_up_by_code = {
            self._normalize_code(item.get("stock_code", "")): item
            for item in limit_up_items
            if self._normalize_code(item.get("stock_code", ""))
        }
        items = [
            self._build_plate_constituent_item(
                candidate,
                quotes.get(self._normalize_code(candidate.get("stock_code", "")), {}),
                limit_up_by_code.get(self._normalize_code(candidate.get("stock_code", ""))),
            )
            for candidate in candidates
        ]
        self._annotate_intraday_tags(items)
        items.sort(
            key=lambda item: (
                item["change_pct"] is None,
                -(item["change_pct"] or 0.0),
                -item["amount"],
                item["stock_code"],
            )
        )

        payload = self._plugin_payload(items, target_date, source_status, is_cache=False, warnings=warnings)
        payload.update({
            "plate_name": normalized_plate,
            "source": source,
            "constituent_source": constituent_source,
            "source_note": source_note,
            "summary": {
                "stock_count": len(items),
                "quoted_count": quoted_count,
                "up_count": sum(1 for item in items if (item["change_pct"] or 0) > 0),
                "down_count": sum(1 for item in items if (item["change_pct"] or 0) < 0),
                "flat_count": sum(1 for item in items if item["change_pct"] == 0),
                "limit_up_count": sum(1 for item in items if item["is_limit_up"]),
            },
        })
        return payload

    async def _get_cached_plate_constituent_fallback(self, plate_name: str) -> Dict[str, Any]:
        cache_key = self._normalize_plate_lookup_name(plate_name)
        now = time_module.monotonic()
        cached = self._plate_constituent_fallback_cache.get(cache_key)
        if cached:
            cache_ttl = (
                self.plate_constituent_error_cache_ttl
                if cached[2]
                else self.plate_constituent_cache_ttl
            )
            if cache_ttl > 0 and now - cached[0] <= cache_ttl:
                if cached[2]:
                    raise RuntimeError(cached[2])
                return copy.deepcopy(cached[1])

        try:
            payload = await self.plate_constituent_fetcher(plate_name)
        except Exception as exc:
            error_message = str(exc).strip()[:200] or exc.__class__.__name__
            self._plate_constituent_fallback_cache[cache_key] = (now, {}, error_message)
            self._trim_plate_constituent_fallback_cache()
            raise
        normalized_payload = copy.deepcopy(payload or {})
        self._plate_constituent_fallback_cache[cache_key] = (now, normalized_payload, None)
        self._trim_plate_constituent_fallback_cache()
        return copy.deepcopy(normalized_payload)

    def _trim_plate_constituent_fallback_cache(self) -> None:
        while len(self._plate_constituent_fallback_cache) > self.plate_constituent_cache_max:
            oldest_key = next(iter(self._plate_constituent_fallback_cache))
            self._plate_constituent_fallback_cache.pop(oldest_key, None)

    async def _fetch_eastmoney_plate_constituents(self, plate_name: str) -> Dict[str, Any]:
        """Resolve an Eastmoney industry/concept board without blocking the event loop."""
        return await asyncio.wait_for(
            asyncio.to_thread(self._fetch_eastmoney_plate_constituents_sync, plate_name),
            timeout=10.0,
        )

    @classmethod
    def _fetch_eastmoney_plate_constituents_sync(cls, plate_name: str) -> Dict[str, Any]:
        import akshare as ak

        normalized_query = cls._normalize_plate_lookup_name(plate_name)
        if not normalized_query:
            return {"items": [], "matched_plate": ""}

        board_candidates: List[Tuple[int, int, int, str, str, str]] = []
        board_sources = (
            ("industry", ak.stock_board_industry_name_em, ak.stock_board_industry_cons_em),
            ("concept", ak.stock_board_concept_name_em, ak.stock_board_concept_cons_em),
        )
        source_errors: List[Exception] = []
        for source_priority, (board_type, name_fetcher, _constituent_fetcher) in enumerate(board_sources):
            try:
                name_frame = name_fetcher()
            except Exception as exc:
                source_errors.append(exc)
                continue
            if name_frame is None or name_frame.empty or "板块名称" not in name_frame.columns:
                source_errors.append(RuntimeError(f"东方财富{board_type}板块列表为空或格式无效"))
                continue
            for _, row in name_frame.iterrows():
                board_name = str(row.get("板块名称") or "").strip()
                normalized_name = cls._normalize_plate_lookup_name(board_name)
                match_priority = cls._plate_lookup_match_priority(normalized_query, normalized_name)
                if match_priority is None:
                    continue
                board_candidates.append((
                    match_priority,
                    source_priority,
                    abs(len(normalized_query) - len(normalized_name)),
                    board_name,
                    str(row.get("板块代码") or "").strip(),
                    board_type,
                ))

        if not board_candidates:
            if source_errors:
                raise RuntimeError("东方财富行业/概念板块列表未完整返回") from source_errors[-1]
            return {"items": [], "matched_plate": ""}

        _, _, _, matched_plate, board_code, board_type = min(board_candidates)
        constituent_fetcher = (
            ak.stock_board_industry_cons_em
            if board_type == "industry"
            else ak.stock_board_concept_cons_em
        )
        frame = constituent_fetcher(board_code or matched_plate)
        if frame is None or frame.empty:
            return {"items": [], "matched_plate": matched_plate}

        items: List[Dict[str, Any]] = []
        for _, row in frame.iterrows():
            stock_code = cls._normalize_code(row.get("代码"))
            if len(stock_code) != 6 or not stock_code.isdigit():
                continue
            items.append({
                "stock_code": stock_code,
                "stock_name": str(row.get("名称") or stock_code).strip(),
                "market": cls._market_from_stock_code(stock_code),
                "match_reason": matched_plate,
            })
        return {"items": items, "matched_plate": matched_plate}

    @staticmethod
    def _normalize_plate_lookup_name(value: Any) -> str:
        return str(value or "").strip().replace(" ", "")[:40]

    @staticmethod
    def _plate_lookup_match_priority(query: str, candidate: str) -> Optional[int]:
        if not query or not candidate:
            return None
        if query == candidate:
            return 0
        if min(len(query), len(candidate)) >= 3 and (query in candidate or candidate in query):
            return 1
        return None

    @staticmethod
    def _market_from_stock_code(stock_code: str) -> str:
        if stock_code.startswith(("4", "8")):
            return "BJ"
        if stock_code.startswith(("5", "6", "9")):
            return "SH"
        return "SZ"

    async def get_news(self, db: Optional[AsyncSession] = None, limit: int = 80) -> Dict[str, Any]:
        warnings: List[str] = []
        source_status: Dict[str, str] = {}
        items: List[Dict[str, Any]] = []

        if self.news_provider:
            try:
                items, provider_status, provider_warnings = await self.news_provider.get_latest_news(limit=limit)
                source_status.update(provider_status)
                warnings.extend(provider_warnings)
            except Exception as exc:
                source_status["market_news"] = "error"
                warnings.append(f"聚合快讯获取失败: {exc}")
        else:
            source_status["market_news"] = "empty"

        if not items:
            source_status.setdefault("market_news", "empty")
            warnings.append("暂无聚合快讯数据")

        return self._plugin_payload(items, date.today(), source_status, is_cache=False, warnings=warnings)

    def compare_samples(
        self,
        *,
        target_items: Iterable[Dict[str, Any]],
        ours_items: Iterable[Dict[str, Any]],
        key_field: str = "stock_code",
    ) -> Dict[str, Any]:
        target_list = list(target_items or [])
        ours_list = list(ours_items or [])
        target_map = {str(item.get(key_field, "")): item for item in target_list if item.get(key_field)}
        ours_map = {str(item.get(key_field, "")): item for item in ours_list if item.get(key_field)}

        missing_keys = [key for key in target_map.keys() if key not in ours_map]
        extra_keys = [key for key in ours_map.keys() if key not in target_map]
        common_keys = [key for key in target_map.keys() if key in ours_map]

        field_diffs: List[Dict[str, Any]] = []
        for key in common_keys:
            target = target_map[key]
            ours = ours_map[key]
            for field in sorted((set(target.keys()) | set(ours.keys())) - {key_field}):
                if target.get(field) != ours.get(field):
                    field_diffs.append({
                        "key": key,
                        "field": field,
                        "target": target.get(field),
                        "ours": ours.get(field),
                    })

        target_positions = {str(item.get(key_field, "")): idx for idx, item in enumerate(target_list) if item.get(key_field)}
        ours_positions = {str(item.get(key_field, "")): idx for idx, item in enumerate(ours_list) if item.get(key_field)}
        order_diffs = [
            {"key": key, "target_index": target_positions[key], "ours_index": ours_positions[key]}
            for key in common_keys
            if target_positions.get(key) != ours_positions.get(key)
        ]

        return {
            "summary": {
                "target_count": len(target_list),
                "ours_count": len(ours_list),
                "missing_count": len(missing_keys),
                "extra_count": len(extra_keys),
                "field_diff_count": len(field_diffs),
                "order_diff_count": len(order_diffs),
            },
            "missing_items": [target_map[key] for key in missing_keys],
            "extra_items": [ours_map[key] for key in extra_keys],
            "field_diffs": field_diffs,
            "order_diffs": order_diffs,
            "updated_at": datetime.now().isoformat(),
        }

    async def _load_external_review_moves(
        self,
        target_date: date,
        source_status: Dict[str, str],
        warnings: List[str],
    ) -> List[ExternalStockMove]:
        if not self.enable_external_sources or not self.external_move_provider:
            return []
        try:
            moves = await self.external_move_provider.get_review_moves(target_date)
        except Exception as exc:
            source_status["lwwhy_review"] = "error"
            warnings.append(f"芦苇复盘异动源获取失败: {exc}")
            return []

        source_status["lwwhy_review"] = "ok" if moves else "empty"
        return moves

    async def _load_external_stock_move(
        self,
        stock_code: str,
        target_date: date,
        source_status: Dict[str, str],
        warnings: List[str],
    ) -> Optional[ExternalStockMove]:
        if not self.enable_external_sources or not self.external_move_provider:
            return None
        try:
            move = await self.external_move_provider.get_stock_move(stock_code, target_date)
        except Exception as exc:
            source_status["lwwhy_move"] = "error"
            warnings.append(f"芦苇复盘个股异动源获取失败: {exc}")
            return None

        source_status["lwwhy_move"] = "ok" if move else "empty"
        return move

    async def _empty_external_stock_move(self) -> Optional[ExternalStockMove]:
        return None

    async def _load_realtime_stock_move_item(
        self,
        stock_code: str,
        target_date: date,
        source_status: Dict[str, str],
        warnings: List[str],
    ) -> Optional[Dict[str, Any]]:
        try:
            return await self.realtime_limit_up_service.get_realtime_limit_up_item(stock_code, target_date)
        except Exception as exc:
            source_status["stock_move"] = "error"
            warnings.append(f"个股异动获取失败: {exc}")
            return None

    async def _await_stock_move_live_item(
        self,
        task: asyncio.Task,
        *,
        has_external_move: bool,
        source_status: Dict[str, str],
        started_at: float,
    ) -> Optional[Dict[str, Any]]:
        if not has_external_move or self.stock_move_live_timeout <= 0:
            return await task

        try:
            timeout = max(0.0, self.stock_move_live_timeout - (time_module.monotonic() - started_at))
            if timeout <= 0 and not task.done():
                raise asyncio.TimeoutError
            return await asyncio.wait_for(task, timeout=timeout)
        except asyncio.TimeoutError:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            source_status["stock_move_live"] = "timeout"
            return None

    async def _load_public_attributions(
        self,
        raw_items: List[Dict[str, Any]],
        source_status: Dict[str, str],
        warnings: List[str],
    ) -> Dict[str, PublicStockAttribution]:
        if not self.enable_external_sources or not self.attribution_provider or not raw_items:
            return {}
        codes = [str(item.get("stock_code", "")) for item in raw_items if item.get("stock_code")]
        if not codes:
            return {}

        try:
            attributions = await self.attribution_provider.get_attributions(codes)
        except Exception as exc:
            source_status["public_attribution"] = "error"
            warnings.append(f"公开题材归因源获取失败: {exc}")
            return {}

        source_status["public_attribution"] = "ok" if attributions else "empty"
        return attributions

    async def _load_limit_up_records_from_db(
        self,
        target_date: date,
        db: Optional[AsyncSession],
    ) -> List[Dict[str, Any]]:
        if db is None:
            return []

        result = await db.execute(
            select(
                Stock.stock_code,
                Stock.stock_name,
                Stock.industry,
                LimitUpRecord.first_limit_up_time,
                LimitUpRecord.limit_up_reason,
                LimitUpRecord.reason_category,
                LimitUpRecord.continuous_limit_up_days,
                LimitUpRecord.open_count,
                LimitUpRecord.is_final_sealed,
                LimitUpRecord.current_status,
                LimitUpRecord.final_seal_time,
                LimitUpRecord.seal_amount,
                LimitUpRecord.amount,
                LimitUpRecord.turnover_rate,
                LimitUpRecord.data_source,
            )
            .join(Stock, LimitUpRecord.stock_id == Stock.id)
            .where(LimitUpRecord.trade_date == target_date)
            .order_by(LimitUpRecord.first_limit_up_time.desc(), Stock.stock_code)
        )

        items: List[Dict[str, Any]] = []
        for row in result.all():
            stock_code = self._normalize_code(row[0])
            is_final_sealed = bool(row[8]) if row[8] is not None else str(row[9] or "").lower() not in {"opened", "broken"}
            items.append({
                "stock_code": stock_code,
                "stock_name": row[1] or stock_code,
                "industry": row[2],
                "first_limit_up_time": row[3],
                "limit_up_reason": row[4] or row[5] or "",
                "reason_category": row[5] or row[2] or "其他",
                "continuous_limit_up_days": int(row[6] or 1),
                "open_count": int(row[7] or 0),
                "is_final_sealed": is_final_sealed,
                "is_sealed": is_final_sealed,
                "current_status": row[9] or ("sealed" if is_final_sealed else "opened"),
                "final_seal_time": row[10],
                "seal_amount": float(row[11] or 0),
                "amount": float(row[12] or 0),
                "turnover_rate": float(row[13] or 0),
                "change_pct": self._fallback_limit_up_change_pct(stock_code, is_final_sealed),
                "data_source": row[14] or "DB",
            })
        return items

    def _build_limit_up_event(
        self,
        item: Dict[str, Any],
        trade_date: date,
        *,
        status_label: Optional[str] = None,
        external_move: Optional[ExternalStockMove] = None,
        public_attribution: Optional[PublicStockAttribution] = None,
    ) -> Dict[str, Any]:
        is_sealed = bool(item.get("is_sealed", item.get("is_final_sealed", True)))
        current_status = item.get("current_status") or ("sealed" if is_sealed else "opened")
        open_count = int(item.get("open_count") or 0)

        if current_status == "opened" or not is_sealed:
            event_type = "limit_up_opened"
            event_label = "涨停打开"
        elif current_status == "resealed" or open_count > 0:
            event_type = "limit_up_resealed"
            event_label = "涨停回封"
        else:
            event_type = "limit_up_sealed"
            event_label = "封死涨停"

        event_time = self._format_time(item.get("first_limit_up_time") or item.get("final_seal_time"))
        base_reason = item.get("limit_up_reason") or item.get("reason_category") or ""
        reason = (
            public_attribution.reason_title
            if public_attribution and public_attribution.reason_title
            else base_reason
            if base_reason
            else external_move.title
            if external_move and external_move.title
            else ""
        )
        target_plate = (
            public_attribution.plate
            if public_attribution and public_attribution.plate
            else external_move.plate
            if external_move and external_move.plate
            else self._target_plate(reason)
        )
        sources = ["东方财富", "同花顺", "腾讯行情"]
        if public_attribution:
            sources = self._dedupe_sources([public_attribution.source_name, *sources])
        if external_move:
            sources = self._dedupe_sources([external_move.source_name, *sources])
        return {
            "event_id": f"{trade_date:%Y%m%d}-{item.get('stock_code', '')}-{event_type}-{event_time}",
            "event_type": event_type,
            "event_label": event_label,
            "event_time": event_time,
            "stock_code": item.get("stock_code", ""),
            "stock_name": item.get("stock_name", ""),
            "board": int(item.get("continuous_limit_up_days") or 1),
            "reason": reason,
            "reason_category": item.get("reason_category") or "其他",
            "change_pct": float(item.get("change_pct") or 0),
            "seal_amount": float(item.get("seal_amount") or 0),
            "amount": float(item.get("amount") or 0),
            "turnover_rate": float(item.get("turnover_rate") or 0),
            "is_sealed": is_sealed,
            "open_count": open_count,
            "sources": sources,
            "target_status_label": status_label or self._target_status_label(
                is_sealed,
                int(item.get("continuous_limit_up_days") or 1),
            ),
            "target_plate": target_plate,
            "target_reason_summary": self._target_reason_summary_from_attribution(public_attribution, reason) or self._target_reason_summary(reason),
            "target_seal_amount": self._target_amount(float(item.get("seal_amount") or 0)),
        }

    def _build_stock_move_item(
        self,
        item: Dict[str, Any],
        stock_code: str,
        source_scope: str,
        trade_date: date,
        *,
        external_move: Optional[ExternalStockMove] = None,
    ) -> Dict[str, Any]:
        reason = item.get("limit_up_reason") or item.get("reason_category") or "暂无异动原因"
        reason_category = item.get("reason_category") or "其他"
        sources = ["同花顺"] if source_scope == "ths" else ["同花顺", "开盘啦", "公告/互动易", "腾讯行情"]
        if external_move:
            sources = self._dedupe_sources([external_move.source_name, *sources])
        stock_name = item.get("stock_name") or stock_code
        target_title = external_move.title if external_move and external_move.title else self._target_reason_summary(reason)
        reason_content = external_move.content if external_move and external_move.content else reason
        display_trade_date = external_move.trade_date if external_move and external_move.trade_date else trade_date
        return {
            "stock_code": stock_code,
            "stock_name": stock_name,
            "trade_date": display_trade_date.isoformat(),
            "source_scope": source_scope,
            "sources": sources,
            "latest_limit_up": {
                "board": int(item.get("continuous_limit_up_days") or 1),
                "event_label": self._build_limit_up_event(item, trade_date)["event_label"],
                "first_limit_up_time": self._format_time(item.get("first_limit_up_time")),
                "final_seal_time": self._format_time(item.get("final_seal_time")),
                "open_count": int(item.get("open_count") or 0),
                "seal_amount": float(item.get("seal_amount") or 0),
            },
            "reasons": [
                {
                    "source": external_move.source_name if external_move else ("同花顺" if source_scope == "ths" else "综合解析"),
                    "title": target_title,
                    "content": reason_content,
                }
            ],
            "concepts": self._split_concepts(target_title),
            "announcements": [],
            "industry": item.get("industry") or "",
            "related_plates": [target_title] if target_title else [],
        }

    def _build_stock_move_from_external(
        self,
        external_move: ExternalStockMove,
        stock_code: str,
        source_scope: str,
        trade_date: date,
    ) -> Dict[str, Any]:
        title = external_move.title or "暂无异动原因"
        return {
            "stock_code": stock_code,
            "stock_name": external_move.stock_name or stock_code,
            "trade_date": (external_move.trade_date or trade_date).isoformat(),
            "source_scope": source_scope,
            "sources": [external_move.source_name],
            "latest_limit_up": None,
            "reasons": [
                {
                    "source": external_move.source_name,
                    "title": title,
                    "content": external_move.content or title,
                }
            ],
            "concepts": self._split_concepts(title),
            "announcements": [],
            "industry": external_move.plate,
            "related_plates": [title],
        }

    def _empty_stock_move(self, stock_code: str, source_scope: str) -> Dict[str, Any]:
        return {
            "stock_code": stock_code,
            "stock_name": stock_code,
            "trade_date": date.today().isoformat(),
            "source_scope": source_scope,
            "sources": ["同花顺"] if source_scope == "ths" else ["综合解析"],
            "latest_limit_up": None,
            "reasons": [],
            "concepts": [],
            "announcements": [],
            "industry": "",
            "related_plates": [],
        }

    def _build_plate_strength_items(
        self,
        raw_items: List[Dict[str, Any]],
        source: str,
    ) -> List[Dict[str, Any]]:
        grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for item in raw_items:
            grouped[self._plate_strength_name(item, source)].append(item)

        items = [
            self._build_plate_strength_item(plate, members)
            for plate, members in grouped.items()
        ]
        items.sort(
            key=lambda item: (
                item["strength_score"],
                item["limit_up_count"],
                item["max_board"],
                item["plate_name"],
            ),
            reverse=True,
        )
        for index, item in enumerate(items, start=1):
            item["rank"] = index
        return items

    @staticmethod
    def _plate_strength_name(item: Dict[str, Any], source: str) -> str:
        if source == "ths":
            from app.services.ths_limit_up_classification_service import ThsLimitUpClassificationService

            reason = item.get("limit_up_reason") or item.get("reason_category") or item.get("industry") or ""
            return ThsLimitUpClassificationService.classify_reason(str(reason)) or "其他"

        category = item.get("reason_category")
        if category and category != "其他":
            return str(category)
        industry = item.get("industry")
        if industry:
            return str(industry)
        reason = item.get("limit_up_reason") or ""
        concepts = TdxPluginService._split_concepts(str(reason))
        return concepts[0] if concepts else "其他"

    def _build_plate_strength_item(self, plate: str, members: List[Dict[str, Any]]) -> Dict[str, Any]:
        sealed_members = [item for item in members if item.get("is_sealed", item.get("is_final_sealed", True))]
        limit_up_count = len(members)
        sealed_count = len(sealed_members)
        opened_count = limit_up_count - sealed_count
        max_board = max([int(item.get("continuous_limit_up_days") or 1) for item in members], default=0)
        seal_rate = round(sealed_count / limit_up_count * 100, 1) if limit_up_count else 0
        total_seal_amount = round(sum(float(item.get("seal_amount") or 0) for item in members), 2)
        total_amount = round(sum(float(item.get("amount") or 0) for item in members), 2)
        total_open_count = sum(int(item.get("open_count") or 0) for item in members)
        amount_score = min(25.0, math.log10(max(1.0, total_seal_amount) + 1.0) * 3.0)
        strength_score = round(
            limit_up_count * 20
            + sealed_count * 10
            + max_board * 5
            + seal_rate * 0.3
            + amount_score
            - total_open_count * 1.5,
            2,
        )
        core_stocks = sorted(
            members,
            key=lambda item: (
                int(item.get("continuous_limit_up_days") or 1),
                bool(item.get("is_sealed", item.get("is_final_sealed", True))),
                float(item.get("seal_amount") or 0),
            ),
            reverse=True,
        )[:5]

        return {
            "plate_name": plate,
            "strength_score": strength_score,
            "limit_up_count": limit_up_count,
            "sealed_count": sealed_count,
            "opened_count": opened_count,
            "seal_rate": seal_rate,
            "max_board": max_board,
            "total_seal_amount": total_seal_amount,
            "total_amount": total_amount,
            "total_open_count": total_open_count,
            "core_stocks": [
                {
                    "stock_code": item.get("stock_code", ""),
                    "stock_name": item.get("stock_name", ""),
                    "board": int(item.get("continuous_limit_up_days") or 1),
                    "is_sealed": bool(item.get("is_sealed", item.get("is_final_sealed", True))),
                    "seal_amount": float(item.get("seal_amount") or 0),
                }
                for item in core_stocks
            ],
            "rank": 0,
            "rank_change": 0,
            "score_change": 0.0,
            "trend": "new",
        }

    def _build_plate_constituent_item(
        self,
        candidate: Dict[str, Any],
        quote: Dict[str, Any],
        limit_up_item: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        stock_code = self._normalize_code(candidate.get("stock_code", ""))
        price = float(quote.get("price") or 0)
        amount = float(quote.get("amount") or 0)
        change_pct = normalize_change_pct(
            quote.get("change_pct"),
            price=price,
            amount=amount,
        )
        limit_up_price = float(quote.get("limit_up") or 0)
        quote_has_limit_state = price > 0 and limit_up_price > 0
        quote_is_limit_up = quote_has_limit_state and price >= limit_up_price - 0.001
        board = int((limit_up_item or {}).get("continuous_limit_up_days") or 0)
        pool_is_sealed = bool(limit_up_item) and bool(
            (limit_up_item or {}).get("is_sealed", (limit_up_item or {}).get("is_final_sealed", False))
        )
        is_sealed = quote_is_limit_up if quote_has_limit_state else pool_is_sealed
        is_limit_up = quote_is_limit_up if quote_has_limit_state else pool_is_sealed
        return {
            "stock_code": stock_code,
            "stock_name": quote.get("name") or candidate.get("stock_name") or stock_code,
            "market": candidate.get("market") or quote.get("market") or "",
            "price": price or None,
            "change_pct": change_pct,
            "amount": amount,
            "turnover_rate": float(quote.get("turnover_rate") or 0),
            "is_limit_up": is_limit_up,
            "is_sealed": is_sealed,
            "board": board,
            "first_limit_up_time": self._format_time((limit_up_item or {}).get("first_limit_up_time")),
            "match_reason": candidate.get("match_reason") or "",
            "dragon_tag": None,
            "dragon_reason": "",
            "tags": [],
        }

    def _annotate_intraday_tags(self, items: List[Dict[str, Any]]) -> None:
        ranked = sorted(
            [
                item
                for item in items
                if int(item.get("board") or 0) > 0 or (item.get("change_pct") or 0) > 0
            ],
            key=lambda item: (
                -int(item.get("board") or 0),
                -int(bool(item.get("is_sealed"))),
                self._intraday_time_rank(item.get("first_limit_up_time")),
                -(item.get("change_pct") if item.get("change_pct") is not None else -999.0),
                -float(item.get("amount") or 0),
                item.get("stock_code") or "",
            ),
        )
        dragon_items = ranked[:5]
        for index, item in enumerate(dragon_items, start=1):
            item["dragon_tag"] = f"龙{index}"
            if item.get("board"):
                status = "封板" if item.get("is_sealed") else "开板"
                first_time = f"·{item['first_limit_up_time']}首封" if item.get("first_limit_up_time") else ""
                item["dragon_reason"] = f"{item['board']}板·{status}{first_time}"
            elif item.get("change_pct") is not None:
                item["dragon_reason"] = f"日内涨幅{item['change_pct']:+.2f}%"
            self._append_intraday_tag(
                item,
                label=item["dragon_tag"],
                tag_type="dragon",
                reason=item["dragon_reason"],
            )

        board_items = [item for item in ranked if int(item.get("board") or 0) > 0]
        max_board = max((int(item.get("board") or 0) for item in board_items), default=0)
        if max_board >= 2:
            high_item = next(item for item in ranked if int(item.get("board") or 0) == max_board)
            self._append_intraday_tag(
                high_item,
                label="高标",
                tag_type="high",
                reason=f"板块最高连板高度·{max_board}板",
            )

        timed_board_items = [
            item
            for item in board_items
            if self._intraday_time_rank(item.get("first_limit_up_time")) < 999999
        ]
        if timed_board_items:
            pioneer_item = min(
                timed_board_items,
                key=lambda item: (
                    self._intraday_time_rank(item.get("first_limit_up_time")),
                    item.get("stock_code") or "",
                ),
            )
            self._append_intraday_tag(
                pioneer_item,
                label="先锋",
                tag_type="pioneer",
                reason=f"板块内最早触板·{pioneer_item['first_limit_up_time']}",
            )

        positive_amount_items = [
            item
            for item in items
            if (item.get("change_pct") or 0) > 0 and float(item.get("amount") or 0) > 0
        ]
        if positive_amount_items:
            core_item = max(
                positive_amount_items,
                key=lambda item: (
                    float(item.get("amount") or 0),
                    float(item.get("change_pct") or 0),
                ),
            )
            self._append_intraday_tag(
                core_item,
                label="中军",
                tag_type="core",
                reason=f"板块内日内成交额最大·{float(core_item.get('amount') or 0):.0f}万",
            )

        catchup_items = [
            item
            for item in items
            if not item.get("dragon_tag")
            and int(item.get("board") or 0) <= 1
            and float(item.get("change_pct") or 0) >= 5.0
        ]
        if catchup_items:
            catchup_item = max(
                catchup_items,
                key=lambda item: (
                    float(item.get("change_pct") or 0),
                    float(item.get("amount") or 0),
                ),
            )
            self._append_intraday_tag(
                catchup_item,
                label="补涨",
                tag_type="catchup",
                reason=f"非前五龙头中日内涨幅领先·{catchup_item['change_pct']:+.2f}%",
            )

        for opened_item in board_items:
            if opened_item.get("is_sealed"):
                continue
            self._append_intraday_tag(
                opened_item,
                label="炸板",
                tag_type="opened",
                reason=f"日内曾触板后打开·{int(opened_item.get('board') or 0)}板",
            )

        tag_priority = {
            "dragon": 0,
            "opened": 1,
            "high": 2,
            "core": 3,
            "pioneer": 4,
            "catchup": 5,
        }
        for item in items:
            item["tags"] = sorted(
                item.get("tags") or [],
                key=lambda tag: (tag_priority.get(tag.get("type"), 99), tag.get("label") or ""),
            )[:3]

    @staticmethod
    def _append_intraday_tag(
        item: Dict[str, Any],
        *,
        label: str,
        tag_type: str,
        reason: str,
    ) -> None:
        tags = item.setdefault("tags", [])
        if any(tag.get("label") == label for tag in tags):
            return
        tags.append({"label": label, "type": tag_type, "reason": reason})

    @staticmethod
    def _intraday_time_rank(value: Any) -> int:
        digits = "".join(character for character in str(value or "") if character.isdigit())
        if len(digits) >= 6:
            return int(digits[-6:])
        return 999999

    async def _get_cached_plate_strength_history(
        self,
        target_date: date,
        db: Optional[AsyncSession],
        *,
        source: str,
        window_days: int,
    ) -> List[Dict[str, Any]]:
        cache_key = (target_date.isoformat(), source, window_days)
        now = time_module.monotonic()
        cached = self._plate_strength_history_cache.get(cache_key)
        if cached and now - cached[0] <= self.plate_strength_history_cache_ttl:
            return copy.deepcopy(cached[1])

        history = await self._load_plate_strength_history(
            target_date,
            db,
            source=source,
            window_days=window_days,
        )
        self._plate_strength_history_cache[cache_key] = (now, copy.deepcopy(history))
        if len(self._plate_strength_history_cache) > 128:
            oldest_key = min(
                self._plate_strength_history_cache,
                key=lambda key: self._plate_strength_history_cache[key][0],
            )
            self._plate_strength_history_cache.pop(oldest_key, None)
        return history

    async def _load_plate_strength_history(
        self,
        target_date: date,
        db: Optional[AsyncSession],
        *,
        source: str,
        window_days: int,
    ) -> List[Dict[str, Any]]:
        if db is None:
            return []

        date_result = await db.execute(
            select(LimitUpRecord.trade_date)
            .where(LimitUpRecord.trade_date <= target_date)
            .distinct()
            .order_by(desc(LimitUpRecord.trade_date))
            .limit(window_days)
        )
        history_dates = [row[0] for row in date_result.all()]
        if not history_dates:
            return []

        result = await db.execute(
            select(
                LimitUpRecord.trade_date,
                Stock.stock_code,
                Stock.stock_name,
                Stock.industry,
                LimitUpRecord.first_limit_up_time,
                LimitUpRecord.limit_up_reason,
                LimitUpRecord.reason_category,
                LimitUpRecord.continuous_limit_up_days,
                LimitUpRecord.open_count,
                LimitUpRecord.is_final_sealed,
                LimitUpRecord.current_status,
                LimitUpRecord.final_seal_time,
                LimitUpRecord.seal_amount,
                LimitUpRecord.amount,
                LimitUpRecord.turnover_rate,
                LimitUpRecord.data_source,
            )
            .join(Stock, LimitUpRecord.stock_id == Stock.id)
            .where(LimitUpRecord.trade_date.in_(history_dates))
            .order_by(LimitUpRecord.trade_date, LimitUpRecord.first_limit_up_time, Stock.stock_code)
        )

        by_date: Dict[date, List[Dict[str, Any]]] = defaultdict(list)
        for row in result.all():
            stock_code = self._normalize_code(row[1])
            is_final_sealed = bool(row[9]) if row[9] is not None else str(row[10] or "").lower() not in {"opened", "broken"}
            by_date[row[0]].append({
                "stock_code": stock_code,
                "stock_name": row[2] or stock_code,
                "industry": row[3],
                "first_limit_up_time": row[4],
                "limit_up_reason": row[5] or row[6] or "",
                "reason_category": row[6] or row[3] or "其他",
                "continuous_limit_up_days": int(row[7] or 1),
                "open_count": int(row[8] or 0),
                "is_final_sealed": is_final_sealed,
                "is_sealed": is_final_sealed,
                "current_status": row[10] or ("sealed" if is_final_sealed else "opened"),
                "final_seal_time": row[11],
                "seal_amount": float(row[12] or 0),
                "amount": float(row[13] or 0),
                "turnover_rate": float(row[14] or 0),
                "data_source": row[15] or "DB",
            })

        return [
            {
                "trade_date": trade_day.isoformat(),
                "items": self._build_plate_strength_items(by_date[trade_day], source),
            }
            for trade_day in sorted(by_date)
        ]

    @staticmethod
    def _annotate_plate_strength_changes(history: List[Dict[str, Any]]) -> None:
        previous_items: Dict[str, Dict[str, Any]] = {}
        for point in history:
            for item in point.get("items") or []:
                previous = previous_items.get(item["plate_name"])
                if previous is None:
                    item["score_change"] = 0.0
                    item["rank_change"] = 0
                    item["trend"] = "new"
                    continue

                score_change = round(item["strength_score"] - previous["strength_score"], 2)
                rank_change = int(previous.get("rank") or 0) - int(item.get("rank") or 0)
                item["score_change"] = score_change
                item["rank_change"] = rank_change
                item["trend"] = "up" if score_change > 0 else "down" if score_change < 0 else "flat"

            previous_items = {
                item["plate_name"]: item
                for item in point.get("items") or []
            }

    def _build_news_item(self, document: Any) -> Dict[str, Any]:
        title = document.title or document.source_name or "市场快讯"
        content = document.abstract or document.introduction or document.content_text or ""
        return {
            "news_id": str(document.id),
            "time": self._format_news_time(
                document.update_time or (document.created_at.isoformat() if getattr(document, "created_at", None) else "")
            ),
            "source": document.source_name or "知识库",
            "title": title,
            "content": content[:300],
            "importance": self._score_news_importance(title, content),
            "related_stocks": [],
            "related_plates": [],
            "jump_url": document.jump_url,
        }

    def _plugin_payload(
        self,
        items: List[Dict[str, Any]],
        trade_date: date,
        source_status: Dict[str, str],
        *,
        is_cache: bool,
        warnings: List[str],
    ) -> Dict[str, Any]:
        now_time = datetime.now().time()
        updated_at = datetime.combine(trade_date, now_time).isoformat()
        return {
            "items": items,
            "updated_at": updated_at,
            "source_status": source_status,
            "is_cache": is_cache,
            "warnings": warnings,
        }

    def _stock_move_cache_key(self, stock_code: str, source_scope: str, target_date: date) -> str:
        return f"{target_date.isoformat()}:{source_scope}:{stock_code}"

    def _read_stock_move_payload_cache(self, cache_key: str) -> Optional[Dict[str, Any]]:
        cached = self._stock_move_payload_cache.get(cache_key)
        if not cached:
            return None
        cached_at, payload = cached
        if time_module.time() - cached_at > self.stock_move_cache_ttl:
            self._stock_move_payload_cache.pop(cache_key, None)
            return None
        return copy.deepcopy(payload)

    def _store_stock_move_payload_cache(self, cache_key: str, payload: Dict[str, Any]) -> None:
        if not cache_key:
            return
        self._stock_move_payload_cache[cache_key] = (time_module.time(), copy.deepcopy(payload))
        if len(self._stock_move_payload_cache) <= self.stock_move_cache_max:
            return
        oldest_key = min(
            self._stock_move_payload_cache,
            key=lambda key: self._stock_move_payload_cache[key][0],
        )
        self._stock_move_payload_cache.pop(oldest_key, None)

    async def _read_persistent_stock_move_cache(
        self,
        db: Optional[AsyncSession],
        stock_code: str,
        source_scope: str,
        target_date: date,
    ) -> Optional[Dict[str, Any]]:
        if db is None:
            return None

        try:
            result = await db.execute(
                select(TdxStockMoveCache)
                .where(TdxStockMoveCache.stock_code == stock_code)
                .where(TdxStockMoveCache.source_scope == source_scope)
                .where(TdxStockMoveCache.trade_date == target_date)
                .order_by(TdxStockMoveCache.generated_at.desc(), TdxStockMoveCache.id.desc())
            )
            cached = result.scalar_one_or_none()
            if cached is None:
                result = await db.execute(
                    select(TdxStockMoveCache)
                    .where(TdxStockMoveCache.stock_code == stock_code)
                    .where(TdxStockMoveCache.source_scope == source_scope)
                    .order_by(TdxStockMoveCache.generated_at.desc(), TdxStockMoveCache.id.desc())
                    .limit(1)
                )
                cached = result.scalar_one_or_none()
        except Exception:
            return None

        if cached is None or not self._stock_move_payload_has_analysis(cached.payload_json):
            return None

        payload = copy.deepcopy(cached.payload_json)
        payload["is_cache"] = True
        payload["updated_at"] = cached.generated_at.isoformat()
        payload.setdefault("source_status", {})
        payload["source_status"]["stock_move_cache"] = "persistent_hit"
        return payload

    async def _store_persistent_stock_move_cache(
        self,
        db: Optional[AsyncSession],
        stock_code: str,
        source_scope: str,
        target_date: date,
        payload: Dict[str, Any],
    ) -> None:
        if db is None or not self._stock_move_payload_has_analysis(payload):
            return

        generated_at = self._payload_generated_at(payload)
        stock_name = self._stock_move_payload_stock_name(payload)
        try:
            result = await db.execute(
                select(TdxStockMoveCache)
                .where(TdxStockMoveCache.stock_code == stock_code)
                .where(TdxStockMoveCache.source_scope == source_scope)
                .where(TdxStockMoveCache.trade_date == target_date)
            )
            cached = result.scalar_one_or_none()
            if cached is None:
                db.add(
                    TdxStockMoveCache(
                        stock_code=stock_code,
                        source_scope=source_scope,
                        trade_date=target_date,
                        stock_name=stock_name,
                        payload_json=copy.deepcopy(payload),
                        source_status=copy.deepcopy(payload.get("source_status") or {}),
                        warnings=copy.deepcopy(payload.get("warnings") or []),
                        generated_at=generated_at,
                    )
                )
            else:
                cached.stock_name = stock_name
                cached.payload_json = copy.deepcopy(payload)
                cached.source_status = copy.deepcopy(payload.get("source_status") or {})
                cached.warnings = copy.deepcopy(payload.get("warnings") or [])
                cached.generated_at = generated_at
                cached.updated_at = datetime.now()
            await db.commit()
        except Exception:
            with contextlib.suppress(Exception):
                await db.rollback()

    @staticmethod
    def _stock_move_payload_has_analysis(payload: Dict[str, Any]) -> bool:
        for item in payload.get("items") or []:
            for reason in item.get("reasons") or []:
                title = str(reason.get("title") or "").strip()
                content = str(reason.get("content") or "").strip()
                if title and title != "暂无异动原因" and content:
                    return True
        return False

    @staticmethod
    def _stock_move_payload_reason_title(payload: Optional[Dict[str, Any]]) -> Optional[str]:
        if not payload:
            return None
        for item in payload.get("items") or []:
            for reason in item.get("reasons") or []:
                title = str(reason.get("title") or "").strip()
                if title and title != "暂无异动原因":
                    return title
        return None

    @staticmethod
    def _payload_generated_at(payload: Dict[str, Any]) -> datetime:
        updated_at = str(payload.get("updated_at") or "").strip()
        if updated_at:
            with contextlib.suppress(ValueError):
                return datetime.fromisoformat(updated_at)
        return datetime.now()

    @staticmethod
    def _stock_move_payload_stock_name(payload: Dict[str, Any]) -> str:
        for item in payload.get("items") or []:
            name = str(item.get("stock_name") or "").strip()
            if name:
                return name[:50]
        return ""

    def _build_plate_filters(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        counts: Dict[str, int] = defaultdict(int)
        first_seen: Dict[str, int] = {}
        for index, item in enumerate(items):
            for plate in self._split_concepts(item.get("target_plate") or item.get("reason_category") or ""):
                counts[plate] += 1
                first_seen.setdefault(plate, index)
        return [
            {"name": name, "count": count}
            for name, count in sorted(counts.items(), key=lambda pair: (-pair[1], first_seen[pair[0]], pair[0]))
        ][:24]

    async def _load_historical_status_labels(
        self,
        raw_items: List[Dict[str, Any]],
        target_date: date,
        db: Optional[AsyncSession],
    ) -> Dict[str, str]:
        if db is None or not raw_items:
            return {}

        codes = [str(item.get("stock_code", "")) for item in raw_items if item.get("stock_code")]
        if not codes:
            return {}

        try:
            history_result = await db.execute(
                select(Stock.stock_code, LimitUpRecord.trade_date)
                .join(Stock, LimitUpRecord.stock_id == Stock.id)
                .where(Stock.stock_code.in_(codes))
                .where(LimitUpRecord.trade_date <= target_date)
                .order_by(Stock.stock_code, LimitUpRecord.trade_date)
            )
            date_result = await db.execute(
                select(LimitUpRecord.trade_date)
                .where(LimitUpRecord.trade_date <= target_date)
                .distinct()
                .order_by(LimitUpRecord.trade_date)
            )
        except Exception:
            return {}

        by_code: Dict[str, List[date]] = defaultdict(list)
        for row in history_result.all():
            code = str(row[0])
            row_date = row[1]
            if isinstance(row_date, date):
                by_code[code].append(row_date)

        market_dates = [
            row[0]
            for row in date_result.all()
            if row and isinstance(row[0], date)
        ]

        labels: Dict[str, str] = {}
        for item in raw_items:
            code = str(item.get("stock_code", ""))
            label = self._target_status_label_from_history(
                bool(item.get("is_sealed", item.get("is_final_sealed", True))),
                target_date,
                by_code.get(code, []),
                market_dates,
                int(item.get("continuous_limit_up_days") or 1),
            )
            if label:
                labels[code] = label
        return labels

    async def _resolve_trade_date(self, trade_date: Optional[date], db: Optional[AsyncSession]) -> date:
        if trade_date:
            return trade_date
        if db is None:
            return date.today()
        try:
            result = await db.execute(select(func.max(LimitUpRecord.trade_date)))
            latest_trade_date = result.scalar_one_or_none()
        except Exception:
            latest_trade_date = None
        return latest_trade_date or date.today()

    @staticmethod
    def _normalize_code(stock_code: str) -> str:
        digits = "".join(ch for ch in str(stock_code or "") if ch.isdigit())
        return digits[-6:].zfill(6) if digits else ""

    @staticmethod
    def _format_time(value: Any) -> str:
        if isinstance(value, datetime):
            return value.strftime("%H:%M:%S")
        if isinstance(value, time):
            return value.strftime("%H:%M:%S")
        if isinstance(value, str):
            return value[-8:] if len(value) >= 8 else value
        return ""

    @staticmethod
    def _format_news_time(value: Any) -> str:
        if isinstance(value, datetime):
            return value.strftime("%H:%M:%S")
        if not value:
            return ""
        text = str(value).strip()
        if text.isdigit():
            timestamp = int(text)
            if timestamp > 10_000_000_000:
                timestamp = timestamp / 1000
            return datetime.fromtimestamp(timestamp).strftime("%H:%M:%S")
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).strftime("%H:%M:%S")
        except ValueError:
            return text[-8:] if len(text) >= 8 else text

    @staticmethod
    def _split_concepts(reason_category: str) -> List[str]:
        if not reason_category:
            return []
        return [part.strip() for part in reason_category.replace("/", "+").replace("，", "+").replace("、", "+").split("+") if part.strip()]

    @staticmethod
    def _dedupe_sources(sources: List[str]) -> List[str]:
        result: List[str] = []
        for source in sources:
            if source and source not in result:
                result.append(source)
        return result

    @staticmethod
    def _target_plate(reason: str) -> str:
        text = reason or ""
        theme_rules = [
            ("锂电池", ["BOPP", "新能源膜"]),
            ("智能电网", ["特高压", "电网设备", "智能电网", "电气设备", "变压器", "逆变器", "HVDC", "固态变压器"]),
            ("元器件", ["电阻", "电容", "超级电容", "MLCC", "磁性材料", "电感", "元器件", "元件"]),
            ("商业航天", ["商业航天"]),
            ("通信", ["光器件", "光模块", "CPO", "电子布", "PCB", "覆铜板", "印制电路板"]),
            ("芯片", ["芯片IP", "先进封装", "半导体设备", "半导体材料", "磷化铟", "芯片", "半导体", "光刻胶", "IGBT", "玻璃基板"]),
            ("通信", ["铜箔", "光纤", "通信"]),
            ("金刚石概念", ["金刚石", "培育钻石", "CVD"]),
            ("消费电子", ["消费电子", "折叠屏"]),
            ("端侧AI", ["端侧AI", "AI手机", "AI眼镜"]),
            ("储能", ["储能", "空气储能", "压缩空气储能"]),
            ("锂电池", ["锂电", "固态电池", "电池"]),
            ("算力", ["算力", "液冷", "数据中心", "IDC", "AI服务器", "AIDC"]),
            ("电力", ["电力", "绿色电力", "绿电", "热电", "发电", "电源"]),
            ("汽车零部件", ["汽车零部件", "汽车热管理", "无人驾驶", "智能驾驶"]),
            ("地产链", ["房地产", "地产链", "物业服务", "城中村"]),
            ("体育产业", ["体育产业", "体育Ⅱ"]),
            ("燃气轮机", ["燃气轮机"]),
            ("宠物经济", ["宠物经济"]),
            ("外贸", ["跨境电商", "外贸"]),
            ("黄金", ["黄金"]),
            ("新型工业化", ["新型工业化", "工业母机"]),
            ("世界杯概念", ["世界杯"]),
            ("有色金属", ["有色", "金属", "铜", "钼", "锗", "铂", "钽铌"]),
            ("医药", ["医药", "兽药", "医疗", "创新药", "原料药"]),
            ("化工", ["化工", "塑料", "玻璃", "碳酸", "电石"]),
        ]
        for theme, keywords in theme_rules:
            if any(keyword.lower() in text.lower() for keyword in keywords):
                return theme
        parts = TdxPluginService._split_concepts(text)
        return parts[0] if parts else "其他"

    @staticmethod
    def _target_reason_summary(reason: str) -> str:
        text = reason or ""
        if "字节算力" in text and "算力租赁" in text:
            return "算力(算力租赁)"
        if "液冷" in text and "算力" in text:
            return "算力(液冷)"
        if "电阻电容" in text and "数据中心" in text:
            return "电阻电容+数据中心"
        if "稀土永磁" in text and any(keyword in text for keyword in ["HVDC", "元器件", "光伏组件"]):
            return "稀土永磁+元器件"
        if "金刚石" in text or "培育钻石" in text or "CVD" in text:
            return "金刚石概念"

        plate = TdxPluginService._target_plate(reason)
        secondary = TdxPluginService._target_secondary_concept(reason, plate)
        return f"{plate}+{secondary}" if secondary and secondary != plate else plate

    @staticmethod
    def _target_reason_summary_from_attribution(public_attribution: Optional[PublicStockAttribution], fallback_reason: str = "") -> str:
        if not public_attribution or not public_attribution.plate:
            return ""
        plate = public_attribution.plate
        raw_plate = TdxPluginService._target_plate(fallback_reason)
        raw_secondary = TdxPluginService._target_secondary_concept(fallback_reason, plate)
        if not public_attribution.reason_title and raw_plate in {"储能", "金刚石概念", "锂矿"}:
            for concept in public_attribution.concepts or []:
                if concept and concept != raw_plate:
                    return f"{raw_plate}+{concept}"
            return raw_plate
        allowed_raw_secondary = {
            "地产链": {"房地产", "物业服务", "深圳国资", "洁净室", "香港牌照", "房屋检测"},
            "电力": {"绿色电力", "火电", "信托概念", "环保"},
            "通信": {"光模块", "光纤概念", "印制电路板", "PCB铜箔", "电子布"},
            "医药": {"原料药", "仿制药", "创新药", "病毒防治", "中药"},
            "智能电网": {"固态断路器", "变压器"},
            "机器人概念": {"新型工业化", "汽车零部件"},
            "汽车零部件": {"比亚迪产业链", "锂电池"},
        }
        if raw_secondary and raw_secondary != plate and raw_secondary in allowed_raw_secondary.get(plate, set()):
            return f"{plate}+{raw_secondary}"
        for concept in public_attribution.concepts or []:
            if concept and concept != plate:
                return f"{plate}+{concept}"
        return plate

    @staticmethod
    def _target_secondary_concept(reason: str, plate: str) -> str:
        text = reason or ""
        concept_rules = [
            ("固态电池", ["固态电池"]),
            ("电阻电容", ["电阻", "电容", "MLCC", "超级电容"]),
            ("光模块", ["光模块", "CPO"]),
            ("印制电路板", ["印制电路板", "PCB", "覆铜板"]),
            ("PCB铜箔", ["PCB铜箔"]),
            ("电子布", ["电子布"]),
            ("液冷", ["液冷"]),
            ("算力租赁", ["算力租赁"]),
            ("数据中心", ["数据中心", "IDC", "AIDC"]),
            ("AI服务器", ["AI服务器"]),
            ("AI手机", ["AI手机"]),
            ("AI眼镜", ["AI眼镜"]),
            ("折叠屏", ["折叠屏"]),
            ("光伏", ["光伏"]),
            ("半导体", ["半导体", "芯片IP"]),
            ("光刻胶", ["光刻胶"]),
            ("玻璃基板", ["玻璃基板"]),
            ("空气储能", ["空气储能", "压缩空气储能"]),
            ("金属铜", ["金属铜", "铜加工"]),
            ("金属钼", ["金属钼"]),
            ("热力", ["热力", "热电"]),
            ("绿色电力", ["绿色电力", "风电"]),
            ("火电", ["火电", "超超临界"]),
            ("原料药", ["原料药"]),
            ("房地产", ["房地产", "城中村", "武汉地产"]),
            ("并购重组", ["并购重组", "拟收购"]),
            ("一季报增长", ["一季报增长", "业绩"]),
        ]
        lowered = text.lower()
        for concept, keywords in concept_rules:
            if any(keyword.lower() in lowered for keyword in keywords):
                if concept != plate:
                    return concept

        for part in TdxPluginService._split_concepts(text):
            if part and part != plate:
                return part
        return ""

    @staticmethod
    def _target_status_label(is_sealed: bool, board: int) -> str:
        if not is_sealed:
            return "炸板"
        if board <= 1:
            return "首板"
        return f"{board}天{board}板"

    @staticmethod
    def _target_status_label_from_history(
        is_sealed: bool,
        target_date: date,
        limit_dates: List[date],
        market_dates: List[date],
        fallback_board: int,
    ) -> str:
        if not is_sealed:
            return "炸板"

        limit_date_set = {item for item in limit_dates if isinstance(item, date) and item <= target_date}
        if target_date not in limit_date_set:
            limit_date_set.add(target_date)

        available_market_dates = sorted({item for item in market_dates if isinstance(item, date) and item <= target_date})
        if target_date not in available_market_dates:
            available_market_dates.append(target_date)
            available_market_dates.sort()

        if len(limit_date_set) <= 1 or not available_market_dates:
            return TdxPluginService._target_status_label(is_sealed, fallback_board)

        recent_market_dates = available_market_dates[-20:]
        recent_limit_dates = sorted(item for item in limit_date_set if item in set(recent_market_dates))
        if len(recent_limit_dates) <= 1:
            return TdxPluginService._target_status_label(is_sealed, fallback_board)

        first_index = recent_market_dates.index(recent_limit_dates[0])
        current_index = recent_market_dates.index(target_date)
        span = current_index - first_index + 1
        count = len(recent_limit_dates)
        if count <= 1:
            return TdxPluginService._target_status_label(is_sealed, fallback_board)
        if span <= 1:
            return f"{count}天{count}板"
        return f"{span}天{count}板"

    @staticmethod
    def _target_amount(value: float) -> str:
        if not value:
            return "--"
        if value >= 10_000_000:
            wan = value / 10_000
            if wan >= 10_000:
                return f"{wan / 10_000:.2f}亿"
            return f"{wan:.0f}万"
        if value >= 10_000:
            return f"{value / 10_000:.2f}亿"
        return f"{value:.0f}万"

    @staticmethod
    def _fallback_limit_up_change_pct(stock_code: str, is_sealed: bool) -> float:
        if not is_sealed:
            return 0.0
        if stock_code.startswith(("300", "301", "688", "689")):
            return 20.0
        if stock_code.startswith(("8", "4", "920")):
            return 30.0
        return 10.0

    @staticmethod
    def _score_news_importance(title: str, content: str) -> int:
        text = f"{title} {content}"
        score = 50
        for keyword in ["涨停", "异动", "公告", "订单", "并购", "业绩", "监管", "停牌", "复牌"]:
            if keyword in text:
                score += 8
        return min(score, 100)


tdx_plugin_service = TdxPluginService(
    external_move_provider=public_stock_move_provider,
    attribution_provider=public_attribution_provider,
    enable_external_sources=True,
    news_provider=public_market_news_provider,
)
