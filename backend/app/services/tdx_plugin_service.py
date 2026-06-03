"""Tongdaxin black-theme watch plugin aggregation service."""
from __future__ import annotations

import copy
import contextlib
import asyncio
import time as time_module
from collections import defaultdict
from datetime import date, datetime, time
from typing import Any, Dict, Iterable, List, Optional, Tuple

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.limit_up import LimitUpRecord
from app.models.stock import Stock
from app.models.tdx_cache import TdxStockMoveCache
from app.services.tdx_attribution_sources import (
    PublicStockAttribution,
    public_attribution_provider,
)
from app.services.tdx_external_sources import ExternalStockMove, public_stock_move_provider
from app.services.tdx_news_sources import public_market_news_provider
from app.services.realtime_limit_up_service import realtime_limit_up_service


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
    ):
        self.realtime_limit_up_service = realtime_limit_up_service
        self.external_move_provider = external_move_provider
        self.attribution_provider = attribution_provider
        self.enable_external_sources = enable_external_sources
        self.news_provider = news_provider
        self.stock_move_cache_ttl = stock_move_cache_ttl
        self.stock_move_cache_max = stock_move_cache_max
        self.stock_move_live_timeout = stock_move_live_timeout
        self._stock_move_payload_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}

    async def get_limit_up_live(self, trade_date: Optional[date] = None, db: Optional[AsyncSession] = None) -> Dict[str, Any]:
        target_date = await self._resolve_trade_date(trade_date, db)
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

    async def get_plate_strength(self, trade_date: Optional[date] = None, db: Optional[AsyncSession] = None) -> Dict[str, Any]:
        target_date = await self._resolve_trade_date(trade_date, db)
        warnings: List[str] = []
        source_status = {"limit_up_pool": "ok", "plate_strength": "ok"}

        try:
            raw_items = await self.realtime_limit_up_service.get_realtime_limit_up_list(target_date)
        except Exception as exc:
            raw_items = []
            source_status["limit_up_pool"] = "error"
            source_status["plate_strength"] = "empty"
            warnings.append(f"板块强度数据获取失败: {exc}")

        grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for item in raw_items:
            plate = item.get("reason_category") or item.get("industry") or "其他"
            grouped[plate].append(item)

        items = [self._build_plate_strength_item(plate, members) for plate, members in grouped.items()]
        items.sort(key=lambda item: item["strength_score"], reverse=True)
        return self._plugin_payload(items, target_date, source_status, is_cache=False, warnings=warnings)

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

    def _build_plate_strength_item(self, plate: str, members: List[Dict[str, Any]]) -> Dict[str, Any]:
        sealed_members = [item for item in members if item.get("is_sealed", item.get("is_final_sealed", True))]
        limit_up_count = len(members)
        sealed_count = len(sealed_members)
        max_board = max([int(item.get("continuous_limit_up_days") or 1) for item in members], default=0)
        seal_rate = round(sealed_count / limit_up_count * 100, 1) if limit_up_count else 0
        strength_score = round(limit_up_count * 20 + sealed_count * 10 + max_board * 5 + seal_rate * 0.3, 2)
        core_stocks = sorted(
            members,
            key=lambda item: (int(item.get("continuous_limit_up_days") or 1), float(item.get("seal_amount") or 0)),
            reverse=True,
        )[:5]

        return {
            "plate_name": plate,
            "strength_score": strength_score,
            "limit_up_count": limit_up_count,
            "sealed_count": sealed_count,
            "seal_rate": seal_rate,
            "max_board": max_board,
            "core_stocks": [
                {
                    "stock_code": item.get("stock_code", ""),
                    "stock_name": item.get("stock_name", ""),
                    "board": int(item.get("continuous_limit_up_days") or 1),
                }
                for item in core_stocks
            ],
            "trend": "up" if sealed_count == limit_up_count else "mixed",
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
