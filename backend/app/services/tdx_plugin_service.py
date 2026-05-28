"""Tongdaxin black-theme watch plugin aggregation service."""
from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, time
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.intelligence import KnowledgeDocument
from app.services.realtime_limit_up_service import realtime_limit_up_service


class TdxPluginService:
    """Build stable payloads for the Tongdaxin embedded plugin pages."""

    def __init__(self):
        self.realtime_limit_up_service = realtime_limit_up_service

    async def get_limit_up_live(self, trade_date: Optional[date] = None) -> Dict[str, Any]:
        target_date = trade_date or date.today()
        warnings: List[str] = []
        source_status = {"limit_up_pool": "ok", "ths_reason": "ok", "tencent_quote": "ok"}

        try:
            raw_items = await self.realtime_limit_up_service.get_realtime_limit_up_list(target_date)
        except Exception as exc:
            raw_items = []
            source_status["limit_up_pool"] = "error"
            warnings.append(f"涨停池获取失败: {exc}")

        items = [self._build_limit_up_event(item, target_date) for item in raw_items]
        items.sort(key=lambda item: (item.get("event_time") or "99:99:99", -item.get("board", 0)))

        return self._plugin_payload(items, target_date, source_status, is_cache=False, warnings=warnings)

    async def get_stock_move(
        self,
        stock_code: str,
        trade_date: Optional[date] = None,
        *,
        source_scope: str = "mixed",
    ) -> Dict[str, Any]:
        target_date = trade_date or date.today()
        normalized_code = self._normalize_code(stock_code)
        warnings: List[str] = []
        source_status = {"stock_move": "ok"}

        try:
            limit_up_item = await self.realtime_limit_up_service.get_realtime_limit_up_item(normalized_code, target_date)
        except Exception as exc:
            limit_up_item = None
            source_status["stock_move"] = "error"
            warnings.append(f"个股异动获取失败: {exc}")

        if not limit_up_item:
            source_status["stock_move"] = "empty"
            warnings.append(f"{normalized_code} 暂无异动解析数据")
            return self._plugin_payload(
                [self._empty_stock_move(normalized_code, source_scope)],
                target_date,
                source_status,
                is_cache=False,
                warnings=warnings,
            )

        item = self._build_stock_move_item(limit_up_item, normalized_code, source_scope, target_date)
        return self._plugin_payload([item], target_date, source_status, is_cache=False, warnings=warnings)

    async def get_plate_strength(self, trade_date: Optional[date] = None) -> Dict[str, Any]:
        target_date = trade_date or date.today()
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

    async def get_news(self, db: AsyncSession, limit: int = 80) -> Dict[str, Any]:
        warnings: List[str] = []
        source_status = {"knowledge_news": "ok"}

        try:
            result = await db.execute(
                select(KnowledgeDocument)
                .order_by(KnowledgeDocument.trade_date.desc(), KnowledgeDocument.id.desc())
                .limit(limit)
            )
            documents = result.scalars().all()
        except Exception as exc:
            documents = []
            source_status["knowledge_news"] = "error"
            warnings.append(f"聚合快讯获取失败: {exc}")

        items = [self._build_news_item(doc) for doc in documents]
        if not items:
            source_status["knowledge_news"] = "empty"
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

    def _build_limit_up_event(self, item: Dict[str, Any], trade_date: date) -> Dict[str, Any]:
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

        event_time = self._format_time(item.get("final_seal_time") or item.get("first_limit_up_time"))
        return {
            "event_id": f"{trade_date:%Y%m%d}-{item.get('stock_code', '')}-{event_type}-{event_time}",
            "event_type": event_type,
            "event_label": event_label,
            "event_time": event_time,
            "stock_code": item.get("stock_code", ""),
            "stock_name": item.get("stock_name", ""),
            "board": int(item.get("continuous_limit_up_days") or 1),
            "reason": item.get("limit_up_reason") or item.get("reason_category") or "",
            "reason_category": item.get("reason_category") or "其他",
            "seal_amount": float(item.get("seal_amount") or 0),
            "amount": float(item.get("amount") or 0),
            "turnover_rate": float(item.get("turnover_rate") or 0),
            "is_sealed": is_sealed,
            "open_count": open_count,
            "sources": ["东方财富", "同花顺", "腾讯行情"],
        }

    def _build_stock_move_item(
        self,
        item: Dict[str, Any],
        stock_code: str,
        source_scope: str,
        trade_date: date,
    ) -> Dict[str, Any]:
        reason = item.get("limit_up_reason") or item.get("reason_category") or "暂无异动原因"
        reason_category = item.get("reason_category") or "其他"
        sources = ["同花顺"] if source_scope == "ths" else ["同花顺", "开盘啦", "公告/互动易", "腾讯行情"]
        stock_name = item.get("stock_name") or stock_code
        return {
            "stock_code": stock_code,
            "stock_name": stock_name,
            "trade_date": trade_date.isoformat(),
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
                    "source": "同花顺" if source_scope == "ths" else "综合解析",
                    "title": reason_category,
                    "content": reason,
                }
            ],
            "concepts": self._split_concepts(reason_category),
            "announcements": [],
            "industry": item.get("industry") or "",
            "related_plates": [reason_category] if reason_category else [],
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

    def _build_news_item(self, document: KnowledgeDocument) -> Dict[str, Any]:
        title = document.title or document.source_name or "市场快讯"
        content = document.abstract or document.introduction or document.content_text or ""
        return {
            "news_id": str(document.id),
            "time": document.update_time or (document.created_at.isoformat() if getattr(document, "created_at", None) else ""),
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

    @staticmethod
    def _normalize_code(stock_code: str) -> str:
        return "".join(ch for ch in stock_code if ch.isdigit())[-6:]

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
    def _split_concepts(reason_category: str) -> List[str]:
        if not reason_category:
            return []
        return [part.strip() for part in reason_category.replace("/", "+").replace("，", "+").split("+") if part.strip()]

    @staticmethod
    def _score_news_importance(title: str, content: str) -> int:
        text = f"{title} {content}"
        score = 50
        for keyword in ["涨停", "异动", "公告", "订单", "并购", "业绩", "监管", "停牌", "复牌"]:
            if keyword in text:
                score += 8
        return min(score, 100)


tdx_plugin_service = TdxPluginService()
