"""Strict TongHuaShun limit-up reason classification service."""
from __future__ import annotations

import asyncio
import hashlib
import json
from collections import defaultdict
from datetime import date, datetime, time
from typing import Any, Dict, Iterable, List, Optional

from loguru import logger
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.limit_up import LimitUpClassificationDigest, LimitUpRecord
from app.models.stock import Stock
from app.services.intelligence_service import DeepSeekSummaryClient
from app.services.realtime_limit_up_service import realtime_limit_up_service


class DeepSeekLimitUpClassificationClient:
    """Classify THS limit-up reasons with DeepSeek, returning strict JSON."""

    def __init__(self, summary_client: Optional[DeepSeekSummaryClient] = None):
        self.summary_client = summary_client or DeepSeekSummaryClient()

    @property
    def api_key(self) -> Optional[str]:
        return getattr(self.summary_client, "api_key", None)

    @property
    def model(self) -> str:
        return getattr(self.summary_client, "model", "")

    async def classify_limit_up_reasons(self, trade_date: date, stocks: List[Dict[str, Any]]) -> Dict[str, Any]:
        fallback = {
            "model": self.model,
            "model_status": "missing_api_key" if not self.api_key else "fallback",
            "classifications": [],
        }
        if not self.api_key:
            return fallback

        compact_stocks = [
            {
                "stock_code": stock["stock_code"],
                "stock_name": stock["stock_name"],
                "limit_up_reason": stock["limit_up_reason"],
                "rule_classified_plate": stock["rule_classified_plate"],
                "first_limit_up_time": stock["first_limit_up_time"],
                "final_seal_time": stock["final_seal_time"],
                "continuous_limit_up_days": stock["continuous_limit_up_days"],
            }
            for stock in stocks
            if stock.get("stock_code")
        ]
        try:
            payload = await self.summary_client._request_json(
                "你是A股涨停同花顺原因分类助手。只根据输入的同花顺涨停原因分类，只输出JSON。",
                (
                    "请把每只股票归入当天最合适的主导题材板块。严格要求："
                    "1. 只能依据limit_up_reason字段，不补充外部新闻或其它来源；"
                    "2. 复合原因按当天主导题材语义分类，避免仅因'智能'等泛词误归人工智能；"
                    "3. plate_name用简短中文题材名，如电力设备、机器人、半导体、业绩增长；"
                    "4. confidence为0到1的小数；"
                    "5. reason_summary一句话说明分类依据；"
                    "6. keywords提取1到4个同花顺原因关键词。"
                    "只输出JSON对象，格式为："
                    "{classifications:[{stock_code,plate_name,confidence,reason_summary,keywords}]}。"
                    f"交易日期：{trade_date.isoformat()}。股票："
                    f"{json.dumps(compact_stocks, ensure_ascii=False)}"
                ),
                fallback,
            )
            return self._normalize_payload(payload)
        except Exception as exc:
            logger.warning(f"DeepSeek limit-up classification failed: {exc}")
            return {
                "model": self.model,
                "model_status": "error",
                "error": str(exc),
                "classifications": [],
            }

    def _normalize_payload(self, payload: Any) -> Dict[str, Any]:
        if not isinstance(payload, dict):
            return {
                "model": self.model,
                "model_status": "fallback",
                "classifications": [],
            }
        raw_items = payload.get("classifications") or payload.get("items") or []
        if isinstance(raw_items, dict):
            raw_items = [
                {"stock_code": code, **value}
                for code, value in raw_items.items()
                if isinstance(value, dict)
            ]
        classifications = []
        for raw in raw_items:
            if not isinstance(raw, dict):
                continue
            stock_code = str(raw.get("stock_code") or "").strip()
            plate_name = self._clean_text(raw.get("plate_name") or raw.get("plate") or raw.get("classified_plate"))
            if not stock_code or not plate_name:
                continue
            classifications.append(
                {
                    "stock_code": stock_code,
                    "plate_name": plate_name,
                    "confidence": self._to_float(raw.get("confidence")),
                    "reason_summary": self._clean_text(raw.get("reason_summary") or raw.get("summary")),
                    "keywords": self._clean_keywords(raw.get("keywords")),
                }
            )
        return {
            "model": payload.get("model") or self.model,
            "model_status": payload.get("model_status") or ("ready" if classifications else "fallback"),
            "classifications": classifications,
            "error": payload.get("error") or "",
        }

    @staticmethod
    def _clean_text(value: Any) -> str:
        text = str(value or "").strip()
        return text[:40]

    @staticmethod
    def _clean_keywords(value: Any) -> List[str]:
        if isinstance(value, str):
            raw_items = value.replace("，", ",").replace("、", ",").split(",")
        elif isinstance(value, list):
            raw_items = value
        else:
            raw_items = []
        keywords = []
        for raw in raw_items:
            text = str(raw or "").strip()
            if text and text not in keywords:
                keywords.append(text[:20])
            if len(keywords) >= 4:
                break
        return keywords

    @staticmethod
    def _to_float(value: Any) -> float:
        try:
            number = float(value)
        except (TypeError, ValueError):
            return 0.0
        return max(0.0, min(1.0, number))


class ThsLimitUpClassificationService:
    """Group A-share limit-up stocks by deterministic THS reason rules."""

    _CATEGORY_KEYWORDS = {
        "人工智能": [
            "AI",
            "人工智能",
            "算力",
            "大模型",
            "ChatGPT",
            "机器人",
            "智能",
            "DeepSeek",
            "数据要素",
        ],
        "半导体": ["半导体", "芯片", "集成电路", "封装", "光刻", "晶圆", "存储", "EDA", "GPU"],
        "新能源": ["新能源", "锂电", "锂电池", "光伏", "风电", "储能", "充电桩", "电池", "氢能", "钠电池", "固态电池"],
        "数字经济": ["数字经济", "云计算", "大数据", "信创", "软件", "数字中国", "数字货币", "区块链"],
        "医药医疗": ["医药", "医疗", "生物", "疫苗", "创新药", "CXO", "器械", "中药", "医美", "原料药", "制药"],
        "军工": ["军工", "国防", "航空", "航天", "军民融合", "舰船", "卫星", "无人机", "北斗"],
        "消费": ["消费", "白酒", "食品", "饮料", "零售", "电商", "酿酒", "预制菜", "旅游", "酒店"],
        "金融": ["金融", "银行", "保险", "证券", "券商", "信托", "期货"],
        "房地产": ["房地产", "地产", "房企", "物业", "城投"],
        "汽车": ["汽车", "整车", "零部件", "新能源车", "智能汽车", "汽车电子"],
        "通信": ["通信", "5G", "6G", "光模块", "光纤", "基站", "天线", "卫星通信", "CPO", "PCB"],
        "传媒": ["传媒", "游戏", "影视", "短视频", "直播", "元宇宙", "VR", "AR"],
        "重组": ["重组", "并购", "借壳", "资产注入", "收购", "股权转让"],
        "业绩": ["业绩", "预增", "净利润", "营收", "扭亏", "高增长", "超预期"],
        "次新股": ["次新", "上市", "新股", "IPO"],
    }

    def __init__(self, *, realtime_service=None, ai_classification_client=None):
        self.realtime_service = realtime_service or realtime_limit_up_service
        self.ai_classification_client = ai_classification_client or DeepSeekLimitUpClassificationClient()

    async def get_classification(
        self,
        requested_date: date,
        *,
        db: Optional[AsyncSession] = None,
        force_ai: bool = False,
    ) -> Dict[str, Any]:
        source_status = {
            "classification_scope": "strict_ths",
            "limit_up_pool": "ok",
            "ths_reason": "ok",
            "ai_classification": "not_requested",
        }
        items, realtime_path = await self._load_realtime_items(requested_date)
        source_status["realtime_path"] = realtime_path
        trade_date = requested_date
        is_fallback = False

        if not items:
            source_status["limit_up_pool"] = "empty"
            if db is not None:
                items = await self._load_db_items(requested_date, db)
                if items:
                    trade_date = items[0]["trade_date"]
                    is_fallback = trade_date != requested_date
                    source_status["limit_up_db"] = "ok"
                else:
                    source_status["limit_up_db"] = "empty"

        normalized = [self._normalize_item(item, trade_date) for item in items]
        classification_method = "rule"
        if normalized and isinstance(db, AsyncSession):
            normalized, classification_method = await self._apply_ai_classification(
                db,
                trade_date,
                normalized,
                source_status,
                force=force_ai,
            )
        groups = self._build_groups(normalized)
        return {
            "requested_date": requested_date,
            "trade_date": trade_date,
            "is_fallback": is_fallback,
            "updated_at": datetime.now().isoformat(timespec="seconds"),
            "source_status": source_status,
            "classification_method": classification_method,
            "total_count": len(normalized),
            "groups": groups,
        }

    async def _load_realtime_items(self, requested_date: date) -> tuple[List[Dict[str, Any]], str]:
        get_fast_pool = getattr(self.realtime_service, "get_fast_limit_up_pool", None)
        get_ths_reason_map = getattr(self.realtime_service, "_fetch_ths_reason_map", None)
        if callable(get_fast_pool) and callable(get_ths_reason_map):
            raw_data, reason_map = await asyncio.gather(
                get_fast_pool(requested_date, wait_for_refresh=False, max_cache_age=None),
                get_ths_reason_map(),
            )
            items = [dict(item) for item in raw_data]
            for item in items:
                code = item.get("stock_code", "")
                reason = reason_map.get(code)
                if reason:
                    item["limit_up_reason"] = reason
            return items, "fast_pool_ths"

        return await self.realtime_service.get_realtime_limit_up_list(requested_date), "full_realtime"

    async def _load_db_items(self, requested_date: date, db: AsyncSession) -> List[Dict[str, Any]]:
        latest_date = (
            select(func.max(LimitUpRecord.trade_date))
            .where(LimitUpRecord.trade_date <= requested_date)
            .scalar_subquery()
        )
        query = (
            select(
                Stock.stock_code,
                Stock.stock_name,
                LimitUpRecord.trade_date,
                LimitUpRecord.first_limit_up_time,
                LimitUpRecord.final_seal_time,
                LimitUpRecord.limit_up_reason,
                LimitUpRecord.continuous_limit_up_days,
                LimitUpRecord.open_count,
                LimitUpRecord.is_final_sealed,
                LimitUpRecord.current_status,
                LimitUpRecord.seal_amount,
                LimitUpRecord.turnover_rate,
                LimitUpRecord.amount,
            )
            .join(Stock, LimitUpRecord.stock_id == Stock.id)
            .where(LimitUpRecord.trade_date == latest_date)
            .order_by(LimitUpRecord.first_limit_up_time, Stock.stock_code)
        )
        result = await db.execute(query)
        return [
            {
                "stock_code": row[0],
                "stock_name": row[1],
                "trade_date": row[2],
                "first_limit_up_time": row[3],
                "final_seal_time": row[4],
                "limit_up_reason": row[5] or "",
                "continuous_limit_up_days": row[6] or 1,
                "open_count": row[7] or 0,
                "is_sealed": bool(row[8]),
                "is_final_sealed": bool(row[8]),
                "current_status": row[9] or ("sealed" if row[8] else "opened"),
                "seal_amount": float(row[10] or 0),
                "turnover_rate": float(row[11] or 0),
                "amount": float(row[12] or 0),
            }
            for row in result.all()
        ]

    async def _apply_ai_classification(
        self,
        db: AsyncSession,
        trade_date: date,
        stocks: List[Dict[str, Any]],
        source_status: Dict[str, str],
        *,
        force: bool = False,
    ) -> tuple[List[Dict[str, Any]], str]:
        content_hash = self._classification_content_hash(stocks)
        existing = await self._get_ai_digest(db, trade_date)
        if not force and existing and existing.status == "ready" and existing.content_hash == content_hash:
            source_status["ai_classification"] = "cache_hit"
            source_status["ai_model"] = existing.model or ""
            payload = dict(existing.classifications_json or {})
            return self._apply_ai_payload(stocks, payload), "ai"

        if not getattr(self.ai_classification_client, "api_key", None):
            source_status["ai_classification"] = "missing_api_key"
            return stocks, "rule"

        payload = await self.ai_classification_client.classify_limit_up_reasons(trade_date, stocks)
        model_status = str(payload.get("model_status") or "fallback")
        source_status["ai_classification"] = model_status
        source_status["ai_model"] = str(payload.get("model") or "")
        if model_status != "ready" or not payload.get("classifications"):
            if payload.get("error"):
                source_status["ai_error"] = str(payload["error"])[:120]
            return stocks, "rule"

        await self._write_ai_digest(db, trade_date, content_hash, payload)
        return self._apply_ai_payload(stocks, payload), "ai"

    async def _get_ai_digest(self, db: AsyncSession, trade_date: date) -> Optional[LimitUpClassificationDigest]:
        result = await db.execute(
            select(LimitUpClassificationDigest).where(LimitUpClassificationDigest.trade_date == trade_date)
        )
        return result.scalars().first()

    async def _write_ai_digest(
        self,
        db: AsyncSession,
        trade_date: date,
        content_hash: str,
        payload: Dict[str, Any],
    ) -> LimitUpClassificationDigest:
        existing = await self._get_ai_digest(db, trade_date)
        now = datetime.now()
        if existing is None:
            existing = LimitUpClassificationDigest(trade_date=trade_date)
            db.add(existing)
        existing.classifications_json = payload
        existing.status = "ready"
        existing.content_hash = content_hash
        existing.model = str(payload.get("model") or "")
        existing.error = ""
        existing.generated_at = now
        existing.updated_at = now
        await db.commit()
        await db.refresh(existing)
        return existing

    def _apply_ai_payload(self, stocks: List[Dict[str, Any]], payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        classification_map = {
            str(item.get("stock_code") or ""): item
            for item in payload.get("classifications") or []
            if isinstance(item, dict) and item.get("stock_code")
        }
        applied = []
        for stock in stocks:
            item = classification_map.get(stock["stock_code"])
            if not item:
                applied.append(stock)
                continue

            plate_name = str(item.get("plate_name") or "").strip()
            if not plate_name:
                applied.append(stock)
                continue

            next_stock = dict(stock)
            next_stock["classified_plate"] = plate_name[:40]
            next_stock["classification_method"] = "ai"
            next_stock["ai_confidence"] = DeepSeekLimitUpClassificationClient._to_float(item.get("confidence"))
            next_stock["ai_reason_summary"] = str(item.get("reason_summary") or "")[:120]
            next_stock["ai_keywords"] = DeepSeekLimitUpClassificationClient._clean_keywords(item.get("keywords"))
            applied.append(next_stock)
        return applied

    def _classification_content_hash(self, stocks: List[Dict[str, Any]]) -> str:
        payload = [
            {
                "stock_code": stock["stock_code"],
                "stock_name": stock["stock_name"],
                "limit_up_reason": stock["limit_up_reason"],
                "rule_classified_plate": stock["rule_classified_plate"],
                "first_limit_up_time": stock["first_limit_up_time"],
                "final_seal_time": stock["final_seal_time"],
                "continuous_limit_up_days": stock["continuous_limit_up_days"],
            }
            for stock in sorted(stocks, key=lambda item: item["stock_code"])
        ]
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    def _normalize_item(self, item: Dict[str, Any], default_trade_date: date) -> Dict[str, Any]:
        is_sealed = bool(item.get("is_sealed", item.get("is_final_sealed", True)))
        current_status = item.get("current_status") or ("sealed" if is_sealed else "opened")
        reason = item.get("limit_up_reason") or ""
        rule_classified_plate = self.classify_reason(reason)
        return {
            "stock_code": item.get("stock_code", ""),
            "stock_name": item.get("stock_name", ""),
            "trade_date": item.get("trade_date") or default_trade_date,
            "continuous_limit_up_days": int(item.get("continuous_limit_up_days") or 1),
            "current_status": current_status,
            "is_sealed": is_sealed,
            "open_count": int(item.get("open_count") or 0),
            "first_limit_up_time": self._format_time(item.get("first_limit_up_time")),
            "final_seal_time": self._format_time(item.get("final_seal_time")),
            "limit_up_reason": reason,
            "classified_plate": rule_classified_plate,
            "rule_classified_plate": rule_classified_plate,
            "classification_method": "rule",
            "ai_confidence": 0.0,
            "ai_reason_summary": "",
            "ai_keywords": [],
            "seal_amount": float(item.get("seal_amount") or 0),
            "turnover_rate": float(item.get("turnover_rate") or 0),
            "amount": float(item.get("amount") or 0),
        }

    def _build_groups(self, stocks: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for stock in stocks:
            grouped[stock["classified_plate"]].append(stock)

        groups = []
        for plate_name, members in grouped.items():
            members.sort(key=lambda item: (item["first_limit_up_time"] or "99:99:99", item["stock_code"]))
            first_times = [item["first_limit_up_time"] for item in members if item["first_limit_up_time"]]
            sealed_count = sum(1 for item in members if item["is_sealed"])
            groups.append(
                {
                    "plate_name": plate_name,
                    "count": len(members),
                    "sealed_count": sealed_count,
                    "opened_count": len(members) - sealed_count,
                    "earliest_first_limit_time": first_times[0] if first_times else "",
                    "latest_first_limit_time": first_times[-1] if first_times else "",
                    "stocks": members,
                }
            )
        groups.sort(
            key=lambda item: (
                -item["count"],
                item["earliest_first_limit_time"] or "99:99:99",
                item["plate_name"],
            )
        )
        return groups

    @classmethod
    def classify_reason(cls, reason: str) -> str:
        text = reason or ""
        if not text:
            return "其他"
        lowered = text.lower()
        for category, keywords in cls._CATEGORY_KEYWORDS.items():
            if any(keyword.lower() in lowered for keyword in keywords):
                return category
        parts = cls._split_reason(text)
        return parts[0] if parts else "其他"

    @staticmethod
    def _split_reason(reason: str) -> List[str]:
        normalized = reason.replace("/", "+").replace("，", "+").replace("、", "+").replace(",", "+")
        return [part.strip() for part in normalized.split("+") if part.strip()]

    @staticmethod
    def _format_time(value: Any) -> str:
        if isinstance(value, datetime):
            return value.strftime("%H:%M:%S")
        if isinstance(value, time):
            return value.strftime("%H:%M:%S")
        if isinstance(value, str):
            text = value.strip()
            return text[-8:] if len(text) >= 8 else text
        return ""


ths_limit_up_classification_service = ThsLimitUpClassificationService()
