"""Strict TongHuaShun limit-up reason classification service."""
from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections import defaultdict
from datetime import date, datetime, time
from typing import Any, Dict, Iterable, List, Optional

from loguru import logger
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import async_session_maker
from app.models.limit_up import LimitUpClassificationArchive, LimitUpClassificationDigest, LimitUpRecord
from app.models.stock import Stock
from app.services.intelligence_service import DeepSeekSummaryClient
from app.services.realtime_limit_up_service import realtime_limit_up_service
from app.utils.time_utils import today_cn


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
                "primary_theme": stock.get("primary_theme") or stock.get("classified_plate") or "",
                "fine_theme": stock.get("fine_theme") or "",
                "secondary_themes": stock.get("secondary_themes") or [],
                "classification_evidence": stock.get("classification_evidence") or "",
                "ths_move_title": stock.get("ths_move_title") or "",
                "ths_move_summary": stock.get("ths_move_summary") or "",
                "classification_basis": stock.get("classification_basis") or "limit_up_reason",
                "rule_classified_plate": stock["rule_classified_plate"],
                "fine_themes": stock.get("fine_themes") or [],
                "first_limit_up_time": stock["first_limit_up_time"],
                "final_seal_time": stock["final_seal_time"],
                "continuous_limit_up_days": stock["continuous_limit_up_days"],
            }
            for stock in stocks
            if stock.get("stock_code")
        ]
        try:
            payload = await self.summary_client._request_json(
                "你是A股涨停同花顺异动解读分类助手。只根据输入的同花顺字段分类，只输出JSON。",
                (
                    "请把每只股票归入当天最合适的主导细分炒作题材。严格要求："
                    "1. 优先依据ths_move_title、ths_move_summary、classification_evidence；为空时才依据limit_up_reason；"
                    "2. 这些字段均为同花顺口径，禁止补充外部新闻或其它来源；"
                    "3. 并购重组、收购资产、股权变更、重大订单、业绩预增等强事件优先于普通概念词；"
                    "4. 产业炒作按细分方向分类，禁止输出人工智能、半导体、新能源等宽泛大类；"
                    "5. plate_name用简短中文题材名，如并购重组、PCB铜箔、AI电源、AI算力PCB、高速覆铜板、人形机器人；"
                    "6. confidence为0到1的小数；"
                    "7. reason_summary一句话说明分类依据；"
                    "8. keywords提取1到4个同花顺异动或原因关键词。"
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

    _FINE_THEME_ALIASES = (
        ("并购重组", "并购重组"),
        ("资产重组", "并购重组"),
        ("重大资产重组", "并购重组"),
        ("收购半导体", "收购半导体"),
        ("收购芯片", "收购半导体"),
        ("AI服务器电源", "AI电源"),
        ("AI算力电源", "AI电源"),
        ("服务器电源", "AI电源"),
        ("英伟达电源", "AI电源"),
        ("AI电源", "AI电源"),
        ("PCB铜箔", "PCB铜箔"),
        ("铜箔", "PCB铜箔"),
        ("复合铜箔", "复合铜箔"),
        ("PET铜箔", "PET铜箔"),
        ("AI算力PCB", "AI算力PCB"),
        ("AI服务器PCB", "AI服务器PCB"),
        ("高速PCB", "高速PCB"),
        ("高频高速覆铜板", "高速覆铜板"),
        ("高频覆铜板", "高速覆铜板"),
        ("高速覆铜板", "高速覆铜板"),
        ("高速电子树脂", "高速电子树脂"),
        ("电子级树脂", "PCB树脂"),
        ("环氧树脂", "PCB树脂"),
        ("PCB化学品", "PCB化学品"),
        ("覆铜板", "覆铜板"),
        ("电子布", "电子布"),
        ("玻璃基板", "玻璃基板"),
        ("先进封装", "先进封装"),
        ("功率半导体", "功率半导体"),
        ("半导体靶材", "半导体靶材"),
        ("存储芯片", "存储芯片"),
        ("存储封装", "存储芯片"),
        ("封装测试", "先进封装"),
        ("光通信芯片", "光通信芯片"),
        ("人形机器人", "人形机器人"),
        ("机器人线缆", "机器人线缆"),
        ("工业机器人", "工业机器人"),
        ("机器人", "机器人"),
        ("减速器", "减速器"),
        ("液冷服务器", "液冷服务器"),
        ("液冷", "液冷服务器"),
        ("铜连接", "铜连接"),
        ("高速连接器", "高速连接器"),
        ("光模块", "光模块"),
        ("CPO", "CPO"),
        ("HVDC", "HVDC"),
        ("AI眼镜电池", "AI眼镜电池"),
        ("固态电池", "固态电池"),
        ("智能电网", "智能电网"),
        ("特高压", "特高压"),
        ("输变电", "输变电设备"),
        ("数据中心", "数据中心"),
        ("算力租赁", "算力租赁"),
        ("AI算力", "AI算力"),
    )

    _EVENT_PRIORITY_RULES = (
        (
            "并购重组",
            (
                "拟收购",
                "收购",
                "并购",
                "重组",
                "资产注入",
                "股权转让",
                "协议转让",
                "控制权变更",
                "控制权拟变更",
                "拟变更控制权",
                "增资扩股",
                "购买资产",
                "购买股权",
                "取得股权",
                "取得控制权",
                "注入资产",
            ),
        ),
        (
            "重大订单",
            (
                "重大合同",
                "签订合同",
                "中标",
                "订单",
                "采购协议",
                "框架协议",
                "供货协议",
            ),
        ),
        (
            "业绩增长",
            (
                "业绩预增",
                "预增",
                "扭亏",
                "净利润同比增长",
                "净利润增长",
                "业绩增长",
                "业绩大增",
            ),
        ),
        (
            "资产处置",
            (
                "资产出售",
                "出售资产",
                "资产处置",
                "转让资产",
                "土地收储",
                "政府收储",
            ),
        ),
    )

    _LOW_SIGNAL_THEME_KEYWORDS = (
        "股权激励",
        "定增",
        "审核通过",
        "H股",
        "递表",
        "一季报",
        "年报",
        "半年报",
        "业绩",
        "分红",
        "权益分派",
        "风险提示",
        "控股股东",
        "总裁受让",
        "市占率",
        "龙头",
        "国资",
        "央企",
        "国企",
        "中科院",
    )

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

    def __init__(
        self,
        *,
        realtime_service=None,
        ai_classification_client=None,
        ths_analysis_source=None,
        ths_move_service=None,
        ths_move_timeout: float = 1.2,
        ths_move_concurrency: int = 16,
    ):
        self.realtime_service = realtime_service or realtime_limit_up_service
        self.ai_classification_client = ai_classification_client or DeepSeekLimitUpClassificationClient()
        self.ths_analysis_source = ths_analysis_source
        self.ths_move_service = ths_move_service
        self.ths_move_timeout = ths_move_timeout
        self.ths_move_concurrency = max(1, int(ths_move_concurrency or 1))

    async def get_classification(
        self,
        requested_date: date,
        *,
        db: Optional[AsyncSession] = None,
        force_ai: bool = False,
        use_archive: bool = True,
    ) -> Dict[str, Any]:
        if use_archive and not force_ai and isinstance(db, AsyncSession):
            archived_payload = await self._get_archived_payload(db, requested_date)
            if archived_payload is not None:
                return archived_payload

        source_status = {
            "classification_scope": "strict_ths",
            "classification_granularity": "fine_theme",
            "limit_up_pool": "ok",
            "ths_reason": "ok",
            "ai_classification": "not_requested",
        }
        trade_date = requested_date
        is_fallback = False
        items: List[Dict[str, Any]] = []

        if isinstance(db, AsyncSession) and requested_date != today_cn():
            source_status["limit_up_pool"] = "skipped_historical"
            source_status["realtime_path"] = "skipped_historical"
            items = await self._load_db_items(requested_date, db)
            if items:
                trade_date = items[0]["trade_date"]
                is_fallback = trade_date != requested_date
                source_status["limit_up_db"] = "ok"
            else:
                source_status["limit_up_db"] = "empty"
        else:
            items, realtime_path = await self._load_realtime_items(requested_date)
            source_status["realtime_path"] = realtime_path

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
        if normalized:
            await self._apply_ths_article_analysis(trade_date, normalized, source_status)
            await self._apply_ths_move_interpretation(db, trade_date, normalized, source_status)
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

    async def archive_daily_classification(
        self,
        requested_date: date,
        *,
        db: Optional[AsyncSession] = None,
    ) -> LimitUpClassificationArchive:
        """Persist a post-close classification snapshot for one trading day."""
        if isinstance(db, AsyncSession):
            return await self._archive_daily_classification_with_session(requested_date, db)

        async with async_session_maker() as session:
            return await self._archive_daily_classification_with_session(requested_date, session)

    async def _archive_daily_classification_with_session(
        self,
        requested_date: date,
        db: AsyncSession,
    ) -> LimitUpClassificationArchive:
        payload = await self.get_classification(
            requested_date,
            db=db,
            force_ai=False,
            use_archive=False,
        )
        payload = self._json_safe_payload(payload)
        trade_date_value = self._payload_trade_date(payload, requested_date)
        now = datetime.now()
        payload.setdefault("source_status", {})["classification_archive"] = "written"
        payload["archived_at"] = now.isoformat(timespec="seconds")
        content_hash = self._payload_content_hash(payload)

        existing = await self._get_archive(db, trade_date_value)
        if existing is None:
            existing = LimitUpClassificationArchive(trade_date=trade_date_value)
            db.add(existing)

        existing.payload_json = payload
        existing.status = "ready"
        existing.total_count = int(payload.get("total_count") or 0)
        existing.group_count = len(payload.get("groups") or [])
        existing.content_hash = content_hash
        existing.source_status = dict(payload.get("source_status") or {})
        existing.error = ""
        existing.archived_at = now
        existing.updated_at = now
        await db.commit()
        await db.refresh(existing)
        return existing

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

    async def rebuild_ai_classification_cache(self, requested_date: date) -> None:
        """Generate DeepSeek classification cache outside the HTTP request path."""
        try:
            async with async_session_maker() as db:
                trade_date = requested_date
                if requested_date != today_cn():
                    items = await self._load_db_items(requested_date, db)
                    if items:
                        trade_date = items[0]["trade_date"]
                else:
                    items, _ = await self._load_realtime_items(requested_date)
                    if not items:
                        items = await self._load_db_items(requested_date, db)
                        if items:
                            trade_date = items[0]["trade_date"]
                normalized = [self._normalize_item(item, trade_date) for item in items]
                if not normalized:
                    return
                await self._apply_ths_article_analysis(trade_date, normalized, {})
                await self._apply_ths_move_interpretation(db, trade_date, normalized, {})
                await self._apply_ai_classification(
                    db,
                    trade_date,
                    normalized,
                    {},
                    force=True,
                )
        except Exception as exc:
            logger.warning(f"Background DeepSeek limit-up classification failed: {exc}")

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

        if not force:
            if existing and existing.status == "ready" and trade_date != today_cn():
                payload = dict(existing.classifications_json or {})
                next_stocks = self._apply_ai_payload(stocks, payload)
                applied_count = self._ai_applied_count(next_stocks)
                if applied_count:
                    source_status["ai_classification"] = "cache_stale_partial_hit"
                    source_status["ai_model"] = existing.model or ""
                    source_status["ai_applied_count"] = str(applied_count)
                    return next_stocks, "ai"
            source_status["ai_classification"] = "cache_stale" if existing else "cache_miss"
            if existing and existing.model:
                source_status["ai_model"] = existing.model
            return stocks, "rule"

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

    async def _get_archive(self, db: AsyncSession, trade_date: date) -> Optional[LimitUpClassificationArchive]:
        result = await db.execute(
            select(LimitUpClassificationArchive).where(LimitUpClassificationArchive.trade_date == trade_date)
        )
        return result.scalars().first()

    async def _get_archived_payload(self, db: AsyncSession, requested_date: date) -> Optional[Dict[str, Any]]:
        if requested_date == today_cn():
            archive = await self._get_archive(db, requested_date)
            if archive is not None and archive.status != "ready":
                archive = None
        else:
            result = await db.execute(
                select(LimitUpClassificationArchive)
                .where(
                    LimitUpClassificationArchive.trade_date <= requested_date,
                    LimitUpClassificationArchive.status == "ready",
                )
                .order_by(LimitUpClassificationArchive.trade_date.desc())
                .limit(1)
            )
            archive = result.scalars().first()
        if archive is None:
            return None
        payload = dict(archive.payload_json or {})
        payload["requested_date"] = requested_date.isoformat()
        payload["trade_date"] = archive.trade_date.isoformat()
        payload["is_fallback"] = archive.trade_date != requested_date
        source_status = dict(payload.get("source_status") or {})
        source_status["classification_archive"] = "hit"
        source_status["classification_archive_at"] = archive.archived_at.isoformat(timespec="seconds") if archive.archived_at else ""
        payload["source_status"] = source_status
        payload["updated_at"] = archive.archived_at.isoformat(timespec="seconds") if archive.archived_at else payload.get("updated_at", "")
        return payload

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

    @staticmethod
    def _ai_applied_count(stocks: List[Dict[str, Any]]) -> int:
        return sum(1 for stock in stocks if stock.get("classification_method") == "ai")

    async def _apply_ths_article_analysis(
        self,
        trade_date: date,
        stocks: List[Dict[str, Any]],
        source_status: Dict[str, str],
    ) -> None:
        if trade_date != today_cn():
            source_status["ths_article_analysis"] = "skipped_historical"
            return
        if not self.ths_analysis_source:
            source_status["ths_article_analysis"] = "not_configured"
            return

        codes = [str(stock.get("stock_code") or "").strip() for stock in stocks if stock.get("stock_code")]
        try:
            analyses = await self.ths_analysis_source.get_daily_analyses(
                trade_date,
                target_codes=codes,
                force_refresh=False,
            )
        except Exception as exc:
            logger.warning(f"THS article analysis fetch failed: {exc}")
            source_status["ths_article_analysis"] = "error"
            source_status["ths_article_error"] = str(exc)[:120]
            return

        analysis_map = {
            str(self._analysis_value(analysis, "stock_code") or "").strip(): analysis
            for analysis in analyses or []
            if self._analysis_value(analysis, "stock_code")
        }
        used_count = 0
        for stock in stocks:
            analysis = analysis_map.get(str(stock.get("stock_code") or "").strip())
            if not analysis:
                continue

            title = self._clean_move_text(self._analysis_value(analysis, "title"), limit=100)
            summary = self._clean_move_text(self._analysis_value(analysis, "summary"), limit=320)
            evidence = self._clean_move_text(self._analysis_value(analysis, "evidence") or summary, limit=280)
            if not title and not evidence:
                continue

            decision = self.classify_ths_article_analysis(
                title=title,
                summary=summary,
                evidence=evidence,
                fallback_reason=stock.get("limit_up_reason") or "",
            )
            if not decision["classified_plate"]:
                continue

            stock["ths_move_title"] = title
            stock["ths_move_summary"] = evidence or summary
            stock["classification_basis"] = "ths_move_analysis"
            stock["classified_plate"] = decision["classified_plate"]
            stock["rule_classified_plate"] = decision["classified_plate"]
            stock["primary_theme"] = decision["primary_theme"]
            stock["fine_theme"] = decision["fine_theme"]
            stock["secondary_themes"] = decision["secondary_themes"]
            stock["fine_themes"] = decision["fine_themes"]
            stock["classification_evidence"] = evidence or summary
            stock["classification_confidence"] = decision["confidence"]
            stock["ths_article_url"] = str(self._analysis_value(analysis, "article_url") or "")
            stock["ths_article_time"] = str(self._analysis_value(analysis, "published_at") or "")
            used_count += 1

        if used_count:
            source_status["ths_article_analysis"] = "ok" if used_count == len(stocks) else "partial"
            source_status["classification_granularity"] = "ths_article_fine_theme"
        else:
            source_status["ths_article_analysis"] = "empty"
        source_status["ths_article_used_count"] = str(used_count)

    async def _apply_ths_move_interpretation(
        self,
        db: Optional[AsyncSession],
        trade_date: date,
        stocks: List[Dict[str, Any]],
        source_status: Dict[str, str],
    ) -> None:
        if trade_date != today_cn():
            source_status["ths_move_classification"] = "skipped_historical"
            return

        pending_stocks = [
            stock for stock in stocks
            if stock.get("classification_basis") != "ths_move_analysis"
        ]
        if not pending_stocks:
            source_status["ths_move_classification"] = "skipped_article_analysis"
            return
        if not self.ths_move_service:
            source_status["ths_move_classification"] = "not_configured"
            return
        if not isinstance(db, AsyncSession):
            source_status["ths_move_classification"] = "skipped_no_db"
            return

        semaphore = asyncio.Semaphore(self.ths_move_concurrency)
        counts = {"ok": 0, "empty": 0, "timeout": 0, "error": 0}

        async def enrich(stock: Dict[str, Any]) -> str:
            stock_code = str(stock.get("stock_code") or "").strip()
            if not stock_code:
                return "empty"
            async with semaphore:
                try:
                    payload = await asyncio.wait_for(
                        self.ths_move_service.get_stock_move(
                            stock_code,
                            trade_date,
                            source_scope="ths",
                            db=db,
                        ),
                        timeout=self.ths_move_timeout,
                    )
                except asyncio.TimeoutError:
                    return "timeout"
                except Exception as exc:
                    logger.warning(f"THS move interpretation failed for {stock_code}: {exc}")
                    return "error"

            title, summary = self._extract_ths_move_interpretation(payload)
            if not title and not summary:
                return "empty"

            fine_themes = self.extract_fine_themes_from_texts([title, summary])
            if not fine_themes:
                fine_themes = self.extract_fine_themes_from_texts([stock.get("limit_up_reason") or ""])
            move_text = self._join_reason_texts([title, summary])
            rule_classified_plate = fine_themes[0] if fine_themes else self.classify_reason(move_text)
            if not rule_classified_plate:
                return "empty"

            stock["ths_move_title"] = title
            stock["ths_move_summary"] = summary
            stock["classification_basis"] = "ths_move"
            stock["rule_classified_plate"] = rule_classified_plate
            stock["classified_plate"] = rule_classified_plate
            stock["fine_themes"] = fine_themes or [rule_classified_plate]
            stock["primary_theme"] = rule_classified_plate
            stock["fine_theme"] = fine_themes[0] if fine_themes else rule_classified_plate
            stock["secondary_themes"] = [theme for theme in fine_themes[1:] if theme != rule_classified_plate]
            stock["classification_evidence"] = summary or title
            stock["classification_confidence"] = 0.78
            return "ok"

        results = await asyncio.gather(*(enrich(stock) for stock in pending_stocks))
        for result in results:
            counts[result if result in counts else "error"] += 1

        if counts["ok"]:
            status = "ok" if counts["ok"] == len(pending_stocks) else "partial"
            if source_status.get("classification_granularity") != "ths_article_fine_theme":
                source_status["classification_granularity"] = "ths_move_fine_theme"
        elif counts["timeout"] and not counts["error"]:
            status = "timeout"
        elif counts["error"]:
            status = "error"
        else:
            status = "empty"
        source_status["ths_move_classification"] = status
        source_status["ths_move_used_count"] = str(counts["ok"])
        source_status["ths_move_empty_count"] = str(counts["empty"])
        source_status["ths_move_timeout_count"] = str(counts["timeout"])
        source_status["ths_move_error_count"] = str(counts["error"])

    def _classification_content_hash(self, stocks: List[Dict[str, Any]]) -> str:
        payload = [
            {
                "stock_code": stock["stock_code"],
                "stock_name": stock["stock_name"],
                "limit_up_reason": stock["limit_up_reason"],
                "primary_theme": stock.get("primary_theme") or "",
                "fine_theme": stock.get("fine_theme") or "",
                "secondary_themes": stock.get("secondary_themes") or [],
                "classification_evidence": stock.get("classification_evidence") or "",
                "ths_move_title": stock.get("ths_move_title") or "",
                "ths_move_summary": stock.get("ths_move_summary") or "",
                "classification_basis": stock.get("classification_basis") or "",
                "ths_article_url": stock.get("ths_article_url") or "",
                "ths_article_time": stock.get("ths_article_time") or "",
                "rule_classified_plate": stock["rule_classified_plate"],
                "fine_themes": stock.get("fine_themes") or [],
                "first_limit_up_time": stock["first_limit_up_time"],
                "final_seal_time": stock["final_seal_time"],
                "continuous_limit_up_days": stock["continuous_limit_up_days"],
            }
            for stock in sorted(stocks, key=lambda item: item["stock_code"])
        ]
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    @classmethod
    def _json_safe_payload(cls, payload: Dict[str, Any]) -> Dict[str, Any]:
        return json.loads(json.dumps(payload, ensure_ascii=False, default=cls._json_default))

    @staticmethod
    def _json_default(value: Any) -> str:
        if isinstance(value, (date, datetime, time)):
            return value.isoformat()
        return str(value)

    @classmethod
    def _payload_content_hash(cls, payload: Dict[str, Any]) -> str:
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=cls._json_default)
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    @staticmethod
    def _payload_trade_date(payload: Dict[str, Any], fallback_date: date) -> date:
        value = payload.get("trade_date") or fallback_date
        if isinstance(value, date):
            return value
        return datetime.strptime(str(value), "%Y-%m-%d").date()

    def _normalize_item(self, item: Dict[str, Any], default_trade_date: date) -> Dict[str, Any]:
        is_sealed = bool(item.get("is_sealed", item.get("is_final_sealed", True)))
        current_status = item.get("current_status") or ("sealed" if is_sealed else "opened")
        reason = item.get("limit_up_reason") or ""
        fine_themes = self.extract_fine_themes(reason)
        rule_classified_plate = fine_themes[0] if fine_themes else self.classify_reason(reason)
        primary_theme = rule_classified_plate
        fine_theme = fine_themes[0] if fine_themes else rule_classified_plate
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
            "fine_themes": fine_themes,
            "primary_theme": primary_theme,
            "fine_theme": fine_theme,
            "secondary_themes": [theme for theme in fine_themes[1:] if theme != fine_theme],
            "classification_evidence": reason[:280],
            "classification_confidence": 0.55 if reason else 0.0,
            "classification_basis": "limit_up_reason",
            "ths_move_title": "",
            "ths_move_summary": "",
            "ths_article_url": "",
            "ths_article_time": "",
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
    def classify_ths_article_analysis(
        cls,
        *,
        title: str,
        summary: str,
        evidence: str,
        fallback_reason: str = "",
    ) -> Dict[str, Any]:
        source_text = cls._join_reason_texts([title, evidence, summary])
        fallback_text = fallback_reason or ""
        title_themes = cls._extract_title_themes(title)
        evidence_alias_themes = cls._find_alias_themes_by_position(cls._join_reason_texts([evidence, summary]))
        fallback_themes = cls.extract_fine_themes(fallback_text, limit=4)
        all_themes = cls._merge_themes(title_themes, evidence_alias_themes, fallback_themes)
        event_theme = cls._detect_priority_event(title)
        if not event_theme and not cls._is_industry_background(evidence):
            event_theme = cls._detect_priority_event(source_text)
        if event_theme:
            fine_theme = cls._event_fine_theme(event_theme, source_text, all_themes)
            secondary_themes = [
                theme for theme in all_themes
                if theme not in {event_theme, fine_theme}
            ]
            fine_themes = cls._merge_themes([fine_theme], secondary_themes, [event_theme])
            return {
                "classified_plate": event_theme,
                "primary_theme": event_theme,
                "fine_theme": fine_theme,
                "secondary_themes": secondary_themes[:4],
                "fine_themes": fine_themes[:5],
                "confidence": 0.92,
            }

        if all_themes:
            plate = all_themes[0]
            return {
                "classified_plate": plate,
                "primary_theme": plate,
                "fine_theme": plate,
                "secondary_themes": all_themes[1:5],
                "fine_themes": all_themes[:5],
                "confidence": 0.82,
            }

        fallback_plate = cls.classify_reason(cls._join_reason_texts([source_text, fallback_text]))
        return {
            "classified_plate": fallback_plate,
            "primary_theme": fallback_plate,
            "fine_theme": fallback_plate,
            "secondary_themes": [],
            "fine_themes": [fallback_plate] if fallback_plate else [],
            "confidence": 0.62 if fallback_plate and fallback_plate != "其他" else 0.4,
        }

    @classmethod
    def classify_reason(cls, reason: str) -> str:
        fine_themes = cls.extract_fine_themes(reason, limit=1)
        if fine_themes:
            return fine_themes[0]
        return cls._classify_broad_reason(reason)

    @classmethod
    def extract_fine_themes(cls, reason: str, limit: int = 4) -> List[str]:
        themes: List[str] = []
        for raw_part in cls._split_reason(reason):
            theme = cls._normalize_fine_theme(raw_part)
            if not theme or theme in themes:
                continue
            themes.append(theme)
            if len(themes) >= limit:
                break
        return themes

    @classmethod
    def extract_fine_themes_from_texts(cls, texts: Iterable[str], limit: int = 4) -> List[str]:
        themes: List[str] = []

        def add(theme: str) -> None:
            if theme and theme not in themes and len(themes) < limit:
                themes.append(theme)

        for text in texts:
            if len(themes) >= limit:
                break
            for theme in cls._find_alias_themes_by_position(text):
                add(theme)
            for theme in cls.extract_fine_themes(text, limit=limit):
                add(theme)
        return themes

    @classmethod
    def _find_alias_themes_by_position(cls, text: str) -> List[str]:
        normalized = re.sub(r"\s+", "", text or "")
        if not normalized:
            return []
        matches = []
        for order, (keyword, canonical) in enumerate(cls._FINE_THEME_ALIASES):
            index = normalized.find(keyword)
            if index >= 0 and not cls._is_low_signal_theme(canonical):
                matches.append((index, order, canonical))

        themes: List[str] = []
        for _, _, theme in sorted(matches):
            if theme not in themes:
                themes.append(theme)
        return themes

    @classmethod
    def _extract_title_themes(cls, title: str) -> List[str]:
        text = cls._clean_move_text(title, limit=160)
        if not text:
            return []
        match = re.search(r"涨停雷达[:：](.*?)(?:触及涨停|涨停)", text)
        theme_text = match.group(1) if match else text
        theme_text = re.split(r"\s+[^+\s]{2,12}$", theme_text.strip(), maxsplit=1)[0]
        if "+" in theme_text and " " in theme_text:
            theme_text = theme_text.split(" ", 1)[0]
        return cls.extract_fine_themes(theme_text, limit=5)

    @classmethod
    def _detect_priority_event(cls, text: str) -> str:
        normalized = re.sub(r"\s+", "", text or "")
        if not normalized:
            return ""
        if re.search(r"(拟取得|拟获得|取得|获得).{0,20}控制权", normalized):
            return "并购重组"
        for event_theme, keywords in cls._EVENT_PRIORITY_RULES:
            if any(keyword in normalized for keyword in keywords):
                return event_theme
        return ""

    @staticmethod
    def _is_industry_background(text: str) -> bool:
        return re.sub(r"\s+", "", text or "").startswith("行业原因")

    @classmethod
    def _event_fine_theme(cls, event_theme: str, text: str, themes: List[str]) -> str:
        normalized = re.sub(r"\s+", "", text or "")
        if event_theme == "并购重组":
            if any(keyword in normalized for keyword in ("半导体", "芯片", "集成电路", "封装测试", "存储")):
                return "收购半导体"
            if any(keyword in normalized for keyword in ("机器人", "减速器", "执行器")):
                return "收购机器人"
            if any(keyword in normalized for keyword in ("算力", "数据中心", "服务器")):
                return "收购算力资产"
            if any(keyword in normalized for keyword in ("电池", "储能", "光伏")):
                return "收购新能源资产"
            return "并购重组"
        if event_theme == "重大订单":
            if any(keyword in normalized for keyword in ("算力", "数据中心", "服务器")):
                return "算力订单"
            if any(keyword in normalized for keyword in ("电力", "储能", "光伏", "风电")):
                return "新能源订单"
            return "重大订单"
        if event_theme == "业绩增长":
            if "扭亏" in normalized:
                return "扭亏增长"
            if "预增" in normalized:
                return "业绩预增"
            return "业绩增长"
        if event_theme == "资产处置":
            return "资产处置"
        return themes[0] if themes else event_theme

    @staticmethod
    def _merge_themes(*theme_lists: Iterable[str]) -> List[str]:
        themes: List[str] = []
        for theme_list in theme_lists:
            for raw_theme in theme_list or []:
                theme = str(raw_theme or "").strip()
                if theme and theme not in themes:
                    themes.append(theme)
        return themes

    @classmethod
    def _normalize_fine_theme(cls, raw_theme: str) -> str:
        theme = re.sub(r"\s+", "", raw_theme or "")
        theme = theme.strip("：:（）()[]【】")
        if not theme:
            return ""
        theme = re.sub(r"[ⅠⅡⅢIV]+$", "", theme)
        theme = re.sub(r"(概念|板块|方向|业务|应用)$", "", theme)
        if cls._is_low_signal_theme(theme):
            return ""
        for keyword, canonical in cls._FINE_THEME_ALIASES:
            if keyword in theme:
                return canonical
        if len(theme) > 18:
            return ""
        return theme

    @classmethod
    def _is_low_signal_theme(cls, theme: str) -> bool:
        return any(keyword in theme for keyword in cls._LOW_SIGNAL_THEME_KEYWORDS)

    @classmethod
    def _classify_broad_reason(cls, reason: str) -> str:
        text = reason or ""
        if not text:
            return "其他"
        lowered = text.lower()
        for category, keywords in cls._CATEGORY_KEYWORDS.items():
            if any(keyword.lower() in lowered for keyword in keywords):
                return category
        parts = cls._split_reason(text)
        return parts[0] if parts else "其他"

    @classmethod
    def _extract_ths_move_interpretation(cls, payload: Any) -> tuple[str, str]:
        if not isinstance(payload, dict):
            return "", ""

        for item in payload.get("items") or []:
            if not isinstance(item, dict):
                continue
            for reason in item.get("reasons") or []:
                if not isinstance(reason, dict):
                    continue
                title = cls._clean_move_text(reason.get("title"), limit=80)
                summary = cls._clean_move_text(reason.get("content"), limit=240)
                if title and title != "暂无异动原因":
                    return title, summary if summary != title else summary

            concepts = item.get("concepts") or item.get("related_plates") or []
            if isinstance(concepts, list):
                concept_title = "+".join(
                    str(concept).strip()
                    for concept in concepts
                    if str(concept or "").strip()
                )
                concept_title = cls._clean_move_text(concept_title, limit=80)
                if concept_title:
                    return concept_title, ""

        return "", ""

    @staticmethod
    def _clean_move_text(value: Any, *, limit: int) -> str:
        text = re.sub(r"\s+", " ", str(value or "")).strip()
        if text in {"暂无异动原因", "暂无同花顺异动", "暂无同花顺异动解析数据"}:
            return ""
        return text[:limit]

    @staticmethod
    def _analysis_value(analysis: Any, field_name: str) -> Any:
        if isinstance(analysis, dict):
            return analysis.get(field_name)
        return getattr(analysis, field_name, None)

    @staticmethod
    def _join_reason_texts(texts: Iterable[str]) -> str:
        return "+".join(str(text or "").strip() for text in texts if str(text or "").strip())

    @staticmethod
    def _split_reason(reason: str) -> List[str]:
        normalized = (
            reason.replace("/", "+")
            .replace("＋", "+")
            .replace("／", "+")
            .replace("，", "+")
            .replace("、", "+")
            .replace(",", "+")
            .replace("；", "+")
            .replace(";", "+")
            .replace("|", "+")
            .replace("｜", "+")
        )
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


from app.services.tdx_plugin_service import tdx_plugin_service  # noqa: E402
from app.services.ths_move_analysis_source import ths_move_analysis_source  # noqa: E402


ths_limit_up_classification_service = ThsLimitUpClassificationService(
    ths_analysis_source=ths_move_analysis_source,
    ths_move_service=tdx_plugin_service,
)
