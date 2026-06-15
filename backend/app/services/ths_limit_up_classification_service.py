"""Strict TongHuaShun limit-up reason classification service."""
from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, time
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.limit_up import LimitUpRecord
from app.models.stock import Stock
from app.services.realtime_limit_up_service import realtime_limit_up_service


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

    def __init__(self, *, realtime_service=None):
        self.realtime_service = realtime_service or realtime_limit_up_service

    async def get_classification(
        self,
        requested_date: date,
        *,
        db: Optional[AsyncSession] = None,
    ) -> Dict[str, Any]:
        source_status = {
            "classification_scope": "strict_ths",
            "limit_up_pool": "ok",
            "ths_reason": "ok",
        }
        items = await self.realtime_service.get_realtime_limit_up_list(requested_date)
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
        groups = self._build_groups(normalized)
        return {
            "requested_date": requested_date,
            "trade_date": trade_date,
            "is_fallback": is_fallback,
            "updated_at": datetime.now().isoformat(timespec="seconds"),
            "source_status": source_status,
            "total_count": len(normalized),
            "groups": groups,
        }

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

    def _normalize_item(self, item: Dict[str, Any], default_trade_date: date) -> Dict[str, Any]:
        is_sealed = bool(item.get("is_sealed", item.get("is_final_sealed", True)))
        current_status = item.get("current_status") or ("sealed" if is_sealed else "opened")
        reason = item.get("limit_up_reason") or ""
        classified_plate = self.classify_reason(reason)
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
            "classified_plate": classified_plate,
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
