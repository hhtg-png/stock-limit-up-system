"""
涨停分析引擎
"""
from typing import Dict, List, Optional, Tuple
from datetime import datetime, date, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_
from loguru import logger

from app.models.stock import Stock
from app.models.limit_up import LimitUpRecord, LimitUpStatusChange
from app.core.event_bus import event_bus, Event, EventType
from app.core.websocket_manager import manager
from app.utils.stock_utils import is_at_limit_up, calculate_limit_up_price


class LimitUpAnalyzer:
    """涨停分析引擎"""
    
    def __init__(self):
        # 当前涨停状态缓存 {stock_code: LimitUpState}
        self._limit_up_cache: Dict[str, Dict] = {}
    
    async def analyze_quote(self, quote: Dict, stock: Stock, 
                           db: AsyncSession) -> Optional[Dict]:
        """
        分析单只股票行情，检测涨停状态变化
        
        Args:
            quote: 行情数据
            stock: 股票对象
            db: 数据库会话
        
        Returns:
            涨停事件数据或None
        """
        stock_code = stock.stock_code
        current_price = quote.get("current_price", 0)
        pre_close = quote.get("pre_close", 0)
        
        if not current_price or not pre_close:
            return None
        
        # 计算涨停价
        limit_up_price = calculate_limit_up_price(pre_close, stock_code, stock.stock_name)
        
        # 检查是否涨停
        is_limit_up = is_at_limit_up(current_price, pre_close, stock_code, stock.stock_name)
        
        # 获取缓存的状态
        cached_state = self._limit_up_cache.get(stock_code)
        
        # 获取卖一挂单量（判断是否封板）
        ask1_volume = quote.get("ask1_volume", 0) if isinstance(quote.get("ask_volumes"), list) else quote.get("ask_volumes", [0])[0] if quote.get("ask_volumes") else 0
        is_sealed = is_limit_up and ask1_volume == 0  # 涨停且无卖盘
        
        now = datetime.now()
        
        if is_limit_up:
            if not cached_state:
                # 首次涨停
                self._limit_up_cache[stock_code] = {
                    "first_limit_up_time": now,
                    "is_sealed": is_sealed,
                    "open_count": 0,
                    "limit_up_price": limit_up_price,
                    "last_status": "sealed" if is_sealed else "limit_up"
                }
                
                # 触发涨停事件
                await self._emit_limit_up_event(stock, now, limit_up_price)
                
                return {
                    "type": "first_limit_up",
                    "stock_code": stock_code,
                    "stock_name": stock.stock_name,
                    "time": now,
                    "price": limit_up_price
                }
            
            elif cached_state.get("last_status") == "opened" and is_sealed:
                # 回封
                cached_state["last_status"] = "sealed"
                cached_state["is_sealed"] = True
                
                # 触发回封事件
                await self._emit_reseal_event(stock, now, limit_up_price)
                
                return {
                    "type": "reseal",
                    "stock_code": stock_code,
                    "stock_name": stock.stock_name,
                    "time": now,
                    "price": limit_up_price
                }
        
        elif cached_state and cached_state.get("is_sealed"):
            # 开板（之前封板，现在不是涨停）
            cached_state["is_sealed"] = False
            cached_state["last_status"] = "opened"
            cached_state["open_count"] = cached_state.get("open_count", 0) + 1
            
            # 触发开板事件
            await self._emit_open_event(stock, now, current_price)
            
            return {
                "type": "open",
                "stock_code": stock_code,
                "stock_name": stock.stock_name,
                "time": now,
                "price": current_price
            }
        
        return None
    
    async def _emit_limit_up_event(self, stock: Stock, time: datetime, price: float):
        """发送涨停事件"""
        event = Event(
            type=EventType.LIMIT_UP,
            data={
                "stock_code": stock.stock_code,
                "stock_name": stock.stock_name,
                "time": time.isoformat(),
                "price": price
            }
        )
        await event_bus.publish(event)
        
        # WebSocket广播
        await manager.broadcast_limit_up_alert(
            stock.stock_code,
            stock.stock_name,
            time.strftime("%H:%M:%S")
        )
    
    async def _emit_open_event(self, stock: Stock, time: datetime, price: float):
        """发送开板事件"""
        event = Event(
            type=EventType.LIMIT_UP_OPEN,
            data={
                "stock_code": stock.stock_code,
                "stock_name": stock.stock_name,
                "time": time.isoformat(),
                "price": price
            }
        )
        await event_bus.publish(event)
        
        # WebSocket广播
        await manager.broadcast_status_change(
            stock.stock_code,
            stock.stock_name,
            "opened",
            time.strftime("%H:%M:%S"),
            price
        )
    
    async def _emit_reseal_event(self, stock: Stock, time: datetime, price: float):
        """发送回封事件"""
        event = Event(
            type=EventType.LIMIT_UP_RESEAL,
            data={
                "stock_code": stock.stock_code,
                "stock_name": stock.stock_name,
                "time": time.isoformat(),
                "price": price
            }
        )
        await event_bus.publish(event)
        
        # WebSocket广播
        await manager.broadcast_status_change(
            stock.stock_code,
            stock.stock_name,
            "resealed",
            time.strftime("%H:%M:%S"),
            price
        )
    
    async def calculate_continuous_days(self, stock_id: int, trade_date: date,
                                        db: AsyncSession) -> int:
        """
        计算连板天数
        
        Args:
            stock_id: 股票ID
            trade_date: 交易日期
            db: 数据库会话
        
        Returns:
            连板天数
        """
        continuous_days = 1
        check_date = trade_date - timedelta(days=1)
        
        while True:
            # 跳过周末
            while check_date.weekday() >= 5:
                check_date -= timedelta(days=1)
            
            # 查询前一天是否涨停
            query = select(LimitUpRecord).where(and_(
                LimitUpRecord.stock_id == stock_id,
                LimitUpRecord.trade_date == check_date
            ))
            result = await db.execute(query)
            record = result.scalar_one_or_none()
            
            if record:
                continuous_days += 1
                check_date -= timedelta(days=1)
            else:
                break
            
            # 最多检查30天
            if continuous_days > 30:
                break
        
        return continuous_days
    
    async def save_limit_up_record(self, stock: Stock, data: Dict,
                                   db: AsyncSession) -> LimitUpRecord:
        """
        保存涨停记录
        
        Args:
            stock: 股票对象
            data: 涨停数据
            db: 数据库会话
        
        Returns:
            涨停记录对象
        """
        trade_date = date.today()
        
        # 检查是否已存在
        query = select(LimitUpRecord).where(and_(
            LimitUpRecord.stock_id == stock.id,
            LimitUpRecord.trade_date == trade_date
        ))
        result = await db.execute(query)
        existing = result.scalar_one_or_none()
        
        if existing:
            # 更新记录
            for key, value in data.items():
                if hasattr(existing, key) and value is not None:
                    setattr(existing, key, value)
            await db.commit()
            return existing
        
        # 计算连板天数
        continuous_days = await self.calculate_continuous_days(stock.id, trade_date, db)
        
        # 创建新记录
        record = LimitUpRecord(
            stock_id=stock.id,
            trade_date=trade_date,
            first_limit_up_time=data.get("first_limit_up_time"),
            limit_up_reason=data.get("limit_up_reason"),
            reason_category=data.get("reason_category"),
            continuous_limit_up_days=continuous_days,
            open_count=data.get("open_count", 0),
            is_final_sealed=data.get("is_final_sealed", True),
            seal_amount=data.get("seal_amount"),
            limit_up_price=data.get("limit_up_price"),
            turnover_rate=data.get("turnover_rate"),
            amount=data.get("amount"),
            data_source=data.get("data_source", "TDX")
        )
        
        db.add(record)
        await db.commit()
        await db.refresh(record)
        
        return record
    
    def clear_cache(self):
        """清空缓存（每日收盘后调用）"""
        self._limit_up_cache.clear()
        logger.info("LimitUpAnalyzer cache cleared")


# 全局分析器实例
limit_up_analyzer = LimitUpAnalyzer()
