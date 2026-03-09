"""
大单分析引擎
"""
from typing import Dict, List, Optional
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from loguru import logger

from app.models.stock import Stock
from app.models.big_order import BigOrder
from app.core.event_bus import event_bus, Event, EventType
from app.core.websocket_manager import manager
from app.config import settings


class BigOrderAnalyzer:
    """大单分析引擎"""
    
    def __init__(self):
        # 阈值配置（区分主板和20cm板块）
        self.threshold_volume = settings.DEFAULT_BIG_ORDER_VOLUME  # 主板手数阈值
        self.threshold_volume_20cm = settings.DEFAULT_BIG_ORDER_VOLUME_20CM  # 20cm板块手数阈值
        
        # 去重缓存 {stock_code: {direction: last_alert_time}}
        self._dedup_cache: Dict[str, Dict] = {}
        self._dedup_interval = settings.ALERT_DEDUP_INTERVAL  # 去重间隔（秒）
    
    def update_threshold(self, volume: int = None, volume_20cm: int = None):
        """更新阈值配置"""
        if volume is not None:
            self.threshold_volume = volume
        if volume_20cm is not None:
            self.threshold_volume_20cm = volume_20cm
        logger.info(f"Big order threshold updated: volume={self.threshold_volume}, volume_20cm={self.threshold_volume_20cm}")
    
    def _is_20cm_stock(self, stock_code: str) -> bool:
        """判断是否为20cm股票(科创板688/创业板300)"""
        return stock_code.startswith("688") or stock_code.startswith("300")
    
    def is_big_order(self, trade_volume: int, stock_code: str = "") -> bool:
        """
        判断是否为大单（根据股票类型使用不同阈值）
        
        Args:
            trade_volume: 成交量（手）
            stock_code: 股票代码，用于判断是否20cm
        
        Returns:
            是否为大单
        """
        if self._is_20cm_stock(stock_code):
            return trade_volume >= self.threshold_volume_20cm
        return trade_volume >= self.threshold_volume
    
    def determine_order_type(self, trade_price: float, bid1_price: float,
                            ask1_price: float) -> tuple:
        """
        判断订单类型（主动买/卖）
        
        Args:
            trade_price: 成交价
            bid1_price: 买一价
            ask1_price: 卖一价
        
        Returns:
            (方向, 类型)
        """
        if ask1_price > 0 and trade_price >= ask1_price:
            # 成交价>=卖一价，主动买入
            return ("buy", "active_buy")
        elif bid1_price > 0 and trade_price <= bid1_price:
            # 成交价<=买一价，主动卖出
            return ("sell", "active_sell")
        else:
            # 介于买卖一价之间
            if bid1_price > 0 and ask1_price > 0:
                mid_price = (bid1_price + ask1_price) / 2
                if trade_price > mid_price:
                    return ("buy", "passive_buy")
                else:
                    return ("sell", "passive_sell")
            return ("unknown", "unknown")
    
    async def analyze_transaction(self, stock: Stock, transaction: Dict,
                                  orderbook: Dict, db: AsyncSession) -> Optional[Dict]:
        """
        分析单笔成交，判断是否为大单
        
        Args:
            stock: 股票对象
            transaction: 成交数据
            orderbook: 盘口数据
            db: 数据库会话
        
        Returns:
            大单数据或None
        """
        trade_price = transaction.get("price", 0)
        trade_volume = transaction.get("volume", 0)
        trade_amount = trade_price * trade_volume * 100  # 手转股
        
        # 判断是否为大单（根据股票类型使用不同阈值）
        if not self.is_big_order(trade_volume, stock.stock_code):
            return None
        
        # 获取盘口数据
        bid1_price = orderbook.get("bid1_price", 0)
        ask1_price = orderbook.get("ask1_price", 0)
        
        # 判断主动买卖
        direction, order_type = self.determine_order_type(trade_price, bid1_price, ask1_price)
        
        # 判断是否涨停价
        limit_up_price = orderbook.get("limit_up_price", 0)
        is_limit_up_price = abs(trade_price - limit_up_price) < 0.01 if limit_up_price else False
        
        now = datetime.now()
        
        # 创建大单记录
        big_order = BigOrder(
            stock_id=stock.id,
            trade_time=now,
            trade_price=trade_price,
            trade_volume=trade_volume,
            trade_amount=trade_amount,
            direction=direction,
            order_type=order_type,
            is_limit_up_price=1 if is_limit_up_price else 0,
            bid1_price=bid1_price,
            ask1_price=ask1_price
        )
        
        db.add(big_order)
        await db.commit()
        
        # 发送大单事件（带去重）
        if await self._should_alert(stock.stock_code, direction):
            await self._emit_big_order_event(stock, big_order)
        
        return {
            "stock_code": stock.stock_code,
            "stock_name": stock.stock_name,
            "direction": direction,
            "order_type": order_type,
            "trade_amount": trade_amount,
            "trade_volume": trade_volume,
            "trade_price": trade_price,
            "is_limit_up_price": is_limit_up_price,
            "time": now
        }
    
    async def _should_alert(self, stock_code: str, direction: str) -> bool:
        """检查是否应该发送播报（去重）"""
        now = datetime.now()
        cache_key = f"{stock_code}_{direction}"
        
        if stock_code not in self._dedup_cache:
            self._dedup_cache[stock_code] = {}
        
        last_alert = self._dedup_cache[stock_code].get(direction)
        
        if last_alert:
            elapsed = (now - last_alert).total_seconds()
            if elapsed < self._dedup_interval:
                return False
        
        self._dedup_cache[stock_code][direction] = now
        return True
    
    async def _emit_big_order_event(self, stock: Stock, big_order: BigOrder):
        """发送大单事件"""
        event = Event(
            type=EventType.BIG_ORDER,
            data={
                "stock_code": stock.stock_code,
                "stock_name": stock.stock_name,
                "direction": big_order.direction,
                "order_type": big_order.order_type,
                "amount": big_order.trade_amount,
                "time": big_order.trade_time.isoformat()
            }
        )
        await event_bus.publish(event)
        
        # WebSocket广播
        await manager.broadcast_big_order_alert(
            stock.stock_code,
            stock.stock_name,
            big_order.direction,
            big_order.trade_amount,
            big_order.trade_time.strftime("%H:%M:%S"),
            big_order.order_type
        )
    
    def clear_cache(self):
        """清空去重缓存"""
        self._dedup_cache.clear()
        logger.info("BigOrderAnalyzer dedup cache cleared")


# 全局分析器实例
big_order_analyzer = BigOrderAnalyzer()
