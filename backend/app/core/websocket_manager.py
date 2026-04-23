"""
WebSocket连接管理器
"""
from fastapi import WebSocket
from typing import Dict, Set, Optional, List
import asyncio
import json
import time as time_module
from datetime import datetime
from loguru import logger

from app.config import settings


class ConnectionManager:
    """WebSocket连接管理器"""
    
    def __init__(self):
        # 活跃连接池
        self.active_connections: Dict[str, WebSocket] = {}
        # 用户订阅的股票
        self.subscriptions: Dict[str, Set[str]] = {}
        # 用户订阅的消息类型
        self.message_types: Dict[str, Set[str]] = {}
        # 连接锁
        self._lock = asyncio.Lock()
        # 涨停播报去重 {stock_code: timestamp}
        self._limit_up_alert_cache: Dict[str, float] = {}
    
    async def connect(self, websocket: WebSocket, client_id: str):
        """建立连接"""
        await websocket.accept()
        async with self._lock:
            self.active_connections[client_id] = websocket
            self.subscriptions[client_id] = set()
            self.message_types[client_id] = {
                "limit_up_alert",
                "big_order_alert",
                "status_change",
                "market_update",
                "continuous_ladder",
                "limit_up_snapshot",
                "limit_up_delta",
            }
        logger.info(f"WebSocket connected: {client_id}, total: {len(self.active_connections)}")
    
    async def disconnect(self, client_id: str):
        """断开连接"""
        async with self._lock:
            if client_id in self.active_connections:
                del self.active_connections[client_id]
            if client_id in self.subscriptions:
                del self.subscriptions[client_id]
            if client_id in self.message_types:
                del self.message_types[client_id]
        logger.info(f"WebSocket disconnected: {client_id}, total: {len(self.active_connections)}")
    
    async def subscribe_stock(self, client_id: str, stock_codes: List[str]):
        """订阅股票"""
        async with self._lock:
            if client_id in self.subscriptions:
                self.subscriptions[client_id].update(stock_codes)
    
    async def unsubscribe_stock(self, client_id: str, stock_codes: List[str]):
        """取消订阅股票"""
        async with self._lock:
            if client_id in self.subscriptions:
                self.subscriptions[client_id] -= set(stock_codes)
    
    async def subscribe_message_type(self, client_id: str, types: List[str]):
        """订阅消息类型"""
        async with self._lock:
            if client_id in self.message_types:
                self.message_types[client_id].update(types)
    
    async def unsubscribe_message_type(self, client_id: str, types: List[str]):
        """取消订阅消息类型"""
        async with self._lock:
            if client_id in self.message_types:
                self.message_types[client_id] -= set(types)
    
    async def send_personal_message(self, message: dict, client_id: str):
        """发送个人消息"""
        if client_id in self.active_connections:
            try:
                await self.active_connections[client_id].send_json(message)
            except Exception as e:
                logger.error(f"Send message error to {client_id}: {e}")
                await self.disconnect(client_id)
    
    async def broadcast(self, message: dict, message_type: str, stock_code: Optional[str] = None):
        """广播消息"""
        disconnected = []
        
        for client_id, websocket in list(self.active_connections.items()):
            # 检查消息类型订阅
            if message_type not in self.message_types.get(client_id, set()):
                continue
            
            # 检查股票订阅（如果指定了股票）
            if stock_code and self.subscriptions.get(client_id):
                if stock_code not in self.subscriptions[client_id]:
                    continue
            
            try:
                await websocket.send_json({
                    "type": message_type,
                    "data": message,
                    "timestamp": datetime.now().isoformat()
                })
            except Exception as e:
                logger.error(f"Broadcast error to {client_id}: {e}")
                disconnected.append(client_id)
        
        # 清理断开的连接
        for client_id in disconnected:
            await self.disconnect(client_id)
    
    async def broadcast_limit_up_alert(self, stock_code: str, stock_name: str, 
                                        time: str, reason: Optional[str] = None,
                                        continuous_days: int = 1):
        """广播涨停提醒"""
        now = time_module.time()
        last_alert_time = self._limit_up_alert_cache.get(stock_code)
        if last_alert_time is not None and now - last_alert_time < settings.ALERT_DEDUP_INTERVAL:
            logger.debug(f"Skip duplicate limit_up_alert for {stock_code}")
            return

        self._limit_up_alert_cache[stock_code] = now
        await self.broadcast(
            {
                "stock_code": stock_code,
                "stock_name": stock_name,
                "time": time,
                "reason": reason,
                "continuous_days": continuous_days
            },
            "limit_up_alert",
            stock_code
        )
    
    async def broadcast_big_order_alert(self, stock_code: str, stock_name: str,
                                         direction: str, amount: float, time: str,
                                         order_type: str):
        """广播大单提醒"""
        await self.broadcast(
            {
                "stock_code": stock_code,
                "stock_name": stock_name,
                "direction": direction,
                "amount": amount,
                "time": time,
                "order_type": order_type
            },
            "big_order_alert",
            stock_code
        )
    
    async def broadcast_status_change(self, stock_code: str, stock_name: str,
                                       status: str, time: str, price: float):
        """广播涨停状态变化"""
        await self.broadcast(
            {
                "stock_code": stock_code,
                "stock_name": stock_name,
                "status": status,
                "time": time,
                "price": price
            },
            "status_change",
            stock_code
        )
    
    async def broadcast_continuous_ladder(self, data: dict):
        """广播连板梯队数据"""
        await self.broadcast(
            data,
            "continuous_ladder"
        )

    async def broadcast_limit_up_snapshot(self, data: dict):
        """广播实时涨停列表快照"""
        await self.broadcast(
            data,
            "limit_up_snapshot"
        )

    async def broadcast_limit_up_delta(self, data: dict):
        """广播实时涨停列表增量更新"""
        await self.broadcast(
            data,
            "limit_up_delta"
        )
    
    @property
    def connection_count(self) -> int:
        """获取连接数"""
        return len(self.active_connections)


# 全局连接管理器实例
manager = ConnectionManager()
