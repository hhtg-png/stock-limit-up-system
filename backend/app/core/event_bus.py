"""
事件总线 - 解耦数据采集和推送
"""
import asyncio
from typing import Dict, List, Callable, Any
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from loguru import logger


class EventType(Enum):
    """事件类型"""
    LIMIT_UP = "limit_up"              # 涨停事件
    LIMIT_UP_OPEN = "limit_up_open"    # 开板事件
    LIMIT_UP_RESEAL = "limit_up_reseal"  # 回封事件
    BIG_ORDER = "big_order"            # 大单事件
    MARKET_UPDATE = "market_update"    # 行情更新


@dataclass
class Event:
    """事件数据"""
    type: EventType
    data: Dict[str, Any]
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


class EventBus:
    """事件总线"""
    
    def __init__(self):
        self._subscribers: Dict[EventType, List[Callable]] = {}
        self._queue: asyncio.Queue = asyncio.Queue()
        self._running = False
        self._task = None
    
    def subscribe(self, event_type: EventType, handler: Callable):
        """订阅事件"""
        if event_type not in self._subscribers:
            self._subscribers[event_type] = []
        self._subscribers[event_type].append(handler)
        logger.debug(f"Subscribed to {event_type.value}: {handler.__name__}")
    
    def unsubscribe(self, event_type: EventType, handler: Callable):
        """取消订阅"""
        if event_type in self._subscribers:
            self._subscribers[event_type].remove(handler)
    
    async def publish(self, event: Event):
        """发布事件"""
        await self._queue.put(event)
        logger.debug(f"Published event: {event.type.value}")
    
    def publish_sync(self, event: Event):
        """同步发布事件（用于非异步上下文）"""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.create_task(self._queue.put(event))
            else:
                loop.run_until_complete(self._queue.put(event))
        except RuntimeError:
            # 如果没有事件循环，创建一个新的
            asyncio.run(self._queue.put(event))
    
    async def _process_events(self):
        """处理事件队列"""
        while self._running:
            try:
                event = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                await self._dispatch(event)
                self._queue.task_done()
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Event processing error: {e}")
    
    async def _dispatch(self, event: Event):
        """分发事件给订阅者"""
        handlers = self._subscribers.get(event.type, [])
        for handler in handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(event)
                else:
                    handler(event)
            except Exception as e:
                logger.error(f"Event handler error ({handler.__name__}): {e}")
    
    async def start(self):
        """启动事件总线"""
        self._running = True
        self._task = asyncio.create_task(self._process_events())
        logger.info("EventBus started")
    
    async def stop(self):
        """停止事件总线"""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("EventBus stopped")


# 全局事件总线实例
event_bus = EventBus()
