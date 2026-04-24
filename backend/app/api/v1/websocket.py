"""
WebSocket路由
"""
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query
from typing import Optional
import json
import asyncio
from datetime import datetime, date, time as time_type
import uuid

from app.core.websocket_manager import manager
from app.services.continuous_ladder_service import continuous_ladder_service
from app.services.realtime_limit_up_service import realtime_limit_up_service
from app.services.realtime_limit_up_alert_tracker import RealtimeLimitUpAlertTracker
from app.services.realtime_limit_up_stream_tracker import RealtimeLimitUpStreamTracker
from loguru import logger

router = APIRouter()

# 实时涨停列表/播报 watcher
realtime_alert_tracker = RealtimeLimitUpAlertTracker()
realtime_stream_tracker = RealtimeLimitUpStreamTracker()
realtime_sync_task: Optional[asyncio.Task] = None
REALTIME_SYNC_INTERVAL = 2
REALTIME_SYNC_IDLE_INTERVAL = 30


@router.websocket("/ws/realtime")
async def websocket_endpoint(
    websocket: WebSocket,
    client_id: Optional[str] = Query(None)
):
    """WebSocket实时数据连接"""
    # 生成客户端ID
    if not client_id:
        client_id = str(uuid.uuid4())[:8]
    
    await manager.connect(websocket, client_id)
    await ensure_realtime_sync_watcher()
    
    try:
        # 发送连接成功消息
        await websocket.send_json({
            "type": "connected",
            "data": {
                "client_id": client_id,
                "message": "连接成功"
            },
            "timestamp": datetime.now().isoformat()
        })
        await send_realtime_limit_up_snapshot(client_id)
        
        # 心跳任务
        async def heartbeat():
            while True:
                try:
                    await asyncio.sleep(30)
                    await websocket.send_json({
                        "type": "ping",
                        "timestamp": datetime.now().isoformat()
                    })
                except:
                    break
        
        # 启动心跳
        heartbeat_task = asyncio.create_task(heartbeat())
        
        try:
            while True:
                # 接收客户端消息
                data = await websocket.receive_text()
                message = json.loads(data)
                
                # 处理客户端消息
                await handle_client_message(client_id, message)
        finally:
            heartbeat_task.cancel()
    
    except WebSocketDisconnect:
        logger.info(f"Client {client_id} disconnected")
    except Exception as e:
        logger.error(f"WebSocket error for {client_id}: {e}")
    finally:
        await manager.disconnect(client_id)
        await stop_realtime_sync_watcher_if_idle()


async def handle_client_message(client_id: str, message: dict):
    """处理客户端消息"""
    msg_type = message.get("type")
    data = message.get("data", {})
    
    if msg_type == "pong":
        # 心跳响应
        pass
    
    elif msg_type == "subscribe_stocks":
        # 订阅股票
        stocks = data.get("stocks", [])
        await manager.subscribe_stock(client_id, stocks)
        logger.debug(f"Client {client_id} subscribed to stocks: {stocks}")
    
    elif msg_type == "unsubscribe_stocks":
        # 取消订阅股票
        stocks = data.get("stocks", [])
        await manager.unsubscribe_stock(client_id, stocks)
    
    elif msg_type == "subscribe_types":
        # 订阅消息类型
        types = data.get("types", [])
        await manager.subscribe_message_type(client_id, types)
    
    elif msg_type == "unsubscribe_types":
        # 取消订阅消息类型
        types = data.get("types", [])
        await manager.unsubscribe_message_type(client_id, types)


async def ensure_realtime_sync_watcher():
    """确保实时涨停同步 watcher 已启动"""
    global realtime_sync_task

    if realtime_sync_task and not realtime_sync_task.done():
        return

    realtime_sync_task = asyncio.create_task(realtime_sync_loop())
    logger.info("Realtime sync watcher started")


async def stop_realtime_sync_watcher_if_idle():
    """没有连接时停止 watcher，避免空转"""
    global realtime_sync_task

    if manager.connection_count > 0:
        return

    if realtime_sync_task and not realtime_sync_task.done():
        realtime_sync_task.cancel()
        try:
            await realtime_sync_task
        except asyncio.CancelledError:
            pass
        logger.info("Realtime sync watcher stopped")
    realtime_sync_task = None


async def send_realtime_limit_up_snapshot(client_id: str):
    """向新连接客户端发送当前实时涨停列表快照"""
    trade_date = date.today()
    snapshot = realtime_stream_tracker.get_cached_snapshot(trade_date)
    if not is_trading_time():
        if not snapshot or not snapshot.get("data", {}).get("items"):
            return

    if snapshot is None:
        realtime_data = await realtime_limit_up_service.get_realtime_limit_up_list(trade_date)
        snapshot = realtime_stream_tracker.sync(realtime_data, trade_date)
        if snapshot is None:
            snapshot = realtime_stream_tracker.get_cached_snapshot(trade_date)

    if snapshot:
        await manager.send_personal_message(
            {
                **snapshot,
                "timestamp": datetime.now().isoformat(),
            },
            client_id,
        )


async def realtime_sync_loop():
    """基于实时涨停池的列表同步与新增涨停播报循环"""
    while True:
        try:
            if manager.connection_count == 0:
                await asyncio.sleep(1)
                continue

            if not is_trading_time():
                await asyncio.sleep(REALTIME_SYNC_IDLE_INTERVAL)
                continue

            trade_date = date.today()
            realtime_data = await realtime_limit_up_service.get_realtime_limit_up_list(trade_date)
            sync_message = realtime_stream_tracker.sync(realtime_data, trade_date)
            if sync_message:
                await broadcast_realtime_sync_message(sync_message)

            alerts = realtime_alert_tracker.collect_new_alerts(realtime_data, trade_date)

            for alert in alerts:
                await manager.broadcast_limit_up_alert(
                    alert["stock_code"],
                    alert["stock_name"],
                    alert["time"],
                    alert.get("reason"),
                    alert.get("continuous_days", 1),
                )

            await asyncio.sleep(REALTIME_SYNC_INTERVAL)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Realtime sync watcher error: {e}")
            await asyncio.sleep(REALTIME_SYNC_INTERVAL)


async def broadcast_realtime_sync_message(message: dict):
    """广播实时涨停列表快照或增量更新"""
    msg_type = message.get("type")
    payload = message.get("data", {})

    if msg_type == "limit_up_snapshot":
        await manager.broadcast_limit_up_snapshot(payload)
        return

    if msg_type == "limit_up_delta":
        await manager.broadcast_limit_up_delta(payload)


def is_trading_time() -> bool:
    """判断是否在交易时间"""
    now = datetime.now()
    current_time = now.time()
    
    morning_start = time_type(9, 30)
    morning_end = time_type(11, 30)
    afternoon_start = time_type(13, 0)
    afternoon_end = time_type(15, 0)
    
    return (morning_start <= current_time <= morning_end) or \
           (afternoon_start <= current_time <= afternoon_end)

async def fetch_continuous_data():
    """获取实时连板梯队数据，统一使用实时涨停池口径。"""
    try:
        realtime_items = await realtime_limit_up_service.get_realtime_limit_up_list(date.today())
        return continuous_ladder_service.build_realtime_ladder(realtime_items, min_days=2)
    except Exception as e:
        logger.error(f"获取连板数据失败: {e}")
        return []


@router.websocket("/ws/continuous")
async def continuous_websocket(
    websocket: WebSocket,
    client_id: Optional[str] = Query(None)
):
    """连板数据实时推送WebSocket"""
    if not client_id:
        client_id = str(uuid.uuid4())[:8]
    
    await websocket.accept()
    logger.info(f"Continuous WS connected: {client_id}")
    
    try:
        # 发送初始数据
        data = await fetch_continuous_data()
        logger.info(f"Continuous WS 初始数据: {len(data)} 个梯队")
        await websocket.send_json({
            "type": "continuous_ladder",
            "data": data,
            "timestamp": datetime.now().isoformat()
        })
        
        # 定时推送数据
        while True:
            await asyncio.sleep(1)  # 1秒推送一次
            
            # 交易时间内每秒推送，非交易时间每30秒推送一次
            if is_trading_time():
                data = await fetch_continuous_data()
                await websocket.send_json({
                    "type": "continuous_ladder",
                    "data": data,
                    "timestamp": datetime.now().isoformat()
                })
            else:
                # 非交易时间，每30秒推送一次
                await asyncio.sleep(29)  # 加上前面的1秒共等30秒
                data = await fetch_continuous_data()
                await websocket.send_json({
                    "type": "continuous_ladder",
                    "data": data,
                    "timestamp": datetime.now().isoformat()
                })
    
    except WebSocketDisconnect:
        logger.info(f"Continuous WS disconnected: {client_id}")
    except Exception as e:
        logger.error(f"Continuous WS error: {e}")
