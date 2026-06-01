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
from app.services.edge_tts_service import edge_tts_service
from app.services.realtime_limit_up_service import realtime_limit_up_service
from app.services.realtime_limit_up_alert_tracker import RealtimeLimitUpAlertTracker
from app.services.realtime_limit_up_stream_tracker import RealtimeLimitUpStreamTracker
from app.services.tdx_news_realtime_tracker import TdxNewsRealtimeTracker
from app.services.tdx_news_sources import public_market_news_provider
from loguru import logger

router = APIRouter()

# 实时涨停列表/播报 watcher
realtime_alert_tracker = RealtimeLimitUpAlertTracker()
realtime_stream_tracker = RealtimeLimitUpStreamTracker()
tdx_news_realtime_tracker = TdxNewsRealtimeTracker()
realtime_sync_task: Optional[asyncio.Task] = None
tdx_news_sync_task: Optional[asyncio.Task] = None
REALTIME_HOT_SYNC_INTERVAL = 0.25
REALTIME_HOT_POOL_MAX_CACHE_AGE = 0.25
REALTIME_HOT_FETCH_TIMEOUT = 0.8
REALTIME_RICH_SYNC_INTERVAL = 3
REALTIME_RICH_POOL_MAX_CACHE_AGE = 2
REALTIME_SYNC_IDLE_INTERVAL = 30
TDX_NEWS_SYNC_INTERVAL = 5
TDX_NEWS_SYNC_LIMIT = 80
TDX_NEWS_BROADCAST_LIMIT = 10
TDX_NEWS_TTS_WARM_LIMIT = 3
TDX_NEWS_TTS_WARM_TIMEOUT = 3


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
    await ensure_tdx_news_sync_watcher()
    
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
        await stop_tdx_news_sync_watcher_if_idle()


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


async def ensure_tdx_news_sync_watcher():
    """确保聚合快讯 watcher 已启动，提供低延迟 tdx_news_event 推送。"""
    global tdx_news_sync_task

    if tdx_news_sync_task and not tdx_news_sync_task.done():
        return

    tdx_news_realtime_tracker.reset()
    tdx_news_sync_task = asyncio.create_task(tdx_news_sync_loop())
    logger.info("TDX news sync watcher started")


async def stop_tdx_news_sync_watcher_if_idle():
    """没有连接时停止聚合快讯 watcher，避免持续请求外部源。"""
    global tdx_news_sync_task

    if manager.connection_count > 0:
        return

    if tdx_news_sync_task and not tdx_news_sync_task.done():
        tdx_news_sync_task.cancel()
        try:
            await tdx_news_sync_task
        except asyncio.CancelledError:
            pass
        logger.info("TDX news sync watcher stopped")
    tdx_news_sync_task = None


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


async def process_realtime_hot_limit_up_tick(trade_date: date) -> int:
    """Fast path: detect new limit-up records and broadcast speech events."""
    realtime_data = await asyncio.wait_for(
        realtime_limit_up_service.get_fast_limit_up_pool(
            trade_date,
            wait_for_refresh=True,
            max_cache_age=REALTIME_HOT_POOL_MAX_CACHE_AGE,
        ),
        timeout=REALTIME_HOT_FETCH_TIMEOUT,
    )
    alerts = realtime_alert_tracker.collect_new_alerts(realtime_data, trade_date)

    for alert in alerts:
        await manager.broadcast_limit_up_alert(
            alert["stock_code"],
            alert["stock_name"],
            alert["time"],
            alert.get("reason"),
            alert.get("continuous_days", 1),
        )
        await broadcast_tdx_limit_up_event(alert)

    return len(alerts)


async def process_realtime_rich_limit_up_sync(trade_date: date):
    """Slow path: refresh complete list data for table fields and deltas."""
    realtime_data = await realtime_limit_up_service.get_realtime_limit_up_list(
        trade_date,
        wait_for_pool_refresh=False,
        pool_max_cache_age=REALTIME_RICH_POOL_MAX_CACHE_AGE,
    )
    sync_message = realtime_stream_tracker.sync(realtime_data, trade_date)
    if sync_message:
        await broadcast_realtime_sync_message(sync_message)


def _consume_background_task_result(task: asyncio.Task, task_name: str):
    try:
        task.result()
    except asyncio.CancelledError:
        pass
    except Exception as exc:
        logger.error(f"{task_name} error: {exc}")


async def realtime_sync_loop():
    """基于实时涨停池的列表同步与新增涨停播报循环"""
    rich_sync_task: Optional[asyncio.Task] = None
    last_rich_sync_at = 0.0

    while True:
        try:
            if manager.connection_count == 0:
                await asyncio.sleep(1)
                continue

            if not is_trading_time():
                await asyncio.sleep(REALTIME_SYNC_IDLE_INTERVAL)
                continue

            trade_date = date.today()
            await process_realtime_hot_limit_up_tick(trade_date)

            now = asyncio.get_running_loop().time()
            rich_due = now - last_rich_sync_at >= REALTIME_RICH_SYNC_INTERVAL
            rich_done = rich_sync_task is None or rich_sync_task.done()
            if rich_due and rich_done:
                last_rich_sync_at = now
                rich_sync_task = asyncio.create_task(
                    process_realtime_rich_limit_up_sync(trade_date)
                )
                rich_sync_task.add_done_callback(
                    lambda task: _consume_background_task_result(task, "Realtime rich sync")
                )

            await asyncio.sleep(REALTIME_HOT_SYNC_INTERVAL)
        except asyncio.CancelledError:
            if rich_sync_task and not rich_sync_task.done():
                rich_sync_task.cancel()
                try:
                    await rich_sync_task
                except asyncio.CancelledError:
                    pass
            raise
        except asyncio.TimeoutError:
            logger.warning("Realtime hot limit-up tick timed out")
            await asyncio.sleep(REALTIME_HOT_SYNC_INTERVAL)
        except Exception as e:
            logger.error(f"Realtime sync watcher error: {e}")
            await asyncio.sleep(REALTIME_HOT_SYNC_INTERVAL)


async def tdx_news_sync_loop():
    """轮询公开快讯源并用 WebSocket 低延迟广播新增聚合快讯。"""
    while True:
        try:
            if manager.connection_count == 0:
                await asyncio.sleep(1)
                continue

            items, _source_status, _warnings = await public_market_news_provider.get_latest_news(
                limit=TDX_NEWS_SYNC_LIMIT,
                force_refresh=True,
            )
            new_items = tdx_news_realtime_tracker.collect_new_items(items)
            if new_items:
                publish_items = new_items[:TDX_NEWS_BROADCAST_LIMIT]
                await warm_tdx_news_speech_cache(publish_items[:TDX_NEWS_TTS_WARM_LIMIT])
                for item in publish_items:
                    await broadcast_tdx_news_event(item)

            await asyncio.sleep(TDX_NEWS_SYNC_INTERVAL)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"TDX news sync watcher error: {e}")
            await asyncio.sleep(TDX_NEWS_SYNC_INTERVAL)


async def broadcast_realtime_sync_message(message: dict):
    """广播实时涨停列表快照或增量更新"""
    msg_type = message.get("type")
    payload = message.get("data", {})

    if msg_type == "limit_up_snapshot":
        await manager.broadcast_limit_up_snapshot(payload)
        return

    if msg_type == "limit_up_delta":
        await manager.broadcast_limit_up_delta(payload)


async def broadcast_tdx_limit_up_event(alert: dict):
    """广播通达信插件涨停事件。"""
    stock_code = alert.get("stock_code", "")
    stock_name = alert.get("stock_name", "")
    event_time = alert.get("time", "")
    continuous_days = alert.get("continuous_days", 1)
    status_label = tdx_limit_up_status_label(continuous_days)
    event_id = f"tdx-limit-up-{stock_code}-{event_time}"
    await manager.broadcast_tdx_plugin_event(
        "tdx_limit_up_event",
        {
            "event_id": event_id,
            "event_type": "limit_up_sealed",
            "event_label": "封死涨停",
            "event_time": event_time,
            "stock_code": stock_code,
            "stock_name": stock_name,
            "board": continuous_days,
            "reason": alert.get("reason"),
            "target_status_label": status_label,
            "speech_text": f"{stock_name}{status_label}",
        },
        stock_code=stock_code,
    )


def tdx_limit_up_status_label(continuous_days: object) -> str:
    try:
        board = int(continuous_days or 1)
    except (TypeError, ValueError):
        board = 1
    return f"{board}板" if board > 1 else "首板"


def tdx_news_speech_text(item: dict) -> str:
    source = str(item.get("source") or "").strip()
    title = " ".join(str(item.get("title") or "").split()).strip()
    if source == "韭研公社":
        return f"{source}新帖，{title}".strip("，")[:120]
    return title[:120]


async def warm_tdx_news_speech_cache(items: list[dict]):
    """提前生成新增快讯的 TTS 缓存，避免前端收到事件后再等待首次合成。"""
    tasks = []
    for item in items:
        text = tdx_news_speech_text(item)
        if not text:
            continue
        tasks.append(asyncio.wait_for(edge_tts_service.synthesize_to_file(text), timeout=TDX_NEWS_TTS_WARM_TIMEOUT))
    if not tasks:
        return
    await asyncio.gather(*tasks, return_exceptions=True)


async def broadcast_tdx_news_event(item: dict):
    """广播通达信插件聚合快讯事件。"""
    payload = dict(item)
    payload["speech_text"] = tdx_news_speech_text(payload)
    await manager.broadcast_tdx_plugin_event(
        "tdx_news_event",
        payload,
        stock_code=None,
    )


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
