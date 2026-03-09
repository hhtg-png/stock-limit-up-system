"""
WebSocket路由
"""
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query
from typing import Optional, Dict, List
import json
import asyncio
from datetime import datetime, date, time as time_type
import uuid
import httpx

from app.core.websocket_manager import manager
from app.data_collectors.tencent_api import tencent_api
from loguru import logger

router = APIRouter()

# 连板股票缓存（从东方财富获取，包含连板天数信息）
continuous_stocks_cache: Dict[str, int] = {}  # code -> 连板天数
cache_update_time: datetime = datetime.min

# 自由流通市值缓存（用于计算真实换手率）
free_float_cache: Dict[str, float] = {}  # code -> 自由流通市值
free_float_cache_date: date = date.min


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


async def fetch_free_float(stock_code: str, price: float = 0) -> Optional[float]:
    """获取单只股票的自由流通市值
    
    通过F10股东数据计算：自由流通市值 = (流通股 - 大股东持股) × 当前价
    真实换手率 = 成交额 / 自由流通市值 × 100%
    
    Args:
        stock_code: 股票代码
        price: 当前价格（如果不传则从腾讯获取）
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://emweb.securities.eastmoney.com/",
    }
    
    try:
        # 构造股票代码格式 (SZ/SH)
        if stock_code.startswith("6"):
            code_fmt = f"SH{stock_code}"
        else:
            code_fmt = f"SZ{stock_code}"
        
        async with httpx.AsyncClient(timeout=10, headers=headers) as client:
            # 1. 获取当前价格（如果未传入）
            if price <= 0:
                prefix = "sz" if stock_code.startswith(("0", "3")) else "sh"
                qq_url = f"https://qt.gtimg.cn/q={prefix}{stock_code}"
                qq_resp = await client.get(qq_url)
                qq_text = qq_resp.text
                if '"' in qq_text:
                    fields = qq_text.split('"')[1].split('~')
                    if len(fields) > 3 and fields[3]:
                        price = float(fields[3])
            
            if price <= 0:
                return None
            
            # 2. 获取F10股东数据
            f10_url = "https://emweb.securities.eastmoney.com/PC_HSF10/ShareholderResearch/PageAjax"
            f10_resp = await client.get(f10_url, params={"code": code_fmt})
            f10_data = f10_resp.json()
            
            # 获取十大流通股东数据
            sdltgd = f10_data.get('sdltgd', [])
            if not sdltgd:
                return None
            
            # 计算流通股本（从第一大流通股东的比例反推）
            top1 = sdltgd[0]
            top1_hold = top1.get('HOLD_NUM', 0)  # 股数
            top1_ratio = top1.get('FREE_HOLDNUM_RATIO', 0)  # 百分比，如 25.10
            
            if not top1_hold or not top1_ratio:
                return None
            
            # 如果ratio是字符串需要转换
            if isinstance(top1_ratio, str):
                top1_ratio = float(top1_ratio)
            
            # 流通股本 = 第一大股东持股 / 占比
            circulation_shares = top1_hold / (top1_ratio / 100)
            
            # 计算大股东持股总量（只统计占比>=5%的核心大股东）
            major_holder_shares = 0
            for gd in sdltgd:
                ratio = gd.get('FREE_HOLDNUM_RATIO', 0)
                if isinstance(ratio, str):
                    ratio = float(ratio) if ratio else 0
                if ratio >= 5.0:  # 占比>=5%视为核心大股东
                    major_holder_shares += gd.get('HOLD_NUM', 0)
            
            # 自由流通股 = 流通股 - 大股东持股
            free_float_shares = circulation_shares - major_holder_shares
            
            # 自由流通市值 = 自由流通股 × 当前价
            free_float_mv = free_float_shares * price  # 单位：元
            
            if free_float_mv > 0:
                logger.debug(f"{stock_code} 自由流通市值: {free_float_mv/100000000:.2f}亿元")
                return free_float_mv
    
    except Exception as e:
        logger.debug(f"获取{stock_code}自由流通市值失败: {e}")
    
    return None


async def enrich_free_float(stock_codes: List[str], prices: Dict[str, float] = None):
    """批量获取自由流通市值
    
    Args:
        stock_codes: 股票代码列表
        prices: 股票价格字典 {code: price}，可选
    """
    global free_float_cache, free_float_cache_date
    
    today = date.today()
    prices = prices or {}
    
    # 缓存日期不是今天则清空
    if free_float_cache_date != today:
        free_float_cache.clear()
        free_float_cache_date = today
    
    # 找出需要获取的股票
    codes_to_fetch = [c for c in stock_codes if c not in free_float_cache]
    
    if codes_to_fetch:
        # 并行获取（限制并发数5，避免请求过快）
        semaphore = asyncio.Semaphore(5)
        success_count = 0
        
        async def fetch_with_limit(code: str):
            nonlocal success_count
            async with semaphore:
                price = prices.get(code, 0)
                val = await fetch_free_float(code, price)
                if val:
                    free_float_cache[code] = val
                    success_count += 1
        
        await asyncio.gather(*[fetch_with_limit(c) for c in codes_to_fetch])
        logger.info(f"自由流通市值获取: {success_count}/{len(codes_to_fetch)} 只")


async def fetch_continuous_data():
    """获取连板数据（混合数据源：东方财富+腾讯）"""
    global continuous_stocks_cache, cache_update_time
    
    today = date.today()
    date_str = today.strftime("%Y%m%d")
    now = datetime.now()
    
    try:
        # 每10秒从东方财富更新连板天数信息
        if (now - cache_update_time).total_seconds() > 10:
            logger.debug(f"尝试从东方财富获取连板数据, 日期: {date_str}")
            url = "https://push2ex.eastmoney.com/getTopicZTPool"
            params = {
                "ut": "7eea3edcaed734bea9cbfc24409ed989",
                "dpt": "wz.ztzt",
                "Pageindex": "0",
                "pagesize": "10000",
                "sort": "fbt:asc",
                "date": date_str,
            }
            
            async with httpx.AsyncClient(timeout=5) as client:
                resp = await client.get(url, headers={"User-Agent": "Mozilla/5.0"}, params=params)
                data = resp.json()
            
            logger.debug(f"东方财富返回: {data.get('data', {}).get('pool', [])[:2] if data.get('data') else 'None'}")
            
            if data.get("data") and data["data"].get("pool"):
                pool = data["data"]["pool"]
                continuous_stocks_cache.clear()
                for item in pool:
                    code = item.get("c", "")
                    continuous_days = item.get("lbc", 1)
                    if code and continuous_days >= 2:
                        continuous_stocks_cache[code] = continuous_days
                cache_update_time = now
                logger.info(f"更新连板缓存: {len(continuous_stocks_cache)}只")
            else:
                logger.warning(f"东方财富返回空数据, 保持现有缓存: {len(continuous_stocks_cache)}只")
                # 即使没有新数据，也更新时间避免频繁请求
                cache_update_time = now
        
        # 使用腾讯API获取这些股票的实时行情
        if not continuous_stocks_cache:
            logger.debug("连板缓存为空")
            return []
        
        stock_codes = list(continuous_stocks_cache.keys())
        quotes = await tencent_api.get_quotes_batch(stock_codes)
        
        # 构建价格字典
        prices = {code: q.get("price", 0) for code, q in quotes.items() if q.get("price", 0) > 0}
        
        # 获取自由流通市值（用于计算真实换手率）
        await enrich_free_float(stock_codes, prices)
        
        # 构建连板梯队数据
        ladder_map = {}
        
        for code, days in continuous_stocks_cache.items():
            quote = quotes.get(code)
            if not quote:
                continue
            
            name = quote.get("name", "")
            price = quote.get("price", 0)
            change_pct = round(quote.get("change_pct", 0), 2)
            limit_up_price = quote.get("limit_up", 0)
            amount = quote.get("amount", 0)  # 成交额(万)
            turnover_rate = quote.get("turnover_rate", 0)  # 普通换手率
            
            # 计算真实换手率 = 成交额 / 自由流通市值 × 100%
            real_turnover_rate = turnover_rate  # 默认用普通换手率
            free_float = free_float_cache.get(code)
            if free_float and free_float > 0 and amount > 0:
                # amount是万元，free_float是元
                real_turnover_rate = round((amount * 10000 / free_float) * 100, 2)
            
            # 判断是否涨停和封板状态
            is_limit_up = price >= limit_up_price - 0.001 if limit_up_price > 0 else False
            bid1_volume = quote.get("bid1_volume", 0)
            is_sealed = is_limit_up and bid1_volume > 0
            
            # 只显示当前涨停的股票
            if not is_limit_up:
                continue
            
            if days not in ladder_map:
                ladder_map[days] = []
            
            ladder_map[days].append({
                "stock_code": code,
                "stock_name": name,
                "first_limit_up_time": None,  # 腾讯API没有这个字段
                "is_sealed": is_sealed,
                "open_count": 0 if is_sealed else 1,  # 当前封板状态
                "change_pct": change_pct,
                "bid1_volume": bid1_volume,  # 封单量
                "turnover_rate": turnover_rate,  # 普通换手率
                "real_turnover_rate": real_turnover_rate,  # 真实换手率
            })
        
        # 构建返回数据
        ladder_list = []
        for days in sorted(ladder_map.keys(), reverse=True):
            stocks = ladder_map[days]
            # 按封单量排序
            stocks.sort(key=lambda x: x.get("bid1_volume", 0), reverse=True)
            ladder_list.append({
                "continuous_days": days,
                "count": len(stocks),
                "stocks": stocks
            })
        
        return ladder_list
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
