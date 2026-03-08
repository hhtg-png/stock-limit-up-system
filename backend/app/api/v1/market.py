"""
行情数据API
"""
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_
from typing import Optional, List
from datetime import date, datetime

import httpx
from loguru import logger

from app.database import get_db
from app.models.stock import Stock
from app.models.big_order import BigOrder
from app.models.order_flow import OrderBookSnapshot
from pydantic import BaseModel, Field

router = APIRouter()


class OrderBookResponse(BaseModel):
    """盘口数据响应"""
    stock_code: str
    snapshot_time: datetime
    current_price: Optional[float]
    pre_close: Optional[float]
    bid_prices: List[float] = Field(default_factory=list)
    bid_volumes: List[int] = Field(default_factory=list)
    ask_prices: List[float] = Field(default_factory=list)
    ask_volumes: List[int] = Field(default_factory=list)
    volume: Optional[int]
    amount: Optional[float]


class BigOrderResponse(BaseModel):
    """大单记录响应"""
    id: int
    stock_code: str
    trade_time: datetime
    trade_price: float
    trade_volume: int
    trade_amount: float
    direction: str
    order_type: str
    is_limit_up_price: bool


class FundFlowResponse(BaseModel):
    """资金流向响应"""
    stock_code: str
    trade_date: date
    main_in: float = Field(0, description="主力流入")
    main_out: float = Field(0, description="主力流出")
    main_net: float = Field(0, description="主力净流入")
    retail_in: float = Field(0, description="散户流入")
    retail_out: float = Field(0, description="散户流出")
    retail_net: float = Field(0, description="散户净流入")


@router.get("/{stock_code}/orderbook", response_model=OrderBookResponse, summary="获取五档盘口")
async def get_order_book(
    stock_code: str,
    db: AsyncSession = Depends(get_db)
):
    """获取实时五档盘口数据，优先本地数据库，无数据时从东方财富获取"""
    # 查询股票
    stock_query = select(Stock).where(Stock.stock_code == stock_code)
    stock_result = await db.execute(stock_query)
    stock = stock_result.scalar_one_or_none()
    
    if not stock:
        raise HTTPException(status_code=404, detail="股票不存在")
    
    # 查询最新盘口快照
    snapshot_query = (
        select(OrderBookSnapshot)
        .where(OrderBookSnapshot.stock_id == stock.id)
        .order_by(OrderBookSnapshot.snapshot_time.desc())
        .limit(1)
    )
    snapshot_result = await db.execute(snapshot_query)
    snapshot = snapshot_result.scalar_one_or_none()
    
    if snapshot:
        return OrderBookResponse(
            stock_code=stock_code,
            snapshot_time=snapshot.snapshot_time,
            current_price=snapshot.current_price,
            pre_close=snapshot.pre_close,
            bid_prices=snapshot.bid_prices or [],
            bid_volumes=snapshot.bid_volumes or [],
            ask_prices=snapshot.ask_prices or [],
            ask_volumes=snapshot.ask_volumes or [],
            volume=snapshot.volume,
            amount=snapshot.amount
        )
    
    # 本地无数据，从东方财富获取实时盘口
    return await _fetch_orderbook_from_em(stock_code, stock.market)


async def _fetch_orderbook_from_em(stock_code: str, market: str):
    """从东方财富获取实时五档盘口"""
    prefix = "0" if market == "SZ" else "1"
    url = "https://push2.eastmoney.com/api/qt/stock/get"
    params = {
        "secid": f"{prefix}.{stock_code}",
        "fields": "f43,f44,f45,f46,f47,f48,f60,f11,f12,f13,f14,f15,f16,f17,f18,f19,f20,f31,f32,f33,f34,f35,f36,f37,f38,f39,f40",
        "ut": "fa5fd1943c7b386f172d6893dbbd1",
        "fltt": "2",
    }
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, headers={"User-Agent": "Mozilla/5.0"}, params=params)
            data = resp.json()
        
        if not data.get("data"):
            raise HTTPException(status_code=404, detail="暂无盘口数据")
        
        d = data["data"]
        bid_prices = [d.get(f"f{i}", 0) or 0 for i in [11, 13, 15, 17, 19]]
        bid_volumes = [d.get(f"f{i}", 0) or 0 for i in [12, 14, 16, 18, 20]]
        ask_prices = [d.get(f"f{i}", 0) or 0 for i in [31, 33, 35, 37, 39]]
        ask_volumes = [d.get(f"f{i}", 0) or 0 for i in [32, 34, 36, 38, 40]]
        
        return OrderBookResponse(
            stock_code=stock_code,
            snapshot_time=datetime.now(),
            current_price=d.get("f43"),
            pre_close=d.get("f60"),
            bid_prices=bid_prices,
            bid_volumes=bid_volumes,
            ask_prices=ask_prices,
            ask_volumes=ask_volumes,
            volume=d.get("f47"),
            amount=d.get("f48"),
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.warning(f"从东方财富获取{stock_code}盘口失败: {e}")
        raise HTTPException(status_code=404, detail="暂无盘口数据")


@router.get("/{stock_code}/big-orders", response_model=List[BigOrderResponse], summary="获取大单记录")
async def get_big_orders(
    stock_code: str,
    start_time: Optional[datetime] = Query(None),
    end_time: Optional[datetime] = Query(None),
    min_amount: Optional[float] = Query(None, description="最小金额(元)"),
    direction: Optional[str] = Query(None, description="方向(buy/sell)"),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=500),
    db: AsyncSession = Depends(get_db)
):
    """获取大单记录，优先本地数据库，无数据时从东方财富逐笔数据筛选"""
    # 查询股票
    stock_query = select(Stock).where(Stock.stock_code == stock_code)
    stock_result = await db.execute(stock_query)
    stock = stock_result.scalar_one_or_none()
    
    if not stock:
        raise HTTPException(status_code=404, detail="股票不存在")
    
    # 构建查询
    query = select(BigOrder).where(BigOrder.stock_id == stock.id)
    
    if start_time:
        query = query.where(BigOrder.trade_time >= start_time)
    if end_time:
        query = query.where(BigOrder.trade_time <= end_time)
    if min_amount:
        query = query.where(BigOrder.trade_amount >= min_amount)
    if direction:
        query = query.where(BigOrder.direction == direction)
    
    query = query.order_by(BigOrder.trade_time.desc())
    query = query.offset((page - 1) * page_size).limit(page_size)
    
    result = await db.execute(query)
    orders = result.scalars().all()
    
    if orders:
        return [
            BigOrderResponse(
                id=order.id,
                stock_code=stock_code,
                trade_time=order.trade_time,
                trade_price=order.trade_price,
                trade_volume=order.trade_volume,
                trade_amount=order.trade_amount,
                direction=order.direction,
                order_type=order.order_type,
                is_limit_up_price=bool(order.is_limit_up_price)
            )
            for order in orders
        ]
    
    # 本地无数据，从东方财富逐笔数据中筛选大单
    return await _fetch_big_orders_from_em(stock_code, stock.market, min_amount, page_size)


async def _fetch_big_orders_from_em(stock_code: str, market: str, min_amount: Optional[float], limit: int):
    """从东方财富逐笔成交中筛选大单（volume >= 500手 或 金额 >= 50万）"""
    prefix = "0" if market == "SZ" else "1"
    url = "https://push2.eastmoney.com/api/qt/stock/details/get"
    
    # 获取最近的逐笔数据
    all_big_orders = []
    for pos_start in range(0, -2000, -500):
        params = {
            "secid": f"{prefix}.{stock_code}",
            "fields1": "f1,f2,f3,f4",
            "fields2": "f51,f52,f53,f54,f55",
            "pos": str(pos_start),
            "ut": "fa5fd1943c7b386f172d6893dbbd1",
            "fltt": "2",
        }
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(url, headers={"User-Agent": "Mozilla/5.0"}, params=params)
                data = resp.json()
            
            if not data.get("data") or not data["data"].get("details"):
                break
            
            details = data["data"]["details"]
            today_str = date.today().isoformat()
            
            vol_threshold = 500  # 500手以上为大单
            amt_threshold = min_amount or 500000  # 默认50万
            
            for i, d in enumerate(details):
                parts = d.split(",")
                if len(parts) < 5:
                    continue
                # 格式: 时间,价格,成交量(手),成交笔数,方向(1=买/2=卖/4=中性)
                time_str = parts[0]
                price = float(parts[1])
                volume = int(parts[2])  # 手
                direction_code = int(parts[4])
                amount = price * volume * 100  # 手 -> 股 -> 金额
                
                if volume >= vol_threshold or amount >= amt_threshold:
                    dir_str = "buy" if direction_code == 1 else "sell"
                    order_type = "active_buy" if direction_code == 1 else "active_sell"
                    
                    all_big_orders.append(BigOrderResponse(
                        id=abs(pos_start) + i + 1,
                        stock_code=stock_code,
                        trade_time=datetime.fromisoformat(f"{today_str}T{time_str}"),
                        trade_price=price,
                        trade_volume=volume,
                        trade_amount=amount,
                        direction=dir_str,
                        order_type=order_type,
                        is_limit_up_price=False,
                    ))
            
            if len(all_big_orders) >= limit:
                break
                
        except Exception as e:
            logger.warning(f"从东方财富获取{stock_code}逐笔数据失败: {e}")
            break
    
    # 按时间倒序排列
    all_big_orders.sort(key=lambda x: x.trade_time, reverse=True)
    return all_big_orders[:limit]


@router.get("/{stock_code}/fund-flow", response_model=FundFlowResponse, summary="获取资金流向")
async def get_fund_flow(
    stock_code: str,
    trade_date: Optional[date] = Query(None),
    db: AsyncSession = Depends(get_db)
):
    """获取资金流向数据"""
    if trade_date is None:
        trade_date = date.today()
    
    # 查询股票
    stock_query = select(Stock).where(Stock.stock_code == stock_code)
    stock_result = await db.execute(stock_query)
    stock = stock_result.scalar_one_or_none()
    
    if not stock:
        raise HTTPException(status_code=404, detail="股票不存在")
    
    # 统计大单买入卖出（简化版，实际需要更复杂的计算）
    # 这里只统计当日大单，实际应该区分主力和散户
    
    # 查询当日大单
    orders_query = (
        select(BigOrder)
        .where(and_(
            BigOrder.stock_id == stock.id,
            BigOrder.trade_time >= datetime.combine(trade_date, datetime.min.time()),
            BigOrder.trade_time <= datetime.combine(trade_date, datetime.max.time())
        ))
    )
    result = await db.execute(orders_query)
    orders = result.scalars().all()
    
    # 统计资金流向
    main_in = sum(o.trade_amount for o in orders if o.direction == "buy")
    main_out = sum(o.trade_amount for o in orders if o.direction == "sell")
    
    return FundFlowResponse(
        stock_code=stock_code,
        trade_date=trade_date,
        main_in=main_in,
        main_out=main_out,
        main_net=main_in - main_out,
        retail_in=0,
        retail_out=0,
        retail_net=0
    )


@router.get("/{stock_code}/timeline", summary="获取分时数据")
async def get_timeline_data(
    stock_code: str,
    trade_date: Optional[date] = Query(None),
    db: AsyncSession = Depends(get_db)
):
    """获取分时数据，优先从本地数据库，无数据时从东方财富API实时获取"""
    if trade_date is None:
        trade_date = date.today()
    
    # 查询股票
    stock_query = select(Stock).where(Stock.stock_code == stock_code)
    stock_result = await db.execute(stock_query)
    stock = stock_result.scalar_one_or_none()
    
    if not stock:
        raise HTTPException(status_code=404, detail="股票不存在")
    
    # 先查本地数据库（L2盘口快照）
    snapshots_query = (
        select(OrderBookSnapshot)
        .where(and_(
            OrderBookSnapshot.stock_id == stock.id,
            OrderBookSnapshot.snapshot_time >= datetime.combine(trade_date, datetime.min.time()),
            OrderBookSnapshot.snapshot_time <= datetime.combine(trade_date, datetime.max.time())
        ))
        .order_by(OrderBookSnapshot.snapshot_time)
    )
    result = await db.execute(snapshots_query)
    snapshots = result.scalars().all()
    
    if snapshots:
        return {
            "stock_code": stock_code,
            "trade_date": trade_date.isoformat(),
            "data": [
                {
                    "time": s.snapshot_time.strftime("%H:%M:%S"),
                    "price": s.current_price,
                    "volume": s.volume,
                    "amount": s.amount
                }
                for s in snapshots
            ]
        }
    
    # 本地无数据，从东方财富API获取分时数据
    return await _fetch_timeline_from_em(stock_code, stock.market, trade_date)


async def _fetch_timeline_from_em(stock_code: str, market: str, trade_date: date):
    """从东方财富获取分时数据"""
    prefix = "0" if market == "SZ" else "1"
    url = "https://push2.eastmoney.com/api/qt/stock/trends2/get"
    params = {
        "secid": f"{prefix}.{stock_code}",
        "fields1": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
        "ut": "fa5fd1943c7b386f172d6893dbbd1",
        "iscr": "0",
        "ndays": "1",
    }
    
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, headers={"User-Agent": "Mozilla/5.0"}, params=params)
            result = resp.json()
        
        if not result.get("data") or not result["data"].get("trends"):
            return {"stock_code": stock_code, "trade_date": trade_date.isoformat(), "data": []}
        
        trends = result["data"]["trends"]
        data = []
        for t in trends:
            parts = t.split(",")
            if len(parts) >= 8:
                # 格式: 时间,开,收,高,低,成交量(手),成交额,均价
                time_str = parts[0]
                if " " in time_str:
                    time_str = time_str.split(" ")[1]  # 只取时间部分
                data.append({
                    "time": time_str,
                    "price": float(parts[2]),        # 收盘价/现价
                    "volume": int(parts[5]),          # 成交量(手)
                    "amount": float(parts[6]),        # 成交额
                    "avg_price": float(parts[7]),     # 均价
                })
        
        return {
            "stock_code": stock_code,
            "trade_date": trade_date.isoformat(),
            "pre_close": result["data"].get("preClose"),
            "data": data
        }
    except Exception as e:
        logger.warning(f"从东方财富获取{stock_code}分时数据失败: {e}")
        return {"stock_code": stock_code, "trade_date": trade_date.isoformat(), "data": []}


@router.get("/tdx/status", summary="获取通达信连接状态")
async def get_tdx_status():
    """获取通达信L2数据连接状态"""
    from app.data_collectors.tdx_collector import tdx_collector
    
    status = tdx_collector.get_connection_status()
    return {
        "code": 0,
        "message": "success",
        "data": status
    }


@router.post("/tdx/reconnect", summary="重连通达信服务器")
async def reconnect_tdx():
    """手动触发重连通达信服务器"""
    from app.data_collectors.tdx_collector import tdx_collector
    
    # 先断开
    await tdx_collector.disconnect()
    
    # 重新连接
    success = await tdx_collector.connect()
    
    return {
        "code": 0 if success else -1,
        "message": "连接成功" if success else "连接失败",
        "data": tdx_collector.get_connection_status()
    }


@router.get("/tdx/l2/status", summary="获取L2本地采集状态")
async def get_l2_status():
    """获取本地通达信L2数据采集状态"""
    from app.data_collectors.tdx_l2_local import tdx_l2_local
    
    return {
        "code": 0,
        "message": "success",
        "data": tdx_l2_local.get_status()
    }


@router.post("/tdx/l2/connect", summary="连接本地通达信L2")
async def connect_l2():
    """连接本地通达信客户端获取L2数据"""
    from app.data_collectors.tdx_l2_local import tdx_l2_local
    
    success = await tdx_l2_local.connect()
    
    return {
        "code": 0 if success else -1,
        "message": "连接成功" if success else "连接失败，请确保通达信客户端已启动",
        "data": tdx_l2_local.get_status()
    }
