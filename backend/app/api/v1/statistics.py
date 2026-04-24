"""
统计分析API
"""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_
from typing import Optional, List
from datetime import date, timedelta

from app.database import get_db
from app.models.stock import Stock
from app.models.limit_up import LimitUpRecord
from app.models.market_data import DailyStatistics
from app.services.continuous_ladder_service import continuous_ladder_service
from app.services.realtime_limit_up_service import realtime_limit_up_service
from app.schemas.statistics import (
    DailyStats, SectorStats, ContinuousLadder, BreakStats, MarketOverview,
    SectorStatsResponse, ContinuousLadderResponse,
    YesterdayContinuousLadder, YesterdayContinuousResponse
)
import httpx
from loguru import logger
from app.utils.trade_date import get_trade_date_with_fallback

router = APIRouter()


@router.get("/daily", response_model=List[DailyStats], summary="获取日统计数据")
async def get_daily_statistics(
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    db: AsyncSession = Depends(get_db)
):
    """获取每日统计数据"""
    query = select(DailyStatistics)
    
    if start_date:
        query = query.where(DailyStatistics.trade_date >= start_date)
    if end_date:
        query = query.where(DailyStatistics.trade_date <= end_date)
    
    query = query.order_by(DailyStatistics.trade_date.desc())
    
    result = await db.execute(query)
    stats = result.scalars().all()
    
    return [DailyStats.model_validate(s) for s in stats]


@router.get("/sectors", response_model=SectorStatsResponse, summary="获取板块热度")
async def get_sector_statistics(
    trade_date: Optional[date] = Query(None),
    db: AsyncSession = Depends(get_db)
):
    """获取板块涨停统计，非交易时段自动回退到最近有数据的交易日"""
    if trade_date is None:
        trade_date = date.today()
    
    # 获取实际有数据的交易日期
    actual_date, is_fallback = await get_trade_date_with_fallback(db, trade_date)
    
    # 按行业统计涨停数量
    query = (
        select(
            Stock.industry,
            func.count(LimitUpRecord.id).label('count')
        )
        .join(LimitUpRecord, LimitUpRecord.stock_id == Stock.id)
        .where(LimitUpRecord.trade_date == actual_date)
        .group_by(Stock.industry)
        .order_by(func.count(LimitUpRecord.id).desc())
    )
    
    result = await db.execute(query)
    sector_counts = result.all()
    
    sector_stats = []
    for industry, count in sector_counts:
        if not industry:
            continue
        
        # 获取该行业的涨停股票
        stocks_query = (
            select(Stock.stock_code)
            .join(LimitUpRecord, LimitUpRecord.stock_id == Stock.id)
            .where(and_(
                LimitUpRecord.trade_date == actual_date,
                Stock.industry == industry
            ))
        )
        stocks_result = await db.execute(stocks_query)
        stocks = [s[0] for s in stocks_result.all()]
        
        sector_stats.append(SectorStats(
            sector_name=industry,
            limit_up_count=count,
            stocks=stocks
        ))
    
    return SectorStatsResponse(
        trade_date=actual_date,
        is_fallback=is_fallback,
        data=sector_stats
    )


@router.get("/continuous", response_model=ContinuousLadderResponse, summary="获取连板梯队")
async def get_continuous_ladder(
    trade_date: Optional[date] = Query(None),
    db: AsyncSession = Depends(get_db)
):
    """获取连板梯队统计，非交易时段自动回退到最近有数据的交易日"""
    if trade_date is None:
        trade_date = date.today()
    
    # 获取实际有数据的交易日期
    actual_date, is_fallback = await get_trade_date_with_fallback(db, trade_date)
    
    # 按连板天数统计
    query = (
        select(
            LimitUpRecord.continuous_limit_up_days,
            func.count(LimitUpRecord.id).label('count')
        )
        .where(LimitUpRecord.trade_date == actual_date)
        .group_by(LimitUpRecord.continuous_limit_up_days)
        .order_by(LimitUpRecord.continuous_limit_up_days.desc())
    )
    
    result = await db.execute(query)
    ladder_counts = result.all()
    
    ladder_list = []
    for days, count in ladder_counts:
        # 获取该连板数的股票详情
        stocks_query = (
            select(LimitUpRecord, Stock)
            .join(Stock, LimitUpRecord.stock_id == Stock.id)
            .where(and_(
                LimitUpRecord.trade_date == actual_date,
                LimitUpRecord.continuous_limit_up_days == days
            ))
            .order_by(LimitUpRecord.first_limit_up_time)
        )
        stocks_result = await db.execute(stocks_query)
        stocks = stocks_result.all()
        
        ladder_list.append(ContinuousLadder(
            continuous_days=days,
            count=count,
            stocks=[
                {
                    "stock_code": stock.stock_code,
                    "stock_name": stock.stock_name,
                    "first_limit_up_time": record.first_limit_up_time.strftime("%H:%M:%S") if record.first_limit_up_time else None,
                    "reason": record.limit_up_reason
                }
                for record, stock in stocks
            ]
        ))
    
    return ContinuousLadderResponse(
        trade_date=actual_date,
        is_fallback=is_fallback,
        data=ladder_list
    )


@router.get("/continuous-realtime", summary="获取实时连板梯队（东方财富）")
async def get_continuous_realtime(
    trade_date: Optional[date] = Query(None, description="交易日期，默认今天"),
    min_days: int = Query(2, description="最小连板天数，默认2")
):
    """
    从实时涨停池获取连板梯队数据
    - 统一使用实时涨停池口径
    - 同时包含当前封板和炸板状态
    """
    if trade_date is None:
        trade_date = date.today()

    try:
        realtime_items = await realtime_limit_up_service.get_realtime_limit_up_list(trade_date)
        ladder_list = continuous_ladder_service.build_realtime_ladder(
            realtime_items,
            min_days=min_days,
        )
        return {
            "trade_date": str(trade_date),
            "is_fallback": False,
            "data": ladder_list,
        }
    except Exception as e:
        logger.error(f"获取实时连板数据失败: {e}")
        return {
            "trade_date": str(trade_date),
            "is_fallback": False,
            "data": [],
        }


@router.get("/yesterday-continuous", response_model=YesterdayContinuousResponse, summary="获取昨日连板今日表现")
async def get_yesterday_continuous(
    trade_date: Optional[date] = Query(None, description="今日日期，默认今天"),
    min_days: int = Query(2, description="最小连板天数，默认2"),
):
    """
    获取昨日连板股票的今日表现（从东方财富实时获取）
    - 筛选昨日连板天数 >= min_days 的股票
    - 返回今日状态（封板/炸板/断板）和涨跌幅
    """
    from datetime import datetime
    
    if trade_date is None:
        trade_date = date.today()
    
    # 转换日期格式为 YYYYMMDD
    date_str = trade_date.strftime("%Y%m%d")
    
    try:
        # 从东方财富获取昨日涨停股池数据
        url = "https://push2ex.eastmoney.com/getYesterdayZTPool"
        params = {
            "ut": "7eea3edcaed734bea9cbfc24409ed989",
            "dpt": "wz.ztzt",
            "Pageindex": "0",
            "pagesize": "5000",
            "sort": "zs:desc",
            "date": date_str,
        }
        
        logger.info(f"请求昨日连板数据: {url}, date={date_str}")
        
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, headers={"User-Agent": "Mozilla/5.0"}, params=params)
            data = resp.json()
        
        logger.info(f"东方财富返回: rc={data.get('rc')}, pool数量={len(data.get('data', {}).get('pool', []))}")
        
        if not data.get("data") or not data["data"].get("pool"):
            logger.warning("东方财富返回空数据")
            return YesterdayContinuousResponse(
                trade_date=trade_date,
                yesterday_date=trade_date,
                is_fallback=False,
                data=[]
            )
        
        pool = data["data"]["pool"]
        realtime_items = await realtime_limit_up_service.get_realtime_limit_up_list(trade_date)
        ladder_list = [
            YesterdayContinuousLadder(**ladder)
            for ladder in continuous_ladder_service.build_yesterday_ladder(
                pool,
                realtime_items,
                min_days=min_days,
            )
        ]
        
        # 计算昨日日期（简单减一天，实际应该是上一交易日）
        yesterday_date = trade_date - timedelta(days=1)
        
        return YesterdayContinuousResponse(
            trade_date=trade_date,
            yesterday_date=yesterday_date,
            is_fallback=False,
            data=ladder_list
        )
        
    except Exception as e:
        logger.error(f"获取昨日连板数据失败: {e}")
        return YesterdayContinuousResponse(
            trade_date=trade_date,
            yesterday_date=trade_date,
            is_fallback=False,
            data=[]
        )


async def _fetch_today_change_pct(stock_code: str, market: str, pre_close: Optional[float]) -> Optional[float]:
    """从东方财富获取今日涨跌幅"""
    if not pre_close:
        return None
    
    try:
        prefix = "0" if market == "SZ" else "1"
        url = "https://push2.eastmoney.com/api/qt/stock/get"
        params = {
            "secid": f"{prefix}.{stock_code}",
            "fields": "f43,f60",  # f43=当前价, f60=昨收价
            "ut": "fa5fd1943c7b386f172d6893dbbd1",
            "fltt": "2",
        }
        
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(url, headers={"User-Agent": "Mozilla/5.0"}, params=params)
            data = resp.json()
        
        if data.get("data"):
            current_price = data["data"].get("f43")
            if current_price:
                return round((current_price - pre_close) / pre_close * 100, 2)
    except Exception as e:
        logger.warning(f"获取{stock_code}今日涨跌幅失败: {e}")
    
    return None


@router.get("/breaks", response_model=List[BreakStats], summary="获取炸板统计")
async def get_break_statistics(
    trade_date: Optional[date] = Query(None),
    db: AsyncSession = Depends(get_db)
):
    """获取炸板股票统计"""
    if trade_date is None:
        trade_date = date.today()
    
    # 获取实际有数据的交易日期
    actual_date, _ = await get_trade_date_with_fallback(db, trade_date)
    
    # 查询有开板记录的股票
    query = (
        select(LimitUpRecord, Stock)
        .join(Stock, LimitUpRecord.stock_id == Stock.id)
        .where(and_(
            LimitUpRecord.trade_date == actual_date,
            LimitUpRecord.open_count > 0
        ))
        .order_by(LimitUpRecord.open_count.desc())
    )
    
    result = await db.execute(query)
    records = result.all()
    
    break_stats = []
    for record, stock in records:
        break_stats.append(BreakStats(
            stock_code=stock.stock_code,
            stock_name=stock.stock_name,
            break_time="",  # 需要从status_changes获取
            is_resealed=record.is_final_sealed,
            reseal_time=None,
            final_status="封板" if record.is_final_sealed else "炸板"
        ))
    
    return break_stats


@router.get("/overview", response_model=MarketOverview, summary="获取市场概览")
async def get_market_overview(
    trade_date: Optional[date] = Query(None),
    db: AsyncSession = Depends(get_db)
):
    """获取市场整体概览，非交易时段自动回退到最近有数据的交易日"""
    if trade_date is None:
        trade_date = date.today()
    
    # 获取实际有数据的交易日期
    actual_date, is_fallback = await get_trade_date_with_fallback(db, trade_date)
    
    # 查询每日统计
    query = select(DailyStatistics).where(DailyStatistics.trade_date == actual_date)
    result = await db.execute(query)
    stats = result.scalar_one_or_none()
    
    if stats:
        return MarketOverview(
            trade_date=actual_date,
            is_fallback=is_fallback,
            total_stocks=stats.total_stocks or 0,
            up_count=stats.up_count or 0,
            down_count=stats.down_count or 0,
            flat_count=0,
            limit_up_count=stats.total_limit_up,
            limit_down_count=stats.limit_down_count,
            up_ratio=round(stats.up_count / stats.total_stocks * 100, 2) if stats.total_stocks else 0,
            total_amount=0
        )
    
    # 没有统计数据时返回空数据
    return MarketOverview(
        trade_date=actual_date,
        is_fallback=is_fallback,
        total_stocks=0,
        up_count=0,
        down_count=0,
        flat_count=0,
        limit_up_count=0,
        limit_down_count=0,
        up_ratio=0,
        total_amount=0
    )
