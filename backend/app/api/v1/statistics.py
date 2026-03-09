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
from app.schemas.statistics import (
    DailyStats, SectorStats, ContinuousLadder, BreakStats, MarketOverview,
    SectorStatsResponse, ContinuousLadderResponse,
    YesterdayContinuousStock, YesterdayContinuousLadder, YesterdayContinuousResponse
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
    从东方财富获取实时连板梯队数据
    - 直接从东方财富 API 获取，数据更准确
    - 包含封板/炸板状态
    """
    if trade_date is None:
        trade_date = date.today()
    
    date_str = trade_date.strftime("%Y%m%d")
    
    try:
        # 从东方财富获取今日涨停池数据
        url = "https://push2ex.eastmoney.com/getTopicZTPool"
        params = {
            "ut": "7eea3edcaed734bea9cbfc24409ed989",
            "dpt": "wz.ztzt",
            "Pageindex": "0",
            "pagesize": "10000",
            "sort": "fbt:asc",
            "date": date_str,
        }
        
        logger.info(f"请求今日涨停池: {url}, date={date_str}")
        
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url, headers={"User-Agent": "Mozilla/5.0"}, params=params)
            data = resp.json()
        
        if not data.get("data") or not data["data"].get("pool"):
            logger.warning("东方财富返回空数据")
            return {
                "trade_date": str(trade_date),
                "is_fallback": False,
                "data": []
            }
        
        pool = data["data"]["pool"]
        logger.info(f"获取到 {len(pool)} 只涨停股")
        
        # 按连板天数分组
        ladder_map = {}  # {连板天数: [股票列表]}
        
        for item in pool:
            # lbc: 连板次数
            continuous_days = item.get("lbc", 1)
            if continuous_days < min_days:
                continue
            
            code = item.get("c", "")
            name = item.get("n", "")
            
            # fbt: 首次封板时间（时间戳）
            fbt = item.get("fbt", 0)
            first_limit_up_time = None
            if fbt:
                try:
                    from datetime import datetime
                    fbt_str = str(fbt)
                    if len(fbt_str) == 6:
                        hour = int(fbt_str[:2])
                        minute = int(fbt_str[2:4])
                        second = int(fbt_str[4:6])
                        first_limit_up_time = f"{hour:02d}:{minute:02d}:{second:02d}"
                except:
                    pass
            
            # zbc: 炸板次数（今天曾经炸板过多少次）
            # 注意：涨停池中的股票都是当前封住的，zbc>0只是表示之前炸板过但现在回封了
            open_count = item.get("zbc", 0)
            is_sealed = True  # 涨停池中的股票都是当前封板状态
            
            # hybk: 行业板块/涨停原因
            reason = item.get("hybk", "")
            
            # zdp: 涨跌幅（已是百分比形式）
            change_pct = item.get("zdp", 0)
            if change_pct:
                change_pct = round(change_pct, 2)
            
            if continuous_days not in ladder_map:
                ladder_map[continuous_days] = []
            
            ladder_map[continuous_days].append({
                "stock_code": code,
                "stock_name": name,
                "first_limit_up_time": first_limit_up_time,
                "reason": reason,
                "is_sealed": is_sealed,
                "open_count": open_count,
                "change_pct": change_pct
            })
        
        # 构建响应数据
        ladder_list = []
        for days in sorted(ladder_map.keys(), reverse=True):
            stocks = ladder_map[days]
            # 按首封时间排序
            stocks.sort(key=lambda x: x.get("first_limit_up_time") or "23:59:59")
            ladder_list.append({
                "continuous_days": days,
                "count": len(stocks),
                "stocks": stocks
            })
        
        return {
            "trade_date": str(trade_date),
            "is_fallback": False,
            "data": ladder_list
        }
        
    except Exception as e:
        logger.error(f"获取实时连板数据失败: {e}")
        return {
            "trade_date": str(trade_date),
            "is_fallback": False,
            "data": []
        }


@router.get("/yesterday-continuous", response_model=YesterdayContinuousResponse, summary="获取昨日连板今日表现")
async def get_yesterday_continuous(
    trade_date: Optional[date] = Query(None, description="今日日期，默认今天"),
    min_days: int = Query(2, description="最小连板天数，默认2"),
    db: AsyncSession = Depends(get_db)
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
        
        # 同时获取今日涨停池数据，用于判断今日是否涨停
        today_zt_url = "https://push2ex.eastmoney.com/getTopicZTPool"
        today_zt_params = {
            "ut": "7eea3edcaed734bea9cbfc24409ed989",
            "dpt": "wz.ztzt",
            "Pageindex": "0",
            "pagesize": "10000",
            "sort": "fbt:asc",
            "date": date_str,
        }
        
        async with httpx.AsyncClient(timeout=10) as client:
            today_resp = await client.get(today_zt_url, headers={"User-Agent": "Mozilla/5.0"}, params=today_zt_params)
            today_data = today_resp.json()
        
        # 构建今日涨停股票集合
        today_zt_stocks = {}  # {code: {is_sealed, open_count}}
        if today_data.get("data") and today_data["data"].get("pool"):
            for item in today_data["data"]["pool"]:
                code = item.get("c", "")
                # zbc: 炸板次数
                open_count = item.get("zbc", 0)
                # 如果炸板次数为0，说明全天未开板（封板）
                today_zt_stocks[code] = {
                    "is_sealed": open_count == 0,
                    "open_count": open_count
                }
        
        # 按连板天数分组
        ladder_map = {}  # {连板天数: [股票列表]}
        
        for item in pool:
            # ylbc: 昨日连板数
            yesterday_days = item.get("ylbc", 1)
            if yesterday_days < min_days:
                continue
            
            code = item.get("c", "")
            name = item.get("n", "")
            # zdp: 涨跌幅（已经是百分比形式，如 -4.13, 1.18）
            change_pct = item.get("zdp", 0)
            if change_pct:
                change_pct = round(change_pct, 2)
            
            # 判断今日状态
            if code in today_zt_stocks:
                zt_info = today_zt_stocks[code]
                if zt_info["is_sealed"]:
                    today_status = "sealed"  # 封板
                else:
                    today_status = "opened"  # 炸板
            else:
                today_status = "broken"  # 断板（今日未涨停）
            
            if yesterday_days not in ladder_map:
                ladder_map[yesterday_days] = []
            
            ladder_map[yesterday_days].append(YesterdayContinuousStock(
                stock_code=code,
                stock_name=name,
                yesterday_days=yesterday_days,
                today_status=today_status,
                today_change_pct=change_pct
            ))
        
        # 构建响应数据
        ladder_list = []
        for days in sorted(ladder_map.keys(), reverse=True):
            stocks = ladder_map[days]
            sealed_count = sum(1 for s in stocks if s.today_status == "sealed")
            opened_count = sum(1 for s in stocks if s.today_status == "opened")
            broken_count = sum(1 for s in stocks if s.today_status == "broken")
            
            ladder_list.append(YesterdayContinuousLadder(
                continuous_days=days,
                count=len(stocks),
                sealed_count=sealed_count,
                opened_count=opened_count,
                broken_count=broken_count,
                stocks=stocks
            ))
        
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
