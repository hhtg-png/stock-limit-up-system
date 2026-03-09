"""
交易日期工具函数
"""
from datetime import date
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from app.models.limit_up import LimitUpRecord


async def get_latest_trade_date(db: AsyncSession, target_date: date = None) -> date:
    """
    获取最近有数据的交易日期
    
    Args:
        db: 数据库会话
        target_date: 目标日期，默认为今天
        
    Returns:
        最近有数据的交易日期，如果没有数据则返回目标日期
    """
    if target_date is None:
        target_date = date.today()
    
    # 首先检查目标日期是否有数据
    count_query = (
        select(func.count(LimitUpRecord.id))
        .where(LimitUpRecord.trade_date == target_date)
    )
    result = await db.execute(count_query)
    count = result.scalar()
    
    if count and count > 0:
        return target_date
    
    # 如果目标日期没有数据，查找最近有数据的日期
    latest_query = (
        select(LimitUpRecord.trade_date)
        .where(LimitUpRecord.trade_date <= target_date)
        .order_by(LimitUpRecord.trade_date.desc())
        .limit(1)
    )
    result = await db.execute(latest_query)
    latest_date = result.scalar()
    
    if latest_date:
        return latest_date
    
    # 如果没有任何历史数据，返回目标日期
    return target_date


async def get_trade_date_with_fallback(db: AsyncSession, target_date: date = None) -> tuple[date, bool]:
    """
    获取交易日期，并标记是否发生了回退
    
    Args:
        db: 数据库会话
        target_date: 目标日期，默认为今天
        
    Returns:
        (实际使用的交易日期, 是否回退到历史数据)
    """
    if target_date is None:
        target_date = date.today()
    
    actual_date = await get_latest_trade_date(db, target_date)
    is_fallback = actual_date != target_date
    
    return actual_date, is_fallback
