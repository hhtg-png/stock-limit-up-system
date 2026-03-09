"""
时间处理工具
"""
from datetime import datetime, date, time, timedelta
from typing import Optional
import pytz

# 中国时区
CN_TZ = pytz.timezone('Asia/Shanghai')


def now_cn() -> datetime:
    """获取当前中国时间"""
    return datetime.now(CN_TZ)


def today_cn() -> date:
    """获取今天日期（中国）"""
    return now_cn().date()


def parse_time(time_str: str, fmt: str = "%H:%M:%S") -> time:
    """解析时间字符串"""
    return datetime.strptime(time_str, fmt).time()


def format_time(dt: datetime, fmt: str = "%H:%M:%S") -> str:
    """格式化时间"""
    return dt.strftime(fmt)


def format_datetime(dt: datetime, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """格式化日期时间"""
    return dt.strftime(fmt)


def is_trading_time(dt: Optional[datetime] = None) -> bool:
    """判断是否为交易时间"""
    if dt is None:
        dt = now_cn()
    
    # 周末不交易
    if dt.weekday() >= 5:
        return False
    
    current_time = dt.time()
    
    # 上午交易时间: 9:30 - 11:30
    morning_start = time(9, 30)
    morning_end = time(11, 30)
    
    # 下午交易时间: 13:00 - 15:00
    afternoon_start = time(13, 0)
    afternoon_end = time(15, 0)
    
    return (morning_start <= current_time <= morning_end or 
            afternoon_start <= current_time <= afternoon_end)


def is_call_auction_time(dt: Optional[datetime] = None) -> bool:
    """判断是否为集合竞价时间"""
    if dt is None:
        dt = now_cn()
    
    if dt.weekday() >= 5:
        return False
    
    current_time = dt.time()
    
    # 早盘集合竞价: 9:15 - 9:25
    morning_auction_start = time(9, 15)
    morning_auction_end = time(9, 25)
    
    # 尾盘集合竞价: 14:57 - 15:00
    closing_auction_start = time(14, 57)
    closing_auction_end = time(15, 0)
    
    return (morning_auction_start <= current_time <= morning_auction_end or
            closing_auction_start <= current_time <= closing_auction_end)


def get_trading_dates(start_date: date, end_date: date) -> list:
    """获取交易日列表（简单版，不含节假日）"""
    dates = []
    current = start_date
    while current <= end_date:
        # 跳过周末
        if current.weekday() < 5:
            dates.append(current)
        current += timedelta(days=1)
    return dates


def time_to_seconds(t: time) -> int:
    """时间转换为秒数（从0点开始）"""
    return t.hour * 3600 + t.minute * 60 + t.second


def seconds_to_time(seconds: int) -> time:
    """秒数转换为时间"""
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return time(hours, minutes, secs)


def get_market_status() -> str:
    """获取市场状态"""
    dt = now_cn()
    
    if dt.weekday() >= 5:
        return "closed"  # 周末休市
    
    current_time = dt.time()
    
    if current_time < time(9, 15):
        return "pre_market"  # 盘前
    elif time(9, 15) <= current_time < time(9, 30):
        return "call_auction"  # 集合竞价
    elif time(9, 30) <= current_time < time(11, 30):
        return "trading"  # 上午交易
    elif time(11, 30) <= current_time < time(13, 0):
        return "lunch_break"  # 午休
    elif time(13, 0) <= current_time < time(14, 57):
        return "trading"  # 下午交易
    elif time(14, 57) <= current_time <= time(15, 0):
        return "closing_auction"  # 尾盘集合竞价
    else:
        return "closed"  # 收盘
