"""
统计相关数据模型
"""
from pydantic import BaseModel, Field
from typing import Optional, List, Dict
from datetime import date


class DailyStats(BaseModel):
    """每日统计数据"""
    trade_date: date = Field(..., description="交易日期")
    total_limit_up: int = Field(0, description="涨停总数")
    new_limit_up: int = Field(0, description="首板数量")
    continuous_2: int = Field(0, description="2连板数量")
    continuous_3: int = Field(0, description="3连板数量")
    continuous_4_plus: int = Field(0, description="4连板及以上")
    break_count: int = Field(0, description="炸板数量")
    break_rate: float = Field(0, description="炸板率(%)")
    average_seal_time: Optional[str] = Field(None, description="平均封板时间")
    strongest_sector: Optional[str] = Field(None, description="最强板块")
    limit_down_count: int = Field(0, description="跌停数量")
    
    class Config:
        from_attributes = True


class SectorStats(BaseModel):
    """板块统计"""
    sector_name: str = Field(..., description="板块名称")
    limit_up_count: int = Field(0, description="涨停数量")
    stocks: List[str] = Field(default_factory=list, description="涨停股票列表")
    average_gain: Optional[float] = Field(None, description="平均涨幅")


class ContinuousLadder(BaseModel):
    """连板梯队"""
    continuous_days: int = Field(..., description="连板天数")
    count: int = Field(0, description="数量")
    stocks: List[Dict] = Field(default_factory=list, description="股票详情列表")


class BreakStats(BaseModel):
    """炸板统计"""
    stock_code: str
    stock_name: str
    break_time: str = Field(..., description="炸板时间")
    is_resealed: bool = Field(False, description="是否回封")
    reseal_time: Optional[str] = Field(None, description="回封时间")
    final_status: str = Field(..., description="最终状态")
    low_after_break: Optional[float] = Field(None, description="炸板后最低价")


class MarketOverview(BaseModel):
    """市场概览"""
    trade_date: date
    is_fallback: bool = Field(False, description="是否回退到历史数据")
    total_stocks: int = Field(0, description="全市场股票数")
    up_count: int = Field(0, description="上涨家数")
    down_count: int = Field(0, description="下跌家数")
    flat_count: int = Field(0, description="平盘家数")
    limit_up_count: int = Field(0, description="涨停家数")
    limit_down_count: int = Field(0, description="跌停家数")
    up_ratio: float = Field(0, description="上涨比例(%)")
    total_amount: float = Field(0, description="总成交额(亿)")


class SectorStatsResponse(BaseModel):
    """板块统计响应（带日期回退信息）"""
    trade_date: date = Field(..., description="实际数据日期")
    is_fallback: bool = Field(False, description="是否回退到历史数据")
    data: List[SectorStats] = Field(default_factory=list, description="板块统计列表")


class ContinuousLadderResponse(BaseModel):
    """连板梯队响应（带日期回退信息）"""
    trade_date: date = Field(..., description="实际数据日期")
    is_fallback: bool = Field(False, description="是否回退到历史数据")
    data: List[ContinuousLadder] = Field(default_factory=list, description="连板梯队列表")


class YesterdayContinuousStock(BaseModel):
    """昨日连板股票今日状态"""
    stock_code: str = Field(..., description="股票代码")
    stock_name: str = Field(..., description="股票名称")
    yesterday_days: int = Field(..., description="昨日连板天数")
    today_status: str = Field(..., description="今日状态: sealed/opened/broken")
    today_change_pct: Optional[float] = Field(None, description="今日涨跌幅(%)")


class YesterdayContinuousLadder(BaseModel):
    """昨日连板梯队"""
    continuous_days: int = Field(..., description="昨日连板天数")
    count: int = Field(0, description="该梯队股票数量")
    sealed_count: int = Field(0, description="今日封板数量")
    opened_count: int = Field(0, description="今日炸板数量")
    broken_count: int = Field(0, description="今日断板数量")
    stocks: List[YesterdayContinuousStock] = Field(default_factory=list, description="股票列表")


class YesterdayContinuousResponse(BaseModel):
    """昨日连板响应"""
    trade_date: date = Field(..., description="今日日期")
    yesterday_date: date = Field(..., description="昨日日期")
    is_fallback: bool = Field(False, description="是否回退到历史数据")
    data: List[YesterdayContinuousLadder] = Field(default_factory=list, description="昨日连板梯队列表")
