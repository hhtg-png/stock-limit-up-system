"""
涨停相关数据模型
"""
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime, date


class LimitUpBase(BaseModel):
    """涨停基础信息"""
    stock_code: str = Field(..., description="股票代码")
    stock_name: str = Field(..., description="股票名称")
    trade_date: date = Field(..., description="交易日期")


class LimitUpRecord(LimitUpBase):
    """涨停记录"""
    id: int
    first_limit_up_time: Optional[datetime] = Field(None, description="首次涨停时间")
    limit_up_reason: Optional[str] = Field(None, description="涨停原因")
    reason_category: Optional[str] = Field(None, description="原因分类")
    continuous_limit_up_days: int = Field(1, description="连板天数")
    open_count: int = Field(0, description="开板次数")
    is_final_sealed: bool = Field(True, description="是否最终封死")
    current_status: Optional[str] = Field("unknown", description="当前状态(sealed/opened/broken)")
    final_seal_time: Optional[datetime] = Field(None, description="最终封板时间")
    seal_amount: Optional[float] = Field(None, description="封单金额(万元)")
    limit_up_price: Optional[float] = Field(None, description="涨停价")
    turnover_rate: Optional[float] = Field(None, description="换手率(%)")
    amount: Optional[float] = Field(None, description="成交额(万元)")
    tradable_market_value: Optional[float] = Field(None, description="实际流通值(万元)")
    data_source: Optional[str] = Field(None, description="数据来源")
    
    class Config:
        from_attributes = True


class LimitUpRealtime(LimitUpBase):
    """实时涨停信息"""
    first_limit_up_time: Optional[str] = Field(None, description="首次涨停时间(HH:MM:SS)")
    final_seal_time: Optional[str] = Field(None, description="最终封板时间(HH:MM:SS)")
    limit_up_reason: Optional[str] = Field(None, description="涨停原因")
    reason_category: Optional[str] = Field(None, description="原因分类")
    continuous_limit_up_days: int = Field(1, description="连板天数")
    open_count: int = Field(0, description="开板次数")
    is_sealed: bool = Field(True, description="当前是否封板")
    current_status: str = Field("unknown", description="当前状态(sealed/opened/broken)")
    seal_amount: Optional[float] = Field(None, description="封单金额(万元)")
    seal_volume: Optional[int] = Field(None, description="封单量(手)")
    limit_up_price: float = Field(..., description="涨停价")
    current_price: float = Field(..., description="当前价")
    turnover_rate: Optional[float] = Field(None, description="换手率(%)")
    amount: Optional[float] = Field(None, description="成交额(万元)")
    tradable_market_value: Optional[float] = Field(None, description="实际流通值(万元)")
    market: str = Field(..., description="市场")
    industry: Optional[str] = Field(None, description="行业")


class LimitUpStatusChange(BaseModel):
    """涨停状态变化"""
    change_time: datetime = Field(..., description="变化时间")
    status: str = Field(..., description="状态(sealed/opened/resealed)")
    price: Optional[float] = Field(None, description="当时价格")
    seal_amount: Optional[float] = Field(None, description="封单金额")
    
    class Config:
        from_attributes = True


class LimitUpDetail(LimitUpRecord):
    """涨停详情"""
    status_changes: List[LimitUpStatusChange] = Field(default_factory=list, description="状态变化列表")
    market: str = Field(..., description="市场")
    industry: Optional[str] = Field(None, description="行业")


class LimitUpHistoryQuery(BaseModel):
    """历史涨停查询参数"""
    stock_code: Optional[str] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    reason_category: Optional[str] = None
    min_continuous_days: Optional[int] = None
    page: int = Field(1, ge=1)
    page_size: int = Field(50, ge=1, le=200)


class LimitUpReasonStats(BaseModel):
    """涨停原因统计"""
    reason_category: str = Field(..., description="原因分类")
    count: int = Field(..., description="数量")
    percentage: float = Field(..., description="占比(%)")
    stocks: List[str] = Field(default_factory=list, description="股票列表")


class LimitUpRealtimeResponse(BaseModel):
    """实时涨停列表响应（带日期回退信息）"""
    trade_date: date = Field(..., description="实际数据日期")
    is_fallback: bool = Field(False, description="是否回退到历史数据")
    data: List[LimitUpRealtime] = Field(default_factory=list, description="涨停列表")
