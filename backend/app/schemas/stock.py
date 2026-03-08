"""
股票相关数据模型
"""
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime


class StockBase(BaseModel):
    """股票基础信息"""
    stock_code: str = Field(..., description="股票代码")
    stock_name: str = Field(..., description="股票名称")
    market: str = Field(..., description="市场(SH/SZ)")


class StockCreate(StockBase):
    """创建股票"""
    industry: Optional[str] = None
    concept: Optional[List[str]] = None
    total_shares: Optional[int] = None
    circulating_shares: Optional[int] = None
    is_st: bool = False
    is_kc: bool = False
    is_cy: bool = False


class StockResponse(StockBase):
    """股票响应"""
    id: int
    industry: Optional[str] = None
    concept: Optional[List[str]] = None
    is_st: bool = False
    is_kc: bool = False
    is_cy: bool = False
    limit_up_ratio: float = Field(..., description="涨停比例")
    created_at: datetime
    
    class Config:
        from_attributes = True


class StockQuote(BaseModel):
    """股票实时行情"""
    stock_code: str
    stock_name: str
    current_price: float = Field(..., description="当前价")
    pre_close: float = Field(..., description="昨收价")
    open_price: float = Field(..., description="开盘价")
    high_price: float = Field(..., description="最高价")
    low_price: float = Field(..., description="最低价")
    volume: int = Field(..., description="成交量(手)")
    amount: float = Field(..., description="成交额(元)")
    change_pct: float = Field(..., description="涨跌幅(%)")
    change_amount: float = Field(..., description="涨跌额")
    turnover_rate: float = Field(0, description="换手率(%)")
    update_time: datetime
