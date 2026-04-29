"""
市场复盘接口响应模型
"""
from datetime import date

from pydantic import BaseModel, ConfigDict, Field


class MarketReviewDailyMetricRow(BaseModel):
    """市场复盘日级指标行"""

    model_config = ConfigDict(from_attributes=True)

    trade_date: date
    limit_up_count: int
    limit_down_count: int
    continuous_count: int
    max_board_height: int
    second_board_height: int
    gem_board_height: int
    first_to_second_rate: float
    continuous_promotion_rate: float
    seal_rate: float
    yesterday_limit_up_avg_change: float
    yesterday_continuous_avg_change: float
    market_turnover: float
    up_count_ex_st: int
    down_count_ex_st: int
    limit_up_amount: float
    broken_amount: float


class MarketReviewDailyData(BaseModel):
    """市场复盘日级指标数据块"""

    series: list[date] = Field(default_factory=list)
    rows: list[MarketReviewDailyMetricRow] = Field(default_factory=list)


class MarketReviewDailyResponse(BaseModel):
    """市场复盘日级指标响应"""

    data: MarketReviewDailyData


class MarketReviewStockItem(BaseModel):
    """市场复盘个股条目"""

    model_config = ConfigDict(from_attributes=True)

    stock_code: str
    stock_name: str
    today_continuous_days: int
    today_sealed_close: bool
    today_opened_close: bool
    change_pct: float | None = None
    amount: float
    limit_up_reason: str | None = None


class MarketReviewDetailResponse(BaseModel):
    """市场复盘明细响应"""

    trade_date: date
    stocks: list[MarketReviewStockItem] = Field(default_factory=list)


class MarketReviewLadderItem(BaseModel):
    """市场复盘梯队条目"""

    continuous_days: int
    count: int
    stocks: list[MarketReviewStockItem] = Field(default_factory=list)


class MarketReviewLadderResponse(BaseModel):
    """市场复盘梯队响应"""

    trade_date: date
    ladders: list[MarketReviewLadderItem] = Field(default_factory=list)
