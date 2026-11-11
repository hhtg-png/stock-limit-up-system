"""
市场复盘接口响应模型
"""
from datetime import date, datetime, time

from pydantic import BaseModel, ConfigDict, Field


class MarketReviewDailyMetricRow(BaseModel):
    """市场复盘日级指标行"""

    model_config = ConfigDict(from_attributes=True)

    trade_date: date
    source_status: str | None = None
    limit_up_count: int | None
    limit_down_count: int | None
    continuous_count: int | None
    max_board_height: int | None
    second_board_height: int | None
    gem_board_height: int | None
    max_board_label: str | None = None
    second_board_label: str | None = None
    gem_board_label: str | None = None
    first_to_second_rate: float | None
    continuous_promotion_rate: float | None
    seal_rate: float | None
    yesterday_limit_up_avg_change: float | None
    yesterday_continuous_avg_change: float | None
    market_turnover: float | None
    up_count_ex_st: int | None
    down_count_ex_st: int | None
    limit_up_amount: float | None
    broken_amount: float | None


class MarketReviewDailyData(BaseModel):
    """市场复盘日级指标数据块"""

    series: list[date] = Field(default_factory=list)
    rows: list[MarketReviewDailyMetricRow] = Field(default_factory=list)


class MarketReviewDailyResponse(BaseModel):
    """市场复盘日级指标响应"""

    data: MarketReviewDailyData
    requested_start_date: date | None = None
    requested_end_date: date | None = None
    start_date: date | None = None
    end_date: date | None = None
    latest_trade_date: date | None = None
    is_fallback: bool = Field(False, description="是否回退到最近可用复盘日")


class MarketReviewStockItem(BaseModel):
    """市场复盘个股条目"""

    model_config = ConfigDict(from_attributes=True)

    stock_code: str
    stock_name: str
    today_continuous_days: int
    today_touched_limit_up: bool | None = None
    today_sealed_close: bool
    today_opened_close: bool
    change_pct: float | None = None
    amount: float | None
    limit_up_reason: str | None = None
    first_limit_time: time | None = None
    final_seal_time: time | None = None
    open_count: int | None = None
    yesterday_continuous_days: int | None = None
    turnover_rate: float | None = None
    close_price: float | None = None
    pre_close: float | None = None
    data_quality_flag: str | None = None


class MarketReviewDetailResponse(BaseModel):
    """市场复盘明细响应"""

    trade_date: date
    is_fallback: bool = Field(False, description="是否回退到历史数据")
    stocks: list[MarketReviewStockItem] = Field(default_factory=list)


class MarketReviewLadderItem(BaseModel):
    """市场复盘梯队条目"""

    continuous_days: int
    count: int
    cohort_count: int | None = Field(0, description="同梯队样本数量，包含未晋级个股")
    cohort_sealed_count: int | None = Field(0, description="同梯队封板数量")
    cohort_opened_count: int | None = Field(0, description="同梯队炸板数量")
    cohort_seal_rate: float | None = Field(0.0, description="同梯队封板率")
    cohort_avg_change: float | None = Field(None, description="同梯队平均涨幅")
    stocks: list[MarketReviewStockItem] = Field(default_factory=list)
    cohort_stocks: list[MarketReviewStockItem] = Field(default_factory=list, description="同梯队样本股票，包含未晋级个股")


class MarketReviewLadderResponse(BaseModel):
    """市场复盘梯队响应"""

    trade_date: date
    is_fallback: bool = Field(False, description="是否回退到历史数据")
    ladders: list[MarketReviewLadderItem] = Field(default_factory=list)


class MarketReviewIntradayResponse(MarketReviewDailyResponse):
    """市场复盘盘中快照响应"""

    data_status: str = Field("ready", description="ready 或 unavailable")
    is_intraday: bool = Field(True, description="是否为盘中实时快照")
    is_live: bool = Field(False, description="是否为交易时段实时采集")
    snapshot_time: datetime
    source_status: str | None = None
    detail: MarketReviewDetailResponse
    ladder: MarketReviewLadderResponse
