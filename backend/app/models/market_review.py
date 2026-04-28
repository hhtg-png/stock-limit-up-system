"""
市场复盘相关模型
"""
from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    JSON,
    String,
    Time,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship

from app.database import Base


class MarketReviewDailyMetric(Base):
    """市场复盘日级指标表"""

    __tablename__ = "market_review_daily_metric"
    __table_args__ = (
        UniqueConstraint("trade_date", name="uq_review_metric_trade_date"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    trade_date = Column(Date, nullable=False, comment="交易日期")
    limit_up_count = Column(Integer, default=0, nullable=False, comment="涨停数量")
    limit_down_count = Column(Integer, default=0, nullable=False, comment="跌停数量")
    continuous_count = Column(Integer, default=0, nullable=False, comment="连板数量")
    max_board_height = Column(Integer, default=0, nullable=False, comment="最高板高度")
    second_board_height = Column(Integer, default=0, nullable=False, comment="二板高度")
    gem_board_height = Column(Integer, default=0, nullable=False, comment="创业板高度")
    first_to_second_rate = Column(Float, default=0, nullable=False, comment="首板晋级率")
    continuous_promotion_rate = Column(Float, default=0, nullable=False, comment="连板晋级率")
    seal_rate = Column(Float, default=0, nullable=False, comment="封板率")
    yesterday_limit_up_avg_change = Column(Float, default=0, nullable=False, comment="昨日涨停均涨幅")
    yesterday_continuous_avg_change = Column(Float, default=0, nullable=False, comment="昨日连板均涨幅")
    market_turnover = Column(Float, default=0, nullable=False, comment="市场成交额")
    up_count_ex_st = Column(Integer, default=0, nullable=False, comment="非ST上涨家数")
    down_count_ex_st = Column(Integer, default=0, nullable=False, comment="非ST下跌家数")
    limit_up_amount = Column(Float, default=0, nullable=False, comment="涨停成交额")
    broken_amount = Column(Float, default=0, nullable=False, comment="炸板成交额")
    calc_version = Column(Integer, default=1, nullable=False, comment="计算版本")
    source_status = Column(String(20), default="primary", nullable=False, comment="来源状态")
    created_at = Column(DateTime, default=datetime.now, nullable=False, comment="创建时间")
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False, comment="更新时间")


class MarketReviewStockDaily(Base):
    """市场复盘个股日级事实表"""

    __tablename__ = "market_review_stock_daily"
    __table_args__ = (
        UniqueConstraint("trade_date", "stock_id", name="uq_review_stock_daily"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    trade_date = Column(Date, nullable=False, comment="交易日期")
    stock_id = Column(Integer, ForeignKey("stocks.id"), nullable=False, index=True, comment="股票ID")
    stock_code = Column(String(10), nullable=False, comment="股票代码")
    stock_name = Column(String(50), nullable=False, comment="股票名称")
    board_type = Column(String(20), default="main", nullable=False, comment="板块类型")
    is_st = Column(Boolean, default=False, nullable=False, comment="是否ST")
    yesterday_limit_up = Column(Boolean, default=False, nullable=False, comment="昨日是否涨停")
    yesterday_continuous_days = Column(Integer, default=0, nullable=False, comment="昨日连板天数")
    today_touched_limit_up = Column(Boolean, default=False, nullable=False, comment="今日是否触板")
    today_sealed_close = Column(Boolean, default=False, nullable=False, comment="今日收盘封死")
    today_opened_close = Column(Boolean, default=False, nullable=False, comment="今日收盘开板")
    today_broken = Column(Boolean, default=False, nullable=False, comment="今日炸板")
    today_continuous_days = Column(Integer, default=0, nullable=False, comment="今日连板天数")
    first_limit_time = Column(Time, comment="首次封板时间")
    final_seal_time = Column(Time, comment="最终封板时间")
    open_count = Column(Integer, default=0, nullable=False, comment="开板次数")
    close_price = Column(Float, comment="收盘价")
    pre_close = Column(Float, comment="昨收价")
    change_pct = Column(Float, comment="涨跌幅")
    amount = Column(Float, default=0, nullable=False, comment="成交额")
    turnover_rate = Column(Float, comment="换手率")
    tradable_market_value = Column(Float, comment="可流通市值")
    limit_up_reason = Column(String(255), comment="涨停原因")
    data_quality_flag = Column(String(20), default="ok", nullable=False, comment="数据质量标记")
    created_at = Column(DateTime, default=datetime.now, nullable=False, comment="创建时间")
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False, comment="更新时间")
    stock = relationship("Stock")


class MarketReviewLimitUpEvent(Base):
    """市场复盘涨停事件轨迹表"""

    __tablename__ = "market_review_limitup_event"
    __table_args__ = (
        UniqueConstraint(
            "trade_date",
            "stock_id",
            "event_type",
            "event_seq",
            name="uq_review_limitup_event",
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    trade_date = Column(Date, nullable=False, index=True, comment="交易日期")
    stock_id = Column(Integer, ForeignKey("stocks.id"), nullable=False, index=True, comment="股票ID")
    stock_code = Column(String(10), nullable=False, index=True, comment="股票代码")
    event_type = Column(String(20), nullable=False, index=True, comment="事件类型")
    event_time = Column(Time, comment="事件时间")
    event_seq = Column(Integer, default=0, nullable=False, comment="事件序号")
    source_name = Column(String(20), nullable=False, comment="来源名称")
    payload_json = Column(JSON, default=dict, nullable=False, comment="事件载荷")
    created_at = Column(DateTime, default=datetime.now, nullable=False, comment="创建时间")
    stock = relationship("Stock")
