"""
大单记录模型
"""
from sqlalchemy import Column, Integer, String, DateTime, Float, ForeignKey, Index, Enum
from sqlalchemy.orm import relationship
from datetime import datetime
import enum

from app.database import Base


class OrderDirection(enum.Enum):
    """订单方向"""
    BUY = "buy"
    SELL = "sell"


class OrderType(enum.Enum):
    """订单类型"""
    ACTIVE_BUY = "active_buy"      # 主动买入（外盘）
    PASSIVE_BUY = "passive_buy"    # 被动买入（内盘）
    ACTIVE_SELL = "active_sell"    # 主动卖出（内盘）
    PASSIVE_SELL = "passive_sell"  # 被动卖出（外盘）
    UNKNOWN = "unknown"            # 未知


class BigOrder(Base):
    """大单记录表"""
    __tablename__ = "big_orders"
    __table_args__ = (
        Index('idx_stock_trade_time', 'stock_id', 'trade_time'),
        Index('idx_trade_time', 'trade_time'),
    )
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_id = Column(Integer, ForeignKey("stocks.id"), nullable=False, comment="股票ID")
    
    trade_time = Column(DateTime, nullable=False, comment="成交时间")
    trade_price = Column(Float, nullable=False, comment="成交价格")
    trade_volume = Column(Integer, nullable=False, comment="成交量(手)")
    trade_amount = Column(Float, nullable=False, comment="成交金额(元)")
    
    direction = Column(String(10), nullable=False, comment="方向(buy/sell)")
    order_type = Column(String(20), nullable=False, comment="类型(active_buy/passive_buy/active_sell/passive_sell)")
    
    is_limit_up_price = Column(Integer, default=0, comment="是否涨停价(0否1是)")
    is_limit_down_price = Column(Integer, default=0, comment="是否跌停价(0否1是)")
    
    # 盘口信息（成交时的盘口状态）
    bid1_price = Column(Float, comment="买一价")
    ask1_price = Column(Float, comment="卖一价")
    
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
    
    # 关联关系
    stock = relationship("Stock", back_populates="big_orders")
    
    def __repr__(self):
        return f"<BigOrder {self.stock_id} {self.direction} {self.trade_amount}>"
