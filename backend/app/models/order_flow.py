"""
盘口数据模型
"""
from sqlalchemy import Column, Integer, String, DateTime, Float, ForeignKey, Index, JSON
from sqlalchemy.orm import relationship
from datetime import datetime

from app.database import Base


class OrderBookSnapshot(Base):
    """五档盘口快照表"""
    __tablename__ = "order_book_snapshots"
    __table_args__ = (
        Index('idx_stock_snapshot_time', 'stock_id', 'snapshot_time'),
    )
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_id = Column(Integer, ForeignKey("stocks.id"), nullable=False, comment="股票ID")
    
    snapshot_time = Column(DateTime, nullable=False, comment="快照时间")
    
    # 当前价格信息
    current_price = Column(Float, comment="当前价")
    pre_close = Column(Float, comment="昨收价")
    open_price = Column(Float, comment="开盘价")
    high_price = Column(Float, comment="最高价")
    low_price = Column(Float, comment="最低价")
    
    # 买五档（JSON数组格式）
    bid_prices = Column(JSON, comment="买五价格[bid1,bid2,bid3,bid4,bid5]")
    bid_volumes = Column(JSON, comment="买五量[vol1,vol2,vol3,vol4,vol5]")
    
    # 卖五档（JSON数组格式）
    ask_prices = Column(JSON, comment="卖五价格[ask1,ask2,ask3,ask4,ask5]")
    ask_volumes = Column(JSON, comment="卖五量[vol1,vol2,vol3,vol4,vol5]")
    
    # 成交信息
    volume = Column(Integer, comment="成交量(手)")
    amount = Column(Float, comment="成交额(元)")
    
    # 内外盘
    buy_volume = Column(Integer, comment="外盘(主动买)")
    sell_volume = Column(Integer, comment="内盘(主动卖)")
    
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
    
    # 关联关系
    stock = relationship("Stock", back_populates="order_book_snapshots")
    
    def __repr__(self):
        return f"<OrderBookSnapshot {self.stock_id} {self.snapshot_time}>"
    
    @property
    def bid1(self) -> tuple:
        """获取买一价和量"""
        if self.bid_prices and self.bid_volumes:
            return (self.bid_prices[0], self.bid_volumes[0])
        return (0, 0)
    
    @property
    def ask1(self) -> tuple:
        """获取卖一价和量"""
        if self.ask_prices and self.ask_volumes:
            return (self.ask_prices[0], self.ask_volumes[0])
        return (0, 0)
    
    @property
    def total_bid_volume(self) -> int:
        """买盘总量"""
        if self.bid_volumes:
            return sum(self.bid_volumes)
        return 0
    
    @property
    def total_ask_volume(self) -> int:
        """卖盘总量"""
        if self.ask_volumes:
            return sum(self.ask_volumes)
        return 0
