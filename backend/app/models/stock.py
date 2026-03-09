"""
股票基础信息模型
"""
from sqlalchemy import Column, Integer, String, DateTime, Text, JSON, Index
from sqlalchemy.orm import relationship
from datetime import datetime

from app.database import Base


class Stock(Base):
    """股票基础信息表"""
    __tablename__ = "stocks"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(10), unique=True, nullable=False, index=True, comment="股票代码")
    stock_name = Column(String(50), nullable=False, comment="股票名称")
    market = Column(String(10), nullable=False, comment="市场(SH/SZ)")
    industry = Column(String(50), comment="所属行业")
    concept = Column(JSON, comment="相关概念")
    total_shares = Column(Integer, comment="总股本(股)")
    circulating_shares = Column(Integer, comment="流通股本(股)")
    is_st = Column(Integer, default=0, comment="是否ST(0否1是)")
    is_kc = Column(Integer, default=0, comment="是否科创板(0否1是)")
    is_cy = Column(Integer, default=0, comment="是否创业板(0否1是)")
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment="更新时间")
    
    # 关联关系
    limit_up_records = relationship("LimitUpRecord", back_populates="stock")
    big_orders = relationship("BigOrder", back_populates="stock")
    order_book_snapshots = relationship("OrderBookSnapshot", back_populates="stock")
    
    def __repr__(self):
        return f"<Stock {self.stock_code} {self.stock_name}>"
    
    @property
    def full_code(self) -> str:
        """获取完整股票代码（带市场前缀）"""
        return f"{self.market.lower()}{self.stock_code}"
    
    @property
    def limit_up_ratio(self) -> float:
        """获取涨停比例"""
        if self.is_st:
            return 0.05
        elif self.is_kc or self.is_cy:
            return 0.20
        else:
            return 0.10
