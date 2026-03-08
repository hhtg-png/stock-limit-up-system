"""
涨停记录相关模型
"""
from sqlalchemy import Column, Integer, String, DateTime, Float, Text, ForeignKey, Date, Index, Boolean
from sqlalchemy.orm import relationship
from datetime import datetime, date

from app.database import Base


class LimitUpRecord(Base):
    """涨停记录表"""
    __tablename__ = "limit_up_records"
    __table_args__ = (
        Index('idx_stock_date', 'stock_id', 'trade_date', unique=True),
        Index('idx_trade_date', 'trade_date'),
    )
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_id = Column(Integer, ForeignKey("stocks.id"), nullable=False, comment="股票ID")
    trade_date = Column(Date, nullable=False, comment="交易日期")
    
    # 涨停时间（精确到秒）
    first_limit_up_time = Column(DateTime, comment="首次涨停时间")
    
    # 涨停原因
    limit_up_reason = Column(Text, comment="涨停原因")
    reason_category = Column(String(50), comment="原因分类(题材/业绩/重组等)")
    
    # 连板信息
    continuous_limit_up_days = Column(Integer, default=1, comment="连板天数")
    
    # 开板信息
    open_count = Column(Integer, default=0, comment="开板次数")
    is_final_sealed = Column(Boolean, default=True, comment="是否最终封死")
    current_status = Column(String(20), default="unknown", comment="当前状态(sealed/opened/broken)")
    final_seal_time = Column(DateTime, comment="最终封板时间")
    
    # 封单信息
    seal_amount = Column(Float, comment="封单金额(万元)")
    seal_volume = Column(Integer, comment="封单量(手)")
    
    # 交易信息
    open_price = Column(Float, comment="开盘价")
    close_price = Column(Float, comment="收盘价")
    limit_up_price = Column(Float, comment="涨停价")
    turnover_rate = Column(Float, comment="换手率(%)")
    amplitude = Column(Float, comment="振幅(%)")
    volume = Column(Integer, comment="成交量(手)")
    amount = Column(Float, comment="成交额(万元)")
    
    # 数据来源
    data_source = Column(String(20), comment="数据来源(THS/KPL/TDX)")
    is_validated = Column(Boolean, default=False, comment="是否已验证")
    
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment="更新时间")
    
    # 关联关系
    stock = relationship("Stock", back_populates="limit_up_records")
    status_changes = relationship("LimitUpStatusChange", back_populates="limit_up_record", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<LimitUpRecord {self.stock_id} {self.trade_date}>"


class LimitUpStatusChange(Base):
    """涨停状态变化表"""
    __tablename__ = "limit_up_status_changes"
    __table_args__ = (
        Index('idx_record_time', 'limit_up_record_id', 'change_time'),
    )
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    limit_up_record_id = Column(Integer, ForeignKey("limit_up_records.id"), nullable=False, comment="涨停记录ID")
    
    change_time = Column(DateTime, nullable=False, comment="变化时间")
    status = Column(String(20), nullable=False, comment="状态(sealed/opened/resealed)")
    price = Column(Float, comment="当时价格")
    seal_amount = Column(Float, comment="封单金额(万元)")
    seal_volume = Column(Integer, comment="封单量(手)")
    
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
    
    # 关联关系
    limit_up_record = relationship("LimitUpRecord", back_populates="status_changes")
    
    def __repr__(self):
        return f"<LimitUpStatusChange {self.limit_up_record_id} {self.status} {self.change_time}>"
