"""Persistent cache models for Tongdaxin plugin data."""
from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import Date, DateTime, Index, JSON, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class TdxStockMoveCache(Base):
    """Cached stock movement analysis payload for fast Tongdaxin linkage."""

    __tablename__ = "tdx_stock_move_cache"
    __table_args__ = (
        UniqueConstraint(
            "stock_code",
            "source_scope",
            "trade_date",
            name="uq_tdx_stock_move_cache_code_scope_date",
        ),
        Index("ix_tdx_stock_move_cache_code_scope", "stock_code", "source_scope"),
        Index("ix_tdx_stock_move_cache_generated_at", "generated_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    stock_code: Mapped[str] = mapped_column(String(6), nullable=False)
    source_scope: Mapped[str] = mapped_column(String(20), nullable=False, default="mixed")
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    stock_name: Mapped[str] = mapped_column(String(50), nullable=False, default="")
    payload_json: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    source_status: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    warnings: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    generated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.now)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.now, onupdate=datetime.now)
