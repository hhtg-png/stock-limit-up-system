"""Durable history jobs and immutable adjusted-price/index revisions."""
from __future__ import annotations

from datetime import date, datetime
from sqlalchemy import Date, DateTime, Index, Integer, JSON, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from app.database import Base
from app.utils.time_utils import now_cn


class DynamicGroupStockHistoryRevision(Base):
    __tablename__ = "dynamic_group_stock_history_revisions"
    __table_args__ = (UniqueConstraint("stock_code", "digest", name="uq_dynamic_stock_history_digest"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    stock_code: Mapped[str] = mapped_column(String(6), index=True)
    digest: Mapped[str] = mapped_column(String(64))
    source: Mapped[str] = mapped_column(String(40))
    adjustment: Mapped[str] = mapped_column(String(10))
    requested_start: Mapped[date] = mapped_column(Date)
    prices: Mapped[dict] = mapped_column(JSON)
    fetched_at: Mapped[datetime] = mapped_column(DateTime, default=now_cn)


class DynamicGroupStockHistoryCache(Base):
    __tablename__ = "dynamic_group_stock_history_cache"
    stock_code: Mapped[str] = mapped_column(String(6), primary_key=True)
    revision_id: Mapped[int] = mapped_column(Integer)
    refreshed_at: Mapped[datetime] = mapped_column(DateTime)


class DynamicGroupHistoryJob(Base):
    __tablename__ = "dynamic_group_history_jobs"
    __table_args__ = (Index("ix_dynamic_history_jobs_due", "next_run_at", "last_attempt_at"),)
    selection_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    signature: Mapped[str] = mapped_column(String(64))
    stock_codes: Mapped[list] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(String(20), default="pending")
    claim_owner: Mapped[str | None] = mapped_column(String(40), nullable=True)
    latest_revision_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    last_attempt_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    next_run_at: Mapped[datetime] = mapped_column(DateTime, default=now_cn)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=now_cn)


class DynamicGroupHistoryRevision(Base):
    __tablename__ = "dynamic_group_history_revisions"
    __table_args__ = (UniqueConstraint("selection_id", "digest", name="uq_dynamic_history_revision_digest"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    selection_id: Mapped[int] = mapped_column(Integer, index=True)
    signature: Mapped[str] = mapped_column(String(64))
    digest: Mapped[str] = mapped_column(String(64))
    base_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    expected_count: Mapped[int] = mapped_column(Integer)
    points: Mapped[list] = mapped_column(JSON)
    provenance: Mapped[list] = mapped_column(JSON)
    warnings: Mapped[list] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(String(20))
    generated_at: Mapped[datetime] = mapped_column(DateTime, default=now_cn)
