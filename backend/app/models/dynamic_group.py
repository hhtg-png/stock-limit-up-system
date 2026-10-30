"""Current state and immutable archives for dynamic stock groups."""
from datetime import date, datetime

from sqlalchemy import Boolean, Date, DateTime, Float, ForeignKey, Index, Integer, JSON, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class DynamicGroup(Base):
    __tablename__ = "dynamic_groups"
    __table_args__ = (Index("ix_dynamic_groups_directory", "archived", "pinned", "updated_at"),)
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100))
    description: Mapped[str] = mapped_column(Text, default="")
    folder: Mapped[str] = mapped_column(String(100), default="", index=True)
    color: Mapped[str] = mapped_column(String(30), default="#409eff")
    pinned: Mapped[bool] = mapped_column(Boolean, default=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    archived: Mapped[bool] = mapped_column(Boolean, default=False)
    universe: Mapped[str] = mapped_column(String(20), default="all_a")
    membership_mode: Mapped[str] = mapped_column(String(20), default="retain")
    match_mode: Mapped[str] = mapped_column(String(10), default="all")
    rules: Mapped[list] = mapped_column(JSON, default=list)
    strategy: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    version: Mapped[int] = mapped_column(Integer, default=1)
    member_count: Mapped[int] = mapped_column(Integer, default=0)
    latest_selection_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    data_status: Mapped[str] = mapped_column(String(30), default="pending")
    trade_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    previous_trade_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    source_time: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_evaluated_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    warnings: Mapped[list] = mapped_column(JSON, default=list)
    index_state: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)


class DynamicGroupVersion(Base):
    __tablename__ = "dynamic_group_versions"
    __table_args__ = (UniqueConstraint("group_id", "version", name="uq_dynamic_group_version"),)
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    group_id: Mapped[int] = mapped_column(ForeignKey("dynamic_groups.id"), index=True)
    version: Mapped[int] = mapped_column(Integer)
    definition: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)


class DynamicGroupEvent(Base):
    __tablename__ = "dynamic_group_events"
    __table_args__ = (Index("ix_dynamic_group_event_archive", "group_id", "trade_date", "id"),)
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    group_id: Mapped[int] = mapped_column(ForeignKey("dynamic_groups.id"))
    version: Mapped[int] = mapped_column(Integer)
    stock_code: Mapped[str] = mapped_column(String(12))
    kind: Mapped[str] = mapped_column(String(30))
    trade_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    observed_at: Mapped[datetime] = mapped_column(DateTime)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)


class DynamicGroupMember(Base):
    __tablename__ = "dynamic_group_members"
    __table_args__ = (UniqueConstraint("group_id", "stock_code", name="uq_dynamic_group_member"),)
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    group_id: Mapped[int] = mapped_column(ForeignKey("dynamic_groups.id"), index=True)
    stock_code: Mapped[str] = mapped_column(String(12))
    entry_event_id: Mapped[int] = mapped_column(ForeignKey("dynamic_group_events.id"))
    entered_at: Mapped[datetime] = mapped_column(DateTime)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    data_status: Mapped[str] = mapped_column(String(30), default="ready")


class DynamicGroupSelection(Base):
    __tablename__ = "dynamic_group_selections"
    __table_args__ = (Index("ix_dynamic_group_selection_date", "group_id", "trade_date", "id"),
                      UniqueConstraint("group_id", "batch_key", name="uq_dynamic_group_selection_key"))
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    group_id: Mapped[int] = mapped_column(ForeignKey("dynamic_groups.id"))
    version: Mapped[int] = mapped_column(Integer)
    trade_date: Mapped[date] = mapped_column(Date)
    selected_at: Mapped[datetime] = mapped_column(DateTime)
    member_count: Mapped[int] = mapped_column(Integer)
    kind: Mapped[str] = mapped_column(String(30))
    batch_key: Mapped[str] = mapped_column(String(150))
    definition: Mapped[dict] = mapped_column(JSON)
    data_status: Mapped[str] = mapped_column(String(30), default="ready")
    warnings: Mapped[list] = mapped_column(JSON, default=list)


class DynamicGroupSelectionItem(Base):
    __tablename__ = "dynamic_group_selection_items"
    __table_args__ = (UniqueConstraint("selection_id", "stock_code", name="uq_dynamic_group_selection_stock"),)
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    selection_id: Mapped[int] = mapped_column(ForeignKey("dynamic_group_selections.id"), index=True)
    stock_code: Mapped[str] = mapped_column(String(12))
    entry_event_id: Mapped[int] = mapped_column(ForeignKey("dynamic_group_events.id"))
    payload: Mapped[dict] = mapped_column(JSON)


class DynamicGroupIntradayPoint(Base):
    __tablename__ = "dynamic_group_intraday_points"
    __table_args__ = (UniqueConstraint("group_id", "version", "minute", name="uq_dynamic_group_point_minute"),
                      Index("ix_dynamic_group_point_day", "group_id", "trade_date", "minute"))
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    group_id: Mapped[int] = mapped_column(ForeignKey("dynamic_groups.id"))
    version: Mapped[int] = mapped_column(Integer)
    trade_date: Mapped[date] = mapped_column(Date)
    minute: Mapped[datetime] = mapped_column(DateTime)
    value: Mapped[float | None] = mapped_column(Float, nullable=True)
    expected_count: Mapped[int] = mapped_column(Integer)
    covered_count: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(30))
    segment: Mapped[int] = mapped_column(Integer, default=1)


class DynamicGroupSampleClaim(Base):
    __tablename__ = "dynamic_group_sample_claims"
    __table_args__ = (UniqueConstraint("group_id", "version", "slot", "input_signature", name="uq_dynamic_group_sample_slot"),)
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    group_id: Mapped[int] = mapped_column(ForeignKey("dynamic_groups.id"))
    version: Mapped[int] = mapped_column(Integer)
    slot: Mapped[datetime] = mapped_column(DateTime)
    input_signature: Mapped[str] = mapped_column(String(64))
