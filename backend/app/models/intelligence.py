"""Knowledge intelligence cache models."""
from datetime import datetime

from sqlalchemy import Column, Date, DateTime, Integer, JSON, String, Text, UniqueConstraint

from app.database import Base


class KnowledgeDocument(Base):
    """Cached ima knowledge document metadata, content and AI summary."""

    __tablename__ = "knowledge_documents"
    __table_args__ = (
        UniqueConstraint("source_key", "media_id", name="uq_knowledge_document_source_media"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_key = Column(String(30), nullable=False, index=True)
    source_name = Column(String(80), nullable=False)
    share_id = Column(String(128), nullable=False)
    source_version = Column(String(80), default="", nullable=False)
    folder_id = Column(String(80), default="", nullable=False)
    folder_path = Column(String(255), default="", nullable=False)
    media_id = Column(String(255), nullable=False, index=True)
    title = Column(String(255), nullable=False)
    media_type = Column(Integer, default=0, nullable=False)
    media_type_name = Column(String(40), default="", nullable=False)
    md5_sum = Column(String(80), default="", nullable=False)
    update_time = Column(String(40), default="", nullable=False)
    create_time = Column(String(40), default="", nullable=False)
    source_path = Column(Text, default="", nullable=False)
    jump_url = Column(Text, default="", nullable=False)
    raw_file_url = Column(Text, default="", nullable=False)
    abstract = Column(Text, default="", nullable=False)
    introduction = Column(Text, default="", nullable=False)
    content_text = Column(Text, default="", nullable=False)
    content_hash = Column(String(64), default="", nullable=False, index=True)
    trade_date = Column(Date, index=True)
    summary_json = Column(JSON, default=dict, nullable=False)
    summary_status = Column(String(20), default="pending", nullable=False)
    summary_error = Column(Text, default="", nullable=False)
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)


class DailyInfoDigest(Base):
    """Daily aggregated digest built from cached knowledge documents."""

    __tablename__ = "daily_info_digests"
    __table_args__ = (
        UniqueConstraint("trade_date", name="uq_daily_info_digest_trade_date"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    trade_date = Column(Date, nullable=False, index=True)
    summary_json = Column(JSON, default=dict, nullable=False)
    status = Column(String(20), default="pending", nullable=False)
    source_count = Column(Integer, default=0, nullable=False)
    content_hash = Column(String(64), default="", nullable=False)
    model = Column(String(80), default="", nullable=False)
    generated_at = Column(DateTime, default=datetime.now, nullable=False)
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)


class JiegeTradingRule(Base):
    """Structured rule distilled from the Jiege knowledge base."""

    __tablename__ = "jiege_trading_rules"
    __table_args__ = (
        UniqueConstraint("rule_key", name="uq_jiege_trading_rule_key"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    rule_key = Column(String(80), nullable=False)
    title = Column(String(120), nullable=False)
    category = Column(String(40), default="", nullable=False)
    summary = Column(Text, default="", nullable=False)
    payload_json = Column(JSON, default=dict, nullable=False)
    source_media_id = Column(String(255), default="", nullable=False)
    content_hash = Column(String(64), default="", nullable=False)
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)


class JiegeModeSignal(Base):
    """Daily prediction/review payload generated from Jiege rules."""

    __tablename__ = "jiege_mode_signals"
    __table_args__ = (
        UniqueConstraint("trade_date", name="uq_jiege_mode_signal_trade_date"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    trade_date = Column(Date, nullable=False, index=True)
    signal_json = Column(JSON, default=dict, nullable=False)
    status = Column(String(20), default="pending", nullable=False)
    content_hash = Column(String(64), default="", nullable=False)
    generated_at = Column(DateTime, default=datetime.now, nullable=False)
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)
