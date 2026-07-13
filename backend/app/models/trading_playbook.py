"""Persistent models for the daily trading playbook."""

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
    Text,
    UniqueConstraint,
)

from app.database import Base


class TradingRuleSource(Base):
    """Source material ingested into the trading rule library."""

    __tablename__ = "trading_rule_sources"
    __table_args__ = (
        UniqueConstraint(
            "source_key",
            "content_hash",
            name="uq_trading_rule_source_hash",
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_key = Column(String(80), nullable=False)
    source_path = Column(String(500), nullable=False)
    source_title = Column(String(255), nullable=False)
    content_hash = Column(String(64), nullable=False)
    transcript_generated_at = Column(DateTime, nullable=True)
    ingested_at = Column(DateTime, default=datetime.now, nullable=False)
    status = Column(String(20), default="ready", nullable=False)


class TradingModeRule(Base):
    """Versioned rule definition for a supported trading mode."""

    __tablename__ = "trading_mode_rules"
    __table_args__ = (
        UniqueConstraint(
            "mode_key",
            "version",
            name="uq_trading_mode_rule_version",
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    mode_key = Column(String(80), nullable=False, index=True)
    version = Column(Integer, nullable=False)
    name = Column(String(120), nullable=False)
    family = Column(String(40), nullable=False)
    style = Column(String(40), nullable=False)
    window = Column(String(80), nullable=False)
    automation_level = Column(String(20), nullable=False)
    description = Column(Text, default="", nullable=False)
    prerequisites_json = Column(JSON, default=dict, nullable=False)
    candidate_filters_json = Column(JSON, default=list, nullable=False)
    entry_trigger_json = Column(JSON, default=dict, nullable=False)
    invalidation_json = Column(JSON, default=dict, nullable=False)
    exit_trigger_json = Column(JSON, default=dict, nullable=False)
    risk_guidance_json = Column(JSON, default=dict, nullable=False)
    source_refs_json = Column(JSON, default=list, nullable=False)
    enabled = Column(Boolean, default=True, nullable=False)
    content_hash = Column(String(64), nullable=False)
    created_at = Column(DateTime, default=datetime.now, nullable=False)


class TradingPlanVersion(Base):
    """Immutable version of a generated or confirmed daily trading plan."""

    __tablename__ = "trading_plan_versions"
    __table_args__ = (
        UniqueConstraint(
            "target_trade_date",
            "stage",
            "version_no",
            name="uq_trading_plan_stage_version",
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_trade_date = Column(Date, nullable=False, index=True)
    target_trade_date = Column(Date, nullable=False, index=True)
    stage = Column(String(20), nullable=False)
    version_no = Column(Integer, nullable=False)
    parent_plan_version_id = Column(
        Integer,
        ForeignKey("trading_plan_versions.id"),
        nullable=True,
    )
    status = Column(String(20), default="draft", nullable=False)
    market_state_json = Column(JSON, default=dict, nullable=False)
    theme_ranking_json = Column(JSON, default=list, nullable=False)
    mode_radar_json = Column(JSON, default=list, nullable=False)
    rule_snapshot_json = Column(JSON, default=list, nullable=False)
    risk_settings_json = Column(JSON, default=dict, nullable=False)
    data_quality_json = Column(JSON, default=dict, nullable=False)
    change_summary_json = Column(JSON, default=dict, nullable=False)
    input_hash = Column(String(64), nullable=False)
    generated_at = Column(DateTime, default=datetime.now, nullable=False)
    confirmed_at = Column(DateTime, nullable=True)
    confirmed_by = Column(String(80), nullable=True)


class TradingPlanCandidate(Base):
    """Ranked stock candidate attached to a trading plan version."""

    __tablename__ = "trading_plan_candidates"
    __table_args__ = (
        UniqueConstraint(
            "plan_version_id",
            "stock_code",
            "primary_mode_key",
            name="uq_trading_plan_candidate",
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    plan_version_id = Column(
        Integer,
        ForeignKey("trading_plan_versions.id"),
        nullable=False,
        index=True,
    )
    stock_code = Column(String(10), nullable=False, index=True)
    stock_name = Column(String(50), nullable=False)
    action_trade_date = Column(Date, nullable=False, index=True)
    theme_name = Column(String(120), default="", nullable=False)
    primary_mode_key = Column(String(80), nullable=False)
    supporting_mode_keys_json = Column(JSON, default=list, nullable=False)
    role = Column(String(60), nullable=False)
    rank = Column(Integer, nullable=False)
    recognition_json = Column(JSON, default=dict, nullable=False)
    entry_trigger_json = Column(JSON, default=dict, nullable=False)
    invalidation_json = Column(JSON, default=dict, nullable=False)
    exit_trigger_json = Column(JSON, default=dict, nullable=False)
    risk_level = Column(String(20), nullable=False)
    position_reference = Column(Float, default=0, nullable=False)
    evidence_json = Column(JSON, default=list, nullable=False)
    manual_overrides_json = Column(JSON, default=dict, nullable=False)
    status = Column(String(20), default="waiting", nullable=False)


class TradingAlertEvent(Base):
    """Deduplicated alert emitted while monitoring a trading plan."""

    __tablename__ = "trading_alert_events"
    __table_args__ = (
        UniqueConstraint("dedup_key", name="uq_trading_alert_dedup"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    plan_version_id = Column(
        Integer,
        ForeignKey("trading_plan_versions.id"),
        nullable=False,
        index=True,
    )
    candidate_id = Column(
        Integer,
        ForeignKey("trading_plan_candidates.id"),
        nullable=True,
        index=True,
    )
    event_type = Column(String(40), nullable=False)
    severity = Column(String(20), nullable=False)
    dedup_key = Column(String(255), nullable=False)
    triggered_at = Column(DateTime, default=datetime.now, nullable=False)
    market_snapshot_json = Column(JSON, default=dict, nullable=False)
    message = Column(Text, nullable=False)
    channel_status_json = Column(JSON, default=dict, nullable=False)
    acknowledged_at = Column(DateTime, nullable=True)


class TradingExecutionReview(Base):
    """End-of-day review of plan signals, execution, and outcomes."""

    __tablename__ = "trading_execution_reviews"
    __table_args__ = (
        UniqueConstraint(
            "trade_date",
            "plan_version_id",
            name="uq_trading_execution_review",
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    trade_date = Column(Date, nullable=False, index=True)
    plan_version_id = Column(
        Integer,
        ForeignKey("trading_plan_versions.id"),
        nullable=False,
    )
    signal_review_json = Column(JSON, default=dict, nullable=False)
    manual_execution_json = Column(JSON, default=dict, nullable=False)
    plan_compliance_json = Column(JSON, default=dict, nullable=False)
    outcome_snapshot_json = Column(JSON, default=dict, nullable=False)
    data_quality_json = Column(JSON, default=dict, nullable=False)
    generated_at = Column(DateTime, default=datetime.now, nullable=False)
    finalized_at = Column(DateTime, nullable=True)


class TradingPlaybookSettings(Base):
    """Singleton configuration for playbook sizing and alert channels."""

    __tablename__ = "trading_playbook_settings"

    id = Column(Integer, primary_key=True, default=1)
    enabled = Column(Boolean, default=True, nullable=False)
    trial_position_pct = Column(Float, default=10, nullable=False)
    confirmed_position_pct = Column(Float, default=30, nullable=False)
    hard_stop_pct = Column(Float, default=5, nullable=False)
    max_action_candidates = Column(Integer, default=3, nullable=False)
    in_app_enabled = Column(Boolean, default=True, nullable=False)
    wechat_enabled = Column(Boolean, default=False, nullable=False)
    channel_config_json = Column(JSON, default=dict, nullable=False)
    updated_at = Column(
        DateTime,
        default=datetime.now,
        onupdate=datetime.now,
        nullable=False,
    )
