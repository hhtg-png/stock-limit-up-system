"""Normalized domain snapshots for trading playbook evaluation."""

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class DataQuality:
    status: str
    as_of: datetime
    source: str
    stale: bool = False
    warnings: List[str] = field(default_factory=list)
    forced_degraded: bool = False
    degradation_reason: Optional[str] = None


@dataclass(frozen=True)
class QuotePoint:
    stock_code: str
    stock_name: str
    price: float
    pre_close: float
    open_price: float
    change_pct: float
    speed_pct: float
    amount: float
    turnover_rate: float
    bid1_price: float
    bid1_volume: float
    limit_up: float
    captured_at: datetime


@dataclass(frozen=True)
class QuoteSnapshot:
    trade_date: date
    quotes: Dict[str, QuotePoint]
    quality: DataQuality


@dataclass
class CandidateSnapshot:
    stock_code: str
    stock_name: str
    theme_name: str
    features: Dict[str, Any]
    evidence: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class MarketSnapshot:
    source_trade_date: date
    target_trade_date: date
    stage: str
    as_of: datetime
    market_features: Dict[str, Any]
    candidates: List[CandidateSnapshot]
    theme_rankings: List[Dict[str, Any]]
    quality: DataQuality


@dataclass(frozen=True)
class ModeEvaluation:
    mode_key: str
    stock_code: str
    status: str
    score: float
    role: str
    risk_level: str
    entry_trigger: Dict[str, Any]
    invalidation: Dict[str, Any]
    exit_trigger: Dict[str, Any]
    evidence: List[Dict[str, Any]]
    rule_version: int = 1
    rule_hash: str = ""
    action_scope: str = "target"
