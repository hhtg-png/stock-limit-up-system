from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy import distinct, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.limit_up import LimitUpRecord
from app.models.market_review import DailyAnalysisRecord
from app.models.stock import Stock


DAILY_ANALYSIS_COLUMNS = [
    "时间",
    "连板唯一性",
    "反包+趋势+弹钢琴",
    "炸板反包",
    "辨识度",
    "二波",
    "20cm",
    "一字套利",
    "板块",
    "负反馈",
]
SIGNAL_COLUMNS = DAILY_ANALYSIS_COLUMNS[1:]


@dataclass(frozen=True)
class DailyAnalysisStockFact:
    trade_date: date
    stock_code: str
    stock_name: str
    reason_category: str
    limit_up_reason: str
    continuous_days: int
    open_count: int
    is_final_sealed: bool
    is_20cm: bool
    first_limit_time: Optional[datetime]
    final_seal_time: Optional[datetime]
    open_price: Optional[float]
    close_price: Optional[float]
    high_price: Optional[float]
    low_price: Optional[float]
    pre_close: Optional[float]
    change_pct: Optional[float]
    amount: float
    turnover_rate: Optional[float]


class DailyAnalysisRuleEngine:
    """Build daily review signal cells from recent limit-up candidate facts."""

    def build_daily_result(
        self,
        trade_date: date,
        facts: Iterable[DailyAnalysisStockFact],
    ) -> Dict[str, Dict[str, Any]]:
        all_facts = sorted(facts, key=lambda fact: (fact.trade_date, fact.stock_code))
        history = self._group_history(all_facts)
        today_facts = [fact for fact in all_facts if fact.trade_date == trade_date]
        previous_trade_date = self._previous_trade_date(trade_date, all_facts)

        result = {column: self._cell([]) for column in SIGNAL_COLUMNS}
        result["连板唯一性"] = self._cell(self._build_unique_board_items(today_facts))
        result["反包+趋势+弹钢琴"] = self._cell(self._build_combined_pattern_items(trade_date, today_facts, history))
        result["炸板反包"] = self._cell(self._build_broken_rebound_items(trade_date, today_facts, history, previous_trade_date))
        result["辨识度"] = self._cell(self._build_recognition_items(today_facts))
        result["二波"] = self._cell(self._build_second_wave_items(trade_date, today_facts, history))
        result["20cm"] = self._cell(self._build_20cm_items(trade_date, today_facts, history))
        result["一字套利"] = self._cell(self._build_one_word_arbitrage_items(today_facts))
        result["板块"] = self._cell(self._build_sector_items(today_facts))
        result["负反馈"] = self._cell(self._build_negative_feedback_items(trade_date, today_facts, history))
        return result

    def _group_history(
        self,
        facts: Iterable[DailyAnalysisStockFact],
    ) -> Dict[str, List[DailyAnalysisStockFact]]:
        grouped: Dict[str, List[DailyAnalysisStockFact]] = defaultdict(list)
        for fact in facts:
            grouped[fact.stock_code].append(fact)
        for values in grouped.values():
            values.sort(key=lambda fact: fact.trade_date)
        return grouped

    def _previous_trade_date(
        self,
        trade_date: date,
        facts: Iterable[DailyAnalysisStockFact],
    ) -> Optional[date]:
        trade_dates = sorted({fact.trade_date for fact in facts if fact.trade_date < trade_date})
        return trade_dates[-1] if trade_dates else None

    def _build_unique_board_items(self, today_facts: List[DailyAnalysisStockFact]) -> List[Dict[str, Any]]:
        if not today_facts:
            return []

        max_height = max(max(fact.continuous_days, 1) for fact in today_facts)
        top_facts = [fact for fact in today_facts if max(fact.continuous_days, 1) == max_height]
        tags = ["唯一", f"{max_height}板"] if len(top_facts) == 1 else ["竞争", f"{max_height}板"]
        return [self._stock_item(fact, tags=tags, score=self._recognition_score(fact)) for fact in top_facts]

    def _build_combined_pattern_items(
        self,
        trade_date: date,
        today_facts: List[DailyAnalysisStockFact],
        history: Dict[str, List[DailyAnalysisStockFact]],
    ) -> List[Dict[str, Any]]:
        items = []
        for fact in today_facts:
            stock_history = history.get(fact.stock_code, [])
            tag = self._combined_pattern_tag(fact, stock_history)
            if tag:
                items.append(self._stock_item(fact, tags=[tag], score=self._recognition_score(fact)))
        return self._sort_items(items)

    def _combined_pattern_tag(
        self,
        fact: DailyAnalysisStockFact,
        stock_history: List[DailyAnalysisStockFact],
    ) -> Optional[str]:
        if self._is_piano(fact, stock_history):
            return "弹钢琴"
        if self._is_rebound(fact, stock_history):
            return "反包"
        if self._is_trend(fact, stock_history):
            return "趋势"
        return None

    def _build_broken_rebound_items(
        self,
        trade_date: date,
        today_facts: List[DailyAnalysisStockFact],
        history: Dict[str, List[DailyAnalysisStockFact]],
        previous_trade_date: Optional[date],
    ) -> List[Dict[str, Any]]:
        items = [
            self._stock_item(fact, tags=["炸板反包"], score=self._recognition_score(fact))
            for fact in today_facts
            if self._is_broken_rebound(fact, history.get(fact.stock_code, []), previous_trade_date)
        ]
        return self._sort_items(items)

    def _build_recognition_items(self, today_facts: List[DailyAnalysisStockFact]) -> List[Dict[str, Any]]:
        scored = [
            self._stock_item(
                fact,
                tags=self._recognition_tags(fact),
                score=self._recognition_score(fact),
            )
            for fact in today_facts
        ]
        return self._sort_items(scored)[:5]

    def _build_second_wave_items(
        self,
        trade_date: date,
        today_facts: List[DailyAnalysisStockFact],
        history: Dict[str, List[DailyAnalysisStockFact]],
    ) -> List[Dict[str, Any]]:
        items = [
            self._stock_item(fact, tags=["二波"], score=self._recognition_score(fact))
            for fact in today_facts
            if self._is_second_wave(fact, history.get(fact.stock_code, []))
        ]
        return self._sort_items(items)

    def _build_20cm_items(
        self,
        trade_date: date,
        today_facts: List[DailyAnalysisStockFact],
        history: Dict[str, List[DailyAnalysisStockFact]],
    ) -> List[Dict[str, Any]]:
        items = []
        for fact in today_facts:
            if not fact.is_20cm:
                continue
            stock_history = history.get(fact.stock_code, [])
            tags = []
            if fact.continuous_days >= 2:
                tags.append("连板")
            if self._has_long_upper_shadow(fact):
                tags.append("长上影")
            if self._is_trend(fact, stock_history):
                tags.append("趋势")
            if self._is_rebound(fact, stock_history):
                tags.append("反包")
            items.append(self._stock_item(fact, tags=tags or ["20cm"], score=self._recognition_score(fact)))
        return self._sort_items(items)

    def _build_one_word_arbitrage_items(self, today_facts: List[DailyAnalysisStockFact]) -> List[Dict[str, Any]]:
        items = [
            self._stock_item(fact, tags=["一字"], score=self._recognition_score(fact))
            for fact in today_facts
            if self._is_one_word_arbitrage(fact)
        ]
        return self._sort_items(items)

    def _build_sector_items(self, today_facts: List[DailyAnalysisStockFact]) -> List[Dict[str, Any]]:
        grouped: Dict[str, List[DailyAnalysisStockFact]] = defaultdict(list)
        for fact in today_facts:
            sector = fact.reason_category or "其他"
            grouped[sector].append(fact)

        items = []
        for sector, stocks in grouped.items():
            stocks.sort(key=lambda fact: self._recognition_score(fact), reverse=True)
            continuous_count = sum(1 for fact in stocks if fact.continuous_days >= 2)
            twenty_count = sum(1 for fact in stocks if fact.is_20cm)
            score = len(stocks) * 10 + continuous_count * 4 + twenty_count * 3
            items.append(
                {
                    "label": sector,
                    "tags": [f"{len(stocks)}只", f"连板{continuous_count}只", f"20cm{twenty_count}只"],
                    "content": "、".join(f"{fact.stock_name}({fact.stock_code})" for fact in stocks[:5]),
                    "score": score,
                }
            )
        items.sort(key=lambda item: (-item["score"], item["label"]))
        return items[:5]

    def _build_negative_feedback_items(
        self,
        trade_date: date,
        today_facts: List[DailyAnalysisStockFact],
        history: Dict[str, List[DailyAnalysisStockFact]],
    ) -> List[Dict[str, Any]]:
        items = []
        for fact in today_facts:
            if self._is_negative_feedback(fact, history.get(fact.stock_code, [])):
                items.append(self._stock_item(fact, tags=["负反馈"], score=self._recognition_score(fact)))
        return self._sort_items(items)

    def _is_rebound(self, today: DailyAnalysisStockFact, history: List[DailyAnalysisStockFact]) -> bool:
        previous = [fact for fact in history if fact.trade_date < today.trade_date]
        if not previous or today.close_price is None or not today.is_final_sealed:
            return False
        if max(today.continuous_days, 1) != 1:
            return False

        recent = previous[-3:]
        prior_success = [fact for fact in previous if fact.is_final_sealed]
        if not prior_success:
            return False
        prior_high = max((fact.high_price or fact.close_price or 0) for fact in recent)
        had_disagreement = any((not fact.is_final_sealed) or fact.open_count > 0 for fact in recent)
        return had_disagreement and prior_high > 0 and today.close_price >= prior_high * 0.995

    def _is_trend(self, today: DailyAnalysisStockFact, history: List[DailyAnalysisStockFact]) -> bool:
        recent = [fact for fact in history if fact.trade_date <= today.trade_date and fact.close_price is not None][-4:]
        if len(recent) < 4:
            return False
        closes = [float(fact.close_price or 0) for fact in recent]
        rising_steps = sum(1 for prev, curr in zip(closes, closes[1:]) if curr >= prev * 0.995)
        return rising_steps >= 3 and closes[-1] > closes[0] * 1.08

    def _is_piano(self, today: DailyAnalysisStockFact, history: List[DailyAnalysisStockFact]) -> bool:
        recent = [fact for fact in history if fact.trade_date <= today.trade_date]
        if len(recent) < 3:
            return False
        previous = recent[:-1]
        prior_success_count = sum(1 for fact in previous if fact.is_final_sealed)
        if prior_success_count < 2:
            return False
        had_disagreement = any((not fact.is_final_sealed) or fact.open_count > 0 for fact in previous)
        strong_today = today.is_final_sealed or (today.change_pct is not None and today.change_pct >= 6)
        return had_disagreement and strong_today

    def _is_broken_rebound(
        self,
        today: DailyAnalysisStockFact,
        history: List[DailyAnalysisStockFact],
        previous_trade_date: Optional[date],
    ) -> bool:
        previous = [fact for fact in history if fact.trade_date < today.trade_date]
        if not previous or previous_trade_date is None:
            return False
        if max(today.continuous_days, 1) != 1:
            return False
        if any(fact.is_final_sealed for fact in previous):
            return False
        broken = [
            fact
            for fact in previous
            if fact.trade_date == previous_trade_date and ((not fact.is_final_sealed) or fact.open_count > 0)
        ]
        if not broken:
            return False
        broken_high = max((fact.high_price or fact.close_price or 0) for fact in broken)
        return today.is_final_sealed and today.close_price is not None and today.close_price >= broken_high * 0.995

    def _is_second_wave(self, today: DailyAnalysisStockFact, history: List[DailyAnalysisStockFact]) -> bool:
        previous = [fact for fact in history if fact.trade_date < today.trade_date]
        if len(previous) < 2 or today.close_price is None:
            return False
        had_first_wave = any(fact.continuous_days >= 2 for fact in previous) or len(previous) >= 2
        prior_high = max((fact.high_price or fact.close_price or 0) for fact in previous)
        had_pullback = any(
            (not fact.is_final_sealed)
            or ((fact.close_price or 0) < (fact.high_price or fact.close_price or 0) * 0.98)
            for fact in previous
        )
        return had_first_wave and had_pullback and today.close_price >= prior_high * 0.995

    def _has_long_upper_shadow(self, fact: DailyAnalysisStockFact) -> bool:
        if not fact.high_price or not fact.close_price or not fact.pre_close:
            return False
        high_gain_pct = (fact.high_price - fact.pre_close) / fact.pre_close * 100
        upper_shadow_pct = (fact.high_price - fact.close_price) / fact.pre_close * 100
        return high_gain_pct >= 12 and upper_shadow_pct >= 5 and fact.close_price <= fact.high_price * 0.94

    def _is_one_word_arbitrage(self, fact: DailyAnalysisStockFact) -> bool:
        clock = self._clock(fact.first_limit_time)
        if clock is None or clock > time(9, 30, 30) or fact.open_count > 0:
            return False
        if not fact.open_price or not fact.low_price or not fact.pre_close:
            return False
        limit_ratio = 0.20 if fact.is_20cm else 0.10
        expected_limit_price = fact.pre_close * (1 + limit_ratio)
        return fact.open_price >= expected_limit_price * 0.995 and fact.low_price >= expected_limit_price * 0.995

    def _is_negative_feedback(self, today: DailyAnalysisStockFact, history: List[DailyAnalysisStockFact]) -> bool:
        previous = [fact for fact in history if fact.trade_date < today.trade_date]
        if not previous:
            return False
        yesterday = previous[-1]
        was_core = yesterday.continuous_days >= 2 or self._recognition_score(yesterday) >= 25
        weak_today = (
            (not today.is_final_sealed and today.open_count > 0)
            or (today.change_pct is not None and today.change_pct <= -5)
            or (today.close_price is not None and today.pre_close is not None and today.close_price <= today.pre_close * 0.95)
        )
        return was_core and weak_today

    def _recognition_score(self, fact: DailyAnalysisStockFact) -> float:
        score = fact.continuous_days * 12
        if fact.is_final_sealed:
            score += 8
        if self._clock(fact.first_limit_time) and self._clock(fact.first_limit_time) <= time(9, 35):
            score += 6
        if fact.is_20cm:
            score += 5
        score += min((fact.amount or 0) / 10000, 10)
        score += min((fact.turnover_rate or 0) / 5, 6)
        score -= min(fact.open_count, 8)
        return round(score, 2)

    def _recognition_tags(self, fact: DailyAnalysisStockFact) -> List[str]:
        tags = []
        if fact.continuous_days >= 2:
            tags.append(f"{fact.continuous_days}板")
        if fact.is_20cm:
            tags.append("20cm")
        if self._clock(fact.first_limit_time) and self._clock(fact.first_limit_time) <= time(9, 35):
            tags.append("早盘")
        if fact.open_count > 0:
            tags.append(f"开{fact.open_count}")
        return tags or ["辨识度"]

    def _stock_item(
        self,
        fact: DailyAnalysisStockFact,
        *,
        tags: List[str],
        score: Optional[float] = None,
    ) -> Dict[str, Any]:
        return {
            "stock_code": fact.stock_code,
            "stock_name": fact.stock_name,
            "label": f"{fact.stock_name}({fact.stock_code})",
            "tags": tags,
            "reason": fact.limit_up_reason or fact.reason_category or "",
            "time": self._format_time(fact.first_limit_time),
            "score": score if score is not None else self._recognition_score(fact),
        }

    def _cell(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        return {
            "items": items,
            "content": self._render_content(items),
        }

    def _render_content(self, items: List[Dict[str, Any]]) -> str:
        rendered = []
        for item in items:
            label = item.get("label") or item.get("stock_name") or ""
            tags = item.get("tags") or []
            rendered.append(f"{label}[{','.join(tags)}]" if tags else label)
        return "；".join(rendered)

    def _sort_items(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return sorted(items, key=lambda item: (-(item.get("score") or 0), item.get("time") or "23:59:59", item.get("stock_code") or ""))

    def _format_time(self, value: Optional[datetime]) -> Optional[str]:
        if value is None:
            return None
        return value.strftime("%H:%M:%S")

    def _clock(self, value: Optional[datetime]) -> Optional[time]:
        if value is None:
            return None
        return value.time()


class DailyAnalysisService:
    def __init__(self, rule_engine: Optional[DailyAnalysisRuleEngine] = None):
        self.rule_engine = rule_engine or DailyAnalysisRuleEngine()

    async def get_month(self, db: AsyncSession, month: str) -> Dict[str, Any]:
        records = await self._get_month_records(db, month)
        return {
            "month": month,
            "data": [self.serialize_record(record) for record in records],
        }

    async def rebuild_for_date(self, db: AsyncSession, trade_date: date) -> Dict[str, Any]:
        record = await self.build_for_date(db, trade_date)
        return self.serialize_record(record)

    async def backfill(self, db: AsyncSession, month: Optional[str] = None) -> Dict[str, Any]:
        dates = await self._get_trade_dates(db, month)
        built = []
        for trade_date in dates:
            await self.build_for_date(db, trade_date)
            built.append(trade_date.isoformat())
        return {"built_count": len(built), "trade_dates": built}

    async def update_overrides(
        self,
        db: AsyncSession,
        trade_date: date,
        overrides: Dict[str, Optional[str]],
    ) -> Dict[str, Any]:
        record = await self._get_record(db, trade_date)
        if record is None:
            record = await self.build_for_date(db, trade_date)

        current = dict(record.manual_overrides or {})
        for column, value in overrides.items():
            if column not in SIGNAL_COLUMNS:
                continue
            if value is None or str(value).strip() == "":
                current.pop(column, None)
            else:
                current[column] = str(value)

        record.manual_overrides = current
        record.updated_at = datetime.now()
        await db.commit()
        await db.refresh(record)
        return self.serialize_record(record)

    async def build_for_date(
        self,
        db: AsyncSession,
        trade_date: date,
        calc_version: Optional[int] = None,
    ) -> DailyAnalysisRecord:
        facts = await self.collect_candidate_facts(db, trade_date)
        auto_result = self.rule_engine.build_daily_result(trade_date, facts)
        record = await self._get_record(db, trade_date)
        now = datetime.now()

        if record is None:
            record = DailyAnalysisRecord(
                trade_date=trade_date,
                month=trade_date.strftime("%Y-%m"),
                auto_result=auto_result,
                manual_overrides={},
                calc_version=calc_version or 1,
                data_status="ready",
                generated_at=now,
            )
            db.add(record)
        else:
            record.auto_result = auto_result
            record.month = trade_date.strftime("%Y-%m")
            record.calc_version = calc_version or (record.calc_version or 0) + 1
            record.data_status = "ready"
            record.generated_at = now
            record.updated_at = now

        await db.commit()
        await db.refresh(record)
        return record

    async def collect_candidate_facts(
        self,
        db: AsyncSession,
        trade_date: date,
        window_size: int = 10,
    ) -> List[DailyAnalysisStockFact]:
        dates = await self._get_recent_trade_dates(db, trade_date, window_size)
        if not dates:
            return []

        query = (
            select(LimitUpRecord, Stock)
            .join(Stock, LimitUpRecord.stock_id == Stock.id)
            .where(LimitUpRecord.trade_date.in_(dates))
            .order_by(LimitUpRecord.trade_date, Stock.stock_code)
        )
        result = await db.execute(query)
        rows = result.all()
        return [self._to_fact(record, stock) for record, stock in rows]

    def serialize_record(self, record: DailyAnalysisRecord) -> Dict[str, Any]:
        auto_result = self._normalize_auto_result(record.auto_result or {})
        manual_overrides = dict(record.manual_overrides or {})
        columns = {}
        for column in SIGNAL_COLUMNS:
            auto_cell = dict(auto_result.get(column) or self.rule_engine._cell([]))
            is_manual = column in manual_overrides
            if is_manual:
                auto_cell["content"] = manual_overrides[column]
            auto_cell["is_manual"] = is_manual
            columns[column] = auto_cell

        return {
            "trade_date": record.trade_date.isoformat(),
            "month": record.month,
            "status": record.data_status,
            "calc_version": record.calc_version,
            "generated_at": record.generated_at.isoformat() if record.generated_at else None,
            "updated_at": record.updated_at.isoformat() if record.updated_at else None,
            "auto_result": auto_result,
            "manual_overrides": manual_overrides,
            "columns": columns,
        }

    def _normalize_auto_result(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        normalized = {}
        for column in SIGNAL_COLUMNS:
            cell = dict(raw.get(column) or {})
            items = cell.get("items") or []
            cell["items"] = items
            cell["content"] = cell.get("content") or self.rule_engine._render_content(items)
            normalized[column] = cell
        return normalized

    async def _get_recent_trade_dates(self, db: AsyncSession, trade_date: date, limit: int) -> List[date]:
        query = (
            select(distinct(LimitUpRecord.trade_date))
            .where(LimitUpRecord.trade_date <= trade_date)
            .order_by(LimitUpRecord.trade_date.desc())
            .limit(limit)
        )
        result = await db.execute(query)
        return sorted([row[0] for row in result.all()])

    async def _get_trade_dates(self, db: AsyncSession, month: Optional[str]) -> List[date]:
        query = select(distinct(LimitUpRecord.trade_date)).where(LimitUpRecord.trade_date <= date.today())
        if month:
            start, end = self._month_bounds(month)
            query = query.where(LimitUpRecord.trade_date >= start, LimitUpRecord.trade_date <= end)
        query = query.order_by(LimitUpRecord.trade_date)
        result = await db.execute(query)
        return [row[0] for row in result.all()]

    async def _get_month_records(self, db: AsyncSession, month: str) -> List[DailyAnalysisRecord]:
        query = (
            select(DailyAnalysisRecord)
            .where(DailyAnalysisRecord.month == month, DailyAnalysisRecord.trade_date <= date.today())
            .order_by(DailyAnalysisRecord.trade_date.desc())
        )
        result = await db.execute(query)
        return list(result.scalars().all())

    async def _get_record(self, db: AsyncSession, trade_date: date) -> Optional[DailyAnalysisRecord]:
        result = await db.execute(select(DailyAnalysisRecord).where(DailyAnalysisRecord.trade_date == trade_date))
        return result.scalar_one_or_none()

    def _to_fact(self, record: LimitUpRecord, stock: Stock) -> DailyAnalysisStockFact:
        is_20cm = bool(stock.is_cy or stock.is_kc or stock.stock_code.startswith(("300", "301", "688")))
        close_price = self._to_float(record.close_price) or self._to_float(record.limit_up_price)
        open_price = self._to_float(record.open_price) or close_price
        pre_close = self._estimate_pre_close(record, stock, close_price, is_20cm)
        high_price = self._estimate_high_price(record, close_price, open_price)
        low_price = min(value for value in [open_price, close_price, pre_close] if value is not None) if any(value is not None for value in [open_price, close_price, pre_close]) else None
        change_pct = None
        if close_price is not None and pre_close:
            change_pct = round((close_price - pre_close) / pre_close * 100, 2)

        return DailyAnalysisStockFact(
            trade_date=record.trade_date,
            stock_code=stock.stock_code,
            stock_name=stock.stock_name,
            reason_category=record.reason_category or stock.industry or "其他",
            limit_up_reason=record.limit_up_reason or record.reason_category or stock.industry or "",
            continuous_days=int(record.continuous_limit_up_days or 1),
            open_count=int(record.open_count or 0),
            is_final_sealed=bool(record.is_final_sealed),
            is_20cm=is_20cm,
            first_limit_time=record.first_limit_up_time,
            final_seal_time=record.final_seal_time,
            open_price=open_price,
            close_price=close_price,
            high_price=high_price,
            low_price=low_price,
            pre_close=pre_close,
            change_pct=change_pct,
            amount=float(record.amount or 0),
            turnover_rate=self._to_float(record.turnover_rate),
        )

    def _estimate_pre_close(
        self,
        record: LimitUpRecord,
        stock: Stock,
        close_price: Optional[float],
        is_20cm: bool,
    ) -> Optional[float]:
        if record.limit_up_price:
            ratio = 0.05 if stock.is_st else 0.20 if is_20cm else 0.10
            return round(float(record.limit_up_price) / (1 + ratio), 4)
        if close_price and record.current_status == "sealed":
            ratio = 0.05 if stock.is_st else 0.20 if is_20cm else 0.10
            return round(close_price / (1 + ratio), 4)
        return None

    def _estimate_high_price(
        self,
        record: LimitUpRecord,
        close_price: Optional[float],
        open_price: Optional[float],
    ) -> Optional[float]:
        values = [value for value in [close_price, open_price, self._to_float(record.limit_up_price)] if value is not None]
        if not values:
            return None
        if record.amplitude and close_price:
            return max(max(values), close_price * (1 + min(float(record.amplitude), 30) / 100))
        return max(values)

    def _month_bounds(self, month: str) -> tuple[date, date]:
        year, month_num = [int(part) for part in month.split("-")]
        start = date(year, month_num, 1)
        if month_num == 12:
            end = date(year + 1, 1, 1)
        else:
            end = date(year, month_num + 1, 1)
        return start, date.fromordinal(end.toordinal() - 1)

    def _to_float(self, value: Any) -> Optional[float]:
        if value in (None, ""):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None


daily_analysis_service = DailyAnalysisService()
