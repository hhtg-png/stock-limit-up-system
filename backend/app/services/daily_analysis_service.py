from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time
import re
from typing import Any, Dict, Iterable, List, Optional, Set

from sqlalchemy import distinct, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.limit_up import LimitUpRecord
from app.models.market_review import DailyAnalysisRecord, MarketReviewStockDaily
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

    LOW_SIGNAL_SECTOR_THEMES = {
        "央企",
        "央企背景",
        "国企",
        "国资",
        "地方国资",
        "业绩",
        "业绩增长",
        "业绩大增",
        "一季报增长",
        "Q1业绩大增",
        "订单放量",
        "现金分红",
        "摘帽",
        "减亏",
    }

    def build_daily_result(
        self,
        trade_date: date,
        facts: Iterable[DailyAnalysisStockFact],
        negative_feedback_facts: Optional[Iterable[DailyAnalysisStockFact]] = None,
    ) -> Dict[str, Dict[str, Any]]:
        all_facts = sorted(facts, key=lambda fact: (fact.trade_date, fact.stock_code))
        history = self._group_history(all_facts)
        today_facts = [fact for fact in all_facts if fact.trade_date == trade_date]
        negative_today_facts = (
            [fact for fact in negative_feedback_facts if fact.trade_date == trade_date]
            if negative_feedback_facts is not None
            else today_facts
        )
        trade_dates = sorted({fact.trade_date for fact in all_facts})
        previous_trade_date = self._previous_trade_date(trade_date, trade_dates)

        result = {column: self._cell([]) for column in SIGNAL_COLUMNS}
        result["连板唯一性"] = self._cell(self._build_unique_board_items(today_facts))
        result["反包+趋势+弹钢琴"] = self._cell(self._build_combined_pattern_items(trade_date, today_facts, history, trade_dates))
        result["炸板反包"] = self._cell(self._build_broken_rebound_items(trade_date, today_facts, history, previous_trade_date))
        result["辨识度"] = self._cell(self._build_recognition_items(today_facts))
        result["二波"] = self._cell(self._build_second_wave_items(trade_date, today_facts, history, trade_dates))
        result["20cm"] = self._cell(self._build_20cm_items(trade_date, today_facts, history, trade_dates))
        result["一字套利"] = self._cell(self._build_one_word_arbitrage_items(today_facts))
        result["板块"] = self._cell(self._build_sector_items(today_facts))
        result["负反馈"] = self._cell(self._build_negative_feedback_items(trade_date, negative_today_facts, history))
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
        trade_dates: Iterable[date],
    ) -> Optional[date]:
        previous_dates = [value for value in trade_dates if value < trade_date]
        return previous_dates[-1] if previous_dates else None

    def _trade_gap_days(
        self,
        start_date: date,
        end_date: date,
        trade_dates: Iterable[date],
    ) -> int:
        return sum(1 for value in trade_dates if start_date < value < end_date)

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
        trade_dates: List[date],
    ) -> List[Dict[str, Any]]:
        items = []
        for fact in today_facts:
            stock_history = history.get(fact.stock_code, [])
            tag = self._combined_pattern_tag(fact, stock_history, trade_dates)
            if tag:
                items.append(self._stock_item(fact, tags=[tag], score=self._recognition_score(fact)))
        return self._sort_items(items)

    def _combined_pattern_tag(
        self,
        fact: DailyAnalysisStockFact,
        stock_history: List[DailyAnalysisStockFact],
        trade_dates: List[date],
    ) -> Optional[str]:
        if self._is_piano(fact, stock_history):
            return "弹钢琴"
        if self._is_rebound(fact, stock_history, trade_dates):
            return "反包"
        if self._is_trend(fact, stock_history, trade_dates):
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
        trade_dates: List[date],
    ) -> List[Dict[str, Any]]:
        items = [
            self._stock_item(fact, tags=["二波"], score=self._recognition_score(fact))
            for fact in today_facts
            if self._is_second_wave(fact, history.get(fact.stock_code, []), trade_dates)
        ]
        return self._sort_items(items)

    def _build_20cm_items(
        self,
        trade_date: date,
        today_facts: List[DailyAnalysisStockFact],
        history: Dict[str, List[DailyAnalysisStockFact]],
        trade_dates: List[date],
    ) -> List[Dict[str, Any]]:
        items = []
        twenty_cm_height_counts = Counter(
            max(fact.continuous_days, 1)
            for fact in today_facts
            if fact.is_20cm and fact.is_final_sealed and max(fact.continuous_days, 1) >= 2
        )
        for fact in today_facts:
            if not fact.is_20cm:
                continue
            stock_history = history.get(fact.stock_code, [])
            tags = []
            height = max(fact.continuous_days, 1)
            if fact.is_final_sealed and height >= 2 and twenty_cm_height_counts.get(height) == 1:
                tags.extend(["唯一高度", f"{height}板"])
            elif self._has_long_upper_shadow(fact):
                tags.append("长上影")
            elif self._is_recent_20cm_limit_new_high(fact, stock_history, trade_dates):
                tags.append("5日涨停新高")
            if tags:
                items.append(self._stock_item(fact, tags=tags, score=self._recognition_score(fact)))
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
            for sector in self._sector_themes(fact):
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
        return items[:8]

    def _sector_themes(self, fact: DailyAnalysisStockFact) -> List[str]:
        themes = []
        for raw_theme in re.split(r"[+＋/／、,，;；|｜]", fact.limit_up_reason or ""):
            theme = self._normalize_sector_theme(raw_theme)
            if not theme or theme in themes:
                continue
            if theme in self.LOW_SIGNAL_SECTOR_THEMES:
                continue
            themes.append(theme)
            if len(themes) >= 2:
                break

        if themes:
            return themes
        return [fact.reason_category or "其他"]

    def _normalize_sector_theme(self, raw_theme: str) -> str:
        theme = re.sub(r"\s+", "", raw_theme or "")
        theme = theme.strip("：:（）()[]【】")
        if not theme:
            return ""
        theme = re.sub(r"概念$", "", theme)
        theme = re.sub(r"(板块|方向|业务)$", "", theme)
        theme = theme.replace("AI算力", "算力")
        if "人形机器人" in theme:
            return "人形机器人"
        if "光模块" in theme:
            return "光模块"
        if "Token工厂" in theme or "token工厂" in theme:
            return "Token工厂"
        if "算力租赁" in theme:
            return "算力租赁"
        if "数据中心" in theme:
            return "数据中心"
        if "固态电池" in theme:
            return "固态电池"
        if "储能" in theme:
            return "储能"
        if "液冷" in theme:
            return "液冷"
        if "商业航天" in theme:
            return "商业航天"
        if "房地产" in theme:
            return "房地产"
        if len(theme) > 12:
            return ""
        return theme

    def _build_negative_feedback_items(
        self,
        trade_date: date,
        today_facts: List[DailyAnalysisStockFact],
        history: Dict[str, List[DailyAnalysisStockFact]],
    ) -> List[Dict[str, Any]]:
        items = []
        for fact in today_facts:
            if self._is_negative_feedback(fact, history.get(fact.stock_code, [])):
                items.append(
                    self._stock_item(
                        fact,
                        tags=["跌停"],
                        score=self._negative_feedback_score(fact, history.get(fact.stock_code, [])),
                    )
                )
        return self._sort_items(items)

    def _is_rebound(
        self,
        today: DailyAnalysisStockFact,
        history: List[DailyAnalysisStockFact],
        trade_dates: List[date],
    ) -> bool:
        previous = [fact for fact in history if fact.trade_date < today.trade_date]
        if not previous or today.close_price is None or not today.is_final_sealed:
            return False
        if max(today.continuous_days, 1) != 1:
            return False

        prior_success = [fact for fact in previous if fact.is_final_sealed]
        if not prior_success:
            return False

        last_success = prior_success[-1]
        break_days = self._trade_gap_days(last_success.trade_date, today.trade_date, trade_dates)
        if break_days != 1:
            return False

        between_facts = [
            fact
            for fact in previous
            if last_success.trade_date <= fact.trade_date < today.trade_date
        ]
        if any(fact.trade_date > last_success.trade_date and fact.is_final_sealed for fact in between_facts):
            return False

        prior_high = max((fact.high_price or fact.close_price or 0) for fact in between_facts)
        return prior_high > 0 and today.close_price >= prior_high * 0.995

    def _is_trend(
        self,
        today: DailyAnalysisStockFact,
        history: List[DailyAnalysisStockFact],
        trade_dates: Optional[List[date]] = None,
    ) -> bool:
        if max(today.continuous_days, 1) >= 2:
            return False
        if trade_dates is not None and self._is_second_wave(today, history, trade_dates):
            return False
        recent = [fact for fact in history if fact.trade_date <= today.trade_date and fact.close_price is not None][-4:]
        if len(recent) < 4:
            return False
        closes = [float(fact.close_price or 0) for fact in recent]
        rising_steps = sum(1 for prev, curr in zip(closes, closes[1:]) if curr >= prev * 0.995)
        return rising_steps >= 3 and closes[-1] > closes[0] * 1.08

    def _is_piano(self, today: DailyAnalysisStockFact, history: List[DailyAnalysisStockFact]) -> bool:
        if max(today.continuous_days, 1) != 1:
            return False
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

    def _is_second_wave(
        self,
        today: DailyAnalysisStockFact,
        history: List[DailyAnalysisStockFact],
        trade_dates: List[date],
    ) -> bool:
        previous = [fact for fact in history if fact.trade_date < today.trade_date]
        if not previous or today.close_price is None:
            return False

        if self._is_ongoing_second_wave(today, previous, trade_dates):
            return True

        major_wave_facts = [
            fact
            for fact in previous
            if fact.is_final_sealed and fact.continuous_days >= 4
        ]
        if not major_wave_facts:
            return False

        first_wave_high = major_wave_facts[-1]
        break_days = self._trade_gap_days(first_wave_high.trade_date, today.trade_date, trade_dates)
        if break_days < 2 or break_days > 8:
            return False
        if any(
            fact.trade_date > first_wave_high.trade_date and fact.is_final_sealed
            for fact in previous
        ):
            return False

        prior_high = max((fact.high_price or fact.close_price or 0) for fact in previous)
        strong_today = today.is_final_sealed or today.close_price >= prior_high * 0.995
        return strong_today and today.close_price >= prior_high * 0.995

    def _is_ongoing_second_wave(
        self,
        today: DailyAnalysisStockFact,
        previous: List[DailyAnalysisStockFact],
        trade_dates: List[date],
    ) -> bool:
        if not today.is_final_sealed or today.continuous_days < 4:
            return False

        successes = [
            fact
            for fact in [*previous, today]
            if fact.is_final_sealed
        ]
        if len(successes) < 3:
            return False

        current_wave_start_index = 0
        current_wave_started_by_height_reset = False
        for index in range(1, len(successes)):
            gap = self._trade_gap_days(successes[index - 1].trade_date, successes[index].trade_date, trade_dates)
            height_reset = successes[index].continuous_days < successes[index - 1].continuous_days
            if gap >= 2 or height_reset:
                current_wave_start_index = index
                current_wave_started_by_height_reset = height_reset

        if current_wave_start_index == 0:
            return False

        current_wave = successes[current_wave_start_index:]
        previous_wave = successes[:current_wave_start_index]
        if not previous_wave:
            return False

        break_days = self._trade_gap_days(previous_wave[-1].trade_date, current_wave[0].trade_date, trade_dates)
        if break_days > 8:
            return False
        if break_days < 2 and not current_wave_started_by_height_reset:
            return False

        current_wave_height = max(fact.continuous_days for fact in current_wave)
        return current_wave_height >= 4

    def _has_long_upper_shadow(self, fact: DailyAnalysisStockFact) -> bool:
        if not fact.high_price or not fact.close_price or not fact.pre_close:
            return False
        high_gain_pct = (fact.high_price - fact.pre_close) / fact.pre_close * 100
        upper_shadow_pct = (fact.high_price - fact.close_price) / fact.pre_close * 100
        return high_gain_pct >= 12 and upper_shadow_pct >= 5 and fact.close_price <= fact.high_price * 0.94

    def _is_recent_20cm_limit_new_high(
        self,
        today: DailyAnalysisStockFact,
        history: List[DailyAnalysisStockFact],
        trade_dates: List[date],
    ) -> bool:
        if not today.is_20cm or not today.is_final_sealed or today.close_price is None:
            return False

        previous_dates = [value for value in trade_dates if value < today.trade_date]
        recent_dates = set(previous_dates[-5:])
        if not recent_dates:
            return False

        recent_previous = [
            fact
            for fact in history
            if fact.trade_date in recent_dates and fact.trade_date < today.trade_date
        ]
        if not any(fact.is_final_sealed for fact in recent_previous):
            return False

        prior_high = max((fact.high_price or fact.close_price or 0) for fact in recent_previous)
        return prior_high > 0 and today.close_price > prior_high

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
        return self._is_popular_stock(yesterday) and self._is_limit_down(today)

    def _is_popular_stock(self, fact: DailyAnalysisStockFact) -> bool:
        return fact.continuous_days >= 2 or self._recognition_score(fact) >= 25

    def _is_limit_down(self, fact: DailyAnalysisStockFact) -> bool:
        if fact.change_pct is not None:
            return fact.change_pct <= self._limit_down_change_threshold(fact)
        if fact.close_price is None or not fact.pre_close:
            return False
        ratio = abs(self._limit_down_change_threshold(fact)) / 100
        return fact.close_price <= fact.pre_close * (1 - ratio) * 1.001

    def _limit_down_change_threshold(self, fact: DailyAnalysisStockFact) -> float:
        if "ST" in fact.stock_name.upper():
            return -4.8
        if fact.stock_code.startswith(("8", "920")):
            return -29.5
        if fact.is_20cm or fact.stock_code.startswith(("300", "301", "688")):
            return -19.5
        return -9.5

    def _negative_feedback_score(self, today: DailyAnalysisStockFact, history: List[DailyAnalysisStockFact]) -> float:
        previous_scores = [
            self._recognition_score(fact)
            for fact in history
            if fact.trade_date < today.trade_date
        ]
        return max(previous_scores or [self._recognition_score(today)])

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
        self._cn_trading_calendar: Optional[Set[date]] = None
        self._cn_trading_calendar_unavailable = False

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
        removed = await self._delete_non_trading_records(db, month)
        if removed:
            await db.commit()
        built = []
        for trade_date in dates:
            await self.build_for_date(db, trade_date)
            built.append(trade_date.isoformat())
        return {
            "built_count": len(built),
            "trade_dates": built,
            "removed_non_trading_count": removed,
        }

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
        negative_feedback_facts = await self.collect_negative_feedback_facts(db, trade_date)
        auto_result = self.rule_engine.build_daily_result(
            trade_date,
            facts,
            negative_feedback_facts=negative_feedback_facts or None,
        )
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

    async def collect_negative_feedback_facts(
        self,
        db: AsyncSession,
        trade_date: date,
    ) -> List[DailyAnalysisStockFact]:
        query = (
            select(MarketReviewStockDaily, Stock)
            .join(Stock, MarketReviewStockDaily.stock_id == Stock.id)
            .where(MarketReviewStockDaily.trade_date == trade_date)
            .order_by(MarketReviewStockDaily.stock_code)
        )
        result = await db.execute(query)
        rows = result.all()
        return [self._to_fact_from_review_stock(row, stock) for row, stock in rows]

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
        return sorted(self._filter_trading_dates([row[0] for row in result.all()]))

    async def _get_trade_dates(self, db: AsyncSession, month: Optional[str]) -> List[date]:
        query = select(distinct(LimitUpRecord.trade_date)).where(LimitUpRecord.trade_date <= date.today())
        if month:
            start, end = self._month_bounds(month)
            query = query.where(LimitUpRecord.trade_date >= start, LimitUpRecord.trade_date <= end)
        query = query.order_by(LimitUpRecord.trade_date)
        result = await db.execute(query)
        return self._filter_trading_dates([row[0] for row in result.all()])

    async def _get_month_records(self, db: AsyncSession, month: str) -> List[DailyAnalysisRecord]:
        query = (
            select(DailyAnalysisRecord)
            .where(DailyAnalysisRecord.month == month, DailyAnalysisRecord.trade_date <= date.today())
            .order_by(DailyAnalysisRecord.trade_date.desc())
        )
        result = await db.execute(query)
        records = list(result.scalars().all())
        trading_dates = set(self._filter_trading_dates([record.trade_date for record in records]))
        return [record for record in records if record.trade_date in trading_dates]

    async def _delete_non_trading_records(self, db: AsyncSession, month: Optional[str]) -> int:
        query = select(DailyAnalysisRecord).where(DailyAnalysisRecord.trade_date <= date.today())
        if month:
            start, end = self._month_bounds(month)
            query = query.where(DailyAnalysisRecord.trade_date >= start, DailyAnalysisRecord.trade_date <= end)
        result = await db.execute(query)
        records = list(result.scalars().all())
        trading_dates = set(self._filter_trading_dates([record.trade_date for record in records]))
        removed = 0
        for record in records:
            if record.trade_date not in trading_dates:
                await db.delete(record)
                removed += 1
        return removed

    def _filter_trading_dates(self, dates: List[date]) -> List[date]:
        ordered_dates = list(dict.fromkeys(dates))
        if not ordered_dates:
            return []

        trading_dates = self._load_cn_trading_date_set(min(ordered_dates), max(ordered_dates))
        if trading_dates is None:
            return [value for value in ordered_dates if value.weekday() < 5]
        return [value for value in ordered_dates if value in trading_dates]

    def _load_cn_trading_date_set(self, start: date, end: date) -> Optional[Set[date]]:
        if self._cn_trading_calendar is not None:
            return {value for value in self._cn_trading_calendar if start <= value <= end}
        if self._cn_trading_calendar_unavailable:
            return None

        try:
            from app.data_collectors.scheduler import _get_cn_trading_dates

            calendar_end = max(end, date.today())
            self._cn_trading_calendar = set(_get_cn_trading_dates(date(1990, 1, 1), calendar_end))
        except Exception:
            self._cn_trading_calendar_unavailable = True
            return None

        return {value for value in self._cn_trading_calendar if start <= value <= end}

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

    def _to_fact_from_review_stock(self, row: MarketReviewStockDaily, stock: Stock) -> DailyAnalysisStockFact:
        is_20cm = bool(
            stock.is_cy
            or stock.is_kc
            or row.board_type in {"gem", "star"}
            or row.stock_code.startswith(("300", "301", "688"))
        )
        return DailyAnalysisStockFact(
            trade_date=row.trade_date,
            stock_code=row.stock_code,
            stock_name=row.stock_name,
            reason_category=stock.industry or "其他",
            limit_up_reason=row.limit_up_reason or stock.industry or "",
            continuous_days=int(row.today_continuous_days or 0),
            open_count=int(row.open_count or 0),
            is_final_sealed=bool(row.today_sealed_close),
            is_20cm=is_20cm,
            first_limit_time=self._combine_date_time(row.trade_date, row.first_limit_time),
            final_seal_time=self._combine_date_time(row.trade_date, row.final_seal_time),
            open_price=None,
            close_price=self._to_float(row.close_price),
            high_price=self._to_float(row.close_price),
            low_price=self._to_float(row.close_price),
            pre_close=self._to_float(row.pre_close),
            change_pct=self._to_float(row.change_pct),
            amount=float(row.amount or 0),
            turnover_rate=self._to_float(row.turnover_rate),
        )

    def _combine_date_time(self, trade_date: date, value: Optional[time]) -> Optional[datetime]:
        if value is None:
            return None
        return datetime.combine(trade_date, value)

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
