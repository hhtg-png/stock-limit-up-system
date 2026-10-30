"""Shared, database-only recent-session facts for deterministic group rules."""
from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import date, datetime, time, timedelta

from sqlalchemy import select

from app.database import async_session_maker
from app.models.market_review import MarketReviewDailyMetric, MarketReviewStockDaily
from app.services.dynamic_group_rules import HISTORICAL_FIELDS, iter_rule_leaves
from app.utils.time_utils import CN_TZ, now_cn


def _calendar(start, end):
    from app.data_collectors.scheduler import _get_cn_trading_dates
    return _get_cn_trading_dates(start, end)


class DynamicGroupFeatureService:
    def __init__(self, *, session_factory=async_session_maker, calendar_loader=_calendar, now_provider=now_cn):
        self.session_factory = session_factory
        self.calendar_loader = calendar_loader
        self.now_provider = now_provider
        self._calendar_key = None
        self._calendar_dates = []
        self._calendar_lock = asyncio.Lock()

    @staticmethod
    def _windows(rulesets):
        requests = set()
        for definition in rulesets:
            rules = definition.get("rules", []) if isinstance(definition, dict) else definition
            for leaf in iter_rule_leaves(rules):
                if leaf["field"] in HISTORICAL_FIELDS:
                    requests.add((leaf["field"], leaf["days"]))
            if isinstance(definition, dict):
                ranking = (definition.get("strategy") or {}).get("ranking")
                if ranking and ranking["field"] in HISTORICAL_FIELDS:
                    requests.add((ranking["field"], ranking["days"]))
        return requests

    def _now(self):
        value = self.now_provider()
        return value.astimezone(CN_TZ).replace(tzinfo=None) if value.tzinfo else value

    async def _sessions(self, day, observed):
        now = self._now()
        # Final daily facts are built after 15:05; a 15:00 quote alone is not
        # proof that today's final archive is available during the session.
        completed_today = day < now.date() or (day == now.date() and now.time() >= time(15, 5)
                                              and observed.time() >= time(15))
        end = day if completed_today else day - timedelta(days=1)
        async with self._calendar_lock:
            if self._calendar_key != end:
                values = await asyncio.wait_for(asyncio.to_thread(
                    self.calendar_loader, end - timedelta(days=180), end), timeout=6)
                self._calendar_dates = sorted({item for item in values if isinstance(item, date) and item <= end})
                self._calendar_key = end
            return list(self._calendar_dates)

    @staticmethod
    def _verified(day, metric, facts):
        if (not facts or metric is None or metric.calc_version < 1 or metric.source_status not in {"primary", "partial"}
                or metric.updated_at is None or metric.updated_at < datetime.combine(day, time(15, 5))
                or sum(row.today_touched_limit_up is True for row in facts) != metric.limit_up_count):
            return False
        for row in facts:
            if row.data_quality_flag != "ok" or row.today_sealed_close and not row.today_touched_limit_up:
                return False
            if row.today_sealed_close and (type(row.today_continuous_days) is not int or row.today_continuous_days < 1):
                return False
        return True

    async def enrich_snapshot(self, snapshot, rulesets):
        """Enrich every requested window with two shared queries, never per-stock HTTP."""
        requests = self._windows(rulesets)
        if not requests:
            return snapshot
        result = deepcopy(snapshot)
        coverage, windows = {}, {days for _, days in requests}
        facts_by_day, verified = {}, {}
        calendar_error = False
        try:
            day = date.fromisoformat(str(snapshot["trade_date"])[:10])
            observed = datetime.fromisoformat(str(snapshot["source_time"]).replace("Z", "+00:00"))
            if observed.tzinfo:
                observed = observed.astimezone(CN_TZ).replace(tzinfo=None)
            sessions = await self._sessions(day, observed)
            needed = sessions[-max(windows):]
            async with self.session_factory() as session:
                metrics = {row.trade_date: row for row in (await session.scalars(select(MarketReviewDailyMetric)
                    .where(MarketReviewDailyMetric.trade_date.in_(needed)))).all()}
                facts = (await session.scalars(select(MarketReviewStockDaily)
                    .where(MarketReviewStockDaily.trade_date.in_(needed)))).all()
            for row in facts:
                facts_by_day.setdefault(row.trade_date, []).append(row)
            verified = {item: self._verified(item, metrics.get(item), facts_by_day.get(item, [])) for item in needed}
        except (asyncio.CancelledError, KeyboardInterrupt):
            raise
        except Exception:
            # Unavailable history must not disable supported live quote rules.
            sessions, calendar_error = [], True
        for days in sorted(windows):
            selected = sessions[-days:]
            complete = len(selected) == days and all(verified.get(item, False) for item in selected)
            coverage[str(days)] = dict(status="ready" if complete else "partial",
                expected_sessions=days, covered_sessions=sum(verified.get(item, False) for item in selected),
                sessions=[item.isoformat() for item in selected], completed_only=True)
        aggregates = {}
        for field, days in sorted(requests):
            if coverage[str(days)]["status"] != "ready":
                aggregates[field, days] = None
                continue
            values = {}
            for item in sessions[-days:]:
                for fact in facts_by_day.get(item, []):
                    code = fact.stock_code
                    if field == "recent_limit_up_count":
                        values[code] = values.get(code, 0) + int(fact.today_sealed_close)
                    elif field == "recent_touched_limit_up_count":
                        values[code] = values.get(code, 0) + int(fact.today_touched_limit_up)
                    elif fact.today_sealed_close:
                        values[code] = max(values.get(code, 0), fact.today_continuous_days)
            aggregates[field, days] = values
        for row in result.get("rows", []):
            # Replace any earlier feature enrichment; stale cached values can
            # never masquerade as a newly verified history window.
            features = {}
            for (field, days), values in aggregates.items():
                features.setdefault(field, {})[str(days)] = values.get(row.get("stock_code"), 0) if values is not None else None
            row["historical_features"] = features
        result["historical_coverage"] = coverage
        warnings = list(result.get("warnings") or [])
        if any(item["status"] != "ready" for item in coverage.values()):
            warnings.append("近期交易日历史未完整核验，相关条件保留未知；已有成员不会因缺数退出")
        if calendar_error:
            warnings.append("历史交易日历或归档暂不可用，后台会自动重试")
        result["warnings"] = warnings
        return result


dynamic_group_features_service = DynamicGroupFeatureService()
