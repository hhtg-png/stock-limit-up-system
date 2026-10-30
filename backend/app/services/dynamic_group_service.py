"""Transactional selection archives; quote fetching happens only in background jobs."""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
from copy import deepcopy
from datetime import date, datetime, time

from fastapi import HTTPException
from sqlalchemy import func, or_, select, update
from sqlalchemy.exc import IntegrityError

from app.database import async_session_maker
from app.models.dynamic_group import (
    DynamicGroup, DynamicGroupEvent, DynamicGroupIntradayPoint, DynamicGroupMember,
    DynamicGroupSampleClaim, DynamicGroupSelection, DynamicGroupSelectionItem, DynamicGroupVersion,
)
from app.schemas.dynamic_group import DynamicGroupDefinition
from app.utils.time_utils import CN_TZ, now_cn

logger = logging.getLogger(__name__)
DEFINITION_KEYS = tuple(DynamicGroupDefinition.model_fields)
RULE_KEYS = {"rules", "match_mode", "universe", "membership_mode"}


def _date(value):
    if isinstance(value, datetime):
        return value.date()
    return value if isinstance(value, date) else date.fromisoformat(str(value)[:10])


def _datetime(value):
    if isinstance(value, datetime):
        result = value
    else:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if result.tzinfo:
        result = result.astimezone(CN_TZ).replace(tzinfo=None)
    return result


def _json(value):
    if isinstance(value, dict):
        return {str(k): _json(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json(v) for v in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


class DynamicGroupService:
    def __init__(self, *, session_factory=None, source=None, history=None, features=None, now=None):
        self.session_factory = session_factory or async_session_maker
        self._source = source
        self._history = history
        self._features = features
        self.now = now or (lambda: now_cn().replace(tzinfo=None))
        self._lock = asyncio.Lock()
        self._task = None
        self._pending = False
        self._closed = False

    @property
    def source(self):
        if self._source is None:
            from app.services.dynamic_group_source_service import dynamic_group_source_service
            self._source = dynamic_group_source_service
        return self._source

    @property
    def features(self):
        if self._features is None:
            from app.services.dynamic_group_features_service import DynamicGroupFeatureService
            self._features = DynamicGroupFeatureService(session_factory=self.session_factory, now_provider=self.now)
        return self._features

    @property
    def history(self):
        if self._history is None:
            from app.services.dynamic_group_history_service import dynamic_group_history_service
            self._history = dynamic_group_history_service
        return self._history

    def request_refresh(self, id=None):
        """Coalesce writes and clicks into one shared asynchronous source scan."""
        if self._closed:
            return
        self._pending = True
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._refresh_worker())

    async def _refresh_worker(self):
        while self._pending and not self._closed:
            self._pending = False
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Dynamic group refresh failed")

    async def close(self):
        self._closed = True
        if self._task and not self._task.done():
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    async def tick(self):
        async with self.session_factory() as session:
            count = await session.scalar(select(func.count()).select_from(DynamicGroup).where(
                DynamicGroup.enabled.is_(True), DynamicGroup.archived.is_(False)))
        if count:
            try:
                snapshot = await self.source.get_snapshot()
            except Exception as exc:
                logger.warning("Dynamic group source unavailable: %s", exc)
                snapshot = {"status": "error", "warnings": ["行情服务暂时不可用，请稍后重试"]}
            await self.evaluate_snapshot(snapshot)

    @staticmethod
    def definition(group):
        return {key: deepcopy(getattr(group, key)) for key in DEFINITION_KEYS}

    @classmethod
    def group_dict(cls, group):
        return _json(dict(cls.definition(group), **{key: getattr(group, key) for key in (
            "id", "version", "archived", "member_count", "latest_selection_id", "data_status", "trade_date",
            "updated_at", "created_at", "last_evaluated_at", "last_error")}))

    async def _require_group(self, session, group_id, lock=False):
        if lock:
            # SQLite ignores FOR UPDATE. Claim its write lock before reading so
            # separate processes cannot apply old state after a newer snapshot.
            await session.execute(update(DynamicGroup).where(DynamicGroup.id == group_id).values(version=DynamicGroup.version))
        statement = select(DynamicGroup).where(DynamicGroup.id == group_id)
        if lock:
            statement = statement.with_for_update()
        group = await session.scalar(statement)
        if not group:
            raise HTTPException(404, "动态板块不存在")
        return group

    async def create_group(self, definition):
        payload = DynamicGroupDefinition.model_validate(definition).model_dump()
        async with self._lock, self.session_factory() as session, session.begin():
            group = DynamicGroup(**payload, created_at=self.now(), updated_at=self.now())
            session.add(group)
            await session.flush()
            session.add(DynamicGroupVersion(group_id=group.id, version=1, definition=payload, created_at=self.now()))
            return self.group_dict(group)

    async def get_group(self, group_id):
        async with self.session_factory() as session:
            return self.group_dict(await self._require_group(session, group_id))

    async def list_groups(self, *, q="", folder=None, state="all", sort="pinned", offset=0, limit=50):
        limit, offset = min(100, max(1, limit)), max(0, offset)
        async with self.session_factory() as session:
            filters = []
            if q:
                escaped = q.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
                filters.append(or_(DynamicGroup.name.ilike(f"%{escaped}%", escape="\\"),
                                   DynamicGroup.description.ilike(f"%{escaped}%", escape="\\")))
            if folder is not None:
                filters.append(DynamicGroup.folder == folder)
            if state == "archived":
                filters.append(DynamicGroup.archived.is_(True))
            else:
                filters.append(DynamicGroup.archived.is_(False))
                if state in ("active", "paused"):
                    filters.append(DynamicGroup.enabled.is_(state == "active"))
            total = await session.scalar(select(func.count()).select_from(DynamicGroup).where(*filters))
            order = [DynamicGroup.name, DynamicGroup.id] if sort == "name" else [DynamicGroup.updated_at.desc(), DynamicGroup.id.desc()]
            if sort == "pinned":
                order.insert(0, DynamicGroup.pinned.desc())
            groups = (await session.scalars(select(DynamicGroup).where(*filters).order_by(*order).offset(offset).limit(limit))).all()
            folders = list((await session.scalars(select(DynamicGroup.folder).where(DynamicGroup.archived.is_(False)).distinct().order_by(DynamicGroup.folder))).all())
            stats_rows = (await session.execute(select(DynamicGroup.archived, DynamicGroup.enabled, func.count()).group_by(DynamicGroup.archived, DynamicGroup.enabled))).all()
            stats = {"total": 0, "active": 0, "paused": 0, "archived": 0}
            for archived, enabled, count in stats_rows:
                stats["total"] += count
                stats["archived" if archived else "active" if enabled else "paused"] += count
            return {"items": [self.group_dict(g) for g in groups], "total": total, "folders": folders, "stats": stats}

    async def update_group(self, group_id, changes):
        async with self._lock, self.session_factory() as session, session.begin():
            group = await self._require_group(session, group_id, lock=True)
            if group.archived:
                raise HTTPException(409, "已归档板块不可修改")
            payload = DynamicGroupDefinition.model_validate(dict(self.definition(group), **changes)).model_dump()
            changed_rules = any(payload[key] != getattr(group, key) for key in RULE_KEYS)
            changed_rules = changed_rules or ((payload.get("strategy") or {}).get("ranking") !=
                                              (group.strategy or {}).get("ranking"))
            for key, value in payload.items():
                setattr(group, key, value)
            group.updated_at = self.now()
            if changed_rules:
                old_version = group.version
                members = (await session.scalars(select(DynamicGroupMember).where(
                    DynamicGroupMember.group_id == group.id))).all()
                for member in members:
                    session.add(DynamicGroupEvent(group_id=group.id, version=old_version,
                        stock_code=member.stock_code, kind="rule_change_exit", trade_date=group.trade_date,
                        observed_at=group.updated_at, payload=dict(member.payload, next_version=old_version + 1)))
                    await session.delete(member)
                group.version += 1
                group.member_count = 0
                group.latest_selection_id = None
                group.last_evaluated_at = None
                group.last_error = None
                group.warnings = []
                group.index_state = {}
                group.data_status = "pending"
                session.add(DynamicGroupVersion(group_id=group.id, version=group.version,
                                               definition=payload, created_at=self.now()))
            return self.group_dict(group)

    async def archive_group(self, group_id):
        async with self._lock, self.session_factory() as session, session.begin():
            group = await self._require_group(session, group_id, lock=True)
            group.archived, group.enabled, group.updated_at = True, False, self.now()
            return self.group_dict(group)

    async def get_current(self, group_id, *, q="", offset=0, limit=50):
        from app.services.dynamic_group_queries import member_page
        async with self.session_factory() as session:
            group = await self._require_group(session, group_id)
            page = await member_page(session, group, q=q, offset=offset, limit=limit)
            return _json(dict(group=self.group_dict(group), **page,
                              trade_date=group.trade_date, previous_trade_date=group.previous_trade_date,
                              source_time=group.source_time, status=group.data_status, warnings=group.warnings,
                              selection_id=group.latest_selection_id))

    async def get_dates(self, group_id):
        async with self.session_factory() as session:
            await self._require_group(session, group_id)
            dates = (await session.scalars(select(DynamicGroupSelection.trade_date).where(
                DynamicGroupSelection.group_id == group_id).distinct().order_by(DynamicGroupSelection.trade_date.desc()).limit(2000))).all()
            return {"dates": [d.isoformat() for d in dates]}

    @staticmethod
    def selection_dict(batch):
        return _json({key: getattr(batch, key) for key in ("id", "trade_date", "selected_at", "member_count", "version", "kind", "data_status", "warnings")})

    async def list_selections(self, group_id, *, trade_date=None, offset=0, limit=50):
        async with self.session_factory() as session:
            await self._require_group(session, group_id)
            filters = [DynamicGroupSelection.group_id == group_id]
            if trade_date:
                filters.append(DynamicGroupSelection.trade_date == _date(trade_date))
            count = await session.scalar(select(func.count()).select_from(DynamicGroupSelection).where(*filters))
            rows = (await session.scalars(select(DynamicGroupSelection).where(*filters).order_by(
                DynamicGroupSelection.selected_at.desc(), DynamicGroupSelection.id.desc()).offset(max(0, offset)).limit(min(100, max(1, limit))))).all()
            return {"items": [self.selection_dict(r) for r in rows], "total": count}

    async def get_selection(self, group_id, selection_id, *, q="", offset=0, limit=50):
        from app.services.dynamic_group_queries import selection_page
        async with self.session_factory() as session:
            await self._require_group(session, group_id)
            batch = await session.scalar(select(DynamicGroupSelection).where(
                DynamicGroupSelection.id == selection_id, DynamicGroupSelection.group_id == group_id))
            if not batch:
                raise HTTPException(404, "入选批次不存在")
            page = await selection_page(session, batch, q=q, offset=offset, limit=limit)
            return dict(self.selection_dict(batch), **batch.definition, definition=batch.definition,
                        **page)

    async def get_trend(self, group_id, selection_id=None, window_days=250):
        group = await self.get_group(group_id)
        selection_id = group["latest_selection_id"] if selection_id is None else selection_id
        if selection_id is None:
            return {"selection_id": None, "status": "empty", "base_date": None, "points": [], "expected_count": 0,
                    "covered_count": 0, "warnings": [], "updated_at": None, "series_type": "fixed_basket_history"}
        async with self.session_factory() as session:
            batch = await session.scalar(select(DynamicGroupSelection.id).where(
                DynamicGroupSelection.id == selection_id, DynamicGroupSelection.group_id == group_id))
            if batch is None:
                raise HTTPException(404, "入选批次不存在")
        result = await self.history.get_series(selection_id, window_days=min(1095, max(0, window_days)))
        if result.get("status") in ("pending", "empty", "missing", "error", "partial"):
            async with self.session_factory() as session:
                codes = list(await session.scalars(select(DynamicGroupSelectionItem.stock_code).where(
                    DynamicGroupSelectionItem.selection_id == selection_id)))
            self.history.request_build(selection_id, [{"stock_code": code} for code in codes])
        # Full per-stock audit revisions remain in the database. The chart
        # needs aggregate points, not thousands of repeated provenance rows.
        return {key: value for key, value in dict(result, series_type="fixed_basket_history").items()
                if key != "provenance"}

    async def get_intraday(self, group_id, trade_date=None):
        async with self.session_factory() as session:
            group = await self._require_group(session, group_id)
            day = _date(trade_date) if trade_date else group.trade_date
            rows = (await session.scalars(select(DynamicGroupIntradayPoint).where(
                DynamicGroupIntradayPoint.group_id == group_id, DynamicGroupIntradayPoint.trade_date == day).order_by(
                DynamicGroupIntradayPoint.minute, DynamicGroupIntradayPoint.version).limit(1200))).all() if day else []
            return _json({"trade_date": day, "status": group.data_status if rows else "empty", "daily_rebased": True, "points": [dict(
                time=r.minute, date=r.minute, value=r.value, version=r.version, expected_count=r.expected_count,
                covered_count=r.covered_count, status=r.status, segment=r.segment) for r in rows]})

    async def preview(self, definition):
        definition = DynamicGroupDefinition.model_validate(definition).model_dump()
        snapshot = self.source.peek_snapshot()
        self.source.request_refresh()
        if not snapshot:
            return {"items": [], "total": 0, "matched_count": 0, "unknown_count": 0, "covered_count": 0,
                    "trade_date": None, "previous_trade_date": None, "source_time": None,
                    "status": "pending", "warnings": ["行情快照正在后台加载，请稍后刷新预览"]}
        snapshot = await self.features.enrich_snapshot(snapshot, [definition])
        screen = self._screen_snapshot(snapshot, definition)
        items = [screen["rows"][code] for code in screen["selected"]]
        return _json({key: snapshot.get(key) for key in ("trade_date", "previous_trade_date", "source_time", "status", "warnings")} | {
            "items": items[:100], "total": screen["total"], "matched_count": len(items),
            "unknown_count": screen["unknown_count"], "covered_count": screen["total"] - screen["unknown_count"],
            "status": "partial" if screen["unknown_count"] or snapshot.get("status") != "ready" else "ready",
            "warnings": list(snapshot.get("warnings", [])) + screen["warnings"],
            "historical_coverage": snapshot.get("historical_coverage", {}),
            "ranking_coverage": screen["ranking_coverage"]})

    @classmethod
    def _screen_snapshot(cls, snapshot, definition):
        """One deterministic result for preview and background membership changes."""
        from app.services.dynamic_group_rules import evaluate_rules, rule_value
        rows, decisions = {}, {}
        try:
            day, observed = _date(snapshot["trade_date"]), _datetime(snapshot["source_time"])
        except (KeyError, ValueError, TypeError):
            day, observed = None, None
        universe = {str(code) for code in snapshot.get("universe_codes", [])}
        for original in snapshot.get("rows", []):
            row = _json(original)
            code = str(row.get("stock_code") or row.get("code") or "")
            if not code:
                continue
            universe.add(code)
            decisions[code] = None
            try:
                quote_at = _datetime(row.get("quote_time") or row.get("source_time"))
                fresh = bool(day and observed and quote_at.date() == day and
                             abs((observed - quote_at).total_seconds()) <= 180)
            except (ValueError, TypeError):
                fresh = False
            if not fresh:
                continue
            row["stock_code"] = code
            row["ranking_position"] = None
            rows[code] = row
            eligible = cls._eligible(row, definition["universe"])
            decisions[code] = (evaluate_rules(row, definition["rules"], definition["match_mode"])
                               if eligible is True else eligible)
        for code in universe - set(decisions):
            decisions[code] = None
        ranking = (definition.get("strategy") or {}).get("ranking")
        candidates = sorted(code for code, result in decisions.items() if result is True)
        coverage, warnings = None, []
        if ranking:
            ranked = []
            values = {}
            for code in candidates:
                value = rule_value(rows[code], ranking)
                if value is None:
                    decisions[code] = None
                else:
                    ranked.append(code)
                    values[code] = value
            ranked.sort(key=lambda code: values[code], reverse=ranking["direction"] == "desc")
            unknown = sum(result is None for result in decisions.values())
            complete = not unknown and snapshot.get("status") == "ready"
            coverage = dict(status="ready" if complete else "partial", candidate_count=len(candidates),
                covered_count=len(ranked), unknown_count=unknown, field=ranking["field"],
                **({"days": ranking["days"]} if "days" in ranking else {}))
            for position, code in enumerate(ranked, 1):
                rows[code]["ranking_position"] = position
                if ranking.get("limit") is not None and position > ranking["limit"]:
                    decisions[code] = False if complete else None
            if not complete:
                warnings.append("排序数据覆盖不完整，暂按已知候选排序；已有成员不会因不确定名次被移出")
            candidates = ranked[:ranking["limit"]] if ranking.get("limit") else ranked
        return dict(rows=rows, decisions=decisions, selected=candidates, total=len(universe),
                    unknown_count=sum(result is None for result in decisions.values()),
                    ranking_coverage=coverage, warnings=warnings)

    @staticmethod
    def _eligible(row, universe):
        if universe == "all_a":
            return True
        today, yesterday = row.get("today_touched_limit_up"), row.get("yesterday_limit_up")
        if today is True or yesterday is True:
            return True
        return False if today is False and yesterday is False else None

    async def evaluate_snapshot(self, snapshot):
        """Evaluate once per real source slot; each group's state and archives commit together."""
        # Context can recover after quotes stop changing (for example on a
        # weekend). Hash once for the shared scan rather than once per group.
        async with self._lock:
            async with self.session_factory() as session:
                groups = list((await session.scalars(select(DynamicGroup).where(
                    DynamicGroup.enabled.is_(True), DynamicGroup.archived.is_(False)))).all())
                definitions = [self.definition(group) for group in groups]
                ids = [group.id for group in groups]
            snapshot = _json(await self.features.enrich_snapshot(snapshot, definitions))
            input_signature = hashlib.sha256(json.dumps(snapshot, sort_keys=True, separators=(",", ":"),
                                                        ensure_ascii=True).encode()).hexdigest()
            builds = []
            for group_id in ids:
                try:
                    build = await self._evaluate_group(group_id, snapshot, input_signature)
                    if build:
                        builds.append(build)
                except IntegrityError as exc:
                    # A competing worker already committed this exact source slot.
                    if "dynamic_group_sample_claims." not in str(exc.orig) and "uq_dynamic_group_sample_slot" not in str(exc.orig):
                        raise
            for batch_id, items in builds:
                self.history.request_build(batch_id, items)

    async def _evaluate_group(self, group_id, snapshot, input_signature):
        async with self.session_factory() as session, session.begin():
            group = await self._require_group(session, group_id, lock=True)
            if not group.enabled or group.archived:
                return
            status = snapshot.get("status", "error")
            detected = self.now()
            try:
                day, observed = _date(snapshot["trade_date"]), _datetime(snapshot["source_time"])
            except (KeyError, TypeError, ValueError):
                day, observed = None, None
            rows = snapshot.get("rows") or []
            if status not in ("ready", "partial") or not rows or not day or observed.date() != day:
                group.data_status = "error" if status not in ("partial", "stale") else status
                group.warnings = _json(snapshot.get("warnings", []))
                group.last_error = "行情源未提供可验证的有效快照"
                return
            if group.source_time and observed < group.source_time:
                return
            slot = observed.replace(second=(observed.second // 30) * 30, microsecond=0)
            claimed = await session.scalar(select(DynamicGroupSampleClaim.id).where(
                DynamicGroupSampleClaim.group_id == group.id, DynamicGroupSampleClaim.version == group.version,
                DynamicGroupSampleClaim.slot == slot, DynamicGroupSampleClaim.input_signature == input_signature))
            if claimed:
                if group.last_error:
                    unknown_members = await session.scalar(select(func.count()).select_from(DynamicGroupMember).where(
                        DynamicGroupMember.group_id == group.id, DynamicGroupMember.data_status == "unknown"))
                    group.data_status = "partial" if unknown_members or status == "partial" else "ready"
                    group.warnings = _json(snapshot.get("warnings", []))
                    group.last_error = None
                    group.last_evaluated_at = detected
                return
            session.add(DynamicGroupSampleClaim(group_id=group.id, version=group.version, slot=slot,
                                                input_signature=input_signature))
            await session.flush()
            members = {m.stock_code: m for m in (await session.scalars(select(DynamicGroupMember).where(
                DynamicGroupMember.group_id == group.id))).all()}
            before = set(members)
            screen = self._screen_snapshot(snapshot, self.definition(group))
            live_rows = screen["rows"]
            unknown = screen["unknown_count"]
            for code, row in live_rows.items():
                result = screen["decisions"][code]
                member = members.get(code)
                if result is True and not member:
                    event = DynamicGroupEvent(group_id=group.id, version=group.version, stock_code=code,
                        kind="enter", trade_date=day, observed_at=detected, payload=dict(row, trigger_version=group.version))
                    session.add(event)
                    await session.flush()
                    member = DynamicGroupMember(group_id=group.id, stock_code=code, entry_event_id=event.id,
                        entered_at=detected, payload=dict(row, selected_at=detected.isoformat(), entry_event_id=event.id, trigger_version=group.version,
                                                        trigger_fields=deepcopy(row)), data_status="ready")
                    session.add(member)
                    members[code] = member
                elif result is False and member and group.membership_mode == "dynamic":
                    session.add(DynamicGroupEvent(group_id=group.id, version=group.version, stock_code=code,
                        kind="exit", trade_date=day, observed_at=detected, payload=row))
                    await session.delete(member)
                    del members[code]
                    member = None
                if member:
                    member.payload = dict(member.payload, **row)
                    member.data_status = "unknown" if result is None else "ready"
            for code in set(members) - set(live_rows):
                members[code].data_status = "unknown"
            group.data_status = "partial" if unknown or set(members) - set(live_rows) or status != "ready" else "ready"
            group.warnings = _json(list(snapshot.get("warnings", [])) + screen["warnings"] +
                ([f"{unknown} 只股票条件或名次未知，保留已有成员"] if unknown else []))
            group.trade_date = day
            try:
                previous = _date(snapshot.get("previous_trade_date"))
                group.previous_trade_date = previous if previous < day else None
            except (ValueError, TypeError):
                group.previous_trade_date = None
            group.source_time = observed
            group.last_evaluated_at = self.now()
            group.last_error = None
            group.member_count = len(members)
            if not live_rows:
                return
            await self._record_intraday(session, group, day, observed, before, live_rows, members)
            latest = await session.get(DynamicGroupSelection, group.latest_selection_id) if group.latest_selection_id else None
            close = observed.time() >= time(15, 0)
            is_close = close and (not latest or latest.trade_date != day or latest.kind != "close" or latest.version != group.version or before != set(members))
            if is_close:
                kind = "close"
            elif group.version > 1 and (not latest or latest.version != group.version):
                kind = "version"
            else:
                kind = "initial" if not latest else "members"
            if not latest or latest.version != group.version or before != set(members) or is_close:
                return await self._make_batch(session, group, members, detected, day, kind,
                                             f"v{group.version}:{slot.isoformat()}:{kind}:{input_signature}")

    async def _make_batch(self, session, group, members, observed, day, kind, batch_key):
        batch = DynamicGroupSelection(group_id=group.id, version=group.version, trade_date=day,
            selected_at=observed, member_count=len(members), kind=kind, batch_key=batch_key,
            definition=self.definition(group), data_status=group.data_status, warnings=group.warnings)
        session.add(batch)
        await session.flush()
        items = []
        for member in members.values():
            payload = dict(member.payload, data_status=member.data_status, batch_selected_at=observed.isoformat())
            session.add(DynamicGroupSelectionItem(selection_id=batch.id, stock_code=member.stock_code,
                                                 entry_event_id=member.entry_event_id, payload=payload))
            items.append(payload)
        group.latest_selection_id = batch.id
        return batch.id, items

    async def remove_member(self, group_id, stock_code):
        build = None
        async with self._lock, self.session_factory() as session, session.begin():
            group = await self._require_group(session, group_id, lock=True)
            if group.archived:
                raise HTTPException(409, "已归档板块不可修改")
            members = {m.stock_code: m for m in (await session.scalars(select(DynamicGroupMember).where(DynamicGroupMember.group_id == group_id))).all()}
            member = members.pop(stock_code, None)
            if not member:
                raise HTTPException(404, "当前成员不存在")
            observed = self.now()
            session.add(DynamicGroupEvent(group_id=group_id, version=group.version, stock_code=stock_code,
                kind="manual_exit", trade_date=group.trade_date, observed_at=observed, payload=member.payload))
            await session.delete(member)
            group.member_count = len(members)
            # A manual exit has no simultaneous fresh settlement quote. Break
            # the segment rather than earning that stock's post-exit movement.
            group.index_state = dict(group.index_state or {}, value=None)
            if group.trade_date:
                build = await self._make_batch(session, group, members, observed, group.trade_date,
                                                "manual_exit", f"manual:{member.entry_event_id}")
        if build:
            self.history.request_build(*build)
        return await self.get_current(group_id)

    async def _record_intraday(self, session, group, day, observed, before, rows, members):
        """Only link same-day, adjacent observed samples with a fully covered previous basket."""
        detected = self.now()
        if detected.date() != day or day.weekday() >= 5 or not (time(9, 30) <= observed.time() <= time(11, 30) or time(13) <= observed.time() <= time(15, 0, 59)):
            return
        state = deepcopy(group.index_state or {})
        live_session = time(9, 30) <= detected.time() <= time(11, 30) or time(13) <= detected.time() <= time(15, 0, 59)
        if not live_session and state.get("trade_date") != day.isoformat():
            return
        previous_prices = state.get("prices", {})
        prices = {code: row.get("price") or row.get("current_price") for code, row in rows.items()}
        valid = lambda value: isinstance(value, (int, float)) and math.isfinite(value) and value > 0
        expected = len(previous_prices)
        covered = sum(valid(prices.get(code)) for code in previous_prices)
        same_day = state.get("trade_date") == day.isoformat()
        first = not state or state.get("version") != group.version or not same_day
        adjacent = state.get("time") and 0 <= (observed - _datetime(state["time"])).total_seconds() <= 120
        prior_time = _datetime(state["time"]).time() if state.get("time") else None
        lunch_bridge = same_day and prior_time and time(11, 25) <= prior_time <= time(11, 30) and time(13) <= observed.time() <= time(13, 5)
        complete = bool(expected and covered == expected and all(valid(p) for p in previous_prices.values()))
        rebase_pending = state.get("rebase_pending", False)
        current_complete = bool(members) and all(valid(prices.get(c)) for c in members)
        value = 1000.0 if (first or rebase_pending) and current_complete else None
        if not first and not rebase_pending and same_day and (adjacent or lunch_bridge) and complete and state.get("value") is not None:
            value = state["value"] * sum(prices[c] / p for c, p in previous_prices.items()) / expected
        segment = 1 if first else state.get("segment", 1)
        minute = observed.replace(second=0, microsecond=0)
        exists = await session.scalar(select(DynamicGroupIntradayPoint.id).where(DynamicGroupIntradayPoint.group_id == group.id,
            DynamicGroupIntradayPoint.version == group.version, DynamicGroupIntradayPoint.minute == minute))
        if not exists:
            session.add(DynamicGroupIntradayPoint(group_id=group.id, version=group.version, trade_date=day, minute=minute,
                value=value, expected_count=len(members) if first or rebase_pending else expected,
                covered_count=sum(valid(prices.get(c)) for c in members) if first or rebase_pending else covered,
                status="ready" if value is not None else "gap", segment=segment))
        if value is not None and all(valid(prices.get(c)) for c in members):
            group.index_state = dict(version=group.version, trade_date=day.isoformat(), time=observed.isoformat(),
                                     value=value, prices={c: prices[c] for c in members}, segment=segment)
        elif before != set(members) or rebase_pending:
            # The outgoing basket could not be settled. It must never earn
            # price movement after an exit just because a missing quote later
            # recovers. Preserve a gap and rebase only the actual current set.
            group.index_state = dict(version=group.version, trade_date=day.isoformat(), time=observed.isoformat(),
                                     value=None, prices={c: prices.get(c) for c in members},
                                     segment=segment if first or rebase_pending else segment + 1, rebase_pending=True)
        elif members and all(valid(prices.get(c)) for c in members):
            # Keep this sample as an explicit gap and establish the next
            # segment's basket. A missing interval never becomes zero return.
            group.index_state = dict(version=group.version, trade_date=day.isoformat(), time=observed.isoformat(),
                                     value=1000.0, prices={c: prices[c] for c in members}, segment=segment + 1)


dynamic_group_service = DynamicGroupService()
