"""Bounded member pages and database aggregates for large/mobile groups."""
from __future__ import annotations

from sqlalchemy import case, func, or_, select

from app.models.dynamic_group import DynamicGroupMember, DynamicGroupSelectionItem
from app.services.dynamic_group_rules import iter_rule_leaves


_PUBLIC_FIELDS = {
    "stock_code", "stock_name", "market", "board_type", "trade_date", "context_date", "previous_trade_date",
    "current_price", "price", "pre_close", "change_pct", "today_open_change_pct", "yesterday_open_change_pct",
    "turnover_rate", "amount", "amount_yi", "tradable_market_value", "market_value_yi", "is_st", "industry",
    "concepts", "quote_time", "is_sealed", "limit_up_price", "today_touched_limit_up", "yesterday_limit_up",
    "continuous_limit_up_days", "yesterday_continuous_days", "yesterday_first_limit_up_time",
    "yesterday_final_seal_time", "first_limit_up_time", "final_seal_time", "reason_category", "limit_up_reason",
    "selected_at", "batch_selected_at", "entry_event_id", "trigger_version", "data_status",
    "historical_features", "ranking_position",
}


def compact_item(payload):
    return {key: value for key, value in payload.items() if key in _PUBLIC_FIELDS}


async def _page(session, model, condition, day, *, q, offset, limit, current=False, trigger_keys=()):
    payload = model.payload
    change = payload["change_pct"].as_float()
    valid = payload["quote_time"].as_string().like(f"{day.isoformat()}%") if day else False
    measured = case((valid, change), else_=None)
    aggregate = (await session.execute(select(
        func.count(), func.count(measured), func.avg(measured),
        func.sum(case((measured > 0, 1), else_=0)),
        func.sum(case((measured < 0, 1), else_=0)),
    ).select_from(model).where(condition))).one()
    summary = dict(stock_count=int(aggregate[0]), valid_change_count=int(aggregate[1]),
        average_change_pct=float(aggregate[2]) if aggregate[2] is not None else None,
        up_count=int(aggregate[3] or 0), down_count=int(aggregate[4] or 0))
    filters = [condition]
    if q and q.strip():
        escaped = q.strip().replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        pattern = f"%{escaped}%"
        filters.append(or_(model.stock_code.ilike(pattern, escape="\\"),
                           payload["stock_name"].as_string().ilike(pattern, escape="\\")))
    total = await session.scalar(select(func.count()).select_from(model).where(*filters))
    statement = (select(model.payload, model.data_status) if current else select(model.payload))
    position = payload["ranking_position"].as_integer()
    result = await session.execute(statement.where(*filters).order_by(
        case((position.is_(None), 1), else_=0), position, model.stock_code)
        .offset(max(0, int(offset))).limit(min(200, max(1, int(limit)))))
    items = []
    for row in result:
        item = compact_item(row[0])
        if current:
            item["data_status"] = row[1]
        elif trigger_keys and row[0].get("trigger_fields"):
            item["trigger_fields"] = {key: value for key, value in row[0]["trigger_fields"].items()
                                      if key in trigger_keys}
        items.append(item)
    return dict(items=items, total=int(total or 0), summary=summary)


async def member_page(session, group, *, q="", offset=0, limit=50):
    return await _page(session, DynamicGroupMember, DynamicGroupMember.group_id == group.id,
                       group.trade_date, q=q, offset=offset, limit=limit, current=True)


async def selection_page(session, batch, *, q="", offset=0, limit=50):
    trigger_keys = {rule["field"] for rule in iter_rule_leaves(batch.definition.get("rules", []))}
    ranking = (batch.definition.get("strategy") or {}).get("ranking")
    if ranking:
        trigger_keys.add(ranking["field"])
    return await _page(session, DynamicGroupSelectionItem, DynamicGroupSelectionItem.selection_id == batch.id,
                       batch.trade_date, q=q, offset=offset, limit=limit,
                       trigger_keys=trigger_keys | {"historical_features"})
