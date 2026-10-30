"""Small, explicit three-valued rule language for dynamic stock groups."""
from __future__ import annotations

import math
from datetime import datetime, time


NUMBER_OPS = ["gte", "lte", "eq", "between", "gt", "lt", "ne"]
HISTORICAL_FIELDS = frozenset({"recent_limit_up_count", "recent_touched_limit_up_count", "recent_max_continuous_days"})


def _field(key, label, kind="number", unit="", options=None):
    operators = (NUMBER_OPS if kind in {"number", "time"} else
                 ["eq", "ne"] if kind == "boolean" else
                 ["eq", "ne", "in"] if kind == "enum" else
                 ["contains", "not_contains", "eq", "ne"])
    return dict(key=key, label=label, kind=kind, unit=unit, operators=list(operators),
                **({"options": options} if options else {}))


FIELD_DEFINITIONS = [
    _field("current_price", "股价", unit="元"),
    _field("change_pct", "实时涨幅", unit="%"),
    _field("today_open_change_pct", "今日竞价", unit="%"),
    _field("yesterday_open_change_pct", "昨日竞价", unit="%"),
    _field("turnover_rate", "换手率", unit="%"),
    _field("amount_yi", "成交额", unit="亿元"),
    _field("market_value_yi", "流通市值", unit="亿元"),
    _field("continuous_limit_up_days", "今日连板数", unit="板"),
    _field("yesterday_continuous_days", "昨日连板数", unit="板"),
    _field("yesterday_limit_up", "昨日涨停", "boolean"),
    _field("today_touched_limit_up", "今日触板", "boolean"),
    _field("is_sealed", "当前封板", "boolean"),
    _field("is_st", "是否 ST", "boolean"),
    _field("market", "市场", "enum", options=[{"label": "沪市", "value": "SH"}, {"label": "深市", "value": "SZ"}]),
    _field("board_type", "交易板", "enum", options=[{"label": "主板", "value": "main"}, {"label": "创业板", "value": "gem"}, {"label": "科创板", "value": "star"}]),
    _field("stock_code", "股票代码", "text"),
    _field("stock_name", "股票名称", "text"),
    _field("industry", "行业", "text"),
    _field("concepts", "概念 / 题材", "text"),
    _field("yesterday_first_limit_up_time", "昨日首封", "time"),
    _field("yesterday_final_seal_time", "昨日终封", "time"),
    _field("first_limit_up_time", "今日首封", "time"),
    _field("final_seal_time", "今日终封", "time"),
    _field("recent_limit_up_count", "近N个已完成交易日收盘涨停次数", unit="次"),
    _field("recent_touched_limit_up_count", "近N个已完成交易日触板次数", unit="次"),
    _field("recent_max_continuous_days", "近N个已完成交易日最高连板数", unit="板"),
]
for _definition in FIELD_DEFINITIONS:
    _definition["sortable"] = _definition["kind"] in {"number", "time"}
    if _definition["key"] in HISTORICAL_FIELDS:
        _definition.update(historical=True, days_min=1, days_max=60)
_FIELDS = {field["key"]: field for field in FIELD_DEFINITIONS}


def _number(value):
    if isinstance(value, bool) or value is None:
        raise ValueError("请输入有效数值")
    try:
        number = float(value)
    except (ValueError, TypeError, OverflowError) as exc:
        raise ValueError("请输入有效数值") from exc
    if not math.isfinite(number):
        raise ValueError("请输入有限数值")
    return number


def _clock(value):
    if isinstance(value, datetime):
        value = value.time()
    if isinstance(value, time):
        return value.strftime("%H:%M:%S")
    try:
        parts = str(value).strip().split(":")
        if len(parts) not in {2, 3} or any(not part.isdigit() for part in parts):
            raise ValueError
        return time(*(int(part) for part in parts)).strftime("%H:%M:%S")
    except (ValueError, TypeError) as exc:
        raise ValueError("请输入 HH:MM 或 HH:MM:SS 时间") from exc


def _boolean(value):
    if isinstance(value, bool):
        return value
    if value in (0, "0", "false", "False"):
        return False
    if value in (1, "1", "true", "True"):
        return True
    raise ValueError("请选择是或否")


def _value(field, value):
    kind = field["kind"]
    if kind == "number":
        return _number(value)
    if kind == "time":
        return _clock(value)
    if kind == "boolean":
        return _boolean(value)
    if kind == "enum":
        allowed = {option["value"] for option in field["options"]}
        if not isinstance(value, str) or value not in allowed:
            raise ValueError(f"{field['label']}选项无效")
        return value
    if not isinstance(value, str) or not value.strip() or len(value.strip()) > 100:
        raise ValueError("请输入 1～100 个字符")
    return value.strip()


def _days(value):
    if type(value) is not int or not 1 <= value <= 60:
        raise ValueError("历史窗口须为 1～60 个已完成交易日")
    return value


def iter_rule_leaves(rules):
    """Traverse a validated legacy list or nested expression without flattening its meaning."""
    nodes = rules if isinstance(rules, list) else [rules]
    for node in nodes:
        if "all" in node or "any" in node:
            yield from iter_rule_leaves(node.get("all", node.get("any")))
        else:
            yield node


def validate_rules(rules):
    """Canonical bounded expression tree; reject every unsupported or ambiguous key."""
    leaves = 0

    def node(rule, depth):
        nonlocal leaves
        if depth > 4:
            raise ValueError("条件最多嵌套 4 层")
        if not isinstance(rule, dict):
            raise ValueError("条件须为对象")
        if "all" in rule or "any" in rule:
            if len(rule) != 1:
                raise ValueError("组合条件只能指定 all 或 any")
            key = next(iter(rule))
            children = rule[key]
            if not isinstance(children, list) or not 1 <= len(children) <= 30:
                raise ValueError("组合条件须包含 1～30 个子条件")
            return {key: [node(child, depth + 1) for child in children]}
        leaves += 1
        if leaves > 30:
            raise ValueError("最多设置 30 条叶条件")
        if (not isinstance(rule, dict) or not isinstance(rule.get("field"), str)
                or rule.get("field") not in _FIELDS):
            raise ValueError("不支持的筛选字段")
        allowed = {"field", "op", "value"}
        if rule.get("op") == "between":
            allowed.add("value2")
        if rule["field"] in HISTORICAL_FIELDS:
            allowed.add("days")
        if set(rule) - allowed:
            raise ValueError("条件包含不支持的参数")
        field = _FIELDS[rule["field"]]
        op = rule.get("op")
        if op not in field["operators"]:
            raise ValueError(f"{field['label']}不支持该比较方式")
        value = rule.get("value")
        if op == "in":
            if not isinstance(value, list) or not 1 <= len(value) <= 10:
                raise ValueError("请选择有效选项")
            value = list(dict.fromkeys(_value(field, item) for item in value))
        else:
            value = _value(field, value)
        normalized = {"field": field["key"], "op": op, "value": value}
        if field["key"] in HISTORICAL_FIELDS:
            normalized["days"] = _days(rule.get("days"))
        if op == "between":
            upper = _value(field, rule.get("value2"))
            if value > upper:
                raise ValueError("区间上限不能小于下限")
            normalized["value2"] = upper
        return normalized

    if not isinstance(rules, list) or not 1 <= len(rules) <= 30:
        raise ValueError("请设置 1～30 条条件")
    return [node(rule, 1) for rule in rules]


def validate_ranking(ranking):
    if not isinstance(ranking, dict) or set(ranking) - {"field", "direction", "limit", "days"}:
        raise ValueError("排序参数无效")
    key = ranking.get("field")
    field = _FIELDS.get(key) if isinstance(key, str) else None
    if not field or not field["sortable"]:
        raise ValueError("排序仅支持已开放的数值或时间字段")
    if ranking.get("direction") not in ("asc", "desc"):
        raise ValueError("排序方向须为 asc 或 desc")
    result = {"field": key, "direction": ranking["direction"]}
    if "limit" in ranking:
        if type(ranking["limit"]) is not int or not 1 <= ranking["limit"] <= 500:
            raise ValueError("排序数量须为 1～500")
        result["limit"] = ranking["limit"]
    if key in HISTORICAL_FIELDS:
        result["days"] = _days(ranking.get("days"))
    elif "days" in ranking:
        raise ValueError("普通排序字段不能设置历史窗口")
    return result


def rule_value(row, rule):
    key = rule["field"]
    value = ((row.get("historical_features") or {}).get(key, {}).get(str(rule["days"]))
             if key in HISTORICAL_FIELDS else row.get(key))
    if value is None or value == "" or value == []:
        return None
    try:
        return _value(_FIELDS[key], value)
    except (ValueError, TypeError, OverflowError):
        return None


def _match(row, rule):
    key = rule["field"]
    # Known absence of an event is distinct from a failed or missing source.
    if key in (row.get("_known_absent_fields") or []):
        return False
    field = _FIELDS[key]
    try:
        if field["kind"] == "text":
            current = row.get(key)
            if current is None or current == "" or current == []:
                return None
            values = current if isinstance(current, list) else [current]
            normalized = [str(value).casefold() for value in values if value is not None]
            expected = str(rule["value"]).casefold()
            matched = any(expected in value if rule["op"] in {"contains", "not_contains"}
                          else expected == value for value in normalized)
            return not matched if rule["op"] in {"ne", "not_contains"} else matched
        current = rule_value(row, rule)
        if current is None:
            return None
        expected = rule["value"]
        op = rule["op"]
        # Only absorb arithmetic noise (e.g. 9.45/9 -> 4.999999999999982%).
        # A real difference such as 4.9999% remains a different value.
        equal = (lambda a, b: math.isclose(a, b, rel_tol=0, abs_tol=1e-12)) if field["kind"] == "number" else (lambda a, b: a == b)
        if op == "eq":
            return equal(current, expected)
        if op == "ne":
            return not equal(current, expected)
        if op == "in":
            return current in expected
        if op == "between":
            return (current > expected or equal(current, expected)) and (current < rule["value2"] or equal(current, rule["value2"]))
        return {"gte": lambda: current > expected or equal(current, expected),
                "lte": lambda: current < expected or equal(current, expected),
                "gt": lambda: current > expected and not equal(current, expected),
                "lt": lambda: current < expected and not equal(current, expected)}[op]()
    except (ValueError, TypeError, KeyError, OverflowError):
        return None


def evaluate_rules(row, rules, match_mode="all"):
    """True/False/None: unknown inputs never accidentally become false exits."""
    results = [evaluate_rules(row, rule["all"], "all") if "all" in rule else
               evaluate_rules(row, rule["any"], "any") if "any" in rule else _match(row, rule)
               for rule in rules]
    if match_mode == "any":
        return True if True in results else None if None in results else False
    return False if False in results else None if None in results else True
