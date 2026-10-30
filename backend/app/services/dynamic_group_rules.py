"""Small, explicit three-valued rule language for dynamic stock groups."""
from __future__ import annotations

import math
from datetime import datetime, time


NUMBER_OPS = ["gte", "lte", "eq", "between", "gt", "lt", "ne"]


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
]
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


def validate_rules(rules):
    """Return canonical JSON-safe rules, rejecting code and unknown operators."""
    if not isinstance(rules, list) or not 1 <= len(rules) <= 30:
        raise ValueError("请设置 1～30 条条件")
    result = []
    for rule in rules:
        if (not isinstance(rule, dict) or not isinstance(rule.get("field"), str)
                or rule.get("field") not in _FIELDS):
            raise ValueError("不支持的筛选字段")
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
        if op == "between":
            upper = _value(field, rule.get("value2"))
            if value > upper:
                raise ValueError("区间上限不能小于下限")
            normalized["value2"] = upper
        result.append(normalized)
    return result


def _match(row, rule):
    key = rule["field"]
    # Known absence of an event is distinct from a failed or missing source.
    if key in (row.get("_known_absent_fields") or []):
        return False
    current = row.get(key)
    if current is None or current == "" or current == []:
        return None
    field = _FIELDS[key]
    try:
        if field["kind"] == "text":
            values = current if isinstance(current, list) else [current]
            normalized = [str(value).casefold() for value in values if value is not None]
            expected = str(rule["value"]).casefold()
            matched = any(expected in value if rule["op"] in {"contains", "not_contains"}
                          else expected == value for value in normalized)
            return not matched if rule["op"] in {"ne", "not_contains"} else matched
        current = _value(field, current)
        expected = rule["value"]
        op = rule["op"]
        if op == "eq":
            return current == expected
        if op == "ne":
            return current != expected
        if op == "in":
            return current in expected
        if op == "between":
            return expected <= current <= rule["value2"]
        return {"gte": lambda: current >= expected, "lte": lambda: current <= expected,
                "gt": lambda: current > expected, "lt": lambda: current < expected}[op]()
    except (ValueError, TypeError, KeyError, OverflowError):
        return None


def evaluate_rules(row, rules, match_mode="all"):
    """True/False/None: unknown inputs never accidentally become false exits."""
    results = [_match(row, rule) for rule in rules]
    if match_mode == "any":
        return True if True in results else None if None in results else False
    return False if False in results else None if None in results else True
