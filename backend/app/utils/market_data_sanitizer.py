"""
Shared guards for external market data fields.

Some upstream quote APIs emit placeholder rows with price and amount set to zero
and change_pct set to -100. Treat those as missing data, not real market moves.
"""
from __future__ import annotations

import math
from typing import Optional


def normalize_change_pct(
    value: object,
    *,
    price: object = None,
    amount: object = None,
) -> Optional[float]:
    """Return a rounded percentage change, or None for known invalid sentinels."""
    change_pct = _to_float(value)
    if change_pct is None or not math.isfinite(change_pct):
        return None

    price_value = _to_float(price)
    amount_value = _to_float(amount)

    if price_value is not None and price_value <= 0:
        return None
    if change_pct <= -99.9 and (price_value is None or price_value <= 0) and (amount_value in (None, 0.0)):
        return None
    if change_pct < -100.0:
        return None

    return round(change_pct, 2)


def _to_float(value: object) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
