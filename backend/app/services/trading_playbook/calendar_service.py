"""Non-blocking shared cache for the authoritative China trading calendar."""

from __future__ import annotations

import asyncio
import time
from datetime import date, datetime, timedelta
from typing import Callable, Iterable, Optional

from app.utils.time_utils import today_cn


class TradingCalendarLookupError(RuntimeError):
    """Raised when no authoritative cached trading calendar is available."""


class TradingCalendarService:
    """Refresh an authoritative calendar off-loop and serve hot reads in memory."""

    def __init__(
        self,
        *,
        loader: Callable[[date, date], Iterable[date]],
        refresh_timeout_seconds: float = 5.0,
        retry_interval_seconds: float = 30.0,
        today_provider: Callable[[], date] = today_cn,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._loader = loader
        self._refresh_timeout_seconds = max(float(refresh_timeout_seconds), 0.01)
        self._retry_interval_seconds = max(float(retry_interval_seconds), 0.0)
        self._today_provider = today_provider
        self._monotonic = monotonic
        self._lock = asyncio.Lock()
        self._dates: tuple[date, ...] = ()
        self._coverage_start: Optional[date] = None
        self._coverage_end: Optional[date] = None
        self._refresh_day: Optional[date] = None
        self._next_retry_at = 0.0
        self._last_error: Optional[TradingCalendarLookupError] = None

    async def ensure_date(self, value: date) -> None:
        end = value + timedelta(days=15)
        refresh_day = self._today_provider()
        if self._is_current(value, end, refresh_day):
            return
        now = self._monotonic()
        if now < self._next_retry_at:
            self._raise_without_covered_cache(value, end)
            return

        async with self._lock:
            refresh_day = self._today_provider()
            if self._is_current(value, end, refresh_day):
                return
            now = self._monotonic()
            if now < self._next_retry_at:
                self._raise_without_covered_cache(value, end)
                return
            try:
                loaded = await asyncio.wait_for(
                    asyncio.to_thread(self._loader, value, end),
                    timeout=self._refresh_timeout_seconds,
                )
                normalized = self._normalize_dates(loaded, value, end)
            except Exception as exc:
                error = (
                    exc
                    if isinstance(exc, TradingCalendarLookupError)
                    else TradingCalendarLookupError(
                        f"Unable to refresh China trading calendar: {exc}"
                    )
                )
                self._last_error = error
                self._next_retry_at = now + self._retry_interval_seconds
                self._raise_without_covered_cache(value, end)
                return

            self._dates = tuple(normalized)
            self._coverage_start = value
            self._coverage_end = end
            self._refresh_day = refresh_day
            self._next_retry_at = 0.0
            self._last_error = None

    def is_trading_day(self, value: date) -> bool:
        if not self._covers(value, value):
            return False
        return value in self._dates

    def next_trade_date(self, value: date) -> date:
        if not self._covers(value, value + timedelta(days=15)):
            raise TradingCalendarLookupError(
                f"China trading calendar cache does not cover {value}"
            )
        for candidate in self._dates:
            if candidate > value:
                return candidate
        raise TradingCalendarLookupError(
            f"Unable to resolve next China trading date after {value}"
        )

    async def close(self) -> None:
        """Calendar refreshes are awaited inline, so no background task remains."""

    def _is_current(self, start: date, end: date, refresh_day: date) -> bool:
        return self._refresh_day == refresh_day and self._covers(start, end)

    def _covers(self, start: date, end: date) -> bool:
        return (
            self._coverage_start is not None
            and self._coverage_end is not None
            and self._coverage_start <= start
            and self._coverage_end >= end
        )

    def _raise_without_covered_cache(self, start: date, end: date) -> None:
        if self._covers(start, end):
            return
        if self._last_error is not None:
            raise self._last_error
        raise TradingCalendarLookupError(
            f"China trading calendar cache does not cover {start} through {end}"
        )

    @staticmethod
    def _normalize_dates(
        values: Iterable[date],
        start: date,
        end: date,
    ) -> list[date]:
        normalized = set()
        for raw in values:
            value = raw
            if isinstance(raw, datetime):
                value = raw.date()
            elif isinstance(raw, str):
                value = datetime.strptime(raw, "%Y-%m-%d").date()
            if type(value) is not date:
                raise TradingCalendarLookupError(
                    "China trading calendar returned an invalid date"
                )
            if start <= value <= end:
                normalized.add(value)
        return sorted(normalized)

