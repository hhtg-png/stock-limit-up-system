"""Live Shanghai/Shenzhen A-share totals from a complete exchange universe."""
import asyncio
import copy
import math
import time
from datetime import date

from app.data_collectors.tencent_api import tencent_api


async def _load_universe():
    import akshare as ak

    def load():
        # AkShare itself memoizes this list indefinitely. Refresh it each day.
        ak.stock_info_a_code_name.cache_clear()
        return ak.stock_info_a_code_name()['code'].astype(str).tolist()

    return await asyncio.to_thread(load)


class LiveMarketStatsService:
    def __init__(self, universe_loader=_load_universe,
                 quote_fetcher=tencent_api.get_quotes_batch, clock=time.monotonic,
                 scan_timeout=30):
        self.universe_loader = universe_loader
        self.quote_fetcher = quote_fetcher
        self.clock = clock
        self.scan_timeout = max(float(scan_timeout), 0.01)
        self._lock = asyncio.Lock()
        self._day = None
        self._codes = []
        self._snapshot = None
        self._snapshot_at = 0.0
        self._quotes_snapshot = None
        self._quotes_at = 0.0

    async def get_quotes_snapshot(self, trade_date: date):
        """Share one bounded full-market scan across stats and custom groups.

        A partial scan is useful for per-stock rules, while get_stats still
        requires complete valid coverage before publishing market totals.
        """
        async with self._lock:
            if self._day != trade_date:
                codes = await asyncio.wait_for(self.universe_loader(), timeout=15)
                codes = sorted({code for code in codes if len(code) == 6
                                and code.startswith(('00', '30', '60', '68'))})
                if not codes:
                    raise ValueError('Empty Shanghai/Shenzhen stock universe')
                self._codes, self._day = codes, trade_date
                self._snapshot = None
                self._quotes_snapshot = None
            if self._quotes_snapshot is not None and self.clock() - self._quotes_at < 30:
                return copy.deepcopy(self._quotes_snapshot)

            semaphore = asyncio.Semaphore(4)

            async def fetch(chunk):
                async with semaphore:
                    result = await self.quote_fetcher(chunk)
                    if isinstance(result, dict):
                        quotes.update(result)
                    return result

            quotes = {}
            pending = self._codes
            deadline = time.monotonic() + self.scan_timeout
            for _ in range(2):
                batch = asyncio.gather(
                    *(fetch(pending[i:i + 80]) for i in range(0, len(pending), 80)),
                    return_exceptions=True,
                )
                try:
                    results = await asyncio.wait_for(batch, timeout=max(0.001, deadline - time.monotonic()))
                except asyncio.CancelledError:
                    batch.cancel()
                    await asyncio.gather(batch, return_exceptions=True)
                    raise
                except asyncio.TimeoutError:
                    await asyncio.gather(batch, return_exceptions=True)
                    pending = [code for code in self._codes
                               if not self._valid_quote(quotes.get(code), trade_date)]
                    break
                for result in results:
                    if isinstance(result, dict):
                        quotes.update(result)
                pending = [code for code in self._codes
                           if not self._valid_quote(quotes.get(code), trade_date)]
                if not pending:
                    break
            result = dict(trade_date=trade_date.isoformat(), codes=list(self._codes),
                          quotes=copy.deepcopy(quotes), missing_codes=list(pending))
            self._quotes_snapshot, self._quotes_at = result, self.clock()
            return copy.deepcopy(result)

    async def get_stats(self, trade_date: date):
        snapshot = await self.get_quotes_snapshot(trade_date)
        pending = snapshot["missing_codes"]
        if pending:
            raise ValueError(f'Incomplete current market quotes: {len(pending)}/{len(snapshot["codes"])}')
        rows = [snapshot["quotes"][code] for code in snapshot["codes"]]
        result = {
                'market_turnover': round(sum(float(q['amount']) for q in rows) / 10000, 2),
                'up_count_ex_st': sum('ST' not in q['name'].upper() and float(q['change_pct']) > 0 for q in rows),
                'down_count_ex_st': sum('ST' not in q['name'].upper() and float(q['change_pct']) < 0 for q in rows),
                'limit_down_count': sum(0 < float(q['price']) <= float(q['limit_down']) + 0.001
                                        and float(q['limit_down']) > 0 for q in rows),
        }
        self._snapshot, self._snapshot_at = result, self.clock()
        return dict(result)

    @staticmethod
    def _valid_quote(quote, trade_date):
        if not quote or not str(quote.get('datetime', '')).startswith(trade_date.strftime('%Y%m%d')):
            return False
        fields = ('amount', 'change_pct', 'price', 'limit_down')
        if not quote.get('name') or set(fields).intersection(quote.get('_missing_fields', [])):
            return False
        try:
            return all(math.isfinite(float(quote[key])) for key in fields)
        except (KeyError, ValueError, TypeError):
            return False


live_market_stats_service = LiveMarketStatsService()
