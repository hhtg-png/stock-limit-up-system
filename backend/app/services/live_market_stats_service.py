"""Live Shanghai/Shenzhen A-share totals from a complete exchange universe."""
import asyncio
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
                 quote_fetcher=tencent_api.get_quotes_batch, clock=time.monotonic):
        self.universe_loader = universe_loader
        self.quote_fetcher = quote_fetcher
        self.clock = clock
        self._lock = asyncio.Lock()
        self._day = None
        self._codes = []
        self._snapshot = None
        self._snapshot_at = 0.0

    async def get_stats(self, trade_date: date):
        async with self._lock:
            if self._day != trade_date:
                codes = await self.universe_loader()
                codes = sorted({code for code in codes if len(code) == 6
                                and code.startswith(('00', '30', '60', '68'))})
                if not codes:
                    raise ValueError('Empty Shanghai/Shenzhen stock universe')
                self._codes, self._day = codes, trade_date
                self._snapshot = None
            if self._snapshot is not None and self.clock() - self._snapshot_at < 30:
                return dict(self._snapshot)

            semaphore = asyncio.Semaphore(4)

            async def fetch(chunk):
                async with semaphore:
                    return await self.quote_fetcher(chunk)

            quotes = {}
            pending = self._codes
            for _ in range(2):
                results = await asyncio.gather(
                    *(fetch(pending[i:i + 80]) for i in range(0, len(pending), 80)),
                    return_exceptions=True,
                )
                for result in results:
                    if isinstance(result, dict):
                        quotes.update(result)
                pending = [code for code in self._codes
                           if not self._valid_quote(quotes.get(code), trade_date)]
                if not pending:
                    break
            if pending:
                raise ValueError(f'Incomplete current market quotes: {len(pending)}/{len(self._codes)}')

            rows = [quotes[code] for code in self._codes]
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
