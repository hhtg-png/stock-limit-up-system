"""Confirmed full-session suspensions; absent price bars alone are not evidence."""
import asyncio
import time
from datetime import date, datetime, time as clock_time

import httpx


class StockSuspensionService:
    def __init__(self):
        self._cache = {}
        self._limit = asyncio.Semaphore(4)

    @staticmethod
    def full_session_codes(rows, day):
        opening = datetime.combine(day, clock_time(9, 30))
        closing = datetime.combine(day, clock_time(15))
        codes = set()
        for row in rows:
            try:
                start = datetime.fromisoformat(row["SUSPEND_START_TIME"])
                end = datetime.fromisoformat(row["SUSPEND_END_TIME"]) if row.get("SUSPEND_END_TIME") else None
                resume = date.fromisoformat(row["PREDICT_RESUME_DATE"][:10]) if row.get("PREDICT_RESUME_DATE") else None
                continuous = row.get("SUSPEND_EXPIRE") == "连续停牌"
                if start <= opening and (end is not None and end >= closing or end is None and continuous) and (resume is None or resume > day):
                    codes.add(row["SECURITY_CODE"])
            except (KeyError, TypeError, ValueError):
                continue
        return codes

    async def get_codes(self, day):
        cached = self._cache.get(day)
        if cached and time.monotonic() - cached[0] < 300:
            return cached[1]
        async with self._limit:
            async with httpx.AsyncClient(timeout=10) as client:
                codes = set()
                page = 1
                while True:
                    response = await client.get("https://datacenter-web.eastmoney.com/api/data/v1/get", params={
                        "reportName": "RPT_CUSTOM_SUSPEND_DATA_INTERFACE", "columns": "ALL",
                        "pageSize": "500", "pageNumber": str(page), "source": "WEB", "client": "WEB",
                        "filter": f"(MARKET=\"全部\")(DATETIME='{day.isoformat()}')",
                    })
                    response.raise_for_status()
                    result = response.json()["result"]
                    codes.update(self.full_session_codes(result["data"], day))
                    if page >= int(result["pages"]):
                        break
                    page += 1
            self._cache[day] = (time.monotonic(), codes)
            return codes


stock_suspension_service = StockSuspensionService()
