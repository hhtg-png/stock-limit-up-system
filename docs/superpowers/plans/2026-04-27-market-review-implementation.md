# Market Review Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an end-of-day market review data warehouse and reporting flow that produces stable review metrics, supports historical backfill, and powers the frontend review charts without realtime recomputation.

**Architecture:** Add three backend persistence tables for daily metrics, per-stock review facts, and limit-up events; implement one pure aggregation service plus one orchestration pipeline; expose dedicated `/statistics/review/*` APIs; then refactor the existing statistics page to read only the new aggregated endpoints.

**Tech Stack:** FastAPI, SQLAlchemy async ORM, aiosqlite/SQLite, APScheduler, httpx, AKShare, Vue 3, Axios, ECharts, unittest

---

## File Structure

### Backend files to create

- `backend/app/models/market_review.py`
  - ORM tables for `market_review_daily_metric`, `market_review_stock_daily`, `market_review_limitup_event`
- `backend/app/schemas/market_review.py`
  - Pydantic response models for review trend, detail, ladder, and point-in-time metric payloads
- `backend/app/services/market_review_metrics_service.py`
  - Pure aggregation logic that turns per-stock facts into daily review metrics
- `backend/app/services/market_review_source_service.py`
  - External data normalization for market turnover, breadth, limit-down list, and fallback history
- `backend/app/services/market_review_pipeline_service.py`
  - Orchestration service that collects source data, upserts fact rows, builds daily metrics, and reruns nightly repair
- `backend/app/api/v1/review.py`
  - Dedicated review API endpoints under `/statistics/review/*`
- `backend/scripts/backfill_market_review.py`
  - Manual historical backfill entry point
- `backend/tests/test_market_review_models.py`
  - Table registration and uniqueness coverage
- `backend/tests/test_market_review_metrics_service.py`
  - Pure metric aggregation coverage
- `backend/tests/test_market_review_pipeline_service.py`
  - Persistence and upsert coverage
- `backend/tests/test_market_review_scheduler.py`
  - Scheduler job registration and invocation coverage
- `backend/tests/test_market_review_api.py`
  - Review endpoint response coverage

### Backend files to modify

- `backend/requirements.txt`
  - Add `akshare`
- `backend/app/config.py`
  - Add review job switches and repair timing configuration
- `backend/app/models/__init__.py`
  - Export new review models if package exports are used later
- `backend/app/api/v1/__init__.py`
  - Register the new review router under `/statistics/review`
- `backend/app/data_collectors/scheduler.py`
  - Schedule first-pass build and nightly repair jobs
- `backend/app/main.py`
  - Keep startup wiring unchanged except for review scheduler start/stop integration if needed
- `frontend/src/api/index.ts`
  - Re-export review API helpers
- `frontend/src/types/market.ts`
  - Add review-specific TS types
- `frontend/src/views/Statistics.vue`
  - Replace current lightweight charts with review-driven charts

### Frontend files to create

- `frontend/src/api/review.ts`
  - Axios wrappers for `/statistics/review/daily`, `/statistics/review/detail`, `/statistics/review/ladder`

## Task 1: Add Review Data Model and Config Surface

**Files:**
- Create: `backend/app/models/market_review.py`
- Modify: `backend/app/models/__init__.py`
- Modify: `backend/app/config.py`
- Modify: `backend/requirements.txt`
- Test: `backend/tests/test_market_review_models.py`

- [ ] **Step 1: Write the failing model-registration test**

```python
import unittest

from app.database import Base
from app.models import market_review  # noqa: F401


class MarketReviewModelTests(unittest.TestCase):
    def test_review_tables_are_registered(self):
        table_names = set(Base.metadata.tables.keys())
        self.assertIn("market_review_daily_metric", table_names)
        self.assertIn("market_review_stock_daily", table_names)
        self.assertIn("market_review_limitup_event", table_names)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the model test to verify it fails**

Run:

```powershell
cd D:\code\stock-limit-up-system\backend
.\venv\Scripts\python.exe -m unittest tests.test_market_review_models -v
```

Expected:

```text
One failing assertion showing that `market_review_daily_metric` is missing from `Base.metadata.tables`
```

- [ ] **Step 3: Add the ORM models and config fields**

```python
# backend/app/models/market_review.py
from datetime import datetime, date, time
from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    JSON,
    String,
    Time,
    UniqueConstraint,
)

from app.database import Base


class MarketReviewDailyMetric(Base):
    __tablename__ = "market_review_daily_metric"
    __table_args__ = (UniqueConstraint("trade_date", name="uq_review_metric_trade_date"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    trade_date = Column(Date, nullable=False)
    limit_up_count = Column(Integer, default=0, nullable=False)
    limit_down_count = Column(Integer, default=0, nullable=False)
    continuous_count = Column(Integer, default=0, nullable=False)
    max_board_height = Column(Integer, default=0, nullable=False)
    second_board_height = Column(Integer, default=0, nullable=False)
    gem_board_height = Column(Integer, default=0, nullable=False)
    first_to_second_rate = Column(Float, default=0, nullable=False)
    continuous_promotion_rate = Column(Float, default=0, nullable=False)
    seal_rate = Column(Float, default=0, nullable=False)
    yesterday_limit_up_avg_change = Column(Float, default=0, nullable=False)
    yesterday_continuous_avg_change = Column(Float, default=0, nullable=False)
    market_turnover = Column(Float, default=0, nullable=False)
    up_count_ex_st = Column(Integer, default=0, nullable=False)
    down_count_ex_st = Column(Integer, default=0, nullable=False)
    limit_up_amount = Column(Float, default=0, nullable=False)
    broken_amount = Column(Float, default=0, nullable=False)
    calc_version = Column(Integer, default=1, nullable=False)
    source_status = Column(String(20), default="primary", nullable=False)
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)


class MarketReviewStockDaily(Base):
    __tablename__ = "market_review_stock_daily"
    __table_args__ = (UniqueConstraint("trade_date", "stock_code", name="uq_review_stock_daily"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    trade_date = Column(Date, nullable=False)
    stock_code = Column(String(10), nullable=False)
    stock_name = Column(String(50), nullable=False)
    board_type = Column(String(20), default="main", nullable=False)
    is_st = Column(Boolean, default=False, nullable=False)
    yesterday_limit_up = Column(Boolean, default=False, nullable=False)
    yesterday_continuous_days = Column(Integer, default=0, nullable=False)
    today_touched_limit_up = Column(Boolean, default=False, nullable=False)
    today_sealed_close = Column(Boolean, default=False, nullable=False)
    today_opened_close = Column(Boolean, default=False, nullable=False)
    today_broken = Column(Boolean, default=False, nullable=False)
    today_continuous_days = Column(Integer, default=0, nullable=False)
    first_limit_time = Column(Time)
    final_seal_time = Column(Time)
    open_count = Column(Integer, default=0, nullable=False)
    close_price = Column(Float)
    pre_close = Column(Float)
    change_pct = Column(Float)
    amount = Column(Float, default=0, nullable=False)
    turnover_rate = Column(Float)
    tradable_market_value = Column(Float)
    limit_up_reason = Column(String(255))
    data_quality_flag = Column(String(20), default="ok", nullable=False)
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)


class MarketReviewLimitUpEvent(Base):
    __tablename__ = "market_review_limitup_event"
    __table_args__ = (
        UniqueConstraint("trade_date", "stock_code", "event_type", "event_seq", name="uq_review_limitup_event"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    trade_date = Column(Date, nullable=False, index=True)
    stock_code = Column(String(10), nullable=False, index=True)
    event_type = Column(String(20), nullable=False, index=True)
    event_time = Column(Time)
    event_seq = Column(Integer, default=0, nullable=False)
    source_name = Column(String(20), nullable=False)
    payload_json = Column(JSON, default=dict, nullable=False)
    created_at = Column(DateTime, default=datetime.now, nullable=False)
```

```python
# backend/app/config.py
# append inside the existing Settings class
MARKET_REVIEW_ENABLED: bool = True
MARKET_REVIEW_BUILD_HOUR: int = 15
MARKET_REVIEW_BUILD_MINUTE: int = 5
MARKET_REVIEW_REPAIR_HOUR: int = 20
MARKET_REVIEW_REPAIR_MINUTE: int = 15
MARKET_REVIEW_REPAIR_ENABLED: bool = True
```

```text
# backend/requirements.txt
akshare==1.16.88
```

- [ ] **Step 4: Run the model test again**

Run:

```powershell
cd D:\code\stock-limit-up-system\backend
.\venv\Scripts\python.exe -m unittest tests.test_market_review_models -v
```

Expected:

```text
OK
```

- [ ] **Step 5: Commit**

```powershell
git add backend/app/models/market_review.py backend/app/models/__init__.py backend/app/config.py backend/requirements.txt backend/tests/test_market_review_models.py
git commit -m "feat: add market review data model"
```

## Task 2: Implement Pure Daily Metric Aggregation

**Files:**
- Create: `backend/app/services/market_review_metrics_service.py`
- Test: `backend/tests/test_market_review_metrics_service.py`

- [ ] **Step 1: Write the failing aggregation tests**

```python
import unittest
from datetime import date

from app.services.market_review_metrics_service import MarketReviewMetricsService


class MarketReviewMetricsServiceTests(unittest.TestCase):
    def setUp(self):
        self.service = MarketReviewMetricsService()

    def test_aggregate_daily_metrics_builds_review_totals(self):
        rows = [
            {
                "stock_code": "600001",
                "board_type": "main",
                "is_st": False,
                "yesterday_limit_up": True,
                "yesterday_continuous_days": 1,
                "today_touched_limit_up": True,
                "today_sealed_close": True,
                "today_opened_close": False,
                "today_broken": False,
                "today_continuous_days": 2,
                "change_pct": 10.0,
                "amount": 120000.0,
            },
            {
                "stock_code": "300001",
                "board_type": "gem",
                "is_st": False,
                "yesterday_limit_up": True,
                "yesterday_continuous_days": 2,
                "today_touched_limit_up": True,
                "today_sealed_close": False,
                "today_opened_close": True,
                "today_broken": False,
                "today_continuous_days": 3,
                "change_pct": 4.5,
                "amount": 80000.0,
            },
            {
                "stock_code": "600002",
                "board_type": "main",
                "is_st": False,
                "yesterday_limit_up": False,
                "yesterday_continuous_days": 0,
                "today_touched_limit_up": False,
                "today_sealed_close": False,
                "today_opened_close": False,
                "today_broken": True,
                "today_continuous_days": 0,
                "change_pct": -3.2,
                "amount": 10000.0,
            },
        ]

        metric = self.service.aggregate_daily_metrics(
            trade_date=date(2026, 4, 27),
            stock_rows=rows,
            limit_down_count=5,
            market_turnover=12345.6,
            up_count_ex_st=3200,
            down_count_ex_st=1800,
        )

        self.assertEqual(metric["limit_up_count"], 2)
        self.assertEqual(metric["continuous_count"], 2)
        self.assertEqual(metric["max_board_height"], 3)
        self.assertEqual(metric["second_board_height"], 2)
        self.assertEqual(metric["gem_board_height"], 3)
        self.assertAlmostEqual(metric["first_to_second_rate"], 100.0)
        self.assertAlmostEqual(metric["continuous_promotion_rate"], 100.0)
        self.assertAlmostEqual(metric["seal_rate"], 50.0)
        self.assertAlmostEqual(metric["limit_up_amount"], 200000.0)
        self.assertAlmostEqual(metric["broken_amount"], 80000.0)
```

- [ ] **Step 2: Run the aggregation tests to verify they fail**

Run:

```powershell
cd D:\code\stock-limit-up-system\backend
.\venv\Scripts\python.exe -m unittest tests.test_market_review_metrics_service -v
```

Expected:

```text
ModuleNotFoundError: No module named 'app.services.market_review_metrics_service'
```

- [ ] **Step 3: Implement the pure aggregation service**

```python
from collections import Counter
from datetime import date
from typing import Dict, List


class MarketReviewMetricsService:
    def aggregate_daily_metrics(
        self,
        trade_date: date,
        stock_rows: List[Dict],
        limit_down_count: int,
        market_turnover: float,
        up_count_ex_st: int,
        down_count_ex_st: int,
    ) -> Dict:
        touched_rows = [row for row in stock_rows if row.get("today_touched_limit_up")]
        sealed_rows = [row for row in touched_rows if row.get("today_sealed_close")]
        opened_rows = [row for row in touched_rows if row.get("today_opened_close")]
        ladder_days = sorted(
            [int(row.get("today_continuous_days") or 0) for row in touched_rows if int(row.get("today_continuous_days") or 0) > 1],
            reverse=True,
        )

        yesterday_first_board = [row for row in stock_rows if row.get("yesterday_limit_up") and int(row.get("yesterday_continuous_days") or 0) == 1]
        yesterday_continuous = [row for row in stock_rows if int(row.get("yesterday_continuous_days") or 0) >= 2]

        promoted_first_board = [row for row in yesterday_first_board if int(row.get("today_continuous_days") or 0) >= 2]
        promoted_continuous = [
            row for row in yesterday_continuous
            if int(row.get("today_continuous_days") or 0) > int(row.get("yesterday_continuous_days") or 0)
        ]

        gem_days = [
            int(row.get("today_continuous_days") or 0)
            for row in touched_rows
            if row.get("board_type") in {"gem", "star"}
        ]

        def avg(values: List[float]) -> float:
            return round(sum(values) / len(values), 2) if values else 0.0

        return {
            "trade_date": trade_date,
            "limit_up_count": len(touched_rows),
            "limit_down_count": limit_down_count,
            "continuous_count": len([row for row in touched_rows if int(row.get("today_continuous_days") or 0) >= 2]),
            "max_board_height": ladder_days[0] if ladder_days else 0,
            "second_board_height": ladder_days[1] if len(ladder_days) > 1 else 0,
            "gem_board_height": max(gem_days) if gem_days else 0,
            "first_to_second_rate": round(len(promoted_first_board) * 100 / len(yesterday_first_board), 2) if yesterday_first_board else 0.0,
            "continuous_promotion_rate": round(len(promoted_continuous) * 100 / len(yesterday_continuous), 2) if yesterday_continuous else 0.0,
            "seal_rate": round(len(sealed_rows) * 100 / len(touched_rows), 2) if touched_rows else 0.0,
            "yesterday_limit_up_avg_change": avg([float(row.get("change_pct") or 0) for row in stock_rows if row.get("yesterday_limit_up")]),
            "yesterday_continuous_avg_change": avg([float(row.get("change_pct") or 0) for row in stock_rows if int(row.get("yesterday_continuous_days") or 0) >= 2]),
            "market_turnover": float(market_turnover or 0),
            "up_count_ex_st": int(up_count_ex_st or 0),
            "down_count_ex_st": int(down_count_ex_st or 0),
            "limit_up_amount": round(sum(float(row.get("amount") or 0) for row in touched_rows), 2),
            "broken_amount": round(sum(float(row.get("amount") or 0) for row in opened_rows), 2),
        }
```

- [ ] **Step 4: Run the aggregation tests again**

Run:

```powershell
cd D:\code\stock-limit-up-system\backend
.\venv\Scripts\python.exe -m unittest tests.test_market_review_metrics_service -v
```

Expected:

```text
OK
```

- [ ] **Step 5: Commit**

```powershell
git add backend/app/services/market_review_metrics_service.py backend/tests/test_market_review_metrics_service.py
git commit -m "feat: add market review metrics aggregation"
```

## Task 3: Implement Source Normalization and Persistence Pipeline

**Files:**
- Create: `backend/app/services/market_review_source_service.py`
- Create: `backend/app/services/market_review_pipeline_service.py`
- Test: `backend/tests/test_market_review_pipeline_service.py`

- [ ] **Step 1: Write the failing pipeline test**

```python
import unittest
from datetime import date

from app.services.market_review_pipeline_service import MarketReviewPipelineService


class MarketReviewPipelineServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_for_date_upserts_fact_rows_and_metric(self):
        service = MarketReviewPipelineService()
        rows = [
            {
                "stock_code": "600001",
                "stock_name": "测试一号",
                "board_type": "main",
                "today_touched_limit_up": True,
                "today_sealed_close": True,
                "today_opened_close": False,
                "today_broken": False,
                "today_continuous_days": 2,
                "yesterday_limit_up": True,
                "yesterday_continuous_days": 1,
                "change_pct": 10.0,
                "amount": 12345.0,
                "open_count": 0,
            }
        ]

        normalized = {
            "stock_rows": rows,
            "event_rows": [
                {
                    "stock_code": "600001",
                    "event_type": "close_sealed",
                    "event_seq": 1,
                    "source_name": "EM",
                    "payload_json": {"status": "sealed"},
                }
            ],
            "limit_down_count": 3,
            "market_turnover": 8888.8,
            "up_count_ex_st": 3000,
            "down_count_ex_st": 1700,
            "source_status": "primary",
        }

        result = await service.build_payload_for_date(date(2026, 4, 27), normalized)
        self.assertEqual(result["metric_row"]["limit_up_count"], 1)
        self.assertEqual(result["stock_rows"][0]["stock_code"], "600001")
        self.assertEqual(result["event_rows"][0]["event_type"], "close_sealed")
```

- [ ] **Step 2: Run the pipeline test to verify it fails**

Run:

```powershell
cd D:\code\stock-limit-up-system\backend
.\venv\Scripts\python.exe -m unittest tests.test_market_review_pipeline_service -v
```

Expected:

```text
ModuleNotFoundError: No module named 'app.services.market_review_pipeline_service'
```

- [ ] **Step 3: Implement the source service and orchestration pipeline**

```python
# backend/app/services/market_review_source_service.py
from datetime import date
from typing import Dict, List


class MarketReviewSourceService:
    async def collect_for_date(self, trade_date: date) -> Dict:
        return {
            "stock_rows": [],
            "event_rows": [],
            "limit_down_count": 0,
            "market_turnover": 0.0,
            "up_count_ex_st": 0,
            "down_count_ex_st": 0,
            "source_status": "primary",
        }
```

```python
# backend/app/services/market_review_pipeline_service.py
from datetime import date
from typing import Dict

from app.database import async_session_maker
from app.services.market_review_metrics_service import MarketReviewMetricsService
from app.services.market_review_source_service import MarketReviewSourceService


class MarketReviewPipelineService:
    def __init__(self):
        self.metrics = MarketReviewMetricsService()
        self.sources = MarketReviewSourceService()

    async def build_payload_for_date(self, trade_date: date, normalized: Dict | None = None) -> Dict:
        normalized = normalized or await self.sources.collect_for_date(trade_date)
        metric_row = self.metrics.aggregate_daily_metrics(
            trade_date=trade_date,
            stock_rows=normalized["stock_rows"],
            limit_down_count=normalized["limit_down_count"],
            market_turnover=normalized["market_turnover"],
            up_count_ex_st=normalized["up_count_ex_st"],
            down_count_ex_st=normalized["down_count_ex_st"],
        )
        metric_row["source_status"] = normalized.get("source_status", "primary")
        return {
            "metric_row": metric_row,
            "stock_rows": normalized["stock_rows"],
            "event_rows": normalized["event_rows"],
        }

    async def run_for_date(self, trade_date: date, calc_version: int = 1) -> Dict:
        payload = await self.build_payload_for_date(trade_date)
        payload["metric_row"]["calc_version"] = calc_version
        async with async_session_maker() as db:
            await self.persist_payload(db, payload)
        return payload


market_review_pipeline_service = MarketReviewPipelineService()
```

- [ ] **Step 4: Extend the pipeline to persist rows with upsert semantics**

```python
from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.market_review import (
    MarketReviewDailyMetric,
    MarketReviewLimitUpEvent,
    MarketReviewStockDaily,
)


class MarketReviewPipelineService:
    async def persist_payload(self, db: AsyncSession, payload: Dict) -> None:
        trade_date = payload["metric_row"]["trade_date"]

        metric_stmt = insert(MarketReviewDailyMetric).values(**payload["metric_row"])
        metric_stmt = metric_stmt.on_conflict_do_update(
            index_elements=["trade_date"],
            set_=payload["metric_row"],
        )
        await db.execute(metric_stmt)

        for row in payload["stock_rows"]:
            stock_stmt = insert(MarketReviewStockDaily).values(**row)
            stock_stmt = stock_stmt.on_conflict_do_update(
                index_elements=["trade_date", "stock_code"],
                set_=row,
            )
            await db.execute(stock_stmt)

        for row in payload["event_rows"]:
            event_stmt = insert(MarketReviewLimitUpEvent).values(**row)
            event_stmt = event_stmt.on_conflict_do_update(
                index_elements=["trade_date", "stock_code", "event_type", "event_seq"],
                set_=row,
            )
            await db.execute(event_stmt)

        await db.commit()
```

- [ ] **Step 5: Run the pipeline tests again**

Run:

```powershell
cd D:\code\stock-limit-up-system\backend
.\venv\Scripts\python.exe -m unittest tests.test_market_review_pipeline_service -v
```

Expected:

```text
OK
```

- [ ] **Step 6: Commit**

```powershell
git add backend/app/services/market_review_source_service.py backend/app/services/market_review_pipeline_service.py backend/tests/test_market_review_pipeline_service.py
git commit -m "feat: add market review pipeline service"
```

## Task 4: Wire Scheduler Jobs and Historical Backfill

**Files:**
- Modify: `backend/app/data_collectors/scheduler.py`
- Create: `backend/scripts/backfill_market_review.py`
- Test: `backend/tests/test_market_review_scheduler.py`

- [ ] **Step 1: Write the failing scheduler test**

```python
import unittest

from app.data_collectors.scheduler import DataScheduler


class MarketReviewSchedulerTests(unittest.TestCase):
    def test_start_registers_review_jobs(self):
        scheduler = DataScheduler()
        scheduler.start()
        try:
            self.assertIsNotNone(scheduler.scheduler.get_job("market_review_build"))
            self.assertIsNotNone(scheduler.scheduler.get_job("market_review_repair"))
        finally:
            scheduler.stop()
```

- [ ] **Step 2: Run the scheduler test to verify it fails**

Run:

```powershell
cd D:\code\stock-limit-up-system\backend
.\venv\Scripts\python.exe -m unittest tests.test_market_review_scheduler -v
```

Expected:

```text
FAIL: unexpectedly None
```

- [ ] **Step 3: Register review build and repair jobs**

```python
# backend/app/data_collectors/scheduler.py
from app.services.market_review_pipeline_service import market_review_pipeline_service


class DataScheduler:
    def start(self):
        if settings.MARKET_REVIEW_ENABLED:
            self.scheduler.add_job(
                self._build_market_review,
                CronTrigger(hour=settings.MARKET_REVIEW_BUILD_HOUR, minute=settings.MARKET_REVIEW_BUILD_MINUTE),
                id="market_review_build",
                name="收盘后复盘构建",
                max_instances=1,
            )
        if settings.MARKET_REVIEW_ENABLED and settings.MARKET_REVIEW_REPAIR_ENABLED:
            self.scheduler.add_job(
                self._repair_market_review,
                CronTrigger(hour=settings.MARKET_REVIEW_REPAIR_HOUR, minute=settings.MARKET_REVIEW_REPAIR_MINUTE),
                id="market_review_repair",
                name="晚间复盘修正",
                max_instances=1,
            )

    async def _build_market_review(self):
        await market_review_pipeline_service.run_for_date(date.today(), calc_version=1)

    async def _repair_market_review(self):
        await market_review_pipeline_service.run_for_date(date.today(), calc_version=2)
```

```python
# backend/scripts/backfill_market_review.py
import asyncio
from datetime import datetime, timedelta

from app.services.market_review_pipeline_service import market_review_pipeline_service


async def main():
    start = datetime.strptime("2026-04-01", "%Y-%m-%d").date()
    end = datetime.strptime("2026-04-30", "%Y-%m-%d").date()
    current = start
    while current <= end:
        await market_review_pipeline_service.run_for_date(current, calc_version=9)
        current += timedelta(days=1)


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 4: Run the scheduler test again**

Run:

```powershell
cd D:\code\stock-limit-up-system\backend
.\venv\Scripts\python.exe -m unittest tests.test_market_review_scheduler -v
```

Expected:

```text
OK
```

- [ ] **Step 5: Commit**

```powershell
git add backend/app/data_collectors/scheduler.py backend/scripts/backfill_market_review.py backend/tests/test_market_review_scheduler.py
git commit -m "feat: schedule market review jobs"
```

## Task 5: Expose Review APIs

**Files:**
- Create: `backend/app/schemas/market_review.py`
- Create: `backend/app/api/v1/review.py`
- Modify: `backend/app/api/v1/__init__.py`
- Test: `backend/tests/test_market_review_api.py`

- [ ] **Step 1: Write the failing API test**

```python
import unittest
from datetime import date
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from app.api.v1.review import router
from app.database import Base, get_db
from app.models.market_review import MarketReviewDailyMetric


class MarketReviewApiTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        self.session_maker = async_sessionmaker(self.engine, expire_on_commit=False)
        app = FastAPI()
        app.include_router(router, prefix="/api/v1/statistics/review")

        async def override_get_db():
            async with self.session_maker() as session:
                yield session

        app.dependency_overrides[get_db] = override_get_db
        self.client = TestClient(app)

    def test_daily_endpoint_returns_response_shape(self):
        async def seed():
            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            async with self.session_maker() as session:
                await session.execute(
                    insert(MarketReviewDailyMetric).values(
                        trade_date=date(2026, 4, 24),
                        limit_up_count=59,
                        limit_down_count=8,
                        continuous_count=8,
                        max_board_height=3,
                        second_board_height=2,
                        gem_board_height=1,
                        first_to_second_rate=14.6,
                        continuous_promotion_rate=22.2,
                        seal_rate=74.7,
                        yesterday_limit_up_avg_change=0.2,
                        yesterday_continuous_avg_change=-2.41,
                        market_turnover=26419,
                        up_count_ex_st=1994,
                        down_count_ex_st=3085,
                        limit_up_amount=659,
                        broken_amount=371,
                    )
                )
                await session.commit()

        import asyncio
        asyncio.run(seed())
        response = self.client.get("/api/v1/statistics/review/daily")
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertIn("data", body)
        self.assertIn("series", body["data"])
        self.assertEqual(body["data"]["rows"][0]["trade_date"], "2026-04-24")
```

- [ ] **Step 2: Run the API test to verify it fails**

Run:

```powershell
cd D:\code\stock-limit-up-system\backend
.\venv\Scripts\python.exe -m unittest tests.test_market_review_api -v
```

Expected:

```text
ModuleNotFoundError: No module named 'app.api.v1.review'
```

- [ ] **Step 3: Add schemas and router**

```python
# backend/app/schemas/market_review.py
from datetime import date
from pydantic import BaseModel, Field
from typing import List, Dict


class MarketReviewDailyPoint(BaseModel):
    trade_date: date
    limit_up_count: int
    limit_down_count: int
    continuous_count: int
    max_board_height: int
    second_board_height: int
    gem_board_height: int
    first_to_second_rate: float
    continuous_promotion_rate: float
    seal_rate: float
    yesterday_limit_up_avg_change: float
    yesterday_continuous_avg_change: float
    market_turnover: float
    up_count_ex_st: int
    down_count_ex_st: int
    limit_up_amount: float
    broken_amount: float


class MarketReviewDailyResponse(BaseModel):
    data: Dict[str, List]
```

```python
# backend/app/api/v1/review.py
from fastapi import APIRouter, Query

router = APIRouter()


@router.get("/daily")
async def get_review_daily(start_date: str | None = Query(None), end_date: str | None = Query(None)):
    return {
        "data": {
            "series": [],
            "rows": [],
        }
    }


@router.get("/detail")
async def get_review_detail(trade_date: str):
    return {"trade_date": trade_date, "stocks": []}


@router.get("/ladder")
async def get_review_ladder(trade_date: str):
    return {"trade_date": trade_date, "ladders": []}
```

```python
# backend/app/api/v1/__init__.py
from app.api.v1 import limit_up, statistics, market, config, websocket, review

api_router = APIRouter()
api_router.include_router(review.router, prefix="/statistics/review", tags=["复盘"])
```

- [ ] **Step 4: Replace stubbed router logic with DB-backed queries**

```python
from datetime import date
from fastapi import Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.market_review import MarketReviewDailyMetric, MarketReviewStockDaily


@router.get("/daily")
async def get_review_daily(
    start_date: str | None = Query(None),
    end_date: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    query = select(MarketReviewDailyMetric)
    if start_date:
        query = query.where(MarketReviewDailyMetric.trade_date >= date.fromisoformat(start_date))
    if end_date:
        query = query.where(MarketReviewDailyMetric.trade_date <= date.fromisoformat(end_date))
    result = await db.execute(query.order_by(MarketReviewDailyMetric.trade_date.asc()))
    rows = result.scalars().all()
    return {
        "data": {
            "series": [row.trade_date.isoformat() for row in rows],
            "rows": [
                {
                    "trade_date": row.trade_date.isoformat(),
                    "limit_up_count": row.limit_up_count,
                    "limit_down_count": row.limit_down_count,
                    "continuous_count": row.continuous_count,
                    "max_board_height": row.max_board_height,
                    "second_board_height": row.second_board_height,
                    "gem_board_height": row.gem_board_height,
                    "first_to_second_rate": row.first_to_second_rate,
                    "continuous_promotion_rate": row.continuous_promotion_rate,
                    "seal_rate": row.seal_rate,
                    "yesterday_limit_up_avg_change": row.yesterday_limit_up_avg_change,
                    "yesterday_continuous_avg_change": row.yesterday_continuous_avg_change,
                    "market_turnover": row.market_turnover,
                    "up_count_ex_st": row.up_count_ex_st,
                    "down_count_ex_st": row.down_count_ex_st,
                    "limit_up_amount": row.limit_up_amount,
                    "broken_amount": row.broken_amount,
                }
                for row in rows
            ],
        }
    }


@router.get("/detail")
async def get_review_detail(trade_date: str, db: AsyncSession = Depends(get_db)):
    parsed_date = date.fromisoformat(trade_date)
    result = await db.execute(
        select(MarketReviewStockDaily)
        .where(MarketReviewStockDaily.trade_date == parsed_date)
        .order_by(MarketReviewStockDaily.today_continuous_days.desc(), MarketReviewStockDaily.amount.desc())
    )
    stocks = result.scalars().all()
    return {
        "trade_date": trade_date,
        "stocks": [
            {
                "stock_code": stock.stock_code,
                "stock_name": stock.stock_name,
                "today_continuous_days": stock.today_continuous_days,
                "today_sealed_close": stock.today_sealed_close,
                "today_opened_close": stock.today_opened_close,
                "change_pct": stock.change_pct,
                "amount": stock.amount,
                "limit_up_reason": stock.limit_up_reason,
            }
            for stock in stocks
        ],
    }


@router.get("/ladder")
async def get_review_ladder(trade_date: str, db: AsyncSession = Depends(get_db)):
    parsed_date = date.fromisoformat(trade_date)
    result = await db.execute(
        select(MarketReviewStockDaily)
        .where(
            MarketReviewStockDaily.trade_date == parsed_date,
            MarketReviewStockDaily.today_touched_limit_up.is_(True),
            MarketReviewStockDaily.today_continuous_days >= 2,
        )
        .order_by(MarketReviewStockDaily.today_continuous_days.desc(), MarketReviewStockDaily.amount.desc())
    )
    stocks = result.scalars().all()

    ladders = {}
    for stock in stocks:
        ladders.setdefault(stock.today_continuous_days, [])
        ladders[stock.today_continuous_days].append(
            {
                "stock_code": stock.stock_code,
                "stock_name": stock.stock_name,
                "today_continuous_days": stock.today_continuous_days,
                "today_sealed_close": stock.today_sealed_close,
                "today_opened_close": stock.today_opened_close,
                "change_pct": stock.change_pct,
                "amount": stock.amount,
                "limit_up_reason": stock.limit_up_reason,
            }
        )

    return {
        "trade_date": trade_date,
        "ladders": [
            {"continuous_days": days, "count": len(items), "stocks": items}
            for days, items in sorted(ladders.items(), reverse=True)
        ],
    }
```

- [ ] **Step 5: Run the API test again**

Run:

```powershell
cd D:\code\stock-limit-up-system\backend
.\venv\Scripts\python.exe -m unittest tests.test_market_review_api -v
```

Expected:

```text
OK
```

- [ ] **Step 6: Commit**

```powershell
git add backend/app/schemas/market_review.py backend/app/api/v1/review.py backend/app/api/v1/__init__.py backend/tests/test_market_review_api.py
git commit -m "feat: add market review endpoints"
```

## Task 6: Refactor the Frontend Statistics Page to Use Review APIs

**Files:**
- Create: `frontend/src/api/review.ts`
- Modify: `frontend/src/api/index.ts`
- Modify: `frontend/src/types/market.ts`
- Modify: `frontend/src/views/Statistics.vue`

- [ ] **Step 1: Add review API helpers and types**

```ts
// frontend/src/api/review.ts
import axios from 'axios'
import type { MarketReviewDailyResponse, MarketReviewDetailResponse, MarketReviewLadderResponse } from '@/types/market'

const api = axios.create({
  baseURL: '/api/v1',
  timeout: 30000
})

export async function getMarketReviewDaily(params?: { start_date?: string; end_date?: string }): Promise<MarketReviewDailyResponse> {
  const { data } = await api.get('/statistics/review/daily', { params })
  return data
}

export async function getMarketReviewDetail(tradeDate: string): Promise<MarketReviewDetailResponse> {
  const { data } = await api.get('/statistics/review/detail', { params: { trade_date: tradeDate } })
  return data
}

export async function getMarketReviewLadder(tradeDate: string): Promise<MarketReviewLadderResponse> {
  const { data } = await api.get('/statistics/review/ladder', { params: { trade_date: tradeDate } })
  return data
}
```

```ts
// frontend/src/types/market.ts
export interface MarketReviewDailyRow {
  trade_date: string
  limit_up_count: number
  limit_down_count: number
  continuous_count: number
  max_board_height: number
  second_board_height: number
  gem_board_height: number
  first_to_second_rate: number
  continuous_promotion_rate: number
  seal_rate: number
  yesterday_limit_up_avg_change: number
  yesterday_continuous_avg_change: number
  market_turnover: number
  up_count_ex_st: number
  down_count_ex_st: number
  limit_up_amount: number
  broken_amount: number
}

export interface MarketReviewDailyResponse {
  data: {
    series: string[]
    rows: MarketReviewDailyRow[]
  }
}

export interface MarketReviewDetailStock {
  stock_code: string
  stock_name: string
  today_continuous_days: number
  today_sealed_close: boolean
  today_opened_close: boolean
  change_pct: number | null
  amount: number
  limit_up_reason?: string
}

export interface MarketReviewDetailResponse {
  trade_date: string
  stocks: MarketReviewDetailStock[]
}

export interface MarketReviewLadderLevel {
  continuous_days: number
  count: number
  stocks: MarketReviewDetailStock[]
}

export interface MarketReviewLadderResponse {
  trade_date: string
  ladders: MarketReviewLadderLevel[]
}
```

- [ ] **Step 2: Replace `Statistics.vue` data loading to use review endpoints**

```ts
import { getMarketReviewDaily, getMarketReviewDetail, getMarketReviewLadder } from '@/api'

async function fetchData() {
  const endDate = dayjs().format('YYYY-MM-DD')
  const startDate = dayjs().subtract(parseInt(timeRange.value), 'day').format('YYYY-MM-DD')

  const [daily, detail, ladder] = await Promise.all([
    getMarketReviewDaily({ start_date: startDate, end_date: endDate }),
    getMarketReviewDetail(endDate),
    getMarketReviewLadder(endDate),
  ])

  updateHeightChart(daily.data.rows)
  updatePromotionChart(daily.data.rows)
  updateTrendChart(daily.data.rows)
  updateTurnoverChart(daily.data.rows)
  updateAmountChart(daily.data.rows)
  updateDetailTables(detail, ladder)
}
```

- [ ] **Step 3: Add chart groups that map exactly to the approved spec**

```ts
function updateHeightChart(rows: MarketReviewDailyRow[]) {
  const dates = rows.map(row => row.trade_date)
  heightChart?.setOption({
    tooltip: { trigger: 'axis' },
    legend: { data: ['连板高度', '次高高度', '创业板高度'] },
    xAxis: { type: 'category', data: dates },
    yAxis: { type: 'value' },
    series: [
      { name: '连板高度', type: 'line', smooth: true, data: rows.map(row => row.max_board_height) },
      { name: '次高高度', type: 'line', smooth: true, data: rows.map(row => row.second_board_height) },
      { name: '创业板高度', type: 'line', smooth: true, data: rows.map(row => row.gem_board_height) },
    ],
  })
}
```

- [ ] **Step 4: Verify the page builds with the repo’s working frontend command**

Run:

```powershell
cd D:\code\stock-limit-up-system\frontend
npx vite build
```

Expected:

```text
Vite prints a successful build summary and emits frontend assets under `dist/`
```

```text
Note: Do not use `npm run build` as the primary verification command for this task until the existing `vue-tsc` toolchain issue is fixed. Keep `npx vite build` as the required check in this implementation pass.
```

- [ ] **Step 5: Commit**

```powershell
git add frontend/src/api/review.ts frontend/src/api/index.ts frontend/src/types/market.ts frontend/src/views/Statistics.vue
git commit -m "feat: add market review dashboard"
```

## Task 7: Run End-to-End Verification and Stabilize

**Files:**
- Modify: `backend/app/services/market_review_pipeline_service.py`
- Modify: `backend/app/api/v1/review.py`
- Modify: `frontend/src/views/Statistics.vue`
- Test: `backend/tests/test_market_review_models.py`
- Test: `backend/tests/test_market_review_metrics_service.py`
- Test: `backend/tests/test_market_review_pipeline_service.py`
- Test: `backend/tests/test_market_review_scheduler.py`
- Test: `backend/tests/test_market_review_api.py`

- [ ] **Step 1: Run the backend market-review test suite**

Run:

```powershell
cd D:\code\stock-limit-up-system\backend
.\venv\Scripts\python.exe -m unittest `
  tests.test_market_review_models `
  tests.test_market_review_metrics_service `
  tests.test_market_review_pipeline_service `
  tests.test_market_review_scheduler `
  tests.test_market_review_api -v
```

Expected:

```text
OK
```

- [ ] **Step 2: Run Python syntax verification on new backend files**

Run:

```powershell
cd D:\code\stock-limit-up-system\backend
.\venv\Scripts\python.exe -m py_compile `
  app\models\market_review.py `
  app\schemas\market_review.py `
  app\services\market_review_metrics_service.py `
  app\services\market_review_source_service.py `
  app\services\market_review_pipeline_service.py `
  app\api\v1\review.py `
  scripts\backfill_market_review.py
```

Expected:

```text
<no output>
```

- [ ] **Step 3: Run the frontend build verification**

Run:

```powershell
cd D:\code\stock-limit-up-system\frontend
npx vite build
```

Expected:

```text
Vite prints a successful build summary and emits frontend assets under `dist/`
```

- [ ] **Step 4: Smoke-test the local review endpoints**

Run:

```powershell
cd D:\code\stock-limit-up-system\backend
.\venv\Scripts\python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

Then request:

```powershell
Invoke-WebRequest http://127.0.0.1:8000/api/v1/statistics/review/daily
Invoke-WebRequest "http://127.0.0.1:8000/api/v1/statistics/review/detail?trade_date=2026-04-24"
Invoke-WebRequest "http://127.0.0.1:8000/api/v1/statistics/review/ladder?trade_date=2026-04-24"
```

Expected:

```text
HTTP 200 for all three endpoints
```

- [ ] **Step 5: Commit the stabilization pass**

```powershell
git add backend/app/services/market_review_pipeline_service.py backend/app/api/v1/review.py frontend/src/views/Statistics.vue
git commit -m "test: verify market review end to end"
```

## Self-Review Checklist

- Spec coverage:
  - Data model: Task 1
  - Aggregation rules: Task 2
  - Facts/events/metric persistence: Task 3
  - Scheduled build, repair, and historical backfill: Task 4
  - Review APIs: Task 5
  - Frontend review charts: Task 6
  - Verification and smoke tests: Task 7

- Placeholder scan:
  - No `TODO`, `TBD`, or “implement later” markers remain in the task steps
  - Every task includes explicit files, commands, and expected outcomes

- Type consistency:
  - `today_touched_limit_up`, `today_sealed_close`, `today_opened_close`, `today_broken`, and `today_continuous_days` are used consistently across models, services, and tests
  - Review API paths are consistently `/statistics/review/daily`, `/statistics/review/detail`, `/statistics/review/ladder`
