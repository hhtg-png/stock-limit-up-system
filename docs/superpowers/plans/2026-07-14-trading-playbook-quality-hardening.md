# Trading Playbook Production Quality Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make all four production playbook stages produce safe actionable plans from real point-in-time evidence while keeping calendar, scheduling, retries, and bounded data collection reliable across workers.

**Architecture:** Split stage-aware evidence completion, cached calendar access, database job claims, and scheduler phase compensation into focused services. The market-data provider will merge persisted context with quote, realtime-pool, K-line, daily-analysis, and prior-plan evidence only after those sources are collected; missing evidence remains missing. Scheduler work is decomposed into claim-protected build, notification, and review-finalization phases so failures retry independently.

**Tech Stack:** Python 3.11, asyncio, SQLAlchemy async ORM, SQLite/PostgreSQL-compatible DML, APScheduler, pytest/unittest.

---

### Task 1: Stage-aware evidence and cached China trading calendar

**Files:**
- Create: `backend/app/services/trading_playbook/context_service.py`
- Create: `backend/app/services/trading_playbook/calendar_service.py`
- Modify: `backend/app/services/trading_playbook/composition.py`
- Modify: `backend/app/services/trading_playbook/market_data.py`
- Modify: `backend/app/services/trading_playbook/orchestrator.py`
- Modify: `backend/app/data_collectors/scheduler.py`
- Modify: `backend/app/main.py`
- Modify: `backend/app/config.py`
- Test: `backend/tests/test_trading_playbook_production_pipeline.py`
- Test: `backend/tests/test_trading_playbook_calendar.py`
- Test: `backend/tests/test_trading_playbook_market_data.py`
- Test: `backend/tests/test_trading_playbook_composition.py`
- Test: `backend/tests/test_trading_playbook_scheduler.py`
- Test: `backend/tests/test_main_lifespan.py`

- [ ] **Step 1: Write real-ORM production pipeline failures**

Create `Stock`, `MarketReviewDailyMetric`, `MarketReviewStockDaily`, `MarketReviewLimitUpEvent`, `DailyAnalysisRecord`, and prior `TradingPlanVersion` rows using `Base.metadata.create_all`. Build the actual production orchestrator with fake network loaders whose rows contain real provenance. For each stage assert:

```python
plan = await orchestrator.build_stage(db, source_date, stage, as_of)
assert plan["market_state_json"]["style"] != "unknown"
assert plan["market_state_json"]["window"] != "unknown"
assert any(row["risk_level"] in {"trial", "confirmed"} for row in plan["candidates"])
```

Add paired cases removing the current/previous evidence or K-line coverage and assert degraded quality with no formal candidates.

- [ ] **Step 2: Run the four-stage tests and record RED**

Run:

```powershell
python -m pytest -q tests/test_trading_playbook_production_pipeline.py
```

Expected: preclose/overnight/auction fail on current-day context lookup; after-close is globally unsafe because the official close quote is older than ten seconds; complete ORM rows still expose only 5/13 context fields.

- [ ] **Step 3: Add the stage-aware production context service**

Implement a service returning a partial contract with explicit provenance:

```python
{
    "scope": "full_market",
    "trade_date": source_trade_date,
    "evidence_trade_date": baseline_trade_date,
    "as_of": captured_at,
    "field_quality": {field: "ready" | "computed" | "missing"},
    "field_provenance": {field: {"source": source, "trade_date": evidence_date}},
}
```

Use the latest prior primary daily metric for overnight/auction, current+previous metrics for after-close, and whatever point-in-time current facts exist for preclose. Derive sell-pressure and breadth only from current-vs-previous stored values; reuse `DailyAnalysisRecord` negative-feedback output and prior persisted plan window/divergence. Do not emit values for missing fields.

- [ ] **Step 4: Merge computed context after quote/realtime/K-line collection**

Move completeness evaluation to the end of `build_market_snapshot`. Compute limit-up/max-board/seal from a successfully loaded realtime pool, limit-down and breadth from sufficiently covered full-market quotes, and current/prior trend-high counts from ready bounded K-lines only when the documented coverage threshold is met. Preserve per-field source, evidence date, coverage, and quality.

- [ ] **Step 5: Add stage-specific quote field freshness**

Keep preclose current quotes and auction 09:15–09:26 quotes strict. Accept the same-day official 15:00 close as after-close baseline and the preceding trading-day close as overnight baseline; mark speed/rank fields missing unless independently fresh. Never change `PlanService._globally_unsafe` and never make an old quote ready for speed.

- [ ] **Step 6: Write cached calendar RED tests**

Use a blocking loader and heartbeat coroutine:

```python
await asyncio.gather(*(scheduler._monitor_trading_playbook() for _ in range(5)), heartbeat())
assert loader_calls == 1
assert heartbeat_ticks > 0
```

Also assert refresh failure uses last-good data, retry is throttled, no-cache failure produces a controlled playbook error while alert monitoring still runs, and no weekday fallback is used.

- [ ] **Step 7: Implement and wire `TradingCalendarService`**

Refresh the external loader only through `asyncio.to_thread` guarded by one async lock and `asyncio.wait_for`. Cache covered ranges, last successful dates, and next retry time. Scheduler async paths call `ensure_date`; orchestrator receives the same service's pure-memory `next_trade_date`. Warm and install the single service in `main.lifespan`, and close it during cleanup.

- [ ] **Step 8: Harden APScheduler registration and catch-up boundary**

Set explicit `misfire_grace_time` and `coalesce=True` on the five cron jobs and monitor. Run alert monitoring independently/first when calendar refresh fails. Change auction catch-up to `time(9, 26) <= current_time < time(15, 0)`.

- [ ] **Step 9: Verify and commit A+B**

Run the focused production pipeline, calendar, market-data, scheduler, and lifespan tests. Commit only when RED cases are GREEN:

```powershell
git commit -m "fix: build playbooks from stage-aware evidence"
```

### Task 2: Structured degradation, database leases, and phase compensation

**Files:**
- Create: `backend/app/services/trading_playbook/job_claim_service.py`
- Modify: `backend/app/models/trading_playbook.py`
- Modify: `backend/app/models/__init__.py`
- Modify: `backend/app/database.py`
- Modify: `backend/app/services/trading_playbook/domain.py`
- Modify: `backend/app/services/trading_playbook/market_data.py`
- Modify: `backend/app/services/trading_playbook/plan_service.py`
- Modify: `backend/app/data_collectors/scheduler.py`
- Modify: `backend/app/main.py`
- Test: `backend/tests/test_trading_playbook_models.py`
- Test: `backend/tests/test_trading_playbook_plan_service.py`
- Test: `backend/tests/test_trading_playbook_job_claims.py`
- Test: `backend/tests/test_trading_playbook_scheduler.py`
- Test: `backend/tests/test_database_sqlite_config.py`

- [ ] **Step 1: Write structured degradation RED**

Create a forced snapshot with 51 ordinary warnings and assert persisted quality contains:

```python
assert quality["forced_degraded"] is True
assert quality["degradation_reason"] == "after_close_barrier_timeout"
```

Assert scheduler upgrade detection uses the structured fields and retains legacy warning compatibility.

- [ ] **Step 2: Implement structured quality serialization**

Extend `DataQuality` with `forced_degraded: bool = False` and `degradation_reason: str | None = None`. Set them when the after-close barrier times out, serialize them through `TradingPlanService`, and prefer them in scheduler detection.

- [ ] **Step 3: Write real dual-engine claim RED**

With two SQLite engines and two `DataScheduler` instances, concurrently run the same source/target/stage generation and assert exactly one orchestrator build, one notification, and one final review. Add an expired-lease takeover case and verify completed claims never run again.

- [ ] **Step 4: Add the claim model and atomic service**

Create a unique job-key table containing owner, lease expiry, status, completed timestamp, and error. Use dialect-specific `INSERT ... ON CONFLICT DO NOTHING`, followed by conditional `UPDATE` for expired takeover. Claim keys include job type, source, target, stage, phase, and readiness generation/forced-plan id.

- [ ] **Step 5: Split scheduler phases and write compensation RED**

Assert notify failure does not prevent final review; final-review failure followed by another monitor retries finalization without rebuilding or notifying; completed phases stop retrying. Assert missing alert/review services do not mark claims completed.

- [ ] **Step 6: Implement build/notify/finalize phases**

Wrap stage build, notification, initial review, and final review in separate claims. Complete each phase only after success and release failed phases for later retry. Query `TradingExecutionReview.finalized_at` rather than row existence for final-review completeness. Monitor and startup scan the relevant latest plan and compensate incomplete notification/finalization phases.

- [ ] **Step 7: Add SQLite and PostgreSQL schema compatibility**

Create the claim table through metadata. Add idempotent SQLite column/index compatibility and PostgreSQL `CREATE TABLE/INDEX IF NOT EXISTS` statements without claiming live PostgreSQL execution.

- [ ] **Step 8: Verify and commit C+D+E**

Run model, database, plan-service, claim, and scheduler tests, then commit:

```powershell
git commit -m "fix: claim and compensate playbook phases"
```

### Task 3: Lifecycle recovery and bounded waits

**Files:**
- Modify: `backend/app/data_collectors/scheduler.py`
- Modify: `backend/app/services/trading_playbook/market_data.py`
- Modify: `backend/app/services/trading_playbook/orchestrator.py`
- Modify: `backend/app/services/trading_playbook/calendar_service.py`
- Modify: `backend/app/config.py`
- Test: `backend/tests/test_trading_playbook_scheduler.py`
- Test: `backend/tests/test_trading_playbook_market_data.py`
- Test: `backend/tests/test_main_lifespan.py`

- [ ] **Step 1: Write scheduler-start recovery RED**

Patch a real `AsyncIOScheduler.start` to raise after registration. Assert cleanup removes pending jobs/replaces the scheduler, `stop` is safe, and a second start has one copy of every job without `ConflictingIdError`. Assert normal stop/restart is also stable.

- [ ] **Step 2: Implement scheduler recreation on failed start/stop**

Store a scheduler factory. If start raises, remove pending jobs, shut down if running, replace the scheduler, and reset state before re-raising. `stop` checks the scheduler's actual `running` state and always clears registered jobs/state.

- [ ] **Step 3: Write monotonic barrier RED**

Inject a slow session/query and a fake monotonic clock. Assert total wall-clock deadline includes database time, each poll is wrapped by `asyncio.wait_for(min(10, remaining))`, cancellation exits the async session, and elapsed time stays within the configured deadline tolerance.

- [ ] **Step 4: Implement deadline-based barrier**

Use the event loop's monotonic clock, recompute remaining time before query and sleep, and cancel a slow one-shot query at the smaller of ten seconds or remaining time. Treat timeout/query errors as not-ready while guaranteeing session closure.

- [ ] **Step 5: Write K-line batch-budget RED**

Run 200 slow K-line loaders with a small configured stage budget. Assert `build_market_snapshot` returns within that budget, timed-out codes have degraded K-line evidence, realtime and prior-plan candidates remain included, and `asyncio.all_tasks()` has no leaked loader tasks.

- [ ] **Step 6: Implement the K-line stage deadline**

Add `TRADING_PLAYBOOK_KLINE_STAGE_TIMEOUT_SECONDS` (default 25). Wait for the batch with one stage deadline, cancel and await pending tasks, and produce explicit missing/degraded results. Exclude review-history-only codes from the nonessential union while preserving realtime-pool and prior-plan candidates.

- [ ] **Step 7: Verify and commit F+G+H+I**

Run scheduler, market-data, calendar, and lifespan tests, then commit:

```powershell
git commit -m "fix: bound playbook scheduler work"
```

### Task 4: Full verification and self-review

**Files:**
- Verify all files changed by Tasks 1–3.

- [ ] **Step 1: Run Task 3–9 regression**

```powershell
python -m pytest -q tests/test_trading_playbook_models.py tests/test_trading_playbook_rule_catalog.py tests/test_trading_playbook_market_data.py tests/test_trading_playbook_market_state.py tests/test_trading_playbook_mode_matcher.py tests/test_trading_playbook_plan_service.py tests/test_trading_playbook_orchestrator.py tests/test_trading_playbook_api.py tests/test_trading_playbook_scheduler.py tests/test_trading_playbook_composition.py tests/test_market_review_scheduler.py tests/test_main_lifespan.py
```

- [ ] **Step 2: Run compilation and diff checks**

```powershell
python -m compileall -q app tests
git diff --check
```

- [ ] **Step 3: Self-review every review item**

Confirm A–I against code and tests, explicitly record that PostgreSQL received static SQL coverage only if no PostgreSQL instance is available, and report `DONE`, `DONE_WITH_CONCERNS`, `NEEDS_CONTEXT`, or `BLOCKED` with exact evidence.
