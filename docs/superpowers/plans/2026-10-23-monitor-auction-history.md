# Monitor auction history Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Add previous-session seal times and previous/current-session opening auction percentages to limit-up monitoring.
**Architecture:** One cached enrichment service supplies four optional fields to rich realtime and historical API rows. Existing Vue table, mobile cards and CSV display them consistently; fast alerts stay independent.
**Tech Stack:** FastAPI, SQLAlchemy async, httpx, unittest; Vue 3, TypeScript, Node tests, Vite.
**Spec:** ../specs/2026-10-23-monitor-auction-history-design.md

## Global Constraints

- Fields: yesterday_first_limit_up_time, yesterday_final_seal_time, yesterday_open_change_pct, today_open_change_pct.
- Percent = (open / same-session reference pre_close - 1) * 100, finite positive price inputs; signed two decimals; 0.00% retained.
- Dates follow exchange calendar and effective API trade_date, including historical fallback. No schema migration required.
- User authorized implementation; deploy only after full verification if completing online feature using existing authorized deployment workflow.

## Review Focus

- Monday/holidays use preceding session, never previous calendar day.
- Historical fallback anchored to actual date and never current quotes.
- Ex-right reference close uses same-day basis; missing values are null.
- Snapshot/delta retains context fields, with day rollover isolation.
- Bounded history fetches and retryable failures do not stall fast alerts.

### Task 1: Context service and providers
Files: backend/app/services/limit_up_context_service.py; backend/tests/test_limit_up_context_service.py.
Interface: async enrich(trade_date: date, rows: list[dict], quotes: dict | None = None) -> list[dict].
- [x] Write and run failing tests for dates, percentages, zeros, missing/stale quotes, cache/retry, bounded failures.
- [x] Implement calendar, batch prior-seal DB lookup, bounded historical daily providers and cache.
- [x] Verify service tests green.

### Task 2: Wire backend fields
Files: backend/app/services/realtime_limit_up_service.py; backend/app/api/v1/limit_up.py; backend/app/schemas/limit_up.py; backend/tests/test_limit_up_context_integration.py.
- [x] Write/run failing tests proving live and historical effective-date paths and schema preservation.
- [x] Add optional response fields; enrich rich realtime rows with existing quotes and database fallback rows with actual_date.
- [x] Verify context, service, API and stream tests green.

### Task 3: Frontend presentation
Files: frontend/src/views/LimitUpList.vue; frontend/src/components/stock/MobileLimitUpCards.vue; frontend/src/types/limit-up.ts; frontend/src/utils/limitUpContext.ts; frontend/tests/limitUpContext.test.mjs.
- [x] Write/run failing behavior tests for signed percentages, zero, missing values, CSV and field preservation.
- [x] Add four desktop columns, four mobile metrics and CSV fields with shared formatting.
- [x] Verify tests and frontend build green.

### Task 4: Review and deliver
- [x] Run full backend tests and frontend tests/build, inspect exit codes.
- [x] Independent review of spec compliance and code quality; fix important issues and rerun relevant tests.
- [x] Commit reviewed changes; deliver verified feature, reporting deployment state explicitly.

## Verification record

- Backend full suite: 1118 tests, 2 skipped, no failures; final context/integration regression: 22 passed.
- Frontend: 132 tests passed; production build passed.
- Independent spec and quality review approved after partial-seal fallback repair.
- Production read-only probe: 94/94 current openings, 87/94 historical openings available; unavailable adjusted history is left null rather than mixing price bases.

- Post-deployment source verification: Tencent web history returned 501 from production; switched the history provider to its public proxy.finance.qq.com API. Endpoint/header regression and all 22 context/integration tests passed.
