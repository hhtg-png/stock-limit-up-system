# Monitor auction history Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Add previous-session seal times and previous/current-session opening auction percentages to limit-up monitoring.
**Architecture:** One cached enrichment service supplies four optional fields to rich realtime and historical API rows. Existing Vue table, mobile cards and CSV display them consistently; fast alerts stay independent.
**Tech Stack:** FastAPI, SQLAlchemy async, httpx, unittest; Vue 3, TypeScript, Node tests, Vite.
**Spec:** ../specs/2026-10-23-monitor-auction-history-design.md

## Global Constraints

- Fields: yesterday_first_limit_up_time, yesterday_final_seal_time, yesterday_open_change_pct, today_open_change_pct.
- Percent = (open / same-session reference pre_close - 1) * 100, finite positive price inputs; signed two decimals; 0.00% retained.
- Comparison dates follow the actual API trade_date and exchange calendar, including fallback. Rows carry context_date (actual data date) and previous_trade_date. No schema migration required.
- User authorized implementation; deploy only after full verification if completing online feature using existing authorized deployment workflow.

## Review Focus

- Monday/holidays use preceding session, never previous calendar day.
- Historical fallback retains actual row dates; yesterday/current opening metrics stay anchored to that actual date and never use another day's quotes.
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
- [x] Add optional response fields; enrich rich realtime rows with existing quotes and database fallback rows with the actual comparison date, retaining actual row dates.
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

### Production follow-up: durable opening facts
Files: backend/app/services/opening_auction_archive_service.py; backend/tests/test_opening_auction_archive_service.py; context service and tests.
- [x] Verify RED→GREEN for atomic merged day files, quote timestamp validation, real zero, independent-day reload and nonblocking bounded-batch capture.
- [x] Load archives before historical requests, overriding recent negative cache entries; retain live quote precedence and enrichment deadline.
- [x] Import verified October 21–23 opening facts, verify API current/historical rows after deployment, and confirm all-market background capture persists current session for future yesterday lookups.

## Verification record

- Backend full suite: 1118 tests, 2 skipped, no failures; final context/integration regression: 22 passed.
- Frontend: 132 tests passed; production build passed.
- Independent spec and quality review approved after partial-seal fallback repair.
- Production read-only probe: 94/94 current openings, 87/94 historical openings available; unavailable adjusted history is left null rather than mixing price bases.

- Post-deployment source verification: Tencent web history returned 501 from production; switched the history provider to its public proxy.finance.qq.com API. Endpoint/header regression and all 22 context/integration tests passed.

- Durable-archive final validation: full backend 1132 tests passed (2 skipped); 34 final context/integration/archive regressions passed; frontend 132 tests and build passed. Worker cancellation/partial quote coverage fixes approved by independent review.
- Production all-market current-session opening archive captured 5572 codes successfully; historical day files imported for October 21–23.
- Monitor metadata latency follow-up: THS reasons and float shares now refresh in the background for monitor snapshots; current quotes remain awaited. Seven new latency/isolation tests, seven existing realtime-service tests and four integration tests passed; independent review approved.

### 2026-10-25: monitor visibility and non-trading sessions

- Reproduced both omissions on production: the navigation's Dashboard desktop table/export lacked the four fields; Sunday requests labelled Friday pool rows as Sunday and returned four null fields, while explicit Friday queries returned valid context.
- Added Dashboard columns and shared CSV presentation; both page exports use the effective data date and have download behavior coverage.
- Current HTTP queries now confirm an exchange session before using a live pool. Closed/unverified dates follow the existing database fallback and its actual session. Weekend, initial WebSocket, rich sync and watcher guards prevent wrongly dated updates from replacing historical context; weekday verification shares the existing cached, off-loop authoritative calendar class.
- Verification: full backend 1155 tests, 2 skipped, no failures; frontend 135 tests and production build passed. Independent review approved; new date and WebSocket regressions were observed failing before the fix.

### 2026-10-26: pre-open provider coverage

- Reproduced Monday 08:12 production requests returning Friday's 82 sealed and 12 broken rows with qdate=20261023, then stamping them as Monday. The Monday context correctly selected Friday, causing the displayed current and yesterday seal times to duplicate.
- Treat qdate as the provider's latest available date, not an exact historical-row date. Both raw crawling and realtime aggregation reject requests later than qdate before enrichment; invalid-date refreshes clear the affected cache and use the existing database fallback. Earlier historical queries and ordinary network failure caching remain compatible.
- Added six provider/cache regressions and an end-to-end API regression: a Monday request with Friday coverage falls back to Friday, with current final seal 09:25:00 and Thursday final seal 10:52:37. The new behavior was observed failing before implementation.
- Verification: full backend 1162 tests, 2 skipped, no failures; fresh frontend build passed with unchanged source. Independent review approved.

### 2026-10-26: clarified actual-date comparisons, unified sorting and automatic queries

- The requested-date interpretation deployed in 37c65fc was an incorrect assumption. The user explicitly chose "10 月 22 日：10:52:37（行情日期的前一交易日）" for query 26 / actual 23. Actual-date semantics are authoritative: yesterday=22, current opening=23. Upstream Eastmoney and independently dated THS records confirm 22 final seal=10:52:37 and 23 final seal=09:25:00.
- [x] Observe regressions and restore actual-date fallback enrichment while retaining date metadata and the provider coverage guard. context_date matches actual trade_date; previous_trade_date is the prior exchange session.
- [x] Backend supports yesterday_final_seal_time (default asc), yesterday_open_change_pct and today_open_change_pct (default desc), explicit asc/desc, missing/invalid values last and stock-code ascending ties. Zero participates in numeric sorting.
- [x] Both pages use a unified seven-option sort group and remove header sorting. Reapply all seven selected sorts after realtime snapshots/deltas, preserving the existing one-word seal-amount tie rule. CSV retains actual data and previous-session dates.
- [x] Remove query buttons; automatically query all filter changes, debounce price input by 300ms, preserve reset/refresh/export, and prevent outdated responses/errors overwriting newer filter results. A→B→A and price-input reversions have behavior coverage.
- [x] Run full backend suite (1169 tests, 2 skipped, no failures) and frontend tests/build (152 tests, production build passed); independent review approved after repairing the three existing sorts' realtime ordering.
- Delivery verification: deploy origin/main and verify both pages, matching frontend assets, service supervision and public API sorting/date results; record the deployed commit and results in the task response.
