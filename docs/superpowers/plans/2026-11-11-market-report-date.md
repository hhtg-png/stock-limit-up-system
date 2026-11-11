# Market Report by Date Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement and review the independent tasks. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Show a consistent market report for the selected trading date and recover supportable facts from 24 missing historical reports.

**Architecture:** Reuse the exact-day snapshot endpoint and trading-date picker sources. Keep incomplete historical samples in a separate database table and return them only as an exact-date fallback for the report page. Persist exact-session market turnover independently of the monitored sample's coverage.

**Tech Stack:** Vue 3, Element Plus, ECharts, FastAPI, SQLAlchemy, SQLite.

## 1. Exact-day report experience

- [x] Extend `frontend/tests/statisticsIntradayMode.test.mjs` to cover all selected-day requests, rejection of stale responses and historical polling behavior.
- [x] In `frontend/src/views/Statistics.vue` and `frontend/src/types/market.ts`, add date navigation, day summary and documented nullable detail fields. Reuse `getLimitUpDates`; keep missing-date content empty and exclude incomplete samples from comparison charts.
- [x] Run `npm.cmd test` and `npm.cmd run build` in `frontend`.
- [x] Verify date controls and layout at 390px with a local browser fixture.

## 2. API contract

- [x] Add focused API cases in `backend/tests/test_market_review_api.py` for the exact requested day, archive sample fallback and optional detail facts.
- [x] Extend `backend/app/schemas/market_review.py` and `backend/app/api/v1/review.py` to provide `source_status`, nullable data and exact-date sample handling. Preserve live and finalized report priority.
- [x] Run `python -m unittest discover -s tests -p test_market_review_api.py -v` in `backend`.

## 3. Historical sample repair

- [x] Test archive insertion with temporary SQLite, including an existing official report, absent source rows, a nontrading date and a second execution.
- [x] Add a separate archive model and a dry-run-first CLI, `backend/scripts/backfill_market_review_archive.py`. Read existing limit-up records and optional verified turnover by date. Insert no data into the official review or source tables.
- [x] Verify exchange turnover and unadjusted daily stock quotes for the 24 missing dates. Rehearse the backfill against public monitor records in an isolated SQLite database.
- [ ] Run the dry-run and apply for 2026-03-06 through 2026-04-09 on the matching deployed revision.

## 4. Integration and release

- [x] Review local API and frontend test cases for a complete historical date, an archive sample, a holiday and today.
- [x] Run `python -m unittest discover tests -v` from `backend`, plus the frontend suite and build.
- [ ] Publish the verified commit using the existing stock deploy workflow once server SSH access is restored.
- [ ] Validate the public API and page; report coverage and fields that still have no source.
