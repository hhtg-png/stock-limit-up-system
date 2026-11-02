# Dynamic Member Header Sorting Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Sort the complete current or historical member list from its table headers.

**Architecture:** Extend the existing bounded SQL member query with validated display-order parameters. Keep separate current/history UI order state and reuse the existing request gates. The strategy ranking and archived selection contents remain unchanged.

**Tech Stack:** FastAPI, SQLAlchemy, SQLite/PostgreSQL, Vue 3, Element Plus, Node tests.

**Spec:** `docs/superpowers/specs/2026-11-02-dynamic-member-header-sorting-design.md`

## Global Constraints

- Work only in the existing isolated worktree, branch `codex/dynamic-member-header-sorting-20261102`, base `953edc6`.
- Do not modify the dirty original checkout or create production fixture groups.
- Sort all matching rows before the existing 50-row page; retain bounded API limits.
- Sorting is read-only and must not update group definitions, ranking, members, or batches.
- Use the exact field whitelist and fallback semantics in the spec.

## Review Focus

- More than 50 matches: concatenated pages must follow one global order.
- Missing values, zero, negatives, and ties: nulls remain last, valid values retain their meaning, ties stay stable.
- In-flight refresh or search: an older reply cannot restore an obsolete order.
- Switching between current/history and clearing order: each view and its header arrows remain consistent.
- Entry-time and price fallbacks: displayed and sorted values must agree without substituting batch time.

## Task 1: Bounded SQL ordering and API contract

**Files:** `backend/app/services/dynamic_group_queries.py`, forwarding methods in `dynamic_group_service.py`, current/selection routes in `backend/app/api/v1/dynamic_groups.py`, and focused backend query/API tests.

**Produces:** optional `sort_by` and `sort_order` arguments through routes, service methods, and query functions; omitted fields preserve default ranking.

- [x] Add failing real-SQLite tests for current/history global pagination, both directions, search, empty values, ties, fallbacks, default order, and invalid API arguments.
- [x] Implement whitelisted SQL ordering before offset/limit; keep aggregate statistics and archived rows unchanged.
- [x] Run focused query/API tests and compile PostgreSQL expressions; report PostgreSQL compilation separately from execution.

## Task 2: Header controls and request state

**Files:** `frontend/src/views/DynamicGroups.vue`, `frontend/src/types/dynamic-groups.ts`, optional API/helper changes, and focused frontend behavior tests.

**Consumes:** the API parameters from Task 1. **Produces:** header controls, direction indicators, and separate current/history query state.

- [x] Add failing tests for header events, API parameters, clear/default behavior, pagination reset, and stale-response protection.
- [x] Implement sorting only in member-table headers; pass server parameters and reuse gates. Preserve mobile card order and strategy definitions.
- [x] Run focused tests, the complete `npm test`, and `npm run build`.

## Task 3: Review, publish, and verify

- [x] Independently review the integrated change against the spec and five failure classes above.
- [x] Run fresh `python -m unittest discover tests -v` after backend code is stable; read the final result.
- [ ] Commit and push to GitHub `main`, then deploy verified `origin/main` through the stock deployment skill.
- [ ] Read-only production checks: HEAD, assets, current/history sorting for existing group 1 and batch 1, pagination/search/default invariants, invalid arguments, and baseline API health.
- [ ] Verify real header interaction in the browser, save a screenshot, close temporary tabs, and report both the sorting feature and already-published top-left source setting (`953edc6`).

## Verification evidence

- Focused SQLite sorting/API tests and independent review passed; PostgreSQL expressions compile (no PostgreSQL execution).
- Frontend full suite:201 tests, exit0. Production build:exit0 in6.54s.
- Backend full suite:1357 tests in438.399s, exit0, OK(skipped2), log `%TEMP%/dynamic-member-sort-backend-full.log` (session1741 closed). Latest strengthened sorting tests also passed8/8 separately. Product backend code was stable before the full suite started.
- Existing production group1 has members and batch1 for read-only final validation; no fixture group is needed.
- The source-setting move is already published as953edc6; screenshot `composite-source-top-left-online-953edc6.png` is saved in the conversation visualization directory.
