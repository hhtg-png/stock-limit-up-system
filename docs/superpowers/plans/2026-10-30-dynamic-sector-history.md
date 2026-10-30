# Dynamic Sector History Implementation Plan

> **For agentic workers:** Use subagents for independent backend persistence, history reconstruction, and responsive UI tasks. User explicitly requested direct implementation without brainstorming or further design approval.

**Goal:** Ship a multi-group dynamic sector module with rule-based selection, permanent selection archives, historical constituent-based indices, and mobile support.

**Architecture:** Shared market snapshot and a tri-state rule evaluator feed database-backed groups and immutable selections. A separate historical-price service caches consistent adjusted series and builds fixed-basket indices. A searchable directory/detail UI reads these archives; scheduled background work runs without an open browser.

**Tech Stack:** Existing FastAPI, SQLAlchemy async, SQLite, Vue 3, Element Plus, ECharts. No added dependencies.

**Spec:** `docs/superpowers/specs/2026-10-30-dynamic-sector-history-design.md`

## Global Constraints

- Work only in the isolated `codex/dynamic-sector-history-20261030` worktree; preserve the dirty original workspace.
- Use actual quote dates and previous trading days; no natural-day subtraction for yesterday.
- Unknown inputs cannot become zero or false exits. Failed sources cannot replace successful archives with empty results.
- Historical indices must include dates before group creation using the selected fixed basket, explicitly labeled.
- Fixed-basket formula: `1000 * mean(adjusted_close(t) / adjusted_close(base_date))`; retain denominator and show gaps on missing constituents.
- No false historical rule backtest or mixtures of raw and adjusted prices.
- All selection/history reads are database reads; background fetching and computation are bounded, shared, retryable and idempotent.
- Directly execute and deploy within existing session authorization; no additional design approval checkpoint.

## Review Focus

1. Cross-day stale quotes / missing rule inputs preserve state and expose coverage.
2. Historical batch remains unchanged after member exits, rule edits, restart or archive.
3. New listings, missing dates and corporate actions never change denominator or mix adjustment bases.
4. Hundreds of groups remain searchable with bounded list responses; mobile has no page-wide overflow.
5. Slow requests cannot overwrite later group / batch selection; background scheduling never depends on browser activity.

## Shared Interfaces

API prefix `/api/v1/dynamic-groups`.
Definition: `{name,description,folder,color,pinned,enabled,universe:'all_a'|'limit_up',membership_mode:'retain'|'dynamic',match_mode:'all'|'any',rules:[{field,op,value,value2?}]}`.
Group output adds `{id,version,archived,member_count,latest_selection_id,data_status,trade_date,updated_at,last_evaluated_at,last_error}`.

- `GET /fields` -> `{fields:[{key,label,kind,operators,unit?,options?}],templates:[...]}`.
- `GET /?q=&folder=&state=all|active|paused|archived&sort=pinned|updated|name&offset=0&limit=50` -> `{items,total,folders,stats}`.
- `POST /`, `GET /{id}`, `PUT /{id}`, `DELETE /{id}` (soft archive).
- `POST /preview`, `POST /{id}/refresh` (queue, no long fetch in write request).
- `GET /{id}/current?q=&offset=0&limit=50` -> `{group,items,total,summary,trade_date,previous_trade_date,source_time,status,warnings,selection_id}`; summary covers all members, independent of q/page.
- `GET /{id}/dates` -> `{dates:[ISO date]}`.
- `GET /{id}/selections?trade_date=&offset=&limit=` -> `{items:[{id,trade_date,selected_at,member_count,version,kind}],total}`.
- `GET /{id}/selections/{selection_id}?q=&offset=0&limit=50` -> batch summary + `items,total,summary` (frozen archive; maximum 200 items).
- `GET /{id}/trend?selection_id=&window_days=250` -> `{selection_id,status,base_date,points:[{date,value,change_pct}],expected_count,covered_count,warnings,updated_at,series_type:'fixed_basket_history'}`.
- `GET /{id}/intraday?trade_date=` -> `{points,status,trade_date}`.

Background source contract `snapshot -> {trade_date,previous_trade_date,source_time,rows:[dict],universe_codes:[str],status,warnings}`. Individual rows use the existing monitor field names plus `change_pct,pre_close,quote_time,amount_yi,market_value_yi,is_st,concepts,today_touched_limit_up,yesterday_limit_up,yesterday_continuous_days`.

Rule module exports `FIELD_DEFINITIONS`, `validate_rules(rules)`, `evaluate_rules(row,rules,match_mode)->bool|None`.

History module exports singleton `dynamic_group_history_service`, `request_build(selection_id,items,refresh=False)`, async `ensure_series(selection_id,items,refresh=False)`, async `get_series(selection_id,window_days=250)`; accepts frozen item dicts with `stock_code`.

## Task 1 — Rules and shared source (root)

Files: new `services/dynamic_group_rules.py`, `services/dynamic_group_source_service.py`, corresponding tests; minimal `live_market_stats_service.py` shared-snapshot extension if useful.

- [ ] Test tri-state all/any, missing vs zero, unit conversion, validation and actual trading-date quote filtering; observe failures.
- [ ] Implement whitelist fields, normalized comparisons, one shared market scan and coherent limit-up/yesterday enrichment.
- [ ] Verify source and rule tests; no existing stats regression.

## Task 2 — Persistence, group API and sampling (backend agent)

Files: new models/schemas/service/API `dynamic_group*`, model/router registrations, focused DB/API tests. Own no scheduler/main changes.

- [ ] Test CRUD paging/pinning/folders, versioning, archive, immutable selection and unknown preservation before implementation.
- [ ] Implement current rows, selection history and bounded batch queries; retain archived history; store member changes and intraday state atomically.
- [ ] Integrate source and history through lazy imports/interfaces above; expose async `evaluate_snapshot(snapshot)` and `tick()`/`request_refresh(id=None)` entrypoints.
- [ ] Validate group/selection ownership and concurrent/idempotent sampling; run focused tests and self-review.

## Task 3 — Historical index reconstruction (history agent)

Files: new `models/dynamic_group_history.py`, `services/dynamic_group_history_service.py`, focused tests; model registration coordinated with backend agent.

- [ ] Test generation-before-creation, fixed denominator, missing gaps, common base, source retries and revision caching before implementation.
- [ ] Fetch consistent Tencent adjusted day series with bounded concurrency, validate symbol/dates/prices; persistent reusable per-stock snapshots and immutable/revisioned per-selection outputs.
- [ ] Queue background warm-up; get_series reads DB and crops display without rebasing; support refresh after subsequent closes and old selections.
- [ ] Run focused tests and review index/provenance math.

## Task 4 — Multi-group responsive UI (frontend agent)

Files: new API/types/view/components/utils/tests, main router/App navigation.

- [ ] Implement API client from shared contract and test request-selection gate/rule helpers.
- [ ] Searchable/paginated/folder/pin directory, detail with history date+batch, historical line chart and frozen member list; empty states with useful templates.
- [ ] New/edit drawer with all/any conditions, membership mode, enabled/scope/folder; auto preview; archive/pause/pin actions.
- [ ] Mobile directory drawer, single-column cards, controlled horizontal table scrolling, resize-aware charts, keyboard labels.
- [ ] Run frontend tests/build; provide exact contract discrepancies to root.

## Task 5 — Integration, verification, release (root + review)

Files: scheduler/main, integration tests, spec/plan progress.

- [ ] Register trading-session sampling, close refresh and startup recovery; drain feature background tasks on shutdown.
- [ ] Run targeted integrated tests and a real-provider smoke probe; fix contract issues.
- [ ] Independent review of persistence/history correctness and mobile/many-group UI.
- [ ] Fresh full backend tests, frontend tests/build; inspect exit codes.
- [ ] Commit/push GitHub main then verified managed deployment; no local-only release.
- [ ] Verify public CRUD/history/index & frontend assets; desktop/mobile browser checks and fresh screenshots; report deployed commit and material data limits.
