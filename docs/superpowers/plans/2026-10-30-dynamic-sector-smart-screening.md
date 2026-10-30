# Dynamic Sector Smart Screening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox syntax for tracking.

**Goal:** Turn natural-language stock-selection ideas into verified, editable, automatically updated dynamic sectors.

**Architecture:** A bounded model interpreter emits a locally validated rule definition. The existing deterministic group service executes nested expressions and database-backed recent-session features, and persists the interpreted strategy with group versions. A responsive smart editor exposes the understanding, questions and real preview before save.

**Tech Stack:** Existing FastAPI/Pydantic/SQLAlchemy/httpx backend; Vue/Element Plus frontend; configured DeepSeek JSON chat completions.

**Spec:** `docs/superpowers/specs/2026-10-30-dynamic-sector-smart-screening-design.md`

## Global Constraints

- Work only in the isolated monitor-auction-history worktree, branch `codex/dynamic-sector-smart-screening-20261030`; original dirty `D:/code/stock-limit-up-system` remains untouched.
- User opted out of brainstorming; execute directly without another approval gate.
- Preserve old groups and immutable archives. Never execute model code/SQL or silently replace unsupported user requirements with defaults.
- Historical windows are 1–60 completed exchange sessions, excluding current intraday data; unknown coverage stays unknown.
- Keep credentials server-side and out of outputs. No production fixture groups.

## Review Focus

- A fluent model reply that omits a difficult clause must not become an apparently complete plan.
- A missing historical trading day must not turn into zero historical events.
- Unknown ranking candidates must not become confirmed exits.
- Editing input during a slow response must not enable saving a stale plan.
- New nested nodes and strategy metadata must not break old flat editors, archived batches or schema migration.

### Task 1: Deterministic rule and historical execution

**Owner/files:** Backend worker; `dynamic_group_rules.py`, new `dynamic_group_features_service.py`, group schema/model/database/service/query files and focused backend tests.

**Interfaces:** Extend the existing rules and `DynamicGroupDefinition` exactly as specified. Provide `iter_rule_leaves(rules)` for safe nested traversal; `DynamicGroupFeatureService.enrich_snapshot(snapshot, rulesets)` for shared database facts; preserve group service preview/evaluation APIs and optional strategy.

- [ ] Add failing tests for nested all/any, depth/leaf bounds, historic day parameters, exact 5% threshold, unverified sessions, ranking/unknowns and legacy definitions.
- [ ] Implement strict canonical rule validation, shared historical enrichment and strategy persistence/migration.
- [ ] Integrate preview and automatic membership evaluation, deterministic ties and immutable historical definitions. Run focused rules/source/service/query/API tests.

### Task 2: Natural-language interpreter API

**Owner/files:** Model worker; new `dynamic_group_interpreter_service.py`, `schemas/dynamic_group_interpreter.py`, API route and interpreter tests. Coordinate imports with Task 1; no edits to its owned files.

**Interfaces:** Implement the exact POST `/interpret` request/response contract. Service `interpret(prompt, current_definition=None)` returns the response shape. Read current settings per call; emit locally validated definitions using Task 1 schema.

- [ ] Write and observe failing tests for a supported definition, refinement, unknown field, unsupported/ambiguous clauses, malformed/truncated JSON, model errors and secrets protection.
- [ ] Implement a bounded real provider request using current configured model, strict parsing, explicit capability catalogue and no fallback stock rules.
- [ ] Register the static route before group-ID routes and run interpreter/API tests. Expose no creation or persistence side effects.

### Task 3: Responsive smart selection editor

**Owner/files:** Frontend worker; dynamic-group types/API/utils/editor and focused smart-editor component/tests; preserve main directory/history behavior.

**Interfaces:** Consume Task 2 response shape, Task 1 nested rules/optional strategy. Smart create is the default; manual mode remains available. Preview/save use existing definition APIs.

- [ ] Add failing interaction tests for interpretation, questions/unsupported state, stale-response edits, refinement and old manual flow.
- [ ] Implement natural-language input, examples, clear interpreted conditions/assumptions, real preview and guarded save. Preserve rule trees without flattening them silently.
- [ ] Run frontend tests/build and verify 1440px/390px layouts with local temporary fixtures only.

### Task 4: Integration, review and deployment

**Owner:** Root integrates and reviews all boundaries with a fresh independent reviewer.

- [ ] Incorporate the user's actual description as an acceptance case if supplied; otherwise use the public real-data example stated in the spec and report supported limits precisely.
- [ ] Run real configured-provider + actual-quote preview in an isolated server process/database; verify no production writes and unsupported-clause preservation.
- [ ] Run full backend tests and frontend tests/build; inspect exit codes, commit/push to origin/main, deploy managed service and verify public schema/interpretation/assets/baseline APIs.
- [ ] Save a fresh desktop/mobile screenshot, clean temporary tools/tabs and deliver the working module link with concise validation evidence.
