# Smart screening recovery ledger

## Task and authorization

- User clarified that fixed dropdown conditions are too rigid and expects intelligent recognition of natural-language screening requests.
- Root asked for a real description asynchronously; no answer yet. Optional example is still pending; implementation proceeds with the stated example. User previously explicitly opted out of brainstorming and authorized direct implementation/deployment.
- Branch `codex/dynamic-sector-smart-screening-20261030`, isolated worktree `C:/Users/Administrator/.codex/worktrees/monitor-auction-history/stock-limit-up-system`. Original dirty `D:/code/stock-limit-up-system` untouched.
- Smart-screening feature was committed as `d960602` and deployed. Display labels were polished in `3c9713f0cebbc0f61c6cffc1ddaa03d6cc666ee1`, which is now local HEAD, GitHub main, and production HEAD. Backend code is identical between those two commits.
- Design and plan are adjacent `2026-10-30-dynamic-sector-smart-screening*` documents. No approval gate remains.

## Worker implementation status

- `/root/review_dynamic_backend` Task1 DONE: nested all/any, max30leaves/depth4; historical fields with days1..60; strategy metadata/ranking (explicit days for historical); compatible SQLite/Postgres strategy column; shared verified historical enrichment; preview/actual same ranking/TopN; stable ties; partial unknown preservation; immutable batches; 5% float boundary fix. Main focused 80 tests +39 subtests passed, final empty-facts correction focused30 +21 subtests passed. Product stable.
- Empty facts plus metric0 was caught by review and fixed RED→GREEN. Nonempty trustworthy zero-event days remain valid. Windows use latest completed session (during market exclude today; after15:05 + final quote/archive may include today).
- `/root/implement_dynamic_history` Task2 initially DONE17tests, now ACTIVE again fixing independent-review and REAL-provider failures; see blockers below. Owns interpreter service/schema/API route/new tests. Do not edit these concurrently.
- `/root/implement_dynamic_frontend` Task3 DONE179tests (176 direct+3 wrapper), latest vue-tsc/Vite build exit0. Owns types/API/utils/editor/new SmartInput/controller/tests/view summary changes. Default new AI groups dynamic; old flat manual retain. Nested trees are preserved and shown read-only in manual mode. Smart save requires current ready interpretation and completed real preview. Legacy flow retained.
- Frontend worker's browser surface was unavailable, but ROOT `dynamicBrowser` ID2 remains available and sees the original3 user tabs. Root must do visual QA. No worker-created tab/viewport changes.
- `/root/review_dynamic_history_final` independent backend review DONE, found LLM blockers below; no other clear backend issues. Can be reused for final recheck once fixes land.

## Root changes and evidence

- Added scheduler `_queue_dynamic_group_archive_refresh`: final market-review build/repair/reconcile success schedules coalesced one-shot DateTrigger `_recover_dynamic_groups` +1sec. Necessary because periodic quote sampling ends15:05 before final facts/20:15 repair. No 24-hour quote polling or blocking archive build.
- New `tests/test_dynamic_group_archive_refresh.py`: observed RED (three missing refresh assertions), then3tests GREEN. Existing market-review scheduler23tests GREEN. Root owns these scheduler files.
- Read-only production history coverage: last10 completed sessions primary, calc_version1/2; touch sums match metrics; latest5 dates10/29,28,27,26,23 complete. Helper `%TEMP%/dynamic-groups-smart-coverage.py`.
- Real configured-provider minimal JSON smoke passed HTTP200, model `deepseek-flash`, thinking disabled, valid JSON. Helper `%TEMP%/dynamic-groups-smart-provider-smoke.py`; never prints keys.
- Official docs browsed for JSON mode and chat completions: `https://api-docs.deepseek.com/guides/json_mode/`, `https://api-docs.deepseek.com/api/create-chat-completion/`.

## Resolved interpreter review findings

1. Real provider returns semantically correct `definition.rules={all:[...]}` object instead of list. `_compile` currently iterates dict keys and errors. Need safe singleton composite wrapping ONLY for LLM input, plus explicit full JSON list example. Never discard conditions. Generic JSON-only retry currently doesn't repair local-schema errors.
2. Real provider references actual ranking field `amount_yi` in clauses although it is not a filter leaf. Compiler references set must include validated ranking.field as actually executed.
3. Blanket `_UNSUPPORTED` keyword prefilter rejects '均线不做要求' and '取消均线条件' before any model call. Must understand explicit cancelled/ignored requirements while still rejecting active '不要低于5日均线'.
4. `_RETAIN_INTENT` turns '不要持续跟踪…不达标就退出' into retain despite model dynamic.
5. ST audit only recognizes eqfalse and rejects equivalent ne true (including valid OR).
6. Whole-sentence clause quoting can hide omitted leaf: '排除ST，股价低于10元' + only is_st false incorrectly ready. Need conservative explicit-metric coverage audit; do not claim formal proof of arbitrary semantics.

All six were fixed with RED→GREEN regressions. An additional cancelled-but-still-executed metric/ranking issue was found and fixed. Independent final recheck passed the exact cancelled/retained/reintroduced cases; interpreter27tests passed. No open scoped review findings remain.

## Real-model helper and UI fixture

- `%TEMP%/dynamic-groups-smart-candidate.py` dynamically reads4 local candidate modules (rules/group schema/interpreter schema/service), loads them into a separate server Python process using configured settings; no production DB writes.
- Its log `%TEMP%/dynamic-groups-smart-candidate.log` now shows PASS/exit0: real configured DeepSeek successfully interpreted mainboard nonST, recent5completed-session closing limit-up >=1, today's opening2..5%, amount descending top20; refinement changed opening1..4% and added amount>=3yi while retaining other leaves. Unsupported MA/volume returned no executable definition. No production DB writes.
- `%TEMP%/dynamic-groups-smart-data-candidate.py` fed that real model definition into candidate preview with a read-only production history connection and real full-market quotes. PASS/exit0: 5223 stocks,14 matches,0 unknown; completed dates10/26..10/30 all5verified, sorted correctly. No production group writes. Initial helper cleanup used a nonexistent market-service close method, then helper-only correction closed the owned Tencent client; rerun passed.
- Frontend fixture was `http://127.0.0.1:3339/dynamic-groups`, PID49768. It has stopped; root confirmed no listener/PID. QA script, README and logs moved out of git to `%TEMP%/dynamic-groups-smart-ui-qa-20261030`. It was in-memory and blocked create writes.
- Root CUA persistent `dynamicBrowser` ID2 and `dynamicFs` fs handle remain valid. Existing production verification tab was closed after prior release; original3 user tabs only. Use a NEW temp tab for local3339 visual QA, reset viewport and close afterward.
- Root verified smart-create, fixture interpretation/preview wiring, 1440px/390px responsive layout and dirty-prompt disabled save. Local fixture output is deliberately fake and does NOT prove model semantics; actual model proof is the server helper. Mobile document width390, drawer/body no horizontal overflow. Screenshot `smart-screening-local-mobile.png` saved. Viewport RESET and `smartVerifyTab` CLOSED; original3tabs untouched.
- Independent frontend review caught lost refinement context when the field catalogue was missing/invalid. Two real editor harness tests observed RED; worker fixed it, full frontend181tests and production build passed. Old preview race checks also passed. Logs in the moved QA folder.
- Fresh full backend PASSED: 1343 tests in443.740seconds, exit0, OK(skipped2), log `%TEMP%/dynamic-groups-smart-backend-full.log`; run started after interpreter27tests and all product backend fixes were stable. No backend product changes since. Latest frontend181tests and production build passed.
- Public post-deploy helper prepared: `%TEMP%/dynamic-groups-smart-release-probe.py --expected-head FULLSHA` validates HEAD/service/assets/strategy migration, a real provider interpretation with counterexample rows, full-market historical preview/ranking and unsupported rejection, without creating groups.

## Remaining

1. Production browser verification passed: intelligent create interpreted the example and showed14 real matches; no group was saved. Production groups remain0. Screenshot saved at `C:/Users/Administrator/.codex/visualizations/2026/09/15/01a0a31f-768a-7aa1-93a2-5203b7f1bd82/smart-screening-online-3c9713f.png`. Browser viewport reset; verification tab closed; original3tabs untouched. Local QA server also stopped.
2. Final repeat public probe on3c9713f found intermittent interpretation error. Further instrumented real calls proved malformed model JSON, specifically a `strategy.assumptions` array closed with `}` rather than `]`, with nesting/closing damage. Logs `%TEMP%/dynamic-groups-smart-diagnose-exact.log` and `...-repeat.log`. Multiple successful calls alone do not resolve this blocker.
3. Minimal fix is implemented and independently reviewed: model no longer duplicates strategy.prompt/assumptions (local compiler fills both); single format repair receives safe failed output plus JSON line/column/character feedback. Max2 calls/total timeout and strict validation remain. Real broken-response fixture observed RED→GREEN;31 focused tests pass. Reviewer independently confirmed1.016s timeout on a1s total budget and that a repaired reply omitting ST still cannot execute. No open scoped findings.
4. Root real-provider validation after this fix passed: two supported descriptions plus refinement all returned valid JSON on their first response; unsupported indicators still rejected. All3 executable plans passed positive and counterexample evaluation for board/ST/history/range/ranking/refinement preservation. Fresh frontend build passed6.54s. Fresh full backend PASSED1347tests in441.185s, exit0, OK(skipped2), log `%TEMP%/dynamic-groups-smart-repair-backend-full.log` (session36999 closed).
5. Latest unchanged frontend181tests passed after label patch, and its new dist is verified. After backend success, update ledger, commit/push HEAD:main, managed deploy via `%TEMP%/monitor-context-requested-date-deploy.py` (clean tree, HEAD==origin/main, verified dist). Include new fixture in commit. Never print/store server password.
6. Verify public /interpret (real configured model), real preview/ranking/history coverage, baseline monitor APIs/assets/HEAD/service; no fixture groups in production. Existing production screenshot is valid if UI unchanged. Do not claim complete until public probes pass.
7. Final concise Chinese module link and natural-language workflow with supported boundaries, commit/ref/remote/API/assets verification, and embedded screenshot.
