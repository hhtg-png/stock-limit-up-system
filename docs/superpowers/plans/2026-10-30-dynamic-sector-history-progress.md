# Dynamic sector implementation progress

Branch: `codex/dynamic-sector-history-20261030` in isolated worktree `C:/Users/Administrator/.codex/worktrees/monitor-auction-history/stock-limit-up-system`.
Base production: `b2206db0ea6054a471f7626457bb822ad8086279`; initial design commit `4b55312`.
User explicitly opted out of brainstorming and requested direct implementation, many groups and mobile support. No more design approval is required. Original dirty `D:/code/stock-limit-up-system` remains untouched.

## Implemented

- Shared full-market quotes (also reused by existing market stats), tri-state whitelist rules, real trading-day/quote-time validation.
- Database groups/versions/entry-exit events/frozen selections/current members, per-day segmented intraday index, jobs and startup/recovery scheduling, resource cleanup.
- Durable consistent-adjusted stock/index revisions, generation-before-creation fixed-basket index, reliable shared base, gaps/coverage, 800-row segmented retrieval with overlap equality, job recovery and continued archived-basket updates.
- Multi-group search/folder/pin/paging UI, responsive desktop directory and mobile drawer/cards, editor/templates/auto-preview, immutable history day/batch view, charts with zoom retention.
- Current/selection member APIs now server-paged (50 default, 200 max), SQL cohort summary independent of search/page. Thin payloads; existing trend reads no full member JSON.

## Review fixes completed and independently reverified

- Changing semantic rules starts a fresh current version, preserves prior archives and source-time watermark.
- Same-price-slot recovered context can re-evaluate using input-signature claims and signature batch keys.
- Unsettled exits cannot keep an old basket and earn exited stocks' later gains; gap/new segment rebase instead.
- Missing completed-day tails produce gaps; regressed coverage preserves previous successful revision as stale.
- Cross-service cold stock-revision/cache unique-key races retry complete transactions.
- Scope reviewers `/root/review_dynamic_backend` and `/root/review_dynamic_history_final` both passed final scoped rechecks.

## Verification evidence so far

- Backend focused service/API after fixes: 39 tests +5 subtests passed; history 18 tests passed; source/rules/shared market/scheduler/lifecycle/query focused all passed.
- Frontend latest `npm test`: 171 passed; latest `npm run build`: exit 0. Logs originally `.tmp/dynamic-ui-qa`, moved to `%TEMP%/dynamic-group-ui-qa-20261030` (outside git).
- Full backend `python -m unittest discover tests -v`: 1291 tests, 441.755 seconds, exit 0, OK (skipped=2). Log `%TEMP%/dynamic-groups-backend-full.log` (session 20796 completed). A subsequent small public-trend projection removes repeated per-stock provenance while retaining it in DB; focused query 4 + API 3 tests passed after that change. EOF-only whitespace normalization followed.
- Real market source smoke: 5223/5223 Shanghai/Shenzhen rows, actual 2026-10-30, previous 2026-10-29.
- Real temporary DB/API/browser smoke: 61 groups, fixed basket 600519/600838, 727 history points, base 2023-10-30, latest 2026-10-29. Mobile form create/save succeeded; directory auto-search succeeded; history batch UI verified.
- Browser viewport 390: document scrollWidth 390, main scrollWidth equals clientWidth. Agent also checked 1440 with fixture 121 groups. Temporary tabs closed; viewport reset; user's original 3 tabs untouched.
- Real temporary browser screenshot artifacts in visualization workspace: `dynamic-groups-real-history-desktop.png`, `dynamic-groups-real-history-mobile.png`. They show LOCAL verification data, not online deployment.
- Temporary API server exec session 82308 was stopped (Ctrl-C; exit 1); 8175 has no listener.

## Initial deployment and production verification

- Feature commit `f6a2df50f915b7b5f9e3a71831f5250615da5f4f` was pushed to GitHub main and deployed through the managed deployment wrapper. Remote HEAD matched and systemd was active.
- Public route, frontend asset/hash, 12 new tables, 23 rule fields, empty directory, real 5223-stock preview and baseline stock/fixed-board APIs passed. Production groups remain empty; capacity fixtures were never inserted.
- Actual production desktop/mobile UI verified. At 390px, document scrollWidth was 390 with no horizontal overflow; mobile navigation reached the module. Production screenshots saved as `dynamic-groups-online-f6a2df5-desktop.png` and `dynamic-groups-online-f6a2df5-mobile.png` in the visualization workspace. User's original tabs remain untouched.
- Extra server-side history smoke uses a temporary SQLite database, never production data. It exposed HTTP 501 from Tencent's old `fqkline` endpoint. Response HTML explicitly identified Tencent WAF; browser UA, raw commas and alternate old-endpoint host also failed.
- A read-only server comparison verified the primary AkShare `newfqkline` endpoint. Injected into the temporary database, it produced ready history with 727 points, base 2023-10-30, latest 2026-10-29, coverage 2/2. It ignores the requested start and has different precision, so the formal fix must filter dates and use one endpoint throughout each acquisition.
- Formal provider fix now uses that one endpoint for every page, records `tencent_newfqkline`, keeps raw 800-row lengths until pagination finishes, filters the requested start afterward, and rejects end-date violations. Historical coverage/overlap validation and preservation of previous successful revisions remain intact.
- Fix verification: 20 history tests passed; independent review passed. Candidate code in an isolated server process retrieved 1652 real historical prices across multiple pages (2020-01-02 to 2026-10-29) and built the real 727-point, 2/2-coverage basket. No production database writes.
- Fresh frontend build after the provider fix passed. Fresh full backend suite passed: 1294 tests in 433.393 seconds, exit 0, OK (skipped=2), log `%TEMP%/dynamic-groups-provider-backend-full.log`. This includes both new provider regressions and the prior public-trend projection.

## Remaining

1. Fresh full backend run, frontend build and independent provider review have passed.
2. Commit, push GitHub main and deploy through the same wrapper.
3. Rerun public probes and the temporary server-side history smoke using `get_series(window_days=0)` so the default 250-point display limit does not invalidate a full-history assertion.
4. Final browser check/screenshot, reset viewport and close temporary tab. Final Chinese outcome/link, many-group/mobile support, archives, pre-generation historical index and deployment checks. No further permission request.

## Operational helpers

SSH credential loader (never print secrets): `C:/Users/Administrator/.codex/skills/stock-limit-up-deploy/scripts/deploy_stock_limit_up.py` imported with importlib; `ssh_client(find_password())`.
Remote root `/opt/stock-limit-up-system`, backend venv, SQLite `backend/data/stock_limit_up.db`, systemd `stock-backend.service`.
Existing release probe `%TEMP%/fixed-board-release-probe.py --expected-head SHA` validates baseline release/head/entry and fixed-board filters; add dynamic module probe for new feature.

## Remaining material limits

- Large baskets' first historical warm-up runs in background; don't claim instantaneous complete history for thousands of stocks.
- Fixed-basket historical curve is labeled composition backcast, not historical rule backtest. Current historical source only accepts verified qfq series; unsupported provider responses retry and preserve saved history.
- Topic metadata is local with explicit coverage/update date; unknown fields do not become false or zero.
