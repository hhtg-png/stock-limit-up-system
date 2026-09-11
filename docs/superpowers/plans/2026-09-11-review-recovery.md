# Review recovery and current-day display

Goal: prevent missed closing jobs leaving statistics stuck on yesterday, and never substitute stored history for an intraday snapshot.

Design: closing jobs tolerate 900 seconds lateness. A five-minute reconciliation checks the latest completed trading session, including intraday version and missing detail rows, then rebuilds only incomplete reviews. Calendar I/O runs in a worker thread. A shared scheduler lock serializes review build/repair/reconciliation. Source failures remain retryable.

Current-day snapshot: during trading hours collect live data only; failures return an explicit unavailable response with the requested date and empty detail/ladder. Outside trading hours use only the exact requested date. Historical chart rows remain history, never the current-day detail. Frontend clears stale current-day state on failures, and live fetch does not depend on history fetch success.

- [x] Add failing scheduler, database completeness, API and frontend behavior tests.
- [x] Implement delayed-job tolerance, periodic reconciliation and serialization.
- [x] Implement strict current-day snapshot and unavailable presentation.
- [x] Run backend suite, frontend suite and build; review diff.
- [ ] Commit and push to origin/main, deploy with shared script, verify public endpoints and scheduled job registration.


Additional accepted requirements: add a separate line chart for stocks that had at least two consecutive limit-up sessions two trading days ago and failed to seal yesterday. For each target date, use the previous trading session's review cohort and target-date quotes. Tooltip includes each name/code/previous board height/return. Use adjusted historical daily closes, then EastMoney daily return fallback for missing history. Null means unavailable, never zero; partial cohorts do not produce a misleading mean. Audit existing archive dates before backfill. Live snapshot remains usable when history fails. Slow chart requests have independent cancellation identity so one-minute refresh does not discard them forever.

Production audit: review archive begins 2026-04-10, ends 2026-09-11. No missing calendar sessions and no calc_version=0-only records after the manual Sep 11 repair. Chart historical price holes are distinct from missing review days and are retried against an alternate source.
