# Trading Playbook Alert Review Fixes Design

## Scope

This change fixes only the four review findings on frozen commit `4e0bfbf`:

1. Drain durable action alerts before calendar and continuous-session gates.
2. Recover delivery failures that happen before the external channel call.
3. Terminalize historical plan-notification claims instead of sending or rotating them.
4. Preserve the complete action condition hash in the deduplication key.

## Action outbox drain

`TradingPlaybookAlertService.monitor()` first drains at most 100 existing action events ordered by `triggered_at ASC, id ASC`. The database query includes only `pending` events or explicitly recoverable `sending` events, excludes future action dates, and runs before calendar lookup or trading-session checks.

The action date is parsed strictly from the persisted action snapshot. Events dated before today become `skipped` with reason `stale`; malformed or missing action dates become `skipped` with reason `invalid_action_date`. They are terminal and never sent. Events dated today are sent through owner-fenced compare-and-set transitions. Future events remain untouched and cannot consume the current drain batch.

## Delivery state boundary

Claiming a pending event persists `sending` with an owner but without `channel_started_at`; this is the recoverable pre-send state. Settings lookup and locking remain under the same transaction and owner fence. Immediately before invoking the channel, the service persists `channel_started_at` while still holding the settings lock and verifying ownership.

Any database, lock, or settings exception before that fence is compensated through a fresh session: only the same owner's still-recoverable `sending` state may return to `pending`, and the error is recorded. A restarted monitor may also recover such a pre-send state. Once `channel_started_at` exists, channel-call and accepted-delivery failures keep the existing `uncertain` at-most-once behavior and are never changed back to `pending`.

## Historical notification claims

Notification compensation always derives an effective lower bound from the scheduler's current China-local date, even if calendar refresh fails or a caller supplies no lower bound. A referenced plan whose target date is before that bound is atomically completed with a stale diagnostic. A future plan may remain retryable when it is beyond a known upper bound. Calendar failure therefore permits only today's or future existing claims and never sends a historical plan.

## Deduplication key

Action deduplication keys include the full 64-character SHA-256 `condition_version`. The existing `TradingAlertEvent.dedup_key` column remains `String(255)`; tests assert both full-hash inclusion and that generated keys fit the column.

## Verification

Tests cover after-hours restart, calendar failure, next-day stale retirement, malformed action dates, two-instance races, the 100-row drain bound, pre-send settings/commit failures, channel-started uncertain behavior, healthy and failed-calendar historical notification claims, and full dedup-key length. Each production change follows a stable RED test and is committed separately by finding group.
