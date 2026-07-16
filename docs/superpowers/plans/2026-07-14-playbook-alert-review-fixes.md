# Trading Playbook Alert Review Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Correct the four frozen-review findings without entering Task11 or changing the alert schema.

**Architecture:** Keep delivery state in `TradingAlertEvent.channel_status_json`. Add an owner-fenced pre-send/channel-started boundary, drain at most 100 actionable outbox rows before monitor gates, retire stale notification claims using the existing job-claim terminal state, and use the complete condition hash in action deduplication keys.

**Tech Stack:** Python 3.11, asyncio, SQLAlchemy async ORM, SQLite/PostgreSQL JSON expressions, unittest/pytest.

---

### Task 1: Recover delivery failures before the channel boundary

**Files:**
- Modify: `backend/tests/test_trading_playbook_alerts.py`
- Modify: `backend/app/services/trading_playbook/alert_service.py`

- [ ] **Step 1: Write stable failing delivery tests**

Add tests to `TradingPlaybookDurableAlertTests` that create a pending event, fail `_lock_delivery_settings()` after `_claim_pending()` commits, and assert a fresh instance can deliver it exactly once:

```python
async def test_settings_lock_failure_returns_owned_pre_send_to_pending(self):
    first = TradingPlaybookAlertService(channel, session_factory=self.Session)
    first._lock_delivery_settings = AsyncMock(
        side_effect=RuntimeError("settings lock failed")
    )
    with self.assertRaisesRegex(RuntimeError, "settings lock failed"):
        await first._deliver(db, row)
    self.assertEqual(await persisted_status(event_id), "pending")
    await second._deliver(second_db, restarted_row)
    self.assertEqual(len(channel.sends), 1)
```

Add a second test that injects failure on the commit which persists `channel_started_at`, then asserts the event is `pending`, a second engine sends once, and no first-engine channel call occurred. Keep the existing accepted-send/delivered-commit tests as the regression for post-boundary `uncertain` behavior.

- [ ] **Step 2: Run RED tests**

Run:

```powershell
python -m pytest tests/test_trading_playbook_alerts.py -k "settings_lock_failure_returns or channel_started_commit_failure" -q
```

Expected: both tests fail because the row remains `sending` after a pre-send exception.

- [ ] **Step 3: Implement the minimal owner-fenced boundary**

In `_deliver`, wrap only pre-channel work in a `try` block. Add a compare-and-set method that persists `channel_started_at` only while status is `sending`, owner matches `self.owner`, and settings remain locked. Add fresh-session compensation that resets only the same owner's `sending` row with no `channel_started_at`:

```python
channel_status.update({
    "status": "pending",
    "pre_send_error": str(error),
    "recovered_at": now_cn().isoformat(),
})
channel_status.pop("owner", None)
channel_status.pop("sending_at", None)
```

The compensation update must fence on event id, `status == "sending"`, expected owner, and missing `channel_started_at`. Immediately before `channel.send`, persist `channel_started_at` under the same settings transaction. All exceptions after that commit continue through `_mark_uncertain_safely()` or `_recover_accepted_delivery()`.

- [ ] **Step 4: Run GREEN and delivery regressions**

Run:

```powershell
python -m pytest tests/test_trading_playbook_alerts.py::TradingPlaybookDurableAlertTests -q
```

Expected: all durable-delivery tests pass, including two-engine and delivered-commit regressions.

- [ ] **Step 5: Commit Task 1**

```powershell
git add backend/app/services/trading_playbook/alert_service.py backend/tests/test_trading_playbook_alerts.py
git commit -m "fix: recover playbook alert pre-send failures"
```

### Task 2: Drain the action outbox before monitor gates

**Files:**
- Modify: `backend/tests/test_trading_playbook_alerts.py`
- Modify: `backend/app/services/trading_playbook/alert_service.py`

- [ ] **Step 1: Write stable failing drain tests**

Add an action-event seeding helper that persists `pending`, recoverable `sending`, stale, future, and malformed rows. Add tests for these exact outcomes:

```python
async def test_after_hours_restart_drains_today_pending_action(self):
    await seed_action_event(self.today, status="pending")
    events = await service.monitor(db, CN_TZ.localize(datetime(2026, 7, 14, 15, 5)))
    self.assertEqual(len(channel.sends), 1)

async def test_calendar_failure_still_drains_today_action(self):
    calendar.ensure_error = RuntimeError("calendar offline")
    await seed_action_event(self.today, status="pending")
    await service.monitor(db, self.now)
    self.assertEqual(len(channel.sends), 1)

async def test_next_day_restart_terminalizes_stale_and_future_is_untouched(self):
    stale_id = await seed_action_event(date(2026, 7, 13), status="pending")
    future_id = await seed_action_event(date(2026, 7, 15), status="pending")
    await service.monitor(db, self.now)
    self.assertEqual(await status(stale_id), ("skipped", "stale"))
    self.assertEqual(await status(future_id), ("pending", None))
    self.assertEqual(channel.sends, [])
```

Add malformed/missing action-date retirement, two simultaneous monitor instances sending once, and 101 eligible rows where one call terminalizes or sends exactly 100 in `triggered_at ASC, id ASC` order.

- [ ] **Step 2: Run RED tests**

Run:

```powershell
python -m pytest tests/test_trading_playbook_alerts.py -k "after_hours_restart or calendar_failure_still_drains or next_day_restart or malformed_action or action_drain_batch or recoverable_sending" -q
```

Expected: after-hours and calendar-failure rows remain pending, stale/malformed rows are not terminal, and the unbounded/ordering assertions fail.

- [ ] **Step 3: Implement the bounded drain**

Add `_ACTION_OUTBOX_DRAIN_BATCH_SIZE = 100`. At the start of `monitor`, query action event types whose channel state is `pending` or `sending` without `channel_started_at`, ordered by `triggered_at ASC, id ASC`, limited to 100. Strictly parse `market_snapshot_json["trade_date"]` with `date.fromisoformat` and canonical ISO equality.

For each selected row:

```python
if action_date is None:
    await terminalize(event, reason="invalid_action_date")
elif action_date < trade_date:
    await terminalize(event, reason="stale")
elif action_date == trade_date:
    await recover_pre_send_if_needed(event)
    await self._deliver(db, event)
# action_date > trade_date remains unchanged
```

Terminalization and recovery use status/owner/channel-started compare-and-set predicates. Only after this drain completes may calendar and continuous-session gates return. Remove the old today-only pending delivery loop but keep today's existing-event query for candidate terminal-state evaluation.

- [ ] **Step 4: Run GREEN and action-monitor regressions**

Run:

```powershell
python -m pytest tests/test_trading_playbook_alerts.py::TradingPlaybookActionMonitorTests -q
python -m pytest tests/test_trading_playbook_alerts.py::TradingPlaybookDurableAlertTests -q
```

- [ ] **Step 5: Commit Task 2**

```powershell
git add backend/app/services/trading_playbook/alert_service.py backend/tests/test_trading_playbook_alerts.py
git commit -m "fix: drain action alerts before market gates"
```

### Task 3: Retire historical notification claims

**Files:**
- Modify: `backend/tests/test_trading_playbook_job_claims.py`
- Modify: `backend/tests/test_trading_playbook_scheduler.py`
- Modify: `backend/app/data_collectors/scheduler.py`

- [ ] **Step 1: Write stable failing historical-claim tests**

Seed a `draft` or `active` plan for `2026-06-02` with a retryable notify claim, run on `2026-07-14`, and assert `notify_plan_ready` is never awaited and the claim is completed with a stale reason. Run this once with a healthy calendar and once with `_ensure_playbook_calendar` raising:

```python
self.assertEqual(claim.status, "completed")
self.assertIn("stale target date", claim.last_error)
alert.notify_plan_ready.assert_not_awaited()
```

Update the scheduler calendar-failure expectation from `(None, None)` to `(today, None)`.

- [ ] **Step 2: Run RED tests**

Run:

```powershell
python -m pytest tests/test_trading_playbook_job_claims.py tests/test_trading_playbook_scheduler.py -k "historical_notification or calendar_failure_does_not_starve" -q
```

Expected: the historical plan is sent or remains retryable, and calendar failure passes no lower bound.

- [ ] **Step 3: Implement the lower bound and stale terminal state**

Initialize `notification_earliest_date = now.date()` before calendar lookup. Inside `_retry_incomplete_playbook_notifications`, compute:

```python
effective_earliest = earliest_target_date or self._playbook_now().date()
```

Classify `plan.target_trade_date < effective_earliest` as permanent with diagnostic `stale target date`; only `target_trade_date > latest_target_date` remains deferred. Reuse the existing completed-state compare-and-set update.

- [ ] **Step 4: Run GREEN and scheduler regressions**

Run:

```powershell
python -m pytest tests/test_trading_playbook_job_claims.py tests/test_trading_playbook_scheduler.py -q
```

- [ ] **Step 5: Commit Task 3**

```powershell
git add backend/app/data_collectors/scheduler.py backend/tests/test_trading_playbook_job_claims.py backend/tests/test_trading_playbook_scheduler.py
git commit -m "fix: retire stale playbook notification claims"
```

### Task 4: Preserve the complete condition version in action dedup keys

**Files:**
- Modify: `backend/tests/test_trading_playbook_alerts.py`
- Modify: `backend/app/services/trading_playbook/alert_service.py`

- [ ] **Step 1: Write the failing full-hash test**

Change the existing action event assertion to require the complete condition version and verify the model capacity:

```python
self.assertIn(f":{state.condition_version}:1", events[0].dedup_key)
self.assertEqual(len(state.condition_version), 64)
self.assertLessEqual(
    len(events[0].dedup_key),
    TradingAlertEvent.dedup_key.type.length,
)
```

- [ ] **Step 2: Run RED test**

Run:

```powershell
python -m pytest tests/test_trading_playbook_alerts.py -k "monitor_persists_entry_and_restart" -q
```

Expected: the key contains only the first 16 condition-version characters.

- [ ] **Step 3: Implement the minimal key change**

Replace `condition_version[:16]` with `condition_version` in `_action_dedup_key`.

- [ ] **Step 4: Run GREEN and model/API regressions**

Run:

```powershell
python -m pytest tests/test_trading_playbook_alerts.py tests/test_trading_playbook_models.py tests/test_trading_playbook_api.py -q
python -m py_compile app/services/trading_playbook/alert_service.py app/data_collectors/scheduler.py tests/test_trading_playbook_alerts.py tests/test_trading_playbook_job_claims.py tests/test_trading_playbook_scheduler.py
git diff --check
```

- [ ] **Step 5: Commit Task 4**

```powershell
git add backend/app/services/trading_playbook/alert_service.py backend/tests/test_trading_playbook_alerts.py docs/superpowers/plans/2026-07-14-playbook-alert-review-fixes.md
git commit -m "fix: retain full action condition version"
```
