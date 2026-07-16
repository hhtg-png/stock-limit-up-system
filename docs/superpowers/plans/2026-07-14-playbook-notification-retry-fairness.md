# Playbook Notification Retry Fairness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the bounded notification compensation queue fair, terminalize permanently invalid notification claims, and preserve calendar-failure and completed-claim safety.

**Architecture:** Keep the existing `TradingPlaybookJobClaim` schema and notification claim workflow. Select at most 100 incomplete notification claims by oldest `updated_at`, rotate transient failures through the existing `fail()` timestamp update, and mark malformed or permanently unresolvable selected claims completed with a diagnostic `last_error` so they cannot consume later batches.

**Tech Stack:** Python 3.11, asyncio, SQLAlchemy async ORM, SQLite, pytest/unittest.

---

### Task 1: Reproduce starvation and poison-claim behavior

**Files:**
- Modify: `backend/tests/test_trading_playbook_job_claims.py`

- [x] **Step 1: Add a failing fairness test**

Seed 100 retryable notification claims whose sends continue to fail and one valid claim outside the first batch. Run compensation twice and assert the outside claim is attempted on the second run because the failures moved to the queue tail.

- [x] **Step 2: Add failing poison-claim tests**

Seed malformed generation keys, missing plans, non-notifiable plans, unrelated phase claims, and a completed claim. Assert malformed/missing/non-notifiable selected notification claims become terminal with a reason, while unrelated and completed claims are never attempted or rewritten.

- [x] **Step 3: Add the calendar-fallback bound test**

Force calendar lookup failure, seed more than 100 existing notification claims plus a historical plan without a notification claim, and assert one monitor iteration attempts no more than 100 existing claims and creates no historical notification claim.

- [x] **Step 4: Run RED tests**

Run:

```powershell
python -m pytest tests/test_trading_playbook_job_claims.py -k "fair or invalid or calendar or completed" -q
```

Expected: failures show descending selection starves outside claims and invalid selected rows remain `retry`.

### Task 2: Implement oldest-due rotation and poison retirement

**Files:**
- Modify: `backend/app/data_collectors/scheduler.py`

- [x] **Step 1: Select claim rows oldest first**

Change notification compensation to select full incomplete `job_type='plan'`, `phase='notify'` rows ordered by `updated_at ASC, id ASC`, limited to 100.

- [x] **Step 2: Classify selected claims**

Parse `generation_key`, load referenced plans once, and classify malformed keys, missing plans, and statuses outside `draft/confirmed/active` as permanent failures. Keep date-window exclusions retryable and rotate them rather than synthesizing notifications.

- [x] **Step 3: Terminalize permanent failures**

Conditionally update only still-incomplete selected rows to `status='completed'`, clear the lease, set `completed_at` and `updated_at`, and retain a bounded diagnostic in `last_error`. Do not touch unrelated phases or completed rows.

- [x] **Step 4: Retry eligible plans**

Call the existing `_notify_trading_playbook_plan` for eligible selected plans. Its existing `fail()` path updates `updated_at`, moving transient failures to the queue tail.

- [x] **Step 5: Run GREEN tests and regressions**

Run:

```powershell
python -m pytest tests/test_trading_playbook_job_claims.py tests/test_trading_playbook_scheduler.py -q
python -m py_compile app/data_collectors/scheduler.py tests/test_trading_playbook_job_claims.py
git diff --check
```

- [ ] **Step 6: Commit**

```powershell
git add backend/app/data_collectors/scheduler.py backend/tests/test_trading_playbook_job_claims.py docs/superpowers/plans/2026-07-14-playbook-notification-retry-fairness.md
git commit -m "fix: rotate playbook notification retries fairly"
```
