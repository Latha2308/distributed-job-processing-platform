# QUEUED-Job Reconciliation: Outbox Deduplication Predicate

**Status:** Step 289B Implementation Specification
**Purpose:** Define precise deduplication logic for `job_queued_recovery` events

---

## Problem Statement

The QUEUED-job reconciler must create durable recovery events in the outbox without:
1. Creating duplicate events for the same recovery cycle
2. Blocking recovery when a previous publication failed
3. Suppressing recovery permanently due to stale events
4. Creating unbounded events during transient outages

## Deduplication Predicate

The reconciler MUST check for existing outbox events before creating a new `job_queued_recovery` event.

### Query Predicate

```sql
SELECT id FROM job_outbox
WHERE job_id = :job_id
AND event_type = 'job_queued_recovery'
AND published = false
AND reconciliation_required = false;
```

### Decision Logic

**If existing event found:** SKIP event creation, UPDATE throttling timestamp only
**If no existing event found:** CREATE new event, UPDATE throttling timestamp

### Rationale

This predicate distinguishes between five event states:

#### State 1: Unpublished, Awaiting First Publication
```
published = false
reconciliation_required = false
attempts = 0
next_publication_attempt_at = NULL
```

**Meaning:** Event created, never published to Redis yet.

**Action:** SKIP event creation. Existing event provides durable delivery intent.

**Why:** Creating another event would duplicate the recovery message when both events eventually publish.

#### State 2: Unpublished, Awaiting Retry After Failed Publication
```
published = false
reconciliation_required = false
attempts > 0
next_publication_attempt_at = <future_timestamp>
```

**Meaning:** Redis publication failed (connection error, timeout), retrying with exponential backoff.

**Action:** SKIP event creation. Existing event will retry automatically.

**Why:** The failed publication is a transient error. Exponential backoff will reattempt. Creating another event would cause duplicate messages when the backoff expires and both events publish.

#### State 3: Published, Message Delivered to Redis
```
published = true
published_at = <timestamp>
```

**Meaning:** Event successfully published to Redis. Message was delivered (may or may not still be in Redis).

**Action:** CREATE new recovery event (predicate does NOT match published=true).

**Why:** The original message MAY have been lost (Redis crash, eviction, race condition between ZPOPMIN and worker claim). At-least-once delivery requires bounded republication. The throttling timestamp prevents unbounded duplication.

**Note:** We do NOT assume a published message is still in Redis. Message loss is the reason reconciliation exists.

#### State 4: Reconciliation Required (Invalid/Undeliverable)
```
published = false
reconciliation_required = true
last_error = <diagnostic_message>
```

**Meaning:** Event determined to be invalid or permanently undeliverable. Excluded from automatic retry.

**Examples:**
- NULL retry_attempt for job_recovered event (invalid state)
- Stale retry_attempt (job has moved to higher retry generation)
- Job not found in database
- Job status mismatch (expected QUEUED, found PROCESSING)

**Action:** CREATE new recovery event (predicate does NOT match reconciliation_required=true).

**Why:** The reconciliation_required flag marks INVALID events, not QUEUED jobs that need recovery. A QUEUED job with a stale invalid event should still be eligible for recovery with a new valid event.

**Critical distinction:** reconciliation_required applies to the EVENT (invalid, undeliverable), NOT to the job (stuck, needs recovery).

#### State 5: Published Event, Bounded Republication
```
published = true
published_at = <old_timestamp>
job.last_recovery_attempt_at = <old_timestamp>
```

**Meaning:** Previous recovery event was published, but throttle period has expired.

**Action:** CREATE new recovery event (predicate does NOT match published=true).

**Why:** Original message may have been lost. Throttling timestamp allows bounded republication (max 24 times/day).

**Bounding mechanism:** Throttling timestamp prevents unbounded republication. Each republication updates `last_recovery_attempt_at`, requiring 1-hour delay before next attempt.

---

## Event Type Distinction

### job_queued_recovery (NEW - Step 289B)
- **Purpose:** Recover QUEUED jobs that may never have been delivered to Redis
- **Trigger:** Reconciler detects job in QUEUED status > age_threshold, respecting throttle
- **Characteristics:**
  - `event_type = 'job_queued_recovery'`
  - `retry_attempt = NULL` (not a retry, initial delivery may have failed)
  - Published to main job queue (not retry queue)
  - Job status remains QUEUED (unchanged)
  - Job retry_count remains unchanged

### job_created (Existing)
- **Purpose:** Initial job creation event
- **Trigger:** Job created via API
- **Characteristics:**
  - `event_type = 'job_created'`
  - `retry_attempt = NULL` (initial creation)
  - Published to main job queue
  - Job status = PENDING → QUEUED

### job_recovered (Existing - PROCESSING recovery)
- **Purpose:** Recover PROCESSING jobs that timed out (worker crash)
- **Trigger:** Recovery service detects PROCESSING job > timeout
- **Characteristics:**
  - `event_type = 'job_recovered'`
  - `retry_attempt = <retry_count>` (immutable retry generation identifier)
  - Published to retry queue (delayed delivery)
  - Job status = PROCESSING → RETRYING
  - Job retry_count incremented

---

## Concurrency Safety

### PostgreSQL Row Locks (Not Python Checks)

**Unsafe (race condition):**
```python
# Check in Python - WRONG
existing = await session.execute(select(JobOutbox).where(...))
if existing:
    return  # RACE: Another reconciler might insert between check and return
```

**Safe (database constraint):**
```python
# Lock job row first with FOR UPDATE SKIP LOCKED
job = await session.execute(
    select(Job).where(Job.id == job_id).with_for_update(skip_locked=True)
)

# Within the same transaction, check outbox
existing = await session.execute(select(JobOutbox).where(...))
if existing:
    # Update throttling timestamp
    job.last_recovery_attempt_at = datetime.utcnow()
    await session.flush()
    return

# Create new event
outbox_event = JobOutbox(...)
session.add(outbox_event)
job.last_recovery_attempt_at = datetime.utcnow()
await session.flush()
```

**Why this works:**
1. `FOR UPDATE SKIP LOCKED` on job row prevents concurrent reconcilers from processing same job
2. Outbox check and event creation happen within the same transaction holding the job lock
3. If two reconcilers somehow reach the same job, only one acquires the lock
4. The other gets `None` from `with_for_update(skip_locked=True)` and moves to next job

---

## Stale Event Scenarios

### Scenario 1: Failed Publication, Backoff Not Expired
```
Event: published=false, attempts=3, next_publication_attempt_at=now+2min
Job: status=QUEUED, last_recovery_attempt_at=now-10min
```

**Decision:** SKIP event creation (existing event found by predicate)

**Why:** Outbox dispatcher will retry automatically when backoff expires. Creating another event causes duplicate messages.

### Scenario 2: Published Event, Message Lost, Throttle Expired
```
Event: published=true, published_at=now-2hours
Job: status=QUEUED, last_recovery_attempt_at=now-2hours
```

**Decision:** CREATE new event (predicate does NOT match published=true)

**Why:** Original message may have been lost. Throttle expired (2 hours > 1 hour interval). Bounded republication.

### Scenario 3: Invalid Event, Job Still QUEUED
```
Event: published=false, reconciliation_required=true, last_error="Job not found"
Job: status=QUEUED, created_at=now-30min, last_recovery_attempt_at=NULL
```

**Decision:** CREATE new event (predicate does NOT match reconciliation_required=true)

**Why:** The invalid event is stale (job DOES exist). Job still needs recovery. Reconciliation_required marks the EVENT as invalid, not the job.

### Scenario 4: Multiple Unpublished Events (Should Never Happen)
```
Event 1: published=false, attempts=0, created_at=now-5min
Event 2: published=false, attempts=0, created_at=now-3min
Job: status=QUEUED
```

**Decision:** SKIP event creation (predicate finds Event 1 or Event 2)

**Why:** Both events will eventually publish, causing duplicate messages. Reconciler should not create a third. This scenario indicates a bug (deduplication failed), but the predicate still prevents making it worse.

---

## Transaction Phases

The reconciler follows this atomic transaction structure:

```
BEGIN TRANSACTION

1. Lock job with FOR UPDATE SKIP LOCKED
   - If lock fails: ROLLBACK, skip to next job

2. Revalidate eligibility (status, age, throttle)
   - If ineligible: ROLLBACK, skip to next job

3. Check for existing unpublished recovery event (deduplication predicate)
   - If found: UPDATE throttling timestamp, COMMIT, skip to next job

4. Create new job_queued_recovery event
   - event_type = 'job_queued_recovery'
   - retry_attempt = NULL
   - published = false

5. Update throttling timestamp
   - last_recovery_attempt_at = NOW()

6. COMMIT
   - Atomically persist event + timestamp
   - If commit fails: rollback, job remains eligible

END TRANSACTION
```

---

## Configuration Parameters

```python
# Initial recovery threshold - jobs younger than this are not eligible
QUEUED_RECOVERY_AGE_THRESHOLD_SECONDS = 600  # 10 minutes

# Subsequent recovery interval - minimum time between recovery attempts
QUEUED_RECOVERY_INTERVAL_SECONDS = 3600  # 1 hour (24 attempts/day max)

# Batch size - maximum jobs to recover per reconciliation cycle
QUEUED_RECOVERY_BATCH_SIZE = 100
```

---

## Testing Requirements

The deduplication predicate MUST be tested with:

1. ✅ No existing event → creates new event
2. ✅ Unpublished event (attempts=0) → skips creation, updates timestamp
3. ✅ Unpublished event (attempts>0, backoff) → skips creation, updates timestamp
4. ✅ Published event (recent) → skips creation (throttle not expired)
5. ✅ Published event (old, throttle expired) → creates new event
6. ✅ Reconciliation_required event → creates new event (invalid event doesn't block recovery)
7. ✅ Concurrent reconcilers → only one creates event (FOR UPDATE SKIP LOCKED)
8. ✅ Transaction rollback before commit → neither event nor timestamp persisted
9. ✅ Transaction commit → both event and timestamp persisted atomically

---

## Summary

**Deduplication predicate:** Check for unpublished, valid `job_queued_recovery` events
**Safe creation:** Within FOR UPDATE SKIP LOCKED transaction on job row
**Throttling:** Update `last_recovery_attempt_at` whether or not event is created
**Bounded republication:** Published events permit new events after throttle expires
**Invalid events:** Do not permanently block recovery (reconciliation_required=true ignored)
**Concurrency:** Database locks (not Python checks) prevent races

This specification ensures at-least-once delivery without unbounded duplication or permanent suppression due to stale events.
