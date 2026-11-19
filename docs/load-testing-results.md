
# Load Testing Results

This document records load testing results for the Distributed Job Processing Platform using Locust.

---

## Baseline Test #1 - Initial Validation

**Date:** November 18, 2026  
**Environment:** Local Docker Compose (8 services: postgres, redis, api, dispatcher, worker1-3, prometheus)  
**Tool:** Locust 2.47.0  
**Test Type:** Small baseline validation test

### Test Configuration

```bash
locust -f load_tests/locustfile.py --host http://localhost:8000 --users 2 --spawn-rate 1 --run-time 10s --headless
```

**Parameters:**
- **Simulated Users:** 2
- **Spawn Rate:** 1 user/second
- **Maximum Duration:** 10 seconds
- **Global Request Limit:** 20 requests (configured in locustfile.py)
- **Job Type:** `test_job` (configured processing_time: 0.5s)
- **Idempotency:** Unique UUID-based key per request

### Results Summary

| Metric | Value |
|--------|-------|
| **Total Requests Submitted** | 14 |
| **HTTP Request Failures** | 0 |
| **API Success Rate** | 100% |
| **Average Response Time** | 31 ms |
| **Median Response Time** | 15 ms |
| **Maximum Response Time** | 198 ms |
| **Throughput** | 1.92 requests/second |
| **Jobs Completed** | 14 of 14 (100%) |
| **Duplicate Idempotency Keys** | 0 |

### What Was Verified

**API Endpoint Availability**
- `POST /api/v1/jobs/` responded successfully to all 14 requests
- All responses returned HTTP 201 with valid job IDs
- All jobs entered `queued` status as expected

**Job Completion**
- All 14 submitted jobs reached `completed` status in the database
- Verified with: `SELECT COUNT(*) FROM jobs WHERE idempotency_key LIKE 'load-test-%' AND status = 'COMPLETED';`

**Idempotency Key Uniqueness**
- All 14 unique idempotency keys were accepted by the API
- No duplicate job records were created in the database
- Verified with: `SELECT idempotency_key, COUNT(*) FROM jobs WHERE idempotency_key LIKE 'load-test-%' GROUP BY idempotency_key HAVING COUNT(*) > 1;`
- **Note:** This test used only unique keys, so it does NOT verify behavior when the same idempotency key is submitted multiple times (which should return the existing job rather than creating a duplicate)

**API Response Times**
- Average response time: 31ms
- Median response time: 15ms
- Maximum response time: 198ms

### Limitations and Scope

**This was a small local baseline test, not a production-scale benchmark.**

- **Scale:** Only 14 requests over 10 seconds with 2 simulated users
- **Environment:** Single-machine Docker Compose setup (not distributed infrastructure)
- **Network:** Localhost connections (no realistic network latency)
- **Load:** Far below production-scale traffic volumes
- **Purpose:** Validate that the API accepts requests and jobs complete successfully under light load, not stress testing

**What Was NOT Verified:**

- **Transactional Outbox Atomicity:** This test did not independently verify that job records and outbox events are committed atomically. Jobs completed successfully, but we did not inspect the outbox table or verify publication behavior.

- **Outbox Dispatcher Behavior:** We did not verify that all outbox events were published to Redis, that `published_at` timestamps were set, or that the dispatcher successfully processed all events.

- **Redis Queue State:** We did not check Redis queue depth during or after the test, so we cannot confirm the queue was empty or that all jobs were dequeued.

- **Processing Duration:** While jobs were configured with `processing_time: 0.5s`, we did not measure actual worker processing duration. The 31ms average response time reflects only API response latency (time to accept and persist the job), not end-to-end job execution time.

- **API Response Time Analysis:** The maximum response time of 198ms was observed, but we have not analyzed what caused it (e.g., database query performance, initial request overhead, or other factors).

- **Duplicate Idempotency Key Behavior:** All requests used unique idempotency keys. We did not test submitting the same key multiple times to verify that the API correctly returns the existing job instead of creating duplicates.

- **Duplicate Worker Execution:** This test did not verify that workers never process the same job twice during concurrent execution or failure scenarios. The at-least-once delivery semantics of the outbox pattern mean duplicate execution is possible in edge cases (e.g., worker crashes after processing but before acknowledging). Application-level idempotency would be required to handle such scenarios safely.

- **High Concurrency:** No testing with many simultaneous users
- **Sustained Load:** No multi-hour endurance testing
- **Failure Recovery:** No intentional service failures or network partitions
- **Queue Backlog:** No testing with large numbers of queued jobs
- **Resource Limits:** No CPU, memory, or disk saturation scenarios

### System State

**Docker Services:** All 8 services were running during the test
- postgres: PostgreSQL 15
- redis: Redis 7
- api: FastAPI on port 8000
- dispatcher: Outbox dispatcher (polls every 5s)
- worker1, worker2, worker3: 3 concurrent workers
- prometheus: Metrics collection on port 9090

**Database Verification (Post-Test):**
- Jobs table: 14 jobs with idempotency keys matching `load-test-%` reached `completed` status
- No duplicate idempotency key records found

### Conclusions

**Baseline Validation: PASSED**

The Distributed Job Processing Platform successfully handled light load with:
- Zero API request failures
- 100% API success rate (all requests returned HTTP 201)
- 100% job completion rate (all 14 jobs reached `completed` status)
- No duplicate job creation with unique idempotency keys
- Healthy API response times (31ms average)

This confirms that the API endpoint accepts job submissions correctly and that submitted jobs complete successfully under controlled, light-load conditions.

### Next Steps

Recommended next test:

**Small Incremental Test**
- 5 users, 30 seconds maximum duration
- Global request cap: 50 requests
- Same `test_job` configuration
- Goal: Validate slightly higher concurrency without jumping to production-scale load

Potential future tests (not yet planned):

- **Idempotency Test:** Submit the same idempotency key multiple times, verify API returns existing job
- **Moderate Load Test:** 20-30 users with 200-500 requests to measure sustained throughput
- **Outbox Verification Test:** Monitor outbox table and Redis queue during load to verify dispatcher behavior
- **Failure Recovery Test:** Intentionally restart dispatcher during load, verify unpublished events are recovered

---

**Report Generated:** November 18, 2026  
**Test Conducted By:** Load testing validation  
**Platform Version:** As of commit (pending)
