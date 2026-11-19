"""
Locust load testing script for Distributed Job Processing Platform.

This script tests the job submission API endpoint using the test_job type
with a strict global request limit to prevent runaway test execution.

Usage:
    # Install Locust first:
    pip install -r requirements-dev.txt

    # Run small test (2 users, 10 seconds max, 20 requests max):
    locust -f load_tests/locustfile.py --host=http://localhost:8000 \
           --users 2 --spawn-rate 1 --run-time 10s --headless

    # Run with Web UI for interactive testing:
    locust -f load_tests/locustfile.py --host=http://localhost:8000
    # Then open http://localhost:8089 in your browser
"""

import uuid
from gevent.lock import Semaphore
from locust import HttpUser, task, between, events


# Global request counter with semaphore for concurrency safety
request_lock = Semaphore()
total_requests_submitted = 0
MAX_REQUESTS = 20


class JobProcessingUser(HttpUser):
    """
    Simulated user that submits jobs to the job processing API.

    Enforces a global maximum of 20 requests across all users to prevent
    runaway test execution during initial validation.
    """

    wait_time = between(0.5, 2.0)

    @task
    def submit_test_job(self):
        """
        Submit a test_job to the API with a unique idempotency key.

        Stops submitting when the global request limit is reached.
        """
        global total_requests_submitted

        # Check if we've reached the limit (concurrency-safe)
        with request_lock:
            if total_requests_submitted >= MAX_REQUESTS:
                # Stop this user from making more requests
                self.environment.runner.quit()
                return
            total_requests_submitted += 1

        # Generate unique idempotency key
        idempotency_key = f"load-test-{uuid.uuid4()}"

        # Job payload using test_job type
        payload = {
            "job_type": "test_job",
            "payload": {
                "processing_time": 0.5,
                "test_id": idempotency_key,
            },
            "priority": 5,
            "idempotency_key": idempotency_key,
            "max_retries": 1,
            "timeout": 10,
        }

        # Submit job to API
        with self.client.post(
            "/api/v1/jobs/",
            json=payload,
            catch_response=True,
            name="POST /api/v1/jobs/",
        ) as response:
            if response.status_code == 201:
                try:
                    job_data = response.json()
                    job_id = job_data.get("id")
                    status = job_data.get("status")

                    # Validate response structure and expected values
                    if job_id and status == "queued":
                        response.success()
                    else:
                        response.failure(
                            f"Invalid response: job_id={job_id}, status={status} (expected status='queued')"
                        )
                except Exception as e:
                    response.failure(f"Failed to parse JSON response: {e}")
            else:
                response.failure(f"Expected HTTP 201, got {response.status_code}")


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    """Called when the load test starts."""
    global total_requests_submitted

    with request_lock:
        total_requests_submitted = 0  # Reset counter

    print("\n" + "=" * 80)
    print("🚀 Load Test Started")
    print("=" * 80)
    print(f"Target: {environment.host}")
    print(f"Job Type: test_job (0.5s processing time)")
    print(f"Request Limit: {MAX_REQUESTS} requests (global cap)")
    print("=" * 80 + "\n")


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    """Called when the load test stops."""
    stats = environment.stats.total

    print("\n" + "=" * 80)
    print("🏁 Load Test Completed")
    print("=" * 80)
    print(f"Total Requests: {stats.num_requests}")
    print(f"Total Failures: {stats.num_failures}")

    if stats.num_requests > 0:
        success_rate = ((stats.num_requests - stats.num_failures) / stats.num_requests) * 100
        print(f"Success Rate: {success_rate:.2f}%")
        print(f"Average Response Time: {stats.avg_response_time:.0f}ms")
        print(f"Max Response Time: {stats.max_response_time:.0f}ms")
        print(f"Requests/sec: {stats.total_rps:.2f}")

    print("=" * 80)
    print("\n📊 Verification Commands:")
    print("curl http://localhost:8000/api/v1/jobs/stats/overview | jq")
    print("curl http://localhost:8000/metrics | grep job_submissions_total")
    print("=" * 80 + "\n")
