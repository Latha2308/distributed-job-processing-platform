"""Prometheus metrics configuration"""

from fastapi import FastAPI
from prometheus_client import Counter, Histogram, Gauge, make_asgi_app

from app.core.config import settings

# Define metrics
job_submissions = Counter(
    "job_submissions_total", "Total number of job submissions", ["job_type", "priority"]
)

job_completions = Counter(
    "job_completions_total", "Total number of completed jobs", ["job_type", "status"]
)

job_processing_duration = Histogram(
    "job_processing_duration_seconds",
    "Job processing duration in seconds",
    ["job_type"],
    buckets=[0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0],
)

queue_depth = Gauge("job_queue_depth", "Current number of jobs in queue", ["priority"])

active_workers = Gauge("active_workers", "Number of active workers")

job_retries = Counter("job_retries_total", "Total number of job retries", ["job_type"])

job_failures = Counter(
    "job_failures_total", "Total number of job failures", ["job_type", "error_type"]
)

api_requests = Counter("api_requests_total", "Total API requests", ["method", "endpoint", "status"])

api_request_duration = Histogram(
    "api_request_duration_seconds",
    "API request duration",
    ["method", "endpoint"],
    buckets=[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)


def setup_metrics(app: FastAPI):
    """Mount Prometheus metrics endpoint"""
    if settings.ENABLE_METRICS:
        metrics_app = make_asgi_app()
        app.mount("/metrics", metrics_app)
