#!/bin/bash
cd /Users/chandrikakolluri/Documents/Latha/distributed-job-processing-platform
source venv/bin/activate
pytest tests/test_jobs.py -v
pytest tests/test_atomic_job_claiming.py -v
pytest test_retry_workflow_integration.py -v
