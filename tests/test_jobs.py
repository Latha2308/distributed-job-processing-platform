"""Tests for job endpoints"""

import pytest
from httpx import AsyncClient

from app.db.models import JobStatus


@pytest.mark.asyncio
async def test_create_job(client: AsyncClient, sample_job_data):
    """Test creating a new job"""
    response = await client.post("/api/v1/jobs/", json=sample_job_data)

    assert response.status_code == 201
    data = response.json()

    assert data["job_type"] == sample_job_data["job_type"]
    assert data["payload"] == sample_job_data["payload"]
    assert data["priority"] == sample_job_data["priority"]
    assert data["status"] == JobStatus.QUEUED.value
    assert "id" in data


@pytest.mark.asyncio
async def test_get_job(client: AsyncClient, sample_job_data):
    """Test getting a job by ID"""
    # Create a job
    create_response = await client.post("/api/v1/jobs/", json=sample_job_data)
    job_id = create_response.json()["id"]

    # Get the job
    response = await client.get(f"/api/v1/jobs/{job_id}")

    assert response.status_code == 200
    data = response.json()

    assert data["id"] == job_id
    assert data["job_type"] == sample_job_data["job_type"]


@pytest.mark.asyncio
async def test_get_nonexistent_job(client: AsyncClient):
    """Test getting a non-existent job"""
    response = await client.get("/api/v1/jobs/00000000-0000-0000-0000-000000000000")

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_list_jobs(client: AsyncClient, sample_job_data):
    """Test listing jobs"""
    # Create multiple jobs
    for _ in range(3):
        await client.post("/api/v1/jobs/", json=sample_job_data)

    # List jobs
    response = await client.get("/api/v1/jobs/")

    assert response.status_code == 200
    data = response.json()

    assert "items" in data
    assert "total" in data
    assert data["total"] >= 3
    assert len(data["items"]) >= 3


@pytest.mark.asyncio
async def test_list_jobs_with_status_filter(client: AsyncClient, sample_job_data):
    """Test listing jobs with status filter"""
    # Create a job
    await client.post("/api/v1/jobs/", json=sample_job_data)

    # List queued jobs
    response = await client.get(f"/api/v1/jobs/?status={JobStatus.QUEUED.value}")

    assert response.status_code == 200
    data = response.json()

    assert all(job["status"] == JobStatus.QUEUED.value for job in data["items"])


@pytest.mark.asyncio
async def test_update_job_status(client: AsyncClient, sample_job_data):
    """Test updating job status"""
    # Create a job
    create_response = await client.post("/api/v1/jobs/", json=sample_job_data)
    job_id = create_response.json()["id"]

    # Update status
    update_data = {"status": JobStatus.COMPLETED.value, "result": {"message": "Success"}}
    response = await client.patch(f"/api/v1/jobs/{job_id}", json=update_data)

    assert response.status_code == 200
    data = response.json()

    assert data["status"] == JobStatus.COMPLETED.value
    assert data["result"] == {"message": "Success"}


@pytest.mark.asyncio
async def test_idempotency(client: AsyncClient, sample_job_data):
    """Test idempotency key prevents duplicate jobs"""
    job_data = {**sample_job_data, "idempotency_key": "test-key-123"}

    # Create first job
    response1 = await client.post("/api/v1/jobs/", json=job_data)
    assert response1.status_code == 201
    job1_id = response1.json()["id"]

    # Try to create duplicate job
    response2 = await client.post("/api/v1/jobs/", json=job_data)
    assert response2.status_code == 201
    job2_id = response2.json()["id"]

    # Should return the same job
    assert job1_id == job2_id


@pytest.mark.asyncio
async def test_get_job_stats(client: AsyncClient, sample_job_data):
    """Test getting job statistics"""
    # Create some jobs
    for _ in range(5):
        await client.post("/api/v1/jobs/", json=sample_job_data)

    # Get stats
    response = await client.get("/api/v1/jobs/stats/overview")

    assert response.status_code == 200
    data = response.json()

    assert "total_jobs" in data
    assert "completed" in data
    assert "failed" in data
    assert "success_rate" in data
