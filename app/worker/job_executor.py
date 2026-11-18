"""Job executor for different job types"""

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict

from app.core.logging_config import setup_logging

logger = setup_logging()


class JobExecutor:
    """
    Executes different job types.

    This is a pluggable system where you can register handlers for different job types.
    """

    def __init__(self):
        self.handlers = {
            "data_processing": self._execute_data_processing,
            "email_notification": self._execute_email_notification,
            "report_generation": self._execute_report_generation,
            "data_sync": self._execute_data_sync,
            "image_processing": self._execute_image_processing,
            "backup": self._execute_backup,
            "test_job": self._execute_test_job,
        }

    async def execute(self, job_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a job based on its type.

        Args:
            job_type: Type of job to execute
            payload: Job payload data

        Returns:
            Job result as dictionary

        Raises:
            ValueError: If job type is not supported
            Exception: If job execution fails
        """
        handler = self.handlers.get(job_type)

        if not handler:
            raise ValueError(f"Unsupported job type: {job_type}")

        logger.info("executing_job", job_type=job_type)

        result = await handler(payload)

        logger.info("job_executed", job_type=job_type)

        return result

    async def _execute_data_processing(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Execute data processing job"""
        # Simulate data processing
        data = payload.get("data", [])

        # Simulate some processing time
        await asyncio.sleep(2)

        processed_count = len(data) if isinstance(data, list) else 1

        return {
            "status": "success",
            "processed_count": processed_count,
            "message": f"Processed {processed_count} items",
        }

    async def _execute_email_notification(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Execute email notification job"""
        recipient = payload.get("recipient")
        subject = payload.get("subject")

        # Simulate email sending
        await asyncio.sleep(1)

        logger.info(
            "email_sent",
            recipient=recipient,
            subject=subject,
        )

        return {"status": "success", "recipient": recipient, "sent_at": "2024-01-01T00:00:00Z"}

    async def _execute_report_generation(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Execute report generation job"""
        report_type = payload.get("report_type")
        start_date = payload.get("start_date")
        end_date = payload.get("end_date")

        # Simulate report generation
        await asyncio.sleep(5)

        return {
            "status": "success",
            "report_type": report_type,
            "period": f"{start_date} to {end_date}",
            "report_url": f"https://example.com/reports/{report_type}.pdf",
        }

    async def _execute_data_sync(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Execute data synchronization job"""
        source = payload.get("source")
        destination = payload.get("destination")

        # Simulate data sync
        await asyncio.sleep(3)

        records_synced = 1000  # Simulated

        return {
            "status": "success",
            "source": source,
            "destination": destination,
            "records_synced": records_synced,
        }

    async def _execute_image_processing(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Execute image processing job"""
        image_url = payload.get("image_url")
        operations = payload.get("operations", [])

        # Simulate image processing
        await asyncio.sleep(4)

        return {
            "status": "success",
            "original_url": image_url,
            "processed_url": f"{image_url}_processed",
            "operations_applied": operations,
        }

    async def _execute_backup(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Execute backup job"""
        resource = payload.get("resource")
        backup_location = payload.get("backup_location")

        # Simulate backup
        await asyncio.sleep(6)

        return {
            "status": "success",
            "resource": resource,
            "backup_location": backup_location,
            "backup_size_mb": 150,  # Simulated
            "backup_id": "backup_123456",
        }

    async def _execute_test_job(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute test job for testing and validation.

        Supports optional failure simulation via payload.

        Args:
            payload: Job payload, supports:
                - should_fail (bool): Whether to intentionally fail
                - fail_message (str): Custom failure message
                - processing_time (float): Simulated processing time in seconds

        Returns:
            Success result with timestamp and payload echo

        Raises:
            ValueError: If should_fail is True
        """
        # Check if job should fail (for testing retry behavior)
        should_fail = payload.get("should_fail", False)
        fail_message = payload.get("fail_message", "Test job intentionally failed")

        # Simulate minimal processing
        processing_time = payload.get("processing_time", 0.5)
        await asyncio.sleep(processing_time)

        if should_fail:
            raise ValueError(fail_message)

        # Use timezone-aware UTC timestamp
        completed_at = datetime.now(timezone.utc)

        logger.info(
            "test_job_completed",
            payload=payload,
            completed_at=completed_at.isoformat(),
        )

        return {
            "status": "success",
            "message": "Test job completed successfully",
            "payload": payload,
            "completed_at": completed_at.isoformat(),
            "processed_by": "job_executor",
        }
