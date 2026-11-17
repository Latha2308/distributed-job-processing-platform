"""Queue service for managing job queues"""

import json
from typing import Optional
from uuid import UUID

import boto3
from botocore.exceptions import ClientError

from app.core.config import settings
from app.core.logging_config import setup_logging
from app.core.redis_client import RedisClient

logger = setup_logging()


class QueueService:
    """Service for job queue management"""
    
    QUEUE_NAME = "job_queue"
    
    def __init__(self, redis: RedisClient):
        self.redis = redis
        self.use_sqs = settings.USE_SQS and settings.SQS_QUEUE_URL
        
        if self.use_sqs:
            self.sqs_client = boto3.client(
                'sqs',
                region_name=settings.AWS_REGION,
                aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
                aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            )
        else:
            self.sqs_client = None
    
    async def enqueue_job(
        self,
        job_id: UUID,
        job_type: str,
        priority: int = 5,
        delay: int = 0,
    ) -> bool:
        """
        Enqueue a job for processing.
        
        Args:
            job_id: Job ID
            job_type: Type of job
            priority: Job priority (0=highest, 10=lowest)
            delay: Delay in seconds before job becomes available
            
        Returns:
            True if enqueued successfully
        """
        message = {
            "job_id": str(job_id),
            "job_type": job_type,
            "priority": priority,
        }
        
        if self.use_sqs:
            return await self._enqueue_sqs(message, delay)
        else:
            return await self._enqueue_redis(message, priority)
    
    async def _enqueue_redis(
        self,
        message: dict,
        priority: int
    ) -> bool:
        """Enqueue job in Redis"""
        try:
            await self.redis.push_to_queue(
                self.QUEUE_NAME,
                message,
                priority
            )
            logger.info(
                "job_enqueued_redis",
                job_id=message["job_id"],
                priority=priority,
            )
            return True
        except Exception as e:
            logger.error(
                "redis_enqueue_failed",
                error=str(e),
                job_id=message["job_id"],
            )
            return False
    
    async def _enqueue_sqs(
        self,
        message: dict,
        delay: int = 0
    ) -> bool:
        """Enqueue job in AWS SQS"""
        try:
            self.sqs_client.send_message(
                QueueUrl=settings.SQS_QUEUE_URL,
                MessageBody=json.dumps(message),
                DelaySeconds=delay,
            )
            logger.info(
                "job_enqueued_sqs",
                job_id=message["job_id"],
                delay=delay,
            )
            return True
        except ClientError as e:
            logger.error(
                "sqs_enqueue_failed",
                error=str(e),
                job_id=message["job_id"],
            )
            return False
    
    async def dequeue_job(self) -> Optional[dict]:
        """
        Dequeue next job for processing.
        
        Returns:
            Job message or None if queue is empty
        """
        if self.use_sqs:
            return await self._dequeue_sqs()
        else:
            return await self._dequeue_redis()
    
    async def _dequeue_redis(self) -> Optional[dict]:
        """Dequeue job from Redis"""
        try:
            message = await self.redis.pop_from_queue(self.QUEUE_NAME)
            if message:
                logger.debug(
                    "job_dequeued_redis",
                    job_id=message.get("job_id"),
                )
            return message
        except Exception as e:
            logger.error("redis_dequeue_failed", error=str(e))
            return None
    
    async def _dequeue_sqs(self) -> Optional[dict]:
        """Dequeue job from AWS SQS"""
        try:
            response = self.sqs_client.receive_message(
                QueueUrl=settings.SQS_QUEUE_URL,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20,  # Long polling
            )
            
            messages = response.get('Messages', [])
            if not messages:
                return None
            
            message = messages[0]
            body = json.loads(message['Body'])
            
            # Delete message from queue
            self.sqs_client.delete_message(
                QueueUrl=settings.SQS_QUEUE_URL,
                ReceiptHandle=message['ReceiptHandle']
            )
            
            logger.debug(
                "job_dequeued_sqs",
                job_id=body.get("job_id"),
            )
            
            return body
        except ClientError as e:
            logger.error("sqs_dequeue_failed", error=str(e))
            return None
    
    async def get_queue_depth(self) -> int:
        """Get number of jobs in queue"""
        if self.use_sqs:
            return await self._get_sqs_queue_depth()
        else:
            return await self._get_redis_queue_depth()
    
    async def _get_redis_queue_depth(self) -> int:
        """Get Redis queue depth"""
        try:
            return await self.redis.queue_length(self.QUEUE_NAME)
        except Exception as e:
            logger.error("redis_queue_depth_failed", error=str(e))
            return 0
    
    async def _get_sqs_queue_depth(self) -> int:
        """Get SQS queue depth"""
        try:
            response = self.sqs_client.get_queue_attributes(
                QueueUrl=settings.SQS_QUEUE_URL,
                AttributeNames=['ApproximateNumberOfMessages']
            )
            return int(response['Attributes']['ApproximateNumberOfMessages'])
        except ClientError as e:
            logger.error("sqs_queue_depth_failed", error=str(e))
            return 0
    
    async def send_to_dead_letter_queue(self, message: dict) -> bool:
        """Send message to dead letter queue"""
        if not self.use_sqs or not settings.SQS_DLQ_URL:
            logger.warning("dlq_not_configured")
            return False
        
        try:
            self.sqs_client.send_message(
                QueueUrl=settings.SQS_DLQ_URL,
                MessageBody=json.dumps(message),
            )
            logger.info(
                "job_sent_to_dlq",
                job_id=message.get("job_id"),
            )
            return True
        except ClientError as e:
            logger.error(
                "dlq_send_failed",
                error=str(e),
                job_id=message.get("job_id"),
            )
            return False
