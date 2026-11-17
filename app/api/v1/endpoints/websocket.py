"""WebSocket endpoints for real-time updates"""

import asyncio
import json
from typing import Dict, Set
from uuid import UUID

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.logging_config import setup_logging
from app.services.job_service import JobService
from app.core.redis_client import get_redis, RedisClient
from app.services.queue_service import QueueService

router = APIRouter()
logger = setup_logging()


class ConnectionManager:
    """Manages WebSocket connections"""
    
    def __init__(self):
        self.active_connections: Set[WebSocket] = set()
        self.job_subscriptions: Dict[UUID, Set[WebSocket]] = {}
    
    async def connect(self, websocket: WebSocket):
        """Accept new WebSocket connection"""
        await websocket.accept()
        self.active_connections.add(websocket)
        logger.info("websocket_connected", total_connections=len(self.active_connections))
    
    def disconnect(self, websocket: WebSocket):
        """Remove WebSocket connection"""
        self.active_connections.discard(websocket)
        
        # Remove from job subscriptions
        for job_id, subscribers in list(self.job_subscriptions.items()):
            subscribers.discard(websocket)
            if not subscribers:
                del self.job_subscriptions[job_id]
        
        logger.info("websocket_disconnected", total_connections=len(self.active_connections))
    
    async def subscribe_to_job(self, job_id: UUID, websocket: WebSocket):
        """Subscribe WebSocket to job updates"""
        if job_id not in self.job_subscriptions:
            self.job_subscriptions[job_id] = set()
        self.job_subscriptions[job_id].add(websocket)
        logger.debug("job_subscription_added", job_id=str(job_id))
    
    async def send_job_update(self, job_id: UUID, message: dict):
        """Send update to all subscribers of a job"""
        if job_id not in self.job_subscriptions:
            return
        
        disconnected = set()
        for websocket in self.job_subscriptions[job_id]:
            try:
                await websocket.send_json(message)
            except Exception as e:
                logger.error("websocket_send_failed", error=str(e))
                disconnected.add(websocket)
        
        # Clean up disconnected WebSockets
        for ws in disconnected:
            self.disconnect(ws)
    
    async def broadcast(self, message: dict):
        """Broadcast message to all connections"""
        disconnected = set()
        for websocket in self.active_connections:
            try:
                await websocket.send_json(message)
            except Exception as e:
                logger.error("websocket_broadcast_failed", error=str(e))
                disconnected.add(websocket)
        
        # Clean up disconnected WebSockets
        for ws in disconnected:
            self.disconnect(ws)


manager = ConnectionManager()


@router.websocket("/jobs/{job_id}")
async def websocket_job_updates(
    websocket: WebSocket,
    job_id: UUID,
    db: AsyncSession = Depends(get_db),
    redis: RedisClient = Depends(get_redis),
):
    """
    WebSocket endpoint for real-time job status updates.
    
    Clients can connect to receive updates about a specific job.
    """
    await manager.connect(websocket)
    await manager.subscribe_to_job(job_id, websocket)
    
    # Send initial job status
    queue_service = QueueService(redis)
    job_service = JobService(db, redis, queue_service)
    
    job = await job_service.get_job(job_id)
    if job:
        await websocket.send_json({
            "type": "job_status",
            "job_id": str(job.id),
            "status": job.status.value,
            "created_at": job.created_at.isoformat(),
            "updated_at": job.updated_at.isoformat(),
        })
    else:
        await websocket.send_json({
            "type": "error",
            "message": f"Job {job_id} not found"
        })
        await websocket.close()
        return
    
    try:
        while True:
            # Keep connection alive and listen for client messages
            data = await websocket.receive_text()
            
            # Handle ping/pong for keepalive
            if data == "ping":
                await websocket.send_text("pong")
            
            # Refresh job status on request
            elif data == "refresh":
                job = await job_service.get_job(job_id)
                if job:
                    await websocket.send_json({
                        "type": "job_status",
                        "job_id": str(job.id),
                        "status": job.status.value,
                        "result": job.result,
                        "error_message": job.error_message,
                        "processing_duration": job.processing_duration,
                        "updated_at": job.updated_at.isoformat(),
                    })
    
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.error("websocket_error", error=str(e), job_id=str(job_id))
        manager.disconnect(websocket)


@router.websocket("/dashboard")
async def websocket_dashboard(
    websocket: WebSocket,
    db: AsyncSession = Depends(get_db),
    redis: RedisClient = Depends(get_redis),
):
    """
    WebSocket endpoint for real-time dashboard updates.
    
    Broadcasts system metrics every few seconds.
    """
    await manager.connect(websocket)
    
    queue_service = QueueService(redis)
    job_service = JobService(db, redis, queue_service)
    
    try:
        while True:
            # Send dashboard update every 5 seconds
            await asyncio.sleep(5)
            
            # Gather metrics
            queue_depth = await queue_service.get_queue_depth()
            job_stats = await job_service.get_job_stats()
            
            await websocket.send_json({
                "type": "dashboard_update",
                "timestamp": asyncio.get_event_loop().time(),
                "queue_depth": queue_depth,
                "job_stats": {
                    "total": job_stats.total_jobs,
                    "pending": job_stats.pending,
                    "processing": job_stats.processing,
                    "completed": job_stats.completed,
                    "failed": job_stats.failed,
                }
            })
    
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.error("websocket_dashboard_error", error=str(e))
        manager.disconnect(websocket)


# Function to notify job updates (called by workers)
async def notify_job_update(job_id: UUID, status: str, **kwargs):
    """Helper function to notify job updates via WebSocket"""
    message = {
        "type": "job_update",
        "job_id": str(job_id),
        "status": status,
        **kwargs
    }
    await manager.send_job_update(job_id, message)
