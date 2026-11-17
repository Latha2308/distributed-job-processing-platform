"""Main API router"""

from fastapi import APIRouter

from app.api.v1.endpoints import jobs, workers, monitoring, websocket

api_router = APIRouter()

# Include endpoint routers
api_router.include_router(jobs.router, prefix="/jobs", tags=["jobs"])
api_router.include_router(workers.router, prefix="/workers", tags=["workers"])
api_router.include_router(monitoring.router, prefix="/monitoring", tags=["monitoring"])
api_router.include_router(websocket.router, prefix="/ws", tags=["websocket"])
