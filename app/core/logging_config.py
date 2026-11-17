"""Logging configuration"""

import logging
import sys
from typing import Any

import structlog

from app.core.config import settings


def setup_logging() -> structlog.BoundLogger:
    """
    Configure structured logging with structlog.
    
    Returns:
        BoundLogger: Configured logger instance
    """
    # Configure standard logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, settings.LOG_LEVEL.upper()),
    )
    
    # Configure structlog processors
    processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
    ]
    
    # Add JSON renderer for production, console renderer for development
    if settings.LOG_FORMAT == "json":
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer())
    
    # Configure structlog
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, settings.LOG_LEVEL.upper())
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    
    return structlog.get_logger()


def log_execution_time(logger: structlog.BoundLogger):
    """Decorator to log function execution time"""
    def decorator(func):
        async def async_wrapper(*args, **kwargs):
            import time
            start = time.time()
            try:
                result = await func(*args, **kwargs)
                duration = time.time() - start
                logger.info(
                    "function_executed",
                    function=func.__name__,
                    duration_seconds=duration,
                )
                return result
            except Exception as e:
                duration = time.time() - start
                logger.error(
                    "function_failed",
                    function=func.__name__,
                    duration_seconds=duration,
                    error=str(e),
                )
                raise
        
        def sync_wrapper(*args, **kwargs):
            import time
            start = time.time()
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start
                logger.info(
                    "function_executed",
                    function=func.__name__,
                    duration_seconds=duration,
                )
                return result
            except Exception as e:
                duration = time.time() - start
                logger.error(
                    "function_failed",
                    function=func.__name__,
                    duration_seconds=duration,
                    error=str(e),
                )
                raise
        
        import asyncio
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper
    
    return decorator
