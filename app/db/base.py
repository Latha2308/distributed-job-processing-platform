"""
SQLAlchemy declarative base for ORM models.

This module is intentionally minimal and engine-independent.
It defines only the Base class, which can be safely imported
by Alembic migrations without triggering application engine initialization.
"""

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Base class for all ORM models with type support"""

    pass
