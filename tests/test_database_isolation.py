"""
Tests for database module initialization and metadata independence.

These tests verify that:
1. Normal application imports always create the engine and session factory
2. Importing Alembic modules does not disable application database initialization
3. Base metadata is shared consistently across application and Alembic
4. The engine and session factory are always available
"""

import sys


def test_normal_import_always_creates_engine_and_session():
    """Test that importing database always creates engine and AsyncSessionLocal."""
    # Force reimport to test fresh import behavior
    if "app.core.database" in sys.modules:
        del sys.modules["app.core.database"]

    from app.core.database import engine, AsyncSessionLocal

    # Verify engine and session factory exist
    assert engine is not None, "Engine should always be created"
    assert AsyncSessionLocal is not None, "AsyncSessionLocal should always be created"

    # Verify engine is configured correctly
    assert str(engine.url).startswith("postgresql+asyncpg://"), "Engine should use asyncpg driver"


def test_import_with_alembic_modules_present():
    """Test that engine is created even when Alembic modules are in sys.modules."""
    # Force reimport
    if "app.core.database" in sys.modules:
        del sys.modules["app.core.database"]

    # Import alembic modules first
    import alembic.runtime.migration  # noqa: F401
    import alembic.script  # noqa: F401

    # Now import database module
    from app.core.database import engine, AsyncSessionLocal

    # Engine and session should still be created
    assert engine is not None, "Engine should be created even with Alembic modules loaded"
    assert (
        AsyncSessionLocal is not None
    ), "AsyncSessionLocal should be created even with Alembic modules loaded"


def test_base_always_available():
    """Test that Base is available from engine-independent module."""
    from app.db.base import Base

    assert Base is not None, "Base should always be available"
    assert hasattr(Base, "metadata"), "Base should have metadata"


def test_base_imported_from_correct_module():
    """Test that Base is imported from app.db.base, not app.core.database."""
    from app.db.base import Base as BaseFromBase

    # Database module should re-export Base
    from app.core.database import Base as BaseFromDatabase

    # Both should be the same class
    assert BaseFromBase is BaseFromDatabase, "Base should be the same class from both imports"


def test_models_use_shared_base():
    """Test that models import Base from the engine-independent module."""
    from app.db.models import Job, JobOutbox, JobStatus
    from app.db.base import Base

    assert Job is not None
    assert JobOutbox is not None
    assert JobStatus is not None

    # Verify Job model is properly configured
    assert hasattr(Job, "__tablename__")
    assert Job.__tablename__ == "jobs"

    # Verify models are registered with Base metadata
    assert "jobs" in Base.metadata.tables
    assert "job_outbox" in Base.metadata.tables


def test_alembic_uses_same_base():
    """Test that Alembic env.py imports the same Base metadata."""
    # Read and verify the alembic/env.py file imports from app.db.base
    with open("alembic/env.py", "r") as f:
        env_py_content = f.read()

    # Verify it imports from app.db.base, not app.core.database
    assert (
        "from app.db.base import Base" in env_py_content
    ), "Alembic env.py should import Base from app.db.base"
    assert (
        "from app.core.database import Base" not in env_py_content
    ), "Alembic env.py should not import Base from app.core.database"

    # Verify Base from both locations is the same object
    from app.db.base import Base
    from app.core.database import Base as DatabaseBase

    assert Base is DatabaseBase, "Base should be the same object"


def test_async_session_factory_works():
    """Test that AsyncSessionLocal creates working sessions."""
    from app.core.database import AsyncSessionLocal

    # Should be able to create a session
    session = AsyncSessionLocal()
    assert session is not None

    # Session should have expected methods
    assert hasattr(session, "execute")
    assert hasattr(session, "commit")
    assert hasattr(session, "rollback")


def test_database_module_exports():
    """Test that database module exports all expected objects."""
    from app.core.database import Base, engine, AsyncSessionLocal, get_db, init_db

    assert Base is not None, "Base should be exported"
    assert engine is not None, "engine should be exported"
    assert AsyncSessionLocal is not None, "AsyncSessionLocal should be exported"
    assert get_db is not None, "get_db should be exported"
    assert init_db is not None, "init_db should be exported"
