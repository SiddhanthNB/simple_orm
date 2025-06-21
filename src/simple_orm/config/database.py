from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import sessionmaker, scoped_session
from typing import Optional

# Global variables for engines and sessions
_engine_sync = None
_engine_async = None
_session_factory_sync = None
_session_factory_async = None

def configure_database(db_url: str, **engine_kwargs):
    """
    Configure sync and async database engines and sessions.

    Args:
        db_url: PostgreSQL URL for both sync and async operations (e.g., postgresql+psycopg://...)
        **engine_kwargs: SQLAlchemy engine configuration parameters from YAML file
                        (pool_size, max_overflow, pool_timeout, pool_recycle, pool_pre_ping, echo, etc.)
    """
    global _engine_sync, _engine_async, _session_factory_sync, _session_factory_async

    # Create sync engine with YAML configuration
    _engine_sync = create_engine(db_url, **engine_kwargs)

    # Create async engine (same URL and config with psycopg3)
    _engine_async = create_async_engine(db_url, **engine_kwargs)

    # Create session factories
    _session_factory_sync = scoped_session(
        sessionmaker(bind=_engine_sync, expire_on_commit=False)
    )

    _session_factory_async = async_sessionmaker(
        bind=_engine_async,
        class_=AsyncSession,
        expire_on_commit=False
    )

def get_session_sync_():
    """Get scoped sync session."""
    if _session_factory_sync is None:
        raise RuntimeError("Database not configured. Call configure_database() first.")
    return _session_factory_sync()

def get_session_async_():
    """Get async session factory."""
    if _session_factory_async is None:
        raise RuntimeError("Database not configured. Call configure_database() first.")
    return _session_factory_async()

def close_session_sync_():
    """Close the sync database session."""
    if _session_factory_sync is not None:
        try:
            _session_factory_sync.remove()
        except Exception as e:
            print(f"Error during sync session cleanup (this is usually harmless): {e}")

            # Fallback: try to clear the registry
            try: _session_factory_sync.registry.clear()
            except Exception: pass

def get_engine_sync():
    """Get sync engine (for advanced usage)."""
    if _engine_sync is None:
        raise RuntimeError("Database not configured. Call configure_database() first.")
    return _engine_sync

def get_engine_async():
    """Get async engine (for advanced usage)."""
    if _engine_async is None:
        raise RuntimeError("Database not configured. Call configure_database() first.")
    return _engine_async

async def get_session_async_context():
    """Get async session with context manager support."""
    async_session = get_session_async_()
    async with async_session() as session:
        yield session
