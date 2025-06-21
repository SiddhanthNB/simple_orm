from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import sessionmaker, scoped_session
from typing import Optional

# Global variables for engines and sessions
_engine_sync = None
_engine_async = None
_session_factory_sync = None
_session_factory_async = None

def configure_database(
    db_url: str,
    db_url_async: str,
    pool_size: int = 10,
    max_overflow: int = 20,
    pool_timeout: int = 30,
    pool_recycle: int = 3600,
    pool_pre_ping: bool = True,
    echo: bool = False
):
    """
    Configure sync and async database engines and sessions.

    Args:
        db_url: PostgreSQL URL for sync operations (e.g., postgresql+psycopg://...)
        db_url_async: PostgreSQL URL for async operations (e.g., postgresql+asyncpg://...)
        pool_size: Number of connections to maintain in pool
        max_overflow: Number of connections that can overflow the pool
        pool_timeout: Timeout for getting connection from pool
        pool_recycle: Time to recycle connections (seconds)
        pool_pre_ping: Validate connections before use (prevents stale connections)
        echo: Whether to echo SQL queries (for debugging)
    """
    global _engine_sync, _engine_async, _session_factory_sync, _session_factory_async

    # Create sync engine
    _engine_sync = create_engine(
        db_url,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=pool_timeout,
        pool_recycle=pool_recycle,
        pool_pre_ping=pool_pre_ping,
        echo=echo
    )

    # Create async engine
    _engine_async = create_async_engine(
        db_url_async,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=pool_timeout,
        pool_recycle=pool_recycle,
        pool_pre_ping=pool_pre_ping,
        echo=echo
    )

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
