from sqlalchemy.ext.asyncio import AsyncSession
from ..config.database import get_session_sync_

class Run:
    """Execution handler for QueryBuilder with sync/async support."""

    def __init__(self, query_builder):
        self.query_builder = query_builder

    def __call__(self):
        """Execute query synchronously."""
        session = get_session_sync_()

        try:
            stmt = self.query_builder._build_sqlalchemy_query()
            result = session.execute(stmt)
            return result.scalars().all()
        except Exception:
            session.rollback()
            raise

    async def async_(self, session: AsyncSession):
        """Execute query asynchronously."""
        if not isinstance(session, AsyncSession):
            raise TypeError("Expected AsyncSession for async operation.")

        try:
            stmt = self.query_builder._build_sqlalchemy_query()
            result = await session.execute(stmt)
            return result.scalars().all()
        except Exception:
            await session.rollback()
            raise

__all__ = ["Run"]
