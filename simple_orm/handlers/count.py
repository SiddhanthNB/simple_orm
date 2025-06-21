from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from typing import Dict, Any, Type, TypeVar, Optional
from ..config.database import get_session_sync_

T = TypeVar('T')

class Count:
    """
    Handler for counting records with sync/async support.

    Provides methods to count the number of database records that match given criteria.
    Useful for pagination, statistics, and conditional logic based on record counts.
    """

    def __init__(self, model_class: Type[T]):
        """
        Initialize the Count handler.

        Args:
            model_class: The SQLAlchemy model class to count records from
        """
        self.model_class = model_class

    def __call__(self, filter_data: Optional[Dict[str, Any]] = None) -> int:
        """
        Count records matching the criteria synchronously.

        Args:
            filter_data: Optional dictionary of field-value pairs to filter results

        Returns:
            Number of records matching the criteria

        Raises:
            ValueError: If filter_data contains invalid field names
            Exception: For other database-related errors

        Example:
            active_users = User.count({"status": "active"})
            total_users = User.count()  # Count all records
        """
        session = get_session_sync_()

        try:
            # Build query
            query = session.query(self.model_class)

            # Apply filters
            if filter_data:
                for key, value in filter_data.items():
                    if hasattr(self.model_class, key):
                        column = getattr(self.model_class, key)
                        query = query.filter(column == value)

            return query.count()
        except Exception:
            session.rollback()
            raise

    async def async_(self, session: AsyncSession, filter_data: Optional[Dict[str, Any]] = None) -> int:
        """
        Count records matching the criteria asynchronously.

        Args:
            session: Active AsyncSession for database operations
            filter_data: Optional dictionary of field-value pairs to filter results

        Returns:
            Number of records matching the criteria

        Raises:
            TypeError: If session is not an AsyncSession
            ValueError: If filter_data contains invalid field names
            Exception: For other database-related errors

        Example:
            async with get_async_session() as session:
                active_users = await User.count.async_(session, {"status": "active"})
        """
        if not isinstance(session, AsyncSession):
            raise TypeError("Expected AsyncSession for async operation.")

        try:
            # Build count statement
            stmt = select(func.count()).select_from(self.model_class)

            # Apply filters
            if filter_data:
                for key, value in filter_data.items():
                    if hasattr(self.model_class, key):
                        column = getattr(self.model_class, key)
                        stmt = stmt.where(column == value)

            # Execute query
            result = await session.execute(stmt)
            return result.scalar()
        except Exception:
            await session.rollback()
            raise
