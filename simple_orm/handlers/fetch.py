from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import Dict, Any, Type, TypeVar, List, Optional
from ..config.database import get_session_sync_

T = TypeVar('T')

class Fetch:
    """
    Handler for fetching multiple records with sync/async support.

    Provides methods to retrieve multiple database records with optional filtering,
    pagination, and ordering. Supports both synchronous and asynchronous operations.
    """

    def __init__(self, model_class: Type[T]):
        """
        Initialize the Fetch handler.

        Args:
            model_class: The SQLAlchemy model class to fetch records from
        """
        self.model_class = model_class

    def __call__(self, filter_data: Optional[Dict[str, Any]] = None,
                 limit: Optional[int] = None, offset: Optional[int] = None) -> List[T]:
        """
        Fetch multiple records synchronously.

        Args:
            filter_data: Optional dictionary of field-value pairs to filter results
            limit: Optional maximum number of records to return
            offset: Optional number of records to skip (for pagination)

        Returns:
            List of model instances matching the criteria

        Raises:
            ValueError: If filter_data contains invalid field names
            Exception: For other database-related errors

        Example:
            users = User.fetch({"status": "active"}, limit=10, offset=20)
            all_users = User.fetch()  # Get all records
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

            # Apply limit and offset
            if offset:
                query = query.offset(offset)
            if limit:
                query = query.limit(limit)

            return query.all()
        except Exception:
            session.rollback()
            raise

    async def async_(self, session: AsyncSession, filter_data: Optional[Dict[str, Any]] = None,
                     limit: Optional[int] = None, offset: Optional[int] = None) -> List[T]:
        """
        Fetch multiple records asynchronously.

        Args:
            session: Active AsyncSession for database operations
            filter_data: Optional dictionary of field-value pairs to filter results
            limit: Optional maximum number of records to return
            offset: Optional number of records to skip (for pagination)

        Returns:
            List of model instances matching the criteria

        Raises:
            TypeError: If session is not an AsyncSession
            ValueError: If filter_data contains invalid field names
            Exception: For other database-related errors

        Example:
            async with get_async_session() as session:
                users = await User.fetch.async_(session, {"status": "active"}, limit=10)
        """
        if not isinstance(session, AsyncSession):
            raise TypeError("Expected AsyncSession for async operation.")

        try:
            # Build select statement
            stmt = select(self.model_class)

            # Apply filters
            if filter_data:
                for key, value in filter_data.items():
                    if hasattr(self.model_class, key):
                        column = getattr(self.model_class, key)
                        stmt = stmt.where(column == value)

            # Apply limit and offset
            if offset:
                stmt = stmt.offset(offset)
            if limit:
                stmt = stmt.limit(limit)

            # Execute query
            result = await session.execute(stmt)
            return result.scalars().all()
        except Exception:
            await session.rollback()
            raise
