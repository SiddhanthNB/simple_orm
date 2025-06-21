from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import Dict, Any, Type, TypeVar, Optional
from ..config.database import get_session_sync_

T = TypeVar('T')

class Find:
    """
    Handler for finding the first matching record with sync/async support.

    Provides methods to retrieve a single database record that matches the given criteria.
    Returns the first record found or None if no matches exist.
    """

    def __init__(self, model_class: Type[T]):
        """
        Initialize the Find handler.

        Args:
            model_class: The SQLAlchemy model class to search records in
        """
        self.model_class = model_class

    def __call__(self, filter_data: Optional[Dict[str, Any]] = None) -> Optional[T]:
        """
        Find the first record matching the criteria synchronously.

        Args:
            filter_data: Optional dictionary of field-value pairs to filter results

        Returns:
            First model instance matching the criteria, or None if no match found

        Raises:
            ValueError: If filter_data contains invalid field names
            Exception: For other database-related errors

        Example:
            user = User.find({"email": "john@example.com"})
            first_user = User.find()  # Get first record in table
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

            return query.first()
        except Exception:
            session.rollback()
            raise

    async def async_(self, session: AsyncSession, filter_data: Optional[Dict[str, Any]] = None) -> Optional[T]:
        """
        Find the first record matching the criteria asynchronously.

        Args:
            session: Active AsyncSession for database operations
            filter_data: Optional dictionary of field-value pairs to filter results

        Returns:
            First model instance matching the criteria, or None if no match found

        Raises:
            TypeError: If session is not an AsyncSession
            ValueError: If filter_data contains invalid field names
            Exception: For other database-related errors

        Example:
            async with get_async_session() as session:
                user = await User.find.async_(session, {"email": "john@example.com"})
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

            # Execute query
            result = await session.execute(stmt)
            return result.scalars().first()
        except Exception:
            await session.rollback()
            raise
