from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import delete
from typing import Dict, Any, Type, TypeVar
from ..config.database import get_session_sync_

T = TypeVar('T')

class Delete:
    """
    Handler for deleting multiple records with sync/async support.

    Provides methods to delete database records that match given criteria.
    Can delete multiple records at once based on filter conditions.
    """

    def __init__(self, model_class: Type[T]):
        """
        Initialize the Delete handler.

        Args:
            model_class: The SQLAlchemy model class to delete records from
        """
        self.model_class = model_class

    def __call__(self, filter_data: Dict[str, Any]) -> int:
        """
        Delete multiple records matching the criteria synchronously.

        Args:
            filter_data: Dictionary of field-value pairs to filter records for deletion

        Returns:
            Number of records that were deleted

        Raises:
            ValueError: If filter_data contains invalid field names or is empty
            IntegrityError: If deletion violates database constraints
            Exception: For other database-related errors

        Example:
            deleted_count = User.delete({"status": "inactive"})

        Warning:
            This operation cannot be undone. Use with caution.
        """
        session = get_session_sync_()

        try:
            # Build query with filters
            query = session.query(self.model_class)
            if filter_data:
                for key, value in filter_data.items():
                    if hasattr(self.model_class, key):
                        column = getattr(self.model_class, key)
                        query = query.filter(column == value)

            # Delete records
            result = query.delete(synchronize_session=False)
            session.commit()  # Commit the transaction

            return result
        except Exception:
            session.rollback()
            raise

    async def async_(self, session: AsyncSession, filter_data: Dict[str, Any]) -> int:
        """
        Delete multiple records matching the criteria asynchronously.

        Args:
            session: Active AsyncSession for database operations
            filter_data: Dictionary of field-value pairs to filter records for deletion

        Returns:
            Number of records that were deleted

        Raises:
            TypeError: If session is not an AsyncSession
            ValueError: If filter_data contains invalid field names or is empty
            IntegrityError: If deletion violates database constraints
            Exception: For other database-related errors

        Example:
            async with get_async_session() as session:
                deleted_count = await User.delete.async_(session, {"status": "inactive"})
                await session.commit()

        Warning:
            This operation cannot be undone. Use with caution.
        """
        if not isinstance(session, AsyncSession):
            raise TypeError("Expected AsyncSession for async operation.")

        try:
            # Build delete statement with filters
            stmt = delete(self.model_class)
            if filter_data:
                for key, value in filter_data.items():
                    if hasattr(self.model_class, key):
                        column = getattr(self.model_class, key)
                        stmt = stmt.where(column == value)

            # Execute delete
            result = await session.execute(stmt)
            await session.commit()  # Commit the transaction

            return result.rowcount
        except Exception:
            await session.rollback()
            raise
