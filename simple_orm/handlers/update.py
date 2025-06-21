from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import update
from typing import Dict, Any, Type, TypeVar
from ..config.database import get_session_sync_

T = TypeVar('T')

class Update:
    """
    Handler for updating multiple records with sync/async support.

    Provides both synchronous and asynchronous methods to update existing database records
    based on filter criteria. Updates can affect multiple records at once.
    """

    def __init__(self, model_class: Type[T]):
        """
        Initialize the Update handler.

        Args:
            model_class: The SQLAlchemy model class to update records for
        """
        self.model_class = model_class

    def __call__(self, filter_data: Dict[str, Any], payload: Dict[str, Any]) -> int:
        """
        Update multiple records synchronously.

        Args:
            filter_data: Dictionary of field-value pairs to filter records for updating
            payload: Dictionary containing field values to update

        Returns:
            Number of records that were updated

        Raises:
            ValueError: If filter_data or payload contains invalid field names
            IntegrityError: If the update violates database constraints
            Exception: For other database-related errors

        Example:
            count = User.update({"status": "active"}, {"last_login": datetime.now()})
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

            # Update records
            result = query.update(payload, synchronize_session=False)
            session.commit()  # Commit the transaction

            return result
        except Exception:
            session.rollback()
            raise

    async def async_(self, session: AsyncSession, filter_data: Dict[str, Any], payload: Dict[str, Any]) -> int:
        """
        Update multiple records asynchronously.

        Args:
            session: Active AsyncSession for database operations
            filter_data: Dictionary of field-value pairs to filter records for updating
            payload: Dictionary containing field values to update

        Returns:
            Number of records that were updated

        Raises:
            TypeError: If session is not an AsyncSession
            ValueError: If filter_data or payload contains invalid field names
            IntegrityError: If the update violates database constraints
            Exception: For other database-related errors

        Example:
            async with get_async_session() as session:
                count = await User.update.async_(session, {"status": "active"}, {"last_login": datetime.now()})
                await session.commit()
        """
        if not isinstance(session, AsyncSession):
            raise TypeError("Expected AsyncSession for async operation.")

        try:
            # Build update statement with filters
            stmt = update(self.model_class)
            if filter_data:
                for key, value in filter_data.items():
                    if hasattr(self.model_class, key):
                        column = getattr(self.model_class, key)
                        stmt = stmt.where(column == value)

            stmt = stmt.values(**payload)

            # Execute update
            result = await session.execute(stmt)
            await session.commit()  # Commit the transaction

            return result.rowcount
        except Exception:
            await session.rollback()
            raise
