from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Dict, Any, Type, TypeVar
from ..config.database import get_session_sync_

T = TypeVar('T')

class Create:
    """
    Handler for creating a single record with sync/async support.

    Provides both synchronous and asynchronous methods to create new database records.
    The sync method manages sessions automatically, while async requires explicit session passing.
    """

    def __init__(self, model_class: Type[T]):
        """
        Initialize the Create handler.

        Args:
            model_class: The SQLAlchemy model class to create instances of
        """
        self.model_class = model_class

    def __call__(self, payload: Dict[str, Any]) -> T:
        """
        Create a single record synchronously.

        Args:
            payload: Dictionary containing field values for the new record

        Returns:
            The created model instance with populated ID and any database defaults

        Raises:
            ValueError: If payload contains invalid field names or values
            IntegrityError: If the data violates database constraints
            Exception: For other database-related errors

        Example:
            user = User.create({"name": "John", "email": "john@example.com"})
            # Record is automatically committed to database
        """

        # Create session
        session = get_session_sync_()

        try:
            # Create instance
            instance = self.model_class(**payload)
            session.add(instance)
            session.commit()  # Commit the transaction
            session.refresh(instance)

            return instance
        except Exception:
            session.rollback()
            raise

    async def async_(self, session: AsyncSession, payload: Dict[str, Any]) -> T:
        """
        Create a single record asynchronously.

        Args:
            session: Active AsyncSession for database operations
            payload: Dictionary containing field values for the new record

        Returns:
            The created model instance with populated ID and any database defaults

        Raises:
            TypeError: If session is not an AsyncSession
            ValueError: If payload contains invalid field names or values
            IntegrityError: If the data violates database constraints
            Exception: For other database-related errors

        Example:
            async with get_async_session() as session:
                user = await User.create.async_(session, {"name": "John", "email": "john@example.com"})
                # Record is automatically committed to database
        """
        if not isinstance(session, AsyncSession):
            raise TypeError("Expected AsyncSession for async operation.")

        try:
            # Create instance
            instance = self.model_class(**payload)
            session.add(instance)
            await session.commit()  # Commit the transaction
            await session.refresh(instance)

            return instance
        except Exception:
            await session.rollback()
            raise
