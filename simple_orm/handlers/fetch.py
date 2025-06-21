from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, inspect
from typing import Dict, Any, Type, TypeVar, List, Optional, Union
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
        self._primary_key_column = None

    def _get_primary_key_column(self):
        """Get the primary key column for default ordering."""
        if self._primary_key_column is None:
            mapper = inspect(self.model_class)
            primary_keys = mapper.primary_key
            if primary_keys:
                # Use the first primary key column
                self._primary_key_column = primary_keys[0]
            else:
                # Fallback to None if no primary key found
                self._primary_key_column = None
        return self._primary_key_column

    def __call__(self, filter_data: Optional[Dict[str, Any]] = None,
                 limit: Optional[int] = None, offset: Optional[int] = None,
                 order_by: Optional[Union[str, List[str]]] = None) -> List[T]:
        """
        Fetch multiple records synchronously.

        Args:
            filter_data: Optional dictionary of field-value pairs to filter results
            limit: Optional maximum number of records to return
            offset: Optional number of records to skip (for pagination)
            order_by: Optional field name(s) to order by. Can be string or list of strings.
                      Prefix with '-' for descending order (e.g., '-created_at').
                      If not provided, defaults to ordering by primary key for consistent pagination.

        Returns:
            List of model instances matching the criteria

        Raises:
            ValueError: If filter_data contains invalid field names or order_by fields
            Exception: For other database-related errors

        Example:
            users = User.fetch({"status": "active"}, limit=10, offset=20, order_by="-created_at")
            all_users = User.fetch(order_by=["name", "-id"])  # Multiple ordering
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

            # Apply ordering
            if order_by:
                order_fields = order_by if isinstance(order_by, list) else [order_by]
                for field in order_fields:
                    if field.startswith('-'):
                        # Descending order
                        field_name = field[1:]
                        if hasattr(self.model_class, field_name):
                            column = getattr(self.model_class, field_name)
                            query = query.order_by(column.desc())
                    else:
                        # Ascending order
                        if hasattr(self.model_class, field):
                            column = getattr(self.model_class, field)
                            query = query.order_by(column.asc())
            else:
                # Default ordering by primary key for consistent pagination
                pk_column = self._get_primary_key_column()
                if pk_column is not None:
                    query = query.order_by(pk_column.asc())

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
                     limit: Optional[int] = None, offset: Optional[int] = None,
                     order_by: Optional[Union[str, List[str]]] = None) -> List[T]:
        """
        Fetch multiple records asynchronously.

        Args:
            session: Active AsyncSession for database operations
            filter_data: Optional dictionary of field-value pairs to filter results
            limit: Optional maximum number of records to return
            offset: Optional number of records to skip (for pagination)
            order_by: Optional field name(s) to order by. Can be string or list of strings.
                      Prefix with '-' for descending order (e.g., '-created_at').
                      If not provided, defaults to ordering by primary key for consistent pagination.

        Returns:
            List of model instances matching the criteria

        Raises:
            TypeError: If session is not an AsyncSession
            ValueError: If filter_data contains invalid field names or order_by fields
            Exception: For other database-related errors

        Example:
            async with get_async_session() as session:
                users = await User.fetch.async_(session, {"status": "active"}, limit=10, order_by="-created_at")
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

            # Apply ordering
            if order_by:
                order_fields = order_by if isinstance(order_by, list) else [order_by]
                for field in order_fields:
                    if field.startswith('-'):
                        # Descending order
                        field_name = field[1:]
                        if hasattr(self.model_class, field_name):
                            column = getattr(self.model_class, field_name)
                            stmt = stmt.order_by(column.desc())
                    else:
                        # Ascending order
                        if hasattr(self.model_class, field):
                            column = getattr(self.model_class, field)
                            stmt = stmt.order_by(column.asc())
            else:
                # Default ordering by primary key for consistent pagination
                pk_column = self._get_primary_key_column()
                if pk_column is not None:
                    stmt = stmt.order_by(pk_column.asc())

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
