from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from typing import Dict, Any, Type, TypeVar, Optional
from pydantic import ValidationError
from ..config.database import get_session_sync_
from ..exceptions import SchemaValidationError

T = TypeVar('T')

class Count:
    """
    Handler for counting records with sync/async support.

    Provides methods to count the number of database records that match given criteria.
    Useful for pagination, statistics, and conditional logic based on record counts.
    """

    def __init__(self, model_class: Type[T], filter_schema: Optional[Type[Any]] = None):
        """
        Initialize the Count handler.

        Args:
            model_class: The SQLAlchemy model class to count records from
            filter_schema: Optional filter schema class for validating filter data
        """
        self.model_class = model_class
        self.filter_schema = filter_schema

    def _validate_schema(self, schema_class: Optional[Type[Any]], data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate data against a Pydantic schema and return dict representation.

        Args:
            schema_class: Pydantic schema class for validation
            data: Data dictionary to validate

        Returns:
            Validated data as dictionary

        Raises:
            SchemaValidationError: If validation fails
        """
        if schema_class is None or not data:
            return data

        try:
            validated_instance = schema_class(**data)
            return validated_instance.model_dump(exclude_unset=True)
        except ValidationError as e:
            raise SchemaValidationError(
                f"Schema validation failed for {self.model_class.__name__}",
                errors=e.errors()
            )

    def _build_query(self, filter_data: Optional[Dict[str, Any]] = None):
        """
        Build the COUNT query statement.

        Args:
            filter_data: Optional dictionary of field-value pairs to filter results

        Returns:
            SQLAlchemy COUNT statement
        """
        # Validate filter data
        validated_data = self._validate_schema(self.filter_schema, filter_data or {})

        # Build COUNT statement
        stmt = select(func.count()).select_from(self.model_class)

        # Apply filters
        if validated_data:
            for key, value in validated_data.items():
                if hasattr(self.model_class, key):
                    column = getattr(self.model_class, key)
                    stmt = stmt.where(column == value)

        return stmt

    def __call__(self, filter_data: Optional[Dict[str, Any]] = None) -> int:
        """
        Count records matching the criteria synchronously.

        Args:
            filter_data: Optional dictionary of field-value pairs to filter results

        Returns:
            Number of records matching the criteria

        Raises:
            SchemaValidationError: If filter_schema is provided and filter_data validation fails
            ValueError: If filter_data contains invalid field names
            Exception: For other database-related errors

        Example:
            active_users = User.count({"status": "active"})
            total_users = User.count()  # Count all records
        """
        session = get_session_sync_()

        try:
            stmt = self._build_query(filter_data)
            result = session.execute(stmt)
            return result.scalar()
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
            SchemaValidationError: If filter_schema is provided and filter_data validation fails
            ValueError: If filter_data contains invalid field names
            Exception: For other database-related errors

        Example:
            async with get_async_session() as session:
                active_users = await User.count.async_(session, {"status": "active"})
        """
        if not isinstance(session, AsyncSession):
            raise TypeError("Expected AsyncSession for async operation.")

        try:
            stmt = self._build_query(filter_data)
            result = await session.execute(stmt)
            return result.scalar()
        except Exception:
            await session.rollback()
            raise
