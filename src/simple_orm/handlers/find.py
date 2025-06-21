from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import Dict, Any, Type, TypeVar, Optional
from pydantic import ValidationError
from ..config.database import get_session_sync_
from ..exceptions import SchemaValidationError

T = TypeVar('T')

class Find:
    """
    Handler for finding the first matching record with sync/async support.

    Provides methods to retrieve a single database record that matches the given criteria.
    Returns the first record found or None if no matches exist.
    """

    def __init__(self, model_class: Type[T], filter_schema: Optional[Type[Any]] = None):
        """
        Initialize the Find handler.

        Args:
            model_class: The SQLAlchemy model class to search records in
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
        Build the SELECT query statement.

        Args:
            filter_data: Optional dictionary of field-value pairs to filter results

        Returns:
            SQLAlchemy SELECT statement
        """
        # Validate filter data
        validated_data = self._validate_schema(self.filter_schema, filter_data or {})

        # Build SELECT statement
        stmt = select(self.model_class)

        # Apply filters
        if validated_data:
            for key, value in validated_data.items():
                if hasattr(self.model_class, key):
                    column = getattr(self.model_class, key)
                    stmt = stmt.where(column == value)

        return stmt

    def __call__(self, filter_data: Optional[Dict[str, Any]] = None) -> Optional[T]:
        """
        Find the first record matching the criteria synchronously.

        Args:
            filter_data: Optional dictionary of field-value pairs to filter results

        Returns:
            First model instance matching the criteria, or None if no match found

        Raises:
            SchemaValidationError: If filter_schema is provided and filter_data validation fails
            ValueError: If filter_data contains invalid field names
            Exception: For other database-related errors

        Example:
            user = User.find({"email": "john@example.com"})
            first_user = User.find()  # Get first record in table
        """
        session = get_session_sync_()

        try:
            stmt = self._build_query(filter_data)
            result = session.execute(stmt)
            return result.scalars().first()
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
            SchemaValidationError: If filter_schema is provided and filter_data validation fails
            ValueError: If filter_data contains invalid field names
            Exception: For other database-related errors

        Example:
            async with get_async_session() as session:
                user = await User.find.async_(session, {"email": "john@example.com"})
        """
        if not isinstance(session, AsyncSession):
            raise TypeError("Expected AsyncSession for async operation.")

        try:
            stmt = self._build_query(filter_data)
            result = await session.execute(stmt)
            return result.scalars().first()
        except Exception:
            await session.rollback()
            raise
