from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import delete
from typing import Dict, Any, Type, TypeVar, Optional
from pydantic import ValidationError
from ..config.database import get_session_sync_
from ..exceptions import SchemaValidationError

T = TypeVar('T')

class Delete:
    """
    Handler for deleting multiple records with sync/async support.

    Provides methods to delete database records that match given criteria.
    Can delete multiple records at once based on filter conditions.
    """

    def __init__(self, model_class: Type[T], filter_schema: Optional[Type[Any]] = None):
        """
        Initialize the Delete handler.

        Args:
            model_class: The SQLAlchemy model class to delete records from
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

    def _build_query(self, filter_data: Dict[str, Any]):
        """
        Build the DELETE query statement.

        Args:
            filter_data: Dictionary of field-value pairs to filter records for deletion

        Returns:
            SQLAlchemy DELETE statement
        """
        # Validate filter data
        validated_data = self._validate_schema(self.filter_schema, filter_data)

        # Build DELETE statement
        stmt = delete(self.model_class)

        if validated_data:
            for key, value in validated_data.items():
                if hasattr(self.model_class, key):
                    column = getattr(self.model_class, key)
                    stmt = stmt.where(column == value)

        return stmt

    def __call__(self, filter_data: Dict[str, Any]) -> int:
        """
        Delete multiple records matching the criteria synchronously.

        Args:
            filter_data: Dictionary of field-value pairs to filter records for deletion

        Returns:
            Number of records that were deleted

        Raises:
            SchemaValidationError: If filter_schema is provided and filter_data validation fails
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
            stmt = self._build_query(filter_data)
            result = session.execute(stmt)
            session.commit()
            return result.rowcount
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
            SchemaValidationError: If filter_schema is provided and filter_data validation fails
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
            stmt = self._build_query(filter_data)
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount
        except Exception:
            await session.rollback()
            raise
