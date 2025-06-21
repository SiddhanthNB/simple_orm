from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import update
from typing import Dict, Any, Type, TypeVar, Optional
from pydantic import ValidationError
from ..config.database import get_session_sync_
from ..exceptions import SchemaValidationError

T = TypeVar('T')

class Update:
    """
    Handler for updating multiple records with sync/async support.

    Provides both synchronous and asynchronous methods to update existing database records
    based on filter criteria. Updates can affect multiple records at once.
    """

    def __init__(self, model_class: Type[T], update_schema: Optional[Type[Any]] = None, filter_schema: Optional[Type[Any]] = None):
        """
        Initialize the Update handler.

        Args:
            model_class: The SQLAlchemy model class to update records for
            update_schema: Optional Pydantic schema for validating update data
            filter_schema: Optional Pydantic schema for validating filter data
        """
        self.model_class = model_class
        self.update_schema = update_schema
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

    def _build_query(self, filter_data: Dict[str, Any], payload: Dict[str, Any]):
        """
        Build the UPDATE query statement.

        Args:
            filter_data: Dictionary of field-value pairs to filter records for updating
            payload: Dictionary containing field values to update

        Returns:
            SQLAlchemy UPDATE statement
        """
        # Validate both filter and update data
        validated_filter = self._validate_schema(self.filter_schema, filter_data)
        validated_payload = self._validate_schema(self.update_schema, payload)
        
        # Build UPDATE statement
        stmt = update(self.model_class)
        
        if validated_filter:
            for key, value in validated_filter.items():
                if hasattr(self.model_class, key):
                    column = getattr(self.model_class, key)
                    stmt = stmt.where(column == value)
        
        stmt = stmt.values(**validated_payload)
        return stmt

    def __call__(self, filter_data: Dict[str, Any], payload: Dict[str, Any]) -> int:
        """
        Update multiple records synchronously.

        Args:
            filter_data: Dictionary of field-value pairs to filter records for updating
            payload: Dictionary containing field values to update

        Returns:
            Number of records that were updated

        Raises:
            SchemaValidationError: If validation fails when schema is provided
            ValueError: If filter_data or payload contains invalid field names
            IntegrityError: If the update violates database constraints
            Exception: For other database-related errors

        Example:
            count = User.update({"status": "active"}, {"last_login": datetime.now()})
        """
        session = get_session_sync_()

        try:
            stmt = self._build_query(filter_data, payload)
            result = session.execute(stmt)
            session.commit()
            return result.rowcount
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
            SchemaValidationError: If validation fails when schema is provided
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
            stmt = self._build_query(filter_data, payload)
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount
        except Exception:
            await session.rollback()
            raise
