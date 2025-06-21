from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Dict, Any, Type, TypeVar, Optional
from pydantic import ValidationError
from ..config.database import get_session_sync_
from ..exceptions import SchemaValidationError

T = TypeVar('T')

class Create:
    """
    Handler for creating a single record with sync/async support.

    Provides both synchronous and asynchronous methods to create new database records.
    The sync method manages sessions automatically, while async requires explicit session passing.
    """

    def __init__(self, model_class: Type[T], create_schema: Optional[Type[Any]] = None):
        """
        Initialize the Create handler.

        Args:
            model_class: The SQLAlchemy model class to create instances of
            create_schema: Optional Pydantic schema for validating create data
        """
        self.model_class = model_class
        self.create_schema = create_schema

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

    def _build_query(self, payload: Dict[str, Any]):
        """
        Build the model instance for creation.

        Args:
            payload: Dictionary containing field values for the new record

        Returns:
            SQLAlchemy model instance
        """
        # Validate payload data
        validated_data = self._validate_schema(self.create_schema, payload)
        
        # Create model instance
        return self.model_class(**validated_data)

    def __call__(self, payload: Dict[str, Any]) -> T:
        """
        Create a single record synchronously.

        Args:
            payload: Dictionary containing field values for the new record

        Returns:
            The created model instance with populated ID and any database defaults

        Raises:
            SchemaValidationError: If validation fails when schema is provided
            ValueError: If payload contains invalid field names or values
            IntegrityError: If the data violates database constraints
            Exception: For other database-related errors

        Example:
            user = User.create({"name": "John", "email": "john@example.com"})
            # Record is automatically committed to database
        """
        session = get_session_sync_()

        try:
            instance = self._build_query(payload)
            session.add(instance)
            session.commit()
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
            SchemaValidationError: If validation fails when schema is provided
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
            instance = self._build_query(payload)
            session.add(instance)
            await session.commit()
            await session.refresh(instance)
            return instance
        except Exception:
            await session.rollback()
            raise
