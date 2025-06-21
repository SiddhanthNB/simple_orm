from sqlalchemy.orm import DeclarativeBase
from typing import Optional, Type, Any
from ..handlers import *
from ..config.database import get_session_sync_
from ..exceptions import SchemaBindingError

class BaseModel(DeclarativeBase):
    __abstract__ = True

    @classmethod
    def bind_crud(cls, validation_schema: Optional[Type[Any]] = None):
        """
        Bind CRUD operations to the model class with optional Pydantic validation.

        Args:
            validation_schema: Optional validation schema class containing:
                - CreateSchema: For create operations
                - UpdateSchema: For update operations
                - FilterSchema: For fetch/find/count/delete operations

        Raises:
            SchemaBindingError: If validation schema is invalid or missing required schemas

        Example:
            class UserSchemas:
                class CreateSchema(BasePydanticModel): ...
                class UpdateSchema(BasePydanticModel): ...
                class FilterSchema(BasePydanticModel): ...

            User.bind_crud(validation_schema=UserSchemas)
        """
        # Validate schema if provided
        if validation_schema is not None:
            cls._validation_schema_check(validation_schema)

        # Extract specific schemas for each handler
        if validation_schema is not None:
            create_schema = validation_schema.CreateSchema
            update_schema = validation_schema.UpdateSchema
            filter_schema = validation_schema.FilterSchema
        else:
            create_schema = update_schema = filter_schema = None

        # Bind handlers with specific schemas
        cls.create = Create(cls, create_schema)
        cls.update = Update(cls, update_schema, filter_schema)
        cls.fetch = Fetch(cls, filter_schema)
        cls.find = Find(cls, filter_schema)
        cls.count = Count(cls, filter_schema)
        cls.delete = Delete(cls, filter_schema)
        cls.update_fields = cls._update_fields_func_
        cls.destroy = cls._destroy_func_

    @classmethod
    def _validation_schema_check(cls, validation_schema: Type[Any]):
        """
        Validate the provided validation schema class structure.

        Args:
            validation_schema: The schema class to validate

        Raises:
            SchemaBindingError: If schema structure is invalid
        """
        schema_class_name = validation_schema.__name__
        model_class_name = cls.__name__

        # Check if schema class name matches model class name
        if schema_class_name != model_class_name:
            raise SchemaBindingError(
                f"Schema class name '{schema_class_name}' does not match "
                f"model class name '{model_class_name}'. They must be identical."
            )

        # Check for required sub-schemas
        required_schemas = ['CreateSchema', 'UpdateSchema', 'FilterSchema']
        missing_schemas = []

        for schema_name in required_schemas:
            if not hasattr(validation_schema, schema_name):
                missing_schemas.append(schema_name)

        if missing_schemas:
            raise SchemaBindingError(
                f"Missing required schemas in {schema_class_name}: {missing_schemas}. "
                f"Required schemas: {required_schemas}"
            )

        # Verify that the required schemas are actually classes
        invalid_schemas = []
        for schema_name in required_schemas:
            schema_attr = getattr(validation_schema, schema_name)
            if not isinstance(schema_attr, type):
                invalid_schemas.append(schema_name)

        if invalid_schemas:
            raise SchemaBindingError(
                f"Invalid schema definitions in {schema_class_name}: {invalid_schemas}. "
                f"Each schema must be a class, not {[type(getattr(validation_schema, name)).__name__ for name in invalid_schemas]}"
            )

    def _update_fields_func_(self, payload: dict):
        """
        Update fields on this model instance.

        Updates the specified fields with new values and commits the changes to the database.
        Only fields that exist on the model will be updated; invalid fields are ignored.

        Args:
            payload: Dictionary containing field-value pairs to update

        Returns:
            self: Returns the updated instance for method chaining

        Raises:
            ValueError: If payload contains invalid field names
            IntegrityError: If the update violates database constraints
            Exception: For other database-related errors

        Example:
            user = User.find({"id": 1})
            user.update_fields({"name": "John", "email": "john@example.com"})
            # User record is updated in database
        """
        session = get_session_sync_()
        try:
            for key, value in payload.items():
                if hasattr(self, key):
                    setattr(self, key, value)
            session.add(self)  # Mark as dirty
            session.commit()   # Commit the transaction
            session.refresh(self)  # Get updated values
            return self  # Return self for method chaining
        except Exception:
            session.rollback()
            raise

    def _destroy_func_(self):
        """
        Delete this model instance from the database.

        Permanently removes this record from the database and commits the transaction.
        After calling this method, the instance should not be used for further operations.

        Raises:
            IntegrityError: If deletion violates database constraints
            Exception: For other database-related errors

        Example:
            user = User.find({"id": 1})
            user.destroy()  # User is deleted from database

        Warning:
            This operation cannot be undone. Use with caution.
        """
        session = get_session_sync_()
        try:
            session.delete(self)
            session.commit()  # Commit the transaction
        except Exception:
            session.rollback()
            raise

__all__ = ["BaseModel"]
