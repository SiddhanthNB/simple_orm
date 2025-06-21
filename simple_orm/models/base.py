from sqlalchemy.orm import DeclarativeBase
from ..handlers import *
from ..config.database import get_session_sync_

class BaseModel(DeclarativeBase):
    __abstract__ = True

    @classmethod
    def bind_crud(cls):
        """Bind CRUD operations to the model class."""
        cls.create = Create(cls)
        cls.update = Update(cls)
        cls.fetch = Fetch(cls)
        cls.find = Find(cls)
        cls.count = Count(cls)
        cls.delete = Delete(cls)
        cls.update_fields = cls._update_fields_func_
        cls.destroy = cls._destroy_func_

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
