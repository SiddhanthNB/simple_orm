"""
Simple ORM - A lightweight, ActiveRecord-inspired ORM layer built on top of SQLAlchemy.

Supports both sync and async operations with clean API design and Pydantic validation.
"""

from .models.base import BaseModel
from .exceptions import SchemaBindingError, SchemaValidationError

__version__ = "0.1.0"
__all__ = ["BaseModel", "SchemaBindingError", "SchemaValidationError"]