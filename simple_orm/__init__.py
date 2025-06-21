"""
Simple ORM - A lightweight, ActiveRecord-inspired ORM layer built on top of SQLAlchemy.

Supports both sync and async operations with clean API design.
"""

from .models.base import BaseModel

__version__ = "0.1.0"
__all__ = ["BaseModel"]