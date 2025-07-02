from typing import Type, TypeVar
from .builder import QueryBuilder

T = TypeVar('T')

class QueryDescriptor:
    """Descriptor to provide QueryBuilder access on model classes."""

    def __get__(self, obj, owner):
        return QueryBuilder(owner)

__all__ = ["QueryDescriptor"]
