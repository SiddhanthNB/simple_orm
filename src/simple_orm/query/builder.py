from sqlalchemy import and_, or_, not_
from typing import Dict, Any, Type, TypeVar
from .run import Run
from ._helpers import *

T = TypeVar('T')

class QueryBuilder:
    """Query builder for constructing complex database queries."""

    def __init__(self, model_class: Type[T]):
        """Initialize QueryBuilder for a specific model class."""
        self.model_class = model_class
        self._where_conditions = []
        self.run = Run(self)

    def where(self, conditions: Dict[str, Any], operation: str = "AND"):
        """Add WHERE conditions with specified logical operation."""
        if not conditions:
            return self

        parsed_conditions = []
        for key, value in conditions.items():
            condition = parse_condition(self.model_class, key, value)
            parsed_conditions.append(condition)

        if operation == "AND":
            combined = and_(*parsed_conditions)
        elif operation == "OR":
            combined = or_(*parsed_conditions)
        elif operation == "NOT_ALL":
            combined = not_(and_(*parsed_conditions))
        elif operation == "NOT_ANY":
            combined = not_(or_(*parsed_conditions))
        else:
            raise ValueError(f"Unsupported operation: {operation}")

        self._where_conditions.append(combined)
        return self

    def _build_sqlalchemy_query(self):
        """Build the final SQLAlchemy SELECT statement."""
        return build_sqlalchemy_query(self.model_class, self._where_conditions)

    def alchemize(self):
        """Return the SQLAlchemy SELECT statement."""
        return self._build_sqlalchemy_query()

__all__ = ["QueryBuilder"]
