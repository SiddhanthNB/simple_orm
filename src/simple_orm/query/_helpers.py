from sqlalchemy import select, extract
from typing import Any, Type, TypeVar

T = TypeVar('T')

# Operator mapping for Django-style filtering
OPERATORS = {
    # Comparison operators
    'gt': lambda col, val: col > val,
    'gte': lambda col, val: col >= val,
    'lt': lambda col, val: col < val,
    'lte': lambda col, val: col <= val,
    'ne': lambda col, val: col != val,

    # List/array operators
    'in': lambda col, val: col.in_(val),
    'not_in': lambda col, val: ~col.in_(val),

    # Text operators (case-sensitive)
    'like': lambda col, val: col.like(val),
    'contains': lambda col, val: col.contains(val),
    'startswith': lambda col, val: col.like(f'{val}%'),
    'endswith': lambda col, val: col.like(f'%{val}'),

    # Text operators (case-insensitive)
    'ilike': lambda col, val: col.ilike(val),
    'icontains': lambda col, val: col.ilike(f'%{val}%'),
    'istartswith': lambda col, val: col.ilike(f'{val}%'),
    'iendswith': lambda col, val: col.ilike(f'%{val}'),

    # Range operators
    'range': lambda col, val: col.between(val[0], val[1]),
    'between': lambda col, val: col.between(val[0], val[1]),
    'not_between': lambda col, val: ~col.between(val[0], val[1]),

    # Null operators
    'isnull': lambda col, val: col.is_(None) if val else col.is_not(None),
    'isnotnull': lambda col, val: col.is_not(None) if val else col.is_(None),

    # Date/time extraction operators (cross-database compatible)
    'year': lambda col, val: extract('year', col) == val,
    'month': lambda col, val: extract('month', col) == val,
    'day': lambda col, val: extract('day', col) == val,
}

def parse_condition(model_class: Type[T], field_name: str, value: Any):
    """Convert where condition to SQLAlchemy expression."""
    from sqlalchemy import and_

    if not hasattr(model_class, field_name):
        raise ValueError(f"Field '{field_name}' not found on {model_class.__name__}")

    column = getattr(model_class, field_name)

    # Handle simple value (no operators)
    if not isinstance(value, dict):
        return column == value

    # Handle nested operators: {'gte': 18, 'lte': 65}
    conditions = []
    for operator, operator_value in value.items():
        if operator == 'exact':
            conditions.append(column == operator_value)
        elif operator in OPERATORS:
            conditions.append(OPERATORS[operator](column, operator_value))
        else:
            raise ValueError(f"Unsupported operator: {operator}")

    # Multiple operators on same field are combined with AND
    if len(conditions) == 1:
        return conditions[0]
    else:
        return and_(*conditions)

def build_sqlalchemy_query(model_class: Type[T], where_conditions: list):
    """Build the final SQLAlchemy SELECT statement."""
    stmt = select(model_class)

    for condition in where_conditions:
        stmt = stmt.where(condition)

    return stmt

__all__ = ["parse_condition", "build_sqlalchemy_query"]
