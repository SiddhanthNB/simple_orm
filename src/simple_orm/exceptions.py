"""
Custom exceptions for simple-orm validation and schema binding.
"""

from typing import Dict, List, Any


class SchemaBindingError(Exception):
    """
    Raised when there are issues with schema binding during bind_crud().

    This includes cases like:
    - Schema class name doesn't match ORM model name
    - Required sub-schemas are missing
    - Invalid schema structure
    """
    pass


class SchemaValidationError(Exception):
    """
    Raised when Pydantic validation fails during handler execution.

    Contains detailed field-level validation errors.
    """

    def __init__(self, message: str, errors: List[Dict[str, Any]] = None):
        """
        Initialize validation error with detailed error information.

        Args:
            message: High-level error message
            errors: List of field-level validation errors from Pydantic
        """
        super().__init__(message)
        self.errors = errors or []

    def __str__(self) -> str:
        """Return formatted error message with field details."""
        base_msg = super().__str__()
        if self.errors:
            error_details = []
            for error in self.errors:
                loc = " -> ".join(str(x) for x in error.get('loc', []))
                msg = error.get('msg', 'Unknown error')
                error_details.append(f"{loc}: {msg}")
            return f"{base_msg}\nField errors:\n" + "\n".join(f"  - {detail}" for detail in error_details)
        return base_msg