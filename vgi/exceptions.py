"""Exception classes for VGI.

This module defines custom exceptions used throughout the VGI framework.

Classes:
    InitIdentifierError: Raised when init_identifier is required but not set.
    SchemaValidationError: Raised when a batch schema doesn't match expected schema.

"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa

__all__ = [
    "InitIdentifierError",
    "SchemaValidationError",
]


class InitIdentifierError(ValueError):
    """Raised when an operation requires an init_identifier that hasn't been set.

    This typically occurs when:
    - store_state() is called before perform_init() or retrieve_init()
    - collect_states() is called before perform_init() or retrieve_init()
    - Work queue operations are attempted before initialization

    The init_identifier is automatically set during:
    - perform_init() for the primary worker
    - retrieve_init() for secondary workers

    Resolution:
    - Ensure your function calls super().perform_init() in perform_init()
    - Ensure the worker correctly calls retrieve_init() for secondary workers

    """


class SchemaValidationError(Exception):
    """Raised when a batch schema doesn't match the expected schema.

    This error is raised by the framework during input/output validation.
    It indicates a programming error where a batch doesn't conform to the
    declared schema.

    The error message includes detailed information about what differs:
    - Missing fields (in expected but not in actual)
    - Extra fields (in actual but not in expected)
    - Type mismatches (same field name, different types)
    - Field order differences

    Attributes:
        expected: The expected Arrow schema.
        actual: The actual Arrow schema that was received.
        context: Description of where the validation occurred.

    """

    def __init__(
        self,
        message: str,
        *,
        expected: pa.Schema | None = None,
        actual: pa.Schema | None = None,
        context: str = "",
    ) -> None:
        """Initialize with schema comparison details.

        Args:
            message: Base error message.
            expected: The expected Arrow schema.
            actual: The actual Arrow schema.
            context: Where the error occurred (e.g., "output from transform()").

        """
        self.expected = expected
        self.actual = actual
        self.context = context

        if expected is not None and actual is not None:
            full_message = self._build_detailed_message(message, expected, actual)
        else:
            full_message = message

        super().__init__(full_message)

    def _build_detailed_message(
        self, base_message: str, expected: pa.Schema, actual: pa.Schema
    ) -> str:
        """Build a detailed message showing exactly what differs."""
        lines = [base_message, ""]

        if self.context:
            lines.append(f"  Context: {self.context}")
            lines.append("")

        # Build field maps for comparison
        expected_fields = {f.name: f for f in expected}
        actual_fields = {f.name: f for f in actual}

        expected_names = set(expected_fields.keys())
        actual_names = set(actual_fields.keys())

        # Find differences
        missing = expected_names - actual_names
        extra = actual_names - expected_names
        common = expected_names & actual_names

        # Check for type mismatches in common fields
        type_mismatches = []
        for name in common:
            exp_field = expected_fields[name]
            act_field = actual_fields[name]
            if exp_field.type != act_field.type:
                type_mismatches.append((name, exp_field.type, act_field.type))
            elif exp_field.nullable != act_field.nullable:
                exp_null = "nullable" if exp_field.nullable else "non-nullable"
                act_null = "nullable" if act_field.nullable else "non-nullable"
                type_mismatches.append((name, exp_null, act_null))

        # Check for order differences (only if names match but order differs)
        order_differs = False
        if not missing and not extra and not type_mismatches:
            expected_order = [f.name for f in expected]
            actual_order = [f.name for f in actual]
            if expected_order != actual_order:
                order_differs = True

        # Report missing fields
        if missing:
            lines.append("  Missing fields (expected but not found):")
            for name in sorted(missing):
                field = expected_fields[name]
                lines.append(f"    - {name}: {field.type}")

        # Report extra fields
        if extra:
            lines.append("  Extra fields (found but not expected):")
            for name in sorted(extra):
                field = actual_fields[name]
                lines.append(f"    - {name}: {field.type}")

        # Report type mismatches
        if type_mismatches:
            lines.append("  Type mismatches:")
            for name, exp_type, act_type in type_mismatches:
                lines.append(f"    - {name}: expected {exp_type}, got {act_type}")

        # Report order differences
        if order_differs:
            lines.append("  Field order differs:")
            lines.append(f"    Expected: {[f.name for f in expected]}")
            lines.append(f"    Actual:   {[f.name for f in actual]}")

        # Summary of schemas
        lines.append("")
        lines.append("  Expected schema:")
        for field in expected:
            nullable = " (nullable)" if field.nullable else ""
            lines.append(f"    {field.name}: {field.type}{nullable}")

        lines.append("  Actual schema:")
        for field in actual:
            nullable = " (nullable)" if field.nullable else ""
            lines.append(f"    {field.name}: {field.type}{nullable}")

        return "\n".join(lines)
