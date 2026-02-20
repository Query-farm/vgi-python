"""Exception classes for VGI.

This module defines custom exceptions used throughout the VGI framework.

Classes:
    InitIdentifierError: Raised when execution_identifier is required but not set.
    SchemaValidationError: Raised when a batch schema doesn't match expected schema.

"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa

__all__ = [
    "BindStateNotFoundError",
    "CatalogReadOnlyError",
    "ExecutionIdentifierError",
    "SchemaValidationError",
]


class BindStateNotFoundError(Exception):
    """Raised when init is called with invalid or missing bind_data.

    This exception is raised when:
    - INIT invocation is missing bind_data field
    - The bind_data is corrupted or cannot be deserialized
    - The function_name in bind state doesn't match the invocation

    The client should catch this and provide a clear error message indicating
    that a BIND call must be made before INIT.

    """


class CatalogReadOnlyError(Exception):
    """Raised when a DDL operation is attempted on a read-only catalog.

    This exception is raised by ReadOnlyCatalogInterface when any
    create, drop, rename, or modify operation is attempted.

    Read-only catalogs only support:
    - catalogs() - list catalogs
    - catalog_attach/detach - attach to/detach from catalogs
    - schemas() - list schemas
    - schema_get() - get schema info
    - schema_contents() - list schema contents
    - table_get(), view_get() - get table/view info
    - table_scan_function_get() - get scan function for tables

    """


class ExecutionIdentifierError(ValueError):
    """Raised when an operation requires an execution_identifier that hasn't been set.

    This typically occurs when:
    - store_state() is called before initialize_global_state() or load_global_state()
    - collect_states() is called before initialize_global_state() or load_global_state()
    - Work queue operations are attempted before initialization

    The execution_identifier is automatically set during:
    - initialize_global_state() for the primary worker
    - load_global_state() for secondary workers

    Resolution:
    - Ensure your function calls super().initialize_global_state()
    - Ensure the worker correctly calls load_global_state() for secondary workers

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

    def _build_detailed_message(self, base_message: str, expected: pa.Schema, actual: pa.Schema) -> str:
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
