# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Tests for vgi.exceptions module."""

import pyarrow as pa
import pytest

from tests.conftest import make_schema
from vgi.exceptions import ExecutionIdentifierError, SchemaValidationError


class TestExecutionIdentifierError:
    """Tests for ExecutionIdentifierError."""

    def test_basic_error(self) -> None:
        """Test that ExecutionIdentifierError can be raised."""
        with pytest.raises(ExecutionIdentifierError):
            raise ExecutionIdentifierError("test message")


class TestSchemaValidationError:
    """Tests for SchemaValidationError detailed message building."""

    def test_simple_message_without_schemas(self) -> None:
        """Test error with just a message, no schemas."""
        error = SchemaValidationError("Simple error")
        assert str(error) == "Simple error"
        assert error.expected is None
        assert error.actual is None

    def test_message_with_context(self) -> None:
        """Test that context is included in detailed message."""
        expected = make_schema([pa.field("x", pa.int64())])
        actual = make_schema([pa.field("y", pa.int64())])

        error = SchemaValidationError(
            "Schema mismatch",
            expected=expected,
            actual=actual,
            context="output from transform()",
        )

        message = str(error)
        assert "Context: output from transform()" in message
        assert error.context == "output from transform()"

    def test_missing_fields_reported(self) -> None:
        """Test that missing fields are reported."""
        expected = make_schema(
            [
                pa.field("a", pa.int64()),
                pa.field("b", pa.string()),
            ]
        )
        actual = make_schema([pa.field("a", pa.int64())])

        error = SchemaValidationError(
            "Schema mismatch",
            expected=expected,
            actual=actual,
        )

        message = str(error)
        assert "Missing fields (expected but not found):" in message
        assert "b: string" in message

    def test_extra_fields_reported(self) -> None:
        """Test that extra fields are reported."""
        expected = make_schema([pa.field("a", pa.int64())])
        actual = make_schema(
            [
                pa.field("a", pa.int64()),
                pa.field("extra", pa.float64()),
            ]
        )

        error = SchemaValidationError(
            "Schema mismatch",
            expected=expected,
            actual=actual,
        )

        message = str(error)
        assert "Extra fields (found but not expected):" in message
        assert "extra: double" in message

    def test_type_mismatch_reported(self) -> None:
        """Test that type mismatches are reported (lines 116-119, 149-151)."""
        expected = make_schema(
            [
                pa.field("x", pa.int64()),
                pa.field("y", pa.string()),
            ]
        )
        actual = make_schema(
            [
                pa.field("x", pa.float64()),  # Different type
                pa.field("y", pa.string()),
            ]
        )

        error = SchemaValidationError(
            "Schema mismatch",
            expected=expected,
            actual=actual,
        )

        message = str(error)
        assert "Type mismatches:" in message
        assert "x: expected int64, got double" in message

    def test_nullable_mismatch_reported(self) -> None:
        """Test that nullable mismatches are reported (lines 120-123)."""
        expected = make_schema(
            [
                pa.field("x", pa.int64(), nullable=False),
            ]
        )
        actual = make_schema(
            [
                pa.field("x", pa.int64(), nullable=True),
            ]
        )

        error = SchemaValidationError(
            "Schema mismatch",
            expected=expected,
            actual=actual,
        )

        message = str(error)
        assert "Type mismatches:" in message
        assert "x: expected non-nullable, got nullable" in message

    def test_field_order_difference_reported(self) -> None:
        """Test that field order differences are reported (lines 128-131, 155-157)."""
        expected = make_schema(
            [
                pa.field("a", pa.int64()),
                pa.field("b", pa.string()),
                pa.field("c", pa.float64()),
            ]
        )
        # Same fields, different order
        actual = make_schema(
            [
                pa.field("c", pa.float64()),
                pa.field("a", pa.int64()),
                pa.field("b", pa.string()),
            ]
        )

        error = SchemaValidationError(
            "Schema mismatch",
            expected=expected,
            actual=actual,
        )

        message = str(error)
        assert "Field order differs:" in message
        assert "Expected: ['a', 'b', 'c']" in message
        assert "Actual:   ['c', 'a', 'b']" in message

    def test_schema_summary_included(self) -> None:
        """Test that full schema summary is included."""
        expected = make_schema([pa.field("x", pa.int64(), nullable=True)])
        actual = make_schema([pa.field("y", pa.string(), nullable=False)])

        error = SchemaValidationError(
            "Schema mismatch",
            expected=expected,
            actual=actual,
        )

        message = str(error)
        assert "Expected schema:" in message
        assert "x: int64 (nullable)" in message
        assert "Actual schema:" in message
        assert "y: string" in message

    def test_multiple_type_mismatches(self) -> None:
        """Test multiple type mismatches are all reported."""
        expected = make_schema(
            [
                pa.field("a", pa.int64()),
                pa.field("b", pa.string()),
                pa.field("c", pa.float64()),
            ]
        )
        actual = make_schema(
            [
                pa.field("a", pa.int32()),  # Mismatch
                pa.field("b", pa.binary()),  # Mismatch
                pa.field("c", pa.float64()),  # OK
            ]
        )

        error = SchemaValidationError(
            "Schema mismatch",
            expected=expected,
            actual=actual,
        )

        message = str(error)
        assert "a: expected int64, got int32" in message
        assert "b: expected string, got binary" in message

    def test_order_not_checked_when_other_differences_exist(self) -> None:
        """Test that order is only checked when fields match exactly."""
        expected = make_schema(
            [
                pa.field("a", pa.int64()),
                pa.field("b", pa.string()),
            ]
        )
        # Missing field 'b', has extra 'c' - order check shouldn't trigger
        actual = make_schema(
            [
                pa.field("c", pa.float64()),
                pa.field("a", pa.int64()),
            ]
        )

        error = SchemaValidationError(
            "Schema mismatch",
            expected=expected,
            actual=actual,
        )

        message = str(error)
        # Should NOT have order differs since there are missing/extra fields
        assert "Field order differs:" not in message
        assert "Missing fields" in message
        assert "Extra fields" in message
