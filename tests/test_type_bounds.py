"""Tests for Arg.validate_type_bound() and type_bound repr.

Tests cover:
- Single predicate: pass and fail
- Multiple predicates with OR logic: pass and fail
- Error message content (param name, predicate names, type info)
- Custom predicate functions
- No type_bound set (no-op)
- Multiple columns validated independently (simulating varargs loop)
- Arg repr with type_bound information
"""

from typing import Any

import pyarrow as pa
import pyarrow.types
import pytest

from vgi.arguments import Arg
from vgi.exceptions import SchemaValidationError


class TestValidateTypeBound:
    """Tests for Arg.validate_type_bound()."""

    def test_single_predicate_passes(self) -> None:
        """Validation passes when the predicate returns True."""
        arg: Arg[Any] = Arg(0, doc="Integer column", type_bound=pa.types.is_integer)
        arg._name = "value"
        # Should not raise
        arg.validate_type_bound(pa.int64())

    def test_single_predicate_fails(self) -> None:
        """Validation raises when the predicate returns False."""
        arg: Arg[Any] = Arg(0, doc="Integer column", type_bound=pa.types.is_integer)
        arg._name = "value"
        with pytest.raises(SchemaValidationError, match="does not match any of"):
            arg.validate_type_bound(pa.string())

    def test_multiple_predicates_or_logic_passes(self) -> None:
        """When multiple predicates given, any match passes (OR logic)."""
        arg: Arg[Any] = Arg(
            0,
            doc="Numeric column",
            type_bound=[pa.types.is_integer, pa.types.is_floating],
        )
        arg._name = "value"
        # Float passes via is_floating
        arg.validate_type_bound(pa.float64())
        # Int passes via is_integer
        arg.validate_type_bound(pa.int32())

    def test_multiple_predicates_or_logic_fails(self) -> None:
        """When all predicates fail, validation raises."""
        arg: Arg[Any] = Arg(
            0,
            doc="Numeric column",
            type_bound=[pa.types.is_integer, pa.types.is_floating],
        )
        arg._name = "value"
        with pytest.raises(SchemaValidationError, match="does not match any of"):
            arg.validate_type_bound(pa.string())

    def test_error_message_includes_context(self) -> None:
        """Error includes param name, predicate name, and actual type."""
        arg: Arg[Any] = Arg(0, doc="Integer column", type_bound=pa.types.is_integer)
        arg._name = "my_column"
        with pytest.raises(SchemaValidationError) as exc_info:
            arg.validate_type_bound(pa.string())

        error_message = str(exc_info.value)
        assert "my_column" in error_message
        assert "is_integer" in error_message
        assert "string" in error_message

    def test_custom_predicate_passes(self) -> None:
        """Custom function predicate works when it returns True."""

        def is_large_int(dtype: pa.DataType) -> bool:
            return dtype in (pa.int64(), pa.uint64())

        arg: Arg[Any] = Arg(0, doc="Large int column", type_bound=is_large_int)
        arg._name = "value"
        arg.validate_type_bound(pa.int64())

    def test_custom_predicate_fails(self) -> None:
        """Custom function predicate raises when it returns False."""

        def is_large_int(dtype: pa.DataType) -> bool:
            return dtype in (pa.int64(), pa.uint64())

        arg: Arg[Any] = Arg(0, doc="Large int column", type_bound=is_large_int)
        arg._name = "value"
        with pytest.raises(SchemaValidationError, match="is_large_int"):
            arg.validate_type_bound(pa.int32())

    def test_no_type_bound_is_noop(self) -> None:
        """Arg without type_bound skips validation entirely."""
        arg: Arg[Any] = Arg(0, doc="Any column")
        arg._name = "value"
        # Any type should be accepted without raising
        arg.validate_type_bound(pa.string())
        arg.validate_type_bound(pa.int64())
        arg.validate_type_bound(pa.float32())

    def test_varargs_pattern_validates_each_column(self) -> None:
        """Simulates varargs loop: each column validated independently."""
        arg: Arg[Any] = Arg(
            0,
            doc="Integer columns",
            type_bound=pa.types.is_integer,
            varargs=True,
        )
        arg._name = "values"

        # All integer columns pass
        for dtype in [pa.int64(), pa.int32(), pa.int16()]:
            arg.validate_type_bound(dtype)

        # One non-integer column fails
        with pytest.raises(SchemaValidationError, match="does not match any of"):
            arg.validate_type_bound(pa.string())

    def test_varargs_pattern_with_multiple_predicates(self) -> None:
        """Varargs with multiple type bounds uses OR logic per element."""
        arg: Arg[Any] = Arg(
            0,
            doc="Numeric columns",
            type_bound=[pa.types.is_integer, pa.types.is_floating],
            varargs=True,
        )
        arg._name = "values"

        # Mix of integer and float all pass
        dtypes: list[pa.DataType] = [pa.int64(), pa.float32(), pa.int16()]
        for dtype in dtypes:
            arg.validate_type_bound(dtype)

    def test_error_uses_position_when_no_name(self) -> None:
        """Error falls back to position when _name is not set."""
        arg: Arg[Any] = Arg(0, doc="Column", type_bound=pa.types.is_integer)
        with pytest.raises(SchemaValidationError) as exc_info:
            arg.validate_type_bound(pa.string())

        error_message = str(exc_info.value)
        assert "0" in error_message


class TestArgRepr:
    """Tests for Arg.__repr__() type_bound display."""

    def test_arg_repr_shows_type_bound(self) -> None:
        """Arg.__repr__() should include type_bound information."""
        arg: Arg[Any] = Arg(0, type_bound=pa.types.is_integer)
        repr_str = repr(arg)
        assert "type_bound=is_integer" in repr_str

    def test_arg_repr_shows_multiple_type_bounds(self) -> None:
        """Arg.__repr__() should show list of type bounds."""
        arg: Arg[Any] = Arg(0, type_bound=[pa.types.is_integer, pa.types.is_floating])
        repr_str = repr(arg)
        assert "type_bound=[is_integer, is_floating]" in repr_str
