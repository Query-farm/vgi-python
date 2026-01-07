"""Tests for Arg type_bound validation."""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pyarrow.types
import pytest
import structlog

from vgi.arguments import AnyArrow, Arg, Arguments
from vgi.exceptions import SchemaValidationError
from vgi.invocation import Invocation, InvocationType
from vgi.scalar_function import ScalarFunction


def make_invocation(
    input_schema: pa.Schema,
    positional: tuple[pa.Scalar[Any], ...] = (),
) -> Invocation:
    """Create an invocation for testing."""
    return Invocation(
        function_name="test",
        input_schema=input_schema,
        function_type=InvocationType.SCALAR,
        correlation_id="test-correlation",
        invocation_id=None,
        arguments=Arguments(positional=positional),
    )


class TestTypeBoundValidation:
    """Tests for type_bound parameter on Arg[AnyArrow]."""

    def test_type_bound_passes_for_valid_type(self) -> None:
        """Type bound validation should pass when predicate returns True."""

        class TestFunc(ScalarFunction):
            col = Arg[AnyArrow](0, type_bound=pa.types.is_integer)

            @classmethod
            def catalog_output_type(cls) -> pa.DataType:
                return pa.int64()

            def compute(self, batch: pa.RecordBatch) -> pa.Array[Any]:
                return batch.column(self.col.value)

        input_schema = pa.schema([("x", pa.int64())])
        invocation = make_invocation(input_schema, (pa.scalar("x"),))

        # Should not raise
        func = TestFunc(invocation=invocation, logger=structlog.get_logger())
        assert func.col.value == "x"

    def test_type_bound_fails_for_invalid_type(self) -> None:
        """Type bound validation should raise when predicate returns False."""

        class TestFunc(ScalarFunction):
            col = Arg[AnyArrow](0, type_bound=pa.types.is_integer)

            @classmethod
            def catalog_output_type(cls) -> pa.DataType:
                return pa.int64()

            def compute(self, batch: pa.RecordBatch) -> pa.Array[Any]:
                return batch.column(self.col.value)

        # String column, not integer
        input_schema = pa.schema([("x", pa.string())])
        invocation = make_invocation(input_schema, (pa.scalar("x"),))

        with pytest.raises(SchemaValidationError, match="does not match any of"):
            TestFunc(invocation=invocation, logger=structlog.get_logger())

    def test_multiple_type_bounds_or_logic_passes(self) -> None:
        """When multiple predicates are given, any match should pass (OR logic)."""

        class TestFunc(ScalarFunction):
            col = Arg[AnyArrow](
                0, type_bound=[pa.types.is_integer, pa.types.is_floating]
            )

            @classmethod
            def catalog_output_type(cls) -> pa.DataType:
                return pa.float64()

            def compute(self, batch: pa.RecordBatch) -> pa.Array[Any]:
                return batch.column(self.col.value)

        # Float column - should pass since is_floating matches
        input_schema = pa.schema([("x", pa.float64())])
        invocation = make_invocation(input_schema, (pa.scalar("x"),))

        func = TestFunc(invocation=invocation, logger=structlog.get_logger())
        assert func.col.value == "x"

    def test_multiple_type_bounds_or_logic_fails(self) -> None:
        """When multiple predicates fail, validation should raise."""

        class TestFunc(ScalarFunction):
            col = Arg[AnyArrow](
                0, type_bound=[pa.types.is_integer, pa.types.is_floating]
            )

            @classmethod
            def catalog_output_type(cls) -> pa.DataType:
                return pa.float64()

            def compute(self, batch: pa.RecordBatch) -> pa.Array[Any]:
                return batch.column(self.col.value)

        # String column - neither is_integer nor is_floating match
        input_schema = pa.schema([("x", pa.string())])
        invocation = make_invocation(input_schema, (pa.scalar("x"),))

        with pytest.raises(SchemaValidationError, match="does not match any of"):
            TestFunc(invocation=invocation, logger=structlog.get_logger())

    def test_type_bound_on_non_anyarrow_warns(self) -> None:
        """type_bound on Arg[str] should issue a warning."""
        with pytest.warns(UserWarning, match="only meaningful for Arg\\[AnyArrow\\]"):

            class TestFunc(ScalarFunction):
                col = Arg[str](0, type_bound=pa.types.is_integer)

                @classmethod
                def catalog_output_type(cls) -> pa.DataType:
                    return pa.string()

                def compute(self, batch: pa.RecordBatch) -> pa.Array[Any]:
                    return batch.column(self.col)

    def test_no_input_schema_skips_validation(self) -> None:
        """Functions without input_schema should skip type_bound validation."""
        from collections.abc import Generator

        from vgi.table_function import Output, TableFunctionGenerator

        class TestFunc(TableFunctionGenerator):
            # This has type_bound but no input_schema
            col = Arg[AnyArrow](0, type_bound=pa.types.is_integer)

            class Meta:
                max_workers = 1

            @property
            def output_schema(self) -> pa.Schema:
                return pa.schema([("result", pa.int64())])

            def process(self) -> Generator[Output, None, None]:
                yield Output(
                    pa.RecordBatch.from_pydict(
                        {"result": [1, 2, 3]}, schema=self.output_schema
                    )
                )

        # TableFunctionGenerator has no input_schema - validation should be skipped
        invocation = Invocation(
            function_name="test",
            input_schema=None,  # No input schema
            function_type=InvocationType.TABLE,
            correlation_id="test-correlation",
            invocation_id=None,
            arguments=Arguments(positional=(pa.scalar("x"),)),
        )

        # Should not raise - validation skipped because no input_schema
        func = TestFunc(invocation=invocation, logger=structlog.get_logger())
        assert func.col.value == "x"

    def test_error_message_includes_context(self) -> None:
        """Error messages should include argument name and predicate names."""

        class TestFunc(ScalarFunction):
            my_column = Arg[AnyArrow](0, type_bound=pa.types.is_integer)

            @classmethod
            def catalog_output_type(cls) -> pa.DataType:
                return pa.int64()

            def compute(self, batch: pa.RecordBatch) -> pa.Array[Any]:
                return batch.column(self.my_column.value)

        input_schema = pa.schema([("x", pa.string())])
        invocation = make_invocation(input_schema, (pa.scalar("x"),))

        with pytest.raises(SchemaValidationError) as exc_info:
            TestFunc(invocation=invocation, logger=structlog.get_logger())

        error_message = str(exc_info.value)
        # Should include argument name
        assert "my_column" in error_message
        # Should include predicate name
        assert "is_integer" in error_message
        # Should include the actual type
        assert "string" in error_message

    def test_type_bound_with_custom_predicate(self) -> None:
        """Custom lambda/function predicate should work."""

        def is_large_int(dtype: pa.DataType) -> bool:
            """Check if type is int64 or larger."""
            return dtype in (pa.int64(), pa.uint64())

        class TestFunc(ScalarFunction):
            col = Arg[AnyArrow](0, type_bound=is_large_int)

            @classmethod
            def catalog_output_type(cls) -> pa.DataType:
                return pa.int64()

            def compute(self, batch: pa.RecordBatch) -> pa.Array[Any]:
                return batch.column(self.col.value)

        # int64 should pass
        input_schema = pa.schema([("x", pa.int64())])
        invocation = make_invocation(input_schema, (pa.scalar("x"),))
        func = TestFunc(invocation=invocation, logger=structlog.get_logger())
        assert func.col.value == "x"

        # int32 should fail
        input_schema_small = pa.schema([("x", pa.int32())])
        invocation_small = make_invocation(input_schema_small, (pa.scalar("x"),))
        with pytest.raises(SchemaValidationError, match="is_large_int"):
            TestFunc(invocation=invocation_small, logger=structlog.get_logger())

    def test_type_bound_with_lambda(self) -> None:
        """Lambda predicates should work and show in error messages."""

        class TestFunc(ScalarFunction):
            col = Arg[AnyArrow](0, type_bound=lambda t: pa.types.is_integer(t))

            @classmethod
            def catalog_output_type(cls) -> pa.DataType:
                return pa.int64()

            def compute(self, batch: pa.RecordBatch) -> pa.Array[Any]:
                return batch.column(self.col.value)

        # Integer should pass
        input_schema = pa.schema([("x", pa.int64())])
        invocation = make_invocation(input_schema, (pa.scalar("x"),))
        func = TestFunc(invocation=invocation, logger=structlog.get_logger())
        assert func.col.value == "x"

    def test_arg_repr_shows_type_bound(self) -> None:
        """Arg.__repr__() should include type_bound information."""
        arg = Arg[AnyArrow](0, type_bound=pa.types.is_integer)
        repr_str = repr(arg)
        assert "type_bound=is_integer" in repr_str

    def test_arg_repr_shows_multiple_type_bounds(self) -> None:
        """Arg.__repr__() should show list of type bounds."""
        arg = Arg[AnyArrow](0, type_bound=[pa.types.is_integer, pa.types.is_floating])
        repr_str = repr(arg)
        assert "type_bound=[is_integer, is_floating]" in repr_str

    def test_varargs_type_bound_passes_for_all_valid_types(self) -> None:
        """Type bound validation passes when all varargs elements are valid."""

        class TestFunc(ScalarFunction):
            columns = Arg[AnyArrow](0, varargs=True, type_bound=pa.types.is_integer)

            @classmethod
            def catalog_output_type(cls) -> pa.DataType:
                return pa.int64()

            def compute(self, batch: pa.RecordBatch) -> pa.Array[Any]:
                # Sum all integer columns (varargs returns tuple at runtime)
                result = batch.column(self.columns[0])  # type: ignore[index]
                for col_name in self.columns[1:]:  # type: ignore[index]
                    result = pa.compute.add(result, batch.column(col_name))
                return result

        # All columns are integers
        input_schema = pa.schema(
            [("a", pa.int64()), ("b", pa.int32()), ("c", pa.int16())]
        )
        invocation = make_invocation(
            input_schema, (pa.scalar("a"), pa.scalar("b"), pa.scalar("c"))
        )

        # Should not raise
        func = TestFunc(invocation=invocation, logger=structlog.get_logger())
        assert func.columns == ("a", "b", "c")  # type: ignore[comparison-overlap]

    def test_varargs_type_bound_fails_when_any_element_invalid(self) -> None:
        """Type bound validation should fail if any varargs element has invalid type."""

        class TestFunc(ScalarFunction):
            columns = Arg[AnyArrow](0, varargs=True, type_bound=pa.types.is_integer)

            @classmethod
            def catalog_output_type(cls) -> pa.DataType:
                return pa.int64()

            def compute(self, batch: pa.RecordBatch) -> pa.Array[Any]:
                result = batch.column(self.columns[0])  # type: ignore[index]
                for col_name in self.columns[1:]:  # type: ignore[index]
                    result = pa.compute.add(result, batch.column(col_name))
                return result

        # Third column is a string, not integer
        input_schema = pa.schema(
            [  # type: ignore[arg-type]
                ("a", pa.int64()),
                ("b", pa.int32()),
                ("c", pa.string()),
            ]
        )
        invocation = make_invocation(
            input_schema, (pa.scalar("a"), pa.scalar("b"), pa.scalar("c"))
        )

        with pytest.raises(SchemaValidationError, match="does not match any of"):
            TestFunc(invocation=invocation, logger=structlog.get_logger())

    def test_varargs_type_bound_with_multiple_predicates(self) -> None:
        """Varargs with multiple type bounds should use OR logic per element."""

        class TestFunc(ScalarFunction):
            columns = Arg[AnyArrow](
                0, varargs=True, type_bound=[pa.types.is_integer, pa.types.is_floating]
            )

            @classmethod
            def catalog_output_type(cls) -> pa.DataType:
                return pa.float64()

            def compute(self, batch: pa.RecordBatch) -> pa.Array[Any]:
                return batch.column(self.columns[0])  # type: ignore[index]

        # Mix of integer and float columns - should all pass
        input_schema = pa.schema(
            [  # type: ignore[arg-type]
                ("a", pa.int64()),
                ("b", pa.float32()),
                ("c", pa.int16()),
            ]
        )
        invocation = make_invocation(
            input_schema, (pa.scalar("a"), pa.scalar("b"), pa.scalar("c"))
        )

        # Should not raise
        func = TestFunc(invocation=invocation, logger=structlog.get_logger())
        assert func.columns == ("a", "b", "c")  # type: ignore[comparison-overlap]
