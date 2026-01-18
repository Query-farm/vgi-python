"""Tests for scalar function base classes."""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest
import structlog

from tests.conftest import make_scalar_invocation
from vgi import schema
from vgi.arguments import Arg, Arguments
from vgi.invocation import Invocation, InvocationType
from vgi.log import Level, Message
from vgi.scalar_function import (
    ProtocolInput,
    ScalarFunction,
    ScalarFunctionGenerator,
    ScalarOutputGenerator,
)
from vgi.table_function import Output


class TestScalarFunctionGenerator:
    """Tests for the generator-based ScalarFunctionGenerator."""

    def test_basic_process(self) -> None:
        """Test basic processing of batches."""

        class DoubleColumn(ScalarFunctionGenerator):
            @property
            def output_schema(self) -> pa.Schema:
                return schema(result=pa.int64())

            def process(self, batch: pa.RecordBatch) -> ScalarOutputGenerator:
                _ = yield Output(self.empty_output_batch)  # Priming yield
                import pyarrow.compute as pc

                while True:
                    result = pc.multiply(batch.column("x"), 2)
                    output = pa.RecordBatch.from_arrays(
                        [result], schema=self.output_schema
                    )
                    received = yield Output(output)
                    if received is None:
                        break
                    batch = received

        input_schema = schema(x=pa.int64())
        invocation = make_scalar_invocation(input_schema)
        logger = structlog.get_logger()

        func = DoubleColumn(invocation=invocation, logger=logger)

        # Run the protocol
        generator = func.run()
        next(generator)  # Prime

        # Send a batch
        input_batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3]}, schema=input_schema)
        output = generator.send(ProtocolInput(batch=input_batch))

        assert output.batch is not None
        assert output.batch.num_rows == 3
        assert output.batch.column("result").to_pylist() == [2, 4, 6]

    def test_requires_input_schema(self) -> None:
        """Test that input schema is required."""

        class TestFunc(ScalarFunctionGenerator):
            @property
            def output_schema(self) -> pa.Schema:
                return schema(result=pa.int64())

            def process(self, batch: pa.RecordBatch) -> ScalarOutputGenerator:
                _ = yield Output(self.empty_output_batch)

        invocation = Invocation(
            function_name="test",
            input_schema=None,
            function_type=InvocationType.SCALAR,
            correlation_id="test",
            invocation_id=None,
            arguments=Arguments(),
        )

        with pytest.raises(ValueError, match="requires an input schema"):
            TestFunc(invocation=invocation, logger=structlog.get_logger())

    def test_log_message_support(self) -> None:
        """Test that log messages can be yielded."""

        class LoggingScalar(ScalarFunctionGenerator):
            @property
            def output_schema(self) -> pa.Schema:
                return schema(result=pa.int64())

            def process(self, batch: pa.RecordBatch) -> ScalarOutputGenerator:
                _ = yield Output(self.empty_output_batch)  # Priming yield
                import pyarrow.compute as pc

                while True:
                    yield Message(Level.INFO, f"Processing {batch.num_rows} rows")
                    result = pc.multiply(batch.column("x"), 2)
                    output = pa.RecordBatch.from_arrays(
                        [result], schema=self.output_schema
                    )
                    received = yield Output(output)
                    if received is None:
                        break
                    batch = received

        input_schema = schema(x=pa.int64())
        invocation = make_scalar_invocation(input_schema)
        func = LoggingScalar(invocation=invocation, logger=structlog.get_logger())

        generator = func.run()
        next(generator)

        input_batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3]}, schema=input_schema)

        # First yield should be the log message
        output = generator.send(ProtocolInput(batch=input_batch))
        assert output.log_message is not None
        assert output.log_message.level == Level.INFO
        assert "Processing 3 rows" in output.log_message.message

        # Re-send to get actual output
        output = generator.send(ProtocolInput(batch=input_batch))
        assert output.batch is not None
        assert output.batch.column("result").to_pylist() == [2, 4, 6]


class TestScalarFunction:
    """Tests for the callback-based ScalarFunction."""

    def test_basic_compute(self) -> None:
        """Test basic compute() method with keyword-only injection."""
        from typing import Annotated

        class DoubleColumn(ScalarFunction):
            class Meta:
                output_type = pa.int64()

            column: Annotated[str, Arg(0)]

            def compute(self, *, column: pa.Array[Any]) -> pa.Array[Any]:
                import pyarrow.compute as pc

                return pc.multiply(column, 2)

        input_schema = schema(x=pa.int64())
        invocation = Invocation(
            function_name="test",
            input_schema=input_schema,
            function_type=InvocationType.SCALAR,
            correlation_id="test",
            invocation_id=None,
            arguments=Arguments(positional=(pa.scalar("x"),)),
        )

        func = DoubleColumn(invocation=invocation, logger=structlog.get_logger())

        # Run the protocol
        generator = func.run()
        next(generator)

        input_batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3]}, schema=input_schema)
        output = generator.send(ProtocolInput(batch=input_batch))

        assert output.batch is not None
        assert output.batch.num_rows == 3
        assert output.batch.column("result").to_pylist() == [2, 4, 6]

    def test_log_method(self) -> None:
        """Test self.log() method."""
        from typing import Annotated

        class LoggingFunc(ScalarFunction):
            class Meta:
                output_type = pa.int64()

            x: Annotated[str, Arg(0)]

            def compute(self, *, x: pa.Array[Any]) -> pa.Array[Any]:
                import pyarrow.compute as pc

                self.log(Level.INFO, f"Processing {len(x)} rows")
                return pc.multiply(x, 2)

        input_schema = schema(x=pa.int64())
        invocation = Invocation(
            function_name="test",
            input_schema=input_schema,
            function_type=InvocationType.SCALAR,
            correlation_id="test",
            invocation_id=None,
            arguments=Arguments(positional=(pa.scalar("x"),)),
        )
        func = LoggingFunc(invocation=invocation, logger=structlog.get_logger())

        generator = func.run()
        next(generator)

        input_batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3]}, schema=input_schema)

        # First yield should be the log message
        output = generator.send(ProtocolInput(batch=input_batch))
        assert output.log_message is not None
        assert output.log_message.level == Level.INFO
        assert "Processing 3 rows" in output.log_message.message

        # Re-send to get actual output
        output = generator.send(ProtocolInput(batch=input_batch))
        assert output.batch is not None
        assert output.batch.column("result").to_pylist() == [2, 4, 6]

    def test_row_count_validation(self) -> None:
        """Test that row count mismatch raises error."""
        from typing import Annotated

        class WrongRowCount(ScalarFunction):
            class Meta:
                output_type = pa.int64()

            x: Annotated[str, Arg(0)]

            def compute(self, *, x: pa.Array[Any]) -> pa.Array[Any]:
                # Return wrong number of rows (fewer than input)
                return pa.array([1, 2])

        input_schema = schema(x=pa.int64())
        invocation = Invocation(
            function_name="test",
            input_schema=input_schema,
            function_type=InvocationType.SCALAR,
            correlation_id="test",
            invocation_id=None,
            arguments=Arguments(positional=(pa.scalar("x"),)),
        )
        func = WrongRowCount(invocation=invocation, logger=structlog.get_logger())

        generator = func.run()
        next(generator)

        input_batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3]}, schema=input_schema)
        output = generator.send(ProtocolInput(batch=input_batch))

        # Should have an exception log message
        assert output.log_message is not None
        assert output.log_message.level == Level.EXCEPTION
        assert "same row count" in output.log_message.message.lower()

    def test_row_count_exceeds_input(self) -> None:
        """Test that output with more rows than input raises error."""
        from typing import Annotated

        class TooManyRows(ScalarFunction):
            class Meta:
                output_type = pa.int64()

            x: Annotated[str, Arg(0)]

            def compute(self, *, x: pa.Array[Any]) -> pa.Array[Any]:
                # Return MORE rows than input (expanding rows is not allowed)
                return pa.array([1, 2, 3, 4, 5])

        input_schema = schema(x=pa.int64())
        invocation = Invocation(
            function_name="test",
            input_schema=input_schema,
            function_type=InvocationType.SCALAR,
            correlation_id="test",
            invocation_id=None,
            arguments=Arguments(positional=(pa.scalar("x"),)),
        )
        func = TooManyRows(invocation=invocation, logger=structlog.get_logger())

        generator = func.run()
        next(generator)

        # Input has 3 rows, output has 5 rows
        input_batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3]}, schema=input_schema)
        output = generator.send(ProtocolInput(batch=input_batch))

        # Should have an exception log message
        assert output.log_message is not None
        assert output.log_message.level == Level.EXCEPTION
        # Check that the error message mentions "more rows"
        assert "more rows than input" in output.log_message.message.lower()

    def test_empty_batch(self) -> None:
        """Test handling of empty batches."""
        from typing import Annotated

        class DoubleFunc(ScalarFunction):
            class Meta:
                output_type = pa.int64()

            x: Annotated[str, Arg(0)]

            def compute(self, *, x: pa.Array[Any]) -> pa.Array[Any]:
                import pyarrow.compute as pc

                return pc.multiply(x, 2)

        input_schema = schema(x=pa.int64())
        invocation = Invocation(
            function_name="test",
            input_schema=input_schema,
            function_type=InvocationType.SCALAR,
            correlation_id="test",
            invocation_id=None,
            arguments=Arguments(positional=(pa.scalar("x"),)),
        )
        func = DoubleFunc(invocation=invocation, logger=structlog.get_logger())

        generator = func.run()
        next(generator)

        # Empty batch
        input_batch = pa.RecordBatch.from_pydict({"x": []}, schema=input_schema)
        output = generator.send(ProtocolInput(batch=input_batch))

        assert output.batch is not None
        assert output.batch.num_rows == 0


class TestNullHandlingFunction:
    """Tests for NullHandlingFunction metadata and behavior."""

    def test_metadata_null_handling_special(self) -> None:
        """Test that null_handling is set to SPECIAL in metadata."""
        from vgi.examples.scalar import NullHandlingFunction
        from vgi.metadata import NullHandling, resolve_metadata

        metadata = resolve_metadata(NullHandlingFunction)
        assert metadata.null_handling == NullHandling.SPECIAL

    def test_compute_replaces_nulls(self) -> None:
        """Test that compute() replaces null values with -5000."""
        from vgi.examples.scalar import NullHandlingFunction

        input_schema = schema(x=pa.int64())

        func = NullHandlingFunction(
            invocation=Invocation(
                function_name="null_handling",
                input_schema=input_schema,
                function_type=InvocationType.SCALAR,
                correlation_id="test",
                invocation_id=None,
                arguments=Arguments(positional=(pa.scalar("x"),)),
            ),
            logger=structlog.get_logger(),
        )

        generator = func.run()
        next(generator)  # Prime

        input_batch = pa.RecordBatch.from_pydict(
            {"x": [1, None, 3, None, 5]}, schema=input_schema
        )
        output = generator.send(ProtocolInput(batch=input_batch))

        assert output.batch is not None
        assert output.batch.num_rows == 5
        assert output.batch.column("result").to_pylist() == [1, -5000, 3, -5000, 5]

    def test_compute_with_no_nulls(self) -> None:
        """Test that compute() passes through values when no nulls present."""
        from vgi.examples.scalar import NullHandlingFunction

        input_schema = schema(x=pa.int64())

        func = NullHandlingFunction(
            invocation=Invocation(
                function_name="null_handling",
                input_schema=input_schema,
                function_type=InvocationType.SCALAR,
                correlation_id="test",
                invocation_id=None,
                arguments=Arguments(positional=(pa.scalar("x"),)),
            ),
            logger=structlog.get_logger(),
        )

        generator = func.run()
        next(generator)

        input_batch = pa.RecordBatch.from_pydict(
            {"x": [10, 20, 30]}, schema=input_schema
        )
        output = generator.send(ProtocolInput(batch=input_batch))

        assert output.batch is not None
        assert output.batch.column("result").to_pylist() == [10, 20, 30]


class TestRandomIntFunction:
    """Tests for RandomIntFunction metadata and behavior."""

    def test_metadata_stability_volatile(self) -> None:
        """Test that stability is set to VOLATILE in metadata."""
        from vgi.examples.scalar import RandomIntFunction
        from vgi.metadata import FunctionStability, resolve_metadata

        metadata = resolve_metadata(RandomIntFunction)
        assert metadata.stability == FunctionStability.VOLATILE

    def test_compute_values_in_range(self) -> None:
        """Test that computed values are within specified range from columns."""
        from vgi.examples.scalar import RandomIntFunction

        # min/max values come from columns
        input_schema = schema(min_val=pa.int64(), max_val=pa.int64())

        func = RandomIntFunction(
            invocation=Invocation(
                function_name="random_int",
                input_schema=input_schema,
                function_type=InvocationType.SCALAR,
                correlation_id="test",
                invocation_id=None,
                # Args are column names, not values
                arguments=Arguments(
                    positional=(pa.scalar("min_val"), pa.scalar("max_val"))
                ),
            ),
            logger=structlog.get_logger(),
        )

        generator = func.run()
        next(generator)  # Prime

        input_batch = pa.RecordBatch.from_pydict(
            {"min_val": [10, 10, 10, 10, 10], "max_val": [20, 20, 20, 20, 20]},
            schema=input_schema,
        )
        output = generator.send(ProtocolInput(batch=input_batch))

        assert output.batch is not None
        assert output.batch.num_rows == 5

        # All values should be within range [10, 20]
        values: list[int] = output.batch.column("result").to_pylist()  # type: ignore[assignment]
        for v in values:
            assert 10 <= v <= 20, f"Value {v} not in range [10, 20]"

    def test_compute_preserves_row_count(self) -> None:
        """Test that output has same row count as input."""
        from vgi.examples.scalar import RandomIntFunction

        input_schema = schema(min_val=pa.int64(), max_val=pa.int64())

        func = RandomIntFunction(
            invocation=Invocation(
                function_name="random_int",
                input_schema=input_schema,
                function_type=InvocationType.SCALAR,
                correlation_id="test",
                invocation_id=None,
                arguments=Arguments(
                    positional=(pa.scalar("min_val"), pa.scalar("max_val"))
                ),
            ),
            logger=structlog.get_logger(),
        )

        generator = func.run()
        next(generator)

        # Test with various row counts
        for num_rows in [0, 1, 10, 100]:
            input_batch = pa.RecordBatch.from_pydict(
                {"min_val": [0] * num_rows, "max_val": [100] * num_rows},
                schema=input_schema,
            )
            output = generator.send(ProtocolInput(batch=input_batch))
            assert output.batch is not None
            assert output.batch.num_rows == num_rows


# =============================================================================
# Tests for New Param/ConstParam/Returns API
# =============================================================================


class TestParamConstParamReturnsAPI:
    """Tests for the new Param(), ConstParam(), and Returns() annotation API."""

    def test_param_with_python_int(self) -> None:
        """Test Param(int, ...) creates correct Arrow type."""
        from vgi.arguments import Param, Returns

        class DoubleInt(ScalarFunction):
            def compute(
                self,
                value: Param(int, "Input value"),  # type: ignore[valid-type]
            ) -> Returns(pa.int64()):  # type: ignore[valid-type]
                import pyarrow.compute as pc

                return pc.multiply(value, 2)

        # Verify the class was set up correctly
        assert DoubleInt._uses_new_param_api is True
        assert "value" in DoubleInt._compute_params
        assert DoubleInt._compute_params["value"].arrow_type == pa.int64()

    def test_param_with_python_str(self) -> None:
        """Test Param(str, ...) creates pa.string() type."""
        from vgi.arguments import Param, Returns

        class UpperStr(ScalarFunction):
            def compute(
                self,
                text: Param(str, "Input text"),  # type: ignore[valid-type]
            ) -> Returns(pa.string()):  # type: ignore[valid-type]
                import pyarrow.compute as pc

                return pc.utf8_upper(text)

        assert UpperStr._uses_new_param_api is True
        assert UpperStr._compute_params["text"].arrow_type == pa.string()

    def test_param_with_arrow_type(self) -> None:
        """Test Param(pa.DataType, ...) preserves Arrow type."""
        from vgi.arguments import Param, Returns

        class DecimalDouble(ScalarFunction):
            def compute(
                self,
                value: Param(pa.decimal128(10, 2), "Decimal"),  # type: ignore[valid-type]
            ) -> Returns(pa.decimal128(10, 2)):  # type: ignore[valid-type]
                import pyarrow.compute as pc

                return pc.multiply(value, 2)

        assert DecimalDouble._uses_new_param_api is True
        assert DecimalDouble._compute_params["value"].arrow_type == pa.decimal128(10, 2)

    def test_const_param_basic(self) -> None:
        """Test ConstParam creates const=True Arg."""
        from vgi.arguments import ConstParam, Param, Returns

        class Multiply(ScalarFunction):
            def compute(
                self,
                value: Param(int, "Value"),  # type: ignore[valid-type]
                factor: ConstParam(int, "Factor"),  # type: ignore[valid-type]
            ) -> Returns(pa.int64()):  # type: ignore[valid-type]
                import pyarrow.compute as pc

                return pc.multiply(value, factor)

        assert Multiply._uses_new_param_api is True
        assert "value" in Multiply._compute_params
        assert "factor" in Multiply._const_params
        assert Multiply._const_params["factor"].const is True

    def test_returns_sets_output_type(self) -> None:
        """Test Returns() annotation sets _returns_output_type."""
        from vgi.arguments import Param, Returns

        class StringFunc(ScalarFunction):
            def compute(
                self,
                x: Param(str, "Input"),  # type: ignore[valid-type]
            ) -> Returns(pa.string()):  # type: ignore[valid-type]
                return x

        assert StringFunc._returns_output_type == pa.string()

    def test_returns_with_any_arrow(self) -> None:
        """Test Returns(AnyArrow) sets _returns_output_type to None."""
        from vgi.arguments import AnyArrow, Param, Returns

        class IdentityFunc(ScalarFunction):
            _output_type: pa.DataType

            def bind(self) -> None:
                self._output_type = self.input_schema.field(0).type

            @property
            def output_type(self) -> pa.DataType:
                return self._output_type

            def compute(
                self,
                x: Param(AnyArrow, "Input"),  # type: ignore[valid-type]
            ) -> Returns(AnyArrow):  # type: ignore[valid-type]
                return x

        assert IdentityFunc._returns_output_type is None
        assert IdentityFunc._compute_params["x"].is_any is True

    def test_position_inference_from_signature(self) -> None:
        """Test that positions are inferred from parameter order."""
        from vgi.arguments import Param, Returns

        class AddFunc(ScalarFunction):
            def compute(
                self,
                a: Param(int, "First"),  # type: ignore[valid-type]
                b: Param(int, "Second"),  # type: ignore[valid-type]
                c: Param(int, "Third"),  # type: ignore[valid-type]
            ) -> Returns(pa.int64()):  # type: ignore[valid-type]
                import pyarrow.compute as pc

                return pc.add(pc.add(a, b), c)

        assert AddFunc._compute_params["a"].position == 0
        assert AddFunc._compute_params["b"].position == 1
        assert AddFunc._compute_params["c"].position == 2

    def test_multiply_function_example(self) -> None:
        """Test the MultiplyFunction example from scalar.py."""
        from vgi.examples.scalar import MultiplyFunction

        assert MultiplyFunction._uses_new_param_api is True
        assert "column" in MultiplyFunction._compute_params
        assert "factor" in MultiplyFunction._const_params
        assert MultiplyFunction._const_params["factor"].const is True
        assert MultiplyFunction._returns_output_type == pa.int64()

    def test_multiply_function_compute(self) -> None:
        """Test MultiplyFunction actually computes correctly."""
        from vgi.examples.scalar import MultiplyFunction

        input_schema = schema(value=pa.int64())
        invocation = Invocation(
            function_name="multiply",
            input_schema=input_schema,
            function_type=InvocationType.SCALAR,
            correlation_id="test",
            invocation_id=None,
            # First positional arg is the column data (passed via batch)
            # Second positional arg is the factor constant
            arguments=Arguments(positional=(pa.scalar(3),)),
        )

        func = MultiplyFunction(invocation=invocation, logger=structlog.get_logger())
        generator = func.run()
        next(generator)

        input_batch = pa.RecordBatch.from_pydict(
            {"value": [1, 2, 3, 4, 5]}, schema=input_schema
        )
        output = generator.send(ProtocolInput(batch=input_batch))

        assert output.batch is not None
        result = output.batch.column("result").to_pylist()
        assert result == [3, 6, 9, 12, 15]

    def test_param_type_error_for_unknown_type(self) -> None:
        """Test that Param() raises TypeError for unsupported types."""
        from vgi.arguments import Param

        with pytest.raises(TypeError, match="Cannot convert type"):
            Param(list, "A list")  # list is not a supported type

    def test_const_param_rejects_any_arrow(self) -> None:
        """Test that ConstParam() rejects AnyArrow."""
        from vgi.arguments import AnyArrow, ConstParam

        with pytest.raises(TypeError, match="does not support AnyArrow"):
            ConstParam(AnyArrow, "Any value")

    def test_returns_type_error_for_invalid_type(self) -> None:
        """Test that Returns() raises TypeError for non-DataType."""
        from vgi.arguments import Returns

        with pytest.raises(TypeError, match="requires pa.DataType or AnyArrow"):
            Returns("string")  # type: ignore[arg-type] # Intentionally bad type


class TestTypeValidation:
    """Tests for input/output type validation in ScalarFunction."""

    def test_input_type_mismatch_raises_error(self) -> None:
        """Test that input type mismatch raises TypeMismatchError."""
        from vgi.arguments import Param, Returns
        from vgi.scalar_function import TypeMismatchError

        class Int64Func(ScalarFunction):
            def compute(
                self,
                x: Param(pa.int64(), "Input"),  # type: ignore[valid-type]
            ) -> Returns(pa.int64()):  # type: ignore[valid-type]
                return x

        # Create function with float64 input instead of int64
        batch = pa.RecordBatch.from_pydict({"x": [1.0, 2.0, 3.0]})
        assert batch.column(0).type == pa.float64()

        invocation = Invocation(
            function_name="int64_func",
            function_type=InvocationType.SCALAR,
            correlation_id="test",
            invocation_id=None,
            arguments=Arguments(positional=(), named={}),
            input_schema=batch.schema,
        )
        func = Int64Func(invocation=invocation, logger=structlog.get_logger())

        # Should raise TypeMismatchError when processing
        with pytest.raises(TypeMismatchError) as exc_info:
            gen = func.process(batch)
            next(gen)  # Prime the generator
            gen.send(batch)  # Send the batch

        assert "Input type mismatch" in str(exc_info.value)
        assert exc_info.value.param_name == "x"
        assert exc_info.value.expected_type == pa.int64()
        assert exc_info.value.actual_type == pa.float64()

    def test_output_type_mismatch_raises_error(self) -> None:
        """Test that output type mismatch raises TypeMismatchError."""
        from vgi.arguments import Param, Returns
        from vgi.scalar_function import TypeMismatchError

        class WrongOutputFunc(ScalarFunction):
            def compute(
                self,
                x: Param(pa.int64(), "Input"),  # type: ignore[valid-type]
            ) -> Returns(pa.int64()):  # type: ignore[valid-type]
                # Intentionally return wrong type (string instead of int64)
                return pa.array(["a", "b", "c"])

        batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3]})
        invocation = Invocation(
            function_name="wrong_output_func",
            function_type=InvocationType.SCALAR,
            correlation_id="test",
            invocation_id=None,
            arguments=Arguments(positional=(), named={}),
            input_schema=batch.schema,
        )
        func = WrongOutputFunc(invocation=invocation, logger=structlog.get_logger())

        # Should raise TypeMismatchError when processing
        with pytest.raises(TypeMismatchError) as exc_info:
            gen = func.process(batch)
            next(gen)  # Prime the generator
            gen.send(batch)  # Send the batch

        assert "Output type mismatch" in str(exc_info.value)
        assert exc_info.value.param_name == "return"
        assert exc_info.value.expected_type == pa.int64()
        assert exc_info.value.actual_type == pa.string()

    def test_any_arrow_skips_input_validation(self) -> None:
        """Test that AnyArrow params skip type validation."""
        from vgi.arguments import AnyArrow, Param, Returns

        class AnyFunc(ScalarFunction):
            _output_type: pa.DataType

            def bind(self) -> None:
                self._output_type = self.input_schema.field(0).type

            @property
            def output_type(self) -> pa.DataType:
                return self._output_type

            def compute(
                self,
                x: Param(AnyArrow, "Input"),  # type: ignore[valid-type]
            ) -> Returns(AnyArrow):  # type: ignore[valid-type]
                return x

        # Should work with any input type
        batch = pa.RecordBatch.from_pydict({"x": ["a", "b", "c"]})
        invocation = Invocation(
            function_name="any_func",
            function_type=InvocationType.SCALAR,
            correlation_id="test",
            invocation_id=None,
            arguments=Arguments(positional=(), named={}),
            input_schema=batch.schema,
        )
        func = AnyFunc(invocation=invocation, logger=structlog.get_logger())

        gen = func.process(batch)
        next(gen)  # Prime the generator
        result = gen.send(batch)

        assert isinstance(result, Output)
        assert result.batch is not None

    def test_correct_types_pass_validation(self) -> None:
        """Test that correct types pass validation without error."""
        from vgi.arguments import Param, Returns

        class CorrectFunc(ScalarFunction):
            def compute(
                self,
                x: Param(pa.int64(), "Input"),  # type: ignore[valid-type]
            ) -> Returns(pa.int64()):  # type: ignore[valid-type]
                import pyarrow.compute as pc

                return pc.multiply(x, 2)

        batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3]})
        invocation = Invocation(
            function_name="correct_func",
            function_type=InvocationType.SCALAR,
            correlation_id="test",
            invocation_id=None,
            arguments=Arguments(positional=(), named={}),
            input_schema=batch.schema,
        )
        func = CorrectFunc(invocation=invocation, logger=structlog.get_logger())

        gen = func.process(batch)
        next(gen)  # Prime the generator
        result = gen.send(batch)

        assert isinstance(result, Output)
        assert result.batch is not None
        assert result.batch.column(0).to_pylist() == [2, 4, 6]

    def test_legacy_api_skips_input_validation(self) -> None:
        """Test that legacy Arg API doesn't do input type validation.

        The legacy API uses Arg descriptors at class level with compute()
        receiving arrays by name. It doesn't have explicit type declarations
        like Param() does, so input type validation is skipped.
        """
        from typing import Annotated

        class LegacyFunc(ScalarFunction):
            class Meta:
                output_type = pa.string()  # Match the actual output type

            col: Annotated[str, Arg(0, doc="Column name")]

            def compute(self, *, col: pa.Array) -> pa.Array:  # type: ignore[type-arg]
                return col

        # Pass string data (no type check in legacy API)
        batch = pa.RecordBatch.from_pydict({"value": ["a", "b", "c"]})
        invocation = Invocation(
            function_name="legacy_func",
            function_type=InvocationType.SCALAR,
            correlation_id="test",
            invocation_id=None,
            arguments=Arguments(positional=(pa.scalar("value"),), named={}),
            input_schema=batch.schema,
        )
        func = LegacyFunc(invocation=invocation, logger=structlog.get_logger())

        # Verify that legacy API doesn't use the new validation
        assert func._uses_new_param_api is False

        # Should work without type validation (legacy API)
        gen = func.process(batch)
        next(gen)  # Prime the generator
        result = gen.send(batch)

        assert isinstance(result, Output)
        assert result.batch is not None

    def test_type_mismatch_error_message(self) -> None:
        """Test TypeMismatchError produces helpful error messages."""
        from vgi.scalar_function import TypeMismatchError

        error = TypeMismatchError(
            "Input type mismatch for parameter 'x'.",
            param_name="x",
            expected_type=pa.int64(),
            actual_type=pa.float64(),
            function_name="TestFunc",
        )

        msg = str(error)
        assert "Function: TestFunc" in msg
        assert "Parameter: x" in msg
        assert "Expected type: int64" in msg
        assert "Actual type:   double" in msg
