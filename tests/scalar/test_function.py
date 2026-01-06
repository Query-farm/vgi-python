"""Tests for scalar function base classes."""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest
import structlog

from tests.conftest import make_scalar_invocation
from vgi.arguments import AnyArrow, Arg, Arguments
from vgi.exceptions import SchemaValidationError
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
                return pa.schema([("result", pa.int64())])

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

        input_schema = pa.schema([("x", pa.int64())])
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
                return pa.schema([("result", pa.int64())])

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

    def test_requires_single_column_output(self) -> None:
        """Test that output schema must have exactly one column."""

        class TwoColumnOutput(ScalarFunctionGenerator):
            @property
            def output_schema(self) -> pa.Schema:
                return pa.schema([("a", pa.int64()), ("b", pa.int64())])

            def process(self, batch: pa.RecordBatch) -> ScalarOutputGenerator:
                _ = yield Output(self.empty_output_batch)

        input_schema = pa.schema([("x", pa.int64())])
        invocation = make_scalar_invocation(input_schema)

        with pytest.raises(SchemaValidationError, match="exactly 1 output column"):
            TwoColumnOutput(invocation=invocation, logger=structlog.get_logger())

    def test_log_message_support(self) -> None:
        """Test that log messages can be yielded."""

        class LoggingScalar(ScalarFunctionGenerator):
            @property
            def output_schema(self) -> pa.Schema:
                return pa.schema([("result", pa.int64())])

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

        input_schema = pa.schema([("x", pa.int64())])
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
        """Test basic compute() method."""

        class DoubleColumn(ScalarFunction):
            column = Arg[str](0)

            @classmethod
            def catalog_output_type(cls) -> pa.DataType | type[AnyArrow]:
                return pa.int64()

            def compute(self, batch: pa.RecordBatch) -> pa.Array[Any]:
                import pyarrow.compute as pc

                return pc.multiply(batch.column(self.column), 2)

        input_schema = pa.schema([("x", pa.int64())])
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

        class LoggingFunc(ScalarFunction):
            @classmethod
            def catalog_output_type(cls) -> pa.DataType | type[AnyArrow]:
                return pa.int64()

            def compute(self, batch: pa.RecordBatch) -> pa.Array[Any]:
                import pyarrow.compute as pc

                self.log(Level.INFO, f"Processing {batch.num_rows} rows")
                return pc.multiply(batch.column("x"), 2)

        input_schema = pa.schema([("x", pa.int64())])
        invocation = make_scalar_invocation(input_schema)
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

        class WrongRowCount(ScalarFunction):
            @classmethod
            def catalog_output_type(cls) -> pa.DataType | type[AnyArrow]:
                return pa.int64()

            def compute(self, batch: pa.RecordBatch) -> pa.Array[Any]:
                # Return wrong number of rows
                return pa.array([1, 2])

        input_schema = pa.schema([("x", pa.int64())])
        invocation = make_scalar_invocation(input_schema)
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
        """Test that output with more rows than input raises error (lines 134-142)."""

        class TooManyRows(ScalarFunction):
            @classmethod
            def catalog_output_type(cls) -> pa.DataType | type[AnyArrow]:
                return pa.int64()

            def compute(self, batch: pa.RecordBatch) -> pa.Array[Any]:
                # Return MORE rows than input (expanding rows is not allowed)
                return pa.array([1, 2, 3, 4, 5])

        input_schema = pa.schema([("x", pa.int64())])
        invocation = make_scalar_invocation(input_schema)
        func = TooManyRows(invocation=invocation, logger=structlog.get_logger())

        generator = func.run()
        next(generator)

        # Input has 3 rows, output has 5 rows
        input_batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3]}, schema=input_schema)
        output = generator.send(ProtocolInput(batch=input_batch))

        # Should have an exception log message
        assert output.log_message is not None
        assert output.log_message.level == Level.EXCEPTION
        # Check that the error message mentions "more rows" (lines 134-142)
        assert "more rows than input" in output.log_message.message.lower()

    def test_empty_batch(self) -> None:
        """Test handling of empty batches."""

        class DoubleFunc(ScalarFunction):
            @classmethod
            def catalog_output_type(cls) -> pa.DataType | type[AnyArrow]:
                return pa.int64()

            def compute(self, batch: pa.RecordBatch) -> pa.Array[Any]:
                import pyarrow.compute as pc

                return pc.multiply(batch.column("x"), 2)

        input_schema = pa.schema([("x", pa.int64())])
        invocation = make_scalar_invocation(input_schema)
        func = DoubleFunc(invocation=invocation, logger=structlog.get_logger())

        generator = func.run()
        next(generator)

        # Empty batch
        input_batch = pa.RecordBatch.from_pydict({"x": []}, schema=input_schema)
        output = generator.send(ProtocolInput(batch=input_batch))

        assert output.batch is not None
        assert output.batch.num_rows == 0
