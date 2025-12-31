"""Generic tests for TableFunctionGenerator behavior."""

import pyarrow as pa
import pytest
import structlog

from vgi.function import Arguments, Invocation
from vgi.table_function import (
    CardinalityInfo,
    Output,
    OutputGenerator,
    TableFunctionGenerator,
)
from vgi.testing import FunctionTestClientError, TableFunctionTestClient


class TestTableFunctionGeneratorProtocol:
    """Tests for the TableFunctionGenerator protocol."""

    def test_empty_process_generator(self) -> None:
        """Default process() should produce no output."""

        class EmptyFunction(TableFunctionGenerator):
            @property
            def output_schema(self) -> pa.Schema:
                return pa.schema([pa.field("x", pa.int64())])

        with TableFunctionTestClient(EmptyFunction) as client:
            outputs = list(client.table_function())

        # StopIteration from empty generator means no outputs
        assert len(outputs) == 0

    def test_single_batch_output(self) -> None:
        """Function yielding single batch should work."""

        class SingleBatchFunction(TableFunctionGenerator):
            @property
            def output_schema(self) -> pa.Schema:
                return pa.schema([pa.field("x", pa.int64())])

            def process(self) -> OutputGenerator:
                yield Output(
                    pa.RecordBatch.from_pydict(
                        {"x": [1, 2, 3]}, schema=self.output_schema
                    )
                )

        with TableFunctionTestClient(SingleBatchFunction) as client:
            outputs = list(client.table_function())

        assert len(outputs) == 1
        assert outputs[0].column("x").to_pylist() == [1, 2, 3]

    def test_multiple_batch_output(self) -> None:
        """Function yielding multiple batches should work."""

        class MultiBatchFunction(TableFunctionGenerator):
            @property
            def output_schema(self) -> pa.Schema:
                return pa.schema([pa.field("n", pa.int64())])

            def process(self) -> OutputGenerator:
                for i in range(3):
                    yield Output(
                        pa.RecordBatch.from_pydict(
                            {"n": [i]}, schema=self.output_schema
                        )
                    )

        with TableFunctionTestClient(MultiBatchFunction) as client:
            outputs = list(client.table_function())

        assert len(outputs) == 3
        for i, batch in enumerate(outputs):
            assert batch.column("n").to_pylist() == [i]


class TestTableFunctionGeneratorLifecycle:
    """Tests for the setup/teardown lifecycle."""

    def test_setup_called_before_process(self) -> None:
        """setup() should be called before process() runs."""
        call_order: list[str] = []

        class LifecycleFunction(TableFunctionGenerator):
            @property
            def output_schema(self) -> pa.Schema:
                return pa.schema([pa.field("x", pa.int64())])

            def setup(self) -> None:
                call_order.append("setup")

            def process(self) -> OutputGenerator:
                call_order.append("process")
                yield Output(
                    pa.RecordBatch.from_pydict({"x": [1]}, schema=self.output_schema)
                )

            def teardown(self) -> None:
                call_order.append("teardown")

        with TableFunctionTestClient(LifecycleFunction) as client:
            list(client.table_function())

        assert call_order == ["setup", "process", "teardown"]

    def test_teardown_called_on_exception(self) -> None:
        """teardown() should be called even if process() raises."""
        teardown_called = False

        class ExceptionFunction(TableFunctionGenerator):
            @property
            def output_schema(self) -> pa.Schema:
                return pa.schema([pa.field("x", pa.int64())])

            def process(self) -> OutputGenerator:
                raise ValueError("test error")
                yield  # Make it a generator

            def teardown(self) -> None:
                nonlocal teardown_called
                teardown_called = True

        with (
            TableFunctionTestClient(ExceptionFunction) as client,
            pytest.raises(FunctionTestClientError),
        ):
            list(client.table_function())

        assert teardown_called


class TestTableFunctionGeneratorSchemaValidation:
    """Tests for output schema validation."""

    def test_valid_schema_passes(self) -> None:
        """Output matching schema should pass validation."""

        class ValidSchemaFunction(TableFunctionGenerator):
            @property
            def output_schema(self) -> pa.Schema:
                return pa.schema([pa.field("x", pa.int64())])

            def process(self) -> OutputGenerator:
                yield Output(
                    pa.RecordBatch.from_pydict({"x": [1]}, schema=self.output_schema)
                )

        with TableFunctionTestClient(ValidSchemaFunction) as client:
            outputs = list(client.table_function())

        assert len(outputs) == 1

    def test_invalid_schema_raises(self) -> None:
        """Output with wrong schema should raise SchemaValidationError."""

        class InvalidSchemaFunction(TableFunctionGenerator):
            @property
            def output_schema(self) -> pa.Schema:
                return pa.schema([pa.field("x", pa.int64())])

            def process(self) -> OutputGenerator:
                # Return batch with wrong column name
                wrong_schema = pa.schema([pa.field("y", pa.int64())])
                wrong_batch = pa.RecordBatch.from_pydict(
                    {"y": [1]}, schema=wrong_schema
                )
                yield Output(wrong_batch)

        with (
            TableFunctionTestClient(InvalidSchemaFunction) as client,
            pytest.raises(FunctionTestClientError) as exc_info,
        ):
            list(client.table_function())

        # The exception message should mention schema mismatch
        assert "schema" in str(exc_info.value).lower()


class TestTableFunctionGeneratorCardinality:
    """Tests for cardinality hints."""

    def test_default_cardinality_is_none(self) -> None:
        """Default cardinality() should return None."""

        class NoCardinalityFunction(TableFunctionGenerator):
            @property
            def output_schema(self) -> pa.Schema:
                return pa.schema([pa.field("x", pa.int64())])

        invocation = Invocation(
            function_name="test",
            arguments=Arguments(),
            in_out_function_input_schema=None,
            correlation_id="test",
            invocation_id=b"test",
        )
        func = NoCardinalityFunction(
            invocation=invocation,
            logger=structlog.get_logger(),
        )

        assert func.cardinality() is None

    def test_custom_cardinality(self) -> None:
        """Custom cardinality() should be respected."""

        class CardinalityFunction(TableFunctionGenerator):
            @property
            def output_schema(self) -> pa.Schema:
                return pa.schema([pa.field("x", pa.int64())])

            def cardinality(self) -> CardinalityInfo:
                return CardinalityInfo(estimate=100, max=1000)

        invocation = Invocation(
            function_name="test",
            arguments=Arguments(),
            in_out_function_input_schema=None,
            correlation_id="test",
            invocation_id=b"test",
        )
        func = CardinalityFunction(
            invocation=invocation,
            logger=structlog.get_logger(),
        )

        cardinality = func.cardinality()
        assert cardinality is not None
        assert cardinality.estimate == 100
        assert cardinality.max == 1000


class TestTableFunctionGeneratorArguments:
    """Tests for argument handling."""

    def test_positional_arguments(self) -> None:
        """Function should receive positional arguments."""
        from vgi.arguments import Arg

        class ArgFunction(TableFunctionGenerator):
            count: int = Arg[int](0, doc="Count")  # type: ignore[assignment]

            @property
            def output_schema(self) -> pa.Schema:
                return pa.schema([pa.field("n", pa.int64())])

            def process(self) -> OutputGenerator:
                yield Output(
                    pa.RecordBatch.from_pydict(
                        {"n": list(range(self.count))}, schema=self.output_schema
                    )
                )

        with TableFunctionTestClient(ArgFunction) as client:
            outputs = list(
                client.table_function(arguments=Arguments(positional=(pa.scalar(5),)))
            )

        assert len(outputs) == 1
        assert outputs[0].column("n").to_pylist() == [0, 1, 2, 3, 4]

    def test_named_arguments(self) -> None:
        """Function should receive named arguments."""
        from vgi.arguments import Arg

        class NamedArgFunction(TableFunctionGenerator):
            value: int = Arg[int]("value", doc="Value")  # type: ignore[assignment]

            @property
            def output_schema(self) -> pa.Schema:
                return pa.schema([pa.field("result", pa.int64())])

            def process(self) -> OutputGenerator:
                yield Output(
                    pa.RecordBatch.from_pydict(
                        {"result": [self.value * 2]}, schema=self.output_schema
                    )
                )

        with TableFunctionTestClient(NamedArgFunction) as client:
            outputs = list(
                client.table_function(
                    arguments=Arguments(named={"value": pa.scalar(21)})
                )
            )

        assert len(outputs) == 1
        assert outputs[0].column("result").to_pylist() == [42]


class TestTableFunctionGeneratorEmptyBatch:
    """Tests for empty batch handling."""

    def test_empty_output_batch_property(self) -> None:
        """empty_output_batch should have correct schema and zero rows."""

        class TestFunction(TableFunctionGenerator):
            @property
            def output_schema(self) -> pa.Schema:
                return pa.schema(
                    [
                        pa.field("a", pa.int64()),
                        pa.field("b", pa.string()),
                    ]
                )

        invocation = Invocation(
            function_name="test",
            arguments=Arguments(),
            in_out_function_input_schema=None,
            correlation_id="test",
            invocation_id=b"test",
        )
        func = TestFunction(
            invocation=invocation,
            logger=structlog.get_logger(),
        )

        empty = func.empty_output_batch
        assert empty.num_rows == 0
        assert empty.schema == func.output_schema
