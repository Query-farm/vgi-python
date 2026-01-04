"""Tests for TableInOutFunction (callback-based API)."""

import pyarrow as pa
import pyarrow.compute as pc
import structlog

from vgi.function import Arg, Arguments, Invocation, InvocationType
from vgi.ipc_utils import RecordBatchState
from vgi.log import Level
from vgi.table_in_out_function import (
    ProtocolInput,
    ProtocolOutput,
    TableInOutFunction,
)


def make_invocation(input_schema: pa.Schema) -> Invocation:
    """Create a minimal Invocation for testing."""
    return Invocation(
        function_name="test",
        input_schema=input_schema,
        function_type=InvocationType.TABLE,
        correlation_id="test",
        invocation_id=b"test",
        arguments=Arguments(),
    )


def make_invocation_with_args(
    input_schema: pa.Schema, positional: list[int]
) -> Invocation:
    """Create an Invocation with positional arguments."""
    return Invocation(
        function_name="test",
        input_schema=input_schema,
        function_type=InvocationType.TABLE,
        correlation_id="test",
        invocation_id=b"test",
        arguments=Arguments(positional=tuple(pa.scalar(v) for v in positional)),
    )


def run_simple_function(
    func: TableInOutFunction,
    input_batches: list[pa.RecordBatch],
) -> list[pa.RecordBatch]:
    """Run a simple function through the protocol and collect outputs."""
    generator = func.run()

    # Prime the generator
    next(generator)

    output_batches = []

    # Send input batches
    for batch in input_batches:
        protocol_input = ProtocolInput(batch=batch)
        output = generator.send(protocol_input)

        if output.batch is not None and output.batch.num_rows > 0:
            output_batches.append(output.batch)

        # Handle multiple outputs per input
        while output.status.value == "HAVE_MORE_OUTPUT":
            output = generator.send(protocol_input)
            if output.batch is not None and output.batch.num_rows > 0:
                output_batches.append(output.batch)

    # Send finalize signal
    empty_batch = pa.RecordBatch.from_arrays(
        [pa.array([], type=field.type) for field in func.output_schema],
        schema=func.output_schema,
    )
    finalize_input = ProtocolInput.create_finalize(empty_batch)
    output = generator.send(finalize_input)

    if output.batch is not None and output.batch.num_rows > 0:
        output_batches.append(output.batch)

    # Collect remaining finalize outputs
    while output.status.value == "HAVE_MORE_OUTPUT":
        output = generator.send(finalize_input)
        if output.batch is not None and output.batch.num_rows > 0:
            output_batches.append(output.batch)

    return output_batches


class TestPassthrough:
    """Tests for default passthrough behavior."""

    def test_passthrough_returns_same_data(self) -> None:
        """Default transform() returns input unchanged."""

        class PassthroughFunction(TableInOutFunction):
            pass

        schema = pa.schema([("x", pa.int64())])
        invocation = make_invocation(schema)
        func = PassthroughFunction(invocation, structlog.get_logger())

        input_batches = [
            pa.RecordBatch.from_pydict({"x": [1, 2, 3]}, schema=schema),
            pa.RecordBatch.from_pydict({"x": [4, 5]}, schema=schema),
        ]

        outputs = run_simple_function(func, input_batches)

        # Should have same number of batches
        assert len(outputs) == 2

        # Data should match
        assert outputs[0].to_pydict() == {"x": [1, 2, 3]}
        assert outputs[1].to_pydict() == {"x": [4, 5]}


class TestTransform:
    """Tests for transform() override."""

    def test_transform_single_batch(self) -> None:
        """transform() can modify each batch."""

        class DoubleFunction(TableInOutFunction):
            def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
                doubled = pc.multiply(batch.column("x"), 2)
                return pa.RecordBatch.from_arrays([doubled], schema=batch.schema)

        schema = pa.schema([("x", pa.int64())])
        invocation = make_invocation(schema)
        func = DoubleFunction(invocation, structlog.get_logger())

        input_batches = [
            pa.RecordBatch.from_pydict({"x": [1, 2, 3]}, schema=schema),
        ]

        outputs = run_simple_function(func, input_batches)

        assert len(outputs) == 1
        assert outputs[0].to_pydict() == {"x": [2, 4, 6]}

    def test_transform_returns_list(self) -> None:
        """transform() can return multiple batches."""

        class TripleFunction(TableInOutFunction):
            def transform(self, batch: pa.RecordBatch) -> list[pa.RecordBatch]:
                return [batch, batch, batch]

        schema = pa.schema([("x", pa.int64())])
        invocation = make_invocation(schema)
        func = TripleFunction(invocation, structlog.get_logger())

        input_batches = [
            pa.RecordBatch.from_pydict({"x": [1, 2]}, schema=schema),
        ]

        outputs = run_simple_function(func, input_batches)

        # Should have 3 outputs from 1 input
        assert len(outputs) == 3
        for output in outputs:
            assert output.to_pydict() == {"x": [1, 2]}

    def test_transform_with_arguments(self) -> None:
        """transform() can use function arguments."""

        class RepeatFunction(TableInOutFunction):
            count = Arg[int](0)

            def transform(self, batch: pa.RecordBatch) -> list[pa.RecordBatch]:
                return [batch] * self.count

        schema = pa.schema([("x", pa.int64())])
        invocation = make_invocation_with_args(schema, [4])
        func = RepeatFunction(invocation, structlog.get_logger())

        input_batches = [
            pa.RecordBatch.from_pydict({"x": [1]}, schema=schema),
        ]

        outputs = run_simple_function(func, input_batches)

        assert len(outputs) == 4


class TestFinish:
    """Tests for finish() override."""

    def test_finish_emits_aggregation(self) -> None:
        """finish() can emit final aggregated results."""

        class SumFunction(TableInOutFunction):
            def __init__(
                self, invocation: Invocation, logger: structlog.stdlib.BoundLogger
            ) -> None:
                super().__init__(invocation, logger)
                self.total = 0

            @property
            def output_schema(self) -> pa.Schema:
                return pa.schema([("sum", pa.int64())])

            def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
                self.total += pc.sum(batch.column("x")).as_py()
                return self.empty_output_batch

            def finish(self) -> list[pa.RecordBatch]:
                return [
                    pa.RecordBatch.from_pydict(
                        {"sum": [self.total]}, schema=self.output_schema
                    )
                ]

            def max_processes(self) -> int:
                return 1

        schema = pa.schema([("x", pa.int64())])
        invocation = make_invocation(schema)
        func = SumFunction(invocation, structlog.get_logger())

        input_batches = [
            pa.RecordBatch.from_pydict({"x": [1, 2, 3]}, schema=schema),
            pa.RecordBatch.from_pydict({"x": [4, 5]}, schema=schema),
        ]

        outputs = run_simple_function(func, input_batches)

        # Should only have the finalize output
        assert len(outputs) == 1
        assert outputs[0].to_pydict() == {"sum": [15]}

    def test_finish_returns_multiple_batches(self) -> None:
        """finish() can return multiple batches."""

        class BufferFunction(TableInOutFunction):
            def __init__(
                self, invocation: Invocation, logger: structlog.stdlib.BoundLogger
            ) -> None:
                super().__init__(invocation, logger)
                self.buffer: list[pa.RecordBatch] = []

            def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
                self.buffer.append(batch)
                return self.empty_output_batch

            def finish(self) -> list[pa.RecordBatch]:
                return self.buffer

            def max_processes(self) -> int:
                return 1

        schema = pa.schema([("x", pa.int64())])
        invocation = make_invocation(schema)
        func = BufferFunction(invocation, structlog.get_logger())

        input_batches = [
            pa.RecordBatch.from_pydict({"x": [1, 2]}, schema=schema),
            pa.RecordBatch.from_pydict({"x": [3, 4]}, schema=schema),
        ]

        outputs = run_simple_function(func, input_batches)

        # Should emit buffered batches in finalize
        assert len(outputs) == 2
        assert outputs[0].to_pydict() == {"x": [1, 2]}
        assert outputs[1].to_pydict() == {"x": [3, 4]}


class TestOutputSchema:
    """Tests for output_schema override."""

    def test_output_schema_different_from_input(self) -> None:
        """output_schema can define different output columns."""

        class LengthFunction(TableInOutFunction):
            @property
            def output_schema(self) -> pa.Schema:
                return pa.schema([("length", pa.int64())])

            def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
                # Type ignore: stubs don't properly type cast result as StringArray
                lengths = pc.utf8_length(batch.column("name"))  # type: ignore[call-overload]
                return pa.RecordBatch.from_arrays([lengths], schema=self.output_schema)

        schema = pa.schema([("name", pa.string())])
        invocation = make_invocation(schema)
        func = LengthFunction(invocation, structlog.get_logger())

        input_batches = [
            pa.RecordBatch.from_pydict({"name": ["hello", "world!"]}, schema=schema),
        ]

        outputs = run_simple_function(func, input_batches)

        assert len(outputs) == 1
        assert outputs[0].schema == pa.schema([("length", pa.int64())])
        assert outputs[0].to_pydict() == {"length": [5, 6]}


class TestEmptyOutput:
    """Tests for empty output handling."""

    def test_transform_returns_empty_batch(self) -> None:
        """transform() can return empty_output_batch to skip output."""

        class FilterOddFunction(TableInOutFunction):
            def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
                # Filter to only even values
                x_col = batch.column("x")
                mask = pc.equal(
                    pc.bit_wise_and(x_col, pa.scalar(1, type=pa.int64())),
                    pa.scalar(0, type=pa.int64()),
                )
                filtered = pc.filter(x_col, mask)
                if len(filtered) == 0:
                    return self.empty_output_batch
                return pa.RecordBatch.from_arrays([filtered], schema=batch.schema)

        schema = pa.schema([("x", pa.int64())])
        invocation = make_invocation(schema)
        func = FilterOddFunction(invocation, structlog.get_logger())

        input_batches = [
            pa.RecordBatch.from_pydict({"x": [1, 3, 5]}, schema=schema),  # All odd
            pa.RecordBatch.from_pydict({"x": [2, 4]}, schema=schema),  # All even
        ]

        outputs = run_simple_function(func, input_batches)

        # Should only have 1 output (the even batch)
        assert len(outputs) == 1
        assert outputs[0].to_pydict() == {"x": [2, 4]}

    def test_finish_returns_empty_list(self) -> None:
        """finish() returns empty list by default."""

        class PassthroughFunction(TableInOutFunction):
            pass

        schema = pa.schema([("x", pa.int64())])
        invocation = make_invocation(schema)
        func = PassthroughFunction(invocation, structlog.get_logger())

        input_batches = [
            pa.RecordBatch.from_pydict({"x": [1, 2]}, schema=schema),
        ]

        outputs = run_simple_function(func, input_batches)

        # Should only have the transform output, no finalize output
        assert len(outputs) == 1
        assert outputs[0].to_pydict() == {"x": [1, 2]}


def run_simple_function_with_logs(
    func: TableInOutFunction,
    input_batches: list[pa.RecordBatch],
) -> tuple[list[pa.RecordBatch], list[ProtocolOutput]]:
    """Run a simple function and collect both outputs and log messages."""
    generator = func.run()

    # Prime the generator
    next(generator)

    output_batches: list[pa.RecordBatch] = []
    all_outputs: list[ProtocolOutput] = []

    # Send input batches
    for batch in input_batches:
        protocol_input = ProtocolInput(batch=batch)
        output = generator.send(protocol_input)
        all_outputs.append(output)

        if output.batch is not None and output.batch.num_rows > 0:
            output_batches.append(output.batch)

        # Handle multiple outputs per input (including log messages)
        while output.status.value == "HAVE_MORE_OUTPUT":
            output = generator.send(protocol_input)
            all_outputs.append(output)
            if output.batch is not None and output.batch.num_rows > 0:
                output_batches.append(output.batch)

    # Send finalize signal
    empty_batch = pa.RecordBatch.from_arrays(
        [pa.array([], type=field.type) for field in func.output_schema],
        schema=func.output_schema,
    )
    finalize_input = ProtocolInput.create_finalize(empty_batch)
    output = generator.send(finalize_input)
    all_outputs.append(output)

    if output.batch is not None and output.batch.num_rows > 0:
        output_batches.append(output.batch)

    # Collect remaining finalize outputs
    while output.status.value == "HAVE_MORE_OUTPUT":
        output = generator.send(finalize_input)
        all_outputs.append(output)
        if output.batch is not None and output.batch.num_rows > 0:
            output_batches.append(output.batch)

    return output_batches, all_outputs


class TestLogging:
    """Tests for log() method."""

    def test_log_in_transform(self) -> None:
        """log() can emit messages during transform()."""

        class LoggingFunction(TableInOutFunction):
            def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
                self.log(Level.INFO, f"Processing {batch.num_rows} rows")
                return batch

        schema = pa.schema([("x", pa.int64())])
        invocation = make_invocation(schema)
        func = LoggingFunction(invocation, structlog.get_logger())

        input_batches = [
            pa.RecordBatch.from_pydict({"x": [1, 2, 3]}, schema=schema),
        ]

        outputs, all_outputs = run_simple_function_with_logs(func, input_batches)

        # Should have 1 output batch
        assert len(outputs) == 1
        assert outputs[0].to_pydict() == {"x": [1, 2, 3]}

        # Should have at least one log message
        log_messages = [o for o in all_outputs if o.log_message is not None]
        assert len(log_messages) == 1
        assert log_messages[0].log_message is not None
        assert log_messages[0].log_message.level == Level.INFO
        assert "Processing 3 rows" in log_messages[0].log_message.message

    def test_log_in_finish(self) -> None:
        """log() can emit messages during finish()."""

        class LoggingAggregateFunction(TableInOutFunction):
            def __init__(
                self, invocation: Invocation, logger: structlog.stdlib.BoundLogger
            ) -> None:
                super().__init__(invocation, logger)
                self.total = 0

            @property
            def output_schema(self) -> pa.Schema:
                return pa.schema([("sum", pa.int64())])

            def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
                self.total += pc.sum(batch.column("x")).as_py()
                return self.empty_output_batch

            def finish(self) -> list[pa.RecordBatch]:
                self.log(Level.INFO, f"Final sum: {self.total}")
                return [
                    pa.RecordBatch.from_pydict(
                        {"sum": [self.total]}, schema=self.output_schema
                    )
                ]

            def max_processes(self) -> int:
                return 1

        schema = pa.schema([("x", pa.int64())])
        invocation = make_invocation(schema)
        func = LoggingAggregateFunction(invocation, structlog.get_logger())

        input_batches = [
            pa.RecordBatch.from_pydict({"x": [1, 2, 3]}, schema=schema),
        ]

        outputs, all_outputs = run_simple_function_with_logs(func, input_batches)

        # Should have 1 output batch with sum
        assert len(outputs) == 1
        assert outputs[0].to_pydict() == {"sum": [6]}

        # Should have log message from finish()
        log_messages = [o for o in all_outputs if o.log_message is not None]
        assert len(log_messages) == 1
        assert log_messages[0].log_message is not None
        assert "Final sum: 6" in log_messages[0].log_message.message

    def test_multiple_log_messages(self) -> None:
        """Multiple log() calls queue multiple messages."""

        class MultiLogFunction(TableInOutFunction):
            def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
                self.log(Level.DEBUG, "Starting transform")
                self.log(Level.INFO, f"Rows: {batch.num_rows}")
                return batch

        schema = pa.schema([("x", pa.int64())])
        invocation = make_invocation(schema)
        func = MultiLogFunction(invocation, structlog.get_logger())

        input_batches = [
            pa.RecordBatch.from_pydict({"x": [1, 2]}, schema=schema),
        ]

        outputs, all_outputs = run_simple_function_with_logs(func, input_batches)

        # Should have 1 output batch
        assert len(outputs) == 1

        # Should have 2 log messages
        log_messages = [o for o in all_outputs if o.log_message is not None]
        assert len(log_messages) == 2
        assert log_messages[0].log_message is not None
        assert log_messages[1].log_message is not None
        assert log_messages[0].log_message.level == Level.DEBUG
        assert log_messages[1].log_message.level == Level.INFO


class TestDistributedState:
    """Tests for save_state() and load_states() methods."""

    def test_save_state_returns_none_by_default(self) -> None:
        """Default save_state() returns None."""

        class SimpleFunction(TableInOutFunction):
            pass

        schema = pa.schema([("x", pa.int64())])
        invocation = make_invocation(schema)
        func = SimpleFunction(invocation, structlog.get_logger())

        assert func.save_state() is None

    def test_save_state_can_be_overridden(self) -> None:
        """save_state() can return RecordBatchState."""

        class StatefulFunction(TableInOutFunction):
            def __init__(
                self, invocation: Invocation, logger: structlog.stdlib.BoundLogger
            ) -> None:
                super().__init__(invocation, logger)
                self.count = 0

            def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
                self.count += batch.num_rows
                return batch

            def save_state(self) -> RecordBatchState:
                return RecordBatchState(
                    batch=pa.RecordBatch.from_pydict(
                        {"count": [self.count]},
                        schema=pa.schema([("count", pa.int64())]),
                    )
                )

        schema = pa.schema([("x", pa.int64())])
        invocation = make_invocation(schema)
        func = StatefulFunction(invocation, structlog.get_logger())

        # Simulate processing
        func.count = 10

        state = func.save_state()
        assert state is not None
        assert state.batch.to_pydict() == {"count": [10]}

    def test_load_states_receives_states(self) -> None:
        """load_states() receives list of RecordBatchState."""

        class DistributedSumFunction(TableInOutFunction):
            def __init__(
                self, invocation: Invocation, logger: structlog.stdlib.BoundLogger
            ) -> None:
                super().__init__(invocation, logger)
                self.total = 0

            @property
            def output_schema(self) -> pa.Schema:
                return pa.schema([("sum", pa.int64())])

            def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
                self.total += pc.sum(batch.column("x")).as_py()
                return self.empty_output_batch

            def save_state(self) -> RecordBatchState:
                return RecordBatchState(
                    batch=pa.RecordBatch.from_pydict(
                        {"partial_sum": [self.total]},
                        schema=pa.schema([("partial_sum", pa.int64())]),
                    )
                )

            def load_states(self, states: list[RecordBatchState]) -> None:
                table = pa.Table.from_batches([s.batch for s in states])
                self.total = pc.sum(table.column("partial_sum")).as_py()

            def finish(self) -> list[pa.RecordBatch]:
                return [
                    pa.RecordBatch.from_pydict(
                        {"sum": [self.total]}, schema=self.output_schema
                    )
                ]

        schema = pa.schema([("x", pa.int64())])
        invocation = make_invocation(schema)
        func = DistributedSumFunction(invocation, structlog.get_logger())

        # Simulate receiving states from multiple workers
        state1 = RecordBatchState(
            batch=pa.RecordBatch.from_pydict(
                {"partial_sum": [10]}, schema=pa.schema([("partial_sum", pa.int64())])
            )
        )
        state2 = RecordBatchState(
            batch=pa.RecordBatch.from_pydict(
                {"partial_sum": [25]}, schema=pa.schema([("partial_sum", pa.int64())])
            )
        )

        func.load_states([state1, state2])

        # Should have combined totals
        assert func.total == 35
