"""Tests for the @streaming decorator."""

import pyarrow as pa
import structlog

from vgi import (
    Invocation,
    Output,
    OutputGenerator,
    StreamingGenerator,
    TableInOutGeneratorFunction,
    streaming,
)
from vgi.log import Level, Message
from vgi.testing import FunctionTestClient, batch


class EchoStreamingFunction(TableInOutGeneratorFunction):
    """Simple echo function using the @streaming decorator."""

    @streaming
    def process(self, b: pa.RecordBatch) -> StreamingGenerator:
        """Process batches without priming yield."""
        while b is not None:
            b = yield Output(b)


class CountingStreamingFunction(TableInOutGeneratorFunction):
    """Function that counts batches using @streaming."""

    def __init__(
        self, invocation: Invocation, logger: structlog.stdlib.BoundLogger
    ) -> None:
        """Initialize with batch counter."""
        super().__init__(invocation, logger)
        self.batch_count = 0

    @streaming
    def process(self, b: pa.RecordBatch) -> StreamingGenerator:
        """Process batches and count them."""
        while b is not None:
            self.batch_count += 1
            b = yield Output(b)


class AccumulatingStreamingFunction(TableInOutGeneratorFunction):
    """Function that accumulates and outputs empty batches during process."""

    def __init__(
        self, invocation: Invocation, logger: structlog.stdlib.BoundLogger
    ) -> None:
        """Initialize with accumulator."""
        super().__init__(invocation, logger)
        self.total = 0

    @property
    def output_schema(self) -> pa.Schema:
        """Define output schema for aggregation result."""
        return pa.schema([("sum", pa.int64())])

    @streaming
    def process(self, b: pa.RecordBatch) -> StreamingGenerator:
        """Accumulate values from batches."""
        while b is not None:
            # Accumulate
            col = b.column(0)
            for val in col.to_pylist():
                self.total += val
            b = yield Output(self.empty_output_batch)

    def finalize(self) -> OutputGenerator:
        """Emit final aggregation result."""
        _ = yield None
        yield Output(
            pa.RecordBatch.from_pydict({"sum": [self.total]}, schema=self.output_schema)
        )


class LoggingStreamingFunction(TableInOutGeneratorFunction):
    """Function that logs using the @streaming decorator."""

    @streaming
    def process(self, b: pa.RecordBatch) -> StreamingGenerator:
        """Process batches with logging."""
        while b is not None:
            yield Message(Level.INFO, f"Processing {b.num_rows} rows")
            b = yield Output(b)


class TestStreamingDecorator:
    """Tests for the @streaming decorator."""

    def test_basic_echo(self) -> None:
        """@streaming decorated function should echo batches."""
        with FunctionTestClient(EchoStreamingFunction) as client:
            outputs = list(
                client.table_in_out_function(input=iter([batch(x=[1, 2, 3])]))
            )

        assert len(outputs) == 1
        assert outputs[0].to_pydict() == {"x": [1, 2, 3]}

    def test_multiple_batches(self) -> None:
        """@streaming should handle multiple input batches."""
        with FunctionTestClient(EchoStreamingFunction) as client:
            outputs = list(
                client.table_in_out_function(
                    input=iter([batch(x=[1]), batch(x=[2]), batch(x=[3])])
                )
            )

        assert len(outputs) == 3
        assert outputs[0].to_pydict() == {"x": [1]}
        assert outputs[1].to_pydict() == {"x": [2]}
        assert outputs[2].to_pydict() == {"x": [3]}

    def test_empty_input(self) -> None:
        """@streaming should handle empty input."""
        with FunctionTestClient(EchoStreamingFunction) as client:
            outputs = list(client.table_in_out_function(input=iter([])))

        assert len(outputs) == 0

    def test_state_accumulation(self) -> None:
        """@streaming should allow state accumulation."""
        with FunctionTestClient(CountingStreamingFunction) as client:
            outputs = list(
                client.table_in_out_function(
                    input=iter([batch(x=[1]), batch(x=[2]), batch(x=[3])])
                )
            )

        assert len(outputs) == 3

    def test_accumulation_with_finalize(self) -> None:
        """@streaming should work with finalize() for aggregations."""
        with FunctionTestClient(AccumulatingStreamingFunction) as client:
            outputs = list(
                client.table_in_out_function(
                    input=iter([batch(x=[1, 2, 3]), batch(x=[4, 5])])
                )
            )

        # Should get empty batches during process, then result in finalize
        # Filter out empty batches
        non_empty = [o for o in outputs if o.num_rows > 0]
        assert len(non_empty) == 1
        assert non_empty[0].to_pydict() == {"sum": [15]}

    def test_logging(self) -> None:
        """@streaming should support yielding log messages."""
        with FunctionTestClient(LoggingStreamingFunction) as client:
            outputs = list(
                client.table_in_out_function(input=iter([batch(x=[1, 2, 3])]))
            )

            assert len(client.logs) >= 1
            assert any("Processing 3 rows" in log.message for log in client.logs)

        assert len(outputs) == 1


class TestStreamingDecoratorComparedToManual:
    """Compare @streaming decorator to manual generator implementation."""

    def test_equivalent_output(self) -> None:
        """@streaming decorated function should produce same output as manual."""

        class ManualEcho(TableInOutGeneratorFunction):
            def process(self, b: pa.RecordBatch) -> OutputGenerator:
                """Manual process without decorator."""
                _ = yield None
                while True:
                    # Combined yield-and-receive (correct pattern)
                    b = yield Output(b)
                    if b is None:
                        break

        # Need to create separate lists for each client since iter() is consumed
        input_batches_1 = [batch(x=[1, 2]), batch(x=[3, 4])]
        input_batches_2 = [batch(x=[1, 2]), batch(x=[3, 4])]

        with FunctionTestClient(ManualEcho) as client:
            manual_outputs = list(
                client.table_in_out_function(input=iter(input_batches_1))
            )

        with FunctionTestClient(EchoStreamingFunction) as client:
            streaming_outputs = list(
                client.table_in_out_function(input=iter(input_batches_2))
            )

        assert len(manual_outputs) == len(streaming_outputs)
        for manual, streaming_out in zip(
            manual_outputs, streaming_outputs, strict=True
        ):
            assert manual.equals(streaming_out)
