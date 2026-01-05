"""Tests for the @streaming decorator."""

import pyarrow as pa
import structlog

from vgi import (
    Invocation,
    Output,
    OutputGenerator,
    StreamingGenerator,
    TableInOutGenerator,
    streaming,
)
from vgi.log import Level, Message
from vgi.testing import TableInOutFunctionTestClient, batch


class EchoStreamingFunction(TableInOutGenerator):
    """Simple echo function using the @streaming decorator."""

    @streaming
    def process(self, b: pa.RecordBatch) -> StreamingGenerator:
        """Process batches without priming yield."""
        current: pa.RecordBatch | None = b
        while current is not None:
            current = yield Output(current)


class CountingStreamingFunction(TableInOutGenerator):
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
        current: pa.RecordBatch | None = b
        while current is not None:
            self.batch_count += 1
            current = yield Output(current)


class AccumulatingStreamingFunction(TableInOutGenerator):
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
        current: pa.RecordBatch | None = b
        while current is not None:
            # Accumulate
            col = current.column(0)
            for val in col.to_pylist():
                if val is not None:
                    self.total += val
            current = yield Output(self.empty_output_batch)

    def finalize(self) -> OutputGenerator:
        """Emit final aggregation result."""
        _ = yield None
        yield Output(
            pa.RecordBatch.from_pydict({"sum": [self.total]}, schema=self.output_schema)
        )


class LoggingStreamingFunction(TableInOutGenerator):
    """Function that logs using the @streaming decorator."""

    @streaming
    def process(self, b: pa.RecordBatch) -> StreamingGenerator:
        """Process batches with logging."""
        current: pa.RecordBatch | None = b
        while current is not None:
            yield Message(Level.INFO, f"Processing {current.num_rows} rows")
            current = yield Output(current)


class TestStreamingDecorator:
    """Tests for the @streaming decorator."""

    def test_basic_echo(self) -> None:
        """@streaming decorated function should echo batches."""
        with TableInOutFunctionTestClient(EchoStreamingFunction) as client:
            outputs = list(
                client.table_in_out_function(input=iter([batch(x=[1, 2, 3])]))
            )

        assert len(outputs) == 1
        assert outputs[0].to_pydict() == {"x": [1, 2, 3]}

    def test_multiple_batches(self) -> None:
        """@streaming should handle multiple input batches."""
        with TableInOutFunctionTestClient(EchoStreamingFunction) as client:
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
        with TableInOutFunctionTestClient(EchoStreamingFunction) as client:
            outputs = list(client.table_in_out_function(input=iter([])))

        assert len(outputs) == 0

    def test_state_accumulation(self) -> None:
        """@streaming should allow state accumulation."""
        with TableInOutFunctionTestClient(CountingStreamingFunction) as client:
            outputs = list(
                client.table_in_out_function(
                    input=iter([batch(x=[1]), batch(x=[2]), batch(x=[3])])
                )
            )

        assert len(outputs) == 3

    def test_accumulation_with_finalize(self) -> None:
        """@streaming should work with finalize() for aggregations."""
        with TableInOutFunctionTestClient(AccumulatingStreamingFunction) as client:
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
        with TableInOutFunctionTestClient(LoggingStreamingFunction) as client:
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

        class ManualEcho(TableInOutGenerator):
            def process(self, b: pa.RecordBatch) -> OutputGenerator:
                """Manual process without decorator."""
                _ = yield None
                current: pa.RecordBatch | None = b
                while True:
                    # Combined yield-and-receive (correct pattern)
                    current = yield Output(current)
                    if current is None:
                        break

        # Need to create separate lists for each client since iter() is consumed
        input_batches_1 = [batch(x=[1, 2]), batch(x=[3, 4])]
        input_batches_2 = [batch(x=[1, 2]), batch(x=[3, 4])]

        with TableInOutFunctionTestClient(ManualEcho) as client:
            manual_outputs = list(
                client.table_in_out_function(input=iter(input_batches_1))
            )

        with TableInOutFunctionTestClient(EchoStreamingFunction) as client:
            streaming_outputs = list(
                client.table_in_out_function(input=iter(input_batches_2))
            )

        assert len(manual_outputs) == len(streaming_outputs)
        for manual, streaming_out in zip(
            manual_outputs, streaming_outputs, strict=True
        ):
            assert manual.equals(streaming_out)
