"""Tests for TableInOutFunction (callback-based API)."""

import pyarrow as pa
import pyarrow.compute as pc
import structlog

from tests.conftest import make_invocation
from vgi import schema
from vgi.arguments import Arg, Arguments
from vgi.invocation import Invocation
from vgi.ipc_utils import RecordBatchState
from vgi.log import Level
from vgi.table_in_out_function import TableInOutFunction
from vgi.testing import TableInOutFunctionTestClient, batch


class TestPassthrough:
    """Tests for default passthrough behavior."""

    def test_passthrough_returns_same_data(self) -> None:
        """Default transform() returns input unchanged."""

        class PassthroughFunction(TableInOutFunction):
            pass

        with TableInOutFunctionTestClient(PassthroughFunction) as client:
            outputs = list(
                client.table_in_out_function(
                    input=iter([batch(x=[1, 2, 3]), batch(x=[4, 5])]),
                )
            )

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

        with TableInOutFunctionTestClient(DoubleFunction) as client:
            outputs = list(
                client.table_in_out_function(input=iter([batch(x=[1, 2, 3])]))
            )

        assert len(outputs) == 1
        assert outputs[0].to_pydict() == {"x": [2, 4, 6]}

    def test_transform_returns_list(self) -> None:
        """transform() can return multiple batches."""

        class TripleFunction(TableInOutFunction):
            def transform(self, batch: pa.RecordBatch) -> list[pa.RecordBatch]:
                return [batch, batch, batch]

        with TableInOutFunctionTestClient(TripleFunction) as client:
            outputs = list(client.table_in_out_function(input=iter([batch(x=[1, 2])])))

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

        with TableInOutFunctionTestClient(RepeatFunction) as client:
            outputs = list(
                client.table_in_out_function(
                    input=iter([batch(x=[1])]),
                    arguments=Arguments(positional=(pa.scalar(4),)),
                )
            )

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

            class Meta:
                max_workers = 1

            @property
            def output_schema(self) -> pa.Schema:
                return schema(sum=pa.int64())

            def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
                self.total += pc.sum(batch.column("x")).as_py()
                return self.empty_output_batch

            def finish(self) -> list[pa.RecordBatch]:
                return [
                    pa.RecordBatch.from_pydict(
                        {"sum": [self.total]}, schema=self.output_schema
                    )
                ]

        with TableInOutFunctionTestClient(SumFunction) as client:
            outputs = list(
                client.table_in_out_function(
                    input=iter([batch(x=[1, 2, 3]), batch(x=[4, 5])])
                )
            )

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

            class Meta:
                max_workers = 1

            def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
                self.buffer.append(batch)
                return self.empty_output_batch

            def finish(self) -> list[pa.RecordBatch]:
                return self.buffer

        with TableInOutFunctionTestClient(BufferFunction) as client:
            outputs = list(
                client.table_in_out_function(
                    input=iter([batch(x=[1, 2]), batch(x=[3, 4])])
                )
            )

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
                return schema(length=pa.int64())

            def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
                # Type ignore: stubs don't properly type cast result as StringArray
                lengths = pc.utf8_length(batch.column("name"))  # type: ignore[call-overload]
                return pa.RecordBatch.from_arrays([lengths], schema=self.output_schema)

        with TableInOutFunctionTestClient(LengthFunction) as client:
            outputs = list(
                client.table_in_out_function(
                    input=iter([batch(name=["hello", "world!"])])
                )
            )

        assert len(outputs) == 1
        assert outputs[0].schema == schema(length=pa.int64())
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

        with TableInOutFunctionTestClient(FilterOddFunction) as client:
            outputs = list(
                client.table_in_out_function(
                    input=iter([batch(x=[1, 3, 5]), batch(x=[2, 4])])  # odd, even
                )
            )

        # Should only have 1 output (the even batch)
        assert len(outputs) == 1
        assert outputs[0].to_pydict() == {"x": [2, 4]}

    def test_finish_returns_empty_list(self) -> None:
        """finish() returns empty list by default."""

        class PassthroughFunction(TableInOutFunction):
            pass

        with TableInOutFunctionTestClient(PassthroughFunction) as client:
            outputs = list(client.table_in_out_function(input=iter([batch(x=[1, 2])])))

        # Should only have the transform output, no finalize output
        assert len(outputs) == 1
        assert outputs[0].to_pydict() == {"x": [1, 2]}


class TestLogging:
    """Tests for log() method."""

    def test_log_in_transform(self) -> None:
        """log() can emit messages during transform()."""

        class LoggingFunction(TableInOutFunction):
            def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
                self.log(Level.INFO, f"Processing {batch.num_rows} rows")
                return batch

        with TableInOutFunctionTestClient(LoggingFunction) as client:
            outputs = list(
                client.table_in_out_function(input=iter([batch(x=[1, 2, 3])]))
            )

            # Should have 1 output batch
            assert len(outputs) == 1
            assert outputs[0].to_pydict() == {"x": [1, 2, 3]}

            # Should have at least one log message
            assert len(client.logs) == 1
            assert client.logs[0].level == Level.INFO
            assert "Processing 3 rows" in client.logs[0].message

    def test_log_in_finish(self) -> None:
        """log() can emit messages during finish()."""

        class LoggingAggregateFunction(TableInOutFunction):
            def __init__(
                self, invocation: Invocation, logger: structlog.stdlib.BoundLogger
            ) -> None:
                super().__init__(invocation, logger)
                self.total = 0

            class Meta:
                max_workers = 1

            @property
            def output_schema(self) -> pa.Schema:
                return schema(sum=pa.int64())

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

        with TableInOutFunctionTestClient(LoggingAggregateFunction) as client:
            outputs = list(
                client.table_in_out_function(input=iter([batch(x=[1, 2, 3])]))
            )

            # Should have 1 output batch with sum
            assert len(outputs) == 1
            assert outputs[0].to_pydict() == {"sum": [6]}

            # Should have log message from finish()
            assert len(client.logs) == 1
            assert "Final sum: 6" in client.logs[0].message

    def test_multiple_log_messages(self) -> None:
        """Multiple log() calls queue multiple messages."""

        class MultiLogFunction(TableInOutFunction):
            def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
                self.log(Level.DEBUG, "Starting transform")
                self.log(Level.INFO, f"Rows: {batch.num_rows}")
                return batch

        with TableInOutFunctionTestClient(MultiLogFunction) as client:
            outputs = list(client.table_in_out_function(input=iter([batch(x=[1, 2])])))

            # Should have 1 output batch
            assert len(outputs) == 1

            # Should have 2 log messages
            assert len(client.logs) == 2
            assert client.logs[0].level == Level.DEBUG
            assert client.logs[1].level == Level.INFO


class TestDistributedState:
    """Tests for save_state() and load_states() methods."""

    def test_save_state_returns_none_by_default(self) -> None:
        """Default save_state() returns None."""

        class SimpleFunction(TableInOutFunction):
            pass

        s = schema(x=pa.int64())
        invocation = make_invocation(s)
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
                        schema=schema(count=pa.int64()),
                    )
                )

        s = schema(x=pa.int64())
        invocation = make_invocation(s)
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
                return schema(sum=pa.int64())

            def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
                self.total += pc.sum(batch.column("x")).as_py()
                return self.empty_output_batch

            def save_state(self) -> RecordBatchState:
                return RecordBatchState(
                    batch=pa.RecordBatch.from_pydict(
                        {"partial_sum": [self.total]},
                        schema=schema(partial_sum=pa.int64()),
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

        s = schema(x=pa.int64())
        invocation = make_invocation(s)
        func = DistributedSumFunction(invocation, structlog.get_logger())

        # Simulate receiving states from multiple workers
        state1 = RecordBatchState(
            batch=pa.RecordBatch.from_pydict(
                {"partial_sum": [10]}, schema=schema(partial_sum=pa.int64())
            )
        )
        state2 = RecordBatchState(
            batch=pa.RecordBatch.from_pydict(
                {"partial_sum": [25]}, schema=schema(partial_sum=pa.int64())
            )
        )

        func.load_states([state1, state2])

        # Should have combined totals
        assert func.total == 35
