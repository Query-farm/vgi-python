"""Example table-in/table-out function implementations.

This module contains ready-to-use table functions that demonstrate common patterns.
Each function is documented to serve as a reference for implementing new functions.

AVAILABLE FUNCTIONS
-------------------
EchoFunction              - Passthrough, no transformation
BufferInputFunction       - Collects all input, emits on finalize
RepeatInputsFunction      - Duplicates each input batch N times
SumAllColumnsFunction     - Aggregates numeric columns into sums
"""

import pyarrow as pa
import pyarrow.compute as pc
import structlog

from vgi.function import Arg, Invocation
from vgi.ipc_utils import RecordBatchState
from vgi.log import Level, Message
from vgi.table_function import CardinalityInfo
from vgi.table_in_out_function import (
    Output,
    OutputGenerator,
    TableInOutFunction,
    TableInOutGeneratorFunction,
)

__all__ = [
    "EchoFunction",
    "BufferInputFunction",
    "RepeatInputsFunction",
    "SumAllColumnsFunction",
    "SumAllColumnsFunctionDistributed",
    "SumAllColumnsSimpleDistributed",
    "SumAllColumnsFunctionWithLogging",
    "ExceptionProcessFunction",
    "ExceptionFinalizeFunction",
]


class EchoFunction(TableInOutGeneratorFunction):
    """Passthrough function that emits each input batch unchanged.

    USE CASE
    --------
    Testing, debugging, or as a no-op placeholder in a pipeline.

    SCHEMA TRANSFORMATION
    ---------------------
    Input:  any schema
    Output: same schema (passthrough)

    Example:
    -------
    Input:  [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    Output: [{"a": 1, "b": 2}, {"a": 3, "b": 4}]

    """


class BufferInputFunction(TableInOutGeneratorFunction):
    """Buffering function that collects all input and emits during finalization.

    USE CASE
    --------
    When you need to see all data before producing output, or when you want to
    delay output until the stream is complete. Useful for sorting, deduplication,
    or operations that need the full dataset.

    BEHAVIOR
    --------
    - output_schema: Returns input schema unchanged (default)
    - process(): Stores batches in buffer and yields empty results
    - finalize(): Yields buffered batches one at a time

    SCHEMA TRANSFORMATION
    ---------------------
    Input:  any schema
    Output: same schema (passthrough)

    STATE
    -----
    buffered_batches: list[pa.RecordBatch]
        Accumulates all input batches in memory (instance attribute).

    Warning:
    -------
    Memory usage grows with input size. Not suitable for very large datasets.

    Example:
    -------
    Input stream:  batch1, batch2, batch3
    During processing: (empty), (empty), (empty)
    On finalize: batch1, batch2, batch3

    """

    def max_processes(self) -> int:
        """Single process only - accumulates all batches in memory."""
        return 1

    def process(self, batch: pa.RecordBatch) -> OutputGenerator:
        """Buffer all input batches without producing output."""
        self.buffered_batches: list[pa.RecordBatch] = [batch]

        _ = yield None

        while True:
            batch = yield None
            if batch is None:
                break
            self.buffered_batches.append(batch)

    def finalize(self) -> OutputGenerator:
        """Emit all buffered batches sequentially."""
        # Emit buffered batches one at a time during finalize
        _ = yield None

        for index, b in enumerate(self.buffered_batches):
            has_more = index < len(self.buffered_batches) - 1
            yield Output(b, has_more)


class RepeatInputsFunction(TableInOutGeneratorFunction):
    """Explosion function that duplicates each input batch N times.

    USE CASE
    --------
    Data augmentation, testing with larger datasets, or any scenario where
    you need multiple copies of each input record.

    Arguments:
    ---------
    repeat_count = Arg[int](0): (required)
        Number of times to repeat each input batch.

    BEHAVIOR
    --------
    - output_schema: Returns input schema unchanged (default)
    - process(): For each input, yields it N times using has_more=True
    - max_processes(): Returns high value to enable parallel processing

    SCHEMA TRANSFORMATION
    ---------------------
    Input:  any schema
    Output: same schema (passthrough)

    STATE
    -----
    self.repeat_count: int
        Number of times to emit each input batch (declared via Arg descriptor).

    KEY PATTERN: MULTIPLE OUTPUTS FROM ONE INPUT
    ---------------------------------------------
    This function demonstrates how to produce multiple output batches from
    a single input batch using the has_more flag. All iterations yield
    has_more=True; the loop's `yield None` receives the next batch:

        while True:
            for i in range(self.repeat_count):
                yield Output(batch, has_more=True)
            batch = yield None
            if batch is None:
                break

    KEY PATTERN: STATELESS DISTRIBUTED PROCESSING
    ----------------------------------------------
    This function is stateless - each batch is processed independently without
    any cross-batch state. This makes it trivially parallelizable using the
    default max_processes() from the base class.

    Example:
    -------
    With repeat_count=3:
    Input:  [{"a": 1}]
    Output: [{"a": 1}], [{"a": 1}], [{"a": 1}]

    """

    repeat_count = Arg[int](0)

    def __init__(
        self, invocation: Invocation, logger: structlog.stdlib.BoundLogger
    ) -> None:
        """Initialize and validate repeat count argument."""
        super().__init__(invocation=invocation, logger=logger)

        # Access to trigger validation early
        if self.repeat_count < 1:
            raise ValueError("Repeat count must be at least 1")

    def process(self, batch: pa.RecordBatch) -> OutputGenerator:
        """Emit each input batch repeat_count times."""
        _ = yield None

        while True:
            for _ in range(self.repeat_count):
                yield Output(batch, has_more=True)

            batch = yield None
            if batch is None:
                break


class SumAllColumnsFunction(TableInOutGeneratorFunction):
    """Aggregation function that computes column-wise sums across all batches.

    USE CASE
    --------
    Computing totals, aggregating metrics, or any full-stream reduction
    that produces a single summary row.

    BEHAVIOR
    --------
    - output_schema: Builds output schema from numeric columns only
    - process(): Accumulates sums and yields empty results
    - finalize(): Yields single row with final sums

    SCHEMA TRANSFORMATION
    ---------------------
    Input:  any schema with numeric columns
    Output: only numeric columns, promoted to int64/float64

    For each input column:
    - Integer types -> int64
    - Floating types -> float64
    - Non-numeric types -> excluded from output

    STATE
    -----
    self.sums: dict[str, pa.Scalar]
        Running sum for each numeric column. Keys are column names,
        values are PyArrow scalars with the output type.

    KEY PATTERN: SCHEMA TRANSFORMATION IN output_schema
    ---------------------------------------------------
    This function demonstrates inspecting input_schema to build a different
    output schema as a property:

        @property
        def output_schema(self) -> pa.Schema:
            output_fields = []
            for field in self.input_schema:
                if pa.types.is_integer(field.type):
                    output_fields.append(pa.field(field.name, pa.int64()))
                elif pa.types.is_floating(field.type):
                    output_fields.append(pa.field(field.name, pa.float64()))
            return pa.schema(output_fields)

    KEY PATTERN: ACCUMULATE IN process(), EMIT IN finalize()
    --------------------------------------------------------
    In process(), accumulate state but yield empty results.
    In finalize(), yield the final aggregated result:

        def process(self, batch: pa.RecordBatch) -> OutputGenerator:
            _ = yield None
            while True:
                for name in self.sums:
                    col_sum = pc.sum(batch.column(name))
                    if col_sum.is_valid:
                        self.sums[name] = pc.add(self.sums[name], col_sum)
                batch = yield None
                if batch is None:
                    break

        def finalize(self) -> OutputGenerator:
            _ = yield None
            yield Output(pa.RecordBatch.from_pydict(...))

    Example:
    -------
    Input schema: {"a": int32, "b": float32, "name": string}
    Output schema: {"a": int64, "b": float64}  (string column excluded)

    Input batches:
      [{"a": 1, "b": 1.5, "name": "x"}, {"a": 2, "b": 2.5, "name": "y"}]
      [{"a": 3, "b": 3.0, "name": "z"}]

    Output (single row):
      [{"a": 6, "b": 7.0}]

    """

    def max_processes(self) -> int:
        """Single process only - accumulates state across batches."""
        return 1

    def cardinality(self) -> CardinalityInfo | None:
        """Return cardinality estimate of exactly 1 row."""
        return CardinalityInfo(estimate=1, max=1)

    def __init__(
        self, invocation: Invocation, logger: structlog.stdlib.BoundLogger
    ) -> None:
        """Initialize the sum accumulator."""
        super().__init__(invocation=invocation, logger=logger)
        self.sums: dict[str, pa.Scalar] = {}

    @property
    def output_schema(self) -> pa.Schema:
        """Build schema with only numeric columns promoted to int64/float64."""
        if self.input_schema is None:
            raise ValueError("input_schema is required but was None")
        output_fields = []
        for field in self.input_schema:
            if pa.types.is_integer(field.type):
                out_type = pa.int64()
            elif pa.types.is_floating(field.type):
                out_type = pa.float64()
            else:
                continue
            output_fields.append(pa.field(field.name, out_type))

        return self.apply_projection(pa.schema(output_fields))

    def process(self, batch: pa.RecordBatch) -> OutputGenerator:
        """Accumulate column sums across all batches."""
        # The priming of the generator
        _ = yield None

        # Initialize sums to zero for each numeric column
        for field in self.output_schema:
            self.sums[field.name] = pa.scalar(0, type=field.type)

        # Process all batches
        while True:
            for name in self.sums:
                col_sum = pc.sum(batch.column(name))
                if col_sum.is_valid:
                    self.sums[name] = pc.add(self.sums[name], col_sum)

            batch = yield None
            if batch is None:
                break

    def finalize(self) -> OutputGenerator:
        """Emit single row containing the column sums."""
        _ = yield None

        # Finalize: emit single row with sums
        yield Output(
            pa.RecordBatch.from_pydict(
                {name: [val] for name, val in self.sums.items()},
                schema=self.output_schema,
            )
        )


class SumAllColumnsFunctionDistributed(SumAllColumnsFunction):
    """Distributed aggregation function that computes column-wise sums.

    This function demonstrates the distributed state management framework:
    - Workers accumulate partial sums during process()
    - On GeneratorExit, each worker stores its state via store_state()
    - During finalize(), the primary worker collects all states via collect_states()

    Uses the default max_processes() from the base class to enable parallelism.

    """

    def process(self, batch: pa.RecordBatch) -> OutputGenerator:
        """Accumulate column sums across all batches."""
        _ = yield None

        sums: dict[str, pa.Scalar] = {}
        # Initialize sums to zero for each numeric column
        for field in self.output_schema:
            sums[field.name] = pa.scalar(0, type=field.type)

        # Process all batches
        try:
            while True:
                for name in sums:
                    col_sum = pc.sum(batch.column(name))
                    if col_sum.is_valid:
                        sums[name] = pc.add(sums[name], col_sum)

                batch = yield None
                if batch is None:
                    break
        except GeneratorExit:
            # Generator is being closed - save state with explicit schema
            state_batch = pa.RecordBatch.from_pydict(
                {k: [v.as_py()] for k, v in sums.items()},
                schema=self.output_schema,
            )
            self.store_state(RecordBatchState(batch=state_batch))
            raise

    def finalize(self) -> OutputGenerator:
        """Emit single row containing the column sums."""
        _ = yield None

        # Collect all worker states using the framework
        states = self.collect_states(RecordBatchState)

        if not states:
            # No data was processed, emit zeros
            yield Output(
                pa.RecordBatch.from_pydict(
                    {field.name: [0] for field in self.output_schema},
                    schema=self.output_schema,
                )
            )
            return

        # Combine all state batches into a table
        table = pa.Table.from_batches([s.batch for s in states])

        # Compute sums using output_schema for consistent column ordering
        sums = {
            field.name: pc.sum(table.column(field.name)).as_py()
            for field in self.output_schema
        }

        # Emit single row with sums
        yield Output(
            pa.RecordBatch.from_pydict(
                {name: [val] for name, val in sums.items()},
                schema=self.output_schema,
            )
        )


class SumAllColumnsFunctionWithLogging(SumAllColumnsFunction):
    """Aggregation function with logging that computes column-wise sums.

    Extends SumAllColumnsFunction to demonstrate logging capabilities.
    Emits log messages during process() and finalize() phases.

    See SumAllColumnsFunction for full documentation of the aggregation pattern.

    """

    def process(self, batch: pa.RecordBatch) -> OutputGenerator:
        """Accumulate column sums across all batches with logging."""
        _ = yield None

        # Initialize sums to zero for each numeric column
        for field in self.output_schema:
            self.sums[field.name] = pa.scalar(0, type=field.type)

        # Process all batches with logging
        while True:
            yield Message(
                level=Level.INFO,
                message=f"Processing batch with {batch.num_rows} rows",
            )

            for name in self.sums:
                col_sum = pc.sum(batch.column(name))
                if col_sum.is_valid:
                    self.sums[name] = pc.add(self.sums[name], col_sum)

            batch = yield None
            if batch is None:
                break

    def finalize(self) -> OutputGenerator:
        """Emit single row containing the column sums with logging."""
        _ = yield None

        yield Message(
            level=Level.INFO,
            message="Finalizing and emitting sums",
        )

        yield Output(
            pa.RecordBatch.from_pydict(
                {name: [val] for name, val in self.sums.items()},
                schema=self.output_schema,
            )
        )


class ExceptionProcessFunction(SumAllColumnsFunction):
    """A function that raises an exception on the second batch."""

    def process(self, batch: pa.RecordBatch) -> OutputGenerator:
        """Raise an exception on the second batch."""
        _ = yield None  # priming

        batch_index = 1  # First batch is from parameter
        while True:
            if batch_index % 2 == 0:
                raise ValueError(f"Intentional exception on batch {batch_index}")
            batch = yield None
            if batch is None:
                break
            batch_index += 1


class ExceptionFinalizeFunction(SumAllColumnsFunction):
    """A class that demonstrates an exception raised during finalize()."""

    def finalize(self) -> OutputGenerator:
        """Emit single row containing the column sums with logging."""
        _ = yield None

        raise ValueError("Intentional exception during finalize()")


class SumAllColumnsSimpleDistributed(TableInOutFunction):
    """Distributed aggregation using the simple callback API.

    This function demonstrates TableInOutFunction with distributed
    state management using save_state() and load_states(). It's equivalent
    to SumAllColumnsFunctionDistributed but uses the simpler callback API.

    PATTERN: DISTRIBUTED AGGREGATION WITH SIMPLE API
    -------------------------------------------------
    1. Accumulate partial results in transform()
    2. Override save_state() to serialize partial results
    3. Override load_states() to merge results from all workers
    4. Emit final result in finish()

    Unlike single-process aggregations, this function:
    - Uses default max_processes() (allows parallelism)
    - Stores partial state via save_state() before finalize
    - Merges all worker states via load_states() before finish()

    Example:
    -------
    Input batches (split across workers):
      Worker 1: [{a: 1, b: 1.0}, {a: 2, b: 2.0}]
      Worker 2: [{a: 3, b: 3.0}]

    Each worker computes partial sums:
      Worker 1 state: {a: 3, b: 3.0}
      Worker 2 state: {a: 3, b: 3.0}

    Primary worker merges states in load_states():
      Combined: {a: 6, b: 6.0}

    Output (single row):
      [{a: 6, b: 6.0}]

    """

    def __init__(
        self, invocation: Invocation, logger: structlog.stdlib.BoundLogger
    ) -> None:
        """Initialize with empty sums dict."""
        super().__init__(invocation=invocation, logger=logger)
        self.sums: dict[str, pa.Scalar] = {}

    def cardinality(self) -> CardinalityInfo | None:
        """Return cardinality estimate of exactly 1 row."""
        return CardinalityInfo(estimate=1, max=1)

    @property
    def output_schema(self) -> pa.Schema:
        """Build schema with only numeric columns promoted to int64/float64."""
        if self.input_schema is None:
            raise ValueError("input_schema is required but was None")
        output_fields = []
        for field in self.input_schema:
            if pa.types.is_integer(field.type):
                out_type = pa.int64()
            elif pa.types.is_floating(field.type):
                out_type = pa.float64()
            else:
                continue
            output_fields.append(pa.field(field.name, out_type))

        return self.apply_projection(pa.schema(output_fields))

    def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        """Accumulate column sums. Emit nothing during processing."""
        # Initialize sums on first batch
        if not self.sums:
            for field in self.output_schema:
                self.sums[field.name] = pa.scalar(0, type=field.type)

        # Add this batch's values to running sums
        for name in self.sums:
            col_sum = pc.sum(batch.column(name))
            if col_sum.is_valid:
                self.sums[name] = pc.add(self.sums[name], col_sum)

        return self.empty_output_batch

    def save_state(self) -> RecordBatchState | None:
        """Save partial sums for distributed processing."""
        if not self.sums:
            return None

        state_batch = pa.RecordBatch.from_pydict(
            {k: [v.as_py()] for k, v in self.sums.items()},
            schema=self.output_schema,
        )
        return RecordBatchState(batch=state_batch)

    def load_states(self, states: list[RecordBatchState]) -> None:
        """Merge partial sums from all workers."""
        if not states:
            return

        # Combine all state batches into a table
        table = pa.Table.from_batches([s.batch for s in states])

        # Sum each column across all workers
        for field in self.output_schema:
            total = pc.sum(table.column(field.name))
            self.sums[field.name] = pa.scalar(
                total.as_py() if total.is_valid else 0, type=field.type
            )

    def finish(self) -> list[pa.RecordBatch]:
        """Emit single row with final sums."""
        if not self.sums:
            # No data was processed, emit zeros
            return [
                pa.RecordBatch.from_pydict(
                    {field.name: [0] for field in self.output_schema},
                    schema=self.output_schema,
                )
            ]

        return [
            pa.RecordBatch.from_pydict(
                {name: [val.as_py()] for name, val in self.sums.items()},
                schema=self.output_schema,
            )
        ]
