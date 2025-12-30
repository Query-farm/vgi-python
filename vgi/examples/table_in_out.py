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

import os
import sqlite3
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import structlog
from platformdirs import user_state_dir

from vgi.function import Request
from vgi.ipc_utils import deserialize_record_batch, serialize_record_batch
from vgi.log import Level, Message
from vgi.table_function import CardinalityInfo
from vgi.table_in_out_function import (
    Function,
    Output,
    OutputGenerator,
)

__all__ = [
    "EchoFunction",
    "BufferInputFunction",
    "RepeatInputsFunction",
    "SumAllColumnsFunction",
    "SumAllColumnsFunctionDistributed",
    "SumAllColumnsFunctionWithLogging",
    "ExceptionProcessFunction",
    "ExceptionFinalizeFunction",
]


class EchoFunction(Function):
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


class BufferInputFunction(Function):
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
            continue_from_current_input = index < len(self.buffered_batches) - 1
            yield Output(b, continue_from_current_input)


class RepeatInputsFunction(Function):
    """Explosion function that duplicates each input batch N times.

    USE CASE
    --------
    Data augmentation, testing with larger datasets, or any scenario where
    you need multiple copies of each input record.

    Arguments:
    ---------
    arguments.positional[0]: int (required)
        Number of times to repeat each input batch.

    BEHAVIOR
    --------
    - output_schema: Returns input schema unchanged (default)
    - process(): For each input, yields it N times using continue_from_current_input

    SCHEMA TRANSFORMATION
    ---------------------
    Input:  any schema
    Output: same schema (passthrough)

    STATE
    -----
    self.repeat_count: int
        Number of times to emit each input batch (set in __init__).

    KEY PATTERN: MULTIPLE OUTPUTS FROM ONE INPUT
    ---------------------------------------------
    This function demonstrates how to produce multiple output batches from
    a single input batch using the continue_from_current_input flag. All
    iterations yield continue_from_current_input=True; the loop's `yield None`
    receives the next batch:

        while True:
            for i in range(self.repeat_count):
                # continue_from_current_input=True for all iterations
                yield Output(batch, continue_from_current_input=True)
            batch = yield None
            if batch is None:
                break

    Example:
    -------
    With repeat_count=3:
    Input:  [{"a": 1}]
    Output: [{"a": 1}], [{"a": 1}], [{"a": 1}]

    """

    def __init__(
        self, invocation: Request, logger: structlog.stdlib.BoundLogger
    ) -> None:
        """Initialize with repeat count from positional argument."""
        super().__init__(invocation=invocation, logger=logger)
        args = invocation.arguments
        if len(args.positional) != 1:
            raise ValueError(
                "RepeatInputsFunction requires exactly one positional argument"
            )
        repeat_count = args.positional[0]
        if repeat_count is None:
            raise ValueError(
                "RepeatInputsFunction requires a non-null repeat count argument"
            )
        repeat_count = repeat_count.as_py()
        if not isinstance(repeat_count, int):
            raise ValueError(
                "RepeatInputsFunction requires an integer repeat count argument"
            )
        if repeat_count < 1:
            raise ValueError("Repeat count must be at least 1")

        self.repeat_count = repeat_count

    def process(self, batch: pa.RecordBatch) -> OutputGenerator:
        """Emit each input batch repeat_count times."""
        _ = yield None

        while True:
            for _ in range(self.repeat_count):
                yield Output(batch, continue_from_current_input=True)

            batch = yield None
            if batch is None:
                break


class SumAllColumnsFunction(Function):
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

    def cardinality(self) -> CardinalityInfo | None:
        """Return cardinality estimate of exactly 1 row."""
        return CardinalityInfo(estimate=1, max=1)

    def __init__(
        self, invocation: Request, logger: structlog.stdlib.BoundLogger
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
    """Distributed aggregation function that computes column-wise sums."""

    def cardinality(self) -> CardinalityInfo | None:
        """Return cardinality estimate of exactly 1 row."""
        return CardinalityInfo(estimate=1, max=1)

    def max_processes(self) -> int:
        """Return the number of processes to use for this function."""
        return 8

    def _database(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        return conn

    def __init__(
        self, invocation: Request, logger: structlog.stdlib.BoundLogger
    ) -> None:
        """Initialize the sum accumulator."""
        super().__init__(invocation=invocation, logger=logger)

        state_dir = Path(user_state_dir("vgi-testing"))
        state_dir.mkdir(parents=True, exist_ok=True)

        self.db_path = (state_dir / "sum-distributed.db").resolve()
        self.pid = os.getpid()

        if not Path(self.db_path).exists():
            conn = self._database()
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sum_state (
                    invoke_id BLOB,
                    process_id INTEGER,
                    state_data BLOB,
                    PRIMARY KEY (invoke_id, process_id)
                )
            """)
            conn.commit()
            conn.close()

    def _store_partial_state(self, state: dict[str, pa.Scalar]) -> None:
        # Convert dict of scalars to a single-row RecordBatch
        batch = pa.RecordBatch.from_pydict({k: [v.as_py()] for k, v in state.items()})
        state_bytes = serialize_record_batch(batch)

        if self.init_identifier is None:
            raise ValueError("init_identifier is not set")

        conn = self._database()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO sum_state (invoke_id, process_id, state_data)
            VALUES (?, ?, ?)
            ON CONFLICT(invoke_id, process_id)
            DO UPDATE SET state_data = excluded.state_data
            """,
            (
                self.init_identifier,
                self.pid,
                state_bytes,
            ),
        )
        conn.commit()
        conn.close()

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

            # Write this to the shared storage, because we don't know
            # if we'll be notified at the end of the batches, our
            # process may just close.
            self._store_partial_state(self.sums)

            batch = yield None
            if batch is None:
                break

    def finalize(self) -> OutputGenerator:
        """Emit single row containing the column sums.

        This function can run on any process.
        """
        _ = yield None

        conn = self._database()
        cursor = conn.cursor()

        # Read all state data for this invocation
        cursor.execute(
            """
            SELECT state_data FROM sum_state WHERE invoke_id = ?
            """,
            (self.init_identifier,),
        )
        rows = cursor.fetchall()

        # Delete the rows since they're no longer needed
        cursor.execute(
            """
            DELETE FROM sum_state WHERE invoke_id = ?
            """,
            (self.init_identifier,),
        )
        conn.commit()
        conn.close()

        # Deserialize all state batches and combine into a table
        batches = [deserialize_record_batch(row[0]) for row in rows]
        if not batches:
            # No data was processed, emit zeros
            yield Output(
                pa.RecordBatch.from_pydict(
                    {field.name: [0] for field in self.output_schema},
                    schema=self.output_schema,
                )
            )
            return

        table = pa.Table.from_batches(batches)

        # Compute sums for all columns
        sums = {col: pc.sum(table.column(col)).as_py() for col in table.schema.names}

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
