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

from vgi.function import CallData, GlobalInitResult
from vgi.table_function import CardinalityInfo
from vgi.table_in_out_function import ProcessResult, TableInOutFunction

__all__ = [
    "EchoFunction",
    "BufferInputFunction",
    "RepeatInputsFunction",
    "SumAllColumnsFunction",
]


class EchoFunction(TableInOutFunction):
    """Passthrough function that emits each input batch unchanged.

    USE CASE
    --------
    Testing, debugging, or as a no-op placeholder in a pipeline.

    SCHEMA TRANSFORMATION
    ---------------------
    Input:  any schema
    Output: same schema (passthrough)

    EXAMPLE
    -------
    Input:  [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    Output: [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    """


class BufferInputFunction(TableInOutFunction):
    """Buffering function that collects all input and emits during finalization.

    USE CASE
    --------
    When you need to see all data before producing output, or when you want to
    delay output until the stream is complete. Useful for sorting, deduplication,
    or operations that need the full dataset.

    BEHAVIOR
    --------
    - _output_schema(): Returns input schema unchanged
    - process_batch(batch, is_finalize=False): Stores batch in buffer, returns empty
    - process_batch(batch, is_finalize=True): Returns buffered batches one at a time

    SCHEMA TRANSFORMATION
    ---------------------
    Input:  any schema
    Output: same schema (passthrough)

    STATE
    -----
    self.buffered_batches: list[pa.RecordBatch]
        Accumulates all input batches in memory.
    self.finalize_index: int
        Tracks position when emitting buffered batches during finalize.

    WARNING
    -------
    Memory usage grows with input size. Not suitable for very large datasets.

    EXAMPLE
    -------
    Input stream:  batch1, batch2, batch3
    During processing: (empty), (empty), (empty)
    On finalize: batch1, batch2, batch3
    """

    def __init__(self, call_data: CallData) -> None:
        super().__init__(call_data)
        self.buffered_batches: list[pa.RecordBatch] = []
        self.finalize_index = 0

    def process_batch(
        self, init_data: GlobalInitResult, batch: pa.RecordBatch, is_finalize: bool
    ) -> ProcessResult:
        if is_finalize:
            if self.finalize_index < len(self.buffered_batches):
                out = self.buffered_batches[self.finalize_index]
                self.finalize_index += 1
                has_more = self.finalize_index < len(self.buffered_batches)
                return ProcessResult(out, has_more)
            return ProcessResult(None)
        # Store batch for later, emit nothing now
        self.buffered_batches.append(batch)
        return ProcessResult(None)


class RepeatInputsFunction(TableInOutFunction):
    """Explosion function that duplicates each input batch N times.

    USE CASE
    --------
    Data augmentation, testing with larger datasets, or any scenario where
    you need multiple copies of each input record.

    ARGUMENTS
    ---------
    arguments[0]: int (optional, default=2)
        Number of times to repeat each input batch.

    BEHAVIOR
    --------
    - _output_schema(): Returns input schema unchanged
    - process_batch(batch, is_finalize=False): Returns batch N times using has_more
    - process_batch(batch, is_finalize=True): Returns ProcessResult(None)

    SCHEMA TRANSFORMATION
    ---------------------
    Input:  any schema
    Output: same schema (passthrough)

    STATE
    -----
    self.repeat_count: int
        Number of times to emit each input batch.
    self.current_repeat: int
        Tracks current repetition count for the current batch.

    KEY PATTERN: MULTIPLE OUTPUTS FROM ONE INPUT
    ---------------------------------------------
    This function demonstrates how to produce multiple output batches from
    a single input batch using the has_more flag. The function tracks state
    to know how many more times to emit the current batch:

        self.current_repeat += 1
        has_more = self.current_repeat < self.repeat_count
        if not has_more:
            self.current_repeat = 0  # Reset for next batch
        return ProcessResult(batch, has_more)

    EXAMPLE
    -------
    With repeat_count=3:
    Input:  [{"a": 1}]
    Output: [{"a": 1}], [{"a": 1}], [{"a": 1}]
    """

    def __init__(self, call_data: CallData) -> None:
        super().__init__(call_data)
        args = call_data.arguments
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
        self.current_repeat = 0

    def process_batch(
        self, init_data: GlobalInitResult, batch: pa.RecordBatch, is_finalize: bool
    ) -> ProcessResult:
        if is_finalize:
            return ProcessResult(None)
        self.current_repeat += 1
        has_more = self.current_repeat < self.repeat_count
        if not has_more:
            self.current_repeat = 0  # Reset for next input batch
        return ProcessResult(batch, has_more)


class SumAllColumnsFunction(TableInOutFunction):
    """Aggregation function that computes column-wise sums across all batches.

    USE CASE
    --------
    Computing totals, aggregating metrics, or any full-stream reduction
    that produces a single summary row.

    BEHAVIOR
    --------
    - _output_schema(): Builds output schema from numeric columns, inits sums
    - process_batch(batch, is_finalize=False): Accumulates sums, returns empty
    - process_batch(batch, is_finalize=True): Returns single row with final sums

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

    KEY PATTERN: SCHEMA TRANSFORMATION IN _output_schema
    ----------------------------------------------------
    This function demonstrates inspecting input_schema to build a different
    output schema:

        def _output_schema(self):
            self.sums = {}
            output_fields = []
            for field in self.input_schema:
                if pa.types.is_integer(field.type):
                    out_type = pa.int64()
                elif pa.types.is_floating(field.type):
                    out_type = pa.float64()
                else:
                    continue  # Skip non-numeric
                output_fields.append(pa.field(field.name, out_type))
                self.sums[field.name] = pa.scalar(0, type=out_type)
            return pa.schema(output_fields)

    KEY PATTERN: ACCUMULATE THEN EMIT ON FINALIZE
    ---------------------------------------------
    During process_batch with is_finalize=False, accumulate state but emit None.
    During process_batch with is_finalize=True, emit the final aggregated result:

        def process_batch(self, batch, is_finalize):
            if is_finalize:
                return ProcessResult(pa.RecordBatch.from_pydict(...))
            # Update accumulators
            for name in self.sums:
                col_sum = pc.sum(batch.column(name))
                if col_sum.is_valid:
                    self.sums[name] = pc.add(self.sums[name], col_sum)
            return ProcessResult(None)

    EXAMPLE
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
        return CardinalityInfo(estimate=1, max=1)

    def _output_schema(self) -> pa.Schema:
        self.sums: dict[str, pa.Scalar] = {}
        output_fields = []
        for field in self.input_schema:
            if pa.types.is_integer(field.type):
                out_type = pa.int64()
            elif pa.types.is_floating(field.type):
                out_type = pa.float64()
            else:
                continue
            output_fields.append(pa.field(field.name, out_type))
            self.sums[field.name] = pa.scalar(0, type=out_type)
        return pa.schema(output_fields)

    def process_batch(
        self, init_data: GlobalInitResult, batch: pa.RecordBatch, is_finalize: bool
    ) -> ProcessResult:
        if is_finalize:
            return ProcessResult(
                pa.RecordBatch.from_pydict(
                    {name: [val] for name, val in self.sums.items()},
                    schema=self.output_schema,
                )
            )
        for name in self.sums:
            col_sum = pc.sum(batch.column(name))
            if col_sum.is_valid:
                self.sums[name] = pc.add(self.sums[name], col_sum)
        return ProcessResult(None)
