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

from collections.abc import Generator

import pyarrow as pa
import pyarrow.compute as pc
import structlog

from vgi.function import CallData
from vgi.table_function import CardinalityInfo
from vgi.table_in_out_function import ProcessInput, ProcessResult, TableInOutFunction

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
    - output_schema: Returns input schema unchanged (default)
    - process_batches(): During DATA phase, stores batches in buffer and yields
      empty results. During FINALIZE phase, yields buffered batches one at a time.

    SCHEMA TRANSFORMATION
    ---------------------
    Input:  any schema
    Output: same schema (passthrough)

    STATE
    -----
    buffered_batches: list[pa.RecordBatch]
        Accumulates all input batches in memory (local to generator).

    WARNING
    -------
    Memory usage grows with input size. Not suitable for very large datasets.

    EXAMPLE
    -------
    Input stream:  batch1, batch2, batch3
    During processing: (empty), (empty), (empty)
    On finalize: batch1, batch2, batch3
    """

    def process_batches(self) -> Generator[ProcessResult, ProcessInput | None, None]:
        self.buffered_batches: list[pa.RecordBatch] = []

        _ = yield ProcessResult(None)

        result = ProcessResult(None)
        while True:
            input = yield result
            if input is None:
                raise ValueError("Expected ProcessInput, got None")

            if input.is_finalize:
                break
            self.buffered_batches.append(input.batch)

        # Emit buffered batches one at a time during finalize
        for index, batch in enumerate(self.buffered_batches):
            has_more = index < len(self.buffered_batches) - 1
            result = ProcessResult(batch, has_more)
            yield result


class RepeatInputsFunction(TableInOutFunction):
    """Explosion function that duplicates each input batch N times.

    USE CASE
    --------
    Data augmentation, testing with larger datasets, or any scenario where
    you need multiple copies of each input record.

    ARGUMENTS
    ---------
    arguments.positional[0]: int (required)
        Number of times to repeat each input batch.

    BEHAVIOR
    --------
    - output_schema: Returns input schema unchanged (default)
    - process_batches(): For each input batch, yields it N times using has_more
      flag. During FINALIZE phase, yields empty result.

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
    a single input batch using the has_more flag in a loop:

        for i in range(self.repeat_count):
            has_more = i < self.repeat_count - 1
            yield ProcessResult(input.batch, has_more)

    EXAMPLE
    -------
    With repeat_count=3:
    Input:  [{"a": 1}]
    Output: [{"a": 1}], [{"a": 1}], [{"a": 1}]
    """

    def __init__(
        self, call_data: CallData, logger: structlog.stdlib.BoundLogger
    ) -> None:
        super().__init__(call_data=call_data, logger=logger)
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

    def process_batches(self) -> Generator[ProcessResult, ProcessInput | None, None]:
        _ = yield ProcessResult(None)

        result = ProcessResult(None)

        while True:
            input = yield result
            if input is None:
                raise ValueError("Expected ProcessInput, got None")

            if input.is_finalize:
                break

            for i in range(self.repeat_count):
                input = yield ProcessResult(
                    input.batch, has_more=(i < self.repeat_count)
                )
                if input is None:
                    raise ValueError("Expected ProcessInput, got None")

        yield ProcessResult(None)


class SumAllColumnsFunction(TableInOutFunction):
    """Aggregation function that computes column-wise sums across all batches.

    USE CASE
    --------
    Computing totals, aggregating metrics, or any full-stream reduction
    that produces a single summary row.

    BEHAVIOR
    --------
    - output_schema: Builds output schema from numeric columns only
    - process_batches(): During DATA phase, accumulates sums and yields empty.
      During FINALIZE phase, yields single row with final sums.

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

    KEY PATTERN: ACCUMULATE THEN EMIT ON FINALIZE
    ---------------------------------------------
    During DATA phase (is_finalize=False), accumulate state but yield None.
    During FINALIZE phase (is_finalize=True), yield the final aggregated result:

        while True:
            if input.is_finalize:
                yield ProcessResult(pa.RecordBatch.from_pydict(...))
                break
            # Update accumulators
            for name in sums:
                col_sum = pc.sum(input.batch.column(name))
                if col_sum.is_valid:
                    sums[name] = pc.add(sums[name], col_sum)
            input = yield ProcessResult(None)

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

    sums: dict[str, pa.Scalar] = {}

    @property
    def output_schema(self) -> pa.Schema:
        output_fields = []
        assert self.input_schema is not None
        for field in self.input_schema:
            if pa.types.is_integer(field.type):
                out_type = pa.int64()
            elif pa.types.is_floating(field.type):
                out_type = pa.float64()
            else:
                continue
            output_fields.append(pa.field(field.name, out_type))

        return self.apply_projection(pa.schema(output_fields))

    def process_batches(self) -> Generator[ProcessResult, ProcessInput | None, None]:
        # The priming of the generator
        _ = yield ProcessResult(None)

        # Initialize sums to zero for each numeric column
        for field in self.output_schema:
            self.sums[field.name] = pa.scalar(0, type=field.type)

        # Need an input to start, so just yield once, but the output
        # will be ignored.
        input = yield ProcessResult(None)
        if input is None:
            raise ValueError("Expected ProcessInput, got None")

        while True:
            if input.is_finalize:
                yield ProcessResult(
                    pa.RecordBatch.from_pydict(
                        {name: [val] for name, val in self.sums.items()},
                        schema=self.output_schema,
                    )
                )
                break

            for name in self.sums:
                col_sum = pc.sum(input.batch.column(name))
                if col_sum.is_valid:
                    self.sums[name] = pc.add(self.sums[name], col_sum)

            input = yield ProcessResult(None)
            if input is None:
                raise ValueError("Expected ProcessInput, got None")
