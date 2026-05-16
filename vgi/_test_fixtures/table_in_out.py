"""Example table-in/table-out function implementations for testing VGI.

WARNING: EXAMPLE/TEST FUNCTIONS ONLY
-------------------------------------
These functions are reference implementations for testing and validating the VGI
protocol. They are NOT intended for production use. The VGI protocol will have
multiple implementations (Python, Go, JavaScript), and these examples serve as:

1. Protocol conformance tests - Verify implementations correctly handle the
   VGI streaming protocol.
2. Pattern demonstrations - Show how to implement common function patterns
3. Cross-implementation test cases - Ensure consistent behavior across languages

Production considerations like memory limits, error recovery, and performance
optimizations are intentionally omitted to keep the examples simple and focused
on protocol correctness.

AVAILABLE FUNCTIONS
-------------------
EchoFunction                   - Passthrough, no transformation
BufferInputFunction            - Collects all input, emits on finalize
FilterBySettingFunction        - Filters rows by threshold setting
RepeatInputsFunction           - Duplicates each input batch N times
SumAllColumnsFunction          - Aggregates numeric columns into sums
ExceptionProcessFunction       - Raises exception during process (test)
ExceptionFinalizeFunction      - Raises exception during finalize (test)
SumAllColumnsSimpleDistributed - Distributed aggregation via callback API
CrashOnProcessFunction         - SIGKILLs the worker mid-process (test)
CrashOnCombineFunction         - Raises during combine() (test)
CrashOnFinalizeFunction        - Raises during finalize() (test)
HangOnProcessFunction          - Sleeps forever in process() (manual cancel test)
LargeStateFunction             - Buffers ~N MB per state_id (IPC chunking test)
"""

from __future__ import annotations

import os
import signal
import time
from dataclasses import dataclass
from typing import Annotated, Any

import pyarrow as pa
import pyarrow.compute as pc
from vgi_rpc import ArrowSerializableDataclass, ArrowType
from vgi_rpc.rpc import OutputCollector
from vgi_rpc.utils import empty_batch

from vgi.arguments import Arg, Setting, TableInput
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.schema_utils import schema
from vgi.table_buffering_function import (
    TableBufferingFunction,
    TableBufferingParams,
)
from vgi.table_function import BindParams, ProcessParams, TableCardinality
from vgi.table_in_out_function import (
    TableInOutFunction,
    TableInOutGenerator,
)


# Per-tick cursor state for finalize streams that drain a state_log via
# state_log_scan. Wire-serializable so the producer-mode stream survives
# HTTP tick boundaries.
@dataclass
class _LogDrainState(ArrowSerializableDataclass):
    """Cursor over a per-state state_log; after_id starts at -1 (before-first)."""

    # Namespace under which finalize draws batches. Defaults to b"buf" —
    # the conventional location process() writes via state_append.
    ns: bytes = b"buf"
    after_id: int = -1

__all__ = [
    "EchoFunction",
    "BufferInputFunction",
    "FilterBySettingFunction",
    "RepeatInputsFunction",
    "SumAllColumnsFunction",
    "SumAllColumnsSimpleDistributed",
    "ExceptionProcessFunction",
    "ExceptionFinalizeFunction",
    "CrashOnProcessFunction",
    "CrashOnCombineFunction",
    "CrashOnFinalizeFunction",
    "HangOnProcessFunction",
    "LargeStateFunction",
    "OrderedBufferInputFunction",
    "BatchIndexBufferInputFunction",
]


@dataclass(slots=True, frozen=True, kw_only=True)
class SingleTableArguments:
    """Arguments for a table in/out function that just takes a table."""

    data: Annotated[TableInput, Arg(0, doc="Input table")]


class EchoFunction(TableInOutGenerator[SingleTableArguments]):
    """Passthrough function that emits each input batch unchanged.

    USE CASE
    --------
    Testing, debugging, or as a no-op placeholder in a pipeline.

    SCHEMA TRANSFORMATION
    ---------------------
    Input:  any schema
    Output: same schema (passthrough), with optional projection and filtering

    PUSHDOWN SUPPORT
    ----------------
    - projection_pushdown: Only returns requested columns
    - filter_pushdown: Filters rows based on pushed-down predicates
    - auto_apply_filters: Automatically applies filters to output batches

    Example:
    -------
    Input:  [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    Output: [{"a": 1, "b": 2}, {"a": 3, "b": 4}]

    """

    class Meta:
        """Metadata for EchoFunction."""

        name = "echo"
        description = "Passthrough function that emits each input batch unchanged"
        categories = ["utility", "debug"]
        tags = {"category": "debug", "type": "passthrough"}
        projection_pushdown = True
        filter_pushdown = True
        auto_apply_filters = True
        examples = [
            FunctionExample(
                sql="SELECT * FROM echo((SELECT * FROM input_table))",
                description="Pass through all rows unchanged",
            )
        ]

    @classmethod
    def on_bind(cls, params: BindParams[SingleTableArguments]) -> BindResponse:
        """Produce the output schema."""
        assert params.bind_call.input_schema is not None
        return BindResponse(output_schema=params.bind_call.input_schema)


class BufferInputFunction(TableBufferingFunction[SingleTableArguments, _LogDrainState]):
    """Buffering function — collects all input, emits during finalize.

    One bucket per execution: ``process()`` returns ``params.execution_id``
    for every call and appends to a single shared state_log under
    ``(b"buf", b"")``. ``combine()`` collapses every state_id (all are
    identical) to a single finalize_state_id. ``finalize()`` cursor-drains
    one batch per tick.

    Schema:
        Input:  any schema
        Output: same schema (passthrough)
    """

    class Meta:
        name = "buffer_input"
        description = "Collects all input batches and emits during finalization"
        categories = ["utility", "buffer"]
        examples = [
            FunctionExample(
                sql="SELECT * FROM buffer_input((SELECT * FROM input_table))",
                description="Buffer all input and emit on finalize",
            )
        ]

    @classmethod
    def on_bind(cls, params: BindParams[SingleTableArguments]) -> BindResponse:
        assert params.bind_call.input_schema is not None
        return BindResponse(output_schema=params.bind_call.input_schema)

    @classmethod
    def process(
        cls,
        batch: pa.RecordBatch,
        params: TableBufferingParams[SingleTableArguments],
    ) -> bytes:
        """Append the batch to the shared state_log; return execution_id."""
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)
        params.storage.state_append(b"buf", b"", sink.getvalue().to_pybytes())
        return params.execution_id

    @classmethod
    def combine(
        cls,
        state_ids: list[bytes],
        params: TableBufferingParams[SingleTableArguments],
    ) -> list[bytes]:
        # Every state_id is params.execution_id; collapse to one stream.
        return [params.execution_id]

    @classmethod
    def initial_finalize_state(
        cls,
        finalize_state_id: bytes,
        params: TableBufferingParams[SingleTableArguments],
    ) -> _LogDrainState:
        return _LogDrainState(ns=b"buf", after_id=-1)

    @classmethod
    def finalize(
        cls,
        params: TableBufferingParams[SingleTableArguments],
        finalize_state_id: bytes,
        state: _LogDrainState,
        out: OutputCollector,
    ) -> None:
        """Emit one buffered batch per tick; finish at end-of-log."""
        rows = params.storage.state_log_scan(
            state.ns, b"", after_id=state.after_id, limit=1,
        )
        if not rows:
            out.finish()
            return
        log_id, value = rows[0]
        out.emit(pa.ipc.open_stream(value).read_next_batch())
        state.after_id = log_id


class FilterBySettingFunction(TableInOutGenerator[SingleTableArguments]):
    """Filters input rows where the value column meets a threshold setting.

    USE CASE
    --------
    Demonstrates how table-in-out functions can use DuckDB settings to control
    behavior. The threshold setting determines which rows pass through: only
    rows where the "value" column >= threshold are emitted.

    The Setting() on on_bind() serves solely to register ``threshold`` in
    required_settings metadata. The actual filtering uses params.settings
    in process().

    SCHEMA TRANSFORMATION
    ---------------------
    Input:  any schema (must contain a "value" column)
    Output: same schema (rows filtered by threshold)

    Example:
    -------
    With threshold=5 and input [{"value": 3}, {"value": 7}]:
    Output: [{"value": 7}]

    """

    class Meta:
        """Metadata for FilterBySettingFunction."""

        name = "filter_by_setting"
        description = "Filter rows where value column >= threshold setting"
        categories = ["transform", "settings"]
        examples = [
            FunctionExample(
                sql="SELECT * FROM filter_by_setting((SELECT * FROM input_table))",
                description="Filter rows using the threshold setting",
            )
        ]

    @classmethod
    def on_bind(
        cls,
        params: BindParams[SingleTableArguments],
        *,
        threshold: Annotated[pa.Scalar[Any] | None, Setting()] = None,
    ) -> BindResponse:
        """Return input schema unchanged. Threshold declared for required_settings."""
        assert params.bind_call.input_schema is not None
        return BindResponse(output_schema=params.bind_call.input_schema)

    @classmethod
    def process(
        cls,
        params: ProcessParams[SingleTableArguments],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        """Filter rows where value >= threshold."""
        raw_threshold = params.settings["threshold"]
        # Cast to column type for compatibility (C++ extension may send as string)
        col = batch.column("value")
        threshold = pa.scalar(int(raw_threshold.as_py()), type=col.type)
        mask = pc.greater_equal(col, threshold)
        out.emit(batch.filter(mask))


@dataclass(slots=True, frozen=True)
class RepeatsInputsFunctionArguments:
    """Arguments for RepeatInputsFunction."""

    repeat_count: Annotated[int, Arg(0, doc="Number of times to repeat each input batch")]
    data: Annotated[TableInput, Arg(1, doc="Input table to repeat")]


class RepeatInputsFunction(TableInOutGenerator[RepeatsInputsFunctionArguments]):
    """Explosion function that duplicates each input batch N times.

    USE CASE
    --------
    Data augmentation, testing with larger datasets, or any scenario where
    you need multiple copies of each input record.

    Arguments:
    ---------
    repeat_count: Annotated[int, Arg(0)] (required)
        Number of times to repeat each input batch.

    BEHAVIOR
    --------
    - output_schema: Returns input schema unchanged
    - process(): For each input, concatenates it N times into one output

    SCHEMA TRANSFORMATION
    ---------------------
    Input:  any schema
    Output: same schema (passthrough)

    Example:
    -------
    With repeat_count=3:
    Input:  [{"a": 1}]
    Output: [{"a": 1}, {"a": 1}, {"a": 1}]

    """

    class Meta:
        """Metadata for RepeatInputsFunction."""

        name = "repeat_inputs"
        description = "Duplicates each input batch N times"
        categories = ["transform", "augmentation"]
        examples = [
            FunctionExample(
                sql="SELECT * FROM repeat_inputs(3, (SELECT * FROM input_table))",
                description="Repeat each row 3 times",
            )
        ]

    @classmethod
    def on_bind(cls, params: BindParams[RepeatsInputsFunctionArguments]) -> BindResponse:
        """Validate repeat count argument."""
        if params.args.repeat_count < 1:
            raise ValueError("Repeat count must be at least 1")
        if params.bind_call.input_schema is None:
            raise ValueError("input_schema is required but was None")
        return BindResponse(output_schema=params.bind_call.input_schema)

    @classmethod
    def process(
        cls,
        params: ProcessParams[RepeatsInputsFunctionArguments],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        """Emit input batch concatenated repeat_count times."""
        combined = pa.Table.from_batches([batch] * params.args.repeat_count).combine_chunks()
        out.emit(combined.to_batches()[0])


@dataclass(slots=True, frozen=True, kw_only=True)
class SumAllColumnsFunctionArguments:
    """Arguments for SumAllColumnsFunction."""

    data: Annotated[TableInput, Arg(0, doc="Input table")]
    logging: Annotated[bool, Arg("logging", doc="Whether to log during processing", default=False)] = False


@dataclass(kw_only=True)
class SumAllColumnsState(ArrowSerializableDataclass):
    """Mutable state for SumAllColumnsFunction - tracks running sums."""

    partial_sums: Annotated[pa.RecordBatch, ArrowType(pa.binary())]


class SumAllColumnsFunction(TableBufferingFunction[SumAllColumnsFunctionArguments, _LogDrainState]):
    """Aggregation function that computes column-wise sums across all batches.

    USE CASE
    --------
    Computing totals, aggregating metrics, or any full-stream reduction
    that produces a single summary row.

    BEHAVIOR
    --------
    - process(): Accumulates sums and emits empty results
    - finalize(): Returns single row with final sums

    SCHEMA TRANSFORMATION
    ---------------------
    Input:  any schema with numeric columns
    Output: only numeric columns, promoted to int64/float64

    For each input column:
    - Integer types -> int64
    - Floating types -> float64
    - Non-numeric types -> excluded from output

    KEY PATTERN: ACCUMULATE IN process(), EMIT IN finalize()
    --------------------------------------------------------
    In process(), accumulate state but emit empty results.
    In finalize(), return the final aggregated result.

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

    class Meta:
        name = "sum_all_columns"
        description = "Computes column-wise sums across all batches"
        categories = ["aggregation", "numeric"]
        examples = [
            FunctionExample(
                sql="SELECT * FROM sum_all_columns((SELECT * FROM input_table))",
                description="Sum all numeric columns",
            )
        ]

    @classmethod
    def cardinality(cls, params: BindParams[SumAllColumnsFunctionArguments]) -> TableCardinality:
        """Return cardinality estimate of exactly 1 row."""
        return TableCardinality(estimate=1, max=1)

    @classmethod
    def on_bind(cls, params: BindParams[SumAllColumnsFunctionArguments]) -> BindResponse:
        """Produce the output schema with only numeric columns.

        Numeric here means integer, floating-point, or fixed-precision
        decimal. DECIMAL inputs are promoted to float64 in the output
        (matching the float path) — DuckDB users routinely expect a sum
        of DECIMAL values to be summable, so silently dropping them would
        be surprising. Non-numeric inputs (strings, lists, timestamps,
        booleans) are filtered out; if NO numeric columns remain we raise
        ValueError at bind time rather than producing an empty output
        schema (which would crash downstream with an internal assertion).
        """
        assert params.bind_call.input_schema is not None
        output_fields: dict[str, pa.DataType] = {}
        for field in params.bind_call.input_schema:
            out_type: pa.DataType
            if pa.types.is_integer(field.type):
                out_type = pa.int64()
            elif pa.types.is_floating(field.type):
                out_type = pa.float64()
            elif pa.types.is_decimal(field.type):
                # Promote DECIMAL to float64 for the summed output.
                # (A more precise implementation would widen the decimal
                # type to absorb sum overflow, but for a test fixture
                # this is sufficient.)
                out_type = pa.float64()
            else:
                continue
            output_fields[field.name] = out_type

        if not output_fields:
            input_summary = ", ".join(
                f"{f.name}: {f.type}" for f in params.bind_call.input_schema
            )
            raise ValueError(
                "sum_all_columns requires at least one numeric (integer, "
                "floating-point, or decimal) input column, got [" + input_summary + "]"
            )

        return BindResponse(output_schema=schema(output_fields))

    @staticmethod
    def _scalars_to_single_row_batch(values: dict[str, pa.Scalar]) -> pa.RecordBatch:  # type: ignore[type-arg]
        arrays = [pa.array([scalar], type=scalar.type) for scalar in values.values()]
        return pa.RecordBatch.from_arrays(arrays, names=list(values.keys()))

    @classmethod
    def process(
        cls,
        batch: pa.RecordBatch,
        params: TableBufferingParams[SumAllColumnsFunctionArguments],
    ) -> bytes:
        """Append this batch's partial sums to the per-execution log.

        Race-safe append (state_append is atomic). combine() reduces.
        """
        if params.args.logging:
            import logging as _lg
            _lg.getLogger("vgi.fixture.sum_all_columns").info(
                "Processing batch with %d rows", batch.num_rows,
            )

        # Compute partial sums for this batch only.
        sums: dict[str, pa.Scalar[Any]] = {}
        for name in params.output_schema.names:
            col_sum = pc.sum(batch.column(name))
            if col_sum.is_valid:
                sums[name] = col_sum
            else:
                sums[name] = pa.scalar(
                    0, type=params.output_schema.field(name).type,
                )
        partial = cls._scalars_to_single_row_batch(sums)
        params.storage.state_append(
            b"partial", b"",
            SumAllColumnsState(partial_sums=partial).serialize_to_bytes(),
        )
        return params.execution_id

    @classmethod
    def combine(
        cls,
        state_ids: list[bytes],
        params: TableBufferingParams[SumAllColumnsFunctionArguments],
    ) -> list[bytes]:
        """Reduce all per-batch partials into one merged batch.

        combine() runs once on the coordinator after every process()
        completes — no race here. Drains the append-only log, sums, and
        writes the merged row to b"buf"/b"" for finalize to drain.

        Empty-input guard: even with no state_ids, writes the zeros row
        so ``SELECT ... FROM sum_all_columns((SELECT 1 WHERE 1=0))``
        produces one row of the expected shape.
        """
        merged: dict[str, pa.Scalar[Any]] = {
            name: pa.scalar(0, type=field.type)
            for name, field in zip(
                params.output_schema.names, params.output_schema, strict=True,
            )
        }
        for _log_id, blob in params.storage.state_log_scan(b"partial", b""):
            partial = SumAllColumnsState.deserialize_from_bytes(blob).partial_sums
            for name in params.output_schema.names:
                merged[name] = pc.add(merged[name], partial.column(name)[0])
        merged_batch = cls._scalars_to_single_row_batch(merged)
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, merged_batch.schema) as w:
            w.write_batch(merged_batch)
        params.storage.state_append(b"buf", b"", sink.getvalue().to_pybytes())
        return [params.execution_id]

    @classmethod
    def initial_finalize_state(
        cls,
        finalize_state_id: bytes,
        params: TableBufferingParams[SumAllColumnsFunctionArguments],
    ) -> _LogDrainState:
        return _LogDrainState(ns=b"buf", after_id=-1)

    @classmethod
    def finalize(
        cls,
        params: TableBufferingParams[SumAllColumnsFunctionArguments],
        finalize_state_id: bytes,
        state: _LogDrainState,
        out: OutputCollector,
    ) -> None:
        rows = params.storage.state_log_scan(
            state.ns, b"", after_id=state.after_id, limit=1,
        )
        if not rows:
            out.finish()
            return
        log_id, value = rows[0]
        out.emit(pa.ipc.open_stream(value).read_next_batch())
        state.after_id = log_id


@dataclass(kw_only=True)
class ExceptionProcessState(ArrowSerializableDataclass):
    """Mutable state for ExceptionProcessFunction."""

    batch_count: int = 0


class ExceptionProcessFunction(SumAllColumnsFunction):
    """Buffered table function that raises an exception on the second batch."""

    class Meta(SumAllColumnsFunction.Meta):
        name = "exception_process"
        description = "Test function that raises exception during process"
        categories = ["test", "error"]

    @classmethod
    def process(
        cls,
        batch: pa.RecordBatch,
        params: TableBufferingParams[SumAllColumnsFunctionArguments],
    ) -> bytes:
        """Raise an exception on the second batch.

        Race-safe counter: append-only log under b"count"/b"" — count is
        the number of log entries seen so far. Concurrent process() calls
        on HTTP serialize through state_append's atomic id minting.
        """
        params.storage.state_append(b"count", b"", b"")
        count = len(params.storage.state_log_scan(b"count", b""))
        if count % 2 == 0:
            raise ValueError(f"Intentional exception on batch {count}")
        return params.execution_id


class ExceptionFinalizeFunction(SumAllColumnsFunction):
    """Buffered table function that raises an exception during finalize()."""

    class Meta(SumAllColumnsFunction.Meta):
        name = "exception_finalize"
        description = "Test function that raises exception during finalize"
        categories = ["test", "error"]

    @classmethod
    def finalize(
        cls,
        params: TableBufferingParams[SumAllColumnsFunctionArguments],
        finalize_state_id: bytes,
        state: _LogDrainState,
        out: OutputCollector,
    ) -> None:
        raise ValueError("Intentional exception during finalize()")


@dataclass(slots=True, kw_only=True)
class SumAllColumnsSimpleDistributedState(ArrowSerializableDataclass):
    """Partial sum state for distributed aggregation."""

    partial_sum: Annotated[pa.RecordBatch, ArrowType(pa.binary())]


class SumAllColumnsSimpleDistributed(TableInOutFunction[SingleTableArguments, SumAllColumnsSimpleDistributedState]):
    """Distributed aggregation using the simple callback API.

    This function demonstrates TableInOutFunction with distributed
    state management.

    It's equivalent to SumAllColumnsFunctionDistributed but uses
    the simpler callback API.

    Example:
    -------
    Input batches (split across workers):
      Worker 1: [{a: 1, b: 1.0}, {a: 2, b: 2.0}]
      Worker 2: [{a: 3, b: 3.0}]

    Each worker computes partial sums:
      Worker 1 state: {a: 3, b: 3.0}
      Worker 2 state: {a: 3, b: 3.0}

    Primary worker merges states in finish():
      Combined: {a: 6, b: 6.0}

    Output (single row):
      [{a: 6, b: 6.0}]

    """

    class Meta:
        """Metadata for SumAllColumnsSimpleDistributed."""

        name = "sum_all_columns_simple_distributed"
        description = "Distributed sum using simple callback API"
        categories = ["aggregation", "numeric", "distributed"]
        examples = [
            FunctionExample(
                sql=("SELECT * FROM sum_all_columns_simple_distributed((SELECT * FROM input_table))"),
                description="Sum columns using distributed workers with callback API",
            )
        ]

    @classmethod
    def cardinality(cls, params: BindParams[SingleTableArguments]) -> TableCardinality:
        """Return cardinality estimate of exactly 1 row."""
        return TableCardinality(estimate=1, max=1)

    @classmethod
    def on_bind(cls, params: BindParams[SingleTableArguments]) -> BindResponse:
        """Produce the output schema with only numeric columns."""
        assert params.bind_call.input_schema is not None
        output_fields: dict[str, pa.DataType] = {}
        for field in params.bind_call.input_schema:
            out_type: pa.DataType
            if pa.types.is_integer(field.type):
                out_type = pa.int64()
            elif pa.types.is_floating(field.type):
                out_type = pa.float64()
            else:
                continue
            output_fields[field.name] = out_type

        return BindResponse(output_schema=schema(output_fields))

    @classmethod
    def initial_state(cls, params: ProcessParams[SingleTableArguments]) -> SumAllColumnsSimpleDistributedState | None:
        """Create the initial state."""
        return SumAllColumnsSimpleDistributedState(
            partial_sum=pa.RecordBatch.from_pylist(
                [{name: 0 for name in params.output_schema.names}], schema=params.output_schema
            )
        )

    @classmethod
    def transform(
        cls,
        batch: pa.RecordBatch,
        params: ProcessParams[SingleTableArguments],
        state: SumAllColumnsSimpleDistributedState | None,
    ) -> pa.RecordBatch:
        """Accumulate column sums. Emit nothing during processing."""
        if state is None:
            raise ValueError("State must not be None in transform()")
        # Add this batch's values to running sums
        sums: dict[str, pa.Scalar[Any]] = {}
        for name in params.output_schema.names:
            col_sum = pc.sum(batch.column(name))
            if col_sum.is_valid:
                sums[name] = pc.add(state.partial_sum.column(name)[0], col_sum)
            else:
                sums[name] = state.partial_sum.column(name)[0]

        state.partial_sum = pa.RecordBatch.from_pylist(
            [{name: val for name, val in sums.items()}],
            schema=params.output_schema,
        )

        return empty_batch(params.output_schema)

    @classmethod
    def finish(
        cls,
        params: ProcessParams[SingleTableArguments],
        states: list[SumAllColumnsSimpleDistributedState],
    ) -> list[pa.RecordBatch]:
        """Emit single row containing the column sums."""
        table = pa.Table.from_batches([state.partial_sum for state in states])

        sums: dict[str, pa.Scalar[Any]] = {}
        for field in params.output_schema:
            sums[field.name] = pa.scalar(0, type=field.type)

        for field in params.output_schema:
            sums[field.name] = pc.sum(table.column(field.name))

        return [pa.RecordBatch.from_pylist([{name: val for name, val in sums.items()}], schema=params.output_schema)]


# ============================================================================
# Failure-injection fixtures
# ============================================================================
# These are table_buffering functions designed to exercise crash/error paths in
# the C++ Sink+Source operator. Each one fails in a specific phase so we can
# test that the operator: throws cleanly, drains the worker pool, doesn't leak
# in-flight workers, and recovers on the next query.


class CrashOnProcessFunction(BufferInputFunction):
    """SIGKILLs its own worker process on the first process() call."""

    class Meta(BufferInputFunction.Meta):
        name = "crash_on_process"
        description = "Worker SIGKILLs itself during process (test)"
        categories = ["test", "crash"]

    @classmethod
    def process(
        cls,
        batch: pa.RecordBatch,
        params: TableBufferingParams[SingleTableArguments],
    ) -> bytes:
        os.kill(os.getpid(), signal.SIGKILL)
        return params.execution_id  # unreachable


class CrashOnCombineFunction(BufferInputFunction):
    """Buffers input normally; raises during combine()."""

    class Meta(BufferInputFunction.Meta):
        name = "crash_on_combine"
        description = "Worker raises during combine (test)"
        categories = ["test", "crash"]

    @classmethod
    def combine(
        cls, state_ids: list[bytes], params: TableBufferingParams[SingleTableArguments]
    ) -> list[bytes]:
        raise RuntimeError("Intentional exception during combine()")


class CrashOnFinalizeFunction(BufferInputFunction):
    """Combine returns normally, finalize raises on first tick."""

    class Meta(BufferInputFunction.Meta):
        name = "crash_on_finalize"
        description = "Worker raises during finalize (test)"
        categories = ["test", "crash"]

    @classmethod
    def finalize(
        cls,
        params: TableBufferingParams[SingleTableArguments],
        finalize_state_id: bytes,
        state: _LogDrainState,
        out: OutputCollector,
    ) -> None:
        raise ValueError("Intentional exception during finalize()")


class HangOnProcessFunction(BufferInputFunction):
    """Sleeps for an hour in process(); used by the manual cancellation smoke."""

    class Meta(BufferInputFunction.Meta):
        name = "hang_on_process"
        description = "Worker sleeps in process (manual cancel test)"
        categories = ["test", "hang"]

    @classmethod
    def process(
        cls,
        batch: pa.RecordBatch,
        params: TableBufferingParams[SingleTableArguments],
    ) -> bytes:
        time.sleep(3600)
        return params.execution_id  # unreachable


@dataclass(kw_only=True)
class LargeStateState(ArrowSerializableDataclass):
    """Holds a single large bytes buffer.

    The C++ side ships this through combine/finalize via the RPC's outer
    envelope, exercising IPC chunking on the response path.
    """

    payload: bytes = b""


class LargeStateFunction(TableBufferingFunction[SingleTableArguments, _LogDrainState]):
    """Accumulates a large payload per state_id and emits it during finalize.

    Each process() call appends 1 MB to the per-worker payload via RMW on
    BoundStorage. combine() materializes one output row per worker into a
    state_log; finalize() drains it cursor-style.
    """

    class Meta:
        name = "large_state"
        description = "Buffers ~1 MB per input batch into state (IPC test)"
        categories = ["test", "memory"]

    @classmethod
    def on_bind(cls, params: BindParams[SingleTableArguments]) -> BindResponse:
        assert params.bind_call.input_schema is not None
        return BindResponse(output_schema=params.bind_call.input_schema)

    @classmethod
    def process(
        cls,
        batch: pa.RecordBatch,
        params: TableBufferingParams[SingleTableArguments],
    ) -> bytes:
        """Append 1 MB per call to the per-execution log.

        Race-safe append; combine() sums the total payload size by
        scanning the log.
        """
        params.storage.state_append(
            b"large", b"", b"\x00" * (1024 * 1024),
        )
        return params.execution_id

    @classmethod
    def combine(
        cls,
        state_ids: list[bytes],
        params: TableBufferingParams[SingleTableArguments],
    ) -> list[bytes]:
        """Materialize one output row carrying the total payload size."""
        total = sum(
            len(blob) for _log_id, blob in params.storage.state_log_scan(b"large", b"")
        )
        out_batch = pa.RecordBatch.from_pydict(
            {name: [total] for name in params.output_schema.names},
            schema=params.output_schema,
        )
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, out_batch.schema) as w:
            w.write_batch(out_batch)
        params.storage.state_append(b"buf", b"", sink.getvalue().to_pybytes())
        return [params.execution_id]

    @classmethod
    def initial_finalize_state(
        cls,
        finalize_state_id: bytes,
        params: TableBufferingParams[SingleTableArguments],
    ) -> _LogDrainState:
        return _LogDrainState(ns=b"buf", after_id=-1)

    @classmethod
    def finalize(
        cls,
        params: TableBufferingParams[SingleTableArguments],
        finalize_state_id: bytes,
        state: _LogDrainState,
        out: OutputCollector,
    ) -> None:
        rows = params.storage.state_log_scan(
            state.ns, b"", after_id=state.after_id, limit=1,
        )
        if not rows:
            out.finish()
            return
        log_id, value = rows[0]
        out.emit(pa.ipc.open_stream(value).read_next_batch())
        state.after_id = log_id


# ============================================================================
# Ordering knobs (sink_order_dependent, requires_input_batch_index)
# ============================================================================


class OrderedBufferInputFunction(BufferInputFunction):
    """Buffered table function with single-threaded ingest.

    Uses ``Meta.sink_order_dependent=True`` to force ``ParallelSink=false`` on
    the C++ operator. Every ``process()`` call arrives on the same worker in
    source order — verifying this works correctly is the integration test's
    job (assert distinct ``conn=`` count is exactly 1).

    Output is identical to ``BufferInputFunction``: passthrough of all
    buffered rows. Because there's only one Sink thread there's only one
    state_id; combine returns ``[0]`` and finalize yields the buffer.
    """

    class Meta(BufferInputFunction.Meta):
        name = "ordered_buffer_input"
        description = "buffer_input variant with sink_order_dependent=True"
        categories = ["test", "ordering"]
        sink_order_dependent = True


def _pack_indexed_batch(batch_index: int, batch_bytes: bytes) -> bytes:
    """Pack (batch_index, batch_bytes) into a single appendable blob.

    Layout: 8 bytes little-endian signed batch_index || raw IPC stream bytes.
    Used by BatchIndexBufferInputFunction to thread per-batch ordering keys
    through the append-only state_log without an extra ArrowSerializableDataclass
    round-trip.
    """
    return batch_index.to_bytes(8, "little", signed=True) + batch_bytes


def _unpack_indexed_batch(blob: bytes) -> tuple[int, bytes]:
    """Inverse of _pack_indexed_batch."""
    return int.from_bytes(blob[:8], "little", signed=True), blob[8:]


class BatchIndexBufferInputFunction(TableBufferingFunction[SingleTableArguments, _LogDrainState]):
    """Buffered table function that demands ``batch_index`` per ``process()``.

    Uses ``Meta.requires_input_batch_index=True`` so the C++ operator
    declares ``RequiredPartitionInfo()=BatchIndex()`` and threads DuckDB's
    per-chunk batch_index into every ``process()`` call. process() packs
    (batch_index, ipc_bytes) into the per-worker state_log; combine() sorts
    globally by batch_index and re-writes a sorted log; finalize() drains
    cursor-style.
    """

    class Meta:
        name = "batch_index_buffer_input"
        description = "buffer_input variant using batch_index to reconstruct order"
        categories = ["test", "ordering"]
        requires_input_batch_index = True

    @classmethod
    def on_bind(cls, params: BindParams[SingleTableArguments]) -> BindResponse:
        assert params.bind_call.input_schema is not None
        return BindResponse(output_schema=params.bind_call.input_schema)

    @classmethod
    def process(
        cls,
        batch: pa.RecordBatch,
        params: TableBufferingParams[SingleTableArguments],
    ) -> bytes:
        if params.batch_index is None:
            raise RuntimeError(
                "batch_index_buffer_input.process() received batch_index=None "
                "— Meta.requires_input_batch_index plumbing is broken"
            )
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)
        # Append-only — race-safe under concurrent Sink threads. combine()
        # collects and sorts globally by batch_index.
        params.storage.state_append(
            b"unsorted", b"",
            _pack_indexed_batch(params.batch_index, sink.getvalue().to_pybytes()),
        )
        return params.execution_id

    @classmethod
    def combine(
        cls,
        state_ids: list[bytes],
        params: TableBufferingParams[SingleTableArguments],
    ) -> list[bytes]:
        """Sort globally by batch_index and re-emit as a single ordered log."""
        all_pairs: list[tuple[int, bytes]] = [
            _unpack_indexed_batch(v)
            for _, v in params.storage.state_log_scan(b"unsorted", b"")
        ]
        all_pairs.sort(key=lambda p: p[0])
        for _idx, batch_bytes in all_pairs:
            params.storage.state_append(b"buf", b"", batch_bytes)
        return [params.execution_id]

    @classmethod
    def initial_finalize_state(
        cls,
        finalize_state_id: bytes,
        params: TableBufferingParams[SingleTableArguments],
    ) -> _LogDrainState:
        return _LogDrainState(ns=b"buf", after_id=-1)

    @classmethod
    def finalize(
        cls,
        params: TableBufferingParams[SingleTableArguments],
        finalize_state_id: bytes,
        state: _LogDrainState,
        out: OutputCollector,
    ) -> None:
        rows = params.storage.state_log_scan(
            state.ns, b"", after_id=state.after_id, limit=1,
        )
        if not rows:
            out.finish()
            return
        log_id, value = rows[0]
        out.emit(pa.ipc.open_stream(value).read_next_batch())
        state.after_id = log_id
