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
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Annotated, Any

import pyarrow as pa
import pyarrow.compute as pc
from vgi_rpc import ArrowSerializableDataclass, ArrowType
from vgi_rpc.log import Level
from vgi_rpc.rpc import OutputCollector
from vgi_rpc.utils import empty_batch

from vgi.arguments import Arg, Setting, TableInput
from vgi.function_storage import BoundStorage
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.schema_utils import schema
from vgi.table_function import BindParams, ProcessParams, TableCardinality
from vgi.table_in_out_function import (
    TableInOutFunction,
    TableInOutGenerator,
)

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


class BufferInputFunction(TableInOutGenerator[SingleTableArguments, None]):
    """Buffering function — collects all input, emits during finalize.

    Uses the buffered table function path (``Meta.buffered_table = True``):
    each parallel Sink thread accumulates into its own ``state_id``,
    ``combine`` returns those state_ids as-is (pass-through partitioning),
    and ``finalize`` yields the buffered batches for one state_id per RPC.
    Under UNION ALL the inputs are distributed across Sink threads and the
    combined output is the union of every buffer.

    State storage: ``state_class = None`` skips the framework's per-call
    state_get/state_put round-trip. process() appends each input batch to
    ``state_log`` keyed by ``(b"buf", state_id)``; finalize() drains via
    ``state_log_scan`` in append order. Storage cost is O(N batches) inserts
    instead of the O(N^2) RMW pattern that state_class would force.

    Schema:
        Input:  any schema
        Output: same schema (passthrough)
    """

    state_class = None

    class Meta:
        """Metadata for BufferInputFunction."""

        name = "buffer_input"
        description = "Collects all input batches and emits during finalization"
        categories = ["utility", "buffer"]
        buffered_table = True
        examples = [
            FunctionExample(
                sql="SELECT * FROM buffer_input((SELECT * FROM input_table))",
                description="Buffer all input and emit on finalize",
            )
        ]

    @classmethod
    def initial_state(cls, params: ProcessParams[SingleTableArguments]) -> None:
        return None

    @classmethod
    def process(
        cls,
        params: ProcessParams[SingleTableArguments],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        """Serialize the batch and append to this state_id's log.

        Under the buffered_table path ``out`` is a NoOpCollector that
        silently accepts the trailing zero-row emit but would raise on any
        non-empty emit — process() is sink-only.
        """
        assert params.state_id is not None, (
            "buffered_table_process did not populate ProcessParams.state_id"
        )
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)
        params.storage.state_append(
            b"buf", BoundStorage.pack_int_key(params.state_id), sink.getvalue().to_pybytes()
        )
        out.emit(empty_batch(params.output_schema))

    @classmethod
    def combine(
        cls, state_ids: list[int], params: ProcessParams[SingleTableArguments]
    ) -> list[int]:
        """Pass-through partitioning: each state_id is its own finalize key.

        No cross-state merging is needed — each thread's log is already a
        complete, ordered slice of the rows that landed on that thread.
        Returning ``state_ids`` lets DuckDB's parallel Source phase drain all
        buffers concurrently.
        """
        return state_ids

    @classmethod
    def finalize(  # type: ignore[override]
        cls, finalize_state_id: int, params: ProcessParams[SingleTableArguments]
    ) -> Iterator[pa.RecordBatch]:
        """Yield the buffered batches for one finalize_state_id, one per RPC.

        Buffered-table finalize signature is ``(finalize_state_id, params)``,
        which is wider than the streaming-shape ``(params)`` declared on
        the TableInOutGenerator base. The ``# type: ignore[override]``
        documents that this is a deliberate signature widening — the
        worker-side dispatcher (vgi/worker.py:buffered_table_finalize)
        only routes Meta.buffered_table=True subclasses through this
        path, so the duck-typed call lands correctly at runtime.
        """
        for batch_bytes in params.storage.state_log_scan(
            b"buf", BoundStorage.pack_int_key(finalize_state_id)
        ):
            yield pa.ipc.open_stream(batch_bytes).read_next_batch()


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


class SumAllColumnsFunction(TableInOutGenerator[SumAllColumnsFunctionArguments, SumAllColumnsState]):
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
        """Metadata for SumAllColumnsFunction."""

        name = "sum_all_columns"
        description = "Computes column-wise sums across all batches"
        categories = ["aggregation", "numeric"]
        # Uses the buffered table function path: each parallel Sink thread
        # accumulates per-thread sums, combine reduces them to a single
        # partition, finalize yields one row with the total.
        buffered_table = True
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
    def initial_state(cls, params: ProcessParams[SumAllColumnsFunctionArguments]) -> SumAllColumnsState:
        """Initialize running sums to zero."""
        return SumAllColumnsState(
            partial_sums=pa.RecordBatch.from_pylist(
                [{name: 0 for name in params.output_schema.names}], schema=params.output_schema
            )
        )

    # The state class must be a dataclass-style class for the framework's
    # auto-detection from TableInOutGenerator[..., TState], but the
    # generic-param plumbing on TableInOutGenerator doesn't auto-populate
    # state_class (only TableInOutFunction does). Set it explicitly.
    state_class = SumAllColumnsState

    @classmethod
    def process(
        cls,
        params: ProcessParams[SumAllColumnsFunctionArguments],
        state: SumAllColumnsState,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        """Accumulate this batch into the per-state_id partial_sums."""
        if params.args.logging:
            out.client_log(
                Level.INFO,
                f"Processing batch with {batch.num_rows} rows",
            )

        sums: dict[str, pa.Scalar[Any]] = {}
        for name in params.output_schema.names:
            col_sum = pc.sum(batch.column(name))
            prev = state.partial_sums.column(name)[0]
            if col_sum.is_valid:
                sums[name] = pc.add(prev, col_sum)
            else:
                sums[name] = prev

        state.partial_sums = cls._scalars_to_single_row_batch(sums)
        # No out.emit needed — the buffered NoOpCollector swallows zero-row
        # emits but emitting nothing is the cleanest signal that process is
        # sink-only here.

    @classmethod
    def combine(
        cls, state_ids: list[int], params: ProcessParams[SumAllColumnsFunctionArguments]
    ) -> list[int]:
        """Merge every per-thread state into a single coordinator state.

        Reads each per-state_id ``partial_sums`` from shared BoundStorage,
        column-sums them across states, writes the merged result back to
        slot 0, and returns ``[0]`` as the only finalize partition. The
        finalize phase will yield one row total.
        """
        if not state_ids:
            return []

        keys = [BoundStorage.pack_int_key(sid) for sid in state_ids]
        stored = params.storage.state_get_many(b"buf", keys)
        merged: dict[str, pa.Scalar[Any]] = {name: pa.scalar(0, type=field.type)
                                             for name, field in zip(params.output_schema.names,
                                                                     params.output_schema,
                                                                     strict=True)}
        for entry in stored:
            if entry is None:
                continue
            partial = SumAllColumnsState.deserialize_from_bytes(entry).partial_sums
            for name in params.output_schema.names:
                merged[name] = pc.add(merged[name], partial.column(name)[0])
        merged_state = SumAllColumnsState(partial_sums=cls._scalars_to_single_row_batch(merged))
        params.storage.state_put(b"buf", BoundStorage.pack_int_key(0), merged_state.serialize_to_bytes())
        return [0]

    @classmethod
    def finalize(  # type: ignore[override]
        cls, finalize_state_id: int, params: ProcessParams[SumAllColumnsFunctionArguments]
    ) -> Iterator[pa.RecordBatch]:
        """Yield one row carrying the merged column sums.

        Buffered-table finalize signature widening — see the
        ``# type: ignore[override]`` comment on
        ``BufferInputFunction.finalize`` for rationale.
        """
        stored = params.storage.state_get(b"buf", BoundStorage.pack_int_key(finalize_state_id))
        if stored is None:
            # No data at all — emit zeros so SQL like
            # `SELECT * FROM sum_all_columns((SELECT 1 WHERE 1=0))` still
            # produces a row with the expected shape.
            sums = {name: pa.scalar(0, type=field.type)
                    for name, field in zip(params.output_schema.names, params.output_schema, strict=True)}
            yield pa.RecordBatch.from_pydict({name: [val] for name, val in sums.items()},
                                              schema=params.output_schema)
            return
        partial = SumAllColumnsState.deserialize_from_bytes(stored).partial_sums
        sums = {name: partial.column(name)[0] for name in params.output_schema.names}
        yield pa.RecordBatch.from_pydict({name: [val] for name, val in sums.items()},
                                          schema=params.output_schema)


@dataclass(kw_only=True)
class ExceptionProcessState(ArrowSerializableDataclass):
    """Mutable state for ExceptionProcessFunction."""

    batch_count: int = 0


class ExceptionProcessFunction(
    SumAllColumnsFunction, TableInOutGenerator[SumAllColumnsFunctionArguments, ExceptionProcessState]
):
    """Buffered table function that raises an exception on the second batch."""

    # Narrowing state_class from SumAllColumnsState to ExceptionProcessState
    # is intentional; both are ArrowSerializableDataclass subclasses, but
    # the base class declares state_class with the concrete type for
    # type-narrowing in generic contexts.
    state_class = ExceptionProcessState  # type: ignore[assignment]

    class Meta(SumAllColumnsFunction.Meta):
        """Metadata for ExceptionProcessFunction."""

        name = "exception_process"
        description = "Test function that raises exception during process"
        categories = ["test", "error"]
        # buffered_table inherited from SumAllColumnsFunction.Meta

    @classmethod
    def initial_state(cls, params: ProcessParams[SumAllColumnsFunctionArguments]) -> ExceptionProcessState:  # type: ignore[override]
        """Create initial state."""
        return ExceptionProcessState()

    @classmethod
    def process(  # type: ignore[override]
        cls,
        params: ProcessParams[SumAllColumnsFunctionArguments],
        state: ExceptionProcessState,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        """Raise an exception on the second batch.

        ``# type: ignore[override]`` — narrows ``state`` from the parent's
        ``SumAllColumnsState`` to ``ExceptionProcessState`` (the override
        on ``state_class`` above pairs with this type).
        """
        state.batch_count += 1
        if state.batch_count % 2 == 0:
            raise ValueError(f"Intentional exception on batch {state.batch_count}")
        # Sink-only — no emit needed.

    @classmethod
    def combine(
        cls, state_ids: list[int], params: ProcessParams[SumAllColumnsFunctionArguments]
    ) -> list[int]:
        # Pass-through partitioning: no aggregation across states needed.
        return state_ids

    @classmethod
    def finalize(  # type: ignore[override]
        cls, finalize_state_id: int, params: ProcessParams[SumAllColumnsFunctionArguments]
    ) -> Iterator[pa.RecordBatch]:
        # If we ever reach finalize, emit a zero-sum row (the only behavior
        # the existing test asserts is the *exception* path from process).
        sums = {name: pa.scalar(0, type=field.type)
                for name, field in zip(params.output_schema.names, params.output_schema, strict=True)}
        yield pa.RecordBatch.from_pydict({name: [val] for name, val in sums.items()},
                                          schema=params.output_schema)


class ExceptionFinalizeFunction(SumAllColumnsFunction):
    """Buffered table function that raises an exception during finalize()."""

    class Meta(SumAllColumnsFunction.Meta):
        """Metadata for ExceptionFinalizeFunction."""

        name = "exception_finalize"
        description = "Test function that raises exception during finalize"
        categories = ["test", "error"]
        # buffered_table inherited from SumAllColumnsFunction.Meta

    @classmethod
    def finalize(  # type: ignore[override]
        cls, finalize_state_id: int, params: ProcessParams[SumAllColumnsFunctionArguments]
    ) -> Iterator[pa.RecordBatch]:
        """Raise an intentional exception when the C++ side pulls a batch."""
        raise ValueError("Intentional exception during finalize()")
        yield  # make this a generator function


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
# These are buffered_table functions designed to exercise crash/error paths in
# the C++ Sink+Source operator. Each one fails in a specific phase so we can
# test that the operator: throws cleanly, drains the worker pool, doesn't leak
# in-flight workers, and recovers on the next query.


class CrashOnProcessFunction(BufferInputFunction):
    """SIGKILLs its own worker process on the first process() call.

    Tests the C++ side's handling of an abrupt worker death mid-RPC:
    subprocess writes the request, worker dies before responding, ReadUnary
    sees EOF / EPIPE / SIGCHLD depending on timing. The exception should
    propagate cleanly and gstate teardown should cancel-dispatch peer workers.
    """

    class Meta(BufferInputFunction.Meta):
        name = "crash_on_process"
        description = "Worker SIGKILLs itself during process (test)"
        categories = ["test", "crash"]

    @classmethod
    def process(
        cls,
        params: ProcessParams[SingleTableArguments],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        # SIGKILL with no handler; the process dies immediately. The C++
        # side has a pending unary RPC awaiting the response.
        os.kill(os.getpid(), signal.SIGKILL)


class CrashOnCombineFunction(BufferInputFunction):
    """Buffers input normally; raises during combine().

    Tests that a combine RPC error propagates as IOException, that the
    combine_worker is dropped (not pushed back into gstate.workers), and
    that the rest of the worker pool drains.
    """

    class Meta(BufferInputFunction.Meta):
        name = "crash_on_combine"
        description = "Worker raises during combine (test)"
        categories = ["test", "crash"]

    @classmethod
    def combine(
        cls, state_ids: list[int], params: ProcessParams[SingleTableArguments]
    ) -> list[int]:
        raise RuntimeError("Intentional exception during combine()")


class CrashOnFinalizeFunction(BufferInputFunction):
    """Buffers input, combine returns normally, finalize raises on first yield.

    Tests source-phase error propagation: GetData calls
    RpcBufferedTableFinalize, the worker raises, the IOException unwinds the
    source thread, and remaining workers are still released cleanly.
    """

    class Meta(BufferInputFunction.Meta):
        name = "crash_on_finalize"
        description = "Worker raises during finalize (test)"
        categories = ["test", "crash"]

    @classmethod
    def finalize(  # type: ignore[override]
        cls, finalize_state_id: int, params: ProcessParams[SingleTableArguments]
    ) -> Iterator[pa.RecordBatch]:
        raise RuntimeError("Intentional exception during finalize()")
        yield  # make this a generator function


class HangOnProcessFunction(BufferInputFunction):
    """Sleeps for an hour in process(); used by the manual cancellation smoke.

    sqllogictest can't simulate Ctrl-C cleanly, so this fixture is driven by
    scripts/smoke_buffered_cancel.sh — it starts the query, waits a moment,
    then SIGINTs the duckdb process and asserts the query was cancelled.
    """

    class Meta(BufferInputFunction.Meta):
        name = "hang_on_process"
        description = "Worker sleeps in process (manual cancel test)"
        categories = ["test", "hang"]

    @classmethod
    def process(
        cls,
        params: ProcessParams[SingleTableArguments],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        time.sleep(3600)


@dataclass(kw_only=True)
class LargeStateState(ArrowSerializableDataclass):
    """Holds a single large bytes buffer.

    The C++ side ships this through combine/finalize via the RPC's outer
    envelope, exercising IPC chunking on the response path.
    """

    payload: bytes = b""


class LargeStateFunction(TableInOutGenerator[SingleTableArguments, LargeStateState]):
    """Accumulates a large payload per state_id and emits it during finalize.

    Each process() call appends N KB to the per-state_id payload. The total
    bytes per finalize_state_id grows linearly with input row count. The
    test passes ~100 MB through combine to exercise the IPC chunking that
    happens transparently in vgi_rpc for large messages.
    """

    state_class = LargeStateState

    class Meta:
        name = "large_state"
        description = "Buffers ~1 MB per input batch into state (IPC test)"
        categories = ["test", "memory"]
        buffered_table = True

    @classmethod
    def initial_state(cls, params: ProcessParams[SingleTableArguments]) -> LargeStateState:
        return LargeStateState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[SingleTableArguments],
        state: LargeStateState,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        # Append 1 MB of zero bytes per input batch. Combine ships the full
        # accumulated payload back via Arrow IPC; chunking is the worker
        # library's responsibility — we just want to land a multi-MB message
        # on the wire.
        state.payload += b"\x00" * (1024 * 1024)
        out.emit(empty_batch(params.output_schema))

    @classmethod
    def combine(
        cls, state_ids: list[int], params: ProcessParams[SingleTableArguments]
    ) -> list[int]:
        return state_ids

    @classmethod
    def finalize(  # type: ignore[override]
        cls, finalize_state_id: int, params: ProcessParams[SingleTableArguments]
    ) -> Iterator[pa.RecordBatch]:
        """Buffered-table finalize signature widening — see BufferInputFunction.finalize."""
        stored = params.storage.state_get(b"buf", BoundStorage.pack_int_key(finalize_state_id))
        if stored is None:
            return
        state = LargeStateState.deserialize_from_bytes(stored)
        # Emit one row with the payload length so the test can assert
        # we round-tripped the right number of bytes through combine.
        yield pa.RecordBatch.from_pydict(
            {name: [len(state.payload)] for name in params.output_schema.names},
            schema=params.output_schema,
        )


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


class BatchIndexBufferInputFunction(TableInOutGenerator[SingleTableArguments, None]):
    """Buffered table function that demands ``batch_index`` per ``process()``.

    Uses ``Meta.requires_input_batch_index=True`` so the C++ operator
    declares ``RequiredPartitionInfo()=BatchIndex()`` and threads DuckDB's
    per-chunk batch_index into every ``process()`` call. process() appends
    ``(batch_index, ipc_bytes)`` tuples (struct-packed) to the per-thread
    state_log; combine() collects all state_ids; finalize() drains every
    log, sorts by batch_index globally, and yields in source order.
    """

    state_class = None

    class Meta:
        # Declared standalone (not inheriting from BufferInputFunction.Meta)
        # because resolve_metadata's vars(meta_class) only sees directly-set
        # attributes — Python class attribute inheritance on Meta doesn't
        # carry through. Explicit beats implicit here.
        name = "batch_index_buffer_input"
        description = "buffer_input variant using batch_index to reconstruct order"
        categories = ["test", "ordering"]
        buffered_table = True
        requires_input_batch_index = True

    @classmethod
    def initial_state(cls, params: ProcessParams[SingleTableArguments]) -> None:
        return None

    @classmethod
    def process(
        cls,
        params: ProcessParams[SingleTableArguments],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        # batch_index must be present when Meta.requires_input_batch_index=True;
        # if it's None the C++ side failed to thread it through.
        if params.batch_index is None:
            raise RuntimeError(
                "batch_index_buffer_input.process() received batch_index=None "
                "— Meta.requires_input_batch_index plumbing is broken"
            )
        assert params.state_id is not None

        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)
        params.storage.state_append(
            b"buf",
            BoundStorage.pack_int_key(params.state_id),
            _pack_indexed_batch(params.batch_index, sink.getvalue().to_pybytes()),
        )
        out.emit(empty_batch(params.output_schema))

    @classmethod
    def combine(
        cls, state_ids: list[int], params: ProcessParams[SingleTableArguments]
    ) -> list[int]:
        """Pass state_ids through; finalize() does the global sort.

        We could instead drain every log here and re-emit a single sorted
        log under state_id 0, but that's an O(N) re-write of every batch.
        Pushing the sort into finalize() keeps Sink->Source latency low and
        only walks each batch once.
        """
        return state_ids

    @classmethod
    def finalize(  # type: ignore[override]
        cls, finalize_state_id: int, params: ProcessParams[SingleTableArguments]
    ) -> Iterator[pa.RecordBatch]:
        """Buffered-table finalize signature widening — see BufferInputFunction.finalize.

        NB. There is exactly one finalize() per state_id (combine returned
        state_ids unchanged), so we only see this thread's log here. To get
        a globally ordered output the caller should choose Meta.sink_order_dependent
        OR use a single Sink thread; otherwise per-thread output order is
        batch_index-ascending only within this slice.
        """
        log_bytes = params.storage.state_log_scan(
            b"buf", BoundStorage.pack_int_key(finalize_state_id)
        )
        pairs = [_unpack_indexed_batch(b) for b in log_bytes]
        pairs.sort(key=lambda p: p[0])
        for _idx, batch_bytes in pairs:
            yield pa.ipc.open_stream(batch_bytes).read_next_batch()
