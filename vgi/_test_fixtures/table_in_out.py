# Copyright 2025, 2026 Query Farm LLC - https://query.farm

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
import sys
import time
from dataclasses import dataclass
from typing import Annotated, Any

import pyarrow as pa
import pyarrow.compute as pc
from vgi_rpc import ArrowSerializableDataclass, ArrowType
from vgi_rpc.log import Level
from vgi_rpc.rpc import OutputCollector

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
    RowTransformFunction,
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
    "EchoWitnessFunction",
    "BufferInputFunction",
    "FilterBySettingFunction",
    "RepeatInputsFunction",
    "GeoEncodeFunction",
    "GeoEncode3Function",
    "RowSumFunction",
    "SubstreamPartialSumFunction",
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
    "OrderedSourceFunction",
    "BufferEmitWideFunction",
]


@dataclass(slots=True, frozen=True, kw_only=True)
class SingleTableArguments:
    """Arguments for a table in/out function that just takes a table."""

    data: Annotated[TableInput, Arg(0, doc="Input table")]


@dataclass(kw_only=True)
class SubstreamPartialSumState(ArrowSerializableDataclass):
    """Running sum for ONE substream's worker (never merged across substreams)."""

    total: int = 0


class SubstreamPartialSumFunction(TableInOutFunction[SingleTableArguments, SubstreamPartialSumState]):
    """Per-substream partial sum emitted at finalize — proves parallel streaming FINALIZE (A4).

    A streaming table-in-out *with* a finalize is still a per-substream operation
    under per-substream worker fan-out: ``transform()`` accumulates only THIS
    substream's rows (emitting nothing), and ``finish()`` emits ONE row = this
    substream's partial sum. DuckDB fans the input across N workers and unions
    their finalize outputs, so the caller re-aggregates with an outer
    ``SELECT sum(...)`` to get the global total — correct no matter how the rows
    were partitioned across substreams. Each substream's ``finish()`` reads only
    its OWN worker's accumulated state (keyed by the substream's execution_id;
    ``params.substream_id`` is the stable client-owned key available for workers
    that manage cross-backend state themselves). This is the per-substream
    finalize contract A4 enables — it is NOT a global cross-substream combine
    (that is a ``TableBufferingFunction``; see ``SumAllColumnsSimpleDistributed``).

    Invariant the tests assert (deterministic regardless of thread/substream
    count): ``SELECT sum(n) FROM substream_partial_sum((SELECT ... AS n))`` equals
    the sum of the input column, because the per-substream partials sum to the
    whole. If a substream's finalize were skipped, hit a stateless worker, or
    cross-contaminated another substream's state, this total would be wrong.
    """

    class Meta:
        name = "substream_partial_sum"
        description = "Per-substream partial sum emitted at finalize (parallel streaming finalize)"
        categories = ["aggregation", "numeric"]

    @classmethod
    def cardinality(cls, params: BindParams[SingleTableArguments]) -> TableCardinality:
        # One row per substream; unknown up-front, so leave the estimate open.
        return TableCardinality(estimate=1)

    @classmethod
    def on_bind(cls, params: BindParams[SingleTableArguments]) -> BindResponse:
        assert params.bind_call.input_schema is not None
        field = params.bind_call.input_schema.field(0)
        return BindResponse(output_schema=schema({field.name: pa.int64()}))

    @classmethod
    def initial_state(cls, params: ProcessParams[SingleTableArguments]) -> SubstreamPartialSumState:
        return SubstreamPartialSumState(total=0)

    @classmethod
    def transform(
        cls,
        batch: pa.RecordBatch,
        params: ProcessParams[SingleTableArguments],
        state: SubstreamPartialSumState | None,
    ) -> list[pa.RecordBatch]:
        if state is None:
            raise ValueError("State must not be None in transform()")
        col_sum = pc.sum(batch.column(0))
        if col_sum.is_valid:
            state.total += col_sum.as_py()
        return []  # accumulate only; emit nothing during processing

    @classmethod
    def finish(
        cls,
        params: ProcessParams[SingleTableArguments],
        states: list[SubstreamPartialSumState],
    ) -> list[pa.RecordBatch]:
        # `states` are THIS substream's accumulated states (one per worker pid that
        # handled this substream's batches); their sum is this substream's partial.
        total = sum(st.total for st in states)
        name = params.output_schema.names[0]
        return [pa.RecordBatch.from_pydict({name: [total]}, schema=params.output_schema)]


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


class EchoWitnessFunction(TableInOutGenerator[SingleTableArguments]):
    """Integer-output fixture that encodes the post-projection column count.

    Designed to verify that projection pushdown ACTUALLY narrows the
    schema reaching the worker (rather than just relying on DuckDB to
    narrow above the operator). Each emitted row has every column set
    to ``len(params.output_schema)`` — i.e., the worker's observed
    column count after framework projection narrowing.

    With pushdown working:
        ``SELECT a FROM echo_witness((SELECT 1 AS a, 2 AS b, 3 AS c))`` → 1

    Without pushdown (DuckDB requests all columns, narrows above):
        ``SELECT a FROM echo_witness((SELECT 1 AS a, 2 AS b, 3 AS c))`` → 3

    Output schema mirrors input (must be all integer columns for the
    encoding to work). Filter pushdown is intentionally off — this
    fixture only probes projection.
    """

    class Meta:
        name = "echo_witness"
        description = "Emits len(observed_output_schema) per column — projection probe"
        categories = ["test", "pushdown"]
        projection_pushdown = True

    @classmethod
    def on_bind(cls, params: BindParams[SingleTableArguments]) -> BindResponse:
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
        observed = len(params.output_schema)
        cols = {field.name: pa.array([observed] * batch.num_rows, type=field.type) for field in params.output_schema}
        out.emit(pa.RecordBatch.from_pydict(cols, schema=params.output_schema))


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
            state.ns,
            b"",
            after_id=state.after_id,
            limit=1,
        )
        if not rows:
            out.finish()
            return
        log_id, value = rows[0]
        out.emit(pa.ipc.open_stream(value).read_next_batch())
        state.after_id = log_id


class EchoBufferingFunction(TableBufferingFunction[SingleTableArguments, _LogDrainState]):
    """Buffered passthrough with projection + filter pushdown enabled.

    Same shape as :class:`BufferInputFunction` (process buffers input,
    finalize drains one batch per tick) but declares all three pushdown
    flags so DuckDB sends ``projection_ids`` / ``pushdown_filters`` on the
    InitRequest. The framework:

    * Narrows ``params.output_schema`` to the projected columns; the
      ``OutputCollector.emit`` call's ``batch.select(target_names)`` then
      drops non-projected columns from the buffered full-width batch.
    * Wraps ``out`` in ``_FilteringOutputCollector`` (because
      ``auto_apply_filters=True``) so emitted batches are filter-applied
      automatically.

    User code stays the streaming-style passthrough — no awareness of
    projection or filters needed. The fixture verifies that buffered
    TableBufferingFunction pushdown actually plumbs through end-to-end.
    """

    class Meta:
        """Metadata for EchoBufferingFunction."""

        name = "echo_buffering"
        description = "Buffered passthrough with projection + filter pushdown"
        categories = ["test", "buffer", "pushdown"]
        projection_pushdown = True
        filter_pushdown = True
        auto_apply_filters = True

    @classmethod
    def on_bind(cls, params: BindParams[SingleTableArguments]) -> BindResponse:
        """Output schema = input schema (passthrough)."""
        assert params.bind_call.input_schema is not None
        return BindResponse(output_schema=params.bind_call.input_schema)

    @classmethod
    def process(
        cls,
        batch: pa.RecordBatch,
        params: TableBufferingParams[SingleTableArguments],
    ) -> bytes:
        """Buffer the full input batch (no projection at storage time)."""
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)
        params.storage.state_append(b"buf", b"", sink.getvalue().to_pybytes())
        return params.execution_id

    @classmethod
    def combine(
        cls,
        state_ids: list[bytes],  # noqa: ARG003 - collapse to one finalize stream
        params: TableBufferingParams[SingleTableArguments],
    ) -> list[bytes]:
        return [params.execution_id]

    @classmethod
    def initial_finalize_state(
        cls,
        finalize_state_id: bytes,  # noqa: ARG003 - one bucket per execution
        params: TableBufferingParams[SingleTableArguments],  # noqa: ARG003
    ) -> _LogDrainState:
        return _LogDrainState(ns=b"buf", after_id=-1)

    @classmethod
    def finalize(
        cls,
        params: TableBufferingParams[SingleTableArguments],
        finalize_state_id: bytes,  # noqa: ARG003
        state: _LogDrainState,
        out: OutputCollector,
    ) -> None:
        """Emit one buffered batch per tick — framework narrows + filters."""
        rows = params.storage.state_log_scan(
            state.ns,
            b"",
            after_id=state.after_id,
            limit=1,
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


class SecretInOutFunction(TableInOutGenerator[SingleTableArguments]):
    """Table-in-out whose on_bind performs a two-phase secret lookup.

    Exercises secrets x table-in-out: ``on_bind`` calls ``params.secrets.get()``
    (the dynamic two-phase bind), and ``process`` appends the resolved secret's
    ``secret_string`` value as a column on every input row. Output schema is the
    input schema plus a ``secret_string`` column.
    """

    class Meta:
        """Metadata for SecretInOutFunction."""

        name = "secret_in_out"
        description = "Append a resolved secret value to each input row"
        categories = ["transform", "secret"]
        examples = [
            FunctionExample(
                sql="SELECT * FROM secret_in_out((SELECT 1 AS n))",
                description="Append the secret_string value to each input row",
            )
        ]

    @classmethod
    def on_bind(cls, params: BindParams[SingleTableArguments]) -> BindResponse:
        """Request the secret (two-phase) and add a secret_string output column."""
        params.secrets.get("vgi_example")
        assert params.bind_call.input_schema is not None
        fields = [*params.bind_call.input_schema, pa.field("secret_string", pa.string())]
        return BindResponse(output_schema=pa.schema(fields))

    @classmethod
    def process(
        cls,
        params: ProcessParams[SingleTableArguments],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        """Emit each input row with the resolved secret's secret_string appended."""
        secret = next(iter(params.secrets.of_type("vgi_example")), {})
        value = secret["secret_string"].as_py() if "secret_string" in secret else None
        columns = {name: batch.column(name) for name in batch.schema.names}
        columns["secret_string"] = pa.array([value] * batch.num_rows, type=pa.string())
        out.emit(pa.record_batch(columns, schema=params.output_schema))


@dataclass(slots=True, frozen=True)
class RepeatsInputsFunctionArguments:
    """Arguments for RepeatInputsFunction."""

    repeat_count: Annotated[int, Arg(0, doc="Number of times to repeat each input batch")]
    data: Annotated[TableInput, Arg(1, doc="Input table to repeat")]


class RepeatInputsFunction(TableInOutGenerator[RepeatsInputsFunctionArguments]):
    """Explosion function that duplicates each input batch N times.

    Useful for data augmentation, testing with larger datasets, or any
    scenario needing multiple copies of each input record. The output schema
    is the input schema unchanged; ``process()`` concatenates each input batch
    ``repeat_count`` times into a single output batch.

    The one SQL argument ``repeat_count`` (``Annotated[int, Arg(0)]``,
    required) sets the number of copies. For example, with ``repeat_count=3``,
    input ``[{"a": 1}]`` yields ``[{"a": 1}, {"a": 1}, {"a": 1}]``.
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
            input_summary = ", ".join(f"{f.name}: {f.type}" for f in params.bind_call.input_schema)
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
            # Goes through the same wire mechanism as the streaming
            # ``out.client_log()`` path — emits a 0-row log batch on the
            # ``table_buffering_process`` response stream that DuckDB
            # surfaces in ``duckdb_logs()`` with type='VGI'. The Python
            # stdlib ``logging.getLogger(...).info(...)`` we used before
            # didn't reach the wire and never showed up in duckdb_logs
            # (the framework provides no stdlib-logging-to-wire bridge).
            params.client_log(
                Level.INFO,
                f"Processing batch with {batch.num_rows} rows",
            )

        # Compute partial sums for this batch only.
        sums: dict[str, pa.Scalar[Any]] = {}
        for name in params.output_schema.names:
            col_sum = pc.sum(batch.column(name))
            if col_sum.is_valid:
                sums[name] = col_sum
            else:
                sums[name] = pa.scalar(
                    0,
                    type=params.output_schema.field(name).type,
                )
        partial = cls._scalars_to_single_row_batch(sums)
        params.storage.state_append(
            b"partial",
            b"",
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
        if params.args.logging:
            # Symmetric with process() — fires through ``params.client_log``
            # (the unary-RPC analogue of ``out.client_log``) so the message
            # lands in DuckDB's ``duckdb_logs()`` with type='VGI'. Used by
            # ``logging.test`` to verify the in-band log path works from
            # ``combine()`` too, not just from ``process()``.
            params.client_log(
                Level.INFO,
                f"Combining {len(state_ids)} state_ids",
            )

        merged: dict[str, pa.Scalar[Any]] = {
            name: pa.scalar(0, type=field.type)
            for name, field in zip(
                params.output_schema.names,
                params.output_schema,
                strict=True,
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
            state.ns,
            b"",
            after_id=state.after_id,
            limit=1,
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


@dataclass(kw_only=True)
class SumAllColumnsSimpleDistributedState(ArrowSerializableDataclass):
    """Partial sum state for distributed aggregation (one per process batch)."""

    partial_sum: Annotated[pa.RecordBatch, ArrowType(pa.binary())]


class SumAllColumnsSimpleDistributed(TableBufferingFunction[SingleTableArguments, _LogDrainState]):
    """Distributed column-wise sum — a *global* reduction over all input.

    Global combine (every input row contributes to a single output row) is a
    **buffered** table function, NOT a streaming table-in-out one: a streaming
    table-in-out is a per-substream map, and under per-substream worker fan-out
    each worker sees only its own substream's rows, so a streaming ``finish``
    that merges across substreams would produce a partial. This fixture used to
    demonstrate that (now-invalid) streaming-distributed ``finish(states)`` API;
    it has been migrated to the buffered Sink+Combine+Source model, which
    coordinates cross-worker state through ``BoundStorage`` keyed by
    ``execution_id`` (the correct home for a full-stream reduction).

    Behaviourally identical to ``SumAllColumnsFunction`` (kept as a distinct
    named fixture so its integration/unit tests keep exercising the buffered
    path under its own name): ``process()`` appends this batch's partial sums to
    an append-only per-execution log; ``combine()`` reduces the log to one merged
    row; ``finalize()`` emits it.

    Example:
    -------
    Input batches (fanned out across workers):
      Worker 1: [{a: 1, b: 1.0}, {a: 2, b: 2.0}]  -> partial {a: 3, b: 3.0}
      Worker 2: [{a: 3, b: 3.0}]                  -> partial {a: 3, b: 3.0}
    combine() reduces the partials:  {a: 6, b: 6.0}
    Output (single row):             [{a: 6, b: 6.0}]

    """

    class Meta:
        """Metadata for SumAllColumnsSimpleDistributed."""

        name = "sum_all_columns_simple_distributed"
        description = "Distributed sum using the buffered (Sink+Combine+Source) model"
        categories = ["aggregation", "numeric", "distributed"]
        examples = [
            FunctionExample(
                sql=("SELECT * FROM sum_all_columns_simple_distributed((SELECT * FROM input_table))"),
                description="Sum columns across buffered workers",
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

    @staticmethod
    def _scalars_to_single_row_batch(values: dict[str, pa.Scalar]) -> pa.RecordBatch:  # type: ignore[type-arg]
        arrays = [pa.array([scalar], type=scalar.type) for scalar in values.values()]
        return pa.RecordBatch.from_arrays(arrays, names=list(values.keys()))

    @classmethod
    def process(
        cls,
        batch: pa.RecordBatch,
        params: TableBufferingParams[SingleTableArguments],
    ) -> bytes:
        """Append this batch's partial sums to the append-only per-execution log.

        Race-safe (``state_append`` is atomic) so parallel process() calls across
        fanned-out workers accumulate without a lock; ``combine()`` reduces.
        """
        sums: dict[str, pa.Scalar[Any]] = {}
        for name in params.output_schema.names:
            col_sum = pc.sum(batch.column(name))
            if col_sum.is_valid:
                sums[name] = col_sum
            else:
                sums[name] = pa.scalar(0, type=params.output_schema.field(name).type)
        partial = cls._scalars_to_single_row_batch(sums)
        params.storage.state_append(
            b"partial",
            b"",
            SumAllColumnsSimpleDistributedState(partial_sum=partial).serialize_to_bytes(),
        )
        return params.execution_id

    @classmethod
    def combine(
        cls,
        state_ids: list[bytes],
        params: TableBufferingParams[SingleTableArguments],
    ) -> list[bytes]:
        """Reduce all per-batch partials into one merged row for finalize.

        Runs once on the coordinator after every process() completes (no race).
        The empty-input guard still writes the zeros row so an empty source
        produces one row of the expected shape.
        """
        merged: dict[str, pa.Scalar[Any]] = {
            name: pa.scalar(0, type=field.type)
            for name, field in zip(params.output_schema.names, params.output_schema, strict=True)
        }
        for _log_id, blob in params.storage.state_log_scan(b"partial", b""):
            partial = SumAllColumnsSimpleDistributedState.deserialize_from_bytes(blob).partial_sum
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
        rows = params.storage.state_log_scan(state.ns, b"", after_id=state.after_id, limit=1)
        if not rows:
            out.finish()
            return
        log_id, value = rows[0]
        out.emit(pa.ipc.open_stream(value).read_next_batch())
        state.after_id = log_id


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
        if sys.platform == "win32":  # pragma: no cover - hard-crash equivalent
            os.kill(os.getpid(), signal.SIGABRT)
        else:
            os.kill(os.getpid(), signal.SIGKILL)
        return params.execution_id  # unreachable


class CrashOnCombineFunction(BufferInputFunction):
    """Buffers input normally; raises during combine()."""

    class Meta(BufferInputFunction.Meta):
        name = "crash_on_combine"
        description = "Worker raises during combine (test)"
        categories = ["test", "crash"]

    @classmethod
    def combine(cls, state_ids: list[bytes], params: TableBufferingParams[SingleTableArguments]) -> list[bytes]:
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
            b"large",
            b"",
            b"\x00" * (1024 * 1024),
        )
        return params.execution_id

    @classmethod
    def combine(
        cls,
        state_ids: list[bytes],
        params: TableBufferingParams[SingleTableArguments],
    ) -> list[bytes]:
        """Materialize one output row carrying the total payload size."""
        total = sum(len(blob) for _log_id, blob in params.storage.state_log_scan(b"large", b""))
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
            state.ns,
            b"",
            after_id=state.after_id,
            limit=1,
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
            b"unsorted",
            b"",
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
            _unpack_indexed_batch(v) for _, v in params.storage.state_log_scan(b"unsorted", b"")
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
            state.ns,
            b"",
            after_id=state.after_id,
            limit=1,
        )
        if not rows:
            out.finish()
            return
        log_id, value = rows[0]
        out.emit(pa.ipc.open_stream(value).read_next_batch())
        state.after_id = log_id


@dataclass
class _OneShotState(ArrowSerializableDataclass):
    """Single-emit cursor for ``OrderedSourceFunction.finalize``."""

    value: int = 0
    emitted: bool = False


class OrderedSourceFunction(TableBufferingFunction[SingleTableArguments, _OneShotState]):
    """Buffered table function with ``source_order_dependent=True``.

    Forces ``ParallelSource()=false`` and ``SourceOrder()=FIXED_ORDER`` on the
    C++ ``PhysicalVgiTableBufferingFunction``. The Source phase serial-drains
    ``finalize_queue`` in whatever order ``combine()`` populated it; without
    ``source_order_dependent`` the parallel Source drains would race and emit
    rows in arbitrary order.

    The fixture deliberately ignores its input and emits a fixed 0..15
    integer sequence so the assertion is deterministic regardless of Sink
    parallelism or input partitioning: ``combine()`` returns sixteen
    finalize_state_ids encoded as 4-byte big-endian integers in ascending
    order; ``finalize()`` decodes its state_id and emits exactly one row
    containing that integer. With ``source_order_dependent`` the C++ Source
    must yield rows in the same 0..15 order.

    Output schema: single ``v`` column (BIGINT).
    """

    class Meta:
        name = "ordered_source"
        description = "Emits a fixed 0..15 sequence via source_order_dependent=True; input is ignored"
        categories = ["test", "ordering"]
        source_order_dependent = True

    _N_ROWS = 16

    @classmethod
    def on_bind(cls, params: BindParams[SingleTableArguments]) -> BindResponse:
        return BindResponse(output_schema=schema(v=pa.int64()))

    @classmethod
    def process(
        cls,
        batch: pa.RecordBatch,
        params: TableBufferingParams[SingleTableArguments],
    ) -> bytes:
        # Input is irrelevant — the test asserts source ordering, not data.
        return params.execution_id

    @classmethod
    def combine(
        cls,
        state_ids: list[bytes],
        params: TableBufferingParams[SingleTableArguments],
    ) -> list[bytes]:
        # Fixed monotonically-ascending list of 4-byte big-endian integers.
        # FIXED_ORDER Source must drain finalize_queue in this exact order.
        return [i.to_bytes(4, "big") for i in range(cls._N_ROWS)]

    @classmethod
    def initial_finalize_state(
        cls,
        finalize_state_id: bytes,
        params: TableBufferingParams[SingleTableArguments],
    ) -> _OneShotState:
        return _OneShotState(value=int.from_bytes(finalize_state_id, "big"))

    @classmethod
    def finalize(
        cls,
        params: TableBufferingParams[SingleTableArguments],
        finalize_state_id: bytes,
        state: _OneShotState,
        out: OutputCollector,
    ) -> None:
        if state.emitted:
            out.finish()
            return
        out.emit(pa.RecordBatch.from_pylist([{"v": state.value}], schema=params.output_schema))
        state.emitted = True


# ---------------------------------------------------------------------------
# Repro fixture: emit a single large finalize batch from a buffering function.
# ---------------------------------------------------------------------------


@dataclass(slots=True, frozen=True, kw_only=True)
class BufferEmitWideArguments:
    """Arguments for BufferEmitWideFunction."""

    rows: Annotated[int, Arg(0, doc="Number of rows to emit in one finalize batch", ge=0)]
    data: Annotated[TableInput, Arg(1, doc="Input table (content ignored)")]


@dataclass
class _EmitOnceState(ArrowSerializableDataclass):
    """Whether the single finalize batch has been emitted."""

    emitted: bool = False


_BUFFER_EMIT_WIDE_SCHEMA = schema(n=pa.int64())


class BufferEmitWideFunction(TableBufferingFunction[BufferEmitWideArguments, _EmitOnceState]):
    """Buffering function whose Source phase emits ONE batch of ``rows`` rows.

    Unlike BufferInputFunction (which echoes input batches, each already
    capped at DuckDB's standard vector size), this emits a single, arbitrarily
    large output batch from ``finalize``. It is a minimal repro for whether the
    buffering Source path supports output batches larger than the standard
    vector size (2048 rows) — a regular TableFunctionGenerator (e.g. sequence)
    does support this.
    """

    class Meta:
        """Metadata for BufferEmitWideFunction."""

        name = "buffer_emit_wide"
        description = "Emit a single finalize batch of N rows (vector-size repro)"
        categories = ["test", "buffer"]
        examples = [
            FunctionExample(
                sql="SELECT count(*) FROM buffer_emit_wide((SELECT 1), 10000)",
                description="Emit a single 10000-row batch from the Source phase",
            )
        ]

    @classmethod
    def on_bind(cls, params: BindParams[BufferEmitWideArguments]) -> BindResponse:
        return BindResponse(output_schema=_BUFFER_EMIT_WIDE_SCHEMA)

    @classmethod
    def process(cls, batch: pa.RecordBatch, params: TableBufferingParams[BufferEmitWideArguments]) -> bytes:
        return params.execution_id

    @classmethod
    def combine(cls, state_ids: list[bytes], params: TableBufferingParams[BufferEmitWideArguments]) -> list[bytes]:
        return [params.execution_id]

    @classmethod
    def initial_finalize_state(
        cls, finalize_state_id: bytes, params: TableBufferingParams[BufferEmitWideArguments]
    ) -> _EmitOnceState:
        return _EmitOnceState(emitted=False)

    @classmethod
    def finalize(
        cls,
        params: TableBufferingParams[BufferEmitWideArguments],
        finalize_state_id: bytes,
        state: _EmitOnceState,
        out: OutputCollector,
    ) -> None:
        if state.emitted:
            out.finish()
            return
        n = params.args.rows
        out.emit(pa.RecordBatch.from_pydict({"n": list(range(n))}, schema=_BUFFER_EMIT_WIDE_SCHEMA))
        state.emitted = True


@dataclass(slots=True, frozen=True, kw_only=True)
class GeoArgs:
    """Blended args: latitude/longitude are per-row input columns; precision is a named option."""

    latitude: Annotated[float, Arg(0, doc="Latitude input column")]
    longitude: Annotated[float, Arg(1, doc="Longitude input column")]
    precision: Annotated[int, Arg("precision", doc="Rounding precision", default=4)] = 4


class GeoEncodeFunction(RowTransformFunction[GeoArgs]):
    """Blended ("UNNEST-style") geo encoder — one registration serves every call shape.

    Proves geo_encode(52.0, 13.0) (literal), FROM t, geo_encode(t.x, t.y) (columns),
    and LATERAL geo_encode(t.x, t.y) all resolve to one registration.

    latitude/longitude are POSITIONAL args = the per-row input columns (read from
    ``batch`` by declared name — the C++ bind builds the input schema from the
    declared arg names). ``precision`` is a str-position NAMED arg, surfaced on
    ``params.args`` (positional args are NOT). Emits one ``geohash`` string per
    input row: ``"<lat>:<lon>"`` rounded to ``precision`` decimals — deterministic
    so tests assert exact values.
    """

    class Meta:
        name = "geo_encode"
        description = "Blended per-row geo encoder (lat, lon -> geohash)"
        categories = ["geo", "blended"]

    @classmethod
    def on_bind(cls, params: BindParams[GeoArgs]) -> BindResponse:
        return BindResponse(output_schema=schema({"geohash": pa.string()}))

    @classmethod
    def process(
        cls,
        params: ProcessParams[GeoArgs],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        precision = params.args.precision
        # NB: a blended LITERAL call delivers the constant's natural type (a DuckDB
        # DECIMAL for 52.0, which round() would pad to `precision` places), while a
        # COLUMN call delivers the cast declared type (DOUBLE). Cast to float so the
        # output is identical across call shapes regardless of that wart.
        lats = batch.column("latitude").to_pylist()
        lons = batch.column("longitude").to_pylist()
        codes = [
            None
            if lat is None or lon is None
            else f"{round(float(lat), precision)}:{round(float(lon), precision)}"
            for lat, lon in zip(lats, lons, strict=True)
        ]
        out.emit(pa.record_batch({"geohash": pa.array(codes, type=pa.string())}))


@dataclass(slots=True, frozen=True, kw_only=True)
class Geo3Args:
    """Blended args for the 3-positional geo_encode overload."""

    latitude: Annotated[float, Arg(0, doc="Latitude input column")]
    longitude: Annotated[float, Arg(1, doc="Longitude input column")]
    altitude: Annotated[float, Arg(2, doc="Altitude input column")]
    precision: Annotated[int, Arg("precision", doc="Rounding precision", default=4)] = 4


class GeoEncode3Function(RowTransformFunction[Geo3Args]):
    """Arity-overloaded blended geo encoder — same Meta.name, 3 positional columns.

    Same ``Meta.name`` as GeoEncodeFunction ("geo_encode") but 3 positional input
    columns (lat, lon, alt). Proves same-name blended overloads resolve by arity:
    blended functions use
    REAL value types (no TABLE-typed arg), so DuckDB permits multiple overloads
    (the bind_table_function.cpp restriction that forbids a TABLE overload mixed
    with others does not apply). geo_encode(52,13) resolves to the 2-arg overload,
    geo_encode(52,13,100) to this 3-arg one, in both literal and column shapes.
    """

    class Meta:
        name = "geo_encode"
        description = "Blended per-row geo encoder (lat, lon, alt -> geohash)"
        categories = ["geo", "blended"]

    @classmethod
    def on_bind(cls, params: BindParams[Geo3Args]) -> BindResponse:
        return BindResponse(output_schema=schema({"geohash": pa.string()}))

    @classmethod
    def process(
        cls,
        params: ProcessParams[Geo3Args],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        p = params.args.precision
        lats = batch.column("latitude").to_pylist()
        lons = batch.column("longitude").to_pylist()
        alts = batch.column("altitude").to_pylist()
        codes = [
            None
            if lat is None or lon is None or alt is None
            else f"{round(float(lat), p)}:{round(float(lon), p)}:{round(float(alt), p)}"
            for lat, lon, alt in zip(lats, lons, alts, strict=True)
        ]
        out.emit(pa.record_batch({"geohash": pa.array(codes, type=pa.string())}))


@dataclass(slots=True, frozen=True, kw_only=True)
class RowSumArgs:
    """Blended VARARGS args: ``values`` is N input columns; ``absolute`` is a named option."""

    values: Annotated[list[float], Arg(0, varargs=True, arrow_type=pa.float64(), doc="Numeric input columns")]
    absolute: Annotated[bool, Arg("absolute", doc="Sum absolute values", default=False)] = False


class RowSumFunction(RowTransformFunction[RowSumArgs]):
    """Blended VARARGS row-wise sum — proves the varargs input path.

    ``values`` is a varargs positional Arg: the per-row input is N columns of the
    declared type. A varargs blended function has no per-column declared names
    (the literal call gives empty input_table_names), so the worker reads the
    columns POSITIONALLY via ``input_columns(batch)`` (the C++ bind names them
    col0..colN-1). ``row_sum(1,2,3) -> 6``; ``FROM t, row_sum(t.a,t.b,t.c)`` sums
    each row's columns. The ``absolute`` named option is surfaced on params.args.
    """

    class Meta:
        name = "row_sum"
        description = "Blended per-row varargs sum"
        categories = ["numeric", "blended"]

    @classmethod
    def on_bind(cls, params: BindParams[RowSumArgs]) -> BindResponse:
        return BindResponse(output_schema=schema({"row_sum": pa.float64()}))

    @classmethod
    def process(
        cls,
        params: ProcessParams[RowSumArgs],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        cols = cls.input_columns(batch)
        absolute = params.args.absolute
        acc = None
        for col in cols:
            c = pc.abs(col) if absolute else col
            acc = c if acc is None else pc.add(acc, c)
        if acc is None:
            acc = pa.array([0.0] * batch.num_rows, type=pa.float64())
        out.emit(pa.record_batch({"row_sum": pc.cast(acc, pa.float64())}))
