"""Sequence-style table generators (sequence, double_sequence, partitioned_sequence, etc.)."""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Annotated, Any, ClassVar

import numpy as np
import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi._test_fixtures.table._common import (
    CountdownState,
    _BaseSequenceFunction,
    _cardinality_from_count,
)
from vgi.arguments import Arg
from vgi.invocation import BindResponse, GlobalInitResponse
from vgi.metadata import FunctionExample
from vgi.schema_utils import schema
from vgi.table_function import (
    BindParams,
    InitParams,
    ProcessParams,
    TableCardinality,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)


@dataclass(slots=True, frozen=True)
class SequenceFunctionArgs:
    """Arguments for SequenceFunction."""

    count: Annotated[int, Arg(0, doc="Number of integers to generate", ge=0)]
    batch_size: Annotated[int, Arg("batch_size", default=1000, doc="Batch size for output", ge=1)]
    increment: Annotated[int, Arg("increment", default=1, doc="Step between values", ge=1)]


@init_single_worker
@bind_fixed_schema
@_cardinality_from_count
class SequenceFunction(_BaseSequenceFunction):
    """Generates a sequence of integers from 0 to n-1 with optional increment.

    USE CASE
    --------
    Generate test data, create row numbers, or produce a fixed sequence
    for joining or filtering. The increment parameter allows generating
    sequences like 0, 2, 4, 6, ... or 0, 10, 20, 30, ...

    SCHEMA
    ------
    Output: {"n": int64}

    Example:
    -------
    SELECT * FROM sequence(5)
    Returns: [{"n": 0}, {"n": 1}, {"n": 2}, {"n": 3}, {"n": 4}]

    SELECT * FROM sequence(5, increment := 2)
    Returns: [{"n": 0}, {"n": 2}, {"n": 4}, {"n": 6}, {"n": 8}]

    SELECT * FROM sequence(1000, batch_size := 100)
    Returns: integers 0-999 in batches of 100 rows each

    """

    FunctionArguments = SequenceFunctionArgs

    class Meta:
        """Metadata for SequenceFunction."""

        name = "sequence"
        description = "Generates a sequence of integers from 0 to n-1"
        categories = ["generator", "utility"]
        tags = {"category": "generator", "type": "utility"}
        projection_pushdown = True
        filter_pushdown = True
        auto_apply_filters = True
        examples = [
            FunctionExample(
                sql="SELECT * FROM sequence(10)",
                description="Generate integers 0-9",
            ),
            FunctionExample(
                sql="SELECT * FROM sequence(1000, batch_size := 100)",
                description="Generate integers 0-999 in batches of 100",
            ),
            FunctionExample(
                sql="SELECT * FROM sequence(5, batch_size := 10000, increment := 10)",
                description="Generate 0, 10, 20, 30, 40",
            ),
        ]

    # Full schema before projection
    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(n=pa.int64())
    NUMPY_DTYPE: ClassVar[type[np.generic]] = np.int64


@dataclass(slots=True, frozen=True)
class NamedParamsEchoFunctionArgs:
    """Arguments for NamedParamsEchoFunction."""

    count: Annotated[int, Arg(0, doc="Number of rows to generate", ge=0)]
    greeting: Annotated[str, Arg("greeting", default="hello", doc="Greeting text echoed in output")]
    multiplier: Annotated[int, Arg("multiplier", default=1, doc="Multiplier for value column")]
    scale: Annotated[float, Arg("scale", default=1.0, doc="Scale factor for float_value column")]
    enabled: Annotated[bool, Arg("enabled", default=True, doc="Boolean echoed in output")]


@init_single_worker
@bind_fixed_schema
@_cardinality_from_count
class NamedParamsEchoFunction(TableFunctionGenerator[NamedParamsEchoFunctionArgs, CountdownState]):
    """Echoes named parameter values directly in output columns.

    USE CASE
    --------
    Testing that named parameters of various types (VARCHAR, BIGINT, DOUBLE,
    BOOLEAN) are correctly passed from DuckDB to the worker. Each named
    parameter value is echoed directly in an output column, making it easy
    to assert correctness.

    SCHEMA
    ------
    Output: {"id": int64, "greeting": string, "value": int64, "float_value": float64, "enabled": bool}

    Example:
    -------
    SELECT * FROM named_params_echo(3)
    Returns: rows with id=0..2, greeting='hello', value=id*1, float_value=id*1.0, enabled=true

    SELECT * FROM named_params_echo(3, greeting := 'hi', multiplier := 10)
    Returns: rows with id=0..2, greeting='hi', value=id*10, float_value=id*1.0, enabled=true

    """

    FunctionArguments = NamedParamsEchoFunctionArgs

    class Meta:
        """Metadata for NamedParamsEchoFunction."""

        name = "named_params_echo"
        description = "Echoes named parameter values in output columns"
        categories = ["generator", "testing"]
        tags = {"category": "testing", "type": "params"}
        examples = [
            FunctionExample(
                sql="SELECT * FROM named_params_echo(3)",
                description="Echo default parameter values for 3 rows",
            ),
            FunctionExample(
                sql="SELECT * FROM named_params_echo(3, greeting := 'hi', multiplier := 10)",
                description="Echo custom greeting and multiplier",
            ),
        ]

    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(
        {
            "id": pa.int64(),
            "greeting": pa.string(),
            "value": pa.int64(),
            "float_value": pa.float64(),
            "enabled": pa.bool_(),
        }
    )

    BATCH_SIZE: ClassVar[int] = 1000

    @classmethod
    def initial_state(cls, params: ProcessParams[NamedParamsEchoFunctionArgs]) -> CountdownState:
        """Create initial state with remaining count."""
        return CountdownState(remaining=params.args.count)

    @classmethod
    def process(
        cls,
        params: ProcessParams[NamedParamsEchoFunctionArgs],
        state: CountdownState,
        out: OutputCollector,
    ) -> None:
        """Generate rows echoing named parameter values."""
        if state.remaining <= 0:
            out.finish()
            return

        size = min(state.remaining, cls.BATCH_SIZE)
        ids = list(range(state.current_index, state.current_index + size))

        data: dict[str, list[int] | list[str] | list[float] | list[bool]] = {
            "id": ids,
            "greeting": [params.args.greeting] * size,
            "value": [i * params.args.multiplier for i in ids],
            "float_value": [i * params.args.scale for i in ids],
            "enabled": [params.args.enabled] * size,
        }

        out.emit(pa.RecordBatch.from_pydict(data, schema=params.output_schema))

        state.current_index += size
        state.remaining -= size


@dataclass(slots=True, frozen=True)
class NestedSequenceFunctionArguments:
    """Arguments for NestedSequenceFunction."""

    count: Annotated[int, Arg(0, doc="Number of rows to generate", ge=0)]
    batch_size: Annotated[int, Arg("batch_size", default=1000, doc="Batch size for output", ge=1)]
    history_size: Annotated[int, Arg("history_size", default=20, doc="Max items in history list", ge=1)]


@init_single_worker
@bind_fixed_schema
@_cardinality_from_count
class NestedSequenceFunction(TableFunctionGenerator[NestedSequenceFunctionArguments, CountdownState]):
    """Generates a sequence with nested struct and list columns.

    USE CASE
    --------
    Test filter pushdown with complex types (structs and lists). The function
    generates rows with:
    - n: sequence index (0 to count-1)
    - metadata: struct with {index: int64, label: string}
    - history: list of the last 20 sequence values

    SCHEMA
    ------
    Output: {
        "n": int64,
        "metadata": struct<index: int64, label: string>,
        "history": list<int64>
    }

    Example:
    -------
    SELECT * FROM nested_sequence(5)
    Returns rows with n=0..4, metadata structs, and history lists

    SELECT * FROM nested_sequence(100) WHERE n >= 50
    Test filter pushdown on the sequence column

    SELECT metadata.index FROM nested_sequence(10)
    Test projection pushdown with struct field access

    """

    class Meta:
        """Metadata for NestedSequenceFunction."""

        name = "nested_sequence"
        description = "Generates a sequence with nested struct and list columns"
        categories = ["generator", "utility", "testing"]
        tags = {"category": "generator", "type": "testing"}
        projection_pushdown = True
        filter_pushdown = True
        auto_apply_filters = True
        examples = [
            FunctionExample(
                sql="SELECT * FROM nested_sequence(10)",
                description="Generate 10 rows with nested columns",
            ),
            FunctionExample(
                sql="SELECT n, metadata FROM nested_sequence(100) WHERE n >= 50",
                description="Filter and project nested sequence",
            ),
        ]

    # Full schema before projection
    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            pa.field("n", pa.int64()),
            pa.field(
                "metadata",
                pa.struct([("index", pa.int64()), ("label", pa.string())]),
            ),
            pa.field("history", pa.list_(pa.int64())),
        ]
    )

    @classmethod
    def _get_projected_column_names(cls, projection_ids: list[int] | None) -> set[str]:
        """Get the set of column names to generate."""
        if projection_ids is not None:
            return {cls.FIXED_SCHEMA.field(i).name for i in projection_ids}
        return {f.name for f in cls.FIXED_SCHEMA}

    @classmethod
    def initial_state(cls, params: ProcessParams[NestedSequenceFunctionArguments]) -> CountdownState:
        """Create initial state with remaining count."""
        return CountdownState(remaining=params.args.count)

    @classmethod
    def process(
        cls,
        params: ProcessParams[NestedSequenceFunctionArguments],
        state: CountdownState,
        out: OutputCollector,
    ) -> None:
        """Generate the next batch of the nested sequence."""
        if state.remaining <= 0:
            out.finish()
            return

        size = min(state.remaining, params.args.batch_size)
        assert params.init_call is not None
        projected_cols = cls._get_projected_column_names(params.init_call.projection_ids)
        indices = list(range(state.current_index, state.current_index + size))
        data: dict[str, Any] = {}

        if "n" in projected_cols:
            data["n"] = indices

        if "metadata" in projected_cols:
            data["metadata"] = [{"index": i, "label": f"row_{i}"} for i in indices]

        if "history" in projected_cols:
            history_list = []
            for i in indices:
                start = max(0, i - params.args.history_size + 1)
                history_list.append(list(range(start, i + 1)))
            data["history"] = history_list

        out.emit(pa.RecordBatch.from_pydict(data, schema=params.output_schema))

        state.current_index += size
        state.remaining -= size


@dataclass(slots=True, frozen=True)
class DoubleSequenceFunctionArguments:
    """Arguments for DoubleSequenceFunction."""

    count: Annotated[int, Arg(0, doc="Number of values to generate", ge=0)]
    batch_size: Annotated[int, Arg("batch_size", default=1000, doc="Batch size for output", ge=1)]
    increment: Annotated[float, Arg("increment", default=1.0, doc="Step between values", gt=0.0)]


@init_single_worker
@bind_fixed_schema
@_cardinality_from_count
class DoubleSequenceFunction(_BaseSequenceFunction):
    """Generates a sequence of floats from 0.0 to n-1 with optional increment.

    USE CASE
    --------
    Generate test data with floating-point values, create sequences for
    interpolation or sampling. The increment parameter allows generating
    sequences like 0.0, 0.5, 1.0, 1.5, ... or 0.0, 0.1, 0.2, 0.3, ...

    SCHEMA
    ------
    Output: {"n": float64}

    Example:
    -------
    SELECT * FROM double_sequence(5)
    Returns: [{"n": 0.0}, {"n": 1.0}, {"n": 2.0}, {"n": 3.0}, {"n": 4.0}]

    SELECT * FROM double_sequence(5, increment := 0.5)
    Returns: [{"n": 0.0}, {"n": 0.5}, {"n": 1.0}, {"n": 1.5}, {"n": 2.0}]

    SELECT * FROM double_sequence(1000, batch_size := 100)
    Returns: floats 0.0-999.0 in batches of 100 rows each

    """

    FunctionArguments = DoubleSequenceFunctionArguments

    class Meta:
        """Metadata for DoubleSequenceFunction."""

        name = "double_sequence"
        description = "Generates a sequence of floating-point numbers from 0 to n-1"
        categories = ["generator", "utility"]
        tags = {"category": "generator", "type": "utility"}
        examples = [
            FunctionExample(
                sql="SELECT * FROM double_sequence(10)",
                description="Generate floats 0.0-9.0",
            ),
            FunctionExample(
                sql="SELECT * FROM double_sequence(1000, batch_size := 100)",
                description="Generate floats 0.0-999.0 in batches of 100",
            ),
            FunctionExample(
                sql="SELECT * FROM double_sequence(5, increment := 0.5)",
                description="Generate 0.0, 0.5, 1.0, 1.5, 2.0",
            ),
        ]

    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(n=pa.float64())
    NUMPY_DTYPE: ClassVar[type[np.generic]] = np.float64
    STATS_ARROW_TYPE: ClassVar[pa.DataType] = pa.float64()


@dataclass(slots=True, frozen=True)
class PartitionedSequenceFunctionArguments:
    """Arguments for PartitionedSequenceFunction."""

    count: Annotated[int, Arg(0, doc="Total number of integers to generate", ge=0)]
    increment: Annotated[int, Arg("increment", default=1, doc="Step between values", ge=1)]


@dataclass(kw_only=True)
class PartitionedSequenceState(ArrowSerializableDataclass):
    """Mutable state for PartitionedSequenceFunction."""

    current_start: int | None = None
    current_end: int | None = None
    current_idx: int = 0


@bind_fixed_schema
@_cardinality_from_count
class PartitionedSequenceFunction(
    TableFunctionGenerator[PartitionedSequenceFunctionArguments, PartitionedSequenceState]
):
    """Generates a partitioned sequence of integers for multi-worker execution.

    USE CASE
    --------
    Generate a sequence of values using a work queue pattern. The primary worker
    populates a queue with work chunks during initialization. All workers
    (including the primary) pull chunks from the queue and generate output.

    This is resilient to fewer workers launching than expected - all work
    will still be completed by the available workers.

    SCHEMA
    ------
    Output: {"n": int64}

    PARALLELIZATION
    ---------------
    Fully parallelizable using a shared work queue. Each worker pulls chunks
    atomically from the queue and generates values for that chunk.

    The union of all workers' output produces the complete sequence.

    Example:
    -------
    With count=3000 and CHUNK_SIZE=1000:
        Queue is populated with: [(0, 1000), (1000, 2000), (2000, 3000)]
        Workers pull chunks and generate values for each range.
        Combined output: [0, 1, 2, ..., 2999]

    With count=5 and increment=10:
        Combined output: [0, 10, 20, 30, 40]

    """

    class Meta:
        """Metadata for PartitionedSequenceFunction."""

        name = "partitioned_sequence"
        description = "Generates a partitioned sequence for multi-worker execution"
        categories = ["generator", "utility"]
        examples = [
            FunctionExample(
                sql="SELECT * FROM partitioned_sequence(100)",
                description="Generate 0-99 in parallel across workers",
            ),
            FunctionExample(
                sql="SELECT * FROM partitioned_sequence(5, increment := 10)",
                description="Generate 0, 10, 20, 30, 40 in parallel",
            ),
        ]

    # Size of each work chunk in the queue
    CHUNK_SIZE: ClassVar[int] = 1000
    # Batch size for output within each chunk
    BATCH_SIZE: ClassVar[int] = 1000

    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(n=pa.int64())

    @classmethod
    def on_init(
        cls,
        params: InitParams[PartitionedSequenceFunctionArguments],
    ) -> GlobalInitResponse:
        """Perform the global init of the worker for this function call."""
        # Create work items for each chunk of the sequence
        work_items: list[bytes] = []
        for start_idx in range(0, params.args.count, cls.CHUNK_SIZE):
            end_idx = min(start_idx + cls.CHUNK_SIZE, params.args.count)
            # Pack as two unsigned 64-bit integers: (start_idx, end_idx)
            work_items.append(struct.pack(">QQ", start_idx, end_idx))

        # Always enqueue (even if empty) to register the invocation
        params.storage.queue_push(work_items)
        return GlobalInitResponse()

    @classmethod
    def initial_state(cls, params: ProcessParams[PartitionedSequenceFunctionArguments]) -> PartitionedSequenceState:
        """Create initial state."""
        return PartitionedSequenceState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[PartitionedSequenceFunctionArguments],
        state: PartitionedSequenceState,
        out: OutputCollector,
    ) -> None:
        """Generate values by pulling chunks from the work queue."""
        # If we have no current chunk or finished current chunk, pop next
        if state.current_start is None or state.current_idx >= (state.current_end or 0):
            work_data = params.storage.queue_pop()
            if work_data is None:
                out.finish()
                return
            state.current_start, state.current_end = struct.unpack(">QQ", work_data)
            assert state.current_start is not None
            state.current_idx = state.current_start

        batch_end_idx = min(state.current_idx + cls.BATCH_SIZE, state.current_end or 0)
        values = [idx * params.args.increment for idx in range(state.current_idx, batch_end_idx)]

        out.emit(pa.RecordBatch.from_pydict({"n": values}, schema=params.output_schema))

        state.current_idx = batch_end_idx


@dataclass(slots=True, frozen=True)
class TenThousandFunctionArguments:
    """Arguments for TenThousandFunction."""


@dataclass(kw_only=True)
class TenThousandState(ArrowSerializableDataclass):
    """Mutable state for TenThousandFunction."""

    start: int = 0


@init_single_worker
@bind_fixed_schema
class TenThousandFunction(TableFunctionGenerator[TenThousandFunctionArguments, TenThousandState]):
    """Generates 10000 rows with integers from 0 to 9999.

    USE CASE
    --------
    Simple test data generator with a fixed row count. Useful for testing
    and benchmarking without needing to specify parameters.

    SCHEMA
    ------
    Output: {"n": int64}

    Example:
    -------
    SELECT * FROM ten_thousand()
    Returns: [{"n": 0}, {"n": 1}, ..., {"n": 9999}]

    """

    class Meta:
        """Metadata for TenThousandFunction."""

        name = "ten_thousand"
        description = "Generates 10000 integers from 0 to 9999"
        categories = ["generator", "utility"]
        examples = [
            FunctionExample(
                sql="SELECT * FROM ten_thousand()",
                description="Generate integers 0-9999",
            ),
        ]

    BATCH_SIZE: ClassVar[int] = 1000

    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(n=pa.int64())

    @classmethod
    def cardinality(cls, params: BindParams[TenThousandFunctionArguments]) -> TableCardinality:
        """Return exact cardinality (always 10000)."""
        return TableCardinality(estimate=10000, max=10000)

    @classmethod
    def initial_state(cls, params: ProcessParams[TenThousandFunctionArguments]) -> TenThousandState:
        """Create initial state."""
        return TenThousandState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[TenThousandFunctionArguments],
        state: TenThousandState,
        out: OutputCollector,
    ) -> None:
        """Generate 10000 integers in batches."""
        if state.start >= 10000:
            out.finish()
            return

        end = min(state.start + cls.BATCH_SIZE, 10000)
        values = np.arange(state.start, end, dtype=np.int64)
        out.emit(pa.RecordBatch.from_pydict({"n": values}, schema=params.output_schema))
        state.start = end


@dataclass(slots=True, frozen=True)
class RowIdSequenceFunctionArgs:
    """Arguments for RowIdSequenceFunction."""

    count: Annotated[int, Arg(0, doc="Number of rows to generate", ge=0)]
    layout: Annotated[str, Arg("layout", default="first", doc="Row ID column position: first, middle, last")]
    row_id_type: Annotated[str, Arg("row_id_type", default="int64", doc="Row ID type: int64, string, struct")]


@init_single_worker
class RowIdSequenceFunction(TableFunctionGenerator[RowIdSequenceFunctionArgs, CountdownState]):
    """Generates a sequence with a row_id column for testing row_id support.

    The layout argument controls where the row_id column appears in the schema,
    and row_id_type controls the type of the row_id column.

    """

    class Meta:
        """Metadata for RowIdSequenceFunction."""

        name = "rowid_sequence"
        description = "Sequence with row_id column"
        projection_pushdown = True

    BATCH_SIZE: ClassVar[int] = 1000

    @classmethod
    def on_bind(cls, params: BindParams[RowIdSequenceFunctionArgs]) -> BindResponse:
        """Build schema with is_row_id metadata on the appropriate field."""
        layout = params.args.layout
        row_id_type = params.args.row_id_type

        # Build the row_id field with is_row_id metadata
        rid_metadata = {b"is_row_id": b""}
        rid_field: pa.Field[Any]
        if row_id_type == "string":
            rid_field = pa.field("row_id", pa.string(), metadata=rid_metadata)
        elif row_id_type == "struct":
            rid_field = pa.field(
                "row_id",
                pa.struct([("a", pa.int64()), ("b", pa.string())]),
                metadata=rid_metadata,
            )
        else:  # int64
            rid_field = pa.field("row_id", pa.int64(), metadata=rid_metadata)

        name_field = pa.field("name", pa.string())
        value_field = pa.field("value", pa.string())

        if layout == "middle":
            fields = [name_field, rid_field, value_field]
        elif layout == "last":
            fields = [name_field, value_field, rid_field]
        else:  # first
            fields = [rid_field, name_field, value_field]

        return BindResponse(output_schema=pa.schema(fields))

    @classmethod
    def initial_state(cls, params: ProcessParams[RowIdSequenceFunctionArgs]) -> CountdownState:
        """Create initial state with remaining count."""
        return CountdownState(remaining=params.args.count)

    @classmethod
    def process(
        cls,
        params: ProcessParams[RowIdSequenceFunctionArgs],
        state: CountdownState,
        out: OutputCollector,
    ) -> None:
        """Generate batch with row_id and data columns."""
        if state.remaining <= 0:
            out.finish()
            return

        size = min(state.remaining, cls.BATCH_SIZE)
        start = state.current_index

        # Build columns matching the output schema field order
        columns: dict[str, Any] = {}
        for f in params.output_schema:
            if f.name == "row_id":
                if pa.types.is_string(f.type):
                    columns["row_id"] = [f"rid_{i}" for i in range(start, start + size)]
                elif pa.types.is_struct(f.type):
                    columns["row_id"] = [{"a": i, "b": f"s_{i}"} for i in range(start, start + size)]
                else:
                    columns["row_id"] = list(range(start, start + size))
            elif f.name == "name":
                columns["name"] = [f"item_{i}" for i in range(start, start + size)]
            elif f.name == "value":
                columns["value"] = [f"val_{i}" for i in range(start, start + size)]

        out.emit(pa.RecordBatch.from_pydict(columns, schema=params.output_schema))

        state.current_index += size
        state.remaining -= size
