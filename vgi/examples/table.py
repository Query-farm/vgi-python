"""Example table function implementations using TableFunctionGenerator.

This module contains table functions that generate output without receiving input.
Each function demonstrates different patterns for generating data.

AVAILABLE FUNCTIONS
-------------------
ConstantColumnsFunction       - Demonstrates varargs with dynamic output schema
DoubleSequenceFunction        - Generates a sequence of floats 0.0..n-1
GeneratorExceptionFunction    - Demonstrates exception handling
LoggingGeneratorFunction      - Demonstrates log message emission
PartitionedSequenceFunction   - Demonstrates multi-worker parallel execution
ProjectedDataFunction         - Demonstrates projection pushdown
SequenceFunction              - Generates a sequence of integers 0..n-1
SettingsAwareFunction         - Demonstrates settings-aware output schema
TenThousandFunction           - Generates 10000 integers 0..9999 (no args)
"""

from __future__ import annotations

import struct
from dataclasses import dataclass, field
from typing import Annotated, Any, ClassVar

import numpy as np
import pyarrow as pa
from vgi_rpc.log import Level
from vgi_rpc.rpc import OutputCollector

from vgi.arguments import Arg, Setting
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

__all__ = [
    "ConstantColumnsFunction",
    "DoubleSequenceFunction",
    "GeneratorExceptionFunction",
    "LoggingGeneratorFunction",
    "PartitionedSequenceFunction",
    "NestedSequenceFunction",
    "ProjectedDataFunction",
    "SequenceFunction",
    "SettingsAwareFunction",
    "TenThousandFunction",
]


def _cardinality_from_count[T: TableFunctionGenerator[Any, Any]](cls: type[T]) -> type[T]:
    """Class decorator to implement cardinality() based on a 'count' argument."""
    if "cardinality" not in cls.__dict__:  # only inject if subclass hasn't overridden

        def cardinality_impl(cls_: type[T], params: BindParams[Any]) -> TableCardinality:
            count = getattr(params.args, "count", None)
            if not isinstance(count, int) or count < 0:
                raise ValueError(f"Expected a non-negative integer 'count' argument for {cls_.__name__}")
            return TableCardinality(estimate=count, max=count)

        cls.cardinality = classmethod(cardinality_impl)  # type: ignore[assignment]

    return cls


@dataclass(slots=True, frozen=True)
class SequenceFunctionArgs:
    """Arguments for SequenceFunction."""

    count: Annotated[int, Arg(0, doc="Number of integers to generate", ge=0)]
    batch_size: Annotated[int, Arg(1, default=1000, doc="Batch size for output", ge=1)]
    increment: Annotated[int, Arg("increment", default=1, doc="Step between values", ge=1)]


@dataclass
class SequenceState:
    """Mutable state for SequenceFunction."""

    remaining: int
    current_index: int = 0


@init_single_worker
@bind_fixed_schema
@_cardinality_from_count
class SequenceFunction(TableFunctionGenerator[SequenceFunctionArgs, SequenceState]):
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

    SELECT * FROM sequence(5, increment=2)
    Returns: [{"n": 0}, {"n": 2}, {"n": 4}, {"n": 6}, {"n": 8}]

    SELECT * FROM sequence(1000, 100)
    Returns: integers 0-999 in batches of 100 rows each

    """

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
                sql="SELECT * FROM sequence(1000, 100)",
                description="Generate integers 0-999 in batches of 100",
            ),
            FunctionExample(
                sql="SELECT * FROM sequence(5, 10000, increment := 10)",
                description="Generate 0, 10, 20, 30, 40",
            ),
        ]

    # Full schema before projection
    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema([pa.field("n", pa.int64())])

    @classmethod
    def initial_state(cls, params: ProcessParams[SequenceFunctionArgs]) -> SequenceState:
        """Create initial state with remaining count."""
        return SequenceState(remaining=params.args.count)

    @classmethod
    def process(cls, params: ProcessParams[SequenceFunctionArgs], state: SequenceState, out: OutputCollector) -> None:
        """Generate the next batch of the sequence."""
        if state.remaining <= 0:
            out.finish()
            return

        size = min(state.remaining, params.args.batch_size)
        values = np.arange(
            state.current_index * params.args.increment,
            (state.current_index + size) * params.args.increment,
            params.args.increment,
            dtype=np.int64,
        )

        out.emit(
            pa.RecordBatch.from_pydict(
                {"n": values},
                schema=params.output_schema,
            )
        )

        state.current_index += size
        state.remaining -= size


@dataclass(slots=True, frozen=True)
class NestedSequenceFunctionArguments:
    count: Annotated[int, Arg(0, doc="Number of rows to generate", ge=0)]
    batch_size: Annotated[int, Arg(1, default=1000, doc="Batch size for output", ge=1)]
    history_size: Annotated[int, Arg("history_size", default=20, doc="Max items in history list", ge=1)]


@dataclass
class NestedSequenceState:
    """Mutable state for NestedSequenceFunction."""

    remaining: int
    current_index: int = 0


@init_single_worker
@bind_fixed_schema
@_cardinality_from_count
class NestedSequenceFunction(TableFunctionGenerator[NestedSequenceFunctionArguments, NestedSequenceState]):
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
    def initial_state(cls, params: ProcessParams[NestedSequenceFunctionArguments]) -> NestedSequenceState:
        """Create initial state with remaining count."""
        return NestedSequenceState(remaining=params.args.count)

    @classmethod
    def process(
        cls,
        params: ProcessParams[NestedSequenceFunctionArguments],
        state: NestedSequenceState,
        out: OutputCollector,
    ) -> None:
        """Generate the next batch of the nested sequence."""
        if state.remaining <= 0:
            out.finish()
            return

        size = min(state.remaining, params.args.batch_size)
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
    batch_size: Annotated[int, Arg(1, default=1000, doc="Batch size for output", ge=1)]
    increment: Annotated[float, Arg("increment", default=1.0, doc="Step between values", gt=0.0)]


@dataclass
class DoubleSequenceState:
    """Mutable state for DoubleSequenceFunction."""

    remaining: int
    current_index: int = 0


@init_single_worker
@bind_fixed_schema
@_cardinality_from_count
class DoubleSequenceFunction(TableFunctionGenerator[DoubleSequenceFunctionArguments, DoubleSequenceState]):
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

    SELECT * FROM double_sequence(5, increment=0.5)
    Returns: [{"n": 0.0}, {"n": 0.5}, {"n": 1.0}, {"n": 1.5}, {"n": 2.0}]

    SELECT * FROM double_sequence(1000, 100)
    Returns: floats 0.0-999.0 in batches of 100 rows each

    """

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
                sql="SELECT * FROM double_sequence(1000, 100)",
                description="Generate floats 0.0-999.0 in batches of 100",
            ),
            FunctionExample(
                sql="SELECT * FROM double_sequence(5, increment=0.5)",
                description="Generate 0.0, 0.5, 1.0, 1.5, 2.0",
            ),
        ]

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema([pa.field("n", pa.float64())])

    @classmethod
    def initial_state(cls, params: ProcessParams[DoubleSequenceFunctionArguments]) -> DoubleSequenceState:
        """Create initial state with remaining count."""
        return DoubleSequenceState(remaining=params.args.count)

    @classmethod
    def process(
        cls,
        params: ProcessParams[DoubleSequenceFunctionArguments],
        state: DoubleSequenceState,
        out: OutputCollector,
    ) -> None:
        """Generate the next batch of the sequence."""
        if state.remaining <= 0:
            out.finish()
            return

        size = min(state.remaining, params.args.batch_size)
        values = np.arange(
            state.current_index * params.args.increment,
            (state.current_index + size) * params.args.increment,
            params.args.increment,
            dtype=np.float64,
        )

        out.emit(pa.RecordBatch.from_pydict({"n": values}, schema=params.output_schema))

        state.current_index += size
        state.remaining -= size


@dataclass(slots=True, frozen=True)
class GeneratorExceptionFunctionArguments:
    """Arguments for GeneratorExceptionFunction."""

    fail_after: Annotated[int, Arg(0, doc="Number of batches before failure", ge=0)]


@dataclass
class GeneratorExceptionState:
    """Mutable state for GeneratorExceptionFunction."""

    batch_count: int = 0


@init_single_worker
@bind_fixed_schema
class GeneratorExceptionFunction(TableFunctionGenerator[GeneratorExceptionFunctionArguments, GeneratorExceptionState]):
    """Function that raises an exception after generating some output.

    USE CASE
    --------
    Testing exception handling in the generator protocol.

    SCHEMA
    ------
    Output: {"n": int64}

    """

    class Meta:
        """Metadata for GeneratorExceptionFunction."""

        name = "generator_exception"
        description = "Raises an exception after N batches for testing"
        categories = ["testing"]
        tags = {"category": "testing", "type": "error-handling"}

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema([pa.field("n", pa.int64())])

    @classmethod
    def initial_state(cls, params: ProcessParams[GeneratorExceptionFunctionArguments]) -> GeneratorExceptionState:
        """Create initial state."""
        return GeneratorExceptionState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[GeneratorExceptionFunctionArguments],
        state: GeneratorExceptionState,
        out: OutputCollector,
    ) -> None:
        """Generate batches then raise an exception."""
        if state.batch_count >= params.args.fail_after:
            raise ValueError(f"Intentional failure after {params.args.fail_after} batches")

        out.emit(pa.RecordBatch.from_pydict({"n": [state.batch_count]}, schema=params.output_schema))
        state.batch_count += 1


@dataclass(slots=True, frozen=True)
class LoggingGeneratorFunctionArguments:
    """Arguments for LoggingGeneratorFunction."""

    count: Annotated[int, Arg(0, doc="Number of values to generate", ge=0)]


@dataclass
class LoggingGeneratorState:
    """Mutable state for LoggingGeneratorFunction."""

    index: int = 0


@init_single_worker
@bind_fixed_schema
class LoggingGeneratorFunction(TableFunctionGenerator[LoggingGeneratorFunctionArguments, LoggingGeneratorState]):
    """Function that emits log messages during generation.

    USE CASE
    --------
    Testing log message handling in the generator protocol.

    SCHEMA
    ------
    Output: {"n": int64}

    """

    class Meta:
        """Metadata for LoggingGeneratorFunction."""

        name = "logging_generator"
        description = "Emits log messages during generation"
        categories = ["testing"]

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema([pa.field("n", pa.int64())])

    @classmethod
    def initial_state(cls, params: ProcessParams[LoggingGeneratorFunctionArguments]) -> LoggingGeneratorState:
        """Create initial state."""
        return LoggingGeneratorState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[LoggingGeneratorFunctionArguments],
        state: LoggingGeneratorState,
        out: OutputCollector,
    ) -> None:
        """Generate values with logging."""
        if state.index == 0:
            out.client_log(Level.INFO, f"Starting generation of {params.args.count} values")

        if state.index >= params.args.count:
            out.client_log(Level.INFO, "Generation complete")
            out.finish()
            return

        out.emit(pa.RecordBatch.from_pydict({"n": [state.index]}, schema=params.output_schema))
        state.index += 1


@dataclass(slots=True, frozen=True)
class PartitionedSequenceFunctionArguments:
    """Arguments for PartitionedSequenceFunction."""

    count: Annotated[int, Arg(0, doc="Total number of integers to generate", ge=0)]
    increment: Annotated[int, Arg("increment", default=1, doc="Step between values", ge=1)]


@dataclass
class PartitionedSequenceState:
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
                sql="SELECT * FROM partitioned_sequence(5, increment=10)",
                description="Generate 0, 10, 20, 30, 40 in parallel",
            ),
        ]

    # Size of each work chunk in the queue
    CHUNK_SIZE: ClassVar[int] = 1000
    # Batch size for output within each chunk
    BATCH_SIZE: ClassVar[int] = 1000

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema([pa.field("n", pa.int64())])

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
            state.current_idx = state.current_start

        batch_end_idx = min(state.current_idx + cls.BATCH_SIZE, state.current_end or 0)
        values = [idx * params.args.increment for idx in range(state.current_idx, batch_end_idx)]

        out.emit(pa.RecordBatch.from_pydict({"n": values}, schema=params.output_schema))

        state.current_idx = batch_end_idx


@dataclass(slots=True, frozen=True)
class ProjectedDataFunctionArguments:
    """Arguments for ProjectedDataFunction."""

    count: Annotated[int, Arg(0, doc="Number of rows to generate", ge=0)]


@dataclass
class ProjectedDataState:
    """Mutable state for ProjectedDataFunction."""

    remaining: int
    current_id: int = 0


@init_single_worker
@bind_fixed_schema
@_cardinality_from_count
class ProjectedDataFunction(TableFunctionGenerator[ProjectedDataFunctionArguments, ProjectedDataState]):
    """Generates data with 4 columns, supporting projection pushdown.

    USE CASE
    --------
    Demonstrates projection pushdown where the function only computes
    columns that are actually requested. This is useful for expensive
    column computations that can be skipped if the column isn't needed.

    SCHEMA
    ------
    Full output: {"id": int64, "name": string, "value": float64, "extra": int64}
    With projection, only the projected columns are included.

    Example:
    -------
    SELECT id, value FROM projected_data(10)  -- Only computes id and value
    Returns: 10 rows with id and value columns only

    """

    class Meta:
        """Metadata for ProjectedDataFunction."""

        name = "projected_data"
        description = "Generates data with 4 columns, supporting projection pushdown"
        categories = ["generator", "utility"]
        projection_pushdown = True
        examples = [
            FunctionExample(
                sql="SELECT * FROM projected_data(10)",
                description="Generate 10 rows with all 4 columns",
            ),
            FunctionExample(
                sql="SELECT id, value FROM projected_data(10)",
                description="Generate 10 rows with only id and value columns",
            ),
        ]

    # Full schema with all 4 columns
    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(
        {
            "id": pa.int64(),
            "name": pa.string(),
            "value": pa.float64(),
            "extra": pa.int64(),
        }
    )

    BATCH_SIZE: ClassVar[int] = 1000

    @classmethod
    def _get_projected_column_indices(cls, projection_ids: list[int] | None) -> list[int]:
        """Get the column indices to generate.

        Returns indices from projection_ids if set, otherwise all columns.
        """
        if projection_ids is not None:
            return projection_ids
        return list(range(len(cls.FIXED_SCHEMA)))

    @classmethod
    def initial_state(cls, params: ProcessParams[ProjectedDataFunctionArguments]) -> ProjectedDataState:
        """Create initial state with remaining count."""
        return ProjectedDataState(remaining=params.args.count)

    @classmethod
    def process(
        cls,
        params: ProcessParams[ProjectedDataFunctionArguments],
        state: ProjectedDataState,
        out: OutputCollector,
    ) -> None:
        """Generate data for only the projected columns."""
        if state.remaining <= 0:
            out.finish()
            return

        projected_indices = cls._get_projected_column_indices(params.init_call.projection_ids)
        batch_size = min(state.remaining, cls.BATCH_SIZE)

        # Only compute columns that are projected
        columns: dict[str, list[int] | list[str] | list[float]] = {}

        for idx in projected_indices:
            f = cls.FIXED_SCHEMA.field(idx)
            if f.name == "id":
                columns["id"] = list(range(state.current_id, state.current_id + batch_size))
            elif f.name == "name":
                columns["name"] = [f"item_{i}" for i in range(state.current_id, state.current_id + batch_size)]
            elif f.name == "value":
                columns["value"] = [float(i) * 1.5 for i in range(state.current_id, state.current_id + batch_size)]
            elif f.name == "extra":
                columns["extra"] = [i * i for i in range(state.current_id, state.current_id + batch_size)]

        out.emit(pa.RecordBatch.from_pydict(columns, schema=params.output_schema))

        state.current_id += batch_size
        state.remaining -= batch_size


@dataclass(slots=True, frozen=True)
class SettingsAwareFunctionArguments:
    """Arguments for SettingsAwareFunction."""

    count: Annotated[int, Arg(0, doc="Number of rows to generate", ge=0)]


@dataclass
class SettingsAwareState:
    """Mutable state for SettingsAwareFunction."""

    remaining: int
    current_id: int = 0


@init_single_worker
@_cardinality_from_count
class SettingsAwareFunction(TableFunctionGenerator[SettingsAwareFunctionArguments, SettingsAwareState]):
    """Generates data demonstrating that settings are passed to functions.

    USE CASE
    --------
    Demonstrates how functions can declare required settings via
    Meta.required_settings and access them via self.settings or
    self.get_setting(). The output includes columns showing the actual
    setting values that were passed.

    This function uses three settings:
    - vgi_verbose_mode: bool - when true, adds a details column
    - greeting: str - a custom greeting message echoed in output
    - multiplier: int - multiplies the value column

    SCHEMA
    ------
    Base output: {"id": int64, "greeting": string, "value": float64}
    With vgi_verbose_mode="true": adds "details": string column

    Example:
    -------
    With settings={"vgi_verbose_mode": "true", "greeting": "Hi", "multiplier": "2"}:
    Returns: [{"id": 0, "greeting": "Hi", "value": 0.0, "details": "row_0"}, ...]

    """

    class Meta:
        """Metadata for SettingsAwareFunction."""

        name = "settings_aware"
        description = "Generates data demonstrating settings are passed"
        categories = ["generator", "settings"]
        examples = [
            FunctionExample(
                sql="SELECT * FROM settings_aware(5)",
                description="Generate 5 rows showing setting values",
            )
        ]

    BATCH_SIZE: ClassVar[int] = 1000

    @classmethod
    def on_bind(
        cls,
        params: BindParams[SettingsAwareFunctionArguments],
        *,
        vgi_verbose_mode: Annotated[pa.Scalar[Any] | None, Setting()] = None,
        greeting: Annotated[pa.Scalar[Any] | None, Setting()] = None,
        multiplier: Annotated[pa.Scalar[Any] | None, Setting()] = None,
    ) -> BindResponse:
        """Return output schema based on vgi_verbose_mode setting.

        Always includes id, greeting (from setting), and value (multiplied).
        When vgi_verbose_mode is "true", includes an extra "details" column.
        """
        fields: list[pa.Field[pa.DataType]] = [
            pa.field("id", pa.int64()),
            pa.field("greeting", pa.string()),
            pa.field("value", pa.float64()),
        ]

        # Add details column if verbose mode is enabled
        verbose_value = vgi_verbose_mode.as_py() if vgi_verbose_mode is not None else "false"
        if verbose_value == "true":
            fields.append(pa.field("details", pa.string()))

        return BindResponse(output_schema=pa.schema(fields))

    @classmethod
    def initial_state(cls, params: ProcessParams[SettingsAwareFunctionArguments]) -> SettingsAwareState:
        """Create initial state with remaining count."""
        return SettingsAwareState(remaining=params.args.count)

    @classmethod
    def process(
        cls,
        params: ProcessParams[SettingsAwareFunctionArguments],
        state: SettingsAwareState,
        out: OutputCollector,
    ) -> None:
        """Generate data based on settings."""
        if state.remaining <= 0:
            out.finish()
            return

        verbose = params.settings.get("vgi_verbose_mode", pa.scalar("false")).as_py() == "true"
        greeting = params.settings.get("greeting", pa.scalar("Hello")).as_py()
        multiplier_str = params.settings.get("multiplier", pa.scalar("1")).as_py()
        multiplier = int(multiplier_str)

        size = min(state.remaining, cls.BATCH_SIZE)
        ids = list(range(state.current_id, state.current_id + size))

        data: dict[str, list[int] | list[float] | list[str]] = {
            "id": ids,
            "greeting": [greeting] * size,
            "value": [float(i) * 2.5 * multiplier for i in ids],
        }

        if verbose:
            data["details"] = [f"row_{i}" for i in ids]

        out.emit(pa.RecordBatch.from_pydict(data, schema=params.output_schema))

        state.current_id += size
        state.remaining -= size


@dataclass(slots=True, frozen=True)
class TenThousandFunctionArguments:
    """Arguments for TenThousandFunction."""


@dataclass
class TenThousandState:
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

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema([pa.field("n", pa.int64())])

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
class ConstantColumnsFunctionArguments:
    """Arguments for ConstantColumnsFunction."""

    count: Annotated[int, Arg(0, doc="Number of rows to generate", ge=0)]
    values: Annotated[
        tuple[Any, ...],
        Arg(
            1,
            varargs=True,
            doc="Values to fill each column (at least one required)",
            arrow_type=pa.null(),  # Type is dynamic based on actual values provided
        ),
    ]


@dataclass
class ConstantColumnsState:
    """Mutable state for ConstantColumnsFunction."""

    remaining: int
    full_batch: pa.RecordBatch = field(repr=False, default=None)  # type: ignore[assignment]


@init_single_worker
@_cardinality_from_count
class ConstantColumnsFunction(TableFunctionGenerator[ConstantColumnsFunctionArguments, ConstantColumnsState]):
    """Generates a table with constant values in each column based on varargs.

    USE CASE
    --------
    Demonstrates varargs with AnyArrow type where the output schema is
    determined by the types of the values provided. Each vararg value
    becomes a column filled with that constant value for all rows.

    This shows how varargs can accept mixed types and produce a dynamic
    output schema based on the argument types.

    SCHEMA
    ------
    Output schema is dynamic based on the types of provided values.
    Column names are auto-generated as col_0, col_1, col_2, etc.

    Example: constant_columns(3, 42, 'hello', 3.14)
    Output schema: {"col_0": int64, "col_1": string, "col_2": double}

    Example:
    -------
    SELECT * FROM constant_columns(3, 42, 'hello')
    Returns: [{"col_0": 42, "col_1": "hello"},
              {"col_0": 42, "col_1": "hello"},
              {"col_0": 42, "col_1": "hello"}]

    SELECT * FROM constant_columns(2, 1, 2, 3, 'apple')
    Returns: [{"col_0": 1, "col_1": 2, "col_2": 3, "col_3": "apple"},
              {"col_0": 1, "col_1": 2, "col_2": 3, "col_3": "apple"}]

    """

    class Meta:
        """Metadata for ConstantColumnsFunction."""

        name = "constant_columns"
        description = "Generates rows with constant values from varargs"
        categories = ["generator", "utility"]
        examples = [
            FunctionExample(
                sql="SELECT * FROM constant_columns(5, 42, 'hello')",
                description="Generate 5 rows with columns containing 42 and 'hello'",
            ),
            FunctionExample(
                sql="SELECT * FROM constant_columns(3, 1, 2, 3, 'test')",
                description="Generate 3 rows with 4 columns of mixed types",
            ),
        ]

    BATCH_SIZE: ClassVar[int] = 2048

    @classmethod
    def on_bind(cls, params: BindParams[ConstantColumnsFunctionArguments]) -> BindResponse:
        """Return output schema with one column per vararg, typed by value."""
        return BindResponse(output_schema=schema({f"col_{i}": v.type for i, v in enumerate(params.args.values)}))

    @classmethod
    def initial_state(cls, params: ProcessParams[ConstantColumnsFunctionArguments]) -> ConstantColumnsState:
        """Create initial state with pre-built full batch."""
        arrays = [pa.repeat(scalar, cls.BATCH_SIZE) for scalar in params.args.values]
        full_batch = pa.RecordBatch.from_arrays(arrays, schema=params.output_schema)
        return ConstantColumnsState(remaining=params.args.count, full_batch=full_batch)

    @classmethod
    def process(
        cls,
        params: ProcessParams[ConstantColumnsFunctionArguments],
        state: ConstantColumnsState,
        out: OutputCollector,
    ) -> None:
        """Generate rows with constant values in each column."""
        if state.remaining <= 0:
            out.finish()
            return

        if state.remaining >= cls.BATCH_SIZE:
            out.emit(state.full_batch)
            state.remaining -= cls.BATCH_SIZE
        else:
            out.emit(state.full_batch.slice(0, state.remaining))
            state.remaining = 0
