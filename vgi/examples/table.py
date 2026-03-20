"""Example table function implementations using TableFunctionGenerator.

This module contains table functions that generate output without receiving input.
Each function demonstrates different patterns for generating data.

AVAILABLE FUNCTIONS
-------------------
ConstantColumnsFunction       - Demonstrates varargs with dynamic output schema
DoubleSequenceFunction        - Generates a sequence of floats 0.0..n-1
FilterEchoFunction            - Echoes pushed-down filter predicates in output
GeneratorExceptionFunction    - Demonstrates exception handling
LoggingGeneratorFunction      - Demonstrates log message emission
NamedParamsEchoFunction       - Echoes named parameter values in output columns
NestedSequenceFunction        - Generates a sequence with nested struct/list columns
PartitionedSequenceFunction   - Demonstrates multi-worker parallel execution
ProjectedDataFunction         - Demonstrates projection pushdown
SequenceFunction              - Generates a sequence of integers 0..n-1
SettingsAwareFunction         - Demonstrates settings-aware output schema
StructSettingsFunction        - Demonstrates struct settings for sequence config
TenThousandFunction           - Generates 10000 integers 0..9999 (no args)
"""

from __future__ import annotations

import struct
from dataclasses import dataclass, field
from typing import Annotated, Any, ClassVar

import numpy as np
import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass, Transient
from vgi_rpc.log import Level
from vgi_rpc.rpc import OutputCollector

from vgi.arguments import Arg, Secret, Setting
from vgi.invocation import BindResponse, GlobalInitResponse
from vgi.metadata import FunctionExample
from vgi.schema_utils import schema
from vgi.table_filter_pushdown import PushdownFilters
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
    "DepartmentsScanFunction",
    "DoubleSequenceFunction",
    "EmployeesScanFunction",
    "FilterEchoFunction",
    "GeneratorExceptionFunction",
    "LoggingGeneratorFunction",
    "MakeSeriesCountFunction",
    "MakeSeriesCsvFunction",
    "MakeSeriesFloatFunction",
    "MakeSeriesRangeFunction",
    "MakeSeriesStepFunction",
    "MakePairsIntFunction",
    "MakePairsIntStrFunction",
    "MakePairsStrFunction",
    "NamedParamsEchoFunction",
    "NestedSequenceFunction",
    "PartitionedSequenceFunction",
    "ProjectedDataFunction",
    "ProjectsScanFunction",
    "RepeatValueIntFunction",
    "RepeatValueStrFunction",
    "RowIdSequenceFunction",
    "ScopedSecretDemoFunction",
    "SecretDemoFunction",
    "SequenceFunction",
    "SettingsAwareFunction",
    "StructSettingsFunction",
    "TenThousandFunction",
    "VersionedConstraintsScanFunction",
    "VersionedDataFunction",
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
    batch_size: Annotated[int, Arg("batch_size", default=1000, doc="Batch size for output", ge=1)]
    increment: Annotated[int, Arg("increment", default=1, doc="Step between values", ge=1)]


@dataclass(kw_only=True)
class CountdownState(ArrowSerializableDataclass):
    """Mutable state tracking remaining rows and current position."""

    remaining: int
    current_index: int = 0


class _BaseSequenceFunction(TableFunctionGenerator[Any, CountdownState]):
    """Shared logic for SequenceFunction and DoubleSequenceFunction.

    Subclasses provide NUMPY_DTYPE and FIXED_SCHEMA as class variables.
    The args class must have count, batch_size, and increment attributes.
    """

    NUMPY_DTYPE: ClassVar[type[np.generic]]

    @classmethod
    def initial_state(cls, params: ProcessParams[Any]) -> CountdownState:
        """Create initial state with remaining count."""
        return CountdownState(remaining=params.args.count)

    @classmethod
    def process(cls, params: ProcessParams[Any], state: CountdownState, out: OutputCollector) -> None:
        """Generate the next batch of the sequence."""
        if state.remaining <= 0:
            out.finish()
            return

        size = min(state.remaining, params.args.batch_size)
        values = np.arange(
            state.current_index * params.args.increment,
            (state.current_index + size) * params.args.increment,
            params.args.increment,
            dtype=cls.NUMPY_DTYPE,
        )

        out.emit(pa.RecordBatch.from_arrays([pa.array(values)], schema=params.output_schema))

        state.current_index += size
        state.remaining -= size


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
    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema([pa.field("n", pa.int64())])
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

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema([pa.field("n", pa.float64())])
    NUMPY_DTYPE: ClassVar[type[np.generic]] = np.float64


@dataclass(slots=True, frozen=True)
class GeneratorExceptionFunctionArguments:
    """Arguments for GeneratorExceptionFunction."""

    fail_after: Annotated[int, Arg(0, doc="Number of batches before failure", ge=0)]


@dataclass(kw_only=True)
class GeneratorExceptionState(ArrowSerializableDataclass):
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


@dataclass(kw_only=True)
class LoggingGeneratorState(ArrowSerializableDataclass):
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
            assert state.current_start is not None
            state.current_idx = state.current_start

        batch_end_idx = min(state.current_idx + cls.BATCH_SIZE, state.current_end or 0)
        values = [idx * params.args.increment for idx in range(state.current_idx, batch_end_idx)]

        out.emit(pa.RecordBatch.from_pydict({"n": values}, schema=params.output_schema))

        state.current_idx = batch_end_idx


@dataclass(slots=True, frozen=True)
class ProjectedDataFunctionArguments:
    """Arguments for ProjectedDataFunction."""

    count: Annotated[int, Arg(0, doc="Number of rows to generate", ge=0)]


@init_single_worker
@bind_fixed_schema
@_cardinality_from_count
class ProjectedDataFunction(TableFunctionGenerator[ProjectedDataFunctionArguments, CountdownState]):
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
    def initial_state(cls, params: ProcessParams[ProjectedDataFunctionArguments]) -> CountdownState:
        """Create initial state with remaining count."""
        return CountdownState(remaining=params.args.count)

    @classmethod
    def process(
        cls,
        params: ProcessParams[ProjectedDataFunctionArguments],
        state: CountdownState,
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
                columns["id"] = list(range(state.current_index, state.current_index + batch_size))
            elif f.name == "name":
                columns["name"] = [f"item_{i}" for i in range(state.current_index, state.current_index + batch_size)]
            elif f.name == "value":
                columns["value"] = [
                    float(i) * 1.5 for i in range(state.current_index, state.current_index + batch_size)
                ]
            elif f.name == "extra":
                columns["extra"] = [i * i for i in range(state.current_index, state.current_index + batch_size)]

        out.emit(pa.RecordBatch.from_pydict(columns, schema=params.output_schema))

        state.current_index += batch_size
        state.remaining -= batch_size


@dataclass(slots=True, frozen=True)
class SettingsAwareFunctionArguments:
    """Arguments for SettingsAwareFunction."""

    count: Annotated[int, Arg(0, doc="Number of rows to generate", ge=0)]


@dataclass(kw_only=True)
class SettingsAwareState(ArrowSerializableDataclass):
    """Mutable state for SettingsAwareFunction with typed settings."""

    remaining: int
    current_index: int = 0
    verbose: bool = False
    greeting: str = "Hello"
    multiplier: int = 1


@init_single_worker
@_cardinality_from_count
class SettingsAwareFunction(TableFunctionGenerator[SettingsAwareFunctionArguments, SettingsAwareState]):
    """Generates data demonstrating that settings are passed to functions.

    USE CASE
    --------
    Demonstrates how functions can declare required settings via
    Setting() annotations and access them via state (resolved once
    in initial_state()). The output includes columns showing the actual
    setting values that were passed.

    This function uses three settings:
    - vgi_verbose_mode: bool - when true, adds a details column
    - greeting: str - a custom greeting message echoed in output
    - multiplier: int - multiplies the value column

    Settings are typed: the C++ extension sends Arrow scalars with proper
    types (bool, int64, string). For backward compatibility, string values
    like "true" are also accepted for vgi_verbose_mode.

    SCHEMA
    ------
    Base output: {"id": int64, "greeting": string, "value": float64}
    With vgi_verbose_mode=true: adds "details": string column

    Example:
    -------
    With settings={vgi_verbose_mode: true, greeting: "Hi", multiplier: 2}:
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

    @staticmethod
    def _is_verbose(val: object) -> bool:
        """Check if verbose mode is enabled, handling both bool and string values."""
        return val is True or val == "true"

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
        When vgi_verbose_mode is true, includes an extra "details" column.
        """
        fields: list[pa.Field[pa.DataType]] = [
            pa.field("id", pa.int64()),
            pa.field("greeting", pa.string()),
            pa.field("value", pa.float64()),
        ]

        # Add details column if verbose mode is enabled (handles bool and string)
        if vgi_verbose_mode is not None and cls._is_verbose(vgi_verbose_mode.as_py()):
            fields.append(pa.field("details", pa.string()))

        return BindResponse(output_schema=pa.schema(fields))

    @classmethod
    def initial_state(cls, params: ProcessParams[SettingsAwareFunctionArguments]) -> SettingsAwareState:
        """Create initial state with typed settings resolved once."""
        verbose_val = params.settings.get("vgi_verbose_mode", pa.scalar(False)).as_py()
        greeting_val = params.settings.get("greeting", pa.scalar("Hello")).as_py()
        multiplier_val = params.settings.get("multiplier", pa.scalar(1)).as_py()

        return SettingsAwareState(
            remaining=params.args.count,
            verbose=cls._is_verbose(verbose_val),
            greeting=str(greeting_val),
            multiplier=int(multiplier_val),
        )

    @classmethod
    def process(
        cls,
        params: ProcessParams[SettingsAwareFunctionArguments],
        state: SettingsAwareState,
        out: OutputCollector,
    ) -> None:
        """Generate data based on settings stored in state."""
        if state.remaining <= 0:
            out.finish()
            return

        size = min(state.remaining, cls.BATCH_SIZE)
        ids = list(range(state.current_index, state.current_index + size))

        data: dict[str, list[int] | list[float] | list[str]] = {
            "id": ids,
            "greeting": [state.greeting] * size,
            "value": [float(i) * 2.5 * state.multiplier for i in ids],
        }

        if state.verbose:
            data["details"] = [f"row_{i}" for i in ids]

        out.emit(pa.RecordBatch.from_pydict(data, schema=params.output_schema))

        state.current_index += size
        state.remaining -= size


@dataclass(slots=True, frozen=True)
class StructSettingsFunctionArguments:
    """Arguments for StructSettingsFunction."""

    count: Annotated[int, Arg(0, doc="Number of rows to generate", ge=0)]


@dataclass(kw_only=True)
class StructSettingsState(ArrowSerializableDataclass):
    """Mutable state for StructSettingsFunction."""

    remaining: int
    current_index: int = 0
    start: int = 0
    step: int = 1
    label: str = "item"


@init_single_worker
@_cardinality_from_count
class StructSettingsFunction(TableFunctionGenerator[StructSettingsFunctionArguments, StructSettingsState]):
    """Generates a sequence configured by a struct setting.

    USE CASE
    --------
    Demonstrates how a single struct setting can configure multiple aspects
    of a function's behavior. The config setting is a struct with fields:
    - start: int64 - starting value for the sequence
    - step: int64 - step between values
    - label: string - prefix for label column

    SCHEMA
    ------
    Output: {"n": int64, "label": string}

    Example:
    -------
    With config={'start': 10, 'step': 5, 'label': 'item'} and count=3:
    Returns: [{"n": 10, "label": "item_0"}, {"n": 15, "label": "item_1"}, {"n": 20, "label": "item_2"}]

    """

    class Meta:
        """Metadata for StructSettingsFunction."""

        name = "struct_settings"
        description = "Generate a sequence configured by a struct setting"
        categories = ["generator", "settings"]
        examples = [
            FunctionExample(
                sql="SELECT * FROM struct_settings(5)",
                description="Generate 5 rows configured by the config setting",
            )
        ]

    FIXED_SCHEMA: ClassVar[pa.Schema] = schema({"n": pa.int64(), "label": pa.string()})

    @classmethod
    def on_bind(
        cls,
        params: BindParams[StructSettingsFunctionArguments],
        *,
        config: Annotated[pa.Scalar[Any] | None, Setting()] = None,
    ) -> BindResponse:
        """Return output schema. Config declared here for required_settings registration."""
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[StructSettingsFunctionArguments]) -> StructSettingsState:
        """Create initial state with struct setting values resolved once."""
        config = params.settings["config"]  # pa.StructScalar
        cfg = config.as_py()  # dict
        return StructSettingsState(
            remaining=params.args.count,
            start=cfg["start"],
            step=cfg["step"],
            label=cfg["label"],
        )

    @classmethod
    def process(
        cls,
        params: ProcessParams[StructSettingsFunctionArguments],
        state: StructSettingsState,
        out: OutputCollector,
    ) -> None:
        """Generate rows with values derived from the struct setting."""
        if state.remaining <= 0:
            out.finish()
            return

        size = min(state.remaining, 1000)
        data: dict[str, list[int] | list[str]] = {
            "n": [state.start + (state.current_index + i) * state.step for i in range(size)],
            "label": [f"{state.label}_{state.current_index + i}" for i in range(size)],
        }
        out.emit(pa.RecordBatch.from_pydict(data, schema=params.output_schema))
        state.current_index += size
        state.remaining -= size


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
        ),
    ]


@dataclass(kw_only=True)
class ConstantColumnsState(ArrowSerializableDataclass):
    """Mutable state for ConstantColumnsFunction."""

    remaining: int
    full_batch: Annotated[pa.RecordBatch | None, Transient()] = field(repr=False, default=None)


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

        if state.full_batch is None:
            arrays = [pa.repeat(scalar, cls.BATCH_SIZE) for scalar in params.args.values]
            state.full_batch = pa.RecordBatch.from_arrays(arrays, schema=params.output_schema)
        if state.remaining >= cls.BATCH_SIZE:
            out.emit(state.full_batch)
            state.remaining -= cls.BATCH_SIZE
        else:
            out.emit(state.full_batch.slice(0, state.remaining))
            state.remaining = 0


# =============================================================================
# Secret Demo Functions
# =============================================================================


@dataclass(kw_only=True)
class SecretDemoState(ArrowSerializableDataclass):
    """State for SecretDemoFunction."""

    keys: list[str] = field(default_factory=list)
    values: list[str] = field(default_factory=list)
    types: list[str] = field(default_factory=list)


@init_single_worker
class SecretDemoFunction(TableFunctionGenerator[None, SecretDemoState]):
    """Table function that outputs secret key-value pairs as rows.

    Demonstrates basic secret access via Secret() annotation.
    """

    class Meta:
        """Metadata for SecretDemoFunction."""

        name = "secret_demo"
        description = "Outputs secret contents as key-value rows"

    @classmethod
    def on_bind(
        cls,
        params: BindParams[None],
    ) -> BindResponse:
        """Bind with secret request via SecretsAccessor."""
        # Request the secret via the accessor — triggers two-phase bind
        # so the resolved secret is available in initial_state().
        params.secrets.get("vgi_example")
        return BindResponse(
            output_schema=schema(
                {
                    "key": pa.string(),
                    "value": pa.string(),
                    "arrow_type": pa.string(),
                }
            )
        )

    @classmethod
    def initial_state(cls, params: ProcessParams[None]) -> SecretDemoState:
        """Build initial state from secret key-value pairs."""
        secret = params.secrets.get("vgi_example", {})
        keys = list(secret.keys())
        values = [str(v.as_py()) for v in secret.values()]
        types = [str(v.type) for v in secret.values()]
        return SecretDemoState(keys=keys, values=values, types=types)

    @classmethod
    def process(
        cls,
        params: ProcessParams[None],
        state: SecretDemoState,
        out: OutputCollector,
    ) -> None:
        """Emit secret entries as rows."""
        if not state.keys:
            out.finish()
            return
        batch = pa.RecordBatch.from_pydict(
            {"key": state.keys, "value": state.values, "arrow_type": state.types},
            schema=params.output_schema,
        )
        out.emit(batch)
        state.keys = []
        state.values = []
        state.types = []


@dataclass(frozen=True)
class ScopedSecretDemoArgs:
    """Arguments for ScopedSecretDemoFunction."""

    path: Annotated[str, Arg(0, doc="Scope path for secret lookup")]


@dataclass(kw_only=True)
class ScopedSecretDemoState(ArrowSerializableDataclass):
    """State for ScopedSecretDemoFunction."""

    found: bool = False
    secret_keys: str = ""


@init_single_worker
class ScopedSecretDemoFunction(TableFunctionGenerator[ScopedSecretDemoArgs, ScopedSecretDemoState]):
    """Demonstrates automatic two-phase bind with scoped secrets.

    Requests a secret with a dynamic scope computed from the function argument.
    The framework automatically handles the two-phase bind retry.
    """

    class Meta:
        """Metadata for ScopedSecretDemoFunction."""

        name = "scoped_secret_demo"
        description = "Demo: resolves scoped secret based on argument"

    @classmethod
    def on_bind(
        cls,
        params: BindParams[ScopedSecretDemoArgs],
        *,
        vgi_example: Annotated[dict[str, pa.Scalar[Any]] | None, Secret("vgi_example")] = None,
    ) -> BindResponse:
        """Bind with dynamic scoped secret lookup."""
        # Request secret with dynamic scope — framework handles retry automatically.
        # The get() call registers a pending scoped lookup; the return value is
        # unused because the framework will trigger a two-phase bind retry.
        params.secrets.get("vgi_example", scope=params.args.path)

        # On first call: secret is None (pending), framework triggers retry
        # On retry: secret is dict (found) or None (genuinely not found)

        return BindResponse(
            output_schema=schema(
                {
                    "scope": pa.string(),
                    "found": pa.bool_(),
                    "secret_keys": pa.string(),
                }
            )
        )

    @classmethod
    def initial_state(cls, params: ProcessParams[ScopedSecretDemoArgs]) -> ScopedSecretDemoState:
        """Build state from resolved secrets."""
        secret = params.secrets.get("vgi_example", {})
        return ScopedSecretDemoState(
            found=bool(secret),
            secret_keys=",".join(secret.keys()) if secret else "",
        )

    @classmethod
    def process(
        cls,
        params: ProcessParams[ScopedSecretDemoArgs],
        state: ScopedSecretDemoState,
        out: OutputCollector,
    ) -> None:
        """Emit scope info and resolved secret keys."""
        batch = pa.RecordBatch.from_pydict(
            {
                "scope": [params.args.path],
                "found": [state.found],
                "secret_keys": [state.secret_keys],
            },
            schema=params.output_schema,
        )
        out.emit(batch)
        out.finish()


# =============================================================================
# FilterEchoFunction — diagnostic: echoes pushed-down filter predicates
# =============================================================================


def _format_pushed_filters(filters: PushdownFilters | None) -> str:
    """Format pushed-down filters as a human-readable SQL-like string."""
    if not filters:
        return "(none)"
    sql, params = filters.to_sql(quote_identifier=lambda s: s)
    if not sql:
        return "(none)"
    # Replace ?-placeholders positionally to avoid issues if param values contain "?"
    parts: list[str] = []
    param_iter = iter(params)
    for chunk in sql.split("?"):
        parts.append(chunk)
        try:
            p = next(param_iter)
            parts.append(repr(p) if isinstance(p, str) else str(p))
        except StopIteration:
            pass
    return "".join(parts)


@dataclass(slots=True, frozen=True)
class FilterEchoFunctionArgs:
    """Arguments for FilterEchoFunction."""

    count: Annotated[int, Arg(0, doc="Number of rows to generate", ge=0, default=10)]
    batch_size: Annotated[int, Arg("batch_size", default=2048, doc="Batch size for output", ge=1)]


@dataclass(kw_only=True)
class FilterEchoState(ArrowSerializableDataclass):
    """Mutable state tracking remaining rows, position, and cached filter string."""

    remaining: int
    current_index: int = 0
    filter_str: Annotated[str, Transient()] = "(none)"


@init_single_worker
@bind_fixed_schema
@_cardinality_from_count
class FilterEchoFunction(TableFunctionGenerator[FilterEchoFunctionArgs, FilterEchoState]):
    """Echoes pushed-down filter predicates in output for diagnostic purposes.

    USE CASE
    --------
    Verify which filters DuckDB pushes down to the VGI worker. The
    ``pushed_filters`` column shows the SQL-like representation of all
    filters the engine sent. Filters are auto-applied by the worker so
    the result set is always correct.

    SCHEMA
    ------
    Output: {"n": int64, "s": string, "pushed_filters": string}

    Example:
    -------
    SELECT * FROM filter_echo(10) WHERE n >= 8
    Returns: rows 8-9 with pushed_filters showing "n >= 8"

    """

    class Meta:
        """Metadata for FilterEchoFunction."""

        name = "filter_echo"
        description = "Echoes pushed-down filter predicates in output"
        categories = ["generator", "diagnostic"]
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        examples = [
            FunctionExample(
                sql="SELECT * FROM filter_echo(10)",
                description="Generate 10 rows showing pushed filters",
            ),
            FunctionExample(
                sql="SELECT pushed_filters FROM filter_echo(10) WHERE n >= 8",
                description="See which filters were pushed down",
            ),
        ]

    FIXED_SCHEMA: ClassVar[pa.Schema] = schema({"n": pa.int64(), "s": pa.utf8(), "pushed_filters": pa.utf8()})

    @classmethod
    def initial_state(cls, params: ProcessParams[FilterEchoFunctionArgs]) -> FilterEchoState:
        """Create initial state with remaining count and cached filter string."""
        pf = params.init_call.pushdown_filters
        filters = cls.pushdown_filters(pf) if pf is not None else None
        return FilterEchoState(
            remaining=params.args.count,
            filter_str=_format_pushed_filters(filters),
        )

    @classmethod
    def process(
        cls,
        params: ProcessParams[FilterEchoFunctionArgs],
        state: FilterEchoState,
        out: OutputCollector,
    ) -> None:
        """Generate rows with n, s, and pushed_filters columns."""
        if state.remaining <= 0:
            out.finish()
            return

        size = min(state.remaining, params.args.batch_size)
        start = state.current_index

        n_values = list(range(start, start + size))
        s_values = [f"row_{i}" for i in n_values]
        filter_values = [state.filter_str] * size

        out.emit(
            pa.RecordBatch.from_pydict(
                {"n": n_values, "s": s_values, "pushed_filters": filter_values},
                schema=params.output_schema,
            )
        )

        state.current_index += size
        state.remaining -= size


# ============================================================================
# make_series — overloaded table function (3 overloads by positional arg count)
# ============================================================================

MAKE_SERIES_SCHEMA = pa.schema([("value", pa.int64())])


@dataclass(kw_only=True)
class MakeSeriesCountArgs:
    """Arguments for MakeSeriesCountFunction."""

    count: Annotated[int, Arg(0, doc="Number of values to generate", ge=0)]


@dataclass(kw_only=True)
class MakeSeriesRangeArgs:
    """Arguments for MakeSeriesRangeFunction."""

    start: Annotated[int, Arg(0, doc="Start value (inclusive)")]
    stop: Annotated[int, Arg(1, doc="Stop value (exclusive)")]


@dataclass(kw_only=True)
class MakeSeriesStepArgs:
    """Arguments for MakeSeriesStepFunction."""

    start: Annotated[int, Arg(0, doc="Start value (inclusive)")]
    stop: Annotated[int, Arg(1, doc="Stop value (exclusive)")]
    step: Annotated[int, Arg(2, doc="Step between values", ge=1)]


@dataclass(kw_only=True)
class MakeSeriesState(ArrowSerializableDataclass):
    """Mutable state for make_series functions."""

    values: list[int]
    offset: int = 0


def _make_series_emit(state: MakeSeriesState, out: OutputCollector) -> None:
    """Shared process logic for all make_series overloads."""
    if state.offset >= len(state.values):
        out.finish()
        return
    batch_values = state.values[state.offset : state.offset + 1024]
    out.emit(pa.RecordBatch.from_pydict({"value": batch_values}, schema=MAKE_SERIES_SCHEMA))
    state.offset += len(batch_values)


@init_single_worker
@bind_fixed_schema
class MakeSeriesCountFunction(TableFunctionGenerator[MakeSeriesCountArgs, MakeSeriesState]):
    """Generate a series of integers from 0 to count-1.

    Example:
        SELECT * FROM make_series(5)
        Returns: 0, 1, 2, 3, 4

    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = MAKE_SERIES_SCHEMA

    class Meta:
        """Function metadata."""

        name = "make_series"
        description = "Generate integers from 0 to count-1"
        examples = [
            FunctionExample(
                sql="SELECT * FROM make_series(5)",
                description="Generate 0..4",
            ),
        ]

    @classmethod
    def initial_state(cls, params: ProcessParams[MakeSeriesCountArgs]) -> MakeSeriesState:
        """Build the full value list."""
        return MakeSeriesState(values=list(range(params.args.count)))

    @classmethod
    def process(cls, params: ProcessParams[MakeSeriesCountArgs], state: MakeSeriesState, out: OutputCollector) -> None:
        """Emit values in batches."""
        _make_series_emit(state, out)


@init_single_worker
@bind_fixed_schema
class MakeSeriesRangeFunction(TableFunctionGenerator[MakeSeriesRangeArgs, MakeSeriesState]):
    """Generate a series of integers from start to stop-1.

    Example:
        SELECT * FROM make_series(3, 7)
        Returns: 3, 4, 5, 6

    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = MAKE_SERIES_SCHEMA

    class Meta:
        """Function metadata."""

        name = "make_series"
        description = "Generate integers from start to stop-1"
        examples = [
            FunctionExample(
                sql="SELECT * FROM make_series(3, 7)",
                description="Generate 3..6",
            ),
        ]

    @classmethod
    def initial_state(cls, params: ProcessParams[MakeSeriesRangeArgs]) -> MakeSeriesState:
        """Build the value list from start..stop."""
        return MakeSeriesState(values=list(range(params.args.start, params.args.stop)))

    @classmethod
    def process(cls, params: ProcessParams[MakeSeriesRangeArgs], state: MakeSeriesState, out: OutputCollector) -> None:
        """Emit values in batches."""
        _make_series_emit(state, out)


@init_single_worker
@bind_fixed_schema
class MakeSeriesStepFunction(TableFunctionGenerator[MakeSeriesStepArgs, MakeSeriesState]):
    """Generate a series of integers from start to stop-1 with step.

    Example:
        SELECT * FROM make_series(0, 10, 3)
        Returns: 0, 3, 6, 9

    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = MAKE_SERIES_SCHEMA

    class Meta:
        """Function metadata."""

        name = "make_series"
        description = "Generate integers from start to stop-1 with step"
        examples = [
            FunctionExample(
                sql="SELECT * FROM make_series(0, 10, 3)",
                description="Generate 0, 3, 6, 9",
            ),
        ]

    @classmethod
    def initial_state(cls, params: ProcessParams[MakeSeriesStepArgs]) -> MakeSeriesState:
        """Build the value list with step."""
        return MakeSeriesState(values=list(range(params.args.start, params.args.stop, params.args.step)))

    @classmethod
    def process(cls, params: ProcessParams[MakeSeriesStepArgs], state: MakeSeriesState, out: OutputCollector) -> None:
        """Emit values in batches."""
        _make_series_emit(state, out)


# ============================================================================
# make_series — string overload (same 1-arg count as MakeSeriesCountFunction)
# ============================================================================


@dataclass(kw_only=True)
class MakeSeriesCsvArgs:
    """Arguments for MakeSeriesCsvFunction."""

    values: Annotated[str, Arg(0, doc="Comma-separated integers")]


@init_single_worker
@bind_fixed_schema
class MakeSeriesCsvFunction(TableFunctionGenerator[MakeSeriesCsvArgs, MakeSeriesState]):
    """Parse a CSV string of integers into rows.

    Example:
        SELECT * FROM make_series('10,20,30')
        Returns: 10, 20, 30

    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = MAKE_SERIES_SCHEMA

    class Meta:
        """Function metadata."""

        name = "make_series"
        description = "Parse comma-separated integers into rows"

    @classmethod
    def initial_state(cls, params: ProcessParams[MakeSeriesCsvArgs]) -> MakeSeriesState:
        """Parse CSV string into value list."""
        return MakeSeriesState(values=[int(x.strip()) for x in params.args.values.split(",")])

    @classmethod
    def process(cls, params: ProcessParams[MakeSeriesCsvArgs], state: MakeSeriesState, out: OutputCollector) -> None:
        """Emit values in batches."""
        _make_series_emit(state, out)


# ============================================================================
# make_series — float overload (same 1-arg count as int and string overloads)
# ============================================================================

MAKE_SERIES_FLOAT_SCHEMA = pa.schema([("value", pa.float64())])


@dataclass(kw_only=True)
class MakeSeriesFloatArgs:
    """Arguments for MakeSeriesFloatFunction."""

    step: Annotated[float, Arg(0, doc="Step size between values")]


@dataclass(kw_only=True)
class MakeSeriesFloatState(ArrowSerializableDataclass):
    """State for float make_series."""

    values: list[float] = field(default_factory=list)
    offset: int = 0


@init_single_worker
@bind_fixed_schema
class MakeSeriesFloatFunction(TableFunctionGenerator[MakeSeriesFloatArgs, MakeSeriesFloatState]):
    """Generate 10 float values: 0.0, step, 2*step, ..., 9*step.

    Example:
        SELECT * FROM make_series(0.5)
        Returns: 0.0, 0.5, 1.0, ..., 4.5

    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = MAKE_SERIES_FLOAT_SCHEMA

    class Meta:
        """Function metadata."""

        name = "make_series"
        description = "Generate 10 float values with given step size"

    @classmethod
    def initial_state(cls, params: ProcessParams[MakeSeriesFloatArgs]) -> MakeSeriesFloatState:
        """Build float value list."""
        return MakeSeriesFloatState(values=[i * params.args.step for i in range(10)])

    @classmethod
    def process(
        cls, params: ProcessParams[MakeSeriesFloatArgs], state: MakeSeriesFloatState, out: OutputCollector
    ) -> None:
        """Emit values in batches."""
        if state.offset >= len(state.values):
            out.finish()
            return
        batch_size = 1024
        end = min(state.offset + batch_size, len(state.values))
        chunk = state.values[state.offset : end]
        state.offset = end
        out.emit(pa.RecordBatch.from_pydict({"value": chunk}, schema=MAKE_SERIES_FLOAT_SCHEMA))


# ============================================================================
# make_pairs — overloaded table function (3 overloads by argument type)
# ============================================================================

MAKE_PAIRS_INT_SCHEMA = pa.schema([("a", pa.int64()), ("b", pa.int64())])
MAKE_PAIRS_STR_SCHEMA = pa.schema([("a", pa.string()), ("b", pa.string())])


@dataclass(kw_only=True)
class MakePairsIntArgs:
    """Arguments for integer make_pairs."""

    start: Annotated[int, Arg(0, doc="Start value")]
    stop: Annotated[int, Arg(1, doc="Stop value")]


@dataclass(kw_only=True)
class MakePairsStrArgs:
    """Arguments for string make_pairs."""

    prefix: Annotated[str, Arg(0, doc="Prefix for column a")]
    suffix: Annotated[str, Arg(1, doc="Suffix for column b")]


@dataclass(kw_only=True)
class MakePairsIntState(ArrowSerializableDataclass):
    """State for integer make_pairs."""

    a_vals: list[int] = field(default_factory=list)
    b_vals: list[int] = field(default_factory=list)
    done: bool = False


@dataclass(kw_only=True)
class MakePairsStrState(ArrowSerializableDataclass):
    """State for string make_pairs."""

    a_vals: list[str] = field(default_factory=list)
    b_vals: list[str] = field(default_factory=list)
    done: bool = False


@init_single_worker
@bind_fixed_schema
class MakePairsIntFunction(TableFunctionGenerator[MakePairsIntArgs, MakePairsIntState]):
    """Generate integer pairs (i, i*2) from start to stop-1.

    Example:
        SELECT * FROM make_pairs(1, 4)
        Returns: (1,2), (2,4), (3,6)

    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = MAKE_PAIRS_INT_SCHEMA

    class Meta:
        """Function metadata."""

        name = "make_pairs"
        description = "Generate integer pairs (i, i*2)"

    @classmethod
    def initial_state(cls, params: ProcessParams[MakePairsIntArgs]) -> MakePairsIntState:
        """Build integer pairs."""
        vals = list(range(params.args.start, params.args.stop))
        return MakePairsIntState(a_vals=vals, b_vals=[v * 2 for v in vals])

    @classmethod
    def process(cls, params: ProcessParams[MakePairsIntArgs], state: MakePairsIntState, out: OutputCollector) -> None:
        """Emit pairs batch."""
        if state.done:
            out.finish()
            return
        state.done = True
        out.emit(pa.RecordBatch.from_pydict({"a": state.a_vals, "b": state.b_vals}, schema=MAKE_PAIRS_INT_SCHEMA))


@init_single_worker
@bind_fixed_schema
class MakePairsStrFunction(TableFunctionGenerator[MakePairsStrArgs, MakePairsStrState]):
    """Generate string pairs (prefix+i, suffix+i) for i in 0..4.

    Example:
        SELECT * FROM make_pairs('row_', '_end')
        Returns: ('row_0','_end0'), ('row_1','_end1'), ...

    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = MAKE_PAIRS_STR_SCHEMA

    class Meta:
        """Function metadata."""

        name = "make_pairs"
        description = "Generate string pairs with prefix and suffix"

    @classmethod
    def initial_state(cls, params: ProcessParams[MakePairsStrArgs]) -> MakePairsStrState:
        """Build string pairs."""
        return MakePairsStrState(
            a_vals=[f"{params.args.prefix}{i}" for i in range(5)],
            b_vals=[f"{params.args.suffix}{i}" for i in range(5)],
        )

    @classmethod
    def process(cls, params: ProcessParams[MakePairsStrArgs], state: MakePairsStrState, out: OutputCollector) -> None:
        """Emit pairs batch."""
        if state.done:
            out.finish()
            return
        state.done = True
        out.emit(pa.RecordBatch.from_pydict({"a": state.a_vals, "b": state.b_vals}, schema=MAKE_PAIRS_STR_SCHEMA))


# ============================================================================
# make_pairs — mixed-type overload: int + str (mirrors scalar pair_type int+str)
# ============================================================================

MAKE_PAIRS_MIXED_SCHEMA = pa.schema(
    [("a", pa.int64()), ("b", pa.string())]  # type: ignore[arg-type]  # PyArrow mixed-type tuple typing
)


@dataclass(kw_only=True)
class MakePairsIntStrArgs:
    """Arguments for mixed-type make_pairs."""

    start: Annotated[int, Arg(0, doc="Start integer value")]
    label: Annotated[str, Arg(1, doc="Label prefix for string column")]


@dataclass(kw_only=True)
class MakePairsIntStrState(ArrowSerializableDataclass):
    """State for mixed-type make_pairs."""

    a_vals: list[int] = field(default_factory=list)
    b_vals: list[str] = field(default_factory=list)
    done: bool = False


@init_single_worker
@bind_fixed_schema
class MakePairsIntStrFunction(TableFunctionGenerator[MakePairsIntStrArgs, MakePairsIntStrState]):
    """Generate mixed int/string pairs (start+i, label+str(i)) for i in 0..4.

    Example:
        SELECT * FROM make_pairs(10, 'item_')
        Returns: (10, 'item_0'), (11, 'item_1'), ..., (14, 'item_4')

    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = MAKE_PAIRS_MIXED_SCHEMA

    class Meta:
        """Function metadata."""

        name = "make_pairs"
        description = "Generate mixed int/string pairs"

    @classmethod
    def initial_state(cls, params: ProcessParams[MakePairsIntStrArgs]) -> MakePairsIntStrState:
        """Build mixed-type pairs."""
        return MakePairsIntStrState(
            a_vals=[params.args.start + i for i in range(5)],
            b_vals=[f"{params.args.label}{i}" for i in range(5)],
        )

    @classmethod
    def process(
        cls, params: ProcessParams[MakePairsIntStrArgs], state: MakePairsIntStrState, out: OutputCollector
    ) -> None:
        """Emit pairs batch."""
        if state.done:
            out.finish()
            return
        state.done = True
        out.emit(pa.RecordBatch.from_pydict({"a": state.a_vals, "b": state.b_vals}, schema=MAKE_PAIRS_MIXED_SCHEMA))


# ============================================================================
# repeat_value — overloaded table function (2 overloads by varargs arg type)
# ============================================================================


@dataclass(kw_only=True)
class RepeatValueIntArgs:
    """Arguments for integer repeat_value."""

    count: Annotated[int, Arg(0, doc="Number of rows to generate")]
    values: Annotated[list[int], Arg(1, varargs=True, arrow_type=pa.int64(), doc="Integer values to repeat")]


@dataclass(kw_only=True)
class RepeatValueStrArgs:
    """Arguments for string repeat_value."""

    count: Annotated[int, Arg(0, doc="Number of rows to generate")]
    values: Annotated[list[str], Arg(1, varargs=True, arrow_type=pa.string(), doc="String values to repeat")]


@dataclass(kw_only=True)
class RepeatValueIntState(ArrowSerializableDataclass):
    """State for integer repeat_value."""

    rows: list[list[int]] = field(default_factory=list)
    done: bool = False


@dataclass(kw_only=True)
class RepeatValueStrState(ArrowSerializableDataclass):
    """State for string repeat_value."""

    rows: list[list[str]] = field(default_factory=list)
    done: bool = False


@init_single_worker
class RepeatValueIntFunction(TableFunctionGenerator[RepeatValueIntArgs, RepeatValueIntState]):
    """Repeat integer values for count rows.

    Example:
        SELECT * FROM repeat_value(3, 10, 20)
        Returns 3 rows with columns v0=10, v1=20

    """

    class Meta:
        """Function metadata."""

        name = "repeat_value"
        description = "Repeat integer values for N rows"

    @classmethod
    def on_bind(cls, params: BindParams[RepeatValueIntArgs]) -> BindResponse:
        """Build output schema from varargs count."""
        num_values = len(params.args.values)
        fields = [pa.field(f"v{i}", pa.int64()) for i in range(num_values)]
        return BindResponse(output_schema=pa.schema(fields))

    @classmethod
    def initial_state(cls, params: ProcessParams[RepeatValueIntArgs]) -> RepeatValueIntState:
        """Build repeated rows."""
        return RepeatValueIntState(
            rows=[[v] * params.args.count for v in params.args.values],
        )

    @classmethod
    def process(
        cls, params: ProcessParams[RepeatValueIntArgs], state: RepeatValueIntState, out: OutputCollector
    ) -> None:
        """Emit repeated values."""
        if state.done:
            out.finish()
            return
        state.done = True
        data = {f"v{i}": col for i, col in enumerate(state.rows)}
        schema = pa.schema([pa.field(f"v{i}", pa.int64()) for i in range(len(state.rows))])
        out.emit(pa.RecordBatch.from_pydict(data, schema=schema))


@init_single_worker
class RepeatValueStrFunction(TableFunctionGenerator[RepeatValueStrArgs, RepeatValueStrState]):
    """Repeat string values for count rows.

    Example:
        SELECT * FROM repeat_value(3, 'a', 'b')
        Returns 3 rows with columns v0='a', v1='b'

    """

    class Meta:
        """Function metadata."""

        name = "repeat_value"
        description = "Repeat string values for N rows"

    @classmethod
    def on_bind(cls, params: BindParams[RepeatValueStrArgs]) -> BindResponse:
        """Build output schema from varargs count."""
        num_values = len(params.args.values)
        fields = [pa.field(f"v{i}", pa.string()) for i in range(num_values)]
        return BindResponse(output_schema=pa.schema(fields))

    @classmethod
    def initial_state(cls, params: ProcessParams[RepeatValueStrArgs]) -> RepeatValueStrState:
        """Build repeated rows."""
        return RepeatValueStrState(
            rows=[[v] * params.args.count for v in params.args.values],
        )

    @classmethod
    def process(
        cls, params: ProcessParams[RepeatValueStrArgs], state: RepeatValueStrState, out: OutputCollector
    ) -> None:
        """Emit repeated values."""
        if state.done:
            out.finish()
            return
        state.done = True
        data = {f"v{i}": col for i, col in enumerate(state.rows)}
        schema = pa.schema([pa.field(f"v{i}", pa.string()) for i in range(len(state.rows))])
        out.emit(pa.RecordBatch.from_pydict(data, schema=schema))


# ============================================================================
# RowIdSequenceFunction - Generates rows with a row_id column
# ============================================================================


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


# ============================================================================
# VersionedDataFunction — time travel with schema evolution
# ============================================================================

# Version definitions: schema and data per version
_VERSIONED_SCHEMAS: dict[int, pa.Schema] = {
    1: pa.schema([pa.field("id", pa.int64())]),
    2: pa.schema(
        [  # type: ignore[arg-type]  # pyarrow stubs: mixed-type fields
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("score", pa.float64()),
            pa.field("active", pa.bool_()),
        ]
    ),
    3: pa.schema(
        [  # type: ignore[arg-type]  # pyarrow stubs: mixed-type fields
            pa.field("id", pa.int64()),
            pa.field("score", pa.float64()),
        ]
    ),
}

_VERSIONED_DATA: dict[int, dict[str, list[Any]]] = {
    1: {"id": [1, 2, 3]},
    2: {
        "id": [1, 2, 3, 4, 5],
        "name": ["alice", "bob", "carol", "dave", "eve"],
        "score": [10.0, 20.0, 30.0, 40.0, 50.0],
        "active": [True, False, True, False, True],
    },
    3: {"id": [1, 2, 3, 4], "score": [15.0, 25.0, 35.0, 45.0]},
}

# Current version (default when no AT clause)
_CURRENT_VERSION = 3


def resolve_version(at_unit: str | None, at_value: str | None) -> int:
    """Resolve AT clause to a version number.

    - ``VERSION``: direct integer version (must exist in ``_VERSIONED_SCHEMAS``)
    - ``TIMESTAMP``: year-based mapping (<=2020→1, <=2021→2, >=2022→3)
    - ``None``: current version (3)

    Raises ``ValueError`` for unknown versions or unsupported AT units.
    """
    if not at_unit:
        return _CURRENT_VERSION

    if at_unit.upper() == "VERSION":
        version = int(at_value)  # type: ignore[arg-type]
        if version not in _VERSIONED_SCHEMAS:
            raise ValueError(f"Unknown version: {version}. Valid versions: {sorted(_VERSIONED_SCHEMAS)}")
        return version

    if at_unit.upper() == "TIMESTAMP":
        # Parse year from timestamp string (e.g. "2020-06-15 00:00:00")
        year = int(str(at_value)[:4])
        if year < 2020:
            raise ValueError(f"No version exists at timestamp {at_value!r}: table did not exist before 2020")
        if year <= 2020:
            return 1
        if year <= 2021:
            return 2
        return 3

    raise ValueError(f"Unsupported at_unit: {at_unit!r}")


@dataclass(slots=True, frozen=True)
class VersionedDataFunctionArgs:
    """Arguments for VersionedDataFunction."""

    version: Annotated[int, Arg(0, doc="Data version to return", default=_CURRENT_VERSION)]


@dataclass(kw_only=True)
class VersionedDataState(ArrowSerializableDataclass):
    """State for VersionedDataFunction."""

    done: bool = False


@init_single_worker
class VersionedDataFunction(TableFunctionGenerator[VersionedDataFunctionArgs, VersionedDataState]):
    """Returns version-specific data demonstrating time travel with schema evolution.

    Each version has a different schema and different data:

    - **Version 1**: ``(id int64)`` — 3 rows
    - **Version 2**: ``(id int64, name string, score double, active bool)`` — 5 rows
    - **Version 3** (current): ``(id int64, score double)`` — 4 rows

    """

    class Meta:
        """Metadata for VersionedDataFunction."""

        name = "versioned_data_scan"
        description = "Returns versioned data with schema evolution"
        categories = ["generator", "testing"]

    @classmethod
    def on_bind(cls, params: BindParams[VersionedDataFunctionArgs]) -> BindResponse:
        """Return version-specific output schema."""
        version = params.args.version
        if version not in _VERSIONED_SCHEMAS:
            raise ValueError(f"Unknown version: {version}. Valid versions: {sorted(_VERSIONED_SCHEMAS)}")
        return BindResponse(output_schema=_VERSIONED_SCHEMAS[version])

    @classmethod
    def initial_state(cls, params: ProcessParams[VersionedDataFunctionArgs]) -> VersionedDataState:
        """Create initial state."""
        return VersionedDataState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[VersionedDataFunctionArgs],
        state: VersionedDataState,
        out: OutputCollector,
    ) -> None:
        """Emit all rows for the requested version in one batch."""
        if state.done:
            out.finish()
            return
        state.done = True
        version = params.args.version
        data = _VERSIONED_DATA[version]
        out.emit(pa.RecordBatch.from_pydict(data, schema=params.output_schema))


# ============================================================================
# Static data table functions for constraint testing
# ============================================================================


@dataclass(slots=True, frozen=True)
class _EmptyArgs:
    """No arguments."""


@dataclass(kw_only=True)
class _OneShotState(ArrowSerializableDataclass):
    """State that emits data once."""

    done: bool = False


def _static_scan_function(
    func_name: str,
    func_description: str,
    output_schema: pa.Schema,
    data: dict[str, list[Any]],
) -> type[TableFunctionGenerator[_EmptyArgs, _OneShotState]]:
    """Create a table function that returns static data in one batch.

    This factory eliminates boilerplate for simple scan functions that
    return a fixed dataset. Each generated class is decorated with
    ``@init_single_worker`` and has a unique ``Meta.name``.
    """

    @init_single_worker
    class StaticScanFunction(TableFunctionGenerator[_EmptyArgs, _OneShotState]):
        """Returns static data."""

        class Meta:
            """Function metadata."""

            name = func_name
            description = func_description

        @classmethod
        def on_bind(cls, params: BindParams[_EmptyArgs]) -> BindResponse:
            """Return output schema."""
            return BindResponse(output_schema=output_schema)

        @classmethod
        def initial_state(cls, params: ProcessParams[_EmptyArgs]) -> _OneShotState:
            """Create initial state."""
            return _OneShotState()

        @classmethod
        def process(
            cls,
            params: ProcessParams[_EmptyArgs],
            state: _OneShotState,
            out: OutputCollector,
        ) -> None:
            """Emit data."""
            if state.done:
                out.finish()
                return
            state.done = True
            out.emit(pa.RecordBatch.from_pydict(data, schema=params.output_schema))

    StaticScanFunction.__name__ = func_name.title().replace("_", "") + "Function"
    StaticScanFunction.__qualname__ = StaticScanFunction.__name__

    return StaticScanFunction  # type: ignore[return-value]


DepartmentsScanFunction = _static_scan_function(
    func_name="departments_scan",
    func_description="Scan departments table",
    output_schema=pa.schema(
        [  # type: ignore[arg-type]  # pyarrow stubs: mixed-type fields
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("budget", pa.float64()),
        ]
    ),
    data={
        "id": [1, 2, 3],
        "name": ["Engineering", "Sales", "HR"],
        "budget": [500000.0, 300000.0, 200000.0],
    },
)

EmployeesScanFunction = _static_scan_function(
    func_name="employees_scan",
    func_description="Scan employees table",
    output_schema=pa.schema(
        [  # type: ignore[arg-type]  # pyarrow stubs: mixed-type fields
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("email", pa.string()),
            pa.field("department_id", pa.int64()),
        ]
    ),
    data={
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
        "email": ["alice@co.com", "bob@co.com", "carol@co.com", "dave@co.com", "eve@co.com"],
        "department_id": [1, 1, 2, 2, 3],
    },
)

ProjectsScanFunction = _static_scan_function(
    func_name="projects_scan",
    func_description="Scan projects table",
    output_schema=pa.schema(
        [  # type: ignore[arg-type]  # pyarrow stubs: mixed-type fields
            pa.field("department_id", pa.int64()),
            pa.field("project_code", pa.string()),
            pa.field("title", pa.string()),
        ]
    ),
    data={
        "department_id": [1, 1, 2],
        "project_code": ["P001", "P002", "P003"],
        "title": ["Backend API", "Frontend UI", "Sales Portal"],
    },
)

ProductsScanFunction = _static_scan_function(
    func_name="products_scan",
    func_description="Scan products table",
    output_schema=pa.schema(
        [  # type: ignore[arg-type]  # pyarrow stubs: mixed-type fields
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("quantity", pa.int64()),
            pa.field("price", pa.float64()),
        ]
    ),
    data={
        "id": [1, 2, 3],
        "name": ["Widget", "Gadget", "Doohickey"],
        "quantity": [100, 50, 200],
        "price": [9.99, 24.99, 4.99],
    },
)


# ============================================================================
# VersionedConstraintsScanFunction — time travel with evolving constraints
# ============================================================================

# Version 1: simple users table (id, name) — NOT NULL on id only
# Version 2: adds email column, PK on id, UNIQUE on email
# Version 3: adds department_id column, FK to departments

_VERSIONED_CONSTRAINTS_SCHEMAS: dict[int, pa.Schema] = {
    1: pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())]),  # type: ignore[arg-type]  # pyarrow stubs: mixed-type fields
    2: pa.schema(
        [  # type: ignore[arg-type]  # pyarrow stubs: mixed-type fields
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("email", pa.string()),
        ]
    ),
    3: pa.schema(
        [  # type: ignore[arg-type]  # pyarrow stubs: mixed-type fields
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("email", pa.string()),
            pa.field("department_id", pa.int64()),
        ]
    ),
}

_VERSIONED_CONSTRAINTS_DATA: dict[int, dict[str, list[Any]]] = {
    1: {"id": [1, 2], "name": ["Alice", "Bob"]},
    2: {"id": [1, 2, 3], "name": ["Alice", "Bob", "Carol"], "email": ["a@co", "b@co", "c@co"]},
    3: {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Carol"],
        "email": ["a@co", "b@co", "c@co"],
        "department_id": [1, 2, 1],
    },
}

_VERSIONED_CONSTRAINTS_CURRENT = 3


def resolve_versioned_constraints_version(at_unit: str | None, at_value: str | None) -> int:
    """Resolve AT clause for versioned_constraints table."""
    if not at_unit:
        return _VERSIONED_CONSTRAINTS_CURRENT

    if at_unit.upper() == "VERSION":
        version = int(at_value)  # type: ignore[arg-type]
        if version not in _VERSIONED_CONSTRAINTS_SCHEMAS:
            raise ValueError(f"Unknown version: {version}. Valid versions: {sorted(_VERSIONED_CONSTRAINTS_SCHEMAS)}")
        return version

    raise ValueError(f"Unsupported at_unit: {at_unit!r}")


@dataclass(slots=True, frozen=True)
class _VersionedConstraintsArgs:
    """Arguments for VersionedConstraintsScanFunction."""

    version: Annotated[int, Arg(0, doc="Data version", default=_VERSIONED_CONSTRAINTS_CURRENT)]


@init_single_worker
class VersionedConstraintsScanFunction(
    TableFunctionGenerator[_VersionedConstraintsArgs, _OneShotState],
):
    """Returns version-specific data for constraint evolution testing."""

    class Meta:
        """Metadata for VersionedConstraintsScanFunction."""

        name = "versioned_constraints_scan"
        description = "Scan versioned constraints table"

    @classmethod
    def on_bind(cls, params: BindParams[_VersionedConstraintsArgs]) -> BindResponse:
        """Return output schema."""
        version = params.args.version
        if version not in _VERSIONED_CONSTRAINTS_SCHEMAS:
            raise ValueError(f"Unknown version: {version}")
        return BindResponse(output_schema=_VERSIONED_CONSTRAINTS_SCHEMAS[version])

    @classmethod
    def initial_state(cls, params: ProcessParams[_VersionedConstraintsArgs]) -> _OneShotState:
        """Create initial state."""
        return _OneShotState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[_VersionedConstraintsArgs],
        state: _OneShotState,
        out: OutputCollector,
    ) -> None:
        """Emit data."""
        if state.done:
            out.finish()
            return
        state.done = True
        version = params.args.version
        data = _VERSIONED_CONSTRAINTS_DATA[version]
        out.emit(pa.RecordBatch.from_pydict(data, schema=params.output_schema))
