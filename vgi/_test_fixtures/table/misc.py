"""Misc fixtures: GeneratorException, LoggingGenerator, ProjectedData, OrderEcho, SampleEcho."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, ClassVar

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass, Transient
from vgi_rpc.log import Level
from vgi_rpc.rpc import OutputCollector

from vgi._test_fixtures.table._common import (
    CountdownState,
    _cardinality_from_count,
)
from vgi.arguments import Arg
from vgi.metadata import FunctionExample
from vgi.schema_utils import schema
from vgi.table_function import (
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)


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

    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(n=pa.int64())

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

    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(n=pa.int64())

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

        assert params.init_call is not None
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


# ============================================================================


@dataclass(slots=True, frozen=True)
class _OrderEchoArgs:
    """Arguments for OrderEchoFunction."""

    count: Annotated[int, Arg(0, doc="Number of rows to generate", ge=0, default=10)]
    batch_size: Annotated[int, Arg("batch_size", default=2048, doc="Batch size for output", ge=1)]


@dataclass(kw_only=True)
class _OrderEchoState(ArrowSerializableDataclass):
    """Mutable state for OrderEchoFunction."""

    remaining: int
    current_index: int = 0
    order_column: Annotated[str, Transient()] = "(none)"
    order_direction: Annotated[str, Transient()] = "(none)"
    order_null_order: Annotated[str, Transient()] = "(none)"
    order_limit: Annotated[int, Transient()] = -1


_ORDER_ECHO_SCHEMA = schema(
    {
        "n": pa.int64(),
        "s": pa.utf8(),
        "order_column": pa.utf8(),
        "order_direction": pa.utf8(),
        "order_null_order": pa.utf8(),
        "order_limit": pa.int64(),
    }
)


@init_single_worker
@bind_fixed_schema
@_cardinality_from_count
class OrderEchoFunction(TableFunctionGenerator[_OrderEchoArgs, _OrderEchoState]):
    """Echoes ORDER BY + LIMIT pushdown hints in output columns.

    USE CASE
    --------
    Verify that DuckDB's RowGroupPruner optimizer pushes ORDER BY + LIMIT
    hints to VGI table functions via the ``set_scan_order`` callback.
    The order_* columns show what hints were received. The function does
    NOT apply the order/limit itself -- DuckDB's operators handle that.

    SCHEMA
    ------
    Output: {"n": int64, "s": string, "order_column": string,
             "order_direction": string, "order_null_order": string,
             "order_limit": int64}

    """

    class Meta:
        """Metadata for OrderEchoFunction."""

        name = "order_echo"
        description = "Echoes ORDER BY + LIMIT pushdown hints in output"
        categories = ["generator", "diagnostic"]
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        examples = [
            FunctionExample(
                sql="SELECT * FROM order_echo(100) ORDER BY n LIMIT 5",
                description="See which ORDER BY hint was pushed down",
            ),
        ]

    FIXED_SCHEMA: ClassVar[pa.Schema] = _ORDER_ECHO_SCHEMA

    @classmethod
    def initial_state(cls, params: ProcessParams[_OrderEchoArgs]) -> _OrderEchoState:
        """Create initial state with cached order hint values."""
        assert params.init_call is not None
        init = params.init_call
        return _OrderEchoState(
            remaining=params.args.count,
            order_column=init.order_by_column_name or "(none)",
            order_direction=init.order_by_direction.name if init.order_by_direction else "(none)",
            order_null_order=init.order_by_null_order.name if init.order_by_null_order else "(none)",
            order_limit=init.order_by_limit if init.order_by_limit is not None else -1,
        )

    @classmethod
    def process(
        cls,
        params: ProcessParams[_OrderEchoArgs],
        state: _OrderEchoState,
        out: OutputCollector,
    ) -> None:
        """Generate rows echoing order pushdown hints."""
        if state.remaining <= 0:
            out.finish()
            return

        size = min(state.remaining, params.args.batch_size)
        start = state.current_index

        n_values = list(range(start, start + size))
        s_values = [f"row_{i}" for i in n_values]

        out.emit(
            pa.RecordBatch.from_pydict(
                {
                    "n": n_values,
                    "s": s_values,
                    "order_column": [state.order_column] * size,
                    "order_direction": [state.order_direction] * size,
                    "order_null_order": [state.order_null_order] * size,
                    "order_limit": [state.order_limit] * size,
                },
                schema=params.output_schema,
            )
        )

        state.current_index += size
        state.remaining -= size


# ============================================================================


@dataclass(slots=True, frozen=True)
class _SampleEchoArgs:
    """Arguments for SampleEchoFunction."""

    count: Annotated[int, Arg(0, doc="Number of rows to generate", ge=0, default=10)]
    batch_size: Annotated[int, Arg("batch_size", default=2048, doc="Batch size for output", ge=1)]


@dataclass(kw_only=True)
class _SampleEchoState(ArrowSerializableDataclass):
    """Mutable state for SampleEchoFunction."""

    remaining: int
    current_index: int = 0
    sample_percentage: Annotated[float, Transient()] = -1.0
    sample_seed: Annotated[int, Transient()] = -1


_SAMPLE_ECHO_SCHEMA = schema(
    {
        "n": pa.int64(),
        "s": pa.utf8(),
        "sample_percentage": pa.float64(),
        "sample_seed": pa.int64(),
    }
)


@init_single_worker
@bind_fixed_schema
@_cardinality_from_count
class SampleEchoFunction(TableFunctionGenerator[_SampleEchoArgs, _SampleEchoState]):
    """Echoes TABLESAMPLE pushdown hints in output columns.

    USE CASE
    --------
    Verify that DuckDB's SamplingPushdown optimizer pushes TABLESAMPLE SYSTEM
    hints to VGI table functions. The sample_* columns show what hints were
    received. The function does NOT apply sampling itself -- it returns all
    rows so tests can verify the echo values.

    SCHEMA
    ------
    Output: {"n": int64, "s": string, "sample_percentage": float64,
             "sample_seed": int64}

    """

    class Meta:
        """Metadata for SampleEchoFunction."""

        name = "sample_echo"
        description = "Echoes TABLESAMPLE pushdown hints in output"
        categories = ["generator", "diagnostic"]
        projection_pushdown = True
        sampling_pushdown = True
        examples = [
            FunctionExample(
                sql="SELECT * FROM sample_echo(100) TABLESAMPLE SYSTEM(10%)",
                description="See which TABLESAMPLE hint was pushed down",
            ),
        ]

    FIXED_SCHEMA: ClassVar[pa.Schema] = _SAMPLE_ECHO_SCHEMA

    @classmethod
    def initial_state(cls, params: ProcessParams[_SampleEchoArgs]) -> _SampleEchoState:
        """Create initial state with cached sample hint values."""
        assert params.init_call is not None
        init = params.init_call
        return _SampleEchoState(
            remaining=params.args.count,
            sample_percentage=init.tablesample_percentage if init.tablesample_percentage is not None else -1.0,
            sample_seed=init.tablesample_seed if init.tablesample_seed is not None else -1,
        )

    @classmethod
    def process(
        cls,
        params: ProcessParams[_SampleEchoArgs],
        state: _SampleEchoState,
        out: OutputCollector,
    ) -> None:
        """Generate rows echoing sample pushdown hints."""
        if state.remaining <= 0:
            out.finish()
            return

        size = min(state.remaining, params.args.batch_size)
        start = state.current_index

        n_values = list(range(start, start + size))
        s_values = [f"row_{i}" for i in n_values]

        out.emit(
            pa.RecordBatch.from_pydict(
                {
                    "n": n_values,
                    "s": s_values,
                    "sample_percentage": [state.sample_percentage] * size,
                    "sample_seed": [state.sample_seed] * size,
                },
                schema=params.output_schema,
            )
        )

        state.current_index += size
        state.remaining -= size
