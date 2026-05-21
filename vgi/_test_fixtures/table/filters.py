# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Filter-pushdown demos (filter_echo, dynamic_filter_echo, expression_filter, spatial_filter)."""

from __future__ import annotations

import os
import struct
from dataclasses import dataclass
from typing import Annotated, ClassVar

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi._test_fixtures.table._common import (
    _cardinality_from_count,
)
from vgi.arguments import Arg
from vgi.invocation import GlobalInitResponse
from vgi.metadata import FunctionExample
from vgi.schema_utils import schema
from vgi.table_filter_pushdown import PushdownFilters
from vgi.table_function import (
    InitParams,
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)

# =============================================================================


def _format_pushed_filters(filters: PushdownFilters | None) -> str:
    """Format pushed-down filters as a human-readable SQL-like string.

    Large IN lists (from join key pushdown) are truncated to avoid
    generating multi-megabyte filter strings.
    """
    if not filters:
        return "(none)"

    from vgi.table_filter_pushdown import AndFilter, InFilter, OrFilter, _filter_to_sql

    def _format_one(f: object) -> str:
        """Format a single filter, truncating large InFilters."""
        if isinstance(f, InFilter) and len(f.values) > 20:
            return f"{f.column_name} IN ({len(f.values)} values)"
        if isinstance(f, AndFilter):
            child_parts = [_format_one(c) for c in f.children]
            return "(" + " AND ".join(child_parts) + ")"
        if isinstance(f, OrFilter):
            child_parts = [_format_one(c) for c in f.children]
            return "(" + " OR ".join(child_parts) + ")"
        # Fall back to SQL rendering for other filter types
        sql, params = _filter_to_sql(f, lambda s: s, "?", 0)  # type: ignore[arg-type]
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

    formatted_parts = [_format_one(f) for f in filters]
    return " AND ".join(formatted_parts) if formatted_parts else "(none)"


@dataclass(slots=True, frozen=True)
class FilterEchoFunctionArgs:
    """Arguments for FilterEchoFunction."""

    count: Annotated[int, Arg(0, doc="Number of rows to generate", ge=0)]
    batch_size: Annotated[int, Arg("batch_size", default=2048, doc="Batch size for output", ge=1)]


@dataclass(kw_only=True)
class FilterEchoState(ArrowSerializableDataclass):
    """Mutable state tracking remaining rows, position, and cached filter string.

    ``filter_str`` is serialized (not Transient): the framework's HTTP
    rehydrate path deserializes user state but does not re-invoke
    ``initial_state``, so a Transient filter string would silently revert
    to ``"(none)"`` after the first state-token round-trip.
    """

    remaining: int
    current_index: int = 0
    filter_str: str = "(none)"


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
        assert params.init_call is not None
        pf = params.init_call.pushdown_filters
        jk = params.init_call.join_keys
        filters = cls.pushdown_filters(pf, join_keys=jk) if pf is not None else None
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


def _make_wkb_point(x: float, y: float) -> bytes:
    """Encode a 2D point as little-endian WKB (byte_order=1, type=1=Point, x, y)."""
    return struct.pack("<bI", 1, 1) + struct.pack("<dd", x, y)


# Arrow field with geoarrow.wkb extension metadata so DuckDB recognizes it as GEOMETRY
_GEOMETRY_FIELD = pa.field(
    "geom",
    pa.binary(),
    metadata={
        b"ARROW:extension:name": b"geoarrow.wkb",
        b"ARROW:extension:metadata": b"{}",
    },
)

_SPATIAL_FILTER_SCHEMA = pa.schema(
    [  # type: ignore[arg-type]  # pyarrow stubs: mixed-type fields
        pa.field("n", pa.int64()),
        pa.field("x", pa.float64()),
        pa.field("y", pa.float64()),
        _GEOMETRY_FIELD,
    ]
)


@dataclass(slots=True, frozen=True)
class _SpatialFilterArgs:
    """Arguments for SpatialFilterExampleFunction."""

    count: Annotated[int, Arg(0, doc="Number of points to generate", ge=1)]
    batch_size: Annotated[int, Arg("batch_size", default=1024, doc="Rows per batch")]


@dataclass(kw_only=True)
class _SpatialFilterState(ArrowSerializableDataclass):
    """Mutable state for SpatialFilterExampleFunction."""

    remaining: int
    total_count: int
    current_index: int = 0


@init_single_worker
@bind_fixed_schema
@_cardinality_from_count
class SpatialFilterExampleFunction(TableFunctionGenerator[_SpatialFilterArgs, _SpatialFilterState]):
    """Generates points on a grid with geometry column for spatial filter testing.

    USE CASE
    --------
    Test expression filter pushdown with spatial predicates. Points are placed
    on a deterministic grid in [0, 1) x [0, 1) so that bounding box filter
    counts are predictable.

    SCHEMA
    ------
    Output: {"n": int64, "x": float64, "y": float64, "geom": GEOMETRY}

    Grid layout: For count=N, point i has coordinates:
        x = (i % cols) / cols
        y = (i // cols) / cols
    where cols = ceil(sqrt(N)).

    Example:
    -------
    SELECT * FROM spatial_filter_example(100) WHERE geom && ST_MakeEnvelope(0, 0, 0.5, 0.5)
    Returns: points in the lower-left quadrant of the unit square.

    """

    class Meta:
        """Metadata for SpatialFilterExampleFunction."""

        name = "spatial_filter_example"
        description = "Generates points on a grid with geometry for spatial filter testing"
        categories = ["generator", "spatial", "testing"]
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        supported_expression_filters = ["&&", "st_intersects_extent"]
        examples = [
            FunctionExample(
                sql="SELECT * FROM spatial_filter_example(100)",
                description="Generate 100 points on a 10x10 grid",
            ),
            FunctionExample(
                sql="SELECT COUNT(*) FROM spatial_filter_example(100) WHERE geom && ST_MakeEnvelope(0, 0, 0.5, 0.5)",
                description="Count points in the lower-left quadrant",
            ),
        ]

    FIXED_SCHEMA: ClassVar[pa.Schema] = _SPATIAL_FILTER_SCHEMA

    @classmethod
    def initial_state(cls, params: ProcessParams[_SpatialFilterArgs]) -> _SpatialFilterState:
        """Create initial state."""
        return _SpatialFilterState(remaining=params.args.count, total_count=params.args.count)

    @classmethod
    def process(
        cls,
        params: ProcessParams[_SpatialFilterArgs],
        state: _SpatialFilterState,
        out: OutputCollector,
    ) -> None:
        """Generate grid points with WKB geometry."""
        if state.remaining <= 0:
            out.finish()
            return

        import math

        cols = max(1, math.ceil(math.sqrt(state.total_count)))
        size = min(state.remaining, params.args.batch_size)
        start = state.current_index

        ns = list(range(start, start + size))
        xs = [(i % cols) / cols for i in ns]
        ys = [(i // cols) / cols for i in ns]
        geoms = [_make_wkb_point(x, y) for x, y in zip(xs, ys, strict=True)]

        out.emit(
            pa.RecordBatch.from_pydict(
                {"n": ns, "x": xs, "y": ys, "geom": geoms},
                schema=params.output_schema,
            )
        )

        state.current_index += size
        state.remaining -= size


# ============================================================================


@dataclass(slots=True, frozen=True)
class _DynFilterEchoArgs:
    """Arguments for DynamicFilterEchoFunction."""

    count: Annotated[int, Arg(0, doc="Number of rows to generate", ge=1)]
    batch_size: Annotated[int, Arg("batch_size", default=100, doc="Rows per batch")]


@dataclass(kw_only=True)
class _DynFilterEchoState(ArrowSerializableDataclass):
    """Mutable state for DynamicFilterEchoFunction."""

    remaining: int
    current_index: int = 0


def _format_pushed_filters_safe(filters: object) -> str:
    """Format PushdownFilters to readable string, returning '(none)' if empty/None."""
    if filters is None:
        return "(none)"
    from vgi.table_filter_pushdown import PushdownFilters

    if isinstance(filters, PushdownFilters) and filters:
        return repr(filters)
    return "(none)"


_DYN_FILTER_ECHO_SCHEMA = pa.schema(
    [  # type: ignore[arg-type]  # pyarrow stubs: mixed-type fields
        pa.field("n", pa.int64()),
        pa.field("pushed_filters", pa.utf8()),
    ]
)


@init_single_worker
@bind_fixed_schema
@_cardinality_from_count
class DynamicFilterEchoFunction(TableFunctionGenerator[_DynFilterEchoArgs, _DynFilterEchoState]):
    """Generates descending integers and echoes the current tick filter per batch.

    USE CASE
    --------
    Demonstrates dynamic filter pushdown. Rows are generated in **descending**
    order (count-1, count-2, ..., 0) so that ``ORDER BY n ASC LIMIT K`` causes
    the Top-N heap to tighten gradually. Each batch's ``pushed_filters`` column
    shows the filter received from the most recent tick.

    SCHEMA
    ------
    Output: {"n": int64, "pushed_filters": string}

    """

    class Meta:
        """Metadata for DynamicFilterEchoFunction."""

        name = "dynamic_filter_echo"
        description = "Generates descending integers, echoes dynamic tick filter per batch"
        categories = ["generator", "diagnostic"]
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    FIXED_SCHEMA: ClassVar[pa.Schema] = _DYN_FILTER_ECHO_SCHEMA

    @classmethod
    def initial_state(cls, params: ProcessParams[_DynFilterEchoArgs]) -> _DynFilterEchoState:
        """Create initial state."""
        return _DynFilterEchoState(remaining=params.args.count)

    @classmethod
    def process(
        cls,
        params: ProcessParams[_DynFilterEchoArgs],
        state: _DynFilterEchoState,
        out: OutputCollector,
    ) -> None:
        """Generate descending rows with current filter echoed."""
        if state.remaining <= 0:
            out.finish()
            return

        total = params.args.count
        size = min(state.remaining, params.args.batch_size)
        start = state.current_index

        # Descending order: first batch has highest values
        ns = [total - 1 - i for i in range(start, start + size)]
        filter_str = _format_pushed_filters_safe(params.current_pushdown_filters)
        filter_values = [filter_str] * size

        out.emit(
            pa.RecordBatch.from_pydict(
                {"n": ns, "pushed_filters": filter_values},
                schema=params.output_schema,
            )
        )

        state.current_index += size
        state.remaining -= size


# ============================================================================

_EXPR_FILTER_TEST_SCHEMA = pa.schema(
    [  # type: ignore[arg-type]  # pyarrow stubs: mixed-type fields
        pa.field("id", pa.int64()),
        pa.field("name", pa.utf8()),
        pa.field("tags", pa.list_(pa.utf8())),
        pa.field("score", pa.float64()),
    ]
)


@dataclass(slots=True, frozen=True)
class _ExprFilterTestArgs:
    """Arguments for ExpressionFilterTestFunction."""

    count: Annotated[int, Arg(0, doc="Number of rows to generate", ge=1)]
    batch_size: Annotated[int, Arg("batch_size", default=1024, doc="Rows per batch")]


@dataclass(kw_only=True)
class _ExprFilterTestState(ArrowSerializableDataclass):
    """Mutable state for ExpressionFilterTestFunction."""

    remaining: int
    current_index: int = 0


@init_single_worker
@bind_fixed_schema
@_cardinality_from_count
class ExpressionFilterTestFunction(TableFunctionGenerator[_ExprFilterTestArgs, _ExprFilterTestState]):
    """Generates rows with list and string columns for non-spatial expression filter testing.

    USE CASE
    --------
    Test expression filter pushdown with non-spatial functions like
    list_contains, prefix, starts_with, etc.

    SCHEMA
    ------
    Output: {"id": int64, "name": string, "tags": list<string>, "score": float64}

    Row i has:
        name = 'item_<i>'
        tags = ['tag_<i%5>', 'tag_<(i+1)%5>']
        score = i * 1.1

    """

    class Meta:
        """Metadata for ExpressionFilterTestFunction."""

        name = "expression_filter_test"
        description = "Generates rows for non-spatial expression filter testing"
        categories = ["generator", "testing"]
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        supported_expression_filters = ["list_contains", "prefix", "starts_with", "contains"]

    FIXED_SCHEMA: ClassVar[pa.Schema] = _EXPR_FILTER_TEST_SCHEMA

    @classmethod
    def initial_state(cls, params: ProcessParams[_ExprFilterTestArgs]) -> _ExprFilterTestState:
        """Create initial state."""
        return _ExprFilterTestState(remaining=params.args.count)

    @classmethod
    def process(
        cls,
        params: ProcessParams[_ExprFilterTestArgs],
        state: _ExprFilterTestState,
        out: OutputCollector,
    ) -> None:
        """Generate rows with list and string columns."""
        if state.remaining <= 0:
            out.finish()
            return

        size = min(state.remaining, params.args.batch_size)
        start = state.current_index

        ids = list(range(start, start + size))
        names = [f"item_{i}" for i in ids]
        tags = [[f"tag_{i % 5}", f"tag_{(i + 1) % 5}"] for i in ids]
        scores = [i * 1.1 for i in ids]

        out.emit(
            pa.RecordBatch.from_pydict(
                {"id": ids, "name": names, "tags": tags, "score": scores},
                schema=params.output_schema,
            )
        )

        state.current_index += size
        state.remaining -= size


# ============================================================================
# FilterEchoPartitionedFunction — multi-worker fixture that exercises filter
# pushdown across parallel workers. Combines the queue-based work distribution
# of PartitionedSequenceFunction with the filter-capture pattern of
# FilterEchoFunction so each worker echoes the filter it observed.
# ============================================================================


_FILTER_ECHO_PARTITIONED_SCHEMA = schema(
    {
        "n": pa.int64(),
        "worker_pid": pa.int64(),
        "pushed_filters": pa.utf8(),
    }
)


@dataclass(slots=True, frozen=True)
class _FilterEchoPartitionedArgs:
    """Arguments for FilterEchoPartitionedFunction."""

    count: Annotated[int, Arg(0, doc="Total number of integers to generate", ge=0)]


@dataclass(kw_only=True)
class _FilterEchoPartitionedState(ArrowSerializableDataclass):
    """Per-worker state.

    ``filter_str`` is serialized (not Transient): the framework's HTTP
    rehydrate path deserializes user state but does not re-invoke
    ``initial_state``, so a Transient filter string would silently revert
    to ``"(none)"`` after the first state-token round-trip — losing the
    pushed-filter echo on every batch produced after a resume.
    """

    current_start: int | None = None
    current_end: int | None = None
    current_idx: int = 0
    filter_str: str = "(none)"


@bind_fixed_schema
@_cardinality_from_count
class FilterEchoPartitionedFunction(TableFunctionGenerator[_FilterEchoPartitionedArgs, _FilterEchoPartitionedState]):
    """Multi-worker filter-echo: queue-distributed sequence with filter pushdown.

    Verifies that predicates DuckDB pushes down are observed *and* applied by
    every parallel worker. Each worker pulls chunks from a shared queue and
    independently deserializes the same pushed filter spec at init. The
    framework auto-applies filters per emitted batch.

    SCHEMA
    ------
    Output: {"n": int64, "worker_pid": int64, "pushed_filters": string}

    PARALLELIZATION
    ---------------
    Uses a shared work queue: ``on_init`` enqueues 1000-row chunks. Workers
    (up to DuckDB's parallel scan limit) pop chunks atomically.
    ``worker_pid`` reveals which OS process produced each row — under
    subprocess transport that is one PID per worker; HTTP workers share a
    process so the column collapses to a single value there.

    """

    class Meta:
        """Metadata for FilterEchoPartitionedFunction."""

        name = "filter_echo_partitioned"
        description = "Multi-worker partitioned sequence that echoes pushed-down filters"
        categories = ["generator", "diagnostic", "testing"]
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        examples = [
            FunctionExample(
                sql="SELECT * FROM filter_echo_partitioned(10) WHERE n >= 8",
                description="Multi-worker generation with filter pushdown",
            ),
        ]

    CHUNK_SIZE: ClassVar[int] = 1000
    BATCH_SIZE: ClassVar[int] = 1000

    FIXED_SCHEMA: ClassVar[pa.Schema] = _FILTER_ECHO_PARTITIONED_SCHEMA

    @classmethod
    def on_init(
        cls,
        params: InitParams[_FilterEchoPartitionedArgs],
    ) -> GlobalInitResponse:
        """Populate the work queue with (start, end) chunks for parallel consumption."""
        work_items: list[bytes] = []
        for start_idx in range(0, params.args.count, cls.CHUNK_SIZE):
            end_idx = min(start_idx + cls.CHUNK_SIZE, params.args.count)
            work_items.append(struct.pack(">QQ", start_idx, end_idx))
        params.storage.queue_push(work_items)
        return GlobalInitResponse()

    @classmethod
    def initial_state(cls, params: ProcessParams[_FilterEchoPartitionedArgs]) -> _FilterEchoPartitionedState:
        """Initialize per-worker state and capture the pushed filter string."""
        assert params.init_call is not None
        pf = params.init_call.pushdown_filters
        jk = params.init_call.join_keys
        filters = cls.pushdown_filters(pf, join_keys=jk) if pf is not None else None
        return _FilterEchoPartitionedState(filter_str=_format_pushed_filters(filters))

    @classmethod
    def process(
        cls,
        params: ProcessParams[_FilterEchoPartitionedArgs],
        state: _FilterEchoPartitionedState,
        out: OutputCollector,
    ) -> None:
        """Pop a work chunk and emit a batch tagged with worker_pid and pushed_filters."""
        if state.current_start is None or state.current_idx >= (state.current_end or 0):
            work_data = params.storage.queue_pop()
            if work_data is None:
                out.finish()
                return
            state.current_start, state.current_end = struct.unpack(">QQ", work_data)
            assert state.current_start is not None
            state.current_idx = state.current_start

        batch_end_idx = min(state.current_idx + cls.BATCH_SIZE, state.current_end or 0)
        size = batch_end_idx - state.current_idx
        ns = list(range(state.current_idx, batch_end_idx))
        pid = os.getpid()

        out.emit(
            pa.RecordBatch.from_pydict(
                {
                    "n": ns,
                    "worker_pid": [pid] * size,
                    "pushed_filters": [state.filter_str] * size,
                },
                schema=params.output_schema,
            )
        )

        state.current_idx = batch_end_idx
