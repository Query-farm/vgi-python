# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Filter-pushdown demos (filter_echo, dynamic_filter_echo, expression_filter, spatial_filter)."""

from __future__ import annotations

import os
import struct
from dataclasses import dataclass
from typing import Annotated, Any, ClassVar

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi._test_fixtures.table._common import (
    _cardinality_from_count,
    _EmptyArgs,
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
# ValuePruneFunction — exercises PushdownFilters.get_column_values('n'), the
# partition-pruning idiom (resolve the discrete value set up front, fetch only
# those keys). filter_echo can't cover this: it auto-applies the predicate
# row-by-row via Filter.evaluate, a different code path. Here the `resolved`
# column echoes exactly what get_column_values returned, so a regression in the
# AND/OR-descent of that accessor is directly observable — e.g. DuckDB pushing
# `n IN (...) AND n >= min AND n <= max` (an AndFilter) or `n = a OR n = b` (an
# OrFilter) must resolve to the discrete set, not collapse to "(scan)".
# ============================================================================


@dataclass(slots=True, frozen=True)
class _ValuePruneArgs:
    """Arguments for ValuePruneFunction."""

    count: Annotated[int, Arg(0, doc="Number of candidate rows (keys 0..count-1)", ge=0)]
    batch_size: Annotated[int, Arg("batch_size", default=2048, doc="Batch size for output", ge=1)]


@dataclass(kw_only=True)
class _ValuePruneState(ArrowSerializableDataclass):
    """Resolved key set to emit plus the echoed get_column_values result.

    Both fields are serialized (not Transient): the HTTP rehydrate path
    deserializes state without re-running initial_state, so the resolution
    must survive a state-token round-trip.
    """

    values: list[int]
    resolved: str
    cursor: int = 0


@init_single_worker
@bind_fixed_schema
@_cardinality_from_count
class ValuePruneFunction(TableFunctionGenerator[_ValuePruneArgs, _ValuePruneState]):
    """Emits only the keys that ``get_column_values('n')`` resolves to.

    The ``resolved`` column carries the sorted, comma-joined discrete set the
    accessor returned (or ``"(scan)"`` when it returned None, i.e. the predicate
    is not enumerable — no filter, a bare range, or an OR with a non-discrete
    branch). Assert on ``resolved`` to verify the accessor end-to-end,
    independent of any residual filtering.
    """

    class Meta:
        """Metadata for ValuePruneFunction."""

        name = "value_prune"
        description = "Prunes the key set via get_column_values('n'); echoes the resolved discrete values"
        categories = ["generator", "diagnostic"]
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        examples = [
            FunctionExample(
                sql="SELECT DISTINCT resolved FROM value_prune(100) WHERE n IN (5, 50, 95)",
                description="Resolve a discrete key set from an IN predicate",
            ),
        ]

    FIXED_SCHEMA: ClassVar[pa.Schema] = schema({"n": pa.int64(), "resolved": pa.utf8()})

    @classmethod
    def initial_state(cls, params: ProcessParams[_ValuePruneArgs]) -> _ValuePruneState:
        """Resolve the discrete key set for `n` from the pushed-down filters."""
        assert params.init_call is not None
        count = params.args.count
        pf = params.init_call.pushdown_filters
        jk = params.init_call.join_keys
        filters = cls.pushdown_filters(pf, join_keys=jk) if pf is not None else None
        discrete = filters.get_column_values("n") if filters is not None else None
        if discrete is not None:
            resolved_vals = sorted(v for v in discrete.to_pylist() if v is not None)
            resolved = ",".join(str(v) for v in resolved_vals)
            emit = [v for v in resolved_vals if 0 <= v < count]
        else:
            resolved = "(scan)"
            emit = list(range(count))
        return _ValuePruneState(values=emit, resolved=resolved)

    @classmethod
    def process(
        cls,
        params: ProcessParams[_ValuePruneArgs],
        state: _ValuePruneState,
        out: OutputCollector,
    ) -> None:
        """Emit the resolved keys (with the echoed `resolved` diagnostic)."""
        if state.cursor >= len(state.values):
            out.finish()
            return
        size = min(len(state.values) - state.cursor, params.args.batch_size)
        chunk = state.values[state.cursor : state.cursor + size]
        out.emit(
            pa.RecordBatch.from_pydict(
                {"n": chunk, "resolved": [state.resolved] * len(chunk)},
                schema=params.output_schema,
            )
        )
        state.cursor += size


# ============================================================================
# FilteredColumnsEchoFunction — echoes the column-introspection accessors on the
# pushed-down filter set: filtered_columns(), has_filter_for_column(), and the
# typed (string-capable) get_column_values(). A query's WHERE clause is reflected
# back as diagnostic columns so each accessor is observable end-to-end.
# ============================================================================


@dataclass(slots=True, frozen=True)
class _FilteredColumnsEchoArgs:
    """Arguments for FilteredColumnsEchoFunction."""

    count: Annotated[int, Arg(0, doc="Number of rows to generate", ge=0)]
    batch_size: Annotated[int, Arg("batch_size", default=2048, doc="Batch size for output", ge=1)]


@dataclass(kw_only=True)
class _FilteredColumnsEchoState(ArrowSerializableDataclass):
    """Resolved diagnostics (serialized so the HTTP rehydrate path preserves them)."""

    count: int
    filtered_cols: str
    has_n: bool
    has_tag: bool
    tag_values: str
    cursor: int = 0


@init_single_worker
@bind_fixed_schema
@_cardinality_from_count
class FilteredColumnsEchoFunction(TableFunctionGenerator[_FilteredColumnsEchoArgs, _FilteredColumnsEchoState]):
    """Report the columns referenced by pushed-down filters and ``tag``'s values.

    Surfaces which columns the pushed-down filters reference and the discrete
    value set resolved for the string column ``tag``.

    ``filtered_cols`` is the sorted, comma-joined ``filtered_columns()`` set;
    ``has_n`` / ``has_tag`` are ``has_filter_for_column()``; ``tag_values`` is
    the sorted, comma-joined ``get_column_values('tag')`` result (``"(none)"``
    when the predicate is not an enumerable equality/IN on ``tag``).
    """

    class Meta:
        """Metadata for FilteredColumnsEchoFunction."""

        name = "filtered_columns_echo"
        description = "Echoes filtered_columns / has_filter_for_column / get_column_values_array"
        categories = ["generator", "diagnostic"]
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(
        {
            "n": pa.int64(),
            "tag": pa.utf8(),
            "filtered_cols": pa.utf8(),
            "has_n": pa.bool_(),
            "has_tag": pa.bool_(),
            "tag_values": pa.utf8(),
        }
    )

    @classmethod
    def initial_state(cls, params: ProcessParams[_FilteredColumnsEchoArgs]) -> _FilteredColumnsEchoState:
        """Resolve the filter-column diagnostics from the pushed-down filters."""
        assert params.init_call is not None
        pf = params.init_call.pushdown_filters
        jk = params.init_call.join_keys
        filters = cls.pushdown_filters(pf, join_keys=jk) if pf is not None else None
        if filters is not None:
            filtered_cols = ",".join(sorted(filters.filtered_columns))
            has_n = filters.has_filter_for_column("n")
            has_tag = filters.has_filter_for_column("tag")
            tag_arr = filters.get_column_values("tag")
            if tag_arr is not None:
                tag_values = ",".join(sorted(str(v) for v in tag_arr.to_pylist() if v is not None))
            else:
                tag_values = "(none)"
        else:
            filtered_cols, has_n, has_tag, tag_values = "", False, False, "(none)"
        return _FilteredColumnsEchoState(
            count=params.args.count,
            filtered_cols=filtered_cols,
            has_n=has_n,
            has_tag=has_tag,
            tag_values=tag_values,
        )

    @classmethod
    def process(
        cls,
        params: ProcessParams[_FilteredColumnsEchoArgs],
        state: _FilteredColumnsEchoState,
        out: OutputCollector,
    ) -> None:
        """Emit the generated rows, each carrying the resolved diagnostics."""
        if state.cursor >= state.count:
            out.finish()
            return
        size = min(state.count - state.cursor, params.args.batch_size)
        ns = list(range(state.cursor, state.cursor + size))
        out.emit(
            pa.RecordBatch.from_pydict(
                {
                    "n": ns,
                    "tag": [f"t{i}" for i in ns],
                    "filtered_cols": [state.filtered_cols] * size,
                    "has_n": [state.has_n] * size,
                    "has_tag": [state.has_tag] * size,
                    "tag_values": [state.tag_values] * size,
                },
                schema=params.output_schema,
            )
        )
        state.cursor += size


# ============================================================================
# DictFilterEchoFunction — output column declared as a *dictionary* Arrow type
# (dictionary<int8, utf8>) with no ENUM metadata. DuckDB maps such a column to
# plain VARCHAR, so a `WHERE s = 'x'` / `s IN (...)` predicate pushes a VARCHAR
# (string) literal down to the worker. The worker then emits the column
# dictionary-encoded, producing a (dictionary column, string literal) pair that
# the filter evaluator must compare. Naively casting the literal up to the
# column's dictionary type makes `pc.is_in(dict, dict)` / `pc.equal(dict, dict)`
# throw `ArrowTypeError: Array type doesn't match type of values set`; the
# correct path decodes the column to its value type. This fixture pins that
# behavior so every language implementation handles it identically.
# ============================================================================


_DICT_FILTER_ECHO_SCHEMA = pa.schema(
    [
        pa.field("n", pa.int64()),
        pa.field("s", pa.dictionary(pa.int8(), pa.utf8())),
    ]
)

# Deterministic, low-cardinality values so dictionary encoding is meaningful and
# the row<->value mapping is easy to assert: row i carries _DICT_VALUES[i % len].
_DICT_VALUES: tuple[str, ...] = ("red", "green", "blue")


@dataclass(slots=True, frozen=True)
class _DictFilterEchoArgs:
    """Arguments for DictFilterEchoFunction."""

    count: Annotated[int, Arg(0, doc="Number of rows to generate", ge=0)]
    batch_size: Annotated[int, Arg("batch_size", default=2048, doc="Rows per batch", ge=1)]


@dataclass(kw_only=True)
class _DictFilterEchoState(ArrowSerializableDataclass):
    """Mutable state tracking remaining rows and position."""

    remaining: int
    current_index: int = 0


@init_single_worker
@bind_fixed_schema
@_cardinality_from_count
class DictFilterEchoFunction(TableFunctionGenerator[_DictFilterEchoArgs, _DictFilterEchoState]):
    """Emits a dictionary-encoded VARCHAR column to exercise filter pushdown.

    USE CASE
    --------
    Regression coverage for filter pushdown over a dictionary-encoded
    column whose DuckDB-facing type is plain VARCHAR. The pushed literal
    arrives as a string while the emitted column is ``dictionary<int8,
    utf8>``; the auto-applied filter must compare the two without
    throwing. See the module comment above.

    SCHEMA
    ------
    Output: {"n": int64, "s": dictionary<int8, utf8> (VARCHAR to DuckDB)}

    Row i has s = ("red", "green", "blue")[i % 3].

    Example:
    -------
    SELECT * FROM dict_filter_echo(6) WHERE s = 'green'
    Returns: rows 1 and 4.

    """

    class Meta:
        """Metadata for DictFilterEchoFunction."""

        name = "dict_filter_echo"
        description = "Emits a dictionary-encoded VARCHAR column for filter-pushdown testing"
        categories = ["generator", "diagnostic", "testing"]
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        examples = [
            FunctionExample(
                sql="SELECT * FROM dict_filter_echo(6) WHERE s = 'green'",
                description="Filter a dictionary-encoded column by an equality predicate",
            ),
            FunctionExample(
                sql="SELECT * FROM dict_filter_echo(6) WHERE s IN ('red', 'blue')",
                description="Filter a dictionary-encoded column by an IN predicate",
            ),
        ]

    FIXED_SCHEMA: ClassVar[pa.Schema] = _DICT_FILTER_ECHO_SCHEMA

    @classmethod
    def initial_state(cls, params: ProcessParams[_DictFilterEchoArgs]) -> _DictFilterEchoState:
        """Create initial state with the remaining row count."""
        return _DictFilterEchoState(remaining=params.args.count)

    @classmethod
    def process(
        cls,
        params: ProcessParams[_DictFilterEchoArgs],
        state: _DictFilterEchoState,
        out: OutputCollector,
    ) -> None:
        """Emit a batch with n and a dictionary-encoded s column."""
        if state.remaining <= 0:
            out.finish()
            return

        size = min(state.remaining, params.args.batch_size)
        start = state.current_index

        n_values = list(range(start, start + size))
        s_values = [_DICT_VALUES[i % len(_DICT_VALUES)] for i in n_values]

        out.emit(
            pa.RecordBatch.from_pydict(
                {"n": n_values, "s": s_values},
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

    # Cap the work queue at ~MAX_PARTITIONS items regardless of count, by sizing
    # each chunk as ceil(count / MAX_PARTITIONS). The queue is drained one item
    # per round-trip and serialized at the per-attach DO, so partition *count*
    # drives remote cost. A fixed chunk size can't serve both a large query and
    # a small distribution query (too-large chunks collapse the small one to one
    # partition and kill fan-out); capping the partition count keeps ~24
    # partitions at any scale. Each work item is a fixed-size (start, end) range
    # — rows are generated locally and emitted in BATCH_SIZE batches — so this
    # changes only the *count* of tiny pops, never any HTTP body size. Output is
    # the echoed/filtered rows (partition-independent), so assertions hold.
    MAX_PARTITIONS: ClassVar[int] = 24
    BATCH_SIZE: ClassVar[int] = 1000

    FIXED_SCHEMA: ClassVar[pa.Schema] = _FILTER_ECHO_PARTITIONED_SCHEMA

    @classmethod
    def on_init(
        cls,
        params: InitParams[_FilterEchoPartitionedArgs],
    ) -> GlobalInitResponse:
        """Populate the work queue with (start, end) chunks for parallel consumption."""
        work_items: list[bytes] = []
        chunk = max(1, -(-params.args.count // cls.MAX_PARTITIONS))  # ceil(count / MAX_PARTITIONS)
        for start_idx in range(0, params.args.count, chunk):
            end_idx = min(start_idx + chunk, params.args.count)
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


# ============================================================================
# FilterEchoTableScanFunction — catalog *table* (not table function) backing for
# example.data.filter_echo_table. Mirrors FilterEchoFunction's pushed_filters
# echo, but is invoked with no positional args (the catalog scan route in the
# fixture worker passes none) so a `SELECT ... FROM example.data.filter_echo_table`
# — and a VIEW over it — can be characterized for filter pushdown. Crucially it
# declares supported_expression_filters so a `col LIKE 'abc%'` predicate (which
# DuckDB lowers to a prefix/starts_with expression filter) actually reaches the
# worker and shows up in the pushed_filters column. See
# test/sql/integration/table/filter_pushdown_through_view.test.
# ============================================================================


_FILTER_ECHO_TABLE_SCHEMA = schema({"n": pa.int64(), "s": pa.utf8(), "pushed_filters": pa.utf8()})

# Fixed 100-row dataset: n in 0..99, s = "row_<n>". The "row_" prefix makes
# LIKE 'row_1%' meaningful (matches row_1 and row_10..row_19).
_FILTER_ECHO_TABLE_ROWS = 100


@dataclass(kw_only=True)
class _FilterEchoTableState(ArrowSerializableDataclass):
    """One-shot state carrying the captured pushed-filter string.

    ``filter_str`` is serialized (not Transient): the framework's HTTP
    rehydrate path deserializes user state but does not re-invoke
    ``initial_state``, so a Transient filter string would silently revert
    to ``"(none)"`` after the first state-token round-trip.
    """

    done: bool = False
    filter_str: str = "(none)"


@init_single_worker
@bind_fixed_schema
class FilterEchoTableScanFunction(TableFunctionGenerator[_EmptyArgs, _FilterEchoTableState]):
    """Catalog-table scan that echoes the pushed-down filters it received.

    Backs ``example.data.filter_echo_table``. Like :class:`FilterEchoFunction`
    the ``pushed_filters`` column shows the SQL-like representation of whatever
    DuckDB pushed down; the framework auto-applies the filters so the result set
    stays correct. Unlike ``filter_echo`` it is a no-arg *table* scan and opts
    into expression-filter pushdown, so a ``LIKE 'prefix%'`` predicate is
    observable here (and through a view over this table).

    SCHEMA
    ------
    Output: {"n": int64, "s": string, "pushed_filters": string}, 100 rows
    (n in 0..99, s = "row_<n>").
    """

    class Meta:
        """Metadata for FilterEchoTableScanFunction."""

        name = "filter_echo_table_scan"
        description = "Catalog-table scan echoing pushed-down filters (backs example.data.filter_echo_table)"
        categories = ["generator", "diagnostic", "testing"]
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        supported_expression_filters = ["prefix", "starts_with"]

    FIXED_SCHEMA: ClassVar[pa.Schema] = _FILTER_ECHO_TABLE_SCHEMA

    @classmethod
    def initial_state(cls, params: ProcessParams[_EmptyArgs]) -> _FilterEchoTableState:
        """Capture the pushed-filter string for echoing."""
        assert params.init_call is not None
        pf = params.init_call.pushdown_filters
        jk = params.init_call.join_keys
        filters = cls.pushdown_filters(pf, join_keys=jk) if pf is not None else None
        return _FilterEchoTableState(filter_str=_format_pushed_filters(filters))

    @classmethod
    def process(
        cls,
        params: ProcessParams[_EmptyArgs],
        state: _FilterEchoTableState,
        out: OutputCollector,
    ) -> None:
        """Emit the fixed dataset once, projecting to the requested columns."""
        if state.done:
            out.finish()
            return
        state.done = True

        ns = list(range(_FILTER_ECHO_TABLE_ROWS))
        full: dict[str, list[Any]] = {
            "n": ns,
            "s": [f"row_{i}" for i in ns],
            "pushed_filters": [state.filter_str] * _FILTER_ECHO_TABLE_ROWS,
        }
        # projection_pushdown=True: emit only the requested columns.
        columns = {f.name: full[f.name] for f in params.output_schema}
        out.emit(pa.RecordBatch.from_pydict(columns, schema=params.output_schema))
