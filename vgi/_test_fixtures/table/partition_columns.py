"""Reference fixtures for the v2 PartitionColumns (Hive-style) batch_index mode.

These exercise the ``Meta.partition_kind`` + ``partition_field()``
opt-in. The C++ extension installs ``TableFunction::get_partition_info``
returning the declared kind, and ``get_partition_data`` populates
``OperatorPartitionData::partition_data`` per chunk so DuckDB's planner
can pick ``PhysicalPartitionedAggregate`` for matching ``GROUP BY``
queries.

Today DuckDB consumes only ``SINGLE_VALUE_PARTITIONS``; OVERLAPPING /
DISJOINT are wire-level declarable and the C++ extension reports them
back to the planner, which falls back to ``HASH_GROUP_BY`` for those
modes until upstream adds consumers.

Fixtures:

* :class:`CountryPartitionedSalesFunction` — single-column
  SINGLE_VALUE. Each emitted chunk has a single ``country`` value.
  Core fixture for the planner-check assertion.

* :class:`RegionYearPartitionedFunction` — multi-column SINGLE_VALUE.
  Each chunk has a single ``(region, year)`` tuple.

* :class:`PartitionedWithProjectedOutColumnFunction` — declares
  partition on ``category`` but DOES NOT include ``category`` in the
  emitted batch. Uses the explicit ``partition_values=`` override on
  ``out.emit`` to supply the value the framework can't auto-extract.

* :class:`DisjointRangePartitionedFunction` — declares
  ``DISJOINT_PARTITIONS``. Each chunk's ``key`` column has a distinct
  disjoint integer range. Verifies the wire path; DuckDB falls back to
  ``HASH_GROUP_BY`` for GROUP BY queries against it.

All fixtures use the in-memory state pattern (no work-queue / no
stream_state) — they're simpler than the v1 partitioned_batch_index
since the v2 plan is about correctness of the partition contract,
not parallelism stress. The v1 stress fixtures already exercise the
parallel-emit code path.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Annotated, ClassVar

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi._test_fixtures.table._common import _cardinality_from_count
from vgi.arguments import Arg
from vgi.invocation import GlobalInitResponse
from vgi.metadata import FunctionExample, PartitionKind
from vgi.schema_utils import partition_field
from vgi.table_function import (
    InitParams,
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
)

# =============================================================================
# Single-column SINGLE_VALUE_PARTITIONS — core fixture
# =============================================================================


@dataclass(slots=True, frozen=True)
class _CountryPartitionedArgs:
    """Arguments for ``country_partitioned_sales``."""

    rows_per_country: Annotated[int, Arg(0, doc="Rows to emit per country partition", ge=1)]


@dataclass(kw_only=True)
class _CountryPartitionedState(ArrowSerializableDataclass):
    """Per-worker cursor. ``current_country`` is set after the worker
    pops a queue item; ``current_idx`` advances through emitted rows
    until the per-country quota is reached, then it pops the next item.
    """

    current_country: str | None = None
    current_country_idx: int = -1
    current_idx: int = 0


# A small, fixed list of partition values gives the SQL tests stable
# expected outputs and a predictable number of partitions (5).
_COUNTRIES: list[str] = ["AU", "BR", "CA", "FR", "US"]
# Queue items are ``(country_idx, country_name_bytes)``. The framework
# emits one Arrow batch per pop.
_QUEUE_ITEM_FMT = ">i"  # int32 country_idx; country name lives in
# ``_COUNTRIES[idx]`` (avoids variable-length
# encoding for what's already a stable index).


@bind_fixed_schema
@_cardinality_from_count
class CountryPartitionedSalesFunction(TableFunctionGenerator[_CountryPartitionedArgs, _CountryPartitionedState]):
    """One Arrow batch per ``country``; ``country`` is single-valued per chunk.

    Demonstrates the SINGLE_VALUE_PARTITIONS contract. The C++ extension
    reports SINGLE_VALUE_PARTITIONS from ``get_partition_info`` when the
    planner asks about ``country``; ``GROUP BY country`` plans as
    ``PARTITIONED_AGGREGATE``.

    Uses the work-queue pattern so multi-worker parallel scan distributes
    partitions across threads (each item processed exactly once), matching
    the v1 ``partitioned_batch_index`` model.
    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            partition_field("country", pa.string()),
            pa.field("sales", pa.int64()),
        ]
    )

    class Meta:
        name = "country_partitioned_sales"
        description = (
            "Per-country sales rows, one Arrow batch per country. Declares country as a SINGLE_VALUE partition column."
        )
        categories = ["generator", "partitioning"]
        partition_kind = PartitionKind.SINGLE_VALUE_PARTITIONS
        examples = [
            FunctionExample(
                sql="SELECT country, SUM(sales) FROM country_partitioned_sales(100) GROUP BY country",
                description="Partitioned aggregate over country",
            ),
        ]

    @classmethod
    def on_init(cls, params: InitParams[_CountryPartitionedArgs]) -> GlobalInitResponse:
        items = [struct.pack(_QUEUE_ITEM_FMT, i) for i in range(len(_COUNTRIES))]
        params.storage.queue_push(items)
        return GlobalInitResponse()

    @classmethod
    def initial_state(cls, params: ProcessParams[_CountryPartitionedArgs]) -> _CountryPartitionedState:
        return _CountryPartitionedState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[_CountryPartitionedArgs],
        state: _CountryPartitionedState,
        out: OutputCollector,
    ) -> None:
        if state.current_country is None or state.current_idx >= params.args.rows_per_country:
            item = params.storage.queue_pop()
            if item is None:
                out.finish()
                return
            (state.current_country_idx,) = struct.unpack(_QUEUE_ITEM_FMT, item)
            state.current_country = _COUNTRIES[state.current_country_idx]
            state.current_idx = 0

        rpc = params.args.rows_per_country
        # Deterministic, unique sales values per (country, row) so the
        # SQL test's SUM checks are easy to write.
        base = state.current_country_idx * 1_000_000
        sales_values = [base + i for i in range(rpc)]
        batch = pa.RecordBatch.from_pydict(
            {"country": [state.current_country] * rpc, "sales": sales_values},
            schema=cls.FIXED_SCHEMA,
        )
        out.emit(batch)
        # One batch per partition; mark current partition exhausted.
        state.current_idx = rpc


# =============================================================================
# Multi-column SINGLE_VALUE_PARTITIONS
# =============================================================================


@dataclass(slots=True, frozen=True)
class _RegionYearArgs:
    """Arguments for ``region_year_partitioned``."""

    rows_per_partition: Annotated[int, Arg(0, doc="Rows per (region, year) partition", ge=1)]


@dataclass(kw_only=True)
class _RegionYearState(ArrowSerializableDataclass):
    current_partition_idx: int = -1
    current_idx: int = 0
    started: bool = False


# (region, year) tuples — 6 partitions total
_REGIONS_YEARS: list[tuple[str, int]] = [
    ("AMER", 2023),
    ("AMER", 2024),
    ("EMEA", 2023),
    ("EMEA", 2024),
    ("APAC", 2023),
    ("APAC", 2024),
]


@bind_fixed_schema
@_cardinality_from_count
class RegionYearPartitionedFunction(TableFunctionGenerator[_RegionYearArgs, _RegionYearState]):
    """Per-(region, year) chunks with both columns single-valued.

    Uses the work-queue pattern so multi-worker scan distributes
    partitions across threads.
    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            partition_field("region", pa.string()),
            partition_field("year", pa.int64()),
            pa.field("value", pa.float64()),
        ]
    )

    class Meta:
        name = "region_year_partitioned"
        description = (
            "Per-(region, year) value rows. Declares both region and year "
            "as SINGLE_VALUE partition columns; GROUP BY region, year "
            "plans as PARTITIONED_AGGREGATE."
        )
        categories = ["generator", "partitioning"]
        partition_kind = PartitionKind.SINGLE_VALUE_PARTITIONS
        examples = [
            FunctionExample(
                sql="SELECT region, year, AVG(value) FROM region_year_partitioned(100) GROUP BY region, year",
                description="Partitioned aggregate over (region, year)",
            ),
        ]

    @classmethod
    def on_init(cls, params: InitParams[_RegionYearArgs]) -> GlobalInitResponse:
        items = [struct.pack(_QUEUE_ITEM_FMT, i) for i in range(len(_REGIONS_YEARS))]
        params.storage.queue_push(items)
        return GlobalInitResponse()

    @classmethod
    def initial_state(cls, params: ProcessParams[_RegionYearArgs]) -> _RegionYearState:
        return _RegionYearState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[_RegionYearArgs],
        state: _RegionYearState,
        out: OutputCollector,
    ) -> None:
        if not state.started or state.current_idx >= params.args.rows_per_partition:
            item = params.storage.queue_pop()
            if item is None:
                out.finish()
                return
            (state.current_partition_idx,) = struct.unpack(_QUEUE_ITEM_FMT, item)
            state.current_idx = 0
            state.started = True

        region, year = _REGIONS_YEARS[state.current_partition_idx]
        rpp = params.args.rows_per_partition
        base = float(state.current_partition_idx * 1000)
        values = [base + float(i) for i in range(rpp)]
        batch = pa.RecordBatch.from_pydict(
            {
                "region": [region] * rpp,
                "year": [year] * rpp,
                "value": values,
            },
            schema=cls.FIXED_SCHEMA,
        )
        out.emit(batch)
        state.current_idx = rpp


# =============================================================================
# Projected-out partition column — exercises explicit override path
# =============================================================================


@dataclass(slots=True, frozen=True)
class _ProjectedOutArgs:
    """Arguments for ``partitioned_with_explicit_override``."""

    rows_per_category: Annotated[int, Arg(0, doc="Rows per category partition", ge=1)]


@dataclass(kw_only=True)
class _ProjectedOutState(ArrowSerializableDataclass):
    current_category_idx: int = -1
    current_idx: int = 0
    started: bool = False


_CATEGORIES: list[str] = ["books", "music", "video"]


@bind_fixed_schema
@_cardinality_from_count
class PartitionedWithExplicitOverrideFunction(TableFunctionGenerator[_ProjectedOutArgs, _ProjectedOutState]):
    """Uses the explicit ``partition_values=`` override on ``out.emit``.

    Emits batches that DO include the partition column (so auto-extract
    would work), but supplies ``partition_values`` explicitly anyway —
    exercises the type-validation + IPC-batch-construction code path
    for the explicit-override variant.

    A worker whose emitted batches don't include the partition column
    (e.g. under aggressive projection pushdown) MUST use this path;
    this fixture covers the contract without needing to wire up
    projection pushdown in the fixture itself.
    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            partition_field("category", pa.string()),
            pa.field("revenue", pa.int64()),
        ]
    )

    class Meta:
        name = "partitioned_with_explicit_override"
        description = (
            "Partition column ``category`` is in the bind schema and the "
            "emitted batches; worker uses the explicit "
            "``partition_values=`` override on ``out.emit`` to exercise "
            "the override code path."
        )
        categories = ["generator", "partitioning", "testing"]
        partition_kind = PartitionKind.SINGLE_VALUE_PARTITIONS

    @classmethod
    def on_init(cls, params: InitParams[_ProjectedOutArgs]) -> GlobalInitResponse:
        items = [struct.pack(_QUEUE_ITEM_FMT, i) for i in range(len(_CATEGORIES))]
        params.storage.queue_push(items)
        return GlobalInitResponse()

    @classmethod
    def initial_state(cls, params: ProcessParams[_ProjectedOutArgs]) -> _ProjectedOutState:
        return _ProjectedOutState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[_ProjectedOutArgs],
        state: _ProjectedOutState,
        out: OutputCollector,
    ) -> None:
        if not state.started or state.current_idx >= params.args.rows_per_category:
            item = params.storage.queue_pop()
            if item is None:
                out.finish()
                return
            (state.current_category_idx,) = struct.unpack(_QUEUE_ITEM_FMT, item)
            state.current_idx = 0
            state.started = True

        category = _CATEGORIES[state.current_category_idx]
        rpc = params.args.rows_per_category
        revenue = [(state.current_category_idx + 1) * 100 + i for i in range(rpc)]
        batch = pa.RecordBatch.from_pydict(
            {"category": [category] * rpc, "revenue": revenue},
            schema=cls.FIXED_SCHEMA,
        )
        out.emit(
            batch,
            partition_values={
                "category": (
                    pa.scalar(category, type=pa.string()),
                    pa.scalar(category, type=pa.string()),
                ),
            },
        )
        state.current_idx = rpc


# =============================================================================
# DISJOINT_PARTITIONS — wire-level declaration only
# =============================================================================


@dataclass(slots=True, frozen=True)
class _DisjointArgs:
    """Arguments for ``disjoint_range_partitioned``."""

    partitions: Annotated[int, Arg(0, doc="Number of disjoint partitions", ge=1)]
    rows_per_partition: Annotated[int, Arg("rows_per_partition", default=10, doc="Rows per partition", ge=1)]


@dataclass(kw_only=True)
class _DisjointState(ArrowSerializableDataclass):
    current_partition_idx: int = -1
    current_idx: int = 0
    started: bool = False


@bind_fixed_schema
@_cardinality_from_count
class DisjointRangePartitionedFunction(TableFunctionGenerator[_DisjointArgs, _DisjointState]):
    """Per-chunk disjoint integer ranges on ``key``.

    Each chunk N emits ``key`` values in ``[N*1000, N*1000 + rows)``
    — disjoint across partitions. Declares
    ``DISJOINT_PARTITIONS``; the C++ extension propagates this to
    DuckDB's ``get_partition_info``. DuckDB doesn't have a consumer
    for DISJOINT today, so GROUP BY queries fall back to
    ``HASH_GROUP_BY`` (verified by the integration test).

    Purpose: verify the wire path (declaration, per-batch min/max
    metadata, C++ extraction) works for the non-SINGLE_VALUE kinds.
    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            partition_field("key", pa.int64()),
            pa.field("value", pa.int64()),
        ]
    )

    class Meta:
        name = "disjoint_range_partitioned"
        description = (
            "Disjoint per-chunk integer ranges on ``key``. Declares "
            "DISJOINT_PARTITIONS (wire-level only; DuckDB falls back to "
            "HASH_GROUP_BY for now)."
        )
        categories = ["generator", "partitioning", "testing"]
        partition_kind = PartitionKind.DISJOINT_PARTITIONS

    @classmethod
    def on_init(cls, params: InitParams[_DisjointArgs]) -> GlobalInitResponse:
        items = [struct.pack(_QUEUE_ITEM_FMT, i) for i in range(params.args.partitions)]
        params.storage.queue_push(items)
        return GlobalInitResponse()

    @classmethod
    def initial_state(cls, params: ProcessParams[_DisjointArgs]) -> _DisjointState:
        return _DisjointState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[_DisjointArgs],
        state: _DisjointState,
        out: OutputCollector,
    ) -> None:
        if not state.started or state.current_idx >= params.args.rows_per_partition:
            item = params.storage.queue_pop()
            if item is None:
                out.finish()
                return
            (state.current_partition_idx,) = struct.unpack(_QUEUE_ITEM_FMT, item)
            state.current_idx = 0
            state.started = True

        rpp = params.args.rows_per_partition
        base = state.current_partition_idx * 1000
        keys = [base + i for i in range(rpp)]
        values = [state.current_partition_idx * 10 + i for i in range(rpp)]
        batch = pa.RecordBatch.from_pydict(
            {"key": keys, "value": values},
            schema=cls.FIXED_SCHEMA,
        )
        out.emit(batch)
        state.current_idx = rpp
