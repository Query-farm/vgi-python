# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Example worker with built-in functions for testing.

This demonstrates how to create a worker by subclassing Worker
and listing function classes. Function names are derived from
each class's metadata (Meta.name or snake_case of class name).

The worker supports:
- TableInOutGenerator: Transforms input batches to output batches
- TableFunctionGenerator: Generates output batches without input
- ScalarFunctionGenerator: Transforms input to single-column output (1:1 rows)

Settings:
- vgi_verbose_mode: Enable verbose output with extra columns (bool, default: false)
- greeting: Custom greeting message (str, default: "Hello")
- multiplier: Value multiplier (int, default: 1)
- threshold: Filter threshold for filter_by_setting (int, default: 0)
- config: Sequence configuration struct for struct_settings (struct, default: None)

Usage:
    vgi-fixture-worker
"""

# Friendly error if numpy is missing. Several fixture modules below depend on
# numpy, which the `vgi-fixtures` distribution installs; surface a clear install
# message instead of a raw ImportError.
try:
    import numpy  # noqa: F401
except ImportError:
    import sys as _sys

    _sys.exit("vgi-fixture-worker requires numpy. Install it with: pip install 'vgi-python[test-fixtures]'")

import uuid
from typing import Annotated, Any

import pyarrow as pa

from vgi._test_fixtures.aggregate import (
    AvgFunction,
    CountFunction,
    DynamicAggregateFunction,
    DynamicMLAggregateFunction,
    GenericSumFunction,
    ListAggFunction,
    PercentileFunction,
    StreamingSumFunction,
    SumAllFunction,
    SumFunction,
    WeightedSumFunction,
    WindowListAggFunction,
    WindowMedianFunction,
    WindowSumBatchFunction,
    WindowSumFunction,
)
from vgi._test_fixtures.cancellable import (
    SlowCancellableBufferingFunction,
    SlowCancellableFunction,
    SlowCancellableInOutFunction,
)
from vgi._test_fixtures.nest_tensor import NestTensorFunction, UnnestTensorFunction, UnnestTensorRowsFunction
from vgi._test_fixtures.scalar import (
    AddValuesFunction,
    AnyMixedIntFunction,
    AnyMixedStrFunction,
    BernoulliFunction,
    BinaryPacketFunction,
    ConcatValuesIntFunction,
    ConcatValuesStrFunction,
    ConditionalMessageFunction,
    DoubleFunction,
    FormatNumberDefaultFunction,
    FormatNumberFullFunction,
    FormatNumberPrecisionFunction,
    GeoCentroidFixedFunction,
    GeoCentroidListFunction,
    GeoCentroidStructFunction,
    GeoDistanceFixedFunction,
    GeoDistanceListFunction,
    GeoDistanceStructFunction,
    HashSeedFunction,
    MultiplyBySettingFunction,
    MultiplyFunction,
    NullHandlingFunction,
    PairTypeIntIntFunction,
    PairTypeIntStrFunction,
    PairTypeStrStrFunction,
    RandomBytesFunction,
    RandomIntFunction,
    ReturnSecretValueFunction,
    ScaleBySettingFunction,
    SecretFieldFunction,
    SmartFormatPrefixFunction,
    SmartFormatWidthFunction,
    SumValuesFunction,
    TypeInfoInt32Function,
    TypeInfoInt64Function,
    TypeInfoStringFunction,
    TypeInfoUInt32Function,
    TypeInfoUInt64Function,
    UpperCaseFunction,
    WhoAmIFunction,
)
from vgi._test_fixtures.table import (
    _VERSIONED_CONSTRAINTS_SCHEMAS,
    _VERSIONED_SCHEMAS,
    RFF_MULTI_COLUMNS,
    RFF_NESTED_COLUMNS,
    RFF_NONE_COLUMNS,
    RFF_ROWID_COLUMNS,
    RFF_SIMPLE_COLUMNS,
    RFF_STRUCT_COLUMNS,
    BatchIndexOverflowFunction,
    BrokenMissingPartitionValuesFunction,
    BrokenPartitionColumnAbsentFromBatchFunction,
    BrokenPartitionMinNeqMaxFunction,
    BrokenPartitionValuesNoAnnotationFunction,
    ColorsScanFunction,
    ConstantColumnsFunction,
    CountryPartitionedSalesFunction,
    DepartmentsScanFunction,
    DictFilterEchoFunction,
    DisjointRangePartitionedFunction,
    DoubleSequenceFunction,
    DynamicFilterEchoFunction,
    EmployeesScanFunction,
    ExpressionFilterTestFunction,
    FilterEchoFunction,
    FilterEchoPartitionedFunction,
    FilterEchoTableScanFunction,
    FilteredColumnsEchoFunction,
    GeneratorExceptionFunction,
    LateMaterializationFunction,
    LoggingGeneratorFunction,
    MakePairsIntFunction,
    MakePairsIntStrFunction,
    MakePairsStrFunction,
    MakeSeriesCountFunction,
    MakeSeriesCsvFunction,
    MakeSeriesFloatFunction,
    MakeSeriesRangeFunction,
    MakeSeriesStepFunction,
    MissingBatchIndexTagFunction,
    NamedParamsEchoFunction,
    NestedSequenceFunction,
    NonMonotoneBatchIndexFunction,
    OrderEchoFunction,
    PartitionedBatchIndexFunction,
    PartitionedBatchIndexMarkedFunction,
    PartitionedFixedOrderFunction,
    PartitionedNoOrderGuaranteeFunction,
    PartitionedPreservesOrderFunction,
    PartitionedSequenceFunction,
    PartitionedWithExplicitOverrideFunction,
    ProductsScanFunction,
    ProfilingDemoFunction,
    ProjectedDataFunction,
    ProjectsScanFunction,
    RegionYearPartitionedFunction,
    RepeatValueIntFunction,
    RepeatValueStrFunction,
    RffMultiScanFunction,
    RffNestedScanFunction,
    RffNoneScanFunction,
    RffRowidScanFunction,
    RffSimpleScanFunction,
    RffStructScanFunction,
    RowIdSequenceFunction,
    SampleEchoFunction,
    ScopedSecretDemoFunction,
    SecretDemoFunction,
    SequenceFunction,
    SettingsAwareFunction,
    SpatialFilterExampleFunction,
    StructSettingsFunction,
    TenThousandFunction,
    TxCachedValueFunction,
    TypedProbeFunction,
    ValuePruneFunction,
    VersionedConstraintsScanFunction,
    VersionedDataFunction,
    resolve_version,
    resolve_versioned_constraints_version,
)
from vgi._test_fixtures.table.tt_pushdown import (
    _TT_SCHEMA,
    TimeTravelPushdownFunction,
    TtPushdownColsScanFunction,
    resolve_tt_version,
)
from vgi._test_fixtures.table_in_out import (
    BatchIndexBufferInputFunction,
    BufferEmitWideFunction,
    BufferInputFunction,
    CrashOnCombineFunction,
    CrashOnFinalizeFunction,
    CrashOnProcessFunction,
    EchoBufferingFunction,
    EchoFunction,
    EchoWitnessFunction,
    ExceptionFinalizeFunction,
    ExceptionProcessFunction,
    FilterBySettingFunction,
    HangOnProcessFunction,
    LargeStateFunction,
    OrderedBufferInputFunction,
    OrderedSourceFunction,
    RepeatInputsFunction,
    SumAllColumnsFunction,
    SumAllColumnsSimpleDistributed,
)
from vgi.arguments import Arguments
from vgi.catalog import (
    AttachOpaqueData,
    Catalog,
    ForeignKeyDef,
    Index,
    IndexConstraintType,
    Macro,
    MacroType,
    ReadOnlyCatalogInterface,
    ScanBranch,
    ScanBranchesResult,
    ScanFunctionResult,
    Schema,
    SecretTypeSpec,
    SerializedSchema,
    Setting,
    Table,
    TableInfo,
    TransactionOpaqueData,
    View,
)
from vgi.catalog.catalog_interface import _validate_at_params
from vgi.catalog.descriptors import ColumnStatisticsInput
from vgi.catalog.duckdb_statistics import statistics_from_duckdb
from vgi.schema_utils import schema
from vgi.worker import Worker


# ---------------------------------------------------------------------------
# DuckDB-backed table: demonstrates statistics_from_duckdb() helper.
# Creates an in-memory table and extracts real statistics from it.
# ---------------------------------------------------------------------------
def _build_numbers_stats() -> dict[str, ColumnStatisticsInput]:
    """Extract statistics for the 'numbers' table (integers 0-99) from DuckDB.

    Demonstrates the ``statistics_from_duckdb()`` helper by creating the same
    data in a DuckDB in-memory table and pulling real statistics from it.
    """
    from vgi._duckdb import connect as engine_connect

    conn = engine_connect()
    conn.execute("CREATE TABLE numbers AS SELECT unnest(range(100)) AS value")
    stats = statistics_from_duckdb(conn, "numbers")
    conn.close()
    return stats


_NUMBERS_STATS = _build_numbers_stats()


def _build_geo_stats() -> tuple[pa.Schema, dict[str, ColumnStatisticsInput]]:
    """Build a geometry table in DuckDB and extract spatial statistics.

    Creates a 5x5 grid of points (0,0) to (4,4) with an integer ID.
    Demonstrates geometry statistics via ``statistics_from_duckdb()``.
    """
    from vgi._duckdb import connect as engine_connect

    conn = engine_connect()
    # INSTALL is a no-op when the extension is already cached; fresh
    # environments (CI runners) need the download before LOAD.
    conn.execute("INSTALL spatial")
    conn.execute("LOAD spatial")
    conn.execute(
        "CREATE TABLE geo_points AS "
        "SELECT row_number() OVER () AS id, "
        "ST_Point(x::DOUBLE, y::DOUBLE)::GEOMETRY AS geom "
        "FROM range(5) t1(x), range(5) t2(y)"
    )
    schema = conn.execute("SELECT * FROM geo_points LIMIT 0").to_arrow_table().schema
    stats = statistics_from_duckdb(conn, "geo_points")
    conn.close()
    return schema, stats


_GEO_SCHEMA, _GEO_STATS = _build_geo_stats()


def _build_enum_stats() -> dict[str, ColumnStatisticsInput]:
    """Extract statistics for a table with ENUM (dictionary-encoded) columns.

    Demonstrates that ``statistics_from_duckdb()`` correctly unwraps
    dictionary-encoded min/max to actual string values rather than
    returning dictionary indices.
    """
    from vgi._duckdb import connect as engine_connect

    conn = engine_connect()
    conn.execute("CREATE TYPE color AS ENUM ('red', 'green', 'blue')")
    conn.execute(
        "CREATE TABLE colors AS "
        "SELECT unnest(range(3)) + 1 AS id, "
        "unnest(['red', 'green', 'blue'])::color AS color, "
        "unnest(['#FF0000', '#00FF00', '#0000FF']) AS hex_code"
    )
    stats = statistics_from_duckdb(conn, "colors")
    conn.close()
    return stats


_ENUM_STATS = _build_enum_stats()

_EXAMPLE_CATALOG = Catalog(
    name="example",
    default_schema="main",
    comment="Example VGI catalog for testing",
    tags={"source": "vgi-fixture-worker", "version": "1"},
    schemas=[
        Schema(
            name="main",
            comment="Example functions for testing VGI",
            functions=[
                # TableInOutGenerator - transform input batches
                EchoFunction,
                EchoWitnessFunction,
                BufferInputFunction,
                FilterBySettingFunction,
                RepeatInputsFunction,
                SlowCancellableInOutFunction,
                SumAllColumnsFunction,
                SumAllColumnsSimpleDistributed,
                UnnestTensorRowsFunction,
                ExceptionFinalizeFunction,
                ExceptionProcessFunction,
                CrashOnProcessFunction,
                CrashOnCombineFunction,
                CrashOnFinalizeFunction,
                HangOnProcessFunction,
                LargeStateFunction,
                OrderedBufferInputFunction,
                OrderedSourceFunction,
                BatchIndexBufferInputFunction,
                EchoBufferingFunction,
                BufferEmitWideFunction,
                SlowCancellableBufferingFunction,
                # TableFunctionGenerator - generate output without input
                ConstantColumnsFunction,
                SlowCancellableFunction,
                FilterEchoFunction,
                FilterEchoPartitionedFunction,
                FilterEchoTableScanFunction,
                FilteredColumnsEchoFunction,
                ValuePruneFunction,
                LateMaterializationFunction,
                DictFilterEchoFunction,
                DoubleSequenceFunction,
                DynamicFilterEchoFunction,
                GeneratorExceptionFunction,
                LoggingGeneratorFunction,
                MakeSeriesCountFunction,
                MakeSeriesCsvFunction,
                MakeSeriesFloatFunction,
                MakeSeriesRangeFunction,
                MakeSeriesStepFunction,
                TypedProbeFunction,
                MakePairsIntFunction,
                MakePairsIntStrFunction,
                MakePairsStrFunction,
                RepeatValueIntFunction,
                RepeatValueStrFunction,
                NamedParamsEchoFunction,
                NestedSequenceFunction,
                ProfilingDemoFunction,
                OrderEchoFunction,
                PartitionedBatchIndexFunction,
                PartitionedBatchIndexMarkedFunction,
                PartitionedFixedOrderFunction,
                PartitionedNoOrderGuaranteeFunction,
                PartitionedPreservesOrderFunction,
                PartitionedSequenceFunction,
                # PartitionColumns (Hive-style partitioning) reference fixtures
                # — see vgi/_test_fixtures/table/partition_columns.py.
                CountryPartitionedSalesFunction,
                DisjointRangePartitionedFunction,
                PartitionedWithExplicitOverrideFunction,
                RegionYearPartitionedFunction,
                # Deliberately-broken batch_index fixtures (see
                # vgi/_test_fixtures/table/batch_index_broken.py). Registered
                # so SQL integration tests in batch_index_contract.test can
                # call them and assert the C++ extension's contract checks
                # fire as typed IOExceptions.
                BatchIndexOverflowFunction,
                MissingBatchIndexTagFunction,
                NonMonotoneBatchIndexFunction,
                # Deliberately-broken PartitionColumns fixtures (see
                # vgi/_test_fixtures/table/partition_columns_broken.py).
                BrokenMissingPartitionValuesFunction,
                BrokenPartitionColumnAbsentFromBatchFunction,
                BrokenPartitionMinNeqMaxFunction,
                BrokenPartitionValuesNoAnnotationFunction,
                ProjectedDataFunction,
                SampleEchoFunction,
                RowIdSequenceFunction,
                SecretDemoFunction,
                ScopedSecretDemoFunction,
                ExpressionFilterTestFunction,
                SequenceFunction,
                SettingsAwareFunction,
                SpatialFilterExampleFunction,
                StructSettingsFunction,
                TenThousandFunction,
                TxCachedValueFunction,
                VersionedDataFunction,
                # Time-travel + filter-pushdown fixtures (one function-backed, one
                # columns-based) — back time_travel_pushdown.test.
                TimeTravelPushdownFunction,
                TtPushdownColsScanFunction,
                # Static data scan functions for constraint-backed tables
                ColorsScanFunction,
                DepartmentsScanFunction,
                EmployeesScanFunction,
                ProductsScanFunction,
                ProjectsScanFunction,
                VersionedConstraintsScanFunction,
                # rff_* scan functions back the Tables exercised by the
                # vgi_required_filters_*.test sqllogictest matrix.
                RffMultiScanFunction,
                RffNestedScanFunction,
                RffNoneScanFunction,
                RffRowidScanFunction,
                RffSimpleScanFunction,
                RffStructScanFunction,
                # ScalarFunctionGenerator - transform to single-column output
                AddValuesFunction,
                BernoulliFunction,
                BinaryPacketFunction,
                ConcatValuesIntFunction,
                ConcatValuesStrFunction,
                ConditionalMessageFunction,
                DoubleFunction,
                FormatNumberDefaultFunction,
                FormatNumberFullFunction,
                FormatNumberPrecisionFunction,
                GeoCentroidFixedFunction,
                GeoCentroidListFunction,
                GeoCentroidStructFunction,
                GeoDistanceFixedFunction,
                GeoDistanceListFunction,
                GeoDistanceStructFunction,
                HashSeedFunction,
                MultiplyBySettingFunction,
                MultiplyFunction,
                NullHandlingFunction,
                PairTypeIntIntFunction,
                PairTypeIntStrFunction,
                PairTypeStrStrFunction,
                RandomBytesFunction,
                RandomIntFunction,
                ReturnSecretValueFunction,
                ScaleBySettingFunction,
                SecretFieldFunction,
                SmartFormatPrefixFunction,
                SmartFormatWidthFunction,
                SumValuesFunction,
                TypeInfoInt32Function,
                TypeInfoInt64Function,
                TypeInfoStringFunction,
                TypeInfoUInt32Function,
                TypeInfoUInt64Function,
                AnyMixedIntFunction,
                AnyMixedStrFunction,
                UnnestTensorFunction,
                UpperCaseFunction,
                WhoAmIFunction,
                # AggregateFunction - aggregate input rows
                AvgFunction,
                CountFunction,
                DynamicAggregateFunction,
                DynamicMLAggregateFunction,
                GenericSumFunction,
                ListAggFunction,
                NestTensorFunction,
                PercentileFunction,
                StreamingSumFunction,
                SumAllFunction,
                SumFunction,
                WeightedSumFunction,
                WindowListAggFunction,
                WindowMedianFunction,
                WindowSumBatchFunction,
                WindowSumFunction,
            ],
            views=[
                View(
                    name="first_ten",
                    definition="SELECT * FROM sequence(10)",
                    comment="First 10 integers",
                    column_comments={"n": "Sequence index 0..9"},
                    tags={"layer": "demo", "origin": "sequence"},
                ),
                View(
                    name="even_numbers",
                    definition="SELECT * FROM sequence(100) WHERE n % 2 = 0",
                    comment="Even numbers from 0 to 98",
                ),
            ],
            macros=[
                Macro(
                    name="vgi_multiply",
                    macro_type=MacroType.SCALAR,
                    parameters=["x", "y"],
                    definition="x * y",
                    comment="Multiply two values",
                ),
                Macro(
                    name="vgi_clamp",
                    macro_type=MacroType.SCALAR,
                    parameters=["val", "lo", "hi"],
                    parameter_default_values=pa.RecordBatch.from_pydict(
                        {"lo": [pa.scalar(0).as_py()], "hi": [pa.scalar(100).as_py()]},
                        schema=schema(lo=pa.int64(), hi=pa.int64()),
                    ),
                    definition="GREATEST(lo, LEAST(hi, val))",
                    comment="Clamp a value between lo and hi (defaults: 0..100)",
                ),
                Macro(
                    name="vgi_range_table",
                    macro_type=MacroType.TABLE,
                    parameters=["n"],
                    definition="SELECT * FROM range(n)",
                    comment="Table macro returning range of values",
                ),
            ],
        ),
        Schema(
            name="data",
            comment="Example tables backed by functions",
            tables=[
                # Function-backed table: schema derived via bind()
                Table(
                    name="large_sequence",
                    function=SequenceFunction,
                    arguments=Arguments(positional=(pa.scalar(1_000_000),)),
                    statistics={
                        "n": ColumnStatisticsInput(min=0, max=999_999, has_null=False, distinct_count=1_000_000),
                    },
                    statistics_cache_max_age_seconds=3600,
                    comment="A large sequence of integers from 0 to 1,000,000",
                ),
                # Function-backed table with a no-arg function. Used by the
                # ``inlined_scan_function.test`` integration test to verify
                # the C++ extension reads the inlined ``scan_function`` from
                # ``TableInfo`` and skips ``catalog_table_scan_function_get``.
                Table(
                    name="ten_thousand_table",
                    function=TenThousandFunction,
                    comment="Function-backed table over the no-arg ten_thousand function",
                ),
                # Function-backed table with inlined cardinality. Used by the
                # ``inlined_cardinality.test`` integration test to verify the
                # C++ extension uses ``Table.cardinality_estimate`` /
                # ``cardinality_max`` from ``TableInfo`` and skips the per-bind
                # ``table_function_cardinality`` RPC.
                Table(
                    name="cardinality_inlined_table",
                    function=TenThousandFunction,
                    cardinality_estimate=10000,
                    cardinality_max=10000,
                    comment="Function-backed table with inlined cardinality (10000 rows)",
                ),
                # Time-travel table: version-specific schema
                Table(
                    name="versioned_data",
                    columns=schema(id=pa.int64(), score=pa.float64()),
                    supports_time_travel=True,
                    comment="Versioned data table demonstrating time travel with schema evolution",
                ),
                # Time travel + filter pushdown together. tt_pushdown_fn is
                # function-backed (reads AT at init); tt_pushdown_cols is
                # columns-based (AT → version arg via table_scan_function_get).
                Table(
                    name="tt_pushdown_fn",
                    function=TimeTravelPushdownFunction,
                    supports_time_travel=True,
                    comment="Function-backed: prunes by filter AND time-travels (AT read at init).",
                ),
                Table(
                    name="tt_pushdown_cols",
                    columns=_TT_SCHEMA,
                    supports_time_travel=True,
                    comment="Columns-based: prunes by filter AND time-travels (AT → version arg).",
                ),
                # Explicit columns table with statistics extracted from DuckDB
                # via statistics_from_duckdb() — demonstrates the helper workflow
                Table(
                    name="numbers",
                    columns=schema(value=pa.int64()),
                    statistics=_NUMBERS_STATS,
                    statistics_cache_max_age_seconds=3600,
                    comment="First 100 integers (demonstrates explicit columns)",
                ),
                # Geometry table with spatial statistics from DuckDB
                Table(
                    name="geo_points",
                    columns=_GEO_SCHEMA,
                    statistics=_GEO_STATS,
                    statistics_cache_max_age_seconds=3600,
                    comment="5x5 grid of points with spatial statistics",
                ),
                # Table with TTL=0 (never cache) for cache expiry testing
                Table(
                    name="volatile_numbers",
                    columns=schema(value=pa.int64()),
                    statistics={
                        "value": ColumnStatisticsInput(min=0, max=99, has_null=False, distinct_count=100),
                    },
                    statistics_cache_max_age_seconds=0,
                    comment="Numbers with volatile stats (TTL=0, always re-fetched)",
                ),
                # Table with NO declared statistics — stats must come from the underlying
                # scan function (SequenceFunction.statistics) via table_function_statistics RPC.
                # Column name matches the function output ("n") so no rename is needed.
                Table(
                    name="funny_numbers",
                    columns=schema(n=pa.int64()),
                    comment="123456 integers; stats served by the sequence function, not the table",
                ),
                # Multi-branch fixture — two ScanBranch entries both calling
                # sequence() with different counts. SELECT count(*) should
                # return 100 (50 + 50). Exercises VgiMultiScanRewriter end-to-end.
                Table(
                    name="multi_branch_numbers",
                    columns=schema(n=pa.int64()),
                    comment="Multi-branch: UNION of sequence(50) + sequence(50) — used by multi_branch_scan.test",
                ),
                # Multi-branch with branch_filters that partition the value range.
                # Branch A: sequence(100) with `n < 50`; branch B: sequence(100)
                # with `n >= 50`. Non-overlapping; total rows = 100.
                Table(
                    name="multi_branch_filtered_numbers",
                    columns=schema(n=pa.int64()),
                    comment="Multi-branch with complementary branch_filters — exercises pruning",
                ),
                # Heterogeneous multi-branch: one VGI arm + one native read_parquet
                # arm. The parquet file is created by the test at a well-known path
                # (see multi_branch_heterogeneous.test). Demonstrates that cold-tier
                # data can come from any DuckDB function the worker names, without
                # tunneling through the worker pipe.
                Table(
                    name="multi_branch_hetero",
                    columns=schema(n=pa.int64()),
                    comment="Multi-branch: sequence(50) + read_parquet — used by multi_branch_heterogeneous.test",
                ),
                # Column reconciliation: 3 read_parquet branches, the test creates
                # the parquet files with deliberately different column orders and
                # a missing column on one branch. Canonical schema (a, b) is
                # populated by name; missing columns NULL-fill.
                Table(
                    name="multi_branch_recon",
                    columns=schema(a=pa.int64(), b=pa.int64()),
                    comment="Multi-branch: column reconciliation — used by multi_branch_reconciliation.test",
                ),
                # Pushdown-incapable arm test (E3): one VGI sequence() arm
                # (filter_pushdown=True) + one read_csv arm (read_csv lacks
                # native filter pushdown, so filters stay as LogicalFilter
                # above the scan). Tests that the rewriter doesn't assume
                # pushdown always succeeds.
                Table(
                    name="multi_branch_nopushdown",
                    columns=schema(n=pa.int64()),
                    comment="Multi-branch: VGI + read_csv — used by multi_branch_pushdown_incapable.test",
                ),
                # Empty-branches loud-fail test (E6): worker returns
                # branches=[] from table_scan_branches_get. The C++ side's
                # ParseScanBranchesResult must reject this at the wire layer
                # with a BinderException before any plan is built.
                Table(
                    name="multi_branch_empty",
                    columns=schema(n=pa.int64()),
                    comment="Multi-branch: empty branches list — used by multi_branch_empty_branches.test",
                ),
                # Parse-time rejection — worker returns two ScanBranch
                # entries both with writable=True. ParseScanBranchesResult
                # must throw BinderException citing DuckDB's
                # single-writable-catalog rule. See multi_branch_two_writable.test.
                Table(
                    name="multi_branch_two_writable",
                    columns=schema(n=pa.int64()),
                    comment="Multi-branch with two writable=True arms — used by multi_branch_two_writable.test",
                ),
                # ENUM (dictionary-encoded) column table — tests that statistics
                # report actual string values, not dictionary indices.
                Table(
                    name="colors",
                    columns=schema(id=pa.int64(), color=pa.string(), hex_code=pa.string()),
                    statistics=_ENUM_STATS,
                    statistics_cache_max_age_seconds=3600,
                    comment="Colors table with ENUM-derived statistics",
                ),
                # Row ID position tests (int64 row_id)
                Table(
                    name="rowid_first",
                    columns=schema(
                        row_id=(pa.int64(), {b"is_row_id": b""}),
                        name=pa.string(),
                        value=pa.string(),
                    ),
                    comment="Table with row_id at column index 0",
                ),
                Table(
                    name="rowid_middle",
                    columns=schema(
                        name=pa.string(),
                        row_id=(pa.int64(), {b"is_row_id": b""}),
                        value=pa.string(),
                    ),
                    comment="Table with row_id at column index 1",
                ),
                Table(
                    name="rowid_last",
                    columns=schema(
                        name=pa.string(),
                        value=pa.string(),
                        row_id=(pa.int64(), {b"is_row_id": b""}),
                    ),
                    comment="Table with row_id at column index 2",
                ),
                # Row ID type tests (row_id at index 0)
                Table(
                    name="rowid_string",
                    columns=schema(
                        row_id=(pa.string(), {b"is_row_id": b""}),
                        value=pa.int64(),
                    ),
                    comment="Table with string row_id",
                ),
                Table(
                    name="rowid_struct",
                    columns=schema(
                        row_id=(
                            pa.struct([("a", pa.int64()), ("b", pa.string())]),
                            {b"is_row_id": b""},
                        ),
                        value=pa.string(),
                    ),
                    comment="Table with struct row_id",
                ),
                # ----- Late-materialization tables (rowid + scrambled ord) -----
                # Backed by the late_materialization scan function, which
                # advertises Meta.late_materialization. The row_id is the row
                # index (unique/deterministic/snapshot-stable); ord is a
                # scrambled function of the index so a Top-N on ord yields
                # scattered survivor rowids. pushed echoes the rowid filter the
                # worker received. See late_materialization.test.
                Table(
                    name="late_mat",
                    columns=schema(
                        row_id=(pa.int64(), {b"is_row_id": b""}),
                        ord=pa.int64(),
                        payload=pa.string(),
                        pushed=pa.string(),
                    ),
                    comment="Late-materialization table (1000 rows, unique rowid)",
                ),
                Table(
                    name="late_mat_dup",
                    columns=schema(
                        row_id=(pa.int64(), {b"is_row_id": b""}),
                        ord=pa.int64(),
                        payload=pa.string(),
                        pushed=pa.string(),
                    ),
                    comment="Late-materialization table with deliberately non-unique rowid (contract violation)",
                ),
                Table(
                    name="late_mat_nulls",
                    columns=schema(
                        row_id=(pa.int64(), {b"is_row_id": b""}),
                        ord=pa.int64(),
                        payload=pa.string(),
                        pushed=pa.string(),
                    ),
                    comment="Late-materialization table with NULLs in the ord column",
                ),
                # ----- Generated column example table -----
                Table(
                    name="generated_sequence",
                    columns=schema(n=pa.int64(), doubled=pa.int64(), label=pa.string()),
                    generated_columns={
                        "doubled": "n * 2",
                        "label": "'item_' || CAST(n AS VARCHAR)",
                    },
                    comment="Table with generated columns backed by sequence(10)",
                ),
                # ----- Constraint example tables -----
                Table(
                    name="departments",
                    columns=schema(id=pa.int64(), name=pa.string(), budget=pa.float64()),
                    primary_key=(("id",),),
                    not_null=("id", "name"),
                    unique=(("name",),),
                    check=("budget >= 0",),
                    defaults={"budget": 0},
                    statistics={
                        "id": ColumnStatisticsInput(min=1, max=10, has_null=False, distinct_count=10),
                        "name": ColumnStatisticsInput(
                            min="Accounting",
                            max="Sales",
                            has_null=False,
                            distinct_count=10,
                            contains_unicode=False,
                            max_string_length=20,
                        ),
                        "budget": ColumnStatisticsInput(min=50000.0, max=500000.0, has_null=False, distinct_count=10),
                    },
                    statistics_cache_max_age_seconds=3600,
                    comment="Department reference table",
                ),
                Table(
                    name="products",
                    columns=schema(
                        id=pa.int64(),
                        name=pa.string(),
                        quantity=pa.int64(),
                        price=pa.float64(),
                    ),
                    not_null=("id",),
                    primary_key=(("id",),),
                    defaults={
                        "quantity": 0,
                        "name": "unknown",
                        "price": 9.99,
                    },
                    column_comments={
                        "id": "Unique product identifier",
                        "name": "Product display name",
                        "price": "Unit price in USD",
                    },
                    statistics={
                        "id": ColumnStatisticsInput(min=1, max=100, has_null=False, distinct_count=100),
                        "name": ColumnStatisticsInput(
                            min="Anvil",
                            max="Zebra Tape",
                            has_null=False,
                            distinct_count=100,
                            contains_unicode=False,
                            max_string_length=30,
                        ),
                        "quantity": ColumnStatisticsInput(min=0, max=10000, has_null=True, distinct_count=50),
                        "price": ColumnStatisticsInput(min=0.99, max=999.99, has_null=False, distinct_count=80),
                    },
                    statistics_cache_max_age_seconds=3600,
                    comment="Product table with column defaults",
                ),
                Table(
                    name="employees",
                    columns=schema(
                        id=pa.int64(),
                        name=pa.string(),
                        email=pa.string(),
                        department_id=pa.int64(),
                    ),
                    primary_key=(("id",),),
                    not_null=("id", "name", "email"),
                    unique=(("email",),),
                    foreign_key=(
                        ForeignKeyDef(
                            columns=("department_id",),
                            referenced_table="departments",
                            referenced_columns=("id",),
                        ),
                    ),
                    comment="Employee table with FK to departments",
                ),
                Table(
                    name="projects",
                    columns=schema(
                        department_id=pa.int64(),
                        project_code=pa.string(),
                        title=pa.string(),
                    ),
                    primary_key=(("department_id", "project_code"),),
                    not_null=("department_id", "project_code", "title"),
                    foreign_key=(
                        ForeignKeyDef(
                            columns=("department_id",),
                            referenced_table="departments",
                            referenced_columns=("id",),
                        ),
                    ),
                    comment="Projects with composite PK and FK to departments",
                ),
                # filter_echo_table — catalog table that echoes the pushed-down
                # filters it received (pushed_filters column). Backs
                # ~/Development/vgi/test/sql/integration/table/filter_pushdown_through_view.test,
                # which characterizes filter pushdown directly and through a VIEW.
                # The backing scan opts into expression-filter pushdown so a
                # `LIKE 'prefix%'` predicate is observable here.
                Table(
                    name="filter_echo_table",
                    columns=schema(n=pa.int64(), s=pa.utf8(), pushed_filters=pa.utf8()),
                    comment="Catalog table echoing pushed-down filters (filter-pushdown-through-view tests).",
                ),
                # ----- required_field_filter_paths fixtures -----
                # Exercised by ~/Development/vgi/test/sql/vgi_required_filters_*.test
                # to verify the C++ optimizer extension that enforces the new
                # Table.required_field_filter_paths field.
                Table(
                    name="rff_simple",
                    columns=RFF_SIMPLE_COLUMNS,
                    required_field_filter_paths=("a",),
                    comment="rff_simple — requires a filter referencing column 'a'.",
                ),
                Table(
                    name="rff_struct",
                    columns=RFF_STRUCT_COLUMNS,
                    required_field_filter_paths=("s.a", "s.b"),
                    comment="rff_struct — requires filters on both struct subfields s.a and s.b.",
                ),
                Table(
                    name="rff_nested",
                    columns=RFF_NESTED_COLUMNS,
                    required_field_filter_paths=("wrapper.mid.leaf",),
                    comment="rff_nested — requires a filter on the 3-deep nested path wrapper.mid.leaf.",
                ),
                Table(
                    name="rff_multi",
                    columns=RFF_MULTI_COLUMNS,
                    required_field_filter_paths=("top", "s.a"),
                    comment="rff_multi — mixed top-level + struct subfield requirements.",
                ),
                Table(
                    name="rff_none",
                    columns=RFF_NONE_COLUMNS,
                    comment="rff_none — control table with no required_field_filter_paths (opt-out fast path).",
                ),
                Table(
                    name="rff_rowid",
                    columns=RFF_ROWID_COLUMNS,
                    required_field_filter_paths=(
                        "bbox.xmin",
                        "bbox.xmax",
                        "bbox.ymin",
                        "bbox.ymax",
                    ),
                    comment="rff_rowid — row_id virtual column + required bbox.* filters.",
                ),
                # rff_parquet — native read_parquet delegation + required_field_filter_paths
                # on a FLOAT bbox struct (mirrors Overture transportation.segment).
                Table(
                    name="rff_parquet",
                    columns=pa.schema(
                        [
                            pa.field(
                                "bbox",
                                pa.struct(
                                    [
                                        pa.field("xmin", pa.float32()),
                                        pa.field("ymin", pa.float32()),
                                        pa.field("xmax", pa.float32()),
                                        pa.field("ymax", pa.float32()),
                                    ]
                                ),
                            ),
                            pa.field("other", pa.int64()),
                        ]
                    ),
                    required_field_filter_paths=(
                        "bbox.xmin",
                        "bbox.xmax",
                        "bbox.ymin",
                        "bbox.ymax",
                    ),
                    comment="rff_parquet — native read_parquet delegation with bbox.* required filters.",
                ),
                # rff_hive — native read_parquet over a Hive-partitioned glob
                # (theme/type partition columns), bbox at a non-zero index —
                # closely mirrors Overture transportation.segment.
                Table(
                    name="rff_hive",
                    columns=pa.schema(
                        [
                            pa.field("id", pa.string()),
                            pa.field(
                                "bbox",
                                pa.struct(
                                    [
                                        pa.field("xmin", pa.float32()),
                                        pa.field("ymin", pa.float32()),
                                        pa.field("xmax", pa.float32()),
                                        pa.field("ymax", pa.float32()),
                                    ]
                                ),
                            ),
                            pa.field("name", pa.string()),
                            pa.field("num", pa.int64()),
                            pa.field("theme", pa.string()),
                            pa.field("type", pa.string()),
                        ]
                    ),
                    required_field_filter_paths=(
                        "bbox.xmin",
                        "bbox.xmax",
                        "bbox.ymin",
                        "bbox.ymax",
                    ),
                    comment="rff_hive — native read_parquet over Hive glob with bbox.* required filters.",
                ),
                # rff_hive_mixed — same Hive layout as rff_hive but a MIXED
                # requirement: a top-level field ('id') plus the struct corners.
                # Exercises the flat-field branch of the path walker over native
                # delegation, where 'id' sits at a permuted column_ids slot.
                Table(
                    name="rff_hive_mixed",
                    columns=pa.schema(
                        [
                            pa.field("id", pa.string()),
                            pa.field(
                                "bbox",
                                pa.struct(
                                    [
                                        pa.field("xmin", pa.float32()),
                                        pa.field("ymin", pa.float32()),
                                        pa.field("xmax", pa.float32()),
                                        pa.field("ymax", pa.float32()),
                                    ]
                                ),
                            ),
                            pa.field("name", pa.string()),
                            pa.field("num", pa.int64()),
                            pa.field("theme", pa.string()),
                            pa.field("type", pa.string()),
                        ]
                    ),
                    required_field_filter_paths=(
                        "id",
                        "bbox.xmin",
                        "bbox.xmax",
                        "bbox.ymin",
                        "bbox.ymax",
                    ),
                    comment="rff_hive_mixed — native read_parquet, top-level 'id' + bbox.* required filters.",
                ),
                # Time-travel constraint evolution table
                Table(
                    name="versioned_constraints",
                    columns=schema(
                        id=pa.int64(),
                        name=pa.string(),
                        email=pa.string(),
                        department_id=pa.int64(),
                    ),
                    supports_time_travel=True,
                    not_null=("id", "name"),
                    primary_key=(("id",),),
                    unique=(("email",),),
                    foreign_key=(
                        ForeignKeyDef(
                            columns=("department_id",),
                            referenced_table="departments",
                            referenced_columns=("id",),
                        ),
                    ),
                    comment="Table with constraints that evolve across versions",
                ),
            ],
            views=[
                View(
                    name="small_numbers",
                    definition="SELECT * FROM numbers WHERE value < 10",
                    comment="Numbers less than 10",
                    column_comments={"value": "Single-digit value 0..9"},
                ),
            ],
            indexes=[
                Index(
                    name="idx_numbers_value",
                    table_name="numbers",
                    expressions=("value",),
                    comment="Index on numbers.value",
                ),
                Index(
                    name="idx_numbers_value_unique",
                    table_name="numbers",
                    expressions=("value",),
                    constraint_type=IndexConstraintType.UNIQUE,
                    comment="Unique index on numbers.value",
                ),
            ],
        ),
    ],
)


class ExampleCatalog(ReadOnlyCatalogInterface):
    """Catalog interface for the example worker.

    Defines table_get and table_scan_function_get for tables with explicit
    columns, including time-travel support for versioned_data.

    """

    catalog = _EXAMPLE_CATALOG

    def table_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
        at_unit: str | None = None,
        at_value: str | None = None,
    ) -> TableInfo | None:
        """Return version-specific schema for time-travel tables."""
        _validate_at_params(at_unit, at_value)
        if schema_name.lower() == "data" and name.lower() == "versioned_data" and at_unit:
            version = resolve_version(at_unit, at_value)
            cols = _VERSIONED_SCHEMAS[version]
            return TableInfo(
                name=name,
                schema_name=schema_name,
                columns=SerializedSchema(cols.serialize().to_pybytes()),
                not_null_constraints=[],
                unique_constraints=[],
                check_constraints=[],
                comment="Versioned data table demonstrating time travel with schema evolution",
                tags={},
            )
        if schema_name.lower() == "data" and name.lower() == "versioned_constraints" and at_unit:
            version = resolve_versioned_constraints_version(at_unit, at_value)
            cols = _VERSIONED_CONSTRAINTS_SCHEMAS[version]
            # Constraints evolve with version:
            # V1: NOT NULL on id only
            # V2: NOT NULL on id+name, PK on id, UNIQUE on email
            # V3: NOT NULL on id+name, PK on id, UNIQUE on email, FK department_id→departments.id
            not_null: list[int] = []
            pk: list[list[int]] = []
            unique: list[list[int]] = []
            fk: list[bytes] = []
            col_names = [f.name for f in cols]
            if version >= 1:
                not_null.append(col_names.index("id"))
            if version >= 2:
                not_null.append(col_names.index("name"))
                pk.append([col_names.index("id")])
                unique.append([col_names.index("email")])
            if version >= 3:
                from vgi_rpc.utils import serialize_record_batch_bytes

                fk_batch = pa.RecordBatch.from_pydict(
                    {
                        "fk_columns": [["department_id"]],
                        "pk_columns": [["id"]],
                        "referenced_table": ["departments"],
                        "referenced_schema": [schema_name],
                    },
                    schema=pa.schema(
                        [
                            ("fk_columns", pa.list_(pa.utf8())),
                            ("pk_columns", pa.list_(pa.utf8())),
                            ("referenced_table", pa.utf8()),
                            ("referenced_schema", pa.utf8()),
                        ]
                    ),
                )
                fk.append(serialize_record_batch_bytes(fk_batch))
            return TableInfo(
                name=name,
                schema_name=schema_name,
                columns=SerializedSchema(cols.serialize().to_pybytes()),
                not_null_constraints=not_null,
                unique_constraints=unique,
                check_constraints=[],
                primary_key_constraints=pk,
                foreign_key_constraints=fk,
                comment="Table with constraints that evolve across versions",
                tags={},
            )
        # Multi-branch tables: accept AT at table_get and pass it through to
        # the underlying handler with AT stripped. The C++ side's B2 guard
        # in VgiTableEntry::GetScanFunctionImpl detects branches.size() > 1
        # and throws BinderException before any scan-function-get RPC fires.
        # Returning TableInfo here lets the C++ binding flow proceed far enough
        # to hit that guard with the documented error message.
        if schema_name.lower() == "data" and name.lower() in ("multi_branch_numbers", "multi_branch_filtered_numbers"):
            return super().table_get(
                attach_opaque_data=attach_opaque_data,
                transaction_opaque_data=transaction_opaque_data,
                schema_name=schema_name,
                name=name,
                at_unit=None,
                at_value=None,
            )
        return super().table_get(
            attach_opaque_data=attach_opaque_data,
            transaction_opaque_data=transaction_opaque_data,
            schema_name=schema_name,
            name=name,
            at_unit=at_unit,
            at_value=at_value,
        )

    def table_scan_branches_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
        at_unit: str | None,
        at_value: str | None,
    ) -> ScanBranchesResult:
        """Return multi-branch scan plans for the multi_branch_* test tables.

        Falls through to the CatalogInterface default-impl shim for every
        other table, which wraps the legacy table_scan_function_get result
        as a one-branch list.
        """
        _validate_at_params(at_unit, at_value)

        # multi_branch_numbers: two arms, each sequence(50). Union size = 100.
        if schema_name.lower() == "data" and name.lower() == "multi_branch_numbers":
            return ScanBranchesResult(
                branches=[
                    ScanBranch(
                        function_name="sequence",
                        positional_arguments=[pa.scalar(50)],
                        named_arguments={},
                    ),
                    ScanBranch(
                        function_name="sequence",
                        positional_arguments=[pa.scalar(50)],
                        named_arguments={},
                    ),
                ],
                required_extensions=[],
            )

        # multi_branch_filtered_numbers: two arms each sequence(100) with
        # complementary branch_filters carving the value range in half.
        # Total rows = 100 (50 from each arm after filtering).
        if schema_name.lower() == "data" and name.lower() == "multi_branch_filtered_numbers":
            return ScanBranchesResult(
                branches=[
                    ScanBranch(
                        function_name="sequence",
                        positional_arguments=[pa.scalar(100)],
                        named_arguments={},
                        branch_filter="n < 50",
                    ),
                    ScanBranch(
                        function_name="sequence",
                        positional_arguments=[pa.scalar(100)],
                        named_arguments={},
                        branch_filter="n >= 50",
                    ),
                ],
                required_extensions=[],
            )

        # multi_branch_hetero: one VGI arm (sequence(50)) + one native
        # read_parquet arm pointing at a well-known path the test creates
        # before querying. The parquet file has a single column "n" holding
        # values 50..99. Total rows = 100.
        if schema_name.lower() == "data" and name.lower() == "multi_branch_hetero":
            return ScanBranchesResult(
                branches=[
                    ScanBranch(
                        function_name="sequence",
                        positional_arguments=[pa.scalar(50)],
                        named_arguments={},
                    ),
                    ScanBranch(
                        function_name="read_parquet",
                        positional_arguments=[pa.scalar("/tmp/vgi_hetero_branch.parquet", pa.string())],
                        named_arguments={},
                    ),
                ],
                required_extensions=[],
            )

        # multi_branch_empty: worker deliberately returns branches=[] to
        # exercise the C++ side's BinderException loud-fail. ParseScanBranchesResult
        # must reject this at the wire layer.
        if schema_name.lower() == "data" and name.lower() == "multi_branch_empty":
            return ScanBranchesResult(branches=[], required_extensions=[])

        # multi_branch_two_writable: two ScanBranch entries both with
        # writable=True. ParseScanBranchesResult must reject loudly with
        # BinderException — DuckDB's single-writable-catalog-per-transaction
        # rule means at most one branch may be writable.
        if schema_name.lower() == "data" and name.lower() == "multi_branch_two_writable":
            return ScanBranchesResult(
                branches=[
                    ScanBranch(
                        function_name="sequence",
                        positional_arguments=[pa.scalar(10)],
                        named_arguments={},
                        writable=True,
                    ),
                    ScanBranch(
                        function_name="sequence",
                        positional_arguments=[pa.scalar(10)],
                        named_arguments={},
                        writable=True,
                    ),
                ],
                required_extensions=[],
            )

        # multi_branch_nopushdown: VGI sequence(50) + read_csv_auto. read_csv
        # has filter_pushdown=false in DuckDB, so any user WHERE clause stays
        # as a LogicalFilter above the csv arm — the rewriter must not assume
        # pushdown always succeeds.
        if schema_name.lower() == "data" and name.lower() == "multi_branch_nopushdown":
            return ScanBranchesResult(
                branches=[
                    ScanBranch(
                        function_name="sequence",
                        positional_arguments=[pa.scalar(50)],
                        named_arguments={},
                    ),
                    ScanBranch(
                        function_name="read_csv_auto",
                        positional_arguments=[pa.scalar("/tmp/vgi_nopushdown_branch.csv", pa.string())],
                        named_arguments={},
                    ),
                ],
                required_extensions=[],
            )

        # multi_branch_recon: three read_parquet branches with deliberately
        # mismatched column shapes — used to exercise column-reconciliation
        # by NAME with NULL-fill for missing canonicals. Canonical schema
        # is (a int64, b int64). The test creates the parquet files at the
        # paths below before querying.
        if schema_name.lower() == "data" and name.lower() == "multi_branch_recon":
            return ScanBranchesResult(
                branches=[
                    ScanBranch(
                        function_name="read_parquet",
                        positional_arguments=[pa.scalar("/tmp/vgi_recon_a_b.parquet", pa.string())],
                        named_arguments={},
                    ),
                    ScanBranch(
                        function_name="read_parquet",
                        positional_arguments=[pa.scalar("/tmp/vgi_recon_b_a.parquet", pa.string())],
                        named_arguments={},
                    ),
                    ScanBranch(
                        function_name="read_parquet",
                        positional_arguments=[pa.scalar("/tmp/vgi_recon_a_only.parquet", pa.string())],
                        named_arguments={},
                    ),
                ],
                required_extensions=[],
            )

        # Everything else: fall through to the default-impl shim (wraps
        # table_scan_function_get as a one-branch list).
        return super().table_scan_branches_get(
            attach_opaque_data=attach_opaque_data,
            transaction_opaque_data=transaction_opaque_data,
            schema_name=schema_name,
            name=name,
            at_unit=at_unit,
            at_value=at_value,
        )

    # Column statistics are defined inline on each Table descriptor using
    # the `statistics` dict. ReadOnlyCatalogInterface auto-serves them —
    # no override of table_column_statistics_get() needed here.

    def table_scan_function_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
        at_unit: str | None,
        at_value: str | None,
    ) -> ScanFunctionResult:
        """Return scan function for tables with explicit columns."""
        _validate_at_params(at_unit, at_value)

        # Handle the "versioned_data" table with time travel
        if schema_name.lower() == "data" and name.lower() == "versioned_data":
            version = resolve_version(at_unit, at_value)
            return ScanFunctionResult(
                function_name="versioned_data_scan",
                positional_arguments=[pa.scalar(version)],
                named_arguments={},
            )

        # Columns-based time-travel + pushdown: resolve AT → version and pass it
        # as a scan-function argument (the native columns-based AT mechanism).
        if schema_name.lower() == "data" and name.lower() == "tt_pushdown_cols":
            version = resolve_tt_version(at_unit, at_value)
            return ScanFunctionResult(
                function_name="tt_pushdown_cols_scan",
                positional_arguments=[pa.scalar(version)],
                named_arguments={},
            )

        # Handle the versioned_constraints table with time travel
        if schema_name.lower() == "data" and name.lower() == "versioned_constraints":
            version = resolve_versioned_constraints_version(at_unit, at_value)
            return ScanFunctionResult(
                function_name="versioned_constraints_scan",
                positional_arguments=[pa.scalar(version)],
                named_arguments={},
            )

        # rff_parquet — single-branch native read_parquet delegation.
        if schema_name.lower() == "data" and name.lower() == "rff_parquet":
            return ScanFunctionResult(
                function_name="read_parquet",
                positional_arguments=[pa.scalar("/tmp/rff_seg.parquet", pa.string())],
                named_arguments={},
            )

        # rff_hive / rff_hive_mixed — native read_parquet over a Hive glob.
        if schema_name.lower() == "data" and name.lower() in ("rff_hive", "rff_hive_mixed"):
            return ScanFunctionResult(
                function_name="read_parquet",
                positional_arguments=[pa.scalar("/tmp/rff_hive/*/*/*.parquet", pa.string())],
                named_arguments={"hive_partitioning": pa.scalar(True)},
            )

        # Reject AT clause on tables that don't support time travel
        if at_unit:
            raise ValueError(f"Table '{schema_name}.{name}' does not support time travel queries")

        # Handle the "generated_sequence" table (generated columns, backed by sequence)
        if schema_name.lower() == "data" and name.lower() == "generated_sequence":
            return ScanFunctionResult(
                function_name="sequence",
                positional_arguments=[pa.scalar(10)],
                named_arguments={},
            )

        # Handle "numbers" and "volatile_numbers" — both use sequence(100)
        if schema_name.lower() == "data" and name.lower() in ("numbers", "volatile_numbers"):
            return ScanFunctionResult(
                function_name="sequence",
                positional_arguments=[pa.scalar(100)],
                named_arguments={},
            )

        # funny_numbers — 123456 rows from sequence; statistics deliberately NOT set on
        # the table so SequenceFunction.statistics() provides them via table_function_statistics.
        if schema_name.lower() == "data" and name.lower() == "funny_numbers":
            return ScanFunctionResult(
                function_name="sequence",
                positional_arguments=[pa.scalar(123456)],
                named_arguments={},
            )

        # Constraint example tables — simple static scan functions
        _static_scan_tables: dict[str, str] = {
            "colors": "colors_scan",
            "departments": "departments_scan",
            "employees": "employees_scan",
            "products": "products_scan",
            "projects": "projects_scan",
            # filter-pushdown-through-view fixture.
            "filter_echo_table": "filter_echo_table_scan",
            # rff_* — required_field_filter_paths fixtures.
            "rff_simple": "rff_simple_scan",
            "rff_struct": "rff_struct_scan",
            "rff_nested": "rff_nested_scan",
            "rff_multi": "rff_multi_scan",
            "rff_none": "rff_none_scan",
            "rff_rowid": "rff_rowid_scan",
        }
        if schema_name.lower() == "data" and name.lower() in _static_scan_tables:
            return ScanFunctionResult(
                function_name=_static_scan_tables[name.lower()],
                positional_arguments=[],
                named_arguments={},
            )

        # Row ID test tables
        rowid_tables: dict[str, dict[str, str]] = {
            "rowid_first": {"layout": "first", "row_id_type": "int64"},
            "rowid_middle": {"layout": "middle", "row_id_type": "int64"},
            "rowid_last": {"layout": "last", "row_id_type": "int64"},
            "rowid_string": {"layout": "first", "row_id_type": "string"},
            "rowid_struct": {"layout": "first", "row_id_type": "struct"},
        }
        if schema_name.lower() == "data" and name.lower() in rowid_tables:
            opts = rowid_tables[name.lower()]
            return ScanFunctionResult(
                function_name="rowid_sequence",
                positional_arguments=[pa.scalar(20)],
                named_arguments={
                    "layout": pa.scalar(opts["layout"]),
                    "row_id_type": pa.scalar(opts["row_id_type"]),
                },
            )

        # Late-materialization tables → late_materialization scan function.
        # 1000 rows is large enough that LIMIT k << count makes the rewrite a
        # real win and that LIMIT 200 exceeds dynamic_or_filter_threshold (50).
        late_mat_tables: dict[str, dict[str, Any]] = {
            "late_mat": {},
            "late_mat_dup": {"dup_row_id": pa.scalar(True)},
            "late_mat_nulls": {"null_ord_stride": pa.scalar(7)},
        }
        if schema_name.lower() == "data" and name.lower() in late_mat_tables:
            return ScanFunctionResult(
                function_name="late_materialization",
                positional_arguments=[pa.scalar(1000)],
                named_arguments=late_mat_tables[name.lower()],
            )

        return super().table_scan_function_get(
            attach_opaque_data=attach_opaque_data,
            transaction_opaque_data=transaction_opaque_data,
            schema_name=schema_name,
            name=name,
            at_unit=at_unit,
            at_value=at_value,
        )

    # --------- Transaction lifecycle ---------
    #
    # The example catalog has no transactional state of its own — these
    # methods exist solely so the C++ extension populates
    # ``BindRequest.transaction_opaque_data`` when SQL is wrapped in
    # ``BEGIN`` / ``COMMIT``. That id is what makes
    # ``BindParams.transaction_storage`` non-None, which lets
    # ``TxCachedValueFunction`` (and any user-written function) cache
    # per-transaction values via ``FunctionStorage.transaction_state_*``.

    supports_transactions = True

    def catalog_transaction_begin(self, *, attach_opaque_data: AttachOpaqueData) -> TransactionOpaqueData | None:
        """Allocate a fresh transaction_opaque_data; no catalog-side state to track."""
        del attach_opaque_data
        return TransactionOpaqueData(uuid.uuid4().bytes)

    def catalog_transaction_commit(
        self, *, attach_opaque_data: AttachOpaqueData, transaction_opaque_data: TransactionOpaqueData
    ) -> None:
        """Clear per-transaction storage on commit (best-effort hygiene)."""
        del attach_opaque_data
        # transaction_opaque_data plays the role of scope_id in the unified
        # state_* API; execution_clear wipes every namespace for that scope.
        TxCachedValueFunction.storage.execution_clear(bytes(transaction_opaque_data))

    def catalog_transaction_rollback(
        self, *, attach_opaque_data: AttachOpaqueData, transaction_opaque_data: TransactionOpaqueData
    ) -> None:
        """Mirror of commit — same cleanup path."""
        del attach_opaque_data
        TxCachedValueFunction.storage.execution_clear(bytes(transaction_opaque_data))


class ExampleWorker(Worker):
    """Example worker with built-in test functions.

    This worker exposes all example functions via the ExampleCatalog interface,
    allowing clients to discover available functions via the "example" catalog.

    Settings exposed via catalog_attach:
    - vgi_verbose_mode: Enable verbose output (used by SettingsAwareFunction)
    - greeting: Custom greeting message (used by SettingsAwareFunction)
    - multiplier: Value multiplier (used by SettingsAwareFunction, MultiplyBySettingFunction)
    - threshold: Filter threshold (used by FilterBySettingFunction)
    - config: Sequence configuration struct (used by StructSettingsFunction)
    """

    catalog_interface = ExampleCatalog
    # catalog is set for introspection (worker page, tests) — runtime catalog
    # operations go through catalog_interface.
    catalog = _EXAMPLE_CATALOG

    class Settings:
        """Settings exposed via catalog_attach."""

        vgi_verbose_mode: Annotated[bool, Setting(desc="Enable verbose output")] = False
        greeting: Annotated[str, Setting(desc="Custom greeting message")] = "Hello"
        multiplier: Annotated[int, Setting(desc="Value multiplier")] = 1
        threshold: Annotated[int, Setting(desc="Filter threshold")] = 0
        scale_factor: Annotated[float, Setting(desc="Float scale factor")] = 1.0
        config: Annotated[  # type: ignore[valid-type]
            pa.struct([("start", pa.int64()), ("step", pa.int64()), ("label", pa.string())]),
            Setting(desc="Sequence configuration struct"),
        ] = None

    secret_types = [
        SecretTypeSpec(
            name="vgi_example",
            description="Example VGI secret for testing",
            schema=pa.schema(
                [
                    pa.field("secret_string", pa.string(), metadata={"redact": "true"}),
                    pa.field("api_key", pa.string(), metadata={"redact": "true"}),
                    pa.field("port", pa.int32()),
                    pa.field("use_ssl", pa.bool_()),
                    pa.field("timeout", pa.float64()),
                ]  # type: ignore[arg-type]  # PyArrow field metadata typing limitation
            ),
        ),
    ]


def main() -> None:
    """Run the fixture worker process.

    Always serves the base ExampleWorker catalog plus the
    ``projection_repro``, ``schema_reconcile``, and ``accumulate``
    fixture catalogs (all depend on the ``vgi[test-fixtures]`` extra).
    Adds the writable catalog when the ``vgi[test-fixtures-writable]``
    extra is also installed.
    """
    from vgi._test_fixtures.accumulate.worker import AccumulateWorker
    from vgi._test_fixtures.narrow_bind.worker import NarrowBindWorker
    from vgi._test_fixtures.projection_repro.worker import ProjReproWorker
    from vgi._test_fixtures.schema_reconcile.worker import SchemaReconcileWorker
    from vgi.meta_worker import MetaWorker

    workers: list[type] = [
        ExampleWorker,
        ProjReproWorker,
        SchemaReconcileWorker,
        AccumulateWorker,
        NarrowBindWorker,
    ]
    try:
        from vgi._test_fixtures.writable.worker import WritableWorker
    except ImportError:
        pass
    else:
        workers.append(WritableWorker)

    MetaWorker.serve(*workers)


if __name__ == "__main__":
    main()
