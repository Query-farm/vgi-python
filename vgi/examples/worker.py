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
    vgi-example-worker
"""

from typing import Annotated

import pyarrow as pa

from vgi.arguments import Arguments
from vgi.catalog import (
    AttachId,
    Catalog,
    ForeignKeyDef,
    Index,
    IndexConstraintType,
    Macro,
    MacroType,
    ReadOnlyCatalogInterface,
    ScanFunctionResult,
    Schema,
    SecretTypeSpec,
    SerializedSchema,
    Setting,
    Table,
    TableInfo,
    TransactionId,
    View,
)
from vgi.catalog.catalog_interface import _validate_at_params
from vgi.catalog.descriptors import ColumnStatisticsInput
from vgi.catalog.duckdb_statistics import statistics_from_duckdb
from vgi.examples.aggregate import (
    AvgFunction,
    CountFunction,
    DynamicAggregateFunction,
    DynamicMLAggregateFunction,
    GenericSumFunction,
    ListAggFunction,
    PercentileFunction,
    SumAllFunction,
    SumFunction,
    WeightedSumFunction,
    WindowListAggFunction,
    WindowMedianFunction,
    WindowSumFunction,
)

try:
    from vgi.examples.distill import DistillFunction
    from vgi.examples.summarize import SummarizeFunction

    _HAS_LLM = True
except ImportError:
    _HAS_LLM = False
from vgi.examples.scalar import (
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
from vgi.examples.table import (
    _VERSIONED_CONSTRAINTS_SCHEMAS,
    _VERSIONED_SCHEMAS,
    ColorsScanFunction,
    ConstantColumnsFunction,
    DepartmentsScanFunction,
    DoubleSequenceFunction,
    DynamicFilterEchoFunction,
    EmployeesScanFunction,
    ExpressionFilterTestFunction,
    FilterEchoFunction,
    GeneratorExceptionFunction,
    LoggingGeneratorFunction,
    MakePairsIntFunction,
    MakePairsIntStrFunction,
    MakePairsStrFunction,
    MakeSeriesCountFunction,
    MakeSeriesCsvFunction,
    MakeSeriesFloatFunction,
    MakeSeriesRangeFunction,
    MakeSeriesStepFunction,
    NamedParamsEchoFunction,
    NestedSequenceFunction,
    OrderEchoFunction,
    PartitionedSequenceFunction,
    ProductsScanFunction,
    ProjectedDataFunction,
    ProjectsScanFunction,
    RepeatValueIntFunction,
    RepeatValueStrFunction,
    RowIdSequenceFunction,
    SampleEchoFunction,
    ScopedSecretDemoFunction,
    SecretDemoFunction,
    SequenceFunction,
    SettingsAwareFunction,
    SpatialFilterExampleFunction,
    StructSettingsFunction,
    TenThousandFunction,
    VersionedConstraintsScanFunction,
    VersionedDataFunction,
    resolve_version,
    resolve_versioned_constraints_version,
)
from vgi.examples.table_in_out import (
    BufferInputFunction,
    EchoFunction,
    ExceptionFinalizeFunction,
    ExceptionProcessFunction,
    FilterBySettingFunction,
    RepeatInputsFunction,
    SumAllColumnsFunction,
    SumAllColumnsSimpleDistributed,
)
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
    import duckdb

    conn = duckdb.connect()
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
    import duckdb

    conn = duckdb.connect()
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
    import duckdb

    conn = duckdb.connect()
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
    tags={"source": "vgi-example-worker", "version": "1"},
    schemas=[
        Schema(
            name="main",
            comment="Example functions for testing VGI",
            functions=[
                # TableInOutGenerator - transform input batches
                EchoFunction,
                BufferInputFunction,
                FilterBySettingFunction,
                RepeatInputsFunction,
                SumAllColumnsFunction,
                SumAllColumnsSimpleDistributed,
                ExceptionFinalizeFunction,
                ExceptionProcessFunction,
                # TableFunctionGenerator - generate output without input
                ConstantColumnsFunction,
                FilterEchoFunction,
                DoubleSequenceFunction,
                DynamicFilterEchoFunction,
                GeneratorExceptionFunction,
                LoggingGeneratorFunction,
                MakeSeriesCountFunction,
                MakeSeriesCsvFunction,
                MakeSeriesFloatFunction,
                MakeSeriesRangeFunction,
                MakeSeriesStepFunction,
                MakePairsIntFunction,
                MakePairsIntStrFunction,
                MakePairsStrFunction,
                RepeatValueIntFunction,
                RepeatValueStrFunction,
                NamedParamsEchoFunction,
                NestedSequenceFunction,
                OrderEchoFunction,
                PartitionedSequenceFunction,
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
                VersionedDataFunction,
                # Static data scan functions for constraint-backed tables
                ColorsScanFunction,
                DepartmentsScanFunction,
                EmployeesScanFunction,
                ProductsScanFunction,
                ProjectsScanFunction,
                VersionedConstraintsScanFunction,
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
                UpperCaseFunction,
                WhoAmIFunction,
                # AggregateFunction - aggregate input rows
                AvgFunction,
                CountFunction,
                DynamicAggregateFunction,
                DynamicMLAggregateFunction,
                GenericSumFunction,
                ListAggFunction,
                PercentileFunction,
                SumAllFunction,
                SumFunction,
                WeightedSumFunction,
                WindowListAggFunction,
                WindowMedianFunction,
                WindowSumFunction,
                *([] if not _HAS_LLM else [DistillFunction, SummarizeFunction]),
            ],
            views=[
                View(
                    name="first_ten",
                    definition="SELECT * FROM sequence(10)",
                    comment="First 10 integers",
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
                        schema=pa.schema([("lo", pa.int64()), ("hi", pa.int64())]),
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
                # Time-travel table: version-specific schema
                Table(
                    name="versioned_data",
                    columns=pa.schema(
                        [  # type: ignore[arg-type]  # pyarrow stubs: mixed-type fields
                            pa.field("id", pa.int64()),
                            pa.field("score", pa.float64()),
                        ]
                    ),
                    supports_time_travel=True,
                    comment="Versioned data table demonstrating time travel with schema evolution",
                ),
                # Explicit columns table with statistics extracted from DuckDB
                # via statistics_from_duckdb() — demonstrates the helper workflow
                Table(
                    name="numbers",
                    columns=pa.schema([("value", pa.int64())]),
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
                    columns=pa.schema([("value", pa.int64())]),
                    statistics={
                        "value": ColumnStatisticsInput(min=0, max=99, has_null=False, distinct_count=100),
                    },
                    statistics_cache_max_age_seconds=0,
                    comment="Numbers with volatile stats (TTL=0, always re-fetched)",
                ),
                # ENUM (dictionary-encoded) column table — tests that statistics
                # report actual string values, not dictionary indices.
                Table(
                    name="colors",
                    columns=pa.schema(
                        [  # type: ignore[arg-type]
                            pa.field("id", pa.int64()),
                            pa.field("color", pa.string()),
                            pa.field("hex_code", pa.string()),
                        ]
                    ),
                    statistics=_ENUM_STATS,
                    statistics_cache_max_age_seconds=3600,
                    comment="Colors table with ENUM-derived statistics",
                ),
                # Row ID position tests (int64 row_id)
                Table(
                    name="rowid_first",
                    columns=pa.schema(
                        [  # type: ignore[arg-type]  # pyarrow stubs: mixed-type fields
                            pa.field("row_id", pa.int64(), metadata={b"is_row_id": b""}),
                            pa.field("name", pa.string()),
                            pa.field("value", pa.string()),
                        ]
                    ),
                    comment="Table with row_id at column index 0",
                ),
                Table(
                    name="rowid_middle",
                    columns=pa.schema(
                        [  # type: ignore[arg-type]  # pyarrow stubs: mixed-type fields
                            pa.field("name", pa.string()),
                            pa.field("row_id", pa.int64(), metadata={b"is_row_id": b""}),
                            pa.field("value", pa.string()),
                        ]
                    ),
                    comment="Table with row_id at column index 1",
                ),
                Table(
                    name="rowid_last",
                    columns=pa.schema(
                        [  # type: ignore[arg-type]  # pyarrow stubs: mixed-type fields
                            pa.field("name", pa.string()),
                            pa.field("value", pa.string()),
                            pa.field("row_id", pa.int64(), metadata={b"is_row_id": b""}),
                        ]
                    ),
                    comment="Table with row_id at column index 2",
                ),
                # Row ID type tests (row_id at index 0)
                Table(
                    name="rowid_string",
                    columns=pa.schema(
                        [  # type: ignore[arg-type]  # pyarrow stubs: mixed-type fields
                            pa.field("row_id", pa.string(), metadata={b"is_row_id": b""}),
                            pa.field("value", pa.int64()),
                        ]
                    ),
                    comment="Table with string row_id",
                ),
                Table(
                    name="rowid_struct",
                    columns=pa.schema(
                        [
                            pa.field(
                                "row_id",
                                pa.struct([("a", pa.int64()), ("b", pa.string())]),
                                metadata={b"is_row_id": b""},
                            ),
                            pa.field("value", pa.string()),
                        ]
                    ),
                    comment="Table with struct row_id",
                ),
                # ----- Generated column example table -----
                Table(
                    name="generated_sequence",
                    columns=pa.schema(
                        [  # type: ignore[arg-type]
                            pa.field("n", pa.int64()),
                            pa.field("doubled", pa.int64()),
                            pa.field("label", pa.string()),
                        ]
                    ),
                    generated_columns={
                        "doubled": "n * 2",
                        "label": "'item_' || CAST(n AS VARCHAR)",
                    },
                    comment="Table with generated columns backed by sequence(10)",
                ),
                # ----- Constraint example tables -----
                Table(
                    name="departments",
                    columns=pa.schema(
                        [  # type: ignore[arg-type]  # pyarrow stubs: mixed-type fields
                            pa.field("id", pa.int64()),
                            pa.field("name", pa.string()),
                            pa.field("budget", pa.float64()),
                        ]
                    ),
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
                    columns=pa.schema(
                        [  # type: ignore[arg-type]  # pyarrow stubs: mixed-type fields
                            pa.field("id", pa.int64()),
                            pa.field("name", pa.string()),
                            pa.field("quantity", pa.int64()),
                            pa.field("price", pa.float64()),
                        ]
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
                    columns=pa.schema(
                        [  # type: ignore[arg-type]  # pyarrow stubs: mixed-type fields
                            pa.field("id", pa.int64()),
                            pa.field("name", pa.string()),
                            pa.field("email", pa.string()),
                            pa.field("department_id", pa.int64()),
                        ]
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
                    columns=pa.schema(
                        [  # type: ignore[arg-type]  # pyarrow stubs: mixed-type fields
                            pa.field("department_id", pa.int64()),
                            pa.field("project_code", pa.string()),
                            pa.field("title", pa.string()),
                        ]
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
                # Time-travel constraint evolution table
                Table(
                    name="versioned_constraints",
                    columns=pa.schema(
                        [  # type: ignore[arg-type]  # pyarrow stubs: mixed-type fields
                            pa.field("id", pa.int64()),
                            pa.field("name", pa.string()),
                            pa.field("email", pa.string()),
                            pa.field("department_id", pa.int64()),
                        ]
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
        attach_id: AttachId,
        transaction_id: TransactionId | None,
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
        return super().table_get(
            attach_id=attach_id,
            transaction_id=transaction_id,
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
        attach_id: AttachId,
        transaction_id: TransactionId | None,
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

        # Handle the versioned_constraints table with time travel
        if schema_name.lower() == "data" and name.lower() == "versioned_constraints":
            version = resolve_versioned_constraints_version(at_unit, at_value)
            return ScanFunctionResult(
                function_name="versioned_constraints_scan",
                positional_arguments=[pa.scalar(version)],
                named_arguments={},
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

        # Constraint example tables — simple static scan functions
        _static_scan_tables: dict[str, str] = {
            "colors": "colors_scan",
            "departments": "departments_scan",
            "employees": "employees_scan",
            "products": "products_scan",
            "projects": "projects_scan",
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

        return super().table_scan_function_get(
            attach_id=attach_id,
            transaction_id=transaction_id,
            schema_name=schema_name,
            name=name,
            at_unit=at_unit,
            at_value=at_value,
        )


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
    """Run the example worker process with both example and writable catalogs."""
    from vgi.examples.writable_worker import WritableWorker
    from vgi.meta_worker import MetaWorker

    MetaWorker.serve(ExampleWorker, WritableWorker)


if __name__ == "__main__":
    main()
