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
    Macro,
    MacroType,
    ReadOnlyCatalogInterface,
    ScanFunctionResult,
    Schema,
    SecretTypeSpec,
    SerializedSchema,
    Setting,
    Sql,
    Table,
    TableInfo,
    TransactionId,
    View,
)
from vgi.catalog.catalog_interface import _validate_at_params
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
    ConstantColumnsFunction,
    DepartmentsScanFunction,
    DoubleSequenceFunction,
    EmployeesScanFunction,
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
    PartitionedSequenceFunction,
    ProductsScanFunction,
    ProjectedDataFunction,
    ProjectsScanFunction,
    RepeatValueIntFunction,
    RepeatValueStrFunction,
    RowIdSequenceFunction,
    ScopedSecretDemoFunction,
    SecretDemoFunction,
    SequenceFunction,
    SettingsAwareFunction,
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
from vgi.examples.writable_table import (
    WritableProductsInsert,
    WritableProductsScan,
    WritableTableDelete,
    WritableTableInsert,
    WritableTableScan,
    WritableTableUpdate,
)
from vgi.worker import Worker

_EXAMPLE_CATALOG = Catalog(
    name="example",
    default_schema="main",
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
                PartitionedSequenceFunction,
                ProjectedDataFunction,
                RowIdSequenceFunction,
                SecretDemoFunction,
                ScopedSecretDemoFunction,
                SequenceFunction,
                SettingsAwareFunction,
                StructSettingsFunction,
                TenThousandFunction,
                VersionedDataFunction,
                # Static data scan functions for constraint-backed tables
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
                # Writable table scan functions (registered here for projection_pushdown metadata)
                WritableTableScan,
                WritableProductsScan,
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
                # Explicit columns table: requires table_scan_function_get
                Table(
                    name="numbers",
                    columns=pa.schema([("value", pa.int64())]),
                    comment="First 100 integers (demonstrates explicit columns)",
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
                # ----- Writable table example -----
                Table(
                    name="writable_data",
                    function=WritableTableScan,
                    insert_function=WritableTableInsert,
                    update_function=WritableTableUpdate,
                    delete_function=WritableTableDelete,
                    comment="In-memory writable table supporting INSERT/UPDATE/DELETE",
                ),
                Table(
                    name="writable_products",
                    function=WritableProductsScan,
                    insert_function=WritableProductsInsert,
                    primary_key=(("product_id",),),
                    not_null=("product_id", "name"),
                    check=("price >= 0",),
                    defaults={
                        "price": 0.0,
                        "status": "draft",
                        "created_at": Sql("'server-assigned'"),
                    },
                    comment="Writable products with defaults, constraints, and server-side modification",
                ),
            ],
            views=[
                View(
                    name="small_numbers",
                    definition="SELECT * FROM numbers WHERE value < 10",
                    comment="Numbers less than 10",
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

        # Handle the "numbers" table with explicit columns
        if schema_name.lower() == "data" and name.lower() == "numbers":
            return ScanFunctionResult(
                function_name="sequence",
                positional_arguments=[pa.scalar(100)],
                named_arguments={},
            )

        # Constraint example tables — simple static scan functions
        _static_scan_tables: dict[str, str] = {
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
    """Run the example worker process."""
    ExampleWorker.main()


if __name__ == "__main__":
    main()
