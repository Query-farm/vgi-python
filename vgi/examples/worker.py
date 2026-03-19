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
    _VERSIONED_SCHEMAS,
    ConstantColumnsFunction,
    DoubleSequenceFunction,
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
    ProjectedDataFunction,
    RepeatValueIntFunction,
    RepeatValueStrFunction,
    RowIdSequenceFunction,
    ScopedSecretDemoFunction,
    SecretDemoFunction,
    SequenceFunction,
    SettingsAwareFunction,
    StructSettingsFunction,
    TenThousandFunction,
    VersionedDataFunction,
    resolve_version,
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
        if bool(at_unit) != bool(at_value):
            raise ValueError("at_unit and at_value must both be provided or both be None")
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
        if bool(at_unit) != bool(at_value):
            raise ValueError("at_unit and at_value must both be provided or both be None")

        # Handle the "versioned_data" table with time travel
        if schema_name.lower() == "data" and name.lower() == "versioned_data":
            version = resolve_version(at_unit, at_value)
            return ScanFunctionResult(
                function_name="versioned_data_scan",
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
