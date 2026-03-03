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

from typing import Annotated, Any

import pyarrow as pa

from vgi.arguments import Arguments
from vgi.catalog import (
    AttachId,
    Catalog,
    Macro,
    MacroType,
    ScanFunctionResult,
    Schema,
    Setting,
    Table,
    TransactionId,
    View,
)
from vgi.examples.scalar import (
    AddValuesFunction,
    BernoulliFunction,
    BinaryPacketFunction,
    ConditionalMessageFunction,
    DoubleFunction,
    MultiplyBySettingFunction,
    MultiplyFunction,
    NullHandlingFunction,
    RandomBytesFunction,
    RandomIntFunction,
    ReturnSecretValueFunction,
    SumValuesFunction,
    UpperCaseFunction,
)
from vgi.examples.table import (
    ConstantColumnsFunction,
    DoubleSequenceFunction,
    GeneratorExceptionFunction,
    LoggingGeneratorFunction,
    NamedParamsEchoFunction,
    NestedSequenceFunction,
    PartitionedSequenceFunction,
    ProjectedDataFunction,
    SequenceFunction,
    SettingsAwareFunction,
    StructSettingsFunction,
    TenThousandFunction,
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


class ExampleWorker(Worker):
    """Example worker with built-in test functions.

    This worker exposes all example functions via the catalog interface,
    allowing clients to discover available functions via the "example" catalog.

    Settings exposed via catalog_attach:
    - vgi_verbose_mode: Enable verbose output (used by SettingsAwareFunction)
    - greeting: Custom greeting message (used by SettingsAwareFunction)
    - multiplier: Value multiplier (used by SettingsAwareFunction, MultiplyBySettingFunction)
    - threshold: Filter threshold (used by FilterBySettingFunction)
    - config: Sequence configuration struct (used by StructSettingsFunction)
    """

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

    catalog = Catalog(
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
                    DoubleSequenceFunction,
                    GeneratorExceptionFunction,
                    LoggingGeneratorFunction,
                    NamedParamsEchoFunction,
                    NestedSequenceFunction,
                    PartitionedSequenceFunction,
                    ProjectedDataFunction,
                    SequenceFunction,
                    SettingsAwareFunction,
                    StructSettingsFunction,
                    TenThousandFunction,
                    # ScalarFunctionGenerator - transform to single-column output
                    AddValuesFunction,
                    BernoulliFunction,
                    BinaryPacketFunction,
                    ConditionalMessageFunction,
                    DoubleFunction,
                    MultiplyBySettingFunction,
                    MultiplyFunction,
                    NullHandlingFunction,
                    RandomBytesFunction,
                    RandomIntFunction,
                    ReturnSecretValueFunction,
                    SumValuesFunction,
                    UpperCaseFunction,
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
                    # Explicit columns table: requires table_scan_function_get
                    Table(
                        name="numbers",
                        columns=pa.schema([("value", pa.int64())]),
                        comment="First 100 integers (demonstrates explicit columns)",
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

    def table_scan_function_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        at_unit: str | None,
        at_value: Any,
    ) -> ScanFunctionResult:
        """Return scan function for tables with explicit columns.

        This method is called when DuckDB needs to scan a table. For tables
        defined with explicit columns (not function-backed), you must implement
        this to specify which function to call for scanning.

        Args:
            attach_id: The catalog attachment identifier.
            transaction_id: Optional transaction identifier.
            schema_name: The schema containing the table.
            name: The table name.
            at_unit: Time travel unit (e.g., "version", "timestamp").
            at_value: Time travel value.

        Returns:
            ScanFunctionResult specifying the function to call for scanning.

        """
        # Handle the "numbers" table with explicit columns
        if schema_name.lower() == "data" and name.lower() == "numbers":
            # Scan using the sequence function with count=100
            return ScanFunctionResult(
                function_name="sequence",
                positional_arguments=[pa.scalar(100)],  # Generate 100 numbers
                named_arguments={},
            )

        # For function-backed tables, delegate to the catalog interface
        # which handles them automatically
        catalog_interface = self._get_catalog_interface()
        if catalog_interface is not None:
            return catalog_interface().table_scan_function_get(
                attach_id=attach_id,
                transaction_id=transaction_id,
                schema_name=schema_name,
                name=name,
                at_unit=at_unit,
                at_value=at_value,
            )

        raise NotImplementedError(f"table_scan_function_get not implemented for {schema_name}.{name}")


def main() -> None:
    """Run the example worker process."""
    ExampleWorker.main()


if __name__ == "__main__":
    main()
