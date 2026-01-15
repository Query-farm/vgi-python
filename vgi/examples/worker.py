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

Usage:
    vgi-example-worker
"""

from typing import Annotated

from vgi.catalog import Setting
from vgi.examples.scalar import (
    AddNumericColumnsFunction,
    DoubleColumnFunction,
    SumColumnsFunction,
    UpperCaseFunction,
)
from vgi.examples.table import (
    ConstantColumnsFunction,
    ConstantTableFunction,
    GeneratorExceptionFunction,
    LoggingGeneratorFunction,
    PartitionedSequenceFunction,
    ProjectedDataFunction,
    SequenceFunction,
    SettingsAwareFunction,
)
from vgi.examples.table_in_out import (
    BufferInputFunction,
    EchoFunction,
    ExceptionFinalizeFunction,
    ExceptionProcessFunction,
    RepeatInputsFunction,
    SumAllColumnsFunction,
    SumAllColumnsFunctionDistributed,
    SumAllColumnsFunctionWithLogging,
    SumAllColumnsSimpleDistributed,
)
from vgi.worker import Worker


class ExampleWorker(Worker):
    """Example worker with built-in test functions.

    This worker exposes all example functions via the catalog interface,
    allowing clients to discover available functions via the "example" catalog.

    Settings exposed via catalog_attach:
    - vgi_verbose_mode: Enable verbose output (used by SettingsAwareFunction)
    """

    catalog_name = "example"

    class Settings:
        """Settings exposed via catalog_attach."""

        vgi_verbose_mode: Annotated[bool, Setting(desc="Enable verbose output")] = False

    functions = [
        # TableInOutGenerator - transform input batches
        EchoFunction,
        BufferInputFunction,
        RepeatInputsFunction,
        SumAllColumnsFunction,
        SumAllColumnsFunctionDistributed,
        SumAllColumnsSimpleDistributed,
        SumAllColumnsFunctionWithLogging,
        ExceptionFinalizeFunction,
        ExceptionProcessFunction,
        # TableFunctionGenerator - generate output without input
        ConstantColumnsFunction,
        ConstantTableFunction,
        GeneratorExceptionFunction,
        LoggingGeneratorFunction,
        PartitionedSequenceFunction,
        ProjectedDataFunction,
        SequenceFunction,
        SettingsAwareFunction,
        # ScalarFunctionGenerator - transform to single-column output
        AddNumericColumnsFunction,
        DoubleColumnFunction,
        SumColumnsFunction,
        UpperCaseFunction,
    ]


def main() -> None:
    """Run the example worker process."""
    parser = ExampleWorker.create_argument_parser()
    args = parser.parse_args()
    ExampleWorker(quiet=args.quiet).run()


if __name__ == "__main__":
    main()
