"""Example worker with built-in functions for testing.

This demonstrates how to create a worker by subclassing Worker
and listing function classes. Function names are derived from
each class's metadata (Meta.name or snake_case of class name).

The worker supports:
- TableInOutGeneratorFunction: Transforms input batches to output batches
- TableFunctionGenerator: Generates output batches without input
- ScalarFunctionGenerator: Transforms input to single-column output (1:1 rows)

Usage:
    vgi-example-worker
"""

from vgi.examples.scalar import (
    AddColumnsFunction,
    DoubleColumnFunction,
    UpperCaseFunction,
)
from vgi.examples.table import (
    ConstantTableFunction,
    GeneratorExceptionFunction,
    LoggingGeneratorFunction,
    PartitionedRangeFunction,
    ProjectedDataFunction,
    RandomSampleFunction,
    RangeFunction,
    SequenceFunction,
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
    """Example worker with built-in test functions."""

    functions = [
        # TableInOutGeneratorFunction - transform input batches
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
        SequenceFunction,
        RangeFunction,
        ConstantTableFunction,
        RandomSampleFunction,
        GeneratorExceptionFunction,
        LoggingGeneratorFunction,
        PartitionedRangeFunction,
        ProjectedDataFunction,
        # ScalarFunctionGenerator - transform to single-column output
        DoubleColumnFunction,
        AddColumnsFunction,
        UpperCaseFunction,
    ]


def main() -> None:
    """Run the example worker process."""
    ExampleWorker().run()


if __name__ == "__main__":
    main()
