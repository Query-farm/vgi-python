"""Example worker with built-in functions for testing.

This demonstrates how to create a worker by subclassing Worker
and listing function classes. Function names are derived from
each class's metadata (Meta.name or snake_case of class name).

Usage:
    vgi-example-worker
"""

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
        EchoFunction,
        BufferInputFunction,
        RepeatInputsFunction,
        SumAllColumnsFunction,
        SumAllColumnsFunctionDistributed,
        SumAllColumnsSimpleDistributed,
        SumAllColumnsFunctionWithLogging,
        ExceptionFinalizeFunction,
        ExceptionProcessFunction,
    ]


def main() -> None:
    """Run the example worker process."""
    ExampleWorker().run()


if __name__ == "__main__":
    main()
