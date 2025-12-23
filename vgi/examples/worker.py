"""Example worker with built-in functions for testing.

This demonstrates how to create a worker by subclassing Worker
and registering functions.

Usage:
    vgi-example-worker
"""

from vgi.examples.table_in_out import (
    BufferInputFunction,
    EchoFunction,
    RepeatInputsFunction,
    SumAllColumnsFunction,
)
from vgi.worker import FunctionRegistry, Worker


class ExampleWorker(Worker):
    """Example worker with built-in test functions."""

    registry: FunctionRegistry = {
        "echo": EchoFunction,
        "buffer_input": BufferInputFunction,
        "repeat_inputs": RepeatInputsFunction,
        "sum_all_columns": SumAllColumnsFunction,
    }


def main() -> None:
    ExampleWorker().run()


if __name__ == "__main__":
    main()
