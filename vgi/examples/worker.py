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

    # mypy doesn't recognize decorator type transformation
    registry: FunctionRegistry = {
        "echo": EchoFunction,  # type: ignore[dict-item]
        "buffer_input": BufferInputFunction,  # type: ignore[dict-item]
        "repeat_inputs": RepeatInputsFunction,  # type: ignore[dict-item]
        "sum_all_columns": SumAllColumnsFunction,  # type: ignore[dict-item]
    }


def main() -> None:
    ExampleWorker().run()


if __name__ == "__main__":
    main()
