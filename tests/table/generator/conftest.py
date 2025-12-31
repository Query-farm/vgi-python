"""Shared fixtures for table generator function tests."""

from collections.abc import Callable, Generator
from typing import Any, Literal

import pyarrow as pa
import pytest

from vgi.client import Client, ClientError
from vgi.function import Arguments
from vgi.log import Message
from vgi.table_function import TableFunctionGenerator
from vgi.testing import FunctionTestClientError, TableFunctionTestClient

# Type alias for the runner function
TableFunctionRunner = Callable[
    [type[TableFunctionGenerator], tuple[Any, ...]],
    tuple[list[pa.RecordBatch], list[Message]],
]

# Type alias for the test mode
TestMode = Literal["in_process", "client"]

# Type alias for the fixture return type
RunnerWithMode = tuple[TableFunctionRunner, TestMode]


def run_in_process(
    func_class: type[TableFunctionGenerator],
    args: tuple[Any, ...],
) -> tuple[list[pa.RecordBatch], list[Message]]:
    """Run a table function in-process using TableFunctionTestClient."""
    with TableFunctionTestClient(func_class) as client:
        outputs = list(
            client.table_function(
                arguments=Arguments(positional=tuple(pa.scalar(a) for a in args))
            )
        )
        return outputs, client.logs


def run_via_client(
    func_class: type[TableFunctionGenerator],
    args: tuple[Any, ...],
) -> tuple[list[pa.RecordBatch], list[Message]]:
    """Run a table function via subprocess using Client.table_function.

    Uses max_workers=1 to ensure consistent behavior with in-process mode.
    For multi-worker tests, use Client directly with explicit max_workers.
    """
    meta = func_class.get_metadata()
    function_name = meta.name

    with Client("vgi-example-worker", max_workers=1) as client:
        outputs = list(
            client.table_function(
                function_name=function_name,
                arguments=Arguments(positional=tuple(pa.scalar(a) for a in args)),
            )
        )
        # Note: logs are not captured via Client (they go to stderr)
        return outputs, []


@pytest.fixture(params=["in_process", "client"])
def run_table_function_mode(
    request: pytest.FixtureRequest,
) -> Generator[RunnerWithMode, None, None]:
    """Fixture that provides both in-process and client-based runners.

    Tests using this fixture will run twice: once in-process and once via Client.
    Returns a tuple of (runner_function, mode_name) for conditional assertions.
    """
    mode: TestMode = request.param
    if mode == "in_process":
        yield run_in_process, mode
    else:
        yield run_via_client, mode


@pytest.fixture
def run_function() -> TableFunctionRunner:
    """Fixture for in-process-only tests (e.g., log capture tests)."""
    return run_in_process


# Re-export error types for convenience
InProcessError = FunctionTestClientError
SubprocessError = ClientError
