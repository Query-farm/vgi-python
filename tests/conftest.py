"""Shared fixtures for VGI tests."""

import logging
from typing import Any

import pyarrow as pa
import pytest

from vgi import schema

# =============================================================================
# Utility Functions (not fixtures, can be imported directly)
# =============================================================================


def make_schema(fields: list[Any]) -> pa.Schema:
    """Create schema with proper typing for field list.

    This is a helper to avoid mypy errors when creating schemas from
    field tuples like [("name", pa.string())].
    """
    return pa.schema(fields)


def filter_non_empty(batches: list[pa.RecordBatch]) -> list[pa.RecordBatch]:
    """Filter out empty batches."""
    return [b for b in batches if b.num_rows > 0]


def assert_single_result(
    batches: list[pa.RecordBatch],
    expected: dict[str, list[Any]],
) -> None:
    """Assert a single-row aggregation result.

    Filters out empty batches, asserts there's exactly one non-empty batch,
    and checks that its contents match the expected dictionary.
    """
    non_empty = filter_non_empty(batches)
    assert len(non_empty) == 1, f"Expected 1 non-empty batch, got {len(non_empty)}"
    assert non_empty[0].to_pydict() == expected


def total_rows(batches: list[pa.RecordBatch]) -> int:
    """Return total row count across all batches."""
    return sum(b.num_rows for b in batches)


def assert_total_rows(batches: list[pa.RecordBatch], expected: int) -> None:
    """Assert total row count across all batches."""
    actual = total_rows(batches)
    assert actual == expected, f"Expected {expected} rows, got {actual}"


def empty_batch_from_schema(schema: pa.Schema) -> pa.RecordBatch:
    """Create an empty batch with the given schema."""
    return pa.RecordBatch.from_pydict({field.name: [] for field in schema}, schema=schema)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def test_logger() -> logging.Logger:
    """Provide a shared test logger."""
    return logging.getLogger("vgi.test")


@pytest.fixture
def example_worker() -> str:
    """Return the path to the example worker."""
    return "vgi-example-worker"


@pytest.fixture(scope="session")
def http_worker() -> Any:
    """Start ``vgi-example-http`` lazily, sharing one server per (extra_args, env) combo.

    Usage::

        def test_something(http_worker):
            base_url = http_worker()                       # defaults
            base_url = http_worker(extra_args=[...])       # pass flags

    Workers are session-scoped and cached by configuration key, so tests that
    need the same flags reuse a single subprocess. Each unique combination
    spawns its own server (one-time per session). The whole pool is torn
    down at session end.
    """
    from contextlib import ExitStack

    from tests._http_fixtures import start_http_worker

    pytest.importorskip("vgi_rpc.http")

    stack = ExitStack()
    cache: dict[tuple[tuple[str, ...], tuple[tuple[str, str], ...]], str] = {}

    def _start(*, extra_args: list[str] | None = None, env: dict[str, str] | None = None) -> str:
        key = (
            tuple(extra_args or ()),
            tuple(sorted((env or {}).items())),
        )
        if key not in cache:
            cache[key] = start_http_worker(stack, extra_args=key[0], env=dict(key[1]) or None)
        return cache[key]

    try:
        yield _start
    finally:
        stack.close()


# Keys used to identify transport modes in conformance tests. Keeping the
# literal values here (rather than inside the fixture) makes it easy for
# individual tests to opt out with ``@pytest.mark.parametrize("client_transport",
# ["subprocess-pooled"], indirect=True)``.
_CLIENT_TRANSPORT_MODES = ["subprocess-pooled", "subprocess-direct", "http"]


@pytest.fixture(scope="session")
def _shared_http_base_url() -> Any:
    """Session-scoped lazy starter for the default-config HTTP worker.

    Used by the http branch of ``client_transport`` so every parametrized
    http-mode test reuses one subprocess instead of spawning fresh ones.
    """
    from contextlib import ExitStack

    from tests._http_fixtures import start_http_worker

    pytest.importorskip("vgi_rpc.http")
    stack = ExitStack()
    cached: dict[str, str] = {}

    def _start() -> str:
        if "url" not in cached:
            cached["url"] = start_http_worker(stack)
        return cached["url"]

    try:
        yield _start
    finally:
        stack.close()


@pytest.fixture(params=_CLIENT_TRANSPORT_MODES)
def client_transport(
    request: pytest.FixtureRequest,
    example_worker: str,
    _shared_http_base_url: Any,
) -> Any:
    """Parametrized factory that builds a configured ``Client`` for each transport.

    Yields a callable ``make_client()`` -> ``Client``. Callers must enter the
    returned client as a context manager.

    Modes:
        subprocess-pooled: Pool-backed subprocess (the default path).
        subprocess-direct: ``pool=None`` — direct Popen management.
        http: ``Client.from_http(base_url)`` backed by a per-test
            ``vgi-example-http`` subprocess. Skips if the HTTP transport
            isn't wired yet.
    """
    from vgi.client.client import _HTTP_TRANSPORT_READY, Client, _default_pool

    mode = request.param

    def _make() -> Client:
        if mode == "subprocess-pooled":
            return Client(example_worker, pool=_default_pool)
        if mode == "subprocess-direct":
            return Client(example_worker, pool=None)
        if mode == "http":
            if not _HTTP_TRANSPORT_READY:
                pytest.skip("Client HTTP transport arrives in Phase 2 of whimsical-mccarthy plan")
            pytest.importorskip("vgi_rpc.http")
            return Client.from_http(_shared_http_base_url())
        raise AssertionError(f"unknown transport mode {mode!r}")

    yield _make


@pytest.fixture
def simple_batches() -> list[pa.RecordBatch]:
    """Create simple test batches with integer and string columns."""
    s = schema(id=pa.int64(), value=pa.int64(), name=pa.string())
    batch1 = pa.RecordBatch.from_pydict(
        {"id": [1, 2], "value": [10, 20], "name": ["a", "b"]},
        schema=s,
    )
    batch2 = pa.RecordBatch.from_pydict(
        {"id": [3, 4], "value": [30, 40], "name": ["c", "d"]},
        schema=s,
    )
    return [batch1, batch2]


@pytest.fixture
def numeric_batches() -> list[pa.RecordBatch]:
    """Create test batches with only numeric columns for sum tests."""
    s = schema(a=pa.int32(), b=pa.float64())
    batch1 = pa.RecordBatch.from_pydict(
        {"a": [1, 2, 3], "b": [1.5, 2.5, 3.0]},
        schema=s,
    )
    batch2 = pa.RecordBatch.from_pydict(
        {"a": [4, 5], "b": [4.0, 5.0]},
        schema=s,
    )
    return [batch1, batch2]


# =============================================================================
# pytest-examples configuration
# =============================================================================


@pytest.fixture
def eval_example(eval_example):  # type: ignore[no-untyped-def]
    """Configure pytest-examples for documentation examples.

    This fixture wraps the default eval_example fixture to configure
    linting rules appropriate for documentation code blocks:
    - Ignore missing docstrings (D100, D101, D102, D103, D104, D105, D106, D107)
    - Ignore import sorting (I001) - docs show imports in readable order
    - Use double quotes to match project style
    - Target Python 3.12
    """
    eval_example.set_config(
        target_version="py310",
        quotes="double",
        ruff_ignore=[
            # Missing docstrings - docs examples don't need module/class/function docs
            "D100",  # Missing docstring in public module
            "D101",  # Missing docstring in public class
            "D102",  # Missing docstring in public method
            "D103",  # Missing docstring in public function
            "D104",  # Missing docstring in public package
            "D105",  # Missing docstring in magic method
            "D106",  # Missing docstring in public nested class
            "D107",  # Missing docstring in __init__
            "D413",  # Missing blank line after last section (docstring)
            # Import organization - docs show imports in logical order for readers
            "I001",  # Import block is un-sorted or un-formatted
            # Undefined names - docs show partial snippets without all imports
            "F821",  # Undefined name
            # Unused imports - docs show import sections that may not use everything
            "F401",  # Imported but unused
            # Redefinition - docs may show multiple import examples in one block
            "F811",  # Redefinition of unused name
            # Import order - docs show imports where they're needed for clarity
            "E402",  # Module level import not at top of file
        ],
    )
    return eval_example
