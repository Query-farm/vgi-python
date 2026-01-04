"""Shared fixtures for VGI tests."""

from typing import Any

import pyarrow as pa
import pytest
import structlog

from vgi.function import Arguments, Invocation, InvocationType

# =============================================================================
# Utility Functions (not fixtures, can be imported directly)
# =============================================================================


def make_schema(fields: list[Any]) -> pa.Schema:
    """Create schema with proper typing for field list.

    This is a helper to avoid mypy errors when creating schemas from
    field tuples like [("name", pa.string())].
    """
    return pa.schema(fields)


def make_invocation(
    input_schema: pa.Schema | None = None,
    function_type: InvocationType = InvocationType.TABLE,
    arguments: Arguments | None = None,
    function_name: str = "test",
) -> Invocation:
    """Create a test invocation with flexible parameters."""
    return Invocation(
        function_name=function_name,
        input_schema=input_schema,
        function_type=function_type,
        correlation_id="test",
        invocation_id=b"test",
        arguments=arguments or Arguments(),
    )


def make_scalar_invocation(
    input_schema: pa.Schema,
    arguments: Arguments | None = None,
) -> Invocation:
    """Create a scalar function test invocation."""
    return make_invocation(
        input_schema=input_schema,
        function_type=InvocationType.SCALAR,
        arguments=arguments,
    )


def make_table_invocation(
    input_schema: pa.Schema,
    arguments: Arguments | None = None,
) -> Invocation:
    """Create a table function test invocation."""
    return make_invocation(
        input_schema=input_schema,
        function_type=InvocationType.TABLE,
        arguments=arguments,
    )


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
    return pa.RecordBatch.from_pydict(
        {field.name: [] for field in schema}, schema=schema
    )


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def test_logger() -> structlog.stdlib.BoundLogger:
    """Provide a shared test logger."""
    logger: structlog.stdlib.BoundLogger = structlog.get_logger().bind(component="test")
    return logger


@pytest.fixture
def example_worker() -> str:
    """Return the path to the example worker."""
    return "vgi-example-worker"


@pytest.fixture
def simple_batches() -> list[pa.RecordBatch]:
    """Create simple test batches with integer and string columns."""
    fields: list[pa.Field[Any]] = [
        pa.field("id", pa.int64()),
        pa.field("value", pa.int64()),
        pa.field("name", pa.string()),
    ]
    schema = pa.schema(fields)
    batch1 = pa.RecordBatch.from_pydict(
        {"id": [1, 2], "value": [10, 20], "name": ["a", "b"]},
        schema=schema,
    )
    batch2 = pa.RecordBatch.from_pydict(
        {"id": [3, 4], "value": [30, 40], "name": ["c", "d"]},
        schema=schema,
    )
    return [batch1, batch2]


@pytest.fixture
def numeric_batches() -> list[pa.RecordBatch]:
    """Create test batches with only numeric columns for sum tests."""
    fields: list[pa.Field[Any]] = [
        pa.field("a", pa.int32()),
        pa.field("b", pa.float64()),
    ]
    schema = pa.schema(fields)
    batch1 = pa.RecordBatch.from_pydict(
        {"a": [1, 2, 3], "b": [1.5, 2.5, 3.0]},
        schema=schema,
    )
    batch2 = pa.RecordBatch.from_pydict(
        {"a": [4, 5], "b": [4.0, 5.0]},
        schema=schema,
    )
    return [batch1, batch2]
