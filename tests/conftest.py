"""Shared fixtures for VGI tests."""

from typing import Any

import pyarrow as pa
import pytest


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
