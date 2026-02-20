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
