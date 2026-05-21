# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Tests for ConstantColumnsFunction via Client."""

from __future__ import annotations

import pyarrow as pa

from tests.conftest import assert_total_rows
from vgi.arguments import Arguments
from vgi.client import Client


class TestConstantColumnsFunction:
    """Tests for ConstantColumnsFunction via Client subprocess."""

    def test_constant_columns_basic(self, fixture_worker: str) -> None:
        """Count=3, values 42 and 'hello' produces 3 rows with 2 columns."""
        with Client(fixture_worker) as client:
            outputs = list(
                client.table_function(
                    function_name="constant_columns",
                    arguments=Arguments(
                        positional=(pa.scalar(3), pa.scalar(42), pa.scalar("hello")),
                    ),
                )
            )

        assert_total_rows(outputs, 3)
        all_col0: list[object] = []
        all_col1: list[object] = []
        for batch in outputs:
            all_col0.extend(batch.column("col_0").to_pylist())
            all_col1.extend(batch.column("col_1").to_pylist())
        assert all_col0 == [42, 42, 42]
        assert all_col1 == ["hello", "hello", "hello"]

    def test_constant_columns_single_value(self, fixture_worker: str) -> None:
        """Single vararg value produces one column."""
        with Client(fixture_worker) as client:
            outputs = list(
                client.table_function(
                    function_name="constant_columns",
                    arguments=Arguments(
                        positional=(pa.scalar(2), pa.scalar(99)),
                    ),
                )
            )

        assert_total_rows(outputs, 2)
        all_values: list[object] = []
        for batch in outputs:
            all_values.extend(batch.column("col_0").to_pylist())
        assert all_values == [99, 99]
