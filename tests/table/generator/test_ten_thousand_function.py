# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Tests for TenThousandFunction via Client."""

from __future__ import annotations

from tests.conftest import assert_total_rows
from vgi.client import Client


class TestTenThousandFunction:
    """Tests for TenThousandFunction via Client subprocess."""

    def test_ten_thousand_basic(self, fixture_worker: str) -> None:
        """Returns exactly 10000 rows with values 0..9999."""
        with Client(fixture_worker) as client:
            outputs = list(
                client.table_function(
                    function_name="ten_thousand",
                )
            )

        assert_total_rows(outputs, 10000)

        # Collect all values and verify complete sequence
        all_values: list[object] = []
        for batch in outputs:
            all_values.extend(batch.column("n").to_pylist())
        assert all_values == list(range(10000))
