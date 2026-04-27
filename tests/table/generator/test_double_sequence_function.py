"""Tests for DoubleSequenceFunction via Client."""

from __future__ import annotations

import pyarrow as pa

from tests.conftest import assert_total_rows
from vgi.arguments import Arguments
from vgi.client import Client


class TestDoubleSequenceFunction:
    """Tests for DoubleSequenceFunction via Client subprocess."""

    def test_double_sequence_basic(self, fixture_worker: str) -> None:
        """Count=5 produces [0.0, 1.0, 2.0, 3.0, 4.0]."""
        with Client(fixture_worker) as client:
            outputs = list(
                client.table_function(
                    function_name="double_sequence",
                    arguments=Arguments(positional=(pa.scalar(5),)),
                )
            )

        assert_total_rows(outputs, 5)
        all_values: list[object] = []
        for batch in outputs:
            all_values.extend(batch.column("n").to_pylist())
        assert all_values == [0.0, 1.0, 2.0, 3.0, 4.0]

    def test_double_sequence_with_increment(self, fixture_worker: str) -> None:
        """Count=3 with increment=0.5 produces [0.0, 0.5, 1.0]."""
        with Client(fixture_worker) as client:
            outputs = list(
                client.table_function(
                    function_name="double_sequence",
                    arguments=Arguments(
                        positional=(pa.scalar(3),),
                        named={"increment": pa.scalar(0.5)},
                    ),
                )
            )

        assert_total_rows(outputs, 3)
        all_values: list[object] = []
        for batch in outputs:
            all_values.extend(batch.column("n").to_pylist())
        assert all_values == [0.0, 0.5, 1.0]
