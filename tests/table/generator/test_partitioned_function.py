"""Tests for the PartitionedSequenceFunction with multi-worker support."""

from __future__ import annotations

from typing import Any

import pyarrow as pa

from vgi.arguments import Arguments
from vgi.client import Client


def _sorted_non_null(values: list[Any | None]) -> list[Any]:
    """Return sorted list, filtering out None values for type safety."""
    return sorted(v for v in values if v is not None)


class TestPartitionedSequenceFunctionMultiWorker:
    """Tests for multi-worker partitioned execution via Client."""

    def test_two_workers_produce_complete_sequence(self) -> None:
        """Two workers should together produce the complete sequence."""
        with Client("vgi-example-worker", worker_limit=2) as client:
            outputs = list(
                client.table_function(
                    function_name="partitioned_sequence",
                    arguments=Arguments(positional=(pa.scalar(20),)),
                )
            )

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 20

        values = _sorted_non_null(table.column("n").to_pylist())
        assert values == list(range(20))

    def test_three_workers_produce_complete_sequence(self) -> None:
        """Three workers should together produce the complete sequence."""
        with Client("vgi-example-worker", worker_limit=3) as client:
            outputs = list(
                client.table_function(
                    function_name="partitioned_sequence",
                    arguments=Arguments(positional=(pa.scalar(30),)),
                )
            )

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 30

        values = _sorted_non_null(table.column("n").to_pylist())
        assert values == list(range(30))

    def test_workers_produce_large_sequence(self) -> None:
        """Multiple workers should handle large sequences."""
        with Client("vgi-example-worker", worker_limit=4) as client:
            outputs = list(
                client.table_function(
                    function_name="partitioned_sequence",
                    arguments=Arguments(positional=(pa.scalar(10000),)),
                )
            )

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 10000

        values = _sorted_non_null(table.column("n").to_pylist())
        assert values == list(range(10000))

    def test_uneven_distribution(self) -> None:
        """Workers should handle sequences that don't divide evenly."""
        with Client("vgi-example-worker", worker_limit=3) as client:
            outputs = list(
                client.table_function(
                    function_name="partitioned_sequence",
                    arguments=Arguments(positional=(pa.scalar(7),)),
                )
            )

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 7

        values = _sorted_non_null(table.column("n").to_pylist())
        assert values == list(range(7))

    def test_single_worker_fallback(self) -> None:
        """worker_limit=1 should work like single worker mode."""
        with Client("vgi-example-worker", worker_limit=1) as client:
            outputs = list(
                client.table_function(
                    function_name="partitioned_sequence",
                    arguments=Arguments(positional=(pa.scalar(15),)),
                )
            )

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 15

        values = _sorted_non_null(table.column("n").to_pylist())
        assert values == list(range(15))

    def test_increment_with_multi_workers(self) -> None:
        """Multiple workers should handle increment parameter."""
        with Client("vgi-example-worker", worker_limit=2) as client:
            outputs = list(
                client.table_function(
                    function_name="partitioned_sequence",
                    arguments=Arguments(
                        positional=(pa.scalar(10),),
                        named={"increment": pa.scalar(5)},
                    ),
                )
            )

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 10

        values = _sorted_non_null(table.column("n").to_pylist())
        assert values == [0, 5, 10, 15, 20, 25, 30, 35, 40, 45]
