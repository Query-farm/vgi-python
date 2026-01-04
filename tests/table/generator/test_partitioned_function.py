"""Tests for the PartitionedRangeFunction with multi-worker support."""

from __future__ import annotations

from typing import Any

import pyarrow as pa

from vgi.arguments import Arguments
from vgi.client import Client
from vgi.examples.table import PartitionedRangeFunction
from vgi.testing import TableFunctionTestClient

from .conftest import RunnerWithMode


def _sorted_non_null(values: list[Any | None]) -> list[Any]:
    """Return sorted list, filtering out None values for type safety."""
    return sorted(v for v in values if v is not None)


class TestPartitionedRangeFunctionInProcess:
    """In-process tests for the partitioned_range function."""

    def test_generates_full_range_single_worker(self) -> None:
        """Single worker should generate the complete range."""
        with TableFunctionTestClient(PartitionedRangeFunction) as client:
            outputs = list(
                client.table_function(arguments=Arguments(positional=(pa.scalar(10),)))
            )

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 10

        values = _sorted_non_null(table.column("value").to_pylist())
        assert values == list(range(10))

    def test_metadata(self) -> None:
        """Partitioned range function should have correct metadata."""
        meta = PartitionedRangeFunction.get_metadata()
        assert meta.name == "partitioned_range"
        # Should not have max_workers limit (parallelizable)
        assert meta.max_workers is None

    def test_zero_count(self) -> None:
        """Partitioned range with count=0 should produce no output."""
        with TableFunctionTestClient(PartitionedRangeFunction) as client:
            outputs = list(
                client.table_function(arguments=Arguments(positional=(pa.scalar(0),)))
            )

        assert len(outputs) == 0


class TestPartitionedRangeFunctionBothModes:
    """Tests that run both in-process and via Client subprocess."""

    def test_generates_correct_count(
        self, run_table_function_mode: RunnerWithMode
    ) -> None:
        """Partitioned range should generate exactly the requested number of rows."""
        runner, mode = run_table_function_mode
        outputs, logs = runner(PartitionedRangeFunction, (100,))

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 100

    def test_values_are_sequential(
        self, run_table_function_mode: RunnerWithMode
    ) -> None:
        """Partitioned range should produce all values in range."""
        runner, mode = run_table_function_mode
        outputs, logs = runner(PartitionedRangeFunction, (50,))

        table = pa.Table.from_batches(outputs)
        values = _sorted_non_null(table.column("value").to_pylist())
        assert values == list(range(50))


class TestPartitionedRangeFunctionMultiWorker:
    """Tests for multi-worker partitioned execution via Client."""

    def test_two_workers_produce_complete_range(self) -> None:
        """Two workers should together produce the complete range."""
        with Client("vgi-example-worker", max_workers=2) as client:
            outputs = list(
                client.table_function(
                    function_name="partitioned_range",
                    arguments=Arguments(positional=(pa.scalar(20),)),
                )
            )

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 20

        values = _sorted_non_null(table.column("value").to_pylist())
        assert values == list(range(20))

    def test_three_workers_produce_complete_range(self) -> None:
        """Three workers should together produce the complete range."""
        with Client("vgi-example-worker", max_workers=3) as client:
            outputs = list(
                client.table_function(
                    function_name="partitioned_range",
                    arguments=Arguments(positional=(pa.scalar(30),)),
                )
            )

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 30

        values = _sorted_non_null(table.column("value").to_pylist())
        assert values == list(range(30))

    def test_workers_produce_large_range(self) -> None:
        """Multiple workers should handle large ranges."""
        with Client("vgi-example-worker", max_workers=4) as client:
            outputs = list(
                client.table_function(
                    function_name="partitioned_range",
                    arguments=Arguments(positional=(pa.scalar(10000),)),
                )
            )

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 10000

        values = _sorted_non_null(table.column("value").to_pylist())
        assert values == list(range(10000))

    def test_uneven_distribution(self) -> None:
        """Workers should handle ranges that don't divide evenly."""
        # 7 items with 3 workers: worker 0 gets [0,3,6], worker 1 gets [1,4],
        # worker 2 gets [2,5]
        with Client("vgi-example-worker", max_workers=3) as client:
            outputs = list(
                client.table_function(
                    function_name="partitioned_range",
                    arguments=Arguments(positional=(pa.scalar(7),)),
                )
            )

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 7

        values = _sorted_non_null(table.column("value").to_pylist())
        assert values == list(range(7))

    def test_single_worker_fallback(self) -> None:
        """max_workers=1 should work like single worker mode."""
        with Client("vgi-example-worker", max_workers=1) as client:
            outputs = list(
                client.table_function(
                    function_name="partitioned_range",
                    arguments=Arguments(positional=(pa.scalar(15),)),
                )
            )

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 15

        values = _sorted_non_null(table.column("value").to_pylist())
        assert values == list(range(15))
