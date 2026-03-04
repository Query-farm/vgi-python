"""Tests for the FilterEchoFunction."""

import pyarrow as pa

from vgi.arguments import Arguments
from vgi.client import Client


class TestFilterEchoFunction:
    """Tests for FilterEchoFunction via Client (wire protocol)."""

    def test_no_filter_output(self) -> None:
        """Without filters, pushed_filters should be '(none)' for all rows."""
        with Client("vgi-example-worker") as client:
            batches = list(
                client.table_function(
                    function_name="filter_echo",
                    arguments=Arguments(positional=(pa.scalar(10),)),
                )
            )

        table = pa.Table.from_batches(batches)
        assert len(table) == 10

        # Check schema
        assert "n" in table.schema.names
        assert "s" in table.schema.names
        assert "pushed_filters" in table.schema.names

        # All pushed_filters should be "(none)"
        for val in table.column("pushed_filters").to_pylist():
            assert val == "(none)"

    def test_count_argument(self) -> None:
        """Verify row count matches the count argument."""
        with Client("vgi-example-worker") as client:
            batches = list(
                client.table_function(
                    function_name="filter_echo",
                    arguments=Arguments(positional=(pa.scalar(5),)),
                )
            )

        table = pa.Table.from_batches(batches)
        assert len(table) == 5

    def test_s_column_values(self) -> None:
        """The s column should match 'row_{n}' pattern."""
        with Client("vgi-example-worker") as client:
            batches = list(
                client.table_function(
                    function_name="filter_echo",
                    arguments=Arguments(positional=(pa.scalar(5),)),
                )
            )

        table = pa.Table.from_batches(batches)
        n_values = table.column("n").to_pylist()
        s_values = table.column("s").to_pylist()

        for n, s in zip(n_values, s_values, strict=True):
            assert s == f"row_{n}"
