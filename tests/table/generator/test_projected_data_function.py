# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Tests for the ProjectedDataFunction demonstrating projection pushdown."""

import pyarrow as pa

from vgi.arguments import Arguments


class TestProjectedDataFunctionViaClient:
    """Tests that run via Client subprocess."""

    def test_projection_via_client(self) -> None:
        """Projection should work correctly via Client subprocess."""
        from vgi.client import Client

        with Client("vgi-fixture-worker", worker_limit=1) as client:
            outputs = list(
                client.table_function(
                    function_name="projected_data",
                    schema_name="main",
                    arguments=Arguments(positional=(pa.scalar(5),)),
                    projection_ids=[0, 2],  # id and value only
                )
            )

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 5
        assert table.num_columns == 2
        assert table.schema.names == ["id", "value"]

        assert table.column("id").to_pylist() == [0, 1, 2, 3, 4]
        assert table.column("value").to_pylist() == [0.0, 1.5, 3.0, 4.5, 6.0]

    def test_all_columns_via_client(self) -> None:
        """All columns should be returned when no projection specified."""
        from vgi.client import Client

        with Client("vgi-fixture-worker", worker_limit=1) as client:
            outputs = list(
                client.table_function(
                    function_name="projected_data",
                    schema_name="main",
                    arguments=Arguments(positional=(pa.scalar(3),)),
                )
            )

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 3
        assert table.num_columns == 4
        assert table.schema.names == ["id", "name", "value", "extra"]
