# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Tests for worker-side projection pushdown enforcement.

Verifies that projection_ids are only applied when a function's metadata
has projection_pushdown=True. When False, projection_ids should be ignored
and all columns returned.
"""

import pyarrow as pa

from vgi.arguments import Arguments
from vgi.client import Client


class TestProjectionEnforcement:
    """Tests that projection_ids are gated by the function's projection_pushdown flag."""

    def test_projection_ids_ignored_when_pushdown_false(self) -> None:
        """Non-projecting function should return all columns even when projection_ids sent."""
        with Client("vgi-fixture-worker", worker_limit=1) as client:
            outputs = list(
                client.table_function(
                    function_name="named_params_echo",
                    arguments=Arguments(positional=(pa.scalar(3),)),
                    projection_ids=[0, 2],  # id and value — should be ignored
                )
            )

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 3
        # All 5 columns should be present since projection_pushdown=False
        assert table.num_columns == 5
        assert table.schema.names == ["id", "greeting", "value", "float_value", "enabled"]

    def test_projection_ids_applied_when_pushdown_true(self) -> None:
        """Projecting function should return only projected columns."""
        with Client("vgi-fixture-worker", worker_limit=1) as client:
            outputs = list(
                client.table_function(
                    function_name="projected_data",
                    arguments=Arguments(positional=(pa.scalar(3),)),
                    projection_ids=[0, 2],  # id and value
                )
            )

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 3
        assert table.num_columns == 2
        assert table.schema.names == ["id", "value"]

    def test_no_projection_ids_returns_all_columns(self) -> None:
        """Both functions return all columns when no projection_ids specified."""
        with Client("vgi-fixture-worker", worker_limit=1) as client:
            echo_outputs = list(
                client.table_function(
                    function_name="named_params_echo",
                    arguments=Arguments(positional=(pa.scalar(2),)),
                )
            )
            proj_outputs = list(
                client.table_function(
                    function_name="projected_data",
                    arguments=Arguments(positional=(pa.scalar(2),)),
                )
            )

        echo_table = pa.Table.from_batches(echo_outputs)
        assert echo_table.num_columns == 5

        proj_table = pa.Table.from_batches(proj_outputs)
        assert proj_table.num_columns == 4
