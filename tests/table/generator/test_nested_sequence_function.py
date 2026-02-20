"""Tests for NestedSequenceFunction via Client."""

from __future__ import annotations

import pyarrow as pa

from tests.conftest import assert_total_rows
from vgi.arguments import Arguments
from vgi.client import Client


class TestNestedSequenceFunction:
    """Tests for NestedSequenceFunction via Client subprocess."""

    def test_nested_sequence_basic(self, example_worker: str) -> None:
        """Count=5 produces 5 rows with n, metadata struct, and history list."""
        with Client(example_worker) as client:
            outputs = list(
                client.table_function(
                    function_name="nested_sequence",
                    arguments=Arguments(positional=(pa.scalar(5),)),
                )
            )

        assert_total_rows(outputs, 5)

        # Collect all rows
        all_n: list[object] = []
        for batch in outputs:
            all_n.extend(batch.column("n").to_pylist())
        assert all_n == [0, 1, 2, 3, 4]

        # Verify schema has expected columns
        output_schema = outputs[0].schema
        assert "n" in output_schema.names
        assert "metadata" in output_schema.names
        assert "history" in output_schema.names
        assert pa.types.is_struct(output_schema.field("metadata").type)
        assert pa.types.is_list(output_schema.field("history").type)

    def test_nested_sequence_struct_content(self, example_worker: str) -> None:
        """Verify metadata.index and metadata.label values."""
        with Client(example_worker) as client:
            outputs = list(
                client.table_function(
                    function_name="nested_sequence",
                    arguments=Arguments(positional=(pa.scalar(3),)),
                )
            )

        assert_total_rows(outputs, 3)

        all_metadata: list[object] = []
        all_history: list[object] = []
        for batch in outputs:
            all_metadata.extend(batch.column("metadata").to_pylist())
            all_history.extend(batch.column("history").to_pylist())

        # Verify struct content
        assert all_metadata[0] == {"index": 0, "label": "row_0"}
        assert all_metadata[1] == {"index": 1, "label": "row_1"}
        assert all_metadata[2] == {"index": 2, "label": "row_2"}

        # Verify history lists
        assert all_history[0] == [0]
        assert all_history[1] == [0, 1]
        assert all_history[2] == [0, 1, 2]
