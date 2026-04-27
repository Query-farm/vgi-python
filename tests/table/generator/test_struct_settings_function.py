"""Tests for the StructSettingsFunction (struct settings)."""

import pyarrow as pa

from vgi.arguments import Arguments
from vgi.client import Client


class TestStructSettingsFunction:
    """Tests for struct_settings function."""

    def test_struct_setting_basic(self) -> None:
        """Struct setting should configure sequence generation."""
        with Client("vgi-fixture-worker") as client:
            outputs = list(
                client.table_function(
                    function_name="struct_settings",
                    arguments=Arguments(positional=(pa.scalar(3),)),
                    settings={
                        "config": {"start": 10, "step": 5, "label": "item"},
                    },
                )
            )

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 3
        assert table.column("n").to_pylist() == [10, 15, 20]
        assert table.column("label").to_pylist() == ["item_0", "item_1", "item_2"]

    def test_struct_setting_different_values(self) -> None:
        """Different struct values should produce different output."""
        with Client("vgi-fixture-worker") as client:
            outputs = list(
                client.table_function(
                    function_name="struct_settings",
                    arguments=Arguments(positional=(pa.scalar(4),)),
                    settings={
                        "config": {"start": 0, "step": 100, "label": "row"},
                    },
                )
            )

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 4
        assert table.column("n").to_pylist() == [0, 100, 200, 300]
        assert table.column("label").to_pylist() == ["row_0", "row_1", "row_2", "row_3"]
