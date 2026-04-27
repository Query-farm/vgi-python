"""Tests for the settings feature."""

import pyarrow as pa
import pytest

from vgi._test_fixtures.table import SettingsAwareFunction
from vgi.arguments import Arguments
from vgi.client.client import Client, ClientError


class TestSettingsViaClient:
    """Tests using the full Client subprocess."""

    def test_settings_passed_to_function_verbose_false(self) -> None:
        """Settings should be passed through Client to function (typed values)."""
        with Client("vgi-fixture-worker") as client:
            outputs = list(
                client.table_function(
                    function_name="settings_aware",
                    arguments=Arguments(positional=(pa.scalar(3),)),
                    settings={
                        "vgi_verbose_mode": False,
                        "greeting": "Hi",
                        "multiplier": 2,
                    },
                )
            )

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 3
        assert table.schema.names == ["id", "greeting", "value"]
        assert table.column("id").to_pylist() == [0, 1, 2]
        assert table.column("greeting").to_pylist() == ["Hi", "Hi", "Hi"]
        # Values are multiplied by 2: 0*2.5*2=0, 1*2.5*2=5, 2*2.5*2=10
        assert table.column("value").to_pylist() == [0.0, 5.0, 10.0]

    def test_settings_passed_to_function_verbose_true(self) -> None:
        """Verbose mode should add details column (typed bool)."""
        with Client("vgi-fixture-worker") as client:
            outputs = list(
                client.table_function(
                    function_name="settings_aware",
                    arguments=Arguments(positional=(pa.scalar(3),)),
                    settings={
                        "vgi_verbose_mode": True,
                        "greeting": "Hello World",
                        "multiplier": 1,
                    },
                )
            )

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 3
        assert table.schema.names == ["id", "greeting", "value", "details"]
        assert table.column("id").to_pylist() == [0, 1, 2]
        assert table.column("greeting").to_pylist() == ["Hello World"] * 3
        assert table.column("value").to_pylist() == [0.0, 2.5, 5.0]
        assert table.column("details").to_pylist() == ["row_0", "row_1", "row_2"]

    def test_settings_as_strings_backward_compat(self) -> None:
        """String settings should still work for backward compatibility."""
        with Client("vgi-fixture-worker") as client:
            outputs = list(
                client.table_function(
                    function_name="settings_aware",
                    arguments=Arguments(positional=(pa.scalar(3),)),
                    settings={
                        "vgi_verbose_mode": "true",
                        "greeting": "Hi",
                        "multiplier": "2",
                    },
                )
            )

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 3
        assert table.schema.names == ["id", "greeting", "value", "details"]
        assert table.column("greeting").to_pylist() == ["Hi", "Hi", "Hi"]
        # Values are multiplied by 2: 0*2.5*2=0, 1*2.5*2=5, 2*2.5*2=10
        assert table.column("value").to_pylist() == [0.0, 5.0, 10.0]
        assert table.column("details").to_pylist() == ["row_0", "row_1", "row_2"]

    def test_missing_required_setting_fails(self) -> None:
        """Missing required setting should raise error."""
        with Client("vgi-fixture-worker") as client:
            with pytest.raises(ClientError) as exc_info:
                # Call without required settings - worker should send bind error
                list(
                    client.table_function(
                        function_name="settings_aware",
                        arguments=Arguments(positional=(pa.scalar(3),)),
                        # No settings provided
                    )
                )

            # The error message should indicate the missing setting
            assert "vgi_verbose_mode" in str(exc_info.value)


class TestMetadataSerialization:
    """Tests for metadata serialization with required_settings."""

    def test_required_settings_roundtrip(self) -> None:
        """required_settings should survive Arrow serialization."""
        from vgi.metadata import arrow_to_metadata, metadata_to_arrow

        meta = SettingsAwareFunction.get_metadata()
        # Auto-populated from Setting() annotations (sorted alphabetically)
        assert sorted(meta.required_settings) == ["greeting", "multiplier", "vgi_verbose_mode"]

        batch = metadata_to_arrow(meta)
        deserialized = arrow_to_metadata(batch)

        assert sorted(deserialized.required_settings) == [
            "greeting",
            "multiplier",
            "vgi_verbose_mode",
        ]
