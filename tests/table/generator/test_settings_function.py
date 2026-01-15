"""Tests for the settings feature."""

import pyarrow as pa
import pytest
import structlog

from tests.conftest import make_invocation
from vgi.arguments import Arguments
from vgi.client.client import Client, ClientError
from vgi.examples.table import SettingsAwareFunction
from vgi.invocation import Invocation, InvocationType
from vgi.ipc_utils import deserialize_record_batch


class TestSettingsInProcess:
    """In-process tests for the settings feature."""

    def test_metadata_has_required_settings(self) -> None:
        """Function should declare required_settings in metadata."""
        meta = SettingsAwareFunction.get_metadata()
        assert meta.required_settings == ["vgi_verbose_mode", "greeting", "multiplier"]

    def test_settings_accessor_returns_empty_when_none(self) -> None:
        """Settings property should return empty dict when no settings provided."""
        invocation = make_invocation(
            function_name="settings_aware",
            arguments=Arguments(positional=(pa.scalar(3),)),
        )
        func = SettingsAwareFunction(
            invocation=invocation,
            logger=structlog.get_logger(),
        )
        assert func.settings == {}

    def test_settings_accessor_returns_settings(self) -> None:
        """Settings property should return provided settings."""
        invocation = make_invocation(
            function_name="settings_aware",
            arguments=Arguments(positional=(pa.scalar(3),)),
            settings={"vgi_verbose_mode": "true", "other": "value"},
        )
        func = SettingsAwareFunction(
            invocation=invocation,
            logger=structlog.get_logger(),
        )
        assert func.settings == {"vgi_verbose_mode": "true", "other": "value"}

    def test_get_setting_with_default(self) -> None:
        """get_setting should return default when setting not present."""
        invocation = make_invocation(
            function_name="settings_aware",
            arguments=Arguments(positional=(pa.scalar(3),)),
        )
        func = SettingsAwareFunction(
            invocation=invocation,
            logger=structlog.get_logger(),
        )
        assert func.get_setting("nonexistent", "default") == "default"
        assert func.get_setting("nonexistent") is None

    def test_get_setting_returns_value(self) -> None:
        """get_setting should return value when setting is present."""
        invocation = make_invocation(
            function_name="settings_aware",
            arguments=Arguments(positional=(pa.scalar(3),)),
            settings={"vgi_verbose_mode": "true"},
        )
        func = SettingsAwareFunction(
            invocation=invocation,
            logger=structlog.get_logger(),
        )
        assert func.get_setting("vgi_verbose_mode") == "true"

    def test_output_schema_without_verbose(self) -> None:
        """Output schema should have 3 columns when verbose is false."""
        invocation = make_invocation(
            function_name="settings_aware",
            arguments=Arguments(positional=(pa.scalar(3),)),
            settings={"vgi_verbose_mode": "false"},
        )
        func = SettingsAwareFunction(
            invocation=invocation,
            logger=structlog.get_logger(),
        )
        assert len(func.output_schema) == 3
        assert func.output_schema.names == ["id", "greeting", "value"]

    def test_output_schema_with_verbose(self) -> None:
        """Output schema should have 4 columns when verbose is true."""
        invocation = make_invocation(
            function_name="settings_aware",
            arguments=Arguments(positional=(pa.scalar(3),)),
            settings={"vgi_verbose_mode": "true"},
        )
        func = SettingsAwareFunction(
            invocation=invocation,
            logger=structlog.get_logger(),
        )
        assert len(func.output_schema) == 4
        assert func.output_schema.names == ["id", "greeting", "value", "details"]


class TestSettingsViaClient:
    """Tests using the full Client subprocess."""

    def test_settings_passed_to_function_verbose_false(self) -> None:
        """Settings should be passed through Client to function."""
        with Client("vgi-example-worker") as client:
            outputs = list(
                client.table_function(
                    function_name="settings_aware",
                    arguments=Arguments(positional=(pa.scalar(3),)),
                    settings={
                        "vgi_verbose_mode": "false",
                        "greeting": "Hi",
                        "multiplier": "2",
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
        """Verbose mode should add details column."""
        with Client("vgi-example-worker") as client:
            outputs = list(
                client.table_function(
                    function_name="settings_aware",
                    arguments=Arguments(positional=(pa.scalar(3),)),
                    settings={
                        "vgi_verbose_mode": "true",
                        "greeting": "Hello World",
                        "multiplier": "1",
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

    def test_missing_required_setting_fails(self) -> None:
        """Missing required setting should raise error."""
        with Client("vgi-example-worker") as client:
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


class TestInvocationSerialization:
    """Tests for Invocation serialization with settings."""

    @pytest.mark.parametrize(
        "settings",
        [{"TimeZone": "UTC", "threads": "4"}, None],
        ids=["with_settings", "none_settings"],
    )
    def test_settings_roundtrip(self, settings: dict[str, str] | None) -> None:
        """Settings should survive serialization/deserialization."""
        original = Invocation(
            function_name="test",
            input_schema=None,
            function_type=InvocationType.TABLE,
            correlation_id="test",
            invocation_id=b"test-id",
            settings=settings,
        )

        batch = deserialize_record_batch(original.serialize())
        deserialized = Invocation.deserialize(batch)

        assert deserialized.settings == settings


class TestMetadataSerialization:
    """Tests for metadata serialization with required_settings."""

    def test_required_settings_roundtrip(self) -> None:
        """required_settings should survive Arrow serialization."""
        from vgi.metadata import arrow_to_metadata, metadata_to_arrow

        meta = SettingsAwareFunction.get_metadata()
        assert meta.required_settings == ["vgi_verbose_mode", "greeting", "multiplier"]

        batch = metadata_to_arrow(meta)
        deserialized = arrow_to_metadata(batch)

        assert deserialized.required_settings == [
            "vgi_verbose_mode",
            "greeting",
            "multiplier",
        ]
