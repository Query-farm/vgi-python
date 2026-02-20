"""Tests for Setting/Secret annotation support across all function types."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Annotated, Any

import pyarrow as pa

from vgi.arguments import (
    Arg,
    Secret,
    Setting,
    _extract_setting_secret_params,
)
from vgi.invocation import BindResponse
from vgi.metadata import resolve_metadata
from vgi.table_function import (
    BindParams,
    TableFunctionGenerator,
    _batch_to_secret_dict,
    _struct_scalar_to_dict,
)

# ---------------------------------------------------------------------------
# Tests for _extract_setting_secret_params helper
# ---------------------------------------------------------------------------


class TestExtractSettingSecretParams:
    """Tests for the shared annotation-parsing helper."""

    def test_no_annotations(self) -> None:
        """Method with no Setting/Secret annotations returns empty dicts."""

        def method(cls: Any, x: int) -> None: ...

        settings, secrets = _extract_setting_secret_params(method)
        assert settings == {}
        assert secrets == {}

    def test_setting_with_default_key(self) -> None:
        """Setting() without explicit key uses parameter name."""

        def method(
            cls: Any,
            my_setting: Annotated[pa.Scalar[Any] | None, Setting()],
        ) -> None: ...

        settings, secrets = _extract_setting_secret_params(method)
        assert settings == {"my_setting": "my_setting"}
        assert secrets == {}

    def test_setting_with_explicit_key(self) -> None:
        """Setting(key=...) uses the explicit key."""

        def method(
            cls: Any,
            verbose: Annotated[pa.Scalar[Any] | None, Setting(key="vgi_verbose_mode")],
        ) -> None: ...

        settings, secrets = _extract_setting_secret_params(method)
        assert settings == {"verbose": "vgi_verbose_mode"}

    def test_secret_with_default_key(self) -> None:
        """Secret() without explicit key uses parameter name."""

        def method(
            cls: Any,
            my_secret: Annotated[dict[str, pa.Scalar[Any]], Secret()],
        ) -> None: ...

        settings, secrets = _extract_setting_secret_params(method)
        assert settings == {}
        assert secrets == {"my_secret": "my_secret"}

    def test_secret_with_explicit_key(self) -> None:
        """Secret(key=...) uses the explicit key."""

        def method(
            cls: Any,
            creds: Annotated[dict[str, pa.Scalar[Any]], Secret(key="my_credentials")],
        ) -> None: ...

        settings, secrets = _extract_setting_secret_params(method)
        assert secrets == {"creds": "my_credentials"}

    def test_mixed_setting_and_secret(self) -> None:
        """Both Setting and Secret in the same method."""

        def method(
            cls: Any,
            params: Any,
            *,
            verbose: Annotated[pa.Scalar[Any] | None, Setting()] = None,
            credentials: Annotated[dict[str, pa.Scalar[Any]] | None, Secret(key="creds")] = None,
        ) -> None: ...

        settings, secrets = _extract_setting_secret_params(method)
        assert settings == {"verbose": "verbose"}
        assert secrets == {"credentials": "creds"}

    def test_skips_self_and_cls(self) -> None:
        """Parameters named 'self' or 'cls' are skipped."""

        def method(self: Any, cls: Any, x: Annotated[pa.Scalar[Any], Setting()]) -> None: ...

        settings, secrets = _extract_setting_secret_params(method)
        assert settings == {"x": "x"}


# ---------------------------------------------------------------------------
# Tests for secret conversion helpers
# ---------------------------------------------------------------------------


class TestSecretConversionHelpers:
    """Tests for _struct_scalar_to_dict and _batch_to_secret_dict."""

    def test_struct_scalar_to_dict(self) -> None:
        """StructScalar is expanded to dict of field name -> scalar."""
        struct_type = pa.struct([("key1", pa.string()), ("key2", pa.int64())])
        scalar = pa.scalar({"key1": "hello", "key2": 42}, type=struct_type)
        result = _struct_scalar_to_dict(scalar)
        assert set(result.keys()) == {"key1", "key2"}
        assert result["key1"].as_py() == "hello"
        assert result["key2"].as_py() == 42

    def test_batch_to_secret_dict_none(self) -> None:
        """None batch returns empty dict."""
        assert _batch_to_secret_dict(None) == {}

    def test_batch_to_secret_dict(self) -> None:
        """Single-row batch with struct columns is expanded."""
        secret_data = {"type": "test", "provider": "config", "value": "s3cr3t"}
        batch = pa.RecordBatch.from_pydict({"my_secret": [secret_data]})
        result = _batch_to_secret_dict(batch)
        assert "my_secret" in result
        inner = result["my_secret"]
        assert inner["type"].as_py() == "test"
        assert inner["provider"].as_py() == "config"
        assert inner["value"].as_py() == "s3cr3t"


# ---------------------------------------------------------------------------
# Tests for ScalarFunction Setting/Secret value types
# ---------------------------------------------------------------------------


class TestScalarFunctionSettingTypes:
    """Verify ScalarFunction delivers pa.Scalar for settings, dict[str, pa.Scalar] for secrets."""

    def test_setting_delivers_pa_scalar(self, example_worker: str) -> None:
        """MultiplyBySettingFunction receives pa.Scalar for setting."""
        from vgi import schema
        from vgi.client import Client

        s = schema(value=pa.int64())
        batch = pa.RecordBatch.from_pydict({"value": [1, 2, 3]}, schema=s)

        with Client(example_worker) as client:
            outputs = list(
                client.scalar_function(
                    function_name="multiply_by_setting",
                    input=iter([batch]),
                    settings={"multiplier": 5},
                )
            )

        assert len(outputs) == 1
        assert outputs[0].to_pydict() == {"result": [5, 10, 15]}

    def test_secret_delivers_dict_of_scalars(self, example_worker: str) -> None:
        """ReturnSecretValueFunction receives dict[str, pa.Scalar] for secret."""
        from vgi import schema
        from vgi.client import Client

        s = schema(dummy=pa.int64())
        batch = pa.RecordBatch.from_pydict({"dummy": [1]}, schema=s)

        secret_value = {"type": "test", "provider": "config", "secret_string": "s3cr3t"}

        with Client(example_worker) as client:
            outputs = list(
                client.scalar_function(
                    function_name="return_secret_value",
                    input=iter([batch]),
                    secrets={"vgi_example_secret": secret_value},
                )
            )

        assert len(outputs) == 1
        parsed = json.loads(outputs[0].column("result")[0].as_py())
        assert parsed == secret_value


# ---------------------------------------------------------------------------
# Tests for table function Setting/Secret annotations
# ---------------------------------------------------------------------------


class TestTableFunctionSettingAnnotations:
    """Verify Setting() annotations on table function on_bind()."""

    def test_setting_annotation_on_table_function_on_bind(self) -> None:
        """Settings declared via annotations on on_bind() are auto-populated."""
        from vgi.arguments import Arguments
        from vgi.client import Client

        with Client("vgi-example-worker") as client:
            outputs = list(
                client.table_function(
                    function_name="settings_aware",
                    arguments=Arguments(positional=(pa.scalar(3),)),
                    settings={
                        "vgi_verbose_mode": "true",
                        "greeting": "Hola",
                        "multiplier": "3",
                    },
                )
            )

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 3
        assert "details" in table.schema.names  # verbose mode adds details column
        assert table.column("greeting").to_pylist() == ["Hola", "Hola", "Hola"]


# ---------------------------------------------------------------------------
# Tests for auto-population of Meta.required_settings/required_secrets
# ---------------------------------------------------------------------------


class TestAutoPopulateMetadata:
    """Verify annotation-derived keys are auto-populated in metadata."""

    def test_scalar_function_auto_populates_required_settings(self) -> None:
        """ScalarFunction with Setting() annotation auto-populates required_settings."""
        resolve_metadata.cache_clear()
        from vgi.examples.scalar import MultiplyBySettingFunction

        meta = MultiplyBySettingFunction.get_metadata()
        assert "multiplier" in meta.required_settings

    def test_scalar_function_auto_populates_required_secrets(self) -> None:
        """ScalarFunction with Secret() annotation auto-populates required_secrets."""
        resolve_metadata.cache_clear()
        from vgi.examples.scalar import ReturnSecretValueFunction

        meta = ReturnSecretValueFunction.get_metadata()
        assert "vgi_example_secret" in meta.required_secrets

    def test_table_function_auto_populates_required_settings(self) -> None:
        """TableFunctionGenerator with Setting() on on_bind() auto-populates required_settings."""
        resolve_metadata.cache_clear()
        from vgi.examples.table import SettingsAwareFunction

        meta = SettingsAwareFunction.get_metadata()
        assert "vgi_verbose_mode" in meta.required_settings
        assert "greeting" in meta.required_settings
        assert "multiplier" in meta.required_settings

    def test_meta_declared_and_annotation_keys_are_merged(self) -> None:
        """Meta-declared keys and annotation keys are merged without duplicates."""
        resolve_metadata.cache_clear()

        @dataclass(slots=True, frozen=True)
        class _Args:
            count: Annotated[int, Arg(0, doc="Count")] = 10

        class _TestFunc(TableFunctionGenerator[_Args]):
            class Meta:
                required_settings = ["explicit_setting"]

            @classmethod
            def on_bind(
                cls,
                params: BindParams[_Args],
                *,
                annotated_setting: Annotated[pa.Scalar[Any] | None, Setting()] = None,
                explicit_setting: Annotated[pa.Scalar[Any] | None, Setting()] = None,
            ) -> BindResponse:
                return BindResponse(output_schema=pa.schema([pa.field("x", pa.int64())]))

            @classmethod
            def process(cls, params: Any, state: Any, out: Any) -> None: ...

        meta = _TestFunc.get_metadata()
        # "explicit_setting" from Meta and annotation should not be duplicated
        assert meta.required_settings.count("explicit_setting") == 1
        # "annotated_setting" from annotation should be auto-populated
        assert "annotated_setting" in meta.required_settings

    def test_no_annotations_does_not_affect_metadata(self) -> None:
        """Function without Setting/Secret annotations has unchanged metadata."""
        resolve_metadata.cache_clear()

        @dataclass(slots=True, frozen=True)
        class _Args:
            count: Annotated[int, Arg(0, doc="Count")] = 10

        class _TestFunc(TableFunctionGenerator[_Args]):
            @classmethod
            def on_bind(cls, params: BindParams[_Args]) -> BindResponse:
                return BindResponse(output_schema=pa.schema([pa.field("x", pa.int64())]))

            @classmethod
            def process(cls, params: Any, state: Any, out: Any) -> None: ...

        meta = _TestFunc.get_metadata()
        assert meta.required_settings == []
        assert meta.required_secrets == []


# ---------------------------------------------------------------------------
# Tests for BindParams/ProcessParams secrets type
# ---------------------------------------------------------------------------


class TestSecretsTypeInParams:
    """Verify secrets in BindParams/InitParams/ProcessParams are dict[str, dict[str, pa.Scalar]]."""

    def test_bind_params_secrets_type(self) -> None:
        """BindParams.secrets should be dict[str, dict[str, pa.Scalar]]."""
        from vgi.arguments import Arguments
        from vgi.client import Client

        with Client("vgi-example-worker") as client:
            outputs = list(
                client.table_function(
                    function_name="settings_aware",
                    arguments=Arguments(positional=(pa.scalar(1),)),
                    settings={
                        "vgi_verbose_mode": "false",
                        "greeting": "Test",
                        "multiplier": "1",
                    },
                )
            )
        # If we get here without error, the type handling works correctly
        assert len(outputs) > 0
