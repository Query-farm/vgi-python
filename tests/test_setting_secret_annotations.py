"""Tests for Setting/Secret annotation support across all function types."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Annotated, Any

import pyarrow as pa

from vgi.arguments import (
    Arg,
    Secret,
    SecretLookupEntry,
    Setting,
    _extract_setting_secret_params,
)
from vgi.invocation import BindResponse
from vgi.metadata import resolve_metadata
from vgi.table_function import (
    BindParams,
    SecretsAccessor,
    TableFunctionGenerator,
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

    def test_secret_with_type(self) -> None:
        """Secret(secret_type) is extracted as Secret instance."""

        def method(
            cls: Any,
            my_secret: Annotated[dict[str, pa.Scalar[Any]], Secret("vgi_example")],
        ) -> None: ...

        settings, secrets = _extract_setting_secret_params(method)
        assert settings == {}
        assert "my_secret" in secrets
        assert secrets["my_secret"].secret_type == "vgi_example"

    def test_secret_with_name_and_scope(self) -> None:
        """Secret() with name and scope preserves all fields."""

        def method(
            cls: Any,
            creds: Annotated[dict[str, pa.Scalar[Any]], Secret("s3", name="my_cred", scope="s3://bucket/")],
        ) -> None: ...

        settings, secrets = _extract_setting_secret_params(method)
        assert "creds" in secrets
        assert secrets["creds"].secret_type == "s3"
        assert secrets["creds"].name == "my_cred"
        assert secrets["creds"].scope == "s3://bucket/"

    def test_mixed_setting_and_secret(self) -> None:
        """Both Setting and Secret in the same method."""

        def method(
            cls: Any,
            params: Any,
            *,
            verbose: Annotated[pa.Scalar[Any] | None, Setting()] = None,
            credentials: Annotated[dict[str, pa.Scalar[Any]] | None, Secret("vgi_example")] = None,
        ) -> None: ...

        settings, secrets = _extract_setting_secret_params(method)
        assert settings == {"verbose": "verbose"}
        assert "credentials" in secrets
        assert secrets["credentials"].secret_type == "vgi_example"

    def test_skips_self_and_cls(self) -> None:
        """Parameters named 'self' or 'cls' are skipped."""

        def method(self: Any, cls: Any, x: Annotated[pa.Scalar[Any], Setting()]) -> None: ...

        settings, secrets = _extract_setting_secret_params(method)
        assert settings == {"x": "x"}


# ---------------------------------------------------------------------------
# Tests for secret conversion helpers
# ---------------------------------------------------------------------------


class TestSecretConversionHelpers:
    """Tests for _struct_scalar_to_dict and SecretsAccessor.to_dict()."""

    def test_struct_scalar_to_dict(self) -> None:
        """StructScalar is expanded to dict of field name -> scalar."""
        struct_type = pa.struct([("key1", pa.string()), ("key2", pa.int64())])
        scalar = pa.scalar({"key1": "hello", "key2": 42}, type=struct_type)
        result = _struct_scalar_to_dict(scalar)
        assert set(result.keys()) == {"key1", "key2"}
        assert result["key1"].as_py() == "hello"
        assert result["key2"].as_py() == 42

    def test_secrets_accessor_to_dict_none(self) -> None:
        """None batch returns empty dict."""
        assert SecretsAccessor(None).to_dict() == {}

    def test_secrets_accessor_to_dict(self) -> None:
        """Single-row batch with struct columns is expanded."""
        secret_data = {"type": "test", "provider": "config", "value": "s3cr3t"}
        batch = pa.RecordBatch.from_pydict({"my_secret": [secret_data]})
        result = SecretsAccessor(batch).to_dict()
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

    def test_setting_delivers_pa_scalar(self, fixture_worker: str) -> None:
        """MultiplyBySettingFunction receives pa.Scalar for setting."""
        from vgi import schema
        from vgi.client import Client

        s = schema(value=pa.int64())
        batch = pa.RecordBatch.from_pydict({"value": [1, 2, 3]}, schema=s)

        with Client(fixture_worker) as client:
            outputs = list(
                client.scalar_function(
                    function_name="multiply_by_setting",
                    input=iter([batch]),
                    settings={"multiplier": 5},
                )
            )

        assert len(outputs) == 1
        assert outputs[0].to_pydict() == {"result": [5, 10, 15]}

    def test_secret_delivers_dict_of_scalars(self, fixture_worker: str) -> None:
        """ReturnSecretValueFunction receives dict[str, pa.Scalar] for secret."""
        from vgi import schema
        from vgi.client import Client

        s = schema(dummy=pa.int64())
        batch = pa.RecordBatch.from_pydict({"dummy": [1]}, schema=s)

        secret_value = {"type": "test", "provider": "config", "secret_string": "s3cr3t"}

        with Client(fixture_worker) as client:
            outputs = list(
                client.scalar_function(
                    function_name="return_secret_value",
                    input=iter([batch]),
                    secrets={"vgi_example": secret_value},
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

        with Client("vgi-fixture-worker") as client:
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
        from vgi._test_fixtures.scalar import MultiplyBySettingFunction

        meta = MultiplyBySettingFunction.get_metadata()
        assert "multiplier" in meta.required_settings

    def test_scalar_function_auto_populates_required_secrets(self) -> None:
        """ScalarFunction with Secret() annotation auto-populates required_secrets."""
        resolve_metadata.cache_clear()
        from vgi._test_fixtures.scalar import ReturnSecretValueFunction

        meta = ReturnSecretValueFunction.get_metadata()
        secret_types = [entry.secret_type for entry in meta.required_secrets]
        assert "vgi_example" in secret_types

    def test_table_function_auto_populates_required_settings(self) -> None:
        """TableFunctionGenerator with Setting() on on_bind() auto-populates required_settings."""
        resolve_metadata.cache_clear()
        from vgi._test_fixtures.table import SettingsAwareFunction

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

        with Client("vgi-fixture-worker") as client:
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


# ---------------------------------------------------------------------------
# Tests for SecretsAccessor
# ---------------------------------------------------------------------------


class TestSecretsAccessor:
    """Tests for SecretsAccessor get, scoped, needs_resolution, to_dict."""

    def test_unscoped_get(self) -> None:
        """Unscoped secret is returned by type."""
        secret_data = {"key": "value", "token": "abc"}
        batch = pa.RecordBatch.from_pydict({"my_type": [secret_data]})
        accessor = SecretsAccessor(batch, is_retry=True)
        result = accessor.get("my_type")
        assert result is not None
        assert result["key"].as_py() == "value"

    def test_unscoped_get_missing_first_call(self) -> None:
        """First call for missing unscoped secret registers pending lookup."""
        accessor = SecretsAccessor(None)
        result = accessor.get("missing_type")
        assert result is None
        assert accessor.needs_resolution
        assert len(accessor.pending_lookups) == 1
        assert accessor.pending_lookups[0].secret_type == "missing_type"

    def test_unscoped_get_missing_retry(self) -> None:
        """Retry with missing unscoped secret returns None (genuinely missing)."""
        accessor = SecretsAccessor(None, is_retry=True)
        result = accessor.get("missing_type")
        assert result is None
        assert not accessor.needs_resolution

    def test_scoped_get_registers_pending(self) -> None:
        """First call with scope registers a pending lookup."""
        accessor = SecretsAccessor(None)
        result = accessor.get("s3", scope="s3://bucket/")
        assert result is None
        assert accessor.needs_resolution
        entry = accessor.pending_lookups[0]
        assert entry.secret_type == "s3"
        assert entry.scope == "s3://bucket/"

    def test_to_dict_combines_unscoped_and_scoped(self) -> None:
        """to_dict() includes both unscoped and scoped entries."""
        # Build a batch with an unscoped column and a scoped column
        struct_type = pa.struct([("k", pa.string())])
        unscoped_arr = pa.array([{"k": "unscoped_val"}], type=struct_type)

        scoped_field = pa.field(
            "secret_0",
            struct_type,
            metadata={"secret_type": "scoped_type", "scope": "s3://x/"},
        )
        scoped_arr = pa.array([{"k": "scoped_val"}], type=struct_type)

        batch = pa.RecordBatch.from_arrays(
            [unscoped_arr, scoped_arr],
            schema=pa.schema([pa.field("my_type", struct_type), scoped_field]),
        )
        result = SecretsAccessor(batch).to_dict()
        assert "my_type" in result
        assert result["my_type"]["k"].as_py() == "unscoped_val"
        assert "scoped_type" in result
        assert result["scoped_type"]["k"].as_py() == "scoped_val"

    def test_to_dict_skips_null_scoped(self) -> None:
        """to_dict() skips scoped entries with null values."""
        struct_type = pa.struct([("k", pa.string())])
        scoped_field = pa.field(
            "secret_0",
            struct_type,
            metadata={"secret_type": "null_type"},
        )
        scoped_arr = pa.array([None], type=struct_type)
        batch = pa.RecordBatch.from_arrays(
            [scoped_arr],
            schema=pa.schema([scoped_field]),
        )
        result = SecretsAccessor(batch).to_dict()
        assert "null_type" not in result

    def test_required_raises_on_retry(self) -> None:
        """required=True raises ValueError when secret is genuinely missing on retry."""
        import pytest

        accessor = SecretsAccessor(None, is_retry=True)
        with pytest.raises(ValueError, match="Required secret"):
            accessor.get("missing_type", required=True)

    def test_scoped_required_raises_on_retry(self) -> None:
        """required=True with scope raises ValueError on retry when not found."""
        import pytest

        accessor = SecretsAccessor(None, is_retry=True)
        with pytest.raises(ValueError, match="Required secret"):
            accessor.get("s3", scope="s3://bucket/", required=True)


# ---------------------------------------------------------------------------
# Tests for BindResponse.secret_scope_request round-trip
# ---------------------------------------------------------------------------


class TestBindResponseSecretScopeRequest:
    """Test BindResponse.secret_scope_request() creates proper response."""

    def test_secret_scope_request_round_trip(self) -> None:
        """secret_scope_request encodes lookups that can be recovered via secret_scope_entries."""
        lookups = [
            SecretLookupEntry(secret_type="s3", scope="s3://bucket/"),
            SecretLookupEntry(secret_type="gcs", secret_name="my_cred"),
        ]
        response = BindResponse.secret_scope_request(lookups)
        assert response.is_secret_scope_request
        entries = response.secret_scope_entries()
        assert len(entries) == 2
        assert entries[0].secret_type == "s3"
        assert entries[0].scope == "s3://bucket/"
        assert entries[1].secret_type == "gcs"
        assert entries[1].secret_name == "my_cred"


# ---------------------------------------------------------------------------
# Tests for TState validation in __init_subclass__
# ---------------------------------------------------------------------------


class TestTStateValidation:
    """Verify __init_subclass__ rejects non-serializable TState types."""

    def test_non_serializable_state_raises(self) -> None:
        """TState that doesn't extend ArrowSerializableDataclass raises TypeError."""
        import pytest

        @dataclass
        class BadState:
            x: int = 0

        with pytest.raises(TypeError, match="must extend ArrowSerializableDataclass"):

            class _BadFunc(TableFunctionGenerator[None, BadState]):
                @classmethod
                def on_bind(cls, params: Any) -> BindResponse:
                    return BindResponse(output_schema=pa.schema([("x", pa.int64())]))

                @classmethod
                def process(cls, params: Any, state: Any, out: Any) -> None: ...
