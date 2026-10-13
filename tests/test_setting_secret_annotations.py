# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Tests for Setting/Secret annotation support across all function types."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Annotated, Any

import pyarrow as pa
import pytest

from tests.conftest import FIXTURE_WORKER
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
                    schema_path=["main"],
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
                    schema_path=["main"],
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

        with Client(FIXTURE_WORKER) as client:
            outputs = list(
                client.table_function(
                    function_name="settings_aware",
                    schema_path=["main"],
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

    def test_secret_delivers_dict_of_scalars(self, fixture_worker: str) -> None:
        """SecretDemoFunction (table function) receives a client-supplied secret.

        Regression for `Client.table_function` hardcoding `secrets=None` on
        the wire — the same `vgi_example` secret shape already proven to work
        for `ReturnSecretValueFunction` (scalar) above, now exercised through
        the table-function path instead.
        """
        from vgi.client import Client

        # SecretDemoFunction resolves via `params.secrets.of_type("vgi_example")`
        # (not the `Secret()`-annotation path `test_secret_delivers_dict_of_scalars`
        # above uses), which filters on the secret payload's OWN `type` field —
        # so, unlike that test's arbitrary `"type": "test"`, this one must be
        # `"type": "vgi_example"` to match.
        secret_value = {"type": "vgi_example", "provider": "config", "secret_string": "s3cr3t"}

        with Client(fixture_worker) as client:
            outputs = list(
                client.table_function(
                    function_name="secret_demo",
                    schema_path=["main"],
                    secrets={"vgi_example": secret_value},
                )
            )

        table = pa.Table.from_batches(outputs)
        assert dict(zip(table.column("key").to_pylist(), table.column("value").to_pylist(), strict=True)) == {
            "type": "vgi_example",
            "provider": "config",
            "secret_string": "s3cr3t",
        }

    def test_secret_delivers_to_table_in_out_function(self, fixture_worker: str) -> None:
        """SecretInOutFunction receives a client-supplied secret.

        Regression for `Client.table_in_out_function` hardcoding `secrets=None`
        on the wire — the fixture (`secret_in_out`) already existed but had no
        test exercising it end to end, which is exactly how the bug went
        unnoticed.
        """
        from vgi.client import Client

        # Same `of_type("vgi_example")` resolution as SecretDemoFunction above —
        # the payload's own `type` field must equal the requested secret_type.
        secret_value = {"type": "vgi_example", "provider": "config", "secret_string": "s3cr3t"}
        batch = pa.RecordBatch.from_pydict({"n": [1, 2]})

        with Client(fixture_worker) as client:
            outputs = list(
                client.table_in_out_function(
                    function_name="secret_in_out",
                    schema_path=["main"],
                    input=iter([batch]),
                    secrets={"vgi_example": secret_value},
                )
            )

        table = pa.Table.from_batches(outputs)
        assert table.column("n").to_pylist() == [1, 2]
        assert table.column("secret_string").to_pylist() == ["s3cr3t", "s3cr3t"]


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

        with Client(FIXTURE_WORKER) as client:
            outputs = list(
                client.table_function(
                    function_name="settings_aware",
                    schema_path=["main"],
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


def _wire_secrets(*secrets: dict[str, str]) -> pa.RecordBatch:
    """Build a secrets batch the way the DuckDB extension does.

    One row, one struct column per secret, the column named by the secret's
    ``name``; ``type``, ``provider``, ``name`` and ``scope`` are fields.
    """
    columns = {s["name"]: [{"provider": "config", **s}] for s in secrets}
    return pa.RecordBatch.from_pydict(columns)


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

    def test_to_dict_is_keyed_by_secret_name(self) -> None:
        """to_dict() keeps every resolved secret under its own name, nulls dropped."""
        batch = _wire_secrets(
            {"type": "s3", "name": "prod", "scope": "s3://prod/", "key_id": "P"},
            {"type": "s3", "name": "dev", "scope": "s3://dev/", "key_id": "D"},
        )
        struct_type = pa.struct([("type", pa.string())])
        batch = batch.append_column(pa.field("gone", struct_type), pa.array([None], type=struct_type))
        result = SecretsAccessor(batch).to_dict()
        assert sorted(result) == ["dev", "prod"]
        assert result["prod"]["key_id"].as_py() == "P"

    def test_a_secret_named_like_the_old_wire_columns_is_an_ordinary_secret(self) -> None:
        """A user may name a secret ``secret_0``; it is not a special column."""
        batch = _wire_secrets({"type": "s3", "name": "secret_0", "scope": "s3://", "key_id": "K"})
        accessor = SecretsAccessor(batch, is_retry=True)
        assert accessor.to_dict()["secret_0"]["key_id"].as_py() == "K"
        found = accessor.get("s3", name="secret_0")
        assert found is not None
        assert found["key_id"].as_py() == "K"

    def test_get_by_name_on_retry_returns_the_secret(self) -> None:
        """The two-phase bind: a by-name lookup registers, then the retry returns it.

        Regression: the retry looked for ``secret_N`` columns that no client
        sends, so the lookup came back ``None`` although the secret was in the
        batch, and ``required=True`` raised.
        """
        first = SecretsAccessor(None)
        assert first.get("s3", name="prod", required=True) is None
        assert first.pending_lookups == [SecretLookupEntry(secret_type="s3", secret_name="prod")]

        batch = _wire_secrets({"type": "s3", "name": "prod", "scope": "s3://", "key_id": "P"})
        retry = SecretsAccessor(batch, is_retry=True)
        found = retry.get("s3", name="prod", required=True)
        assert found is not None
        assert found["key_id"].as_py() == "P"
        assert retry.all_resolved

    def test_get_by_name_checks_the_type(self) -> None:
        """A secret with the right name but another type is not a match."""
        batch = _wire_secrets({"type": "gcs", "name": "prod", "scope": "gcs://"})
        accessor = SecretsAccessor(batch, is_retry=True)
        assert accessor.get("s3", name="prod") is None
        with pytest.raises(ValueError, match="Required secret 's3' not found"):
            accessor.get("s3", name="prod", required=True)

    def test_get_by_name_already_resolved_needs_no_retry(self) -> None:
        """A by-name lookup the client pre-resolved is answered on the first bind."""
        batch = _wire_secrets({"type": "s3", "name": "prod", "scope": "s3://", "key_id": "P"})
        accessor = SecretsAccessor(batch)
        assert accessor.get("s3", name="prod") is not None
        assert not accessor.needs_resolution

    def test_get_by_scope_on_retry_picks_the_longest_prefix(self) -> None:
        """A scope lookup returns the secret whose scope is the longest prefix."""
        batch = _wire_secrets(
            {"type": "s3", "name": "any", "scope": "s3://", "key_id": "ANY"},
            {"type": "s3", "name": "prod", "scope": "s3://prod/\ns3n://prod/", "key_id": "PROD"},
            {"type": "gcs", "name": "other", "scope": "s3://prod/data/", "key_id": "WRONG-TYPE"},
        )
        accessor = SecretsAccessor(batch, is_retry=True)
        prod = accessor.get("s3", scope="s3://prod/data/x.parquet", required=True)
        assert prod is not None and prod["key_id"].as_py() == "PROD"
        other = accessor.get("s3", scope="s3://elsewhere/x.parquet", required=True)
        assert other is not None and other["key_id"].as_py() == "ANY"
        assert accessor.get("s3", scope="az://nope/") is None

    def test_get_by_scope_always_asks_the_client_first(self) -> None:
        """Only the client knows whether a longer-scoped secret exists."""
        batch = _wire_secrets({"type": "s3", "name": "any", "scope": "s3://", "key_id": "ANY"})
        accessor = SecretsAccessor(batch)
        assert accessor.get("s3", scope="s3://prod/x") is None
        assert accessor.pending_lookups == [SecretLookupEntry(secret_type="s3", scope="s3://prod/x")]

    def test_get_by_type_finds_a_secret_whatever_its_name(self) -> None:
        """A by-type lookup matches the ``type`` field, not the column name."""
        batch = _wire_secrets(
            {"type": "vgi_example", "name": "scoped", "scope": "x://a/", "v": "scoped"},
            {"type": "vgi_example", "name": "__default_vgi_example", "scope": "", "v": "default"},
            {"type": "s3", "name": "aaa", "scope": "", "v": "s3"},
        )
        accessor = SecretsAccessor(batch)
        found = accessor.get("vgi_example")
        assert found is not None
        assert found["v"].as_py() == "default"
        assert not accessor.needs_resolution

    def test_get_by_type_falls_back_to_a_scoped_secret(self) -> None:
        """With no unscoped secret of the type, the first one of that type is used."""
        batch = _wire_secrets({"type": "s3", "name": "prod", "scope": "s3://", "v": "p"})
        found = SecretsAccessor(batch).get("s3")
        assert found is not None
        assert found["v"].as_py() == "p"

    def test_annotated_secret_is_selected_by_name_and_scope(self) -> None:
        """A ``Secret()`` annotation on on_bind gets the secret it names, through a real bind.

        Regression: the table-function path looked for a column named by the
        secret *type*, so an annotated secret arrived as ``None`` unless the
        user had named the secret after its type.
        """
        from vgi.arguments import Arguments
        from vgi.invocation import FunctionType
        from vgi.protocol import BindRequest

        seen: dict[str, Any] = {}

        @dataclass(slots=True, frozen=True)
        class _Args:
            count: Annotated[int, Arg(0, doc="Count")] = 10

        class _TestFunc(TableFunctionGenerator[_Args]):
            @classmethod
            def on_bind(
                cls,
                params: BindParams[_Args],
                *,
                by_name: Annotated[dict[str, pa.Scalar[Any]] | None, Secret("s3", name="dev")] = None,
                by_scope: Annotated[dict[str, pa.Scalar[Any]] | None, Secret("s3", scope="s3://prod/x")] = None,
                missing: Annotated[dict[str, pa.Scalar[Any]] | None, Secret("gcs")] = None,
            ) -> BindResponse:
                seen.update(by_name=by_name, by_scope=by_scope, missing=missing)
                return BindResponse(output_schema=pa.schema([pa.field("x", pa.int64())]))

            @classmethod
            def process(cls, params: Any, state: Any, out: Any) -> None: ...

        request = BindRequest(
            function_name="_test_func",
            arguments=Arguments(positional=(pa.scalar(1),)),
            function_type=FunctionType.TABLE,
            secrets=_wire_secrets(
                {"type": "s3", "name": "dev", "scope": "s3://dev/", "key_id": "DEV"},
                {"type": "s3", "name": "prod", "scope": "s3://prod/", "key_id": "PROD"},
            ),
        )
        _TestFunc.bind(request)

        assert seen["by_name"]["key_id"].as_py() == "DEV"
        assert seen["by_scope"]["key_id"].as_py() == "PROD"
        assert seen["missing"] is None

    def test_required_raises_on_retry(self) -> None:
        """required=True raises ValueError when secret is genuinely missing on retry."""
        accessor = SecretsAccessor(None, is_retry=True)
        with pytest.raises(ValueError, match="Required secret"):
            accessor.get("missing_type", required=True)

    def test_scoped_required_raises_on_retry(self) -> None:
        """required=True with scope raises ValueError on retry when not found."""
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
        """TState that cannot carry itself through the state token raises TypeError.

        The requirement is the two codec methods, not a particular base class
        -- see ``StreamStateCodec``. A plain dataclass has neither, so it is
        still rejected, and rejected at class-definition time rather than
        silently falling back to initial_state() on every HTTP exchange.
        """

        @dataclass
        class BadState:
            x: int = 0

        with pytest.raises(TypeError, match="needs serialize_to_bytes"):

            class _BadFunc(TableFunctionGenerator[None, BadState]):
                @classmethod
                def on_bind(cls, params: Any) -> BindResponse:
                    return BindResponse(output_schema=pa.schema([("x", pa.int64())]))

                @classmethod
                def process(cls, params: Any, state: Any, out: Any) -> None: ...

    def test_state_with_its_own_codec_is_accepted(self) -> None:
        """A TState that implements the two codec methods needs no base class.

        This is the escape hatch that lets a worker choose its own encoding --
        a packed struct, a protobuf, whatever a sibling VGI implementation in
        another language already uses -- instead of being forced through Arrow
        IPC, which for a small state costs far more in framing than the state
        itself.
        """
        import struct as _struct

        from vgi.protocol import _resolve_state_type

        @dataclass
        class PackedState:
            n: int = 0

            def serialize_to_bytes(self) -> bytes:
                return _struct.pack("<q", self.n)

            @classmethod
            def deserialize_from_bytes(cls, data: bytes) -> PackedState:
                return cls(n=_struct.unpack("<q", data)[0])

        class _GoodFunc(TableFunctionGenerator[None, PackedState]):
            @classmethod
            def on_bind(cls, params: Any) -> BindResponse:
                return BindResponse(output_schema=pa.schema([("n", pa.int64())]))

            @classmethod
            def initial_state(cls, params: Any) -> PackedState:
                return PackedState()

            @classmethod
            def process(cls, params: Any, state: Any, out: Any) -> None: ...

        assert _resolve_state_type(_GoodFunc) is PackedState
        assert PackedState.deserialize_from_bytes(PackedState(n=7).serialize_to_bytes()) == PackedState(n=7)
