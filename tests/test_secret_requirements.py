"""Secret requirement declarations and by-type secret lookup.

Two ways a function used to end up without its credentials:

* ``Meta.required_secrets = ["llm"]`` — a plain type string was silently
  dropped, so the client was never asked to resolve the secret.
* looking a secret up by TYPE as if the type were the batch's column name — the
  client keys each column by the secret's unique NAME (``__default_<type>`` for
  an unnamed secret), so only a secret named after its type matched.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Any

import pyarrow as pa
import pytest

from vgi.arguments import Arg, Secret, SecretLookupEntry
from vgi.invocation import BindResponse
from vgi.metadata import _coerce_required_secrets, resolve_metadata
from vgi.table_function import BindParams, ResolvedSecrets, SecretsAccessor, TableFunctionGenerator


@dataclass(slots=True, frozen=True)
class _Args:
    count: Annotated[int, Arg(0, doc="Count")] = 1


def _table_function(required_secrets: Any) -> type[TableFunctionGenerator[_Args]]:
    class _Func(TableFunctionGenerator[_Args]):
        class Meta:
            pass

        @classmethod
        def on_bind(cls, params: BindParams[_Args]) -> BindResponse:
            return BindResponse(output_schema=pa.schema([pa.field("x", pa.int64())]))

        @classmethod
        def process(cls, params: Any, state: Any, out: Any) -> None: ...

    _Func.Meta.required_secrets = required_secrets  # type: ignore[attr-defined]
    return _Func


class TestCoerceRequiredSecrets:
    """Every accepted declaration form becomes a lookup; anything else is an error."""

    def test_bare_type_string(self) -> None:
        """A bare type string becomes a type-only lookup."""
        assert _coerce_required_secrets("F", ["llm"]) == [SecretLookupEntry(secret_type="llm")]

    def test_all_forms_in_order(self) -> None:
        """Every supported form is converted, preserving declaration order."""
        entries = _coerce_required_secrets(
            "F",
            [
                "a",
                SecretLookupEntry(secret_type="b", scope="s3://x/"),
                Secret("c", name="n"),
                {"secret_type": "d", "scope": "gs://y/"},
            ],
        )
        assert entries == [
            SecretLookupEntry(secret_type="a"),
            SecretLookupEntry(secret_type="b", scope="s3://x/"),
            SecretLookupEntry(secret_type="c", secret_name="n"),
            SecretLookupEntry(secret_type="d", scope="gs://y/"),
        ]

    def test_tuple_and_none(self) -> None:
        """A tuple is accepted and None means no requirements."""
        assert _coerce_required_secrets("F", ("a",)) == [SecretLookupEntry(secret_type="a")]
        assert _coerce_required_secrets("F", None) == []

    @pytest.mark.parametrize("raw", ["llm", SecretLookupEntry(secret_type="llm"), {"secret_type": "llm"}, 7])
    def test_non_list_container_rejected(self, raw: Any) -> None:
        """A non-list value is rejected instead of iterated."""
        # A bare string would otherwise iterate as one requirement per character.
        with pytest.raises(TypeError, match="must be a list"):
            _coerce_required_secrets("F", raw)

    @pytest.mark.parametrize("entry", [7, ("llm",), b"llm", object()])
    def test_unknown_entry_rejected(self, entry: Any) -> None:
        """An entry of an unsupported type raises instead of being dropped."""
        with pytest.raises(TypeError, match=r"required_secrets\[0\]"):
            _coerce_required_secrets("F", [entry])

    def test_dict_without_type_rejected(self) -> None:
        """A dict entry must name its secret type."""
        with pytest.raises(TypeError, match="secret_type"):
            _coerce_required_secrets("F", [{"scope": "s3://x/"}])

    @pytest.mark.parametrize("entry", ["", SecretLookupEntry(secret_type=""), {"secret_type": ""}])
    def test_empty_type_rejected(self, entry: Any) -> None:
        """An empty secret type is rejected."""
        # The client skips an entry with an empty type, so it would never resolve.
        with pytest.raises(ValueError, match="empty secret_type"):
            _coerce_required_secrets("F", [entry])


class TestRequiredSecretsMetadata:
    """``Meta.required_secrets`` reaches the function metadata the client reads."""

    def test_string_entry_is_requested(self) -> None:
        """A bare string in Meta.required_secrets reaches the metadata."""
        resolve_metadata.cache_clear()
        meta = _table_function(["llm"]).get_metadata()
        assert meta.required_secrets == [SecretLookupEntry(secret_type="llm")]

    def test_unrecognised_entry_fails_loudly(self) -> None:
        """An unreadable entry fails metadata resolution."""
        resolve_metadata.cache_clear()
        with pytest.raises(TypeError, match="required_secrets"):
            _table_function([42]).get_metadata()

    def test_conformance_fixture_declares_bare_string(self) -> None:
        """The conformance fixtures declare their requirements as the contract says."""
        resolve_metadata.cache_clear()
        from vgi._test_fixtures.secret_lookup import SecretTypesSeenFunction, SecretValueByTypeFunction

        assert SecretValueByTypeFunction.get_metadata().required_secrets == [
            SecretLookupEntry(secret_type="vgi_example")
        ]
        assert SecretTypesSeenFunction.get_metadata().required_secrets == [
            SecretLookupEntry(secret_type="vgi_example"),
            SecretLookupEntry(secret_type="s3", scope="s3://vgi-conformance/"),
        ]

    def test_same_type_annotations_with_different_scopes_are_both_requested(self) -> None:
        """Same-type annotations with different scopes are all requested."""
        resolve_metadata.cache_clear()

        class _Func(TableFunctionGenerator[_Args]):
            class Meta:
                required_secrets = ["s3"]

            @classmethod
            def on_bind(
                cls,
                params: BindParams[_Args],
                *,
                a: Annotated[dict[str, Any] | None, Secret("s3", scope="s3://a/")] = None,
                b: Annotated[dict[str, Any] | None, Secret("s3", scope="s3://b/")] = None,
                again: Annotated[dict[str, Any] | None, Secret("s3")] = None,
            ) -> BindResponse:
                return BindResponse(output_schema=pa.schema([pa.field("x", pa.int64())]))

            @classmethod
            def process(cls, params: Any, state: Any, out: Any) -> None: ...

        lookups = {(e.secret_type, e.secret_name, e.scope) for e in _Func.get_metadata().required_secrets}
        assert lookups == {("s3", None, None), ("s3", None, "s3://a/"), ("s3", None, "s3://b/")}
        # The unscoped annotation duplicates the Meta entry and is not repeated.
        assert len(_Func.get_metadata().required_secrets) == 3


def _secrets_batch(columns: dict[str, dict[str, str]]) -> pa.RecordBatch:
    """A secrets batch as the DuckDB extension builds it: one struct column per secret NAME."""
    arrays = [pa.array([fields], type=pa.struct([(k, pa.string()) for k in fields])) for fields in columns.values()]
    return pa.RecordBatch.from_arrays(arrays, names=list(columns))


class TestLookupByType:
    """A by-type lookup matches each secret's ``type`` field, not its column name."""

    def test_unnamed_default_secret_found_by_type(self) -> None:
        """An unnamed secret (column __default_<type>) is found by type."""
        batch = _secrets_batch(
            {"__default_brave": {"type": "brave", "name": "__default_brave", "scope": "", "api_key": "k"}}
        )
        accessor = SecretsAccessor(batch)
        secret = accessor.get("brave")
        assert secret is not None
        assert secret["api_key"].as_py() == "k"
        assert accessor.all_resolved  # no needless two-phase request
        assert accessor.to_dict().lookup("brave") is secret

    def test_secret_named_unlike_its_type_found_by_type(self) -> None:
        """A secret named unlike its type is found by type."""
        batch = _secrets_batch({"prod_creds": {"type": "brave", "name": "prod_creds", "scope": "", "api_key": "p"}})
        secret = SecretsAccessor(batch).get("brave", required=True)
        assert secret is not None
        assert secret["api_key"].as_py() == "p"

    def test_by_type_prefers_unscoped(self) -> None:
        """A by-type lookup prefers the unscoped secret of that type."""
        secrets = ResolvedSecrets(
            {
                "a_scoped": {"type": "s3", "scope": "s3://a/", "k": "scoped"},
                "z_unscoped": {"type": "s3", "scope": "", "k": "unscoped"},
            }
        )
        found = secrets.lookup("s3")
        assert found is not None
        assert found["k"] == "unscoped"

    def test_name_lookup_checks_type(self) -> None:
        """A by-name lookup also checks the secret's type."""
        secrets = ResolvedSecrets({"creds": {"type": "gcs", "scope": "", "k": "g"}})
        assert secrets.lookup("s3", name="creds") is None
        found = secrets.lookup("gcs", name="creds")
        assert found is not None
        assert found["k"] == "g"

    def test_missing_type_is_none_then_required_error_on_retry(self) -> None:
        """A missing secret is requested, then reported as missing on the retry."""
        batch = _secrets_batch({"other": {"type": "gcs", "name": "other", "scope": "", "k": "g"}})
        first = SecretsAccessor(batch)
        assert first.get("brave") is None
        assert first.pending_lookups == [SecretLookupEntry(secret_type="brave")]
        retry = SecretsAccessor(batch, is_retry=True)
        with pytest.raises(ValueError, match="Required secret 'brave' not found"):
            retry.get("brave", required=True)
