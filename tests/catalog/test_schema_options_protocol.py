# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103

"""Real RPC, durable transactions and capability isolation for schema options."""

from dataclasses import replace
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from vgi_rpc.http._testing import _SyncTestClient

from vgi import attach_capabilities
from vgi.catalog.capabilities import SCHEMA_OPTIONS_ALTER, SCHEMA_OPTIONS_READ, TRANSACTION_DDL, CatalogCapabilities
from vgi.catalog.options import OptionChanges, OptionDefinitions
from vgi.client import Client
from vgi.exceptions import CatalogCapabilityError, OpaqueDataNotRecognizedError
from vgi.meta_worker import MetaWorker
from vgi.protocol import CatalogAttachRequest, ClientCapabilities
from vgi.serve import create_app
from vgi.typed_values import TypedValueBatch

from .schema_options_reference import DEFINITIONS, PROFILES, SchemaOptionsCatalog, SchemaOptionsWorker, values

KEY = b"catalog-options-reference-signing-key"
OFFER = ClientCapabilities(
    engine="reference-client", native_formats=[], catalogs=[], can_stream=False, filter_encodings=[], catalog=PROFILES
)


@pytest.fixture
def backend(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> SchemaOptionsWorker:
    monkeypatch.setattr(SchemaOptionsCatalog, "database_path", str(tmp_path / "catalog.sqlite"), raising=False)
    worker = SchemaOptionsWorker(quiet=True)
    worker._signing_key = KEY
    return worker


def _request(offer: ClientCapabilities | None = OFFER) -> CatalogAttachRequest:
    return CatalogAttachRequest(
        name="options", data_version_spec=None, implementation_version=None, client_capabilities=offer
    )


def _client() -> Client:
    return Client.from_http(
        "http://vgi.test",
        httpx_client=_SyncTestClient(create_app(SchemaOptionsWorker, describe=False, signing_key=KEY)),
    )


def _attach(client: Client, offer: ClientCapabilities | None = OFFER) -> Any:
    return client.catalog_attach(
        name="options", data_version_spec=None, implementation_version=None, client_capabilities=offer
    )


def test_public_http_lifecycle_and_durable_transaction_reconnect(backend: SchemaOptionsWorker) -> None:
    with _client() as client:
        attach = _attach(client)
        token = attach.attach_opaque_data
        assert attach.selected_capabilities == PROFILES
        assert attach.catalog_capabilities == PROFILES
        definitions = client.schema_option_definitions(attach_opaque_data=token, path=["a.b"])
        assert definitions.schema.equals(DEFINITIONS.schema, check_metadata=True)
        client.schema_create(attach_opaque_data=token, path=["a.b"], options=values(label=None))
        created = client.schema_get(attach_opaque_data=token, path=["a.b"])
        assert created is not None and created.options is not None
        assert created.options.explicit_names == ["label"]
        assert created.options.values.batch.column("label")[0].as_py() is None
        tx = client.catalog_transaction_begin(attach_opaque_data=token)
        assert tx is not None
        client.schema_alter_options(
            attach_opaque_data=token,
            path=["a.b"],
            transaction_opaque_data=tx,
            changes=OptionChanges(set_values=values(limit=25), reset_names=["label"]),
        )
        pending = client.schema_get(attach_opaque_data=token, path=["a.b"], transaction_opaque_data=tx)
        assert pending.options.explicit_names == ["limit"]
        assert pending.options.values.batch.column("label")[0].as_py() == "default"
        assert client.schema_get(attach_opaque_data=token, path=["a.b"]).options.explicit_names == ["label"]
    with _client() as client:
        client.catalog_transaction_rollback(attach_opaque_data=token, transaction_opaque_data=tx)
        client.catalog_transaction_rollback(attach_opaque_data=token, transaction_opaque_data=tx)
        assert client.schema_get(attach_opaque_data=token, path=["a.b"]).options.explicit_names == ["label"]
        tx = client.catalog_transaction_begin(attach_opaque_data=token)
        client.schema_alter_options(
            attach_opaque_data=token,
            path=["a.b"],
            transaction_opaque_data=tx,
            changes=OptionChanges(set_values=values(limit=25), reset_names=["label"]),
        )
    with _client() as client:
        client.catalog_transaction_commit(attach_opaque_data=token, transaction_opaque_data=tx)
        client.catalog_transaction_commit(attach_opaque_data=token, transaction_opaque_data=tx)
        committed = client.schema_get(attach_opaque_data=token, path=["a.b"])
        assert committed.options.explicit_names == ["limit"]
        assert committed.options.values.batch.column("limit")[0].as_py() == 25
        assert committed.options.values.schema.equals(DEFINITIONS.schema, check_metadata=True)


@pytest.mark.parametrize("offer", [None, replace(OFFER, catalog=None), replace(OFFER, catalog=CatalogCapabilities())])
def test_missing_offer_rejects_new_operations_before_provider(
    backend: SchemaOptionsWorker, offer: ClientCapabilities | None, monkeypatch: pytest.MonkeyPatch
) -> None:
    attach = backend.catalog_attach(_request(offer))
    assert attach.selected_capabilities == CatalogCapabilities()
    calls = []
    monkeypatch.setattr(backend._get_catalog(), "schema_alter_options", lambda **kw: calls.append(kw))
    with pytest.raises(CatalogCapabilityError, match="catalog_schema_alter_options"):
        backend.catalog_schema_alter_options(
            attach.attach_opaque_data, ["main"], OptionChanges(set_values=values(limit=2)).serialize_to_bytes()
        )
    with pytest.raises(CatalogCapabilityError, match="catalog_schema_create"):
        backend.catalog_schema_create(attach.attach_opaque_data, ["new"], options=values(limit=2).serialize_to_bytes())
    assert not calls
    backend.catalog_schema_create(attach.attach_opaque_data, ["legacy"])
    assert all(info.options is None for info in backend.catalog_schemas(attach.attach_opaque_data).to_infos())


def test_selection_is_intersection_with_dependencies_and_unknown_versions() -> None:
    unknown = CatalogCapabilities(read=[SCHEMA_OPTIONS_READ, "vgi.schema.options.read.v2"], ddl=[SCHEMA_OPTIONS_ALTER])
    assert PROFILES.select(unknown) == CatalogCapabilities(read=[SCHEMA_OPTIONS_READ], ddl=[SCHEMA_OPTIONS_ALTER])
    assert PROFILES.select(CatalogCapabilities(ddl=[SCHEMA_OPTIONS_ALTER])) == CatalogCapabilities()
    assert PROFILES.select(None) == CatalogCapabilities()
    with pytest.raises(ValueError, match="selected"):
        unknown.validate_selected()


def test_current_provider_downgrade_rejects_before_effects(
    backend: SchemaOptionsWorker, monkeypatch: pytest.MonkeyPatch
) -> None:
    attach = backend.catalog_attach(_request())
    before = backend.catalog_schema_get(attach.attach_opaque_data, ["main"]).to_optional().options.serialize_to_bytes()
    monkeypatch.setattr(SchemaOptionsCatalog, "profiles", CatalogCapabilities(read=[SCHEMA_OPTIONS_READ]))
    with pytest.raises(CatalogCapabilityError):
        backend.catalog_schema_alter_options(
            attach.attach_opaque_data, ["main"], OptionChanges(set_values=values(limit=20)).serialize_to_bytes()
        )
    assert (
        backend.catalog_schema_get(attach.attach_opaque_data, ["main"]).to_optional().options.serialize_to_bytes()
        == before
    )


def test_transaction_requires_ddl_guarantee(backend: SchemaOptionsWorker) -> None:
    attach = backend.catalog_attach(_request(replace(OFFER, catalog=replace(PROFILES, transaction=[]))))
    tx = backend.catalog_transaction_begin(attach.attach_opaque_data).transaction_opaque_data
    assert tx is not None
    with pytest.raises(CatalogCapabilityError, match=TRANSACTION_DDL):
        backend.catalog_schema_alter_options(
            attach.attach_opaque_data, ["main"], OptionChanges().serialize_to_bytes(), tx
        )
    backend.catalog_transaction_rollback(attach.attach_opaque_data, tx)


def test_metadata_and_etags_are_scoped_to_agreement_even_with_provider_cache(backend: SchemaOptionsWorker) -> None:
    full = backend.catalog_attach(_request())
    legacy = backend.catalog_attach(_request(None))
    full_contents = backend.catalog_contents(full.attach_opaque_data)
    legacy_contents = backend.catalog_contents(legacy.attach_opaque_data, if_none_match=full_contents.etag)
    assert not legacy_contents.not_modified
    assert full_contents.etag != legacy_contents.etag
    from vgi.catalog import SchemaInfo
    from vgi.catalog.contents_codec import decode_collection

    assert decode_collection(SchemaInfo, full_contents.schemas)[0].options is not None
    assert decode_collection(SchemaInfo, legacy_contents.schemas)[0].options is None
    assert backend.catalog_contents(full.attach_opaque_data, if_none_match=full_contents.etag).not_modified
    assert backend.catalog_contents(legacy.attach_opaque_data, if_none_match=legacy_contents.etag).not_modified


@pytest.mark.parametrize("meta", [False, True])
def test_sealed_agreement_survives_routing_and_restart(backend: SchemaOptionsWorker, meta: bool) -> None:
    worker = MetaWorker([backend]) if meta else backend
    attach = worker.catalog_attach(_request())
    fresh = SchemaOptionsWorker(quiet=True)
    fresh._signing_key = KEY
    restarted = MetaWorker([fresh]) if meta else fresh
    assert fresh._catalog_agreement(attach.attach_opaque_data) == PROFILES
    assert len(fresh._unwrap_attach_full(attach.attach_opaque_data)) == 32
    assert len(fresh._unwrap_attach(attach.attach_opaque_data)) == 16
    restarted.catalog_schema_alter_options(
        attach_opaque_data=attach.attach_opaque_data,
        path=["main"],
        changes=OptionChanges(set_values=values(limit=33)).serialize_to_bytes(),
    )
    assert (
        restarted.catalog_schema_get(attach_opaque_data=attach.attach_opaque_data, path=["main"])
        .to_optional()
        .options.values.batch.column("limit")[0]
        .as_py()
        == 33
    )


def test_tampered_or_malformed_agreement_rejects(backend: SchemaOptionsWorker) -> None:
    attach = backend.catalog_attach(_request()).attach_opaque_data
    tampered = attach[:-1] + bytes([attach[-1] ^ 1])
    malformed = backend._seal_attach(b"VGI\x00CAP\x00\x00\x00\x00\xffshort")
    for token in (tampered, malformed):
        with pytest.raises(OpaqueDataNotRecognizedError):
            backend.catalog_schema_alter_options(token, ["main"], OptionChanges().serialize_to_bytes())
    legacy = backend._seal_attach(b"x" * 32)
    assert backend._catalog_agreement(legacy) == CatalogCapabilities()
    assert attach_capabilities.split(b"x" * 32) == (None, b"x" * 32)


def test_invalid_mixed_change_has_no_effect_or_version_bump(backend: SchemaOptionsWorker) -> None:
    token = backend.catalog_attach(_request()).attach_opaque_data
    before = backend.catalog_version(token).version
    with pytest.raises(ValueError, match="not mutable"):
        backend.catalog_schema_alter_options(
            token, ["main"], OptionChanges(set_values=values(limit=99), reset_names=["locked"]).serialize_to_bytes()
        )
    after = backend.catalog_schema_get(token, ["main"]).to_optional()
    assert after.options.values.batch.column("limit")[0].as_py() == 10
    assert backend.catalog_version(token).version == before


def test_conflicting_commits_do_not_overwrite_other_attachment(backend: SchemaOptionsWorker) -> None:
    one = backend.catalog_attach(_request()).attach_opaque_data
    two = backend.catalog_attach(_request()).attach_opaque_data
    tx = backend.catalog_transaction_begin(one).transaction_opaque_data
    backend.catalog_schema_alter_options(
        one, ["main"], OptionChanges(set_values=values(limit=20)).serialize_to_bytes(), tx
    )
    backend.catalog_schema_alter_options(two, ["main"], OptionChanges(set_values=values(limit=30)).serialize_to_bytes())
    with pytest.raises(ValueError, match="conflict"):
        backend.catalog_transaction_commit(one, tx)
    assert backend.catalog_schema_get(two, ["main"]).to_optional().options.values.batch.column("limit")[0].as_py() == 30
    backend.catalog_transaction_rollback(one, tx)


def test_required_nullable_option_and_reset_without_default() -> None:
    schema = pa.schema([pa.field("required", pa.string()), pa.field("defaulted", pa.int32(), nullable=False)])
    defs = OptionDefinitions(
        schema=schema,
        defaults=TypedValueBatch(
            pa.record_batch([pa.array([1], type=pa.int32())], schema=pa.schema([schema.field("defaulted")]))
        ),
        create_names=["required"],
        alter_names=["required", "defaulted"],
    )
    with pytest.raises(ValueError, match="required options"):
        defs.create()
    required = TypedValueBatch(
        pa.record_batch([pa.array([None], type=pa.string())], schema=pa.schema([schema.field("required")]))
    )
    state = defs.create(required)
    assert state.explicit_names == ["required"]
    assert state.values.batch.column("required")[0].as_py() is None
    with pytest.raises(ValueError, match="without defaults"):
        defs.apply(state, OptionChanges(reset_names=["required"]))
    restored = OptionDefinitions.deserialize_from_bytes(defs.serialize_to_bytes())
    assert restored.schema.equals(defs.schema, check_metadata=True)
    assert restored.defaults.batch.equals(defs.defaults.batch, check_metadata=True)


def test_agreement_cache_remains_bound_to_authenticated_principal(
    backend: SchemaOptionsWorker, monkeypatch: pytest.MonkeyPatch
) -> None:
    from vgi_rpc.rpc import AuthContext

    alice = AuthContext(domain="reference", authenticated=True, principal="alice")
    bob = AuthContext(domain="reference", authenticated=True, principal="bob")
    monkeypatch.setattr("vgi.worker.current_auth", lambda: alice)
    token = backend.catalog_attach(_request()).attach_opaque_data
    assert backend._catalog_agreement(token) == PROFILES
    monkeypatch.setattr("vgi.worker.current_auth", lambda: bob)
    with pytest.raises(OpaqueDataNotRecognizedError):
        backend.catalog_schema_alter_options(token, ["main"], OptionChanges().serialize_to_bytes())
    monkeypatch.setattr("vgi.worker.current_auth", lambda: alice)
    assert backend._catalog_agreement(token) == PROFILES


def test_transaction_cannot_move_to_another_negotiated_attachment(backend: SchemaOptionsWorker) -> None:
    one = backend.catalog_attach(_request()).attach_opaque_data
    two = backend.catalog_attach(_request()).attach_opaque_data
    transaction = backend.catalog_transaction_begin(one).transaction_opaque_data
    assert transaction is not None
    with pytest.raises(OpaqueDataNotRecognizedError):
        backend.catalog_schema_alter_options(
            two, ["main"], OptionChanges(set_values=values(limit=45)).serialize_to_bytes(), transaction
        )
    assert backend.catalog_schema_get(two, ["main"]).to_optional().options.values.batch.column("limit")[0].as_py() == 10
    backend.catalog_transaction_rollback(one, transaction)
