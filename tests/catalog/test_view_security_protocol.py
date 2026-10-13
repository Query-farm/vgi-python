# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103
"""Barrier metadata must survive RPC/persistence and never reach unaware readers."""

from dataclasses import replace
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from vgi_rpc.http._testing import _SyncTestClient

from vgi.catalog import OnConflict, SchemaObjectType, ViewInfo
from vgi.catalog.capabilities import TRANSACTION_DDL, VIEW_BARRIER_CREATE, VIEW_BARRIER_READ, CatalogCapabilities
from vgi.client import Client
from vgi.exceptions import CatalogCapabilityError
from vgi.protocol import CatalogAttachRequest, ClientCapabilities
from vgi.serve import create_app

from .view_security_reference import PROFILES, ViewSecurityCatalog, ViewSecurityWorker

KEY = b"view-security-reference-signing-key"
OFFER = ClientCapabilities(
    engine="metadata-reference", native_formats=[], catalogs=[], can_stream=False, filter_encodings=[], catalog=PROFILES
)


@pytest.fixture
def backend(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> ViewSecurityWorker:
    monkeypatch.setattr(ViewSecurityCatalog, "database_path", str(tmp_path / "views.sqlite"), raising=False)
    worker = ViewSecurityWorker(quiet=True)
    worker._signing_key = KEY
    return worker


def request(offer: ClientCapabilities | None = OFFER) -> CatalogAttachRequest:
    return CatalogAttachRequest(
        name="options", data_version_spec=None, implementation_version=None, client_capabilities=offer
    )


def client() -> Client:
    return Client.from_http(
        "http://vgi.test", httpx_client=_SyncTestClient(create_app(ViewSecurityWorker, describe=False, signing_key=KEY))
    )


def attach(c: Client) -> bytes:
    return c.catalog_attach(
        name="options", data_version_spec=None, implementation_version=None, client_capabilities=OFFER
    ).attach_opaque_data


def create(worker: ViewSecurityWorker, token: bytes, **kwargs: Any) -> None:
    worker.catalog_view_create(
        token, ["main"], "v", "SELECT 1 AS x", kwargs.pop("on_conflict", OnConflict.ERROR), aliases=["a.b"], **kwargs
    )


def test_public_rpc_transactions_reconnect_and_metadata_mutations(backend: ViewSecurityWorker) -> None:
    with client() as c:
        token = attach(c)
        target = dict(attach_opaque_data=token, schema_path=["main"], name="v")
        c.view_create(**target, definition="SELECT 1 AS x", aliases=["a.b"], security="barrier")
        first = c.view_get(**target)
        assert first.security == "barrier" and first.aliases == ["a.b"]
        tx = c.catalog_transaction_begin(attach_opaque_data=token)
        c.view_create(
            **target,
            definition="SELECT 2 AS x",
            security="regular",
            on_conflict=OnConflict.REPLACE,
            transaction_opaque_data=tx,
        )
        assert c.view_get(**target, transaction_opaque_data=tx).security == "regular"
        assert c.view_get(**target).security == "barrier"
    with client() as c:
        c.catalog_transaction_rollback(attach_opaque_data=token, transaction_opaque_data=tx)
        assert c.view_get(**target) == first
        tx = c.catalog_transaction_begin(attach_opaque_data=token)
        c.view_rename(**target, new_name="renamed", transaction_opaque_data=tx)
        target["name"] = "renamed"
        c.view_comment_set(**target, comment="kept", transaction_opaque_data=tx)
    with client() as c:
        c.catalog_transaction_commit(attach_opaque_data=token, transaction_opaque_data=tx)
        c.catalog_transaction_commit(attach_opaque_data=token, transaction_opaque_data=tx)
        info = c.view_get(**target)
        assert info.security == "barrier" and info.aliases == ["a.b"] and info.comment == "kept"
        assert c.schema_contents(attach_opaque_data=token, path=["main"], type=SchemaObjectType.VIEW)[0] == info


@pytest.mark.parametrize(
    "offer",
    [
        None,
        replace(OFFER, catalog=CatalogCapabilities()),
        replace(OFFER, catalog=CatalogCapabilities(ddl=[VIEW_BARRIER_CREATE])),
    ],
)
def test_unnegotiated_create_and_replace_have_no_effect(
    backend: ViewSecurityWorker, offer: ClientCapabilities | None
) -> None:
    full = backend.catalog_attach(request()).attach_opaque_data
    token = backend.catalog_attach(request(offer)).attach_opaque_data
    create(backend, full, security="barrier")
    original = backend.catalog_view_get(full, ["main"], "v").to_optional()
    before = backend.catalog_version(full).version
    for security in ("regular", "barrier"):
        with pytest.raises(CatalogCapabilityError):
            create(backend, token, security=security, on_conflict=OnConflict.REPLACE)
    with pytest.raises(CatalogCapabilityError):
        create(backend, token, security="barrier", on_conflict=OnConflict.IGNORE)
    assert backend.catalog_version(full).version == before
    assert backend.catalog_view_get(full, ["main"], "v").to_optional() == original


@pytest.mark.parametrize("operation", ["get", "listing", "contents", "rename", "comment", "drop"])
def test_incapable_and_withdrawn_readers_reject_including_cached_revalidation(
    backend: ViewSecurityWorker,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    full = backend.catalog_attach(request()).attach_opaque_data
    empty = backend.catalog_attach(request(None)).attach_opaque_data
    create(backend, full, security="barrier")
    snapshot = backend.catalog_contents(full)
    assert backend.catalog_contents(full, snapshot.etag).not_modified

    def read(token: bytes) -> Any:
        if operation == "get":
            return backend.catalog_view_get(token, ["main"], "v")
        if operation == "listing":
            return backend.catalog_schema_contents_views(token, ["main"])
        if operation == "contents":
            return backend.catalog_contents(token, snapshot.etag)
        if operation == "rename":
            return backend.catalog_view_rename(token, ["main"], "v", "new")
        if operation == "comment":
            return backend.catalog_view_comment_set(token, ["main"], "v", "new")
        return backend.catalog_view_drop(token, ["main"], "v")

    with pytest.raises(CatalogCapabilityError, match=VIEW_BARRIER_READ):
        read(empty)
    monkeypatch.setattr(ViewSecurityCatalog, "profiles", CatalogCapabilities())
    with pytest.raises(CatalogCapabilityError, match=VIEW_BARRIER_READ):
        read(full)


def test_barrier_change_invalidates_etag_even_with_constant_provider_token(backend: ViewSecurityWorker) -> None:
    token = backend.catalog_attach(request()).attach_opaque_data
    create(backend, token)
    regular = backend.catalog_contents(token)
    create(backend, token, security="barrier", on_conflict=OnConflict.REPLACE)
    barrier = backend.catalog_contents(token, regular.etag)
    assert not barrier.not_modified and barrier.etag != regular.etag
    assert ViewInfo.deserialize_from_bytes(barrier.schemas[0].views[0]).security == "barrier"


def test_transactional_barrier_create_requires_ddl_guarantee(backend: ViewSecurityWorker) -> None:
    offer = replace(OFFER, catalog=replace(PROFILES, transaction=[]))
    token = backend.catalog_attach(request(offer)).attach_opaque_data
    tx = backend.catalog_transaction_begin(token).transaction_opaque_data
    with pytest.raises(CatalogCapabilityError, match=TRANSACTION_DDL):
        create(backend, token, security="barrier", transaction_opaque_data=tx)
    assert backend.catalog_view_get(token, ["main"], "v", tx).to_optional() is None
    backend.catalog_transaction_rollback(token, tx)


@pytest.mark.parametrize("operation", ["rename", "comment", "drop"])
def test_transactional_barrier_mutation_requires_ddl_guarantee(backend: ViewSecurityWorker, operation: str) -> None:
    full = backend.catalog_attach(request()).attach_opaque_data
    create(backend, full, security="barrier")
    token = backend.catalog_attach(
        request(replace(OFFER, catalog=replace(PROFILES, transaction=[])))
    ).attach_opaque_data
    tx = backend.catalog_transaction_begin(token).transaction_opaque_data
    with pytest.raises(CatalogCapabilityError, match=TRANSACTION_DDL):
        if operation == "rename":
            backend.catalog_view_rename(token, ["main"], "v", "new", transaction_opaque_data=tx)
        elif operation == "comment":
            backend.catalog_view_comment_set(token, ["main"], "v", "new", transaction_opaque_data=tx)
        else:
            backend.catalog_view_drop(token, ["main"], "v", transaction_opaque_data=tx)
    assert backend.catalog_view_get(token, ["main"], "v", tx).to_optional().security == "barrier"
    backend.catalog_transaction_rollback(token, tx)


@pytest.mark.parametrize("security", [None, "", "secure", "BARRIER", "barrier\0", 1])
def test_unknown_modes_reject_before_provider(backend: ViewSecurityWorker, security: Any) -> None:
    token = backend.catalog_attach(request()).attach_opaque_data
    before = backend.catalog_version(token).version
    with pytest.raises(ValueError, match="security"):
        create(backend, token, security=security)
    assert backend.catalog_version(token).version == before


def test_missing_wire_security_never_defaults_to_regular() -> None:
    info = ViewInfo(name="v", schema_path=["main"], definition="SELECT 1", comment=None, tags={}, security="barrier")
    batch = pa.ipc.open_stream(info.serialize_to_bytes()).read_next_batch()
    index = batch.schema.get_field_index("security")
    invalid = [
        batch.remove_column(index),
        batch.set_column(index, batch.schema.field(index), pa.array([None], type=pa.string())),
    ]
    for wire in invalid:
        with pytest.raises(ValueError, match="security"):
            ViewInfo.deserialize_from_batch(wire)


def test_create_profile_requires_read_and_unknown_profile_versions_are_not_equivalent() -> None:
    assert PROFILES.select(CatalogCapabilities(ddl=[VIEW_BARRIER_CREATE])) == CatalogCapabilities()
    assert (
        PROFILES.select(CatalogCapabilities(read=["vgi.view.barrier.read.v2"], ddl=[VIEW_BARRIER_CREATE]))
        == CatalogCapabilities()
    )
