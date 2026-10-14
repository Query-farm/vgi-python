# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103
"""Durable cross-client dependency authority through public HTTP and sealed routing."""

from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pytest
from vgi_rpc.http._testing import _SyncTestClient

from vgi.catalog import OnConflict, SerializedSchema
from vgi.catalog.capabilities import CATALOG_DEPENDENCIES_READ, SCHEMA_LIFECYCLE, TRANSACTION_DDL
from vgi.client import Client
from vgi.client.catalog_mixin import CatalogClientError
from vgi.exceptions import CatalogCapabilityError, OpaqueDataNotRecognizedError
from vgi.meta_worker import MetaWorker
from vgi.protocol import CatalogAttachRequest, ClientCapabilities
from vgi.serve import create_app

from .dependencies_reference import DEPENDENCY_PROFILES, DependencyCatalog, DependencyWorker
from .schema_options_reference import values

KEY = b"dependency-reference-signing-key"
OFFER = ClientCapabilities(
    engine="reference",
    native_formats=[],
    catalogs=[],
    can_stream=False,
    filter_encodings=[],
    catalog=DEPENDENCY_PROFILES,
)
COLUMNS = SerializedSchema(pa.schema([pa.field("a", pa.int64())]).serialize().to_pybytes())


@pytest.fixture
def backend(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> DependencyWorker:
    monkeypatch.setattr(DependencyCatalog, "database_path", str(tmp_path / "dependencies.sqlite"), raising=False)
    worker = DependencyWorker(quiet=True)
    worker._signing_key = KEY
    return worker


def request(offer=OFFER):
    return CatalogAttachRequest(
        name="dependencies", data_version_spec=None, implementation_version=None, client_capabilities=offer
    )


def client() -> Client:
    return Client.from_http(
        "http://vgi.test", httpx_client=_SyncTestClient(create_app(DependencyWorker, describe=False, signing_key=KEY))
    )


def attach(c: Client):
    return c.catalog_attach(
        name="dependencies", data_version_spec=None, implementation_version=None, client_capabilities=OFFER
    ).attach_opaque_data


def seed(c: Client, token: bytes) -> None:
    for path in [["a.b"], ["a"], ["a", "b"], ["outside"]]:
        c.schema_create(
            attach_opaque_data=token, path=path, comment="kept", tags={"owner": "one"}, options=values(limit=25)
        )
    c.table_create(attach_opaque_data=token, schema_path=["a.b"], name="t", columns=COLUMNS)
    c.table_create(attach_opaque_data=token, schema_path=["a", "b"], name="t", columns=COLUMNS)
    c.view_create(attach_opaque_data=token, schema_path=["outside"], name="v", definition='SELECT * FROM "a.b".t')
    c.view_create(attach_opaque_data=token, schema_path=["outside"], name="v2", definition="SELECT * FROM v")


def identities(snapshot):
    return {r.key() for r in snapshot.object_refs()}


def test_resolved_graph_paths_use_the_owning_schema_spelling(backend) -> None:
    with client() as c:
        token = attach(c)
        c.schema_create(attach_opaque_data=token, path=["Root"])
        c.schema_create(attach_opaque_data=token, path=["root", "Child"])
        c.table_create(attach_opaque_data=token, schema_path=["ROOT", "CHILD"], name="T.A", columns=COLUMNS)
        c.view_create(
            attach_opaque_data=token,
            schema_path=["root", "child"],
            name="V",
            definition='SELECT * FROM "t.a"',
        )
        snapshot = c.catalog_dependencies(attach_opaque_data=token)
        assert ("schema", ("Root",), "Child", "") in identities(snapshot)
        assert ("table", ("Root", "Child"), "T.A", "") in identities(snapshot)
        assert ("view", ("Root", "Child"), "V", "") in identities(snapshot)
        assert c.schema_get(attach_opaque_data=token, path=["ROOT", "child"]).path == ["Root", "Child"]
        assert c.table_get(attach_opaque_data=token, schema_path=["root", "child"], name="t.a").schema_path == [
            "Root",
            "Child",
        ]
        with pytest.raises(CatalogClientError, match="dependent"):
            c.table_drop(attach_opaque_data=token, schema_path=["root", "CHILD"], name="t.a")
        c.table_drop(attach_opaque_data=token, schema_path=["root", "CHILD"], name="t.a", cascade=True)
        assert c.view_get(attach_opaque_data=token, schema_path=["ROOT", "child"], name="v") is None
        c.schema_drop(attach_opaque_data=token, path=["ROOT"], cascade=True)
        assert c.schema_get(attach_opaque_data=token, path=["root", "child"]) is None


def test_restrict_replace_cascade_and_literal_paths_are_atomic(backend) -> None:
    with client() as c:
        token = attach(c)
        seed(c, token)
        original = c.catalog_dependencies(attach_opaque_data=token)
        contents = c.contents(attach_opaque_data=token)
        for path in [["a.b"], ["a"]]:
            with pytest.raises(CatalogClientError, match="dependent"):
                c.schema_drop(attach_opaque_data=token, path=path)
            with pytest.raises(CatalogClientError, match="dependent"):
                c.schema_create(attach_opaque_data=token, path=path, on_conflict=OnConflict.REPLACE, comment="lost")
        assert c.catalog_dependencies(attach_opaque_data=token) == original
        assert c.contents(attach_opaque_data=token, if_none_match=contents.etag).not_modified
        c.schema_drop(attach_opaque_data=token, path=["a.b"], cascade=True)
        changed = c.catalog_dependencies(attach_opaque_data=token)
        assert changed.catalog_version == original.catalog_version + 1
        assert c.schema_get(attach_opaque_data=token, path=["a.b"]) is None
        assert c.table_get(attach_opaque_data=token, schema_path=["a", "b"], name="t") is not None
        assert c.view_get(attach_opaque_data=token, schema_path=["outside"], name="v") is None
        assert c.view_get(attach_opaque_data=token, schema_path=["outside"], name="v2") is None
        assert c.schema_get(attach_opaque_data=token, path=["outside"]) is not None
        assert not c.contents(attach_opaque_data=token, if_none_match=contents.etag).not_modified
        c.schema_drop(attach_opaque_data=token, path=["a"], cascade=True)
        assert c.schema_get(attach_opaque_data=token, path=["a", "b"]) is None
        c.schema_create(attach_opaque_data=token, path=["outside"], on_conflict=OnConflict.REPLACE)
        replaced = c.schema_get(attach_opaque_data=token, path=["outside"])
        assert replaced.comment is None and replaced.tags == {} and replaced.options.explicit_names == []


def test_transaction_visibility_rollback_commit_and_reconnect(backend) -> None:
    with client() as c:
        token = attach(c)
        seed(c, token)
        before = c.catalog_dependencies(attach_opaque_data=token)
        tx = c.catalog_transaction_begin(attach_opaque_data=token)
        c.schema_drop(attach_opaque_data=token, path=["a.b"], cascade=True, transaction_opaque_data=tx)
        pending = c.catalog_dependencies(attach_opaque_data=token, transaction_opaque_data=tx)
        assert identities(pending) < identities(before)
        assert c.catalog_dependencies(attach_opaque_data=token) == before
    with client() as c:
        assert c.catalog_dependencies(attach_opaque_data=token, transaction_opaque_data=tx) == pending
        c.catalog_transaction_rollback(attach_opaque_data=token, transaction_opaque_data=tx)
        assert c.catalog_dependencies(attach_opaque_data=token) == before
        tx = c.catalog_transaction_begin(attach_opaque_data=token)
        c.schema_drop(attach_opaque_data=token, path=["a.b"], cascade=True, transaction_opaque_data=tx)
    with client() as c:
        c.catalog_transaction_commit(attach_opaque_data=token, transaction_opaque_data=tx)
        c.catalog_transaction_commit(attach_opaque_data=token, transaction_opaque_data=tx)
        assert identities(c.catalog_dependencies(attach_opaque_data=token)) == identities(pending)


def test_discovery_does_not_reserve_or_authorize_a_later_drop(backend) -> None:
    with client() as c, client() as other:
        token, second = attach(c), attach(other)
        c.schema_create(attach_opaque_data=token, path=["empty"])
        before = c.catalog_dependencies(attach_opaque_data=token)
        other.table_create(attach_opaque_data=second, schema_path=["empty"], name="new", columns=COLUMNS)
        with pytest.raises(CatalogClientError, match="dependent"):
            c.schema_drop(attach_opaque_data=token, path=["empty"])
        assert c.catalog_dependencies(attach_opaque_data=token).catalog_version == before.catalog_version + 1
        tx = c.catalog_transaction_begin(attach_opaque_data=token)
        c.schema_drop(attach_opaque_data=token, path=["empty"], cascade=True, transaction_opaque_data=tx)
        other.table_create(attach_opaque_data=second, schema_path=["empty"], name="concurrent", columns=COLUMNS)
        with pytest.raises(CatalogClientError, match="transaction conflict"):
            c.catalog_transaction_commit(attach_opaque_data=token, transaction_opaque_data=tx)
        assert c.table_get(attach_opaque_data=token, schema_path=["empty"], name="concurrent") is not None
        c.catalog_transaction_rollback(attach_opaque_data=token, transaction_opaque_data=tx)
        readonly = c.catalog_transaction_begin(attach_opaque_data=token)
        snapshot = c.catalog_dependencies(attach_opaque_data=token, transaction_opaque_data=readonly)
        other.schema_create(attach_opaque_data=second, path=["after_snapshot"])
        assert c.catalog_dependencies(attach_opaque_data=token, transaction_opaque_data=readonly) == snapshot
        c.catalog_transaction_commit(attach_opaque_data=token, transaction_opaque_data=readonly)
        assert c.schema_get(attach_opaque_data=token, path=["after_snapshot"]) is not None


@pytest.mark.parametrize("failure", ["authorization", "after_write"])
def test_failed_cascade_preserves_all_metadata_and_versions(backend, monkeypatch, failure: str) -> None:
    with client() as c:
        token = attach(c)
        seed(c, token)
        before = c.catalog_dependencies(attach_opaque_data=token)

        def deny(self, ref=None):
            if ref is None or ref.name == "v2":
                raise ValueError("denied cascade member")

        monkeypatch.setattr(
            DependencyCatalog, "_authorize_drop" if failure == "authorization" else "_after_write", deny
        )
        with pytest.raises(CatalogClientError, match="denied"):
            c.schema_drop(attach_opaque_data=token, path=["a.b"], cascade=True)
        assert c.catalog_dependencies(attach_opaque_data=token) == before
        assert c.view_get(attach_opaque_data=token, schema_path=["outside"], name="v2") is not None


@pytest.mark.parametrize("meta", [False, True])
def test_capability_lifecycle_guarantees_ownership_and_routing(backend, monkeypatch, meta: bool) -> None:
    worker = MetaWorker([backend]) if meta else backend
    legacy = worker.catalog_attach(request(None)).attach_opaque_data
    with pytest.raises(CatalogCapabilityError, match=CATALOG_DEPENDENCIES_READ):
        worker.catalog_dependencies(attach_opaque_data=legacy)
    token = worker.catalog_attach(request()).attach_opaque_data
    assert worker.catalog_dependencies(attach_opaque_data=token).catalog_version == 1
    no_tx = replace(OFFER, catalog=replace(DEPENDENCY_PROFILES, transaction=[]))
    limited = worker.catalog_attach(request(no_tx)).attach_opaque_data
    tx = worker.catalog_transaction_begin(attach_opaque_data=limited).transaction_opaque_data
    with pytest.raises(CatalogCapabilityError, match=TRANSACTION_DDL):
        worker.catalog_schema_create(attach_opaque_data=limited, path=["new"], transaction_opaque_data=tx)
    with pytest.raises(OpaqueDataNotRecognizedError):
        worker.catalog_dependencies(attach_opaque_data=token, transaction_opaque_data=tx)
    monkeypatch.setattr(DependencyCatalog, "profiles", replace(DEPENDENCY_PROFILES, ddl=[], read=[]))
    with pytest.raises(CatalogCapabilityError, match=SCHEMA_LIFECYCLE):
        worker.catalog_schema_drop(attach_opaque_data=token, path=["absent"], ignore_not_found=True)
    with pytest.raises(CatalogCapabilityError, match=CATALOG_DEPENDENCIES_READ):
        worker.catalog_dependencies(attach_opaque_data=token)


def test_missing_and_invalid_requests_preserve_graph(backend) -> None:
    with client() as c:
        token = attach(c)
        before = c.catalog_dependencies(attach_opaque_data=token)
        c.schema_drop(attach_opaque_data=token, path=["missing", "child"], ignore_not_found=True, cascade=True)
        with pytest.raises(CatalogClientError, match="does not exist"):
            c.schema_drop(attach_opaque_data=token, path=["missing"])
        for path in [[], [""], ["bad\0name"]]:
            with pytest.raises(CatalogClientError):
                c.schema_drop(attach_opaque_data=token, path=path, ignore_not_found=True)
        for sql in [
            "SELECT 1",
            "SELECT * FROM missing",
            "SELECT * FROM main.t WHERE 1=1",
            "SELECT * FROM t; SELECT * FROM t",
        ]:
            with pytest.raises(CatalogClientError):
                c.view_create(attach_opaque_data=token, schema_path=["main"], name="bad", definition=sql)
        assert c.catalog_dependencies(attach_opaque_data=token) == before
