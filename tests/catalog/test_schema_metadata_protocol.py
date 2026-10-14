# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103, F811
"""Schema creation metadata shares durable snapshots with typed options."""

from dataclasses import replace

import pyarrow as pa
import pytest
from vgi_rpc.utils import serialize_record_batch_bytes

from vgi.catalog import CatalogMetadataChanges, OnConflict
from vgi.catalog.capabilities import SCHEMA_METADATA_ALTER, SCHEMA_OPTIONS_ALTER, SCHEMA_OPTIONS_CREATE, TRANSACTION_DDL
from vgi.catalog.options import OptionChanges
from vgi.exceptions import CatalogCapabilityError, OpaqueDataNotRecognizedError
from vgi.meta_worker import MetaWorker

from .schema_options_reference import PROFILES, SchemaOptionsCatalog, values
from .test_schema_options_protocol import OFFER, _attach, _client, _request, backend  # noqa: F401

pytestmark = pytest.mark.usefixtures("backend")


def test_metadata_options_and_conflicts_survive_transaction_reconnect() -> None:
    with _client() as client:
        token = _attach(client).attach_opaque_data
        target = dict(attach_opaque_data=token, path=["a.b"])
        client.schema_create(**target, comment="", tags={"owner": "one"}, options=values(limit=25))
        original = client.schema_get(**target)
        version = client.catalog_version(attach_opaque_data=token)
        client.schema_create(**target, on_conflict=OnConflict.IGNORE, comment="ignored", options=values(limit=3))
        assert client.catalog_version(attach_opaque_data=token) == version
        ignored = client.schema_get(**target)
        assert ignored.comment == "" and ignored.tags == {"owner": "one"}
        assert ignored.options.values.batch.equals(original.options.values.batch)
        tx = client.catalog_transaction_begin(attach_opaque_data=token)
        client.schema_create(**target, on_conflict=OnConflict.REPLACE, transaction_opaque_data=tx)
        pending = client.schema_get(**target, transaction_opaque_data=tx)
        assert pending.comment is None and pending.tags == {} and pending.options.explicit_names == []
        assert client.schema_get(**target).comment == ""
    with _client() as client:
        client.catalog_transaction_rollback(attach_opaque_data=token, transaction_opaque_data=tx)
        restored = client.schema_get(**target)
        assert restored.comment == "" and restored.tags == {"owner": "one"}
        assert restored.options.values.batch.equals(original.options.values.batch)
        assert client.catalog_version(attach_opaque_data=token) == version
        tx = client.catalog_transaction_begin(attach_opaque_data=token)
        client.schema_alter_options(
            **target, transaction_opaque_data=tx, changes=OptionChanges(set_values=values(limit=30))
        )
    with _client() as client:
        client.catalog_transaction_commit(attach_opaque_data=token, transaction_opaque_data=tx)
        updated = client.schema_get(**target)
        assert updated.comment == "" and updated.tags == {"owner": "one"}
        assert updated.options.values.batch.column("limit")[0].as_py() == 30
        tx = client.catalog_transaction_begin(attach_opaque_data=token)
        client.schema_create(
            **target,
            on_conflict=OnConflict.REPLACE,
            comment="committed",
            tags={"owner": "two"},
            transaction_opaque_data=tx,
        )
    with _client() as client:
        client.catalog_transaction_commit(attach_opaque_data=token, transaction_opaque_data=tx)
        replaced = client.schema_get(**target)
        assert replaced.comment == "committed" and replaced.tags == {"owner": "two"}
        assert replaced.options.explicit_names == []


def test_atomic_patch_noops_versions_and_snapshot_invalidation() -> None:
    with _client() as client:
        token = _attach(client).attach_opaque_data
        target = dict(attach_opaque_data=token, path=["a.b"])
        client.schema_create(
            **target, comment="before", tags={"keep": "yes", "old": "remove"}, options=values(limit=25)
        )
        original = client.schema_get(**target)
        snapshot = client.contents(attach_opaque_data=token)
        client.schema_alter_metadata(
            **target,
            changes=CatalogMetadataChanges(set_comment=True, comment="", set_tags={"new": ""}, remove_tags=["old"]),
        )
        changed = client.schema_get(**target)
        assert changed.comment == "" and changed.tags == {"keep": "yes", "new": ""}
        assert changed.options.values.batch.equals(original.options.values.batch, check_metadata=True)
        version = client.catalog_version(attach_opaque_data=token)
        assert version > snapshot.catalog_version
        refreshed = client.contents(attach_opaque_data=token, if_none_match=snapshot.etag)
        assert not refreshed.not_modified and refreshed.etag != snapshot.etag
        assert next(s.schema for s in refreshed.schemas if s.schema.path == ["a.b"]).tags == changed.tags
        for patch in [
            CatalogMetadataChanges(),
            CatalogMetadataChanges(remove_tags=["absent"]),
            CatalogMetadataChanges(set_comment=True, comment="", set_tags={"new": ""}),
        ]:
            client.schema_alter_metadata(**target, changes=patch)
            assert client.catalog_version(attach_opaque_data=token) == version
            assert client.contents(attach_opaque_data=token, if_none_match=refreshed.etag).not_modified
        # Nested and literal dotted schemas must remain separate mutation targets.
        client.schema_create(attach_opaque_data=token, path=["a"])
        client.schema_create(attach_opaque_data=token, path=["a", "b"], comment="nested")
        client.schema_alter_metadata(**target, changes=CatalogMetadataChanges(set_comment=True))
        assert client.schema_get(**target).comment is None
        assert client.schema_get(attach_opaque_data=token, path=["a", "b"]).comment == "nested"


def test_patch_transaction_isolation_rollback_commit_and_reconnect() -> None:
    with _client() as client:
        token = _attach(client).attach_opaque_data
        target = dict(attach_opaque_data=token, path=["main"])
        snapshot = client.contents(attach_opaque_data=token)
        tx = client.catalog_transaction_begin(attach_opaque_data=token)
        client.schema_alter_metadata(
            **target,
            transaction_opaque_data=tx,
            changes=CatalogMetadataChanges(set_comment=True, comment="pending", set_tags={"a": "one"}),
        )
        pending = client.schema_get(**target, transaction_opaque_data=tx)
        assert pending.comment == "pending" and pending.tags == {"a": "one"}
        assert client.schema_get(**target).comment is None
        assert client.contents(attach_opaque_data=token, if_none_match=snapshot.etag).not_modified
    with _client() as client:
        client.catalog_transaction_rollback(attach_opaque_data=token, transaction_opaque_data=tx)
        assert client.schema_get(**target).comment is None
        assert client.contents(attach_opaque_data=token, if_none_match=snapshot.etag).not_modified
        tx = client.catalog_transaction_begin(attach_opaque_data=token)
        client.schema_alter_metadata(
            **target,
            transaction_opaque_data=tx,
            changes=CatalogMetadataChanges(set_comment=True, comment="committed", set_tags={"a": "two"}),
        )
    with _client() as client:
        client.catalog_transaction_commit(attach_opaque_data=token, transaction_opaque_data=tx)
        committed = client.schema_get(**target)
        assert committed.comment == "committed" and committed.tags == {"a": "two"}
        assert not client.contents(attach_opaque_data=token, if_none_match=snapshot.etag).not_modified


@pytest.mark.parametrize("meta", [False, True])
def test_patch_requires_current_negotiation_and_routes_sealed_handles(backend, monkeypatch, meta: bool) -> None:
    worker = MetaWorker([backend]) if meta else backend
    legacy = worker.catalog_attach(_request(None)).attach_opaque_data
    patch = CatalogMetadataChanges(set_tags={"a": "new"}).serialize_to_bytes()
    with pytest.raises(CatalogCapabilityError, match=SCHEMA_METADATA_ALTER):
        worker.catalog_schema_alter_metadata(attach_opaque_data=legacy, path=["main"], changes=patch)
    token = worker.catalog_attach(_request()).attach_opaque_data
    worker.catalog_schema_alter_metadata(attach_opaque_data=token, path=["main"], changes=patch)
    assert worker.catalog_schema_get(attach_opaque_data=token, path=["main"]).to_optional().tags == {"a": "new"}
    before = worker.catalog_version(attach_opaque_data=token).version
    monkeypatch.setattr(
        SchemaOptionsCatalog, "profiles", replace(PROFILES, ddl=[SCHEMA_OPTIONS_ALTER, SCHEMA_OPTIONS_CREATE])
    )
    with pytest.raises(CatalogCapabilityError, match=SCHEMA_METADATA_ALTER):
        worker.catalog_schema_alter_metadata(
            attach_opaque_data=token,
            path=["main"],
            changes=CatalogMetadataChanges(set_comment=True, comment="bad").serialize_to_bytes(),
        )
    assert worker.catalog_version(attach_opaque_data=token).version == before
    assert worker.catalog_schema_get(attach_opaque_data=token, path=["main"]).to_optional().comment is None


def test_patch_rejects_without_transaction_ddl_and_foreign_transaction(backend) -> None:
    token = backend.catalog_attach(
        _request(replace(OFFER, catalog=replace(PROFILES, transaction=[])))
    ).attach_opaque_data
    tx = backend.catalog_transaction_begin(token).transaction_opaque_data
    patch = CatalogMetadataChanges(set_tags={"a": "new"}).serialize_to_bytes()
    with pytest.raises(CatalogCapabilityError, match=TRANSACTION_DDL):
        backend.catalog_schema_alter_metadata(token, ["main"], patch, transaction_opaque_data=tx)
    full = backend.catalog_attach(_request()).attach_opaque_data
    with pytest.raises(OpaqueDataNotRecognizedError):
        backend.catalog_schema_alter_metadata(full, ["main"], patch, transaction_opaque_data=tx)
    backend.catalog_transaction_rollback(token, tx)


def test_invalid_patch_rejects_before_provider_even_with_missing_target_ignore(backend, monkeypatch) -> None:
    token = backend.catalog_attach(_request()).attach_opaque_data
    calls = []
    monkeypatch.setattr(backend._get_catalog(), "schema_alter_metadata", lambda **kwargs: calls.append(kwargs))
    row = {"set_comment": True, "comment": "bad", "set_tags": [("a", "new")], "remove_tags": ["a"]}
    batch = pa.RecordBatch.from_pylist([row], schema=CatalogMetadataChanges.ARROW_SCHEMA)
    with pytest.raises(ValueError, match="SET and REMOVE"):
        backend.catalog_schema_alter_metadata(
            token, ["absent"], serialize_record_batch_bytes(batch), ignore_not_found=True
        )
    assert not calls


def test_missing_target_and_malformed_path_preserve_versions(backend) -> None:
    token = backend.catalog_attach(_request()).attach_opaque_data
    patch = CatalogMetadataChanges().serialize_to_bytes()
    version = backend.catalog_version(token).version
    backend.catalog_schema_alter_metadata(token, ["absent"], patch, ignore_not_found=True)
    with pytest.raises(ValueError, match="does not exist"):
        backend.catalog_schema_alter_metadata(token, ["absent"], patch)
    for path in ([], [""], ["a\0b"]):
        with pytest.raises(ValueError):
            backend.catalog_schema_alter_metadata(token, path, patch, ignore_not_found=True)
    assert backend.catalog_version(token).version == version
