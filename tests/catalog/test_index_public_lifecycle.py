# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103

"""Public HTTP lifecycle proving real SQLite indexes and atomic catalog changes."""

import sqlite3
from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pytest
from vgi_rpc.http._testing import _SyncTestClient

from vgi.catalog import OnConflict, SchemaObjectType
from vgi.catalog.indexes import IndexConstraintType, IndexDefinition, IndexKey
from vgi.client import Client
from vgi.client.catalog_mixin import CatalogClientError
from vgi.protocol import ClientCapabilities
from vgi.serve import create_app
from vgi.typed_values import TypedValueBatch

from .index_protocol_reference import ALGORITHM, PROFILES, TABLE, IndexProvider, IndexWorker

OFFER = ClientCapabilities(
    engine="reference", native_formats=[], catalogs=[], can_stream=False, filter_encodings=[], catalog=PROFILES
)


@pytest.fixture
def backend(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(IndexProvider, "database_path", str(tmp_path / "indexes.sqlite"), raising=False)
    return IndexProvider()


def client():
    return Client.from_http(
        "http://vgi.test",
        httpx_client=_SyncTestClient(
            create_app(IndexWorker, describe=False, signing_key=b"index-reference-signing-key")
        ),
    )


def attach(c):
    return c.catalog_attach(
        name="indexes", data_version_spec=None, implementation_version=None, client_capabilities=OFFER
    ).attach_opaque_data


def definition(*columns, unique=False):
    return IndexDefinition(
        table=TABLE,
        algorithm=ALGORITHM,
        keys=[IndexKey(column_path=[column]).serialize_to_bytes() for column in columns or ("id",)],
        options=TypedValueBatch.empty(),
        constraint_type=IndexConstraintType.UNIQUE if unique else IndexConstraintType.NONE,
    )


def create(c, token, name="idx", value=None, tx=None, conflict=OnConflict.ERROR):
    c.index_create(
        attach_opaque_data=token,
        schema_path=["main"],
        name=name,
        definition=definition() if value is None else value,
        transaction_opaque_data=tx,
        on_conflict=conflict,
    )


def get(c, token, name="idx", tx=None):
    return c.index_get(attach_opaque_data=token, schema_path=["main"], name=name, transaction_opaque_data=tx)


def drop(c, token, name="idx", tx=None, **kwargs):
    c.index_drop(attach_opaque_data=token, schema_path=["main"], name=name, transaction_opaque_data=tx, **kwargs)


def test_real_index_discovery_options_reconnect_and_drop(backend):
    with client() as c:
        token = attach(c)
        options = c.index_option_definitions(attach_opaque_data=token, table=TABLE, algorithm=ALGORITHM)
        assert options.schema.names == [] and options.create_names == []
        create(c, token, value=definition("id", "label"))
        original = get(c, token)
        assert original.definition == definition("id", "label")
        assert original.options.explicit_names == []
        assert c.contents(attach_opaque_data=token).schemas[0].indexes == [original]
        assert list(c.schema_contents(attach_opaque_data=token, path=["main"], type=SchemaObjectType.INDEX)) == [
            original
        ]
    # Fresh worker instance resolves durable metadata and verifies physical index.
    with client() as c:
        assert get(c, token) == original
        drop(c, token)
        assert get(c, token) is None
        drop(c, token, ignore_not_found=True)


def test_unique_is_enforced_on_real_rows_without_inventing_logical_primary(backend):
    provider_token = backend.authority.attach()
    with client() as c:
        token = attach(c)
        create(c, token, value=definition(unique=True))
        assert get(c, token).definition.associated_constraint is None
        table = c.table_get(attach_opaque_data=token, schema_path=["main"], name="items")
        assert table.constraints == []
        before = backend.authority.rows(provider_token)
        with pytest.raises(sqlite3.IntegrityError):
            backend.authority.replace_rows(provider_token, [{"id": 1, "label": "a"}, {"id": 1, "label": "b"}])
        assert backend.authority.rows(provider_token) == before
        drop(c, token)
        backend.authority.replace_rows(provider_token, [{"id": 1, "label": "a"}, {"id": 1, "label": "b"}])
        with pytest.raises(CatalogClientError, match="UNIQUE"):
            create(c, token, value=definition(unique=True))
        assert get(c, token) is None


def test_replacement_failure_preserves_original_physical_index_and_metadata(backend):
    provider_token = backend.authority.attach()
    backend.authority.replace_rows(provider_token, [{"id": 1, "label": "a"}, {"id": 1, "label": "b"}])
    with client() as c:
        token = attach(c)
        create(c, token)
        original = get(c, token)
        with pytest.raises(CatalogClientError, match="UNIQUE"):
            create(c, token, value=definition(unique=True), conflict=OnConflict.REPLACE)
        assert get(c, token) == original
        create(c, token, value=definition("label"), conflict=OnConflict.REPLACE)
        assert get(c, token).definition == definition("label")


@pytest.mark.parametrize(
    "value",
    [
        replace(definition(), algorithm="ART"),
        replace(definition(), keys=[IndexKey(column_path=["id", "child"]).serialize_to_bytes()]),
        replace(definition(), options=TypedValueBatch(pa.record_batch([pa.array([1])], names=["limit"]))),
    ],
)
def test_unsupported_replacement_rejects_before_effects(backend, value):
    with client() as c:
        token = attach(c)
        create(c, token)
        original = get(c, token)
        with pytest.raises(CatalogClientError):
            create(c, token, value=value, conflict=OnConflict.REPLACE)
        assert get(c, token) == original
        with pytest.raises(CatalogClientError, match="CASCADE"):
            drop(c, token, cascade=True)
        assert get(c, token) == original


def test_transaction_local_physical_indexes_rollback_commit_and_attachment_scope(backend):
    with client() as c:
        token, other = attach(c), attach(c)
        tx = c.catalog_transaction_begin(attach_opaque_data=token)
        create(c, token, tx=tx)
        assert get(c, token, tx=tx) is not None and get(c, token) is None
        with pytest.raises(CatalogClientError, match="transaction"):
            get(c, other, tx=tx)
        c.catalog_transaction_rollback(attach_opaque_data=token, transaction_opaque_data=tx)
        assert get(c, token) is None
        tx = c.catalog_transaction_begin(attach_opaque_data=token)
        create(c, token, tx=tx)
    with client() as c:
        assert get(c, token, tx=tx) is not None
        c.catalog_transaction_commit(attach_opaque_data=token, transaction_opaque_data=tx)
        assert get(c, token) is not None
        with pytest.raises(CatalogClientError, match="transaction"):
            get(c, token, tx=tx)
        tx = c.catalog_transaction_begin(attach_opaque_data=token)
        drop(c, token, tx=tx)
        assert get(c, token, tx=tx) is None and get(c, token) is not None
        c.catalog_transaction_rollback(attach_opaque_data=token, transaction_opaque_data=tx)
        assert get(c, token) is not None


def test_stale_transaction_cannot_overwrite_new_committed_index(backend):
    with client() as c:
        token = attach(c)
        tx = c.catalog_transaction_begin(attach_opaque_data=token)
        create(c, token, "pending", tx=tx)
        create(c, token, "committed")
        with pytest.raises(CatalogClientError, match="revision"):
            c.catalog_transaction_commit(attach_opaque_data=token, transaction_opaque_data=tx)
        assert get(c, token, "committed") is not None and get(c, token, "pending") is None
        c.catalog_transaction_rollback(attach_opaque_data=token, transaction_opaque_data=tx)


def test_schema_population_tracks_committed_and_transaction_local_indexes(backend):
    with client() as c:
        token = attach(c)

        def count(tx=None):
            schemas = c.schemas(attach_opaque_data=token, transaction_opaque_data=tx)
            direct = c.schema_get(attach_opaque_data=token, transaction_opaque_data=tx, path=["main"])
            assert schemas == [direct]
            assert direct.estimated_object_count["table"] == 1
            assert direct.estimated_object_count["table_function"] == 1
            return direct.estimated_object_count["index"]

        assert count() == 0
        create(c, token)
        assert count() == 1
        assert c.contents(attach_opaque_data=token).schemas[0].schema.estimated_object_count["index"] == 1
        tx = c.catalog_transaction_begin(attach_opaque_data=token)
        create(c, token, "pending", tx=tx)
        assert count(tx) == 2 and count() == 1
        drop(c, token, tx=tx)
        assert count(tx) == 1 and count() == 1
        c.catalog_transaction_rollback(attach_opaque_data=token, transaction_opaque_data=tx)
        assert count() == 1
        drop(c, token)
        assert count() == 0
