# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103
"""Durable constraint identities and atomic revisions over the public HTTP RPC."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pytest
from vgi_rpc.http._testing import _SyncTestClient

from vgi.catalog import TableInfo
from vgi.catalog.catalog_interface import AttachOpaqueData, SerializedSchema, TransactionOpaqueData
from vgi.catalog.constraints import (
    ConstraintChange,
    ConstraintDefinition,
    ConstraintKind,
    ConstraintOperation,
    ConstraintsApplyRequest,
    ConstraintSnapshot,
)
from vgi.client import Client
from vgi.client.catalog_mixin import CatalogClientError
from vgi.protocol import ClientCapabilities
from vgi.serve import create_app

from .constraints_protocol_reference import PROFILES, ConstraintProvider, ConstraintWorker
from .constraints_reference import ConstraintCatalog
from .test_constraints_reference import add, check, definition, foreign_key, table
from .typing_helpers import present

KEY = b"constraint-reference-signing-key"
OFFER = ClientCapabilities(
    engine="reference", native_formats=[], catalogs=[], can_stream=False, filter_encodings=[], catalog=PROFILES
)
COLUMNS = pa.schema(cast("list[pa.Field[Any]]", [pa.field("id", pa.int64()), pa.field("label", pa.string())]))


@pytest.fixture
def backend(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> ConstraintProvider:
    monkeypatch.setattr(ConstraintProvider, "database_path", str(tmp_path / "constraints.sqlite"), raising=False)
    return ConstraintProvider()


def client() -> Client:
    return Client.from_http(
        "http://vgi.test", httpx_client=_SyncTestClient(create_app(ConstraintWorker, describe=False, signing_key=KEY))
    )


def attach(c: Client) -> AttachOpaqueData:
    return c.catalog_attach(
        name="constraints", data_version_spec=None, implementation_version=None, client_capabilities=OFFER
    ).attach_opaque_data


def create(
    c: Client,
    token: AttachOpaqueData,
    name: str = "items",
    definitions: Sequence[ConstraintDefinition] = (),
    columns: pa.Schema = COLUMNS,
    tx: TransactionOpaqueData | None = None,
) -> None:
    c.table_create(
        attach_opaque_data=token,
        schema_path=["main"],
        name=name,
        columns=SerializedSchema(columns.serialize().to_pybytes()),
        constraint_definitions=[item.serialize_to_bytes() for item in definitions],
        transaction_opaque_data=tx,
    )


def get(c: Client, token: AttachOpaqueData, name: str = "items", tx: TransactionOpaqueData | None = None) -> TableInfo:
    return present(c.table_get(attach_opaque_data=token, schema_path=["main"], name=name, transaction_opaque_data=tx))


def apply(
    c: Client,
    token: AttachOpaqueData,
    before: TableInfo,
    *changes: ConstraintChange,
    tx: TransactionOpaqueData | None = None,
) -> ConstraintSnapshot:
    return c.table_constraints_apply(
        ConstraintsApplyRequest(
            attach_opaque_data=token,
            table=table(before.name),
            expected_revision=present(before.definition_revision),
            changes=[change.serialize_to_bytes() for change in changes],
            transaction_opaque_data=tx,
        )
    )


def test_create_discovery_mutation_restart_and_stale_revision(backend: ConstraintProvider) -> None:
    with client() as c:
        token = attach(c)
        create(c, token, definitions=[definition(ConstraintKind.UNIQUE, "id", name="first"), check("id > 0")])
        original = get(c, token)
        infos = original.constraint_infos()
        assert len(infos) == 2 and all(info.trusted for info in infos)
        contents = c.contents(attach_opaque_data=token)
        assert contents.schemas[0].tables[0] == original
        changed = apply(
            c,
            token,
            original,
            ConstraintChange(operation=ConstraintOperation.RENAME, ref=infos[0].ref, declared_name="second"),
            add(definition(ConstraintKind.NOT_NULL, "label")),
        )
        assert changed.definition_revision != original.definition_revision
        assert changed.constraint_infos()[0].ref == infos[0].ref
        current = get(c, token)
        assert current.constraints == changed.constraints
        assert not pa.ipc.read_schema(pa.py_buffer(current.columns)).field("label").nullable
        with pytest.raises(CatalogClientError, match="revision"):
            apply(
                c,
                token,
                original,
                ConstraintChange(operation=ConstraintOperation.DROP, ref=infos[0].ref, cascade=False),
            )
        assert get(c, token) == current
    with client() as c:
        assert get(c, token) == current
        replacement = replace(changed.constraint_infos()[0].definition, column_paths=[["label"]])
        newer = apply(
            c,
            token,
            current,
            ConstraintChange(
                operation=ConstraintOperation.REPLACE,
                ref=infos[0].ref,
                definition=replacement,
                enforcement=infos[0].enforcement,
                validation=infos[0].validation,
            ),
        )
        assert newer.constraint_infos()[0].ref == infos[0].ref
        assert newer.definition_revision != current.definition_revision


def test_transaction_visibility_rollback_commit_and_attachment_scope(backend: ConstraintProvider) -> None:
    with client() as c, client() as other:
        token, another = attach(c), attach(other)
        create(c, token)
        original = get(c, token)
        tx = c.catalog_transaction_begin(attach_opaque_data=token)
        pending = apply(c, token, original, add(definition(ConstraintKind.NOT_NULL, "id")), tx=tx)
        assert get(c, token) == original
        assert get(c, token, tx=tx).definition_revision == pending.definition_revision
        with pytest.raises(CatalogClientError):
            apply(other, another, original, add(definition(ConstraintKind.NOT_NULL, "id")), tx=tx)
    with client() as c:
        c.catalog_transaction_rollback(attach_opaque_data=token, transaction_opaque_data=present(tx))
        assert get(c, token) == original
        tx = c.catalog_transaction_begin(attach_opaque_data=token)
        pending = apply(c, token, original, add(definition(ConstraintKind.UNIQUE, "id")), tx=tx)
    with client() as c:
        c.catalog_transaction_commit(attach_opaque_data=token, transaction_opaque_data=present(tx))
        assert get(c, token).definition_revision == pending.definition_revision


def test_foreign_key_identity_restrict_and_cascade_are_atomic(backend: ConstraintProvider) -> None:
    with client() as c:
        token = attach(c)
        create(c, token, "parent", [definition(ConstraintKind.UNIQUE, "id")])
        create(c, token, "child", [foreign_key(table("parent"), ["id"], ["id"])])
        parent, child = get(c, token, "parent"), get(c, token, "child")
        key = parent.constraint_infos()[0].ref
        assert child.constraint_infos()[0].referenced_constraint == key
        with pytest.raises(CatalogClientError, match="depend|referenc"):
            apply(c, token, parent, ConstraintChange(operation=ConstraintOperation.DROP, ref=key, cascade=False))
        assert get(c, token, "parent") == parent and get(c, token, "child") == child
        apply(c, token, parent, ConstraintChange(operation=ConstraintOperation.DROP, ref=key, cascade=True))
        assert get(c, token, "parent").constraints == []
        assert get(c, token, "child").constraints == []
        assert get(c, token, "child").definition_revision != child.definition_revision


def test_batch_failure_and_postwrite_failure_preserve_revision(
    backend: ConstraintProvider, monkeypatch: pytest.MonkeyPatch
) -> None:
    with client() as c:
        token = attach(c)
        create(c, token)
        original = get(c, token)
        with pytest.raises(CatalogClientError):
            apply(
                c,
                token,
                original,
                add(definition(ConstraintKind.UNIQUE, "id")),
                add(definition(ConstraintKind.UNIQUE, "missing")),
            )
        assert get(c, token) == original

        def fail(self: ConstraintCatalog) -> None:
            raise ValueError("injected postwrite failure")

        monkeypatch.setattr(type(backend.authority), "_after_write", fail)
        with pytest.raises(CatalogClientError, match="injected"):
            apply(c, token, original, add(definition(ConstraintKind.UNIQUE, "id")))
        assert get(c, token) == original
