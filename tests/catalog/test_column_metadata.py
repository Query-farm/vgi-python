# Copyright 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103

"""Column comments/tags survive durable RPC edits and reject invalid metadata."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from typing import Any, cast

import pyarrow as pa
import pytest

from vgi.catalog import (
    ColumnMetadata,
    OnConflict,
    SchemaInfo,
    Table,
    TableInfo,
    column_metadata,
    validate_column_metadata,
    with_column_metadata,
)
from vgi.catalog.catalog_interface import AttachOpaqueData, CatalogContentsResult, SchemaContentsInfo, SerializedSchema
from vgi.protocol import TableCreateRequest, TablesResponse

from .column_paths_reference import ColumnPathsWorker
from .test_column_paths_protocol import args, attach, client, info, rows, seed
from .test_column_paths_protocol import backend as backend
from .typing_helpers import present


@pytest.mark.parametrize(
    "value",
    [
        ColumnMetadata(),
        ColumnMetadata(comment="", tags={}),
        ColumnMetadata(comment="snow 雪\x00", tags={"": "", "nul\x00": "雪"}),
    ],
)
def test_helper_distinguishes_absence_and_empty_metadata(value: ColumnMetadata) -> None:
    field = pa.field("a.b", pa.int32(), metadata={b"provider.opaque": b"\x00\xff"})
    updated = with_column_metadata(field, value)
    assert column_metadata(updated) == value
    assert updated.metadata is not None
    assert updated.metadata[b"provider.opaque"] == b"\x00\xff"
    assert (b"comment" in updated.metadata) == (value.comment is not None)
    assert (b"tags" in updated.metadata) == (value.tags is not None)
    assert with_column_metadata(updated, ColumnMetadata()).equals(field, check_metadata=True)


def test_comment_edit_preserves_existing_tag_encoding_and_unrelated_metadata() -> None:
    field = pa.field("x", pa.int32(), metadata={b"tags": b' { "a" : "b" } ', b"other": b"kept"})
    updated = with_column_metadata(field, replace(column_metadata(field), comment=""))
    assert updated.metadata == {b"comment": b"", b"tags": b' { "a" : "b" } ', b"other": b"kept"}


@pytest.mark.parametrize("key", [b"comment", b"tags", b"opaque"])
def test_existing_duplicate_arrow_key_restriction_is_preserved(key: bytes) -> None:
    # PyArrow supports duplicate metadata entries; its stubs only admit mappings.
    metadata_factory = cast(Callable[[list[tuple[bytes, bytes]]], pa.KeyValueMetadata], pa.KeyValueMetadata)
    field_factory = cast("Callable[[str, pa.DataType, bool, pa.KeyValueMetadata], pa.Field[Any]]", pa.field)
    field = field_factory("x", pa.int32(), True, metadata_factory([(key, b"{}"), (key, b"{}")]))
    with pytest.raises(ValueError, match="unique"):
        column_metadata(field)
    with pytest.raises(ValueError, match="unique"):
        with_column_metadata(field, ColumnMetadata(comment="replacement"))


def test_mutated_python_tag_maps_are_revalidated_before_editing() -> None:
    value = ColumnMetadata(tags={"a": "valid"})
    assert value.tags is not None
    # Deliberately corrupt the validated mapping to exercise publication checks.
    value.tags["a"] = cast(str, 1)
    with pytest.raises(TypeError, match="string"):
        with_column_metadata(pa.field("x", pa.int32()), value)


def test_recursive_validator_reaches_extension_storage() -> None:
    class Wrapped(pa.ExtensionType):
        """An opaque wrapper with an invalid metadata-bearing child."""

        def __init__(self) -> None:
            super().__init__(pa.struct([pa.field("x", pa.int32(), metadata={b"tags": b'{"a":1}'})]), "test.column_meta")

        def __arrow_ext_serialize__(self) -> bytes:
            return b""

        @classmethod
        def __arrow_ext_deserialize__(cls, storage_type: pa.DataType, serialized: bytes) -> Wrapped:
            return cls()

    with pytest.raises(ValueError, match="object of strings"):
        validate_column_metadata(pa.schema([pa.field("wrapped", Wrapped())]))


def test_declarative_empty_comment_is_present_and_tags_are_preserved() -> None:
    field = pa.field("x", pa.int32(), metadata={b"tags": b"{}", b"other": b"kept"})
    table = Table(name="t", columns=pa.schema([field]), column_comments={"x": ""})
    result = pa.ipc.read_schema(pa.py_buffer(table.to_table_info(["main"]).columns)).field("x")
    assert result.metadata == {b"comment": b"", b"tags": b"{}", b"other": b"kept"}


def _columns(*, invalid: bool = False) -> SerializedSchema:
    return SerializedSchema(
        pa.schema([pa.field("x", pa.int32(), metadata={b"tags": b'{"a":1}'} if invalid else None)])
        .serialize()
        .to_pybytes()
    )


def test_record_publication_revalidates_replaced_column_bytes() -> None:
    records = [
        TableInfo(name="t", schema_path=["main"], columns=_columns(), constraints=[], comment=None, tags={}),
        TableCreateRequest(
            attach_opaque_data=b"attach",
            schema_path=["main"],
            name="t",
            columns=_columns(),
            constraint_definitions=[],
            on_conflict=OnConflict.ERROR,
        ),
    ]
    for record in records:
        record.serialize_to_bytes()
        object.__setattr__(record, "columns", _columns(invalid=True))
        with pytest.raises(ValueError, match="object of strings"):
            record.serialize_to_bytes()


@pytest.mark.parametrize("operation", ["create", "add"])
def test_bad_metadata_rejects_before_provider_mutation(
    backend: ColumnPathsWorker, monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    with client() as c:
        token = attach(c, None)
        c.table_create(**args(token), columns=_columns())
    called = []
    monkeypatch.setattr(
        backend._get_catalog(),
        "table_" + operation if operation == "create" else "table_column_add",
        lambda **kwargs: called.append(kwargs),
    )
    with pytest.raises(ValueError, match="object of strings"):
        if operation == "create":
            request = TableCreateRequest(
                **args(token), columns=_columns(), constraint_definitions=[], on_conflict=OnConflict.ERROR
            )
            object.__setattr__(request, "columns", _columns(invalid=True))
            backend.catalog_table_create(request)
        else:
            backend.catalog_table_column_add(**args(token), column_path=["x"], column_definition=_columns(invalid=True))
    assert called == []


@pytest.mark.parametrize("operation", ["get", "list", "contents"])
def test_discovery_validates_even_previously_serialized_provider_records(
    backend: ColumnPathsWorker, monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    with client() as c:
        token = attach(c, None)
    entry = TableInfo(name="t", schema_path=["main"], columns=_columns(), constraints=[], comment=None, tags={})
    TablesResponse.from_infos([entry])
    object.__setattr__(entry, "columns", _columns(invalid=True))
    catalog = backend._get_catalog()
    monkeypatch.setattr(catalog, "table_get", lambda **kwargs: entry)
    monkeypatch.setattr(catalog, "schema_contents", lambda **kwargs: [entry])
    monkeypatch.setattr(
        catalog,
        "catalog_contents",
        lambda **kwargs: CatalogContentsResult(
            schemas=[
                SchemaContentsInfo(
                    schema=SchemaInfo(attach_opaque_data=AttachOpaqueData(b""), path=["main"], comment=None, tags={}),
                    tables=[entry],
                )
            ]
        ),
    )
    with pytest.raises(ValueError, match="object of strings"):
        if operation == "get":
            backend.catalog_table_get(token, ["main"], "t")
        elif operation == "list":
            backend.catalog_schema_contents_tables(token, ["main"])
        else:
            backend.catalog_contents(token)


def test_signed_http_create_add_metadata_rollback_and_reconnect(backend: ColumnPathsWorker) -> None:
    base = pa.schema(
        [
            with_column_metadata(
                pa.field("x", pa.int32(), metadata={b"origin": b"kept"}), ColumnMetadata(comment="", tags={})
            ),
            pa.field(
                "nested",
                pa.struct(
                    [
                        with_column_metadata(
                            pa.field("a.b", pa.string()),
                            ColumnMetadata(comment="snow 雪", tags={"nul\x00": "value\x00"}),
                        )
                    ]
                ),
            ),
        ],
        metadata={b"schema": b"kept"},
    )
    added = with_column_metadata(
        pa.field("added", pa.string(), metadata={b"origin": b"new"}),
        ColumnMetadata(comment="\x00", tags={"team": "雪"}),
    )
    with client() as c:
        token = attach(c, None)
        c.table_create(**args(token), columns=SerializedSchema(base.serialize().to_pybytes()))
        seed(backend, token, [{"x": 1, "nested": {"a.b": "data"}}])
        assert info(c, token).equals(base, check_metadata=True)
        tx = c.catalog_transaction_begin(attach_opaque_data=AttachOpaqueData(token))
        c.table_column_add(
            **args(token, tx),
            column_path=["added"],
            column_definition=SerializedSchema(pa.schema([added]).serialize().to_pybytes()),
        )
        assert info(c, token, tx).field("added").equals(added, check_metadata=True)
        assert "added" not in info(c, token).names
    with client() as c:
        c.catalog_transaction_rollback(attach_opaque_data=AttachOpaqueData(token), transaction_opaque_data=present(tx))
        assert info(c, token).equals(base, check_metadata=True)
        tx = c.catalog_transaction_begin(attach_opaque_data=AttachOpaqueData(token))
        c.table_column_add(
            **args(token, tx),
            column_path=["added"],
            column_definition=SerializedSchema(pa.schema([added]).serialize().to_pybytes()),
        )
        c.catalog_transaction_commit(attach_opaque_data=AttachOpaqueData(token), transaction_opaque_data=present(tx))
    with client() as c:
        assert info(c, token).equals(base.append(added), check_metadata=True)
        assert rows(backend, token) == [{"x": 1, "nested": {"a.b": "data"}, "added": None}]
        provider = backend._get_catalog()
        with pytest.raises(ValueError, match="object of strings"):
            provider.table_column_add(
                **args(backend._unwrap_attach(token)),
                column_path=["bad"],
                column_definition=pa.schema([pa.field("bad", pa.int32(), metadata={b"tags": b'{"a":1}'})])
                .serialize()
                .to_pybytes(),
            )
        assert info(c, token).equals(base.append(added), check_metadata=True)
