# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103

"""Mandatory column expressions preserve existing transaction promises."""

from dataclasses import replace

import pyarrow as pa
import pytest

from vgi.catalog import OnConflict, TableInfo
from vgi.catalog.capabilities import CATALOG_DEPENDENCIES_READ, ROW_SCALAR_EXPRESSION, TRANSACTION_DDL
from vgi.catalog.sql import ColumnExpressions, with_column_expressions
from vgi.exceptions import CatalogCapabilityError
from vgi.protocol import TableCreateRequest

from .dependencies_reference import DEPENDENCY_PROFILES, DependencyCatalog
from .test_dependencies_protocol import OFFER, request
from .test_dependencies_protocol import backend as backend
from .test_dependency_alter_lifecycle import expression, generated, schema


def create_request(token, columns, tx=None):
    return TableCreateRequest(
        attach_opaque_data=token,
        schema_path=["main"],
        name="items",
        columns=columns,
        on_conflict=OnConflict.ERROR,
        constraint_definitions=[],
        transaction_opaque_data=tx,
    )


def limited_expression_token(backend, monkeypatch, mode):
    profiles = replace(DEPENDENCY_PROFILES, read=[p for p in DEPENDENCY_PROFILES.read if p != ROW_SCALAR_EXPRESSION])
    token = backend.catalog_attach(request(replace(OFFER, catalog=profiles) if mode == "unselected" else OFFER))
    if mode == "withdrawn":
        monkeypatch.setattr(DependencyCatalog, "profiles", profiles)
    return token.attach_opaque_data


@pytest.mark.parametrize("mode", ["unselected", "withdrawn"])
@pytest.mark.parametrize("operation", ["create", "add"])
@pytest.mark.parametrize("nested", [False, True])
def test_canonical_generated_representation_needs_no_expression_read_opt_in(
    backend, monkeypatch, mode, operation, nested
):
    token = limited_expression_token(backend, monkeypatch, mode)
    field = generated("computed", "id + 1")
    if nested:
        field = pa.field("nested", pa.struct([field]))
    calls = []
    monkeypatch.setattr(backend._get_catalog(), "table_create", lambda **kwargs: calls.append(kwargs))
    monkeypatch.setattr(backend._get_catalog(), "table_column_add", lambda **kwargs: calls.append(kwargs))
    before = backend.catalog_dependencies(token)
    if operation == "create":
        backend.catalog_table_create(create_request(token, schema(pa.field("id", pa.int64()), field)))
    else:
        backend.catalog_table_column_add(token, ["main"], "items", [field.name], schema(field))
    assert len(calls) == 1 and backend.catalog_dependencies(token) == before


@pytest.mark.parametrize("mode", ["unselected", "withdrawn"])
def test_all_table_discovery_paths_preserve_generated_without_read_opt_in(backend, monkeypatch, mode):
    full = backend.catalog_attach(request()).attach_opaque_data
    backend.catalog_table_create(
        create_request(full, schema(pa.field("id", pa.int64()), generated("computed", "id + 1")))
    )
    token = limited_expression_token(backend, monkeypatch, mode)
    before = backend.catalog_dependencies(token)
    for read in [
        lambda: backend.catalog_table_get(token, ["main"], "items"),
        lambda: backend.catalog_schema_contents_tables(token, ["main"]),
        lambda: backend.catalog_contents(token),
    ]:
        read()
    assert backend.catalog_dependencies(token) == before


def test_default_and_nested_expression_readback_is_mandatory_without_profile_negotiation(backend, monkeypatch):
    field = with_column_expressions(pa.field("child", pa.int64()), ColumnExpressions(default=expression("42")))
    columns = schema(pa.field("parent", pa.struct([field])))
    info = TableInfo(name="items", schema_path=["main"], columns=columns, constraints=[], comment=None, tags={})
    monkeypatch.setattr(backend._get_catalog(), "table_get", lambda **kwargs: info)
    full = backend.catalog_attach(request()).attach_opaque_data
    result = backend.catalog_table_get(full, ["main"], "items")
    assert TableInfo.deserialize_from_bytes(result.items[0]).columns == columns
    limited = limited_expression_token(backend, monkeypatch, "unselected")
    result = backend.catalog_table_get(limited, ["main"], "items")
    assert TableInfo.deserialize_from_bytes(result.items[0]).columns == columns


@pytest.mark.parametrize("mode", ["unselected", "omit_both", "withdrawn"])
def test_dependency_mutations_require_selected_current_transaction_ddl_before_effects(backend, monkeypatch, mode):
    profiles = replace(
        DEPENDENCY_PROFILES,
        transaction=[],
        read=[p for p in DEPENDENCY_PROFILES.read if mode != "omit_both" or p != CATALOG_DEPENDENCIES_READ],
    )
    offer = OFFER if mode == "withdrawn" else replace(OFFER, catalog=profiles)
    token = backend.catalog_attach(request(offer)).attach_opaque_data
    tx = backend.catalog_transaction_begin(token).transaction_opaque_data
    if mode == "withdrawn":
        monkeypatch.setattr(DependencyCatalog, "profiles", profiles)
    args = dict(attach_opaque_data=token, schema_path=["main"], name="items", transaction_opaque_data=tx)
    column = dict(args, column_path=["id"])
    methods = {
        "schema_create": lambda: backend.catalog_schema_create(token, ["new"], transaction_opaque_data=tx),
        "schema_drop": lambda: backend.catalog_schema_drop(token, ["new"], transaction_opaque_data=tx),
        "table_create": lambda: backend.catalog_table_create(
            create_request(token, schema(pa.field("id", pa.int64())), tx)
        ),
        "table_drop": lambda: backend.catalog_table_drop(**args),
        "table_rename": lambda: backend.catalog_table_rename(**args, new_name="renamed"),
        "table_comment_set": lambda: backend.catalog_table_comment_set(**args, comment="comment"),
        "table_column_comment_set": lambda: backend.catalog_table_column_comment_set(**column, comment="comment"),
        "table_column_add": lambda: backend.catalog_table_column_add(
            **column, column_definition=schema(pa.field("id", pa.int64()))
        ),
        "table_column_drop": lambda: backend.catalog_table_column_drop(**column, cascade=True),
        "table_column_rename": lambda: backend.catalog_table_column_rename(**column, new_column_name="renamed"),
        "table_column_type_change": lambda: backend.catalog_table_column_type_change(
            **column, column_definition=schema(pa.field("id", pa.int32()))
        ),
        "table_column_default_set": lambda: backend.catalog_table_column_default_set(
            **column, expression=expression("42").serialize_to_bytes()
        ),
        "table_column_default_drop": lambda: backend.catalog_table_column_default_drop(**column),
        "view_create": lambda: backend.catalog_view_create(
            **args, definition="SELECT * FROM source", on_conflict=OnConflict.ERROR
        ),
        "view_drop": lambda: backend.catalog_view_drop(**args),
        "view_rename": lambda: backend.catalog_view_rename(**args, new_name="renamed"),
    }
    calls = []
    for name in methods:
        monkeypatch.setattr(backend._get_catalog(), name, lambda **kwargs: calls.append(kwargs))
    version = backend.catalog_version(token).version
    for mutation in methods.values():
        with pytest.raises(CatalogCapabilityError, match=TRANSACTION_DDL):
            mutation()
    assert calls == [] and backend.catalog_version(token).version == version
    backend.catalog_transaction_rollback(token, tx)


def test_legacy_expression_metadata_rejects_but_plain_non_dependency_transactions_remain(backend, monkeypatch):
    profiles = replace(DEPENDENCY_PROFILES, read=[], transaction=[])
    monkeypatch.setattr(DependencyCatalog, "profiles", profiles)
    token = backend.catalog_attach(request(None)).attach_opaque_data
    tx = backend.catalog_transaction_begin(token).transaction_opaque_data
    calls = []
    monkeypatch.setattr(backend._get_catalog(), "table_create", lambda **kwargs: calls.append(kwargs))
    legacy = schema(pa.field("id", pa.int64(), metadata={b"default": b"42"}))
    with pytest.raises(ValueError, match="canonical SqlExpression"):
        backend.catalog_table_create(create_request(token, legacy, tx))
    assert calls == []
    plain = schema(pa.field("id", pa.int64()))
    backend.catalog_table_create(create_request(token, plain, tx))
    assert len(calls) == 1 and calls[0]["columns"] == plain
    backend.catalog_transaction_rollback(token, tx)


@pytest.mark.parametrize("operation", ["create", "add"])
def test_canonical_expression_ddl_requires_transaction_profile_without_graph_profile(backend, monkeypatch, operation):
    profiles = replace(
        DEPENDENCY_PROFILES,
        read=[p for p in DEPENDENCY_PROFILES.read if p != CATALOG_DEPENDENCIES_READ],
        transaction=[],
    )
    monkeypatch.setattr(DependencyCatalog, "profiles", profiles)
    token = backend.catalog_attach(request(replace(OFFER, catalog=profiles))).attach_opaque_data
    tx = backend.catalog_transaction_begin(token).transaction_opaque_data
    calls = []
    monkeypatch.setattr(backend._get_catalog(), "table_create", lambda **kwargs: calls.append(kwargs))
    monkeypatch.setattr(backend._get_catalog(), "table_column_add", lambda **kwargs: calls.append(kwargs))
    field = generated("computed", "id + 1")
    with pytest.raises(CatalogCapabilityError, match=TRANSACTION_DDL):
        if operation == "create":
            backend.catalog_table_create(create_request(token, schema(pa.field("id", pa.int64()), field), tx))
        else:
            backend.catalog_table_column_add(
                token, ["main"], "items", ["computed"], schema(field), transaction_opaque_data=tx
            )
    assert calls == []
    backend.catalog_transaction_rollback(token, tx)
