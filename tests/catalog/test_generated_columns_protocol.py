# Copyright 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103

"""Canonical VIRTUAL creation, durable physical scans and rejection boundaries."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from typing import Any, Literal

import pyarrow as pa
import pytest
from vgi_rpc.http._testing import _SyncTestClient

from vgi.arguments import Arguments
from vgi.catalog import OnConflict, Table, TableInfo
from vgi.catalog.capabilities import ROW_SCALAR_EXPRESSION
from vgi.catalog.catalog_interface import AttachOpaqueData, SerializedSchema, TransactionOpaqueData
from vgi.catalog.evaluation_context import QueryEvaluationContext
from vgi.catalog.sql import ColumnExpressions, SqlExpression, column_expressions, with_column_expressions
from vgi.client import Client
from vgi.client.catalog_mixin import CatalogClientError
from vgi.protocol import ClientCapabilities
from vgi.serve import create_app
from vgi.worker import Worker

from .dependencies_reference import DEPENDENCY_PROFILES
from .generated_columns_reference import GeneratedColumnsCatalog, GeneratedColumnsWorker
from .typing_helpers import CatalogObjectTransactionArgs, present

KEY = b"generated-reference-signing-key"
OFFER = ClientCapabilities(
    engine="reference",
    native_formats=[],
    catalogs=[],
    can_stream=False,
    filter_encodings=[],
    catalog=replace(DEPENDENCY_PROFILES, read=[p for p in DEPENDENCY_PROFILES.read if p != ROW_SCALAR_EXPRESSION]),
)


def expression(
    sql: str, *, owner: list[str] | None = None, context: QueryEvaluationContext | None = None
) -> SqlExpression:
    return SqlExpression(
        sql=sql,
        profile=ROW_SCALAR_EXPRESSION,
        schema_path=owner or ["main"],
        evaluation_context=context or QueryEvaluationContext(),
    )


def generated(
    name: str,
    sql: str,
    *,
    dtype: pa.DataType | None = None,
    generation: Literal["virtual", "stored"] = "virtual",
    owner: list[str] | None = None,
    context: QueryEvaluationContext | None = None,
) -> pa.Field[Any]:
    return with_column_expressions(
        pa.field(name, dtype or pa.int64()),
        ColumnExpressions(generated=expression(sql, owner=owner, context=context), generation=generation),
    )


def columns() -> pa.Schema:
    return pa.schema(
        [
            generated("doubled", "n * 2"),
            pa.field("n", pa.int64(), metadata={b"comment": b"physical"}),
            generated("label", "'item_' || CAST(n AS VARCHAR)", dtype=pa.string()),
            pa.field("text", pa.string()),
            generated("after", "doubled + 1"),
        ]
    )


@pytest.fixture
def backend(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> GeneratedColumnsWorker:
    monkeypatch.setattr(GeneratedColumnsCatalog, "database_path", str(tmp_path / "generated.sqlite"))
    worker = GeneratedColumnsWorker(quiet=True)
    worker._signing_key = KEY
    return worker


def client(token: AttachOpaqueData | None = None) -> Client:
    return Client.from_http(
        "http://vgi.test",
        attach_opaque_data=token,
        httpx_client=_SyncTestClient(create_app(GeneratedColumnsWorker, describe=False, signing_key=KEY)),
    )


def attach(c: Client, offer: ClientCapabilities | None = OFFER) -> AttachOpaqueData:
    return c.catalog_attach(
        name="generated_columns", data_version_spec=None, implementation_version=None, client_capabilities=offer
    ).attach_opaque_data


def args(
    token: AttachOpaqueData, name: str = "items", tx: TransactionOpaqueData | None = None
) -> CatalogObjectTransactionArgs:
    return dict(attach_opaque_data=token, schema_path=["main"], name=name, transaction_opaque_data=tx)


def seed(
    backend: GeneratedColumnsWorker,
    token: AttachOpaqueData,
    rows: list[dict[str, Any]],
    name: str = "items",
    tx: TransactionOpaqueData | None = None,
) -> None:
    catalog = backend._get_catalog()
    assert isinstance(catalog, GeneratedColumnsCatalog)
    catalog.seed_rows(
        **args(backend._unwrap_attach(token), name, backend._unwrap_transaction(tx, token) if tx else None), rows=rows
    )


def scan(c: Client, token: AttachOpaqueData, name: str = "items", tx: TransactionOpaqueData | None = None) -> pa.Table:
    function = c.table_scan_function_get(**args(token, name, tx))
    batches = list(
        c.table_function(
            function_name=function.function_name,
            schema_path=["main"],
            arguments=Arguments(positional=tuple(function.positional_arguments), named=function.named_arguments),
            transaction_opaque_data=tx,
        )
    )
    return pa.Table.from_batches(batches)


def test_convenience_generated_columns_use_explicit_owner_and_canonical_virtual_metadata() -> None:
    table = Table(
        name="t",
        columns=pa.schema([pa.field("n", pa.int64()), pa.field("g", pa.int64())]),
        generated_columns={"g": "n * 2"},
    )
    for owner in [["a.b"], ["a", "b"]]:
        schema = pa.ipc.read_schema(pa.py_buffer(table.to_table_info(owner).columns))
        expr = column_expressions(schema.field("g"))
        assert expr == ColumnExpressions(generated=expression("n * 2", owner=owner), generation="virtual")
        assert b"generated_expression" not in (schema.field("g").metadata or {})
    conflict = with_column_expressions(pa.field("g", pa.int64()), ColumnExpressions(default=expression("1")))
    with pytest.raises(ValueError, match="cannot also have a default"):
        Table(name="t", columns=pa.schema([conflict]), generated_columns={"g": "2"}).to_table_info(["main"])


@pytest.mark.parametrize(
    "field,error,message",
    [
        (generated("g", "n + 1", generation="stored"), NotImplementedError, "stored"),
        (pa.field("g", pa.int64(), metadata={b"generated_expression": b"n + 1"}), ValueError, "canonical"),
        (
            pa.field("nested", pa.struct([pa.field("g", pa.int64(), metadata={b"generated_expression": b"n + 1"})])),
            ValueError,
            "canonical",
        ),
    ],
)
def test_unsupported_generation_rejects_before_provider_and_during_discovery(
    backend: GeneratedColumnsWorker,
    monkeypatch: pytest.MonkeyPatch,
    field: pa.Field[Any],
    error: type[Exception],
    message: str,
) -> None:
    from vgi.catalog.capabilities import CatalogCapabilities
    from vgi.protocol import TableCreateRequest

    schema = pa.schema([pa.field("n", pa.int64()), field])
    calls = []
    monkeypatch.setattr(backend._get_catalog(), "table_create", lambda **kwargs: calls.append(kwargs))
    with client() as c:
        token = attach(c, None)
    with pytest.raises(error, match=message):
        backend.catalog_table_create(
            TableCreateRequest(
                **args(token),
                columns=SerializedSchema(schema.serialize().to_pybytes()),
                on_conflict=OnConflict.ERROR,
                constraint_definitions=[],
            )
        )
    assert calls == []
    with pytest.raises(error, match=message):
        Worker._require_column_expression_profiles(schema, CatalogCapabilities(), "read", ["main"])
    info = TableInfo(
        name="items",
        schema_path=["main"],
        columns=SerializedSchema(schema.serialize().to_pybytes()),
        constraints=[],
        comment=None,
        tags={},
    )
    monkeypatch.setattr(backend._get_catalog(), "table_get", lambda **kwargs: info)
    with pytest.raises(error, match=message):
        backend.catalog_table_get(**args(token))


def test_signed_http_generated_create_scan_reconnect_and_dependency_readback(backend: GeneratedColumnsWorker) -> None:
    schema = columns()
    data: list[dict[str, Any]] = [{"n": 3, "text": "first"}, {"n": None, "text": "null"}]
    with client() as c:
        token = attach(c)
        c.table_create(**args(token), columns=SerializedSchema(schema.serialize().to_pybytes()))
        seed(backend, token, data)
        info = present(c.table_get(**args(token)))
        assert pa.ipc.read_schema(pa.py_buffer(info.columns)).equals(schema, check_metadata=True)
        graph = c.catalog_dependencies(attach_opaque_data=token)
        edges = [edge for edge in graph.edges() if edge.dependent_column_path]
        assert {tuple(present(edge.dependent_column_path)) for edge in edges} == {("doubled",), ("label",), ("after",)}
    with client(token) as c:
        physical = scan(c, token)
        assert physical.schema.names == ["n", "text"]
        assert physical.to_pylist() == data
        assert all(column_expressions(field) == ColumnExpressions() for field in physical.schema)
        assert present(c.table_get(**args(token))).columns == info.columns
        assert c.catalog_dependencies(attach_opaque_data=token) == graph
    # Representation remains readable even without any catalog capability offer.
    with client() as c:
        unaware = attach(c, None)
        assert present(c.table_get(**args(unaware))).columns == info.columns


def test_generated_create_rollback_and_commit_survive_reconnect(backend: GeneratedColumnsWorker) -> None:
    with client() as c:
        token = attach(c)
        tx = present(c.catalog_transaction_begin(attach_opaque_data=token))
        c.table_create(**args(token, tx=tx), columns=SerializedSchema(columns().serialize().to_pybytes()))
        seed(backend, token, [{"n": 7, "text": "pending"}], tx=tx)
        assert c.table_get(**args(token)) is None
    with client(token) as c:
        assert scan(c, token, tx=tx).to_pylist() == [{"n": 7, "text": "pending"}]
        c.catalog_transaction_rollback(attach_opaque_data=token, transaction_opaque_data=tx)
        assert c.table_get(**args(token)) is None
        tx = present(c.catalog_transaction_begin(attach_opaque_data=token))
        c.table_create(**args(token, tx=tx), columns=SerializedSchema(columns().serialize().to_pybytes()))
        seed(backend, token, [{"n": 9, "text": "committed"}], tx=tx)
        c.catalog_transaction_commit(attach_opaque_data=token, transaction_opaque_data=tx)
    with client(token) as c:
        assert scan(c, token).to_pylist() == [{"n": 9, "text": "committed"}]


@pytest.mark.parametrize(
    "bad",
    [
        generated("g", "missing + 1"),
        generated("g", "random()"),
        generated("g", "sum(n)"),
        generated("g", "(SELECT 1)"),
        generated("g", "n + 1", dtype=pa.string()),
        generated("g", "n + 1", generation="stored"),
        generated("g", "n + 1", owner=["elsewhere"]),
        generated("g", "n + 1", context=QueryEvaluationContext(timezone="Europe/London")),
        generated("g", "row_number() OVER ()"),
        generated("g", "main.abs(n)"),
        generated("g", "n) FROM input_data; SELECT (1"),
    ],
)
def test_invalid_generated_replace_preserves_definition_rows_and_version(
    backend: GeneratedColumnsWorker, bad: pa.Field[Any]
) -> None:
    with client() as c:
        token = attach(c)
        c.table_create(**args(token), columns=SerializedSchema(columns().serialize().to_pybytes()))
        seed(backend, token, [{"n": 4, "text": "kept"}])
        before = c.table_get(**args(token))
        version = c.catalog_version(attach_opaque_data=token)
        with pytest.raises(CatalogClientError):
            c.table_create(
                **args(token),
                columns=SerializedSchema(pa.schema([pa.field("n", pa.int64()), bad]).serialize().to_pybytes()),
                on_conflict=OnConflict.REPLACE,
            )
        assert c.table_get(**args(token)) == before
        assert c.catalog_version(attach_opaque_data=token) == version
    with client(token) as c:
        assert scan(c, token).to_pylist() == [{"n": 4, "text": "kept"}]


def test_cycles_and_unsupported_alter_reject_without_changes(backend: GeneratedColumnsWorker) -> None:
    with client() as c:
        token = attach(c)
        version = c.catalog_version(attach_opaque_data=token)
        cycle = pa.schema([pa.field("n", pa.int64()), generated("a", "b + 1"), generated("b", "a + 1")])
        with pytest.raises(CatalogClientError, match="cycle"):
            c.table_create(**args(token), columns=SerializedSchema(cycle.serialize().to_pybytes()))
        assert c.catalog_version(attach_opaque_data=token) == version
        c.table_create(**args(token), columns=SerializedSchema(columns().serialize().to_pybytes()))
        seed(backend, token, [{"n": 1, "text": "kept"}])
        before = c.table_get(**args(token))
        version = c.catalog_version(attach_opaque_data=token)
        operations: list[Callable[[], None]] = [
            lambda: c.table_column_rename(**args(token), column_path=["n"], new_column_name="renamed"),
            lambda: c.table_column_drop(**args(token), column_path=["n"], cascade=True),
            lambda: c.table_column_type_change(
                **args(token),
                column_path=["n"],
                column_definition=SerializedSchema(pa.schema([pa.field("n", pa.int32())]).serialize().to_pybytes()),
            ),
            lambda: c.table_column_add(
                **args(token),
                column_path=["extra"],
                column_definition=SerializedSchema(pa.schema([generated("extra", "n + 1")]).serialize().to_pybytes()),
            ),
        ]
        for operation in operations:
            with pytest.raises(CatalogClientError):
                operation()
            assert c.table_get(**args(token)) == before
            assert c.catalog_version(attach_opaque_data=token) == version
    with client(token) as c:
        assert scan(c, token).to_pylist() == [{"n": 1, "text": "kept"}]
