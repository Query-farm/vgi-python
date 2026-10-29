# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103

"""Atomic semantic ALTER against authoritative object and generated-column edges."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal, cast, overload

import pyarrow as pa
import pytest

from vgi.catalog import TableInfo
from vgi.catalog.capabilities import ROW_SCALAR_EXPRESSION
from vgi.catalog.catalog_interface import AttachOpaqueData, SerializedSchema, TransactionOpaqueData
from vgi.catalog.dependencies import CatalogObjectRef, DependencySnapshot
from vgi.catalog.evaluation_context import QueryEvaluationContext
from vgi.catalog.sql import ColumnExpressions, SqlExpression, with_column_expressions
from vgi.client import Client
from vgi.client.catalog_mixin import CatalogClientError

from .dependencies_reference import DependencyCatalog, DependencyWorker
from .test_dependencies_protocol import attach, client
from .test_dependencies_protocol import backend as backend
from .typing_helpers import CatalogColumnTransactionArgs, CatalogObjectTransactionArgs, present


def expression(sql: str, *, context: QueryEvaluationContext | None = None) -> SqlExpression:
    return SqlExpression(
        sql=sql,
        profile=ROW_SCALAR_EXPRESSION,
        schema_path=["main"],
        evaluation_context=context or QueryEvaluationContext(),
    )


def generated(
    name: str,
    sql: str,
    *,
    generation: Literal["virtual", "stored"] = "virtual",
    dtype: pa.DataType | None = None,
    context: QueryEvaluationContext | None = None,
) -> pa.Field[Any]:
    return with_column_expressions(
        pa.field(name, dtype or pa.int64()),
        ColumnExpressions(generated=expression(sql, context=context), generation=generation),
    )


def schema(*fields: pa.Field[Any]) -> SerializedSchema:
    return SerializedSchema(pa.schema(fields).serialize().to_pybytes())


def create(c: Client, token: AttachOpaqueData, columns: SerializedSchema, name: str = "items") -> None:
    c.table_create(attach_opaque_data=token, schema_path=["main"], name=name, columns=columns)


def get(
    c: Client, token: AttachOpaqueData, name: str = "items", tx: TransactionOpaqueData | None = None
) -> TableInfo | None:
    return c.table_get(attach_opaque_data=token, schema_path=["main"], name=name, transaction_opaque_data=tx)


def columns(
    c: Client, token: AttachOpaqueData, name: str = "items", tx: TransactionOpaqueData | None = None
) -> pa.Schema:
    return pa.ipc.read_schema(pa.py_buffer(present(get(c, token, name, tx)).columns))


def graph(c: Client, token: AttachOpaqueData, tx: TransactionOpaqueData | None = None) -> DependencySnapshot:
    return c.catalog_dependencies(attach_opaque_data=token, transaction_opaque_data=tx)


@overload
def kwargs(
    token: AttachOpaqueData, path: None = None, *, tx: TransactionOpaqueData | None = None
) -> CatalogObjectTransactionArgs: ...


@overload
def kwargs(
    token: AttachOpaqueData, path: list[str], *, tx: TransactionOpaqueData | None = None
) -> CatalogColumnTransactionArgs: ...


def kwargs(
    token: AttachOpaqueData, path: list[str] | None = None, *, tx: TransactionOpaqueData | None = None
) -> CatalogColumnTransactionArgs:
    result: CatalogColumnTransactionArgs = dict(
        attach_opaque_data=token, schema_path=["main"], name="items", transaction_opaque_data=tx
    )
    if path is not None:
        result["column_path"] = path
    return result


def seed_generated(c: Client, token: AttachOpaqueData, *, views: bool = False) -> None:
    create(
        c,
        token,
        schema(
            pa.field("id", pa.int64()),
            generated("plus_one", "id + 1"),
            generated("plus_two", "plus_one + 1"),
            pa.field("keep", pa.string()),
        ),
    )
    if views:
        c.view_create(attach_opaque_data=token, schema_path=["main"], name="v", definition="SELECT * FROM items")
        c.view_create(attach_opaque_data=token, schema_path=["main"], name="v2", definition="SELECT * FROM v")


def test_semantic_view_guards_allow_comments_without_weakening_graph(backend: DependencyWorker) -> None:
    with client() as c:
        token = attach(c)
        create(
            c,
            token,
            schema(
                pa.field("a", pa.struct([pa.field("b", pa.int64()), pa.field("keep", pa.int64())])),
                pa.field("keep", pa.int64()),
            ),
        )
        c.view_create(attach_opaque_data=token, schema_path=["main"], name="v", definition="SELECT * FROM items")
        before = graph(c, token)
        edges = before.edges()
        protected = [
            edge for edge in edges if edge.dependent_ref().kind == "view" and edge.subject_ref().kind == "table"
        ]
        assert len(protected) == 1 and protected[0].blocks_alter and protected[0].subject_column_paths is None
        for call in cast(
            list[Callable[[], object]],
            [
                lambda: c.table_rename(**kwargs(token), new_name="renamed"),
                lambda: c.table_column_rename(**kwargs(token, ["a", "b"]), new_column_name="renamed"),
                lambda: c.table_column_type_change(
                    **kwargs(token, ["a", "b"]), column_definition=schema(pa.field("b", pa.int32()))
                ),
                lambda: c.table_column_drop(**kwargs(token, ["a", "b"])),
            ],
        ):
            with pytest.raises(CatalogClientError, match="depend|ALTER|alter"):
                call()
            assert graph(c, token) == before
        c.table_comment_set(**kwargs(token), comment="harmless table label")
        c.table_column_comment_set(**kwargs(token, ["a", "b"]), comment="harmless nested label")
        assert graph(c, token).dependencies == before.dependencies
        assert present(get(c, token)).comment == "harmless table label"
        assert columns(c, token).field("a").type.field("b").metadata[b"comment"] == b"harmless nested label"


def test_raw_paths_and_table_rename_preserve_exact_graph_identity(backend: DependencyWorker) -> None:
    with client() as c:
        token = attach(c)
        create(
            c,
            token,
            schema(
                pa.field("a.b", pa.int64()),
                pa.field("a", pa.struct([pa.field("b", pa.int64()), pa.field("keep", pa.int64())])),
                pa.field("keep", pa.int64()),
            ),
        )
        c.table_column_rename(**kwargs(token, ["a", "b"]), new_column_name="renamed.b")
        assert columns(c, token).field("a.b").type == pa.int64()
        assert columns(c, token).field("a").type.names == ["renamed.b", "keep"]
        c.table_column_type_change(
            **kwargs(token, ["a", "renamed.b"]), column_definition=schema(pa.field("renamed.b", pa.string()))
        )
        c.table_column_drop(**kwargs(token, ["a.b"]))
        assert columns(c, token).names == ["a", "keep"]
        c.table_rename(**kwargs(token), new_name="renamed.table")
        assert get(c, token) is None
        renamed = present(get(c, token, "renamed.table"))
        assert renamed.name == "renamed.table"
        names = {ref.name for ref in graph(c, token).object_refs() if ref.kind == "table"}
        assert names == {"renamed.table"}
    with client() as c:
        assert get(c, token, "renamed.table") == renamed


def test_generated_edges_are_column_scoped_and_leaf_rename_updates_identity(backend: DependencyWorker) -> None:
    with client() as c:
        token = attach(c)
        seed_generated(c, token)
        before = graph(c, token)
        edges = [edge for edge in before.edges() if edge.dependent_column_path is not None]
        assert {
            (tuple(present(edge.dependent_column_path)), tuple(map(tuple, present(edge.subject_column_paths))))
            for edge in edges
        } == {(("plus_one",), (("id",),)), (("plus_two",), (("plus_one",),))}
        for path in [["id"], ["plus_one"]]:
            with pytest.raises(CatalogClientError, match="depend|ALTER|alter"):
                c.table_column_rename(**kwargs(token, path), new_column_name="changed")
            with pytest.raises(CatalogClientError, match="depend|ALTER|alter"):
                c.table_column_type_change(
                    **kwargs(token, path), column_definition=schema(pa.field(path[-1], pa.int32()))
                )
        c.table_column_rename(**kwargs(token, ["plus_two"]), new_column_name="last")
        current = graph(c, token)
        assert any(edge.dependent_column_path == ["last"] for edge in current.edges())
        assert not any(edge.dependent_column_path == ["plus_two"] for edge in current.edges())
        c.table_column_drop(**kwargs(token, ["last"]))
        assert columns(c, token).names == ["id", "plus_one", "keep"]
        assert len([edge for edge in graph(c, token).edges() if edge.dependent_column_path]) == 1


def test_dotted_generated_reference_does_not_protect_unrelated_nested_path(backend: DependencyWorker) -> None:
    with client() as c:
        token = attach(c)
        create(
            c,
            token,
            schema(
                pa.field("a.b", pa.int64()),
                pa.field("a", pa.struct([pa.field("b", pa.int64()), pa.field("keep", pa.int64())])),
                generated("derived", '"a.b" + 1'),
                pa.field("keep", pa.string()),
            ),
        )
        edge = next(edge for edge in graph(c, token).edges() if edge.dependent_column_path)
        assert edge.subject_column_paths == [["a.b"]]
        c.table_column_rename(**kwargs(token, ["a", "b"]), new_column_name="different")
        with pytest.raises(CatalogClientError, match="depend|ALTER|alter"):
            c.table_column_rename(**kwargs(token, ["a.b"]), new_column_name="different")


def test_drop_cascade_removes_generated_columns_and_views_without_dropping_table(backend: DependencyWorker) -> None:
    with client() as c:
        token = attach(c)
        seed_generated(c, token, views=True)
        before = graph(c, token)
        with pytest.raises(CatalogClientError, match="depend|ALTER|alter"):
            c.table_column_drop(**kwargs(token, ["id"]))
        assert graph(c, token) == before
        c.table_column_drop(**kwargs(token, ["id"]), cascade=True)
        assert columns(c, token).names == ["keep"]
        assert c.view_get(attach_opaque_data=token, schema_path=["main"], name="v") is None
        assert c.view_get(attach_opaque_data=token, schema_path=["main"], name="v2") is None
        current = graph(c, token)
        assert current.catalog_version == before.catalog_version + 1
        assert not any(edge.dependent_column_path is not None for edge in current.edges())
        assert {ref.kind for ref in current.object_refs()} == {"schema", "table"}
    with client() as c:
        assert graph(c, token) == current


@pytest.mark.parametrize("failure", ["column_authorization", "view_authorization", "publication"])
def test_denied_cascade_closure_or_publication_preserves_every_object(
    backend: DependencyWorker, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    with client() as c:
        token = attach(c)
        seed_generated(c, token, views=True)
        before, original = graph(c, token), get(c, token)

        def deny_column(self: DependencyCatalog, ref: CatalogObjectRef, path: list[str], action: str) -> None:
            if path == ["plus_two"]:
                raise ValueError("denied generated column")

        def deny_view(self: DependencyCatalog, ref: CatalogObjectRef) -> None:
            if ref.name == "v2":
                raise ValueError("denied view")

        def fail_publish(self: DependencyCatalog) -> None:
            raise ValueError("denied publication")

        name, hook = {
            "column_authorization": ("_authorize_alter", deny_column),
            "view_authorization": ("_authorize_drop", deny_view),
            "publication": ("_after_write", fail_publish),
        }[failure]
        monkeypatch.setattr(DependencyCatalog, name, hook)
        with pytest.raises(CatalogClientError, match="denied"):
            c.table_column_drop(**kwargs(token, ["id"]), cascade=True)
        assert graph(c, token) == before and get(c, token) == original
        assert c.view_get(attach_opaque_data=token, schema_path=["main"], name="v2") is not None


def test_semantic_transactions_reconnect_rollback_and_conflict_preserve_metadata(backend: DependencyWorker) -> None:
    with client() as c:
        token = attach(c)
        seed_generated(c, token, views=True)
        before = graph(c, token)
        tx = c.catalog_transaction_begin(attach_opaque_data=token)
        c.table_column_drop(**kwargs(token, ["id"], tx=tx), cascade=True)
        pending = graph(c, token, tx)
        assert columns(c, token, tx=tx).names == ["keep"]
        assert graph(c, token) == before
    with client() as c:
        assert graph(c, token, tx) == pending
        c.catalog_transaction_rollback(attach_opaque_data=token, transaction_opaque_data=present(tx))
        assert graph(c, token) == before
        tx = c.catalog_transaction_begin(attach_opaque_data=token)
        c.table_column_drop(**kwargs(token, ["id"], tx=tx), cascade=True)
        c.table_comment_set(**kwargs(token), comment="concurrent metadata")
        with pytest.raises(CatalogClientError, match="transaction conflict"):
            c.catalog_transaction_commit(attach_opaque_data=token, transaction_opaque_data=present(tx))
        assert columns(c, token).names == ["id", "plus_one", "plus_two", "keep"]
        assert present(get(c, token)).comment == "concurrent metadata"
        c.catalog_transaction_rollback(attach_opaque_data=token, transaction_opaque_data=present(tx))
        tx = c.catalog_transaction_begin(attach_opaque_data=token)
        c.table_column_drop(**kwargs(token, ["id"], tx=tx), cascade=True)
        c.catalog_transaction_commit(attach_opaque_data=token, transaction_opaque_data=present(tx))
        assert columns(c, token).names == ["keep"]


@pytest.mark.parametrize(
    "bad",
    [
        generated("bad", "missing + 1"),
        generated("bad", "sum(id)"),
        generated("bad", "random()"),
        generated("bad", "(SELECT 1)"),
        generated("bad", "id + 1", generation="stored"),
        generated("bad", "id + 1", context=QueryEvaluationContext(timezone="Europe/London")),
        generated("bad", "id + 1", dtype=pa.string()),
    ],
)
def test_unsupported_generated_semantics_do_not_publish_incomplete_dependencies(
    backend: DependencyWorker, bad: pa.Field[Any]
) -> None:
    with client() as c:
        token = attach(c)
        before = graph(c, token)
        with pytest.raises(CatalogClientError):
            create(c, token, schema(pa.field("id", pa.int64()), bad))
        assert get(c, token) is None and graph(c, token) == before


def test_generated_cycles_and_last_column_cascade_are_atomic(backend: DependencyWorker) -> None:
    with client() as c:
        token = attach(c)
        before = graph(c, token)
        with pytest.raises(CatalogClientError, match="cycle"):
            create(c, token, schema(generated("a", "b + 1"), generated("b", "a + 1")))
        assert graph(c, token) == before
        create(c, token, schema(pa.field("id", pa.int64()), generated("derived", "id + 1")))
        before = graph(c, token)
        with pytest.raises(CatalogClientError, match="last"):
            c.table_column_drop(**kwargs(token, ["id"]), cascade=True)
        assert graph(c, token) == before


def test_generated_case_aliases_share_one_resolved_subject_path(backend: DependencyWorker) -> None:
    with client() as c:
        token = attach(c)
        create(c, token, schema(pa.field("a", pa.int64()), generated("both", "a + A")))
        edges = [edge for edge in graph(c, token).edges() if edge.dependent_column_path]
        assert len(edges) == 1
        assert edges[0].dependent_column_path == ["both"]
        assert edges[0].subject_column_paths == [["a"]]
        with pytest.raises(CatalogClientError, match="dependent"):
            c.table_column_drop(**kwargs(token, ["a"]))


def test_table_create_rejects_schema_trailing_bytes_without_publication(backend: DependencyWorker) -> None:
    with client() as c:
        token = attach(c)
        before = graph(c, token)
        with pytest.raises(CatalogClientError, match="trailing"):
            create(c, token, SerializedSchema(schema(pa.field("a", pa.int64())) + b"unexpected-suffix"))
        assert get(c, token) is None
        assert graph(c, token) == before
