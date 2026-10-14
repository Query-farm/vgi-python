# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103

"""Public table-option/layout RPCs, persistent readback and actual row rewrites."""

from dataclasses import replace
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from vgi_rpc.http._testing import _SyncTestClient

from vgi.catalog.capabilities import (
    ROW_SCALAR_EXPRESSION,
    TABLE_LAYOUT_ALTER_REWRITE,
    TRANSACTION_DDL,
)
from vgi.catalog.catalog_interface import OnConflict, SchemaObjectType
from vgi.catalog.evaluation_context import QueryEvaluationContext
from vgi.catalog.layout import (
    LayoutChangeMode,
    LayoutCoverage,
    NullOrder,
    SortDirection,
    SortKey,
    SqlExpression,
    TableLayout,
    TableLayoutChange,
)
from vgi.catalog.options import OptionChanges
from vgi.client import Client
from vgi.exceptions import CatalogCapabilityError
from vgi.meta_worker import MetaWorker
from vgi.protocol import CatalogAttachRequest, ClientCapabilities, TableCreateRequest
from vgi.serve import create_app

from .schema_options_reference import values
from .table_layout_reference import TABLE_PROFILES, TableLayoutCatalog, TableLayoutWorker

KEY = b"table-layout-reference-signing-key"
COLUMNS = pa.schema([pa.field("a.b", pa.int32()), pa.field("score", pa.int64())], metadata={b"owner": b"layout"})
OFFER = ClientCapabilities(
    engine="reference", native_formats=[], catalogs=[], can_stream=False, filter_encodings=[], catalog=TABLE_PROFILES
)


@pytest.fixture
def backend(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TableLayoutWorker:
    """Isolate persistent metadata and physical row snapshots per test."""
    monkeypatch.setattr(TableLayoutCatalog, "database_path", str(tmp_path / "tables.sqlite"), raising=False)
    worker = TableLayoutWorker(quiet=True)
    worker._signing_key = KEY
    return worker


def client() -> Client:
    return Client.from_http(
        "http://vgi.test", httpx_client=_SyncTestClient(create_app(TableLayoutWorker, describe=False, signing_key=KEY))
    )


def attach(c: Client, offer: ClientCapabilities | None = OFFER) -> Any:
    return c.catalog_attach(
        name="layout", data_version_spec=None, implementation_version=None, client_capabilities=offer
    ).attach_opaque_data


def expr(sql: str, path: list[str] | None = None) -> SqlExpression:
    return SqlExpression(
        evaluation_context=QueryEvaluationContext(),
        sql=sql,
        profile=ROW_SCALAR_EXPRESSION,
        schema_path=path or ["main"],
    )


def sort(sql: str, direction: SortDirection = SortDirection.DESC, nulls: NullOrder = NullOrder.LAST) -> bytes:
    return SortKey(expression=expr(sql), direction=direction, null_order=nulls).serialize_to_bytes()


def layout() -> TableLayout:
    return TableLayout(partition_by=[expr('"a.b" % 2').serialize_to_bytes()], sorted_by=[sort("abs(score)")])


def args(token: bytes) -> dict[str, Any]:
    return dict(attach_opaque_data=token, schema_path=["main"], name="t.with.dot")


def get(c: Client, token: bytes, tx: bytes | None = None) -> Any:
    return c.table_get(**args(token), transaction_opaque_data=tx)


def create(c: Client, token: bytes, **kwargs: Any) -> None:
    c.table_create(**args(token), columns=COLUMNS.serialize().to_pybytes(), **kwargs)


def physical(worker: TableLayoutWorker, token: bytes, tx: bytes | None = None) -> list[dict[str, Any]]:
    return worker._get_catalog().physical_rows(
        **args(worker._unwrap_attach(token)),
        transaction_opaque_data=worker._unwrap_transaction(tx, token) if tx else None,
    )


def seed(worker: TableLayoutWorker, token: bytes, rows: list[dict[str, Any]]) -> None:
    worker._get_catalog().seed_rows(**args(worker._unwrap_attach(token)), transaction_opaque_data=None, rows=rows)


def test_create_discovery_options_and_restart(backend: TableLayoutWorker) -> None:
    with client() as c:
        token = attach(c)
        definitions = c.table_option_definitions(**args(token))
        assert definitions.alter_names == ["label", "limit"]
        create(c, token, options=values(label=None), layout=layout(), comment="kept", tags={"team": "one"})
        info = get(c, token)
        assert info.comment == "kept" and info.tags == {"team": "one"}
        assert info.options.explicit_names == ["label"]
        assert info.options.values.batch.column("label")[0].as_py() is None
        assert info.layout == layout()
        assert pa.ipc.read_schema(pa.BufferReader(info.columns)).equals(COLUMNS, check_metadata=True)
        listed = c.schema_contents(type=SchemaObjectType.TABLE, attach_opaque_data=token, path=["main"])
        assert listed[0].layout == info.layout
        c.table_alter_options(**args(token), changes=OptionChanges(set_values=values(limit=50), reset_names=["label"]))
    with client() as c:
        info = get(c, token)
        assert info.options.explicit_names == ["limit"]
        assert info.options.values.batch.column("label")[0].as_py() == "default"
        assert info.options.values.batch.column("limit")[0].as_py() == 50
        assert info.layout == layout()


def test_future_writes_and_atomic_rewrite_rollback_commit(backend: TableLayoutWorker) -> None:
    with client() as c:
        token = attach(c)
        create(c, token, layout=layout())
        seed(backend, token, [{"a.b": 1, "score": 2}, {"a.b": 3, "score": -5}, {"a.b": 2, "score": None}])
        original = physical(backend, token)
        assert [r["score"] for r in original[0]["rows"]] == [-5, 2]
        c.table_alter_layout(
            **args(token),
            changes=TableLayoutChange(
                mode=LayoutChangeMode.FUTURE_WRITES,
                partition_by=[],
                sorted_by=[sort("score", SortDirection.ASC, NullOrder.FIRST)],
            ),
        )
        assert physical(backend, token) == original
        assert get(c, token).layout.coverage == LayoutCoverage.FUTURE_WRITES
        seed(backend, token, [{"a.b": 4, "score": 9}, {"a.b": 5, "score": None}, {"a.b": 6, "score": 1}])
        assert [r["score"] for r in physical(backend, token)[-1]["rows"]] == [None, 1, 9]
        before = physical(backend, token)
        tx = c.catalog_transaction_begin(attach_opaque_data=token)
        c.table_alter_layout(
            **args(token),
            transaction_opaque_data=tx,
            changes=TableLayoutChange(mode=LayoutChangeMode.REWRITE, partition_by=[]),
        )
        assert len(physical(backend, token, tx)) == 1
        assert [r["score"] for r in physical(backend, token, tx)[0]["rows"]] == [None, None, -5, 1, 2, 9]
        assert physical(backend, token) == before
    with client() as c:
        c.catalog_transaction_rollback(attach_opaque_data=token, transaction_opaque_data=tx)
        assert physical(backend, token) == before
        tx = c.catalog_transaction_begin(attach_opaque_data=token)
        c.table_alter_layout(
            **args(token),
            transaction_opaque_data=tx,
            changes=TableLayoutChange(mode=LayoutChangeMode.REWRITE, sorted_by=[]),
        )
    with client() as c:
        c.catalog_transaction_commit(attach_opaque_data=token, transaction_opaque_data=tx)
        assert len(physical(backend, token)) == 1
        assert get(c, token).layout.coverage == LayoutCoverage.ALL_ROWS
        c.table_drop(**args(token))
        assert get(c, token) is None


@pytest.mark.parametrize("sql", ["random()", "(select 42)", "missing_column", "1); DROP TABLE input_data; SELECT (1"])
def test_invalid_expression_rejected_before_catalog_change(backend: TableLayoutWorker, sql: str) -> None:
    with client() as c:
        token = attach(c)
        version = c.catalog_version(attach_opaque_data=token)
        with pytest.raises(Exception, match="scalar|expression|column|syntax"):
            create(c, token, layout=TableLayout(sorted_by=[sort(sql)]))
        assert get(c, token) is None
        assert c.catalog_version(attach_opaque_data=token) == version


def test_rewrite_evaluation_failure_preserves_metadata_and_rows(backend: TableLayoutWorker) -> None:
    with client() as c:
        token = attach(c)
        create(c, token)
        seed(backend, token, [{"a.b": 1, "score": -(2**63)}])
        before, version = physical(backend, token), c.catalog_version(attach_opaque_data=token)
        with pytest.raises(Exception, match="overflow|Overflow|range"):
            c.table_alter_layout(
                **args(token), changes=TableLayoutChange(mode=LayoutChangeMode.REWRITE, sorted_by=[sort("abs(score)")])
            )
        assert physical(backend, token) == before
        assert get(c, token).layout == TableLayout()
        assert c.catalog_version(attach_opaque_data=token) == version


def test_legacy_projection_and_capability_scoped_etags(backend: TableLayoutWorker) -> None:
    with client() as c:
        token = attach(c)
        old = attach(c, None)
        create(c, token, layout=layout(), options=values(limit=19))
        assert get(c, old).layout is None and get(c, old).options is None
        assert c.schema_contents(type=SchemaObjectType.TABLE, attach_opaque_data=old, path=["main"])[0].layout is None
        with c._catalog_connect() as proxy:
            full = proxy.catalog_contents(attach_opaque_data=token)
            legacy = proxy.catalog_contents(attach_opaque_data=old, if_none_match=full.etag)
            assert not legacy.not_modified and legacy.etag != full.etag
            assert proxy.catalog_contents(attach_opaque_data=old, if_none_match=legacy.etag).not_modified
        from vgi.catalog import TableInfo

        assert TableInfo.deserialize_from_bytes(legacy.schemas[0].tables[0]).layout is None


def test_missing_profiles_and_mode_downgrade_never_call_provider(
    backend: TableLayoutWorker, monkeypatch: pytest.MonkeyPatch
) -> None:
    token = backend.catalog_attach(
        CatalogAttachRequest(
            name="layout", data_version_spec=None, implementation_version=None, client_capabilities=None
        )
    ).attach_opaque_data
    calls = []
    monkeypatch.setattr(backend._get_catalog(), "table_create", lambda **kw: calls.append(kw))
    for extra in ({"options": values(limit=1)}, {"layout": layout()}, {"comment": "c"}, {"tags": {}}):
        with pytest.raises(CatalogCapabilityError):
            backend.catalog_table_create(
                TableCreateRequest(
                    constraint_definitions=[],
                    **args(token),
                    columns=COLUMNS.serialize().to_pybytes(),
                    on_conflict=OnConflict.ERROR,
                    **extra,
                )
            )
    assert not calls
    token = backend.catalog_attach(
        CatalogAttachRequest(
            name="layout", data_version_spec=None, implementation_version=None, client_capabilities=OFFER
        )
    ).attach_opaque_data
    monkeypatch.setattr(
        backend._get_catalog(),
        "profiles",
        replace(TABLE_PROFILES, ddl=[p for p in TABLE_PROFILES.ddl if p != TABLE_LAYOUT_ALTER_REWRITE]),
    )
    monkeypatch.setattr(backend._get_catalog(), "table_alter_layout", lambda **kw: calls.append(kw))
    with pytest.raises(CatalogCapabilityError):
        backend.catalog_table_alter_layout(
            **args(token), changes=TableLayoutChange(mode=LayoutChangeMode.REWRITE, sorted_by=[]).serialize_to_bytes()
        )
    assert not calls


def test_new_table_ddl_requires_transaction_guarantee(backend: TableLayoutWorker) -> None:
    limited = replace(OFFER, catalog=replace(TABLE_PROFILES, transaction=[]))
    token = backend.catalog_attach(
        CatalogAttachRequest(
            name="layout", data_version_spec=None, implementation_version=None, client_capabilities=limited
        )
    ).attach_opaque_data
    tx = backend.catalog_transaction_begin(token).transaction_opaque_data
    with pytest.raises(CatalogCapabilityError, match=TRANSACTION_DDL):
        backend.catalog_table_create(
            TableCreateRequest(
                constraint_definitions=[],
                **args(token),
                transaction_opaque_data=tx,
                columns=COLUMNS.serialize().to_pybytes(),
                on_conflict=OnConflict.ERROR,
                layout=layout(),
            )
        )
    backend.catalog_transaction_rollback(token, tx)


def test_layout_mode_and_nested_record_validation() -> None:
    with pytest.raises(ValueError):
        TableLayoutChange(mode=LayoutChangeMode.REWRITE)
    with pytest.raises(ValueError):
        SortKey(expression=expr("score"), direction="default", null_order=NullOrder.LAST)
    with pytest.raises(pa.ArrowInvalid):
        TableLayout(partition_by=[b"not IPC"])
    assert (
        TableLayoutChange(mode=LayoutChangeMode.REWRITE, sorted_by=[]).apply_to(layout()).partition_by
        == layout().partition_by
    )


def test_partial_layout_change_rechecks_retained_expression_profiles(
    backend: TableLayoutWorker, monkeypatch: pytest.MonkeyPatch
) -> None:
    with client() as c:
        token = attach(c)
        create(c, token, layout=layout())
        seed(backend, token, [{"a.b": 1, "score": 2}])
        before = physical(backend, token)
        version = c.catalog_version(attach_opaque_data=token)
        no_expressions = replace(
            OFFER, catalog=replace(TABLE_PROFILES, read=[p for p in TABLE_PROFILES.read if p != ROW_SCALAR_EXPRESSION])
        )
        limited = attach(c, no_expressions)
        assert get(c, limited).layout is None
        calls = []
        monkeypatch.setattr(backend._get_catalog(), "table_alter_layout", lambda **kw: calls.append(kw))
        change = TableLayoutChange(mode=LayoutChangeMode.REWRITE, partition_by=[])
        with pytest.raises(CatalogCapabilityError, match=ROW_SCALAR_EXPRESSION):
            backend.catalog_table_alter_layout(**args(limited), changes=change.serialize_to_bytes())
        monkeypatch.setattr(backend._get_catalog(), "profiles", no_expressions.catalog)
        with pytest.raises(CatalogCapabilityError, match=ROW_SCALAR_EXPRESSION):
            backend.catalog_table_alter_layout(**args(token), changes=change.serialize_to_bytes())
        assert not calls and physical(backend, token) == before
        assert c.catalog_version(attach_opaque_data=token) == version


def test_invalid_mixed_option_changes_are_atomic(backend: TableLayoutWorker) -> None:
    with client() as c:
        token = attach(c)
        create(c, token, options=values(limit=19))
        before, version = get(c, token), c.catalog_version(attach_opaque_data=token)
        with pytest.raises(ValueError, match="unknown"):
            backend.catalog_table_alter_options(
                **args(token),
                changes=OptionChanges(set_values=values(limit=25), reset_names=["unknown"]).serialize_to_bytes(),
            )
        assert get(c, token) == before
        assert c.catalog_version(attach_opaque_data=token) == version


def test_nested_owner_paths_do_not_collapse_dotted_names(backend: TableLayoutWorker) -> None:
    with client() as c:
        token = attach(c)
        c.schema_create(attach_opaque_data=token, path=["one"])
        for path in (["one.two"], ["one", "two"]):
            c.schema_create(attach_opaque_data=token, path=path)
            c.table_create(
                attach_opaque_data=token,
                schema_path=path,
                name="t",
                columns=COLUMNS.serialize().to_pybytes(),
                layout=TableLayout(partition_by=[expr('"a.b"', path).serialize_to_bytes()]),
            )
        first = c.table_get(attach_opaque_data=token, schema_path=["one.two"], name="t")
        second = c.table_get(attach_opaque_data=token, schema_path=["one", "two"], name="t")
        assert first.layout.partition_expressions()[0].schema_path == ["one.two"]
        assert second.layout.partition_expressions()[0].schema_path == ["one", "two"]
        version = c.catalog_version(attach_opaque_data=token)
        with pytest.raises(ValueError, match="owner schema"):
            backend.catalog_table_create(
                TableCreateRequest(
                    constraint_definitions=[],
                    attach_opaque_data=token,
                    schema_path=["one.two"],
                    name="bad",
                    columns=COLUMNS.serialize().to_pybytes(),
                    on_conflict=OnConflict.ERROR,
                    layout=TableLayout(partition_by=[expr('"a.b"', ["one", "two"]).serialize_to_bytes()]),
                )
            )
        assert c.catalog_version(attach_opaque_data=token) == version


def test_meta_worker_routes_all_table_option_and_layout_operations(backend: TableLayoutWorker) -> None:
    worker = MetaWorker([backend])
    token = worker.catalog_attach(
        CatalogAttachRequest(
            name="layout", data_version_spec=None, implementation_version=None, client_capabilities=OFFER
        )
    ).attach_opaque_data
    worker.catalog_table_create(
        TableCreateRequest(
            constraint_definitions=[],
            **args(token),
            columns=COLUMNS.serialize().to_pybytes(),
            on_conflict=OnConflict.ERROR,
            layout=layout(),
        )
    )
    assert worker.catalog_table_option_definitions(**args(token)).alter_names == ["label", "limit"]
    worker.catalog_table_alter_options(
        **args(token), changes=OptionChanges(set_values=values(limit=27)).serialize_to_bytes()
    )
    worker.catalog_table_alter_layout(
        **args(token),
        changes=TableLayoutChange(mode=LayoutChangeMode.REWRITE, partition_by=[], sorted_by=[]).serialize_to_bytes(),
    )
    info = worker.catalog_table_get(**args(token)).to_optional()
    assert info.options.values.batch.column("limit")[0].as_py() == 27
    assert info.layout == TableLayout()


def test_conflicting_layout_commit_preserves_winner(backend: TableLayoutWorker) -> None:
    with client() as c:
        token = attach(c)
        create(c, token, layout=layout())
        seed(backend, token, [{"a.b": 1, "score": 2}])
        before = physical(backend, token)
        first = c.catalog_transaction_begin(attach_opaque_data=token)
        second = c.catalog_transaction_begin(attach_opaque_data=token)
        c.table_alter_options(
            **args(token), transaction_opaque_data=first, changes=OptionChanges(set_values=values(limit=31))
        )
        c.table_alter_layout(
            **args(token),
            transaction_opaque_data=second,
            changes=TableLayoutChange(mode=LayoutChangeMode.REWRITE, partition_by=[], sorted_by=[]),
        )
        c.catalog_transaction_commit(attach_opaque_data=token, transaction_opaque_data=first)
        with pytest.raises(ValueError, match="conflict"):
            backend.catalog_transaction_commit(token, second)
        assert get(c, token).layout == layout()
        assert get(c, token).options.values.batch.column("limit")[0].as_py() == 31
        assert physical(backend, token) == before
        c.catalog_transaction_rollback(attach_opaque_data=token, transaction_opaque_data=second)
