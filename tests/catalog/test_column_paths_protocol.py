# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103

"""Real RPC column-path edits, data rewrites, rollback, and capability guards."""

from dataclasses import replace
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from vgi_rpc.http._testing import _SyncTestClient

from vgi.catalog.capabilities import (
    COLUMN_NESTED_ADD,
    COLUMN_NESTED_RENAME,
    CONSTRAINT_ENFORCE,
    CONSTRAINT_MUTATE,
    CONSTRAINT_RECORDS_READ,
    ROW_SCALAR_EXPRESSION,
    TRANSACTION_DDL,
    CatalogCapabilities,
)
from vgi.catalog.constraints import (
    ConstraintChange,
    ConstraintDefinition,
    ConstraintEnforcement,
    ConstraintKind,
    ConstraintOperation,
    ConstraintsApplyRequest,
    ConstraintValidation,
)
from vgi.catalog.dependencies import CatalogObjectRef
from vgi.catalog.evaluation_context import QueryEvaluationContext
from vgi.catalog.layout import LayoutChangeMode, NullOrder, SortDirection, SortKey, SqlExpression, TableLayoutChange
from vgi.catalog.sql import ColumnExpressions, column_expressions, with_column_expressions
from vgi.client import Client
from vgi.client.catalog_mixin import CatalogClientError
from vgi.column_path import column_definition, replace_column_field
from vgi.exceptions import CatalogCapabilityError
from vgi.meta_worker import MetaWorker
from vgi.protocol import CatalogAttachRequest, ClientCapabilities
from vgi.serve import create_app

from .column_paths_reference import COLUMN_PROFILES, ColumnPathsCatalog, ColumnPathsWorker

KEY = b"column-path-reference-signing-key"
OFFER = ClientCapabilities(
    engine="reference", native_formats=[], catalogs=[], can_stream=False, filter_encodings=[], catalog=COLUMN_PROFILES
)
SCHEMA = pa.schema(
    [
        pa.field("id", pa.int32(), nullable=False),
        pa.field("a.b", pa.int32(), metadata={b"origin": b"dotted"}),
        pa.field(
            "a",
            pa.struct([pa.field("b", pa.int32(), metadata={b"origin": b"nested"}), pa.field("keep", pa.string())]),
            metadata={b"parent": b"retained"},
        ),
    ],
    metadata={b"table": b"retained"},
)
ROWS = [
    {"id": 1, "a.b": 7, "a": {"b": 2, "keep": "k"}},
    {"id": 2, "a.b": 8, "a": None},
    {"id": 3, "a.b": 9, "a": {"b": None, "keep": "z"}},
]


@pytest.fixture
def backend(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> ColumnPathsWorker:
    """Isolate the durable reference behind both direct and HTTP workers."""
    monkeypatch.setattr(ColumnPathsCatalog, "database_path", str(tmp_path / "columns.sqlite"), raising=False)
    worker = ColumnPathsWorker(quiet=True)
    worker._signing_key = KEY
    return worker


def client() -> Client:
    return Client.from_http(
        "http://vgi.test", httpx_client=_SyncTestClient(create_app(ColumnPathsWorker, describe=False, signing_key=KEY))
    )


def attach(c: Client, offer: ClientCapabilities | None = OFFER) -> bytes:
    return c.catalog_attach(
        name="columns", data_version_spec=None, implementation_version=None, client_capabilities=offer
    ).attach_opaque_data


def args(token: bytes, tx: bytes | None = None) -> dict[str, Any]:
    return dict(attach_opaque_data=token, schema_path=["main"], name="t.with.dot", transaction_opaque_data=tx)


def field(name: str, typ: pa.DataType | None = None, **kwargs: Any) -> bytes:
    return pa.schema([pa.field(name, typ or pa.int32(), **kwargs)]).serialize().to_pybytes()


def expression(sql: str, path: list[str] | None = None) -> SqlExpression:
    return SqlExpression(
        sql=sql,
        profile=ROW_SCALAR_EXPRESSION,
        schema_path=["main"] if path is None else path,
        evaluation_context=QueryEvaluationContext(),
    )


def default_field(name: str, sql: str, **kwargs: Any) -> bytes:
    return (
        pa.schema(
            [with_column_expressions(pa.field(name, pa.int32(), **kwargs), ColumnExpressions(default=expression(sql)))]
        )
        .serialize()
        .to_pybytes()
    )


def info(c: Client, token: bytes, tx: bytes | None = None) -> pa.Schema:
    return pa.ipc.read_schema(pa.BufferReader(c.table_get(**args(token, tx)).columns))


def seed(worker: ColumnPathsWorker, token: bytes, rows: list[dict[str, Any]]) -> None:
    worker._get_catalog().seed_rows(**args(worker._unwrap_attach(token)), rows=rows)


def rows(worker: ColumnPathsWorker, token: bytes, tx: bytes | None = None) -> list[dict[str, Any]]:
    parts = worker._get_catalog().physical_rows(
        **args(worker._unwrap_attach(token), worker._unwrap_transaction(tx, token) if tx is not None else None)
    )
    return [row for part in parts for row in part["rows"]]


def setup(c: Client, backend: ColumnPathsWorker) -> bytes:
    token = attach(c)
    c.table_create(
        **args(token),
        columns=SCHEMA.serialize().to_pybytes(),
        constraint_definitions=[
            ConstraintDefinition(kind=ConstraintKind.NOT_NULL, column_paths=[["id"]]).serialize_to_bytes()
        ],
    )
    seed(backend, token, ROWS)
    return token


def test_nested_add_default_comment_and_nullability(backend: ColumnPathsWorker) -> None:
    with client() as c:
        token = setup(c, backend)
        c.table_column_add(
            **args(token),
            column_path=["a", "x.y"],
            column_definition=default_field("x.y", "9", metadata={b"comment": b"created", b"tags": b'{"team":"one"}'}),
        )
        assert [row["a"] for row in rows(backend, token)] == [
            {"b": 2, "keep": "k", "x.y": 9},
            None,
            {"b": None, "keep": "z", "x.y": 9},
        ]
        schema = info(c, token)
        assert schema.metadata == SCHEMA.metadata and schema.field("a").metadata == {b"parent": b"retained"}
        assert schema.field("a.b").metadata == {b"origin": b"dotted"}
        c.table_column_default_set(**args(token), column_path=["a", "x.y"], expression=expression("3"))
        c.table_not_null_set(**args(token), column_path=["a", "x.y"])
        c.table_column_comment_set(**args(token), column_path=["a", "x.y"], comment="changed")
        seed(backend, token, [{"id": 4, "a.b": 10, "a": {}}])
        assert rows(backend, token)[-1]["a"]["x.y"] == 3
        assert rows(backend, token)[0]["a"]["x.y"] == 9
        c.table_column_default_drop(**args(token), column_path=["a", "x.y"])
        before = rows(backend, token)
        with pytest.raises(ValueError, match="NOT NULL"):
            seed(backend, token, [{"id": 5, "a": {}}])
        assert rows(backend, token) == before
        c.table_not_null_drop(**args(token), column_path=["a", "x.y"])
        c.table_column_comment_set(**args(token), column_path=["a", "x.y"], comment=None)
        definition = info(c, token).field("a").type.field("x.y")
        assert definition.nullable and definition.metadata == {b"tags": b'{"team":"one"}'}
    with client() as c:
        assert info(c, token).field("a").type.field("x.y") == definition


def test_constraint_identity_and_revision_survive_rename_and_noop(backend: ColumnPathsWorker) -> None:
    with client() as c:
        token = setup(c, backend)
        original = c.table_get(**args(token))
        c.table_not_null_set(**args(token), column_path=["id"])
        unchanged = c.table_get(**args(token))
        assert unchanged.definition_revision == original.definition_revision
        c.table_column_rename(**args(token), column_path=["id"], new_column_name="renamed.id")
        renamed = c.table_get(**args(token))
        assert renamed.definition_revision != original.definition_revision
        assert renamed.constraint_infos()[0].ref == original.constraint_infos()[0].ref
        assert renamed.constraint_infos()[0].definition.column_paths == [["renamed.id"]]


def test_type_using_rename_and_drop_keep_literal_dotted_column_distinct(backend: ColumnPathsWorker) -> None:
    with client() as c:
        token = setup(c, backend)
        c.table_column_type_change(
            **args(token),
            column_path=["a", "b"],
            column_definition=field("b", pa.float64()),
            expression=expression('"a"."b" + 0.5'),
        )
        assert rows(backend, token)[0]["a"]["b"] == 2.5
        assert rows(backend, token)[1]["a"] is None
        c.table_column_rename(**args(token), column_path=["a", "b"], new_column_name="b.c")
        schema = info(c, token)
        assert schema.field("a").type.field("b.c").metadata == {b"origin": b"nested"}
        assert schema.field("a.b").type == pa.int32()
        assert rows(backend, token)[0]["a"]["b.c"] == 2.5
        c.table_column_drop(**args(token), column_path=["a", "b.c"])
        assert [row["a.b"] for row in rows(backend, token)] == [7, 8, 9]
        assert rows(backend, token)[0]["a"] == {"keep": "k"}


def test_existing_nulls_and_failed_cast_do_not_change_data_or_version(backend: ColumnPathsWorker) -> None:
    with client() as c:
        token = setup(c, backend)
        original, schema, version = rows(backend, token), info(c, token), c.catalog_version(attach_opaque_data=token)
        with pytest.raises(ValueError, match="NOT NULL"):
            backend.catalog_table_not_null_set(**args(token), column_path=["a", "b"])
        with pytest.raises((ValueError, pa.ArrowInvalid), match="range|convert|truncate"):
            backend.catalog_table_column_type_change(
                **args(token),
                column_path=["a", "b"],
                column_definition=field("b", pa.int8()),
                expression=expression("1000").serialize_to_bytes(),
            )
        assert rows(backend, token) == original and info(c, token).equals(schema, check_metadata=True)
        assert c.catalog_version(attach_opaque_data=token) == version


def test_metadata_edits_preserve_existing_future_write_partitions(backend: ColumnPathsWorker) -> None:
    with client() as c:
        token = setup(c, backend)
        c.table_alter_layout(
            **args(token),
            changes=TableLayoutChange(
                mode=LayoutChangeMode.FUTURE_WRITES,
                sorted_by=[
                    SortKey(
                        expression=SqlExpression(
                            evaluation_context=QueryEvaluationContext(),
                            sql="id",
                            profile=ROW_SCALAR_EXPRESSION,
                            schema_path=["main"],
                        ),
                        direction=SortDirection.DESC,
                        null_order=NullOrder.LAST,
                    ).serialize_to_bytes()
                ],
            ),
        )
        catalog_args = args(backend._unwrap_attach(token))
        original = backend._get_catalog().physical_rows(**catalog_args)
        c.table_column_comment_set(**args(token), column_path=["a", "b"], comment="metadata only")
        c.table_column_default_set(**args(token), column_path=["a", "b"], expression=expression("3"))
        c.table_not_null_set(**args(token), column_path=["id"])
        assert backend._get_catalog().physical_rows(**catalog_args) == original
        seed(backend, token, [{"id": 4}, {"id": 5}])
        assert [row["id"] for row in rows(backend, token)] == [1, 2, 3, 5, 4]


def test_transactional_column_edits_rollback_reconnect_and_conflict(backend: ColumnPathsWorker) -> None:
    with client() as c:
        token = setup(c, backend)
        tx = c.catalog_transaction_begin(attach_opaque_data=token)
        c.table_column_rename(**args(token, tx), column_path=["a", "b"], new_column_name="changed")
        c.table_column_add(**args(token, tx), column_path=["new"], column_definition=default_field("new", "42"))
        assert rows(backend, token, tx)[0]["new"] == 42
        assert "new" not in rows(backend, token)[0]
    with client() as c:
        c.catalog_transaction_rollback(attach_opaque_data=token, transaction_opaque_data=tx)
        assert info(c, token).equals(SCHEMA, check_metadata=True)
        first = c.catalog_transaction_begin(attach_opaque_data=token)
        second = c.catalog_transaction_begin(attach_opaque_data=token)
        c.table_column_rename(**args(token, first), column_path=["a.b"], new_column_name="renamed.literal")
        c.table_column_drop(**args(token, second), column_path=["a", "b"])
    with client() as c:
        c.catalog_transaction_commit(attach_opaque_data=token, transaction_opaque_data=first)
        with pytest.raises(ValueError, match="conflict"):
            backend.catalog_transaction_commit(token, second)
        assert rows(backend, token)[0]["renamed.literal"] == 7
        assert rows(backend, token)[0]["a"]["b"] == 2
        c.catalog_transaction_rollback(attach_opaque_data=token, transaction_opaque_data=second)


def test_capability_absence_downgrade_and_transaction_guarantee(
    backend: ColumnPathsWorker, monkeypatch: pytest.MonkeyPatch
) -> None:
    with client() as c:
        token = setup(c, backend)
        unaware = attach(c, None)
        with pytest.raises(CatalogCapabilityError, match=CONSTRAINT_RECORDS_READ):
            backend.catalog_table_column_rename(**args(unaware), column_path=["id"], new_column_name="hidden")
        old = attach(
            c,
            replace(
                OFFER,
                catalog=CatalogCapabilities(
                    read=[CONSTRAINT_RECORDS_READ], ddl=[CONSTRAINT_MUTATE], write=[CONSTRAINT_ENFORCE]
                ),
            ),
        )
        with pytest.raises(CatalogCapabilityError, match=COLUMN_NESTED_ADD):
            backend.catalog_table_column_add(**args(old), column_path=["a", "new"], column_definition=field("new"))
        # Flat edits need no nested profile but must preserve canonical constraints.
        c.table_column_rename(**args(old), column_path=["a.b"], new_column_name="literal")
        limited = attach(c, replace(OFFER, catalog=replace(COLUMN_PROFILES, transaction=[])))
        tx = c.catalog_transaction_begin(attach_opaque_data=limited)
        with pytest.raises(CatalogCapabilityError, match=TRANSACTION_DDL):
            backend.catalog_table_column_rename(**args(limited, tx), column_path=["a", "b"], new_column_name="other")
        c.catalog_transaction_rollback(attach_opaque_data=limited, transaction_opaque_data=tx)
        monkeypatch.setattr(
            backend._get_catalog(),
            "profiles",
            replace(COLUMN_PROFILES, ddl=[p for p in COLUMN_PROFILES.ddl if p != COLUMN_NESTED_RENAME]),
        )
        with pytest.raises(CatalogCapabilityError, match=COLUMN_NESTED_RENAME):
            backend.catalog_table_column_rename(**args(token), column_path=["a", "b"], new_column_name="other")
        assert rows(backend, token)[0]["a"]["b"] == 2


def test_missing_target_flags_and_parent_validation(backend: ColumnPathsWorker) -> None:
    with client() as c:
        token = setup(c, backend)
        version = c.catalog_version(attach_opaque_data=token)
        c.table_column_add(
            **args(token), column_path=["a", "b"], column_definition=field("b"), if_column_not_exists=True
        )
        c.table_column_drop(**args(token), column_path=["a", "absent"], if_column_exists=True)
        assert c.catalog_version(attach_opaque_data=token) == version
        with pytest.raises(ValueError, match="non-struct"):
            backend.catalog_table_column_add(**args(token), column_path=["a.b", "bad"], column_definition=field("bad"))
        with pytest.raises(ValueError, match="already exists"):
            backend.catalog_table_column_rename(**args(token), column_path=["a", "b"], new_column_name="keep")
        assert c.catalog_version(attach_opaque_data=token) == version


def test_meta_worker_routes_path_without_rendering_it(backend: ColumnPathsWorker) -> None:
    with client() as c:
        token = setup(c, backend)
    meta = MetaWorker([backend])
    token = meta.catalog_attach(
        CatalogAttachRequest(
            name="columns", data_version_spec=None, implementation_version=None, client_capabilities=OFFER
        )
    ).attach_opaque_data
    meta.catalog_table_column_comment_set(**args(token), column_path=["a", "b"], comment="nested only")
    schema = pa.ipc.read_schema(pa.BufferReader(meta.catalog_table_get(**args(token)).to_optional().columns))
    assert b"comment" not in schema.field("a.b").metadata
    assert schema.field("a").type.field("b").metadata[b"comment"] == b"nested only"


def test_definition_validation_and_last_field_guard() -> None:
    with pytest.raises(ValueError, match="match"):
        column_definition(field("b"), ["a"])
    with pytest.raises(ValueError, match="trailing"):
        column_definition(field("b") + b"trailing", ["b"])
    with pytest.raises(ValueError, match="only the target type"):
        column_definition(field("b", metadata={b"default": b"4"}), ["b"], type_only=True)
    with pytest.raises(ValueError, match="only the target type"):
        column_definition(default_field("b", "4"), ["b"], type_only=True)
    with pytest.raises(ValueError, match="last"):
        replace_column_field(pa.schema([pa.field("only", pa.int32())]), ["only"], None)
    with pytest.raises(ValueError, match="non-struct"):
        replace_column_field(SCHEMA, ["a.b", "b"], None)


def test_mandatory_defaults_and_using_without_new_capabilities(backend: ColumnPathsWorker) -> None:
    with client() as c:
        token = attach(c, None)
        c.table_create(**args(token), columns=default_field("temperature", "250"))
        seed(backend, token, [{}, {"temperature": None}, {"temperature": 310}])
        assert rows(backend, token) == [{"temperature": 250}, {"temperature": None}, {"temperature": 310}]
        assert column_expressions(info(c, token).field("temperature")).default == expression("250")
        c.table_column_default_set(**args(token), column_path=["temperature"], expression=expression("NULL"))
        seed(backend, token, [{}])
        assert rows(backend, token)[-1] == {"temperature": None}
        c.table_column_type_change(
            **args(token), column_path=["temperature"], column_definition=field("temperature", pa.int64())
        )
        c.table_column_type_change(
            **args(token),
            column_path=["temperature"],
            column_definition=field("temperature", pa.float64()),
            expression=expression("temperature / 10.0"),
        )
        assert rows(backend, token) == [
            {"temperature": 25.0},
            {"temperature": None},
            {"temperature": 31.0},
            {"temperature": None},
        ]
        c.table_column_add(**args(token), column_path=["added"], column_definition=default_field("added", "7"))
        assert all(row["added"] == 7 for row in rows(backend, token))
        before = c.table_get(**args(token))
        with pytest.raises(CatalogClientError, match="range|convert|truncate"):
            c.table_column_type_change(
                **args(token),
                column_path=["temperature"],
                column_definition=field("temperature", pa.int8()),
                expression=expression("temperature * 1000"),
            )
        assert c.table_get(**args(token)) == before
        c.table_column_default_drop(**args(token), column_path=["temperature"])
        assert column_expressions(info(c, token).field("temperature")).default is None
    with client() as c:
        assert rows(backend, token)[0] == {"temperature": 25.0, "added": 7}
        assert column_expressions(info(c, token).field("added")).default == expression("7")


def test_signed_http_expression_transaction_rollback_and_commit(backend: ColumnPathsWorker) -> None:
    with client() as c:
        token = attach(c)
        c.table_create(**args(token), columns=default_field("temperature", "200"))
        seed(backend, token, [{"temperature": 123}])
        initial = c.table_get(**args(token))
        tx = c.catalog_transaction_begin(attach_opaque_data=token)
        c.table_column_default_set(**args(token, tx), column_path=["temperature"], expression=expression("300"))
        c.table_column_type_change(
            **args(token, tx),
            column_path=["temperature"],
            column_definition=field("temperature", pa.float64()),
            expression=expression("temperature / 10.0"),
        )
        assert rows(backend, token, tx) == [{"temperature": 12.3}]
        assert c.table_get(**args(token)) == initial
    with client() as c:
        c.catalog_transaction_rollback(attach_opaque_data=token, transaction_opaque_data=tx)
        assert c.table_get(**args(token)) == initial and rows(backend, token) == [{"temperature": 123}]
        tx = c.catalog_transaction_begin(attach_opaque_data=token)
        c.table_column_default_set(**args(token, tx), column_path=["temperature"], expression=expression("500"))
        c.table_column_type_change(
            **args(token, tx),
            column_path=["temperature"],
            column_definition=field("temperature", pa.float64()),
            expression=expression("temperature / 10.0"),
        )
        c.catalog_transaction_commit(attach_opaque_data=token, transaction_opaque_data=tx)
    with client() as c:
        assert column_expressions(info(c, token).field("temperature")).default == expression("500")
        seed(backend, token, [{}])
        assert rows(backend, token) == [{"temperature": 12.3}, {"temperature": 500.0}]


@pytest.mark.parametrize("operation", ["default", "using"])
@pytest.mark.parametrize("failure", ["owner", "profile", "context", "script", "malformed", "trailing"])
def test_expression_validation_precedes_effects(backend: ColumnPathsWorker, operation: str, failure: str) -> None:
    with client() as c:
        token = attach(c, None)
        c.table_create(**args(token), columns=field("temperature"))
        seed(backend, token, [{"temperature": 123}])
        original, version = c.table_get(**args(token)), c.catalog_version(attach_opaque_data=token)
        value = expression("7")
        if failure == "owner":
            value = replace(value, schema_path=["other"])
        elif failure == "profile":
            value = replace(value, profile="unsupported.profile.v1")
        elif failure == "context":
            value = replace(value, evaluation_context=QueryEvaluationContext(timezone="America/New_York"))
        elif failure == "script":
            value = replace(value, sql="7); DROP TABLE input_data; SELECT (8")
        payload = value.serialize_to_bytes()
        if failure == "malformed":
            payload = b"not an IPC record"
        elif failure == "trailing":
            payload += b"trailing"
        with pytest.raises((ValueError, pa.ArrowInvalid, OSError)):
            if operation == "default":
                backend.catalog_table_column_default_set(**args(token), column_path=["temperature"], expression=payload)
            else:
                backend.catalog_table_column_type_change(
                    **args(token),
                    column_path=["temperature"],
                    column_definition=field("temperature", pa.int64()),
                    expression=payload,
                )
        assert c.table_get(**args(token)) == original and c.catalog_version(attach_opaque_data=token) == version
        assert rows(backend, token) == [{"temperature": 123}]


def test_legacy_column_metadata_and_untyped_public_expressions_reject(backend: ColumnPathsWorker) -> None:
    with client() as c:
        token = attach(c, None)
        with pytest.raises(CatalogClientError, match="canonical SqlExpression"):
            c.table_create(**args(token), columns=field("temperature", metadata={b"default": b"7"}))
        c.table_create(**args(token), columns=field("temperature"))
        with pytest.raises(CatalogClientError, match="canonical SqlExpression"):
            c.table_column_add(
                **args(token), column_path=["old"], column_definition=field("old", metadata={b"default": b"7"})
            )
        for value in (None, "NULL"):
            with pytest.raises(TypeError, match="SqlExpression"):
                c.table_column_default_set(**args(token), column_path=["temperature"], expression=value)
        with pytest.raises(TypeError, match="SqlExpression"):
            c.table_column_type_change(
                **args(token),
                column_path=["temperature"],
                column_definition=field("temperature", pa.int64()),
                expression="temperature + 1",
            )


def test_native_not_null_request_uses_atomic_revision_and_transaction(backend: ColumnPathsWorker) -> None:
    with client() as c:
        token = setup(c, backend)
        initial = c.table_get(**args(token))
        change = ConstraintChange(
            operation=ConstraintOperation.ADD,
            definition=ConstraintDefinition(kind=ConstraintKind.NOT_NULL, column_paths=[["a.b"]]),
            enforcement=ConstraintEnforcement.ENFORCED,
            validation=ConstraintValidation.ALL_ROWS,
        )
        request = ConstraintsApplyRequest(
            attach_opaque_data=token,
            table=CatalogObjectRef(kind="table", schema_path=["main"], name="t.with.dot"),
            expected_revision=initial.definition_revision,
            changes=[change.serialize_to_bytes()],
        )
        snapshot = c.table_constraints_apply(request)
        assert snapshot.definition_revision != initial.definition_revision
        assert not info(c, token).field("a.b").nullable
        before, version = c.table_get(**args(token)), c.catalog_version(attach_opaque_data=token)
        with pytest.raises(CatalogClientError, match="stale"):
            c.table_constraints_apply(request)
        assert c.table_get(**args(token)) == before and c.catalog_version(attach_opaque_data=token) == version
        bad = replace(change, definition=ConstraintDefinition(kind=ConstraintKind.NOT_NULL, column_paths=[["a", "b"]]))
        with pytest.raises(CatalogClientError, match="NOT NULL"):
            c.table_constraints_apply(
                replace(request, expected_revision=snapshot.definition_revision, changes=[bad.serialize_to_bytes()])
            )
        assert c.table_get(**args(token)) == before and rows(backend, token) == ROWS
        added = next(item for item in snapshot.constraint_infos() if item.definition.column_paths == [["a.b"]])
        tx = c.catalog_transaction_begin(attach_opaque_data=token)
        c.table_constraints_apply(
            replace(
                request,
                expected_revision=snapshot.definition_revision,
                transaction_opaque_data=tx,
                changes=[
                    ConstraintChange(
                        operation=ConstraintOperation.DROP, ref=added.ref, cascade=False
                    ).serialize_to_bytes()
                ],
            )
        )
        assert info(c, token, tx).field("a.b").nullable
        c.catalog_transaction_rollback(attach_opaque_data=token, transaction_opaque_data=tx)
        assert c.table_get(**args(token)) == before
