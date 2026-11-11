# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103

"""The ``simple_writable`` fixture's created tables (protocol 3.3 CTAS contract).

Mirrors what ``vgi/test/sql/integration/simple_writable/ctas*.test`` need from the
fixture (``docs/writable_tx_fixtures.md``, "simple_writable: created tables"):
``catalog_table_create`` reports ``created`` (false for IF NOT EXISTS on an
existing table, which stays unchanged), created tables scan and insert,
``catalog_table_drop`` removes them, and the write functions are table_only.
"""

from __future__ import annotations

import sys
from typing import Any

import pyarrow as pa
import pytest
from vgi_rpc.utils import serialize_record_batch_bytes

from vgi.arguments import Arguments
from vgi.catalog import FUNCTION_VISIBILITY_TABLE_ONLY, OnConflict, SchemaObjectType, SerializedSchema
from vgi.client.client import Client

_WORKER = f"{sys.executable} -m vgi._test_fixtures.simple_writable"
_COLUMNS = pa.schema(
    [
        ("i", pa.int32()),
        ("b", pa.int64()),
        ("v", pa.string()),
        ("d", pa.float64()),
        ("f", pa.bool_()),
    ]
)


@pytest.fixture
def attached() -> Any:
    with Client(_WORKER, pool=None) as client:
        result = client.catalog_attach(
            name="simple_writable", options={}, data_version_spec=None, implementation_version=None
        )
    with Client(_WORKER, pool=None, attach_opaque_data=result.attach_opaque_data) as client:
        yield client, result.attach_opaque_data


def _create(client: Client, attach: Any, name: str, on_conflict: OnConflict, columns: pa.Schema = _COLUMNS) -> bool:
    return client.table_create(
        attach_opaque_data=attach,
        schema_path=["main"],
        name=name,
        columns=SerializedSchema(columns.serialize().to_pybytes()),
        on_conflict=on_conflict,
    )


def _insert(client: Client, name: str, rows: list[dict[str, Any]], schema: pa.Schema = _COLUMNS) -> list[Any]:
    options = serialize_record_batch_bytes(pa.RecordBatch.from_pylist([{"result_mode": "rows"}]))
    out = client.table_in_out_function(
        function_name="simple_writable_insert",
        schema_path=["main"],
        input=iter([pa.RecordBatch.from_pylist(rows, schema=schema)]),
        arguments=Arguments(positional=[pa.scalar(name)], named={"write_options": pa.scalar(options)}),
    )
    return [row for b in out for row in b.to_pylist()]


def _scan(client: Client, name: str) -> list[dict[str, Any]]:
    out = client.table_function(
        function_name="simple_writable_scan", schema_path=["main"], arguments=Arguments(positional=[pa.scalar(name)])
    )
    return [row for b in out for row in b.to_pylist()]


def test_created_flag_and_if_not_exists(attached: Any) -> None:
    client, attach = attached
    version = client.catalog_version(attach_opaque_data=attach)
    assert _create(client, attach, "made", OnConflict.ERROR) is True
    assert client.catalog_version(attach_opaque_data=attach) > version
    row = {"i": 1, "b": 2, "v": "x", "d": 1.5, "f": True}
    assert _insert(client, "made", [row]) == [row]
    assert _scan(client, "made") == [row]
    # IF NOT EXISTS on an existing table: not created, left exactly as it was.
    assert _create(client, attach, "made", OnConflict.IGNORE, pa.schema([("other", pa.int64())])) is False
    assert _scan(client, "made") == [row]
    assert _create(client, attach, "items", OnConflict.IGNORE) is False
    with pytest.raises(Exception, match="already exists"):
        _create(client, attach, "made", OnConflict.ERROR)
    with pytest.raises(Exception, match="already exists"):
        _create(client, attach, "items", OnConflict.ERROR)
    # REPLACE drops the old table and its rows first.
    assert _create(client, attach, "made", OnConflict.REPLACE, pa.schema([("only", pa.int64())])) is True
    assert _scan(client, "made") == []


def test_created_table_is_listed_insert_only_without_rowid(attached: Any) -> None:
    client, attach = attached
    assert _create(client, attach, "Listed", OnConflict.ERROR) is True
    info = client.table_get(attach_opaque_data=attach, schema_path=["main"], name="listed")
    assert info is not None
    assert info.write_result_modes == {"insert": "rows"}
    columns = pa.ipc.read_schema(pa.py_buffer(info.columns))
    assert columns.names == _COLUMNS.names
    tables = client.schema_contents(
        attach_opaque_data=attach, transaction_opaque_data=None, path=["main"], type=SchemaObjectType.TABLE
    )
    assert "listed" in {t.name for t in tables}


def test_drop_created_table(attached: Any) -> None:
    client, attach = attached
    _create(client, attach, "gone", OnConflict.ERROR)
    client.table_drop(attach_opaque_data=attach, schema_path=["main"], name="gone")
    assert client.table_get(attach_opaque_data=attach, schema_path=["main"], name="gone") is None
    client.table_drop(attach_opaque_data=attach, schema_path=["main"], name="gone", ignore_not_found=True)
    with pytest.raises(Exception, match="does not exist"):
        client.table_drop(attach_opaque_data=attach, schema_path=["main"], name="gone")
    with pytest.raises(Exception, match="cannot be dropped"):
        client.table_drop(attach_opaque_data=attach, schema_path=["main"], name="items")


def test_unsupported_column_type_creates_nothing(attached: Any) -> None:
    client, attach = attached
    with pytest.raises(Exception, match="unsupported Arrow type"):
        _create(client, attach, "bad", OnConflict.ERROR, pa.schema([("t", pa.date32())]))
    assert client.table_get(attach_opaque_data=attach, schema_path=["main"], name="bad") is None


def test_write_functions_are_table_only(attached: Any) -> None:
    client, attach = attached
    infos = client.schema_contents(
        attach_opaque_data=attach, transaction_opaque_data=None, path=["main"], type=SchemaObjectType.TABLE_FUNCTION
    )
    visibility = {info.name: info.visibility for info in infos}
    for name in (
        "simple_writable_insert",
        "simple_writable_update",
        "simple_writable_delete",
        "simple_writable_broken_returning_insert",
    ):
        assert visibility[name] == FUNCTION_VISIBILITY_TABLE_ONLY
    assert visibility["simple_writable_scan"] in (None, "public")
