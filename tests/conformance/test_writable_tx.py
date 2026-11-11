# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Conformance for ``vgi/test/sql/integration/writable_tx/``: the ``txfix`` fixture worker.

Drives ``vgi._test_fixtures.writable_tx`` through the Python ``Client`` over a
subprocess and over HTTP (where the worker seals attach and transaction data
on the wire), checking what the C++ suite checks through DuckDB: ATTACH options
(including a secret) reach every function, ``supports_transactions`` is per
attach, the transaction hooks see the same plaintext id as functions, UPDATE
stages under it and commit applies or refuses it. Also covers the ``changes``
write result mode, which DuckDB never requests, and (protocol 3.3 round 2) the
insert and delete functions, ``writes()`` and finalize, and staged inserts/deletes.
"""

from __future__ import annotations

import os
import subprocess
import sys
import threading
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pyarrow as pa
import pytest
from vgi_rpc.utils import serialize_record_batch_bytes

from tests._http_fixtures import wait_for_http_server
from vgi.arguments import Arguments
from vgi.client.client import Client

pytest.importorskip("vgi._test_fixtures.writable_tx")

_WORKER = [sys.executable, "-m", "vgi._test_fixtures.writable_tx"]


@contextmanager
def _http_server(env: dict[str, str]) -> Iterator[str]:
    proc = subprocess.Popen(
        [*_WORKER, "--http", "--port", "0"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        assert proc.stdout is not None
        port = None
        for line in proc.stdout:
            if line.startswith("PORT:"):
                port = int(line.strip().split(":", 1)[1])
                break
        assert port is not None, "txfix worker never reported its port"
        # Keep draining so a chatty server never blocks on a full pipe.
        threading.Thread(target=lambda: [None for _ in proc.stdout or []], daemon=True).start()
        url = f"http://127.0.0.1:{port}"
        wait_for_http_server(url)
        yield url
    finally:
        proc.terminate()
        proc.wait(timeout=10)


@pytest.fixture
def token() -> str:
    """A token of this test's own: the fixture partitions its state by token.

    The state directory stays at its default: the client's catalog RPCs reuse
    pooled worker processes across tests, so a per-test
    ``VGI_TXFIX_STATE_DIR`` would not reach all of them.
    """
    return f"tok-{uuid.uuid4().hex}"


@pytest.fixture(params=["subprocess", "http"])
def connect(request: pytest.FixtureRequest) -> Iterator[Any]:
    """Yield ``connect(attach_opaque_data=None) -> Client`` for one transport."""
    if request.param == "subprocess":
        command = " ".join(_WORKER)
        yield lambda attach=None: Client(command, pool=None, attach_opaque_data=attach)
        return
    pytest.importorskip("vgi_rpc.http")
    with _http_server(dict(os.environ)) as url:
        yield lambda attach=None: Client.from_http(url, attach_opaque_data=attach)


def _attach(connect: Any, **options: Any) -> tuple[Any, Any]:
    with connect() as client:
        result = client.catalog_attach(
            name="txfix", options=options, data_version_spec=None, implementation_version=None
        )
    return result, connect(result.attach_opaque_data)


def _table(client: Client, name: str, tx: bytes | None = None) -> list[dict[str, Any]]:
    batches = list(client.table_function(function_name=name, schema_path=["main"], transaction_opaque_data=tx))
    return [row for b in batches for row in b.to_pylist()]


def _update(client: Client, rows: list[dict[str, Any]], mode: str, tx: bytes | None = None) -> list[dict[str, Any]]:
    options = serialize_record_batch_bytes(pa.RecordBatch.from_pylist([{"result_mode": mode}]))
    batch = pa.RecordBatch.from_pylist(rows, schema=pa.schema([("qty", pa.int64()), ("rowid", pa.int64())]))
    out = client.table_in_out_function(
        function_name="items_update",
        schema_path=["main"],
        input=iter([batch]),
        arguments=Arguments(named={"write_options": pa.scalar(options)}),
        transaction_opaque_data=tx,
    )
    return [row for b in out for row in b.to_pylist()]


def test_attach_options_and_per_attach_transactions(connect: Any, token: str) -> None:
    """Attach options and per attach transactions."""
    plain, client = _attach(connect, token=token)
    assert plain.supports_transactions is False
    assert plain.attach_opaque_data_required is True
    with client:
        assert _table(client, "whoami") == [{"token": token, "transactional": False, "token_in_attach_data": False}]
        assert _table(client, "reset") == [{"ok": True}]
    tx_result, tx_client = _attach(connect, token=token, transactional=True)
    assert tx_result.supports_transactions is True
    with tx_client:
        assert _table(tx_client, "whoami") == [{"token": token, "transactional": True, "token_in_attach_data": False}]
    anonymous, anon_client = _attach(connect)
    with anon_client:
        assert _table(anon_client, "whoami") == [{"token": None, "transactional": False, "token_in_attach_data": False}]


def test_update_modes_on_a_plain_attach(connect: Any, token: str) -> None:
    """Update modes on a plain attach."""
    _, client = _attach(connect, token=token)
    with client:
        _table(client, "reset")
        assert _update(client, [{"qty": 11, "rowid": 1}], "count") == [{"count": 1}]
        assert _update(client, [{"qty": 12, "rowid": 1}], "rows") == [{"id": 1, "name": "a", "qty": 12}]
        assert _update(client, [{"qty": 13, "rowid": 1}], "changes") == [
            {"old": {"id": 1, "name": "a", "qty": 12}, "new": {"id": 1, "name": "a", "qty": 13}}
        ]
        with pytest.raises(Exception, match="qty must be >= 0"):
            _update(client, [{"qty": -1, "rowid": 2}], "count")
        assert [r["qty"] for r in _table(client, "items_scan")] == [13, 20, 30]


def test_transaction_hooks_see_the_function_plaintext(connect: Any, token: str) -> None:
    """Transaction hooks see the function plaintext."""
    _, plain = _attach(connect, token=token)
    _, client = _attach(connect, token=token, transactional=True)
    attach = client._attach_opaque_data
    with plain, client:
        _table(plain, "reset")
        # Commit: staged, read-your-writes, then visible to the other attach.
        tx = client.catalog_transaction_begin(attach_opaque_data=attach)
        assert tx is not None
        (current,) = _table(client, "current_tx", tx)
        assert current["in_tx"] is True and len(current["tx_id"]) == 32
        assert _update(client, [{"qty": 99, "rowid": 2}], "rows", tx) == [{"id": 2, "name": "b", "qty": 99}]
        assert [r["qty"] for r in _table(client, "items_scan", tx)] == [10, 99, 30]
        assert [r["qty"] for r in _table(plain, "items_scan")] == [10, 20, 30]
        client.catalog_transaction_commit(attach_opaque_data=attach, transaction_opaque_data=tx)
        assert [r["qty"] for r in _table(plain, "items_scan")] == [10, 99, 30]

        # Refused commit: nothing applies.
        tx2 = client.catalog_transaction_begin(attach_opaque_data=attach)
        assert tx2 is not None
        _update(client, [{"qty": -5, "rowid": 3}], "count", tx2)
        with pytest.raises(Exception, match="commit refused: qty -5 for id 3"):
            client.catalog_transaction_commit(attach_opaque_data=attach, transaction_opaque_data=tx2)
        assert [r["qty"] for r in _table(plain, "items_scan")] == [10, 99, 30]

        # Rollback.
        tx3 = client.catalog_transaction_begin(attach_opaque_data=attach)
        assert tx3 is not None
        _update(client, [{"qty": 1, "rowid": 1}], "count", tx3)
        client.catalog_transaction_rollback(attach_opaque_data=attach, transaction_opaque_data=tx3)
        assert [r["qty"] for r in _table(plain, "items_scan")] == [10, 99, 30]

        events = _table(client, "events")
        assert [(e["kind"], e["ok"]) for e in events] == [
            ("begin", True),
            ("commit", True),
            ("begin", True),
            ("commit", False),
            ("begin", True),
            ("rollback", True),
        ]
        # The hooks saw the plaintext the function saw, not the (sealed, on HTTP) wire bytes.
        assert events[0]["tx_id"] == events[1]["tx_id"] == current["tx_id"]
        assert len({events[0]["tx_id"], events[2]["tx_id"], events[4]["tx_id"]}) == 3


def test_scan_function_listing(connect: Any) -> None:
    """Mirror of ``writable_tx/scan_function_listing.test`` at the protocol level (3.3).

    items_scan and items_update are listed (DuckDB needs their capabilities)
    but table_only; labels_scan is exposed. Every one stays callable by name
    through the Python client.
    """
    from vgi.catalog import FUNCTION_VISIBILITY_TABLE_ONLY, SchemaObjectType

    with connect() as client:
        result = client.catalog_attach(name="txfix", options={}, data_version_spec=None, implementation_version=None)
    with connect(result.attach_opaque_data) as client:
        infos = client.schema_contents(
            attach_opaque_data=result.attach_opaque_data,
            transaction_opaque_data=None,
            path=["main"],
            type=SchemaObjectType.TABLE_FUNCTION,
        )
        visibility = {info.name: info.visibility for info in infos}
        assert visibility["items_scan"] == FUNCTION_VISIBILITY_TABLE_ONLY
        assert visibility["items_update"] == FUNCTION_VISIBILITY_TABLE_ONLY
        assert visibility["items_insert"] == FUNCTION_VISIBILITY_TABLE_ONLY
        assert visibility["items_delete"] == FUNCTION_VISIBILITY_TABLE_ONLY
        assert visibility["writes"] in (None, "public")
        for name in ("items_insert", "items_update", "items_delete"):
            assert next(info for info in infos if info.name == name).has_finalize is True
        assert visibility["labels_scan"] in (None, "public")
        items_scan = next(info for info in infos if info.name == "items_scan")
        assert items_scan.projection_pushdown is True
        assert isinstance(_table(client, "labels_scan"), list)
        assert isinstance(_table(client, "items_scan"), list)


_ITEMS_IN = pa.schema([("id", pa.int32()), ("name", pa.string()), ("qty", pa.int64())])
_ROWID_IN = pa.schema([("rowid", pa.int64())])


def _write(
    client: Client,
    function: str,
    batches: list[list[dict[str, Any]]],
    schema: pa.Schema,
    mode: str,
    tx: bytes | None = None,
) -> list[dict[str, Any]]:
    """One write call: every batch through ``function`` (init, process each batch, finalize)."""
    options = serialize_record_batch_bytes(pa.RecordBatch.from_pylist([{"result_mode": mode}]))
    out = client.table_in_out_function(
        function_name=function,
        schema_path=["main"],
        input=iter([pa.RecordBatch.from_pylist(rows, schema=schema) for rows in batches]),
        arguments=Arguments(named={"write_options": pa.scalar(options)}),
        transaction_opaque_data=tx,
    )
    return [row for b in out for row in b.to_pylist()]


def _ids(client: Client, tx: bytes | None = None) -> list[int]:
    return [r["id"] for r in _table(client, "items_scan", tx)]


def test_insert_validates_each_batch_before_applying(connect: Any, token: str) -> None:
    """INSERT: modes, NOT NULL/duplicate/qty checks, whole-batch validation, writes() and finalize."""
    _, client = _attach(connect, token=token)
    with client:
        _table(client, "reset")
        assert _write(client, "items_insert", [[{"id": 4, "name": "d", "qty": 0}]], _ITEMS_IN, "count") == [
            {"count": 1}
        ]
        assert _write(client, "items_insert", [[{"id": 5, "name": "e", "qty": None}]], _ITEMS_IN, "rows") == [
            {"id": 5, "name": "e", "qty": None}
        ]
        assert _write(client, "items_insert", [[{"id": 6, "name": "f", "qty": 6}]], _ITEMS_IN, "changes") == [
            {"old": None, "new": {"id": 6, "name": "f", "qty": 6}}
        ]
        for rows, message in [
            ([{"id": 7, "name": "g", "qty": 1}, {"id": 1, "name": "dup", "qty": 1}], "txfix: duplicate id 1"),
            ([{"id": 7, "name": "g", "qty": 1}, {"id": 7, "name": "g2", "qty": 1}], "txfix: duplicate id 7"),
            ([{"id": 7, "name": None, "qty": 1}], "txfix: name required"),
            ([{"id": None, "name": "g", "qty": 1}], "txfix: id required"),
            ([{"id": 7, "name": "g", "qty": -1}], "txfix: qty must be >= 0"),
        ]:
            with pytest.raises(Exception, match=message):
                _write(client, "items_insert", [rows], _ITEMS_IN, "count")
        # A failing batch applied nothing (id 7 is not there).
        assert _ids(client) == [1, 2, 3, 4, 5, 6]
        # A later failing batch leaves the earlier batches of the call applied (plain attach).
        with pytest.raises(Exception, match="txfix: duplicate id 8"):
            _write(
                client,
                "items_insert",
                [[{"id": 8, "name": "h", "qty": 1}], [{"id": 8, "name": "h", "qty": 1}]],
                _ITEMS_IN,
                "count",
            )
        assert _ids(client) == [1, 2, 3, 4, 5, 6, 8]
        writes = _table(client, "writes")
        finalized = [(w["op"], w["rows"]) for w in writes if w["finalized"]]
        assert finalized == [("insert", 1), ("insert", 1), ("insert", 1)]
        assert all(w["tx_id"] is None for w in writes)
        assert [w["seq"] for w in writes] == sorted(w["seq"] for w in writes)
        _table(client, "reset")
        assert _table(client, "writes") == []


def test_delete_counts_each_visible_rowid_once(connect: Any, token: str) -> None:
    """DELETE: rowid = id, missing ids are no-ops, a repeated rowid counts once, rows mode returns old rows."""
    _, client = _attach(connect, token=token)
    with client:
        _table(client, "reset")
        assert _write(client, "items_delete", [[{"rowid": 2}, {"rowid": 2}, {"rowid": 99}]], _ROWID_IN, "count") == [
            {"count": 1}
        ]
        assert _write(client, "items_delete", [[{"rowid": 3}], [{"rowid": 3}]], _ROWID_IN, "rows") == [
            {"id": 3, "name": "c", "qty": 30}
        ]
        assert _ids(client) == [1]
        assert [(w["op"], w["rows"], w["finalized"]) for w in _table(client, "writes")] == [
            ("delete", 1, True),
            ("delete", 1, True),
        ]
        # A deleted id can be inserted again.
        _write(client, "items_insert", [[{"id": 2, "name": "b2", "qty": 2}]], _ITEMS_IN, "count")
        assert _ids(client) == [1, 2]


def test_transaction_stages_inserts_and_deletes(connect: Any, token: str) -> None:
    """ttx: staged INSERT/DELETE, read-your-writes, edits of staged inserts, commit/refusal/rollback."""
    _, plain = _attach(connect, token=token)
    _, client = _attach(connect, token=token, transactional=True)
    attach = client._attach_opaque_data
    with plain, client:
        _table(plain, "reset")
        tx = client.catalog_transaction_begin(attach_opaque_data=attach)
        assert tx is not None
        _write(
            client,
            "items_insert",
            [[{"id": 4, "name": "d", "qty": 4}, {"id": 5, "name": "e", "qty": 5}]],
            _ITEMS_IN,
            "count",
            tx,
        )
        _write(client, "items_delete", [[{"rowid": 1}, {"rowid": 5}]], _ROWID_IN, "count", tx)
        assert _write(
            client,
            "items_update",
            [[{"qty": 44, "rowid": 4}]],
            pa.schema([("qty", pa.int64()), ("rowid", pa.int64())]),
            "rows",
            tx,
        ) == [{"id": 4, "name": "d", "qty": 44}]
        with pytest.raises(Exception, match="txfix: duplicate id 4"):
            _write(client, "items_insert", [[{"id": 4, "name": "x", "qty": 1}]], _ITEMS_IN, "count", tx)
        # Deleted in this transaction, so it may be inserted again.
        _write(client, "items_insert", [[{"id": 1, "name": "a2", "qty": 1}]], _ITEMS_IN, "count", tx)
        assert [(r["id"], r["name"], r["qty"]) for r in _table(client, "items_scan", tx)] == [
            (1, "a2", 1),
            (2, "b", 20),
            (3, "c", 30),
            (4, "d", 44),
        ]
        assert _ids(plain) == [1, 2, 3]
        client.catalog_transaction_commit(attach_opaque_data=attach, transaction_opaque_data=tx)
        assert [(r["id"], r["name"], r["qty"]) for r in _table(plain, "items_scan")] == [
            (1, "a2", 1),
            (2, "b", 20),
            (3, "c", 30),
            (4, "d", 44),
        ]
        # writes() carries the plaintext id the functions saw (the wire bytes are sealed on HTTP).
        begin = next(e for e in _table(client, "events") if e["kind"] == "begin")
        assert {w["tx_id"] for w in _table(client, "writes")} == {begin["tx_id"]}

        # An inserted negative qty is accepted at write time and refuses the commit.
        tx2 = client.catalog_transaction_begin(attach_opaque_data=attach)
        assert tx2 is not None
        _write(client, "items_insert", [[{"id": 9, "name": "i", "qty": -7}]], _ITEMS_IN, "count", tx2)
        _write(client, "items_delete", [[{"rowid": 2}]], _ROWID_IN, "count", tx2)
        with pytest.raises(Exception, match="commit refused: qty -7 for id 9"):
            client.catalog_transaction_commit(attach_opaque_data=attach, transaction_opaque_data=tx2)
        assert _ids(plain) == [1, 2, 3, 4]

        # Rollback discards staged inserts and deletes.
        tx3 = client.catalog_transaction_begin(attach_opaque_data=attach)
        assert tx3 is not None
        _write(client, "items_insert", [[{"id": 10, "name": "j", "qty": 1}]], _ITEMS_IN, "count", tx3)
        _write(client, "items_delete", [[{"rowid": 3}]], _ROWID_IN, "count", tx3)
        assert _ids(client, tx3) == [1, 2, 4, 10]
        client.catalog_transaction_rollback(attach_opaque_data=attach, transaction_opaque_data=tx3)
        assert _ids(plain) == [1, 2, 3, 4]


def test_items_qty_default_is_served(connect: Any) -> None:
    """items.qty carries the constant default 0 through the public column-default API (``Table.defaults``)."""
    with connect() as client:
        result = client.catalog_attach(name="txfix", options={}, data_version_spec=None, implementation_version=None)
    with connect(result.attach_opaque_data) as client:
        info = client.table_get(attach_opaque_data=result.attach_opaque_data, schema_path=["main"], name="items")
        assert info is not None
        schema = pa.ipc.read_schema(pa.py_buffer(info.columns))
        # Table.defaults serves the SQL text under the ``default`` field-metadata key,
        # which the extension binds as the column default.
        assert (schema.field("qty").metadata or {}).get(b"default") == b"0"
        assert info.write_result_modes == {"insert": "changes", "update": "changes", "delete": "changes"}
