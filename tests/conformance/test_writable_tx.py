# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Conformance for ``vgi/test/sql/integration/writable_tx/``: the ``txfix`` fixture worker.

Drives ``vgi._test_fixtures.writable_tx`` through the Python ``Client`` over a
subprocess and over HTTP (where the worker seals attach and transaction data
on the wire), checking what the C++ suite checks through DuckDB: ATTACH options
(including a secret) reach every function, ``supports_transactions`` is per
attach, the transaction hooks see the same plaintext id as functions, UPDATE
stages under it and commit applies or refuses it. Also covers the ``changes``
write result mode, which DuckDB never requests.
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
