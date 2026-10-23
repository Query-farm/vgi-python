# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D102, D103, D106, D107
"""Signed HTTP token plumbing through the actual writable fixture handlers.

The embedded DuckDB transactor needs a private subcursor-capable build. This
test substitutes a small real SQLite transaction service at that transport
boundary; it does not qualify the unavailable DuckDB backend. Worker sealing,
public Client calls, HTTP continuation, generic write binding/processing and
catalog transaction callbacks remain the actual implementations.
"""

from __future__ import annotations

import sqlite3
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from vgi_rpc import AnnotatedBatch
from vgi_rpc.http import make_wsgi_app
from vgi_rpc.http._testing import _SyncTestClient
from vgi_rpc.utils import serialize_record_batch_bytes

from vgi._test_fixtures.writable.generic import GenericTableInsert
from vgi._test_fixtures.writable.table import transactor_proxy
from vgi._test_fixtures.writable.worker import WritableWorker
from vgi.arguments import Arguments
from vgi.client import Client
from vgi.client.catalog_mixin import CatalogClientError
from vgi.client.errors import ClientError
from vgi.invocation import GlobalInitResponse
from vgi.meta_worker import MetaWorker
from vgi.protocol import VgiCallState
from vgi.rpc_server import build_rpc_server
from vgi.serve import create_app

_KEY = b"writable-signed-transaction-test-key"
_ROWS = pa.schema([("id", pa.int64()), ("name", pa.string())])
_COUNT = pa.schema([pa.field("count", pa.int64(), nullable=False)])


class SQLiteTransactorService:
    """Real transactional storage, limited to the transactor calls under test."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.paths: dict[bytes, Path] = {}
        self.transactions: dict[bytes, tuple[bytes, sqlite3.Connection]] = {}
        self.calls: list[tuple[str, bytes, bytes]] = []

    def register(self, *, attach_opaque_data: bytes, **kwargs: Any) -> None:
        del kwargs
        path = self.root / f"{attach_opaque_data.hex()}.sqlite"
        with sqlite3.connect(path) as db:
            db.execute("CREATE TABLE items (id INTEGER, name TEXT)")
        self.paths[attach_opaque_data] = path

    def begin(self, *, attach_opaque_data: bytes) -> bytes:
        token = uuid.uuid4().bytes
        db = sqlite3.connect(self.paths[attach_opaque_data], isolation_level=None, check_same_thread=False)
        db.execute("BEGIN")
        self.transactions[token] = (attach_opaque_data, db)
        self.calls.append(("begin", attach_opaque_data, token))
        return token

    def _connection(self, attach: bytes, token: bytes) -> sqlite3.Connection:
        if token not in self.transactions:
            raise ValueError("test transaction service received an unknown plaintext transaction token")
        owner, db = self.transactions[token]
        if owner != attach:
            raise ValueError("test transaction service received a transaction for another attachment")
        return db

    def commit(self, *, attach_opaque_data: bytes, tx_id: bytes) -> None:
        db = self._connection(attach_opaque_data, tx_id)
        db.commit()
        db.close()
        del self.transactions[tx_id]
        self.calls.append(("commit", attach_opaque_data, tx_id))

    def rollback(self, *, attach_opaque_data: bytes, tx_id: bytes) -> None:
        db = self._connection(attach_opaque_data, tx_id)
        db.rollback()
        db.close()
        del self.transactions[tx_id]
        self.calls.append(("rollback", attach_opaque_data, tx_id))

    def catalog_version(self, **kwargs: Any) -> int:
        del kwargs
        return 1

    def table_schema(self, *, attach_opaque_data: bytes, tx_id: bytes, table_name: str) -> bytes:
        assert table_name == "items"
        self._connection(attach_opaque_data, tx_id)
        self.calls.append(("table_schema", attach_opaque_data, tx_id))
        return _ROWS.serialize().to_pybytes()

    @contextmanager
    def insert(self, *, attach_opaque_data: bytes, tx_id: bytes, table_name: str, result_mode: str) -> Iterator[Any]:
        assert table_name == "items"
        db = self._connection(attach_opaque_data, tx_id)
        service = self

        class Exchange:
            def exchange(self, input: AnnotatedBatch) -> AnnotatedBatch:
                rows = input.batch.to_pylist()
                db.executemany("INSERT INTO items VALUES (?, ?)", [(row["id"], row["name"]) for row in rows])
                service.calls.append(("insert", attach_opaque_data, tx_id))
                result = (
                    input.batch
                    if result_mode == "rows"
                    else pa.RecordBatch.from_pydict({"count": [len(rows)]}, schema=_COUNT)
                )
                return AnnotatedBatch(batch=result)

        yield Exchange()

    def committed_rows(self, attach: bytes) -> list[tuple[int, str]]:
        with sqlite3.connect(self.paths[attach]) as db:
            return db.execute("SELECT id, name FROM items ORDER BY id").fetchall()

    def close(self) -> None:
        for _, db in self.transactions.values():
            db.close()


@pytest.fixture
def service(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[SQLiteTransactorService]:
    backend = SQLiteTransactorService(tmp_path)
    monkeypatch.setattr(transactor_proxy, "_get_proxy", lambda: backend)
    yield backend
    backend.close()


def _client(app: Any, attach: bytes | None = None) -> Client:
    return Client.from_http(
        "http://vgi.test", httpx_client=_SyncTestClient(app), attach_opaque_data=attach, worker_limit=1
    )


def _attach(client: Client) -> bytes:
    result = client.catalog_attach(name="writable", data_version_spec=None, implementation_version=None)
    assert result.attach_opaque_data is not None
    return result.attach_opaque_data


def _write(client: Client, tx: bytes, mode: str, start: int = 1, *, has_finalize: bool = False) -> list[dict[str, Any]]:
    options = pa.RecordBatch.from_pydict({"result_mode": [mode]})
    batches = [
        pa.RecordBatch.from_pylist([{"id": value, "name": f"row-{value}"}], schema=_ROWS)
        for value in (start, start + 1)
    ]
    result = client.table_in_out_function(
        function_name="generic_writable_insert",
        schema_path=["main"],
        input=iter(batches),
        arguments=Arguments(
            positional=(pa.scalar("items"),),
            named={"write_options": pa.scalar(serialize_record_batch_bytes(options))},
        ),
        transaction_opaque_data=tx,
        has_finalize=has_finalize,
    )
    return [row for batch in result for row in batch.to_pylist()]


@pytest.mark.parametrize("mode", ["count", "rows"])
def test_signed_http_write_commit_and_rollback(service: SQLiteTransactorService, mode: str) -> None:
    app = create_app(WritableWorker, describe=False, signing_key=_KEY)
    with _client(app) as catalog:
        sealed_attach = _attach(catalog)
        owner = next(iter(service.paths))
        with _client(app, sealed_attach) as writer:
            for operation, start in [("commit", 1), ("rollback", 3)]:
                sealed_tx = catalog.catalog_transaction_begin(attach_opaque_data=sealed_attach)
                assert sealed_tx is not None
                plaintext = service.calls[-1][2]
                assert sealed_tx != plaintext
                rows = _write(writer, sealed_tx, mode, start)
                expected = (
                    [{"count": 1}, {"count": 1}]
                    if mode == "count"
                    else [{"id": value, "name": f"row-{value}"} for value in (start, start + 1)]
                )
                assert rows == expected
                assert service.committed_rows(owner) == ([] if operation == "commit" else [(1, "row-1"), (2, "row-2")])
                getattr(catalog, f"catalog_transaction_{operation}")(
                    attach_opaque_data=sealed_attach, transaction_opaque_data=sealed_tx
                )
                assert (operation, owner, plaintext) in service.calls
                assert service.calls.count(("insert", owner, plaintext)) == 2
                assert not any(token == sealed_tx for _, _, token in service.calls)
        assert service.committed_rows(owner) == [(1, "row-1"), (2, "row-2")]
        assert not service.transactions


@pytest.mark.parametrize("mode", ["count", "rows"])
def test_signed_http_wrong_attachment_rejected_before_service(service: SQLiteTransactorService, mode: str) -> None:
    app = create_app(WritableWorker, describe=False, signing_key=_KEY)
    with _client(app) as catalog:
        owner = _attach(catalog)
        other = _attach(catalog)
        tx = catalog.catalog_transaction_begin(attach_opaque_data=owner)
        assert tx is not None
        before = list(service.calls)
        with _client(app, other) as writer, pytest.raises(ClientError, match="transaction_opaque_data"):
            _write(writer, tx, mode)
        assert service.calls == before
        for attach in service.paths:
            assert service.committed_rows(attach) == []
        for operation in (catalog.catalog_transaction_commit, catalog.catalog_transaction_rollback):
            with pytest.raises(CatalogClientError, match="transaction_opaque_data"):
                operation(attach_opaque_data=other, transaction_opaque_data=tx)
        catalog.catalog_transaction_rollback(attach_opaque_data=owner, transaction_opaque_data=tx)
        assert not service.transactions


@pytest.mark.parametrize("use_meta_worker", [False, True])
def test_signed_http_callback_token_survives_init_continuation_and_finalize(
    service: SQLiteTransactorService, monkeypatch: pytest.MonkeyPatch, use_meta_worker: bool
) -> None:
    """All callbacks see provider bytes, while serialized call state keeps the seal."""
    seen: list[tuple[str, bytes | None]] = []
    original_bind = GenericTableInsert.on_bind
    original_process = GenericTableInsert.process
    plaintext: bytes | None = None
    sealed: bytes | None = None

    def check(phase: str, params: Any) -> None:
        seen.append((phase, params.transaction_opaque_data))
        bind = params.bind_call if phase == "bind" else params.init_call.bind_call
        assert bind.transaction_opaque_data == sealed
        assert params.transaction_opaque_data == plaintext
        if phase in {"process", "finalize"}:
            state = VgiCallState(init_call=params.init_call, init_response=params.init_response)
            raw = state.serialize_to_bytes()
            assert plaintext not in raw
            assert sealed in raw

    def on_bind(cls: Any, params: Any) -> Any:
        check("bind", params)
        return original_bind(params)

    def on_init(cls: Any, params: Any) -> GlobalInitResponse:
        check("init", params)
        return GlobalInitResponse()

    def process(cls: Any, params: Any, state: None, batch: pa.RecordBatch, out: Any) -> None:
        check("process", params)
        original_process(params, state, batch, out)

    def finalize(cls: Any, params: Any) -> list[pa.RecordBatch]:
        check("finalize", params)
        return []

    monkeypatch.setattr(GenericTableInsert, "on_bind", classmethod(on_bind))
    monkeypatch.setattr(GenericTableInsert, "on_init", classmethod(on_init))
    monkeypatch.setattr(GenericTableInsert, "process", classmethod(process))
    monkeypatch.setattr(GenericTableInsert, "finalize", classmethod(finalize))
    if use_meta_worker:
        meta = MetaWorker([WritableWorker(quiet=True)])
        meta._signing_key = _KEY
        app = make_wsgi_app(build_rpc_server(meta, transport="http", describe=False), token_key=_KEY)
    else:
        app = create_app(WritableWorker, describe=False, signing_key=_KEY)
    with _client(app) as catalog:
        attach = _attach(catalog)
        sealed = catalog.catalog_transaction_begin(attach_opaque_data=attach)
        assert sealed is not None
        plaintext = service.calls[-1][2]
        with _client(app, attach) as writer:
            assert _write(writer, sealed, "count", has_finalize=True) == [{"count": 1}, {"count": 1}]
        catalog.catalog_transaction_rollback(attach_opaque_data=attach, transaction_opaque_data=sealed)
    phases = [phase for phase, _ in seen]
    assert phases.count("bind") >= 1
    assert phases.count("init") >= 1
    assert phases.count("process") == 2
    assert phases.count("finalize") == 1
    assert all(token == plaintext for _, token in seen)
