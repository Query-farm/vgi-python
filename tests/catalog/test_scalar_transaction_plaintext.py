# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Scalar functions see the provider's transaction plaintext over signed HTTP.

The worker opens transaction_opaque_data for the catalog's commit/rollback
callbacks and (since cf1a06e) for table-function params. Scalar
BindParameters still carried the sealed envelope, so a scalar could not tie
its work to the transaction the catalog later commits. Like table functions,
the raw ``bind_call`` keeps the sealed wire value.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest
from vgi_rpc.http._testing import _SyncTestClient

from vgi._test_fixtures.scalar.arithmetic import DoubleFunction
from vgi._test_fixtures.worker import ExampleCatalog, ExampleWorker
from vgi.catalog.catalog_interface import AttachOpaqueData
from vgi.client import Client
from vgi.serve import create_app

_KEY = b"scalar-transaction-plaintext-test-key"


def _client(app: Any, attach: bytes | None = None) -> Client:
    return Client.from_http(
        "http://vgi.test", httpx_client=_SyncTestClient(app), attach_opaque_data=attach, worker_limit=1
    )


def test_scalar_bind_sees_transaction_plaintext(monkeypatch: pytest.MonkeyPatch) -> None:
    """A scalar bound inside a signed-HTTP transaction sees the plaintext the catalog minted."""
    minted: list[bytes] = []
    seen: list[bytes | None] = []
    original_begin = ExampleCatalog.catalog_transaction_begin
    original_bind = DoubleFunction.on_bind.__func__  # type: ignore[attr-defined]

    def begin(self: Any, *, attach_opaque_data: AttachOpaqueData) -> Any:
        tx = original_begin(self, attach_opaque_data=attach_opaque_data)
        assert tx is not None
        minted.append(bytes(tx))
        return tx

    def on_bind(cls: Any, params: Any) -> Any:
        seen.append(params.transaction_opaque_data)
        return original_bind(cls, params)

    monkeypatch.setattr(ExampleCatalog, "catalog_transaction_begin", begin)
    monkeypatch.setattr(DoubleFunction, "on_bind", classmethod(on_bind))
    app = create_app(ExampleWorker, describe=False, signing_key=_KEY)
    with _client(app) as catalog:
        attach = catalog.catalog_attach(name="example", data_version_spec=None, implementation_version=None)
        sealed = catalog.catalog_transaction_begin(attach_opaque_data=AttachOpaqueData(attach.attach_opaque_data))
        assert sealed is not None and minted and sealed != minted[0]
        with _client(app, attach.attach_opaque_data) as worker:
            batches = list(
                worker.scalar_function(
                    function_name="double",
                    schema_path=["main"],
                    input=iter([pa.RecordBatch.from_pydict({"x": pa.array([1, 2], pa.int64())})]),
                    transaction_opaque_data=sealed,
                )
            )
        catalog.catalog_transaction_rollback(
            attach_opaque_data=AttachOpaqueData(attach.attach_opaque_data), transaction_opaque_data=sealed
        )
    assert [b.column(0).to_pylist() for b in batches] == [[2, 4]]
    assert seen == [minted[0]]
