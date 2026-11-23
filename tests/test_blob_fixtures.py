# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""The ``blob_test`` fixture catalog.

End to end through the subprocess fixture worker with
``VGI_STORAGE_EXTERNALIZE=1`` over SQLite and vgi-rpc's fake vendor: the
hermetic setup the DuckDB extension's externalization lane uses (there the
fake runs as ``vgi-rpc-fake-vendor --test-routes`` and SQL reads its
``/__stats__``; here the same counters come from the in-process fake).
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pytest
from vgi_rpc.conformance.fake_vendor import FakeVendorServer, serve_fake_vendor

from vgi._test_fixtures.blob_catalog import CATALOG_NAME, BlobTestWorker
from vgi.arguments import Arguments
from vgi.client import Client

pytest.importorskip("aiohttp", reason="blob downloads need vgi-rpc[external]")

WORKER = "vgi-fixture-worker"
THRESHOLD = 4096


@pytest.fixture
def vendor() -> Iterator[FakeVendorServer]:
    """Run the fake vendor on a background thread."""
    with serve_fake_vendor({"test-token": "tenant-a"}) as server:
        yield server


def _stats(vendor: FakeVendorServer) -> dict[str, int]:
    """Vendor API call counts (the ``/__stats__`` keys that tests here use)."""
    hits = vendor.vendor.stats()
    return {
        "puts": hits.get("/v1/blobs/put", 0),
        "gets": hits.get("/v1/blobs/get", 0),
        "deletes": hits.get("/v1/blobs/delete", 0),
    }


def test_catalog_functions_match_the_contract() -> None:
    """The catalog serves exactly the functions docs/testing/blob-test-catalog.md documents."""
    assert CATALOG_NAME == "blob_test"
    (schema,) = BlobTestWorker.catalog.schemas
    names = sorted(f.Meta.name for f in schema.functions)  # type: ignore[attr-defined]
    assert names == [
        "blob_buffer_echo",
        "blob_len",
        "blob_param_batches",
        "blob_param_echo",
        "blob_param_scalar",
        "blob_param_streams",
        "blob_repeat",
        "blob_rows",
        "blob_store_values",
        "blob_tio",
    ]
    # Protocol 3.4: the catalog table that binds blob_param_echo's twin over RPC.
    (table,) = schema.tables
    assert table.name == "blob_param_table"
    assert not table.inline_bind


@pytest.fixture
def externalizing_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, vendor: FakeVendorServer) -> FakeVendorServer:
    """Fixture-worker environment: SQLite storage wrapped by the vendor, 4 KiB threshold."""
    monkeypatch.setenv("VGI_WORKER_SHARED_STORAGE", "sqlite")
    monkeypatch.setenv("VGI_WORKER_SQLITE_PATH", str(tmp_path / "storage.db"))
    monkeypatch.setenv("VGI_STORAGE_EXTERNALIZE", "1")
    monkeypatch.setenv("VGI_STORAGE_EXTERNALIZE_BYTES", str(THRESHOLD))
    monkeypatch.setenv("VGI_BLOB_VENDOR_URL", vendor.url)
    monkeypatch.setenv("VGI_BLOB_VENDOR_TOKEN", "test-token")
    return vendor


def _expected(values: list[str]) -> tuple[list[int], list[str]]:
    return [len(v.encode()) for v in values], [hashlib.sha256(str(v).encode()).hexdigest() for v in values]


def _input(width: int, n: int) -> list[pa.RecordBatch]:
    return [
        pa.RecordBatch.from_pydict(
            {"s": [chr(97 + (b * 2 + i) % 26) * width for i in range(2)], "k": [b * 2, b * 2 + 1]}
        )
        for b in range(n)
    ]


def test_blob_buffer_echo_externalizes_storage(externalizing_env: FakeVendorServer) -> None:
    """Each buffered batch goes to the vendor (over 4 KiB) and is replayed with checksums."""
    batches = _input(50_000, 3)
    with Client(WORKER, pool=None) as client:
        out = list(
            client.table_buffering_function(function_name="blob_buffer_echo", schema_path=["main"], input=iter(batches))
        )
    rows = sorted(pa.Table.from_batches([b for b in out if b.num_rows]).to_pylist(), key=lambda r: str(r["s"]))
    values: list[str] = sorted(str(v) for b in batches for v in b.column(0).to_pylist())
    lens, digests = _expected(values)
    assert [r["s"] for r in rows] == values
    assert [r["payload_len"] for r in rows] == lens
    assert [r["payload_sha256"] for r in rows] == digests
    stats = _stats(externalizing_env)
    assert stats["puts"] >= 3 and stats["gets"] >= 3


def test_blob_tio_externalizes_storage(externalizing_env: FakeVendorServer) -> None:
    """blob_tio round-trips every batch through externalized storage."""
    batches = _input(20_000, 2)
    with Client(WORKER, pool=None) as client:
        out = list(client.table_in_out_function(function_name="blob_tio", schema_path=["main"], input=iter(batches)))
    rows = pa.Table.from_batches([b for b in out if b.num_rows]).to_pylist()
    values = sorted(str(v) for b in batches for v in b.column(0).to_pylist())
    assert sorted(str(r["s"]) for r in rows) == values
    assert {r["payload_len"] for r in rows} == {20_000}
    stats = _stats(externalizing_env)
    assert stats["puts"] >= 2 and stats["gets"] >= 2 and stats["deletes"] == 0


def test_scalars_and_table_functions(externalizing_env: FakeVendorServer) -> None:
    """blob_repeat, blob_len, blob_rows and blob_param_echo."""
    with Client(WORKER, pool=None) as client:
        rep = list(
            client.scalar_function(
                function_name="blob_repeat",
                schema_path=["main"],
                input=iter([pa.RecordBatch.from_pydict({"s": ["ab", None], "n": [3, 2]})]),
            )
        )
        assert rep[0].column(0).to_pylist() == ["ababab", None]
        lens = list(
            client.scalar_function(
                function_name="blob_len",
                schema_path=["main"],
                input=iter([pa.RecordBatch.from_pydict({"s": ["x" * 100_000, "é"]})]),
            )
        )
        assert lens[0].column(0).to_pylist() == [100_000, 2]
        rows = list(
            client.table_function(
                function_name="blob_rows",
                schema_path=["main"],
                arguments=Arguments(positional=(pa.scalar(3000), pa.scalar(5))),
            )
        )
        table = pa.Table.from_batches([b for b in rows if b.num_rows])
        assert table.num_rows == 3000
        assert table.column("payload")[27].as_py() == "bbbbb"
        payload = "p" * 300_000
        echo = list(
            client.table_function(
                function_name="blob_param_echo",
                schema_path=["main"],
                arguments=Arguments(positional=(pa.scalar(payload),)),
            )
        )
        row = pa.Table.from_batches([b for b in echo if b.num_rows]).to_pylist()
        assert row == [{"payload_len": 300_000, "payload_sha256": hashlib.sha256(payload.encode()).hexdigest()}]


def _store_values(values: list[str | None]) -> list[dict[str, object]]:
    batch = pa.RecordBatch.from_pydict({"s": pa.array(values, pa.string()), "id": list(range(len(values)))})
    with Client(WORKER, pool=None) as client:
        out = list(
            client.table_in_out_function(function_name="blob_store_values", schema_path=["main"], input=iter([batch]))
        )
    return sorted(pa.Table.from_batches([b for b in out if b.num_rows]).to_pylist(), key=lambda r: int(r["id"]))


def test_blob_store_values_threshold_boundary(externalizing_env: FakeVendorServer) -> None:
    """Values one below, at and one above the threshold round-trip; only the last two are vendored."""
    values: list[str | None] = ["a" * (THRESHOLD - 1), "b" * THRESHOLD, "c" * (THRESHOLD + 1)]
    rows = _store_values(values)
    assert list(rows[0]) == ["id", "payload_len", "payload_sha256"]
    assert [r["payload_len"] for r in rows] == [len(str(v)) for v in values]
    assert [r["payload_sha256"] for r in rows] == [hashlib.sha256(str(v).encode()).hexdigest() for v in values]
    assert _stats(externalizing_env)["puts"] == 2


def test_blob_store_values_empty_and_null(externalizing_env: FakeVendorServer) -> None:
    """An empty value round-trips as empty; NULL stores nothing and reports NULLs."""
    rows = _store_values(["", None, "é"])
    assert [(r["payload_len"], r["payload_sha256"]) for r in rows] == [
        (0, hashlib.sha256(b"").hexdigest()),
        (None, None),
        (2, hashlib.sha256("é".encode()).hexdigest()),
    ]
    assert _stats(externalizing_env)["puts"] == 0
