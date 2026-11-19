# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""The ``blob_test`` fixture catalog and the runnable fake blob vendor.

End to end through the subprocess fixture worker with
``VGI_STORAGE_EXTERNALIZE=1`` over SQLite and the fake vendor: the hermetic
setup the DuckDB extension's externalization lane uses.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import httpx2
import pyarrow as pa
import pytest
from vgi_rpc.conformance.fake_vendor import FakeVendorServer
from vgi_rpc.vendor import VendorBlobClient

from vgi._test_fixtures.fake_blob_vendor import serve_fake_blob_vendor
from vgi.arguments import Arguments
from vgi.client import Client

pytest.importorskip("aiohttp", reason="blob downloads need vgi-rpc[external]")

WORKER = "vgi-fixture-worker"


@pytest.fixture
def vendor() -> Iterator[FakeVendorServer]:
    """The fake vendor with its stats route."""
    server = serve_fake_blob_vendor()
    try:
        yield server
    finally:
        server.close()


def _stats(vendor: FakeVendorServer) -> dict[str, int]:
    resp = httpx2.get(vendor.url + "/__stats__", timeout=10.0)
    resp.raise_for_status()
    stats: dict[str, int] = json.loads(resp.content)
    return stats


def test_stats_endpoint_counts_traffic(vendor: FakeVendorServer) -> None:
    """``/__stats__`` counts vendor calls, signed-URL traffic and live bytes."""
    assert _stats(vendor)["puts"] == 0
    client = VendorBlobClient(vendor.url, "test-token")
    ref = client.put_bytes(b"x" * 1000)
    assert client.get_bytes(ref) == b"x" * 1000
    client.delete([ref])
    client.close()
    stats = _stats(vendor)
    assert stats["puts"] == 1 and stats["gets"] == 1 and stats["deletes"] == 1
    assert stats["storage_puts"] == 1 and stats["storage_gets"] >= 1
    assert stats["bytes_uploaded"] == 1000 and stats["bytes_downloaded"] >= 1000
    assert stats["objects"] == 0 and stats["bytes"] == 0


def test_launcher_cli_prints_port_and_url(tmp_path: Path) -> None:
    """``python -m vgi._test_fixtures.fake_blob_vendor`` prints PORT and URL, honours --token."""
    proc = subprocess.Popen(
        [sys.executable, "-m", "vgi._test_fixtures.fake_blob_vendor", "--token", "T=ten"],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert proc.stdout is not None
        port = proc.stdout.readline().strip()
        url = proc.stdout.readline().strip()
        assert port.startswith("PORT:") and url == f"URL:http://127.0.0.1:{port[5:]}"
        base = url[4:]
        assert httpx2.get(base + "/__stats__", timeout=10.0).json()["puts"] == 0
        assert VendorBlobClient(base, "T").put_bytes(b"hi").size == 2
    finally:
        proc.terminate()
        proc.wait(timeout=10)


@pytest.fixture
def externalizing_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, vendor: FakeVendorServer) -> FakeVendorServer:
    """Fixture-worker environment: SQLite storage wrapped by the vendor, 4 KiB threshold."""
    monkeypatch.setenv("VGI_WORKER_SHARED_STORAGE", "sqlite")
    monkeypatch.setenv("VGI_WORKER_SQLITE_PATH", str(tmp_path / "storage.db"))
    monkeypatch.setenv("VGI_STORAGE_EXTERNALIZE", "1")
    monkeypatch.setenv("VGI_STORAGE_EXTERNALIZE_BYTES", "4096")
    monkeypatch.setenv("VGI_BLOB_VENDOR_URL", vendor.url)
    monkeypatch.setenv("VGI_BLOB_VENDOR_TOKEN", "test-token")
    return vendor


def _expected(values: list[str]) -> tuple[list[int], list[str]]:
    return [len(v.encode()) for v in values], [hashlib.sha256(v.encode()).hexdigest() for v in values]


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
    """blob_repeat, blob_len, blob_rows, blob_param_echo and vgi_test_blob_stats."""
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
        stats = list(client.table_function(function_name="vgi_test_blob_stats", schema_path=["main"]))
        stats_row = pa.Table.from_batches([b for b in stats if b.num_rows]).to_pylist()
        assert len(stats_row) == 1 and set(stats_row[0]) >= {"puts", "gets", "bytes", "upload_urls", "deletes"}
