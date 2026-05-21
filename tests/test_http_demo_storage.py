# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Tests for in-process demo blob storage and external batch offloading."""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager

import httpx
import pyarrow as pa
import pytest

from vgi.arguments import Arguments
from vgi.http.demo_storage import DemoBlobStorage, localhost_only_validator
from vgi.invocation import FunctionType
from vgi.protocol import BindRequest, InitRequest, VgiProtocol

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _free_port() -> int:
    """Allocate an available localhost TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@contextmanager
def _run_demo_http_server(
    *,
    port: int,
    threshold_bytes: int,
    compression: str = "none",
) -> Iterator[None]:
    """Run vgi._test_fixtures.http_server with --demo-storage in a subprocess."""
    cmd = [
        sys.executable,
        "-m",
        "vgi._test_fixtures.http_server",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--demo-storage",
        "--externalize-threshold-bytes",
        str(threshold_bytes),
        "--max-upload-bytes",
        str(threshold_bytes),
        "--externalize-compression",
        compression,
    ]
    env = os.environ.copy()
    proc = subprocess.Popen(
        cmd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        yield
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)

        if proc.returncode not in (0, -15):
            stderr = proc.stderr.read() if proc.stderr is not None else ""
            raise RuntimeError(f"example HTTP worker exited with code {proc.returncode}: {stderr}")


def _wait_for_http_server(base_url: str) -> None:
    """Wait until the HTTP server responds to capabilities requests."""
    from vgi_rpc.http import http_capabilities

    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            http_capabilities(base_url=base_url)
            return
        except Exception:
            time.sleep(0.25)
    raise TimeoutError(f"Timed out waiting for HTTP server at {base_url}")


def _batch_to_ipc_bytes(batch: pa.RecordBatch) -> bytes:
    """Serialize a single batch as Arrow IPC stream bytes."""
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    return sink.getvalue().to_pybytes()


# ---------------------------------------------------------------------------
# Unit tests — DemoBlobStorage class (no subprocess)
# ---------------------------------------------------------------------------


class TestDemoBlobStorageUnit:
    """Unit tests for DemoBlobStorage (no subprocess)."""

    def test_upload_and_get(self) -> None:
        """Upload bytes and retrieve them by key."""
        storage = DemoBlobStorage()
        storage.set_base_url("http://127.0.0.1:9999")
        url = storage.upload(b"hello", pa.schema([]))
        assert "/__blobs__/" in url
        assert url.endswith(".arrow")

        key = url.rsplit("/", 1)[1]
        entry = storage.get(key)
        assert entry is not None
        data, encoding = entry
        assert data == b"hello"
        assert encoding is None

    def test_upload_with_zstd_encoding(self) -> None:
        """Upload with content_encoding='zstd' stores encoding and uses .arrow.zst extension."""
        storage = DemoBlobStorage()
        storage.set_base_url("http://127.0.0.1:9999")
        url = storage.upload(b"compressed", pa.schema([]), content_encoding="zstd")
        assert url.endswith(".arrow.zst")

        key = url.rsplit("/", 1)[1]
        entry = storage.get(key)
        assert entry is not None
        data, encoding = entry
        assert data == b"compressed"
        assert encoding == "zstd"

    def test_upload_with_gzip_encoding(self) -> None:
        """Upload with content_encoding='gzip' stores encoding and uses .arrow.gz extension."""
        storage = DemoBlobStorage()
        storage.set_base_url("http://127.0.0.1:9999")
        url = storage.upload(b"compressed", pa.schema([]), content_encoding="gzip")
        assert url.endswith(".arrow.gz")

        key = url.rsplit("/", 1)[1]
        entry = storage.get(key)
        assert entry is not None
        data, encoding = entry
        assert data == b"compressed"
        assert encoding == "gzip"

    def test_eviction(self) -> None:
        """Exceeding max_blobs evicts oldest entries."""
        storage = DemoBlobStorage(max_blobs=3)
        storage.set_base_url("http://127.0.0.1:9999")

        urls = []
        for i in range(5):
            urls.append(storage.upload(str(i).encode(), pa.schema([])))

        # First two should have been evicted.
        for url in urls[:2]:
            key = url.rsplit("/", 1)[1]
            assert storage.get(key) is None

        # Last three should still be present.
        for url in urls[2:]:
            key = url.rsplit("/", 1)[1]
            assert storage.get(key) is not None

    def test_generate_upload_url(self) -> None:
        """generate_upload_url returns matching PUT/GET URLs with .arrow extension."""
        storage = DemoBlobStorage()
        storage.set_base_url("http://127.0.0.1:9999")
        upload_url = storage.generate_upload_url(pa.schema([]))

        assert "/__blobs__/" in upload_url.upload_url
        assert upload_url.upload_url == upload_url.download_url
        assert upload_url.upload_url.endswith(".arrow")
        assert upload_url.expires_at is not None

    def test_put_and_get(self) -> None:
        """Direct put/get stores and retrieves blob data with encoding."""
        storage = DemoBlobStorage()
        storage.put("test-key.arrow", b"payload", "zstd")
        entry = storage.get("test-key.arrow")
        assert entry is not None
        assert entry == (b"payload", "zstd")

    def test_localhost_only_validator_accepts_localhost(self) -> None:
        """localhost_only_validator accepts 127.0.0.1 and localhost URLs."""
        localhost_only_validator("http://127.0.0.1:8080/__blobs__/abc.arrow")
        localhost_only_validator("http://localhost:9000/__blobs__/def.arrow")

    def test_localhost_only_validator_rejects_remote(self) -> None:
        """localhost_only_validator rejects non-localhost URLs."""
        with pytest.raises(ValueError, match="localhost"):
            localhost_only_validator("https://s3.amazonaws.com/bucket/key.arrow")


# ---------------------------------------------------------------------------
# Integration tests — full HTTP server with demo blob storage
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("compression", "expected_extension"),
    [("none", ".arrow"), ("zstd", ".arrow.zst"), ("gzip", ".arrow.gz")],
)
def test_demo_output_offload_large_response(compression: str, expected_extension: str) -> None:
    """Large random_bytes output should be externalized to demo blobs and resolved by HTTP client."""
    pytest.importorskip("vgi_rpc.http")
    pytest.importorskip("aiohttp", reason="aiohttp required for external location resolution")

    if compression == "zstd":
        http_parser = pytest.importorskip("aiohttp.http_parser")
        if not bool(getattr(http_parser, "HAS_ZSTD", False)):
            pytest.skip("zstd test requires aiohttp zstd decode support (backports.zstd)")
    # gzip needs no capability check — stdlib zlib always handles it
    # and aiohttp's default Accept-Encoding includes gzip.

    from vgi_rpc import ExternalLocationConfig
    from vgi_rpc.http import http_capabilities, http_connect
    from vgi_rpc.metadata import LOCATION_FETCH_MS_KEY, LOCATION_SOURCE_KEY
    from vgi_rpc.rpc import AnnotatedBatch

    threshold_bytes = 4 * 1024
    payload_bytes = threshold_bytes + 1024
    seed = 12345

    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"

    with _run_demo_http_server(
        port=port,
        threshold_bytes=threshold_bytes,
        compression=compression,
    ):
        _wait_for_http_server(base_url)

        caps = http_capabilities(base_url=base_url)
        assert caps.upload_url_support
        assert caps.max_upload_bytes == threshold_bytes

        with http_connect(
            VgiProtocol,  # type: ignore[type-abstract]
            base_url=base_url,
            external_location=ExternalLocationConfig(url_validator=localhost_only_validator),
        ) as proxy:
            input_schema = pa.schema([("dummy", pa.int64())])
            input_batch = pa.RecordBatch.from_pydict({"dummy": [1]}, schema=input_schema)
            bind_request = BindRequest(
                function_name="random_bytes",
                arguments=Arguments(positional=(pa.scalar(seed), pa.scalar(payload_bytes))),
                function_type=FunctionType.SCALAR,
                input_schema=input_schema,
            )

            bind_response = proxy.bind(request=bind_request)
            stream = proxy.init(
                request=InitRequest(
                    bind_call=bind_request,
                    output_schema=bind_response.output_schema,
                    bind_opaque_data=bind_response.opaque_data,
                )
            )
            try:
                output = stream.exchange(AnnotatedBatch(batch=input_batch))
            finally:
                stream.close()

        assert output.batch.num_rows == 1
        result_values = output.batch.column("result").to_pylist()
        assert len(result_values) == 1
        assert isinstance(result_values[0], bytes)
        assert len(result_values[0]) == payload_bytes

        assert output.custom_metadata is not None
        source = output.custom_metadata.get(LOCATION_SOURCE_KEY)
        fetch_ms = output.custom_metadata.get(LOCATION_FETCH_MS_KEY)
        assert isinstance(source, bytes)
        assert b"/__blobs__/" in source
        assert source.decode().endswith(expected_extension)
        assert fetch_ms is not None


@pytest.mark.parametrize("compression", ["none", "zstd", "gzip"])
def test_demo_input_upload_url_then_exchange(compression: str) -> None:
    """Upload input via server-vended URL and process via external-location input batch."""
    pytest.importorskip("vgi_rpc.http")
    pytest.importorskip("aiohttp", reason="aiohttp required for external location resolution")

    if compression == "zstd":
        http_parser = pytest.importorskip("aiohttp.http_parser")
        if not bool(getattr(http_parser, "HAS_ZSTD", False)):
            pytest.skip("zstd test requires aiohttp zstd decode support (backports.zstd)")
        pytest.importorskip("zstandard")

    from vgi_rpc import ExternalLocationConfig
    from vgi_rpc.http import http_capabilities, http_connect, request_upload_urls
    from vgi_rpc.metadata import LOCATION_KEY
    from vgi_rpc.rpc import AnnotatedBatch

    threshold_bytes = 4 * 1024
    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"

    with _run_demo_http_server(port=port, threshold_bytes=threshold_bytes):
        _wait_for_http_server(base_url)

        caps = http_capabilities(base_url=base_url)
        assert caps.upload_url_support
        assert caps.max_upload_bytes == threshold_bytes

        urls = request_upload_urls(base_url=base_url, count=1)
        assert len(urls) == 1
        upload = urls[0]

        input_schema = pa.schema([("x", pa.int64())])
        uploaded_batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3]}, schema=input_schema)
        payload = _batch_to_ipc_bytes(uploaded_batch)
        headers: dict[str, str] = {}
        if compression == "zstd":
            import zstandard

            payload = zstandard.ZstdCompressor(level=3).compress(payload)
            headers["Content-Encoding"] = "zstd"
        elif compression == "gzip":
            import zlib

            co = zlib.compressobj(6, zlib.DEFLATED, 31)
            payload = co.compress(payload) + co.flush(zlib.Z_FINISH)
            headers["Content-Encoding"] = "gzip"

        put_resp = httpx.put(upload.upload_url, content=payload, headers=headers, timeout=30.0)
        assert put_resp.status_code == 201

        with http_connect(
            VgiProtocol,  # type: ignore[type-abstract]
            base_url=base_url,
            external_location=ExternalLocationConfig(url_validator=localhost_only_validator),
        ) as proxy:
            bind_request = BindRequest(
                function_name="double",
                arguments=Arguments(positional=(pa.scalar("x"),)),
                function_type=FunctionType.SCALAR,
                input_schema=input_schema,
            )
            bind_response = proxy.bind(request=bind_request)
            stream = proxy.init(
                request=InitRequest(
                    bind_call=bind_request,
                    output_schema=bind_response.output_schema,
                    bind_opaque_data=bind_response.opaque_data,
                )
            )
            try:
                pointer_batch = pa.RecordBatch.from_pydict({"x": []}, schema=input_schema)
                cm = pa.KeyValueMetadata({LOCATION_KEY: upload.download_url.encode()})
                output = stream.exchange(AnnotatedBatch(batch=pointer_batch, custom_metadata=cm))
            finally:
                stream.close()

        assert output.batch.to_pydict() == {"result": [2, 4, 6]}


def test_demo_capabilities_advertised() -> None:
    """Demo storage server should advertise upload_url_support and max_upload_bytes."""
    pytest.importorskip("vgi_rpc.http")

    from vgi_rpc.http import http_capabilities

    threshold_bytes = 8192
    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"

    with _run_demo_http_server(port=port, threshold_bytes=threshold_bytes):
        _wait_for_http_server(base_url)

        caps = http_capabilities(base_url=base_url)
        assert caps.upload_url_support is True
        assert caps.max_upload_bytes == threshold_bytes
        assert caps.max_request_bytes == threshold_bytes


def test_demo_blob_404() -> None:
    """GET for a nonexistent blob should return 404."""
    pytest.importorskip("vgi_rpc.http")

    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"

    with _run_demo_http_server(port=port, threshold_bytes=4096):
        _wait_for_http_server(base_url)

        resp = httpx.get(f"{base_url}/__blobs__/nonexistent.arrow", timeout=10.0)
        assert resp.status_code == 404
