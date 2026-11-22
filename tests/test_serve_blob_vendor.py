# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""``vgi-serve`` wiring of the blob vendor (``VGI_BLOB_VENDOR_URL``).

With the vendor configured, the HTTP worker externalizes large results to it
(pointers carry vendor download URLs), vends client upload URLs, and pins
inbound pointer URLs to the vendor's storage hosts.
"""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Iterator
from typing import Protocol
from urllib.parse import urlsplit

import pyarrow as pa
import pytest
from vgi_rpc.conformance.fake_vendor import FakeVendorServer, serve_fake_vendor
from vgi_rpc.vendor import VendorStorage

from vgi.serve import resolve_blob_vendor_http_config

pytest.importorskip("aiohttp", reason="external location resolution needs vgi-rpc[external]")


@pytest.fixture
def vendor() -> Iterator[FakeVendorServer]:
    """A fake blob vendor on a background thread."""
    with serve_fake_vendor() as server:
        yield server


class _Echo(Protocol):
    def echo(self, value: int) -> int: ...


class _EchoImpl:
    def echo(self, value: int) -> int:
        return value


def test_nothing_configured() -> None:
    """No vendor and no request cap: everything off, as before."""
    cfg = resolve_blob_vendor_http_config({})
    assert (cfg.external_location, cfg.upload_url_provider, cfg.max_upload_bytes, cfg.max_request_bytes) == (
        None,
        None,
        None,
        None,
    )


def test_request_cap_without_vendor() -> None:
    """VGI_RPC_MAX_REQUEST_BYTES is honoured on its own (an explicit operator setting)."""
    cfg = resolve_blob_vendor_http_config({"VGI_RPC_MAX_REQUEST_BYTES": "1048576"})
    assert cfg.max_request_bytes == 1048576
    assert cfg.external_location is None and cfg.upload_url_provider is None


@pytest.mark.parametrize("bad", ["0", "-1", "big"])
def test_malformed_request_cap(bad: str) -> None:
    """A malformed cap stops startup."""
    with pytest.raises(ValueError, match="VGI_RPC_MAX_REQUEST_BYTES"):
        resolve_blob_vendor_http_config({"VGI_RPC_MAX_REQUEST_BYTES": bad})


def test_vendor_configured(vendor: FakeVendorServer) -> None:
    """The vendor supplies the external config, the upload URL provider and the advertised upload size."""
    cfg = resolve_blob_vendor_http_config(
        {
            "VGI_BLOB_VENDOR_URL": vendor.url,
            "VGI_CF_DO_TOKEN": "test-token",
            "VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES": "2048",
            "VGI_RPC_MAX_UPLOAD_BYTES": "1073741824",
            "VGI_RPC_MAX_REQUEST_BYTES": "65536",
        }
    )
    assert cfg.external_location is not None
    assert isinstance(cfg.upload_url_provider, VendorStorage)
    assert cfg.external_location.storage is cfg.upload_url_provider
    assert cfg.external_location.externalize_threshold_bytes == 2048
    assert cfg.max_upload_bytes == 1073741824
    # The inbound fetch cap follows the advertised upload size (a 300 MB
    # upload used to be refused after upload by the 256 MiB default).
    assert cfg.external_location.fetch_config.max_fetch_bytes == 1073741824
    assert cfg.max_request_bytes == 65536


def test_vendor_upload_cap_defaults_to_fetch_cap(vendor: FakeVendorServer) -> None:
    """Unset VGI_RPC_MAX_UPLOAD_BYTES: vgi-rpc advertises the fetch cap, so a client is never surprised after upload."""
    import falcon.testing
    from vgi_rpc.http import make_wsgi_app
    from vgi_rpc.rpc import RpcServer

    cfg = resolve_blob_vendor_http_config(
        {"VGI_BLOB_VENDOR_URL": vendor.url, "VGI_CF_DO_TOKEN": "test-token", "VGI_RPC_MAX_FETCH_BYTES": "536870912"}
    )
    assert cfg.max_upload_bytes is None
    assert cfg.external_location is not None
    assert cfg.external_location.fetch_config.max_fetch_bytes == 536870912
    server = RpcServer(_Echo, _EchoImpl(), external_location=cfg.external_location)
    app = make_wsgi_app(
        server, token_key=b"k" * 32, upload_url_provider=cfg.upload_url_provider, max_upload_bytes=cfg.max_upload_bytes
    )
    assert falcon.testing.TestClient(app).simulate_get("/health").headers["VGI-Max-Upload-Bytes"] == "536870912"


def test_vendor_contradictory_caps_stop_startup(vendor: FakeVendorServer) -> None:
    """VGI_RPC_MAX_UPLOAD_BYTES above an explicit VGI_RPC_MAX_FETCH_BYTES is a startup error."""
    with pytest.raises(ValueError, match="exceeds VGI_RPC_MAX_FETCH_BYTES"):
        resolve_blob_vendor_http_config(
            {
                "VGI_BLOB_VENDOR_URL": vendor.url,
                "VGI_CF_DO_TOKEN": "test-token",
                "VGI_RPC_MAX_UPLOAD_BYTES": "1073741824",
                "VGI_RPC_MAX_FETCH_BYTES": "268435456",
            }
        )


def test_vendor_without_token(vendor: FakeVendorServer) -> None:
    """A vendor URL with no token stops startup."""
    with pytest.raises(ValueError, match="VGI_BLOB_VENDOR_TOKEN"):
        resolve_blob_vendor_http_config({"VGI_BLOB_VENDOR_URL": vendor.url})


def test_vgi_serve_externalizes_results_and_vends_upload_urls(vendor: FakeVendorServer) -> None:
    """End to end: a large scalar result comes back as a vendor pointer and resolves."""
    from vgi_rpc import ExternalLocationConfig
    from vgi_rpc.http import http_capabilities, http_connect
    from vgi_rpc.metadata import LOCATION_SOURCE_KEY
    from vgi_rpc.rpc import AnnotatedBatch
    from vgi_rpc.vendor import pinned_hosts_validator

    from vgi.arguments import Arguments
    from vgi.invocation import FunctionType
    from vgi.protocol import BindRequest, InitRequest, VgiProtocol

    env = {
        **os.environ,
        "VGI_BLOB_VENDOR_URL": vendor.url,
        "VGI_BLOB_VENDOR_TOKEN": "test-token",
        "VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES": "16384",
        "VGI_RPC_MAX_REQUEST_BYTES": "262144",
        "VGI_RPC_MAX_UPLOAD_BYTES": "1073741824",
        "PYTHONUNBUFFERED": "1",
    }
    proc = subprocess.Popen(
        [sys.executable, "-m", "vgi.serve", "vgi._test_fixtures.worker:ExampleWorker", "--http"]
        + ["--host", "127.0.0.1", "--port", "0"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline()
        assert line.startswith("PORT:"), line
        base_url = f"http://127.0.0.1:{line.strip().split(':', 1)[1]}"

        caps = http_capabilities(base_url=base_url)
        assert caps.upload_url_support is True
        assert caps.upload_url_version == 2
        assert caps.max_request_bytes == 262144
        assert caps.max_upload_bytes == 1073741824

        payload_bytes = 64 * 1024
        validator = pinned_hosts_validator([vendor.vendor.host], allow_http=True)
        with http_connect(
            VgiProtocol,  # type: ignore[type-abstract]
            base_url=base_url,
            external_location=ExternalLocationConfig(url_validator=validator),
        ) as proxy:
            schema = pa.schema([("dummy", pa.int64())])
            bind = BindRequest(
                function_name="random_bytes",
                arguments=Arguments(positional=(pa.scalar(7), pa.scalar(payload_bytes))),
                function_type=FunctionType.SCALAR,
                input_schema=schema,
            )
            bound = proxy.bind(request=bind)
            stream = proxy.init(
                request=InitRequest(
                    bind_call=bind, output_schema=bound.output_schema, bind_opaque_data=bound.opaque_data
                )
            )
            try:
                out = stream.exchange(AnnotatedBatch(batch=pa.RecordBatch.from_pydict({"dummy": [1]}, schema=schema)))
            finally:
                stream.close()
        assert len(out.batch.column("result")[0].as_py()) == payload_bytes
        assert out.custom_metadata is not None
        source = out.custom_metadata.get(LOCATION_SOURCE_KEY)
        assert isinstance(source, bytes)
        assert urlsplit(source.decode()).netloc == vendor.vendor.host
        assert vendor.vendor.stats().get("/v1/blobs/put", 0) >= 1
    finally:
        proc.terminate()
        proc.wait(timeout=10)


def _serve_with_vendor(vendor: FakeVendorServer, *, max_request_bytes: int) -> tuple[subprocess.Popen[str], str]:
    """Start ``vgi.serve --http`` wired to *vendor*; return the process and base URL."""
    env = {
        **os.environ,
        "VGI_BLOB_VENDOR_URL": vendor.url,
        "VGI_BLOB_VENDOR_TOKEN": "test-token",
        "VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES": "1048576",
        "VGI_RPC_MAX_REQUEST_BYTES": str(max_request_bytes),
        "VGI_RPC_MAX_UPLOAD_BYTES": "1073741824",
        "PYTHONUNBUFFERED": "1",
    }
    proc = subprocess.Popen(
        [sys.executable, "-m", "vgi.serve", "vgi._test_fixtures.worker:ExampleWorker", "--http"]
        + ["--host", "127.0.0.1", "--port", "0"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    assert proc.stdout is not None
    line = proc.stdout.readline()
    assert line.startswith("PORT:"), line
    return proc, f"http://127.0.0.1:{line.strip().split(':', 1)[1]}"


def _double_via_client(vendor: FakeVendorServer, base_url: str, values: list[int]) -> list[object]:
    """Run the fixture ``double`` scalar through ``vgi.client.Client`` over HTTP."""
    from vgi_rpc import ExternalLocationConfig
    from vgi_rpc.vendor import pinned_hosts_validator

    from vgi.arguments import Arguments
    from vgi.client.client import Client

    schema = pa.schema([("x", pa.int64())])
    config = ExternalLocationConfig(url_validator=pinned_hosts_validator([vendor.vendor.host], allow_http=True))
    with Client.from_http(base_url, external_location=config) as client:
        out = list(
            client.scalar_function(
                function_name="double",
                schema_path=["main"],
                arguments=Arguments(positional=(pa.scalar("x"),)),
                input=iter([pa.RecordBatch.from_pydict({"x": values}, schema=schema)]),
            )
        )
    return [v for b in out for v in b.column("result").to_pylist()]


def test_client_oversize_input_uses_sized_vendor_upload(vendor: FakeVendorServer) -> None:
    """An input over max_request_bytes goes through a sized, checksummed vendor grant (upload-URL v2)."""
    proc, base_url = _serve_with_vendor(vendor, max_request_bytes=65536)
    try:
        values = list(range(20_000))  # ~160 KB of int64, over the 64 KiB cap
        assert _double_via_client(vendor, base_url, values) == [v * 2 for v in values]
        stats = vendor.vendor.test_stats()
        assert stats["puts"] >= 1 and stats["upload_urls"] == 0
        assert stats["storage_puts"] >= 1 and stats["upload_refusals"] == 0
        puts = [headers for method, _path, headers in vendor.vendor.requests if method == "PUT"]
        assert puts and all("HTTP_X_AMZ_CHECKSUM_SHA256" in h for h in puts)
        assert all("HTTP_AUTHORIZATION" not in h for h in puts)
    finally:
        proc.terminate()
        proc.wait(timeout=10)


def test_client_corrupted_upload_is_refused_by_storage(vendor: FakeVendorServer) -> None:
    """A PUT corrupted in transit is refused by the store (BadDigest) and the call fails."""
    from vgi.client.errors import ClientError

    proc, base_url = _serve_with_vendor(vendor, max_request_bytes=65536)
    try:
        vendor.vendor.set_upload_fault("corrupt", 1)
        with pytest.raises(ClientError, match="HTTP 400 BadDigest") as exc_info:
            _double_via_client(vendor, base_url, list(range(20_000)))
        assert "X-Amz-Signature" not in str(exc_info.value)
        stats = vendor.vendor.test_stats()
        assert stats["uploads_corrupted"] == 1 and stats["upload_bad_digest"] == 1
    finally:
        proc.terminate()
        proc.wait(timeout=10)
