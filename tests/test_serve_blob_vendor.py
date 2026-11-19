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
    assert cfg.max_request_bytes == 65536


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
