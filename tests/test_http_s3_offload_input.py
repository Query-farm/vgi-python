"""HTTP transport integration tests for S3 upload-URL input offload."""

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
from vgi.invocation import FunctionType
from vgi.protocol import BindRequest, InitRequest, VgiProtocol

RUN_S3_HTTP_TESTS_ENV = "VGI_RUN_S3_HTTP_TESTS"
S3_BUCKET_ENV = "VGI_HTTP_S3_BUCKET"
S3_ENDPOINT_ENV = "VGI_HTTP_S3_ENDPOINT_URL"
DEFAULT_S3_BUCKET = "rusty-vgi-test"


def _free_port() -> int:
    """Allocate an available localhost TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@contextmanager
def _run_example_http_server(*, port: int, bucket: str, threshold_bytes: int) -> Iterator[None]:
    """Run vgi.examples.http_server in a subprocess for the duration of a test."""
    env = os.environ.copy()
    env[S3_BUCKET_ENV] = bucket

    cmd = [
        sys.executable,
        "-m",
        "vgi.examples.http_server",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--prefix",
        "/vgi",
        "--externalize-threshold-bytes",
        str(threshold_bytes),
        "--max-upload-bytes",
        str(threshold_bytes),
        "--s3-prefix",
        "vgi-python-tests/",
    ]
    s3_endpoint = os.environ.get(S3_ENDPOINT_ENV)
    if s3_endpoint:
        cmd.extend(["--s3-endpoint-url", s3_endpoint])

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
            http_capabilities(base_url=base_url, prefix="/vgi")
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


@pytest.mark.parametrize("compression", ["none", "zstd"])
@pytest.mark.skipif(
    os.environ.get(RUN_S3_HTTP_TESTS_ENV) != "1",
    reason=f"Set {RUN_S3_HTTP_TESTS_ENV}=1 to run live S3 HTTP offload tests",
)
def test_http_input_upload_url_then_external_location_scalar_exchange(compression: str) -> None:
    """Upload input via server-vended URL and process via external-location input batch."""
    pytest.importorskip("vgi_rpc.http")
    pytest.importorskip("vgi_rpc.s3")

    if compression == "zstd":
        import aiohttp.http_parser as http_parser  # type: ignore[import-not-found]

        if not bool(getattr(http_parser, "HAS_ZSTD", False)):
            pytest.skip("zstd input test requires aiohttp zstd decode support (backports.zstd)")
        pytest.importorskip("zstandard")

    from vgi_rpc import ExternalLocationConfig
    from vgi_rpc.http import http_capabilities, http_connect, request_upload_urls
    from vgi_rpc.metadata import LOCATION_KEY
    from vgi_rpc.rpc import AnnotatedBatch

    threshold_bytes = 4 * 1024 * 1024
    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"
    bucket = os.environ.get(S3_BUCKET_ENV, DEFAULT_S3_BUCKET)

    with _run_example_http_server(port=port, bucket=bucket, threshold_bytes=threshold_bytes):
        _wait_for_http_server(base_url)

        caps = http_capabilities(base_url=base_url, prefix="/vgi")
        assert caps.upload_url_support
        assert caps.max_upload_bytes == threshold_bytes

        urls = request_upload_urls(base_url=base_url, prefix="/vgi", count=1)
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

        put_resp = httpx.put(upload.upload_url, content=payload, headers=headers, timeout=30.0)
        assert put_resp.status_code in (200, 201)

        with http_connect(
            VgiProtocol,  # type: ignore[type-abstract]
            base_url=base_url,
            prefix="/vgi",
            external_location=ExternalLocationConfig(),
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
