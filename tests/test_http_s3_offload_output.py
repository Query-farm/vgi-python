"""HTTP transport integration tests for S3 externalized output responses."""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from urllib.parse import urlparse

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
def _run_example_http_server(
    *,
    port: int,
    bucket: str,
    threshold_bytes: int,
    compression: str = "none",
) -> Iterator[None]:
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
        "--externalize-compression",
        compression,
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


@pytest.mark.parametrize(
    ("compression", "expected_extension"),
    [("none", ".arrow"), ("zstd", ".arrow.zst")],
)
@pytest.mark.skipif(
    os.environ.get(RUN_S3_HTTP_TESTS_ENV) != "1",
    reason=f"Set {RUN_S3_HTTP_TESTS_ENV}=1 to run live S3 HTTP offload tests",
)
def test_http_output_offload_random_bytes_large_response(compression: str, expected_extension: str) -> None:
    """Large random_bytes output should be externalized to S3 and resolved by HTTP client."""
    pytest.importorskip("vgi_rpc.http")
    pytest.importorskip("vgi_rpc.s3")
    if compression == "zstd":
        import aiohttp.http_parser as http_parser  # type: ignore[import-not-found]

        if not bool(getattr(http_parser, "HAS_ZSTD", False)):
            pytest.skip("zstd externalization test requires aiohttp zstd decode support (backports.zstd)")

    from vgi_rpc import ExternalLocationConfig
    from vgi_rpc.http import http_capabilities, http_connect
    from vgi_rpc.metadata import LOCATION_FETCH_MS_KEY, LOCATION_SOURCE_KEY
    from vgi_rpc.rpc import AnnotatedBatch

    threshold_bytes = 4 * 1024 * 1024
    payload_bytes = threshold_bytes + 1024
    seed = 12345

    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"
    bucket = os.environ.get(S3_BUCKET_ENV, DEFAULT_S3_BUCKET)

    with _run_example_http_server(
        port=port,
        bucket=bucket,
        threshold_bytes=threshold_bytes,
        compression=compression,
    ):
        _wait_for_http_server(base_url)

        caps = http_capabilities(base_url=base_url, prefix="/vgi")
        assert caps.upload_url_support
        assert caps.max_upload_bytes == threshold_bytes

        with http_connect(
            VgiProtocol,  # type: ignore[type-abstract]
            base_url=base_url,
            prefix="/vgi",
            external_location=ExternalLocationConfig(),
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
        assert source.startswith(b"https://")
        assert urlparse(source.decode()).path.endswith(expected_extension)
        assert fetch_ms is not None
