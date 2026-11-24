"""End to end: an HTTP worker whose large values live in an S3-compatible bucket (VGI_STORAGE_*).

Opt-in: set ``VGI_TEST_S3_ENDPOINT`` (plain http, e.g. RustFS or MinIO) and
``VGI_TEST_S3_ACCESS_KEY`` / ``VGI_TEST_S3_SECRET_KEY``. CI runs it in the
"Direct S3 (RustFS)" job, which fails on skips.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from urllib.parse import urlsplit

import pyarrow as pa
import pytest

from vgi.arguments import Arguments
from vgi.invocation import FunctionType
from vgi.protocol import BindRequest, InitRequest, VgiProtocol

_ENDPOINT = os.environ.get("VGI_TEST_S3_ENDPOINT", "")
pytestmark = pytest.mark.skipif(not _ENDPOINT, reason="set VGI_TEST_S3_ENDPOINT (+ _ACCESS_KEY, _SECRET_KEY)")

THRESHOLD = 4096


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _s3() -> object:
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=_ENDPOINT,
        region_name="us-east-1",
        aws_access_key_id=os.environ["VGI_TEST_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["VGI_TEST_S3_SECRET_KEY"],
    )


@pytest.fixture
def bucket() -> str:
    """Create a fresh bucket."""
    name = f"vgi-e2e-{uuid.uuid4().hex[:12]}"
    _s3().create_bucket(Bucket=name)  # type: ignore[attr-defined]
    return name


@contextmanager
def _worker(bucket: str, **extra: str) -> Iterator[str]:
    """Run the HTTP fixture server with direct S3 storage; yield its base URL."""
    port = _free_port()
    env = {
        **os.environ,
        "VGI_STORAGE_S3_BUCKET": bucket,
        "VGI_STORAGE_S3_ENDPOINT": _ENDPOINT,
        "VGI_STORAGE_S3_PREFIX": "e2e/",
        "VGI_STORAGE_S3_ACCESS_KEY_ID": os.environ["VGI_TEST_S3_ACCESS_KEY"],
        "VGI_STORAGE_S3_SECRET_ACCESS_KEY": os.environ["VGI_TEST_S3_SECRET_KEY"],
        "VGI_STORAGE_S3_VERIFY": "1",
        "VGI_STORAGE_ALLOW_PLAIN_HTTP": "1",
        "VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES": str(THRESHOLD),
        "VGI_RPC_MAX_REQUEST_BYTES": "32768",
        **extra,
    }
    env.pop("VGI_BLOB_VENDOR_URL", None)
    cmd = [sys.executable, "-m", "vgi._test_fixtures.http_server", "--host", "127.0.0.1", "--port", str(port)]
    proc = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    base = f"http://127.0.0.1:{port}"
    try:
        from vgi_rpc.http import http_capabilities

        deadline = time.time() + 60
        while True:
            if proc.poll() is not None:
                raise RuntimeError(f"worker exited: {proc.stderr.read() if proc.stderr else ''}")
            try:
                http_capabilities(base_url=base)
                break
            except Exception:
                if time.time() > deadline:
                    raise
                time.sleep(0.25)
        yield base
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


def _random_bytes(base: str, size: int) -> tuple[bytes, bytes | None]:
    """Call random_bytes(seed, size); return the value and its vgi_rpc.location source."""
    from vgi_rpc import ExternalLocationConfig
    from vgi_rpc.http import http_connect
    from vgi_rpc.metadata import LOCATION_SOURCE_KEY
    from vgi_rpc.rpc import AnnotatedBatch

    from vgi.http.demo_storage import localhost_only_validator

    with http_connect(
        VgiProtocol,  # type: ignore[type-abstract]
        base_url=base,
        external_location=ExternalLocationConfig(url_validator=localhost_only_validator),
    ) as proxy:
        schema = pa.schema([("dummy", pa.int64())])
        bind = BindRequest(
            function_name="random_bytes",
            arguments=Arguments(positional=(pa.scalar(7), pa.scalar(size))),
            function_type=FunctionType.SCALAR,
            input_schema=schema,
        )
        bound = proxy.bind(request=bind)
        stream = proxy.init(
            request=InitRequest(bind_call=bind, output_schema=bound.output_schema, bind_opaque_data=bound.opaque_data)
        )
        try:
            out = stream.exchange(AnnotatedBatch(batch=pa.RecordBatch.from_pydict({"dummy": [1]}, schema=schema)))
        finally:
            stream.close()
    value = out.batch.column("result").to_pylist()[0]
    source = out.custom_metadata.get(LOCATION_SOURCE_KEY) if out.custom_metadata is not None else None
    return value, source


def _keys(bucket: str) -> list[str]:
    resp = _s3().list_objects_v2(Bucket=bucket)  # type: ignore[attr-defined]
    return [o["Key"] for o in resp.get("Contents", [])]


def test_results_externalize_to_the_bucket(bucket: str) -> None:
    """A result over the threshold is stored in the bucket and fetched back through a presigned URL."""
    with _worker(bucket) as base:
        from vgi_rpc.http import http_capabilities

        caps = http_capabilities(base_url=base)
        assert caps.upload_url_support
        value, source = _random_bytes(base, 200_000)
    assert len(value) == 200_000
    assert source is not None
    url = urlsplit(source.decode())
    assert url.netloc == urlsplit(_ENDPOINT).netloc
    assert url.path.startswith(f"/{bucket}/e2e/")
    assert any(k.startswith("e2e/") for k in _keys(bucket))


def test_checksum_none_issues_no_grants(bucket: str) -> None:
    """VGI_STORAGE_S3_CHECKSUM=none: results still externalize, upload URLs are not advertised."""
    with _worker(bucket, VGI_STORAGE_S3_CHECKSUM="none") as base:
        from vgi_rpc.http import http_capabilities

        assert not http_capabilities(base_url=base).upload_url_support
        value, source = _random_bytes(base, 100_000)
    assert len(value) == 100_000 and source is not None
    assert _keys(bucket)


def test_missing_bucket_fails_at_startup(bucket: str) -> None:
    """VGI_STORAGE_S3_VERIFY=1 with a missing bucket: the worker refuses to start and says why."""
    with pytest.raises(RuntimeError, match="does not exist"), _worker(f"{bucket}-nope"):
        pass
