# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Argument references and ``init: StateRef`` over HTTP (protocol 3.4).

Drives ``vgi-serve --http`` (the ``blob_test`` catalog) against vgi-rpc's fake
blob vendor the way the DuckDB extension does (vgi ``docs/large-values-3.4.md``):
the client uploads the arguments IPC once through the worker's
``__upload_url__`` grant, then binds and scans with ``arguments = null`` and an
``arguments_location`` carrying the grant's sealed ``ref``.

Covered: the ``VGI-Arguments-Ref-Bytes`` capability header (and its test
switch); resolution in bind, init and every continuation of a multi-turn
stream, with one vendor GET per process (the object cache); the stream's init
state staying small because it carries the reference, not the value; the
refusal kinds (``invalid_arguments_ref``, ``invalid_bind_request``,
``arguments_ref_missing`` with the prepared-statement message); and the
split-token fingerprint for a location.
"""

from __future__ import annotations

import dataclasses
import hashlib
import os
import subprocess
import sys
from collections.abc import Iterator
from typing import Any

import httpx2
import pyarrow as pa
import pytest
from vgi_rpc.conformance.fake_vendor import FakeVendorServer, serve_fake_vendor
from vgi_rpc.errors import StatusError

from vgi.arguments import Arguments
from vgi.arguments_ref import (
    ARGUMENTS_REF_BYTES_HEADER,
    ARGUMENTS_REF_MISSING_MESSAGE,
    arguments_ref_bytes_from_env,
    resolve_bind_call,
    resolved_arguments,
)
from vgi.invocation import FunctionType
from vgi.protocol import (
    INVALID_BIND_REQUEST_MESSAGE,
    ArgumentsLocation,
    BindRequest,
    InitRequest,
    VgiProtocol,
)
from vgi.split_token import bind_fingerprint

pytest.importorskip("aiohttp", reason="external location resolution needs vgi-rpc[external]")

_PAYLOAD = "x" * 200_000 + "end"


@pytest.fixture(scope="module")
def vendor() -> Iterator[FakeVendorServer]:
    """A fake blob vendor with its test routes (stats, delete)."""
    with serve_fake_vendor(test_routes=True) as server:
        yield server


def _serve(vendor: FakeVendorServer, **extra_env: str) -> tuple[subprocess.Popen[str], str]:
    env = {
        **os.environ,
        "VGI_BLOB_VENDOR_URL": vendor.url,
        "VGI_BLOB_VENDOR_TOKEN": "test-token",
        "VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES": "1048576",
        "VGI_RPC_MAX_REQUEST_BYTES": "32768",
        "VGI_RPC_MAX_UPLOAD_BYTES": "268435456",
        "PYTHONUNBUFFERED": "1",
        **extra_env,
    }
    proc = subprocess.Popen(
        [sys.executable, "-m", "vgi.serve", "vgi._test_fixtures.blob_catalog:BlobTestWorker", "--http"]
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


@pytest.fixture
def worker(vendor: FakeVendorServer) -> Iterator[str]:
    """``vgi-serve --http`` for ``blob_test`` with the vendor; yields the base URL."""
    proc, url = _serve(vendor)
    try:
        yield url
    finally:
        proc.terminate()
        proc.wait(timeout=10)


def _upload(base_url: str, data: bytes) -> ArgumentsLocation:
    """Upload *data* through the worker's grant, as the extension does; return the location."""
    from vgi_rpc.http import request_upload_urls
    from vgi_rpc.http._client import upload_put_headers

    sha = hashlib.sha256(data).hexdigest()
    (grant,) = request_upload_urls(base_url, uploads=[(len(data), sha)])
    assert grant.ref, "a protocol 3.4 grant returns the sealed argument reference"
    put = httpx2.put(grant.upload_url, content=data, headers=upload_put_headers(grant))
    assert put.status_code < 300, put.text
    return ArgumentsLocation(ref=grant.ref, size=len(data), sha256=sha)


def _connect(base_url: str, vendor: FakeVendorServer) -> Any:
    from vgi_rpc import ExternalLocationConfig
    from vgi_rpc.http import http_connect
    from vgi_rpc.vendor import pinned_hosts_validator

    validator = pinned_hosts_validator([vendor.vendor.host], allow_http=True)
    return http_connect(
        VgiProtocol,
        base_url=base_url,
        external_location=ExternalLocationConfig(url_validator=validator),
    )


def _bind(function_name: str, *, location: ArgumentsLocation) -> BindRequest:
    return BindRequest(
        function_name=function_name,
        arguments=None,
        arguments_location=location,
        function_type=FunctionType.TABLE,
    )


def _scan(proxy: Any, bind: BindRequest) -> pa.Table:
    bound = proxy.bind(request=bind)
    stream = proxy.init(
        request=InitRequest(bind_call=bind, output_schema=bound.output_schema, bind_opaque_data=bound.opaque_data)
    )
    batches = [ab.batch for ab in stream]
    return pa.Table.from_batches(batches, schema=bound.output_schema)


def _args_ipc(*values: object) -> bytes:
    return Arguments(positional=tuple(pa.scalar(v) for v in values)).serialize_to_bytes()  # type: ignore[call-overload]


def test_header_and_env() -> None:
    """The advertised value defaults to 4 MiB; a malformed override stops startup."""
    assert arguments_ref_bytes_from_env({}) == 4194304
    assert arguments_ref_bytes_from_env({"VGI_ARGUMENTS_REF_BYTES": "65536"}) == 65536
    for bad in ("0", "-1", "012", "1e6", "12345678901234567890"):
        with pytest.raises(ValueError, match="VGI_ARGUMENTS_REF_BYTES"):
            arguments_ref_bytes_from_env({"VGI_ARGUMENTS_REF_BYTES": bad})


def test_capability_header(worker: str, vendor: FakeVendorServer) -> None:
    """Sent with the upload-URL capability; the test switch removes it."""
    resp = httpx2.options(f"{worker}/health")
    assert resp.headers.get("VGI-Upload-URL-Support") == "2"
    assert resp.headers.get(ARGUMENTS_REF_BYTES_HEADER) == "4194304"
    proc, url = _serve(vendor, VGI_TEST_DISABLE_ARGUMENTS_REF="1", VGI_ARGUMENTS_REF_BYTES="65536")
    try:
        resp = httpx2.options(f"{url}/health")
        assert resp.headers.get("VGI-Upload-URL-Support") == "2"
        assert ARGUMENTS_REF_BYTES_HEADER not in resp.headers
    finally:
        proc.terminate()
        proc.wait(timeout=10)


def test_resolves_in_bind_init_and_every_turn(worker: str, vendor: FakeVendorServer) -> None:
    """A 12-turn producer on a referenced argument: one vendor GET, every turn correct."""
    data = _args_ipc(_PAYLOAD, 12)
    location = _upload(worker, data)
    before = vendor.vendor.test_stats()
    with _connect(worker, vendor) as proxy:
        table = _scan(proxy, _bind("blob_param_batches", location=location))
    after = vendor.vendor.test_stats()
    assert table.column("i").to_pylist() == list(range(12))
    assert set(table.column("payload_len").to_pylist()) == {len(_PAYLOAD)}
    assert set(table.column("payload_sha256").to_pylist()) == {hashlib.sha256(_PAYLOAD.encode()).hexdigest()}
    # bind, init and 12 continuations all resolved it; the object cache fetched it once.
    assert after["gets_upload"] - before["gets_upload"] == 1
    # The init request carries the reference, so the init state stays inline.
    assert after["puts_state_ref"] == before["puts_state_ref"]


def test_split_streams_share_one_fetch(worker: str, vendor: FakeVendorServer) -> None:
    """Every split's init embeds the same bind call; the plan's fingerprint matches each init."""
    location = _upload(worker, _args_ipc(_PAYLOAD, 3))
    before = vendor.vendor.test_stats()
    with _connect(worker, vendor) as proxy:
        bind = _bind("blob_param_streams", location=location)
        bound = proxy.bind(request=bind)
        from vgi.protocol import ScanSplit, TableFunctionPlanRequest

        plan = proxy.table_function_plan(request=TableFunctionPlanRequest(bind_call=bind))
        assert len(plan.splits) == 3
        rows: list[dict[str, Any]] = []
        for token in plan.splits:
            stream = proxy.init(
                request=InitRequest(
                    bind_call=bind,
                    output_schema=bound.output_schema,
                    bind_opaque_data=bound.opaque_data,
                    split_tokens=[ScanSplit.deserialize_from_bytes(token).token],
                )
            )
            rows.extend(r for ab in stream for r in ab.batch.to_pylist())
    assert sorted(r["stream"] for r in rows) == [0, 1, 2]
    assert vendor.vendor.test_stats()["gets_upload"] - before["gets_upload"] == 1


def test_tampered_reference(worker: str, vendor: FakeVendorServer) -> None:
    """A ref that does not open is ``invalid_arguments_ref``."""
    from vgi_rpc.rpc import RpcError

    location = _upload(worker, _args_ipc(_PAYLOAD))
    ref = bytearray(location.ref)
    ref[-1] ^= 0x01
    tampered = dataclasses.replace(location, ref=bytes(ref))
    with _connect(worker, vendor) as proxy, pytest.raises(RpcError) as excinfo:
        proxy.bind(request=_bind("blob_param_echo", location=tampered))
    assert excinfo.value.error_kind == "invalid_arguments_ref"
    # A size that differs from the sealed one is refused the same way.
    wrong_size = dataclasses.replace(location, size=location.size + 1)
    with _connect(worker, vendor) as proxy, pytest.raises(RpcError) as excinfo:
        proxy.bind(request=_bind("blob_param_echo", location=wrong_size))
    assert excinfo.value.error_kind == "invalid_arguments_ref"


def test_missing_object_is_the_prepared_statement_message(vendor: FakeVendorServer) -> None:
    """With the object deleted and a cold cache: ``arguments_ref_missing`` and the exact message."""
    from vgi_rpc.rpc import RpcError

    proc, url = _serve(vendor, VGI_RPC_OBJECT_CACHE_BYTES="1")
    try:
        location = _upload(url, _args_ipc(_PAYLOAD))
        with _connect(url, vendor) as proxy:
            assert _scan(proxy, _bind("blob_param_echo", location=location)).num_rows == 1
            deleted = httpx2.get(f"{vendor.url}/__admin__/delete", params={"origin": "client_upload"})
            assert deleted.json()["ok"] is True
            with pytest.raises(RpcError) as excinfo:
                proxy.bind(request=_bind("blob_param_echo", location=location))
        assert excinfo.value.error_kind == "arguments_ref_missing"
        assert excinfo.value.error_code == "NOT_FOUND"
        assert ARGUMENTS_REF_MISSING_MESSAGE in excinfo.value.error_message
    finally:
        proc.terminate()
        proc.wait(timeout=10)


def test_invalid_combination_message() -> None:
    """Both or neither of arguments / arguments_location: the spec's exact message."""
    location = ArgumentsLocation(ref=b"r", size=1, sha256="0" * 64)
    cases: list[tuple[Arguments | None, ArgumentsLocation | None]] = [(None, None), (Arguments(), location)]
    for arguments, loc in cases:
        with pytest.raises(StatusError) as excinfo:
            BindRequest(
                function_name="f", arguments=arguments, arguments_location=loc, function_type=FunctionType.TABLE
            )
        assert excinfo.value.error_kind == "invalid_bind_request"
        assert str(excinfo.value) == INVALID_BIND_REQUEST_MESSAGE


def test_no_context_and_pipe_refuse() -> None:
    """Without an HTTP server's context, a reference cannot resolve (invalid_arguments_ref)."""
    from vgi_rpc.rpc import AuthContext, CallContext

    bind = _bind("f", location=ArgumentsLocation(ref=b"r", size=1, sha256="0" * 64))
    with pytest.raises(StatusError) as excinfo:
        resolve_bind_call(bind, None)
    assert excinfo.value.error_kind == "invalid_arguments_ref"
    ctx = CallContext(auth=AuthContext.anonymous(), emit_client_log=lambda *a, **kw: None)
    with pytest.raises(Exception) as excinfo2:
        resolved_arguments(bind, ctx)
    assert getattr(excinfo2.value, "error_kind", None) == "invalid_arguments_ref"


def test_resolved_bind_call_reencodes_as_received() -> None:
    """The resolved bind call carries the value but serializes (and fingerprints) as the location."""
    location = ArgumentsLocation(ref=b"sealed-ref", size=5, sha256="ab" * 32)
    received = BindRequest.deserialize_from_bytes(_bind("f", location=location).serialize_to_bytes())
    resolved = received.with_resolved_arguments(Arguments(positional=(pa.scalar("hello"),)))
    assert resolved.arguments is not None and resolved.arguments_location is None
    assert resolved.serialize_to_bytes() == received.serialize_to_bytes()
    assert resolved.received_arguments_location == location
    init = InitRequest(bind_call=resolved, output_schema=pa.schema([]))
    assert InitRequest.deserialize_from_bytes(init.serialize_to_bytes()).bind_call.arguments is None
    # Fingerprint: size + sha256 only, never the ref (it holds a random nonce).
    other_ref = BindRequest.deserialize_from_bytes(
        _bind("f", location=dataclasses.replace(location, ref=b"another-nonce")).serialize_to_bytes()
    )
    assert bind_fingerprint(resolved) == bind_fingerprint(received) == bind_fingerprint(other_ref)
    inline = BindRequest(function_name="f", arguments=resolved.arguments, function_type=FunctionType.TABLE)
    assert bind_fingerprint(inline) != bind_fingerprint(resolved)


def test_stream_init_carries_the_output_schema_once() -> None:
    """The stream's own schema (in vgi-rpc's call token) stands in for an unnarrowed init output schema."""
    from vgi.invocation import GlobalInitResponse
    from vgi.protocol import VgiStreamInit

    full = pa.schema([(f"c{i}", pa.int64()) for i in range(50)])
    bind = BindRequest(function_name="f", arguments=Arguments(), function_type=FunctionType.TABLE)
    init_call = InitRequest(bind_call=bind, output_schema=full)
    response = GlobalInitResponse(execution_id=b"e")

    elided = VgiStreamInit.for_stream(init_call, response, full)
    assert elided.output_schema_from_stream and len(elided.init_call.output_schema) == 0
    kept = VgiStreamInit.for_stream(init_call, response, pa.schema([full.field(0)]))
    assert not kept.output_schema_from_stream and kept.init_call.output_schema.equals(full)
    assert len(elided.serialize_to_bytes()) < len(kept.serialize_to_bytes())

    back = VgiStreamInit.deserialize_from_bytes(elided.serialize_to_bytes()).restored(full)
    assert back.init_call.output_schema.equals(full) and not back.output_schema_from_stream
    assert back.init_call.bind_call.serialize_to_bytes() == bind.serialize_to_bytes()
