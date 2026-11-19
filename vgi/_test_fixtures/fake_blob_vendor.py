# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""A runnable fake blob vendor with a stats endpoint, for externalization tests.

Starts vgi-rpc's in-process fake vendor (`vgi_rpc.conformance.fake_vendor`)
on a background thread and adds one unauthenticated route,
``GET /__stats__``, returning counters as JSON (see `stats`). The DuckDB
extension's externalization lane reads them through the fixture worker's
``vgi_test_blob_stats()`` to assert that externalization really happened.

Usage::

    python -m vgi._test_fixtures.fake_blob_vendor [--host 127.0.0.1] [--port 0]
        [--token TOKEN[=TENANT] ...] [--port-file PATH]

Prints ``PORT:<n>`` and then ``URL:<base url>`` on stdout and serves until
killed. Point workers at it with ``VGI_BLOB_VENDOR_URL=<base url>`` and
``VGI_BLOB_VENDOR_TOKEN=<token>`` (default token ``test-token``).

Not for production: the fake keeps every object in memory and signs nothing.
"""

from __future__ import annotations

import argparse
import json
import threading
from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from vgi_rpc.conformance.fake_vendor import FakeVendorServer

__all__ = ["STATS_PATH", "main", "serve_fake_blob_vendor", "stats"]

STATS_PATH = "/__stats__"


class _Counters:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.bytes_uploaded = 0
        self.bytes_downloaded = 0


def stats(server: FakeVendorServer, counters: Any = None) -> dict[str, int]:
    """Return the fake vendor's counters.

    Keys: ``puts`` (``/v1/blobs/put`` calls), ``gets`` (``/v1/blobs/get``),
    ``deletes`` (``/v1/blobs/delete``), ``delete_scopes``
    (``/v1/blobs/delete_scope``), ``upload_urls`` (``/v1/upload_urls``
    calls), ``storage_puts`` / ``storage_gets`` (signed-URL uploads and
    downloads, including RPC results and client uploads), ``objects`` and
    ``bytes`` (live objects and their total size), and ``bytes_uploaded`` /
    ``bytes_downloaded`` (cumulative signed-URL traffic).

    Args:
        server: The running fake.
        counters: Byte counters from `serve_fake_blob_vendor`, if any.

    Returns:
        The counters.
    """
    vendor = server.vendor
    hits = vendor.stats()
    with vendor._lock:
        live = [o.data for o in vendor._objects.values() if o.data is not None]
    out = {
        "puts": hits.get("/v1/blobs/put", 0),
        "gets": hits.get("/v1/blobs/get", 0),
        "deletes": hits.get("/v1/blobs/delete", 0),
        "delete_scopes": hits.get("/v1/blobs/delete_scope", 0),
        "upload_urls": hits.get("/v1/upload_urls", 0),
        "storage_puts": hits.get("r2_put", 0),
        "storage_gets": hits.get("r2_get", 0),
        "objects": len(live),
        "bytes": sum(len(d) for d in live),
        "bytes_uploaded": 0,
        "bytes_downloaded": 0,
    }
    if counters is not None:
        with counters.lock:
            out["bytes_uploaded"] = counters.bytes_uploaded
            out["bytes_downloaded"] = counters.bytes_downloaded
    return out


def serve_fake_blob_vendor(
    tokens: dict[str, str] | None = None, *, host: str = "127.0.0.1", port: int = 0
) -> FakeVendorServer:
    """Start the fake vendor with the ``/__stats__`` route on a background thread.

    Args:
        tokens: Bearer token to tenant (default ``{"test-token": "tenant-a"}``).
        host: Bind host.
        port: Bind port (``0`` picks one).

    Returns:
        The running server; ``.url`` is its base URL. Close it to stop.
    """
    from vgi_rpc.conformance.fake_vendor import serve_fake_vendor

    server = serve_fake_vendor(tokens, host=host, port=port)
    inner = server.vendor.app()
    counters = _Counters()

    def app(environ: dict[str, Any], start_response: Any) -> Iterable[bytes]:
        path = str(environ.get("PATH_INFO", ""))
        method = str(environ.get("REQUEST_METHOD", "")).upper()
        if path == STATS_PATH and method == "GET":
            body = json.dumps(stats(server, counters)).encode()
            start_response("200 OK", [("Content-Type", "application/json"), ("Content-Length", str(len(body)))])
            return [body]
        if path.startswith("/r2/") and method == "PUT":
            with counters.lock:
                counters.bytes_uploaded += int(environ.get("CONTENT_LENGTH") or 0)
            return inner(environ, start_response)
        if path.startswith("/r2/") and method == "GET":
            return _count(inner(environ, start_response), counters)
        return inner(environ, start_response)

    server._server.set_app(app)
    server.stats_counters = counters  # type: ignore[attr-defined]
    return server


def _count(chunks: Iterable[bytes], counters: _Counters) -> Iterator[bytes]:
    for chunk in chunks:
        with counters.lock:
            counters.bytes_downloaded += len(chunk)
        yield chunk


def main() -> None:
    """Run the fake vendor until killed; prints ``PORT:<n>`` and ``URL:<url>``."""
    parser = argparse.ArgumentParser(prog="python -m vgi._test_fixtures.fake_blob_vendor")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--token", action="append", default=[], help="TOKEN[=TENANT] (repeatable)")
    parser.add_argument("--port-file", default=None, help="Also write the port to this file (atomically)")
    args = parser.parse_args()
    tokens = None
    if args.token:
        tokens = {}
        for entry in args.token:
            token, _, tenant = entry.partition("=")
            tokens[token] = tenant or "tenant-a"
    server = serve_fake_blob_vendor(tokens, host=args.host, port=args.port)
    port = int(server.url.rsplit(":", 1)[1])
    if args.port_file:
        from vgi.worker import _write_port_file

        _write_port_file(args.port_file, port)
    print(f"PORT:{port}", flush=True)
    print(f"URL:{server.url}", flush=True)
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        pass
    finally:
        server.close()


if __name__ == "__main__":
    main()
