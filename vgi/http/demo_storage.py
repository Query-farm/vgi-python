# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""In-process blob storage for demonstrating and testing external batch offloading.

Provides a simple HTTP blob store that implements the ``ExternalStorage`` and
``UploadUrlProvider`` protocols from vgi_rpc, served from the same HTTP server
process.  This allows the example worker to demonstrate external record batch
offloading without requiring S3 or any cloud infrastructure.

**Not for production use** — blobs are held in memory with LRU eviction.
"""

from __future__ import annotations

import threading
import uuid
from collections import OrderedDict
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

if TYPE_CHECKING:
    from vgi_rpc.external import UploadUrl


class DemoBlobStorage:
    """In-memory blob store implementing ``ExternalStorage`` and ``UploadUrlProvider``.

    Blobs are stored in an ``OrderedDict`` with LRU eviction when ``max_blobs``
    is exceeded.  Thread-safe for use with multi-threaded WSGI servers like
    waitress.
    """

    def __init__(self, *, max_blobs: int = 1000) -> None:  # noqa: D107
        self._blobs: OrderedDict[str, tuple[bytes, str | None]] = OrderedDict()
        self._lock = threading.Lock()
        self._max_blobs = max_blobs
        self._base_url = ""

    def set_base_url(self, base_url: str) -> None:
        """Set the base URL for blob URLs.  Call after port discovery."""
        self._base_url = base_url.rstrip("/")

    # -- ExternalStorage protocol --

    def upload(self, data: bytes, schema: Any, *, content_encoding: str | None = None) -> str:
        """Upload IPC bytes and return a fetch URL.

        Extension reflects the codec so that operators rummaging through
        the in-memory blob store can tell at a glance what they're
        looking at.  Content-Encoding is what actually drives the GET
        response header; the extension is cosmetic.
        """
        ext_for_codec = {"zstd": ".arrow.zst", "gzip": ".arrow.gz"}
        ext = ext_for_codec.get(content_encoding or "", ".arrow")
        key = f"{uuid.uuid4().hex}{ext}"
        with self._lock:
            self._blobs[key] = (data, content_encoding)
            self._evict()
        return f"{self._base_url}/__blobs__/{key}"

    # -- UploadUrlProvider protocol --

    def generate_upload_url(self, schema: Any) -> UploadUrl:
        """Generate PUT/GET URL pair for client-side uploads."""
        from vgi_rpc.external import UploadUrl

        key = f"{uuid.uuid4().hex}.arrow"
        # Create placeholder — will be filled by the client's PUT.
        with self._lock:
            self._blobs[key] = (b"", None)
            self._evict()
        url = f"{self._base_url}/__blobs__/{key}"
        return UploadUrl(
            upload_url=url,
            download_url=url,
            expires_at=datetime.now(UTC) + timedelta(hours=1),
        )

    # -- Internal accessors for BlobResource --

    def get(self, key: str) -> tuple[bytes, str | None] | None:
        """Return ``(data, content_encoding)`` or ``None``."""
        with self._lock:
            entry = self._blobs.get(key)
            if entry is not None:
                self._blobs.move_to_end(key)
            return entry

    def put(self, key: str, data: bytes, content_encoding: str | None = None) -> None:
        """Store blob data (used by PUT requests from clients)."""
        with self._lock:
            self._blobs[key] = (data, content_encoding)
            self._blobs.move_to_end(key)
            self._evict()

    def _evict(self) -> None:
        """Evict oldest entries if over capacity.  Caller must hold lock."""
        while len(self._blobs) > self._max_blobs:
            self._blobs.popitem(last=False)


class BlobResource:
    """Falcon resource serving blobs at ``/__blobs__/{blob_id}``."""

    def __init__(self, storage: DemoBlobStorage) -> None:  # noqa: D107
        self._storage = storage

    def on_get(self, req: Any, resp: Any, blob_id: str) -> None:  # noqa: D102
        import falcon

        entry = self._storage.get(blob_id)
        if entry is None:
            raise falcon.HTTPNotFound(description=f"Blob {blob_id!r} not found")
        data, content_encoding = entry
        resp.data = data
        resp.content_length = len(data)
        resp.content_type = "application/octet-stream"
        resp.set_header("Accept-Ranges", "none")
        if content_encoding:
            resp.set_header("Content-Encoding", content_encoding)
            resp.set_header("X-VGI-Content-Encoding", content_encoding)

    def on_head(self, req: Any, resp: Any, blob_id: str) -> None:  # noqa: D102
        # Mirror on_get headers (Content-Length/-Type/-Encoding) without a body.
        # Required so external_fetch._head_probe can discover Content-Encoding
        # (zstd or gzip); otherwise a 405 forces a plain GET path that skips
        # decompression.
        import falcon

        entry = self._storage.get(blob_id)
        if entry is None:
            raise falcon.HTTPNotFound(description=f"Blob {blob_id!r} not found")
        data, content_encoding = entry
        resp.content_length = len(data)
        resp.content_type = "application/octet-stream"
        resp.set_header("Accept-Ranges", "none")
        if content_encoding:
            resp.set_header("Content-Encoding", content_encoding)
            resp.set_header("X-VGI-Content-Encoding", content_encoding)

    def on_put(self, req: Any, resp: Any, blob_id: str) -> None:  # noqa: D102
        # vgi_rpc's _CompressionMiddleware drains ``req.bounded_stream`` when
        # the request carries a supported ``Content-Encoding`` (zstd or gzip)
        # and stashes the decompressed payload on
        # ``req.context.decompressed_stream``. Prefer that stream when
        # present so we capture the raw IPC bytes; the producer's
        # SHA-256 in custom_metadata is computed pre-compression so
        # downstream verification still succeeds when we serve uncompressed.
        decompressed_stream = getattr(req.context, "decompressed_stream", None)
        if decompressed_stream is not None:
            data = decompressed_stream.read()
            content_encoding: str | None = None
        else:
            data = req.bounded_stream.read()
            content_encoding = req.get_header("Content-Encoding")
        self._storage.put(blob_id, data, content_encoding)
        resp.status = "201 Created"


def add_blob_routes(app: Any, storage: DemoBlobStorage, prefix: str = "") -> None:
    """Add blob GET/PUT routes to a Falcon app."""
    app.add_route(f"{prefix}/__blobs__/{{blob_id}}", BlobResource(storage))


def localhost_only_validator(url: str) -> None:
    """URL validator that accepts only ``http://127.0.0.1`` and ``http://localhost``.

    Raises ``ValueError`` for any other URL.  Use as the ``url_validator``
    parameter of ``ExternalLocationConfig`` for demo/test use.
    """
    parsed = urlparse(url)
    if parsed.hostname not in ("127.0.0.1", "localhost"):
        msg = f"Demo storage only accepts localhost URLs, got: {url}"
        raise ValueError(msg)


class MaxRequestBytesMiddleware:
    """WSGI middleware that rejects RPC requests exceeding a size limit with 413.

    The limit models ``VGI-Max-Request-Bytes`` — the cap that drives clients to
    offload oversized batches through an upload URL. The blob upload endpoint
    (``/__blobs__/``) is the escape hatch for exactly those oversized payloads,
    so it is exempt: enforcing the limit there would 413 the very requests the
    externalization protocol relies on it to accept.
    """

    def __init__(self, app: Any, max_bytes: int) -> None:  # noqa: D107
        self._app = app
        self._max_bytes = max_bytes

    def __call__(self, environ: dict[str, Any], start_response: Any) -> Any:  # noqa: D102
        path = environ.get("PATH_INFO", "")
        if "/__blobs__/" in path:
            # Upload/download endpoint — must accept payloads larger than the
            # RPC request limit; that is its entire purpose.
            return self._app(environ, start_response)
        content_length = environ.get("CONTENT_LENGTH", "")
        if content_length:
            try:
                if int(content_length) > self._max_bytes:
                    start_response(
                        "413 Request Entity Too Large",
                        [
                            ("Content-Type", "text/plain"),
                            ("Content-Length", "24"),
                        ],
                    )
                    return [b"Request body too large.\n"]
            except ValueError:
                pass
        return self._app(environ, start_response)
