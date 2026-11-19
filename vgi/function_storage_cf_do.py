# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Cloudflare Durable Object storage for VGI function state.

This module provides a `FunctionStorage` implementation backed by a Cloudflare
Worker + Durable Object. The DO runs SQLite internally, providing the same
semantics as `FunctionStorageSqlite` but accessible over HTTP from any platform.

Implementation:
    `FunctionStorageCfDo`: HTTP client for the Cloudflare DO storage backend.

Usage:
    Set ``VGI_WORKER_SHARED_STORAGE=cloudflare-do`` plus ``VGI_CF_DO_URL``
    to enable. ``VGI_CF_DO_TOKEN`` carries the per-worker API key minted by
    the storage service's admin CLI; the multi-tenant deployment requires it.
    The key resolves (server-side) to this worker's tenant, which isolates
    its storage from other workers sharing the same deployment — the key is
    sent as an opaque ``Authorization: Bearer`` value and is never parsed
    client-side.

Workflow contract:
    Every ``execution_id`` (and ``transaction_opaque_data``) has a single linear
    lifecycle: create → push/put repeatedly → terminal op → DONE. The
    terminal op is ``queue_clear``, ``state_drain``, or
    ``execution_clear``. Ids are never reused after their terminal op.

    ``_post``'s retry loop is synchronous: all retries of one logical call
    (same ``attempt_id``) finish or exhaust before the caller can issue
    the next call. Combined with the lifecycle above, no two different
    attempts can write the same row in interleaved order — a retry of
    attempt A lands before any other attempt B can be in flight against
    the same id. That property is what makes the server's column-only
    replay model sound. If you change this client to break lockstep
    (async fire-and-forget retries, multi-coordinator writes to one
    execution_id, etc.) you also need to revisit the server's replay
    semantics in the ``vgi-cloudflare-durable-object-storage`` repo
    (``src/index.ts``).

Value envelope and blob externalization:
    The DO refuses any stored value over 1 MiB. Every value this client
    writes (work-queue items, ``state_*`` values, log items) is wrapped in a
    one-byte-tagged envelope:

    * ``0x00 || raw`` -- the value, inline;
    * ``0x01 || u64be(size) || sha256(32) || object_id`` -- a pointer to a
      blob held by the blob vendor (``VGI_BLOB_VENDOR_URL``).

    With the vendor configured, a value of ``VGI_CF_DO_EXTERNALIZE_BYTES``
    (default 512 KiB) or more is uploaded to the vendor *before* the DO
    request and the DO stores only the ~73-byte pointer; reads fetch it back
    and verify size and SHA-256. Without the vendor every value is inline
    and one whose envelope exceeds 1 MiB is refused client-side, naming
    ``VGI_BLOB_VENDOR_URL``.

    The envelope is applied whether or not the vendor is configured, so the
    stored format never depends on a per-process environment variable: two
    machines of one deployment that disagree about the vendor still read
    each other's values (a pointer read without a vendor fails clearly).
    Decoding is strict: an empty value or an unknown tag (e.g. a raw value
    written by a release before the envelope) raises `StorageEnvelopeError`.
    As with sealed framework payloads, mixing old and new clients within one
    running query is not supported; executions are short-lived and never
    reused, so nothing needs migrating.

    Sealed framework payloads (`vgi._storage_seal`) are sealed by
    ``BoundStorage`` before they reach this client, so an externalized
    sealed payload is still authenticated by its slot-bound AAD when read.

    Blobs are uploaded under the vendor scope ``(shard_key, "q.<hex
    execution_id>")`` for queue items and ``(shard_key, "s.<hex scope_id>")``
    for state and log values. ``queue_clear`` and ``execution_clear`` delete
    the matching scope, ``state_drain`` deletes the blobs it drained and
    ``queue_pop`` the blob it claimed -- all best-effort and on a background
    thread (never on the query's critical path); the vendor's idle sweep and
    the bucket lifecycle rules are the backstop.

"""

import base64
import hashlib
import json
import logging
import os
import re
import threading
import time
import uuid
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from typing import TYPE_CHECKING, Any

import httpx2

if TYPE_CHECKING:
    from vgi_rpc.vendor import BlobRef, VendorBlobClient

__all__ = [
    "FunctionStorageCfDo",
    "StorageEnvelopeError",
    "StorageValueTooLargeError",
]

_logger = logging.getLogger("vgi.storage.cf_do")


# Storage profiler (opt-in via VGI_STORAGE_PROFILE=1). The backend class is
# instrumented per logical call like every other backend (see the end of this
# module); a single logical state_scan / state_drain can fan out into several
# paginated _post round trips, so _post reports each one with note_round_trip
# and the call's record carries the true round-trip count.
from vgi._storage_profile import _PROFILE_ON, note_round_trip, profile_backend_class  # noqa: E402

# Optional file-based debug logging
_debug_log_path = os.environ.get("VGI_CF_DO_DEBUG_LOG")
if _debug_log_path:
    _fh = logging.FileHandler(_debug_log_path)
    _fh.setLevel(logging.DEBUG)
    _fh.setFormatter(logging.Formatter("%(asctime)s %(process)d %(message)s"))
    _logger.addHandler(_fh)
    _logger.setLevel(logging.DEBUG)


# ---------------------------------------------------------------------------
# Value envelope (see the module docstring)
# ---------------------------------------------------------------------------

#: The storage service's per-value cap (``MAX_VALUE_BYTES`` in its src/index.ts).
MAX_VALUE_BYTES = 1024 * 1024
#: Values of at least this many bytes go to the blob vendor when it is configured.
DEFAULT_EXTERNALIZE_BYTES = 512 * 1024
#: Largest blob a read will fetch (the vendor's per-object cap).
MAX_BLOB_BYTES = 4 * 1024**3

ENV_EXTERNALIZE_BYTES = "VGI_CF_DO_EXTERNALIZE_BYTES"
ENV_VENDOR_URL = "VGI_BLOB_VENDOR_URL"
ENV_VENDOR_TOKEN = "VGI_BLOB_VENDOR_TOKEN"

TAG_INLINE = 0x00
TAG_POINTER = 0x01
_POINTER_FIXED = 1 + 8 + 32
_OBJECT_ID_RE = re.compile(rb"[A-Za-z0-9._:-]{1,128}")
# The vendor's scope.shard charset; shard keys outside it are hashed.
_SHARD_RE = re.compile(r"[A-Za-z0-9._-]{1,128}")
# Concurrent vendor transfers per storage call.
_TRANSFER_PARALLELISM = 8


class StorageEnvelopeError(OSError):
    """A value read from the Cloudflare DO is not a valid envelope.

    Raised for an empty value, an unknown tag (such as a raw value written by
    a client older than the envelope) or a malformed pointer, and for a
    pointer read by a client with no blob vendor configured.
    """


class StorageValueTooLargeError(RuntimeError):
    """A value is too large for the Durable Object and no blob vendor is configured."""


def _encode_inline(value: bytes) -> bytes:
    return bytes((TAG_INLINE,)) + value


def _encode_pointer(ref: "BlobRef") -> bytes:
    object_id = ref.object_id.encode("ascii")
    if not _OBJECT_ID_RE.fullmatch(object_id):
        raise ValueError(f"blob vendor returned an unusable object id {ref.object_id!r}")
    return bytes((TAG_POINTER,)) + ref.size.to_bytes(8, "big") + bytes.fromhex(ref.sha256) + object_id


def _decode_pointer(envelope: bytes) -> "BlobRef":
    from vgi_rpc.vendor import BlobRef

    object_id = envelope[_POINTER_FIXED:]
    if len(envelope) <= _POINTER_FIXED or not _OBJECT_ID_RE.fullmatch(object_id):
        raise StorageEnvelopeError(f"malformed blob pointer in Cloudflare DO value ({len(envelope)} bytes)")
    return BlobRef(
        object_id=object_id.decode("ascii"),
        size=int.from_bytes(envelope[1:9], "big"),
        sha256=envelope[9:_POINTER_FIXED].hex(),
    )


def _tag_of(envelope: bytes) -> int:
    if not envelope:
        raise StorageEnvelopeError("empty Cloudflare DO value: not a value envelope (written by an older client?)")
    tag = envelope[0]
    if tag not in (TAG_INLINE, TAG_POINTER):
        raise StorageEnvelopeError(
            f"Cloudflare DO value has unknown envelope tag 0x{tag:02x} (written by an older or newer "
            "client? mixing client versions within one query is not supported)"
        )
    return tag


def _blob_execution_id(kind: str, ident: bytes) -> str:
    """Vendor ``scope.execution_id`` for one queue (``q``) or state scope (``s``)."""
    text = f"{kind}.{ident.hex()}"
    if len(text) <= 128:
        return text
    return f"{kind}.h{hashlib.sha256(ident).hexdigest()}"


def _blob_shard(shard_key: str) -> str:
    shard = shard_key or "loc-anon"
    if _SHARD_RE.fullmatch(shard):
        return shard
    return "h-" + hashlib.sha256(shard.encode()).hexdigest()


def _parallel_map[T, R](fn: Callable[[T], R], items: Sequence[T]) -> list[R]:
    if len(items) <= 1:
        return [fn(item) for item in items]
    with ThreadPoolExecutor(max_workers=min(_TRANSFER_PARALLELISM, len(items))) as pool:
        return list(pool.map(fn, items))


def _externalize_bytes_from_env() -> int:
    raw = os.environ.get(ENV_EXTERNALIZE_BYTES, "").strip()
    if not raw:
        return DEFAULT_EXTERNALIZE_BYTES
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(f"{ENV_EXTERNALIZE_BYTES} must be an integer, got {raw!r}") from None
    return value


def _blob_client_from_env() -> "VendorBlobClient | None":
    url = os.environ.get(ENV_VENDOR_URL, "").strip()
    if not url:
        return None
    token = os.environ.get(ENV_VENDOR_TOKEN, "").strip() or os.environ.get("VGI_CF_DO_TOKEN", "").strip()
    if not token:
        raise ValueError(f"{ENV_VENDOR_URL} is set but neither {ENV_VENDOR_TOKEN} nor VGI_CF_DO_TOKEN is")
    try:
        import aiohttp  # noqa: F401  (get_bytes downloads with it; fail at startup, not mid-query)
        from vgi_rpc.external_fetch import FetchConfig
        from vgi_rpc.vendor import VendorBlobClient
    except ImportError as exc:  # pragma: no cover - depends on installed extras
        raise ImportError(
            f"{ENV_VENDOR_URL} needs vgi-rpc's blob vendor client: pip install 'vgi[external]' ({exc})"
        ) from exc
    return VendorBlobClient(url, token, purpose="fs", fetch_config=FetchConfig(max_fetch_bytes=MAX_BLOB_BYTES))


class FunctionStorageCfDo:
    """Cloudflare Durable Object-backed storage for VGI function state.

    Communicates with a Cloudflare Worker that routes requests to a single
    Durable Object running SQLite. The DO is single-threaded, so all
    operations are inherently atomic — no locking needed.

    Uses a single ``httpx2.Client`` shared across threads. ``httpx2.Client`` is
    thread-safe by design (its connection pool serialises access per-conn),
    so callers from concurrent producer turns can hit this storage instance
    without coordination.

    """

    # Remote-sharding backend: every request must carry a valid shard_key, so a
    # BoundStorage built without a sealed attach is a hard error rather than a
    # silent collapse onto one DO. See _resolve_shard_key in function_storage.py.
    requires_shard_key = True

    # Connection-level retries (DNS / TCP / TLS handshake failures).
    # Status- and read-level retries are layered on top in ``_post`` so
    # 5xx responses and mid-response disconnects also recover.
    _CONNECT_RETRIES = 2
    _POST_ATTEMPTS = 3

    def __init__(
        self,
        *,
        url: str,
        token: str | None = None,
        blob_client: "VendorBlobClient | None" = None,
        externalize_bytes: int = DEFAULT_EXTERNALIZE_BYTES,
    ) -> None:
        """Initialize Cloudflare DO storage client.

        Args:
            url: Base URL of the Cloudflare Worker
                (e.g., ``https://vgi-storage.myaccount.workers.dev``).
            token: Optional bearer token for authentication.
            blob_client: Blob vendor client for values too large for the DO.
                ``None`` keeps every value inline (and refuses one over the
                DO's 1 MiB cap). Closed by :meth:`close`.
            externalize_bytes: With a ``blob_client``, values of at least
                this many bytes are stored in the vendor. At most 1 MiB.

        Raises:
            ValueError: If ``externalize_bytes`` is outside ``1..1 MiB``.

        """
        if not 1 <= externalize_bytes <= MAX_VALUE_BYTES:
            raise ValueError(
                f"externalize_bytes ({ENV_EXTERNALIZE_BYTES}) must be between 1 and {MAX_VALUE_BYTES}, "
                f"got {externalize_bytes}"
            )
        self._url = url.rstrip("/")
        self._token = token
        self._blobs = blob_client
        self._externalize_bytes = externalize_bytes
        self._cleanup_lock = threading.Lock()
        self._cleanup_pool: ThreadPoolExecutor | None = None
        self._cleanup_futures: list[Future[None]] = []
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        # HTTP/1.1 with keep-alive. HTTP/2 was tested (May 2026) and turned out
        # to regress the cold-DO path 2.5× while only marginally helping warm
        # reads (~20% on transaction_state_get/put). The bottleneck is
        # geographic RTT + DO instantiation, not TLS handshake overhead, so
        # HTTP/2 multiplexing brings little upside and Cloudflare's h2 frontend
        # appears to add latency to cold-path calls.
        self._client = httpx2.Client(
            base_url=self._url,
            headers=headers,
            timeout=httpx2.Timeout(30.0),
            limits=httpx2.Limits(
                max_keepalive_connections=20,
                max_connections=100,
                keepalive_expiry=30.0,
            ),
            transport=httpx2.HTTPTransport(retries=self._CONNECT_RETRIES),
        )

    def close(self) -> None:
        """Close the HTTP client and, after pending blob clean-up finishes, the blob client."""
        self._client.close()
        with self._cleanup_lock:
            pool, self._cleanup_pool = self._cleanup_pool, None
        if pool is not None:
            pool.shutdown(wait=True)
        if self._blobs is not None:
            self._blobs.close()

    # --- Value envelope / blob externalization -------------------------------

    def _encode_values(
        self,
        values: Sequence[bytes],
        *,
        attempt_id: str,
        shard_key: str,
        blob_execution_id: str,
    ) -> list[str]:
        """Envelope each value (uploading large ones) and base64 it for the DO.

        Uploads happen before the DO request. Each carries the idempotency key
        ``<attempt_id>.<index>``, so a retried upload of the same logical
        write reuses the vendor object instead of creating another.
        """
        large = [i for i, v in enumerate(values) if self._blobs is not None and len(v) >= self._externalize_bytes]
        large_set = set(large)
        envelopes: list[bytes | None] = [None if i in large_set else _encode_inline(v) for i, v in enumerate(values)]
        if self._blobs is None:
            for env in envelopes:
                assert env is not None
                if len(env) > MAX_VALUE_BYTES:
                    raise StorageValueTooLargeError(
                        f"Cloudflare DO storage value of {len(env) - 1} bytes exceeds the Durable Object's "
                        f"{MAX_VALUE_BYTES}-byte limit; set {ENV_VENDOR_URL} (the blob vendor) so large "
                        "values are stored as blobs"
                    )
        if large:
            from vgi_rpc.vendor import BlobScope

            blobs = self._blobs
            assert blobs is not None
            scope = BlobScope(shard=_blob_shard(shard_key), execution_id=blob_execution_id)

            def upload(i: int) -> bytes:
                ref = blobs.put_bytes(values[i], scope=scope, idempotency_key=f"{attempt_id}.{i}")
                return _encode_pointer(ref)

            for i, env in zip(large, _parallel_map(upload, large), strict=True):
                envelopes[i] = env
        out: list[str] = []
        for env in envelopes:
            assert env is not None
            out.append(base64.b64encode(env).decode())
        return out

    def _decode_values(
        self, encoded: Sequence[str | None], *, refs_out: "list[BlobRef] | None" = None
    ) -> list[bytes | None]:
        """Decode base64 envelopes from the DO, fetching pointers (in parallel).

        ``refs_out``, when given, receives the blob references that were read.
        """
        raw = [None if e is None else base64.b64decode(e) for e in encoded]
        pointers = [i for i, env in enumerate(raw) if env is not None and _tag_of(env) == TAG_POINTER]
        out: list[bytes | None] = [None if env is None else env[1:] for env in raw]
        if pointers:
            blobs = self._blobs
            if blobs is None:
                raise StorageEnvelopeError(
                    f"Cloudflare DO value is a blob pointer but {ENV_VENDOR_URL} is not configured in this process"
                )
            refs = []
            for i in pointers:
                env = raw[i]
                assert env is not None
                refs.append(_decode_pointer(env))
            for i, data in zip(pointers, _parallel_map(blobs.get_bytes, refs), strict=True):
                out[i] = data
            if refs_out is not None:
                refs_out.extend(refs)
        return out

    # Blob clean-up runs on a background thread: it is best-effort (the
    # vendor's idle sweep and the bucket lifecycle rules are the backstop), so
    # it never adds a vendor round trip to a query's critical path -- in
    # particular execution_clear, which runs at the end of every query.

    def _submit_cleanup(self, fn: Callable[[], object], what: str) -> None:
        def run() -> None:
            try:
                fn()
            except Exception as exc:
                _logger.warning("blob vendor clean-up after %s failed: %s", what, exc)

        with self._cleanup_lock:
            if self._cleanup_pool is None:
                self._cleanup_pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="vgi-blob-cleanup")
            self._cleanup_futures = [f for f in self._cleanup_futures if not f.done()]
            self._cleanup_futures.append(self._cleanup_pool.submit(run))

    def flush_blob_cleanup(self) -> None:
        """Wait for pending background blob clean-up (for tests and shutdown)."""
        with self._cleanup_lock:
            pending = list(self._cleanup_futures)
        for future in pending:
            future.result()

    def _delete_blobs(self, refs: list["BlobRef"], what: str) -> None:
        blobs = self._blobs
        if blobs is None or not refs:
            return
        self._submit_cleanup(lambda: blobs.delete(refs), what)

    def _delete_blob_scope(self, shard_key: str, blob_execution_id: str, what: str) -> None:
        blobs = self._blobs
        if blobs is None:
            return
        shard = _blob_shard(shard_key)
        self._submit_cleanup(lambda: blobs.delete_scope(shard, blob_execution_id), what)

    def __enter__(self) -> "FunctionStorageCfDo":
        """Enter the context manager."""
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        """Close the HTTP client on exit."""
        self.close()

    def _post(
        self,
        endpoint: str,
        body: dict[str, object],
        *,
        attempt_id: str | None = None,
        shard_key: str = "",
    ) -> dict[str, Any]:
        """POST JSON to the CF Worker, with retry on transient failure.

        ``attempt_id`` (when provided) is spliced into the body once, before
        the retry loop, so every retry carries the same id. This is what
        gives the server-side idempotency check something to match against:
        a retried write whose previous response was lost on the wire will
        find the prior attempt's tombstone/row and replay the original
        response instead of re-executing the operation.

        ``shard_key`` selects the Durable Object instance via
        ``idFromName(shard_key)`` on the Worker side. Empty/missing falls
        back to ``"loc-anon"`` (single shared DO for anonymous, no-attach
        callers) — see ``_derive_shard_key`` in function_storage.py.

        Returns the parsed JSON response. Raises:
            PermissionError: on 401
            ValueError: on 400 (contract violation — usually a bug)
            RuntimeError: on other 4xx (non-retryable) and exhausted retries
                          on 5xx (retryable but failed every time)
        """
        path = f"/{endpoint}"
        last_exc: Exception | None = None

        # Always send shard_key — the Worker's router rejects requests
        # without one with 400. Default to "loc-anon" so direct CfDo
        # callers (outside BoundStorage) still work.
        body = {**body, "shard_key": shard_key or "loc-anon"}
        if attempt_id is not None:
            body = {**body, "attempt_id": attempt_id}

        for attempt in range(self._POST_ATTEMPTS):
            try:
                resp = self._client.post(path, json=body)
            except httpx2.RequestError as exc:
                # Connection error, read error, timeout, etc. Narrowed to
                # ``RequestError`` (not the broader ``HTTPError``) so we
                # don't accidentally swallow programmer errors like
                # ``InvalidURL``. The transport layer already retried
                # connect-level failures; if we're here it's something the
                # higher-level retry may still help with (e.g. server
                # closed an idle keep-alive between our last response and
                # this request).
                _logger.debug(
                    "post %s attempt=%d transport error: %s: %s",
                    endpoint,
                    attempt,
                    type(exc).__name__,
                    exc,
                )
                last_exc = exc
                continue

            try:
                data: dict[str, Any] = resp.json()
            except (json.JSONDecodeError, ValueError):
                # Non-JSON response (HTML error page, empty body, etc.) —
                # treat as a transient server problem rather than letting
                # JSONDecodeError bubble up unhelpfully.
                _logger.debug(
                    "post %s attempt=%d non-json status=%d body=%r",
                    endpoint,
                    attempt,
                    resp.status_code,
                    resp.content[:200],
                )
                last_exc = RuntimeError(
                    f"CF DO storage returned non-JSON response (status={resp.status_code}): {resp.content[:200]!r}"
                )
                continue

            if resp.status_code == 401:
                raise PermissionError(f"Authentication failed: {data.get('error', 'unauthorized')}")
            if resp.status_code == 400:
                # Client-contract violation (e.g. missing/invalid attempt_id).
                # Not retryable and almost always a bug worth surfacing loudly.
                raise ValueError(f"CF DO storage rejected request: {data.get('message') or data.get('error') or data}")
            if 500 <= resp.status_code < 600:
                # Transient server error — retry.
                _logger.debug(
                    "post %s attempt=%d server error status=%d data=%r",
                    endpoint,
                    attempt,
                    resp.status_code,
                    data,
                )
                last_exc = RuntimeError(f"CF DO storage error {resp.status_code}: {data}")
                continue
            if resp.status_code >= 400:
                # Other 4xx — don't retry, the request itself is bad.
                raise RuntimeError(f"CF DO storage error {resp.status_code}: {data}")
            if _PROFILE_ON:
                # Largest single wire body, either direction — what provider
                # request/response size caps apply to. resp.request.content is
                # the actual serialized (base64-inflated) request payload.
                req_len = len(resp.request.content) if resp.request is not None else 0
                note_round_trip(max(req_len, len(resp.content)))
            return data

        assert last_exc is not None
        raise last_exc

    # --- Work Queue ---

    def queue_push(self, execution_id: bytes, items: list[bytes], *, shard_key: str = "") -> int:
        """Add work items to the queue and register the invocation."""
        t0 = time.monotonic()
        attempt_id = uuid.uuid4().hex
        encoded = self._encode_values(
            items,
            attempt_id=attempt_id,
            shard_key=shard_key,
            blob_execution_id=_blob_execution_id("q", execution_id),
        )
        data = self._post(
            "queue_push",
            {
                "execution_id": base64.b64encode(execution_id).decode(),
                "items": encoded,
            },
            attempt_id=attempt_id,
            shard_key=shard_key,
        )
        count = int(data["count"])
        _logger.debug(
            "queue_push eid=%s items=%d elapsed_ms=%.1f",
            execution_id.hex()[:8],
            count,
            (time.monotonic() - t0) * 1000,
        )
        return count

    def queue_pop(self, execution_id: bytes, *, shard_key: str = "") -> bytes | None:
        """Atomically claim one work item from the queue.

        Returns None when the queue is empty *or* the execution_id was
        never pushed — see the base-class docstring.
        """
        t0 = time.monotonic()
        data = self._post(
            "queue_pop",
            {
                "execution_id": base64.b64encode(execution_id).decode(),
            },
            attempt_id=uuid.uuid4().hex,
            shard_key=shard_key,
        )
        result: bytes | None = None
        if data["item"]:
            claimed: list[BlobRef] = []
            result = self._decode_values([data["item"]], refs_out=claimed)[0]
            # The item is claimed; its blob (if any) is not needed again.
            self._delete_blobs(claimed, "queue_pop")
        got_item = result is not None
        _logger.debug(
            "queue_pop eid=%s result=%s elapsed_ms=%.1f",
            execution_id.hex()[:8],
            "item" if got_item else "empty",
            (time.monotonic() - t0) * 1000,
        )
        return result

    def queue_clear(self, execution_id: bytes, *, shard_key: str = "") -> int:
        """Clear all remaining work items and unregister the invocation."""
        t0 = time.monotonic()
        data = self._post(
            "queue_clear",
            {
                "execution_id": base64.b64encode(execution_id).decode(),
            },
            attempt_id=uuid.uuid4().hex,
            shard_key=shard_key,
        )
        cleared = int(data["cleared"])
        self._delete_blob_scope(shard_key, _blob_execution_id("q", execution_id), "queue_clear")
        _logger.debug(
            "queue_clear eid=%s cleared=%d elapsed_ms=%.1f",
            execution_id.hex()[:8],
            cleared,
            (time.monotonic() - t0) * 1000,
        )
        return cleared

    # ========================================================================
    # Unified state_* client (composite-key K/V over (scope_id, ns, key))
    # ========================================================================
    #
    # Mirrors the server-side handlers in
    # vgi-cloudflare-durable-object-storage/src/index.ts. attempt_id is
    # generated client-side and spliced into the request body before the
    # retry loop in _post() so a retried HTTP call carries the same id and
    # the server's replay-detection returns the prior result rather than
    # re-executing. Read-only methods (state_get_many, state_scan,
    # state_log_scan) don't carry attempt_id.

    def state_get_many(
        self,
        scope_id: bytes,
        ns: bytes,
        keys: list[bytes],
        *,
        shard_key: str = "",
    ) -> list[bytes | None]:
        """Batched non-destructive read of values keyed by ``(scope_id, ns, key)``.

        Single HTTP roundtrip regardless of key count.
        """
        if not keys:
            return []
        t0 = time.monotonic()
        data = self._post(
            "state_get_many",
            {
                "scope_id": base64.b64encode(scope_id).decode(),
                "ns": base64.b64encode(ns).decode(),
                "keys": [base64.b64encode(k).decode() for k in keys],
            },
            shard_key=shard_key,
        )
        # Server returns rows as a list parallel to the input keys.
        rows = data["rows"]
        result = self._decode_values([None if r is None else r["value"] for r in rows])
        _logger.debug(
            "state_get_many scope=%s ns=%s n_keys=%d hits=%d elapsed_ms=%.1f",
            scope_id.hex()[:8],
            ns.hex()[:8],
            len(keys),
            sum(1 for r in result if r is not None),
            (time.monotonic() - t0) * 1000,
        )
        return result

    def state_put_many(
        self,
        scope_id: bytes,
        ns: bytes,
        items: list[tuple[bytes, bytes]],
        *,
        shard_key: str = "",
    ) -> None:
        """Batched atomic upsert of ``(key, value)`` pairs in one namespace."""
        if not items:
            return
        t0 = time.monotonic()
        attempt_id = uuid.uuid4().hex
        encoded = self._encode_values(
            [v for _k, v in items],
            attempt_id=attempt_id,
            shard_key=shard_key,
            blob_execution_id=_blob_execution_id("s", scope_id),
        )
        self._post(
            "state_put_many",
            {
                "scope_id": base64.b64encode(scope_id).decode(),
                "ns": base64.b64encode(ns).decode(),
                "items": [
                    {"key": base64.b64encode(k).decode(), "value": v} for (k, _v), v in zip(items, encoded, strict=True)
                ],
            },
            attempt_id=attempt_id,
            shard_key=shard_key,
        )
        _logger.debug(
            "state_put_many scope=%s ns=%s n_items=%d elapsed_ms=%.1f",
            scope_id.hex()[:8],
            ns.hex()[:8],
            len(items),
            (time.monotonic() - t0) * 1000,
        )

    def state_scan(
        self,
        scope_id: bytes,
        ns: bytes,
        *,
        start: bytes | None = None,
        end: bytes | None = None,
        reverse: bool = False,
        limit: int | None = None,
        shard_key: str = "",
    ) -> Iterator[tuple[bytes, bytes]]:
        """Stream (key, value) in one namespace, paging under the hood.

        Ordered by key (``reverse=True`` descending), bounded to ``[start, end)``
        and capped at ``limit`` rows. The server returns ordered pages bounded by
        a byte budget plus a ``next_after`` continuation cursor (interpreted
        server-side per ``reverse``), so an arbitrarily large range never builds
        an oversized response. Yields lazily; consumers should iterate.

        Args:
            scope_id: Storage scope identifier (tenant/catalog isolation).
            ns: Namespace within the scope to scan.
            start: Inclusive lower key bound; ``None`` leaves it unbounded.
            end: Exclusive upper key bound; ``None`` leaves it unbounded.
            reverse: Iterate descending by key when ``True``.
            limit: Maximum number of rows to yield; ``None`` is unbounded.
            shard_key: Routing key selecting the backing Durable Object shard.

        Yields:
            ``(key, value)`` byte tuples in key order.
        """
        t0 = time.monotonic()
        after_key: str | None = None
        n = 0
        remaining = limit
        while True:
            body: dict[str, object] = {
                "scope_id": base64.b64encode(scope_id).decode(),
                "ns": base64.b64encode(ns).decode(),
            }
            if start is not None:
                body["start"] = base64.b64encode(start).decode()
            if end is not None:
                body["end"] = base64.b64encode(end).decode()
            if reverse:
                body["reverse"] = True
            if remaining is not None:
                body["limit"] = int(remaining)
            if after_key is not None:
                body["after_key"] = after_key
            data = self._post("state_scan", body, shard_key=shard_key)
            rows = data["rows"]
            values = self._decode_values([r["value"] for r in rows])
            for r, value in zip(rows, values, strict=True):
                assert value is not None
                yield (base64.b64decode(r["key"]), value)
                n += 1
                if remaining is not None:
                    remaining -= 1
            after_key = data.get("next_after")
            if not after_key or (remaining is not None and remaining <= 0):
                break
        _logger.debug(
            "state_scan scope=%s ns=%s rows=%d elapsed_ms=%.1f",
            scope_id.hex()[:8],
            ns.hex()[:8],
            n,
            (time.monotonic() - t0) * 1000,
        )

    def state_drain(
        self,
        scope_id: bytes,
        ns: bytes,
        *,
        shard_key: str = "",
    ) -> Iterator[tuple[bytes, bytes]]:
        """Stream-and-tombstone every (key, value) in one namespace, paged.

        A single ``attempt_id`` is minted once and reused across every page so
        the server's snapshot-then-page drain stays atomic and replay-safe: the
        first page tombstones the whole namespace, later pages read the
        tombstoned snapshot. A retried page (same attempt_id + cursor) replays
        identically. Beginning to iterate commits the drain, so consume fully.

        Args:
            scope_id: Storage scope identifier (tenant/catalog isolation).
            ns: Namespace within the scope to drain.
            shard_key: Routing key selecting the backing Durable Object shard.

        Yields:
            ``(key, value)`` byte tuples as each page is drained.
        """
        t0 = time.monotonic()
        attempt_id = uuid.uuid4().hex
        after_key: str | None = None
        drained_refs: list[BlobRef] = []
        n = 0
        while True:
            body: dict[str, object] = {
                "scope_id": base64.b64encode(scope_id).decode(),
                "ns": base64.b64encode(ns).decode(),
            }
            if after_key is not None:
                body["after_key"] = after_key
            data = self._post("state_drain", body, attempt_id=attempt_id, shard_key=shard_key)
            rows = data["rows"]
            values = self._decode_values([r["value"] for r in rows], refs_out=drained_refs)
            for r, value in zip(rows, values, strict=True):
                assert value is not None
                yield (base64.b64decode(r["key"]), value)
                n += 1
            after_key = data.get("next_after")
            if not after_key:
                break
        # The drained rows are tombstoned: their blobs are not read again.
        self._delete_blobs(drained_refs, "state_drain")
        _logger.debug(
            "state_drain scope=%s ns=%s rows=%d elapsed_ms=%.1f",
            scope_id.hex()[:8],
            ns.hex()[:8],
            n,
            (time.monotonic() - t0) * 1000,
        )

    def state_delete(
        self,
        scope_id: bytes,
        ns: bytes,
        keys: list[bytes] | None = None,
        *,
        start: bytes | None = None,
        end: bytes | None = None,
        shard_key: str = "",
    ) -> int:
        """Delete by key list, by ``[start, end)`` range, or wipe the namespace.

        ``keys`` and the range are mutually exclusive. Naturally idempotent — an
        attempt_id is sent for audit but the server doesn't gate on it
        (delete-of-already-deleted is a no-op).
        """
        if keys is not None and (start is not None or end is not None):
            raise ValueError("state_delete: keys and start/end are mutually exclusive")
        t0 = time.monotonic()
        body: dict[str, object] = {
            "scope_id": base64.b64encode(scope_id).decode(),
            "ns": base64.b64encode(ns).decode(),
        }
        mode = "all"
        if keys is not None:
            body["keys"] = [base64.b64encode(k).decode() for k in keys]
            mode = f"n_keys={len(keys)}"
        elif start is not None or end is not None:
            if start is not None:
                body["start"] = base64.b64encode(start).decode()
            if end is not None:
                body["end"] = base64.b64encode(end).decode()
            mode = "range"
        data = self._post(
            "state_delete",
            body,
            attempt_id=uuid.uuid4().hex,
            shard_key=shard_key,
        )
        deleted = int(data["deleted"])
        _logger.debug(
            "state_delete scope=%s ns=%s mode=%s deleted=%d elapsed_ms=%.1f",
            scope_id.hex()[:8],
            ns.hex()[:8],
            mode,
            deleted,
            (time.monotonic() - t0) * 1000,
        )
        return deleted

    def execution_clear(
        self,
        scope_id: bytes,
        *,
        shard_key: str = "",
    ) -> int:
        """Wipe ALL state and log rows for ``scope_id`` across every namespace."""
        t0 = time.monotonic()
        data = self._post(
            "execution_clear",
            {"scope_id": base64.b64encode(scope_id).decode()},
            attempt_id=uuid.uuid4().hex,
            shard_key=shard_key,
        )
        deleted = int(data["deleted"])
        self._delete_blob_scope(shard_key, _blob_execution_id("s", scope_id), "execution_clear")
        _logger.debug(
            "execution_clear scope=%s deleted=%d elapsed_ms=%.1f",
            scope_id.hex()[:8],
            deleted,
            (time.monotonic() - t0) * 1000,
        )
        return deleted

    def state_append(
        self,
        scope_id: bytes,
        ns: bytes,
        key: bytes,
        item: bytes,
        *,
        shard_key: str = "",
    ) -> int:
        """Append item to (scope_id, ns, key) log; return assigned ordinal.

        Replay: if a prior call with the same attempt_id already inserted,
        the server returns the prior ordinal so retries are idempotent.
        """
        t0 = time.monotonic()
        attempt_id = uuid.uuid4().hex
        (encoded,) = self._encode_values(
            [item],
            attempt_id=attempt_id,
            shard_key=shard_key,
            blob_execution_id=_blob_execution_id("s", scope_id),
        )
        data = self._post(
            "state_append",
            {
                "scope_id": base64.b64encode(scope_id).decode(),
                "ns": base64.b64encode(ns).decode(),
                "key": base64.b64encode(key).decode(),
                "item": encoded,
            },
            attempt_id=attempt_id,
            shard_key=shard_key,
        )
        ordinal = int(data["ordinal"])
        _logger.debug(
            "state_append scope=%s ns=%s key=%s ordinal=%d elapsed_ms=%.1f",
            scope_id.hex()[:8],
            ns.hex()[:8],
            key.hex()[:8],
            ordinal,
            (time.monotonic() - t0) * 1000,
        )
        return ordinal

    def state_log_scan(
        self,
        scope_id: bytes,
        ns: bytes,
        key: bytes,
        *,
        after_id: int = -1,
        limit: int | None = None,
        shard_key: str = "",
    ) -> list[tuple[int, bytes]]:
        """Return (id, value) pairs for (scope_id, ns, key) with id > after_id."""
        t0 = time.monotonic()
        body: dict[str, object] = {
            "scope_id": base64.b64encode(scope_id).decode(),
            "ns": base64.b64encode(ns).decode(),
            "key": base64.b64encode(key).decode(),
            "after_id": after_id,
        }
        if limit is not None:
            body["limit"] = int(limit)
        data = self._post("state_log_scan", body, shard_key=shard_key)
        values = self._decode_values([r["value"] for r in data["rows"]])
        rows = [(int(r["id"]), v) for r, v in zip(data["rows"], values, strict=True) if v is not None]
        _logger.debug(
            "state_log_scan scope=%s ns=%s key=%s after_id=%d rows=%d elapsed_ms=%.1f",
            scope_id.hex()[:8],
            ns.hex()[:8],
            key.hex()[:8],
            after_id,
            len(rows),
            (time.monotonic() - t0) * 1000,
        )
        return rows

    # --- Atomic int64 counters (function_counter) ---

    def state_counter_get(self, scope_id: bytes, ns: bytes, key: bytes, *, shard_key: str = "") -> int:
        """Read the int64 counter; 0 if absent."""
        data = self._post(
            "state_counter_get",
            {
                "scope_id": base64.b64encode(scope_id).decode(),
                "ns": base64.b64encode(ns).decode(),
                "key": base64.b64encode(key).decode(),
            },
            shard_key=shard_key,
        )
        return int(data["n"])

    def state_counter_add(self, scope_id: bytes, ns: bytes, key: bytes, delta: int, *, shard_key: str = "") -> int:
        """Atomically add ``delta`` and return the new value.

        Carries an ``attempt_id`` so an HTTP retry replays the prior result on
        the server instead of double-adding.
        """
        data = self._post(
            "state_counter_add",
            {
                "scope_id": base64.b64encode(scope_id).decode(),
                "ns": base64.b64encode(ns).decode(),
                "key": base64.b64encode(key).decode(),
                "delta": int(delta),
            },
            attempt_id=uuid.uuid4().hex,
            shard_key=shard_key,
        )
        return int(data["n"])

    def state_counter_set(self, scope_id: bytes, ns: bytes, key: bytes, value: int, *, shard_key: str = "") -> None:
        """Overwrite the counter with ``value`` (idempotent)."""
        self._post(
            "state_counter_set",
            {
                "scope_id": base64.b64encode(scope_id).decode(),
                "ns": base64.b64encode(ns).decode(),
                "key": base64.b64encode(key).decode(),
                "value": int(value),
            },
            attempt_id=uuid.uuid4().hex,
            shard_key=shard_key,
        )

    def state_counter_delete(self, scope_id: bytes, ns: bytes, key: bytes, *, shard_key: str = "") -> None:
        """Delete the counter (no-op if absent)."""
        self._post(
            "state_counter_delete",
            {
                "scope_id": base64.b64encode(scope_id).decode(),
                "ns": base64.b64encode(ns).decode(),
                "key": base64.b64encode(key).decode(),
            },
            attempt_id=uuid.uuid4().hex,
            shard_key=shard_key,
        )

    # --- Factory ---

    @classmethod
    def from_env(cls) -> "FunctionStorageCfDo":
        """Create an instance from environment variables.

        Required:
            VGI_CF_DO_URL: Base URL of the Cloudflare Worker.

        Optional:
            VGI_CF_DO_TOKEN: Per-worker API key (sent as a bearer token).
                Required by the multi-tenant cloudflare-do deployment, where
                it resolves server-side to this worker's tenant.
            VGI_BLOB_VENDOR_URL: Blob vendor for values too large for the DO
                (see the module docstring). Its token is
                ``VGI_BLOB_VENDOR_TOKEN``, falling back to ``VGI_CF_DO_TOKEN``.
            VGI_CF_DO_EXTERNALIZE_BYTES: Externalization threshold with the
                vendor (default 512 KiB, at most 1 MiB).

        """
        url = os.environ.get("VGI_CF_DO_URL")
        if not url:
            raise ValueError(
                "VGI_CF_DO_URL environment variable is required when VGI_WORKER_SHARED_STORAGE=cloudflare-do"
            )
        return cls(
            url=url,
            token=os.environ.get("VGI_CF_DO_TOKEN") or None,
            blob_client=_blob_client_from_env(),
            externalize_bytes=_externalize_bytes_from_env(),
        )


profile_backend_class(FunctionStorageCfDo)
