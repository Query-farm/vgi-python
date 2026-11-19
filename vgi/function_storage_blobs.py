# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Value envelope and blob-vendor externalization for any `FunctionStorage`.

`ExternalizingStorage` wraps a backend and stores values too large for it in
the *blob vendor* (``VGI_BLOB_VENDOR_URL``), keeping only a small pointer in
the backend. The ``FunctionStorage`` API does not change.

Envelope:
    Every value the wrapper writes (work-queue items, ``state_*`` values, log
    items) carries a one-byte tag:

    * ``0x00 || raw`` -- the value, inline;
    * ``0x01 || u64be(size) || sha256(32) || object_id`` -- a pointer to a
      blob in the vendor (~73 bytes).

    With a vendor client, a value of ``threshold`` bytes or more is uploaded
    *before* the backend call; reads fetch it back and verify size and
    SHA-256. Without one every value is inline, and when the backend has a
    per-value cap (the Cloudflare DO's 1 MiB) a value whose envelope exceeds
    it is refused client-side with `StorageValueTooLargeError`, naming
    ``VGI_BLOB_VENDOR_URL``.

    Decoding is strict: an empty value or an unknown tag (e.g. a raw value
    written without the wrapper) raises `StorageEnvelopeError`. A pointer
    whose blob cannot be read raises a `StorageBlobError` (see "Errors"). User bytes
    that start with ``0x01`` are safe because every value is enveloped.
    Mixing wrapped and unwrapped clients over one store in one running query
    is not supported; executions are short-lived and never reused, so no
    stored value needs migrating.

Where it applies (see `wrap_storage_from_env`):
    * ``cloudflare-do``: always wrapped, with or without the vendor, so the
      stored format never depends on a per-process environment variable (two
      machines that disagree about the vendor still read each other's
      values; a pointer read without a vendor fails clearly).
    * Any other backend: only with ``VGI_STORAGE_EXTERNALIZE=1`` (which then
      requires the vendor). SQLite's on-disk format is unchanged by default.

Sealed framework payloads (`vgi._storage_seal`) are sealed by
``BoundStorage`` before they reach the wrapper, so an externalized sealed
payload is still authenticated by its slot-bound AAD when read.

Clean-up:
    Blobs are uploaded under the vendor scope ``(shard_key, "q.<hex
    execution_id>")`` for queue items and ``(shard_key, "s.<hex scope_id>")``
    for state and log values. ``queue_clear`` and ``execution_clear`` delete
    the matching scope, ``state_drain`` deletes the blobs it drained and
    ``queue_pop`` the blob it claimed -- all best-effort and on a background
    thread (never on the query's critical path); the vendor's idle sweep and
    the bucket lifecycle rules are the backstop.

Errors:
    The messages follow the SDK-neutral contract in the vgi extension's
    ``docs/testing/blob-test-catalog.md``, so the same sqllogictests check
    every SDK:

    * `StorageBlobIntegrityError` -- ``storage blob <id> failed its
      integrity check: sha256 mismatch ...`` or ``... size mismatch ...``;
    * `StorageBlobMissingError` -- ``storage blob <id> is missing: not
      found in the blob vendor ...`` (expired, deleted, never uploaded);
    * `StorageBlobError` -- ``storage blob <id> could not be read: ...``.

Environment:
    ``VGI_BLOB_VENDOR_URL``, ``VGI_BLOB_VENDOR_TOKEN`` (falls back to
    ``VGI_CF_DO_TOKEN``), ``VGI_STORAGE_EXTERNALIZE`` (``1`` wraps any
    backend), ``VGI_STORAGE_EXTERNALIZE_BYTES`` (threshold, default 512 KiB;
    ``VGI_CF_DO_EXTERNALIZE_BYTES`` is an alias).
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import threading
import uuid
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from vgi_rpc.vendor import BlobRef, VendorBlobClient

    from vgi.function_storage import FunctionStorage

__all__ = [
    "DEFAULT_EXTERNALIZE_BYTES",
    "ExternalizingStorage",
    "StorageBlobError",
    "StorageBlobIntegrityError",
    "StorageBlobMissingError",
    "StorageEnvelopeError",
    "StorageValueTooLargeError",
    "blob_client_from_env",
    "externalize_bytes_from_env",
    "wrap_storage_from_env",
]

_logger = logging.getLogger("vgi.storage.blobs")

#: Values of at least this many bytes go to the blob vendor.
DEFAULT_EXTERNALIZE_BYTES = 512 * 1024
#: Largest blob a read will fetch (the vendor's per-object cap).
MAX_BLOB_BYTES = 4 * 1024**3

ENV_VENDOR_URL = "VGI_BLOB_VENDOR_URL"
ENV_VENDOR_TOKEN = "VGI_BLOB_VENDOR_TOKEN"
ENV_EXTERNALIZE = "VGI_STORAGE_EXTERNALIZE"
ENV_EXTERNALIZE_BYTES = "VGI_STORAGE_EXTERNALIZE_BYTES"
ENV_EXTERNALIZE_BYTES_ALIAS = "VGI_CF_DO_EXTERNALIZE_BYTES"

TAG_INLINE = 0x00
TAG_POINTER = 0x01
_POINTER_FIXED = 1 + 8 + 32
_OBJECT_ID_RE = re.compile(rb"[A-Za-z0-9._:-]{1,128}")
# The vendor's scope.shard charset; shard keys outside it are hashed.
_SHARD_RE = re.compile(r"[A-Za-z0-9._-]{1,128}")
# Concurrent vendor transfers per storage call.
_TRANSFER_PARALLELISM = 8
_TRUE = frozenset({"1", "true", "yes", "on"})


class StorageEnvelopeError(OSError):
    """A value read from storage is not a valid envelope.

    Raised for an empty value, an unknown tag (such as a raw value written
    without the envelope) or a malformed pointer, and for a pointer read by a
    client with no blob vendor configured.
    """


class StorageBlobError(OSError):
    """An externalized storage value's blob could not be read.

    The message starts ``storage blob <object id>``. Subclasses name the
    failures a test (or an operator) needs to tell apart.
    """


class StorageBlobIntegrityError(StorageBlobError):
    """The downloaded blob does not match the pointer's size or SHA-256."""


class StorageBlobMissingError(StorageBlobError):
    """The blob is gone from the vendor or its storage (expired, deleted, never uploaded)."""


_MISSING_CODES = frozenset({"not_found", "not_uploaded", "NoSuchKey"})


def _blob_read_error(ref: BlobRef, exc: Exception) -> StorageBlobError:
    """Map a vendor client failure onto the storage-blob error contract."""
    code = getattr(exc, "code", None)
    status = getattr(exc, "status", None)
    text = str(exc)
    # vgi-rpc >= 0.56.1 sets the code; older releases only say so in the message.
    if code == "sha256_mismatch" or (code is None and "SHA-256 mismatch" in text):
        return StorageBlobIntegrityError(f"storage blob {ref.object_id} failed its integrity check: sha256 mismatch")
    if code == "size_mismatch" or (code is None and "size mismatch" in text):
        return StorageBlobIntegrityError(
            f"storage blob {ref.object_id} failed its integrity check: size mismatch (expected {ref.size} bytes)"
        )
    if status == 404 or code in _MISSING_CODES:
        return StorageBlobMissingError(
            f"storage blob {ref.object_id} is missing: not found in the blob vendor (expired or deleted?)"
        )
    return StorageBlobError(f"storage blob {ref.object_id} could not be read: {text}")


class StorageValueTooLargeError(RuntimeError):
    """A value is too large for the backend and no blob vendor is configured."""


# ---------------------------------------------------------------------------
# Envelope
# ---------------------------------------------------------------------------


def encode_inline(value: bytes) -> bytes:
    """Return the inline envelope of *value*."""
    return bytes((TAG_INLINE,)) + value


def encode_pointer(ref: BlobRef) -> bytes:
    """Return the pointer envelope for a vendored blob.

    Args:
        ref: The blob's reference from the vendor.

    Returns:
        ``0x01 || u64be(size) || sha256 || object_id``.

    Raises:
        ValueError: If the vendor's object id is outside the pointer charset.
    """
    object_id = ref.object_id.encode("ascii")
    if not _OBJECT_ID_RE.fullmatch(object_id):
        raise ValueError(f"blob vendor returned an unusable object id {ref.object_id!r}")
    return bytes((TAG_POINTER,)) + ref.size.to_bytes(8, "big") + bytes.fromhex(ref.sha256) + object_id


def decode_pointer(envelope: bytes) -> BlobRef:
    """Parse a pointer envelope.

    Args:
        envelope: A stored value whose tag is ``0x01``.

    Returns:
        The blob reference it names.

    Raises:
        StorageEnvelopeError: If the pointer is truncated or its object id is malformed.
    """
    from vgi_rpc.vendor import BlobRef

    object_id = envelope[_POINTER_FIXED:]
    if len(envelope) <= _POINTER_FIXED or not _OBJECT_ID_RE.fullmatch(object_id):
        raise StorageEnvelopeError(f"malformed blob pointer in stored value ({len(envelope)} bytes)")
    return BlobRef(
        object_id=object_id.decode("ascii"),
        size=int.from_bytes(envelope[1:9], "big"),
        sha256=envelope[9:_POINTER_FIXED].hex(),
    )


def envelope_tag(envelope: bytes) -> int:
    """Return the envelope's tag.

    Args:
        envelope: A stored value.

    Returns:
        `TAG_INLINE` or `TAG_POINTER`.

    Raises:
        StorageEnvelopeError: If the value is empty or the tag is unknown.
    """
    if not envelope:
        raise StorageEnvelopeError("empty stored value: not a value envelope (written without the envelope?)")
    tag = envelope[0]
    if tag not in (TAG_INLINE, TAG_POINTER):
        raise StorageEnvelopeError(
            f"stored value has unknown envelope tag 0x{tag:02x} (written by an older client or without "
            "the envelope? mixing client versions within one query is not supported)"
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


# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------


def externalize_bytes_from_env() -> int:
    """Read ``VGI_STORAGE_EXTERNALIZE_BYTES`` (alias ``VGI_CF_DO_EXTERNALIZE_BYTES``).

    Returns:
        The threshold, or `DEFAULT_EXTERNALIZE_BYTES` when unset.

    Raises:
        ValueError: If the value is not a positive integer.
    """
    for name in (ENV_EXTERNALIZE_BYTES, ENV_EXTERNALIZE_BYTES_ALIAS):
        raw = os.environ.get(name, "").strip()
        if not raw:
            continue
        try:
            value = int(raw)
        except ValueError:
            raise ValueError(f"{name} must be an integer, got {raw!r}") from None
        if value <= 0:
            raise ValueError(f"{name} must be positive, got {value}")
        return value
    return DEFAULT_EXTERNALIZE_BYTES


def blob_client_from_env() -> VendorBlobClient | None:
    """Build the vendor client from ``VGI_BLOB_VENDOR_URL`` and its token.

    Returns:
        The client, or ``None`` when ``VGI_BLOB_VENDOR_URL`` is unset.

    Raises:
        ValueError: If the URL is set but no token is.
        ImportError: If vgi-rpc's blob vendor client cannot be imported.
    """
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


def wrap_storage_from_env(base: FunctionStorage, *, max_value_bytes: int | None = None) -> FunctionStorage:
    """Wrap *base* in `ExternalizingStorage` when the environment asks for it.

    A backend with a per-value cap (*max_value_bytes*, i.e. the Cloudflare DO)
    is always wrapped, vendor or not, so its stored format is the same in
    every process. Any other backend is wrapped only with
    ``VGI_STORAGE_EXTERNALIZE=1``, which then requires ``VGI_BLOB_VENDOR_URL``.

    Args:
        base: The backend.
        max_value_bytes: The backend's per-value cap, or ``None`` for none.

    Returns:
        *base* or the wrapper.

    Raises:
        ValueError: ``VGI_STORAGE_EXTERNALIZE=1`` without a vendor, or a
            malformed variable.
    """
    enabled = os.environ.get(ENV_EXTERNALIZE, "").strip().lower() in _TRUE
    if max_value_bytes is None and not enabled:
        return base
    blobs = blob_client_from_env()
    if enabled and blobs is None:
        raise ValueError(f"{ENV_EXTERNALIZE}=1 needs the blob vendor: set {ENV_VENDOR_URL}")
    threshold = externalize_bytes_from_env()
    if max_value_bytes is not None:
        threshold = min(threshold, max_value_bytes)
    return ExternalizingStorage(base, blob_client=blobs, threshold=threshold, max_value_bytes=max_value_bytes)


# ---------------------------------------------------------------------------
# The wrapper
# ---------------------------------------------------------------------------


class ExternalizingStorage:
    """A `FunctionStorage` that envelopes values and externalizes large ones.

    Delegates every operation to *base*; see the module docstring for the
    envelope, scoping and clean-up. Counters pass through untouched.

    Thread-safe when *base* is: the wrapper's own state is the background
    clean-up pool, guarded by a lock.
    """

    def __init__(
        self,
        base: FunctionStorage,
        *,
        blob_client: VendorBlobClient | None,
        threshold: int = DEFAULT_EXTERNALIZE_BYTES,
        max_value_bytes: int | None = None,
    ) -> None:
        """Wrap a backend.

        Args:
            base: The backend that stores the envelopes.
            blob_client: The vendor client, or ``None`` to keep every value
                inline. Closed by :meth:`close`.
            threshold: With a vendor, values of at least this many bytes are
                stored as blobs.
            max_value_bytes: The backend's per-value cap. The envelope of an
                inline value must fit, so *threshold* may not exceed it; with
                no vendor, an over-cap value raises
                `StorageValueTooLargeError`.

        Raises:
            ValueError: If *threshold* is not positive or exceeds *max_value_bytes*.
        """
        if threshold < 1 or (max_value_bytes is not None and threshold > max_value_bytes):
            raise ValueError(
                f"externalize threshold ({ENV_EXTERNALIZE_BYTES}) must be between 1 and "
                f"{max_value_bytes if max_value_bytes is not None else 'unbounded'}, got {threshold}"
            )
        self._base = base
        self._blobs = blob_client
        self._threshold = threshold
        self._max_value_bytes = max_value_bytes
        self._cleanup_lock = threading.Lock()
        self._cleanup_pool: ThreadPoolExecutor | None = None
        self._cleanup_futures: list[Future[None]] = []

    @property
    def base(self) -> FunctionStorage:
        """The wrapped backend."""
        return self._base

    @property
    def blob_client(self) -> VendorBlobClient | None:
        """The vendor client, if any."""
        return self._blobs

    @property
    def threshold(self) -> int:
        """Values of at least this many bytes are externalized (with a vendor)."""
        return self._threshold

    @property
    def requires_shard_key(self) -> bool:
        """Whether the wrapped backend shards remotely (see ``_resolve_shard_key``)."""
        return bool(getattr(self._base, "requires_shard_key", False))

    def close(self) -> None:
        """Close the backend and, after pending clean-up finishes, the vendor client."""
        close = getattr(self._base, "close", None)
        if close is not None:
            close()
        with self._cleanup_lock:
            pool, self._cleanup_pool = self._cleanup_pool, None
        if pool is not None:
            pool.shutdown(wait=True)
        if self._blobs is not None:
            self._blobs.close()

    def __enter__(self) -> ExternalizingStorage:
        """Enter the context manager."""
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        """Close on exit."""
        self.close()

    # --- encode / decode -----------------------------------------------------

    def encode_values(self, values: Sequence[bytes], *, shard_key: str, blob_execution_id: str) -> list[bytes]:
        """Envelope each value, uploading large ones first.

        Each upload carries the idempotency key ``<call id>.<index>``, so a
        vendor-level retry of the same logical write reuses the object.

        Args:
            values: The values to store.
            shard_key: The storage shard (the vendor scope's shard).
            blob_execution_id: The vendor scope's execution id.

        Returns:
            The envelopes, parallel to *values*.

        Raises:
            StorageValueTooLargeError: No vendor and an envelope over the backend's cap.
        """
        blobs = self._blobs
        large = [i for i, v in enumerate(values) if blobs is not None and len(v) >= self._threshold]
        large_set = set(large)
        envelopes: list[bytes | None] = [None if i in large_set else encode_inline(v) for i, v in enumerate(values)]
        if blobs is None and self._max_value_bytes is not None:
            for env in envelopes:
                assert env is not None
                if len(env) > self._max_value_bytes:
                    raise StorageValueTooLargeError(
                        f"storage value of {len(env) - 1} bytes exceeds the backend's {self._max_value_bytes}-byte "
                        f"limit; set {ENV_VENDOR_URL} (the blob vendor) so large values are stored as blobs"
                    )
        if large:
            from vgi_rpc.vendor import BlobScope

            assert blobs is not None
            call_id = uuid.uuid4().hex
            scope = BlobScope(shard=_blob_shard(shard_key), execution_id=blob_execution_id)

            def upload(i: int) -> bytes:
                return encode_pointer(blobs.put_bytes(values[i], scope=scope, idempotency_key=f"{call_id}.{i}"))

            for i, env in zip(large, _parallel_map(upload, large), strict=True):
                envelopes[i] = env
        out: list[bytes] = []
        for env in envelopes:
            assert env is not None
            out.append(env)
        return out

    def decode_values(
        self, envelopes: Sequence[bytes | None], *, refs_out: list[BlobRef] | None = None
    ) -> list[bytes | None]:
        """Decode envelopes, fetching pointers in parallel (``None`` passes through).

        Args:
            envelopes: Stored values (``None`` for misses).
            refs_out: When given, receives the blob references that were read.

        Returns:
            The values.

        Raises:
            StorageEnvelopeError: A value is not a valid envelope, or a pointer
                was read with no vendor configured.
            StorageBlobError: A pointer's blob is missing, fails its size or
                SHA-256 check, or cannot be fetched (see the module's "Errors").
        """
        pointers = [i for i, env in enumerate(envelopes) if env is not None and envelope_tag(env) == TAG_POINTER]
        out: list[bytes | None] = [None if env is None else bytes(env[1:]) for env in envelopes]
        if pointers:
            blobs = self._blobs
            if blobs is None:
                raise StorageEnvelopeError(
                    f"stored value is a blob pointer but {ENV_VENDOR_URL} is not configured in this process"
                )
            refs = []
            for i in pointers:
                env = envelopes[i]
                assert env is not None
                refs.append(decode_pointer(env))

            def fetch(ref: BlobRef) -> bytes:
                from vgi_rpc.vendor import VendorError

                try:
                    return blobs.get_bytes(ref)
                except VendorError as exc:
                    raise _blob_read_error(ref, exc) from exc

            for i, data in zip(pointers, _parallel_map(fetch, refs), strict=True):
                out[i] = data
            if refs_out is not None:
                refs_out.extend(refs)
        return out

    def _decode_pairs(
        self, rows: Sequence[tuple[bytes, bytes]], refs_out: list[BlobRef] | None = None
    ) -> list[tuple[bytes, bytes]]:
        values = self.decode_values([v for _k, v in rows], refs_out=refs_out)
        result = []
        for (k, _v), value in zip(rows, values, strict=True):
            assert value is not None
            result.append((k, value))
        return result

    # --- background clean-up -------------------------------------------------

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

    def _delete_blobs(self, refs: list[BlobRef], what: str) -> None:
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

    # --- FunctionStorage -----------------------------------------------------

    def queue_push(self, execution_id: bytes, items: list[bytes], *, shard_key: str = "") -> int:
        """Envelope (and externalize) the items, then push them."""
        encoded = self.encode_values(
            items, shard_key=shard_key, blob_execution_id=_blob_execution_id("q", execution_id)
        )
        return self._base.queue_push(execution_id, encoded, shard_key=shard_key)

    def queue_pop(self, execution_id: bytes, *, shard_key: str = "") -> bytes | None:
        """Pop one item; its blob, if any, is deleted once fetched."""
        env = self._base.queue_pop(execution_id, shard_key=shard_key)
        if env is None:
            return None
        claimed: list[BlobRef] = []
        value = self.decode_values([env], refs_out=claimed)[0]
        self._delete_blobs(claimed, "queue_pop")
        return value

    def queue_clear(self, execution_id: bytes, *, shard_key: str = "") -> int:
        """Clear the queue and delete its blob scope."""
        cleared = self._base.queue_clear(execution_id, shard_key=shard_key)
        self._delete_blob_scope(shard_key, _blob_execution_id("q", execution_id), "queue_clear")
        return cleared

    def state_get_many(
        self, scope_id: bytes, ns: bytes, keys: list[bytes], *, shard_key: str = ""
    ) -> list[bytes | None]:
        """Read and decode values (``None`` for misses)."""
        return self.decode_values(self._base.state_get_many(scope_id, ns, keys, shard_key=shard_key))

    def state_put_many(
        self, scope_id: bytes, ns: bytes, items: list[tuple[bytes, bytes]], *, shard_key: str = ""
    ) -> None:
        """Envelope (and externalize) the values, then upsert."""
        if not items:
            self._base.state_put_many(scope_id, ns, items, shard_key=shard_key)
            return
        encoded = self.encode_values(
            [v for _k, v in items], shard_key=shard_key, blob_execution_id=_blob_execution_id("s", scope_id)
        )
        self._base.state_put_many(
            scope_id, ns, [(k, e) for (k, _v), e in zip(items, encoded, strict=True)], shard_key=shard_key
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
        """Scan and decode, a batch of rows at a time."""
        rows = self._base.state_scan(
            scope_id, ns, start=start, end=end, reverse=reverse, limit=limit, shard_key=shard_key
        )
        return self._decoded_stream(rows)

    def _decoded_stream(
        self, rows: Any, refs_out: list[BlobRef] | None = None, batch: int = 64
    ) -> Iterator[tuple[bytes, bytes]]:
        pending: list[tuple[bytes, bytes]] = []
        for row in rows:
            pending.append(row)
            if len(pending) >= batch:
                yield from self._decode_pairs(pending, refs_out)
                pending = []
        if pending:
            yield from self._decode_pairs(pending, refs_out)

    def state_drain(self, scope_id: bytes, ns: bytes, *, shard_key: str = "") -> Iterator[tuple[bytes, bytes]]:
        """Drain and decode; once fully consumed, delete the drained blobs.

        The backend call is made here, not on first iteration, so an eager
        backend (SQLite) still drains when the result is not consumed.
        """
        return self._drain_stream(self._base.state_drain(scope_id, ns, shard_key=shard_key))

    def _drain_stream(self, rows: Any) -> Iterator[tuple[bytes, bytes]]:
        drained: list[BlobRef] = []
        yield from self._decoded_stream(rows, drained)
        # The drained rows are gone: their blobs are not read again.
        self._delete_blobs(drained, "state_drain")

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
        """Delete rows (their blobs stay until the scope is cleared)."""
        return self._base.state_delete(scope_id, ns, keys, start=start, end=end, shard_key=shard_key)

    def execution_clear(self, scope_id: bytes, *, shard_key: str = "") -> int:
        """Clear the scope and delete its blob scope."""
        deleted = self._base.execution_clear(scope_id, shard_key=shard_key)
        self._delete_blob_scope(shard_key, _blob_execution_id("s", scope_id), "execution_clear")
        return deleted

    def state_append(self, scope_id: bytes, ns: bytes, key: bytes, item: bytes, *, shard_key: str = "") -> int:
        """Envelope (and externalize) the item, then append it."""
        (encoded,) = self.encode_values(
            [item], shard_key=shard_key, blob_execution_id=_blob_execution_id("s", scope_id)
        )
        return self._base.state_append(scope_id, ns, key, encoded, shard_key=shard_key)

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
        """Scan a log and decode its values."""
        rows = self._base.state_log_scan(scope_id, ns, key, after_id=after_id, limit=limit, shard_key=shard_key)
        values = self.decode_values([v for _i, v in rows])
        return [(i, v) for (i, _e), v in zip(rows, values, strict=True) if v is not None]

    def state_counter_get(self, scope_id: bytes, ns: bytes, key: bytes, *, shard_key: str = "") -> int:
        """Pass through."""
        return self._base.state_counter_get(scope_id, ns, key, shard_key=shard_key)

    def state_counter_add(self, scope_id: bytes, ns: bytes, key: bytes, delta: int, *, shard_key: str = "") -> int:
        """Pass through."""
        return self._base.state_counter_add(scope_id, ns, key, delta, shard_key=shard_key)

    def state_counter_set(self, scope_id: bytes, ns: bytes, key: bytes, value: int, *, shard_key: str = "") -> None:
        """Pass through."""
        self._base.state_counter_set(scope_id, ns, key, value, shard_key=shard_key)

    def state_counter_delete(self, scope_id: bytes, ns: bytes, key: bytes, *, shard_key: str = "") -> None:
        """Pass through."""
        self._base.state_counter_delete(scope_id, ns, key, shard_key=shard_key)
