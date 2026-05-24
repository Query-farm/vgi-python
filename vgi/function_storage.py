# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Storage for VGI function state.

This module provides a storage protocol and implementation for sharing state
across worker processes in distributed VGI function execution.

Protocol:
    FunctionStorage: Unified protocol for all VGI state storage needs.

Implementations:
    FunctionStorageSqlite: SQLite-backed storage (local/subprocess transport).
    FunctionStorageAzureSql: Azure SQL Database-backed storage (cloud deployments).
        See ``vgi.function_storage_azure_sql`` for details.
    FunctionStorageCfDo: Cloudflare Durable Object-backed storage (edge deployments).
        See ``vgi.function_storage_cf_do`` for details.

"""

import enum
import functools
import hashlib
import logging
import os
import sqlite3
import threading
import time
from collections.abc import Callable, Iterable
from typing import Any, Protocol, TypeVar

import pyarrow as pa

from vgi._storage_profile import _PROFILE_ON, _profiler, io_call_bytes

# When the parent vgi.* logger is configured at DEBUG, this emits one line
# per BoundStorage construction with the resolved shard_key — handy for
# cross-referencing storage-routing bugs with MetaWorker dispatch logs.
_shard_logger = logging.getLogger("vgi.storage.shard")

_F = TypeVar("_F", bound=Callable[..., Any])


def _profiled(op: str) -> Callable[[_F], _F]:
    """Record a ``BoundStorage`` op to the shared per-shard profiler.

    No-op (returns the method unchanged, zero overhead) unless
    ``VGI_STORAGE_PROFILE=1``. Backends that already self-profile at their
    transport layer (cloudflare-do, ``_profiles_at_transport=True``) are
    skipped so the two layers never double-count. Records
    ``(shard_key, op, elapsed, resp_bytes)`` — keyed per shard, i.e. per test.
    """

    def deco(fn: _F) -> _F:
        if not _PROFILE_ON:
            return fn

        @functools.wraps(fn)
        def wrapper(self: "BoundStorage", *args: Any, **kwargs: Any) -> Any:
            if getattr(self._base, "_profiles_at_transport", False):
                return fn(self, *args, **kwargs)
            t0 = time.monotonic()
            result = fn(self, *args, **kwargs)
            _profiler.record(
                self._shard_key, op, time.monotonic() - t0, io_call_bytes(args, kwargs, result)
            )
            return result

        return wrapper  # type: ignore[return-value]

    return deco

__all__ = [
    "FrameworkNS",
    "FunctionStorage",
    "FunctionStorageSqlite",
]


_RESERVED_NS_PREFIX = b"_vgi/"


class FrameworkNS(bytes, enum.Enum):
    """Framework-reserved storage namespaces.

    All members start with ``b"_vgi/"``; user code may NOT pass a bytes
    namespace with that prefix to ``BoundStorage.state_*`` — the reserved
    prefix is checked at every entry point. Framework code threads a
    member of this enum instead; the wrappers accept either form and
    normalise to plain bytes downstream.

    Adding a new entry: keep it ASCII-only, snake_case, prefixed
    ``_vgi/``. Don't rename existing entries — names are persisted in
    sqlite / Azure SQL / CfDo rows on disk and an unbounded backfill
    would be required.
    """

    BUFFERING_INIT = b"_vgi/buffering_init"
    STREAMING_FINALIZE = b"_vgi/streaming_finalize"
    TIO_STATE = b"_vgi/tio_state"
    AGGREGATE_STATE = b"_vgi/aggregate_state"
    AGGREGATE_WINDOW_PARTITION = b"_vgi/aggregate_window_partition"
    STREAMING_SESSION = b"_vgi/streaming_session"


def _coerce_ns(ns: "bytes | FrameworkNS") -> bytes:
    """Validate the namespace and return plain bytes.

    ``FrameworkNS`` members carry the reserved prefix legitimately and
    pass through. Caller-supplied bytes starting with ``_vgi/`` raise
    ``ValueError`` — that prefix is reserved for framework-owned state.
    """
    if isinstance(ns, FrameworkNS):
        return bytes(ns.value)
    if not isinstance(ns, (bytes, bytearray)):
        raise TypeError(
            f"namespace must be bytes or FrameworkNS, got {type(ns).__name__}"
        )
    ns_bytes = bytes(ns)
    if ns_bytes.startswith(_RESERVED_NS_PREFIX):
        raise ValueError(
            f"namespace {ns_bytes!r} starts with the reserved prefix "
            f"{_RESERVED_NS_PREFIX!r} — use a vgi.function_storage.FrameworkNS "
            "member or choose a different prefix"
        )
    return ns_bytes


# Structural (key-free) check that an attach_opaque_data is a sealed AEAD
# envelope. vgi_rpc.crypto.seal_bytes emits `version(1) || nonce(24) || ct+tag`,
# so the first byte is the envelope version and the minimum length is fixed.
# Mirrors worker._ATTACH_ENVELOPE_VERSION and crypto._MIN_TOKEN_LEN.
_SEALED_ATTACH_VERSION = 1
_SEALED_ATTACH_MIN_LEN = 1 + 24 + 16  # version + xchacha20 nonce + poly1305 tag
# Strict mode (opt-in via VGI_REQUIRE_SEALED_ATTACH=1): when attaches are sealed,
# every attach_opaque_data reaching storage MUST be a sealed envelope. A
# plaintext value (an unwrapped attach, or a fixed catalog id like
# b"readonly-catalog-") means a code path forgot the sealed form — which would
# silently split one logical attach across Durable Objects on cloudflare-do.
# Fail loudly here so the stack trace names the offending storage call site.
_REQUIRE_SEALED_ATTACH = os.environ.get("VGI_REQUIRE_SEALED_ATTACH") == "1"


def _attach_looks_sealed(value: bytes) -> bool:
    """True if ``value`` is structurally a sealed attach envelope (no key needed)."""
    return len(value) >= _SEALED_ATTACH_MIN_LEN and value[0] == _SEALED_ATTACH_VERSION


def _derive_shard_key(*, attach_opaque_data: bytes | None, auth: Any, _origin: str = "?") -> str:
    """Return the routing key for the ``FunctionStorageCfDo`` Durable Object.

    Server-derived inside the trusted worker process. The CF DO routes by
    this key (``idFromName(shard_key)``), so one DO instance hosts every
    storage op carrying the same shard_key. Precedence:

      1. ``attach_opaque_data`` (worker-vended bytes from ``catalog_attach``) — one
         DO per logical ATTACH. Best amortization for ATTACH-ed catalogs.
         Note: workers are NOT required to make attach_opaque_data values globally unique,
         so collisions across processes are possible. MetaWorker prepends
         a 1-byte sub-worker index, which keeps shards distinct *within*
         one DuckDB session.
      2. Hash of ``(auth.domain, auth.principal)`` — HTTP transport,
         authenticated, no ATTACH. One DO per (user, deployment).
      3. ``"loc-anon"`` — anonymous, no-ATTACH callers. Single shared DO
         for this entire class of traffic. Reintroduces a per-class
         single-DO bottleneck, accepted because anonymous workloads
         (subprocess transport, local CLIs) are typically dev/test.

    No-op for non-CfDo backends — they ignore the value.

    ``_origin`` labels the call site (e.g. ``"BoundStorage(InitRequest)"``).
    Emitted as a ``vgi.storage.shard`` debug log for cross-referencing
    storage-routing bugs with MetaWorker dispatch logs.
    """
    if attach_opaque_data is not None:
        if _REQUIRE_SEALED_ATTACH and not _attach_looks_sealed(attach_opaque_data):
            raise ValueError(
                f"unsealed attach_opaque_data reached storage sharding "
                f"(origin={_origin!r}, {len(attach_opaque_data)} bytes, "
                f"prefix={attach_opaque_data[:24].hex()!r}): expected a sealed "
                f"AEAD envelope. A code path passed the plaintext/unwrapped "
                f"attach instead of the sealed form — sharding on it would split "
                f"this attach across Durable Objects (VGI_REQUIRE_SEALED_ATTACH=1)."
            )
        key = "att-" + attach_opaque_data.hex()
    elif auth is not None and getattr(auth, "authenticated", False):
        domain = getattr(auth, "domain", "")
        principal = getattr(auth, "principal", "")
        digest = hashlib.sha256(f"{domain}\0{principal}".encode()).hexdigest()
        key = "prn-" + digest[:32]
    else:
        key = "loc-anon"
    if _shard_logger.isEnabledFor(logging.DEBUG):
        _shard_logger.debug(
            "shard derived origin=%s attach_opaque_data=%s authed=%d key=%s",
            _origin,
            attach_opaque_data.hex()[:16] if attach_opaque_data else "-",
            int(bool(auth is not None and getattr(auth, "authenticated", False))),
            key,
        )
    return key


def _scan_worker_stream_id() -> bytes:
    """Return raw stream-id bytes for the current scan worker.

    HTTP transport: pulls the per-stream UUID from
    ``vgi_rpc.rpc._common._current_stream_id`` and returns its raw 16-byte
    form. The framework sets this once per ``_serve_stream`` call and
    preserves it across HTTP turns via the state token, so every tick of
    one scan worker yields the same bytes regardless of which machine
    or thread serves it.

    Stdio transport / any non-stream path: returns
    ``struct.pack("<Q", os.getpid())`` so we still have a stable
    per-pid identifier and the storage row doesn't collide. Distinct
    pids → distinct keys; same pid across queries → overwrite (same
    semantics as the old per-pid ``BoundStorage.put``).
    """
    import struct

    try:
        from vgi_rpc.rpc._common import _current_stream_id
    except ImportError:
        return struct.pack("<Q", os.getpid())
    sid = _current_stream_id.get()
    if not sid:
        return struct.pack("<Q", os.getpid())
    # Stream ids are hex-encoded 128-bit UUIDs. Decode to the canonical
    # 16-byte form so the storage column doesn't carry the encoding tax.
    try:
        return bytes.fromhex(sid)
    except ValueError:
        # Defensively fall back to UTF-8 bytes — preserves uniqueness
        # even if a future framework version uses a non-hex stream id.
        return sid.encode("utf-8")


def _get_default_db_path() -> str:
    """Return the default SQLite database path for VGI storage."""
    from pathlib import Path

    from platformdirs import user_state_dir

    state_dir = Path(user_state_dir("vgi"))
    state_dir.mkdir(parents=True, exist_ok=True)
    return str((state_dir / "vgi_storage.db").resolve())


class FunctionStorage(Protocol):
    """Storage protocol for VGI distributed function execution.

    Two access patterns:

    **Unified state_*** - Composite-key K/V over ``(scope_id, ns, key)``.
    The catch-all family for per-execution state, per-transaction state,
    per-group aggregate state, and any other "this caller picks the
    namespace" pattern. Read-modify-write singletons via
    ``state_get_many`` / ``state_put_many``; non-destructive enumeration
    via ``state_scan``; atomic scan-and-delete via ``state_drain``;
    targeted or namespace-wide deletion via ``state_delete``;
    cross-namespace teardown via ``execution_clear``.

    **Work Queue** - Atomic FIFO work distribution. Producer pushes,
    workers atomically claim. Distinct from state_* (destructive consume,
    not key-addressable).

    Idempotency: backends generate ``attempt_id`` per call internally
    (CfDo before its HTTP retry loop, SQLite/Azure SQL fresh per call)
    and use it to detect replays. ``state_put_many`` retries silent
    no-op; ``state_drain`` retries return prior tombstoned values
    byte-identically.

    """

    # --- Work Queue (distributed work items) ---

    def queue_push(self, execution_id: bytes, items: list[bytes], *, shard_key: str = "") -> int:
        """Add work items to the queue and register the invocation.

        This method registers the execution_id as valid, allowing subsequent
        queue_pop calls. Even if items is empty, the invocation is registered.

        Args:
            execution_id: Unique identifier for the function invocation.
            items: List of serialized work item bytes.
            shard_key: Routing key for the CF DO backend; ignored by
                SQLite / Azure backends. Set automatically by BoundStorage
                from the caller's attach_opaque_data / auth context.

        Returns:
            Number of items added.

        """
        ...

    def queue_pop(self, execution_id: bytes, *, shard_key: str = "") -> bytes | None:
        """Atomically claim one work item from the queue.

        Args:
            execution_id: Unique identifier for the function invocation.
            shard_key: Routing key for the CF DO backend; ignored by
                SQLite / Azure backends. Set automatically by BoundStorage
                from the caller's attach_opaque_data / auth context.

        Returns:
            Serialized work item bytes, or None if the queue is empty or
            the execution_id was never registered. The protocol contract
            says ids are never reused and clients always push before pop,
            so a None result on a never-registered id indicates a buggy
            client; the backend does not distinguish that case from a
            drained queue.

        """
        ...

    def queue_clear(self, execution_id: bytes, *, shard_key: str = "") -> int:
        """Clear all remaining work items and unregister the invocation.

        Args:
            execution_id: Unique identifier for the function invocation.
            shard_key: Routing key for the CF DO backend; ignored by
                SQLite / Azure backends. Set automatically by BoundStorage
                from the caller's attach_opaque_data / auth context.

        Returns:
            Number of items deleted.

        """
        ...

    # ========================================================================
    # Unified state_* API (composite-key K/V over (scope_id, ns, key))
    # ========================================================================
    #
    # See /Users/rusty/.claude/plans/yes-lets-make-a-elegant-sparrow.md for
    # the full design rationale. This API replaces the four RMW families
    # (``worker_*``, ``stream_state_*``, ``aggregate_state_*``,
    # ``aggregate_window_partition_*``) plus ``transaction_state_*`` with a
    # single composite-key shape:
    #
    #   ``(scope_id, ns, key) -> value``
    #
    # ``scope_id`` carries the role of today's ``execution_id`` /
    # ``transaction_opaque_data`` — caller decides whether they're scoping by
    # invocation or by transaction. ``ns`` is a namespace selector chosen by
    # the caller (e.g. ``b"agg"`` for aggregate state, ``b"buf"`` for buffered
    # accumulators); the storage doesn't interpret it.
    #
    # Idempotency: the public API does not expose ``attempt_id``. Each backend
    # generates its own ``attempt_id = uuid.uuid4().bytes`` per call and uses
    # it for replay-detection (silent no-op for ``state_put_many`` retries,
    # read-back for ``state_drain`` retries). The CfDo HTTP client splices
    # the same id into retries within its ``_post()`` retry loop so server-
    # side replay-detection works across the network.

    def state_get_many(
        self,
        scope_id: bytes,
        ns: bytes,
        keys: list[bytes],
        *,
        shard_key: str = "",
    ) -> list[bytes | None]:
        """Batched non-destructive read of values keyed by ``(scope_id, ns, key)``.

        Returns a list parallel to ``keys`` with the stored ``bytes`` for
        hits and ``None`` for misses. Single-call so cloud backends (CfDo)
        can serve a 100-key request as one HTTP roundtrip.

        Args:
            scope_id: Caller's scope identifier (typically ``execution_id`` for
                per-query state, ``transaction_opaque_data`` for txn-scoped state).
            ns: Caller-chosen namespace bytes; the storage doesn't interpret.
            keys: List of binary keys to look up.
            shard_key: CF DO routing key; ignored by SQLite/Azure backends.

        Returns:
            List parallel to ``keys`` of stored values or ``None``.

        """
        ...

    def state_put_many(
        self,
        scope_id: bytes,
        ns: bytes,
        items: list[tuple[bytes, bytes]],
        *,
        shard_key: str = "",
    ) -> None:
        """Batched atomic upsert of ``(key, value)`` pairs in one namespace.

        Atomic per backend's single-statement isolation: either every item
        in the batch is written, or none are. Existing values for the same
        ``(scope_id, ns, key)`` are overwritten.

        Internally generates an ``attempt_id`` for backend replay-detection;
        a retried call (e.g. CfDo HTTP retry on transport failure) carrying
        the same id is detected as a replay and silently no-ops (the
        prior successful call's result is the ground truth).
        """
        ...

    def state_scan(
        self,
        scope_id: bytes,
        ns: bytes,
        *,
        shard_key: str = "",
    ) -> Iterable[tuple[bytes, bytes]]:
        """Non-destructive scan of every ``(key, value)`` in one namespace.

        Returns an iterable of ``(key, value)`` ordered by key. Large result
        sets may be streamed in pages by the backend (the ``cloudflare-do``
        backend pages under the hood), so callers should iterate rather than
        assume a materialized list. Use when you need to enumerate an unknown
        key set (e.g. drainer-side discovery of which sink threads produced
        state).
        """
        ...

    def state_drain(
        self,
        scope_id: bytes,
        ns: bytes,
        *,
        shard_key: str = "",
    ) -> Iterable[tuple[bytes, bytes]]:
        """Atomically scan-and-delete every ``(key, value)`` in one namespace.

        Returns an iterable of ``(key, value)`` ordered by key. Tombstones the
        rows internally for replay-detection: a retried call with the same
        internal ``attempt_id`` returns the same drained values without
        re-deleting. The ``cloudflare-do`` backend streams the result in pages
        but the drain is atomic — beginning to iterate claims the whole
        namespace, so always consume the iterable fully.
        """
        ...

    def state_delete(
        self,
        scope_id: bytes,
        ns: bytes,
        keys: list[bytes] | None = None,
        *,
        shard_key: str = "",
    ) -> int:
        """Delete by key list, or wipe the entire namespace if ``keys is None``.

        Naturally idempotent — deleting an already-deleted row is a no-op.
        Returns the count of rows actually removed. Replaces today's
        per-family ``*_clear`` methods.
        """
        ...

    def execution_clear(
        self,
        scope_id: bytes,
        *,
        shard_key: str = "",
    ) -> int:
        """Wipe ALL state and log rows for ``scope_id`` across every namespace.

        Used as a safety-sweep at end-of-execution / on crash recovery.
        Naturally idempotent. Returns total row count deleted across both
        ``function_state`` and ``function_state_log`` tables.

        Does NOT touch ``queue_*`` rows.
        """
        ...

    # --- Append-only log ---

    def state_append(
        self,
        scope_id: bytes,
        ns: bytes,
        key: bytes,
        item: bytes,
        *,
        shard_key: str = "",
    ) -> int:
        """Append ``item`` to the log keyed by (scope_id, ns, key); return ordinal.

        Ordinals are globally monotonic across all (scope, ns, key) triples
        on a given backend (one IDENTITY/AUTOINCREMENT column for the table).
        Per-key order is recovered via the ``(scope_id, ns, key, id)`` index;
        ``state_log_scan`` yields rows in id order, which corresponds to
        append order. Concurrent appenders to the *same* key get distinct
        ordinals but interleaving across writers is undefined.

        **Idempotency scope.** The internal ``attempt_id`` covers
        *transport-layer* retries within a single backend call: an HTTP
        retry on CfDo carries the same id and replays correctly; a
        pymssql ``OperationalError`` retry on Azure SQL replays correctly.
        **Caller-level retries** (re-invoking ``state_append`` for the
        same logical record after the call already returned) generate a
        fresh id and produce duplicate rows. If you need caller-level
        idempotency, dedupe on the caller side — e.g., check
        ``state_log_scan`` before appending, or key your namespace on a
        stable content hash.
        """
        ...

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
        """Yield (id, value) pairs for (scope_id, ns, key) with id > after_id.

        Returns rows in ascending ``id`` order. ``after_id=-1`` is the
        before-first sentinel (returns from the start). ``limit=None`` is
        unbounded; positive values cap the result at that many rows.
        Use the returned ``id`` of the last row as the next ``after_id``
        for cursor-based scrolling.

        Non-destructive. Repeat calls with the same parameters return
        identical results until ``execution_clear`` wipes the log rows.
        """
        ...


class TransactionBoundStorage:
    """Convenience wrapper bound to a single transaction_opaque_data.

    Lets a function read/write transaction-scoped state without
    threading the transaction_opaque_data through every call site. Get one via
    ``BoundStorage.transaction(transaction_opaque_data)``.
    """

    def __init__(
        self,
        storage: "FunctionStorage",
        transaction_opaque_data: bytes,
        *,
        request: Any = None,
        attach_opaque_data: bytes | None = None,
        auth: Any = None,
        shard_key: str | None = None,
    ) -> None:
        self._base = storage
        self._transaction_opaque_data = transaction_opaque_data
        # Caller may pass shard_key directly (e.g. inherited from a parent
        # BoundStorage), or pass request= / attach_opaque_data= / auth= and let us
        # derive. See BoundStorage for the request= polymorphism.
        if shard_key is None:
            origin = "TransactionBoundStorage"
            if attach_opaque_data is None and request is not None:
                attach_opaque_data = getattr(request, "attach_opaque_data", None)
                if attach_opaque_data is None:
                    bind_call = getattr(request, "bind_call", None)
                    if bind_call is not None:
                        attach_opaque_data = getattr(bind_call, "attach_opaque_data", None)
                origin = f"TransactionBoundStorage({type(request).__name__})"
            shard_key = _derive_shard_key(attach_opaque_data=attach_opaque_data, auth=auth, _origin=origin)
        self._shard_key = shard_key

    # Backed by the unified state_* API: scope_id = transaction_opaque_data,
    # ns = b"txn". Caller-supplied keys are user-chosen bytes (typically
    # short ASCII like b"watermark:topic-A"); the storage doesn't interpret
    # them. This class is preserved as a convenience wrapper so callers
    # don't have to thread the (transaction_opaque_data, b"txn") pair on
    # every call — the surface stays clean.

    _NS = b"txn"

    def get(self, keys: list[bytes]) -> list[bytes | None]:
        """Load values for a list of keys; parallel return list."""
        return self._base.state_get_many(
            self._transaction_opaque_data,
            self._NS,
            keys,
            shard_key=self._shard_key,
        )

    def get_one(self, key: bytes) -> bytes | None:
        """Load a single value, or None if missing."""
        return self.get([key])[0]

    def put(self, items: list[tuple[bytes, bytes]]) -> None:
        """Write a batch of (key, value) pairs."""
        self._base.state_put_many(
            self._transaction_opaque_data,
            self._NS,
            items,
            shard_key=self._shard_key,
        )

    def put_one(self, key: bytes, value: bytes) -> None:
        """Write a single (key, value) pair."""
        self.put([(key, value)])

    def clear(self) -> None:
        """Drop every value for this transaction (every namespace)."""
        # execution_clear sweeps all namespaces — same effect as the old
        # transaction_state_clear since the only namespace under txn scope
        # is b"txn".
        self._base.execution_clear(
            self._transaction_opaque_data,
            shard_key=self._shard_key,
        )


class BoundStorage:
    def __init__(
        self,
        storage: FunctionStorage,
        execution_id: bytes,
        *,
        request: Any = None,
        attach_opaque_data: bytes | None = None,
        auth: Any = None,
    ):
        self._base = storage
        self._execution_id = execution_id
        # ``request=`` is a convenience for the worker sites that already
        # have a BindRequest / InitRequest / AggregateBindRequest in scope:
        # we pull attach_opaque_data off either ``request.attach_opaque_data`` (Bind variants)
        # or ``request.bind_call.attach_opaque_data`` (InitRequest). Callers may
        # alternatively pass ``attach_opaque_data=`` directly; anonymous callers
        # fall through to "loc-anon".
        origin = "BoundStorage"
        if attach_opaque_data is None and request is not None:
            attach_opaque_data = getattr(request, "attach_opaque_data", None)
            if attach_opaque_data is None:
                bind_call = getattr(request, "bind_call", None)
                if bind_call is not None:
                    attach_opaque_data = getattr(bind_call, "attach_opaque_data", None)
            origin = f"BoundStorage({type(request).__name__})"
        self._shard_key = _derive_shard_key(attach_opaque_data=attach_opaque_data, auth=auth, _origin=origin)

    def transaction(self, transaction_opaque_data: bytes) -> TransactionBoundStorage:
        """Return a transaction-scoped storage view.

        Used for state that the user expects to be stable across
        multiple statements in one SQL transaction (e.g. Kafka topic
        watermarks, for snapshot-isolation reads).
        """
        # Inherit our shard_key directly — both views are part of the
        # same logical attach.
        return TransactionBoundStorage(
            self._base,
            transaction_opaque_data,
            shard_key=self._shard_key,
        )

    @_profiled("queue_push")
    def queue_push(self, items: list[bytes]) -> int:
        """Add work items to the queue and register the invocation."""
        return self._base.queue_push(
            self._execution_id,
            items,
            shard_key=self._shard_key,
        )

    def queue_push_batches(self, batches: list[pa.RecordBatch]) -> int:
        """Serialize and push RecordBatches as work items."""
        return self.queue_push([self.serialize_record_batch(b) for b in batches])

    @_profiled("queue_pop")
    def queue_pop(self) -> bytes | None:
        """Atomically claim one work item from the queue."""
        return self._base.queue_pop(
            self._execution_id,
            shard_key=self._shard_key,
        )

    def queue_pop_batch(self) -> pa.RecordBatch | None:
        """Pop and deserialize one work item as a RecordBatch."""
        data = self.queue_pop()
        if data is None:
            return None
        return self.deserialize_record_batch(data)

    @_profiled("queue_clear")
    def queue_clear(self) -> int:
        """Clear all remaining work items and unregister the invocation."""
        return self._base.queue_clear(
            self._execution_id,
            shard_key=self._shard_key,
        )

    # ========================================================================
    # Unified state_* facade — composite-key K/V over (ns, key)
    # ========================================================================
    #
    # See FunctionStorage.state_* docstrings for the full semantic. These
    # facade wrappers bind ``scope_id = execution_id`` (the common case);
    # for transaction-scoped state, use BoundStorage.transaction() to get
    # a separate facade bound to ``transaction_opaque_data``.

    @_profiled("state_get")
    def state_get(self, ns: "bytes | FrameworkNS", key: bytes) -> bytes | None:
        """Read one key's value (or None)."""
        result = self._base.state_get_many(
            self._execution_id, _coerce_ns(ns), [key], shard_key=self._shard_key
        )
        return result[0]

    @_profiled("state_get_many")
    def state_get_many(self, ns: "bytes | FrameworkNS", keys: list[bytes]) -> list[bytes | None]:
        """Batched non-destructive read."""
        return self._base.state_get_many(
            self._execution_id, _coerce_ns(ns), keys, shard_key=self._shard_key
        )

    @_profiled("state_put")
    def state_put(self, ns: "bytes | FrameworkNS", key: bytes, value: bytes) -> None:
        """Upsert one (key, value)."""
        self._base.state_put_many(
            self._execution_id, _coerce_ns(ns), [(key, value)], shard_key=self._shard_key
        )

    @_profiled("state_put_many")
    def state_put_many(self, ns: "bytes | FrameworkNS", items: list[tuple[bytes, bytes]]) -> None:
        """Batched atomic upsert."""
        self._base.state_put_many(
            self._execution_id, _coerce_ns(ns), items, shard_key=self._shard_key
        )

    @_profiled("state_scan")
    def state_scan(self, ns: "bytes | FrameworkNS") -> Iterable[tuple[bytes, bytes]]:
        """Non-destructive scan of every (key, value) in one namespace.

        Returns an iterable (the cloudflare-do backend streams it in pages).
        """
        return self._base.state_scan(
            self._execution_id, _coerce_ns(ns), shard_key=self._shard_key
        )

    @_profiled("state_drain")
    def state_drain(self, ns: "bytes | FrameworkNS") -> Iterable[tuple[bytes, bytes]]:
        """Atomic scan-and-delete of every (key, value) in one namespace.

        Returns an iterable; consume it fully (beginning to iterate claims the
        whole namespace on the cloudflare-do backend).
        """
        return self._base.state_drain(
            self._execution_id, _coerce_ns(ns), shard_key=self._shard_key
        )

    @_profiled("state_delete")
    def state_delete(self, ns: "bytes | FrameworkNS", keys: list[bytes] | None = None) -> int:
        """Delete by key list, or wipe entire namespace if keys is None."""
        return self._base.state_delete(
            self._execution_id, _coerce_ns(ns), keys, shard_key=self._shard_key
        )

    @_profiled("execution_clear")
    def execution_clear(self) -> int:
        """Wipe ALL state and log rows for this execution across every namespace."""
        return self._base.execution_clear(
            self._execution_id, shard_key=self._shard_key
        )

    @_profiled("state_append")
    def state_append(self, ns: "bytes | FrameworkNS", key: bytes, item: bytes) -> int:
        """Append an item to the (ns, key) log; return the assigned ordinal.

        Idempotency covers transport-layer retries only (HTTP retry on
        CfDo, pymssql driver-level retry on Azure SQL). Caller-level
        retries — re-invoking ``state_append`` for the same logical
        record after it returned — produce duplicate rows. See the
        underlying ``FunctionStorage.state_append`` for the full contract.
        """
        return self._base.state_append(
            self._execution_id, _coerce_ns(ns), key, item, shard_key=self._shard_key
        )

    @_profiled("state_log_scan")
    def state_log_scan(
        self,
        ns: "bytes | FrameworkNS",
        key: bytes,
        *,
        after_id: int = -1,
        limit: int | None = None,
    ) -> list[tuple[int, bytes]]:
        """Yield (id, value) pairs for (ns, key) with id > after_id.

        See ``FunctionStorage.state_log_scan`` for the full contract.
        """
        return self._base.state_log_scan(
            self._execution_id, _coerce_ns(ns), key,
            after_id=after_id, limit=limit, shard_key=self._shard_key,
        )

    @staticmethod
    def pack_int_key(i: int) -> bytes:
        """Sugar: encode an int as 8-byte little-endian for use as ``state_*`` key.

        The common case for table_buffering state_id, aggregate group_id,
        window partition_id is an int. This canonicalizes the encoding so
        every caller produces the same bytes for the same int.
        """
        return i.to_bytes(8, "little", signed=True)

    @staticmethod
    def serialize_record_batch(batch: pa.RecordBatch) -> bytes:
        """Serialize a RecordBatch to Arrow IPC stream bytes."""
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)
        return sink.getvalue().to_pybytes()

    @staticmethod
    def deserialize_record_batch(data: bytes) -> pa.RecordBatch:
        with pa.ipc.open_stream(data) as ipc_reader:
            return ipc_reader.read_next_batch()


class FunctionStorageSqlite:
    """SQLite-backed storage for VGI function state.

    This implementation uses SQLite with WAL mode to allow multiple worker
    processes to share state. It manages these tables:

    - global_state_storage: Key-value store for init data
    - worker_state: Per-worker partial state keyed by (execution_id, worker_id)
    - work_queue: FIFO queue of work items per invocation
    - aggregate_state: Per-group-id state for aggregate functions

    """

    def __init__(self, db_path: str | None = None) -> None:
        """Initialize SQLite storage.

        Args:
            db_path: Path to the SQLite database file. If None, uses a default
                location in the user's state directory. Pass ``":memory:"`` to
                use a process-local in-memory database; the storage uses a
                shared-cache URI plus an anchor connection so the per-op
                connections in ``_connect`` see the same DB. Suitable for
                single-process test fixtures where commit-fsync overhead
                dominates and persistence isn't needed.

        """
        if db_path == ":memory:":
            # Shared-cache in-memory: every connection to this URI sees the
            # same database for as long as at least one connection is open.
            # We hold ``_anchor_conn`` for the storage instance's lifetime so
            # the DB survives between transient ``_connect`` calls. The
            # per-instance UUID namespaces the DB so independent storage
            # instances within a single process don't collide.
            import uuid

            self._memory_uri: str | None = f"file:vgi_storage_{uuid.uuid4().hex}?mode=memory&cache=shared"
            self._anchor_conn: sqlite3.Connection | None = sqlite3.connect(self._memory_uri, uri=True, timeout=30.0)
            self.db_path = ":memory:"
        else:
            self._memory_uri = None
            self._anchor_conn = None
            self.db_path = db_path if db_path is not None else _get_default_db_path()
        self._tls = threading.local()
        self._ensure_tables()

    def _connect(self) -> sqlite3.Connection:
        """Create a new short-lived database connection (used for one-shot DDL)."""
        if self._memory_uri is not None:
            # Memory DBs use MEMORY journal mode implicitly; no WAL,
            # no fsync — the whole point of using :memory: here.
            return sqlite3.connect(self._memory_uri, uri=True, timeout=30.0)
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _conn(self) -> sqlite3.Connection:
        """Return the calling thread's persistent connection, creating it lazily.

        WAL coordinates writes across processes via file locking; within a
        process, each thread gets its own connection so SQLite's per-connection
        locking serializes writers without a Python-level lock and without
        forfeiting WAL's reader-writer concurrency. Pragmas are applied once
        per connection — ``synchronous=NORMAL`` is the dominant win, since it
        skips fsync on every commit and only fsyncs at WAL checkpoint.
        """
        conn: sqlite3.Connection | None = getattr(self._tls, "conn", None)
        if conn is not None:
            return conn
        if self._memory_uri is not None:
            conn = sqlite3.connect(self._memory_uri, uri=True, timeout=30.0)
        else:
            conn = sqlite3.connect(self.db_path, timeout=30.0)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=30000")
            conn.execute("PRAGMA temp_store=MEMORY")
            conn.execute("PRAGMA cache_size=-65536")
        self._tls.conn = conn
        return conn

    def close(self) -> None:
        """Close the calling thread's persistent connection, if any."""
        conn: sqlite3.Connection | None = getattr(self._tls, "conn", None)
        if conn is not None:
            conn.close()
            self._tls.conn = None

    def _ensure_tables(self) -> None:
        """Create all storage tables if they don't exist.

        Handles schema migration from older versions (e.g. invocation_id → execution_id)
        by dropping and recreating tables with stale schemas. The data in these tables
        is ephemeral (in-progress worker state), so dropping is safe.
        """
        conn = self._connect()
        try:
            # Drop tables with stale schema (e.g. invocation_id instead of execution_id)
            for table, required_col in [
                ("worker_state", "execution_id"),
                ("work_queue", "execution_id"),
                ("invocation_registry", "execution_id"),
            ]:
                cursor = conn.execute(f"PRAGMA table_info({table})")  # noqa: S608
                columns = {row[1] for row in cursor.fetchall()}
                if columns and required_col not in columns:
                    conn.execute(f"DROP TABLE IF EXISTS {table}")  # noqa: S608

            # Also drop old table names from previous versions
            conn.execute("DROP TABLE IF EXISTS init_storage")

            # Global state table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS global_state_storage (
                    key BLOB PRIMARY KEY,
                    value BLOB NOT NULL,
                    created_at REAL DEFAULT (julianday('now'))
                )
            """)
            # Work queue table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS work_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    execution_id BLOB NOT NULL,
                    work_item BLOB NOT NULL,
                    created_at REAL DEFAULT (julianday('now'))
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_work_queue_invocation
                ON work_queue(execution_id)
            """)
            # Invocation registry - tracks valid invocation IDs for queue operations
            conn.execute("""
                CREATE TABLE IF NOT EXISTS invocation_registry (
                    execution_id BLOB PRIMARY KEY,
                    created_at REAL DEFAULT (julianday('now'))
                )
            """)
            # ----------------------------------------------------------------
            # Unified state_* tables — composite-key K/V over (scope_id, ns,
            # key). The single home for per-execution / per-transaction /
            # per-group / per-pid state. Caller chooses the namespace via
            # the ``ns`` column; storage doesn't interpret it.
            #
            # last_attempt_id powers internal replay-detection: a retried
            # state_put_many with the same id is a silent no-op; a retried
            # state_drain returns the prior tombstoned values. CfDo client
            # generates the id (per-HTTP-call); SQLite generates per-call too
            # so the semantic is uniform across backends.
            #
            # Tombstone columns drained_at / drained_by_attempt mark rows
            # that state_drain has consumed; rows linger until cleanup_old_entries
            # sweeps them past the retention horizon.
            # ----------------------------------------------------------------
            conn.execute("""
                CREATE TABLE IF NOT EXISTS function_state (
                    scope_id           BLOB NOT NULL,
                    ns                 BLOB NOT NULL,
                    key                BLOB NOT NULL,
                    value              BLOB NOT NULL,
                    last_attempt_id    BLOB NOT NULL,
                    drained_at         REAL DEFAULT NULL,
                    drained_by_attempt BLOB DEFAULT NULL,
                    created_at         REAL DEFAULT (julianday('now')),
                    PRIMARY KEY (scope_id, ns, key)
                )
            """)
            # function_state_log: append-only log keyed by (scope_id, ns, key).
            # Each (scope, ns, key, attempt_id) is unique so a retried
            # state_append (Step 2) maps back to the prior ordinal.
            conn.execute("""
                CREATE TABLE IF NOT EXISTS function_state_log (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    scope_id    BLOB NOT NULL,
                    ns          BLOB NOT NULL,
                    key         BLOB NOT NULL,
                    value       BLOB NOT NULL,
                    attempt_id  BLOB NOT NULL,
                    created_at  REAL DEFAULT (julianday('now')),
                    UNIQUE (scope_id, ns, key, attempt_id)
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS function_state_log_lookup_idx
                    ON function_state_log(scope_id, ns, key, id)
            """)
            conn.commit()
        finally:
            conn.close()

    # --- Work Queue ---

    def queue_push(self, execution_id: bytes, items: list[bytes], *, shard_key: str = "") -> int:
        """Add work items to the queue and register the invocation."""
        conn = self._conn()
        # Register the execution_id (idempotent)
        conn.execute(
            "INSERT OR IGNORE INTO invocation_registry (execution_id) VALUES (?)",
            (execution_id,),
        )
        # Add work items if any
        if items:
            conn.executemany(
                """
                INSERT INTO work_queue (execution_id, work_item)
                VALUES (?, ?)
                """,
                [(execution_id, item) for item in items],
            )
        conn.commit()
        return len(items)

    def queue_pop(self, execution_id: bytes, *, shard_key: str = "") -> bytes | None:
        """Atomically claim one work item from the queue.

        Returns None when the queue is empty *or* the execution_id was
        never pushed — see the base-class docstring.
        """
        conn = self._conn()
        cursor = conn.execute(
            """
            DELETE FROM work_queue
            WHERE id = (
                SELECT id FROM work_queue
                WHERE execution_id = ?
                ORDER BY id ASC
                LIMIT 1
            )
            RETURNING work_item
            """,
            (execution_id,),
        )
        row = cursor.fetchone()
        conn.commit()
        return row[0] if row else None

    def queue_clear(self, execution_id: bytes, *, shard_key: str = "") -> int:
        """Clear all remaining work items and unregister the invocation."""
        conn = self._conn()
        cursor = conn.execute(
            "DELETE FROM work_queue WHERE execution_id = ?",
            (execution_id,),
        )
        # Unregister the invocation
        conn.execute(
            "DELETE FROM invocation_registry WHERE execution_id = ?",
            (execution_id,),
        )
        conn.commit()
        return cursor.rowcount

    # ========================================================================
    # Unified state_* implementation
    # ========================================================================
    #
    # See FunctionStorage protocol docstrings + plan file for contracts.
    # Idempotency: every mutating call generates ``attempt_id = uuid.uuid4().bytes``
    # internally. SQLite is single-process (modulo WAL cross-process), so
    # retries within one Python process don't actually happen — but
    # implementing the same replay-detection pattern as the CfDo backend
    # keeps the semantic uniform across backends and protects multi-process
    # subprocess workers retrying after a crash.

    def state_get_many(
        self,
        scope_id: bytes,
        ns: bytes,
        keys: list[bytes],
        *,
        shard_key: str = "",
    ) -> list[bytes | None]:
        """Batched read by key list. Returns parallel list with None for misses."""
        del shard_key
        if not keys:
            return []
        conn = self._conn()
        # Returns rows in key-list order via a CASE, so callers don't have to
        # re-sort. Tombstoned rows (drained_at IS NOT NULL) are invisible.
        placeholders = ",".join("?" for _ in keys)
        rows = conn.execute(
            f"""
            SELECT key, value FROM function_state
            WHERE scope_id = ? AND ns = ? AND key IN ({placeholders})
              AND drained_at IS NULL
            """,
            (scope_id, ns, *keys),
        ).fetchall()
        found: dict[bytes, bytes] = {bytes(k): bytes(v) for k, v in rows}
        return [found.get(bytes(k)) for k in keys]

    def state_put_many(
        self,
        scope_id: bytes,
        ns: bytes,
        items: list[tuple[bytes, bytes]],
        *,
        shard_key: str = "",
    ) -> None:
        """Atomic batched upsert. First-key replay-detection on attempt_id."""
        del shard_key
        if not items:
            return
        import uuid

        attempt_id = uuid.uuid4().bytes
        # Replay-detection: check whether the FIRST item is already present
        # with this attempt_id. Mirrors today's CfDo aggregate_state_put
        # check (`index.ts:618`); first-key check is sufficient because
        # state_put_many is atomic per call (all-or-none under SQLite's
        # statement isolation), so seeing the first key persisted with our
        # attempt_id means the whole batch was persisted.
        first_key, _ = items[0]
        conn = self._conn()
        prior_row = conn.execute(
            """
            SELECT 1 FROM function_state
            WHERE scope_id = ? AND ns = ? AND key = ? AND last_attempt_id = ?
            """,
            (scope_id, ns, first_key, attempt_id),
        ).fetchone()
        # Per-process attempt_id collision is astronomically rare (UUID4),
        # but we check anyway for symmetry with CfDo.
        if prior_row is not None:
            return
        conn.executemany(
            """
            INSERT INTO function_state
                (scope_id, ns, key, value, last_attempt_id, created_at,
                 drained_at, drained_by_attempt)
            VALUES (?, ?, ?, ?, ?, julianday('now'), NULL, NULL)
            ON CONFLICT(scope_id, ns, key) DO UPDATE SET
                value = excluded.value,
                last_attempt_id = excluded.last_attempt_id,
                created_at = julianday('now'),
                drained_at = NULL,
                drained_by_attempt = NULL
            """,
            [(scope_id, ns, k, v, attempt_id) for k, v in items],
        )
        conn.commit()

    def state_scan(
        self,
        scope_id: bytes,
        ns: bytes,
        *,
        shard_key: str = "",
    ) -> list[tuple[bytes, bytes]]:
        """Non-destructive scan of all live (key, value) in a namespace."""
        del shard_key
        conn = self._conn()
        rows = conn.execute(
            """
            SELECT key, value FROM function_state
            WHERE scope_id = ? AND ns = ? AND drained_at IS NULL
            """,
            (scope_id, ns),
        ).fetchall()
        return [(bytes(k), bytes(v)) for k, v in rows]

    def state_drain(
        self,
        scope_id: bytes,
        ns: bytes,
        *,
        shard_key: str = "",
    ) -> list[tuple[bytes, bytes]]:
        """Destructive scan-and-tombstone. Replay returns prior tombstoned values."""
        del shard_key
        import uuid

        attempt_id = uuid.uuid4().bytes
        conn = self._conn()
        # Replay: if any rows are already tombstoned with this attempt_id,
        # return them without re-tombstoning. Mirrors CfDo's worker_collect
        # read-back replay (`index.ts:368`).
        replay_rows = conn.execute(
            """
            SELECT key, value FROM function_state
            WHERE scope_id = ? AND ns = ? AND drained_by_attempt = ?
            ORDER BY key
            """,
            (scope_id, ns, attempt_id),
        ).fetchall()
        if replay_rows:
            return [(bytes(k), bytes(v)) for k, v in replay_rows]
        # Fresh drain: tombstone live rows for this attempt_id, then read
        # them back. UPDATE ... RETURNING is SQLite ≥3.35; available in
        # all supported Python builds. Two-statement is also fine —
        # SQLite serializes within one connection.
        rows = conn.execute(
            """
            UPDATE function_state
            SET drained_at = julianday('now'),
                drained_by_attempt = ?
            WHERE scope_id = ? AND ns = ? AND drained_at IS NULL
            RETURNING key, value
            """,
            (attempt_id, scope_id, ns),
        ).fetchall()
        conn.commit()
        return [(bytes(k), bytes(v)) for k, v in rows]

    def state_delete(
        self,
        scope_id: bytes,
        ns: bytes,
        keys: list[bytes] | None = None,
        *,
        shard_key: str = "",
    ) -> int:
        """Delete by key list, or whole namespace if keys is None. Returns count deleted."""
        del shard_key
        conn = self._conn()
        if keys is None:
            cur = conn.execute(
                "DELETE FROM function_state WHERE scope_id = ? AND ns = ?",
                (scope_id, ns),
            )
        else:
            if not keys:
                return 0
            placeholders = ",".join("?" for _ in keys)
            cur = conn.execute(
                f"""
                DELETE FROM function_state
                WHERE scope_id = ? AND ns = ? AND key IN ({placeholders})
                """,
                (scope_id, ns, *keys),
            )
        conn.commit()
        return int(cur.rowcount)

    def execution_clear(
        self,
        scope_id: bytes,
        *,
        shard_key: str = "",
    ) -> int:
        """Wipe all state and log rows for scope_id across every namespace."""
        del shard_key
        conn = self._conn()
        c1 = conn.execute(
            "DELETE FROM function_state WHERE scope_id = ?",
            (scope_id,),
        )
        c2 = conn.execute(
            "DELETE FROM function_state_log WHERE scope_id = ?",
            (scope_id,),
        )
        conn.commit()
        return int(c1.rowcount) + int(c2.rowcount)

    def state_append(
        self,
        scope_id: bytes,
        ns: bytes,
        key: bytes,
        item: bytes,
        *,
        shard_key: str = "",
    ) -> int:
        """Append item; return ordinal. Defensive against transport-layer replay only."""
        del shard_key
        import uuid

        attempt_id = uuid.uuid4().bytes
        conn = self._conn()
        # INSERT OR IGNORE + SELECT-by-attempt_id is structurally a replay
        # primitive but currently has no path to fire: SQLite has no retry
        # layer above this method, and the public state_append signature
        # generates a fresh UUID4 per call (no caller-supplied attempt_id).
        # The pattern is here for symmetry with the Azure SQL / CfDo
        # implementations and to leave the door open for future exposure
        # of caller-level attempt_id. Could be simplified to
        # `INSERT ... RETURNING id` (SQLite ≥3.35) if we commit to never
        # exposing that contract — would save one SQL round-trip per call.
        conn.execute(
            """
            INSERT OR IGNORE INTO function_state_log
                (scope_id, ns, key, value, attempt_id)
            VALUES (?, ?, ?, ?, ?)
            """,
            (scope_id, ns, key, item, attempt_id),
        )
        conn.commit()
        row = conn.execute(
            """
            SELECT id FROM function_state_log
            WHERE scope_id = ? AND ns = ? AND key = ? AND attempt_id = ?
            """,
            (scope_id, ns, key, attempt_id),
        ).fetchone()
        return int(row[0])

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
        """Yield (id, value) pairs for (scope_id, ns, key) with id > after_id."""
        del shard_key
        conn = self._conn()
        # SQLite supports LIMIT -1 as unbounded, but we pass NULL via
        # a parameter for clarity. Use LIMIT ? with -1 sentinel.
        sql = """
            SELECT id, value FROM function_state_log
            WHERE scope_id = ? AND ns = ? AND key = ? AND id > ?
            ORDER BY id
            LIMIT ?
        """
        sqlite_limit = -1 if limit is None else int(limit)
        rows = conn.execute(
            sql, (scope_id, ns, key, after_id, sqlite_limit),
        ).fetchall()
        return [(int(rid), bytes(v)) for (rid, v) in rows]

    # --- Maintenance (not part of protocol) ---

    def cleanup_old_entries(self, max_age_days: float = 1.0) -> int:
        """Remove entries older than the specified age from all tables.

        Args:
            max_age_days: Maximum age in days for entries to keep.

        Returns:
            Total number of entries deleted.

        """
        conn = self._conn()
        total = 0
        for table in (
            "global_state_storage",
            "work_queue",
            "invocation_registry",
            "function_state",
            "function_state_log",
        ):
            cursor = conn.execute(
                f"DELETE FROM {table} WHERE julianday('now') - created_at > ?",  # noqa: S608
                (max_age_days,),
            )
            total += int(cursor.rowcount)
        conn.commit()
        return total


class ShardedSqliteStorage:
    """Debug-only SQLite backend that PARTITIONS storage by ``shard_key``.

    The normal SQLite backend ignores ``shard_key`` (one shared DB), masking
    shard-routing bugs that only bite ``cloudflare-do`` (which truly shards per
    Durable Object). This wrapper isolates shards by PREFIXING the scope_id /
    execution_id with the shard_key, so an op under shard A can't see state
    written under shard B — reproducing cloudflare-do isolation locally — while
    using ONE inner store, so concurrency behaves exactly like the normal sqlite
    backend. (Per-shard databases instead exploded connections and deadlocked
    the shared-cache :memory: DB under load.) Enabled via ``VGI_SQLITE_SHARD=1``
    (see ``vgi/function.py:_resolve_storage``). Not for production.

    With ``VGI_SQLITE_SHARD_LOG=1`` it logs every op's (op, shard_key, scope) so
    a write and a read for one execution can be compared without a remote tail.
    """

    _SEP = b"\x1f"  # unit separator — absent from attach/execution id bytes

    def __init__(self, db_path: str | None = None) -> None:
        self._inner = FunctionStorageSqlite(db_path=db_path or ":memory:")
        self._log = logging.getLogger("vgi.storage.sqlite_shard")
        self._dbg_on = os.environ.get("VGI_SQLITE_SHARD_LOG") == "1"

    def _p(self, shard_key: str, id_bytes: bytes) -> bytes:
        """Namespace an execution_id / scope_id by shard_key (transparent to the
        worker — only the sqlite row key changes, never returned data)."""
        return shard_key.encode("utf-8") + self._SEP + id_bytes

    def _dbg(self, op: str, shard_key: str, scope: bytes) -> None:
        if self._dbg_on:
            self._log.warning("op=%s shard=%s scope=%s", op, shard_key, scope.hex()[:16])

    # --- Work Queue ---
    def queue_push(self, execution_id: bytes, items: list[bytes], *, shard_key: str = "") -> int:
        self._dbg("queue_push", shard_key, execution_id)
        return self._inner.queue_push(self._p(shard_key, execution_id), items)

    def queue_pop(self, execution_id: bytes, *, shard_key: str = "") -> bytes | None:
        self._dbg("queue_pop", shard_key, execution_id)
        return self._inner.queue_pop(self._p(shard_key, execution_id))

    def queue_clear(self, execution_id: bytes, *, shard_key: str = "") -> int:
        self._dbg("queue_clear", shard_key, execution_id)
        return self._inner.queue_clear(self._p(shard_key, execution_id))

    # --- Unified state (scope_id namespaced by shard_key) ---
    def state_get_many(
        self, scope_id: bytes, ns: bytes, keys: list[bytes], *, shard_key: str = ""
    ) -> list[bytes | None]:
        self._dbg("state_get_many", shard_key, scope_id)
        return self._inner.state_get_many(self._p(shard_key, scope_id), ns, keys)

    def state_put_many(
        self, scope_id: bytes, ns: bytes, items: list[tuple[bytes, bytes]], *, shard_key: str = ""
    ) -> None:
        self._dbg("state_put_many", shard_key, scope_id)
        self._inner.state_put_many(self._p(shard_key, scope_id), ns, items)

    def state_scan(self, scope_id: bytes, ns: bytes, *, shard_key: str = "") -> list[tuple[bytes, bytes]]:
        self._dbg("state_scan", shard_key, scope_id)
        return self._inner.state_scan(self._p(shard_key, scope_id), ns)

    def state_drain(self, scope_id: bytes, ns: bytes, *, shard_key: str = "") -> list[tuple[bytes, bytes]]:
        self._dbg("state_drain", shard_key, scope_id)
        return self._inner.state_drain(self._p(shard_key, scope_id), ns)

    def state_delete(
        self, scope_id: bytes, ns: bytes, keys: list[bytes] | None = None, *, shard_key: str = ""
    ) -> int:
        self._dbg("state_delete", shard_key, scope_id)
        return self._inner.state_delete(self._p(shard_key, scope_id), ns, keys)

    def execution_clear(self, scope_id: bytes, *, shard_key: str = "") -> int:
        self._dbg("execution_clear", shard_key, scope_id)
        return self._inner.execution_clear(self._p(shard_key, scope_id))

    def state_append(self, scope_id: bytes, ns: bytes, key: bytes, item: bytes, *, shard_key: str = "") -> int:
        self._dbg("state_append", shard_key, scope_id)
        return self._inner.state_append(self._p(shard_key, scope_id), ns, key, item)

    def state_log_scan(
        self, scope_id: bytes, ns: bytes, key: bytes, *, after_id: int = -1,
        limit: int | None = None, shard_key: str = "",
    ) -> list[tuple[int, bytes]]:
        self._dbg("state_log_scan", shard_key, scope_id)
        return self._inner.state_log_scan(self._p(shard_key, scope_id), ns, key, after_id=after_id, limit=limit)

    def close(self) -> None:
        self._inner.close()
