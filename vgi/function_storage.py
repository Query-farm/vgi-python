# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Storage for VGI function state.

This module provides a storage protocol and implementation for sharing state
across worker processes in distributed VGI function execution.

Protocol:
    `FunctionStorage`: Unified protocol for all VGI state storage needs.

Implementations:
    `FunctionStorageSqlite`: SQLite-backed storage (local/subprocess transport).
    `FunctionStorageAzureSql`: Azure SQL Database-backed storage (cloud deployments).
        See ``vgi.function_storage_azure_sql`` for details.
    `FunctionStorageCfDo`: Cloudflare Durable Object-backed storage (edge deployments).
        See ``vgi.function_storage_cf_do`` for details.

"""

import enum
import functools
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
    """Record a `[`BoundStorage`][]` op to the shared per-shard profiler.

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
            _profiler.record(self._shard_key, op, time.monotonic() - t0, io_call_bytes(args, kwargs, result))
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

    `[`FrameworkNS`][]` members carry the reserved prefix legitimately and
    pass through. Caller-supplied bytes starting with ``_vgi/`` raise
    ``ValueError`` — that prefix is reserved for framework-owned state.
    """
    if isinstance(ns, FrameworkNS):
        return bytes(ns.value)
    if not isinstance(ns, (bytes, bytearray)):
        raise TypeError(f"namespace must be bytes or FrameworkNS, got {type(ns).__name__}")
    ns_bytes = bytes(ns)
    if ns_bytes.startswith(_RESERVED_NS_PREFIX):
        raise ValueError(
            f"namespace {ns_bytes!r} starts with the reserved prefix "
            f"{_RESERVED_NS_PREFIX!r} — use a vgi.function_storage.FrameworkNS "
            "member or choose a different prefix"
        )
    return ns_bytes


# Width of the framework UUID at the head of every unwrapped attach plaintext
# (``uuid(16) || catalog_bytes``). Mirrors ``worker._ATTACH_UUID_LEN``.
_ATTACH_UUID_LEN = 16


def attach_catalog_bytes(attach_plaintext: bytes | None) -> bytes | None:
    """Strip the framework shard-UUID prefix from a full attach plaintext.

    The framework unwraps an attach to ``uuid(16) || catalog_bytes``; function
    bodies see only ``catalog_bytes`` (what the catalog returned). Returns None
    when there is no attach.
    """
    return attach_plaintext[_ATTACH_UUID_LEN:] if attach_plaintext else None


def _derive_shard_key(*, attach_uuid: bytes | None, _origin: str = "?") -> str:
    """Return the routing key for the ``FunctionStorageCfDo`` Durable Object.

    Server-derived inside the trusted worker process. The CF DO routes by
    this key (``idFromName(shard_key)``), so one DO instance hosts every
    storage op carrying the same shard_key.

    Single rule: ``"att-" + attach_uuid.hex()`` where ``attach_uuid`` is the
    framework-minted 16-byte UUID at the head of the **unwrapped** attach
    (``catalog_attach`` prepends it; see ``_AttachUnwrapper`` in worker.py).
    One DO per logical ATTACH. We shard on the UUID — not the sealed bytes —
    because the seal uses a random nonce (re-sealing the same attach would
    otherwise scatter its state across DOs) and the catalog-vended plaintext
    isn't guaranteed unique (distinct attaches would otherwise collide). The
    UUID is stable across re-seals and globally unique; 36-char key, ≤128.

    ``attach_uuid`` must be exactly 16 bytes: the storage path is always bound
    to a logical ATTACH, so a missing/short value is a programming error and
    raises rather than collapsing traffic onto a single fallback DO.

    No-op for non-CfDo backends — they ignore the value.

    ``_origin`` labels the call site (e.g. ``"BoundStorage(InitRequest)"``).
    Emitted as a ``vgi.storage.shard`` debug log for cross-referencing
    storage-routing bugs with MetaWorker dispatch logs.
    """
    if not attach_uuid or len(attach_uuid) != 16:
        raise ValueError(
            f"cannot derive shard_key without a 16-byte attach uuid (origin={_origin}, "
            f"got {len(attach_uuid) if attach_uuid else 0} bytes); the storage path "
            "must be bound to a logical ATTACH"
        )
    key = "att-" + attach_uuid.hex()
    if _shard_logger.isEnabledFor(logging.DEBUG):
        _shard_logger.debug("shard derived origin=%s uuid=%s key=%s", _origin, attach_uuid.hex(), key)
    return key


def _resolve_shard_key(backend: Any, attach_plaintext: bytes | None, _origin: str) -> str:
    """Compute the shard_key for a `[`BoundStorage`][]` over ``backend``.

    ``attach_plaintext`` is the framework-unwrapped attach, laid out as
    ``uuid(16) || catalog_bytes`` (the worker unwraps and threads it in; see
    ``_AttachUnwrapper``-free flow in worker.py), or None when there is no
    ATTACH. We shard on the leading UUID for any backend (so the debug
    `[`ShardedSqliteStorage`][]` partitions too). When there is no attach, only a
    remote-sharding backend (``requires_shard_key``, i.e. CfDo) treats it as a
    hard error; everything else gets an empty key — local / subprocess
    executions are routinely not bound to an ATTACH and ignore the value anyway.
    """
    uuid = (
        attach_plaintext[:_ATTACH_UUID_LEN] if attach_plaintext and len(attach_plaintext) >= _ATTACH_UUID_LEN else None
    )
    if uuid:
        return _derive_shard_key(attach_uuid=uuid, _origin=_origin)
    if getattr(backend, "requires_shard_key", False):
        # Remote-sharding backend with no attach: refuse rather than collapse
        # onto a single hot DO.
        return _derive_shard_key(attach_uuid=uuid, _origin=_origin)
    return ""


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

    Idempotency: a concern of the remote (HTTP) tier only. The CfDo backend
    generates an internal ``attempt_id`` per call so a retried ``state_put_many``
    is a silent no-op and a retried ``state_drain`` returns the prior values.
    The local SQLite tier is a single connection per process with no network
    retries, so it carries no replay-detection (and no idempotency columns).

    Eviction / lifecycle. Every scope-keyed table (``function_state``,
    ``function_state_log``, ``function_counter``) is reclaimed for a scope by
    ``execution_clear`` — called at operator teardown for execution-scoped state
    and on commit/rollback for transaction-scoped state. Beyond that, each
    backend differs: the **CfDo** DO self-evicts via an orphan-horizon alarm
    (idle DO → ``deleteAll``); **Azure SQL** relies on ``cleanup_old_entries``,
    an age-based sweep over the ``created_at`` column that must be scheduled
    externally (so every age-managed table needs a ``created_at``); the local
    **SQLite** tier is durable with no auto-eviction — long-lived, attach-scoped
    data (e.g. an accumulate collection) is the consumer's responsibility to
    bound (``ttl`` / ``max_row_size`` / explicit clear). The
    ``test_execution_clear_covers_all_scope_keyed_tables`` audit pins that every
    scope-keyed table is wiped by ``execution_clear``. (Follow-up: some
    ``worker.py`` teardown paths call ``execution_clear`` without a try/except,
    so a cleanup exception there can still leak — to be hardened separately.)

    """

    # Note: backends that route remotely on ``shard_key`` (CfDo) set a
    # ``requires_shard_key = True`` class attribute; ``_resolve_shard_key`` reads
    # it via ``getattr(.., False)``. It is intentionally NOT declared here as a
    # Protocol member so the in-process backends (SQLite / Azure), which ignore
    # shard_key, still structurally satisfy ``FunctionStorage`` without it.

    # --- Work Queue (distributed work items) ---

    def queue_push(self, execution_id: bytes, items: list[bytes], *, shard_key: str = "") -> int:
        """Append work items to the queue.

        There is no registration step — the queue tracks only the items
        themselves (matching the Durable Object).

        Args:
            execution_id: Unique identifier for the function invocation.
            items: List of serialized work item bytes.
            shard_key: Routing key for the CF DO backend; ignored by
                SQLite / Azure backends. Set automatically by [`BoundStorage`][]
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
                SQLite / Azure backends. Set automatically by [`BoundStorage`][]
                from the caller's attach_opaque_data / auth context.

        Returns:
            Serialized work item bytes, or None if the queue is empty or the
            execution_id was never pushed. There is no registration, so the
            backend does not distinguish a never-pushed id from a drained
            queue — both return None (matching the Durable Object).

        """
        ...

    def queue_clear(self, execution_id: bytes, *, shard_key: str = "") -> int:
        """Clear all remaining work items for the execution.

        Args:
            execution_id: Unique identifier for the function invocation.
            shard_key: Routing key for the CF DO backend; ignored by
                SQLite / Azure backends. Set automatically by [`BoundStorage`][]
                from the caller's attach_opaque_data / auth context.

        Returns:
            Number of items deleted.

        """
        ...

    # ========================================================================
    # Unified state_* API (composite-key K/V over (scope_id, ns, key))
    # ========================================================================
    #
    # This API replaces the four RMW families
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
    # Idempotency: the public API does not expose ``attempt_id``. It is an
    # HTTP-tier concern — the CfDo client generates one per call and splices the
    # same id into retries within its ``_post()`` loop so the Durable Object can
    # detect a replay across the network. The local SQLite tier has no network
    # retries and carries no replay-detection (nor the idempotency columns).

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

        Remote backends (CfDo) carry an internal ``attempt_id`` so an HTTP
        retry is detected as a replay and silently no-ops. Local backends
        (SQLite) are a single connection per process with no network retries,
        so they need no replay-detection.
        """
        ...

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
    ) -> Iterable[tuple[bytes, bytes]]:
        """Non-destructive scan of ``(key, value)`` in one namespace.

        Returns an iterable of ``(key, value)`` ordered by key bytes (unsigned
        lexicographic / memcmp). ``reverse=True`` orders descending. The scan is
        bounded to the half-open key range ``[start, end)`` (either bound
        ``None`` is open) and capped at ``limit`` rows (``None`` = unbounded).
        Large result sets may be streamed in pages by the backend (the
        ``cloudflare-do`` backend pages under the hood), so callers should
        iterate rather than assume a materialized list. Use when you need to
        enumerate an unknown key set (e.g. drainer-side discovery of which sink
        threads produced state).
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

        Returns an iterable of ``(key, value)`` ordered by key. Remote backends
        (CfDo) tombstone the rows for HTTP replay-detection (a retried drain
        returns the same values without re-deleting) and stream the result in
        pages; local backends delete outright. The drain is atomic — beginning
        to iterate claims the whole namespace, so always consume it fully.
        """
        ...

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
        """Delete by key list, by key range, or wipe the entire namespace.

        ``keys=[...]`` deletes those keys. ``keys is None`` with a ``start``
        and/or ``end`` deletes the half-open key range ``[start, end)`` (either
        bound ``None`` is open). ``keys is None`` with no range wipes the whole
        namespace. ``keys`` and the range are mutually exclusive.

        Naturally idempotent — deleting an already-deleted key/range is a no-op.
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
        """Wipe ALL state, log, and counter rows for ``scope_id`` across every namespace.

        Used as a safety-sweep at end-of-execution / on crash recovery.
        Naturally idempotent. Returns total row count deleted across the
        ``function_state``, ``function_state_log``, and ``function_counter`` tables.

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

        **Idempotency scope.** Remote backends carry an internal
        ``attempt_id`` covering *transport-layer* retries within a single
        backend call (an HTTP retry on CfDo replays correctly); local SQLite
        has no retry layer. **Caller-level retries** (re-invoking
        ``state_append`` for the same logical record after the call already
        returned) always produce duplicate rows. If you need caller-level
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

    # --- Atomic int64 counters (separate ``function_counter`` table) ---
    #
    # A typed numeric facet kept apart from the opaque ``function_state`` K/V so
    # the value column never has to carry numeric semantics. Keyed by the same
    # ``(scope_id, ns, key)`` shape. ``state_counter_add`` is an atomic
    # read-add-return in one statement (no caller-side CAS loop); the others are
    # plain upsert / select / delete.

    def state_counter_get(
        self,
        scope_id: bytes,
        ns: bytes,
        key: bytes,
        *,
        shard_key: str = "",
    ) -> int:
        """Return the int64 counter at ``(scope_id, ns, key)``; 0 if absent."""
        ...

    def state_counter_add(
        self,
        scope_id: bytes,
        ns: bytes,
        key: bytes,
        delta: int,
        *,
        shard_key: str = "",
    ) -> int:
        """Atomically add ``delta`` and return the new value (init 0 if absent).

        Single-statement upsert — no read-modify-write race, no caller loop.
        Not idempotent: a retried add double-applies. Remote/cloud backends
        carry an internal ``attempt_id`` (as ``state_put_many`` does) so a
        transport retry replays the prior result instead of re-adding; the
        local SQLite tier has no retry layer.
        """
        ...

    def state_counter_set(
        self,
        scope_id: bytes,
        ns: bytes,
        key: bytes,
        value: int,
        *,
        shard_key: str = "",
    ) -> None:
        """Overwrite the counter at ``(scope_id, ns, key)`` with ``value``."""
        ...

    def state_counter_delete(
        self,
        scope_id: bytes,
        ns: bytes,
        key: bytes,
        *,
        shard_key: str = "",
    ) -> None:
        """Delete the counter at ``(scope_id, ns, key)`` (no-op if absent)."""
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
        attach_plaintext: bytes | None = None,
        shard_key: str | None = None,
    ) -> None:
        self._base = storage
        self._transaction_opaque_data = transaction_opaque_data
        # ``attach_plaintext`` is the framework-unwrapped attach
        # (``uuid(16) || catalog_bytes``); we shard on its leading UUID. Callers
        # may instead pass an already-resolved ``shard_key=`` (e.g. inherited
        # from a parent BoundStorage). ``request=`` only labels the origin.
        if shard_key is None:
            origin = (
                f"TransactionBoundStorage({type(request).__name__})"
                if request is not None
                else "TransactionBoundStorage"
            )
            shard_key = _resolve_shard_key(storage, attach_plaintext, origin)
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
        attach_plaintext: bytes | None = None,
    ):
        self._base = storage
        self._execution_id = execution_id
        # ``attach_plaintext`` is the framework-unwrapped attach
        # (``uuid(16) || catalog_bytes``); we shard on its leading UUID. The
        # worker unwraps once and threads it in. ``request=`` only labels the
        # derivation origin for debug logs.
        origin = f"BoundStorage({type(request).__name__})" if request is not None else "BoundStorage"
        self._shard_key = _resolve_shard_key(storage, attach_plaintext, origin)

    def transaction(self, transaction_opaque_data: bytes) -> TransactionBoundStorage:
        """Return a transaction-scoped storage view.

        Used for state that the user expects to be stable across
        multiple statements in one SQL transaction (e.g. Kafka topic
        watermarks, for snapshot-isolation reads).
        """
        # Inherit our resolved shard_key directly — both views are part of the
        # same logical attach and shard identically.
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
        result = self._base.state_get_many(self._execution_id, _coerce_ns(ns), [key], shard_key=self._shard_key)
        return result[0]

    @_profiled("state_get_many")
    def state_get_many(self, ns: "bytes | FrameworkNS", keys: list[bytes]) -> list[bytes | None]:
        """Batched non-destructive read."""
        return self._base.state_get_many(self._execution_id, _coerce_ns(ns), keys, shard_key=self._shard_key)

    @_profiled("state_put")
    def state_put(self, ns: "bytes | FrameworkNS", key: bytes, value: bytes) -> None:
        """Upsert one (key, value)."""
        self._base.state_put_many(self._execution_id, _coerce_ns(ns), [(key, value)], shard_key=self._shard_key)

    @_profiled("state_put_many")
    def state_put_many(self, ns: "bytes | FrameworkNS", items: list[tuple[bytes, bytes]]) -> None:
        """Batched atomic upsert."""
        self._base.state_put_many(self._execution_id, _coerce_ns(ns), items, shard_key=self._shard_key)

    @_profiled("state_scan")
    def state_scan(
        self,
        ns: "bytes | FrameworkNS",
        *,
        start: bytes | None = None,
        end: bytes | None = None,
        reverse: bool = False,
        limit: int | None = None,
    ) -> Iterable[tuple[bytes, bytes]]:
        """Non-destructive scan of (key, value) in one namespace.

        Ordered by key bytes (``reverse=True`` for descending), bounded to the
        half-open range ``[start, end)`` and capped at ``limit``. Returns an
        iterable (the cloudflare-do backend streams it in pages).
        """
        return self._base.state_scan(
            self._execution_id,
            _coerce_ns(ns),
            start=start,
            end=end,
            reverse=reverse,
            limit=limit,
            shard_key=self._shard_key,
        )

    @_profiled("state_drain")
    def state_drain(self, ns: "bytes | FrameworkNS") -> Iterable[tuple[bytes, bytes]]:
        """Atomic scan-and-delete of every (key, value) in one namespace.

        Returns an iterable; consume it fully (beginning to iterate claims the
        whole namespace on the cloudflare-do backend).
        """
        return self._base.state_drain(self._execution_id, _coerce_ns(ns), shard_key=self._shard_key)

    @_profiled("state_delete")
    def state_delete(
        self,
        ns: "bytes | FrameworkNS",
        keys: list[bytes] | None = None,
        *,
        start: bytes | None = None,
        end: bytes | None = None,
    ) -> int:
        """Delete by key list, by half-open ``[start, end)`` range, or wipe all.

        ``keys`` and the range are mutually exclusive. See
        ``FunctionStorage.state_delete`` for the full contract.
        """
        return self._base.state_delete(
            self._execution_id,
            _coerce_ns(ns),
            keys,
            start=start,
            end=end,
            shard_key=self._shard_key,
        )

    @_profiled("execution_clear")
    def execution_clear(self) -> int:
        """Wipe ALL state and log rows for this execution across every namespace."""
        return self._base.execution_clear(self._execution_id, shard_key=self._shard_key)

    @_profiled("state_append")
    def state_append(self, ns: "bytes | FrameworkNS", key: bytes, item: bytes) -> int:
        """Append an item to the (ns, key) log; return the assigned ordinal.

        Idempotency covers transport-layer retries only (HTTP retry on
        CfDo, pymssql driver-level retry on Azure SQL). Caller-level
        retries — re-invoking ``state_append`` for the same logical
        record after it returned — produce duplicate rows. See the
        underlying ``FunctionStorage.state_append`` for the full contract.
        """
        return self._base.state_append(self._execution_id, _coerce_ns(ns), key, item, shard_key=self._shard_key)

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
            self._execution_id,
            _coerce_ns(ns),
            key,
            after_id=after_id,
            limit=limit,
            shard_key=self._shard_key,
        )

    # --- Atomic int64 counters (function_counter table) ---

    @_profiled("state_counter_get")
    def counter_get(self, ns: "bytes | FrameworkNS", key: bytes) -> int:
        """Read the int64 counter (0 if absent)."""
        return self._base.state_counter_get(self._execution_id, _coerce_ns(ns), key, shard_key=self._shard_key)

    @_profiled("state_counter_add")
    def counter_add(self, ns: "bytes | FrameworkNS", key: bytes, delta: int) -> int:
        """Atomically add ``delta``; return the new value. See FunctionStorage."""
        return self._base.state_counter_add(self._execution_id, _coerce_ns(ns), key, delta, shard_key=self._shard_key)

    @_profiled("state_counter_set")
    def counter_set(self, ns: "bytes | FrameworkNS", key: bytes, value: int) -> None:
        """Overwrite the counter with ``value``."""
        self._base.state_counter_set(self._execution_id, _coerce_ns(ns), key, value, shard_key=self._shard_key)

    @_profiled("state_counter_delete")
    def counter_delete(self, ns: "bytes | FrameworkNS", key: bytes) -> None:
        """Delete the counter (no-op if absent)."""
        self._base.state_counter_delete(self._execution_id, _coerce_ns(ns), key, shard_key=self._shard_key)

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
    processes to share state. It manages the three unified tables (the same
    shape every backend uses):

    - function_state: composite-key K/V over (scope_id, ns, key) — the single
      home for per-execution / per-transaction / per-group / per-pid state
    - function_state_log: append-only log keyed by (scope_id, ns, key)
    - work_queue: FIFO queue of work items per execution

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
            # Self-heal an older on-disk DB to the unified minimal schema. The
            # local SQLite tier is single-connection-per-process with no network
            # retries, so it carries none of the DO's HTTP idempotency machinery
            # (last_attempt_id / drained_* / attempt_id / created_at). Drop any
            # table left over with the old idempotency columns so the CREATEs
            # below recreate the minimal shape. All of this state is ephemeral
            # in-progress worker state, so dropping + recreating is safe.
            for table, stale_col in [
                ("function_state", "last_attempt_id"),
                ("function_state_log", "attempt_id"),
                ("work_queue", "created_at"),
            ]:
                cols = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}  # noqa: S608
                if stale_col in cols:
                    conn.execute(f"DROP TABLE IF EXISTS {table}")  # noqa: S608
            # Tables eliminated by the unified schema: worker collect now rides
            # function_state + state_drain, and the queue carries no registration
            # (matching the Durable Object — pop on an unknown id returns None).
            for dead in ("global_state_storage", "worker_state", "invocation_registry", "init_storage"):
                conn.execute(f"DROP TABLE IF EXISTS {dead}")  # noqa: S608

            # ----------------------------------------------------------------
            # Unified schema — the same three tables every backend uses (the
            # Durable Object adds an HTTP-idempotency column layer on top).
            #   work_queue          — FIFO work items, destructive pop.
            #   function_state      — composite-key K/V over (scope_id, ns, key);
            #                          the single home for per-execution /
            #                          per-transaction / per-group / per-pid
            #                          state. Caller picks ``ns``; storage
            #                          doesn't interpret it.
            #   function_state_log  — append-only log keyed by (scope, ns, key);
            #                          the AUTOINCREMENT id is the scan cursor.
            # ----------------------------------------------------------------
            conn.execute("""
                CREATE TABLE IF NOT EXISTS work_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    execution_id BLOB NOT NULL,
                    work_item BLOB NOT NULL
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_work_queue_execution
                ON work_queue(execution_id, id)
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS function_state (
                    scope_id BLOB NOT NULL,
                    ns       BLOB NOT NULL,
                    key      BLOB NOT NULL,
                    value    BLOB NOT NULL,
                    PRIMARY KEY (scope_id, ns, key)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS function_state_log (
                    id       INTEGER PRIMARY KEY AUTOINCREMENT,
                    scope_id BLOB NOT NULL,
                    ns       BLOB NOT NULL,
                    key      BLOB NOT NULL,
                    value    BLOB NOT NULL
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_function_state_log_lookup
                    ON function_state_log(scope_id, ns, key, id)
            """)
            # function_counter — atomic int64 counters, a typed numeric facet
            # kept apart from the opaque function_state K/V. No idempotency
            # columns: the local SQLite tier has no network retry layer.
            conn.execute("""
                CREATE TABLE IF NOT EXISTS function_counter (
                    scope_id BLOB NOT NULL,
                    ns       BLOB NOT NULL,
                    key      BLOB NOT NULL,
                    n        INTEGER NOT NULL,
                    PRIMARY KEY (scope_id, ns, key)
                )
            """)
            conn.commit()
        finally:
            conn.close()

    # --- Work Queue ---

    def queue_push(self, execution_id: bytes, items: list[bytes], *, shard_key: str = "") -> int:
        """Append work items to the queue."""
        conn = self._conn()
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

        Returns None when the queue is empty or the execution_id was never
        pushed — there is no registration, matching the Durable Object.
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
        """Clear all remaining work items for the execution."""
        conn = self._conn()
        cursor = conn.execute(
            "DELETE FROM work_queue WHERE execution_id = ?",
            (execution_id,),
        )
        conn.commit()
        return cursor.rowcount

    # ========================================================================
    # Unified state_* implementation
    # ========================================================================
    #
    # See FunctionStorage protocol docstrings for contracts. No idempotency /
    # replay-detection here: that exists only on the HTTP tier (the Durable
    # Object) to dedup network retries. A local SQLite connection has no retry
    # layer above these methods, so mutations are plain writes.

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
        placeholders = ",".join("?" for _ in keys)
        rows = conn.execute(
            f"""
            SELECT key, value FROM function_state
            WHERE scope_id = ? AND ns = ? AND key IN ({placeholders})
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
        """Atomic batched upsert by (scope_id, ns, key)."""
        del shard_key
        if not items:
            return
        conn = self._conn()
        conn.executemany(
            """
            INSERT INTO function_state (scope_id, ns, key, value)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(scope_id, ns, key) DO UPDATE SET
                value = excluded.value
            """,
            [(scope_id, ns, k, v) for k, v in items],
        )
        conn.commit()

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
    ) -> list[tuple[bytes, bytes]]:
        """Non-destructive scan of (key, value) in a namespace.

        Ordered by key bytes (BLOB compares bytewise / memcmp), descending when
        ``reverse``, bounded to ``[start, end)`` and capped at ``limit``.
        """
        del shard_key
        conn = self._conn()
        params: list[object] = [scope_id, ns]
        clauses = ""
        if start is not None:
            clauses += " AND key >= ?"
            params.append(start)
        if end is not None:
            clauses += " AND key < ?"
            params.append(end)
        order = "DESC" if reverse else "ASC"
        params.append(-1 if limit is None else int(limit))
        rows = conn.execute(
            f"""
            SELECT key, value FROM function_state
            WHERE scope_id = ? AND ns = ?{clauses}
            ORDER BY key {order}
            LIMIT ?
            """,  # noqa: S608 — order is a fixed ASC/DESC literal, not user input
            tuple(params),
        ).fetchall()
        return [(bytes(k), bytes(v)) for k, v in rows]

    def state_drain(
        self,
        scope_id: bytes,
        ns: bytes,
        *,
        shard_key: str = "",
    ) -> list[tuple[bytes, bytes]]:
        """Atomic destructive scan: read all (key, value) in a namespace and delete them."""
        del shard_key
        conn = self._conn()
        # DELETE ... RETURNING (SQLite ≥3.35) reads and removes in one
        # statement; the connection serializes it. No tombstone/replay layer —
        # that exists only on the HTTP tier (the Durable Object) for retry
        # safety, which a single local connection doesn't face.
        rows = conn.execute(
            """
            DELETE FROM function_state
            WHERE scope_id = ? AND ns = ?
            RETURNING key, value
            """,
            (scope_id, ns),
        ).fetchall()
        conn.commit()
        return [(bytes(k), bytes(v)) for k, v in rows]

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
        """Delete by key list, by ``[start, end)`` range, or whole namespace.

        ``keys`` and the range are mutually exclusive. Returns count deleted.
        """
        del shard_key
        if keys is not None and (start is not None or end is not None):
            raise ValueError("state_delete: keys and start/end are mutually exclusive")
        conn = self._conn()
        if keys is not None:
            if not keys:
                return 0
            placeholders = ",".join("?" for _ in keys)
            cur = conn.execute(
                f"""
                DELETE FROM function_state
                WHERE scope_id = ? AND ns = ? AND key IN ({placeholders})
                """,  # noqa: S608 — placeholders are bound '?' params, not interpolated values
                (scope_id, ns, *keys),
            )
        else:
            params: list[object] = [scope_id, ns]
            clauses = ""
            if start is not None:
                clauses += " AND key >= ?"
                params.append(start)
            if end is not None:
                clauses += " AND key < ?"
                params.append(end)
            cur = conn.execute(
                f"DELETE FROM function_state WHERE scope_id = ? AND ns = ?{clauses}",  # noqa: S608
                tuple(params),
            )
        conn.commit()
        return int(cur.rowcount)

    def execution_clear(
        self,
        scope_id: bytes,
        *,
        shard_key: str = "",
    ) -> int:
        """Wipe all state, log, and counter rows for scope_id across every namespace."""
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
        c3 = conn.execute(
            "DELETE FROM function_counter WHERE scope_id = ?",
            (scope_id,),
        )
        conn.commit()
        return int(c1.rowcount) + int(c2.rowcount) + int(c3.rowcount)

    def state_append(
        self,
        scope_id: bytes,
        ns: bytes,
        key: bytes,
        item: bytes,
        *,
        shard_key: str = "",
    ) -> int:
        """Append item to the (scope_id, ns, key) log; return its ordinal (the row id)."""
        del shard_key
        conn = self._conn()
        row = conn.execute(
            """
            INSERT INTO function_state_log (scope_id, ns, key, value)
            VALUES (?, ?, ?, ?)
            RETURNING id
            """,
            (scope_id, ns, key, item),
        ).fetchone()
        conn.commit()
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
            sql,
            (scope_id, ns, key, after_id, sqlite_limit),
        ).fetchall()
        return [(int(rid), bytes(v)) for (rid, v) in rows]

    # --- Atomic int64 counters (function_counter) ---
    # No idempotency layer: a local single-connection backend has no retries.

    def state_counter_get(self, scope_id: bytes, ns: bytes, key: bytes, *, shard_key: str = "") -> int:
        """Read the int64 counter; 0 if absent."""
        del shard_key
        row = (
            self._conn()
            .execute(
                "SELECT n FROM function_counter WHERE scope_id = ? AND ns = ? AND key = ?",
                (scope_id, ns, key),
            )
            .fetchone()
        )
        return int(row[0]) if row else 0

    def state_counter_add(
        self,
        scope_id: bytes,
        ns: bytes,
        key: bytes,
        delta: int,
        *,
        shard_key: str = "",
    ) -> int:
        """Atomically add ``delta`` and return the new value (init 0 if absent)."""
        del shard_key
        conn = self._conn()
        row = conn.execute(
            """
            INSERT INTO function_counter (scope_id, ns, key, n)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(scope_id, ns, key) DO UPDATE SET n = n + excluded.n
            RETURNING n
            """,
            (scope_id, ns, key, int(delta)),
        ).fetchone()
        conn.commit()
        return int(row[0])

    def state_counter_set(
        self,
        scope_id: bytes,
        ns: bytes,
        key: bytes,
        value: int,
        *,
        shard_key: str = "",
    ) -> None:
        """Overwrite the counter with ``value``."""
        del shard_key
        conn = self._conn()
        conn.execute(
            """
            INSERT INTO function_counter (scope_id, ns, key, n)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(scope_id, ns, key) DO UPDATE SET n = excluded.n
            """,
            (scope_id, ns, key, int(value)),
        )
        conn.commit()

    def state_counter_delete(self, scope_id: bytes, ns: bytes, key: bytes, *, shard_key: str = "") -> None:
        """Delete the counter (no-op if absent)."""
        del shard_key
        conn = self._conn()
        conn.execute(
            "DELETE FROM function_counter WHERE scope_id = ? AND ns = ? AND key = ?",
            (scope_id, ns, key),
        )
        conn.commit()


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
        """Namespace an execution_id / scope_id by shard_key.

        Transparent to the worker — only the sqlite row key changes, never the
        returned data.
        """
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
    ) -> list[tuple[bytes, bytes]]:
        self._dbg("state_scan", shard_key, scope_id)
        return self._inner.state_scan(
            self._p(shard_key, scope_id),
            ns,
            start=start,
            end=end,
            reverse=reverse,
            limit=limit,
        )

    def state_drain(self, scope_id: bytes, ns: bytes, *, shard_key: str = "") -> list[tuple[bytes, bytes]]:
        self._dbg("state_drain", shard_key, scope_id)
        return self._inner.state_drain(self._p(shard_key, scope_id), ns)

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
        self._dbg("state_delete", shard_key, scope_id)
        return self._inner.state_delete(self._p(shard_key, scope_id), ns, keys, start=start, end=end)

    def execution_clear(self, scope_id: bytes, *, shard_key: str = "") -> int:
        self._dbg("execution_clear", shard_key, scope_id)
        return self._inner.execution_clear(self._p(shard_key, scope_id))

    def state_append(self, scope_id: bytes, ns: bytes, key: bytes, item: bytes, *, shard_key: str = "") -> int:
        self._dbg("state_append", shard_key, scope_id)
        return self._inner.state_append(self._p(shard_key, scope_id), ns, key, item)

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
        self._dbg("state_log_scan", shard_key, scope_id)
        return self._inner.state_log_scan(self._p(shard_key, scope_id), ns, key, after_id=after_id, limit=limit)

    # --- Atomic int64 counters ---
    def state_counter_get(self, scope_id: bytes, ns: bytes, key: bytes, *, shard_key: str = "") -> int:
        self._dbg("state_counter_get", shard_key, scope_id)
        return self._inner.state_counter_get(self._p(shard_key, scope_id), ns, key)

    def state_counter_add(
        self,
        scope_id: bytes,
        ns: bytes,
        key: bytes,
        delta: int,
        *,
        shard_key: str = "",
    ) -> int:
        self._dbg("state_counter_add", shard_key, scope_id)
        return self._inner.state_counter_add(self._p(shard_key, scope_id), ns, key, delta)

    def state_counter_set(
        self,
        scope_id: bytes,
        ns: bytes,
        key: bytes,
        value: int,
        *,
        shard_key: str = "",
    ) -> None:
        self._dbg("state_counter_set", shard_key, scope_id)
        self._inner.state_counter_set(self._p(shard_key, scope_id), ns, key, value)

    def state_counter_delete(self, scope_id: bytes, ns: bytes, key: bytes, *, shard_key: str = "") -> None:
        self._dbg("state_counter_delete", shard_key, scope_id)
        self._inner.state_counter_delete(self._p(shard_key, scope_id), ns, key)

    def close(self) -> None:
        self._inner.close()
