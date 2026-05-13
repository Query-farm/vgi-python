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

import hashlib
import logging
import os
import random
import sqlite3
import threading
from typing import Any, Protocol

import pyarrow as pa


# When the parent vgi.* logger is configured at DEBUG, this emits one line
# per BoundStorage construction with the resolved shard_key — handy for
# cross-referencing storage-routing bugs with MetaWorker dispatch logs.
_shard_logger = logging.getLogger("vgi.storage.shard")

__all__ = [
    "FunctionStorage",
    "FunctionStorageSqlite",
]


def _derive_shard_key(*, attach_id: bytes | None, auth: Any, _origin: str = "?") -> str:
    """Return the routing key for the ``FunctionStorageCfDo`` Durable Object.

    Server-derived inside the trusted worker process. The CF DO routes by
    this key (``idFromName(shard_key)``), so one DO instance hosts every
    storage op carrying the same shard_key. Precedence:

      1. ``attach_id`` (worker-vended bytes from ``catalog_attach``) — one
         DO per logical ATTACH. Best amortization for ATTACH-ed catalogs.
         Note: workers are NOT required to make attach_ids globally unique,
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
    if attach_id is not None:
        key = "att-" + attach_id.hex()
    elif auth is not None and getattr(auth, "authenticated", False):
        domain = getattr(auth, "domain", "")
        principal = getattr(auth, "principal", "")
        digest = hashlib.sha256(f"{domain}\0{principal}".encode()).hexdigest()
        key = "prn-" + digest[:32]
    else:
        key = "loc-anon"
    if _shard_logger.isEnabledFor(logging.DEBUG):
        _shard_logger.debug(
            "shard derived origin=%s attach_id=%s authed=%d key=%s",
            _origin,
            attach_id.hex()[:16] if attach_id else "-",
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

    Provides three access patterns for distributed function state:

    **Worker State** - Partial results from each worker process.
    Each worker stores its state during processing, primary worker collects
    all states during finalization for aggregation.

    **Work Queue** - Atomic work distribution across workers.
    Primary worker enqueues work items, workers atomically claim items
    from the queue for processing.

    **Aggregate State** - Per-group-id state for aggregate functions.
    Each group_id maps to a serialized state blob. Thread-local hash tables
    in DuckDB guarantee no concurrent writes to the same group_id, so no
    versioning or locking is needed.

    """

    def worker_put(
        self,
        execution_id: bytes,
        worker_id: int,
        state: bytes,
        *,
        shard_key: str = "",
    ) -> None:
        """Store state for a specific worker.

        If state already exists for this (execution_id, worker_id) pair,
        it will be replaced.

        Args:
            execution_id: Unique identifier for the function invocation.
            worker_id: Process ID of the worker storing the state.
            state: Serialized state bytes.
            shard_key: Routing key for the CF DO backend. Ignored by
                SQLite / Azure backends. Set automatically by
                BoundStorage from the caller's attach_id / auth context;
                manual callers can leave it empty for the default
                ``"loc-anon"`` shard.

        """
        ...

    def worker_collect(self, execution_id: bytes, *, shard_key: str = "") -> list[bytes]:
        """Atomically collect and delete all worker states.

        This is typically called by the primary worker during finalization
        to collect all worker states for aggregation.

        Args:
            execution_id: Unique identifier for the function invocation.

        Returns:
            List of serialized state bytes from all workers.

        """
        ...

    def worker_scan(self, execution_id: bytes, *, shard_key: str = "") -> list[tuple[int, bytes]]:
        """Non-destructive read of all worker states for an execution.

        Companion to ``worker_collect`` for use cases where multiple
        readers need to observe the same set of worker states without
        mutating storage. The intended consumer is the table-function
        ``dynamic_to_string`` hook, which fires once per parallel scan
        thread at end-of-stream and is best-effort: every thread must
        be able to read the full set, which precludes the
        atomic-destructive semantics of ``worker_collect``.

        Args:
            execution_id: Unique identifier for the function invocation.

        Returns:
            List of ``(worker_id, state_bytes)`` pairs. Order is
            implementation-defined.

        """
        ...

    # --- Scan Worker State (per-stream-id, distinct from worker_state) ---
    #
    # ``worker_state`` above is keyed by ``os.getpid()`` and conflates threads
    # in one process — under HTTP transport (waitress thread pool) every scan
    # worker landing in the same Python process collides on a single row.
    # ``scan_worker_state`` keys by the framework's per-scan-worker UUID
    # (``vgi_rpc.rpc._common._current_stream_id``), giving each scan worker
    # its own row regardless of pid/thread/machine.

    def stream_state_put(self, execution_id: bytes, stream_id: bytes, state: bytes, *, shard_key: str = "") -> None:
        """Store per-scan-worker state for an execution.

        Replaces any prior state for the same ``(execution_id, stream_id)``
        pair — successive ticks of one scan worker overwrite each other.

        Args:
            execution_id: Unique identifier for the function invocation.
            stream_id: The framework's per-scan-worker stream UUID
                (raw bytes; typically the 16-byte form of the hex
                ``_current_stream_id`` ContextVar).
            state: Serialized state bytes.

        """
        ...

    def stream_state_scan(self, execution_id: bytes, *, shard_key: str = "") -> list[tuple[bytes, bytes]]:
        """Non-destructive read of all per-scan-worker states for an execution.

        The intended consumer is the table-function ``dynamic_to_string``
        hook. Each scan worker writes one row keyed by its stream_id;
        ``dynamic_to_string`` reads all of them to build the EXPLAIN ANALYZE
        Extra Info block.

        Args:
            execution_id: Unique identifier for the function invocation.

        Returns:
            List of ``(stream_id, state_bytes)`` pairs. Order is
            implementation-defined.

        """
        ...

    # --- Work Queue (distributed work items) ---

    def queue_push(self, execution_id: bytes, items: list[bytes], *, shard_key: str = "") -> int:
        """Add work items to the queue and register the invocation.

        This method registers the execution_id as valid, allowing subsequent
        queue_pop calls. Even if items is empty, the invocation is registered.

        Args:
            execution_id: Unique identifier for the function invocation.
            items: List of serialized work item bytes.

        Returns:
            Number of items added.

        """
        ...

    def queue_pop(self, execution_id: bytes, *, shard_key: str = "") -> bytes | None:
        """Atomically claim one work item from the queue.

        Args:
            execution_id: Unique identifier for the function invocation.

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

        Returns:
            Number of items deleted.

        """
        ...

    # --- Aggregate State (per-group-id storage for aggregate functions) ---

    def aggregate_state_get(self, execution_id: bytes, group_ids: list[int], *, shard_key: str = "") -> list[tuple[int, bytes] | None]:
        """Load aggregate states for specific group_ids.

        Non-destructive read. Returns a list parallel to the input
        ``group_ids``: each element is ``(group_id, state_bytes)`` for
        groups that exist, or ``None`` for unknown group_ids.

        Args:
            execution_id: Unique identifier for the function invocation.
            group_ids: List of group_ids to look up.

        Returns:
            List parallel to ``group_ids`` with found states or None.

        """
        ...

    def aggregate_state_put(self, execution_id: bytes, data: list[tuple[int, bytes]], *, shard_key: str = "") -> None:
        """Unconditionally write aggregate states for the given group_ids.

        Uses INSERT OR REPLACE semantics — existing states are overwritten.
        No version checking is performed; DuckDB's thread-local hash tables
        guarantee that each group_id is only written by one thread during
        the UPDATE phase.

        Args:
            execution_id: Unique identifier for the function invocation.
            data: List of ``(group_id, state_bytes)`` pairs to store.

        """
        ...

    def aggregate_state_clear(self, execution_id: bytes, *, shard_key: str = "") -> None:
        """Remove all aggregate states for an execution_id.

        Called after all FINALIZE exchanges complete to clean up storage.

        Args:
            execution_id: Unique identifier for the function invocation.

        """
        ...

    # --- Transaction State (per-transaction K/V; visible across executions) ---
    #
    # Unlike ``worker_state`` and ``aggregate_state`` (both keyed by
    # ``execution_id`` — i.e. one query/init), transaction state is keyed by
    # ``transaction_id`` so that every execution within a SQL transaction
    # sees the same store. The intended use is "snapshot data the user
    # expects to be stable for the lifetime of the transaction" —
    # e.g. Kafka topic watermarks. Repeated reads of the same key return
    # the same bytes regardless of the broker's current state, so a user
    # who does ``BEGIN; SELECT ...; SELECT ...; COMMIT;`` gets the
    # row count they expect.
    #
    # Implementations should bound staleness via ``cleanup_old_entries``
    # (transaction_id never reused after rollback/commit, but we don't
    # always get a callback).

    def transaction_state_get(self, transaction_id: bytes, keys: list[bytes], *, shard_key: str = "") -> list[bytes | None]:
        """Load transaction-scoped state values for the given keys.

        Returns a list parallel to ``keys``: each element is the stored
        ``bytes`` value or ``None`` if the (transaction_id, key) pair is
        unknown. Non-destructive — repeated calls return the same bytes.

        Args:
            transaction_id: Caller-supplied transaction identifier
                (typically the framework's catalog ``transaction_id``).
            keys: List of binary keys to look up.

        Returns:
            List parallel to ``keys`` with found values or ``None``.

        """
        ...

    def transaction_state_put(self, transaction_id: bytes, items: list[tuple[bytes, bytes]], *, shard_key: str = "") -> None:
        """Unconditionally write transaction-scoped state values.

        ``INSERT OR REPLACE`` semantics — existing values for the same
        ``(transaction_id, key)`` are overwritten. Implementations
        should be safe to call concurrently from multiple workers.

        Args:
            transaction_id: Caller-supplied transaction identifier.
            items: List of ``(key, value)`` byte tuples.

        """
        ...

    def transaction_state_clear(self, transaction_id: bytes, *, shard_key: str = "") -> None:
        """Drop all state for a transaction.

        Called by the catalog implementation when it observes a commit
        or rollback. Implementations also cleanup old entries via TTL
        sweep so a leaked transaction_id eventually GCs even without
        the explicit clear.

        Args:
            transaction_id: Caller-supplied transaction identifier.

        """
        ...

    # --- Aggregate Window Partition (per-partition cached input for windowed aggregates) ---

    def aggregate_window_partition_put(self, execution_id: bytes, partition_id: int, data: bytes, *, shard_key: str = "") -> None:
        """Write a cached partition payload for a windowed aggregate.

        The payload is typically an Arrow IPC stream carrying the full
        partition input plus any derived window_state. Keyed by
        ``(execution_id, partition_id)`` with INSERT OR REPLACE semantics.
        """
        ...

    def aggregate_window_partition_get(self, execution_id: bytes, partition_id: int, *, shard_key: str = "") -> bytes | None:
        """Load the cached partition payload for a windowed aggregate."""
        ...

    def aggregate_window_partition_delete(self, execution_id: bytes, partition_id: int, *, shard_key: str = "") -> None:
        """Delete one cached partition. No-op if not present."""
        ...

    def aggregate_window_partition_clear(self, execution_id: bytes, *, shard_key: str = "") -> None:
        """Remove all cached partitions for an execution_id.

        Safety-sweep called from ``aggregate_destructor`` to catch any
        partitions whose destructor RPCs were dropped mid-query.
        """
        ...


class TransactionBoundStorage:
    """Convenience wrapper bound to a single transaction_id.

    Lets a function read/write transaction-scoped state without
    threading the transaction_id through every call site. Get one via
    ``BoundStorage.transaction(transaction_id)``.
    """

    def __init__(
        self,
        storage: "FunctionStorage",
        transaction_id: bytes,
        *,
        request: Any = None,
        attach_id: bytes | None = None,
        auth: Any = None,
        shard_key: str | None = None,
    ) -> None:
        self._base = storage
        self._transaction_id = transaction_id
        # Caller may pass shard_key directly (e.g. inherited from a parent
        # BoundStorage), or pass request= / attach_id= / auth= and let us
        # derive. See BoundStorage for the request= polymorphism.
        if shard_key is None:
            origin = "TransactionBoundStorage"
            if attach_id is None and request is not None:
                attach_id = getattr(request, "attach_id", None)
                if attach_id is None:
                    bind_call = getattr(request, "bind_call", None)
                    if bind_call is not None:
                        attach_id = getattr(bind_call, "attach_id", None)
                origin = f"TransactionBoundStorage({type(request).__name__})"
            shard_key = _derive_shard_key(attach_id=attach_id, auth=auth, _origin=origin)
        self._shard_key = shard_key

    def get(self, keys: list[bytes]) -> list[bytes | None]:
        """Load values for a list of keys; parallel return list."""
        return self._base.transaction_state_get(
            self._transaction_id, keys, shard_key=self._shard_key,
        )

    def get_one(self, key: bytes) -> bytes | None:
        """Load a single value, or None if missing."""
        return self.get([key])[0]

    def put(self, items: list[tuple[bytes, bytes]]) -> None:
        """Write a batch of (key, value) pairs."""
        self._base.transaction_state_put(
            self._transaction_id, items, shard_key=self._shard_key,
        )

    def put_one(self, key: bytes, value: bytes) -> None:
        """Write a single (key, value) pair."""
        self.put([(key, value)])

    def clear(self) -> None:
        """Drop every value for this transaction."""
        self._base.transaction_state_clear(
            self._transaction_id, shard_key=self._shard_key,
        )


class BoundStorage:
    def __init__(
        self,
        storage: FunctionStorage,
        execution_id: bytes,
        *,
        request: Any = None,
        attach_id: bytes | None = None,
        auth: Any = None,
    ):
        self._base = storage
        self._execution_id = execution_id
        # ``request=`` is a convenience for the worker sites that already
        # have a BindRequest / InitRequest / AggregateBindRequest in scope:
        # we pull attach_id off either ``request.attach_id`` (Bind variants)
        # or ``request.bind_call.attach_id`` (InitRequest). Callers may
        # alternatively pass ``attach_id=`` directly; anonymous callers
        # fall through to "loc-anon".
        origin = "BoundStorage"
        if attach_id is None and request is not None:
            attach_id = getattr(request, "attach_id", None)
            if attach_id is None:
                bind_call = getattr(request, "bind_call", None)
                if bind_call is not None:
                    attach_id = getattr(bind_call, "attach_id", None)
            origin = f"BoundStorage({type(request).__name__})"
        self._shard_key = _derive_shard_key(attach_id=attach_id, auth=auth, _origin=origin)

    def transaction(self, transaction_id: bytes) -> TransactionBoundStorage:
        """Return a transaction-scoped storage view.

        Used for state that the user expects to be stable across
        multiple statements in one SQL transaction (e.g. Kafka topic
        watermarks, for snapshot-isolation reads).
        """
        # Inherit our shard_key directly — both views are part of the
        # same logical attach.
        return TransactionBoundStorage(
            self._base, transaction_id, shard_key=self._shard_key,
        )

    def put(self, state: bytes) -> None:
        """Store state for a specific worker."""
        self._base.worker_put(
            self._execution_id, os.getpid(), state, shard_key=self._shard_key,
        )

    def collect(self) -> list[bytes]:
        """Atomically collect and delete all worker states."""
        return self._base.worker_collect(
            self._execution_id, shard_key=self._shard_key,
        )

    def worker_scan(self) -> list[tuple[int, bytes]]:
        """Non-destructive read of (worker_id, state) pairs for this execution."""
        return self._base.worker_scan(
            self._execution_id, shard_key=self._shard_key,
        )

    # --- Scan Worker State (per-stream-id) ---

    def stream_state_put(self, state: bytes) -> None:
        """Store state for the current scan worker.

        Keyed by the framework's per-stream UUID (``_current_stream_id``
        ContextVar) so each scan worker has its own row regardless of
        pid/thread/machine — distinct from ``put()`` which conflates
        multiple threads in one Python process. Falls back to a
        pid-derived key when no stream is active (stdio transport, or
        any code path called outside an HTTP scan request).
        """
        sid = _scan_worker_stream_id()
        self._base.stream_state_put(
            self._execution_id, sid, state, shard_key=self._shard_key,
        )

    def stream_state_scan(self) -> list[tuple[bytes, bytes]]:
        """Non-destructive read of (stream_id, state) pairs for this execution."""
        return self._base.stream_state_scan(
            self._execution_id, shard_key=self._shard_key,
        )

    def queue_push(self, items: list[bytes]) -> int:
        """Add work items to the queue and register the invocation."""
        return self._base.queue_push(
            self._execution_id, items, shard_key=self._shard_key,
        )

    def queue_push_batches(self, batches: list[pa.RecordBatch]) -> int:
        """Serialize and push RecordBatches as work items."""
        return self.queue_push([self.serialize_record_batch(b) for b in batches])

    def queue_pop(self) -> bytes | None:
        """Atomically claim one work item from the queue."""
        return self._base.queue_pop(
            self._execution_id, shard_key=self._shard_key,
        )

    def queue_pop_batch(self) -> pa.RecordBatch | None:
        """Pop and deserialize one work item as a RecordBatch."""
        data = self.queue_pop()
        if data is None:
            return None
        return self.deserialize_record_batch(data)

    def queue_clear(self) -> int:
        """Clear all remaining work items and unregister the invocation."""
        return self._base.queue_clear(
            self._execution_id, shard_key=self._shard_key,
        )

    # --- Aggregate State ---

    def aggregate_get(self, group_ids: list[int]) -> list[tuple[int, bytes] | None]:
        """Load aggregate states for specific group_ids."""
        return self._base.aggregate_state_get(
            self._execution_id, group_ids, shard_key=self._shard_key,
        )

    def aggregate_put(self, data: list[tuple[int, bytes]]) -> None:
        """Unconditionally write aggregate states."""
        self._base.aggregate_state_put(
            self._execution_id, data, shard_key=self._shard_key,
        )

    def aggregate_clear(self) -> None:
        """Remove all aggregate states for this execution."""
        self._base.aggregate_state_clear(
            self._execution_id, shard_key=self._shard_key,
        )

    # --- Aggregate Window Partition ---

    def aggregate_window_partition_put(self, partition_id: int, data: bytes) -> None:
        """Write a cached partition payload for a windowed aggregate."""
        self._base.aggregate_window_partition_put(
            self._execution_id, partition_id, data, shard_key=self._shard_key,
        )

    def aggregate_window_partition_get(self, partition_id: int) -> bytes | None:
        """Load the cached partition payload."""
        return self._base.aggregate_window_partition_get(
            self._execution_id, partition_id, shard_key=self._shard_key,
        )

    def aggregate_window_partition_delete(self, partition_id: int) -> None:
        """Delete one cached partition."""
        self._base.aggregate_window_partition_delete(
            self._execution_id, partition_id, shard_key=self._shard_key,
        )

    def aggregate_window_partition_clear(self) -> None:
        """Remove all cached partitions for this execution."""
        self._base.aggregate_window_partition_clear(
            self._execution_id, shard_key=self._shard_key,
        )

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
            # Worker state table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS worker_state (
                    execution_id BLOB NOT NULL,
                    process_id INTEGER NOT NULL,
                    state_data BLOB NOT NULL,
                    created_at REAL DEFAULT (julianday('now')),
                    PRIMARY KEY (execution_id, process_id)
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
            # Aggregate state - per-group-id storage for aggregate functions
            conn.execute("""
                CREATE TABLE IF NOT EXISTS aggregate_state (
                    execution_id BLOB NOT NULL,
                    group_id INTEGER NOT NULL,
                    state_data BLOB NOT NULL,
                    created_at REAL DEFAULT (julianday('now')),
                    PRIMARY KEY (execution_id, group_id)
                )
            """)
            # Transaction state - per-(transaction_id, key) storage for state
            # the user expects to be stable across multiple statements
            # within one SQL transaction (e.g. Kafka topic watermarks for
            # snapshot isolation).
            conn.execute("""
                CREATE TABLE IF NOT EXISTS transaction_state (
                    transaction_id BLOB NOT NULL,
                    key BLOB NOT NULL,
                    value BLOB NOT NULL,
                    created_at REAL DEFAULT (julianday('now')),
                    PRIMARY KEY (transaction_id, key)
                )
            """)
            # Scan worker state - per-(execution_id, stream_id) storage for
            # diagnostic blobs that must be scoped to the exact scan worker
            # that produced them. Distinct from worker_state above (which is
            # keyed by os.getpid() and conflates threads of one process).
            conn.execute("""
                CREATE TABLE IF NOT EXISTS scan_worker_state (
                    execution_id BLOB NOT NULL,
                    stream_id BLOB NOT NULL,
                    state_data BLOB NOT NULL,
                    created_at REAL DEFAULT (julianday('now')),
                    PRIMARY KEY (execution_id, stream_id)
                )
            """)
            # Aggregate window partitions - per-partition cached input for windowed aggregates
            conn.execute("""
                CREATE TABLE IF NOT EXISTS aggregate_window_partitions (
                    execution_id BLOB NOT NULL,
                    partition_id INTEGER NOT NULL,
                    payload BLOB NOT NULL,
                    created_at REAL DEFAULT (julianday('now')),
                    PRIMARY KEY (execution_id, partition_id)
                )
            """)
            conn.commit()
        finally:
            conn.close()

    # --- Worker State ---

    def worker_put(self, execution_id: bytes, worker_id: int, state: bytes, *, shard_key: str = "") -> None:
        """Store state for a specific worker. ``shard_key`` is ignored (no sharding in SQLite)."""
        del shard_key
        # Opportunistically clean old entries (1% of calls)
        if random.random() < 0.01:
            self.cleanup_old_entries(max_age_days=1.0)

        conn = self._conn()
        conn.execute(
            """
            INSERT OR REPLACE INTO worker_state
            (execution_id, process_id, state_data, created_at)
            VALUES (?, ?, ?, julianday('now'))
            """,
            (execution_id, worker_id, state),
        )
        conn.commit()

    def worker_collect(self, execution_id: bytes, *, shard_key: str = "") -> list[bytes]:
        """Atomically collect and delete all worker states. ``shard_key`` is ignored (no sharding in SQLite)."""
        del shard_key
        conn = self._conn()
        cursor = conn.execute(
            """
            DELETE FROM worker_state
            WHERE execution_id = ?
            RETURNING state_data
            """,
            (execution_id,),
        )
        states = [row[0] for row in cursor.fetchall()]
        conn.commit()
        return states

    def worker_scan(self, execution_id: bytes, *, shard_key: str = "") -> list[tuple[int, bytes]]:
        """Non-destructive read of (process_id, state_data) for execution_id."""
        conn = self._conn()
        cursor = conn.execute(
            """
            SELECT process_id, state_data
            FROM worker_state
            WHERE execution_id = ?
            """,
            (execution_id,),
        )
        return [(int(row[0]), row[1]) for row in cursor.fetchall()]

    # --- Scan Worker State ---

    def stream_state_put(self, execution_id: bytes, stream_id: bytes, state: bytes, *, shard_key: str = "") -> None:
        """Store per-(execution_id, stream_id) state."""
        conn = self._conn()
        conn.execute(
            """
            INSERT OR REPLACE INTO scan_worker_state
            (execution_id, stream_id, state_data, created_at)
            VALUES (?, ?, ?, julianday('now'))
            """,
            (execution_id, stream_id, state),
        )
        conn.commit()

    def stream_state_scan(self, execution_id: bytes, *, shard_key: str = "") -> list[tuple[bytes, bytes]]:
        """Non-destructive read of (stream_id, state_data) for execution_id."""
        conn = self._conn()
        cursor = conn.execute(
            """
            SELECT stream_id, state_data
            FROM scan_worker_state
            WHERE execution_id = ?
            """,
            (execution_id,),
        )
        return [(row[0], row[1]) for row in cursor.fetchall()]

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

    # --- Aggregate State ---

    def aggregate_state_get(self, execution_id: bytes, group_ids: list[int], *, shard_key: str = "") -> list[tuple[int, bytes] | None]:
        """Load aggregate states for specific group_ids.

        Batches queries in chunks of 500 to stay under SQLite's default
        999-parameter limit.
        """
        if not group_ids:
            return []
        conn = self._conn()
        found: dict[int, bytes] = {}
        chunk_size = 500
        for i in range(0, len(group_ids), chunk_size):
            chunk = group_ids[i : i + chunk_size]
            placeholders = ",".join("?" * len(chunk))
            cursor = conn.execute(
                f"SELECT group_id, state_data FROM aggregate_state "  # noqa: S608
                f"WHERE execution_id = ? AND group_id IN ({placeholders})",
                (execution_id, *chunk),
            )
            for row in cursor.fetchall():
                found[row[0]] = row[1]
        return [(gid, found[gid]) if gid in found else None for gid in group_ids]

    def aggregate_state_put(self, execution_id: bytes, data: list[tuple[int, bytes]], *, shard_key: str = "") -> None:
        """Unconditionally write aggregate states."""
        if not data:
            return
        # Opportunistically clean old entries (1% of calls)
        if random.random() < 0.01:
            self.cleanup_old_entries(max_age_days=1.0)

        conn = self._conn()
        conn.executemany(
            """
            INSERT OR REPLACE INTO aggregate_state
            (execution_id, group_id, state_data, created_at)
            VALUES (?, ?, ?, julianday('now'))
            """,
            [(execution_id, gid, state_bytes) for gid, state_bytes in data],
        )
        conn.commit()

    def aggregate_state_clear(self, execution_id: bytes, *, shard_key: str = "") -> None:
        """Remove all aggregate states for an execution_id."""
        conn = self._conn()
        conn.execute(
            "DELETE FROM aggregate_state WHERE execution_id = ?",
            (execution_id,),
        )
        conn.commit()

    # --- Transaction State ---

    def transaction_state_get(self, transaction_id: bytes, keys: list[bytes], *, shard_key: str = "") -> list[bytes | None]:
        """Load transaction-scoped values for the given keys."""
        if not keys:
            return []
        conn = self._conn()
        found: dict[bytes, bytes] = {}
        chunk_size = 500  # stay under SQLite's 999-parameter limit
        for i in range(0, len(keys), chunk_size):
            chunk = keys[i : i + chunk_size]
            placeholders = ",".join("?" * len(chunk))
            cursor = conn.execute(
                f"SELECT key, value FROM transaction_state "  # noqa: S608
                f"WHERE transaction_id = ? AND key IN ({placeholders})",
                (transaction_id, *chunk),
            )
            for row in cursor.fetchall():
                found[row[0]] = row[1]
        return [found.get(k) for k in keys]

    def transaction_state_put(self, transaction_id: bytes, items: list[tuple[bytes, bytes]], *, shard_key: str = "") -> None:
        """Write transaction-scoped values."""
        if not items:
            return
        # Opportunistically clean old entries (1% of calls). Transaction
        # state is short-lived and dropped explicitly on commit/rollback,
        # but a TTL sweep covers leaks from missed callbacks.
        if random.random() < 0.01:
            self.cleanup_old_entries(max_age_days=1.0)

        conn = self._conn()
        conn.executemany(
            """
            INSERT OR REPLACE INTO transaction_state
            (transaction_id, key, value, created_at)
            VALUES (?, ?, ?, julianday('now'))
            """,
            [(transaction_id, k, v) for k, v in items],
        )
        conn.commit()

    def transaction_state_clear(self, transaction_id: bytes, *, shard_key: str = "") -> None:
        """Drop all state for a transaction."""
        conn = self._conn()
        conn.execute(
            "DELETE FROM transaction_state WHERE transaction_id = ?",
            (transaction_id,),
        )
        conn.commit()

    # --- Aggregate Window Partition ---

    def aggregate_window_partition_put(self, execution_id: bytes, partition_id: int, data: bytes, *, shard_key: str = "") -> None:
        """Store a cached windowed-aggregate partition payload."""
        if random.random() < 0.01:
            self.cleanup_old_entries(max_age_days=1.0)
        conn = self._conn()
        conn.execute(
            """
            INSERT OR REPLACE INTO aggregate_window_partitions
            (execution_id, partition_id, payload, created_at)
            VALUES (?, ?, ?, julianday('now'))
            """,
            (execution_id, partition_id, data),
        )
        conn.commit()

    def aggregate_window_partition_get(self, execution_id: bytes, partition_id: int, *, shard_key: str = "") -> bytes | None:
        """Load a cached windowed-aggregate partition payload."""
        conn = self._conn()
        cursor = conn.execute(
            "SELECT payload FROM aggregate_window_partitions WHERE execution_id = ? AND partition_id = ?",
            (execution_id, partition_id),
        )
        row = cursor.fetchone()
        return row[0] if row else None

    def aggregate_window_partition_delete(self, execution_id: bytes, partition_id: int, *, shard_key: str = "") -> None:
        """Delete a single cached windowed-aggregate partition."""
        conn = self._conn()
        conn.execute(
            "DELETE FROM aggregate_window_partitions WHERE execution_id = ? AND partition_id = ?",
            (execution_id, partition_id),
        )
        conn.commit()

    def aggregate_window_partition_clear(self, execution_id: bytes, *, shard_key: str = "") -> None:
        """Remove all cached windowed-aggregate partitions for an execution_id."""
        conn = self._conn()
        conn.execute(
            "DELETE FROM aggregate_window_partitions WHERE execution_id = ?",
            (execution_id,),
        )
        conn.commit()

    # --- Maintenance (not part of protocol) ---

    def cleanup_old_entries(self, max_age_days: float = 1.0) -> int:
        """Remove entries older than the specified age from all tables.

        Args:
            max_age_days: Maximum age in days for entries to keep.

        Returns:
            Total number of entries deleted.

        """
        conn = self._conn()
        cursor1 = conn.execute(
            """
            DELETE FROM global_state_storage
            WHERE julianday('now') - created_at > ?
            """,
            (max_age_days,),
        )
        cursor2 = conn.execute(
            """
            DELETE FROM worker_state
            WHERE julianday('now') - created_at > ?
            """,
            (max_age_days,),
        )
        cursor3 = conn.execute(
            """
            DELETE FROM work_queue
            WHERE julianday('now') - created_at > ?
            """,
            (max_age_days,),
        )
        cursor4 = conn.execute(
            """
            DELETE FROM invocation_registry
            WHERE julianday('now') - created_at > ?
            """,
            (max_age_days,),
        )
        cursor5 = conn.execute(
            """
            DELETE FROM aggregate_state
            WHERE julianday('now') - created_at > ?
            """,
            (max_age_days,),
        )
        cursor6 = conn.execute(
            """
            DELETE FROM aggregate_window_partitions
            WHERE julianday('now') - created_at > ?
            """,
            (max_age_days,),
        )
        cursor7 = conn.execute(
            """
            DELETE FROM transaction_state
            WHERE julianday('now') - created_at > ?
            """,
            (max_age_days,),
        )
        cursor8 = conn.execute(
            """
            DELETE FROM scan_worker_state
            WHERE julianday('now') - created_at > ?
            """,
            (max_age_days,),
        )
        conn.commit()
        return (
            int(cursor1.rowcount)
            + int(cursor2.rowcount)
            + int(cursor3.rowcount)
            + int(cursor4.rowcount)
            + int(cursor5.rowcount)
            + int(cursor6.rowcount)
            + int(cursor7.rowcount)
            + int(cursor8.rowcount)
        )
