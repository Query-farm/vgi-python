# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Result-cache fixtures — table generators that advertise ``vgi.cache.*``.

These exist so SQL integration tests (and the C++ result-cache) can exercise
cacheable table-function results end to end. Each generator returns a small
deterministic result and folds cache-control metadata onto its **first**
emitted batch via ``out.emit(batch, cache_control=CacheControl(...))``:

* ``cacheable_numbers(n, ttl := 300)`` — ``n`` rows ``[0..n)``; advertises a
  ``ttl`` freshness lifetime. The baseline cacheable generator.
* ``cache_nonce()`` — ONE row whose value changes on every *real* invocation
  (a process-global counter). A cache HIT is provable by the value NOT
  changing across calls; a MISS by it changing. Row count is fixed (1) and
  never depends on wall clock.
* ``cache_no_store(n)`` — emits ``n`` rows but advertises
  ``vgi.cache.no_store`` so the client must never cache it.
* ``cache_scoped_txn(n)`` — advertises ``scope = transaction`` (reused only
  within the same transaction).
* ``cache_big(rows)`` — ``rows`` rows across MANY small batches (batch size
  1000) so multi-batch capture / parallel serve + the size ceiling are
  exercised; advertises a ``ttl``.
"""

from __future__ import annotations

import itertools
import struct
from dataclasses import dataclass
from typing import Annotated, ClassVar, cast

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi.arguments import Arg
from vgi.cache_control import CACHE_SCOPE_TRANSACTION, CacheControl
from vgi.invocation import GlobalInitResponse
from vgi.metadata import FunctionExample, OrderPreservation
from vgi.protocol import VgiOutputCollector
from vgi.schema_utils import schema
from vgi.table_function import (
    InitParams,
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)

# Default freshness lifetime (seconds) for the fixtures that don't take a
# ``ttl`` argument. Long enough that TTL never lapses mid-test.
_DEFAULT_TTL_SECONDS = 300

# Process-global monotonic counter. Incremented once per *real* invocation of
# ``cache_nonce`` (in ``initial_state``, which the client only reaches on a
# cache MISS). A pooled worker persists it across calls, so a served-from-cache
# hit never advances it — that's exactly the HIT/MISS signal tests assert on.
_NONCE_COUNTER = itertools.count()


# ---------------------------------------------------------------------------
# cacheable_numbers
# ---------------------------------------------------------------------------
@dataclass(slots=True, frozen=True)
class CacheableNumbersArgs:
    """Arguments for CacheableNumbersFunction."""

    n: Annotated[int, Arg("n", default=10, doc="Number of rows to generate", ge=0)]
    ttl: Annotated[int, Arg("ttl", default=_DEFAULT_TTL_SECONDS, doc="Cache TTL in seconds", ge=0)]


@dataclass(kw_only=True)
class _CacheCountdownState(ArrowSerializableDataclass):
    """Mutable state tracking remaining rows and current position."""

    remaining: int
    current_index: int = 0


@init_single_worker
@bind_fixed_schema
class CacheableNumbersFunction(TableFunctionGenerator[CacheableNumbersArgs, _CacheCountdownState]):
    """Emits ``n`` rows ``[0..n)`` and advertises a cache ``ttl``.

    The baseline cacheable result: a fresh call MISSes and stores; an
    identical repeat within ``ttl`` seconds serves from the client cache.
    """

    class Meta:
        """Metadata for CacheableNumbersFunction."""

        name = "cacheable_numbers"
        description = "Emits n rows [0..n) and advertises a cache TTL"
        categories = ["generator", "cache"]
        tags = {"category": "cache", "type": "generator"}
        examples = [
            FunctionExample(
                sql="SELECT * FROM cacheable_numbers(10)",
                description="Cacheable sequence 0-9 with the default TTL",
            ),
            FunctionExample(
                sql="SELECT * FROM cacheable_numbers(10, ttl := 60)",
                description="Cacheable sequence 0-9 with a 60s TTL",
            ),
        ]

    FunctionArguments = CacheableNumbersArgs
    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(n=pa.int64())
    BATCH_SIZE: ClassVar[int] = 1000

    @classmethod
    def initial_state(cls, params: ProcessParams[CacheableNumbersArgs]) -> _CacheCountdownState:
        """Create initial state with the requested row count."""
        return _CacheCountdownState(remaining=params.args.n)

    @classmethod
    def process(
        cls,
        params: ProcessParams[CacheableNumbersArgs],
        state: _CacheCountdownState,
        out: OutputCollector,
    ) -> None:
        """Emit the next batch of the sequence, advertising cache control first."""
        if state.remaining <= 0:
            out.finish()
            return

        first_batch = state.current_index == 0
        size = min(state.remaining, cls.BATCH_SIZE)
        values = list(range(state.current_index, state.current_index + size))
        batch = pa.RecordBatch.from_pydict({"n": values}, schema=params.output_schema)

        cache_control = CacheControl(ttl=params.args.ttl) if first_batch else None
        cast(VgiOutputCollector, out).emit(batch, cache_control=cache_control)

        state.current_index += size
        state.remaining -= size


# ---------------------------------------------------------------------------
# cache_nonce
# ---------------------------------------------------------------------------
@dataclass(slots=True, frozen=True)
class CacheNonceArgs:
    """Arguments for CacheNonceFunction (none)."""


@dataclass(kw_only=True)
class _CacheNonceState(ArrowSerializableDataclass):
    """Mutable state carrying the per-invocation nonce."""

    nonce: int
    done: bool = False


@init_single_worker
@bind_fixed_schema
class CacheNonceFunction(TableFunctionGenerator[CacheNonceArgs, _CacheNonceState]):
    """Emits ONE row whose ``nonce`` changes on every real invocation.

    ``initial_state`` (reached only on a cache MISS) advances a process-global
    counter, so the emitted value is stable across cache HITs and changes
    across MISSes — a value-level proof of cache behaviour independent of the
    log. Row count is always 1 and never depends on wall clock.
    """

    class Meta:
        """Metadata for CacheNonceFunction."""

        name = "cache_nonce"
        description = "Emits one row with a per-invocation nonce; cacheable"
        categories = ["generator", "cache", "testing"]
        tags = {"category": "cache", "type": "nonce"}
        examples = [
            FunctionExample(
                sql="SELECT * FROM cache_nonce()",
                description="One-row cacheable result; nonce is stable on a cache hit",
            ),
        ]

    FunctionArguments = CacheNonceArgs
    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(nonce=pa.int64())

    @classmethod
    def initial_state(cls, params: ProcessParams[CacheNonceArgs]) -> _CacheNonceState:
        """Mint a fresh nonce for this (real) invocation."""
        return _CacheNonceState(nonce=next(_NONCE_COUNTER))

    @classmethod
    def process(
        cls,
        params: ProcessParams[CacheNonceArgs],
        state: _CacheNonceState,
        out: OutputCollector,
    ) -> None:
        """Emit the single nonce row once, advertising a cache TTL."""
        if state.done:
            out.finish()
            return

        batch = pa.RecordBatch.from_pydict({"nonce": [state.nonce]}, schema=params.output_schema)
        cast(VgiOutputCollector, out).emit(batch, cache_control=CacheControl(ttl=_DEFAULT_TTL_SECONDS))
        state.done = True


# ---------------------------------------------------------------------------
# cache_no_store
# ---------------------------------------------------------------------------
@dataclass(slots=True, frozen=True)
class CacheNoStoreArgs:
    """Arguments for CacheNoStoreFunction."""

    n: Annotated[int, Arg("n", default=10, doc="Number of rows to generate", ge=0)]


@init_single_worker
@bind_fixed_schema
class CacheNoStoreFunction(TableFunctionGenerator[CacheNoStoreArgs, _CacheCountdownState]):
    """Emits ``n`` rows but advertises ``vgi.cache.no_store`` (never cached).

    The client must always re-invoke the worker even though it advertises
    cache metadata — ``no_store`` overrides any freshness key.
    """

    class Meta:
        """Metadata for CacheNoStoreFunction."""

        name = "cache_no_store"
        description = "Emits n rows but advertises no_store (never cached)"
        categories = ["generator", "cache", "testing"]
        tags = {"category": "cache", "type": "no_store"}
        examples = [
            FunctionExample(
                sql="SELECT * FROM cache_no_store(5)",
                description="Emit 5 rows that must never be cached",
            ),
        ]

    FunctionArguments = CacheNoStoreArgs
    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(n=pa.int64())
    BATCH_SIZE: ClassVar[int] = 1000

    @classmethod
    def initial_state(cls, params: ProcessParams[CacheNoStoreArgs]) -> _CacheCountdownState:
        """Create initial state with the requested row count."""
        return _CacheCountdownState(remaining=params.args.n)

    @classmethod
    def process(
        cls,
        params: ProcessParams[CacheNoStoreArgs],
        state: _CacheCountdownState,
        out: OutputCollector,
    ) -> None:
        """Emit the next batch, advertising no_store on the first batch."""
        if state.remaining <= 0:
            out.finish()
            return

        first_batch = state.current_index == 0
        size = min(state.remaining, cls.BATCH_SIZE)
        values = list(range(state.current_index, state.current_index + size))
        batch = pa.RecordBatch.from_pydict({"n": values}, schema=params.output_schema)

        cache_control = CacheControl(no_store=True) if first_batch else None
        cast(VgiOutputCollector, out).emit(batch, cache_control=cache_control)

        state.current_index += size
        state.remaining -= size


# ---------------------------------------------------------------------------
# cache_scoped_txn
# ---------------------------------------------------------------------------
@dataclass(slots=True, frozen=True)
class CacheScopedTxnArgs:
    """Arguments for CacheScopedTxnFunction."""

    n: Annotated[int, Arg("n", default=10, doc="Number of rows to generate", ge=0)]


@dataclass(kw_only=True)
class _CacheScopedTxnState(ArrowSerializableDataclass):
    """Countdown state + a per-invocation nonce (proves same-txn hit vs new-txn miss)."""

    remaining: int
    current_index: int = 0
    nonce: int = 0


@init_single_worker
@bind_fixed_schema
class CacheScopedTxnFunction(TableFunctionGenerator[CacheScopedTxnArgs, _CacheScopedTxnState]):
    """Emits ``(n, nonce)`` rows and advertises ``scope = transaction``.

    The result is only reusable within the same transaction (the client folds
    the transaction id into the cache key); a fresh transaction MISSes. ``nonce``
    is a process-global counter bumped once per REAL invocation (initial_state,
    reached only on a MISS), so a same-transaction HIT returns the SAME nonce
    while a new-transaction MISS returns a fresh one — the hit/miss is provable
    from the value, not just logs.
    """

    class Meta:
        """Metadata for CacheScopedTxnFunction."""

        name = "cache_scoped_txn"
        description = "Emits n rows and advertises scope=transaction"
        categories = ["generator", "cache", "testing"]
        tags = {"category": "cache", "type": "scope"}
        examples = [
            FunctionExample(
                sql="SELECT * FROM cache_scoped_txn(5)",
                description="Transaction-scoped cacheable result",
            ),
        ]

    FunctionArguments = CacheScopedTxnArgs
    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(n=pa.int64(), nonce=pa.int64())
    BATCH_SIZE: ClassVar[int] = 1000

    @classmethod
    def initial_state(cls, params: ProcessParams[CacheScopedTxnArgs]) -> _CacheScopedTxnState:
        """Create initial state; bump the nonce once per REAL invocation (MISS)."""
        return _CacheScopedTxnState(remaining=params.args.n, nonce=next(_NONCE_COUNTER))

    @classmethod
    def process(
        cls,
        params: ProcessParams[CacheScopedTxnArgs],
        state: _CacheScopedTxnState,
        out: OutputCollector,
    ) -> None:
        """Emit the next batch, advertising transaction scope on the first batch."""
        if state.remaining <= 0:
            out.finish()
            return

        first_batch = state.current_index == 0
        size = min(state.remaining, cls.BATCH_SIZE)
        values = list(range(state.current_index, state.current_index + size))
        batch = pa.RecordBatch.from_pydict(
            {"n": values, "nonce": [state.nonce] * size}, schema=params.output_schema
        )

        cache_control = (
            CacheControl(ttl=_DEFAULT_TTL_SECONDS, scope=CACHE_SCOPE_TRANSACTION) if first_batch else None
        )
        cast(VgiOutputCollector, out).emit(batch, cache_control=cache_control)

        state.current_index += size
        state.remaining -= size


# ---------------------------------------------------------------------------
# cache_big
# ---------------------------------------------------------------------------
@dataclass(slots=True, frozen=True)
class CacheBigArgs:
    """Arguments for CacheBigFunction."""

    rows: Annotated[int, Arg("rows", default=5000, doc="Number of rows to generate", ge=0)]


@init_single_worker
@bind_fixed_schema
class CacheBigFunction(TableFunctionGenerator[CacheBigArgs, _CacheCountdownState]):
    """Emits ``rows`` rows across MANY small batches; advertises a ``ttl``.

    The small batch size (1000) forces multi-batch capture and multi-batch
    replay on serve, exercising parallel capture / serve and the size ceiling.
    """

    class Meta:
        """Metadata for CacheBigFunction."""

        name = "cache_big"
        description = "Emits many small batches totaling `rows` rows; cacheable"
        categories = ["generator", "cache", "testing"]
        tags = {"category": "cache", "type": "multi_batch"}
        examples = [
            FunctionExample(
                sql="SELECT count(*) FROM cache_big(50000)",
                description="Large multi-batch cacheable result",
            ),
        ]

    FunctionArguments = CacheBigArgs
    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(n=pa.int64())
    BATCH_SIZE: ClassVar[int] = 1000

    @classmethod
    def initial_state(cls, params: ProcessParams[CacheBigArgs]) -> _CacheCountdownState:
        """Create initial state with the requested row count."""
        return _CacheCountdownState(remaining=params.args.rows)

    @classmethod
    def process(
        cls,
        params: ProcessParams[CacheBigArgs],
        state: _CacheCountdownState,
        out: OutputCollector,
    ) -> None:
        """Emit one small batch per tick, advertising a TTL on the first batch."""
        if state.remaining <= 0:
            out.finish()
            return

        first_batch = state.current_index == 0
        size = min(state.remaining, cls.BATCH_SIZE)
        values = list(range(state.current_index, state.current_index + size))
        batch = pa.RecordBatch.from_pydict({"n": values}, schema=params.output_schema)

        cache_control = CacheControl(ttl=_DEFAULT_TTL_SECONDS) if first_batch else None
        cast(VgiOutputCollector, out).emit(batch, cache_control=cache_control)

        state.current_index += size
        state.remaining -= size


# ---------------------------------------------------------------------------
# cache_revalidatable — conditional revalidation (304 / not_modified)
# ---------------------------------------------------------------------------
@dataclass(slots=True, frozen=True)
class CacheRevalidatableArgs:
    """Arguments for CacheRevalidatableFunction (none)."""


@init_single_worker
@bind_fixed_schema
class CacheRevalidatableFunction(TableFunctionGenerator[CacheRevalidatableArgs, _CacheNonceState]):
    """Emits ONE nonce row and advertises a validated, always-revalidate result.

    It advertises ``ttl=0`` + ``etag`` + ``revalidatable`` — the "no-cache"
    semantic: the client stores the payload but marks it immediately stale, so
    every repeat sends a conditional request (``vgi.cache.if_none_match``) on the
    first tick. Because this fixture's data never changes, ``process`` sees the
    matching ``if_none_match`` and answers with a 0-row ``not_modified`` batch
    instead of re-emitting — so the client reuses the STORED nonce. A stable
    nonce across repeats therefore proves the not_modified path served cached
    bytes without re-streaming; the worker was contacted but did not recompute.
    """

    ETAG: ClassVar[str] = '"rev-v1"'

    class Meta:
        """Metadata for CacheRevalidatableFunction."""

        name = "cache_revalidatable"
        description = "Emits one nonce row; always-revalidate (304 not_modified)"
        categories = ["generator", "cache", "testing"]
        tags = {"category": "cache", "type": "revalidatable"}
        examples = [
            FunctionExample(
                sql="SELECT * FROM cache_revalidatable()",
                description="Conditionally-revalidated result (304 reuses stored bytes)",
            ),
        ]

    FunctionArguments = CacheRevalidatableArgs
    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(nonce=pa.int64())

    @classmethod
    def initial_state(cls, params: ProcessParams[CacheRevalidatableArgs]) -> _CacheNonceState:
        """Mint a fresh nonce for this (real) invocation."""
        return _CacheNonceState(nonce=next(_NONCE_COUNTER))

    @classmethod
    def process(
        cls,
        params: ProcessParams[CacheRevalidatableArgs],
        state: _CacheNonceState,
        out: OutputCollector,
    ) -> None:
        """Answer a conditional request with 304, else emit the nonce row."""
        if state.done:
            out.finish()
            return

        emit = cast(VgiOutputCollector, out).emit
        if params.if_none_match == cls.ETAG:
            # 304 Not Modified: the client's stored copy is still valid. Emit a
            # 0-row not_modified batch (fresh validators + ttl=0 so it keeps
            # revalidating) — the client reuses its stored payload.
            empty = pa.RecordBatch.from_pydict({"nonce": []}, schema=params.output_schema)
            emit(
                empty,
                cache_control=CacheControl(
                    not_modified=True, ttl=0, etag=cls.ETAG, revalidatable=True
                ),
            )
            state.done = True
            return

        # Fresh result: emit the nonce + advertise the always-revalidate contract.
        batch = pa.RecordBatch.from_pydict({"nonce": [state.nonce]}, schema=params.output_schema)
        emit(batch, cache_control=CacheControl(ttl=0, etag=cls.ETAG, revalidatable=True))
        state.done = True


# ---------------------------------------------------------------------------
# cache_multicol — multi-column cacheable result (projection-coverage reuse)
# ---------------------------------------------------------------------------
@dataclass(slots=True, frozen=True)
class CacheMultiColArgs:
    """Arguments for CacheMultiColFunction."""

    n: Annotated[int, Arg("n", default=4, doc="Number of rows to generate", ge=0)]
    ttl: Annotated[int, Arg("ttl", default=_DEFAULT_TTL_SECONDS, doc="Cache TTL in seconds", ge=0)]


@init_single_worker
@bind_fixed_schema
class CacheMultiColFunction(TableFunctionGenerator[CacheMultiColArgs, _CacheCountdownState]):
    """Emits ``n`` rows of three columns ``(a, b, c) = (i, i*10, i*100)``.

    A multi-column cacheable result: ``SELECT b`` reuses the ``SELECT *`` cache
    entry (the generator doesn't push projection, so both scans share the same
    key and DuckDB projects locally — projection-coverage reuse).
    """

    class Meta:
        """Metadata for CacheMultiColFunction."""

        name = "cache_multicol"
        description = "Emits n rows of (a, b, c); cacheable, multi-column"
        categories = ["generator", "cache", "testing"]
        tags = {"category": "cache", "type": "multicol"}
        examples = [
            FunctionExample(
                sql="SELECT b FROM cache_multicol()",
                description="Subset projection reuses the full-result cache entry",
            ),
        ]

    FunctionArguments = CacheMultiColArgs
    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(a=pa.int64(), b=pa.int64(), c=pa.int64())

    @classmethod
    def initial_state(cls, params: ProcessParams[CacheMultiColArgs]) -> _CacheCountdownState:
        """Create initial state with the requested row count."""
        return _CacheCountdownState(remaining=params.args.n)

    @classmethod
    def process(
        cls,
        params: ProcessParams[CacheMultiColArgs],
        state: _CacheCountdownState,
        out: OutputCollector,
    ) -> None:
        """Emit all rows in one batch, advertising a TTL."""
        if state.remaining <= 0:
            out.finish()
            return
        rows = list(range(state.remaining))
        batch = pa.RecordBatch.from_pydict(
            {"a": rows, "b": [i * 10 for i in rows], "c": [i * 100 for i in rows]},
            schema=params.output_schema,
        )
        cast(VgiOutputCollector, out).emit(batch, cache_control=CacheControl(ttl=_DEFAULT_TTL_SECONDS))
        state.remaining = 0


# ---------------------------------------------------------------------------
# cache_whoami — identity-echoing cacheable result (cache token isolation)
# ---------------------------------------------------------------------------
@dataclass(slots=True, frozen=True)
class CacheWhoamiArgs:
    """Arguments for CacheWhoamiFunction (none)."""


@init_single_worker
@bind_fixed_schema
class CacheWhoamiFunction(TableFunctionGenerator[CacheWhoamiArgs, _CacheNonceState]):
    """Emits ONE row = the caller's auth principal ("anonymous" if none); cacheable.

    The linchpin of the cache token-isolation test: two attaches of the same
    worker with different bearer tokens map to different principals, so their
    results MUST land under different (identity-scoped) cache keys and never
    cross-serve. Bearer/OAuth identity is HTTP-only; over subprocess every caller
    is "anonymous". Reuses ``_CacheNonceState`` (``nonce`` field is unused here).
    """

    class Meta:
        """Metadata for CacheWhoamiFunction."""

        name = "cache_whoami"
        description = "Emits the caller's auth principal; cacheable (identity-scoped)"
        categories = ["generator", "cache", "testing"]
        tags = {"category": "cache", "type": "identity"}
        examples = [
            FunctionExample(
                sql="SELECT who FROM cache_whoami()",
                description="One-row cacheable result echoing the caller's principal",
            ),
        ]

    FunctionArguments = CacheWhoamiArgs
    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(who=pa.string())

    @classmethod
    def initial_state(cls, params: ProcessParams[CacheWhoamiArgs]) -> _CacheNonceState:
        """Create state (nonce unused for this fixture)."""
        return _CacheNonceState(nonce=0)

    @classmethod
    def process(
        cls,
        params: ProcessParams[CacheWhoamiArgs],
        state: _CacheNonceState,
        out: OutputCollector,
    ) -> None:
        """Emit the caller's principal once, advertising a TTL."""
        if state.done:
            out.finish()
            return
        who = params.auth_context.principal or "anonymous"
        batch = pa.RecordBatch.from_pydict({"who": [who]}, schema=params.output_schema)
        cast(VgiOutputCollector, out).emit(batch, cache_control=CacheControl(ttl=_DEFAULT_TTL_SECONDS))
        state.done = True


# ---------------------------------------------------------------------------
# cache_versioned — time-travel cacheable result (AT cache isolation)
# ---------------------------------------------------------------------------
# Version → row data (fixed schema so table_get needs no per-version override;
# only the scan-function arg changes). The catalog maps AT → the version arg.
_CACHE_VERSIONED_DATA: dict[int, list[int]] = {
    1: [101, 102, 103],
    2: [201, 202],
    3: [301, 302, 303, 304],
}
_CACHE_VERSIONED_CURRENT = 3


@dataclass(slots=True, frozen=True)
class CacheVersionedArgs:
    """Arguments for CacheVersionedFunction."""

    version: Annotated[int, Arg(0, doc="Data version, resolved from the AT clause by the catalog")]


@init_single_worker
@bind_fixed_schema
class CacheVersionedFunction(TableFunctionGenerator[CacheVersionedArgs, _CacheNonceState]):
    """Version-specific rows (fixed schema); cacheable.

    For AT cache-isolation: ``AT (VERSION => 1)`` / ``AT (VERSION => 2)`` / live
    must produce distinct cache entries whose bytes never cross-serve — the cache
    key folds ``at_unit``/``at_value``. An AT-pinned scan is an immutable snapshot
    (the client marks it never-expires); live uses the TTL.
    """

    class Meta:
        """Metadata for CacheVersionedFunction."""

        name = "cache_versioned_scan"
        description = "Version-specific rows; cacheable (AT-keyed)"
        categories = ["generator", "cache", "testing"]
        tags = {"category": "cache", "type": "time_travel"}

    FunctionArguments = CacheVersionedArgs
    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(v=pa.int64())

    @classmethod
    def initial_state(cls, params: ProcessParams[CacheVersionedArgs]) -> _CacheNonceState:
        """Create state (nonce unused)."""
        return _CacheNonceState(nonce=0)

    @classmethod
    def process(
        cls,
        params: ProcessParams[CacheVersionedArgs],
        state: _CacheNonceState,
        out: OutputCollector,
    ) -> None:
        """Emit the requested version's rows, advertising a TTL."""
        if state.done:
            out.finish()
            return
        data = _CACHE_VERSIONED_DATA.get(params.args.version, _CACHE_VERSIONED_DATA[_CACHE_VERSIONED_CURRENT])
        batch = pa.RecordBatch.from_pydict({"v": data}, schema=params.output_schema)
        cast(VgiOutputCollector, out).emit(batch, cache_control=CacheControl(ttl=_DEFAULT_TTL_SECONDS))
        state.done = True


# ---------------------------------------------------------------------------
# cache_projection — projection-pushdown cacheable result (cross-serve check)
# ---------------------------------------------------------------------------
_CACHE_PROJ_DATA: dict[str, list[int]] = {
    "a": [1, 2, 3],
    "b": [10, 20, 30],
    "c": [100, 200, 300],
}


@dataclass(slots=True, frozen=True)
class CacheProjectionArgs:
    """Arguments for CacheProjectionFunction (none)."""


@init_single_worker
@bind_fixed_schema
class CacheProjectionFunction(TableFunctionGenerator[CacheProjectionArgs, _CacheNonceState]):
    """3-column generator that PUSHES projection; cacheable.

    For the projection-pushdown cross-serve check: because ``projection_pushdown``
    is on, ``SELECT a`` and ``SELECT b`` push distinct ``projection_ids`` that are
    part of the cache key — so each column's scan caches only its own bytes under a
    distinct key, and one column's result can never be served for another's.
    """

    class Meta:
        """Metadata for CacheProjectionFunction."""

        name = "cache_projection"
        description = "3-column projection-pushdown generator; cacheable"
        categories = ["generator", "cache", "testing"]
        tags = {"category": "cache", "type": "projection"}
        projection_pushdown = True

    FunctionArguments = CacheProjectionArgs
    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(a=pa.int64(), b=pa.int64(), c=pa.int64())

    @classmethod
    def initial_state(cls, params: ProcessParams[CacheProjectionArgs]) -> _CacheNonceState:
        """Create state (nonce unused)."""
        return _CacheNonceState(nonce=0)

    @classmethod
    def process(
        cls,
        params: ProcessParams[CacheProjectionArgs],
        state: _CacheNonceState,
        out: OutputCollector,
    ) -> None:
        """Emit only the projected columns (per output_schema), advertising a TTL."""
        if state.done:
            out.finish()
            return
        cols = {f.name: _CACHE_PROJ_DATA[f.name] for f in params.output_schema}
        batch = pa.RecordBatch.from_pydict(cols, schema=params.output_schema)
        cast(VgiOutputCollector, out).emit(batch, cache_control=CacheControl(ttl=_DEFAULT_TTL_SECONDS))
        state.done = True


# ---------------------------------------------------------------------------
# cache_poison — cacheable first batch then a mid-stream worker error
# ---------------------------------------------------------------------------
@dataclass(slots=True, frozen=True)
class CachePoisonArgs:
    """Arguments for CachePoisonFunction (none)."""


@dataclass(kw_only=True)
class _CachePoisonState(ArrowSerializableDataclass):
    """Tracks the two-tick poison sequence (cacheable batch, then the failure)."""

    emitted: bool = False
    poisoned: bool = False


@init_single_worker
@bind_fixed_schema
class CachePoisonFunction(TableFunctionGenerator[CachePoisonArgs, _CachePoisonState]):
    """Emits a cacheable first batch, then RAISES on the next tick.

    Adversarial check of the never-partial invariant: a worker error AFTER a
    cacheable batch has streamed must commit NOTHING to the cache (the failing
    thread never reaches EOS, so ``eos < launched`` and no entry is stored).
    """

    class Meta:
        """Metadata for CachePoisonFunction."""

        name = "cache_poison"
        description = "Cacheable first batch then a mid-stream error (never-partial check)"
        categories = ["generator", "cache", "testing"]
        tags = {"category": "cache", "type": "poison"}

    FunctionArguments = CachePoisonArgs
    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(n=pa.int64())

    @classmethod
    def initial_state(cls, params: ProcessParams[CachePoisonArgs]) -> _CachePoisonState:
        """Create initial state."""
        return _CachePoisonState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[CachePoisonArgs],
        state: _CachePoisonState,
        out: OutputCollector,
    ) -> None:
        """Emit one cacheable batch, then fail on the following tick."""
        if not state.emitted:
            batch = pa.RecordBatch.from_pydict({"n": [0, 1, 2]}, schema=params.output_schema)
            cast(VgiOutputCollector, out).emit(batch, cache_control=CacheControl(ttl=_DEFAULT_TTL_SECONDS))
            state.emitted = True
            return
        raise ValueError("cache_poison: intentional mid-stream failure after a cacheable batch")


# ---------------------------------------------------------------------------
# cache_external_fail — cacheable first batch then an unresolvable pointer batch
# ---------------------------------------------------------------------------
# An unreachable loopback URL (http, no TLS handshake). Port 9 (discard) is
# closed, so resolution fails fast with connection-refused. The poison test also
# lowers http_retries/http_timeout so the failure is bounded and quick.
_UNRESOLVABLE_LOCATION = "http://127.0.0.1:9/vgi-cache-poison-nonexistent"


@init_single_worker
@bind_fixed_schema
class CacheExternalFailFunction(TableFunctionGenerator[CachePoisonArgs, _CachePoisonState]):
    """Emits a cacheable first batch, then an EXTERNAL_LOCATION pointer batch whose
    URL is unreachable, so the client's resolution throws mid-stream.

    Second adversarial never-partial check: an external-location resolution failure
    after a cacheable batch must also commit nothing. The 0-row pointer batch
    carries ``vgi_rpc.location`` metadata (the same key the transport uses for
    externalized batches); the client fetches the URL, fails, and aborts the scan.
    """

    class Meta:
        """Metadata for CacheExternalFailFunction."""

        name = "cache_external_fail"
        description = "Cacheable first batch then an unresolvable external-location pointer"
        categories = ["generator", "cache", "testing"]
        tags = {"category": "cache", "type": "poison_external"}

    FunctionArguments = CachePoisonArgs
    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(n=pa.int64())

    @classmethod
    def initial_state(cls, params: ProcessParams[CachePoisonArgs]) -> _CachePoisonState:
        """Create initial state."""
        return _CachePoisonState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[CachePoisonArgs],
        state: _CachePoisonState,
        out: OutputCollector,
    ) -> None:
        """Emit one cacheable batch, then an unresolvable external-location pointer.

        Over HTTP the client resolves the pointer, fails, and aborts the scan
        before this method is ticked again. The terminal ``finish()`` (reached
        only if resolution were to somehow succeed) keeps the producer from
        looping forever on transports that don't resolve external locations.
        """
        if not state.emitted:
            batch = pa.RecordBatch.from_pydict({"n": [0, 1, 2]}, schema=params.output_schema)
            cast(VgiOutputCollector, out).emit(batch, cache_control=CacheControl(ttl=_DEFAULT_TTL_SECONDS))
            state.emitted = True
            return
        if not state.poisoned:
            # 0-row pointer batch to an unreachable URL — the client tries to fetch
            # it and throws, aborting the scan before EOS.
            empty = pa.RecordBatch.from_pydict({"n": []}, schema=params.output_schema)
            cast(VgiOutputCollector, out).emit(empty, metadata={"vgi_rpc.location": _UNRESOLVABLE_LOCATION})
            state.poisoned = True
            return
        out.finish()


# ---------------------------------------------------------------------------
# cache_bench — parametrizable large cacheable result (scaling bench + S8 guard)
# ---------------------------------------------------------------------------
@dataclass(slots=True, frozen=True)
class CacheBenchArgs:
    """Arguments for CacheBenchFunction."""

    # POSITIONAL Arg(0) (unlike the other cache fixtures' named-with-default args)
    # so the direct path `vgi_table_function(w, 'cache_bench', [rows])` actually
    # honors the requested row count — the scaling bench + the S8 flat-RAM guard
    # need a result whose size they control.
    rows: Annotated[int, Arg(0, doc="Number of rows to generate", ge=0)]


@init_single_worker
@bind_fixed_schema
class CacheBenchFunction(TableFunctionGenerator[CacheBenchArgs, _CacheCountdownState]):
    """Emits ``rows`` int64 rows across many small batches; cacheable.

    Purpose-built for the scaling work: a caller-controlled result size lets the
    C++ concurrency bench build a ~``max_entry_bytes`` result (S6 in-flight-RAM)
    and lets the disk-streaming guard build a result larger than ``memory_limit``
    (S8). Advertises a ``ttl`` so it is cached like any other result.
    """

    class Meta:
        """Metadata for CacheBenchFunction."""

        name = "cache_bench"
        description = "Emits `rows` int64 rows (positional arg); cacheable — scaling bench fixture"
        categories = ["generator", "cache", "testing"]
        tags = {"category": "cache", "type": "bench"}
        examples = [
            FunctionExample(
                sql="SELECT count(*) FROM cache_bench(1000000)",
                description="Million-row cacheable result for scaling tests",
            ),
        ]

    FunctionArguments = CacheBenchArgs
    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(v=pa.int64())
    BATCH_SIZE: ClassVar[int] = 2048

    @classmethod
    def initial_state(cls, params: ProcessParams[CacheBenchArgs]) -> _CacheCountdownState:
        """Create initial state with the requested row count."""
        return _CacheCountdownState(remaining=params.args.rows)

    @classmethod
    def process(
        cls,
        params: ProcessParams[CacheBenchArgs],
        state: _CacheCountdownState,
        out: OutputCollector,
    ) -> None:
        """Emit one batch per tick, advertising a TTL on the first batch."""
        if state.remaining <= 0:
            out.finish()
            return
        first_batch = state.current_index == 0
        size = min(state.remaining, cls.BATCH_SIZE)
        values = list(range(state.current_index, state.current_index + size))
        batch = pa.RecordBatch.from_pydict({"v": values}, schema=params.output_schema)
        cache_control = CacheControl(ttl=_DEFAULT_TTL_SECONDS) if first_batch else None
        cast(VgiOutputCollector, out).emit(batch, cache_control=cache_control)
        state.current_index += size
        state.remaining -= size


# ---------------------------------------------------------------------------
# cache_parallel — MULTI-WORKER cacheable result (parallel capture)
# ---------------------------------------------------------------------------
# Work-queue fan-out (like partitioned_sequence): the primary worker enqueues
# fixed-size (start, end) chunks at on_init; ANY worker pops a chunk and emits
# batches for it. Because it is NOT @init_single_worker, the framework advertises
# max_workers=DEFAULT (clamped to `SET threads`), so a cached scan captures ONE
# SUBSTREAM PER WORKER THREAD — the only cache fixture that exercises parallel
# capture (assert `vgi_result_cache().num_substreams > 1`). Values are the plain
# sequence [0..rows) so COUNT=rows and SUM=rows*(rows-1)/2 regardless of how the
# chunks were distributed across workers. Positional Arg(0) so the direct path
# `vgi_table_function(w, 'cache_parallel', [rows])` honors the size; a named
# `batch_size` (default 24000) controls batch width for the 2 GB streaming guard.
_PC_ITEM_FMT = ">QQ"  # (start, end) as two uint64
_PC_ITEM_SIZE = struct.calcsize(_PC_ITEM_FMT)


@dataclass(slots=True, frozen=True)
class CacheParallelArgs:
    """Arguments for CacheParallelFunction."""

    rows: Annotated[int, Arg(0, doc="Total number of rows to generate", ge=0)]
    batch_size: Annotated[int, Arg("batch_size", default=24000, doc="Rows per output batch", ge=1)]


@dataclass(kw_only=True)
class _CacheParallelState(ArrowSerializableDataclass):
    """Per-worker cursor + one-shot cache-control advertise flag."""

    advertised: bool = False
    current_start: int | None = None
    current_end: int | None = None
    current_idx: int = 0


@bind_fixed_schema
class CacheParallelFunction(TableFunctionGenerator[CacheParallelArgs, _CacheParallelState]):
    """Multi-worker cacheable sequence — one capture substream per worker.

    Purpose-built to prove **parallel capture** and correct **single-thread
    serve reassembly** of N substreams: run under ``SET threads=8`` and the
    cached entry holds >1 substream, yet a serve returns the complete union.
    Also backs the 2 GB disk-streaming memory guard (caller-controlled size +
    batch width). Advertises a ``ttl`` on each worker's first batch so the
    cache-control latches regardless of which worker emits first.
    """

    # ~24 chunks max regardless of size (like partitioned_sequence) so remote
    # cost scales with fan-out, not row count.
    MAX_CHUNKS: ClassVar[int] = 24
    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(v=pa.int64())

    class Meta:
        """Metadata for CacheParallelFunction."""

        name = "cache_parallel"
        description = "Multi-worker cacheable sequence (one substream per worker); parallel-capture fixture"
        categories = ["generator", "cache", "testing"]
        tags = {"category": "cache", "type": "parallel"}
        examples = [
            FunctionExample(
                sql="SELECT count(*) FROM cache_parallel(1000000)",
                description="Parallel-captured cacheable result across workers",
            ),
        ]

    FunctionArguments = CacheParallelArgs

    @classmethod
    def on_init(cls, params: InitParams[CacheParallelArgs]) -> GlobalInitResponse:
        """Primary worker enqueues (start, end) chunks covering [0, rows)."""
        rows = params.args.rows
        chunk = max(1, -(-rows // cls.MAX_CHUNKS))  # ceil(rows / MAX_CHUNKS)
        work_items = [
            struct.pack(_PC_ITEM_FMT, start, min(start + chunk, rows))
            for start in range(0, rows, chunk)
        ]
        params.storage.queue_push(work_items)  # always push (registers the invocation)
        return GlobalInitResponse()

    @classmethod
    def initial_state(cls, params: ProcessParams[CacheParallelArgs]) -> _CacheParallelState:
        """Create initial per-worker state."""
        return _CacheParallelState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[CacheParallelArgs],
        state: _CacheParallelState,
        out: OutputCollector,
    ) -> None:
        """Pull a chunk, emit one batch; advertise the TTL on this worker's first batch."""
        if state.current_start is None or state.current_idx >= (state.current_end or 0):
            work_data = params.storage.queue_pop()
            if work_data is None:
                out.finish()
                return
            state.current_start, state.current_end = struct.unpack(_PC_ITEM_FMT, work_data)
            state.current_idx = state.current_start

        batch_end = min(state.current_idx + params.args.batch_size, state.current_end or 0)
        values = list(range(state.current_idx, batch_end))
        batch = pa.RecordBatch.from_pydict({"v": values}, schema=params.output_schema)
        cache_control = CacheControl(ttl=_DEFAULT_TTL_SECONDS) if not state.advertised else None
        cast(VgiOutputCollector, out).emit(batch, cache_control=cache_control)
        state.advertised = True
        state.current_idx = batch_end


# ---------------------------------------------------------------------------
# cache_ordered — MULTI-WORKER, ORDER-SENSITIVE cacheable result
# ---------------------------------------------------------------------------
# Like cache_parallel but opts into supports_batch_index / FIXED_ORDER: each
# chunk carries a monotonic partition_id emitted as the batch's batch_index. The
# FIXED_ORDER MaxThreads=1 clamp is DROPPED for supports_batch_index functions,
# so capture still fans out across workers (>1 substream), but on a cache HIT the
# single-thread CachedReplayConnection re-sorts the flattened substreams by
# batch_index — proving the replay reconstructs SOURCE ORDER, not just the row
# set. Correct output is the monotonic sequence 0,1,2,…,rows-1.
_CO_ITEM_FMT = ">QQQ"  # (partition_id, start, end)
_CO_ITEM_SIZE = struct.calcsize(_CO_ITEM_FMT)


@dataclass(slots=True, frozen=True)
class CacheOrderedArgs:
    """Arguments for CacheOrderedFunction."""

    # Named-with-default (not positional) so this can back a catalog *data Table*
    # — the parallel + order-sensitive capture path only exists on the catalog
    # scan (vgi_table_function_set sets fixed_order=false for supports_batch_index;
    # the direct vgi_table_function() path serializes FIXED_ORDER to one thread).
    rows: Annotated[int, Arg("rows", default=200000, doc="Total number of rows to generate", ge=0)]
    chunk_size: Annotated[int, Arg("chunk_size", default=1000, doc="Rows per partition", ge=1)]


@dataclass(kw_only=True)
class _CacheOrderedState(ArrowSerializableDataclass):
    """Per-worker cursor + one-shot advertise flag + current partition_id."""

    advertised: bool = False
    partition_id: int | None = None
    current_start: int | None = None
    current_end: int | None = None
    current_idx: int = 0


@bind_fixed_schema
class CacheOrderedFunction(TableFunctionGenerator[CacheOrderedArgs, _CacheOrderedState]):
    """Multi-worker, order-sensitive cacheable sequence (batch_index tagged).

    Parallel capture (>1 substream) of a FIXED_ORDER / ``supports_batch_index``
    result whose correct output is strictly ``0,1,…,rows-1``. A cache HIT must
    replay in batch_index order (exercising CachedReplayConnection's stable
    sort), so tests assert row ORDER — not merely the row set.
    """

    BATCH_SIZE: ClassVar[int] = 256
    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(n=pa.int64())

    class Meta:
        """Metadata for CacheOrderedFunction."""

        name = "cache_ordered"
        description = "Multi-worker order-sensitive cacheable sequence (batch_index); order-preservation cache fixture"
        categories = ["generator", "cache", "testing"]
        tags = {"category": "cache", "type": "ordered"}
        preserves_order = OrderPreservation.FIXED_ORDER
        supports_batch_index = True
        examples = [
            FunctionExample(
                sql="SELECT * FROM cache_ordered(1000)",
                description="Order-preserving cacheable result across workers",
            ),
        ]

    FunctionArguments = CacheOrderedArgs

    @classmethod
    def on_init(cls, params: InitParams[CacheOrderedArgs]) -> GlobalInitResponse:
        """Primary worker enqueues (partition_id, start, end) chunks in order."""
        rows = params.args.rows
        chunk = params.args.chunk_size
        work_items = [
            struct.pack(_CO_ITEM_FMT, pid, start, min(start + chunk, rows))
            for pid, start in enumerate(range(0, rows, chunk))
        ]
        params.storage.queue_push(work_items)
        return GlobalInitResponse()

    @classmethod
    def initial_state(cls, params: ProcessParams[CacheOrderedArgs]) -> _CacheOrderedState:
        """Create initial per-worker state."""
        return _CacheOrderedState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[CacheOrderedArgs],
        state: _CacheOrderedState,
        out: OutputCollector,
    ) -> None:
        """Pull a partition; emit batch_index-tagged batches; advertise TTL once."""
        if state.partition_id is None or state.current_idx >= (state.current_end or 0):
            work_data = params.storage.queue_pop()
            if work_data is None:
                out.finish()
                return
            state.partition_id, state.current_start, state.current_end = struct.unpack(_CO_ITEM_FMT, work_data)
            state.current_idx = state.current_start

        batch_end = min(state.current_idx + cls.BATCH_SIZE, state.current_end or 0)
        values = list(range(state.current_idx, batch_end))
        batch = pa.RecordBatch.from_pydict({"n": values}, schema=params.output_schema)
        cache_control = CacheControl(ttl=_DEFAULT_TTL_SECONDS) if not state.advertised else None
        cast(VgiOutputCollector, out).emit(batch, cache_control=cache_control, batch_index=state.partition_id)
        state.advertised = True
        state.current_idx = batch_end


# ---------------------------------------------------------------------------
# cache_interleaved — PARALLEL + batch_index reassembly (real reorder on serve)
# ---------------------------------------------------------------------------
# Unlike cache_ordered (FIXED_ORDER → single worker on subprocess → in-order
# arrival → the replay sort is a no-op), this is NOT FIXED_ORDER, so it fans out
# across workers exactly like cache_parallel — multiple substreams whose batches
# INTERLEAVE in arrival order. Each partition is LARGE (many batches, >2048 rows)
# and tagged with batch_index=partition_id; `n` is the global row index. So:
#   * the live (uncached) scan returns rows in worker-arrival order — NOT sorted;
#   * a cached serve flattens the interleaved substreams and stable-sorts by
#     batch_index, reconstructing 0,1,…,N-1.
# The gap between the two proves the replay sort genuinely reorders real parallel
# multi-batch output (not a trivially-already-ordered single batch).
@dataclass(slots=True, frozen=True)
class CacheInterleavedArgs:
    """Arguments for CacheInterleavedFunction."""

    rows: Annotated[int, Arg(0, doc="Total number of rows to generate", ge=0)]
    # Chunk spans MANY batches (chunk // BATCH_SIZE) so the serve reassembly is
    # tested across batch boundaries, not a single already-sorted batch.
    chunk_size: Annotated[int, Arg("chunk_size", default=20000, doc="Rows per partition", ge=1)]


@bind_fixed_schema
class CacheInterleavedFunction(TableFunctionGenerator[CacheInterleavedArgs, _CacheOrderedState]):
    """batch_index-tagged cacheable sequence emitted OUT OF ORDER (serve reorders).

    Proves the cache's serve-side ``batch_index`` reassembly genuinely REORDERS
    real multi-batch output — not a trivially-already-sorted single batch. The
    single worker emits partitions in DESCENDING partition order (highest ``n``
    first) but tags each batch with ``batch_index = partition_id``; ``n`` is the
    global row index. So across many batches:

      * the live (uncached) scan returns rows in emission (descending-block)
        order — NOT monotonic;
      * a cached serve flattens + stable-sorts by ``batch_index``, producing
        strictly ``0,1,…,N-1``.

    (``supports_batch_index=True`` functions capture single-substream on
    subprocess — DuckDB serializes the scan — so scrambled single-worker emission,
    not parallel interleaving, is what forces the replay sort to do real work.
    Parallel multi-substream capture correctness is covered by cache_parallel.)
    """

    BATCH_SIZE: ClassVar[int] = 2048
    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(n=pa.int64())

    class Meta:
        """Metadata for CacheInterleavedFunction."""

        name = "cache_interleaved"
        description = "Parallel batch_index-tagged cacheable sequence; cache serve reassembles order"
        categories = ["generator", "cache", "testing"]
        tags = {"category": "cache", "type": "interleaved"}
        supports_batch_index = True
        examples = [
            FunctionExample(
                sql="SELECT count(*) FROM cache_interleaved(100000)",
                description="Parallel batch_index reassembly on cache serve",
            ),
        ]

    FunctionArguments = CacheInterleavedArgs

    @classmethod
    def on_init(cls, params: InitParams[CacheInterleavedArgs]) -> GlobalInitResponse:
        """Enqueue chunks in DESCENDING partition order so emission ≠ batch_index order."""
        rows = params.args.rows
        chunk = params.args.chunk_size
        work_items = [
            struct.pack(_CO_ITEM_FMT, pid, start, min(start + chunk, rows))
            for pid, start in enumerate(range(0, rows, chunk))
        ]
        work_items.reverse()  # highest partition_id popped first → scrambled arrival
        params.storage.queue_push(work_items)
        return GlobalInitResponse()

    @classmethod
    def initial_state(cls, params: ProcessParams[CacheInterleavedArgs]) -> _CacheOrderedState:
        """Create initial per-worker state."""
        return _CacheOrderedState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[CacheInterleavedArgs],
        state: _CacheOrderedState,
        out: OutputCollector,
    ) -> None:
        """Pull a partition; emit batch_index=partition_id batches; n is the global index."""
        if state.partition_id is None or state.current_idx >= (state.current_end or 0):
            work_data = params.storage.queue_pop()
            if work_data is None:
                out.finish()
                return
            state.partition_id, state.current_start, state.current_end = struct.unpack(_CO_ITEM_FMT, work_data)
            state.current_idx = state.current_start

        batch_end = min(state.current_idx + cls.BATCH_SIZE, state.current_end or 0)
        values = list(range(state.current_idx, batch_end))
        batch = pa.RecordBatch.from_pydict({"n": values}, schema=params.output_schema)
        cache_control = CacheControl(ttl=_DEFAULT_TTL_SECONDS) if not state.advertised else None
        cast(VgiOutputCollector, out).emit(batch, cache_control=cache_control, batch_index=state.partition_id)
        state.advertised = True
        state.current_idx = batch_end


# ---------------------------------------------------------------------------
# cache_types — nested / wide / NULL columns through the spill + disk blob
# ---------------------------------------------------------------------------
# Every other cacheable fixture emits flat int64/string. The disk blob + the
# streaming TOC (seek-past-payload) path is therefore only exercised on
# fixed-width int64. This fixture emits STRUCT / LIST / DECIMAL / TIMESTAMP /
# string columns WITH interleaved NULLs (validity bitmaps + variable/nested
# buffers + dictionary framing) across many batches, so a spilled + streamed
# serve must reassemble all of that byte-identically — not just a matching COUNT.
_CT_SCHEMA = pa.schema(
    [
        ("id", pa.int64()),
        ("tags", pa.list_(pa.int64())),
        ("attrs", pa.struct([("x", pa.int64()), ("y", pa.string())])),
        ("amt", pa.decimal128(18, 2)),
        ("ts", pa.timestamp("us")),
        ("label", pa.string()),
    ]
)


@dataclass(slots=True, frozen=True)
class CacheTypesArgs:
    """Arguments for CacheTypesFunction."""

    rows: Annotated[int, Arg(0, doc="Total number of rows to generate", ge=0)]


@bind_fixed_schema
class CacheTypesFunction(TableFunctionGenerator[CacheTypesArgs, _CacheCountdownState]):
    """Nested/wide/NULL cacheable result — exercises the disk blob on rich types.

    Row ``i`` is deterministic: ``id=i``; every 5th row is NULL in the nullable
    columns (``tags``/``attrs``/``amt``/``ts``/``label``) so validity bitmaps must
    round-trip. Purpose-built for the spill+streaming byte-identity test.
    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = _CT_SCHEMA
    BATCH_SIZE: ClassVar[int] = 2048

    class Meta:
        """Metadata for CacheTypesFunction."""

        name = "cache_types"
        description = "Nested/wide/NULL cacheable result (STRUCT/LIST/DECIMAL/TIMESTAMP + NULLs)"
        categories = ["generator", "cache", "testing"]
        tags = {"category": "cache", "type": "types"}
        examples = [
            FunctionExample(
                sql="SELECT count(*) FROM cache_types(10000)",
                description="Nested/NULL cacheable result",
            ),
        ]

    FunctionArguments = CacheTypesArgs

    @classmethod
    def initial_state(cls, params: ProcessParams[CacheTypesArgs]) -> _CacheCountdownState:
        """Create initial state with the requested row count."""
        return _CacheCountdownState(remaining=params.args.rows)

    @classmethod
    def process(
        cls,
        params: ProcessParams[CacheTypesArgs],
        state: _CacheCountdownState,
        out: OutputCollector,
    ) -> None:
        """Emit one batch per tick; every 5th row is NULL in the nullable columns."""
        if state.remaining <= 0:
            out.finish()
            return
        first_batch = state.current_index == 0
        size = min(state.remaining, cls.BATCH_SIZE)
        from decimal import Decimal
        from typing import Any

        ids: list[int] = []
        tags: list[list[int] | None] = []
        attrs: list[dict[str, Any] | None] = []
        amt: list[Decimal | None] = []
        ts: list[int | None] = []
        label: list[str | None] = []

        for j in range(state.current_index, state.current_index + size):
            ids.append(j)
            if j % 5 == 0:  # NULL row in every nullable column
                tags.append(None)
                attrs.append(None)
                amt.append(None)
                ts.append(None)
                label.append(None)
            else:
                tags.append([j, j + 1, j + 2])
                attrs.append({"x": j, "y": f"y{j}"})
                amt.append(Decimal(f"{j}.{j % 100:02d}"))
                ts.append(j)  # int64 micros → timestamp('us')
                label.append(f"label-{j}")
        batch = pa.RecordBatch.from_arrays(
            [
                pa.array(ids, pa.int64()),
                pa.array(tags, pa.list_(pa.int64())),
                pa.array(attrs, pa.struct([("x", pa.int64()), ("y", pa.string())])),
                pa.array(amt, pa.decimal128(18, 2)),
                pa.array(ts, pa.timestamp("us")),
                pa.array(label, pa.string()),
            ],
            schema=params.output_schema,
        )
        cache_control = CacheControl(ttl=_DEFAULT_TTL_SECONDS) if first_batch else None
        cast(VgiOutputCollector, out).emit(batch, cache_control=cache_control)
        state.current_index += size
        state.remaining -= size


# ---------------------------------------------------------------------------
# cache_filtered — cacheable + STATIC filter pushdown (filter_bytes in the key)
# ---------------------------------------------------------------------------
# The key includes `filter_bytes`, but no other cacheable fixture pushes filters,
# so the "a pushed WHERE n>=5 must never cross-serve a pushed WHERE n>=7" boundary
# (the filter analog of the tested projection cross-serve) is otherwise uncovered.
# filter_pushdown + auto_apply_filters means the framework applies the pushed
# predicate to the emitted rows, so distinct static filters return distinct rows
# AND key on distinct filter_bytes → distinct entries.
@dataclass(slots=True, frozen=True)
class CacheFilteredArgs:
    """Arguments for CacheFilteredFunction."""

    # named-default so it can back a catalog data Table (filter pushdown is wired on
    # the catalog scan path, not the direct vgi_table_function path).
    rows: Annotated[int, Arg("rows", default=100, doc="Total number of rows to generate", ge=0)]


@init_single_worker
@bind_fixed_schema
class CacheFilteredFunction(TableFunctionGenerator[CacheFilteredArgs, _CacheCountdownState]):
    """Cacheable sequence with static filter pushdown (n int64, 0..rows)."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(n=pa.int64())
    BATCH_SIZE: ClassVar[int] = 2048

    class Meta:
        """Metadata for CacheFilteredFunction."""

        name = "cache_filtered"
        description = "Cacheable sequence with static filter pushdown (filter_bytes keying)"
        categories = ["generator", "cache", "testing"]
        tags = {"category": "cache", "type": "filtered"}
        filter_pushdown = True
        auto_apply_filters = True
        examples = [
            FunctionExample(
                sql="SELECT count(*) FROM cache_filtered(100) WHERE n >= 50",
                description="Cacheable filtered result; WHERE keys the entry",
            ),
        ]

    FunctionArguments = CacheFilteredArgs

    @classmethod
    def initial_state(cls, params: ProcessParams[CacheFilteredArgs]) -> _CacheCountdownState:
        """Create initial state with the requested row count."""
        return _CacheCountdownState(remaining=params.args.rows)

    @classmethod
    def process(
        cls,
        params: ProcessParams[CacheFilteredArgs],
        state: _CacheCountdownState,
        out: OutputCollector,
    ) -> None:
        """Emit one batch per tick (framework auto-applies the pushed filter)."""
        if state.remaining <= 0:
            out.finish()
            return
        first_batch = state.current_index == 0
        size = min(state.remaining, cls.BATCH_SIZE)
        values = list(range(state.current_index, state.current_index + size))
        batch = pa.RecordBatch.from_pydict({"n": values}, schema=params.output_schema)
        cache_control = CacheControl(ttl=_DEFAULT_TTL_SECONDS) if first_batch else None
        cast(VgiOutputCollector, out).emit(batch, cache_control=cache_control)
        state.current_index += size
        state.remaining -= size


# ---------------------------------------------------------------------------
# cache_partitioned — partition_values (min/max hints) through the spill blob
# ---------------------------------------------------------------------------
# No other cacheable fixture emits partition_values, so the non-empty pv_bytes
# framing in the disk blob (AppendBatch writes pv_len+pv; LoadFromDiskStreaming
# reads pv_len then SEEKS past it) is untested. A single-valued `country`
# partition column makes the framework emit pv per batch; forced to spill and
# served back, any misframed pv_len would misalign the streaming TOC seek → the
# GROUP BY would return wrong rows. Single-worker → deterministic 5-batch output.
from vgi.metadata import PartitionKind as _PartitionKind
from vgi.schema_utils import partition_field as _partition_field

_CACHE_COUNTRIES: list[str] = ["AU", "BR", "CA", "FR", "US"]


@dataclass(slots=True, frozen=True)
class CachePartitionedArgs:
    """Arguments for CachePartitionedFunction."""

    rows_per_country: Annotated[int, Arg(0, doc="Rows per country partition", ge=1)]


@dataclass(kw_only=True)
class _CachePartitionedState(ArrowSerializableDataclass):
    """Cursor over the fixed country list."""

    country_idx: int = 0
    advertised: bool = False


@init_single_worker
@bind_fixed_schema
class CachePartitionedFunction(TableFunctionGenerator[CachePartitionedArgs, _CachePartitionedState]):
    """Cacheable single-value-partitioned result (country + sales); emits pv per batch."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [_partition_field("country", pa.string()), pa.field("sales", pa.int64())]
    )

    class Meta:
        """Metadata for CachePartitionedFunction."""

        name = "cache_partitioned"
        description = "Cacheable single-value-partitioned result (partition_values through the spill blob)"
        categories = ["generator", "cache", "testing", "partitioning"]
        tags = {"category": "cache", "type": "partitioned"}
        partition_kind = _PartitionKind.SINGLE_VALUE_PARTITIONS
        examples = [
            FunctionExample(
                sql="SELECT country, SUM(sales) FROM cache_partitioned(100) GROUP BY country",
                description="Partitioned cacheable aggregate over country",
            ),
        ]

    FunctionArguments = CachePartitionedArgs

    @classmethod
    def initial_state(cls, params: ProcessParams[CachePartitionedArgs]) -> _CachePartitionedState:
        """Create initial state."""
        return _CachePartitionedState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[CachePartitionedArgs],
        state: _CachePartitionedState,
        out: OutputCollector,
    ) -> None:
        """Emit one single-country batch per tick (pv auto-extracted from `country`)."""
        if state.country_idx >= len(_CACHE_COUNTRIES):
            out.finish()
            return
        country = _CACHE_COUNTRIES[state.country_idx]
        rpc = params.args.rows_per_country
        base = state.country_idx * 1_000_000
        batch = pa.RecordBatch.from_pydict(
            {"country": [country] * rpc, "sales": [base + i for i in range(rpc)]},
            schema=params.output_schema,
        )
        cache_control = CacheControl(ttl=_DEFAULT_TTL_SECONDS) if not state.advertised else None
        cast(VgiOutputCollector, out).emit(batch, cache_control=cache_control)
        state.advertised = True
        state.country_idx += 1
