# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Split-capable table generators, for the ``splits/`` SQL integration suite.

Every fixture here is a *twin* of something already in the suite: it must return
row-for-row identical output to a non-split function, because that equivalence is
the baseline every other split test rests on. If ``split_sequence(n)`` ever
disagrees with ``sequence(n)``, nothing else in the suite means anything.

The shapes deliberately cover the ways a split scan goes wrong rather than the
ways it goes right:

* ``split_sequence(n, splits := k)`` — the parity twin of ``sequence(n)``.
* ``split_zero()`` — returns **no splits at all**. Legal, and must produce an
  empty result rather than a crash.
* ``split_empty_ranges(n)`` — some splits yield **zero rows**. Distinct from
  "zero splits" and far likelier in practice (a filter pruned one), and it is the
  shape that silently truncates a scan if a reader treats an empty split as EOS.
* ``split_skewed(n)`` — one split ~100x the others, so greedy claiming can be
  told apart from static assignment.
* ``split_many(n, splits := 1000)`` — far more splits than reader threads, which
  forces sequential re-init on a reused connection.
* ``split_echo_filters(n)`` — reports the pushdown it saw *per split*, so a
  pushdown regression is visible without inferring it from row counts.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Annotated, Any, ClassVar, cast

import pyarrow as pa
from vgi_rpc.rpc import OutputCollector

from vgi.protocol import VgiOutputCollector

from vgi.arguments import Arg
from vgi.metadata import FunctionExample, PartitionKind
from vgi.protocol import PlanResponse, ScanSplit
from vgi.schema_utils import partition_field, schema
from vgi.table_function import (
    BindParams,
    InitParams,
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
)

_ROW = schema({"n": pa.int64()})


def _encode(lo: int, hi: int) -> bytes:
    """A split payload: the half-open range ``[lo, hi)`` this split owns.

    Note this *names* the work — a redemption reads the same rows however many
    times it runs, and whichever process runs it.
    """
    return struct.pack("<qq", lo, hi)


def _decode(payload: bytes) -> tuple[int, int]:
    lo, hi = struct.unpack("<qq", payload)
    return lo, hi


def _ranges(n: int, k: int) -> list[tuple[int, int]]:
    """Divide ``[0, n)`` into ``k`` contiguous ranges, remainder spread over the first few."""
    if k <= 0:
        return []
    base, extra = divmod(max(n, 0), k)
    out: list[tuple[int, int]] = []
    lo = 0
    for i in range(k):
        hi = lo + base + (1 if i < extra else 0)
        out.append((lo, hi))
        lo = hi
    return out


class SplitState:
    """Mutable cursor over the ranges this reader claimed.

    Own encoding rather than Arrow IPC, following ``CountdownState``: this is a
    handful of integers, and a one-row IPC stream would pay for a schema message,
    a batch message and an end-of-stream marker to carry them.
    """

    __slots__ = ("cur", "hi", "idx", "lo")

    def __init__(self, lo: list[int], hi: list[int], idx: int = 0, cur: int = 0) -> None:
        self.lo = lo
        self.hi = hi
        self.idx = idx
        self.cur = cur

    def serialize_to_bytes(self) -> bytes:
        """Pack as: count, idx, cur, then the (lo, hi) pairs."""
        n = len(self.lo)
        return struct.pack(f"<qqq{2 * n}q", n, self.idx, self.cur, *self.lo, *self.hi)

    @classmethod
    def deserialize_from_bytes(cls, data: bytes) -> SplitState:
        """Inverse of :meth:`serialize_to_bytes`."""
        n, idx, cur = struct.unpack_from("<qqq", data, 0)
        rest = struct.unpack_from(f"<{2 * n}q", data, struct.calcsize("<qqq"))
        return cls(lo=list(rest[:n]), hi=list(rest[n:]), idx=idx, cur=cur)


@dataclass(frozen=True)
class SplitSequenceArgs:
    """Arguments for the split-capable sequence twin."""

    n: Annotated[int, Arg("n", doc="Number of rows to produce", ge=0)]
    splits: Annotated[int, Arg("splits", default=4, doc="How many splits to divide the scan into", ge=0)]


class _SplitBase(TableFunctionGenerator[SplitSequenceArgs, SplitState]):
    """Shared plan/redeem/emit machinery for the split fixtures."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = _ROW

    @classmethod
    def _split_count(cls, params: BindParams[SplitSequenceArgs]) -> int:
        return params.args.splits

    @classmethod
    def _plan_ranges(cls, params: BindParams[SplitSequenceArgs]) -> list[tuple[int, int]]:
        return _ranges(params.args.n, cls._split_count(params))

    @classmethod
    def on_plan(cls, params: BindParams[SplitSequenceArgs], request: Any) -> PlanResponse:
        """Divide ``[0, n)`` into contiguous ranges, one split each."""
        ranges = cls._plan_ranges(params)
        return PlanResponse(
            splits=[
                ScanSplit(
                    payload=_encode(lo, hi),
                    estimated_rows=hi - lo,
                    rows_exact=True,
                    estimated_bytes=(hi - lo) * 8,
                )
                for lo, hi in ranges
            ],
            estimated_total_splits=len(ranges),
            estimated_total_rows=params.args.n,
        )

    @classmethod
    def on_split(cls, params: InitParams[SplitSequenceArgs], request: Any, payloads: list[bytes]) -> None:
        """Explicit opt-in: a worker that mints splits must be able to redeem them.

        The ranges are read off ``params.split_payloads`` in ``initial_state``, so
        there is nothing to do here beyond declaring the capability.
        """
        return None

    @classmethod
    def initial_state(cls, params: ProcessParams[SplitSequenceArgs]) -> SplitState:
        """Seed the cursor from the verified split payloads.

        Raises when this init carries no split tokens at all. That is the
        ``vgi_split_scans=false`` case: the client stopped planning, so a
        split-only worker like this one has no way to know what to read. Failing
        here is the point — quietly returning zero rows would be *a different
        answer to the same query*, which is worse than an error. The flag is a
        client-side kill switch, not a compatibility promise about workers.

        Note this is distinct from a plan that legitimately produced ZERO splits
        (``split_zero``): there the client never inits at all, so we are never
        reached.
        """
        if getattr(params.init_call, "split_tokens", None) is None:
            msg = (
                f"{cls.__name__} is split-only but was initialized with no split tokens. "
                "vgi_split_scans is probably off; this worker implements on_plan()/on_split() "
                "and has no primary/secondary path to fall back to."
            )
            raise RuntimeError(msg)
        ranges = [_decode(p) for p in (params.split_payloads or [])]
        return SplitState(
            lo=[lo for lo, _ in ranges],
            hi=[hi for _, hi in ranges],
            idx=0,
            cur=ranges[0][0] if ranges else 0,
        )

    @classmethod
    def process(
        cls,
        params: ProcessParams[SplitSequenceArgs],
        state: SplitState,
        out: OutputCollector,
    ) -> None:
        """Emit one batch, stepping over exhausted ranges without ending the scan.

        The loop is the point. Every tick must either emit or finish, so an
        exhausted range cannot simply return — and it must not finish either,
        because that would end the whole scan and silently drop every remaining
        split. Stepping to the next range inside the loop is what makes a ZERO-ROW
        split a non-event.
        """
        while True:
            if state.idx >= len(state.lo):
                out.finish()
                return
            hi = state.hi[state.idx]
            if state.cur >= hi:
                state.idx += 1
                state.cur = state.lo[state.idx] if state.idx < len(state.lo) else 0
                continue
            end = min(state.cur + 1024, hi)
            out.emit(pa.RecordBatch.from_pydict({"n": list(range(state.cur, end))}, schema=_ROW))
            state.cur = end
            return


@bind_fixed_schema
class SplitSequenceFunction(_SplitBase):
    """Row-for-row identical to ``sequence(n)``, but produced through splits.

    This is the baseline: if this ever disagrees with ``sequence(n)``, no other
    split test is meaningful.

    Example:
    -------
    SELECT * FROM split_sequence(5)
    Returns: [{"n": 0}, {"n": 1}, {"n": 2}, {"n": 3}, {"n": 4}]

    """


    class Meta:
        name = "split_sequence"
        supports_splits = True
        split_token_ttl_seconds = None
        examples = (FunctionExample(sql="SELECT * FROM split_sequence(5)"),)


@bind_fixed_schema
class SplitZeroFunction(_SplitBase):
    """Returns **no splits**. Must yield an empty result, not a crash.

    Zero splits is legal — a fully-pruned scan reaches it — so the client must
    clamp its reader count to at least one and still terminate cleanly.
    """


    class Meta:
        name = "split_zero"
        supports_splits = True

    @classmethod
    def _split_count(cls, params: BindParams[SplitSequenceArgs]) -> int:
        return 0


@bind_fixed_schema
class SplitEmptyRangesFunction(_SplitBase):
    """Half the splits are empty, interleaved with non-empty ones.

    The P0's twin: a reader that treats a zero-row split as end-of-scan silently
    drops every split after the first empty one, and the query still looks
    correct. Interleaving is deliberate — a trailing empty split would not catch it.
    """


    class Meta:
        name = "split_empty_ranges"
        supports_splits = True

    @classmethod
    def _plan_ranges(cls, params: BindParams[SplitSequenceArgs]) -> list[tuple[int, int]]:
        out: list[tuple[int, int]] = []
        for lo, hi in _ranges(params.args.n, cls._split_count(params)):
            out.append((lo, lo))  # empty, first
            out.append((lo, hi))
        return out


@bind_fixed_schema
class SplitSkewedFunction(_SplitBase):
    """One split holds ~99% of the rows.

    Greedy per-split claiming keeps the other readers busy on the small splits;
    static assignment would leave them idle behind the big one. The row count is
    identical either way, so this is about *makespan*, not correctness.
    """


    class Meta:
        name = "split_skewed"
        supports_splits = True

    @classmethod
    def _plan_ranges(cls, params: BindParams[SplitSequenceArgs]) -> list[tuple[int, int]]:
        n = params.args.n
        k = max(cls._split_count(params), 2)
        small = max(n // (100 * (k - 1)), 1) if n else 0
        out: list[tuple[int, int]] = []
        lo = 0
        for _ in range(k - 1):
            hi = min(lo + small, n)
            out.append((lo, hi))
            lo = hi
        out.append((lo, n))
        return out


@bind_fixed_schema
class SplitManyFunction(_SplitBase):
    """Far more splits than reader threads, forcing sequential re-init per reader.

    This is where a reused connection must be reset correctly between splits: the
    prior stream has to be closed before the next init is written, or the init
    request lands inside an unterminated stream.
    """


    class Meta:
        name = "split_many"
        supports_splits = True

    @classmethod
    def _split_count(cls, params: BindParams[SplitSequenceArgs]) -> int:
        return max(params.args.splits, 1000)


@dataclass(frozen=True)
class SplitFailArgs:
    """Arguments for the fixture that fails on a chosen split."""

    n: Annotated[int, Arg("n", doc="Number of rows to produce", ge=0)]
    splits: Annotated[int, Arg("splits", default=4, doc="How many splits to divide the scan into", ge=0)]
    fail_at: Annotated[
        int,
        Arg("fail_at", default=-1, doc="Split ordinal to fail on; -1 never fails"),
    ]
    fail_in_init: Annotated[
        bool,
        Arg("fail_in_init", default=False, doc="Fail during the split's init rather than mid-stream"),
    ]


class FailState(SplitState):
    """A cursor that also remembers WHICH split each range came from.

    :class:`SplitState` is deliberately ``__slots__``-ed — it is serialized on
    every HTTP continuation — so the ordinal gets a subclass rather than an ad-hoc
    attribute.
    """

    __slots__ = ("ordinals",)

    def __init__(
        self,
        lo: list[int],
        hi: list[int],
        ordinals: list[int],
        idx: int = 0,
        cur: int = 0,
    ) -> None:
        super().__init__(lo=lo, hi=hi, idx=idx, cur=cur)
        self.ordinals = ordinals

    def serialize_to_bytes(self) -> bytes:
        """Pack as: count, idx, cur, then the (lo, hi, ordinal) triples."""
        n = len(self.lo)
        return struct.pack(
            f"<qqq{3 * n}q", n, self.idx, self.cur, *self.lo, *self.hi, *self.ordinals
        )

    @classmethod
    def deserialize_from_bytes(cls, data: bytes) -> FailState:
        """Inverse of :meth:`serialize_to_bytes`."""
        n, idx, cur = struct.unpack_from("<qqq", data, 0)
        rest = struct.unpack_from(f"<{3 * n}q", data, struct.calcsize("<qqq"))
        return cls(
            lo=list(rest[:n]),
            hi=list(rest[n : 2 * n]),
            ordinals=list(rest[2 * n :]),
            idx=idx,
            cur=cur,
        )


@bind_fixed_schema
class SplitFailAtFunction(TableFunctionGenerator[SplitFailArgs, FailState]):
    """Fails on a chosen split, in either of the two places that matter.

    The two are genuinely different failure paths, not variations:

    * ``fail_in_init`` fails while REDEEMING the token, before any row is
      produced. The client must not return that connection to the pool — the init
      request is on the wire with no answer, so a later checkout would read this
      split's init response as its own stream header, which is silent
      cross-query corruption on the ``pool true`` default.
    * Otherwise it fails MID-STREAM, after emitting rows. The client must surface
      the error and cache nothing: a partial result committed as complete is the
      failure class the whole never-partial gate exists to prevent.

    ``fail_at`` is a SPLIT ORDINAL rather than a row number, so the test says what
    it means regardless of how the rows divide.
    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = _ROW

    class Meta:
        name = "split_fail_at"
        supports_splits = True

    @classmethod
    def on_plan(cls, params: BindParams[SplitFailArgs], request: Any) -> PlanResponse:
        """Divide evenly; the ordinal rides the payload so a reader knows which it holds."""
        ranges = _ranges(params.args.n, params.args.splits)
        return PlanResponse(
            splits=[
                ScanSplit(
                    # ordinal ‖ lo ‖ hi — the ordinal is what fail_at names.
                    payload=struct.pack("<qqq", i, lo, hi),
                    estimated_rows=hi - lo,
                    rows_exact=True,
                )
                for i, (lo, hi) in enumerate(ranges)
            ],
            estimated_total_splits=len(ranges),
        )

    @classmethod
    def on_split(cls, params: InitParams[SplitFailArgs], request: Any, payloads: list[bytes]) -> None:
        """Fail here to exercise the connection-poisoning path."""
        if not params.args.fail_in_init:
            return
        for payload in payloads:
            ordinal, _lo, _hi = struct.unpack("<qqq", payload)
            if ordinal == params.args.fail_at:
                msg = f"split {ordinal} refuses to initialize (fixture)"
                raise RuntimeError(msg)

    @classmethod
    def initial_state(cls, params: ProcessParams[SplitFailArgs]) -> FailState:
        """Seed from the verified payloads, keeping each range's split ordinal."""
        if getattr(params.init_call, "split_tokens", None) is None:
            msg = "split_fail_at is split-only but was initialized with no split tokens."
            raise RuntimeError(msg)
        decoded = [struct.unpack("<qqq", p) for p in (params.split_payloads or [])]
        return FailState(
            lo=[lo for _, lo, _ in decoded],
            hi=[hi for _, _, hi in decoded],
            ordinals=[o for o, _, _ in decoded],
            idx=0,
            cur=decoded[0][1] if decoded else 0,
        )

    @classmethod
    def process(
        cls,
        params: ProcessParams[SplitFailArgs],
        state: FailState,
        out: OutputCollector,
    ) -> None:
        """Emit a batch, or raise once the failing split has produced a row."""
        while True:
            if state.idx >= len(state.lo):
                out.finish()
                return
            ordinals = state.ordinals
            hi = state.hi[state.idx]
            if state.cur >= hi:
                state.idx += 1
                state.cur = state.lo[state.idx] if state.idx < len(state.lo) else 0
                continue
            # Fail AFTER at least one row of this split has gone out, so the
            # never-partial gate is tested against a genuinely partial capture
            # rather than an empty one.
            if (
                params.args.fail_at >= 0
                and state.idx < len(ordinals)
                and ordinals[state.idx] == params.args.fail_at
                and state.cur > state.lo[state.idx]
            ):
                msg = f"split {params.args.fail_at} failed mid-stream (fixture)"
                raise RuntimeError(msg)
            end = min(state.cur + 8, hi)
            out.emit(pa.RecordBatch.from_pydict({"n": list(range(state.cur, end))}, schema=_ROW))
            state.cur = end
            return


_FILTER_ROW = schema({"split_ordinal": pa.int64(), "saw_filters": pa.bool_(), "n_projection": pa.int64()})


@dataclass(frozen=True)
class SplitEchoArgs:
    """Arguments for the pushdown-reporting fixture."""

    splits: Annotated[int, Arg("splits", default=3, doc="How many splits to report", ge=1)]


@dataclass
class EchoState:
    """One row per split this reader claimed."""

    rows: list[tuple[int, bool, int]]
    done: bool = False

    def serialize_to_bytes(self) -> bytes:
        """Pack as: count, then (ordinal, saw_filters, n_projection) triples."""
        return struct.pack(
            f"<q{3 * len(self.rows)}q",
            len(self.rows),
            *[v for r in self.rows for v in (r[0], int(r[1]), r[2])],
        )

    @classmethod
    def deserialize_from_bytes(cls, data: bytes) -> EchoState:
        """Inverse of :meth:`serialize_to_bytes`."""
        (n,) = struct.unpack_from("<q", data, 0)
        flat = struct.unpack_from(f"<{3 * n}q", data, struct.calcsize("<q"))
        rows = [(flat[i * 3], bool(flat[i * 3 + 1]), flat[i * 3 + 2]) for i in range(n)]
        return cls(rows=rows)


@bind_fixed_schema
class SplitEchoFiltersFunction(TableFunctionGenerator[SplitEchoArgs, EchoState]):
    """Reports, per split, what pushdown the PLAN call actually received.

    A row-count assertion cannot catch a pushdown regression — the rows are the
    same either way — so this fixture makes the pushdown itself the data. What it
    reports is recorded at PLAN time and baked into each split's payload, which is
    the claim under test: filters and projection must reach ``plan()``, not merely
    reach the per-split ``init()`` afterwards.
    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = _FILTER_ROW

    class Meta:
        name = "split_echo_filters"
        supports_splits = True
        projection_pushdown = False
        # filter_pushdown declares that this worker APPLIES the filter, so
        # DuckDB stops re-checking it above the scan. Declaring it while only
        # reporting the filter would be the "wrong answers if declared falsely"
        # hazard in miniature — the rows would come back unfiltered and nothing
        # would catch it. auto_apply_filters makes the declaration true: the
        # framework applies the pushdown to each emitted batch.
        filter_pushdown = True
        auto_apply_filters = True

    @classmethod
    def on_plan(cls, params: BindParams[SplitEchoArgs], request: Any) -> PlanResponse:
        """Bake the pushdown this call saw into every split's payload."""
        saw_filters = getattr(request, "pushdown_filters", None) is not None
        projection = getattr(request, "projection_ids", None) or []
        return PlanResponse(
            splits=[
                ScanSplit(payload=struct.pack("<qqq", i, int(saw_filters), len(projection)))
                for i in range(params.args.splits)
            ],
            estimated_total_splits=params.args.splits,
        )

    @classmethod
    def on_split(cls, params: InitParams[SplitEchoArgs], request: Any, payloads: list[bytes]) -> None:
        """Declare the capability; the payloads are read in initial_state."""
        return None

    @classmethod
    def initial_state(cls, params: ProcessParams[SplitEchoArgs]) -> EchoState:
        """One output row per claimed split, carrying what plan() saw."""
        if getattr(params.init_call, "split_tokens", None) is None:
            msg = "split_echo_filters is split-only but was initialized with no split tokens."
            raise RuntimeError(msg)
        rows = []
        for payload in params.split_payloads or []:
            ordinal, saw, nproj = struct.unpack("<qqq", payload)
            rows.append((ordinal, bool(saw), nproj))
        return EchoState(rows=rows)

    @classmethod
    def process(
        cls,
        params: ProcessParams[SplitEchoArgs],
        state: EchoState,
        out: OutputCollector,
    ) -> None:
        """Emit this reader's rows once, then finish."""
        if state.done:
            out.finish()
            return
        state.done = True
        if not state.rows:
            out.finish()
            return
        out.emit(
            pa.RecordBatch.from_pydict(
                {
                    "split_ordinal": [r[0] for r in state.rows],
                    "saw_filters": [r[1] for r in state.rows],
                    "n_projection": [r[2] for r in state.rows],
                },
                schema=_FILTER_ROW,
            )
        )


@bind_fixed_schema
class SplitEndlessCursorFunction(_SplitBase):
    """Paginates forever: every plan page returns a cursor and never exhausts it.

    A worker can hang a client this way by accident as easily as on purpose, and
    the failure mode is the bad one: a client that stopped early would scan a
    PARTIAL enumeration and report it as the whole answer. The client must instead
    hit its page cap and throw an error naming it — never truncate and proceed.
    """

    class Meta:
        name = "split_endless_cursor"
        supports_splits = True

    @classmethod
    def on_plan(cls, params: BindParams[SplitSequenceArgs], request: Any) -> PlanResponse:
        """Always hand back one split and a fresh cursor."""
        page = len(getattr(request, "cursor", b"") or b"")
        return PlanResponse(
            splits=[ScanSplit(payload=_encode(0, 1))],
            next_cursors=[b"x" * (page + 1)],
        )


@bind_fixed_schema
class SplitStalePlanFunction(_SplitBase):
    """Plans against a catalog version that is not the live one.

    This is the only way a bad split token is reachable through SQL, and that is
    by design: the framework owns the envelope, so a worker cannot mint a token
    with a wrong fingerprint or a cleared seal even deliberately. What it CAN do
    is plan against a snapshot that has since moved — which is exactly the
    real-world situation ``SPLIT_SNAPSHOT_EXPIRED`` exists for, a plan outliving
    the version it was pinned to.

    So ``on_plan`` reports a ``catalog_version`` the catalog will not agree with;
    the framework stamps that as the anchor, and redemption compares it against
    the live version and refuses. The refusal must be distinguishable from
    ``SPLIT_TOKEN_INVALID``, because only this one means "re-run the query" —
    re-running under a fresh plan produces a valid token, whereas re-running a
    wrongly-bound token just reproduces it.

    The forged-token cases (a cleared seal on a keyed worker, a mismatched bind
    fingerprint) are NOT reachable this way and are not faked here: they are
    covered byte-for-byte by the shared cross-SDK vectors in
    ``tests/data/split_tokens/``, which every SDK parses and reproduces.
    """

    class Meta:
        name = "split_stale_plan"
        supports_splits = True

    @classmethod
    def on_plan(cls, params: BindParams[SplitSequenceArgs], request: Any) -> PlanResponse:
        """Divide normally, but pin the plan to a version that has moved on."""
        ranges = _ranges(params.args.n, params.args.splits)
        return PlanResponse(
            splits=[ScanSplit(payload=_encode(lo, hi)) for lo, hi in ranges],
            # Any value the live catalog will not report. The fixture catalog's
            # version is small, so a large constant is reliably "not current"
            # without depending on what that version happens to be.
            catalog_version=987_654_321,
        )


@bind_fixed_schema
class SplitBatchIndexFunction(_SplitBase):
    """Split-capable and ``supports_batch_index``, which together are a contract.

    A batch index must be globally monotonic per reader, and greedy per-split
    claiming re-initializes the same connection for each split — so every split
    starts a fresh stream, and a worker that restarted its numbering per split
    would hand the same reader a decreasing index. Nothing in the transport
    prevents that; the client throws when it happens, which is the right
    behaviour but only useful if the contract is written down and exercised.

    What makes it work is that ``fetch_add`` hands each reader strictly ASCENDING
    split indices, so a worker deriving its batch index from the split's position
    in a globally-ordered index space is monotonic per reader by construction.
    That is the whole reason claiming is greedy rather than grouped — and it is
    NOT something multi-token init provides, since a group's tokens carry no
    ordering of their own.

    Each split here owns a slice of the index space (``ordinal * _STRIDE``), so
    the emitted indices ascend across split boundaries as well as within them.
    The stride bounds how many batches one split may emit before colliding with
    the next; ``VGI_BATCH_INDEX_CAP`` bounds the product, so a worker choosing a
    stride is really choosing ``cap / n_splits``.
    """

    _STRIDE: ClassVar[int] = 1_000

    class Meta:
        name = "split_batch_index"
        supports_splits = True
        supports_batch_index = True

    @classmethod
    def on_plan(cls, params: BindParams[SplitSequenceArgs], request: Any) -> PlanResponse:
        """Give each split its ordinal, which is what its index space keys on."""
        ranges = _ranges(params.args.n, params.args.splits)
        return PlanResponse(
            splits=[
                ScanSplit(payload=struct.pack("<qqq", i, lo, hi), estimated_rows=hi - lo, rows_exact=True)
                for i, (lo, hi) in enumerate(ranges)
            ],
            estimated_total_splits=len(ranges),
        )

    @classmethod
    def on_split(cls, params: InitParams[SplitSequenceArgs], request: Any, payloads: list[bytes]) -> None:
        """Declare the capability; the ordinals are read in initial_state."""
        return None

    @classmethod
    def initial_state(cls, params: ProcessParams[SplitSequenceArgs]) -> FailState:
        """Seed the cursor, keeping each range's split ordinal for its index base."""
        if getattr(params.init_call, "split_tokens", None) is None:
            msg = "split_batch_index is split-only but was initialized with no split tokens."
            raise RuntimeError(msg)
        decoded = [struct.unpack("<qqq", p) for p in (params.split_payloads or [])]
        return FailState(
            lo=[lo for _, lo, _ in decoded],
            hi=[hi for _, _, hi in decoded],
            ordinals=[o for o, _, _ in decoded],
            idx=0,
            cur=decoded[0][1] if decoded else 0,
        )

    @classmethod
    def process(  # type: ignore[override]  # narrower state: FailState carries the ordinals
        cls,
        params: ProcessParams[SplitSequenceArgs],
        state: FailState,
        out: OutputCollector,
    ) -> None:
        """Emit a batch tagged with an index drawn from this split's own space."""
        while True:
            if state.idx >= len(state.lo):
                out.finish()
                return
            lo, hi = state.lo[state.idx], state.hi[state.idx]
            if state.cur >= hi:
                state.idx += 1
                state.cur = state.lo[state.idx] if state.idx < len(state.lo) else 0
                continue
            end = min(state.cur + 8, hi)
            # Base from the split's ordinal, offset by how far into this split we
            # are — ascending within a split, and ascending across splits because
            # the ordinals a reader claims ascend.
            batch_index = state.ordinals[state.idx] * cls._STRIDE + (state.cur - lo) // 8
            cast(VgiOutputCollector, out).emit(
                pa.RecordBatch.from_pydict({"n": list(range(state.cur, end))}, schema=_ROW),
                batch_index=batch_index,
            )
            state.cur = end
            return


@bind_fixed_schema
class SplitShortTtlFunction(_SplitBase):
    """Declares a split-token lifetime far shorter than any client's horizon.

    A token that expires is not a degradation, it is a failed query: nothing
    re-plans when one does, because a distributed engine retries the serialized
    task it was handed and has no path back to the planner. So the only useful
    moment to notice a too-short lifetime is BEFORE the plan is issued, which is
    what a client-side floor gives — a legible refusal naming the shortfall,
    instead of a scan that dies partway through with the work already scheduled.

    One second is unusable everywhere: even DuckDB, whose horizon is the shortest
    of any engine (it plans at execution start), can take longer than that to
    reach a split.
    """

    class Meta:
        name = "split_short_ttl"
        supports_splits = True
        split_token_ttl_seconds = 1


@bind_fixed_schema
class SplitOverlapCursorFunction(_SplitBase):
    """Paginates with OVERLAPPING pages: page 2 re-emits what page 1 already gave.

    Returning several cursors lets a client enumerate a large plan in parallel,
    and that is only sound if the cursors partition the remaining enumeration
    disjointly — no split reachable from two of them. That is a worker obligation
    with no enforcement, and violating it does not produce a tidy error: it
    produces DUPLICATE ROWS, arriving through the very mechanism meant to make
    enumeration faster.

    So the client dedups by token regardless of the contract, and this fixture is
    what proves it. It hands out the same two splits on every page for three
    pages, then stops. A client honouring the contract naively would read six
    splits and return each row three times; one that dedups returns each once and
    can say how many duplicates it dropped.
    """

    _PAGES: ClassVar[int] = 3

    class Meta:
        name = "split_overlap_cursor"
        supports_splits = True

    @classmethod
    def on_plan(cls, params: BindParams[SplitSequenceArgs], request: Any) -> PlanResponse:
        """Emit the same splits on every page, so pages overlap completely."""
        page = len(getattr(request, "cursor", b"") or b"")
        ranges = _ranges(params.args.n, params.args.splits)
        splits = [ScanSplit(payload=_encode(lo, hi)) for lo, hi in ranges]
        if page + 1 >= cls._PAGES:
            # Last page: stop paginating, so the client finishes rather than
            # hitting its page cap — the property under test is dedup, not the cap.
            return PlanResponse(splits=splits)
        return PlanResponse(splits=splits, next_cursors=[b"x" * (page + 1)])


_PART_ROW = schema({"country": pa.string(), "sales": pa.int64()})
_PART_COUNTRIES: tuple[str, ...] = ("US", "DE", "JP", "BR")


@dataclass
class PartSplitState:
    """The countries this reader claimed, and how far into the current one it is."""

    countries: list[str]
    idx: int = 0
    emitted: int = 0

    def serialize_to_bytes(self) -> bytes:
        """Pack as: count, idx, emitted, then each 2-char country code."""
        packed = b"".join(c.encode("ascii").ljust(2, b" ") for c in self.countries)
        return struct.pack("<qqq", len(self.countries), self.idx, self.emitted) + packed

    @classmethod
    def deserialize_from_bytes(cls, data: bytes) -> PartSplitState:
        """Inverse of :meth:`serialize_to_bytes`."""
        n, idx, emitted = struct.unpack_from("<qqq", data, 0)
        base = struct.calcsize("<qqq")
        countries = [data[base + i * 2 : base + i * 2 + 2].decode("ascii").strip() for i in range(n)]
        return cls(countries=countries, idx=idx, emitted=emitted)


@dataclass(frozen=True)
class SplitPartitionArgs:
    """Arguments for the partitioned split fixture."""

    rows_per_country: Annotated[int, Arg("rows_per_country", default=5, doc="Rows in each partition", ge=0)]


@bind_fixed_schema
class SplitPartitionedFunction(TableFunctionGenerator[SplitPartitionArgs, PartSplitState]):
    """One split per partition — the shape a partitioned table naturally takes.

    A partition and a split are different things that usually coincide: a
    partition is a property of the DATA (all rows here share a value), while a
    split is a unit of WORK. A worker that already stores data per partition has
    the split boundaries handed to it, so this is the common case rather than a
    contrived one.

    What it exercises is that the two survive each other. Each split declares its
    partition value on the batches it emits, and the client must keep that
    association through greedy claiming — where splits are handed out in an order
    nobody chose, to readers that each hold several. Losing it does not error: it
    produces a GROUP BY that silently mixes partitions, which is why the
    assertions are per-partition sums rather than a total.
    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [partition_field("country", pa.string()), pa.field("sales", pa.int64())]
    )

    class Meta:
        name = "split_partitioned"
        supports_splits = True
        partition_kind = PartitionKind.SINGLE_VALUE_PARTITIONS

    @classmethod
    def on_plan(cls, params: BindParams[SplitPartitionArgs], request: Any) -> PlanResponse:
        """One split per country, each naming its own partition."""
        return PlanResponse(
            splits=[
                ScanSplit(
                    payload=country.encode("ascii"),
                    estimated_rows=params.args.rows_per_country,
                    rows_exact=True,
                )
                for country in _PART_COUNTRIES
            ],
            estimated_total_splits=len(_PART_COUNTRIES),
        )

    @classmethod
    def on_split(cls, params: InitParams[SplitPartitionArgs], request: Any, payloads: list[bytes]) -> None:
        """Declare the capability; the countries are read in initial_state."""
        return None

    @classmethod
    def initial_state(cls, params: ProcessParams[SplitPartitionArgs]) -> PartSplitState:
        """Seed from the verified payloads — each one names a country."""
        if getattr(params.init_call, "split_tokens", None) is None:
            msg = "split_partitioned is split-only but was initialized with no split tokens."
            raise RuntimeError(msg)
        return PartSplitState(countries=[p.decode("ascii") for p in (params.split_payloads or [])])

    @classmethod
    def process(
        cls,
        params: ProcessParams[SplitPartitionArgs],
        state: PartSplitState,
        out: OutputCollector,
    ) -> None:
        """Emit one single-valued batch per claimed country."""
        while True:
            if state.idx >= len(state.countries):
                out.finish()
                return
            country = state.countries[state.idx]
            rows = params.args.rows_per_country
            state.idx += 1
            if rows <= 0:
                continue
            # Every row in the batch carries the same country, which is what makes
            # it SINGLE_VALUE: the client reads the partition value off the batch
            # rather than being told separately.
            out.emit(
                pa.RecordBatch.from_pydict(
                    {"country": [country] * rows, "sales": [i + 1 for i in range(rows)]},
                    schema=_PART_ROW,
                )
            )
            return


_DYN_ROW = schema({"n": pa.int64(), "pushed_filters": pa.utf8()})


@bind_fixed_schema
class SplitDynamicFilterFunction(TableFunctionGenerator[SplitSequenceArgs, SplitState]):
    """Echoes the DYNAMIC filter each tick carried, per split.

    A plan is built from STATIC filters only — join-key values are not known when
    the plan RPC fires, so they cannot prune the split SET. They arrive later, per
    tick, and prune WITHIN each split. Both halves of that have to keep working
    once a reader re-initializes the same connection per split: the tick filter
    state is a property of the connection, and a split that lost it would silently
    stop pruning.

    "Silently" is the operative word, and it is why this fixture reports the
    filter as DATA rather than leaving the test to infer it from row counts. A
    scan that stopped receiving dynamic filters returns exactly the same rows —
    DuckDB re-checks the predicate above the scan — just after shipping more of
    them. No assertion about the result set can tell the difference.

    Note this fixture deliberately does NOT declare projection_pushdown. A live
    bug had the client attach tick filter state only inside the
    projection-pushdown branch, so a function with dynamic filters and no
    projection pushdown got none at all — exactly this shape.
    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = _DYN_ROW

    class Meta:
        name = "split_dynamic_filter"
        supports_splits = True
        filter_pushdown = True
        auto_apply_filters = True

    @classmethod
    def on_plan(cls, params: BindParams[SplitSequenceArgs], request: Any) -> PlanResponse:
        """Divide descending ranges, so a Top-N tightens its filter as it goes."""
        ranges = _ranges(params.args.n, params.args.splits)
        return PlanResponse(
            splits=[ScanSplit(payload=_encode(lo, hi)) for lo, hi in ranges],
            estimated_total_splits=len(ranges),
        )

    @classmethod
    def on_split(cls, params: InitParams[SplitSequenceArgs], request: Any, payloads: list[bytes]) -> None:
        """Declare the capability; the ranges are read in initial_state."""
        return None

    @classmethod
    def initial_state(cls, params: ProcessParams[SplitSequenceArgs]) -> SplitState:
        """Seed the cursor from the verified split payloads."""
        if getattr(params.init_call, "split_tokens", None) is None:
            msg = "split_dynamic_filter is split-only but was initialized with no split tokens."
            raise RuntimeError(msg)
        ranges = [_decode(p) for p in (params.split_payloads or [])]
        return SplitState(
            lo=[lo for lo, _ in ranges],
            hi=[hi for _, hi in ranges],
            idx=0,
            cur=ranges[0][0] if ranges else 0,
        )

    @classmethod
    def process(
        cls,
        params: ProcessParams[SplitSequenceArgs],
        state: SplitState,
        out: OutputCollector,
    ) -> None:
        """Emit a batch, stamping every row with the filter this tick carried."""
        from vgi._test_fixtures.table.filters import _format_pushed_filters_safe

        while True:
            if state.idx >= len(state.lo):
                out.finish()
                return
            hi = state.hi[state.idx]
            if state.cur >= hi:
                state.idx += 1
                state.cur = state.lo[state.idx] if state.idx < len(state.lo) else 0
                continue
            end = min(state.cur + 4, hi)
            # Read the same way the non-split filter fixtures do: join keys ride
            # the INIT request, and merging them with the tick's own filters is
            # what produces the IN filter a join pushes down.
            # Read the same way the non-split filter fixtures do: the INIT request
            # carries both the serialized filters and the join keys, and merging
            # them is what produces the IN filter a join pushes down. Falling back
            # to the per-tick filters covers a dynamic filter that tightened after
            # this split's init.
            init = params.init_call
            merged = None
            if init is not None and init.pushdown_filters is not None:
                merged = cls.pushdown_filters(init.pushdown_filters, join_keys=init.join_keys)
            if merged is None:
                merged = params.current_pushdown_filters
            filter_str = _format_pushed_filters_safe(merged)
            values = list(range(state.cur, end))
            out.emit(
                pa.RecordBatch.from_pydict(
                    {"n": values, "pushed_filters": [filter_str] * len(values)},
                    schema=_DYN_ROW,
                )
            )
            state.cur = end
            return
