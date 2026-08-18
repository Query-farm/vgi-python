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
from typing import Annotated, Any, ClassVar

import pyarrow as pa
from vgi_rpc.rpc import OutputCollector

from vgi.arguments import Arg
from vgi.metadata import FunctionExample
from vgi.protocol import PlanResponse, ScanSplit
from vgi.schema_utils import schema
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
                ).serialize_to_bytes()
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
