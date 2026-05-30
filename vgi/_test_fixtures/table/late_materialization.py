# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Late-materialization fixture.

Exercises DuckDB's late-materialization optimizer end-to-end against a VGI
worker. When ``Meta.late_materialization`` is advertised and the table has a
rowid virtual column, a ``TOP_N`` / ``LIMIT`` / ``SAMPLE`` over the scan is
rewritten by DuckDB into a SEMI join on the rowid: a narrow ordering scan
selects survivors, then the wide scan re-fetches their columns with the
surviving rowids pushed down as a filter.

Schema ``(row_id int64 [is_row_id], ord int64, payload utf8, pushed utf8)``:

* ``row_id == row index`` — unique, deterministic, and snapshot-stable, so a
  rowid emitted by the ordering scan resolves to the same logical row in the
  (independent) wide scan, even across worker processes. This satisfies the
  late-materialization worker contract.
* ``ord`` is a *scrambled* function of the index so a Top-N on ``ord`` yields
  scattered survivor rowids — that drives the exact ``IN``-list pushdown path
  (DuckDB only builds an ``IN`` list for ``2..dynamic_or_filter_threshold``
  survivors; above that it pushes a rowid min/max range instead).
* ``payload`` is the wide column whose materialization the rewrite avoids.
* ``pushed`` is the **witness**: it echoes, per row, the rowid filter the
  worker received (``in=<n>`` join keys, ``rng=<lo>..<hi>`` bounds). Because
  the rewrite's output columns come from the *wide* scan, selecting ``pushed``
  unambiguously reports what was pushed to that scan. This works over both
  subprocess and HTTP transports (unlike in-band ``client_log``).

``dup_row_id=True`` deliberately violates the uniqueness invariant (row_id =
index // 2) to back the negative gating test. ``null_ord_stride>0`` injects
NULLs into ``ord`` for the NULL-ordering test.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Any

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi._test_fixtures.table._common import _cardinality_from_count
from vgi.arguments import Arg
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.table_filter_pushdown import PushdownFilters
from vgi.table_function import (
    BindParams,
    ProcessParams,
    TableFunctionGenerator,
    init_single_worker,
)

# Field name of the rowid column; the C++ extension resolves a pushed rowid
# filter to this name on the wire, so the worker matches it by name.
_ROWID_NAME = "row_id"

# Scramble multiplier (odd, coprime with any reasonable count) used to turn the
# monotonic index into a scattered ordering key.
_SCRAMBLE = 2654435761


def _scramble_ord(index: int) -> int:
    """Deterministic, scattered ordering key for a given row index."""
    return (index * _SCRAMBLE) % 1_000_000_007


def _rowid_pushdown_witness(filters: PushdownFilters | None) -> str:
    """Summarize the rowid filter the worker received as a stable string.

    ``in=<n>``  — total number of rowid ``IN``-list (join-key) values.
    ``rng=<lo>..<hi>`` — min/max rowid range bounds, or ``none`` if absent.
    """
    if filters is None:
        return "rid:in=0;rng=none"

    from vgi.table_filter_pushdown import AndFilter, ConstantFilter, InFilter, OrFilter

    in_count = 0
    lo: Any = None
    hi: Any = None

    def walk(f: object) -> None:
        nonlocal in_count, lo, hi
        if isinstance(f, (AndFilter, OrFilter)):
            for child in f.children:
                walk(child)
        elif isinstance(f, InFilter) and f.column_name == _ROWID_NAME:
            in_count += len(f.values)
        elif isinstance(f, ConstantFilter) and f.column_name == _ROWID_NAME:
            sym = f.op.symbol
            if sym.startswith(">"):
                lo = f.value if lo is None else min(lo, f.value)
            elif sym.startswith("<"):
                hi = f.value if hi is None else max(hi, f.value)
            elif sym == "=":
                lo = hi = f.value

    for f in filters.filters:
        walk(f)

    rng = f"{lo}..{hi}" if (lo is not None or hi is not None) else "none"
    return f"rid:in={in_count};rng={rng}"


@dataclass(slots=True, frozen=True)
class LateMaterializationFunctionArgs:
    """Arguments for LateMaterializationFunction."""

    count: Annotated[int, Arg(0, doc="Number of rows to generate", ge=0)]
    batch_size: Annotated[int, Arg("batch_size", default=2048, doc="Batch size for output", ge=1)]
    dup_row_id: Annotated[
        bool,
        Arg("dup_row_id", default=False, doc="Emit a deliberately non-unique row_id (index // 2)"),
    ]
    null_ord_stride: Annotated[
        int,
        Arg("null_ord_stride", default=0, doc="Emit NULL ord every Nth row (0 = never)", ge=0),
    ]


@dataclass(kw_only=True)
class LateMaterializationState(ArrowSerializableDataclass):
    """Mutable state: position, remaining count, and the cached witness string.

    ``witness`` is serialized (not Transient) so the HTTP rehydrate path — which
    deserializes user state without re-invoking ``initial_state`` — preserves
    the observed pushdown filters across state-token round-trips.
    """

    remaining: int
    current_index: int = 0
    witness: str = "rid:in=0;rng=none"


@init_single_worker
@_cardinality_from_count
class LateMaterializationFunction(
    TableFunctionGenerator[LateMaterializationFunctionArgs, LateMaterializationState]
):
    """Rowid-bearing generator that participates in late materialization.

    SCHEMA
    ------
    Output: {"row_id": int64 [is_row_id], "ord": int64, "payload": utf8,
             "pushed": utf8}

    Example:
    -------
    SELECT row_id, payload FROM late_materialization(100000) ORDER BY ord LIMIT 10
    """

    FunctionArguments = LateMaterializationFunctionArgs

    class Meta:
        """Metadata for LateMaterializationFunction."""

        name = "late_materialization"
        description = "Rowid generator that participates in late materialization"
        categories = ["generator", "diagnostic"]
        projection_pushdown = True
        filter_pushdown = True
        auto_apply_filters = True
        late_materialization = True
        examples = [
            FunctionExample(
                sql="SELECT row_id, payload FROM late_materialization(100000) ORDER BY ord LIMIT 10",
                description="Top-N is late-materialized: payload fetched only for survivors",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[LateMaterializationFunctionArgs]) -> BindResponse:
        """Build the rowid-bearing output schema."""
        rid_field = pa.field(_ROWID_NAME, pa.int64(), metadata={b"is_row_id": b""})
        fields = [
            rid_field,
            pa.field("ord", pa.int64()),
            pa.field("payload", pa.utf8()),
            pa.field("pushed", pa.utf8()),
        ]
        return BindResponse(output_schema=pa.schema(fields))

    @classmethod
    def initial_state(
        cls, params: ProcessParams[LateMaterializationFunctionArgs]
    ) -> LateMaterializationState:
        """Seed state and capture the init-time rowid filter into the witness.

        For the wide probe scan, the SEMI join's build side completes before the
        scan inits, so the surviving rowid range arrives as a *concrete* filter on
        the init-time ``pushdown_filters`` (not a per-tick dynamic filter).
        process() additionally latches anything that shows up per-tick.
        """
        init_witness = "rid:in=0;rng=none"
        ic = params.init_call
        if ic is not None and ic.pushdown_filters is not None:
            init_filters = cls.pushdown_filters(ic.pushdown_filters, join_keys=ic.join_keys)
            init_witness = _rowid_pushdown_witness(init_filters)
        return LateMaterializationState(remaining=params.args.count, witness=init_witness)

    @classmethod
    def process(
        cls,
        params: ProcessParams[LateMaterializationFunctionArgs],
        state: LateMaterializationState,
        out: OutputCollector,
    ) -> None:
        """Emit the next batch of (projected) rowid rows.

        The surviving-rowid filter from late materialization is pushed as a
        *dynamic* filter (populated after the SEMI join's build side completes),
        so it surfaces on ``params.current_pushdown_filters`` per tick — not on
        the init-time ``pushdown_filters``. The probe (wide) scan runs after the
        build, so it sees the full rowid filter from its first tick.
        """
        # Refresh the witness from the current per-tick dynamic filters. Once a
        # rowid filter is present, latch it (later ticks of the probe scan keep
        # seeing it, but guard against a transient empty tick clobbering it).
        tick_witness = _rowid_pushdown_witness(params.current_pushdown_filters)
        if tick_witness != "rid:in=0;rng=none" or state.witness == "rid:in=0;rng=none":
            state.witness = tick_witness

        if state.remaining <= 0:
            out.finish()
            return

        size = min(state.remaining, params.args.batch_size)
        start = state.current_index
        stride = params.args.null_ord_stride

        columns: dict[str, list[Any]] = {}
        for f in params.output_schema:
            if f.name == _ROWID_NAME:
                if params.args.dup_row_id:
                    columns[_ROWID_NAME] = [i // 2 for i in range(start, start + size)]
                else:
                    columns[_ROWID_NAME] = list(range(start, start + size))
            elif f.name == "ord":
                columns["ord"] = [
                    None if (stride > 0 and i % stride == 0) else _scramble_ord(i)
                    for i in range(start, start + size)
                ]
            elif f.name == "payload":
                columns["payload"] = [f"payload_{i}" for i in range(start, start + size)]
            elif f.name == "pushed":
                columns["pushed"] = [state.witness] * size

        out.emit(pa.RecordBatch.from_pydict(columns, schema=params.output_schema))

        state.current_index += size
        state.remaining -= size
