# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""typed_probe — exercises typed const-argument binding and typed column emit.

Const args cover the less-common Arrow scalar types — TIMESTAMP, INTERVAL
(duration), BLOB and UBIGINT — each with a default so calling ``typed_probe(n)``
drives the default path and passing named args drives the scalar-extraction
path. The output echoes the bound values into uint64 / int64 / blob / double
columns. Values are echoed in normalized integer/byte form so this fixture and
its vgi-go counterpart produce byte-identical results for the shared test.
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass
from typing import Annotated, ClassVar

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi.arguments import Arg
from vgi.schema_utils import schema
from vgi.table_function import (
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)

_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.UTC)


def _iv_to_ms(iv: object) -> int:
    """Collapse a duration/interval const to whole milliseconds.

    A declared default arrives as a ``datetime.timedelta``; a SQL ``INTERVAL``
    literal arrives as a pyarrow ``MonthDayNano`` (DuckDB intervals are
    month-day-nano). Mirror vgi-go's GetScalarDuration collapse — months→30d,
    days→24h — so both implementations agree.
    """
    if isinstance(iv, datetime.timedelta):
        return iv // datetime.timedelta(milliseconds=1)
    months = getattr(iv, "months", 0)
    days = getattr(iv, "days", 0)
    nanos = getattr(iv, "nanoseconds", 0)
    return months * 30 * 24 * 3600 * 1000 + days * 24 * 3600 * 1000 + nanos // 1_000_000


TYPED_PROBE_SCHEMA = schema(
    idx=pa.uint64(),
    ts_us=pa.int64(),
    iv_ms=pa.int64(),
    payload=pa.binary(),
    ub=pa.uint64(),
    f=pa.float64(),
)


@dataclass(kw_only=True)
class TypedProbeArgs:
    """Arguments for TypedProbeFunction — one named const per scalar type."""

    n: Annotated[int, Arg(0, doc="Number of rows to emit", ge=0)]
    ts: Annotated[
        datetime.datetime,
        Arg(
            "ts",
            default=datetime.datetime(2026, 1, 2, 3, 4, 5, tzinfo=datetime.UTC),
            arrow_type=pa.timestamp("us", tz="UTC"),
            doc="Timestamp const (TIMESTAMPTZ)",
        ),
    ]
    iv: Annotated[
        datetime.timedelta,
        Arg(
            "iv",
            default=datetime.timedelta(milliseconds=1500),
            arrow_type=pa.duration("ns"),
            doc="Interval const (INTERVAL)",
        ),
    ]
    blob: Annotated[
        bytes,
        Arg("blob", default=b"vgi", arrow_type=pa.binary(), doc="Blob const (BLOB)"),
    ]
    ub: Annotated[
        int,
        Arg("ub", default=9, arrow_type=pa.uint64(), doc="Unsigned const (UBIGINT)"),
    ]
    f: Annotated[float, Arg("f", default=2.5, doc="Float const (DOUBLE)")]


@dataclass(kw_only=True)
class TypedProbeState(ArrowSerializableDataclass):
    """Mutable state — the resolved const values plus emit cursor."""

    n: int
    ts_us: int
    iv_ms: int
    payload: bytes
    ub: int
    f: float
    offset: int = 0


@init_single_worker
@bind_fixed_schema
class TypedProbeFunction(TableFunctionGenerator[TypedProbeArgs, TypedProbeState]):
    """Echo typed const args (timestamp/interval/blob/ubigint) into typed columns."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = TYPED_PROBE_SCHEMA

    class Meta:
        """Function metadata."""

        name = "typed_probe"
        description = "Echoes typed const args (timestamp/interval/blob/ubigint) into typed columns"

    @classmethod
    def initial_state(cls, params: ProcessParams[TypedProbeArgs]) -> TypedProbeState:
        """Resolve const args into normalized integer/byte form."""
        a = params.args
        return TypedProbeState(
            n=a.n,
            ts_us=(a.ts - _EPOCH) // datetime.timedelta(microseconds=1),
            iv_ms=_iv_to_ms(a.iv),
            payload=a.blob,
            ub=a.ub,
            f=a.f,
        )

    @classmethod
    def process(cls, params: ProcessParams[TypedProbeArgs], state: TypedProbeState, out: OutputCollector) -> None:
        """Emit all rows in a single batch."""
        if state.offset >= state.n:
            out.finish()
            return
        rows = list(range(state.offset, state.n))
        state.offset = state.n
        out.emit(
            pa.RecordBatch.from_pydict(
                {
                    "idx": pa.array(rows, type=pa.uint64()),
                    "ts_us": pa.array([state.ts_us] * len(rows), type=pa.int64()),
                    "iv_ms": pa.array([state.iv_ms] * len(rows), type=pa.int64()),
                    "payload": pa.array([state.payload] * len(rows), type=pa.binary()),
                    "ub": pa.array([state.ub] * len(rows), type=pa.uint64()),
                    "f": pa.array([state.f + i for i in rows], type=pa.float64()),
                },
                schema=TYPED_PROBE_SCHEMA,
            )
        )
