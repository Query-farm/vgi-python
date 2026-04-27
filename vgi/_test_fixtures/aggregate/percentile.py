"""Percentile aggregate fixture (sorted-quantile demo)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass, ArrowType

from vgi.aggregate_function import AggregateFunction
from vgi.arguments import ConstParam, Param, Returns
from vgi.metadata import DistinctDependence, NullHandling, OrderDependence
from vgi.table_function import ProcessParams


@dataclass(kw_only=True)
class PercentileState(ArrowSerializableDataclass):
    # Store values as comma-separated string (simple serialization)
    values_csv: Annotated[str, ArrowType(pa.string())] = ""


class PercentileFunction(AggregateFunction[PercentileState]):
    """Approximate percentile — demonstrates ConstParam on aggregate functions.

    SQL: ``SELECT vgi_percentile(value, 0.5) FROM t GROUP BY category``
    The percentile parameter (0.5) is constant-folded at bind time.
    """

    class Meta:
        name = "vgi_percentile"
        description = "Approximate percentile (demonstrates ConstParam)"
        null_handling = NullHandling.DEFAULT
        order_dependent = OrderDependence.NOT_ORDER_DEPENDENT
        distinct_dependent = DistinctDependence.NOT_DISTINCT_DEPENDENT

    @classmethod
    def initial_state(cls, params: ProcessParams[None]) -> PercentileState:
        return PercentileState()

    @classmethod
    def update(
        cls,
        states: dict[int, PercentileState],
        group_ids: pa.Int64Array,
        value: Annotated[pa.DoubleArray, Param(doc="Values")],
        percentile: Annotated[float, ConstParam("Percentile (0-1)", phase="finalize")] = 0.5,
    ) -> None:
        # percentile is NOT injected here (phase="finalize") — only needed in finalize
        for i in range(len(group_ids)):
            gid: int = group_ids[i].as_py()
            val = value[i].as_py()
            if val is not None:
                s = states[gid]
                if s.values_csv:
                    states[gid] = PercentileState(values_csv=s.values_csv + "," + str(val))
                else:
                    states[gid] = PercentileState(values_csv=str(val))

    @classmethod
    def combine(cls, source: PercentileState, target: PercentileState, params: ProcessParams[None]) -> PercentileState:
        if source.values_csv and target.values_csv:
            return PercentileState(values_csv=target.values_csv + "," + source.values_csv)
        return PercentileState(values_csv=target.values_csv or source.values_csv)

    @classmethod
    def finalize(
        cls,
        group_ids: pa.Int64Array,
        states: dict[int, PercentileState],
        params: ProcessParams[None],
    ) -> Annotated[pa.RecordBatch, Returns(pa.float64())]:
        # Access percentile via params.args (loaded from FunctionStorage)
        pct = params.args.positional[0].as_py() if params.args and params.args.positional else 0.5
        results: list[float | None] = []
        for gid in group_ids:
            s = states[gid.as_py()]
            if s is not None and s.values_csv:
                vals = sorted(float(v) for v in s.values_csv.split(","))
                idx = min(int(pct * len(vals)), len(vals) - 1)
                results.append(vals[idx])
            else:
                results.append(None)
        return pa.record_batch({"result": pa.array(results, type=pa.float64())})


# ---------------------------------------------------------------------------
# GenericSumFunction — demonstrates AnyArrow / dynamic output type
# ---------------------------------------------------------------------------
