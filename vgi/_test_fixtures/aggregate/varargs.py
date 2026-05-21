# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""SumAllFunction — varargs aggregate (sums any number of numeric columns)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass, ArrowType

from vgi.aggregate_function import AggregateFunction
from vgi.arguments import Param, Returns
from vgi.metadata import DistinctDependence, NullHandling, OrderDependence
from vgi.table_function import ProcessParams


@dataclass(kw_only=True)
class SumAllState(ArrowSerializableDataclass):
    total: Annotated[float, ArrowType(pa.float64())] = 0.0


class SumAllFunction(AggregateFunction[SumAllState]):
    """Sum all numeric columns — demonstrates varargs aggregate.

    Accepts any number of numeric columns and sums them all together.
    SQL: ``SELECT vgi_sum_all(a, b, c) FROM t GROUP BY category``
    """

    class Meta:
        name = "vgi_sum_all"
        description = "Sum all numeric columns"
        null_handling = NullHandling.DEFAULT
        order_dependent = OrderDependence.NOT_ORDER_DEPENDENT
        distinct_dependent = DistinctDependence.NOT_DISTINCT_DEPENDENT

    @classmethod
    def initial_state(cls, params: ProcessParams[None]) -> SumAllState:
        return SumAllState()

    @classmethod
    def update(
        cls,
        states: dict[int, SumAllState],
        group_ids: pa.Int64Array,
        columns: Annotated[pa.Array, Param(doc="Numeric columns to sum", varargs=True)],  # type: ignore[type-arg]
    ) -> None:
        for i in range(len(group_ids)):
            gid: int = group_ids[i].as_py()
            row_total = 0.0
            for col in columns:
                val = col[i].as_py()
                if val is not None:
                    row_total += float(val)
            states[gid] = SumAllState(total=states[gid].total + row_total)

    @classmethod
    def combine(cls, source: SumAllState, target: SumAllState, params: ProcessParams[None]) -> SumAllState:
        return SumAllState(total=source.total + target.total)

    @classmethod
    def finalize(
        cls,
        group_ids: pa.Int64Array,
        states: dict[int, SumAllState],
        params: ProcessParams[None],
    ) -> Annotated[pa.RecordBatch, Returns(pa.float64())]:
        results = [s.total if (s := states[gid.as_py()]) is not None else None for gid in group_ids]
        return pa.record_batch({"result": pa.array(results, type=pa.float64())})


# ---------------------------------------------------------------------------
# DynamicAggregateFunction — aggregate behavior defined by Python code string
# ---------------------------------------------------------------------------
