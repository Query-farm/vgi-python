# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""ListAgg aggregate fixture (order-dependent string concatenation)."""

from __future__ import annotations

from typing import Annotated

import pyarrow as pa

from vgi._test_fixtures.aggregate._common import ListAggState
from vgi.aggregate_function import AggregateFunction
from vgi.arguments import Param, Returns
from vgi.metadata import DistinctDependence, NullHandling, OrderDependence
from vgi.table_function import ProcessParams


class ListAggFunction(AggregateFunction[ListAggState]):
    """List aggregate — order-dependent, concatenates strings with comma separator.

    SQL: ``SELECT vgi_listagg(name ORDER BY name) FROM t GROUP BY category``
    """

    class Meta:
        name = "vgi_listagg"
        description = "Concatenate strings with comma separator"
        null_handling = NullHandling.DEFAULT
        order_dependent = OrderDependence.ORDER_DEPENDENT
        distinct_dependent = DistinctDependence.DISTINCT_DEPENDENT

    @classmethod
    def initial_state(cls, params: ProcessParams[None]) -> ListAggState:
        return ListAggState()

    @classmethod
    def update(
        cls,
        states: dict[int, ListAggState],
        group_ids: pa.Int64Array,
        value: Annotated[pa.StringArray, Param(doc="String column")],
    ) -> None:
        for i in range(len(group_ids)):
            gid: int = group_ids[i].as_py()
            val = value[i].as_py()
            if val is not None:
                s = states[gid]
                if s.values:
                    states[gid] = ListAggState(values=s.values + "," + val)
                else:
                    states[gid] = ListAggState(values=val)

    @classmethod
    def combine(cls, source: ListAggState, target: ListAggState, params: ProcessParams[None]) -> ListAggState:
        if source.values and target.values:
            return ListAggState(values=target.values + "," + source.values)
        return ListAggState(values=target.values or source.values)

    @classmethod
    def finalize(
        cls,
        group_ids: pa.Int64Array,
        states: dict[int, ListAggState],
        params: ProcessParams[None],
    ) -> Annotated[pa.RecordBatch, Returns(pa.string())]:
        results = [s.values or None if (s := states[gid.as_py()]) is not None else None for gid in group_ids]
        return pa.record_batch({"result": pa.array(results, type=pa.string())})


# ---------------------------------------------------------------------------
# PercentileFunction — demonstrates ConstParam on aggregate
# ---------------------------------------------------------------------------
