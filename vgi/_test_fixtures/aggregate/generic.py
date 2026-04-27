"""GenericSumFunction — any-type aggregate (uses on_bind to derive output type)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass, ArrowType

from vgi.aggregate_function import AggregateBindParams, AggregateFunction
from vgi.arguments import Param, Returns
from vgi.invocation import BindResponse
from vgi.metadata import DistinctDependence, NullHandling, OrderDependence
from vgi.schema_utils import schema
from vgi.table_function import ProcessParams


@dataclass(kw_only=True)
class GenericSumState(ArrowSerializableDataclass):
    total: Annotated[float, ArrowType(pa.float64())] = 0.0


class GenericSumFunction(AggregateFunction[GenericSumState]):
    """Sum aggregate that accepts any numeric type and returns the same type.

    Demonstrates AnyArrow input with dynamic output type resolved in on_bind().
    SQL: ``SELECT vgi_generic_sum(value) FROM t``
    """

    class Meta:
        name = "vgi_generic_sum"
        description = "Sum any numeric type"
        null_handling = NullHandling.DEFAULT
        order_dependent = OrderDependence.NOT_ORDER_DEPENDENT
        distinct_dependent = DistinctDependence.NOT_DISTINCT_DEPENDENT

    @classmethod
    def on_bind(cls, params: AggregateBindParams, **kwargs: object) -> BindResponse:
        """Resolve output type from input type."""
        if params.input_schema:
            input_type = params.input_schema.field(0).type
            return BindResponse(output_schema=schema(result=input_type))
        return BindResponse(output_schema=schema(result=pa.float64()))

    @classmethod
    def initial_state(cls, params: ProcessParams[None]) -> GenericSumState:
        return GenericSumState()

    @classmethod
    def update(
        cls,
        states: dict[int, GenericSumState],
        group_ids: pa.Int64Array,
        value: Annotated[pa.Array, Param(doc="Numeric value to sum")],  # type: ignore[type-arg]
    ) -> None:
        table = pa.table({"gid": group_ids, "value": value.cast(pa.float64())})
        grouped = table.group_by("gid").aggregate([("value", "sum")])
        for i in range(grouped.num_rows):
            gid: int = grouped.column("gid")[i].as_py()
            val = grouped.column("value_sum")[i].as_py()
            if val is not None:
                states[gid] = GenericSumState(total=states[gid].total + val)

    @classmethod
    def combine(cls, source: GenericSumState, target: GenericSumState, params: ProcessParams[None]) -> GenericSumState:
        return GenericSumState(total=source.total + target.total)

    @classmethod
    def finalize(
        cls,
        group_ids: pa.Int64Array,
        states: dict[int, GenericSumState],
        params: ProcessParams[None],
    ) -> Annotated[pa.RecordBatch, Returns()]:
        # Output type determined by on_bind(), available via params.output_schema
        output_type = params.output_schema.field(0).type if params.output_schema else pa.float64()
        results = [s.total if (s := states[gid.as_py()]) is not None else None for gid in group_ids]
        return pa.record_batch({"result": pa.array(results, type=output_type)})


# ---------------------------------------------------------------------------
# SumAllFunction — demonstrates varargs aggregate (sums all numeric columns)
# ---------------------------------------------------------------------------
