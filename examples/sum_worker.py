# /// script
# requires-python = ">=3.13"
# dependencies = ["vgi-python"]
# ///
"""An aggregate function: sum a column per group.

Aggregate functions accumulate input rows into per-group state, then emit one
result row per group. They are driven by DuckDB's ``GROUP BY``::

    ATTACH 'aggregates' (TYPE vgi, LOCATION 'uv run sum_worker.py');
    SELECT category, aggregates.vgi_sum(value) FROM t GROUP BY category;

The three phases:

- ``update``   — fold a batch of values into per-group state (keyed by group id)
- ``combine``  — merge two partial states for the same group (parallel workers)
- ``finalize`` — turn each group's state into its output row
"""

from dataclasses import dataclass
from typing import Annotated

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass, ArrowType

from vgi import Worker
from vgi.aggregate_function import AggregateFunction
from vgi.arguments import Param, Returns
from vgi.catalog import Catalog, Schema
from vgi.metadata import DistinctDependence, NullHandling, OrderDependence
from vgi.table_function import ProcessParams


@dataclass(kw_only=True)
class SumState(ArrowSerializableDataclass):
    """Running total for one group. Serializable so it survives parallel combine."""

    total: Annotated[int, ArrowType(pa.int64())] = 0


class Sum(AggregateFunction[SumState]):
    """Sum an int64 column, grouped by DuckDB's ``GROUP BY`` columns."""

    class Meta:
        """Function metadata: name, description, and aggregate semantics."""

        name = "vgi_sum"
        description = "Sum integer values per group"
        null_handling = NullHandling.DEFAULT
        order_dependent = OrderDependence.NOT_ORDER_DEPENDENT
        distinct_dependent = DistinctDependence.NOT_DISTINCT_DEPENDENT

    @classmethod
    def initial_state(cls, params: ProcessParams[None]) -> SumState:
        """One fresh accumulator per group."""
        return SumState()

    @classmethod
    def update(
        cls,
        states: dict[int, SumState],
        group_ids: pa.Int64Array,
        value: Annotated[pa.Int64Array, Param(doc="Column to sum")],
    ) -> None:
        """Fold a batch of values into each group's running total."""
        table = pa.table({"gid": group_ids, "value": value})
        grouped = table.group_by("gid").aggregate([("value", "sum")])
        for i in range(grouped.num_rows):
            gid: int = grouped.column("gid")[i].as_py()
            val = grouped.column("value_sum")[i].as_py()
            if val is not None:
                states[gid] = SumState(total=states[gid].total + val)

    @classmethod
    def combine(cls, source: SumState, target: SumState, params: ProcessParams[None]) -> SumState:
        """Merge two partial sums for the same group."""
        return SumState(total=source.total + target.total)

    @classmethod
    def finalize(
        cls,
        group_ids: pa.Int64Array,
        states: dict[int, SumState],
        params: ProcessParams[None],
    ) -> Annotated[pa.RecordBatch, Returns(pa.int64())]:
        """Emit one total per group."""
        results = [s.total if (s := states[gid.as_py()]) is not None else None for gid in group_ids]
        return pa.record_batch({"result": pa.array(results, type=pa.int64())})


class AggregateWorker(Worker):
    """A worker exposing the ``aggregates`` catalog."""

    catalog = Catalog(
        name="aggregates",
        schemas=[Schema(name="main", functions=[Sum])],
    )


if __name__ == "__main__":
    AggregateWorker().run()
