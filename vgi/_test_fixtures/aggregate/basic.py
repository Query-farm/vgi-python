# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Basic aggregate fixtures: count, sum, avg, weighted_sum."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass, ArrowType

from vgi._test_fixtures.aggregate._common import SumState
from vgi.aggregate_function import AggregateFunction
from vgi.arguments import Param, Returns
from vgi.metadata import DistinctDependence, NullHandling, OrderDependence
from vgi.table_function import ProcessParams


@dataclass(kw_only=True)
class CountState(ArrowSerializableDataclass):
    count: Annotated[int, ArrowType(pa.int64())] = 0


@dataclass(kw_only=True)
class AvgState(ArrowSerializableDataclass):
    total: Annotated[float, ArrowType(pa.float64())] = 0.0
    count: Annotated[int, ArrowType(pa.int64())] = 0


@dataclass(kw_only=True)
class WeightedSumState(ArrowSerializableDataclass):
    total: Annotated[float, ArrowType(pa.float64())] = 0.0


class CountFunction(AggregateFunction[CountState]):
    """Count aggregate — nullary (no input columns).

    SQL: ``SELECT vgi_count() FROM t``
    """

    class Meta:
        name = "vgi_count"
        description = "Count rows"
        null_handling = NullHandling.SPECIAL
        order_dependent = OrderDependence.NOT_ORDER_DEPENDENT
        distinct_dependent = DistinctDependence.NOT_DISTINCT_DEPENDENT

    @classmethod
    def initial_state(cls, params: ProcessParams[None]) -> CountState:
        return CountState()

    @classmethod
    def update(
        cls,
        states: dict[int, CountState],
        group_ids: pa.Int64Array,
    ) -> None:
        table = pa.table({"gid": group_ids})
        grouped = table.group_by("gid").aggregate([("gid", "count")])
        for i in range(grouped.num_rows):
            gid: int = grouped.column("gid")[i].as_py()
            cnt: int = grouped.column("gid_count")[i].as_py()
            states[gid] = CountState(count=states[gid].count + cnt)

    @classmethod
    def combine(cls, source: CountState, target: CountState, params: ProcessParams[None]) -> CountState:
        return CountState(count=source.count + target.count)

    @classmethod
    def finalize(
        cls,
        group_ids: pa.Int64Array,
        states: dict[int, CountState],
        params: ProcessParams[None],
    ) -> Annotated[pa.RecordBatch, Returns(pa.int64())]:
        results = [s.count if (s := states[gid.as_py()]) is not None else 0 for gid in group_ids]
        return pa.record_batch({"result": pa.array(results, type=pa.int64())})


class SumFunction(AggregateFunction[SumState]):
    """Sum aggregate — single int64 input.

    SQL: ``SELECT vgi_sum(value) FROM t GROUP BY category``
    """

    class Meta:
        name = "vgi_sum"
        description = "Sum integer values"
        null_handling = NullHandling.DEFAULT
        order_dependent = OrderDependence.NOT_ORDER_DEPENDENT
        distinct_dependent = DistinctDependence.NOT_DISTINCT_DEPENDENT

    @classmethod
    def initial_state(cls, params: ProcessParams[None]) -> SumState:
        return SumState()

    @classmethod
    def update(
        cls,
        states: dict[int, SumState],
        group_ids: pa.Int64Array,
        value: Annotated[pa.Int64Array, Param(doc="Column to sum")],
    ) -> None:
        table = pa.table({"gid": group_ids, "value": value})
        grouped = table.group_by("gid").aggregate([("value", "sum")])
        for i in range(grouped.num_rows):
            gid: int = grouped.column("gid")[i].as_py()
            val = grouped.column("value_sum")[i].as_py()
            if val is not None:
                states[gid] = SumState(total=states[gid].total + val)

    @classmethod
    def combine(cls, source: SumState, target: SumState, params: ProcessParams[None]) -> SumState:
        return SumState(total=source.total + target.total)

    @classmethod
    def finalize(
        cls,
        group_ids: pa.Int64Array,
        states: dict[int, SumState],
        params: ProcessParams[None],
    ) -> Annotated[pa.RecordBatch, Returns(pa.int64())]:
        results = [s.total if (s := states[gid.as_py()]) is not None else None for gid in group_ids]
        return pa.record_batch({"result": pa.array(results, type=pa.int64())})


class AvgFunction(AggregateFunction[AvgState]):
    """Average aggregate — two-field state (sum + count).

    SQL: ``SELECT vgi_avg(value) FROM t GROUP BY category``
    """

    class Meta:
        name = "vgi_avg"
        description = "Average of integer values"
        null_handling = NullHandling.DEFAULT
        order_dependent = OrderDependence.NOT_ORDER_DEPENDENT
        distinct_dependent = DistinctDependence.NOT_DISTINCT_DEPENDENT

    @classmethod
    def initial_state(cls, params: ProcessParams[None]) -> AvgState:
        return AvgState()

    @classmethod
    def update(
        cls,
        states: dict[int, AvgState],
        group_ids: pa.Int64Array,
        value: Annotated[pa.Int64Array, Param(doc="Column to average")],
    ) -> None:
        table = pa.table({"gid": group_ids, "value": value})
        grouped = table.group_by("gid").aggregate([("value", "sum"), ("value", "count")])
        for i in range(grouped.num_rows):
            gid: int = grouped.column("gid")[i].as_py()
            val_sum = grouped.column("value_sum")[i].as_py()
            val_count: int = grouped.column("value_count")[i].as_py()
            s = states[gid]
            states[gid] = AvgState(
                total=s.total + (val_sum if val_sum is not None else 0.0),
                count=s.count + val_count,
            )

    @classmethod
    def combine(cls, source: AvgState, target: AvgState, params: ProcessParams[None]) -> AvgState:
        return AvgState(total=source.total + target.total, count=source.count + target.count)

    @classmethod
    def finalize(
        cls,
        group_ids: pa.Int64Array,
        states: dict[int, AvgState],
        params: ProcessParams[None],
    ) -> Annotated[pa.RecordBatch, Returns(pa.float64())]:
        results = []
        for gid in group_ids:
            s = states[gid.as_py()]
            results.append(s.total / s.count if s is not None and s.count > 0 else None)
        return pa.record_batch({"result": pa.array(results, type=pa.float64())})


class WeightedSumFunction(AggregateFunction[WeightedSumState]):
    """Weighted sum aggregate — multi-input (value + weight).

    SQL: ``SELECT vgi_weighted_sum(value, weight) FROM t GROUP BY category``
    """

    class Meta:
        name = "vgi_weighted_sum"
        description = "Weighted sum of values"
        null_handling = NullHandling.DEFAULT
        order_dependent = OrderDependence.NOT_ORDER_DEPENDENT
        distinct_dependent = DistinctDependence.NOT_DISTINCT_DEPENDENT

    @classmethod
    def initial_state(cls, params: ProcessParams[None]) -> WeightedSumState:
        return WeightedSumState()

    @classmethod
    def update(
        cls,
        states: dict[int, WeightedSumState],
        group_ids: pa.Int64Array,
        value: Annotated[pa.DoubleArray, Param(doc="Values to sum")],
        weight: Annotated[pa.DoubleArray, Param(doc="Weights")],
    ) -> None:
        import pyarrow.compute as pc

        products = pc.multiply(value, weight)
        table = pa.table({"gid": group_ids, "product": products})
        grouped = table.group_by("gid").aggregate([("product", "sum")])
        for i in range(grouped.num_rows):
            gid: int = grouped.column("gid")[i].as_py()
            val = grouped.column("product_sum")[i].as_py()
            if val is not None:
                states[gid] = WeightedSumState(total=states[gid].total + val)

    @classmethod
    def combine(
        cls, source: WeightedSumState, target: WeightedSumState, params: ProcessParams[None]
    ) -> WeightedSumState:
        return WeightedSumState(total=source.total + target.total)

    @classmethod
    def finalize(
        cls,
        group_ids: pa.Int64Array,
        states: dict[int, WeightedSumState],
        params: ProcessParams[None],
    ) -> Annotated[pa.RecordBatch, Returns(pa.float64())]:
        results = [s.total if (s := states[gid.as_py()]) is not None else None for gid in group_ids]
        return pa.record_batch({"result": pa.array(results, type=pa.float64())})
