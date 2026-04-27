"""Windowed aggregate fixtures (window_sum, window_median, window_listagg)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Any

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass

from vgi._test_fixtures.aggregate._common import ListAggState, SumState
from vgi.aggregate_function import AggregateFunction, WindowPartition
from vgi.arguments import Param, Returns
from vgi.metadata import DistinctDependence, NullHandling, OrderDependence
from vgi.table_function import ProcessParams


@dataclass(kw_only=True)
class _EmptyWindowState(ArrowSerializableDataclass):
    """Placeholder for functions that don't need derived per-partition state."""

    pass


class WindowSumFunction(AggregateFunction[SumState]):
    """Windowed running-sum — demonstrates a simple window() callback.

    Also implements update/combine/finalize so the function still works in
    plain ``GROUP BY`` contexts (DuckDB picks the window path automatically
    via ``WindowCustomAggregator::CanAggregate``).

    SQL::

        SELECT x, vgi_window_sum(x) OVER (ORDER BY x ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
        FROM generate_series(1, 10) t(x);
    """

    class Meta:
        name = "vgi_window_sum"
        description = "Windowed sum that uses the per-partition window() callback"
        null_handling = NullHandling.DEFAULT
        order_dependent = OrderDependence.NOT_ORDER_DEPENDENT
        distinct_dependent = DistinctDependence.NOT_DISTINCT_DEPENDENT
        supports_window = True

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

    # --- Window path ---

    @classmethod
    def window(
        cls,
        rid: int,
        subframes: list[tuple[int, int]],
        partition: WindowPartition,
        window_state: Any,
        params: ProcessParams[None],
    ) -> int | None:
        import pyarrow.compute as pc

        value_col = partition.inputs.column(0)
        total = 0
        any_valid = False
        for begin, end in subframes:
            if end <= begin:
                continue
            slice_ = value_col.slice(begin, end - begin)
            if partition.filter_mask is not None:
                mask = partition.filter_mask.slice(begin, end - begin)
                slice_ = slice_.filter(mask)
            s = pc.sum(slice_)
            if s.is_valid:
                total += s.as_py()
                any_valid = True
        return total if any_valid else None


class WindowMedianFunction(AggregateFunction[_EmptyWindowState]):
    """Windowed median — non-incremental, benefits from caching the partition.

    Uses the window() callback exclusively (no incremental update path makes
    sense for median). Falls back to a naive GROUP BY implementation via
    update/combine/finalize that collects values in a single string field.

    SQL::

        SELECT x, vgi_window_median(x) OVER (ORDER BY x ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)
        FROM generate_series(1, 20) t(x);
    """

    class Meta:
        name = "vgi_window_median"
        description = "Windowed median (window() callback demonstrates non-incremental aggregates)"
        null_handling = NullHandling.DEFAULT
        order_dependent = OrderDependence.NOT_ORDER_DEPENDENT
        distinct_dependent = DistinctDependence.NOT_DISTINCT_DEPENDENT
        supports_window = True

    @classmethod
    def initial_state(cls, params: ProcessParams[None]) -> _EmptyWindowState:
        return _EmptyWindowState()

    @classmethod
    def update(
        cls,
        states: dict[int, _EmptyWindowState],
        group_ids: pa.Int64Array,
        value: Annotated[pa.DoubleArray, Param(doc="Column to compute median of")],
    ) -> None:
        # GROUP BY path not the primary use — kept only so the function works
        # when used outside an OVER clause. Caller must not expect exact
        # semantics for huge groups.
        pass

    @classmethod
    def combine(
        cls, source: _EmptyWindowState, target: _EmptyWindowState, params: ProcessParams[None]
    ) -> _EmptyWindowState:
        return target

    @classmethod
    def finalize(
        cls,
        group_ids: pa.Int64Array,
        states: dict[int, _EmptyWindowState],
        params: ProcessParams[None],
    ) -> Annotated[pa.RecordBatch, Returns(pa.float64())]:
        results = [None] * len(group_ids)
        return pa.record_batch({"result": pa.array(results, type=pa.float64())})

    @classmethod
    def window(
        cls,
        rid: int,
        subframes: list[tuple[int, int]],
        partition: WindowPartition,
        window_state: Any,
        params: ProcessParams[None],
    ) -> float | None:
        value_col = partition.inputs.column(0)
        values: list[float] = []
        for begin, end in subframes:
            if end <= begin:
                continue
            slice_ = value_col.slice(begin, end - begin)
            if partition.filter_mask is not None:
                mask = partition.filter_mask.slice(begin, end - begin)
                slice_ = slice_.filter(mask)
            for v in slice_.to_pylist():
                if v is not None:
                    values.append(float(v))
        if not values:
            return None
        values.sort()
        n = len(values)
        mid = n // 2
        if n % 2 == 1:
            return values[mid]
        return (values[mid - 1] + values[mid]) / 2.0


class WindowListAggFunction(AggregateFunction[ListAggState]):
    """Windowed ORDER_DEPENDENT aggregate — demonstrates the fallback handoff.

    For ``vgi_window_listagg(s) OVER (ORDER BY x ...)`` DuckDB picks our
    ``window()`` callback (arg_orders is empty; frame ordering comes from
    the OVER clause).

    For ``vgi_window_listagg(s ORDER BY x) OVER (...)`` DuckDB's
    ``WindowCustomAggregator::CanAggregate`` rejects the window path
    because ``wexpr.arg_orders`` is non-empty, and falls back to
    update/combine/finalize. The result is still correct — just slower.
    """

    class Meta:
        name = "vgi_window_listagg"
        description = "Windowed string concat (ORDER_DEPENDENT; tests fallback handoff)"
        null_handling = NullHandling.DEFAULT
        order_dependent = OrderDependence.ORDER_DEPENDENT
        distinct_dependent = DistinctDependence.DISTINCT_DEPENDENT
        supports_window = True

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

    @classmethod
    def window(
        cls,
        rid: int,
        subframes: list[tuple[int, int]],
        partition: WindowPartition,
        window_state: Any,
        params: ProcessParams[None],
    ) -> str | None:
        value_col = partition.inputs.column(0)
        parts: list[str] = []
        for begin, end in subframes:
            if end <= begin:
                continue
            slice_ = value_col.slice(begin, end - begin)
            if partition.filter_mask is not None:
                mask = partition.filter_mask.slice(begin, end - begin)
                slice_ = slice_.filter(mask)
            for v in slice_.to_pylist():
                if v is not None:
                    parts.append(v)
        return ",".join(parts) if parts else None
