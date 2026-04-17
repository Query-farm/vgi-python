# ruff: noqa: D102, D106
"""Example aggregate function implementations.

Demonstrates the AggregateFunction API with several aggregate types:

- CountFunction: Nullary aggregate (no input columns)
- SumFunction: Single int64 input with grouping
- AvgFunction: Two-field state (sum + count)
- WeightedSumFunction: Multi-input aggregate (value + weight)
- ListAggFunction: Order-dependent aggregate (concatenates strings)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Annotated, Any

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass, ArrowType

from vgi.aggregate_function import AggregateBindParams, AggregateFunction
from vgi.arguments import ConstParam, Param, Returns
from vgi.invocation import BindResponse
from vgi.metadata import DistinctDependence, NullHandling, OrderDependence
from vgi.table_function import ProcessParams

__all__ = [
    "AvgFunction",
    "CountFunction",
    "DynamicAggregateFunction",
    "DynamicMLAggregateFunction",
    "GenericSumFunction",
    "ListAggFunction",
    "PercentileFunction",
    "SumAllFunction",
    "SumFunction",
    "WeightedSumFunction",
]

# ---------------------------------------------------------------------------
# State dataclasses
# ---------------------------------------------------------------------------


@dataclass(kw_only=True)
class CountState(ArrowSerializableDataclass):
    count: Annotated[int, ArrowType(pa.int64())] = 0


@dataclass(kw_only=True)
class SumState(ArrowSerializableDataclass):
    total: Annotated[int, ArrowType(pa.int64())] = 0


@dataclass(kw_only=True)
class AvgState(ArrowSerializableDataclass):
    total: Annotated[float, ArrowType(pa.float64())] = 0.0
    count: Annotated[int, ArrowType(pa.int64())] = 0


@dataclass(kw_only=True)
class WeightedSumState(ArrowSerializableDataclass):
    total: Annotated[float, ArrowType(pa.float64())] = 0.0


@dataclass(kw_only=True)
class ListAggState(ArrowSerializableDataclass):
    values: Annotated[str, ArrowType(pa.string())] = ""


@dataclass(kw_only=True)
class DynamicState(ArrowSerializableDataclass):
    state_bytes: Annotated[bytes, ArrowType(pa.binary())] = b""
    code: Annotated[str, ArrowType(pa.string())] = ""
    params: Annotated[dict[str, float], ArrowType(pa.map_(pa.string(), pa.float64()))] = field(default_factory=dict)


def _serialize_table(table: pa.Table) -> bytes:
    """Serialize a Table to Arrow IPC stream bytes."""
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        for batch in table.to_batches():
            writer.write_batch(batch)
    return sink.getvalue().to_pybytes()


def _deserialize_table(data: bytes) -> pa.Table:
    """Deserialize Arrow IPC stream bytes to a Table."""
    return pa.ipc.open_stream(data).read_all()


# ---------------------------------------------------------------------------
# Aggregate functions
# ---------------------------------------------------------------------------


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
            return BindResponse(output_schema=pa.schema([("result", input_type)]))
        return BindResponse(output_schema=pa.schema([("result", pa.float64())]))

    @classmethod
    def initial_state(cls, params: ProcessParams[None]) -> GenericSumState:
        return GenericSumState()

    @classmethod
    def update(
        cls,
        states: dict[int, GenericSumState],
        group_ids: pa.Int64Array,
        value: Annotated[pa.Array, Param(doc="Numeric value to sum")],
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
        columns: Annotated[pa.Array, Param(doc="Numeric columns to sum", varargs=True)],
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

import numpy as np  # noqa: E402

_DYNAMIC_EXEC_NAMESPACE: dict[str, Any] = {
    "dataclass": dataclass,
    "field": field,
    "Annotated": Annotated,
    "pa": pa,
    "np": np,
    "ArrowSerializableDataclass": ArrowSerializableDataclass,
    "ArrowType": ArrowType,
}

_dynamic_class_cache: dict[str, Any] = {}


def _get_aggregate_class(code: str) -> Any:
    """Exec the code string, validate, cache, and return the Aggregate class."""
    if code not in _dynamic_class_cache:
        namespace: dict[str, Any] = dict(_DYNAMIC_EXEC_NAMESPACE)
        # Compile with dont_inherit=True so `from __future__ import annotations`
        # in this module doesn't make the exec'd annotations into strings.
        compiled = compile(code, "<dynamic_aggregate>", "exec", dont_inherit=True)
        exec(compiled, namespace)  # noqa: S102
        if "Aggregate" not in namespace:
            raise ValueError("Dynamic aggregate code must define a class named 'Aggregate'")
        agg_cls = namespace["Aggregate"]
        for method in ("finalize",):
            if not hasattr(agg_cls, method):
                raise ValueError(f"Aggregate class must define a '{method}' method")
        _dynamic_class_cache[code] = agg_cls
    return _dynamic_class_cache[code]


def _pack_dynamic_state(
    dynamic_state: ArrowSerializableDataclass,
    code: str = "",
    params: dict[str, float] | None = None,
) -> DynamicState:
    return DynamicState(
        state_bytes=dynamic_state.serialize_to_bytes(),
        code=code,
        params=params or {},
    )


def _unpack_dynamic_state(
    wrapper: DynamicState, state_cls: type[ArrowSerializableDataclass]
) -> ArrowSerializableDataclass:
    return state_cls.deserialize_from_bytes(wrapper.state_bytes)


class _DynamicAggregateBase(AggregateFunction[DynamicState]):
    """Shared logic for dynamic aggregate functions.

    The dynamic code's ``update(state, *arrays)`` receives Arrow arrays
    directly — no per-row Python scalar conversion. State stores accumulated
    data as Arrow IPC bytes for zero-copy round-trips.

    For the ML variant, ``finalize(state, params)`` receives the params dict.
    """

    @classmethod
    def initial_state(cls, params: ProcessParams[None]) -> DynamicState:
        return DynamicState()

    @classmethod
    def _do_update(
        cls,
        states: dict[int, DynamicState],
        group_ids: pa.Int64Array,
        code_col: pa.StringArray,
        columns: list[pa.Array],
        params_col: pa.Array | None = None,
    ) -> None:
        code: str = code_col[0].as_py()
        raw_params = params_col[0].as_py() if params_col is not None else None
        if isinstance(raw_params, list):
            params: dict[str, float] = {str(k): float(v) for k, v in raw_params}
        elif isinstance(raw_params, dict):
            params = {str(k): float(v) for k, v in raw_params.items()}
        else:
            params = {}
        _get_aggregate_class(code)  # validate + cache the code early

        # Build a table from the incoming columns (drop nulls)
        col_names = [f"c{i}" for i in range(len(columns))]
        incoming = pa.table({col_names[i]: columns[i] for i in range(len(columns))})
        # Filter null rows
        mask = None
        for col in incoming.columns:
            valid = col.is_valid()
            mask = valid if mask is None else pa.compute.and_(mask, valid)
        if mask is not None:
            incoming = incoming.filter(mask)

        # Group by group_id and dispatch. For window aggregates there's
        # typically one group, so this is just one iteration.
        unique_gids = group_ids.unique()
        for gid_scalar in unique_gids:
            gid: int = gid_scalar.as_py()
            wrapper = states[gid]
            # Get row indices for this group
            gid_mask = pa.compute.equal(group_ids, gid_scalar)
            group_table = incoming.filter(gid_mask)
            if group_table.num_rows == 0:
                continue

            # Accumulate: concat with existing state data.
            if wrapper.state_bytes:
                combined = pa.concat_tables([_deserialize_table(wrapper.state_bytes), group_table])
            else:
                combined = group_table

            states[gid] = DynamicState(
                state_bytes=_serialize_table(combined),
                code=code,
                params=params,
            )

    @classmethod
    def combine(cls, source: DynamicState, target: DynamicState, params: ProcessParams[None]) -> DynamicState:
        code = target.code or source.code
        if not code:
            return target
        p = target.params or source.params
        src_table = _deserialize_table(source.state_bytes) if source.state_bytes else None
        tgt_table = _deserialize_table(target.state_bytes) if target.state_bytes else None
        if src_table is not None and tgt_table is not None:
            combined = pa.concat_tables([tgt_table, src_table])
        else:
            combined = tgt_table or src_table
        return DynamicState(
            state_bytes=_serialize_table(combined) if combined is not None else b"",
            code=code,
            params=p,
        )


class DynamicAggregateFunction(_DynamicAggregateBase):
    """Dynamic aggregate — behavior defined by a Python code string.

    ``vgi_dynamic_agg(code, col1, col2, ...)``

    The code and columns are regular parameters (not constants), so the code
    can come from a table lookup, subquery, or variable.

    The exec namespace pre-provides: ``dataclass``, ``Annotated``, ``pa``,
    ``ArrowSerializableDataclass``, ``ArrowType``.
    """

    class Meta:
        name = "vgi_dynamic_agg"
        description = "Dynamic aggregate defined by Python code string"
        null_handling = NullHandling.DEFAULT
        order_dependent = OrderDependence.NOT_ORDER_DEPENDENT
        distinct_dependent = DistinctDependence.NOT_DISTINCT_DEPENDENT

    @classmethod
    def update(
        cls,
        states: dict[int, DynamicState],
        group_ids: pa.Int64Array,
        code: Annotated[pa.StringArray, Param(doc="Python code defining Aggregate class")],
        columns: Annotated[pa.Array, Param(doc="Input columns", varargs=True)],  # type: ignore[type-arg]
    ) -> None:
        cls._do_update(states, group_ids, code, columns)

    @classmethod
    def finalize(
        cls,
        group_ids: pa.Int64Array,
        states: dict[int, DynamicState],
        params: ProcessParams[None],
    ) -> Annotated[pa.RecordBatch, Returns(pa.float64())]:
        results: list[float | None] = []
        for gid in group_ids:
            wrapper = states[gid.as_py()]
            if wrapper is not None and wrapper.code and wrapper.state_bytes:
                table = _deserialize_table(wrapper.state_bytes)
                agg_cls = _get_aggregate_class(wrapper.code)
                result = agg_cls.finalize(table)
                results.append(float(result) if result is not None else None)
            else:
                results.append(None)
        return pa.record_batch({"result": pa.array(results, type=pa.float64())})


class DynamicMLAggregateFunction(_DynamicAggregateBase):
    """Dynamic ML aggregate with params dict.

    ``vgi_dynamic_ml_agg(code, params, col1, col2, ...)``

    Like ``vgi_dynamic_agg`` but with a ``MAP(VARCHAR, DOUBLE)`` params
    column forwarded to ``Aggregate.finalize(state, params)`` so the
    dynamic code can access arbitrary parameters (seed, lookback, alpha, etc.).

    SQL::

        SELECT vgi_dynamic_ml_agg(
            code,
            MAP {'seed': 42, 'lb': 5, 'alpha': 1.0},
            col1, col2
        ) ...
    """

    class Meta:
        name = "vgi_dynamic_ml_agg"
        description = "Dynamic ML aggregate with params dict"
        null_handling = NullHandling.DEFAULT
        order_dependent = OrderDependence.NOT_ORDER_DEPENDENT
        distinct_dependent = DistinctDependence.NOT_DISTINCT_DEPENDENT

    @classmethod
    def update(
        cls,
        states: dict[int, DynamicState],
        group_ids: pa.Int64Array,
        code: Annotated[pa.StringArray, Param(doc="Python code defining Aggregate class")],
        params_col: Annotated[pa.Array, Param(doc="MAP(VARCHAR, DOUBLE) parameters")],  # type: ignore[type-arg]
        columns: Annotated[pa.Array, Param(doc="Input columns", varargs=True)],  # type: ignore[type-arg]
    ) -> None:
        cls._do_update(states, group_ids, code, columns, params_col=params_col)

    @classmethod
    def finalize(
        cls,
        group_ids: pa.Int64Array,
        states: dict[int, DynamicState],
        params: ProcessParams[None],
    ) -> Annotated[pa.RecordBatch, Returns(pa.float64())]:
        results: list[float | None] = []
        for gid in group_ids:
            wrapper = states[gid.as_py()]
            if wrapper is not None and wrapper.code and wrapper.state_bytes:
                table = _deserialize_table(wrapper.state_bytes)
                agg_cls = _get_aggregate_class(wrapper.code)
                result = agg_cls.finalize(table, wrapper.params)
                results.append(float(result) if result is not None else None)
            else:
                results.append(None)
        return pa.record_batch({"result": pa.array(results, type=pa.float64())})
