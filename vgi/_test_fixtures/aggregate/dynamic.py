"""Dynamic-code aggregate fixtures (DynamicAggregate, DynamicMLAggregate)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Annotated, Any

import numpy as np
import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass, ArrowType

from vgi.aggregate_function import AggregateFunction, WindowPartition
from vgi.arguments import Param, Returns
from vgi.metadata import DistinctDependence, NullHandling, OrderDependence
from vgi.table_function import ProcessParams


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
        columns: list[pa.Array[Any]],
        params_col: pa.Array[Any] | None = None,
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
        mask: pa.ChunkedArray[pa.BooleanScalar] | pa.BooleanArray | None = None
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
        combined: pa.Table | None
        if src_table is not None and tgt_table is not None:
            combined = pa.concat_tables([tgt_table, src_table])
        else:
            combined = tgt_table or src_table
        return DynamicState(
            state_bytes=_serialize_table(combined) if combined is not None else b"",
            code=code,
            params=p,
        )

    # ------------------------------------------------------------------
    # Windowed path
    # ------------------------------------------------------------------
    # Shared logic for both vgi_dynamic_agg and vgi_dynamic_ml_agg.
    # Each subclass overrides window() directly — the shared helper below just
    # slices all partition columns to the current frame with filter_mask and
    # NULL-drop applied. Reading code/params from the sliced frame (rather
    # than partition.inputs.column(X)[0]) avoids aliasing across partitions
    # when DuckDB batches many partitions into shared buffers.

    @staticmethod
    def _slice_to_frame(  # noqa: D417
        partition: WindowPartition,
        subframes: list[tuple[int, int]],
        data_start: int,
    ) -> pa.Table:
        """Slice all partition columns to the frame rows.

        Args:
            data_start: Index where data columns begin (header columns are
                ``[0 .. data_start)``). NULL-drop is applied on data columns
                only — matches the filtering ``_do_update`` performs in the
                non-window path.

        """
        num_cols = partition.inputs.num_columns
        cols = [partition.inputs.column(i) for i in range(num_cols)]
        col_names = [f"c{i}" for i in range(num_cols)]
        slices: list[pa.Table] = []
        for begin, end in subframes:
            if end <= begin:
                continue
            length = end - begin
            sliced = {col_names[i]: cols[i].slice(begin, length) for i in range(num_cols)}
            t = pa.table(sliced)
            if partition.filter_mask is not None:
                t = t.filter(partition.filter_mask.slice(begin, length))
            data_cols_of_t = t.columns[data_start:]
            if data_cols_of_t:
                null_mask: pa.ChunkedArray[pa.BooleanScalar] | pa.BooleanArray | None = None
                for col in data_cols_of_t:
                    valid = col.is_valid()
                    null_mask = valid if null_mask is None else pa.compute.and_(null_mask, valid)
                if null_mask is not None:
                    t = t.filter(null_mask)
            slices.append(t)
        if not slices:
            return pa.table({c: pa.array([], type=cols[i].type) for i, c in enumerate(col_names)})
        return pa.concat_tables(slices)

    @staticmethod
    def _data_table_from(frame: pa.Table, data_start: int) -> pa.Table:
        """Rebuild a 0-indexed ``c0, c1, …`` data-only table for user code."""
        data_cols = frame.columns[data_start:]
        return pa.table({f"c{i}": col for i, col in enumerate(data_cols)})

    @staticmethod
    def _call_user(agg_cls: Any, data_table: pa.Table, user_params: dict[str, float] | None) -> Any:
        """Prefer the user's ``window()``; fall back to ``finalize()``."""
        fn = getattr(agg_cls, "window", None) or agg_cls.finalize
        if user_params is None:
            return fn(data_table)
        return fn(data_table, user_params)


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
        # User code is free-form Python that may depend on input order (e.g. data[-1]
        # for "last row", slicing like data[:-1] / data[1:]). The framework can't
        # introspect what the user does, so conservatively assume order matters.
        order_dependent = OrderDependence.ORDER_DEPENDENT
        distinct_dependent = DistinctDependence.NOT_DISTINCT_DEPENDENT
        supports_window = True

    @classmethod
    def update(
        cls,
        states: dict[int, DynamicState],
        group_ids: pa.Int64Array,
        code: Annotated[pa.StringArray, Param(doc="Python code defining Aggregate class")],
        columns: Annotated[list[pa.Array], Param(doc="Input columns", varargs=True)],  # type: ignore[type-arg]
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

    @classmethod
    def window(
        cls,
        rid: int,
        subframes: list[tuple[int, int]],
        partition: WindowPartition,
        window_state: Any,
        params: ProcessParams[None],
    ) -> float | None:
        # Column layout: [code, col1, col2, ...]
        frame = cls._slice_to_frame(partition, subframes, data_start=1)
        if frame.num_rows == 0:
            return None
        code = frame.column(0)[0].as_py()
        data_table = cls._data_table_from(frame, data_start=1)
        agg_cls = _get_aggregate_class(code)
        result = cls._call_user(agg_cls, data_table, user_params=None)
        return float(result) if result is not None else None


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
        # User code is free-form Python that may depend on input order (e.g. data[-1]
        # for "last row", slicing like data[:-1] / data[1:]). The framework can't
        # introspect what the user does, so conservatively assume order matters.
        order_dependent = OrderDependence.ORDER_DEPENDENT
        distinct_dependent = DistinctDependence.NOT_DISTINCT_DEPENDENT
        supports_window = True

    @classmethod
    def update(
        cls,
        states: dict[int, DynamicState],
        group_ids: pa.Int64Array,
        code: Annotated[pa.StringArray, Param(doc="Python code defining Aggregate class")],
        params_col: Annotated[pa.Array, Param(doc="MAP(VARCHAR, DOUBLE) parameters")],  # type: ignore[type-arg]
        columns: Annotated[list[pa.Array], Param(doc="Input columns", varargs=True)],  # type: ignore[type-arg]
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

    @classmethod
    def window(
        cls,
        rid: int,
        subframes: list[tuple[int, int]],
        partition: WindowPartition,
        window_state: Any,
        params: ProcessParams[None],
    ) -> float | None:
        # Column layout: [code, params_map, col1, col2, ...]
        frame = cls._slice_to_frame(partition, subframes, data_start=2)
        if frame.num_rows == 0:
            return None
        code = frame.column(0)[0].as_py()
        raw = frame.column(1)[0].as_py()
        if isinstance(raw, list):
            user_params: dict[str, float] = {str(k): float(v) for k, v in raw}
        elif isinstance(raw, dict):
            user_params = {str(k): float(v) for k, v in raw.items()}
        else:
            user_params = {}
        data_table = cls._data_table_from(frame, data_start=2)
        agg_cls = _get_aggregate_class(code)
        result = cls._call_user(agg_cls, data_table, user_params=user_params)
        return float(result) if result is not None else None


# ---------------------------------------------------------------------------
# Window-capable aggregates (Meta.supports_window = True)
# ---------------------------------------------------------------------------
# These demonstrate the window() callback which lets DuckDB ship the whole
# partition once and call the worker per output row with frame bounds.
