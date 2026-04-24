# ruff: noqa: D102, D106
"""Aggregate that collects rows into a dense N-D tensor, plus its inverse.

Two functions that work as a pair:

- ``nest_tensor(value, {axis1: ..., axis2: ...})`` aggregate — collects rows
  from a group into a struct ``{tensor, axes}`` where ``tensor`` is a dense
  nested-list representation of the values keyed by the axis coordinates, and
  ``axes`` is a struct mirroring the input axes argument with each field
  holding that axis's sorted, distinct coordinate values.
- ``unnest_tensor(t)`` table function — inverts the aggregate, emitting one
  row per cell of the Cartesian product (including null-valued cells).

See ``docs/nest_tensor.md`` (if it exists) and the plan at
``/Users/rusty/.claude/plans/i-want-to-write-functional-rossum.md`` for full
semantics.
"""

from __future__ import annotations

import itertools
import os
from dataclasses import dataclass
from typing import Annotated, Any

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass, ArrowType
from vgi_rpc.rpc import OutputCollector

from vgi.aggregate_function import AggregateBindParams, AggregateFunction
from vgi.arguments import Arg, Param, Returns, TableInput
from vgi.invocation import BindResponse
from vgi.metadata import DistinctDependence, NullHandling, OrderDependence
from vgi.scalar_function import BindParameters, BindResult, ScalarFunction
from vgi.table_function import BindParams, ProcessParams
from vgi.table_in_out_function import TableInOutGenerator

__all__ = [
    "NestTensorError",
    "NestTensorFunction",
    "UnnestTensorFunction",
    "UnnestTensorRowsFunction",
]


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class NestTensorError(ValueError):
    """Base error for nest_tensor / unnest_tensor."""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_DEFAULT_MAX_CELLS = 10_000_000


def _max_cells() -> int:
    raw = os.environ.get("VGI_NEST_TENSOR_MAX_CELLS")
    if raw is None:
        return _DEFAULT_MAX_CELLS
    try:
        value = int(raw)
    except ValueError as exc:
        raise NestTensorError(f"VGI_NEST_TENSOR_MAX_CELLS must be an integer, got {raw!r}") from exc
    if value <= 0:
        raise NestTensorError("VGI_NEST_TENSOR_MAX_CELLS must be positive")
    return value


def _validate_coord_type(name: str, arrow_type: pa.DataType) -> None:
    """Raise if an axis coord type is unsupported.

    Allowed: integers, decimals, strings, binary, bool, date, timestamp, time.
    Rejected: floating-point (NaN breaks equality/ordering), nested types.
    """
    if pa.types.is_floating(arrow_type):
        raise NestTensorError(
            f"nest_tensor: axis '{name}' has floating-point type {arrow_type}; "
            f"floats are not supported as coord types (NaN breaks equality)"
        )
    if (
        pa.types.is_struct(arrow_type)
        or pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
        or pa.types.is_map(arrow_type)
    ):
        raise NestTensorError(
            f"nest_tensor: axis '{name}' has nested type {arrow_type}; only scalar coord types are supported"
        )


def _serialize_table(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        for batch in table.to_batches():
            writer.write_batch(batch)
    return sink.getvalue().to_pybytes()


def _deserialize_table(data: bytes) -> pa.Table:
    return pa.ipc.open_stream(data).read_all()


def _read_rows(state: NestTensorState) -> pa.Table | None:
    if not state.rows_ipc:
        return None
    return _deserialize_table(state.rows_ipc)


def _make_nested_lists(shape: list[int], fill: Any = None) -> Any:
    """Build a nested Python list of the given shape, filled with ``fill``."""
    if not shape:
        return fill
    head, *rest = shape
    return [_make_nested_lists(rest, fill) for _ in range(head)]


def _nested_list_type(inner: pa.DataType, depth: int) -> pa.DataType:
    t = inner
    for _ in range(depth):
        t = pa.list_(t)
    return t


def _output_struct_type(value_type: pa.DataType, axes_type: pa.StructType) -> pa.StructType:
    n = len(axes_type)
    tensor_type = _nested_list_type(value_type, n)
    axes_out = pa.struct([pa.field(f.name, pa.list_(f.type)) for f in axes_type])
    return pa.struct([("tensor", tensor_type), ("axes", axes_out)])


# ---------------------------------------------------------------------------
# Aggregate state
# ---------------------------------------------------------------------------


@dataclass(kw_only=True)
class NestTensorState(ArrowSerializableDataclass):
    rows_ipc: Annotated[bytes, ArrowType(pa.binary())] = b""


# ---------------------------------------------------------------------------
# NestTensorFunction
# ---------------------------------------------------------------------------


class NestTensorFunction(AggregateFunction[NestTensorState]):
    """Collect rows into an N-D tensor plus per-axis coordinate lists.

    SQL::

        SELECT nest_tensor(value, {x: col_x, y: col_y}) FROM t GROUP BY g;

    Returns a struct ``{tensor, axes}`` where ``tensor`` is a nested
    ``list<list<...>>`` (one level per axis) and ``axes`` is a struct
    mirroring the input axes argument with each field holding that axis's
    sorted distinct coordinate values.
    """

    class Meta:
        name = "nest_tensor"
        description = "Collect rows into a dense N-D tensor plus per-axis coordinates"
        null_handling = NullHandling.DEFAULT
        order_dependent = OrderDependence.NOT_ORDER_DEPENDENT
        distinct_dependent = DistinctDependence.NOT_DISTINCT_DEPENDENT

    # ------------------------------------------------------------------ bind

    @classmethod
    def on_bind(cls, params: AggregateBindParams, **kwargs: object) -> BindResponse:
        input_schema = params.input_schema
        if input_schema is None or len(input_schema) < 2:
            raise NestTensorError("nest_tensor: expected 2 arguments (value, axes struct)")
        value_type = input_schema.field(0).type
        axes_type = input_schema.field(1).type
        if not pa.types.is_struct(axes_type):
            raise NestTensorError(f"nest_tensor: second argument must be a struct, got {axes_type}")
        if len(axes_type) == 0:
            raise NestTensorError("nest_tensor: axes struct must have at least one field")
        for f in axes_type:
            _validate_coord_type(f.name, f.type)

        out = _output_struct_type(value_type, axes_type)
        return BindResponse(output_schema=pa.schema([("result", out)]))

    # -------------------------------------------------------------- lifecycle

    @classmethod
    def initial_state(cls, params: ProcessParams[Any]) -> NestTensorState:
        return NestTensorState()

    # ------------------------------------------------------------------ update

    @classmethod
    def update(
        cls,
        states: dict[int, NestTensorState],
        group_ids: pa.Int64Array,
        value: Annotated[pa.Array, Param(doc="Tensor cell value")],  # type: ignore[type-arg]
        axes: Annotated[pa.Array, Param(doc="Struct of axis coordinates")],  # type: ignore[type-arg]
    ) -> None:
        if not isinstance(axes, pa.StructArray):
            raise NestTensorError(f"nest_tensor: axes argument must be a struct array, got {type(axes).__name__}")

        n_rows = len(group_ids)
        axis_names = [f.name for f in axes.type]

        # Materialise axes as a dict of field -> Array for fast per-row access.
        axis_columns = {name: axes.field(i) for i, name in enumerate(axis_names)}

        # Group rows by group_id, validating nulls and intra-batch duplicates.
        per_group_rows: dict[int, list[int]] = {}
        per_group_seen: dict[int, set[tuple[Any, ...]]] = {}
        gids_py = group_ids.to_pylist()
        axes_validity = axes.is_valid()
        for i in range(n_rows):
            gid_raw = gids_py[i]
            if gid_raw is None:
                continue  # Null group_id — shouldn't happen but skip defensively.
            gid = gid_raw
            if not axes_validity[i].as_py():
                continue  # Null axes struct → skip.
            coord = []
            for name in axis_names:
                col = axis_columns[name]
                cell = col[i]
                if not cell.is_valid:
                    raise NestTensorError(f"nest_tensor: null coord value for axis '{name}' at row {i} (group {gid})")
                coord.append(cell.as_py())
            coord_t = tuple(coord)
            seen = per_group_seen.setdefault(gid, set())
            if coord_t in seen:
                raise NestTensorError(
                    f"nest_tensor: duplicate coordinate {dict(zip(axis_names, coord, strict=True))} in group {gid}"
                )
            seen.add(coord_t)
            per_group_rows.setdefault(gid, []).append(i)

        if not per_group_rows:
            return

        # Build per-group mini-tables and append to rows_ipc.
        parent_schema = pa.schema(
            [
                pa.field("value", value.type),
                pa.field("axes", axes.type),
            ]
        )
        for gid, indices in per_group_rows.items():
            idx = pa.array(indices, type=pa.int64())
            value_slice = value.take(idx)
            axes_slice = axes.take(idx)
            batch = pa.RecordBatch.from_arrays([value_slice, axes_slice], schema=parent_schema)
            table = pa.Table.from_batches([batch])
            prior_bytes = states[gid].rows_ipc
            if prior_bytes:
                prior = _deserialize_table(prior_bytes)
                table = pa.concat_tables([prior, table])
            states[gid] = NestTensorState(rows_ipc=_serialize_table(table))

    # ---------------------------------------------------------------- combine

    @classmethod
    def combine(
        cls,
        source: NestTensorState,
        target: NestTensorState,
        params: ProcessParams[Any],
    ) -> NestTensorState:
        if not source.rows_ipc:
            return target
        if not target.rows_ipc:
            return NestTensorState(rows_ipc=source.rows_ipc)
        s = _deserialize_table(source.rows_ipc)
        t = _deserialize_table(target.rows_ipc)
        return NestTensorState(rows_ipc=_serialize_table(pa.concat_tables([t, s])))

    # ---------------------------------------------------------------- finalize

    @classmethod
    def finalize(
        cls,
        group_ids: pa.Int64Array,
        states: dict[int, NestTensorState],
        params: ProcessParams[Any],
    ) -> Annotated[pa.RecordBatch, Returns()]:
        output_schema = params.output_schema
        assert output_schema is not None, "nest_tensor: finalize called without output_schema"
        out_type = output_schema.field(0).type
        assert pa.types.is_struct(out_type)
        tensor_type = out_type.field("tensor").type
        axes_out_type = out_type.field("axes").type
        axis_names = [axes_out_type.field(i).name for i in range(len(axes_out_type))]

        max_cells = _max_cells()

        tensors: list[Any] = []
        axes_rows: list[dict[str, list[Any]]] = []
        for gid_scalar in group_ids:
            gid = gid_scalar.as_py()
            state = states.get(gid)
            table = _read_rows(state) if state is not None else None
            if table is None or table.num_rows == 0:
                # No rows for this group (e.g., filtered-out during update or
                # an empty group). Emit zero-shape tensor + empty axes lists.
                tensors.append(_make_nested_lists([0] * len(axis_names)))
                axes_rows.append({name: [] for name in axis_names})
                continue

            tensors_entry, axes_entry = _materialise_group(
                table=table,
                axis_names=axis_names,
                gid=gid,
                max_cells=max_cells,
            )
            tensors.append(tensors_entry)
            axes_rows.append(axes_entry)

        tensor_array = pa.array(tensors, type=tensor_type)
        axes_array = pa.array(axes_rows, type=axes_out_type)
        result_array = pa.StructArray.from_arrays(
            [tensor_array, axes_array], fields=[out_type.field("tensor"), out_type.field("axes")]
        )
        return pa.record_batch([result_array], schema=output_schema)


def _materialise_group(
    *,
    table: pa.Table,
    axis_names: list[str],
    gid: int,
    max_cells: int,
) -> tuple[Any, dict[str, list[Any]]]:
    """Build the nested tensor + axes dict for a single group's accumulated rows."""
    value_col = table.column("value")
    axes_col = table.column("axes")
    n_rows = table.num_rows

    # Collect distinct coord values per axis, sorted ascending. We sort here
    # (rather than preserve insertion order) for deterministic output across
    # parallel combine orderings.
    axis_values: list[list[Any]] = []
    axis_idx: list[dict[Any, int]] = []
    # Combine chunks into a single StructArray for easier field access.
    axes_combined = axes_col.combine_chunks()
    assert isinstance(axes_combined, pa.StructArray)
    for name in axis_names:
        field_array = axes_combined.field(name)
        distinct = sorted({field_array[i].as_py() for i in range(n_rows)})
        axis_values.append(distinct)
        axis_idx.append({v: i for i, v in enumerate(distinct)})

    shape = [len(v) for v in axis_values]
    total = 1
    for s in shape:
        total *= s
    if total > max_cells:
        raise NestTensorError(
            f"nest_tensor: tensor has {total} cells (shape {shape}) "
            f"exceeds VGI_NEST_TENSOR_MAX_CELLS={max_cells} (group {gid})"
        )

    tensor = _make_nested_lists(shape, fill=None)
    filled = _make_nested_lists(shape, fill=False)

    value_flat = value_col.combine_chunks()
    for row in range(n_rows):
        idx_tuple = tuple(axis_idx[a][axes_combined.field(name)[row].as_py()] for a, name in enumerate(axis_names))
        cell = tensor
        flag = filled
        for d in idx_tuple[:-1]:
            cell = cell[d]
            flag = flag[d]
        last = idx_tuple[-1]
        if flag[last]:
            coord = {name: axes_combined.field(name)[row].as_py() for name in axis_names}
            raise NestTensorError(
                f"nest_tensor: duplicate coordinate {coord} in group {gid} (arrived from parallel partitions)"
            )
        cell[last] = value_flat[row].as_py()
        flag[last] = True

    axes_entry = {name: axis_values[i] for i, name in enumerate(axis_names)}
    return tensor, axes_entry


# ---------------------------------------------------------------------------
# UnnestTensorFunction
# ---------------------------------------------------------------------------


class UnnestTensorFunction(ScalarFunction):
    """Invert ``nest_tensor``: return a list of ``{value, axes}`` structs.

    SQL::

        SELECT u.value, u.axes.x, u.axes.y
        FROM (SELECT nest_tensor(v, {x: a, y: b}) AS t FROM rows GROUP BY g) r,
             UNNEST(unnest_tensor(r.t)) AS u(value, axes);

    Every cell of the axes Cartesian product is returned, including cells
    whose ``value`` is null (unfilled slots or null input values).

    Implemented as a scalar (not table) function because DuckDB table
    functions cannot accept correlated column inputs from a lateral join.
    """

    class Meta:
        name = "unnest_tensor"
        description = "Invert nest_tensor: list of {value, axes} structs per cell"

    @classmethod
    def on_bind(cls, params: BindParameters) -> BindResult:
        struct_type = params.arguments_schema.field(0).type
        if not pa.types.is_struct(struct_type):
            raise NestTensorError(f"unnest_tensor: argument must be a struct, got {struct_type}")
        field_names = {struct_type.field(i).name for i in range(len(struct_type))}
        if "tensor" not in field_names or "axes" not in field_names:
            raise NestTensorError(
                f"unnest_tensor: struct must have 'tensor' and 'axes' fields, got {sorted(field_names)}"
            )
        axes_type = struct_type.field("axes").type
        if not pa.types.is_struct(axes_type):
            raise NestTensorError(f"unnest_tensor: 'axes' field must be a struct, got {axes_type}")

        tensor_type = struct_type.field("tensor").type
        depth = 0
        inner = tensor_type
        while pa.types.is_list(inner) or pa.types.is_large_list(inner) or pa.types.is_fixed_size_list(inner):
            depth += 1
            inner = inner.value_type
        if depth != len(axes_type):
            raise NestTensorError(
                f"unnest_tensor: tensor nesting depth {depth} does not match number of axes {len(axes_type)}"
            )

        out_axes_type = pa.struct(
            [pa.field(axes_type.field(i).name, axes_type.field(i).type.value_type) for i in range(len(axes_type))]
        )
        row_type = pa.struct([pa.field("value", inner), pa.field("axes", out_axes_type)])
        return BindResult(pa.list_(row_type))

    @classmethod
    def compute(
        cls,
        tensor: Annotated[pa.Array, Param(doc="Struct produced by nest_tensor")],  # type: ignore[type-arg]
    ) -> Annotated[pa.Array, Returns()]:  # type: ignore[type-arg]
        struct_array = tensor
        if not pa.types.is_struct(struct_array.type):
            raise NestTensorError("unnest_tensor: input must be a struct array")

        axes_type = struct_array.type.field("axes").type
        axis_names = [axes_type.field(i).name for i in range(len(axes_type))]

        out_axes_type = pa.struct(
            [pa.field(axes_type.field(i).name, axes_type.field(i).type.value_type) for i in range(len(axes_type))]
        )
        # Determine cell type by walking tensor nesting.
        tensor_type = struct_array.type.field("tensor").type
        inner = tensor_type
        while pa.types.is_list(inner) or pa.types.is_large_list(inner) or pa.types.is_fixed_size_list(inner):
            inner = inner.value_type
        row_type = pa.struct([pa.field("value", inner), pa.field("axes", out_axes_type)])

        result_rows: list[list[dict[str, Any]] | None] = []
        for i in range(len(struct_array)):
            scalar = struct_array[i]
            if not scalar.is_valid:
                result_rows.append(None)
                continue
            struct_value = scalar.as_py()
            tensor_val = struct_value["tensor"]
            axes_dict = struct_value["axes"]
            coord_lists = [axes_dict.get(name) or [] for name in axis_names]
            if any(len(v) == 0 for v in coord_lists):
                result_rows.append([])
                continue
            rows: list[dict[str, Any]] = []
            for index_tuple in itertools.product(*(range(len(v)) for v in coord_lists)):
                cell: Any = tensor_val
                for d in index_tuple:
                    cell = cell[d]
                rows.append(
                    {
                        "value": cell,
                        "axes": {name: coord_lists[a][index_tuple[a]] for a, name in enumerate(axis_names)},
                    }
                )
            result_rows.append(rows)

        return pa.array(result_rows, type=pa.list_(row_type))


# ---------------------------------------------------------------------------
# UnnestTensorRowsFunction (table-in-out variant for LATERAL joins)
# ---------------------------------------------------------------------------


@dataclass(slots=True, frozen=True, kw_only=True)
class UnnestTensorRowsArgs:
    data: Annotated[TableInput, Arg(0, doc="Input table: one column of nest_tensor structs")]


class UnnestTensorRowsFunction(TableInOutGenerator[UnnestTensorRowsArgs]):
    """Invert ``nest_tensor`` as a table-in-out function.

    Accepts a one-column input table whose column is a nest_tensor-shaped
    struct. Emits one output row per cell of the Cartesian product for every
    input row. Unlike the scalar ``unnest_tensor``, this streams output
    without materialising a full list column, and composes with DuckDB's
    ``LATERAL`` joins on correlated columns.

    SQL::

        SELECT u.value, u.axes.x, u.axes.y
        FROM (SELECT nest_tensor(v, {x: a, y: b}) AS t FROM rows GROUP BY g) r,
             LATERAL unnest_tensor_rows((SELECT r.t)) u;
    """

    class Meta:
        name = "unnest_tensor_rows"
        description = "Invert nest_tensor, streaming one row per cell (LATERAL-friendly)"

    @classmethod
    def on_bind(cls, params: BindParams[UnnestTensorRowsArgs]) -> BindResponse:
        input_schema = params.bind_call.input_schema
        if input_schema is None or len(input_schema) != 1:
            raise NestTensorError(
                "unnest_tensor_rows: input table must have exactly one column (the nest_tensor struct)"
            )
        struct_type = input_schema.field(0).type
        if not pa.types.is_struct(struct_type):
            raise NestTensorError(f"unnest_tensor_rows: input column must be a struct, got {struct_type}")
        field_names = {struct_type.field(i).name for i in range(len(struct_type))}
        if "tensor" not in field_names or "axes" not in field_names:
            raise NestTensorError(
                f"unnest_tensor_rows: struct must have 'tensor' and 'axes' fields, got {sorted(field_names)}"
            )
        axes_type = struct_type.field("axes").type
        if not pa.types.is_struct(axes_type):
            raise NestTensorError(f"unnest_tensor_rows: 'axes' field must be a struct, got {axes_type}")

        tensor_type = struct_type.field("tensor").type
        depth = 0
        inner = tensor_type
        while pa.types.is_list(inner) or pa.types.is_large_list(inner) or pa.types.is_fixed_size_list(inner):
            depth += 1
            inner = inner.value_type
        if depth != len(axes_type):
            raise NestTensorError(
                f"unnest_tensor_rows: tensor nesting depth {depth} does not match number of axes {len(axes_type)}"
            )

        out_axes_type = pa.struct(
            [pa.field(axes_type.field(i).name, axes_type.field(i).type.value_type) for i in range(len(axes_type))]
        )
        output_schema = pa.schema([pa.field("value", inner), pa.field("axes", out_axes_type)])
        return BindResponse(output_schema=output_schema)

    @classmethod
    def process(
        cls,
        params: ProcessParams[UnnestTensorRowsArgs],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        output_schema = params.output_schema
        value_type = output_schema.field("value").type
        axes_out_type = output_schema.field("axes").type
        axis_names = [axes_out_type.field(i).name for i in range(len(axes_out_type))]

        if batch.num_rows == 0:
            out.emit(
                pa.RecordBatch.from_arrays(
                    [pa.array([], type=value_type), pa.array([], type=axes_out_type)],
                    schema=output_schema,
                )
            )
            return

        struct_array = batch.column(0)
        values_buf: list[Any] = []
        axes_buf: list[dict[str, Any]] = []

        for i in range(batch.num_rows):
            scalar = struct_array[i]
            if not scalar.is_valid:
                continue
            struct_value = scalar.as_py()
            tensor_val = struct_value["tensor"]
            axes_dict = struct_value["axes"]
            coord_lists = [axes_dict.get(name) or [] for name in axis_names]
            if any(len(v) == 0 for v in coord_lists):
                continue
            for index_tuple in itertools.product(*(range(len(v)) for v in coord_lists)):
                cell: Any = tensor_val
                for d in index_tuple:
                    cell = cell[d]
                values_buf.append(cell)
                axes_buf.append({name: coord_lists[a][index_tuple[a]] for a, name in enumerate(axis_names)})

        out.emit(
            pa.RecordBatch.from_arrays(
                [pa.array(values_buf, type=value_type), pa.array(axes_buf, type=axes_out_type)],
                schema=output_schema,
            )
        )
