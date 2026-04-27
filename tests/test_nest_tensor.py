# ruff: noqa: D100, D101, D102, D103, D106
"""Tests for nest_tensor aggregate + unnest_tensor table function."""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

from vgi._test_fixtures.nest_tensor import (
    NestTensorError,
    NestTensorFunction,
    NestTensorState,
    UnnestTensorFunction,
    _output_struct_type,
)
from vgi.aggregate_function import AggregateBindParams
from vgi.arguments import Arguments
from vgi.function_storage import BoundStorage, FunctionStorageSqlite
from vgi.scalar_function import BindParameters
from vgi.table_function import ProcessParams, SecretsAccessor

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _bind_params(value_type: pa.DataType, axes_type: pa.DataType) -> AggregateBindParams:
    schema = pa.schema([("value", value_type), ("axes", axes_type)])
    return AggregateBindParams(
        args=None,
        input_schema=schema,
        settings={},
        secrets=SecretsAccessor(None),
    )


def _process_params(value_type: pa.DataType, axes_type: pa.StructType) -> ProcessParams[Any]:
    out_type = _output_struct_type(value_type, axes_type)
    output_schema = pa.schema([("result", out_type)])
    storage = FunctionStorageSqlite(":memory:")
    return ProcessParams(
        args=None,
        init_call=None,
        init_response=None,
        output_schema=output_schema,
        settings={},
        secrets={},
        storage=BoundStorage(storage, b"nest_tensor_test"),
    )


def _run_update(
    value: pa.Array[Any],
    axes: pa.StructArray,
    group_ids: pa.Int64Array,
    states: dict[int, NestTensorState] | None = None,
) -> dict[int, NestTensorState]:
    if states is None:
        states = {}
    # Pre-populate with initial_state for any new group_ids, mirroring the
    # framework's behavior before update() is called.
    for gid in {g.as_py() for g in group_ids}:
        if gid not in states:
            states[gid] = NestTensorState()
    NestTensorFunction.update(states, group_ids, value, axes)
    return states


def _finalize_group(
    value_type: pa.DataType,
    axes_type: pa.StructType,
    states: dict[int, NestTensorState],
    gid: int = 0,
) -> dict[str, Any]:
    params = _process_params(value_type, axes_type)
    group_ids = pa.array([gid], type=pa.int64())
    batch = NestTensorFunction.finalize(group_ids, states, params)
    row: dict[str, Any] = batch.column("result")[0].as_py()
    return row


def _axes_struct(fields: dict[str, pa.Array[Any]]) -> pa.StructArray:
    names = list(fields.keys())
    arrays = [fields[n] for n in names]
    return pa.StructArray.from_arrays(arrays, names=names)


def _axes_type(fields: dict[str, pa.DataType]) -> pa.StructType:
    return pa.struct([pa.field(n, t) for n, t in fields.items()])


# ---------------------------------------------------------------------------
# Bind-time validation
# ---------------------------------------------------------------------------


class TestBind:
    def test_basic_bind(self) -> None:
        params = _bind_params(pa.int64(), _axes_type({"x": pa.int64()}))
        resp = NestTensorFunction.on_bind(params)
        out_type = resp.output_schema.field(0).type
        assert pa.types.is_struct(out_type)
        assert out_type.field("tensor").type == pa.list_(pa.int64())
        axes_out = out_type.field("axes").type
        assert axes_out.field("x").type == pa.list_(pa.int64())

    def test_multi_axis_bind(self) -> None:
        params = _bind_params(
            pa.float64(),
            _axes_type({"x": pa.int64(), "y": pa.string(), "z": pa.date32()}),
        )
        resp = NestTensorFunction.on_bind(params)
        out_type = resp.output_schema.field(0).type
        assert out_type.field("tensor").type == pa.list_(pa.list_(pa.list_(pa.float64())))

    def test_reject_non_struct_axes(self) -> None:
        schema = pa.schema([("value", pa.int64()), ("axes", pa.int64())])
        params = AggregateBindParams(args=None, input_schema=schema, settings={}, secrets=SecretsAccessor(None))
        with pytest.raises(NestTensorError, match="must be a struct"):
            NestTensorFunction.on_bind(params)

    def test_reject_empty_struct(self) -> None:
        params = _bind_params(pa.int64(), _axes_type({}))
        with pytest.raises(NestTensorError, match="at least one field"):
            NestTensorFunction.on_bind(params)

    def test_reject_float_coord(self) -> None:
        params = _bind_params(pa.int64(), _axes_type({"x": pa.float64()}))
        with pytest.raises(NestTensorError, match="floating-point"):
            NestTensorFunction.on_bind(params)

    def test_reject_nested_coord(self) -> None:
        params = _bind_params(pa.int64(), _axes_type({"x": pa.list_(pa.int64())}))
        with pytest.raises(NestTensorError, match="nested type"):
            NestTensorFunction.on_bind(params)

    def test_allow_float_value(self) -> None:
        # Floats are fine as *cell* types, just not axis types.
        params = _bind_params(pa.float64(), _axes_type({"x": pa.int64()}))
        resp = NestTensorFunction.on_bind(params)
        assert resp.output_schema.field(0).type.field("tensor").type == pa.list_(pa.float64())


# ---------------------------------------------------------------------------
# 1D, 2D, 3D happy paths
# ---------------------------------------------------------------------------


class TestHappyPath:
    def test_1d(self) -> None:
        value = pa.array([10, 20, 30], type=pa.int64())
        axes = _axes_struct({"i": pa.array([0, 1, 2], type=pa.int64())})
        group_ids = pa.array([0, 0, 0], type=pa.int64())
        states = _run_update(value, axes, group_ids)
        row = _finalize_group(pa.int64(), _axes_type({"i": pa.int64()}), states)
        assert row["tensor"] == [10, 20, 30]
        assert row["axes"] == {"i": [0, 1, 2]}

    def test_2d_dense(self) -> None:
        # 2x2 grid: (0,0)=1, (0,1)=2, (1,0)=3, (1,1)=4
        value = pa.array([1, 2, 3, 4], type=pa.int64())
        axes = _axes_struct(
            {
                "x": pa.array([0, 0, 1, 1], type=pa.int64()),
                "y": pa.array([0, 1, 0, 1], type=pa.int64()),
            }
        )
        group_ids = pa.array([0] * 4, type=pa.int64())
        states = _run_update(value, axes, group_ids)
        row = _finalize_group(pa.int64(), _axes_type({"x": pa.int64(), "y": pa.int64()}), states)
        assert row["tensor"] == [[1, 2], [3, 4]]
        assert row["axes"] == {"x": [0, 1], "y": [0, 1]}

    def test_2d_sparse(self) -> None:
        # Only (0,0) and (1,1) populated
        value = pa.array([10, 20], type=pa.int64())
        axes = _axes_struct(
            {
                "x": pa.array([0, 1], type=pa.int64()),
                "y": pa.array([0, 1], type=pa.int64()),
            }
        )
        states = _run_update(value, axes, pa.array([0, 0], type=pa.int64()))
        row = _finalize_group(pa.int64(), _axes_type({"x": pa.int64(), "y": pa.int64()}), states)
        assert row["tensor"] == [[10, None], [None, 20]]

    def test_3d_dense_fill_pattern(self) -> None:
        # Fill a 2x3x2 grid with value = 100*x + 10*y + z
        rows = []
        for x in range(2):
            for y in range(3):
                for z in range(2):
                    rows.append((100 * x + 10 * y + z, x, y, z))
        value = pa.array([r[0] for r in rows], type=pa.int64())
        axes = _axes_struct(
            {
                "x": pa.array([r[1] for r in rows], type=pa.int64()),
                "y": pa.array([r[2] for r in rows], type=pa.int64()),
                "z": pa.array([r[3] for r in rows], type=pa.int64()),
            }
        )
        states = _run_update(value, axes, pa.array([0] * len(rows), type=pa.int64()))
        row = _finalize_group(
            pa.int64(),
            _axes_type({"x": pa.int64(), "y": pa.int64(), "z": pa.int64()}),
            states,
        )
        assert row["axes"] == {"x": [0, 1], "y": [0, 1, 2], "z": [0, 1]}
        for x in range(2):
            for y in range(3):
                for z in range(2):
                    assert row["tensor"][x][y][z] == 100 * x + 10 * y + z

    def test_3d_sparse(self) -> None:
        value = pa.array([1, 2], type=pa.int64())
        axes = _axes_struct(
            {
                "x": pa.array([0, 1], type=pa.int64()),
                "y": pa.array([0, 1], type=pa.int64()),
                "z": pa.array([0, 1], type=pa.int64()),
            }
        )
        states = _run_update(value, axes, pa.array([0, 0], type=pa.int64()))
        row = _finalize_group(
            pa.int64(),
            _axes_type({"x": pa.int64(), "y": pa.int64(), "z": pa.int64()}),
            states,
        )
        # Shape 2x2x2; only (0,0,0)=1 and (1,1,1)=2 filled.
        assert row["tensor"][0][0][0] == 1
        assert row["tensor"][1][1][1] == 2
        # Spot-check one null cell
        assert row["tensor"][0][1][0] is None


# ---------------------------------------------------------------------------
# Ordering / determinism
# ---------------------------------------------------------------------------


class TestOrdering:
    def test_axis_coords_sorted_ascending(self) -> None:
        # Insert rows in reverse order; coords must come out ascending.
        value = pa.array([3, 2, 1], type=pa.int64())
        axes = _axes_struct({"i": pa.array([2, 1, 0], type=pa.int64())})
        states = _run_update(value, axes, pa.array([0, 0, 0], type=pa.int64()))
        row = _finalize_group(pa.int64(), _axes_type({"i": pa.int64()}), states)
        assert row["axes"]["i"] == [0, 1, 2]
        assert row["tensor"] == [1, 2, 3]

    def test_string_axis_sorted(self) -> None:
        value = pa.array([10, 20, 30], type=pa.int64())
        axes = _axes_struct({"k": pa.array(["c", "a", "b"], type=pa.string())})
        states = _run_update(value, axes, pa.array([0, 0, 0], type=pa.int64()))
        row = _finalize_group(pa.int64(), _axes_type({"k": pa.string()}), states)
        assert row["axes"]["k"] == ["a", "b", "c"]
        assert row["tensor"] == [20, 30, 10]


# ---------------------------------------------------------------------------
# Duplicates and null handling
# ---------------------------------------------------------------------------


class TestDuplicatesAndNulls:
    def test_intra_batch_duplicate_raises(self) -> None:
        value = pa.array([1, 2], type=pa.int64())
        axes = _axes_struct({"x": pa.array([0, 0], type=pa.int64())})
        with pytest.raises(NestTensorError, match="duplicate coordinate"):
            _run_update(value, axes, pa.array([0, 0], type=pa.int64()))

    def test_cross_partition_duplicate_raises_in_finalize(self) -> None:
        # Two update()s into the same gid, each valid on its own but sharing a coord.
        axes_type = _axes_type({"x": pa.int64()})
        states: dict[int, NestTensorState] = {}
        _run_update(
            pa.array([1], type=pa.int64()),
            _axes_struct({"x": pa.array([0], type=pa.int64())}),
            pa.array([0], type=pa.int64()),
            states=states,
        )
        _run_update(
            pa.array([2], type=pa.int64()),
            _axes_struct({"x": pa.array([0], type=pa.int64())}),
            pa.array([0], type=pa.int64()),
            states=states,
        )
        with pytest.raises(NestTensorError, match="arrived from parallel partitions"):
            _finalize_group(pa.int64(), axes_type, states)

    def test_null_axes_row_skipped(self) -> None:
        value = pa.array([1, 2], type=pa.int64())
        axes = pa.StructArray.from_arrays(
            [pa.array([0, 99], type=pa.int64())],
            names=["x"],
            mask=pa.array([False, True]),  # second row has null struct
        )
        states = _run_update(value, axes, pa.array([0, 0], type=pa.int64()))
        row = _finalize_group(pa.int64(), _axes_type({"x": pa.int64()}), states)
        assert row["tensor"] == [1]
        assert row["axes"] == {"x": [0]}

    def test_null_coord_field_raises(self) -> None:
        value = pa.array([1, 2], type=pa.int64())
        axes = _axes_struct({"x": pa.array([0, None], type=pa.int64())})
        with pytest.raises(NestTensorError, match="null coord value"):
            _run_update(value, axes, pa.array([0, 0], type=pa.int64()))

    def test_null_value_stored(self) -> None:
        value = pa.array([None, 5], type=pa.int64())
        axes = _axes_struct({"x": pa.array([0, 1], type=pa.int64())})
        states = _run_update(value, axes, pa.array([0, 0], type=pa.int64()))
        row = _finalize_group(pa.int64(), _axes_type({"x": pa.int64()}), states)
        assert row["tensor"] == [None, 5]


# ---------------------------------------------------------------------------
# Combine + grouping
# ---------------------------------------------------------------------------


class TestCombineAndGrouping:
    def test_combine_concatenates_rows(self) -> None:
        axes_type = _axes_type({"x": pa.int64()})
        states_a: dict[int, NestTensorState] = {}
        _run_update(
            pa.array([1, 2], type=pa.int64()),
            _axes_struct({"x": pa.array([0, 1], type=pa.int64())}),
            pa.array([0, 0], type=pa.int64()),
            states=states_a,
        )
        states_b: dict[int, NestTensorState] = {}
        _run_update(
            pa.array([3], type=pa.int64()),
            _axes_struct({"x": pa.array([2], type=pa.int64())}),
            pa.array([0], type=pa.int64()),
            states=states_b,
        )
        params = _process_params(pa.int64(), axes_type)
        merged = NestTensorFunction.combine(states_a[0], states_b[0], params)
        row = _finalize_group(pa.int64(), axes_type, {0: merged})
        assert row["tensor"] == [1, 2, 3]

    def test_multiple_groups(self) -> None:
        value = pa.array([1, 2, 3, 4], type=pa.int64())
        axes = _axes_struct({"x": pa.array([0, 0, 1, 1], type=pa.int64())})
        group_ids = pa.array([0, 1, 0, 1], type=pa.int64())
        states = _run_update(value, axes, group_ids)
        row0 = _finalize_group(pa.int64(), _axes_type({"x": pa.int64()}), states, gid=0)
        row1 = _finalize_group(pa.int64(), _axes_type({"x": pa.int64()}), states, gid=1)
        assert row0["tensor"] == [1, 3]
        assert row1["tensor"] == [2, 4]

    def test_parallel_determinism(self) -> None:
        # Simulate two different partition orderings and check merged output is identical.
        axes_type = _axes_type({"x": pa.int64()})

        def build(coords_a: list[int], coords_b: list[int]) -> dict[str, Any]:
            states_a: dict[int, NestTensorState] = {}
            _run_update(
                pa.array(list(range(len(coords_a))), type=pa.int64()),
                _axes_struct({"x": pa.array(coords_a, type=pa.int64())}),
                pa.array([0] * len(coords_a), type=pa.int64()),
                states=states_a,
            )
            states_b: dict[int, NestTensorState] = {}
            _run_update(
                pa.array([100 + i for i in range(len(coords_b))], type=pa.int64()),
                _axes_struct({"x": pa.array(coords_b, type=pa.int64())}),
                pa.array([0] * len(coords_b), type=pa.int64()),
                states=states_b,
            )
            params = _process_params(pa.int64(), axes_type)
            merged = NestTensorFunction.combine(states_a[0], states_b[0], params)
            return _finalize_group(pa.int64(), axes_type, {0: merged})

        r1 = build([3, 1], [2, 4])
        r2 = build([4, 2], [1, 3])
        assert r1["axes"] == r2["axes"] == {"x": [1, 2, 3, 4]}


# ---------------------------------------------------------------------------
# Memory guard
# ---------------------------------------------------------------------------


class TestMemoryGuard:
    def test_over_limit_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("VGI_NEST_TENSOR_MAX_CELLS", "3")
        value = pa.array([1, 2, 3, 4], type=pa.int64())
        axes = _axes_struct({"x": pa.array([0, 1, 2, 3], type=pa.int64())})
        states = _run_update(value, axes, pa.array([0] * 4, type=pa.int64()))
        with pytest.raises(NestTensorError, match="exceeds VGI_NEST_TENSOR_MAX_CELLS"):
            _finalize_group(pa.int64(), _axes_type({"x": pa.int64()}), states)


# ---------------------------------------------------------------------------
# Unnest tensor
# ---------------------------------------------------------------------------


def _unnest_bind(struct_scalar: pa.StructScalar) -> pa.DataType:
    schema = pa.schema([("t", struct_scalar.type)])
    params = BindParameters(
        constant_arguments=Arguments(),
        arguments_schema=schema,
        settings=None,
        secrets=SecretsAccessor(None),
    )
    return UnnestTensorFunction.on_bind(params).output_type


def _unnest_run(struct_scalar: pa.StructScalar) -> list[dict[str, Any]]:
    """Invoke unnest_tensor scalar and return the flat list of rows."""
    array = pa.array([struct_scalar], type=struct_scalar.type)
    result = UnnestTensorFunction.compute(array)
    rows: list[dict[str, Any]] = result[0].as_py()
    return rows


def _make_nest_result(
    value: pa.Array[Any],
    axes_fields: dict[str, pa.Array[Any]],
    axes_type_map: dict[str, pa.DataType],
) -> pa.StructScalar:
    axes = _axes_struct(axes_fields)
    group_ids = pa.array([0] * len(value), type=pa.int64())
    states = _run_update(value, axes, group_ids)
    params = _process_params(value.type, _axes_type(axes_type_map))
    batch = NestTensorFunction.finalize(pa.array([0], type=pa.int64()), states, params)
    scalar: pa.StructScalar = batch.column("result")[0]
    return scalar


class TestUnnest:
    def test_roundtrip_1d_dense(self) -> None:
        nested = _make_nest_result(
            pa.array([10, 20, 30], type=pa.int64()),
            {"i": pa.array([0, 1, 2], type=pa.int64())},
            {"i": pa.int64()},
        )
        _unnest_bind(nested)  # validates shape
        rows = _unnest_run(nested)
        pairs = sorted((row["value"], row["axes"]["i"]) for row in rows)
        assert pairs == [(10, 0), (20, 1), (30, 2)]

    def test_roundtrip_2d_dense(self) -> None:
        nested = _make_nest_result(
            pa.array([1, 2, 3, 4], type=pa.int64()),
            {
                "x": pa.array([0, 0, 1, 1], type=pa.int64()),
                "y": pa.array([0, 1, 0, 1], type=pa.int64()),
            },
            {"x": pa.int64(), "y": pa.int64()},
        )
        _unnest_bind(nested)
        rows = _unnest_run(nested)
        triples = sorted((row["value"], row["axes"]["x"], row["axes"]["y"]) for row in rows)
        assert triples == [(1, 0, 0), (2, 0, 1), (3, 1, 0), (4, 1, 1)]

    def test_sparse_emits_nulls(self) -> None:
        nested = _make_nest_result(
            pa.array([1, 2], type=pa.int64()),
            {
                "x": pa.array([0, 1], type=pa.int64()),
                "y": pa.array([0, 1], type=pa.int64()),
            },
            {"x": pa.int64(), "y": pa.int64()},
        )
        rows = _unnest_run(nested)
        # 2x2 Cartesian product: 4 rows; 2 non-null, 2 null.
        assert len(rows) == 4
        values = [row["value"] for row in rows]
        assert sorted(v for v in values if v is not None) == [1, 2]
        assert values.count(None) == 2

    def test_malformed_input_missing_field(self) -> None:
        bad = pa.scalar({"tensor": [1, 2, 3]}, type=pa.struct([("tensor", pa.list_(pa.int64()))]))
        with pytest.raises(NestTensorError, match="'tensor' and 'axes'"):
            _unnest_bind(bad)

    def test_malformed_input_wrong_depth(self) -> None:
        bad_type = pa.struct(
            [
                ("tensor", pa.list_(pa.int64())),  # depth 1
                ("axes", pa.struct([("x", pa.list_(pa.int64())), ("y", pa.list_(pa.int64()))])),
            ]
        )
        bad = pa.scalar({"tensor": [1, 2], "axes": {"x": [0, 1], "y": [0, 1]}}, type=bad_type)
        with pytest.raises(NestTensorError, match="nesting depth"):
            _unnest_bind(bad)
