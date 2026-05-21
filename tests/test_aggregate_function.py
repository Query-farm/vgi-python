# Copyright 2025, 2026 Query Farm LLC - https://query.farm

# ruff: noqa: D101, D102, D106
"""Tests for vgi.aggregate_function module."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Any

import pyarrow as pa
import pytest
from vgi_rpc import ArrowSerializableDataclass, ArrowType

from vgi.aggregate_function import AggregateFunction
from vgi.arguments import ConstParam, Param, Returns
from vgi.table_function import ProcessParams

# ---------------------------------------------------------------------------
# Shared state classes for test functions
# ---------------------------------------------------------------------------


@dataclass(kw_only=True)
class SimpleState(ArrowSerializableDataclass):
    value: Annotated[int, ArrowType(pa.int64())] = 0


@dataclass(kw_only=True)
class TwoFieldState(ArrowSerializableDataclass):
    total: Annotated[float, ArrowType(pa.float64())] = 0.0
    count: Annotated[int, ArrowType(pa.int64())] = 0


# =========================================================================
# Test Group 1: __init_subclass__ parameter extraction
# =========================================================================


class TestInitSubclassParameterExtraction:
    """Test AggregateFunction.__init_subclass__ parameter parsing."""

    def test_nullary_function_no_compute_params(self) -> None:
        """Nullary aggregate (like CountFunction): no Param on update."""

        class NullaryAgg(AggregateFunction[SimpleState]):
            class Meta:
                name = "test_nullary"

            @classmethod
            def initial_state(cls, params: ProcessParams[Any]) -> SimpleState:
                return SimpleState()

            @classmethod
            def update(
                cls,
                states: dict[int, SimpleState],
                group_ids: pa.Int64Array,
            ) -> None:
                pass

            @classmethod
            def combine(cls, source: SimpleState, target: SimpleState, params: ProcessParams[Any]) -> SimpleState:
                return SimpleState()

            @classmethod
            def finalize(
                cls,
                group_ids: pa.Int64Array,
                states: dict[int, SimpleState],
                params: ProcessParams[Any],
            ) -> Annotated[pa.RecordBatch, Returns(pa.int64())]:
                return pa.record_batch({"result": pa.array([], type=pa.int64())})

        assert NullaryAgg._compute_params == {}

    def test_single_param(self) -> None:
        """Single Param annotation extracts correct position, arrow_type, name."""

        class SingleParamAgg(AggregateFunction[SimpleState]):
            class Meta:
                name = "test_single_param"

            @classmethod
            def initial_state(cls, params: ProcessParams[Any]) -> SimpleState:
                return SimpleState()

            @classmethod
            def update(
                cls,
                states: dict[int, SimpleState],
                group_ids: pa.Int64Array,
                value: Annotated[pa.Int64Array, Param(doc="Column to sum")],
            ) -> None:
                pass

            @classmethod
            def combine(cls, source: SimpleState, target: SimpleState, params: ProcessParams[Any]) -> SimpleState:
                return SimpleState()

            @classmethod
            def finalize(
                cls,
                group_ids: pa.Int64Array,
                states: dict[int, SimpleState],
                params: ProcessParams[Any],
            ) -> Annotated[pa.RecordBatch, Returns(pa.int64())]:
                return pa.record_batch({"result": pa.array([], type=pa.int64())})

        assert len(SingleParamAgg._compute_params) == 1
        assert "value" in SingleParamAgg._compute_params
        arg = SingleParamAgg._compute_params["value"]
        assert arg._name == "value"
        assert arg.arrow_type == pa.int64()
        assert arg.position == 0

    def test_multiple_params(self) -> None:
        """Two Param annotations extract correct positions and types."""

        class MultiParamAgg(AggregateFunction[SimpleState]):
            class Meta:
                name = "test_multi_param"

            @classmethod
            def initial_state(cls, params: ProcessParams[Any]) -> SimpleState:
                return SimpleState()

            @classmethod
            def update(
                cls,
                states: dict[int, SimpleState],
                group_ids: pa.Int64Array,
                value: Annotated[pa.DoubleArray, Param(doc="Values")],
                weight: Annotated[pa.DoubleArray, Param(doc="Weights")],
            ) -> None:
                pass

            @classmethod
            def combine(cls, source: SimpleState, target: SimpleState, params: ProcessParams[Any]) -> SimpleState:
                return SimpleState()

            @classmethod
            def finalize(
                cls,
                group_ids: pa.Int64Array,
                states: dict[int, SimpleState],
                params: ProcessParams[Any],
            ) -> Annotated[pa.RecordBatch, Returns(pa.float64())]:
                return pa.record_batch({"result": pa.array([], type=pa.float64())})

        assert len(MultiParamAgg._compute_params) == 2
        assert "value" in MultiParamAgg._compute_params
        assert "weight" in MultiParamAgg._compute_params
        value_arg = MultiParamAgg._compute_params["value"]
        weight_arg = MultiParamAgg._compute_params["weight"]
        assert value_arg.arrow_type == pa.float64()
        assert weight_arg.arrow_type == pa.float64()
        assert value_arg.position == 0
        assert weight_arg.position == 1

    def test_const_param(self) -> None:
        """ConstParam annotation appears in _const_params with correct position and type."""

        class ConstParamAgg(AggregateFunction[SimpleState]):
            class Meta:
                name = "test_const_param"

            @classmethod
            def initial_state(cls, params: ProcessParams[Any]) -> SimpleState:
                return SimpleState()

            @classmethod
            def update(
                cls,
                states: dict[int, SimpleState],
                group_ids: pa.Int64Array,
                value: Annotated[pa.DoubleArray, Param(doc="Values")],
                percentile: Annotated[float, ConstParam("Percentile (0-1)")] = 0.5,
            ) -> None:
                pass

            @classmethod
            def combine(cls, source: SimpleState, target: SimpleState, params: ProcessParams[Any]) -> SimpleState:
                return SimpleState()

            @classmethod
            def finalize(
                cls,
                group_ids: pa.Int64Array,
                states: dict[int, SimpleState],
                params: ProcessParams[Any],
            ) -> Annotated[pa.RecordBatch, Returns(pa.float64())]:
                return pa.record_batch({"result": pa.array([], type=pa.float64())})

        assert len(ConstParamAgg._const_params) == 1
        assert "percentile" in ConstParamAgg._const_params
        arg = ConstParamAgg._const_params["percentile"]
        assert arg._name == "percentile"
        assert arg.arrow_type == pa.float64()
        assert arg.const is True

    def test_const_param_phases(self) -> None:
        """ConstParam phase values are stored correctly."""

        class PhaseAgg(AggregateFunction[SimpleState]):
            class Meta:
                name = "test_phases"

            @classmethod
            def initial_state(cls, params: ProcessParams[Any]) -> SimpleState:
                return SimpleState()

            @classmethod
            def update(
                cls,
                states: dict[int, SimpleState],
                group_ids: pa.Int64Array,
                value: Annotated[pa.Int64Array, Param(doc="Value")],
                update_only: Annotated[int, ConstParam("Update only", phase="update")] = 0,
                finalize_only: Annotated[int, ConstParam("Finalize only", phase="finalize")] = 0,
                all_phases: Annotated[int, ConstParam("All phases", phase="all")] = 0,
            ) -> None:
                pass

            @classmethod
            def combine(cls, source: SimpleState, target: SimpleState, params: ProcessParams[Any]) -> SimpleState:
                return SimpleState()

            @classmethod
            def finalize(
                cls,
                group_ids: pa.Int64Array,
                states: dict[int, SimpleState],
                params: ProcessParams[Any],
            ) -> Annotated[pa.RecordBatch, Returns(pa.int64())]:
                return pa.record_batch({"result": pa.array([], type=pa.int64())})

        assert PhaseAgg._const_param_phases["update_only"] == "update"
        assert PhaseAgg._const_param_phases["finalize_only"] == "finalize"
        assert PhaseAgg._const_param_phases["all_phases"] == "all"

    def test_varargs_param(self) -> None:
        """Param(varargs=True) sets varargs on the Arg."""

        class VarargsAgg(AggregateFunction[SimpleState]):
            class Meta:
                name = "test_varargs"

            @classmethod
            def initial_state(cls, params: ProcessParams[Any]) -> SimpleState:
                return SimpleState()

            @classmethod
            def update(
                cls,
                states: dict[int, SimpleState],
                group_ids: pa.Int64Array,
                columns: Annotated[pa.Array, Param(doc="Columns", varargs=True)],  # type: ignore[type-arg]
            ) -> None:
                pass

            @classmethod
            def combine(cls, source: SimpleState, target: SimpleState, params: ProcessParams[Any]) -> SimpleState:
                return SimpleState()

            @classmethod
            def finalize(
                cls,
                group_ids: pa.Int64Array,
                states: dict[int, SimpleState],
                params: ProcessParams[Any],
            ) -> Annotated[pa.RecordBatch, Returns(pa.float64())]:
                return pa.record_batch({"result": pa.array([], type=pa.float64())})

        assert len(VarargsAgg._compute_params) == 1
        arg = VarargsAgg._compute_params["columns"]
        assert arg.varargs is True

    def test_returns_annotation(self) -> None:
        """Returns(pa.int64()) on finalize sets _returns_output_type."""

        class ReturnsAgg(AggregateFunction[SimpleState]):
            class Meta:
                name = "test_returns"

            @classmethod
            def initial_state(cls, params: ProcessParams[Any]) -> SimpleState:
                return SimpleState()

            @classmethod
            def update(
                cls,
                states: dict[int, SimpleState],
                group_ids: pa.Int64Array,
            ) -> None:
                pass

            @classmethod
            def combine(cls, source: SimpleState, target: SimpleState, params: ProcessParams[Any]) -> SimpleState:
                return SimpleState()

            @classmethod
            def finalize(
                cls,
                group_ids: pa.Int64Array,
                states: dict[int, SimpleState],
                params: ProcessParams[Any],
            ) -> Annotated[pa.RecordBatch, Returns(pa.int64())]:
                return pa.record_batch({"result": pa.array([], type=pa.int64())})

        assert ReturnsAgg._returns_output_type == pa.int64()

    def test_dynamic_returns(self) -> None:
        """Returns() with no type sets _returns_output_type to None."""

        class DynamicReturnsAgg(AggregateFunction[SimpleState]):
            class Meta:
                name = "test_dynamic_returns"

            @classmethod
            def initial_state(cls, params: ProcessParams[Any]) -> SimpleState:
                return SimpleState()

            @classmethod
            def update(
                cls,
                states: dict[int, SimpleState],
                group_ids: pa.Int64Array,
            ) -> None:
                pass

            @classmethod
            def combine(cls, source: SimpleState, target: SimpleState, params: ProcessParams[Any]) -> SimpleState:
                return SimpleState()

            @classmethod
            def finalize(
                cls,
                group_ids: pa.Int64Array,
                states: dict[int, SimpleState],
                params: ProcessParams[Any],
            ) -> Annotated[pa.RecordBatch, Returns()]:
                return pa.record_batch({"result": pa.array([], type=pa.float64())})

        assert DynamicReturnsAgg._returns_output_type is None

    def test_state_class_extraction(self) -> None:
        """Generic type parameter extracts state_class correctly."""

        class StateExtractAgg(AggregateFunction[TwoFieldState]):
            class Meta:
                name = "test_state_extract"

            @classmethod
            def initial_state(cls, params: ProcessParams[Any]) -> TwoFieldState:
                return TwoFieldState()

            @classmethod
            def update(
                cls,
                states: dict[int, TwoFieldState],
                group_ids: pa.Int64Array,
            ) -> None:
                pass

            @classmethod
            def combine(cls, source: TwoFieldState, target: TwoFieldState, params: ProcessParams[Any]) -> TwoFieldState:
                return TwoFieldState()

            @classmethod
            def finalize(
                cls,
                group_ids: pa.Int64Array,
                states: dict[int, TwoFieldState],
                params: ProcessParams[Any],
            ) -> Annotated[pa.RecordBatch, Returns(pa.float64())]:
                return pa.record_batch({"result": pa.array([], type=pa.float64())})

        assert StateExtractAgg.state_class is TwoFieldState  # type: ignore[misc]


# =========================================================================
# Test Group 3: Error paths
# =========================================================================


class TestErrorPaths:
    """Tests for aggregate function error conditions."""

    def test_wrong_function_type_in_resolve(self) -> None:
        """_resolve_function_by_name with wrong function_type raises ValueError."""
        from vgi.scalar_function import ScalarFunction
        from vgi.worker import Worker

        class MyScalar(ScalarFunction):
            class Meta:
                name = "my_scalar_for_agg_test"

            @classmethod
            def compute(
                cls,
                value: Annotated[pa.Int64Array, Param(doc="Value")],
            ) -> Annotated[pa.Int64Array, Returns(pa.int64())]:
                return value

        class TestWorker(Worker):
            functions = [MyScalar]

        worker = TestWorker(quiet=True)

        with pytest.raises(ValueError, match="No AggregateFunction named"):
            worker._resolve_function_by_name("my_scalar_for_agg_test", function_type=AggregateFunction)

    def test_missing_state_class(self) -> None:
        """AggregateFunction subclass without generic type parameter has state_class = None."""

        class NoGenericAgg(AggregateFunction):  # type: ignore[type-arg]
            class Meta:
                name = "test_no_generic"

            @classmethod
            def initial_state(cls, params: ProcessParams[Any]) -> SimpleState:
                return SimpleState()

            @classmethod
            def update(
                cls,
                states: dict[int, SimpleState],
                group_ids: pa.Int64Array,
            ) -> None:
                pass

            @classmethod
            def combine(cls, source: Any, target: Any, params: ProcessParams[Any]) -> Any:
                return target

            @classmethod
            def finalize(
                cls,
                group_ids: pa.Int64Array,
                states: dict[int, Any],
                params: ProcessParams[Any],
            ) -> Annotated[pa.RecordBatch, Returns(pa.int64())]:
                return pa.record_batch({"result": pa.array([], type=pa.int64())})

        assert NoGenericAgg.state_class is None  # type: ignore[misc]

    def test_on_bind_without_returns_raises(self) -> None:
        """AggregateFunction with no Returns and no on_bind override raises NotImplementedError."""
        from vgi.aggregate_function import AggregateBindParams
        from vgi.table_function import SecretsAccessor

        class NoReturnsAgg(AggregateFunction[SimpleState]):
            class Meta:
                name = "test_no_returns"

            @classmethod
            def initial_state(cls, params: ProcessParams[Any]) -> SimpleState:
                return SimpleState()

            @classmethod
            def update(
                cls,
                states: dict[int, SimpleState],
                group_ids: pa.Int64Array,
            ) -> None:
                pass

            @classmethod
            def combine(cls, source: SimpleState, target: SimpleState, params: ProcessParams[Any]) -> SimpleState:
                return SimpleState()

            @classmethod
            def finalize(
                cls,
                group_ids: pa.Int64Array,
                states: dict[int, SimpleState],
                params: ProcessParams[Any],
            ) -> pa.RecordBatch:
                return pa.record_batch({"result": pa.array([], type=pa.int64())})

        bind_params = AggregateBindParams(
            args=None,
            input_schema=None,
            settings={},
            secrets=SecretsAccessor(None),
        )
        with pytest.raises(NotImplementedError, match="must either implement on_bind"):
            NoReturnsAgg.on_bind(bind_params)

    def test_catalog_output_schema_with_returns(self) -> None:
        """catalog_output_schema returns correct schema when Returns is set."""

        class CatalogReturnsAgg(AggregateFunction[SimpleState]):
            class Meta:
                name = "test_catalog_returns"

            @classmethod
            def initial_state(cls, params: ProcessParams[Any]) -> SimpleState:
                return SimpleState()

            @classmethod
            def update(
                cls,
                states: dict[int, SimpleState],
                group_ids: pa.Int64Array,
            ) -> None:
                pass

            @classmethod
            def combine(cls, source: SimpleState, target: SimpleState, params: ProcessParams[Any]) -> SimpleState:
                return SimpleState()

            @classmethod
            def finalize(
                cls,
                group_ids: pa.Int64Array,
                states: dict[int, SimpleState],
                params: ProcessParams[Any],
            ) -> Annotated[pa.RecordBatch, Returns(pa.string())]:
                return pa.record_batch({"result": pa.array([], type=pa.string())})

        schema = CatalogReturnsAgg.catalog_output_schema()
        assert schema == pa.schema([pa.field("result", pa.string())])

    def test_catalog_output_schema_without_returns(self) -> None:
        """catalog_output_schema returns vgi:any metadata when Returns has no type."""

        class CatalogDynamicAgg(AggregateFunction[SimpleState]):
            class Meta:
                name = "test_catalog_dynamic"

            @classmethod
            def initial_state(cls, params: ProcessParams[Any]) -> SimpleState:
                return SimpleState()

            @classmethod
            def update(
                cls,
                states: dict[int, SimpleState],
                group_ids: pa.Int64Array,
            ) -> None:
                pass

            @classmethod
            def combine(cls, source: SimpleState, target: SimpleState, params: ProcessParams[Any]) -> SimpleState:
                return SimpleState()

            @classmethod
            def finalize(
                cls,
                group_ids: pa.Int64Array,
                states: dict[int, SimpleState],
                params: ProcessParams[Any],
            ) -> Annotated[pa.RecordBatch, Returns()]:
                return pa.record_batch({"result": pa.array([], type=pa.float64())})

        schema = CatalogDynamicAgg.catalog_output_schema()
        assert schema.field(0).name == "result"
        assert schema.field(0).type == pa.null()
        assert schema.field(0).metadata == {b"vgi:any": b"true"}


# =========================================================================
# Test Group 4: NULL semantics
# =========================================================================


class TestNullSemantics:
    """Tests for state serialization determinism and finalize NULL handling."""

    def test_unmodified_initial_state_serialization_deterministic(self) -> None:
        """Serializing the same initial state twice produces identical bytes."""
        state1 = SimpleState()
        state2 = SimpleState()
        bytes1 = state1.serialize_to_bytes()
        bytes2 = state2.serialize_to_bytes()
        assert bytes1 == bytes2

    def test_modified_state_differs_from_initial(self) -> None:
        """Modified state serializes to different bytes than initial state."""
        initial = SimpleState()
        modified = SimpleState(value=42)
        assert initial.serialize_to_bytes() != modified.serialize_to_bytes()

    def test_sum_finalize_with_none_state(self) -> None:
        """SumFunction.finalize with None state produces NULL."""
        from vgi._test_fixtures.aggregate import SumFunction

        group_ids = pa.array([0], type=pa.int64())
        states: dict[int, Any] = {0: None}
        # We need a minimal ProcessParams; finalize only uses group_ids and states
        result = SumFunction.finalize(group_ids, states, _make_dummy_params(pa.int64()))
        assert result.column("result")[0].as_py() is None

    def test_count_finalize_with_none_state(self) -> None:
        """CountFunction.finalize with None state produces 0."""
        from vgi._test_fixtures.aggregate import CountFunction

        group_ids = pa.array([0], type=pa.int64())
        states: dict[int, Any] = {0: None}
        result = CountFunction.finalize(group_ids, states, _make_dummy_params(pa.int64()))
        assert result.column("result")[0].as_py() == 0

    def test_sum_finalize_with_real_state(self) -> None:
        """SumFunction.finalize with real state produces correct value."""
        from vgi._test_fixtures.aggregate import SumFunction, SumState

        group_ids = pa.array([0], type=pa.int64())
        states: dict[int, SumState] = {0: SumState(total=42)}
        result = SumFunction.finalize(group_ids, states, _make_dummy_params(pa.int64()))
        assert result.column("result")[0].as_py() == 42


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_dummy_params(output_type: pa.DataType) -> ProcessParams[None]:
    """Create a minimal ProcessParams for direct finalize() calls."""
    from vgi.function_storage import FunctionStorageSqlite

    storage = FunctionStorageSqlite(":memory:")

    from vgi.function_storage import BoundStorage

    return ProcessParams(
        args=None,
        init_call=None,
        init_response=None,
        output_schema=pa.schema([("result", output_type)]),
        settings={},
        secrets={},
        storage=BoundStorage(storage, b"dummy"),
    )


# =========================================================================
# Test Group 5: window_batch hook
# =========================================================================


def _make_window_partition(values: list[int | None]) -> Any:
    """Build a single-column int64 WindowPartition for batch-window tests."""
    from vgi.aggregate_function import WindowPartition

    arr = pa.array(values, type=pa.int64())
    batch = pa.record_batch({"value": arr})
    return WindowPartition(
        inputs=batch,
        row_count=len(values),
        filter_mask=None,
        frame_stats=((0, 0), (0, 0)),
        all_valid=[True],
    )


class TestWindowBatchDefault:
    """Default ``window_batch`` should call ``window`` once per row."""

    def test_dispatches_to_window_per_row(self) -> None:
        from vgi._test_fixtures.aggregate import WindowSumFunction

        partition = _make_window_partition([1, 2, 3, 4, 5])
        result = WindowSumFunction.window_batch(
            row_ids=[0, 1, 2, 3, 4],
            subframes=[
                [(0, 1)],
                [(0, 2)],
                [(0, 3)],
                [(0, 4)],
                [(0, 5)],
            ],
            partition=partition,
            window_state=None,
            params=_make_dummy_params(pa.int64()),
        )
        # Default returns list[Any] — same shape as calling window() per row.
        assert isinstance(result, list)
        assert result == [1, 3, 6, 10, 15]

    def test_default_matches_window_per_row(self) -> None:
        """Per-row window() and default window_batch() return the same values."""
        from vgi._test_fixtures.aggregate import WindowSumFunction

        partition = _make_window_partition([10, 20, 30])
        params = _make_dummy_params(pa.int64())

        per_row = [WindowSumFunction.window(rid, [(0, rid + 1)], partition, None, params) for rid in range(3)]
        batched = WindowSumFunction.window_batch(
            row_ids=[0, 1, 2],
            subframes=[[(0, 1)], [(0, 2)], [(0, 3)]],
            partition=partition,
            window_state=None,
            params=params,
        )
        assert per_row == batched


class TestWindowBatchOverride:
    """Overridden ``window_batch`` may return a pa.Array directly."""

    def test_returns_pa_array_when_overridden(self) -> None:
        from vgi._test_fixtures.aggregate import WindowSumBatchFunction

        partition = _make_window_partition([1, 2, 3, 4, 5])
        result = WindowSumBatchFunction.window_batch(
            row_ids=[0, 1, 2, 3, 4],
            subframes=[
                [(0, 1)],
                [(0, 2)],
                [(0, 3)],
                [(0, 4)],
                [(0, 5)],
            ],
            partition=partition,
            window_state=None,
            params=_make_dummy_params(pa.int64()),
        )
        assert isinstance(result, pa.Array)
        assert result.type == pa.int64()
        assert result.to_pylist() == [1, 3, 6, 10, 15]

    def test_override_matches_default(self) -> None:
        """The overriding fixture computes the same answers as the default path."""
        from vgi._test_fixtures.aggregate import (
            WindowSumBatchFunction,
            WindowSumFunction,
        )

        partition = _make_window_partition([1, 2, 3, 4, 5])
        params = _make_dummy_params(pa.int64())
        subframes = [[(0, i + 1)] for i in range(5)]

        default_path = WindowSumFunction.window_batch(
            row_ids=[0, 1, 2, 3, 4],
            subframes=subframes,
            partition=partition,
            window_state=None,
            params=params,
        )
        override_path = WindowSumBatchFunction.window_batch(
            row_ids=[0, 1, 2, 3, 4],
            subframes=subframes,
            partition=partition,
            window_state=None,
            params=params,
        )
        assert default_path == override_path.to_pylist()


class TestBuildBatchResult:
    """``_build_batch_result`` accepts both list and pa.Array, validates shape."""

    def test_list_input_packs_via_pa_array(self) -> None:
        from vgi.worker import _build_batch_result

        schema = pa.schema([("result", pa.int64())])
        batch = _build_batch_result([1, 2, 3], schema, expected_count=3)
        assert batch.column("result").to_pylist() == [1, 2, 3]

    def test_pa_array_input_shipped_directly(self) -> None:
        from vgi.worker import _build_batch_result

        schema = pa.schema([("result", pa.int64())])
        arr = pa.array([10, 20, 30], type=pa.int64())
        batch = _build_batch_result(arr, schema, expected_count=3)
        # The returned column is the same Arrow array we passed in.
        assert batch.column("result").to_pylist() == [10, 20, 30]

    def test_pa_array_wrong_type_raises(self) -> None:
        from vgi.worker import _build_batch_result

        schema = pa.schema([("result", pa.int64())])
        arr = pa.array([1.0, 2.0, 3.0], type=pa.float64())
        with pytest.raises(TypeError, match="expected int64"):
            _build_batch_result(arr, schema, expected_count=3)

    def test_count_mismatch_raises(self) -> None:
        from vgi.worker import _build_batch_result

        schema = pa.schema([("result", pa.int64())])
        with pytest.raises(ValueError, match="expected 5"):
            _build_batch_result([1, 2, 3], schema, expected_count=5)
