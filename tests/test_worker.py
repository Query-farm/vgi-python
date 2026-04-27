"""Tests for vgi.worker module, including function overloading and cardinality."""

from dataclasses import dataclass
from typing import Annotated

import pyarrow as pa
import pytest
from vgi_rpc.rpc import AuthContext, CallContext, OutputCollector
from vgi_rpc.utils import deserialize_record_batch

from vgi import Arg, TableInOutFunction, TableInput
from vgi.arguments import Arguments, ConstParam, Param, Returns
from vgi.catalog.catalog_interface import ColumnStatistics
from vgi.invocation import FunctionType
from vgi.protocol import (
    BindRequest,
    TableFunctionCardinalityRequest,
    TableFunctionStatisticsRequest,
)
from vgi.scalar_function import ScalarFunction
from vgi.table_function import (
    BindParams,
    ProcessParams,
    TableCardinality,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)
from vgi.worker import Worker


class TestFunctionOverloading:
    """Tests for function overloading based on argument signatures."""

    def test_single_candidate_always_matches(self) -> None:
        """With only one candidate, it's always selected."""

        class SingleFunction(TableInOutFunction):  # type: ignore[type-arg]
            """Single function."""

            class Meta:
                name = "single"

            data: TableInput = Arg[TableInput](0, doc="Input")  # type: ignore[assignment]

        result = Worker._match_function_arguments(
            function_name="single",
            arguments=Arguments(),
            input_schema=pa.schema([]),
            candidates=[SingleFunction],
        )
        assert result is SingleFunction

    def test_match_by_positional_count(self) -> None:
        """Match function by number of positional arguments."""

        class NoArgsFunc(TableInOutFunction):  # type: ignore[type-arg]
            """No args."""

            class Meta:
                name = "func"

            data: TableInput = Arg[TableInput](0, doc="Input")  # type: ignore[assignment]

        class OneArgFunc(TableInOutFunction):  # type: ignore[type-arg]
            """One arg."""

            class Meta:
                name = "func"

            count = Arg[int](0, doc="Count")
            data: TableInput = Arg[TableInput](1, doc="Input")  # type: ignore[assignment]

        class TwoArgsFunc(TableInOutFunction):  # type: ignore[type-arg]
            """Two args."""

            class Meta:
                name = "func"

            count = Arg[int](0, doc="Count")
            multiplier = Arg[int](1, doc="Multiplier")
            data: TableInput = Arg[TableInput](2, doc="Input")  # type: ignore[assignment]

        candidates = [NoArgsFunc, OneArgFunc, TwoArgsFunc]

        # No arguments -> NoArgsFunc
        assert (
            Worker._match_function_arguments(
                function_name="func",
                arguments=Arguments(positional=()),
                input_schema=pa.schema([]),
                candidates=candidates,
            )
            is NoArgsFunc
        )

        # One argument -> OneArgFunc
        assert (
            Worker._match_function_arguments(
                function_name="func",
                arguments=Arguments(positional=(pa.scalar(5),)),
                input_schema=pa.schema([]),
                candidates=candidates,
            )
            is OneArgFunc
        )

        # Two arguments -> TwoArgsFunc
        assert (
            Worker._match_function_arguments(
                function_name="func",
                arguments=Arguments(positional=(pa.scalar(5), pa.scalar(10))),
                input_schema=pa.schema([]),
                candidates=candidates,
            )
            is TwoArgsFunc
        )

    def test_match_with_optional_args(self) -> None:
        """Match considers optional arguments with defaults."""

        class RequiredFunc(TableInOutFunction):  # type: ignore[type-arg]
            """Required arg."""

            class Meta:
                name = "func"

            count = Arg[int](0, doc="Count")
            data: TableInput = Arg[TableInput](1, doc="Input")  # type: ignore[assignment]

        class OptionalFunc(TableInOutFunction):  # type: ignore[type-arg]
            """Optional arg."""

            class Meta:
                name = "func"

            count = Arg[int](0, default=10, doc="Count")
            data: TableInput = Arg[TableInput](1, doc="Input")  # type: ignore[assignment]

        # With argument provided, both match (ambiguous)
        with pytest.raises(ValueError, match="Ambiguous"):
            Worker._match_function_arguments(
                function_name="func",
                arguments=Arguments(positional=(pa.scalar(5),)),
                input_schema=pa.schema([]),
                candidates=[RequiredFunc, OptionalFunc],
            )

        # Without argument, only OptionalFunc matches
        result = Worker._match_function_arguments(
            function_name="func",
            arguments=Arguments(positional=()),
            input_schema=pa.schema([]),
            candidates=[RequiredFunc, OptionalFunc],
        )
        assert result is OptionalFunc

    def test_match_by_named_args(self) -> None:
        """Match function by named argument keys."""

        class FormatFunc(TableInOutFunction):  # type: ignore[type-arg]
            """Format func."""

            class Meta:
                name = "func"

            fmt = Arg[str]("format", doc="Format")
            data: TableInput = Arg[TableInput](0, doc="Input")  # type: ignore[assignment]

        class SepFunc(TableInOutFunction):  # type: ignore[type-arg]
            """Separator func."""

            class Meta:
                name = "func"

            sep = Arg[str]("separator", doc="Separator")
            data: TableInput = Arg[TableInput](0, doc="Input")  # type: ignore[assignment]

        candidates = [FormatFunc, SepFunc]

        # Named arg "format" -> FormatFunc
        assert (
            Worker._match_function_arguments(
                function_name="func",
                arguments=Arguments(positional=(), named={"format": pa.scalar("json")}),
                input_schema=pa.schema([]),
                candidates=candidates,
            )
            is FormatFunc
        )

        # Named arg "separator" -> SepFunc
        assert (
            Worker._match_function_arguments(
                function_name="func",
                arguments=Arguments(positional=(), named={"separator": pa.scalar(",")}),
                input_schema=pa.schema([]),
                candidates=candidates,
            )
            is SepFunc
        )

    def test_no_match_raises_error(self) -> None:
        """ValueError raised when no function matches."""

        class OneArgFunc(TableInOutFunction):  # type: ignore[type-arg]
            """One arg."""

            class Meta:
                name = "func"

            count = Arg[int](0, doc="Count")
            data: TableInput = Arg[TableInput](1, doc="Input")  # type: ignore[assignment]

        # Too many positional arguments
        with pytest.raises(ValueError, match="No matching function"):
            Worker._match_function_arguments(
                function_name="func",
                arguments=Arguments(positional=(pa.scalar(1), pa.scalar(2), pa.scalar(3))),
                input_schema=pa.schema([]),
                candidates=[OneArgFunc],
            )

    def test_no_match_error_shows_overloads(self) -> None:
        """Error message lists available overloads."""

        class OneArgFunc(TableInOutFunction):  # type: ignore[type-arg]
            """One arg."""

            class Meta:
                name = "func"

            count = Arg[int](0, doc="Count")
            data: TableInput = Arg[TableInput](1, doc="Input")  # type: ignore[assignment]

        class TwoArgFunc(TableInOutFunction):  # type: ignore[type-arg]
            """Two args."""

            class Meta:
                name = "func"

            x = Arg[int](0, doc="X")
            y = Arg[int](1, doc="Y")
            data: TableInput = Arg[TableInput](2, doc="Input")  # type: ignore[assignment]

        with pytest.raises(ValueError, match="OneArgFunc") as exc_info:
            Worker._match_function_arguments(
                function_name="func",
                arguments=Arguments(positional=()),
                input_schema=pa.schema([]),
                candidates=[OneArgFunc, TwoArgFunc],
            )
        assert "TwoArgFunc" in str(exc_info.value)

    def test_unknown_named_arg_rejects(self) -> None:
        """Function rejected if invocation has unknown named args."""

        class KnownArgFunc(TableInOutFunction):  # type: ignore[type-arg]
            """Known arg."""

            class Meta:
                name = "func"

            fmt = Arg[str]("format", doc="Format")
            data: TableInput = Arg[TableInput](0, doc="Input")  # type: ignore[assignment]

        # Named arg "unknown" not in function
        with pytest.raises(ValueError, match="No matching function"):
            Worker._match_function_arguments(
                function_name="func",
                arguments=Arguments(positional=(), named={"unknown": pa.scalar("x")}),
                input_schema=pa.schema([]),
                candidates=[KnownArgFunc],
            )

    def test_missing_required_named_rejects(self) -> None:
        """Function rejected if required named arg missing."""

        class RequiredNamedFunc(TableInOutFunction):  # type: ignore[type-arg]
            """Required named arg."""

            class Meta:
                name = "func"

            fmt = Arg[str]("format", doc="Format")  # Required (no default)
            data: TableInput = Arg[TableInput](0, doc="Input")  # type: ignore[assignment]

        # Missing required named arg
        with pytest.raises(ValueError, match="No matching function"):
            Worker._match_function_arguments(
                function_name="func",
                arguments=Arguments(positional=(), named=None),
                input_schema=pa.schema([]),
                candidates=[RequiredNamedFunc],
            )


class TestWorkerRegistry:
    """Tests for Worker._build_registry()."""

    def test_single_function_per_name(self) -> None:
        """Registry maps name to list with single function."""

        class MyWorker(Worker):
            functions = [
                type(
                    "TestFunc",
                    (TableInOutFunction,),
                    {
                        "Meta": type("Meta", (), {"name": "test"}),
                        "__annotations__": {"data": TableInput},
                        "data": Arg[TableInput](0, doc="Input"),
                    },
                )
            ]

        registry = MyWorker._build_registry()
        assert "test" in registry
        assert len(registry["test"]) == 1

    def test_multiple_functions_same_name(self) -> None:
        """Registry allows multiple functions with same name."""

        class Func1(TableInOutFunction):  # type: ignore[type-arg]
            """Func1."""

            class Meta:
                name = "shared"

            data: TableInput = Arg[TableInput](0, doc="Input")  # type: ignore[assignment]

        class Func2(TableInOutFunction):  # type: ignore[type-arg]
            """Func2."""

            class Meta:
                name = "shared"

            count = Arg[int](0)
            data: TableInput = Arg[TableInput](1, doc="Input")  # type: ignore[assignment]

        class MyWorker(Worker):
            functions = [Func1, Func2]

        registry = MyWorker._build_registry()
        assert "shared" in registry
        assert len(registry["shared"]) == 2
        assert Func1 in registry["shared"]
        assert Func2 in registry["shared"]

    def test_registry_cached_on_second_call(self) -> None:
        """Registry is cached after first build (line 161)."""

        class MyWorker(Worker):
            functions = [
                type(
                    "TestFunc",
                    (TableInOutFunction,),
                    {
                        "Meta": type("Meta", (), {"name": "test"}),
                        "__annotations__": {"data": TableInput},
                        "data": Arg[TableInput](0, doc="Input"),
                    },
                )
            ]

        # First call builds registry
        registry1 = MyWorker._build_registry()
        # Second call returns cached registry (line 161)
        registry2 = MyWorker._build_registry()
        # Should be the same object
        assert registry1 is registry2


class TestSuggestSimilarNames:
    """Tests for Worker._suggest_similar_names()."""

    def test_empty_candidates_returns_empty(self) -> None:
        """Empty candidates list returns empty (line 310)."""
        result = Worker._suggest_similar_names("test", [])
        assert result == []

    def test_exact_prefix_match(self) -> None:
        """Exact prefix match has highest priority (lines 319-320)."""
        candidates = ["get_users", "set_users", "getter"]
        result = Worker._suggest_similar_names("get", candidates)
        # "get_users" and "getter" start with "get", so they should be first
        assert "get_users" in result[:2]
        assert "getter" in result[:2]

    def test_reverse_prefix_match(self) -> None:
        """Candidate is prefix of name (lines 321-322)."""
        result = Worker._suggest_similar_names("get_all_users", ["get", "set"])
        # "get" is prefix of "get_all_users"
        assert "get" in result

    def test_substring_match(self) -> None:
        """Substring matching (lines 324-325)."""
        candidates = ["get_users", "users_list", "admin"]
        result = Worker._suggest_similar_names("user", candidates)
        # "user" is substring of "get_users" and "users_list"
        assert "get_users" in result
        assert "users_list" in result

    def test_character_overlap_match(self) -> None:
        """Character overlap for typos (lines 328-333)."""
        result = Worker._suggest_similar_names("geet", ["get", "set", "put"])
        # "geet" shares characters with "get" (g, e, t)
        # Overlap is 3 out of 4 > half of 4
        assert "get" in result

    def test_no_matches_returns_empty(self) -> None:
        """No matching candidates returns empty list."""
        result = Worker._suggest_similar_names("xyz", ["abc", "def"])
        # "xyz" has no overlap with "abc" or "def"
        assert result == []


# ---------------------------------------------------------------------------
# Fixtures: minimal table function definitions for cardinality tests
# ---------------------------------------------------------------------------


@dataclass(slots=True, frozen=True)
class _CountArgs:
    count: Annotated[int, Arg(0, doc="Number of rows")]


@init_single_worker
@bind_fixed_schema
class _FixedCardinalityFunc(TableFunctionGenerator[_CountArgs]):
    """Table function that returns exact cardinality from count arg."""

    class Meta:
        name = "fixed_card"

    FIXED_SCHEMA = pa.schema([pa.field("n", pa.int64())])

    @classmethod
    def cardinality(cls, params: BindParams[_CountArgs]) -> TableCardinality:
        return TableCardinality(estimate=params.args.count, max=params.args.count)

    @classmethod
    def process(cls, params: ProcessParams[_CountArgs], state: None, out: OutputCollector) -> None:
        out.finish()


@init_single_worker
@bind_fixed_schema
class _DefaultCardinalityFunc(TableFunctionGenerator[_CountArgs]):
    """Table function that uses the default (unknown) cardinality."""

    class Meta:
        name = "default_card"

    FIXED_SCHEMA = pa.schema([pa.field("n", pa.int64())])

    @classmethod
    def process(cls, params: ProcessParams[_CountArgs], state: None, out: OutputCollector) -> None:
        out.finish()


class _ScalarOnlyFunc(TableInOutFunction):  # type: ignore[type-arg]
    """Non-table-function to test the error path."""

    class Meta:
        name = "scalar_only"

    data: TableInput = Arg[TableInput](0, doc="Input")  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def _make_bind_request(function_name: str, *positional: int) -> BindRequest:
    """Create a BindRequest for a table function."""
    return BindRequest(
        function_name=function_name,
        arguments=Arguments(positional=tuple(pa.scalar(v) for v in positional)),
        function_type=FunctionType.TABLE,
    )


def _anon_ctx() -> CallContext:
    return CallContext(auth=AuthContext.anonymous(), emit_client_log=lambda *a, **kw: None)


class TestTableFunctionCardinality:
    """Tests for Worker.table_function_cardinality()."""

    def test_returns_custom_cardinality(self) -> None:
        """Cardinality from a function that overrides cardinality()."""

        class MyWorker(Worker):
            functions = [_FixedCardinalityFunc]

        worker = MyWorker()
        request = TableFunctionCardinalityRequest(
            bind_call=_make_bind_request("fixed_card", 42),
        )
        result = worker.table_function_cardinality(request, _anon_ctx())

        assert isinstance(result, TableCardinality)
        assert result.estimate == 42
        assert result.max == 42

    def test_returns_default_cardinality(self) -> None:
        """Default cardinality is (None, None) when not overridden."""

        class MyWorker(Worker):
            functions = [_DefaultCardinalityFunc]

        worker = MyWorker()
        request = TableFunctionCardinalityRequest(
            bind_call=_make_bind_request("default_card", 10),
        )
        result = worker.table_function_cardinality(request, _anon_ctx())

        assert isinstance(result, TableCardinality)
        assert result.estimate is None
        assert result.max is None

    def test_rejects_non_table_function(self) -> None:
        """Raises ValueError for non-TableFunctionGenerator functions."""

        class MyWorker(Worker):
            functions = [_ScalarOnlyFunc]

        worker = MyWorker()
        request = TableFunctionCardinalityRequest(
            bind_call=_make_bind_request("scalar_only"),
        )
        with pytest.raises(ValueError, match="not a TableFunctionGenerator"):
            worker.table_function_cardinality(request, _anon_ctx())

    def test_unknown_function_raises(self) -> None:
        """Raises ValueError for unknown function names."""

        class MyWorker(Worker):
            functions = [_FixedCardinalityFunc]

        worker = MyWorker()
        request = TableFunctionCardinalityRequest(
            bind_call=_make_bind_request("nonexistent", 1),
        )
        with pytest.raises(ValueError, match="Unknown function"):
            worker.table_function_cardinality(request, _anon_ctx())

    def test_passes_bind_opaque_data(self) -> None:
        """bind_opaque_data on the request is accepted (though unused by default)."""

        class MyWorker(Worker):
            functions = [_FixedCardinalityFunc]

        worker = MyWorker()
        request = TableFunctionCardinalityRequest(
            bind_call=_make_bind_request("fixed_card", 99),
            bind_opaque_data=None,
        )
        result = worker.table_function_cardinality(request, _anon_ctx())
        assert result.estimate == 99

    def test_cardinality_with_settings(self) -> None:
        """Settings from bind_call are forwarded to cardinality()."""

        @init_single_worker
        @bind_fixed_schema
        class SettingsCardFunc(TableFunctionGenerator[_CountArgs]):
            class Meta:
                name = "settings_card"

            FIXED_SCHEMA = pa.schema([pa.field("n", pa.int64())])

            @classmethod
            def cardinality(cls, params: BindParams[_CountArgs]) -> TableCardinality:
                multiplier = params.settings.get("multiplier")
                mult = multiplier.as_py() if multiplier is not None else 1
                return TableCardinality(estimate=params.args.count * mult, max=None)

            @classmethod
            def process(cls, params: ProcessParams[_CountArgs], state: None, out: OutputCollector) -> None:
                out.finish()

        class MyWorker(Worker):
            functions = [SettingsCardFunc]

        worker = MyWorker()
        settings_batch = pa.RecordBatch.from_pydict(
            {"multiplier": [3]},
            schema=pa.schema([pa.field("multiplier", pa.int64())]),
        )
        request = TableFunctionCardinalityRequest(
            bind_call=BindRequest(
                function_name="settings_card",
                arguments=Arguments(positional=(pa.scalar(10),)),
                function_type=FunctionType.TABLE,
                settings=settings_batch,
            ),
        )
        result = worker.table_function_cardinality(request, _anon_ctx())
        assert result.estimate == 30


@init_single_worker
@bind_fixed_schema
class _FixedStatsFunc(TableFunctionGenerator[_CountArgs]):
    """Table function that returns fixed per-column stats derived from count."""

    class Meta:
        name = "fixed_stats"

    FIXED_SCHEMA = pa.schema([pa.field("n", pa.int64())])

    @classmethod
    def statistics(cls, params: BindParams[_CountArgs]) -> list[ColumnStatistics] | None:
        if params.args.count <= 0:
            return []
        return [
            ColumnStatistics(
                column_name="n",
                min=pa.scalar(0, pa.int64()),
                max=pa.scalar(params.args.count - 1, pa.int64()),
                has_null=False,
                has_not_null=True,
                distinct_count=params.args.count,
            )
        ]

    @classmethod
    def process(cls, params: ProcessParams[_CountArgs], state: None, out: OutputCollector) -> None:
        out.finish()


@init_single_worker
@bind_fixed_schema
class _NoStatsFunc(TableFunctionGenerator[_CountArgs]):
    """Table function that uses the default (no stats) behavior."""

    class Meta:
        name = "no_stats"

    FIXED_SCHEMA = pa.schema([pa.field("n", pa.int64())])

    @classmethod
    def process(cls, params: ProcessParams[_CountArgs], state: None, out: OutputCollector) -> None:
        out.finish()


class TestTableFunctionStatistics:
    """Tests for Worker.table_function_statistics()."""

    def test_returns_stats_from_bind_args(self) -> None:
        """Stats are derived from user-supplied bind args and serialized to IPC bytes."""

        class MyWorker(Worker):
            functions = [_FixedStatsFunc]

        worker = MyWorker()
        request = TableFunctionStatisticsRequest(
            bind_call=_make_bind_request("fixed_stats", 100),
        )
        result = worker.table_function_statistics(request, _anon_ctx())

        assert isinstance(result, bytes)
        batch, _ = deserialize_record_batch(result)
        assert batch.num_rows == 1
        assert batch.column("column_name")[0].as_py() == "n"
        assert batch.column("min")[0].as_py() == 0
        assert batch.column("max")[0].as_py() == 99
        assert batch.column("has_null")[0].as_py() is False
        assert batch.column("has_not_null")[0].as_py() is True
        assert batch.column("distinct_count")[0].as_py() == 100

    def test_default_returns_none(self) -> None:
        """Functions that don't override statistics() return None."""

        class MyWorker(Worker):
            functions = [_NoStatsFunc]

        worker = MyWorker()
        request = TableFunctionStatisticsRequest(
            bind_call=_make_bind_request("no_stats", 10),
        )
        assert worker.table_function_statistics(request, _anon_ctx()) is None

    def test_empty_stats_list_returns_none(self) -> None:
        """An explicit empty list is treated as 'no stats' → None over the wire."""

        class MyWorker(Worker):
            functions = [_FixedStatsFunc]

        worker = MyWorker()
        # count=0 triggers the `return []` branch in _FixedStatsFunc.statistics
        request = TableFunctionStatisticsRequest(
            bind_call=_make_bind_request("fixed_stats", 0),
        )
        assert worker.table_function_statistics(request, _anon_ctx()) is None

    def test_non_table_function_returns_none(self) -> None:
        """Scalar / non-TableFunctionGenerator functions return None (no error)."""

        class MyWorker(Worker):
            functions = [_ScalarOnlyFunc]

        worker = MyWorker()
        request = TableFunctionStatisticsRequest(
            bind_call=_make_bind_request("scalar_only"),
        )
        assert worker.table_function_statistics(request, _anon_ctx()) is None

    def test_passes_bind_opaque_data(self) -> None:
        """bind_opaque_data is accepted on the request (mirrors cardinality)."""

        class MyWorker(Worker):
            functions = [_FixedStatsFunc]

        worker = MyWorker()
        request = TableFunctionStatisticsRequest(
            bind_call=_make_bind_request("fixed_stats", 5),
            bind_opaque_data=None,
        )
        result = worker.table_function_statistics(request, _anon_ctx())
        assert isinstance(result, bytes)


class TestScalarOverloading:
    """Tests for scalar function overloading by ConstParam count."""

    def _make_scalar_candidates(self) -> list[type]:
        """Create three scalar overloads with 0, 1, and 2 ConstParams."""

        class ZeroConst(ScalarFunction):
            class Meta:
                name = "fmt"

            @classmethod
            def compute(
                cls,
                val: Annotated[pa.DoubleArray, Param(doc="Value")],
            ) -> Annotated[pa.StringArray, Returns()]:
                return pa.array([str(v) for v in val.to_pylist()], type=pa.string())

        class OneConst(ScalarFunction):
            class Meta:
                name = "fmt"

            @classmethod
            def compute(
                cls,
                prec: Annotated[int, ConstParam("Precision")],
                val: Annotated[pa.DoubleArray, Param(doc="Value")],
            ) -> Annotated[pa.StringArray, Returns()]:
                return pa.array([f"{v:.{prec}f}" for v in val.to_pylist()], type=pa.string())

        class TwoConst(ScalarFunction):
            class Meta:
                name = "fmt"

            @classmethod
            def compute(
                cls,
                prec: Annotated[int, ConstParam("Precision")],
                pfx: Annotated[str, ConstParam("Prefix")],
                val: Annotated[pa.DoubleArray, Param(doc="Value")],
            ) -> Annotated[pa.StringArray, Returns()]:
                return pa.array([f"{pfx}{v:.{prec}f}" for v in val.to_pylist()], type=pa.string())

        return [ZeroConst, OneConst, TwoConst]

    def test_match_by_const_param_count(self) -> None:
        """Scalar overloads are matched by ConstParam count."""
        candidates = self._make_scalar_candidates()

        # 0 const args -> ZeroConst
        result = Worker._match_function_arguments(
            function_name="fmt",
            arguments=Arguments(positional=()),
            input_schema=pa.schema([("val", pa.float64())]),
            candidates=candidates,
        )
        assert result is candidates[0]

        # 1 const arg -> OneConst
        result = Worker._match_function_arguments(
            function_name="fmt",
            arguments=Arguments(positional=(pa.scalar(2),)),
            input_schema=pa.schema([("val", pa.float64())]),
            candidates=candidates,
        )
        assert result is candidates[1]

        # 2 const args -> TwoConst
        result = Worker._match_function_arguments(
            function_name="fmt",
            arguments=Arguments(positional=(pa.scalar(2), pa.scalar("$"))),
            input_schema=pa.schema([("val", pa.float64())]),
            candidates=candidates,
        )
        assert result is candidates[2]

    def test_zero_const_params_matches(self) -> None:
        """A scalar function with 0 ConstParams correctly matches 0 positional args."""
        candidates = self._make_scalar_candidates()

        result = Worker._match_function_arguments(
            function_name="fmt",
            arguments=Arguments(positional=()),
            input_schema=pa.schema([("val", pa.float64())]),
            candidates=candidates,
        )
        # Should match ZeroConst (0 ConstParams), not fail
        assert result is candidates[0]

    def test_no_match_error_scalar(self) -> None:
        """Too many const args gives helpful error for scalar overloads."""
        candidates = self._make_scalar_candidates()

        with pytest.raises(ValueError, match="No matching function"):
            Worker._match_function_arguments(
                function_name="fmt",
                arguments=Arguments(positional=(pa.scalar(1), pa.scalar(2), pa.scalar(3))),
                input_schema=pa.schema([("val", pa.float64())]),
                candidates=candidates,
            )


class TestTypeBasedOverloading:
    """Tests for type-based function overloading dispatch."""

    def test_scalar_single_column_type_dispatch(self) -> None:
        """Scalar overloads with same arg count but different column types."""

        class IntFunc(ScalarFunction):
            class Meta:
                name = "info"

            @classmethod
            def compute(
                cls,
                v: Annotated[pa.Int64Array, Param(doc="v")],
            ) -> Annotated[pa.StringArray, Returns()]:
                return pa.array(["int64"], type=pa.string())

        class StrFunc(ScalarFunction):
            class Meta:
                name = "info"

            @classmethod
            def compute(
                cls,
                v: Annotated[pa.StringArray, Param(doc="v")],
            ) -> Annotated[pa.StringArray, Returns()]:
                return pa.array(["varchar"], type=pa.string())

        candidates = [IntFunc, StrFunc]

        # int64 input -> IntFunc
        result = Worker._match_function_arguments(
            function_name="info",
            arguments=Arguments(positional=()),
            input_schema=pa.schema([("v", pa.int64())]),
            candidates=candidates,
        )
        assert result is IntFunc

        # string input -> StrFunc
        result = Worker._match_function_arguments(
            function_name="info",
            arguments=Arguments(positional=()),
            input_schema=pa.schema([("v", pa.string())]),
            candidates=candidates,
        )
        assert result is StrFunc

    def test_scalar_multi_column_type_dispatch(self) -> None:
        """Scalar overloads disambiguated by multiple column types."""

        class IntIntFunc(ScalarFunction):
            class Meta:
                name = "pair"

            @classmethod
            def compute(
                cls,
                a: Annotated[pa.Int64Array, Param(doc="a")],
                b: Annotated[pa.Int64Array, Param(doc="b")],
            ) -> Annotated[pa.StringArray, Returns()]:
                return pa.array(["ii"], type=pa.string())

        class StrStrFunc(ScalarFunction):
            class Meta:
                name = "pair"

            @classmethod
            def compute(
                cls,
                a: Annotated[pa.StringArray, Param(doc="a")],
                b: Annotated[pa.StringArray, Param(doc="b")],
            ) -> Annotated[pa.StringArray, Returns()]:
                return pa.array(["ss"], type=pa.string())

        class IntStrFunc(ScalarFunction):
            class Meta:
                name = "pair"

            @classmethod
            def compute(
                cls,
                a: Annotated[pa.Int64Array, Param(doc="a")],
                b: Annotated[pa.StringArray, Param(doc="b")],
            ) -> Annotated[pa.StringArray, Returns()]:
                return pa.array(["is"], type=pa.string())

        candidates = [IntIntFunc, StrStrFunc, IntStrFunc]

        result = Worker._match_function_arguments(
            function_name="pair",
            arguments=Arguments(positional=()),
            input_schema=pa.schema([("a", pa.int64()), ("b", pa.int64())]),
            candidates=candidates,
        )
        assert result is IntIntFunc

        result = Worker._match_function_arguments(
            function_name="pair",
            arguments=Arguments(positional=()),
            input_schema=pa.schema([("a", pa.string()), ("b", pa.string())]),
            candidates=candidates,
        )
        assert result is StrStrFunc

        result = Worker._match_function_arguments(
            function_name="pair",
            arguments=Arguments(positional=()),
            input_schema=pa.schema([("a", pa.int64()), ("b", pa.string())]),  # type: ignore[arg-type]
            candidates=candidates,
        )
        assert result is IntStrFunc

    def test_scalar_constparam_type_dispatch(self) -> None:
        """Scalar overloads with same ConstParam count but different types."""

        class IntConstFunc(ScalarFunction):
            class Meta:
                name = "fmt"

            @classmethod
            def compute(
                cls,
                width: Annotated[int, ConstParam("Width")],
                val: Annotated[pa.DoubleArray, Param(doc="v")],
            ) -> Annotated[pa.StringArray, Returns()]:
                return pa.array(["int"], type=pa.string())

        class StrConstFunc(ScalarFunction):
            class Meta:
                name = "fmt"

            @classmethod
            def compute(
                cls,
                prefix: Annotated[str, ConstParam("Prefix")],
                val: Annotated[pa.DoubleArray, Param(doc="v")],
            ) -> Annotated[pa.StringArray, Returns()]:
                return pa.array(["str"], type=pa.string())

        candidates = [IntConstFunc, StrConstFunc]

        # int ConstParam -> IntConstFunc
        result = Worker._match_function_arguments(
            function_name="fmt",
            arguments=Arguments(positional=(pa.scalar(10),)),
            input_schema=pa.schema([("val", pa.float64())]),
            candidates=candidates,
        )
        assert result is IntConstFunc

        # str ConstParam -> StrConstFunc
        result = Worker._match_function_arguments(
            function_name="fmt",
            arguments=Arguments(positional=(pa.scalar("$"),)),
            input_schema=pa.schema([("val", pa.float64())]),
            candidates=candidates,
        )
        assert result is StrConstFunc

    def test_scalar_any_arrow_mixed_dispatch(self) -> None:
        """AnyArrow params are skipped; dispatch on fixed-type params."""

        class AnyIntFunc(ScalarFunction):
            class Meta:
                name = "mix"

            @classmethod
            def compute(
                cls,
                a: Annotated[pa.Array, Param(doc="any")],  # type: ignore[type-arg]
                b: Annotated[pa.Int64Array, Param(doc="int")],
            ) -> Annotated[pa.StringArray, Returns()]:
                return pa.array(["ai"], type=pa.string())

        class AnyStrFunc(ScalarFunction):
            class Meta:
                name = "mix"

            @classmethod
            def compute(
                cls,
                a: Annotated[pa.Array, Param(doc="any")],  # type: ignore[type-arg]
                b: Annotated[pa.StringArray, Param(doc="str")],
            ) -> Annotated[pa.StringArray, Returns()]:
                return pa.array(["as"], type=pa.string())

        candidates = [AnyIntFunc, AnyStrFunc]

        # Second col is int64 -> AnyIntFunc
        result = Worker._match_function_arguments(
            function_name="mix",
            arguments=Arguments(positional=()),
            input_schema=pa.schema([("a", pa.float32()), ("b", pa.int64())]),  # type: ignore[arg-type]
            candidates=candidates,
        )
        assert result is AnyIntFunc

        # Second col is string -> AnyStrFunc
        result = Worker._match_function_arguments(
            function_name="mix",
            arguments=Arguments(positional=()),
            input_schema=pa.schema([("a", pa.float32()), ("b", pa.string())]),  # type: ignore[arg-type]
            candidates=candidates,
        )
        assert result is AnyStrFunc

    def test_table_function_type_dispatch(self) -> None:
        """Table function overloads with same arg count but different types."""

        @dataclass(kw_only=True)
        class IntArgs:
            start: Annotated[int, Arg(0, doc="start")]
            stop: Annotated[int, Arg(1, doc="stop")]

        @dataclass(kw_only=True)
        class StrArgs:
            prefix: Annotated[str, Arg(0, doc="prefix")]
            suffix: Annotated[str, Arg(1, doc="suffix")]

        @init_single_worker
        @bind_fixed_schema
        class IntPairFunc(TableFunctionGenerator[IntArgs, None]):
            FIXED_SCHEMA = pa.schema([("a", pa.int64())])

            class Meta:
                name = "pairs"

            @classmethod
            def process(cls, params: ProcessParams[IntArgs], state: None, out: OutputCollector) -> None:
                out.finish()

        @init_single_worker
        @bind_fixed_schema
        class StrPairFunc(TableFunctionGenerator[StrArgs, None]):
            FIXED_SCHEMA = pa.schema([("a", pa.string())])

            class Meta:
                name = "pairs"

            @classmethod
            def process(cls, params: ProcessParams[StrArgs], state: None, out: OutputCollector) -> None:
                out.finish()

        candidates: list[type] = [IntPairFunc, StrPairFunc]

        # int args -> IntPairFunc
        result = Worker._match_function_arguments(
            function_name="pairs",
            arguments=Arguments(positional=(pa.scalar(1), pa.scalar(5))),
            input_schema=None,
            candidates=candidates,
        )
        assert result is IntPairFunc

        # str args -> StrPairFunc
        result = Worker._match_function_arguments(
            function_name="pairs",
            arguments=Arguments(positional=(pa.scalar("a"), pa.scalar("b"))),
            input_schema=None,
            candidates=candidates,
        )
        assert result is StrPairFunc


class TestTypesCompatible:
    """Direct tests for Worker._types_compatible."""

    def test_exact_match(self) -> None:
        """Same type is always compatible."""
        assert Worker._types_compatible(pa.int64(), pa.int64()) is True

    def test_integer_family(self) -> None:
        """All integer types are compatible with each other."""
        assert Worker._types_compatible(pa.int32(), pa.int64()) is True
        assert Worker._types_compatible(pa.uint8(), pa.int64()) is True
        assert Worker._types_compatible(pa.int16(), pa.uint32()) is True

    def test_float_decimal_family(self) -> None:
        """Float and decimal types are compatible."""
        assert Worker._types_compatible(pa.decimal128(10, 2), pa.float64()) is True
        assert Worker._types_compatible(pa.float32(), pa.float64()) is True
        assert Worker._types_compatible(pa.float64(), pa.decimal128(5, 3)) is True

    def test_string_family(self) -> None:
        """String and large_string are in the same family."""
        assert Worker._types_compatible(pa.string(), pa.string()) is True
        assert Worker._types_compatible(pa.string(), pa.large_string()) is True
        assert Worker._types_compatible(pa.large_string(), pa.string()) is True

    def test_binary_family(self) -> None:
        """Binary and large_binary are in the same family."""
        assert Worker._types_compatible(pa.binary(), pa.large_binary()) is True

    def test_boolean(self) -> None:
        """Boolean matches boolean."""
        assert Worker._types_compatible(pa.bool_(), pa.bool_()) is True

    def test_cross_family_rejected(self) -> None:
        """Types from different families are incompatible."""
        assert Worker._types_compatible(pa.int64(), pa.string()) is False
        assert Worker._types_compatible(pa.int32(), pa.float64()) is False
        assert Worker._types_compatible(pa.string(), pa.bool_()) is False
        assert Worker._types_compatible(pa.binary(), pa.string()) is False


class TestTypeDispatchEdgeCases:
    """Edge cases for type-based overload dispatch."""

    def test_family_match_int32_to_int64(self) -> None:
        """int32 arg matches int64 declared param via family matching."""

        class Int64Func(ScalarFunction):
            class Meta:
                name = "f"

            @classmethod
            def compute(
                cls,
                v: Annotated[pa.Int64Array, Param(doc="v")],
            ) -> Annotated[pa.StringArray, Returns()]:
                return pa.array(["ok"], type=pa.string())

        class StrFunc(ScalarFunction):
            class Meta:
                name = "f"

            @classmethod
            def compute(
                cls,
                v: Annotated[pa.StringArray, Param(doc="v")],
            ) -> Annotated[pa.StringArray, Returns()]:
                return pa.array(["ok"], type=pa.string())

        # int32 input should match Int64Func via family, not StrFunc
        result = Worker._match_function_arguments(
            function_name="f",
            arguments=Arguments(positional=()),
            input_schema=pa.schema([("v", pa.int32())]),
            candidates=[Int64Func, StrFunc],
        )
        assert result is Int64Func

    def test_type_filter_eliminates_all_raises_error(self) -> None:
        """When type filtering eliminates all candidates, error is raised."""

        class IntFunc(ScalarFunction):
            class Meta:
                name = "f"

            @classmethod
            def compute(
                cls,
                v: Annotated[pa.Int64Array, Param(doc="v")],
            ) -> Annotated[pa.StringArray, Returns()]:
                return pa.array(["ok"], type=pa.string())

        class StrFunc(ScalarFunction):
            class Meta:
                name = "f"

            @classmethod
            def compute(
                cls,
                v: Annotated[pa.StringArray, Param(doc="v")],
            ) -> Annotated[pa.StringArray, Returns()]:
                return pa.array(["ok"], type=pa.string())

        # bool input matches neither int64 nor string
        with pytest.raises(ValueError, match="No matching function"):
            Worker._match_function_arguments(
                function_name="f",
                arguments=Arguments(positional=()),
                input_schema=pa.schema([("v", pa.bool_())]),
                candidates=[IntFunc, StrFunc],
            )

    def test_ambiguous_type_dispatch_raises_error(self) -> None:
        """When type filtering leaves multiple candidates, error is raised."""

        class Int32Func(ScalarFunction):
            class Meta:
                name = "f"

            @classmethod
            def compute(
                cls,
                v: Annotated[pa.Int32Array, Param(doc="v")],
            ) -> Annotated[pa.StringArray, Returns()]:
                return pa.array(["ok"], type=pa.string())

        class Int64Func(ScalarFunction):
            class Meta:
                name = "f"

            @classmethod
            def compute(
                cls,
                v: Annotated[pa.Int64Array, Param(doc="v")],
            ) -> Annotated[pa.StringArray, Returns()]:
                return pa.array(["ok"], type=pa.string())

        # int16 input family-matches both with same score -> ambiguous
        with pytest.raises(ValueError, match="Ambiguous function call"):
            Worker._match_function_arguments(
                function_name="f",
                arguments=Arguments(positional=()),
                input_schema=pa.schema([("v", pa.int16())]),
                candidates=[Int32Func, Int64Func],
            )

    def test_exact_match_preferred_over_family(self) -> None:
        """Exact type match scores higher than family match."""

        class Int32Func(ScalarFunction):
            class Meta:
                name = "f"

            @classmethod
            def compute(
                cls,
                v: Annotated[pa.Int32Array, Param(doc="v")],
            ) -> Annotated[pa.StringArray, Returns()]:
                return pa.array(["int32"], type=pa.string())

        class Int64Func(ScalarFunction):
            class Meta:
                name = "f"

            @classmethod
            def compute(
                cls,
                v: Annotated[pa.Int64Array, Param(doc="v")],
            ) -> Annotated[pa.StringArray, Returns()]:
                return pa.array(["int64"], type=pa.string())

        # int32 input exactly matches Int32Func (score 2) vs family-matches Int64Func (score 1)
        result = Worker._match_function_arguments(
            function_name="f",
            arguments=Arguments(positional=()),
            input_schema=pa.schema([("v", pa.int32())]),
            candidates=[Int32Func, Int64Func],
        )
        assert result is Int32Func

        # int64 input exactly matches Int64Func
        result = Worker._match_function_arguments(
            function_name="f",
            arguments=Arguments(positional=()),
            input_schema=pa.schema([("v", pa.int64())]),
            candidates=[Int32Func, Int64Func],
        )
        assert result is Int64Func


class TestVarargsOverloading:
    """Tests for varargs type-based overload dispatch."""

    def test_scalar_varargs_type_dispatch(self) -> None:
        """Scalar varargs overloads dispatched by column type."""

        class IntVarargs(ScalarFunction):
            class Meta:
                name = "cv"

            @classmethod
            def compute(
                cls,
                values: Annotated[list[pa.Int64Array], Param(doc="ints", varargs=True)],
            ) -> Annotated[pa.StringArray, Returns()]:
                return pa.array(["int"], type=pa.string())

        class StrVarargs(ScalarFunction):
            class Meta:
                name = "cv"

            @classmethod
            def compute(
                cls,
                values: Annotated[list[pa.StringArray], Param(doc="strs", varargs=True)],
            ) -> Annotated[pa.StringArray, Returns()]:
                return pa.array(["str"], type=pa.string())

        candidates: list[type] = [IntVarargs, StrVarargs]

        # int64 columns -> IntVarargs
        result = Worker._match_function_arguments(
            function_name="cv",
            arguments=Arguments(positional=()),
            input_schema=pa.schema([("a", pa.int64()), ("b", pa.int64())]),
            candidates=candidates,
        )
        assert result is IntVarargs

        # string columns -> StrVarargs
        result = Worker._match_function_arguments(
            function_name="cv",
            arguments=Arguments(positional=()),
            input_schema=pa.schema([("a", pa.string()), ("b", pa.string())]),
            candidates=candidates,
        )
        assert result is StrVarargs

    def test_scalar_varargs_all_elements_checked(self) -> None:
        """ALL varargs elements are checked, not just the first one."""

        class IntVarargs(ScalarFunction):
            class Meta:
                name = "cv"

            @classmethod
            def compute(
                cls,
                values: Annotated[list[pa.Int64Array], Param(doc="ints", varargs=True)],
            ) -> Annotated[pa.StringArray, Returns()]:
                return pa.array(["int"], type=pa.string())

        class StrVarargs(ScalarFunction):
            class Meta:
                name = "cv"

            @classmethod
            def compute(
                cls,
                values: Annotated[list[pa.StringArray], Param(doc="strs", varargs=True)],
            ) -> Annotated[pa.StringArray, Returns()]:
                return pa.array(["str"], type=pa.string())

        candidates: list[type] = [IntVarargs, StrVarargs]

        # Mixed types: first is int64, second is string -> neither should match
        with pytest.raises(ValueError, match="No matching function"):
            Worker._match_function_arguments(
                function_name="cv",
                arguments=Arguments(positional=()),
                input_schema=pa.schema([("a", pa.int64()), ("b", pa.string())]),  # type: ignore[arg-type]
                candidates=candidates,
            )

    def test_table_varargs_type_dispatch(self) -> None:
        """Table function varargs overloads dispatched by argument type."""

        @dataclass(kw_only=True)
        class IntRepeatArgs:
            count: Annotated[int, Arg(0, doc="count")]
            values: Annotated[list[int], Arg(1, varargs=True, arrow_type=pa.int64(), doc="vals")]

        @dataclass(kw_only=True)
        class StrRepeatArgs:
            count: Annotated[int, Arg(0, doc="count")]
            values: Annotated[list[str], Arg(1, varargs=True, arrow_type=pa.string(), doc="vals")]

        @init_single_worker
        @bind_fixed_schema
        class IntRepeatFunc(TableFunctionGenerator[IntRepeatArgs, None]):
            FIXED_SCHEMA = pa.schema([("v", pa.int64())])

            class Meta:
                name = "rv"

            @classmethod
            def process(cls, params: ProcessParams[IntRepeatArgs], state: None, out: OutputCollector) -> None:
                out.finish()

        @init_single_worker
        @bind_fixed_schema
        class StrRepeatFunc(TableFunctionGenerator[StrRepeatArgs, None]):
            FIXED_SCHEMA = pa.schema([("v", pa.string())])

            class Meta:
                name = "rv"

            @classmethod
            def process(cls, params: ProcessParams[StrRepeatArgs], state: None, out: OutputCollector) -> None:
                out.finish()

        candidates: list[type] = [IntRepeatFunc, StrRepeatFunc]

        # int varargs -> IntRepeatFunc
        result = Worker._match_function_arguments(
            function_name="rv",
            arguments=Arguments(positional=(pa.scalar(3), pa.scalar(10), pa.scalar(20))),
            input_schema=None,
            candidates=candidates,
        )
        assert result is IntRepeatFunc

        # string varargs -> StrRepeatFunc
        result = Worker._match_function_arguments(
            function_name="rv",
            arguments=Arguments(positional=(pa.scalar(3), pa.scalar("a"), pa.scalar("b"))),
            input_schema=None,
            candidates=candidates,
        )
        assert result is StrRepeatFunc

    def test_varargs_any_type_matches_all(self) -> None:
        """AnyArrow varargs should still match any type without scoring."""

        class AnyVarargs(ScalarFunction):
            class Meta:
                name = "av"

            @classmethod
            def compute(
                cls,
                values: Annotated[  # type: ignore[type-arg]
                    list[pa.Array],
                    Param(doc="any", varargs=True),
                ],
            ) -> Annotated[pa.StringArray, Returns()]:
                return pa.array(["any"], type=pa.string())

        # Should match any column types
        result = Worker._match_function_arguments(
            function_name="av",
            arguments=Arguments(positional=()),
            input_schema=pa.schema([("a", pa.int64()), ("b", pa.string())]),  # type: ignore[arg-type]
            candidates=[AnyVarargs],
        )
        assert result is AnyVarargs


class TestGeoFunctions:
    """Tests for geo scalar functions with complex Arrow types."""

    @pytest.mark.parametrize(
        ("func_name", "func_cls_name", "col_type"),
        [
            ("geo_distance_struct", "GeoDistanceStructFunction", "struct"),
            ("geo_distance_list", "GeoDistanceListFunction", "list"),
            ("geo_distance_fixed", "GeoDistanceFixedFunction", "fixed"),
        ],
    )
    def test_distance_resolves(self, func_name: str, func_cls_name: str, col_type: str) -> None:
        """GeoDistance functions resolve with their respective point column types."""
        import vgi._test_fixtures.scalar as geo

        func_cls = getattr(geo, func_cls_name)
        point_type = {
            "struct": geo._POINT_STRUCT_TYPE,
            "list": pa.list_(pa.float64()),
            "fixed": pa.list_(pa.float64(), 2),
        }[col_type]

        result = Worker._match_function_arguments(
            function_name=func_name,
            arguments=Arguments(positional=()),
            input_schema=pa.schema([("p1", point_type), ("p2", point_type)]),
            candidates=[func_cls],
        )
        assert result is func_cls

    @pytest.mark.parametrize(
        ("func_name", "func_cls_name", "col_type", "num_points"),
        [
            ("geo_centroid_struct", "GeoCentroidStructFunction", "struct", 3),
            ("geo_centroid_list", "GeoCentroidListFunction", "list", 2),
            ("geo_centroid_fixed", "GeoCentroidFixedFunction", "fixed", 2),
        ],
    )
    def test_centroid_resolves(self, func_name: str, func_cls_name: str, col_type: str, num_points: int) -> None:
        """GeoCentroid functions resolve with varargs point columns."""
        import vgi._test_fixtures.scalar as geo

        func_cls = getattr(geo, func_cls_name)
        point_type = {
            "struct": geo._POINT_STRUCT_TYPE,
            "list": pa.list_(pa.float64()),
            "fixed": pa.list_(pa.float64(), 2),
        }[col_type]

        result = Worker._match_function_arguments(
            function_name=func_name,
            arguments=Arguments(positional=()),
            input_schema=pa.schema([(f"p{i}", point_type) for i in range(num_points)]),
            candidates=[func_cls],
        )
        assert result is func_cls
