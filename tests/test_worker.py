"""Tests for vgi.worker module, including function overloading and cardinality."""

from dataclasses import dataclass
from typing import Annotated

import pyarrow as pa
import pytest
from vgi_rpc.rpc import OutputCollector

from vgi import Arg, TableInOutFunction, TableInput
from vgi.arguments import Arguments, ConstParam, Param, Returns
from vgi.invocation import FunctionType
from vgi.protocol import BindRequest, TableFunctionCardinalityRequest
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
        result = worker.table_function_cardinality(request)

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
        result = worker.table_function_cardinality(request)

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
            worker.table_function_cardinality(request)

    def test_unknown_function_raises(self) -> None:
        """Raises ValueError for unknown function names."""

        class MyWorker(Worker):
            functions = [_FixedCardinalityFunc]

        worker = MyWorker()
        request = TableFunctionCardinalityRequest(
            bind_call=_make_bind_request("nonexistent", 1),
        )
        with pytest.raises(ValueError, match="Unknown function"):
            worker.table_function_cardinality(request)

    def test_passes_bind_opaque_data(self) -> None:
        """bind_opaque_data on the request is accepted (though unused by default)."""

        class MyWorker(Worker):
            functions = [_FixedCardinalityFunc]

        worker = MyWorker()
        request = TableFunctionCardinalityRequest(
            bind_call=_make_bind_request("fixed_card", 99),
            bind_opaque_data=None,
        )
        result = worker.table_function_cardinality(request)
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
        result = worker.table_function_cardinality(request)
        assert result.estimate == 30


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
