"""Tests for vgi.worker module, including function overloading."""

import pyarrow as pa
import pytest

from vgi import Arg, TableInOutFunction, TableInput
from vgi.arguments import Arguments
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
