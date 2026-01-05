"""Tests for vgi.worker module, including function overloading."""

import pyarrow as pa
import pytest

from vgi import Arg, TableInOutFunction, TableInput
from vgi.arguments import Arguments
from vgi.invocation import Invocation, InvocationType
from vgi.worker import Worker


class TestFunctionOverloading:
    """Tests for function overloading based on argument signatures."""

    def test_single_candidate_always_matches(self) -> None:
        """With only one candidate, it's always selected."""

        class SingleFunction(TableInOutFunction):
            """Single function."""

            class Meta:
                name = "single"

            data: TableInput = Arg[TableInput](0, doc="Input")  # type: ignore[assignment]

        invocation = Invocation(
            function_name="single",
            input_schema=pa.schema([]),
            function_type=InvocationType.TABLE,
            correlation_id="test",
            invocation_id=b"test",
            arguments=Arguments(),
        )

        result = Worker._match_function(invocation, [SingleFunction])
        assert result is SingleFunction

    def test_match_by_positional_count(self) -> None:
        """Match function by number of positional arguments."""

        class NoArgsFunc(TableInOutFunction):
            """No args."""

            class Meta:
                name = "func"

            data: TableInput = Arg[TableInput](0, doc="Input")  # type: ignore[assignment]

        class OneArgFunc(TableInOutFunction):
            """One arg."""

            class Meta:
                name = "func"

            count = Arg[int](0, doc="Count")
            data: TableInput = Arg[TableInput](1, doc="Input")  # type: ignore[assignment]

        class TwoArgsFunc(TableInOutFunction):
            """Two args."""

            class Meta:
                name = "func"

            count = Arg[int](0, doc="Count")
            multiplier = Arg[int](1, doc="Multiplier")
            data: TableInput = Arg[TableInput](2, doc="Input")  # type: ignore[assignment]

        candidates = [NoArgsFunc, OneArgFunc, TwoArgsFunc]

        # No arguments -> NoArgsFunc
        inv0 = Invocation(
            function_name="func",
            arguments=Arguments(positional=()),
            input_schema=pa.schema([]),
            function_type=InvocationType.TABLE,
            correlation_id="test",
            invocation_id=b"test",
        )
        assert Worker._match_function(inv0, candidates) is NoArgsFunc

        # One argument -> OneArgFunc
        inv1 = Invocation(
            function_name="func",
            arguments=Arguments(positional=(pa.scalar(5),)),
            input_schema=pa.schema([]),
            function_type=InvocationType.TABLE,
            correlation_id="test",
            invocation_id=b"test",
        )
        assert Worker._match_function(inv1, candidates) is OneArgFunc

        # Two arguments -> TwoArgsFunc
        inv2 = Invocation(
            function_name="func",
            arguments=Arguments(positional=(pa.scalar(5), pa.scalar(10))),
            input_schema=pa.schema([]),
            function_type=InvocationType.TABLE,
            correlation_id="test",
            invocation_id=b"test",
        )
        assert Worker._match_function(inv2, candidates) is TwoArgsFunc

    def test_match_with_optional_args(self) -> None:
        """Match considers optional arguments with defaults."""

        class RequiredFunc(TableInOutFunction):
            """Required arg."""

            class Meta:
                name = "func"

            count = Arg[int](0, doc="Count")
            data: TableInput = Arg[TableInput](1, doc="Input")  # type: ignore[assignment]

        class OptionalFunc(TableInOutFunction):
            """Optional arg."""

            class Meta:
                name = "func"

            count = Arg[int](0, default=10, doc="Count")
            data: TableInput = Arg[TableInput](1, doc="Input")  # type: ignore[assignment]

        # With argument provided, both match (ambiguous)
        inv_with = Invocation(
            function_name="func",
            arguments=Arguments(positional=(pa.scalar(5),)),
            input_schema=pa.schema([]),
            function_type=InvocationType.TABLE,
            correlation_id="test",
            invocation_id=b"test",
        )
        with pytest.raises(ValueError, match="Ambiguous"):
            Worker._match_function(inv_with, [RequiredFunc, OptionalFunc])

        # Without argument, only OptionalFunc matches
        inv_without = Invocation(
            function_name="func",
            arguments=Arguments(positional=()),
            input_schema=pa.schema([]),
            function_type=InvocationType.TABLE,
            correlation_id="test",
            invocation_id=b"test",
        )
        result = Worker._match_function(inv_without, [RequiredFunc, OptionalFunc])
        assert result is OptionalFunc

    def test_match_by_named_args(self) -> None:
        """Match function by named argument keys."""

        class FormatFunc(TableInOutFunction):
            """Format func."""

            class Meta:
                name = "func"

            fmt = Arg[str]("format", doc="Format")
            data: TableInput = Arg[TableInput](0, doc="Input")  # type: ignore[assignment]

        class SepFunc(TableInOutFunction):
            """Separator func."""

            class Meta:
                name = "func"

            sep = Arg[str]("separator", doc="Separator")
            data: TableInput = Arg[TableInput](0, doc="Input")  # type: ignore[assignment]

        candidates = [FormatFunc, SepFunc]

        # Named arg "format" -> FormatFunc
        inv_format = Invocation(
            function_name="func",
            arguments=Arguments(positional=(), named={"format": pa.scalar("json")}),
            input_schema=pa.schema([]),
            function_type=InvocationType.TABLE,
            correlation_id="test",
            invocation_id=b"test",
        )
        assert Worker._match_function(inv_format, candidates) is FormatFunc

        # Named arg "separator" -> SepFunc
        inv_sep = Invocation(
            function_name="func",
            arguments=Arguments(positional=(), named={"separator": pa.scalar(",")}),
            input_schema=pa.schema([]),
            function_type=InvocationType.TABLE,
            correlation_id="test",
            invocation_id=b"test",
        )
        assert Worker._match_function(inv_sep, candidates) is SepFunc

    def test_no_match_raises_error(self) -> None:
        """ValueError raised when no function matches."""

        class OneArgFunc(TableInOutFunction):
            """One arg."""

            class Meta:
                name = "func"

            count = Arg[int](0, doc="Count")
            data: TableInput = Arg[TableInput](1, doc="Input")  # type: ignore[assignment]

        # Too many positional arguments
        inv = Invocation(
            function_name="func",
            arguments=Arguments(positional=(pa.scalar(1), pa.scalar(2), pa.scalar(3))),
            input_schema=pa.schema([]),
            function_type=InvocationType.TABLE,
            correlation_id="test",
            invocation_id=b"test",
        )
        with pytest.raises(ValueError, match="No matching function"):
            Worker._match_function(inv, [OneArgFunc])

    def test_no_match_error_shows_overloads(self) -> None:
        """Error message lists available overloads."""

        class OneArgFunc(TableInOutFunction):
            """One arg."""

            class Meta:
                name = "func"

            count = Arg[int](0, doc="Count")
            data: TableInput = Arg[TableInput](1, doc="Input")  # type: ignore[assignment]

        class TwoArgFunc(TableInOutFunction):
            """Two args."""

            class Meta:
                name = "func"

            x = Arg[int](0, doc="X")
            y = Arg[int](1, doc="Y")
            data: TableInput = Arg[TableInput](2, doc="Input")  # type: ignore[assignment]

        inv = Invocation(
            function_name="func",
            arguments=Arguments(positional=()),
            input_schema=pa.schema([]),
            function_type=InvocationType.TABLE,
            correlation_id="test",
            invocation_id=b"test",
        )
        with pytest.raises(ValueError, match="OneArgFunc") as exc_info:
            Worker._match_function(inv, [OneArgFunc, TwoArgFunc])
        assert "TwoArgFunc" in str(exc_info.value)

    def test_unknown_named_arg_rejects(self) -> None:
        """Function rejected if invocation has unknown named args."""

        class KnownArgFunc(TableInOutFunction):
            """Known arg."""

            class Meta:
                name = "func"

            fmt = Arg[str]("format", doc="Format")
            data: TableInput = Arg[TableInput](0, doc="Input")  # type: ignore[assignment]

        # Named arg "unknown" not in function
        inv = Invocation(
            function_name="func",
            arguments=Arguments(positional=(), named={"unknown": pa.scalar("x")}),
            input_schema=pa.schema([]),
            function_type=InvocationType.TABLE,
            correlation_id="test",
            invocation_id=b"test",
        )
        with pytest.raises(ValueError, match="No matching function"):
            Worker._match_function(inv, [KnownArgFunc])

    def test_missing_required_named_rejects(self) -> None:
        """Function rejected if required named arg missing."""

        class RequiredNamedFunc(TableInOutFunction):
            """Required named arg."""

            class Meta:
                name = "func"

            fmt = Arg[str]("format", doc="Format")  # Required (no default)
            data: TableInput = Arg[TableInput](0, doc="Input")  # type: ignore[assignment]

        # Missing required named arg
        inv = Invocation(
            function_name="func",
            arguments=Arguments(positional=(), named=None),
            input_schema=pa.schema([]),
            function_type=InvocationType.TABLE,
            correlation_id="test",
            invocation_id=b"test",
        )
        with pytest.raises(ValueError, match="No matching function"):
            Worker._match_function(inv, [RequiredNamedFunc])


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

        class Func1(TableInOutFunction):
            """Func1."""

            class Meta:
                name = "shared"

            data: TableInput = Arg[TableInput](0, doc="Input")  # type: ignore[assignment]

        class Func2(TableInOutFunction):
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
