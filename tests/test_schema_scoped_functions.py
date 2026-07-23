# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""A function name may be registered in more than one catalog schema.

The bare name is therefore not a unique key: the worker resolves the pair
``(BindRequest.schema_name, BindRequest.function_name)``. These tests pin that
behaviour on the Python side; ``vgi/test/sql/integration/scalar/
same_name_schemas.test`` covers the same ground end-to-end through DuckDB.
"""

from __future__ import annotations

import dataclasses
from typing import Annotated

import pyarrow as pa
import pytest

from vgi._test_fixtures.scalar.same_name import SameNameDataFunction, SameNameMainFunction
from vgi._test_fixtures.twin_catalogs import TwinAFunction, TwinAWorker, TwinBFunction, TwinBWorker
from vgi._test_fixtures.worker import ExampleWorker
from vgi.arguments import Arguments, Param, Returns
from vgi.catalog.descriptors import Catalog, Schema
from vgi.meta_worker import MetaWorker
from vgi.protocol import BindRequest, FunctionType
from vgi.scalar_function import ScalarFunction
from vgi.worker import Worker

_INPUT_SCHEMA = pa.schema([pa.field("value", pa.int64())])


def _bind_request(schema_name: str | None) -> BindRequest:
    return BindRequest(
        function_name="test_same_name_bind",
        arguments=Arguments(positional=()),
        function_type=FunctionType.SCALAR,
        input_schema=_INPUT_SCHEMA,
        schema_name=schema_name,
    )


class TestSchemaScopedResolution:
    """The example worker declares ``test_same_name_bind`` in `main` and `data`."""

    def test_main_schema_resolves_to_main_implementation(self) -> None:
        """A `main`-qualified bind reaches the `main` class."""
        worker = ExampleWorker()
        assert worker._resolve_function(_bind_request("main")) is SameNameMainFunction

    def test_data_schema_resolves_to_data_implementation(self) -> None:
        """A `data`-qualified bind reaches the `data` class."""
        worker = ExampleWorker()
        assert worker._resolve_function(_bind_request("data")) is SameNameDataFunction

    def test_schema_lookup_is_case_insensitive(self) -> None:
        """DuckDB lowercases unquoted identifiers; a quoted "Main" must still match."""
        worker = ExampleWorker()
        assert worker._resolve_function(_bind_request("MAIN")) is SameNameMainFunction

    def test_unqualified_call_reports_the_cross_schema_ambiguity(self) -> None:
        """Without a schema the name is genuinely ambiguous — say so, actionably."""
        worker = ExampleWorker()
        with pytest.raises(ValueError, match="Ambiguous function call") as exc_info:
            worker._resolve_function(_bind_request(None))
        message = str(exc_info.value)
        assert "different schemas" in message
        assert "qualify the call with a schema" in message

    def test_naming_a_schema_without_the_function_lists_where_it_lives(self) -> None:
        """A wrong schema names the schemas that do hold the function."""
        worker = ExampleWorker()
        with pytest.raises(ValueError, match="not registered in schema 'nope'") as exc_info:
            worker._resolve_function(_bind_request("nope"))
        assert "['data', 'main']" in str(exc_info.value)

    def test_registry_keeps_one_bucket_per_schema(self) -> None:
        """The (schema, name) index does not merge the two declarations."""
        registry = ExampleWorker._build_schema_registry()
        assert registry[("main", "test_same_name_bind")] == [SameNameMainFunction]
        assert registry[("data", "test_same_name_bind")] == [SameNameDataFunction]


class _Uncontested(ScalarFunction):
    """A name declared in exactly one schema."""

    class Meta:
        """Function metadata."""

        name = "only_here"

    @classmethod
    def compute(
        cls,
        value: Annotated[pa.Int64Array, Param()],
    ) -> Annotated[pa.Int64Array, Returns()]:
        return value


class _OnlyHereWorker(Worker):
    """Declares its single function in a schema that isn't named `main`."""

    catalog = Catalog(name="probe", default_schema="side", schemas=[Schema(name="side", functions=[_Uncontested])])


class TestUnambiguousNames:
    """Scoping must not make single-schema lookups harder than they were."""

    def test_resolves_without_a_schema(self) -> None:
        """The pure-Python Client sends no schema; a unique name still resolves."""
        worker = _OnlyHereWorker()
        request = BindRequest(
            function_name="only_here",
            arguments=Arguments(positional=()),
            function_type=FunctionType.SCALAR,
            input_schema=_INPUT_SCHEMA,
        )
        assert worker._resolve_function(request) is _Uncontested

    def test_resolves_with_its_own_schema(self) -> None:
        """Qualifying with the declaring schema resolves too."""
        worker = _OnlyHereWorker()
        request = BindRequest(
            function_name="only_here",
            arguments=Arguments(positional=()),
            function_type=FunctionType.SCALAR,
            input_schema=_INPUT_SCHEMA,
            schema_name="side",
        )
        assert worker._resolve_function(request) is _Uncontested


class _LegacyWorker(Worker):
    """The legacy ``functions`` list has no schema of its own."""

    catalog_name = "legacy"
    functions = [_Uncontested]


class TestLegacyFunctionsList:
    """Legacy-list functions are registered into the catalog's default schema."""

    def test_default_schema_qualified_lookup_succeeds(self) -> None:
        """DuckDB registers them into `main`, so `main` must find them."""
        worker = _LegacyWorker()
        request = BindRequest(
            function_name="only_here",
            arguments=Arguments(positional=()),
            function_type=FunctionType.SCALAR,
            input_schema=_INPUT_SCHEMA,
            schema_name="main",
        )
        assert worker._resolve_function(request) is _Uncontested


class TestCrossCatalogResolution:
    """Two catalogs in one worker process, colliding on schema *and* function name.

    Only the catalog name sealed into ``attach_opaque_data`` distinguishes them,
    so it is the routing key.
    ``vgi/test/sql/integration/scalar/same_name_catalogs.test`` drives the same
    scenario end-to-end through DuckDB.
    """

    @staticmethod
    def _meta() -> MetaWorker:
        return MetaWorker([TwinAWorker(), TwinBWorker()])

    @staticmethod
    def _request(attach_opaque_data: bytes | None) -> BindRequest:
        return BindRequest(
            function_name="test_same_name_catalog",
            arguments=Arguments(positional=()),
            function_type=FunctionType.SCALAR,
            input_schema=_INPUT_SCHEMA,
            schema_name="main",
            attach_opaque_data=attach_opaque_data,
        )

    def test_attach_opaque_data_selects_the_catalog(self) -> None:
        """The catalog name sealed into the attach routes to that catalog's class."""
        meta = self._meta()
        for index, catalog, expected in ((0, "twin_a", TwinAFunction), (1, "twin_b", TwinBFunction)):
            attach = meta._workers[index]._seal_attach_with_catalog(b"\x00" * 16, catalog)
            assert meta._resolve_function(self._request(attach)) is expected

    def test_routing_survives_the_strip_that_http_serialization_performs(self) -> None:
        """A rehydrated attach still routes — the regression behind the twin bug.

        HTTP stream state persists the attach the sub-worker was handed, then
        ``rehydrate`` re-resolves against the MetaWorker. When routing lived in a
        prefix that ``init`` stripped before the sub-worker saw it, that
        round-trip lost the key and silently resolved to whichever sub-worker
        declared the name first. The name is inside the sealed plaintext now, so
        the value that gets persisted is itself routable.
        """
        meta = self._meta()
        for index, catalog, expected in ((0, "twin_a", TwinAFunction), (1, "twin_b", TwinBFunction)):
            worker = meta._workers[index]
            attach = worker._seal_attach_with_catalog(b"\x00" * 16, catalog)
            # What init() hands the sub-worker is what ends up serialized.
            assert meta._maybe_worker_for_attach(attach) is worker
            assert meta._resolve_function(self._request(attach)) is expected

    def test_both_catalogs_declare_the_same_schema_and_name(self) -> None:
        """The fixture is only meaningful if the collision is total."""
        key = ("main", "test_same_name_catalog")
        assert TwinAWorker._build_schema_registry()[key] == [TwinAFunction]
        assert TwinBWorker._build_schema_registry()[key] == [TwinBFunction]

    def test_without_an_attach_an_ambiguous_name_raises(self) -> None:
        """No routing key and two declarers — refuse rather than guess.

        This previously returned ``TwinAFunction``: whichever sub-worker declared
        the name first. That is what made ``b.main.test_same_name_catalog(1)``
        answer ``twin_a:1`` over HTTP instead of failing, so a routing bug read
        as a plausible result. An attach-less call to an ambiguous name means the
        key was lost upstream, which is worth surfacing.
        """
        meta = self._meta()
        with pytest.raises(ValueError, match="Cannot route"):
            meta._resolve_function(self._request(None))

    def test_without_an_attach_an_unambiguous_name_still_resolves(self) -> None:
        """The legitimate attach-less path is untouched.

        Non-catalog callers (``Worker.functions``) reach ``_resolve_function``
        with no attach at all. With a single declarer there is no ambiguity to
        refuse, so resolution must still succeed — the guard keys off the
        candidate count, not off the attach being absent.
        """
        meta = MetaWorker([TwinAWorker()])
        assert meta._resolve_function(self._request(None)) is TwinAFunction

    def test_an_unknown_name_reports_unknown_not_ambiguous(self) -> None:
        """A name no sub-worker declares is still a plain 'unknown function'."""
        meta = self._meta()
        request = dataclasses.replace(self._request(None), function_name="no_such_function")
        with pytest.raises(ValueError, match="Unknown function"):
            meta._resolve_function(request)
