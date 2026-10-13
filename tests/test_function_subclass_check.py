# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Regression tests: ``issubclass`` against a function base must not scale with its subclasses.

Found building a worker that generates one function class per AWS API operation
(~8,000 of them). Every function base is an ``ABC``, and a *failed*
``issubclass(X, Base)`` makes ``ABCMeta`` ask ``issubclass(X, S)`` of every
subclass ``S`` of ``Base`` in search of a virtual subclass. The framework asks
"is this a ``RowTransformFunction``?" of each table-backed scan, so declaring
N tables cost N x (number of row-transform classes) checks: 2.6 s for 4,000
functions, 9 s of a 12 s ATTACH for 8,000.

``Function.__subclasshook__`` answers from the MRO instead. These tests pin the
cost (one check, whatever the number of sibling classes) and the answers.
"""

from __future__ import annotations

import abc
from dataclasses import dataclass
from typing import Annotated, Any

import pyarrow as pa
import pytest

from vgi import Arg
from vgi.aggregate_function import AggregateFunction
from vgi.function import Function
from vgi.invocation import BindResponse
from vgi.scalar_function import ScalarFunctionGenerator
from vgi.table_function import (
    BindParams,
    OutputCollector,
    ProcessParams,
    TableFunctionBase,
    TableFunctionGenerator,
    init_single_worker,
)
from vgi.table_in_out_function import RowTransformFunction, TableInOutGenerator

_SCHEMA = pa.schema([("value", pa.string())])


@dataclass(slots=True, frozen=True, kw_only=True)
class _NoArgs:
    pass


@dataclass(slots=True, frozen=True, kw_only=True)
class _KeyArgs:
    key: Annotated[str, Arg(0, doc="Lookup key", arrow_type=pa.string())]


def _make_scan(index: int) -> type[TableFunctionGenerator[Any, Any]]:
    @init_single_worker
    class Scan(TableFunctionGenerator[_NoArgs, None]):
        """A generated scan."""

        class Meta:
            name = f"subclass_check_scan_{index}"

        @classmethod
        def on_bind(cls, params: BindParams[_NoArgs]) -> BindResponse:
            return BindResponse(output_schema=_SCHEMA)

        @classmethod
        def process(cls, params: ProcessParams[_NoArgs], state: None, out: OutputCollector) -> None:
            out.finish()

    return Scan


def _make_row_transform(index: int) -> type[RowTransformFunction[Any]]:
    class Lookup(RowTransformFunction[_KeyArgs]):
        """A generated row transform."""

        class Meta:
            name = f"subclass_check_lookup_{index}"

        @classmethod
        def on_bind(cls, params: BindParams[_KeyArgs]) -> BindResponse:
            return BindResponse(output_schema=_SCHEMA)

        @classmethod
        def process(
            cls, params: ProcessParams[_KeyArgs], state: None, batch: pa.RecordBatch, out: OutputCollector
        ) -> None:
            out.emit(pa.RecordBatch.from_pylist([], schema=_SCHEMA))

    return Lookup


@pytest.fixture
def count_subclass_checks(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """Count every ``ABCMeta.__subclasscheck__`` call, including ABCMeta's own recursion."""
    calls = [0]
    original = abc.ABCMeta.__subclasscheck__

    def counting(cls: abc.ABCMeta, subclass: type) -> bool:
        calls[0] += 1
        return original(cls, subclass)

    monkeypatch.setattr(abc.ABCMeta, "__subclasscheck__", counting)
    return calls


def test_failed_issubclass_does_not_visit_sibling_classes(count_subclass_checks: list[int]) -> None:
    """A scan is not a RowTransformFunction: that takes one check, not one per row-transform class."""
    row_transforms = [_make_row_transform(i) for i in range(50)]
    scan = _make_scan(0)

    count_subclass_checks[0] = 0
    assert not issubclass(scan, RowTransformFunction)
    assert count_subclass_checks[0] == 1

    count_subclass_checks[0] = 0
    assert not scan._is_blended()
    assert count_subclass_checks[0] == 1

    assert all(issubclass(cls, RowTransformFunction) for cls in row_transforms)


def test_issubclass_answers_follow_the_class_hierarchy() -> None:
    """The MRO-based answer agrees with real inheritance in both directions."""
    scan = _make_scan(1)
    lookup = _make_row_transform(1)

    for base in (Function, TableFunctionBase, TableFunctionGenerator):
        assert issubclass(scan, base)
    for base in (TableInOutGenerator, RowTransformFunction, ScalarFunctionGenerator, AggregateFunction):
        assert not issubclass(scan, base)

    for base in (Function, TableFunctionBase, TableInOutGenerator, RowTransformFunction):
        assert issubclass(lookup, base)
    for base in (TableFunctionGenerator, ScalarFunctionGenerator, AggregateFunction):
        assert not issubclass(lookup, base)

    assert lookup._is_blended()
    assert not issubclass(scan, lookup)
    assert not issubclass(int, Function)
    assert not isinstance(object(), Function)


def test_registering_a_virtual_subclass_has_no_effect() -> None:
    """Function classes are real subclasses; ``ABC.register`` cannot make one."""

    class NotAFunction:
        pass

    RowTransformFunction.register(NotAFunction)
    assert not issubclass(NotAFunction, RowTransformFunction)
