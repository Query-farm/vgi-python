# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""make_series_* generators (count/range/step/csv/float)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Annotated, ClassVar

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi.arguments import Arg
from vgi.metadata import FunctionExample
from vgi.schema_utils import schema
from vgi.table_function import (
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)

# ============================================================================

MAKE_SERIES_SCHEMA = schema(value=pa.int64())


@dataclass(kw_only=True)
class MakeSeriesCountArgs:
    """Arguments for MakeSeriesCountFunction."""

    count: Annotated[int, Arg(0, doc="Number of values to generate", ge=0)]


@dataclass(kw_only=True)
class MakeSeriesRangeArgs:
    """Arguments for MakeSeriesRangeFunction."""

    start: Annotated[int, Arg(0, doc="Start value (inclusive)")]
    stop: Annotated[int, Arg(1, doc="Stop value (exclusive)")]


@dataclass(kw_only=True)
class MakeSeriesStepArgs:
    """Arguments for MakeSeriesStepFunction."""

    start: Annotated[int, Arg(0, doc="Start value (inclusive)")]
    stop: Annotated[int, Arg(1, doc="Stop value (exclusive)")]
    step: Annotated[int, Arg(2, doc="Step between values", ge=1)]


@dataclass(kw_only=True)
class MakeSeriesState(ArrowSerializableDataclass):
    """Mutable state for make_series functions."""

    values: list[int]
    offset: int = 0


def _make_series_emit(state: MakeSeriesState, out: OutputCollector) -> None:
    """Shared process logic for all make_series overloads."""
    if state.offset >= len(state.values):
        out.finish()
        return
    batch_values = state.values[state.offset : state.offset + 1024]
    out.emit(pa.RecordBatch.from_pydict({"value": batch_values}, schema=MAKE_SERIES_SCHEMA))
    state.offset += len(batch_values)


@init_single_worker
@bind_fixed_schema
class MakeSeriesCountFunction(TableFunctionGenerator[MakeSeriesCountArgs, MakeSeriesState]):
    """Generate a series of integers from 0 to count-1.

    Example:
        SELECT * FROM make_series(5)
        Returns: 0, 1, 2, 3, 4

    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = MAKE_SERIES_SCHEMA

    class Meta:
        """Function metadata."""

        name = "make_series"
        description = "Generate integers from 0 to count-1"
        examples = [
            FunctionExample(
                sql="SELECT * FROM make_series(5)",
                description="Generate 0..4",
            ),
        ]

    @classmethod
    def initial_state(cls, params: ProcessParams[MakeSeriesCountArgs]) -> MakeSeriesState:
        """Build the full value list."""
        return MakeSeriesState(values=list(range(params.args.count)))

    @classmethod
    def process(cls, params: ProcessParams[MakeSeriesCountArgs], state: MakeSeriesState, out: OutputCollector) -> None:
        """Emit values in batches."""
        _make_series_emit(state, out)


@init_single_worker
@bind_fixed_schema
class MakeSeriesRangeFunction(TableFunctionGenerator[MakeSeriesRangeArgs, MakeSeriesState]):
    """Generate a series of integers from start to stop-1.

    Example:
        SELECT * FROM make_series(3, 7)
        Returns: 3, 4, 5, 6

    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = MAKE_SERIES_SCHEMA

    class Meta:
        """Function metadata."""

        name = "make_series"
        description = "Generate integers from start to stop-1"
        examples = [
            FunctionExample(
                sql="SELECT * FROM make_series(3, 7)",
                description="Generate 3..6",
            ),
        ]

    @classmethod
    def initial_state(cls, params: ProcessParams[MakeSeriesRangeArgs]) -> MakeSeriesState:
        """Build the value list from start..stop."""
        return MakeSeriesState(values=list(range(params.args.start, params.args.stop)))

    @classmethod
    def process(cls, params: ProcessParams[MakeSeriesRangeArgs], state: MakeSeriesState, out: OutputCollector) -> None:
        """Emit values in batches."""
        _make_series_emit(state, out)


@init_single_worker
@bind_fixed_schema
class MakeSeriesStepFunction(TableFunctionGenerator[MakeSeriesStepArgs, MakeSeriesState]):
    """Generate a series of integers from start to stop-1 with step.

    Example:
        SELECT * FROM make_series(0, 10, 3)
        Returns: 0, 3, 6, 9

    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = MAKE_SERIES_SCHEMA

    class Meta:
        """Function metadata."""

        name = "make_series"
        description = "Generate integers from start to stop-1 with step"
        examples = [
            FunctionExample(
                sql="SELECT * FROM make_series(0, 10, 3)",
                description="Generate 0, 3, 6, 9",
            ),
        ]

    @classmethod
    def initial_state(cls, params: ProcessParams[MakeSeriesStepArgs]) -> MakeSeriesState:
        """Build the value list with step."""
        return MakeSeriesState(values=list(range(params.args.start, params.args.stop, params.args.step)))

    @classmethod
    def process(cls, params: ProcessParams[MakeSeriesStepArgs], state: MakeSeriesState, out: OutputCollector) -> None:
        """Emit values in batches."""
        _make_series_emit(state, out)


# ============================================================================


@dataclass(kw_only=True)
class MakeSeriesCsvArgs:
    """Arguments for MakeSeriesCsvFunction."""

    values: Annotated[str, Arg(0, doc="Comma-separated integers")]


@init_single_worker
@bind_fixed_schema
class MakeSeriesCsvFunction(TableFunctionGenerator[MakeSeriesCsvArgs, MakeSeriesState]):
    """Parse a CSV string of integers into rows.

    Example:
        SELECT * FROM make_series('10,20,30')
        Returns: 10, 20, 30

    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = MAKE_SERIES_SCHEMA

    class Meta:
        """Function metadata."""

        name = "make_series"
        description = "Parse comma-separated integers into rows"

    @classmethod
    def initial_state(cls, params: ProcessParams[MakeSeriesCsvArgs]) -> MakeSeriesState:
        """Parse CSV string into value list."""
        return MakeSeriesState(values=[int(x.strip()) for x in params.args.values.split(",")])

    @classmethod
    def process(cls, params: ProcessParams[MakeSeriesCsvArgs], state: MakeSeriesState, out: OutputCollector) -> None:
        """Emit values in batches."""
        _make_series_emit(state, out)


MAKE_SERIES_FLOAT_SCHEMA = schema(value=pa.float64())


@dataclass(kw_only=True)
class MakeSeriesFloatArgs:
    """Arguments for MakeSeriesFloatFunction."""

    step: Annotated[float, Arg(0, doc="Step size between values")]


@dataclass(kw_only=True)
class MakeSeriesFloatState(ArrowSerializableDataclass):
    """State for float make_series."""

    values: list[float] = field(default_factory=list)
    offset: int = 0


@init_single_worker
@bind_fixed_schema
class MakeSeriesFloatFunction(TableFunctionGenerator[MakeSeriesFloatArgs, MakeSeriesFloatState]):
    """Generate 10 float values: 0.0, step, 2*step, ..., 9*step.

    Example:
        SELECT * FROM make_series(0.5)
        Returns: 0.0, 0.5, 1.0, ..., 4.5

    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = MAKE_SERIES_FLOAT_SCHEMA

    class Meta:
        """Function metadata."""

        name = "make_series"
        description = "Generate 10 float values with given step size"

    @classmethod
    def initial_state(cls, params: ProcessParams[MakeSeriesFloatArgs]) -> MakeSeriesFloatState:
        """Build float value list."""
        return MakeSeriesFloatState(values=[i * params.args.step for i in range(10)])

    @classmethod
    def process(
        cls, params: ProcessParams[MakeSeriesFloatArgs], state: MakeSeriesFloatState, out: OutputCollector
    ) -> None:
        """Emit values in batches."""
        if state.offset >= len(state.values):
            out.finish()
            return
        batch_size = 1024
        end = min(state.offset + batch_size, len(state.values))
        chunk = state.values[state.offset : end]
        state.offset = end
        out.emit(pa.RecordBatch.from_pydict({"value": chunk}, schema=MAKE_SERIES_FLOAT_SCHEMA))
