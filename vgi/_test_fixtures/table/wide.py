# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Very wide output schemas, for measuring per-column framework cost.

Most fixtures emit one or two columns, which is the cheapest possible case
for anything the framework does per *column* rather than per batch — building
the zero-row sentinel batch that carries a continuation token, scanning field
metadata for partition columns, converting a row dict through Arrow. A
one-column benchmark reports those as noise even when they dominate a real
workload.

This function makes the width a parameter so that cost is visible. Measured
on the zero-row sentinel alone: 3.9us at one column, 33.9us at twelve.

Deliberately **not** registered on ``ExampleWorker``. Adding a function there
changes the registered-function count that the C++ integration suite asserts
(see ``function_registration.test``), and this exists to be pointed at by a
benchmark, not to be part of the example surface.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Any

import numpy as np
import pyarrow as pa
from vgi_rpc.rpc import OutputCollector

from vgi._test_fixtures.table._common import CountdownState
from vgi.arguments import Arg
from vgi.invocation import BindResponse
from vgi.table_function import (
    BindParams,
    ProcessParams,
    TableFunctionGenerator,
    init_single_worker,
)

DEFAULT_COLUMNS = 2000
"""Wide enough that per-column work dominates per-batch work."""


@dataclass(frozen=True)
class WideArgs:
    """Arguments for :class:`WideFunction`."""

    count: Annotated[int, Arg(0, doc="Number of rows to generate", ge=0)]
    columns: Annotated[
        int,
        Arg("columns", default=DEFAULT_COLUMNS, doc="Number of output columns", ge=1),
    ] = DEFAULT_COLUMNS
    batch_size: Annotated[int, Arg("batch_size", default=64, doc="Rows per batch", ge=1)] = 64


@init_single_worker
class WideFunction(TableFunctionGenerator[WideArgs, CountdownState]):
    """Emit ``columns`` int64 columns per row.

    SQL::

        SELECT * FROM wide(1000, columns := 2000, batch_size := 64);

    The schema is derived from the ``columns`` argument at bind time rather
    than fixed, so one function covers the whole width range.

    Attributes:
        FunctionArguments: The argument dataclass bound to this function.

    """

    FunctionArguments = WideArgs

    class Meta:
        """Metadata for WideFunction."""

        name = "wide"
        description = "Generates a configurable number of int64 columns (benchmark fixture)"
        categories = ["test", "benchmark"]

    @classmethod
    def on_bind(cls, params: BindParams[WideArgs]) -> BindResponse:
        """Derive an ``n``-column int64 schema from the ``columns`` argument."""
        return BindResponse(output_schema=pa.schema([(f"c{i}", pa.int64()) for i in range(params.args.columns)]))

    @classmethod
    def initial_state(cls, params: ProcessParams[WideArgs]) -> CountdownState:
        """Start with the full row count outstanding."""
        return CountdownState(remaining=params.args.count)

    @classmethod
    def process(
        cls,
        params: ProcessParams[WideArgs],
        state: CountdownState,
        out: OutputCollector,
    ) -> None:
        """Emit one batch per tick until the row count is exhausted."""
        if state.remaining <= 0:
            out.finish()
            return
        rows = min(params.args.batch_size, state.remaining)
        base = np.arange(state.current_index, state.current_index + rows, dtype=np.int64)
        # Same column values throughout: this fixture measures per-column
        # framework overhead, not data generation.
        column: Any = pa.array(base)
        batch = pa.RecordBatch.from_arrays(
            [column] * len(params.output_schema),
            schema=params.output_schema,
        )
        state.remaining -= rows
        state.current_index += rows
        out.emit(batch)
