# /// script
# requires-python = ">=3.13"
# dependencies = ["vgi-python"]
# ///
"""The full tutorial worker: a scalar function and a table function.

Run from a DuckDB-compatible engine (Haybarn shown here)::

    uvx haybarn-cli
    ATTACH 'calc' (TYPE vgi, LOCATION 'uv run calc_worker.py');
    SELECT calc.double(21);            -- scalar -> 42
    SELECT * FROM calc.series(3);       -- table  -> 0, 1, 2
"""

from dataclasses import dataclass
from typing import Annotated, ClassVar

import pyarrow as pa
import pyarrow.compute as pc
from vgi_rpc import ArrowSerializableDataclass

from vgi import Arg, Param, Returns, ScalarFunction, Worker
from vgi.catalog import Catalog, Schema
from vgi.table_function import (
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)


class Double(ScalarFunction):
    """Double each input value (one row in, one row out)."""

    @classmethod
    def compute(
        cls,
        value: Annotated[pa.Int64Array, Param(doc="Values to double")],
    ) -> Annotated[pa.Int64Array, Returns()]:
        """Multiply the whole column by 2."""
        return pc.multiply(value, 2)


@dataclass(slots=True, frozen=True, kw_only=True)
class SeriesArgs:
    """Arguments for :class:`Series` (one positional ``count``)."""

    count: Annotated[int, Arg(0, doc="How many numbers to generate", ge=0)]


@dataclass(kw_only=True)
class SeriesState(ArrowSerializableDataclass):
    """Per-invocation cursor tracking how many rows we've emitted so far.

    Extends ``ArrowSerializableDataclass`` so the cursor survives HTTP state
    round-trips (the framework requires this for table-generator state).
    """

    emitted: int = 0


@init_single_worker
@bind_fixed_schema
class Series(TableFunctionGenerator[SeriesArgs, SeriesState]):
    """Generate the integers ``0 .. count-1`` as a one-column table.

    ``@bind_fixed_schema`` derives the output schema from ``FIXED_SCHEMA``;
    ``@init_single_worker`` runs the generator in a single worker.
    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema([("n", pa.int64())])

    @classmethod
    def initial_state(cls, params: ProcessParams[SeriesArgs]) -> SeriesState:
        """Start a fresh cursor at zero."""
        return SeriesState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[SeriesArgs],
        state: SeriesState,
        out: OutputCollector,
    ) -> None:
        """Emit one batch per call; signal completion with ``out.finish()``."""
        if state.emitted >= params.args.count:
            out.finish()
            return
        batch_size = min(params.args.count - state.emitted, 1000)
        values = list(range(state.emitted, state.emitted + batch_size))
        out.emit(pa.RecordBatch.from_pydict({"n": values}, schema=params.output_schema))
        state.emitted += batch_size


class CalcWorker(Worker):
    """A worker exposing the ``calc`` catalog with both functions."""

    catalog = Catalog(
        name="calc",
        schemas=[Schema(name="main", functions=[Double, Series])],
    )


if __name__ == "__main__":
    CalcWorker().run()
