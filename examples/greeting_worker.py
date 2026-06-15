# /// script
# requires-python = ">=3.13"
# dependencies = ["vgi-python"]
# ///
"""A minimal VGI worker: one scalar function and one table function.

This is the worker built in the getting-started tutorial. Run it from a
DuckDB-compatible engine (Haybarn shown here)::

    uvx haybarn-cli
    ATTACH 'greetings' (TYPE vgi, LOCATION 'uv run examples/greeting_worker.py');
    SELECT greetings.greeting('Alice');              -- scalar
    SELECT * FROM greetings.greeting_series(3);       -- table
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


class Greeting(ScalarFunction):
    """Return a friendly greeting for each name (one row in, one row out)."""

    @classmethod
    def compute(
        cls,
        name: Annotated[pa.StringArray, Param(doc="Column of names to greet")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Join ``Hello, `` + name + ``!`` element-wise across the column."""
        return pc.binary_join_element_wise("Hello, ", name, "!", "")


@dataclass(slots=True, frozen=True, kw_only=True)
class GreetingSeriesArgs:
    """Arguments for :class:`GreetingSeries` (one positional ``count``)."""

    count: Annotated[int, Arg(0, doc="How many greetings to generate", ge=0)]


@dataclass(kw_only=True)
class GreetingSeriesState(ArrowSerializableDataclass):
    """Per-invocation cursor tracking how many rows we've emitted so far.

    Extends ``ArrowSerializableDataclass`` so the cursor survives HTTP state
    round-trips (the framework requires this for table-generator state).
    """

    emitted: int = 0


@init_single_worker
@bind_fixed_schema
class GreetingSeries(TableFunctionGenerator[GreetingSeriesArgs, GreetingSeriesState]):
    """Generate ``count`` numbered greetings as a table (no input table).

    ``@bind_fixed_schema`` derives the bind-time output schema from
    ``FIXED_SCHEMA``; ``@init_single_worker`` runs the generator in a single
    worker (no distributed init).
    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema([("greeting", pa.string())])

    @classmethod
    def initial_state(cls, params: ProcessParams[GreetingSeriesArgs]) -> GreetingSeriesState:
        """Start a fresh cursor at zero."""
        return GreetingSeriesState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[GreetingSeriesArgs],
        state: GreetingSeriesState,
        out: OutputCollector,
    ) -> None:
        """Emit one batch per call; signal completion with ``out.finish()``."""
        if state.emitted >= params.args.count:
            out.finish()
            return
        batch_size = min(params.args.count - state.emitted, 1000)
        greetings = [f"Hello, friend #{i}!" for i in range(state.emitted, state.emitted + batch_size)]
        out.emit(pa.RecordBatch.from_pydict({"greeting": greetings}, schema=params.output_schema))
        state.emitted += batch_size


class GreetingWorker(Worker):
    """A worker exposing the ``greetings`` catalog with both functions."""

    catalog = Catalog(
        name="greetings",
        schemas=[Schema(name="main", functions=[Greeting, GreetingSeries])],
    )


if __name__ == "__main__":
    GreetingWorker().run()
