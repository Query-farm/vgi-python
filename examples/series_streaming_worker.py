# /// script
# requires-python = ">=3.13"
# dependencies = ["vgi-python"]
# ///
"""A table function that streams its output in chunks, using generator state.

The tutorial's ``series`` emits everything in one call. When the output is large
you instead emit a bounded batch per ``process`` call and remember your place in
**state** — the framework calls ``process`` repeatedly until you ``out.finish()``.

    ATTACH 'calc' (TYPE vgi, LOCATION 'uv run series_streaming_worker.py');
    SELECT * FROM calc.series(1000000);   -- streamed 10k rows at a time
"""

from dataclasses import dataclass
from typing import Annotated, ClassVar

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass

from vgi import Arg, Worker
from vgi.catalog import Catalog, Schema
from vgi.table_function import (
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)

CHUNK = 10_000


@dataclass(slots=True, frozen=True, kw_only=True)
class SeriesArgs:
    """Arguments for :class:`Series` (one positional ``count``)."""

    count: Annotated[int, Arg(0, doc="How many numbers to generate", ge=0)]


@dataclass(kw_only=True)
class SeriesState(ArrowSerializableDataclass):
    """Cursor remembering how many rows we've emitted across ``process`` calls.

    Extends ``ArrowSerializableDataclass`` so the cursor survives HTTP state
    round-trips (the framework requires serializable state for generators).
    """

    emitted: int = 0


@init_single_worker
@bind_fixed_schema
class Series(TableFunctionGenerator[SeriesArgs, SeriesState]):
    """Generate ``0 .. count-1`` in chunks, keeping a cursor in state."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema([("n", pa.int64())])

    @classmethod
    def initial_state(cls, params: ProcessParams[SeriesArgs]) -> SeriesState:
        """Start a fresh cursor at zero."""
        return SeriesState()

    @classmethod
    def process(cls, params: ProcessParams[SeriesArgs], state: SeriesState, out: OutputCollector) -> None:
        """Emit one bounded chunk per call; finish when the cursor reaches count."""
        if state.emitted >= params.args.count:
            out.finish()
            return
        batch_size = min(params.args.count - state.emitted, CHUNK)
        values = list(range(state.emitted, state.emitted + batch_size))
        out.emit(pa.RecordBatch.from_pydict({"n": values}, schema=params.output_schema))
        state.emitted += batch_size


class CalcWorker(Worker):
    """A worker exposing the ``calc`` catalog with the streaming series."""

    catalog = Catalog(
        name="calc",
        schemas=[Schema(name="main", functions=[Series])],
    )


if __name__ == "__main__":
    CalcWorker().run()
