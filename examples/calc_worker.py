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


@init_single_worker
@bind_fixed_schema
class Series(TableFunctionGenerator[SeriesArgs]):
    """Generate the integers ``0 .. count-1`` as a one-column table.

    Stateless: it emits every row in a single ``process`` call, then finishes.
    ``@bind_fixed_schema`` derives the output schema from ``FIXED_SCHEMA``;
    ``@init_single_worker`` runs the generator in a single worker.
    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema([("n", pa.int64())])

    @classmethod
    def process(cls, params: ProcessParams[SeriesArgs], state: None, out: OutputCollector) -> None:
        """Emit all rows at once, then signal completion."""
        out.emit(pa.RecordBatch.from_pydict({"n": list(range(params.args.count))}, schema=params.output_schema))
        out.finish()


class CalcWorker(Worker):
    """A worker exposing the ``calc`` catalog with both functions."""

    catalog = Catalog(
        name="calc",
        schemas=[Schema(name="main", functions=[Double, Series])],
    )


if __name__ == "__main__":
    CalcWorker().run()
