# /// script
# requires-python = ">=3.13"
# dependencies = ["vgi-python"]
# ///
"""Stage 1 of the tutorial: a worker with a single scalar function.

Run it from a DuckDB-compatible engine (Haybarn shown here)::

    uvx haybarn-cli
    ATTACH 'calc' (TYPE vgi, LOCATION 'uv run calc_scalar_worker.py');
    SELECT calc.double(21);   -- 42
"""

from typing import Annotated

import pyarrow as pa
import pyarrow.compute as pc

from vgi import Param, Returns, ScalarFunction, Worker
from vgi.catalog import Catalog, Schema


class Double(ScalarFunction):
    """Double each input value (one row in, one row out)."""

    @classmethod
    def compute(
        cls,
        value: Annotated[pa.Int64Array, Param(doc="Values to double")],
    ) -> Annotated[pa.Int64Array, Returns()]:
        """Multiply the whole column by 2."""
        return pc.multiply(value, 2)


class CalcWorker(Worker):
    """A worker exposing the ``calc`` catalog with one scalar function."""

    catalog = Catalog(
        name="calc",
        schemas=[Schema(name="main", functions=[Double])],
    )


if __name__ == "__main__":
    CalcWorker().run()
