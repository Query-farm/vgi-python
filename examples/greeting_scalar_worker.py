# /// script
# requires-python = ">=3.13"
# dependencies = ["vgi-python"]
# ///
"""Stage 1 of the tutorial: a worker with a single scalar function.

Run it from a DuckDB-compatible engine (Haybarn shown here)::

    uvx haybarn-cli
    ATTACH 'greetings' (TYPE vgi, LOCATION 'uv run greeting_scalar_worker.py');
    SELECT greetings.greeting('Alice');
"""

from typing import Annotated

import pyarrow as pa
import pyarrow.compute as pc

from vgi import Param, Returns, ScalarFunction, Worker
from vgi.catalog import Catalog, Schema


class Greeting(ScalarFunction):
    """Return a friendly greeting for each name (one row in, one row out)."""

    @classmethod
    def compute(
        cls,
        name: Annotated[pa.StringArray, Param(doc="Column of names to greet")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Join ``Hello, `` + name + ``!`` element-wise across the column."""
        return pc.binary_join_element_wise("Hello, ", name, "!", "")


class GreetingWorker(Worker):
    """A worker exposing the ``greetings`` catalog with one scalar function."""

    catalog = Catalog(
        name="greetings",
        schemas=[Schema(name="main", functions=[Greeting])],
    )


if __name__ == "__main__":
    GreetingWorker().run()
