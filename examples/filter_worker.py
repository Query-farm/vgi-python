# /// script
# requires-python = ">=3.13"
# dependencies = ["vgi-python"]
# ///
"""A table-in-out function: keep only rows where ``value`` is positive.

Table-in-out functions stream an input table through, batch by batch, emitting
transformed output. Run from a DuckDB-compatible engine::

    ATTACH 'filters' (TYPE vgi, LOCATION 'uv run filter_worker.py');
    SELECT * FROM filters.filter_positive((SELECT * FROM my_table));
"""

from dataclasses import dataclass
from typing import Annotated

import pyarrow as pa
import pyarrow.compute as pc

from vgi import Arg, Worker
from vgi.arguments import TableInput
from vgi.catalog import Catalog, Schema
from vgi.invocation import BindResponse
from vgi.table_function import BindParams, ProcessParams
from vgi.table_in_out_function import OutputCollector, TableInOutGenerator


@dataclass(slots=True, frozen=True, kw_only=True)
class FilterArgs:
    """Arguments: a single input table."""

    data: Annotated[TableInput, Arg(0, doc="Input table to filter")]


class FilterPositive(TableInOutGenerator[FilterArgs]):
    """Emit only the input rows whose ``value`` column is greater than zero."""

    @classmethod
    def on_bind(cls, params: BindParams[FilterArgs]) -> BindResponse:
        """Output schema equals the input schema (rows are filtered, not reshaped)."""
        assert params.bind_call.input_schema is not None
        return BindResponse(output_schema=params.bind_call.input_schema)

    @classmethod
    def process(
        cls,
        params: ProcessParams[FilterArgs],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        """Filter each input batch and emit the surviving rows."""
        mask = pc.greater(batch.column("value"), pa.scalar(0, type=batch.column("value").type))
        out.emit(batch.filter(mask))


class FilterWorker(Worker):
    """A worker exposing the ``filters`` catalog."""

    catalog = Catalog(
        name="filters",
        schemas=[Schema(name="main", functions=[FilterPositive])],
    )


if __name__ == "__main__":
    FilterWorker().run()
