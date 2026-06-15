# /// script
# requires-python = ">=3.13"
# dependencies = ["vgi-python"]
# ///
"""A buffering function: count every input row, then emit one total.

A buffering function must see the *whole* input before it can produce output —
the basis for sorts, top-k, and full-stream reductions. It runs in three phases:

- **sink** (``process``): called per input batch; stash a partial in shared
  storage and return a ``state_id``.
- **combine**: called once after all input; reduce the partials into a result.
- **source** (``finalize``): called per tick to stream the result out.

State crosses process boundaries, so it lives in ``params.storage`` (scoped by
``execution_id``), not in memory.

    ATTACH 'buffers' (TYPE vgi, LOCATION 'uv run row_count_worker.py');
    SELECT * FROM buffers.row_count((SELECT * FROM big_table));
"""

from dataclasses import dataclass
from typing import Annotated

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass

from vgi import Arg, Worker
from vgi.arguments import TableInput
from vgi.catalog import Catalog, Schema
from vgi.invocation import BindResponse
from vgi.table_buffering_function import OutputCollector, TableBufferingFunction, TableBufferingParams
from vgi.table_function import BindParams

_RESULT = pa.schema([("count", pa.int64())])


@dataclass(slots=True, frozen=True, kw_only=True)
class RowCountArgs:
    """Arguments: a single input table to count."""

    data: Annotated[TableInput, Arg(0, doc="Input table")]


@dataclass(kw_only=True)
class DrainState(ArrowSerializableDataclass):
    """Per-finalize-stream cursor: emit the total once, then finish."""

    done: bool = False


class RowCount(TableBufferingFunction[RowCountArgs, DrainState]):
    """Count all input rows and emit a single ``count`` row."""

    class Meta:
        """Function metadata."""

        name = "row_count"

    @classmethod
    def on_bind(cls, params: BindParams[RowCountArgs]) -> BindResponse:
        """Output is one int64 column regardless of input shape."""
        return BindResponse(output_schema=_RESULT)

    @classmethod
    def process(cls, batch: pa.RecordBatch, params: TableBufferingParams[RowCountArgs]) -> bytes:
        """Sink: stash this batch's row count; one bucket per execution."""
        params.storage.state_append(b"counts", b"", batch.num_rows.to_bytes(8, "little"))
        return params.execution_id

    @classmethod
    def combine(cls, state_ids: list[bytes], params: TableBufferingParams[RowCountArgs]) -> list[bytes]:
        """Combine: sum the partial counts into a single result."""
        total = sum(int.from_bytes(v, "little") for _id, v in params.storage.state_log_scan(b"counts", b""))
        params.storage.state_append(b"result", b"", total.to_bytes(8, "little"))
        return [params.execution_id]

    @classmethod
    def initial_finalize_state(cls, finalize_state_id: bytes, params: TableBufferingParams[RowCountArgs]) -> DrainState:
        """One cursor per finalize stream."""
        return DrainState()

    @classmethod
    def finalize(
        cls,
        params: TableBufferingParams[RowCountArgs],
        finalize_state_id: bytes,
        state: DrainState,
        out: OutputCollector,
    ) -> None:
        """Source: emit the total once, then signal completion."""
        if state.done:
            out.finish()
            return
        rows = params.storage.state_log_scan(b"result", b"")
        total = int.from_bytes(rows[-1][1], "little") if rows else 0
        out.emit(pa.RecordBatch.from_pydict({"count": [total]}, schema=params.output_schema))
        state.done = True


class BufferWorker(Worker):
    """A worker exposing the ``buffers`` catalog."""

    catalog = Catalog(
        name="buffers",
        schemas=[Schema(name="main", functions=[RowCount])],
    )


if __name__ == "__main__":
    BufferWorker().run()
