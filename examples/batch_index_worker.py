# /// script
# requires-python = ">=3.13"
# dependencies = ["vgi-python"]
# ///
"""A buffering function that asks for input order, and reports what it got.

A buffering sink normally runs in parallel across DuckDB threads, so batches
arrive in no particular order. Setting ``Meta.requires_input_batch_index`` asks
for DuckDB's per-chunk index alongside each batch, which is what lets a worker
put the input back in order — the prerequisite for anything order-sensitive,
like row pattern matching or a running total.

Not every source can supply one: a base table scan can, while ``range()`` and
``VALUES`` cannot. When it cannot, the extension serializes the sink and numbers
the batches itself, so a worker sees a valid monotonic index either way and never
has to care which route produced it.

This function emits one row per buffered batch — its index and its row count — so
the guarantee is directly observable:

    ATTACH 'bi' (TYPE vgi, LOCATION 'uv run batch_index_worker.py');
    SELECT * FROM bi.batch_indexes((SELECT * FROM range(5000))) ORDER BY batch_index;
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

_RESULT = pa.schema([("batch_index", pa.int64()), ("rows", pa.int64())])

# One log entry per buffered batch: the index DuckDB gave us, and the batch size.
_NS = b"batches"


@dataclass(slots=True, frozen=True, kw_only=True)
class BatchIndexArgs:
    """Arguments: the input table whose batches should be reported."""

    data: Annotated[TableInput, Arg(0, doc="Input table")]


@dataclass(kw_only=True)
class DrainState(ArrowSerializableDataclass):
    """Per-finalize-stream cursor: emit the report once, then finish."""

    done: bool = False


class BatchIndexes(TableBufferingFunction[BatchIndexArgs, DrainState]):
    """Report the batch index and row count of every buffered input batch."""

    class Meta:
        """Function metadata."""

        name = "batch_indexes"
        # Ask for DuckDB's per-chunk index. Mutually exclusive with
        # sink_order_dependent, which orders the input by serializing the sink
        # instead of by numbering it.
        requires_input_batch_index = True

    @classmethod
    def on_bind(cls, params: BindParams[BatchIndexArgs]) -> BindResponse:
        """Output shape is fixed: one row per input batch."""
        return BindResponse(output_schema=_RESULT)

    @classmethod
    def process(cls, batch: pa.RecordBatch, params: TableBufferingParams[BatchIndexArgs]) -> bytes:
        """Sink: record this batch's index and size.

        ``params.batch_index`` is populated because ``Meta`` asked for it; -1
        stands in for the absent case so an older host that does not supply one
        degrades to a visible marker instead of a crash.
        """
        index = params.batch_index if params.batch_index is not None else -1
        payload = index.to_bytes(8, "little", signed=True) + batch.num_rows.to_bytes(8, "little")
        params.storage.state_append(_NS, b"", payload)
        return params.execution_id

    @classmethod
    def combine(cls, state_ids: list[bytes], params: TableBufferingParams[BatchIndexArgs]) -> list[bytes]:
        """Nothing to reduce: the log already holds one entry per batch."""
        return [params.execution_id]

    @classmethod
    def initial_finalize_state(
        cls, finalize_state_id: bytes, params: TableBufferingParams[BatchIndexArgs]
    ) -> DrainState:
        """One cursor per finalize stream."""
        return DrainState()

    @classmethod
    def finalize(
        cls,
        params: TableBufferingParams[BatchIndexArgs],
        finalize_state_id: bytes,
        state: DrainState,
        out: OutputCollector,
    ) -> None:
        """Source: emit the report, ordered by batch index."""
        if state.done:
            out.finish()
            return
        entries = [
            (
                int.from_bytes(value[:8], "little", signed=True),
                int.from_bytes(value[8:16], "little"),
            )
            for _id, value in params.storage.state_log_scan(_NS, b"")
        ]
        entries.sort()
        out.emit(
            pa.RecordBatch.from_pydict(
                {
                    "batch_index": [index for index, _rows in entries],
                    "rows": [rows for _index, rows in entries],
                },
                schema=params.output_schema,
            )
        )
        state.done = True


class BatchIndexWorker(Worker):
    """A worker exposing the ``bi`` catalog."""

    catalog = Catalog(
        name="bi",
        schemas=[Schema(name="main", functions=[BatchIndexes])],
    )


if __name__ == "__main__":
    BatchIndexWorker().run()
