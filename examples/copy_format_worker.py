# /// script
# requires-python = ">=3.13"
# dependencies = ["vgi-python"]
# ///
"""Custom ``COPY ... FROM`` and ``COPY ... TO`` formats, in one catalog.

A VGI catalog can register its own ``COPY`` formats, so users read and write a
format DuckDB has never heard of. Both directions reuse machinery that already
exists, which is why they add so few new rules:

- a :class:`CopyFromFunction` is an ordinary producer-mode **table function**;
- a :class:`CopyToFunction` is a **buffered** function with no Source phase —
  ``write`` is the sink (per batch, parallel), ``close`` is the combine (once).

The format here is tab-separated values, one record per line, no header, no
quoting. Deliberately trivial — the point is the wiring, not the parser.

Note the reader and writer declare **different** format names (``tsvlite`` and
``tsvlite_out``). A reader and a writer that share one name look fine on the
Python side — the catalog advertises both, keyed by (direction, format) — but the
extension registers COPY functions by name alone, so the writer is silently
dropped and ``COPY ... TO`` fails with "COPY TO is not supported for FORMAT".
Give each direction its own name.

Two rules the framework enforces, and this file demonstrates:

1. **The reader must emit ``expected_schema`` exactly.** DuckDB inserts no cast
   between the scan and the INSERT, so the target table's columns are the output
   schema — that is why ``on_bind`` is ``@final`` on a COPY-FROM function.
2. **The writer must finish inside ``close``.** There is no finalize phase, and
   ``write`` and ``close`` may run in different processes, so shards go through
   ``params.storage`` scoped by ``execution_id`` — never on ``self``.

The SQL ``FORMAT`` name is qualified by the catalog alias you attached as, NOT
the bare ``COPY_*_FORMAT`` string — the extension namespaces formats per attach,
so two workers may both call theirs ``tsvlite``:

    ATTACH 'tsv' (TYPE vgi, LOCATION 'uv run copy_format_worker.py');
    CREATE TABLE people (name VARCHAR, age BIGINT);
    COPY people FROM 'people.tsv' (FORMAT 'tsv.tsvlite');
    COPY (SELECT * FROM people) TO 'out.tsv' (FORMAT 'tsv.tsvlite_out', header true);
"""

from dataclasses import dataclass
from typing import Annotated

import pyarrow as pa

from vgi import Arg, Worker
from vgi.catalog import Catalog, Schema
from vgi.copy_from_function import CopyFromFunction
from vgi.copy_to_function import CopyToFunction
from vgi.table_buffering_function import TableBufferingParams
from vgi.table_function import OutputCollector, ProcessParams


@dataclass(slots=True, frozen=True, kw_only=True)
class ReadOptions:
    """Options accepted by ``COPY ... FROM ... (FORMAT tsvlite, ...)``.

    These are ordinary ``Arg``-annotated arguments — the source path is NOT one
    of them; it arrives on the bind. Each ``doc`` becomes the option's
    description in ``vgi_copy_formats()``.
    """

    skip_rows: Annotated[int, Arg("skip_rows", doc="Leading lines to discard", default=0)] = 0


@dataclass(slots=True, frozen=True, kw_only=True)
class WriteOptions:
    """Options accepted by ``COPY ... TO ... (FORMAT 'tsv.tsvlite_out', ...)``."""

    header: Annotated[bool, Arg("header", doc="Write a header line of column names", default=False)] = False


class ReadTsvLite(CopyFromFunction[ReadOptions]):
    """Read a tab-separated file into the COPY target table."""

    COPY_FROM_FORMAT = "tsvlite"
    COPY_FROM_COMMENT = "Tab-separated, one record per line, no quoting"

    @classmethod
    def read(
        cls,
        *,
        path: str,
        options: ReadOptions,
        expected_schema: pa.Schema,
        params: ProcessParams[ReadOptions],
        out: OutputCollector,
    ) -> None:
        """Parse ``path`` and emit batches matching ``expected_schema`` exactly."""
        with open(path, encoding="utf-8") as handle:
            lines = [line.rstrip("\n") for line in handle if line.strip()]
        lines = lines[options.skip_rows :]

        # Split into columns positionally, then let Arrow cast each column to the
        # type the target table declared. Emitting a mismatched type or arity is
        # rejected by the extension at COPY bind, not silently coerced.
        columns: list[list[str | None]] = [[] for _ in expected_schema]
        for line in lines:
            cells = line.split("\t")
            for index in range(len(expected_schema)):
                value = cells[index] if index < len(cells) else ""
                columns[index].append(value if value != "" else None)

        out.emit(
            pa.RecordBatch.from_arrays(
                [
                    pa.array(col, type=pa.string()).cast(field.type)
                    for col, field in zip(columns, expected_schema, strict=True)
                ],
                schema=expected_schema,
            )
        )
        out.finish()


class WriteTsvLite(CopyToFunction[WriteOptions]):
    """Write query results out as a tab-separated file."""

    COPY_TO_FORMAT = "tsvlite_out"
    COPY_TO_COMMENT = "Tab-separated, one record per line, no quoting"

    @classmethod
    def write(
        cls,
        *,
        batch: pa.RecordBatch,
        options: WriteOptions,
        file_path: str,
        params: TableBufferingParams[WriteOptions],
    ) -> None:
        """Sink: stash this batch as a shard. Runs per batch, possibly in parallel."""
        rows = ["\t".join("" if value is None else str(value) for value in row.values()) for row in batch.to_pylist()]
        # execution_id-scoped, because close() may run in a different process.
        params.storage.state_append(b"shards", b"", "\n".join(rows).encode("utf-8"))

    @classmethod
    def close(
        cls,
        *,
        options: WriteOptions,
        file_path: str,
        params: TableBufferingParams[WriteOptions],
    ) -> int:
        """Combine: read every shard back and write the file, once. Returns rows written."""
        shards = params.storage.state_log_scan(b"shards", b"")
        body = [chunk.decode("utf-8") for _key, chunk in shards if chunk]

        written = 0
        with open(file_path, "w", encoding="utf-8") as handle:
            if options.header:
                schema = params.init_call.bind_call.input_schema
                handle.write("\t".join(schema.names) + "\n")
            for chunk in body:
                handle.write(chunk + "\n")
                written += len(chunk.split("\n"))
        # Called even for an empty COPY — the file is created either way.
        return written


class TsvWorker(Worker):
    """A worker whose catalog advertises both COPY directions.

    The declarative ``Catalog`` introspects its function list for
    ``CopyFromFunction`` / ``CopyToFunction`` subclasses and advertises what it
    finds, so registering them here is all the wiring there is.
    """

    catalog = Catalog(
        name="tsv",
        schemas=[Schema(name="main", functions=[ReadTsvLite, WriteTsvLite])],
    )


if __name__ == "__main__":
    TsvWorker().run()
