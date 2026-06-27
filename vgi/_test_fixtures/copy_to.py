# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Fixture ``COPY ... TO`` format writer for VGI integration tests.

``ExampleLinesCopyToFunction`` registers the SQL format ``example_lines_out`` — a
toy delimited-text writer, the symmetric counterpart of the ``example_lines``
reader. It exercises the COPY-TO Sink+Combine path plus the option machinery: a
required option (``null_string``), a defaulted option (``delimiter``), a BOOLEAN
option (``header``), and an enum/``choices`` option (``on_exists``).

Shards are buffered in ``params.storage`` (``execution_id``-scoped) by ``write()``
and concatenated to the destination by ``close()`` — the cross-process-safe
pattern, so it works under pool rotation / HTTP.

Usage::

    COPY (SELECT * FROM t) TO '/path/out.txt' (FORMAT 'acme.example_lines_out', null_string 'NA');
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Annotated, ClassVar

import pyarrow as pa

from vgi.arguments import Arg
from vgi.copy_to_function import CopyToFunction

if TYPE_CHECKING:
    from vgi.table_buffering_function import TableBufferingParams

__all__ = ["ExampleLinesCopyToFunction", "ExampleLinesOrderedCopyToFunction"]

_SHARD_NS = b"copy_to_shard"


@dataclass(slots=True, frozen=True, kw_only=True)
class ExampleLinesCopyToArgs:
    """Options for the ``example_lines_out`` COPY format."""

    null_string: Annotated[str, Arg("null_string", doc="Token written for SQL NULL")]
    delimiter: Annotated[str, Arg("delimiter", default=",", doc="Field separator")] = ","
    header: Annotated[bool, Arg("header", default=False, doc="Write a header row of column names")] = False
    header_repeat: Annotated[
        int,
        Arg("header_repeat", default=1, ge=0, le=3, doc="When header=true, write the header line this many times"),
    ] = 1
    on_exists: Annotated[
        str,
        Arg(
            "on_exists",
            default="overwrite",
            choices=["overwrite", "error"],
            doc="Behavior when the destination file already exists",
        ),
    ] = "overwrite"
    fail_on_value: Annotated[
        str,
        Arg("fail_on_value", default="", doc="If non-empty, fail mid-write when a cell equals this value"),
    ] = ""


class ExampleLinesCopyToFunction(CopyToFunction[ExampleLinesCopyToArgs]):
    """Toy delimited-text ``COPY ... TO`` writer (test fixture)."""

    COPY_TO_FORMAT: ClassVar[str] = "example_lines_out"
    COPY_TO_COMMENT: ClassVar[str | None] = "Toy delimited-text writer for tests"

    class Meta:
        name = "example_lines_writer"
        description = "Write the COPY source to a delimited text file"
        categories = ["copy", "test"]
        tags = {"category": "copy_to", "stability": "test"}

    @classmethod
    def write(
        cls,
        *,
        batch: pa.RecordBatch,
        options: ExampleLinesCopyToArgs,
        file_path: str,
        params: TableBufferingParams[ExampleLinesCopyToArgs],
    ) -> None:
        """Buffer one input batch as an IPC blob in execution-scoped storage."""
        # Mid-sink failure trigger: raise during a process() call when a cell
        # matches fail_on_value. Exercises the in-flight teardown/recovery path.
        if options.fail_on_value:
            for col in batch.columns:
                for value in col.to_pylist():
                    if value is not None and str(value) == options.fail_on_value:
                        raise ValueError(f"example_lines_out: fail_on_value hit: {options.fail_on_value!r}")
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)
        # state_append is atomic + race-safe across parallel sink threads/workers.
        params.storage.state_append(_SHARD_NS, b"", sink.getvalue().to_pybytes())

    @classmethod
    def close(
        cls,
        *,
        options: ExampleLinesCopyToArgs,
        file_path: str,
        params: TableBufferingParams[ExampleLinesCopyToArgs],
    ) -> int:
        """Concatenate every shard and write the delimited destination file (once)."""
        import os

        if options.on_exists == "error" and os.path.exists(file_path):
            raise FileExistsError(f"example_lines_out: destination already exists: {file_path}")

        shards = params.storage.state_log_scan(_SHARD_NS, b"", after_id=-1)

        def fmt(value: object) -> str:
            return options.null_string if value is None else str(value)

        def write_header(fh: object, names: list[str]) -> None:
            # header=true writes the column-name line `header_repeat` times.
            if options.header:
                for _ in range(options.header_repeat):
                    fh.write(options.delimiter.join(names) + "\n")  # type: ignore[attr-defined]

        rows_written = 0
        with open(file_path, "w", encoding="utf-8") as fh:
            wrote_header = False
            for _log_id, blob in shards:
                table = pa.ipc.open_stream(blob).read_all()
                if not wrote_header:
                    write_header(fh, list(table.schema.names))
                    wrote_header = True
                for row in table.to_pylist():
                    fh.write(options.delimiter.join(fmt(row[name]) for name in table.schema.names) + "\n")
                    rows_written += 1
            # Empty COPY with header=true still emits the header row(s). We need the
            # source column names; they ride the bind's input_schema.
            if not wrote_header:
                assert params.init_call is not None
                in_schema = params.init_call.bind_call.input_schema
                if in_schema is not None:
                    write_header(fh, list(in_schema.names))
        return rows_written


class ExampleLinesOrderedCopyToFunction(ExampleLinesCopyToFunction):
    """Ordered variant of :class:`ExampleLinesCopyToFunction`.

    ``Meta.ordered = True`` makes the extension use a single-threaded sink, so the
    worker receives every batch in source order and writes the file in order.
    """

    COPY_TO_FORMAT: ClassVar[str] = "example_lines_ordered_out"
    COPY_TO_COMMENT: ClassVar[str | None] = "Toy delimited-text writer (ordered, single-thread sink)"

    class Meta:
        name = "example_lines_ordered_writer"
        description = "Write the COPY source to a delimited file, preserving source order"
        categories = ["copy", "test"]
        tags = {"category": "copy_to", "stability": "test"}
        sink_order_dependent = True  # ordered COPY TO → single-thread sink
