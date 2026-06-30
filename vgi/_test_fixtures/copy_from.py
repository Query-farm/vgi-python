# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Fixture ``COPY ... FROM`` format reader for VGI integration tests.

``ExampleLinesCopyFromFunction`` registers the SQL format ``example_lines`` — a
toy delimited-text reader. It exercises the full COPY-FROM path plus the option
machinery: a defaulted option (``delimiter``), an ``INTEGER`` option with a range
constraint (``skip_rows``), a required option (``null_string``), and an
enum/``choices`` option (``on_error``).

Usage::

    CREATE TABLE t (a INTEGER, b VARCHAR);
    COPY t FROM '/path/data.txt' (FORMAT example_lines, null_string 'NA');
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Annotated, ClassVar

import pyarrow as pa

from vgi.arguments import Arg
from vgi.copy_from_function import CopyFromFunction

if TYPE_CHECKING:
    from vgi_rpc.rpc import OutputCollector

    from vgi.table_function import BindParams, ProcessParams

__all__ = ["ExampleLinesCopyFromFunction", "SecretLinesCopyFromFunction"]


@dataclass(slots=True, frozen=True, kw_only=True)
class ExampleLinesCopyFromArgs:
    """Options for the ``example_lines`` COPY format."""

    null_string: Annotated[str, Arg("null_string", doc="Token parsed as SQL NULL")]
    delimiter: Annotated[str, Arg("delimiter", default=",", doc="Field separator")] = ","
    skip_rows: Annotated[int, Arg("skip_rows", default=0, ge=0, doc="Leading lines to skip before data")] = 0
    on_error: Annotated[
        str,
        Arg(
            "on_error",
            default="fail",
            choices=["fail", "skip"],
            doc="Behavior on a row whose column count does not match the target",
        ),
    ] = "fail"


class ExampleLinesCopyFromFunction(CopyFromFunction[ExampleLinesCopyFromArgs]):
    """Toy delimited-text ``COPY ... FROM`` reader (test fixture)."""

    COPY_FROM_FORMAT: ClassVar[str] = "example_lines"
    COPY_FROM_COMMENT: ClassVar[str | None] = "Toy delimited-text reader for tests"

    class Meta:
        name = "example_lines_copy_reader"
        description = "Read a delimited text file into the COPY target table"
        categories = ["copy", "test"]
        tags = {"category": "copy_from", "stability": "test"}

    @classmethod
    def read(
        cls,
        *,
        path: str,
        options: ExampleLinesCopyFromArgs,
        expected_schema: pa.Schema,
        params: ProcessParams[ExampleLinesCopyFromArgs],
        out: OutputCollector,
    ) -> None:
        """Parse ``path`` line-by-line and emit one batch matching ``expected_schema``."""
        with open(path, encoding="utf-8") as fh:
            lines = fh.read().splitlines()
        lines = lines[options.skip_rows :]

        ncols = len(expected_schema)
        rows: list[list[str]] = []
        for line in lines:
            if line == "":
                continue
            cells = line.split(options.delimiter)
            if len(cells) != ncols:
                if options.on_error == "skip":
                    continue
                raise ValueError(f"example_lines: row has {len(cells)} fields, expected {ncols}: {line!r}")
            rows.append(cells)

        # Column-major string arrays, NULL where the cell equals null_string,
        # then cast each column to the target type (DuckDB inserts no cast).
        columns = list(zip(*rows, strict=True)) if rows else [() for _ in range(ncols)]
        arrays = []
        for idx, field in enumerate(expected_schema):
            raw = [None if v == options.null_string else v for v in columns[idx]]
            arrays.append(pa.array(raw, type=pa.string()).cast(field.type))
        out.emit(pa.RecordBatch.from_arrays(arrays, schema=expected_schema))


@dataclass(slots=True, frozen=True, kw_only=True)
class SecretLinesCopyFromArgs:
    """Options for the ``secret_lines_in`` COPY format."""

    secret_type: Annotated[
        str,
        Arg("secret_type", default="vgi_example", doc="Secret type to fetch, scoped by the source path"),
    ] = "vgi_example"


class SecretLinesCopyFromFunction(CopyFromFunction[SecretLinesCopyFromArgs]):
    """COPY ... FROM reader that forwards a ``CREATE SECRET`` credential.

    Exercises the COPY-FROM secret-bind hook (:meth:`CopyFromFunction.on_secrets`):
    it requests the ``secret_type`` secret scoped to the source path during bind,
    and :meth:`read` emits a single VARCHAR row holding the resolved secret's
    ``api_key`` (or ``NONE``) — so a test can assert that the caller's secret
    reached the reader for a secret-backed cloud source.
    """

    COPY_FROM_FORMAT: ClassVar[str] = "secret_lines_in"
    COPY_FROM_COMMENT: ClassVar[str | None] = "Reader that forwards a CREATE SECRET credential (test fixture)"

    class Meta:
        name = "secret_lines_reader"
        description = "Emit the resolved secret's api_key as a single VARCHAR row"
        categories = ["copy", "test", "secret"]
        tags = {"category": "copy_from", "stability": "test"}

    @classmethod
    def on_secrets(cls, params: BindParams[SecretLinesCopyFromArgs]) -> None:
        """Request the source-scoped secret; framework two-phase resolves it."""
        cf = params.bind_call.copy_from
        scope = cf.file_path if cf is not None else None
        params.secrets.get(params.args.secret_type, scope=scope)

    @classmethod
    def read(
        cls,
        *,
        path: str,
        options: SecretLinesCopyFromArgs,
        expected_schema: pa.Schema,
        params: ProcessParams[SecretLinesCopyFromArgs],
        out: OutputCollector,
    ) -> None:
        """Emit one row carrying the forwarded secret's api_key (or NONE)."""
        secret = params.secrets.for_scope_of_type(path, options.secret_type) or {}
        api_key = secret.get("api_key")
        if api_key is not None and hasattr(api_key, "as_py"):
            api_key = api_key.as_py()
        value = "NONE" if api_key is None else str(api_key)
        arrays = [pa.array([value], type=pa.string()).cast(expected_schema.field(0).type)]
        out.emit(pa.RecordBatch.from_arrays(arrays, schema=expected_schema))
