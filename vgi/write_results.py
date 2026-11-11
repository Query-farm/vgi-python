# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Writable-table result modes shared by catalogs and write functions.

A writable table advertises, per operation (insert / update / delete), the
richest result mode its write function can produce. Modes are ordered
``count < rows < changes``, and a table that supports one mode supports every
lower one. DuckDB asks for one mode per statement, in the ``write_options``
named argument of the write function's bind (decode it with
[`write_options`][vgi.write_results.write_options]):

* ``count`` -- plain INSERT/UPDATE/DELETE: one row, ``count BIGINT NOT NULL``
  ([`write_count_batch`][vgi.write_results.write_count_batch]).
* ``rows`` -- ``... RETURNING``: the written rows, in the table's user columns
  ([`write_rows_batch`][vgi.write_results.write_rows_batch]).
* ``changes`` -- OLD/NEW pairs: ``old STRUCT(<table>)``, ``new STRUCT(<table>)``
  ([`write_changes_batch`][vgi.write_results.write_changes_batch]).

[`write_result_schema`][vgi.write_results.write_result_schema] gives the exact
output schema to bind for a mode, and
[`write_result_batch`][vgi.write_results.write_result_batch] builds the result
batch for any mode from the written rows' before/after images.

UPDATE and DELETE identify rows by the table's row-id column: the one field
carrying the ``is_row_id`` metadata key
([`ROW_ID_METADATA_KEY`][vgi.write_results.ROW_ID_METADATA_KEY]), built with
[`row_id_field`][vgi.write_results.row_id_field]. The scan function must emit
it; write results never include it (see
[`user_columns`][vgi.write_results.user_columns]).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

import pyarrow as pa

if TYPE_CHECKING:
    from vgi.protocol import BindRequest

WriteOperation = Literal["insert", "update", "delete"]
WriteResultMode = Literal["count", "rows", "changes"]

WRITE_OPERATIONS: tuple[WriteOperation, ...] = ("insert", "update", "delete")
WRITE_RESULT_MODES: tuple[WriteResultMode, ...] = ("count", "rows", "changes")
_MODE_RANK = {mode: rank for rank, mode in enumerate(WRITE_RESULT_MODES)}

#: Arrow field-metadata key that marks a table's row-id column. Its value is
#: ignored (conventionally empty). DuckDB reads the marked column as the
#: table's ``rowid`` virtual column, and UPDATE/DELETE hand it to the write
#: function to identify each row.
ROW_ID_METADATA_KEY = b"is_row_id"


def row_id_field(name: str = "rowid", type: pa.DataType | None = None) -> pa.Field[Any]:  # noqa: A002
    """Build a row-id field for a writable table's columns and its scan's output.

    The default is DuckDB's conventional ``rowid BIGINT NOT NULL``.

    DuckDB reads the row id as a *virtual* column, which it only requests from
    a scan that supports projection pushdown. So a table with UPDATE or DELETE
    needs a scan ``function`` that

    1. emits this field (include it in the table's columns / bind schema),
    2. declares ``projection_pushdown = True`` in its ``Meta`` and honours
       ``params.output_schema`` (DuckDB asks only for the columns it needs).

    DuckDB learns (2) from the scan's function listing, which the catalog sends
    automatically (as ``"table_only"`` unless ``Table.expose_scan``). Missing
    either, UPDATE/DELETE fail with "Virtual columns require projection
    pushdown"; [`Table`][vgi.catalog.Table] rejects such declarations at
    construction.
    Row ids must be unique and stable for the life of a statement.

    Args:
        name: Column name (default ``rowid``).
        type: Arrow type (default ``pa.int64()``).

    Returns:
        A non-nullable field carrying ``ROW_ID_METADATA_KEY``.

    """
    return pa.field(name, type if type is not None else pa.int64(), nullable=False, metadata={ROW_ID_METADATA_KEY: b""})


def is_row_id_field(f: pa.Field[Any]) -> bool:
    """Whether ``f`` is marked as a row-id column (carries ``ROW_ID_METADATA_KEY``)."""
    return ROW_ID_METADATA_KEY in (f.metadata or {})


def row_id_column(columns: pa.Schema) -> pa.Field[Any] | None:
    """Return the row-id field of ``columns``, or ``None`` when there is none."""
    for f in columns:
        if is_row_id_field(f):
            return f
    return None


def user_columns(columns: pa.Schema) -> pa.Schema:
    """Return ``columns`` without the row-id field.

    Write results (``rows``, and both halves of ``changes``) use this schema;
    [`write_result_schema`][vgi.write_results.write_result_schema] applies it
    for you, so passing a table's full columns there is fine.
    """
    if not any(is_row_id_field(f) for f in columns):
        return columns
    return pa.schema([f for f in columns if not is_row_id_field(f)], metadata=columns.metadata)  # type: ignore[arg-type]


def validate_write_result_modes(modes: Mapping[str, str]) -> dict[str, str]:
    """Return a validated copy of an operation-to-maximum-mode mapping."""
    result = dict(modes)
    unknown_operations = sorted(set(result).difference(WRITE_OPERATIONS))
    if unknown_operations:
        raise ValueError(f"Unknown write operation(s): {', '.join(unknown_operations)}")
    for operation, mode in result.items():
        if mode not in _MODE_RANK:
            raise ValueError(f"Unknown write result mode {mode!r} for {operation!r}")
    return result


def supports_write_result_mode(maximum: str, requested: str) -> bool:
    """Whether ``maximum`` promises support for ``requested``."""
    if maximum not in _MODE_RANK or requested not in _MODE_RANK:
        return False
    return _MODE_RANK[maximum] >= _MODE_RANK[requested]


def write_result_schema(mode: WriteResultMode, table_schema: pa.Schema) -> pa.Schema:
    """Build the exact output schema a write function must bind for ``mode``.

    ``table_schema`` is the table's columns; a row-id column in it is dropped
    (write results carry user columns only, see
    [`user_columns`][vgi.write_results.user_columns]).
    """
    if mode == "count":
        return pa.schema([pa.field("count", pa.int64(), nullable=False)])
    columns = user_columns(table_schema)
    if mode == "rows":
        return columns
    row_type = pa.struct(list(columns))
    return pa.schema(
        [
            pa.field("old", row_type, nullable=True),
            pa.field("new", row_type, nullable=True),
        ]
    )


def write_count_batch(count: int) -> pa.RecordBatch:
    """Build a ``count`` result batch: one row, ``count BIGINT NOT NULL``."""
    return pa.RecordBatch.from_pydict({"count": [int(count)]}, schema=write_result_schema("count", pa.schema([])))


def _pick(columns: pa.Schema, row: Mapping[str, Any]) -> dict[str, Any]:
    return {name: row.get(name) for name in columns.names}


def write_rows_batch(table_schema: pa.Schema, rows: Sequence[Mapping[str, Any]]) -> pa.RecordBatch:
    """Build a ``rows`` (RETURNING) result batch from the written rows.

    Each row is a mapping keyed by column name; keys outside the table's user
    columns (such as ``rowid``) are ignored and missing columns are NULL.
    """
    schema = write_result_schema("rows", table_schema)
    return pa.RecordBatch.from_pylist([_pick(schema, row) for row in rows], schema=schema)


def write_changes_batch(
    table_schema: pa.Schema,
    old_rows: Sequence[Mapping[str, Any] | None],
    new_rows: Sequence[Mapping[str, Any] | None],
) -> pa.RecordBatch:
    """Build a ``changes`` batch while enforcing one OLD/NEW pair per row.

    ``old`` is ``None`` for an insert and ``new`` is ``None`` for a delete.
    Keys outside the table's user columns are ignored.
    """
    if len(old_rows) != len(new_rows):
        raise ValueError("old_rows and new_rows must have equal length")
    columns = user_columns(table_schema)
    rows = [
        {
            "old": _pick(columns, old) if old is not None else None,
            "new": _pick(columns, new) if new is not None else None,
        }
        for old, new in zip(old_rows, new_rows, strict=True)
    ]
    return pa.RecordBatch.from_pylist(rows, schema=write_result_schema("changes", table_schema))


def write_result_batch(
    mode: WriteResultMode,
    table_schema: pa.Schema,
    old_rows: Sequence[Mapping[str, Any] | None],
    new_rows: Sequence[Mapping[str, Any] | None],
) -> pa.RecordBatch:
    """Build the result batch for ``mode`` from the written rows' before/after images.

    One entry per written row in both sequences: ``old_rows[i]`` is ``None``
    for an insert, ``new_rows[i]`` is ``None`` for a delete. ``count`` counts
    the rows, ``rows`` returns each row as written (the new image, or the old
    one for a delete), and ``changes`` returns the pairs.
    """
    if len(old_rows) != len(new_rows):
        raise ValueError("old_rows and new_rows must have equal length")
    if mode == "count":
        return write_count_batch(len(new_rows))
    if mode == "changes":
        return write_changes_batch(table_schema, old_rows, new_rows)
    written: list[Mapping[str, Any]] = []
    for old, new in zip(old_rows, new_rows, strict=True):
        row = new if new is not None else old
        if row is None:
            raise ValueError("a written row needs an old or a new image")
        written.append(row)
    return write_rows_batch(table_schema, written)


def coerce_write_result_mode(value: str) -> WriteResultMode:
    """Validate a wire value and narrow it to ``WriteResultMode``."""
    if value not in _MODE_RANK:
        raise ValueError(f"Unknown write result mode: {value!r}")
    return value


@dataclass(frozen=True, slots=True)
class WriteOptions:
    """What DuckDB asked a write function for, decoded from ``write_options``.

    Attributes:
        result_mode: The result mode to produce (``count`` for plain DML).
        on_conflict: ``throw`` (default), or the ``INSERT ... ON CONFLICT``
            action DuckDB requested.
        on_conflict_columns: The conflict-target columns, if any.

    """

    result_mode: WriteResultMode = "count"
    on_conflict: str = "throw"
    on_conflict_columns: list[str] = field(default_factory=list)


def write_options(source: Any) -> WriteOptions:
    """Decode the ``write_options`` named argument of a write function call.

    ``source`` is the function's ``BindParams`` or ``ProcessParams`` (on
    process the options come from ``params.init_call.bind_call``), or a
    [`BindRequest`][vgi.protocol.BindRequest] directly. The argument is a
    serialized one-row batch; missing options mean a plain ``count`` write::

        @classmethod
        def on_bind(cls, params):
            mode = write_options(params).result_mode
            return BindResponse(output_schema=write_result_schema(mode, TABLE_COLUMNS))

    Args:
        source: ``BindParams``, ``ProcessParams``/``InitParams`` or a ``BindRequest``.

    Returns:
        The decoded options (defaults when DuckDB sent none).

    Raises:
        ValueError: If the requested result mode is unknown.

    """
    bind_call = _bind_call_of(source)
    arguments = getattr(bind_call, "arguments", None)
    named = arguments.named if arguments is not None else None
    raw = named.get("write_options") if named else None
    if raw is None:
        return WriteOptions()
    value = raw.as_py() if isinstance(raw, pa.Scalar) else raw
    if value is None:
        return WriteOptions()
    from vgi_rpc.utils import deserialize_record_batch

    batch, _ = deserialize_record_batch(bytes(value))
    if batch.num_rows == 0:
        return WriteOptions()
    row = batch.slice(0, 1).to_pylist()[0]
    return WriteOptions(
        result_mode=coerce_write_result_mode(row["result_mode"]) if row.get("result_mode") is not None else "count",
        on_conflict=str(row["on_conflict"]) if row.get("on_conflict") is not None else "throw",
        on_conflict_columns=[str(c) for c in row["on_conflict_columns"]]
        if row.get("on_conflict_columns") is not None
        else [],
    )


def _bind_call_of(source: Any) -> BindRequest:
    """Find the bind request behind a params object (or return a BindRequest as is)."""
    bind_call = getattr(source, "bind_call", None)
    if bind_call is not None:
        return bind_call  # type: ignore[no-any-return]
    init_call = getattr(source, "init_call", None)
    if init_call is not None:
        return init_call.bind_call  # type: ignore[no-any-return]
    if hasattr(source, "arguments"):
        return source  # type: ignore[no-any-return]
    raise TypeError(
        "write_options() needs BindParams, ProcessParams (with an init_call) or a BindRequest, "
        f"got {type(source).__name__}"
    )
