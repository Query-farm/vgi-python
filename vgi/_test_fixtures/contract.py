# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""The ``example.contract`` schema: the cross-SDK table-only / write-contract fixtures.

Every SDK's standard example worker serves the same schema, so the C++
extension's ``visibility/example_table_only.test``,
``write_contract/example_insert_finalize.test`` and
``write_contract/example_decimal_width.test`` run unchanged against each of them::

    good     id BIGINT, label VARCHAR, row id ``rowid`` (= id); scan
             ``contract_good_scan`` with projection pushdown. Rows (1,a) (2,b) (3,c).
    ranged   who VARCHAR; scan named exactly ``range`` (table_only, so the
             extension binds the worker's function, not DuckDB's range()).
    notes    id BIGINT (NOT_NULL record ``notes_id_not_null``), note VARCHAR;
             scan ``contract_notes_scan`` (always empty) and INSERT function
             ``contract_notes_insert`` (count only) whose count comes from
             finalize only. It refuses a non-nullable input field and a NULL id.
    amounts  id BIGINT, d32 DECIMAL(7,2) as decimal32, d64 DECIMAL(15,3) as
             decimal64, d128 DECIMAL(9,2) as decimal128; INSERT function
             ``contract_amounts_insert`` (count or rows) refuses any column not
             at exactly its declared Arrow type.

Nothing is stored: the insert functions validate, count and discard. Every
function is table_only (no ``expose_scan``), so none is listed in
``duckdb_functions()``.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, ClassVar

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi.catalog import Schema, Table, constraint_record
from vgi.invocation import BindResponse
from vgi.metadata import FunctionStability
from vgi.table_function import BindParams, ProcessParams, TableFunctionGenerator, init_single_worker
from vgi.table_in_out_function import TableInOutGenerator
from vgi.write_results import row_id_field, write_count_batch, write_options, write_result_batch, write_result_schema


def _schema(*fields: pa.Field[Any]) -> pa.Schema:
    return pa.schema(list(fields))


GOOD = _schema(pa.field("id", pa.int64()), pa.field("label", pa.string()))
RANGED = _schema(pa.field("who", pa.string()))
NOTES = _schema(pa.field("id", pa.int64(), nullable=False), pa.field("note", pa.string()))
AMOUNTS = _schema(
    pa.field("id", pa.int64()),
    pa.field("d32", pa.decimal32(7, 2)),
    pa.field("d64", pa.decimal64(15, 3)),
    pa.field("d128", pa.decimal128(9, 2)),
)

# Counter namespace for contract_notes_insert in the execution-scoped storage.
_NOTES_COUNT_NS = b"contract_notes_count"


@dataclass(kw_only=True)
class _Once(ArrowSerializableDataclass):
    done: bool = False


@init_single_worker
class _ContractScan(TableFunctionGenerator[None, _Once]):
    """A no-argument scan emitting its rows once, only the columns DuckDB asked for."""

    OUTPUT: ClassVar[pa.Schema]

    @classmethod
    def rows(cls) -> list[dict[str, Any]]:
        """The rows to emit, keyed by column name."""
        raise NotImplementedError

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        """The table's columns."""
        del params
        return BindResponse(output_schema=cls.OUTPUT)

    @classmethod
    def initial_state(cls, params: ProcessParams[None]) -> _Once:
        """Nothing emitted yet."""
        del params
        return _Once()

    @classmethod
    def process(cls, params: ProcessParams[None], state: _Once, out: OutputCollector) -> None:
        """Emit the rows once (none for an empty table)."""
        if not state.done:
            state.done = True
            rows = cls.rows()
            if rows:
                arrays = [pa.array([row[f.name] for row in rows], type=f.type) for f in params.output_schema]
                out.emit(pa.RecordBatch.from_arrays(arrays, schema=params.output_schema))
        out.finish()


class ContractGoodScan(_ContractScan):
    """contract.good, with a row id equal to id."""

    OUTPUT = pa.schema([*GOOD, row_id_field()])

    class Meta:
        """Function metadata."""

        name = "contract_good_scan"
        description = "The rows of contract.good, with rowid"
        stability = FunctionStability.VOLATILE
        # DuckDB reads `rowid` as a virtual column, which needs projection pushdown.
        projection_pushdown = True

    @classmethod
    def rows(cls) -> list[dict[str, Any]]:
        """Three fixed rows."""
        return [{"id": i, "label": label, "rowid": i} for i, label in ((1, "a"), (2, "b"), (3, "c"))]


class ContractRangeScan(_ContractScan):
    """contract.ranged: a table_only scan named like DuckDB's range()."""

    OUTPUT = RANGED

    class Meta:
        """Function metadata."""

        name = "range"
        description = "The rows of contract.ranged (table_only, so it shadows nothing)"
        stability = FunctionStability.VOLATILE

    @classmethod
    def rows(cls) -> list[dict[str, Any]]:
        """One row only this worker produces."""
        return [{"who": "vgi example worker"}]


class ContractNotesScan(_ContractScan):
    """contract.notes: nothing is stored, so always empty."""

    OUTPUT = NOTES

    class Meta:
        """Function metadata."""

        name = "contract_notes_scan"
        description = "The rows of contract.notes (always empty)"
        stability = FunctionStability.VOLATILE

    @classmethod
    def rows(cls) -> list[dict[str, Any]]:
        """No rows."""
        return []


class ContractNotesInsert(TableInOutGenerator[None, None]):
    """Backs INSERT INTO contract.notes; the row count comes from finalize only."""

    class Meta:
        """Function metadata."""

        name = "contract_notes_insert"
        description = "Backs INSERT INTO contract.notes, counting in finalize"
        stability = FunctionStability.VOLATILE
        has_finalize = True

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        """Count mode only."""
        return BindResponse(output_schema=write_result_schema(write_options(params).result_mode, NOTES))

    @classmethod
    def process(cls, params: ProcessParams[None], state: None, batch: pa.RecordBatch, out: OutputCollector) -> None:
        """Validate the batch and count it; report nothing yet (finalize reports the count)."""
        del state
        for field in batch.schema:
            if not field.nullable:
                raise ValueError(f"contract: notes_insert input field {field.name} is non-nullable")
        if batch.column("id").null_count:
            raise ValueError("contract: notes.id required")
        # Execution-scoped shared storage: pooled worker processes may serve one statement.
        params.storage.counter_add(_NOTES_COUNT_NS, b"", batch.num_rows)
        out.emit(write_count_batch(0).slice(0, 0))

    @classmethod
    def finalize(cls, params: ProcessParams[None]) -> list[pa.RecordBatch]:
        """The call's row count, once."""
        total = params.storage.counter_get(_NOTES_COUNT_NS, b"")
        params.storage.counter_delete(_NOTES_COUNT_NS, b"")
        return [write_count_batch(total)]


class ContractAmountsScan(_ContractScan):
    """contract.amounts: fixed rows at the declared decimal widths."""

    OUTPUT = AMOUNTS

    class Meta:
        """Function metadata."""

        name = "contract_amounts_scan"
        description = "The fixed rows of contract.amounts"
        stability = FunctionStability.VOLATILE

    @classmethod
    def rows(cls) -> list[dict[str, Any]]:
        """Three rows exercising each width's range."""
        return [
            {"id": 1, "d32": Decimal("12345.67"), "d64": Decimal("-123456789012.345"), "d128": Decimal("1234567.89")},
            {"id": 2, "d32": Decimal("-99999.99"), "d64": Decimal("999999999999.999"), "d128": Decimal("-9999999.99")},
            {"id": 3, "d32": None, "d64": Decimal("0.001"), "d128": None},
        ]


class ContractAmountsInsert(TableInOutGenerator[None, None]):
    """Backs INSERT INTO contract.amounts; refuses a column not at its declared type."""

    class Meta:
        """Function metadata."""

        name = "contract_amounts_insert"
        description = "Backs INSERT INTO contract.amounts, checking decimal widths"
        stability = FunctionStability.VOLATILE

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        """Count or rows."""
        return BindResponse(output_schema=write_result_schema(write_options(params).result_mode, AMOUNTS))

    @classmethod
    def process(cls, params: ProcessParams[None], state: None, batch: pa.RecordBatch, out: OutputCollector) -> None:
        """Check every declared type arrived bit-for-bit, then report the rows."""
        del state
        for field in AMOUNTS:
            got = batch.schema.field(field.name).type
            if got != field.type:
                raise ValueError(f"contract: amounts.{field.name} arrived as {got}, declared {field.type}")
        rows = batch.to_pylist()
        out.emit(write_result_batch(write_options(params).result_mode, AMOUNTS, [None] * len(rows), rows))


CONTRACT_SCHEMA = Schema(
    path=["contract"],
    functions=[],
    tables=[
        Table(name="good", function=ContractGoodScan),
        Table(name="ranged", function=ContractRangeScan),
        Table(
            name="notes",
            function=ContractNotesScan,
            insert_function=ContractNotesInsert,
            constraints=(
                constraint_record(
                    ["contract"],
                    "notes",
                    "notes_id_not_null",
                    "NOT_NULL",
                    [["id"]],
                    enforcement="ENFORCED",
                    validation="ALL_ROWS",
                ),
            ),
        ),
        Table(
            name="amounts",
            function=ContractAmountsScan,
            insert_function=ContractAmountsInsert,
            write_result_modes={"insert": "rows"},
        ),
    ],
)
