# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Same-name-in-two-schemas producer, backing a DECLARATIVE TABLE in each schema.

The table-dispatch member of the schema-disambiguation family (see
``scalar/same_name.py``, ``table_in_out_same_name.py``, ``aggregate/same_name.py``,
and ``table/same_name_cached.py`` for the result-cache member). Those probe a
function called directly (or the result cache); this one probes
``catalog_table_scan_function_get``/``catalog_table_scan_branches_get`` — the RPC
pair that tells a client which function backs a *declarative catalog table*.

``test_same_name_table_scan`` is a one-row producer registered under the SAME
NAME in BOTH the ``main`` and ``data`` schemas of the ``example`` catalog, each
emitting a single row tagged with its own schema. A declarative ``Table``
descriptor named ``test_same_name_table`` is registered in each schema too, each
backed by that schema's own implementation.

This is also the end-to-end regression guard for protocol 1.5.0's
``ScanFunctionResult.schema_name``/``ScanBranch.schema_name``: the C++ extension
now prefers the worker-declared schema over its old table-schema/default-schema
heuristic when resolving which catalog entry ``function_name`` refers to. A
worker that regressed to leaving ``schema_name`` unset, or a client that stopped
consuming it, would silently fall back to the old heuristic — which still
happens to get *this* two-schema case right (the table's own schema is tried
first and always matches here), so a regression here would show up not as a
wrong row, but as the exact filter/projection-pushdown breakage this session's
own investigation traced to passing the wire-parsed schema string somewhere the
resolver couldn't safely reuse it — see ``cache/filter_pushdown_keys.test`` and
this table family's own pushdown assertions below. Driven by
``table/same_name_schemas.test``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi.metadata import FunctionExample
from vgi.schema_utils import schema
from vgi.table_function import (
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)

FUNCTION_NAME = "test_same_name_table_scan"
TABLE_NAME = "test_same_name_table"


@dataclass(slots=True, frozen=True)
class _SameNameTableArgs:
    """Arguments for the same-name table producer (none)."""


@dataclass(kw_only=True)
class _SameNameTableState(ArrowSerializableDataclass):
    """One-shot emit latch for the single output row."""

    done: bool = False


class _SameNameTableScan(TableFunctionGenerator[_SameNameTableArgs, _SameNameTableState]):
    """Shared body; each subclass supplies the schema it is declared in."""

    #: Schema this implementation is declared in — the tag it stamps.
    OWNING_SCHEMA: ClassVar[str] = ""

    FunctionArguments = _SameNameTableArgs
    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(tag=pa.string())

    @classmethod
    def initial_state(cls, params: ProcessParams[_SameNameTableArgs]) -> _SameNameTableState:
        """Fresh latch per invocation."""
        return _SameNameTableState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[_SameNameTableArgs],
        state: _SameNameTableState,
        out: OutputCollector,
    ) -> None:
        """Emit the single schema-tagged row once."""
        if state.done:
            out.finish()
            return
        batch = pa.RecordBatch.from_pydict({"tag": [cls.OWNING_SCHEMA]}, schema=params.output_schema)
        out.emit(batch)
        state.done = True


@init_single_worker
@bind_fixed_schema
class SameNameTableMainScan(_SameNameTableScan):
    """``test_same_name_table_scan`` as declared in the ``main`` schema."""

    OWNING_SCHEMA = "main"

    class Meta:
        """Function metadata."""

        name = FUNCTION_NAME
        description = "Schema-disambiguation probe; the main-schema table producer"
        categories = ["generator", "testing"]
        examples = [
            FunctionExample(
                sql="SELECT * FROM example.main.test_same_name_table",
                description="One row tagged 'main'",
            ),
        ]


@init_single_worker
@bind_fixed_schema
class SameNameTableDataScan(_SameNameTableScan):
    """``test_same_name_table_scan`` as declared in the ``data`` schema."""

    OWNING_SCHEMA = "data"

    class Meta:
        """Function metadata."""

        name = FUNCTION_NAME
        description = "Schema-disambiguation probe; the data-schema table producer"
        categories = ["generator", "testing"]
        examples = [
            FunctionExample(
                sql="SELECT * FROM example.data.test_same_name_table",
                description="One row tagged 'data'",
            ),
        ]
