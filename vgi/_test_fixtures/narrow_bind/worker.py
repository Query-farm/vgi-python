# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Narrow-bind reproducer worker.

Two virtual tables, each backed by a table function:

  * ``mismatch`` — advertises columns ``{id, val}`` in its catalog listing
    but its scan function ``narrow_scan`` binds to ``{id}`` only. This is
    the inconsistency that used to segfault the client at scan time
    (``ArrowTableFunction::ArrowToDuckDB`` walking off the end of the
    worker's 1-column batch). The client must now refuse it at bind with a
    clear ``BinderException``.

  * ``consistent`` — advertises ``{id, val}`` and its scan function
    ``wide_scan`` binds to ``{id, val}``. Positive control: this must keep
    working unchanged.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Any

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi import Worker
from vgi.arguments import Arg
from vgi.catalog import Catalog, Schema
from vgi.catalog.catalog_interface import (
    AttachOpaqueData,
    ReadOnlyCatalogInterface,
    ScanFunctionResult,
    SchemaInfo,
    SchemaObjectType,
    SerializedSchema,
    TableInfo,
    TransactionOpaqueData,
)
from vgi.function import Function
from vgi.invocation import BindResponse
from vgi.table_function import (
    BindParams,
    ProcessParams,
    TableFunctionGenerator,
    init_single_worker,
)

CATALOG_NAME = "narrow_bind"

# What the catalog advertises for both tables: two columns.
_TABLE_SCHEMA: pa.Schema = pa.schema(
    [pa.field("id", pa.int64()), pa.field("val", pa.int64())]
)
# What the narrow scan function actually binds to: one column.
_NARROW_BIND_SCHEMA: pa.Schema = pa.schema([pa.field("id", pa.int64())])


@dataclass(kw_only=True)
class _State(ArrowSerializableDataclass):
    done: bool = False


@dataclass(frozen=True)
class _Args:
    count: Annotated[int, Arg(0, doc="rows", ge=0)]


@init_single_worker
class NarrowScan(TableFunctionGenerator[_Args, _State]):
    """Binds to a NARROWER schema than the catalog advertises (the bug)."""

    class Meta:
        name = "narrow_scan"
        description = "bind reports a narrower schema than the table advertises"

    @classmethod
    def on_bind(cls, params: BindParams[_Args]) -> BindResponse:
        return BindResponse(output_schema=_NARROW_BIND_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[_Args]) -> _State:
        return _State()

    @classmethod
    def process(cls, params: ProcessParams[_Args], state: _State, out: OutputCollector) -> None:
        if state.done:
            out.finish()
            return
        state.done = True
        out.emit(pa.RecordBatch.from_pydict({"id": [0, 1, 2]}, schema=params.output_schema))


@init_single_worker
class WideScan(TableFunctionGenerator[_Args, _State]):
    """Binds to the full advertised schema (positive control — must work)."""

    class Meta:
        name = "wide_scan"
        description = "bind matches the table's advertised schema"

    @classmethod
    def on_bind(cls, params: BindParams[_Args]) -> BindResponse:
        return BindResponse(output_schema=_TABLE_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[_Args]) -> _State:
        return _State()

    @classmethod
    def process(cls, params: ProcessParams[_Args], state: _State, out: OutputCollector) -> None:
        if state.done:
            out.finish()
            return
        state.done = True
        out.emit(
            pa.RecordBatch.from_pydict({"id": [0, 1, 2], "val": [10, 20, 30]}, schema=params.output_schema)
        )


_FUNCTIONS: list[type[Function]] = [NarrowScan, WideScan]

_CATALOG = Catalog(
    name=CATALOG_NAME,
    default_schema="main",
    schemas=[
        Schema(
            name="main",
            comment="narrow-bind reproducer catalog",
            functions=list(_FUNCTIONS),
            tables=[],
        ),
    ],
)


def _serialize_schema(s: pa.Schema) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, s):
        pass
    return sink.getvalue().to_pybytes()


# table name -> scan function name. Both advertise _TABLE_SCHEMA (2 cols).
_TABLE_FUNCTIONS = {
    "mismatch": "narrow_scan",
    "consistent": "wide_scan",
}


class NarrowBindCatalog(ReadOnlyCatalogInterface):
    catalog = _CATALOG
    catalog_name = CATALOG_NAME

    def _info(self, table_name: str) -> TableInfo:
        return TableInfo(
            comment=f"narrow-bind reproducer table -> {_TABLE_FUNCTIONS[table_name]}",
            tags={},
            name=table_name,
            schema_name="main",
            columns=SerializedSchema(_serialize_schema(_TABLE_SCHEMA)),
            not_null_constraints=[],
            unique_constraints=[],
            check_constraints=[],
        )

    def schemas(
        self, *, attach_opaque_data: AttachOpaqueData, transaction_opaque_data: TransactionOpaqueData | None
    ) -> list[SchemaInfo]:
        infos = super().schemas(attach_opaque_data=attach_opaque_data, transaction_opaque_data=transaction_opaque_data)
        for i, info in enumerate(infos):
            if info.name == "main":
                infos[i] = SchemaInfo(
                    attach_opaque_data=info.attach_opaque_data,
                    name=info.name,
                    comment=info.comment,
                    tags=info.tags,
                    estimated_object_count={
                        **(info.estimated_object_count or {}),
                        "table": len(_TABLE_FUNCTIONS),
                    },
                )
        return infos

    def schema_contents(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        name: str,
        type: Any,
    ) -> Any:
        if name.lower() == "main" and type == SchemaObjectType.TABLE:
            return [self._info(table_name) for table_name in _TABLE_FUNCTIONS]
        return super().schema_contents(
            attach_opaque_data=attach_opaque_data, transaction_opaque_data=transaction_opaque_data, name=name, type=type
        )

    def table_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
        at_unit: str | None = None,
        at_value: str | None = None,
    ) -> TableInfo | None:
        if schema_name.lower() != "main":
            return None
        if name in _TABLE_FUNCTIONS:
            return self._info(name)
        return None

    def table_scan_function_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
        at_unit: str | None,
        at_value: str | None,
    ) -> ScanFunctionResult:
        fn = _TABLE_FUNCTIONS.get(name)
        if fn is None:
            raise ValueError(f"unknown narrow-bind reproducer table: {name}")
        return ScanFunctionResult(
            function_name=fn,
            positional_arguments=[pa.scalar(3, type=pa.int64())],
            named_arguments={},
            required_extensions=[],
        )


class NarrowBindWorker(Worker):
    catalog_interface = NarrowBindCatalog
    catalog_name = CATALOG_NAME
    catalog = _CATALOG
    functions = list(_FUNCTIONS)
