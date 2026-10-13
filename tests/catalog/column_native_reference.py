# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D102, D106, D107

"""SQL integration bridge to the durable column reference, using baseline INSERT.

This small fixture reads and appends rows in the same SQLite snapshots as DDL.
It is not a production transactor: UPDATE, DELETE, RETURNING, replay, and
distributed transactions are not implemented or advertised.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, replace
from typing import Any

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi.catalog import Catalog, ReadOnlyCatalogInterface, ScanFunctionResult, Schema, SchemaObjectType
from vgi.invocation import BindResponse, GlobalInitResponse
from vgi.table_function import BindParams, InitParams, ProcessParams, TableFunctionGenerator
from vgi.table_in_out_function import TableInOutGenerator
from vgi.worker import Worker

from .column_paths_reference import ColumnPathsCatalog

COUNT = pa.schema([pa.field("count", pa.int64(), nullable=False)])


def _args(params: Any) -> dict[str, Any]:
    call = params.bind_call if isinstance(params, BindParams) else params.init_call.bind_call
    target = json.loads(call.arguments.positional[0].as_py())
    return dict(
        attach_opaque_data=params.attach_opaque_data,
        transaction_opaque_data=call.transaction_opaque_data,
        **target,
    )


@dataclass
class ScanState(ArrowSerializableDataclass):
    """Track whether the transaction snapshot has been emitted."""

    emitted: bool = False


class ReferenceScan(TableFunctionGenerator[None, ScanState]):
    """Expose physical reference rows through normal VGI TABLE execution."""

    class Meta:
        name = "column_reference_scan"
        projection_pushdown = False
        filter_pushdown = False

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        info = NativeColumnsCatalog().table_get(**_args(params))
        if info is None:
            raise ValueError("table does not exist")
        return BindResponse(output_schema=pa.ipc.read_schema(pa.BufferReader(info.columns)))

    @classmethod
    def on_init(cls, params: InitParams[None]) -> GlobalInitResponse:
        return GlobalInitResponse(max_workers=1)

    @classmethod
    def initial_state(cls, params: ProcessParams[None]) -> ScanState:
        return ScanState()

    @classmethod
    def process(cls, params: ProcessParams[None], state: ScanState, out: OutputCollector) -> None:
        if state.emitted:
            out.finish()
        else:
            partitions = NativeColumnsCatalog().physical_rows(**_args(params))
            rows = [row for partition in partitions for row in partition["rows"]]
            out.emit(pa.RecordBatch.from_pylist(rows, schema=params.output_schema))
            state.emitted = True


class ReferenceInsert(TableInOutGenerator[None, None]):
    """Append count-mode rows within the catalog's existing transaction snapshot."""

    class Meta:
        name = "column_reference_insert"

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        from vgi._test_fixtures.writable.table import _parse_write_options

        options = _parse_write_options(params.bind_call)
        if options.get("result_mode", "count") != "count" or options.get("on_conflict", "throw") != "throw":
            raise NotImplementedError("reference supports count INSERT without conflict handling")
        return BindResponse(output_schema=COUNT)

    @classmethod
    def process(cls, params: ProcessParams[None], state: None, batch: pa.RecordBatch, out: OutputCollector) -> None:
        NativeColumnsCatalog().seed_rows(**_args(params), rows=batch.to_pylist())
        out.emit(pa.RecordBatch.from_pylist([{"count": batch.num_rows}], schema=COUNT))


CATALOG = Catalog(
    name="columns",
    default_schema="main",
    schemas=[Schema(path=["main"], functions=[ReferenceScan, ReferenceInsert])],
)


class NativeColumnsCatalog(ColumnPathsCatalog):
    """Add baseline scans/INSERT to the same durable DDL reference."""

    catalog = CATALOG
    database_path = os.environ["VGI_COLUMN_REFERENCE_DB"]

    def schema_contents(self, **kwargs: Any) -> Any:
        if kwargs["type"] == SchemaObjectType.TABLE:
            return [replace(info, write_result_modes={"insert": "count"}) for info in super().schema_contents(**kwargs)]
        return ReadOnlyCatalogInterface.schema_contents(self, **kwargs)

    def table_get(self, **kwargs: Any) -> Any:
        info = super().table_get(**kwargs)
        return replace(info, write_result_modes={"insert": "count"}) if info is not None else None

    def _function(self, kind: str, **kwargs: Any) -> ScanFunctionResult:
        target = json.dumps({"schema_path": kwargs["schema_path"], "name": kwargs["name"]})
        return ScanFunctionResult(
            function_name=f"column_reference_{kind}",
            positional_arguments=[pa.scalar(target)],
            named_arguments={},
        )

    def table_scan_function_get(self, **kwargs: Any) -> ScanFunctionResult:
        return self._function("scan", **kwargs)

    def table_insert_function_get(self, **kwargs: Any) -> ScanFunctionResult:
        return self._function("insert", **kwargs)


class NativeColumnsWorker(Worker):
    """Host the fixture using normal protocol, binding and streaming paths."""

    catalog_interface = NativeColumnsCatalog
    catalog = CATALOG


if __name__ == "__main__":
    NativeColumnsWorker.main()
