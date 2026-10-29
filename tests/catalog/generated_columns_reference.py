# Copyright 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D102, D103, D106, D107

"""Durable read-only physical rows with owner-validated VIRTUAL definitions.

This fixture reuses the dependency authority's closed scalar binder and SQLite
snapshots. It supplies physical fields only; the native consumer computes the
virtual fields. It does not expose DML or claim semantic column rewrites.
"""

from __future__ import annotations

import json
import os
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from typing import Any

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi.catalog import (
    Catalog,
    CatalogObjectRef,
    ReadOnlyCatalogInterface,
    ScanFunctionResult,
    Schema,
    SchemaObjectType,
)
from vgi.catalog.sql import ColumnExpressions, column_expressions, with_column_expressions
from vgi.invocation import BindResponse, GlobalInitResponse
from vgi.table_function import BindParams, InitParams, ProcessParams, TableFunctionGenerator
from vgi.worker import Worker

from .dependencies_reference import DependencyCatalog, _generated, _schema, object_key
from .typing_helpers import present


def physical_schema(schema: pa.Schema) -> pa.Schema:
    return pa.schema(
        [
            with_column_expressions(field, ColumnExpressions())
            for field in schema
            if column_expressions(field).generated is None
        ],
        metadata=None if schema.metadata is None else {key: value for key, value in (schema.metadata).items()},
    )


def _args(params: Any) -> dict[str, Any]:
    call = params.bind_call if isinstance(params, BindParams) else params.init_call.bind_call
    return dict(
        attach_opaque_data=params.attach_opaque_data,
        transaction_opaque_data=params.transaction_opaque_data,
        **json.loads(present(call.arguments.positional[0]).as_py()),
    )


@dataclass
class GeneratedScanState(ArrowSerializableDataclass):
    """Remember whether the physical snapshot has been emitted."""

    emitted: bool = False


class GeneratedScan(TableFunctionGenerator[None, GeneratedScanState]):
    """Return stored physical columns in their original relative order."""

    class Meta:
        name = "generated_reference_scan"
        projection_pushdown = False
        filter_pushdown = False

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        info = GeneratedColumnsCatalog().table_get(**_args(params))
        if info is None:
            raise ValueError("table does not exist")
        return BindResponse(output_schema=physical_schema(_schema(info)))

    @classmethod
    def on_init(cls, params: InitParams[None]) -> GlobalInitResponse:
        return GlobalInitResponse(max_workers=1)

    @classmethod
    def initial_state(cls, params: ProcessParams[None]) -> GeneratedScanState:
        return GeneratedScanState()

    @classmethod
    def process(cls, params: ProcessParams[None], state: GeneratedScanState, out: OutputCollector) -> None:
        if state.emitted:
            out.finish()
        else:
            rows = GeneratedColumnsCatalog().physical_rows(**_args(params))
            out.emit(pa.RecordBatch.from_pylist(rows, schema=params.output_schema))
            state.emitted = True


CATALOG = Catalog(
    name="generated_columns", default_schema="main", schemas=[Schema(path=["main"], functions=[GeneratedScan])]
)


class GeneratedColumnsCatalog(DependencyCatalog):
    """Keep physical rows and the complete generated definition in one snapshot."""

    catalog_name = "generated_columns"
    catalog = CATALOG
    database_path = os.environ.get("VGI_GENERATED_REFERENCE_DB", "")

    def _alter_table(self, operation: str, **kwargs: Any) -> None:
        if operation != "comment":
            raise NotImplementedError("generated row reference rejects semantic column edits without changes")
        super()._alter_table(operation, **kwargs)

    def table_create(self, **kwargs: Any) -> None:
        schema = pa.ipc.read_schema(pa.py_buffer(kwargs["columns"]))
        _generated(schema, kwargs["schema_path"])
        if not len(physical_schema(schema)):
            raise ValueError("generated row reference requires at least one physical column")
        super().table_create(**kwargs)

    def seed_rows(self, *, rows: list[dict[str, Any]], **kwargs: Any) -> None:
        """Seed physical fixture data atomically, without exposing a write endpoint."""
        ref = CatalogObjectRef(kind="table", schema_path=kwargs["schema_path"], name=kwargs["name"])
        key = object_key(ref)
        with closing(sqlite3.connect(self.database_path)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            version, data = self._snapshot(db, kwargs["attach_opaque_data"], kwargs["transaction_opaque_data"])
            record = json.loads(data[key])
            schema = physical_schema(_schema(self._info(data[key])))
            if any(set(row) - set(schema.names) for row in rows):
                raise ValueError("seed rows must contain only physical column names")
            batch = pa.Table.from_pylist(rows, schema=schema)
            batch.validate(full=True)
            record["rows"] = [*record.get("rows", []), *batch.to_pylist()]
            data[key] = json.dumps(record)
            self._publish(db, version, data, kwargs["transaction_opaque_data"])

    def physical_rows(self, **kwargs: Any) -> list[dict[str, Any]]:
        ref = CatalogObjectRef(kind="table", schema_path=kwargs["schema_path"], name=kwargs["name"])
        with closing(sqlite3.connect(self.database_path)) as db:
            _, data = self._snapshot(db, kwargs["attach_opaque_data"], kwargs["transaction_opaque_data"])
            rows: list[dict[str, Any]] = json.loads(data[object_key(ref)]).get("rows", [])
            return rows

    def schema_contents(self, **kwargs: Any) -> Any:
        if kwargs["type"] in (SchemaObjectType.TABLE, SchemaObjectType.VIEW):
            return super().schema_contents(**kwargs)
        return ReadOnlyCatalogInterface.schema_contents(self, **kwargs)

    def table_scan_function_get(self, **kwargs: Any) -> ScanFunctionResult:
        return ScanFunctionResult(
            function_name="generated_reference_scan",
            positional_arguments=[
                pa.scalar(json.dumps({"schema_path": kwargs["schema_path"], "name": kwargs["name"]}))
            ],
            named_arguments={},
        )


class GeneratedColumnsWorker(Worker):
    """Host discovery and physical scans through normal signed HTTP or stdio RPC."""

    catalog_interface = GeneratedColumnsCatalog
    catalog = CATALOG


if __name__ == "__main__":
    GeneratedColumnsWorker.main()
