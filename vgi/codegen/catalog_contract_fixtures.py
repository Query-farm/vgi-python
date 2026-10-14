# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Emit shared Arrow IPC codec vectors for the catalog contract records.

Every worker SDK decodes these Python-written records through its real codec.
Binary value payloads must survive byte-for-byte, including nested metadata.
These vectors prove representation; option mutation and path-policy semantics
have separate reference tests and are not implied by a codec round trip.
"""

from __future__ import annotations

import base64
import json
from typing import Any, TextIO

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc._types import rpc_methods

from vgi.catalog.capabilities import SCHEMA_OPTIONS_CREATE, SCHEMA_OPTIONS_READ, CatalogCapabilities
from vgi.catalog.catalog_interface import OnConflict, TableInfo, ViewInfo
from vgi.catalog.layout import (
    LayoutChangeMode,
    NullOrder,
    SortDirection,
    SortKey,
    SqlExpression,
    TableLayout,
    TableLayoutChange,
)
from vgi.catalog.metadata_changes import CatalogMetadataChanges
from vgi.catalog.options import OptionChanges, OptionDefinitions, OptionState
from vgi.column_path import RequiredFilterPaths
from vgi.protocol import TableCreateRequest, VgiProtocol
from vgi.typed_values import TypedValueBatch


def column_requests() -> dict[str, bytes]:
    """Encode literal and nested targets through each actual RPC parameter schema."""
    methods = rpc_methods(VgiProtocol)
    result = {}
    for operation in (
        "column_add",
        "column_drop",
        "column_rename",
        "column_type_change",
        "column_default_set",
        "column_default_drop",
        "column_comment_set",
        "not_null_set",
        "not_null_drop",
    ):
        schema = methods[f"catalog_table_{operation}"].params_schema
        for variant, path in (("literal", ["a.b"]), ("nested", ["a", 'b."quoted'])):
            nested = variant == "nested"
            definition = pa.schema(
                [pa.field(path[-1], pa.int32(), metadata={b"default": b"7"} if operation == "column_add" else None)]
            )
            values = dict(
                attach_opaque_data=b"attach",
                transaction_opaque_data=b"transaction" if nested else None,
                schema_path=["one.two", "nested"],
                name="table.with.dot",
                column_path=path,
                column_definition=definition.serialize().to_pybytes(),
                new_column_name='new."name',
                expression=None if operation == "column_type_change" and not nested else "7",
                comment="kept" if nested else None,
                ignore_not_found=nested,
                if_column_exists=nested,
                if_column_not_exists=nested,
                cascade=nested,
            )
            batch = pa.RecordBatch.from_pylist([{field.name: values[field.name] for field in schema}], schema=schema)
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, schema) as writer:
                writer.write_batch(batch)
            result[f"{operation}_{variant}"] = sink.getvalue().to_pybytes()
    return result


def render() -> str:
    """Return deterministic JSON with populated and empty record encodings."""
    schema = pa.schema(
        [
            pa.field("optional", pa.int32(), metadata={b"logical": b"example.optional"}),
            pa.field(
                "record",
                pa.struct([pa.field("a.b", pa.string(), nullable=False, metadata={b"nested": b"retained"})]),
                metadata={b"logical": b"example.record"},
            ),
        ],
        metadata={b"provider": b"catalog-contracts/v1"},
    )
    values = TypedValueBatch(
        pa.RecordBatch.from_pylist([{"optional": None, "record": {"a.b": 'literal."name'}}], schema=schema)
    )
    empty = TypedValueBatch.empty(metadata={b"provider": b"no-options"})
    expression = SqlExpression(
        sql='"a.b" + 1', profile="vgi.expression.duckdb.scalar.v1", schema_path=["one.two", "nested"]
    )
    sort_key = SortKey(expression=expression, direction=SortDirection.DESC, null_order=NullOrder.FIRST)
    layout = TableLayout.from_expressions(partition_by=[expression], sorted_by=[sort_key])
    table_args: dict[str, Any] = dict(
        name="table.with.dot", schema_path=expression.schema_path, columns=schema.serialize().to_pybytes()
    )
    records: dict[str, TypedValueBatch | ArrowSerializableDataclass] = {
        "view_info": ViewInfo(
            name="view.with.dot",
            schema_path=["one.two", "nested"],
            definition="SELECT 1 AS original, 2 AS second",
            aliases=["a.b", 'with"quote'],
            column_comments={"a.b": "kept"},
            comment="view comment",
            tags={"team": "one"},
        ),
        "view_info_barrier": ViewInfo(
            name="barrier",
            schema_path=["main"],
            definition="SELECT 1 AS original",
            comment=None,
            tags={},
            security="barrier",
        ),
        "view_info_implicit": ViewInfo(
            name="implicit", schema_path=["main"], definition="SELECT 1 AS original", comment=None, tags={}
        ),
        "capabilities": CatalogCapabilities(read=[SCHEMA_OPTIONS_READ], ddl=[SCHEMA_OPTIONS_CREATE]),
        "empty_capabilities": CatalogCapabilities(),
        "definitions": OptionDefinitions(
            schema=schema, defaults=values, create_names=schema.names, alter_names=["optional"]
        ),
        "values": values,
        "empty_values": empty,
        "metadata_patch": CatalogMetadataChanges(
            set_comment=True,
            comment="",
            set_tags={"a.b": "", "__proto__": "literal", "": "empty-key"},
            remove_tags=["old", "OLD"],
        ),
        "metadata_clear_comment": CatalogMetadataChanges(set_comment=True),
        "metadata_noop": CatalogMetadataChanges(),
        "changes": OptionChanges(set_values=values, reset_names=["reset.me"]),
        "reset_only": OptionChanges(reset_names=["reset.me"]),
        "state": OptionState(values=values, explicit_names=["optional"]),
        "empty_state": OptionState(values=empty),
        "paths": RequiredFilterPaths(groups=[[["a.b"], ["a", "b"]], [["record", 'literal."name']]]),
        "empty_paths": RequiredFilterPaths(),
        "expression": expression,
        "sort_key": sort_key,
        "layout": layout,
        "empty_layout": TableLayout(),
        "layout_change": TableLayoutChange(mode=LayoutChangeMode.REWRITE, partition_by=[], sorted_by=layout.sorted_by),
        "layout_clear": TableLayoutChange(mode=LayoutChangeMode.FUTURE_WRITES, sorted_by=[]),
        "table_create": TableCreateRequest(
            **table_args,
            attach_opaque_data=b"attach",
            on_conflict=OnConflict.ERROR,
            comment="kept",
            tags={"team": "one"},
            options=values,
            layout=layout,
        ),
        "table_create_legacy": TableCreateRequest(
            **table_args, attach_opaque_data=b"attach", on_conflict=OnConflict.ERROR
        ),
        "table_info": TableInfo(
            **table_args,
            comment="kept",
            tags={"team": "one"},
            not_null_constraints=[],
            unique_constraints=[],
            check_constraints=[],
            required_filters=[[["optional"], ["record", "a.b"]]],
            options=OptionState(values=values, explicit_names=["optional"]),
            layout=layout,
        ),
    }
    encoded = {name: record.serialize_to_bytes() for name, record in records.items()}
    encoded.update(column_requests())
    return (
        json.dumps(
            {
                "format_version": 1,
                "generator": "vgi.codegen.catalog_contract_fixtures",
                "records": {name: base64.b64encode(payload).decode("ascii") for name, payload in encoded.items()},
            },
            indent=2,
        )
        + "\n"
    )


def emit(out: TextIO) -> None:
    """Write the corpus through the normal multi-repository generation driver."""
    out.write(render())
