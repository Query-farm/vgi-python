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
from dataclasses import replace
from typing import Any, TextIO

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc._types import rpc_methods

from vgi.catalog.capabilities import SCHEMA_OPTIONS_CREATE, SCHEMA_OPTIONS_READ, CatalogCapabilities
from vgi.catalog.catalog_interface import MacroInfo, MacroType, OnConflict, TableInfo, ViewInfo
from vgi.catalog.dependencies import CatalogObjectRef, DependencySnapshot, ObjectDependency
from vgi.catalog.layout import (
    LayoutChangeMode,
    NullOrder,
    SortDirection,
    SortKey,
    SqlExpression,
    TableLayout,
    TableLayoutChange,
)
from vgi.catalog.macros import MACRO_DEFAULT_PROFILE, MACRO_PROFILE, MacroOverload, MacroParameter
from vgi.catalog.metadata_changes import CatalogMetadataChanges
from vgi.catalog.options import OptionChanges, OptionDefinitions, OptionState
from vgi.catalog.query import (
    QUERY_AUTOMATIC,
    QUERY_ENCODING,
    QUERY_EXPLICIT,
    CatalogQueryPrepareRequest,
    CatalogQueryPrepareResult,
    ClientQueryCapabilities,
    QueryCapabilities,
    QueryEvaluationContext,
    QueryObjectReference,
)
from vgi.column_path import RequiredFilterPaths
from vgi.logical_types import encode_variant, tuple_field, variant_field
from vgi.protocol import MacroCreateRequest, TableCreateRequest, VgiProtocol
from vgi.query_parameters import QueryParameters
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


def logical_and_macro_records() -> dict[str, TypedValueBatch | ArrowSerializableDataclass]:
    """Cover dynamic type identity and complete macro families in every SDK corpus."""

    def scalar(field: pa.Field[Any], value: Any) -> TypedValueBatch:
        return TypedValueBatch(
            pa.RecordBatch.from_arrays([pa.array([value], type=field.type)], schema=pa.schema([field]))
        )

    pair = tuple_field("pair", [pa.field("", pa.uint64()), pa.field("", pa.string())])
    tuple_values = scalar(pair, {"element1": 2**64 - 1, "element2": None})
    struct_values = scalar(pair.remove_metadata(), {"element1": 2**64 - 1, "element2": None})
    variant_null = encode_variant(scalar(pa.field("value", pa.null()), None))
    absent = encode_variant(scalar(pa.field("value", pa.struct([])), {}))
    null_member = encode_variant(
        scalar(pa.field("value", pa.struct([pa.field("present", pa.int64())])), {"present": None})
    )
    scope = ["one.two", "nested"]
    literal = scalar(pa.field("fallback", pa.int64(), metadata={b"domain": b"retained"}), None)
    parameter = MacroParameter(
        name="fallback", declared_type=literal.schema, default_value=literal, documentation="A typed NULL default"
    )
    expression_parameter = MacroParameter(
        name="scale",
        declared_type=pa.schema([pa.field("scale", pa.int32())]),
        default_expression=SqlExpression(sql="2 + 3", profile=MACRO_DEFAULT_PROFILE, schema_path=scope),
    )
    overload = MacroOverload(
        parameters=[MacroParameter(name="x"), parameter, expression_parameter],
        definition="coalesce(x, fallback) * scale",
        profile=MACRO_PROFILE,
        schema_path=scope,
        macro_type="scalar",
    )
    second = MacroOverload(
        parameters=[MacroParameter(name="x", declared_type=pa.schema([pa.field("x", pa.string())]))],
        definition="upper(x)",
        profile=MACRO_PROFILE,
        schema_path=scope,
        macro_type="scalar",
    )
    published = replace(
        overload,
        parameters=[
            overload.parameters[0],
            parameter,
            replace(
                expression_parameter, default_expression=None, default_value=scalar(pa.field("scale", pa.int32()), 5)
            ),
        ],
    )
    common: dict[str, Any] = dict(name="macro.with.dot", schema_path=scope, macro_type=MacroType.SCALAR)
    return {
        "logical_tuple": tuple_values,
        "logical_struct": struct_values,
        "logical_variant_sql_null": scalar(variant_field("value"), None),
        "logical_variant_null": scalar(variant_field("value"), variant_null),
        "logical_variant_absent": scalar(variant_field("value"), absent),
        "logical_variant_null_member": scalar(variant_field("value"), null_member),
        "macro_parameter": parameter,
        "macro_parameter_expression": expression_parameter,
        "macro_parameter_untyped": MacroParameter(name="x"),
        "macro_overload": overload,
        "macro_create": MacroCreateRequest(
            **common,
            attach_opaque_data=b"attach",
            parameters=[],
            definition="",
            on_conflict=OnConflict.REPLACE,
            transaction_opaque_data=b"transaction",
            overloads=[overload, second],
        ),
        "macro_create_legacy": MacroCreateRequest(
            **common, attach_opaque_data=b"attach", parameters=["x"], definition="x + 1", on_conflict=OnConflict.ERROR
        ),
        "macro_info": MacroInfo(**common, parameters=[], overloads=[published, second], comment=None, tags={}),
        "macro_info_legacy": MacroInfo(**common, parameters=["x"], definition="x + 1", comment=None, tags={}),
    }


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
    root = CatalogObjectRef(kind="schema", schema_path=[], name="one.two")
    nested = CatalogObjectRef(kind="schema", schema_path=["one.two"], name="nested")
    table = CatalogObjectRef(kind="table", schema_path=["one.two", "nested"], name='table."name')
    trigger = CatalogObjectRef(kind="trigger", schema_path=table.schema_path, name="on.change", owner_table=table.name)
    owned = ObjectDependency(dependent=trigger.serialize_to_bytes(), subject=table.serialize_to_bytes(), kind="owned")
    edges = [
        ObjectDependency(dependent=nested.serialize_to_bytes(), subject=root.serialize_to_bytes()),
        ObjectDependency(dependent=table.serialize_to_bytes(), subject=nested.serialize_to_bytes()),
        owned,
    ]
    records.update(
        object_ref=table,
        trigger_ref=trigger,
        object_dependency=owned,
        dependency_snapshot=DependencySnapshot(
            catalog_version=42,
            objects=[r.serialize_to_bytes() for r in [root, nested, table, trigger]],
            dependencies=[e.serialize_to_bytes() for e in edges],
        ),
        empty_dependency_snapshot=DependencySnapshot(catalog_version=0),
    )
    records.update(logical_and_macro_records())
    encoded = {name: record.serialize_to_bytes() for name, record in records.items()}
    parameters = QueryParameters.from_batch(values.batch, mode="named")
    context = QueryEvaluationContext()
    query_offer = ClientQueryCapabilities(
        encodings=[QUERY_ENCODING],
        semantic_profiles=[QUERY_AUTOMATIC, QUERY_EXPLICIT],
        engine_version="fixture-client-build",
    )
    query_capabilities = QueryCapabilities(
        encoding=QUERY_ENCODING,
        semantic_profiles=query_offer.semantic_profiles,
        execution_modes=["automatic", "explicit"],
        backend_engine="duckdb",
        backend_version="fixture-provider-build",
        capability_revision="policy-1",
        default_evaluation_context=context,
    )
    reference = QueryObjectReference(object=table, expected_schema=schema.serialize().to_pybytes())
    prepare = CatalogQueryPrepareRequest(
        attach_opaque_data=b"attachment",
        transaction_opaque_data=b"transaction",
        encoding=QUERY_ENCODING,
        semantic_profile=QUERY_AUTOMATIC,
        mode="automatic",
        capability_revision="policy-1",
        query='SELECT * FROM "table"',
        parameter_schema=parameters.schema_bytes(),
        evaluation_context=context,
        catalog_version=0,
        catalog_etag="",
        referenced_objects=[reference],
        expected_output_schema=schema.serialize().to_pybytes(),
        required_ordering="query_order",
    )
    accepted = CatalogQueryPrepareResult(
        status="accepted",
        prepared_query_token=b"opaque-preparation",
        output_schema=prepare.expected_output_schema,
        result_ordering="query_order",
        validated_catalog_version=0,
        validated_catalog_etag="",
        context_digest=context.digest(prepare.encoding, prepare.semantic_profile),
        expires_at_epoch_ms=2_000_000_000_000,
        estimated_rows=0,
        estimated_bytes=0,
    )
    query_records: dict[str, ArrowSerializableDataclass | QueryParameters] = dict(
        query_offer=query_offer,
        query_capabilities=query_capabilities,
        query_context=context,
        query_object_reference=reference,
        query_prepare=prepare,
        query_accepted=accepted,
        query_declined=CatalogQueryPrepareResult(status="declined", reason_code="unsupported_query"),
        query_parameters=parameters,
        query_empty_parameters=QueryParameters.empty(),
    )
    encoded.update({name: record.serialize_to_bytes() for name, record in query_records.items()})
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
