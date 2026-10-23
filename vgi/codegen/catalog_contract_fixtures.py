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

from vgi.catalog.capabilities import (
    ROW_SCALAR_EXPRESSION,
    SCHEMA_OPTIONS_CREATE,
    SCHEMA_OPTIONS_READ,
    CatalogCapabilities,
)
from vgi.catalog.catalog_interface import IndexInfo, MacroInfo, MacroType, OnConflict, TableInfo, ViewInfo
from vgi.catalog.constraints import (
    ConstraintChange,
    ConstraintDefinition,
    ConstraintEnforcement,
    ConstraintInfo,
    ConstraintKind,
    ConstraintOperation,
    ConstraintsApplyRequest,
    ConstraintSnapshot,
    ConstraintValidation,
    ForeignKeyDefinition,
)
from vgi.catalog.dependencies import CatalogObjectRef, DependencySnapshot, ObjectDependency
from vgi.catalog.indexes import IndexConstraintType, IndexDefinition, IndexKey
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
from vgi.catalog.sql import ColumnExpressions, SqlStatement, with_column_expressions
from vgi.column_path import RequiredFilterPaths
from vgi.logical_types import tuple_field, variant_field
from vgi.protocol import IndexCreateRequest, MacroCreateRequest, TableCreateRequest, VgiProtocol
from vgi.query_parameters import QueryParameters
from vgi.typed_values import TypedValueBatch


def column_requests() -> dict[str, bytes]:
    """Encode literal and nested targets through each actual RPC parameter schema."""
    methods = rpc_methods(VgiProtocol)
    result = {}
    scope = ["one.two", "nested"]
    default = SqlExpression(
        sql="7", profile=ROW_SCALAR_EXPRESSION, schema_path=scope, evaluation_context=QueryEvaluationContext()
    )
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
            field = pa.field(path[-1], pa.int32())
            if operation == "column_add":
                field = with_column_expressions(field, ColumnExpressions(default=default))
            definition = pa.schema([field])
            values = dict(
                attach_opaque_data=b"attach",
                transaction_opaque_data=b"transaction" if nested else None,
                schema_path=scope,
                name="table.with.dot",
                column_path=path,
                column_definition=definition.serialize().to_pybytes(),
                new_column_name='new."name',
                expression=(
                    None
                    if operation == "column_type_change" and not nested
                    else SqlExpression(
                        sql="7",
                        profile=ROW_SCALAR_EXPRESSION,
                        schema_path=scope,
                        evaluation_context=QueryEvaluationContext(),
                    ).serialize_to_bytes()
                ),
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
    # Canonical Parquet Variant v1 bytes, carried opaquely by Arrow's
    # arrow.parquet.variant extension. These are not nested Arrow IPC streams.
    variant_null = {"metadata": bytes.fromhex("010000"), "value": bytes.fromhex("00")}
    absent = {"metadata": bytes.fromhex("010000"), "value": bytes.fromhex("020000")}
    null_member = {
        "metadata": bytes.fromhex("0101000770726573656e74"),
        "value": bytes.fromhex("020100000100"),
    }
    scope = ["one.two", "nested"]
    literal = scalar(pa.field("fallback", pa.int64(), metadata={b"domain": b"retained"}), None)
    parameter = MacroParameter(
        name="fallback", declared_type=literal.schema, default_value=literal, documentation="A typed NULL default"
    )
    expression_parameter = MacroParameter(
        name="scale",
        declared_type=pa.schema([pa.field("scale", pa.int32())]),
        default_expression=SqlExpression(
            evaluation_context=QueryEvaluationContext(), sql="2 + 3", profile=MACRO_DEFAULT_PROFILE, schema_path=scope
        ),
    )
    overload = MacroOverload(
        evaluation_context=QueryEvaluationContext(),
        parameters=[MacroParameter(name="x"), parameter, expression_parameter],
        definition="coalesce(x, fallback) * scale",
        profile=MACRO_PROFILE,
        schema_path=scope,
        macro_type="scalar",
    )
    second = MacroOverload(
        evaluation_context=QueryEvaluationContext(),
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


def constraint_records() -> dict[str, ArrowSerializableDataclass]:
    """Exercise every canonical variant, opaque identity and binary revision."""
    table = CatalogObjectRef(kind="table", schema_path=["one.two", "nested"], name='child."table')
    parent = replace(table, name="parent.with.dot")
    ref = CatalogObjectRef(kind="constraint", schema_path=table.schema_path, owner_table=table.name, name="key.01")
    referenced = replace(ref, owner_table=parent.name, name="parent.key.01")
    context = QueryEvaluationContext(
        default_order="desc", default_null_order="nulls_first", preserve_insertion_order=False
    )
    expression = SqlExpression(
        sql='"a.b" > 0',
        profile="vgi.expression.duckdb.scalar.v1",
        schema_path=table.schema_path,
        evaluation_context=context,
    )
    foreign = ForeignKeyDefinition(referenced_table=parent, referenced_column_paths=[["parent.id"], ["record", "id"]])
    definitions = {
        "not_null": ConstraintDefinition(kind=ConstraintKind.NOT_NULL, column_paths=[["a.b"]]),
        "unique": ConstraintDefinition(kind=ConstraintKind.UNIQUE, column_paths=[["a.b"], ["record", "id"]]),
        "primary": ConstraintDefinition(
            kind=ConstraintKind.PRIMARY_KEY, column_paths=[["id"]], declared_name="primary.label"
        ),
        "check": ConstraintDefinition(
            kind=ConstraintKind.CHECK, check_expression=expression, declared_name="check.label"
        ),
        "foreign": ConstraintDefinition(
            kind=ConstraintKind.FOREIGN_KEY, column_paths=[["a.b"], ["record", "id"]], foreign_key=foreign
        ),
    }
    info = ConstraintInfo(
        ref=ref,
        definition=definitions["foreign"],
        enforcement=ConstraintEnforcement.ENFORCED,
        validation=ConstraintValidation.ALL_ROWS,
        referenced_constraint=referenced,
    )
    changes = {
        "add": ConstraintChange(
            operation=ConstraintOperation.ADD,
            definition=definitions["check"],
            enforcement=ConstraintEnforcement.ENFORCED,
            validation=ConstraintValidation.ALL_ROWS,
        ),
        "drop": ConstraintChange(operation=ConstraintOperation.DROP, ref=ref, cascade=False),
        "rename": ConstraintChange(operation=ConstraintOperation.RENAME, ref=ref, declared_name='label."new'),
        "unname": ConstraintChange(operation=ConstraintOperation.RENAME, ref=ref, declared_name=None),
        "replace": ConstraintChange(
            operation=ConstraintOperation.REPLACE,
            ref=ref,
            definition=definitions["unique"],
            enforcement=ConstraintEnforcement.METADATA_ONLY,
            validation=ConstraintValidation.UNVALIDATED,
        ),
    }
    revision = b"\x00revision\xff\x80"
    return {
        "sql_statement": SqlStatement(
            sql='SELECT "a.b" FROM "child.""table"',
            profile="vgi.statement.duckdb.query.v1",
            schema_path=table.schema_path,
            evaluation_context=context,
        ),
        "constraint_ref": ref,
        "foreign_key_definition": foreign,
        **{f"constraint_definition_{name}": value for name, value in definitions.items()},
        "constraint_info": info,
        "constraint_info_untrusted": ConstraintInfo(
            ref=replace(ref, name="key.02"),
            definition=definitions["unique"],
            enforcement=ConstraintEnforcement.METADATA_ONLY,
            validation=ConstraintValidation.FUTURE_WRITES,
        ),
        **{f"constraint_change_{name}": value for name, value in changes.items()},
        "constraint_snapshot": ConstraintSnapshot(
            table=table, definition_revision=revision, constraints=[info.serialize_to_bytes()]
        ),
        "constraint_snapshot_empty": ConstraintSnapshot(table=table, definition_revision=revision, constraints=[]),
        "constraints_apply": ConstraintsApplyRequest(
            attach_opaque_data=b"attachment",
            transaction_opaque_data=b"transaction",
            table=table,
            expected_revision=revision,
            changes=[changes["add"].serialize_to_bytes(), changes["rename"].serialize_to_bytes()],
        ),
        "constraints_apply_autocommit": ConstraintsApplyRequest(
            attach_opaque_data=b"attachment",
            table=table,
            expected_revision=revision,
            changes=[changes["unname"].serialize_to_bytes()],
        ),
    }


def index_records(values: TypedValueBatch) -> dict[str, ArrowSerializableDataclass]:
    """Typed index keys and options preserve complete raw object identities."""
    scope = ["one.two", "nested"]
    table = CatalogObjectRef(kind="table", schema_path=scope, name='table."name')
    column = IndexKey(column_path=["a.b"])
    nested = IndexKey(column_path=["record", "a.b"])
    expression = IndexKey(
        expression=SqlExpression(
            sql='"a.b" + 1',
            profile="vgi.expression.duckdb.scalar.v1",
            schema_path=scope,
            evaluation_context=QueryEvaluationContext(),
        )
    )
    definition = IndexDefinition(
        table=table,
        algorithm="fixture.typed-index.v1",
        keys=[column.serialize_to_bytes(), expression.serialize_to_bytes()],
        options=values,
        constraint_type=IndexConstraintType.NONE,
    )
    state = OptionState(values=values, explicit_names=["optional"])
    primary = replace(
        definition,
        keys=[column.serialize_to_bytes()],
        constraint_type=IndexConstraintType.PRIMARY,
        associated_constraint=CatalogObjectRef(
            kind="constraint", schema_path=scope, name="fixture.primary", owner_table=table.name
        ),
    )
    return {
        "index_key_column": column,
        "index_key_nested": nested,
        "index_key_expression": expression,
        "index_definition": definition,
        "index_definition_primary": primary,
        "index_info": IndexInfo(
            name='index."name',
            schema_path=scope,
            definition=definition,
            options=state,
            comment="",
            tags={"owner": "index.fixture"},
        ),
        "index_create": IndexCreateRequest(
            attach_opaque_data=b"attach",
            schema_path=scope,
            name='index."name',
            definition=definition,
            on_conflict=OnConflict.REPLACE,
            transaction_opaque_data=b"txn\0\xff",
        ),
        "index_create_cross_schema": IndexCreateRequest(
            attach_opaque_data=b"attach",
            schema_path=["indexes"],
            name="elsewhere",
            definition=definition,
        ),
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
        evaluation_context=QueryEvaluationContext(),
        sql='"a.b" + 1',
        profile="vgi.expression.duckdb.scalar.v1",
        schema_path=["one.two", "nested"],
    )
    default_expression = replace(expression, sql="42")
    expression_metadata = TypedValueBatch(
        pa.RecordBatch.from_arrays(
            [pa.array([None], type=pa.int64()), pa.array([None], type=pa.int64())],
            schema=pa.schema(
                [
                    with_column_expressions(pa.field("a.b", pa.int64()), ColumnExpressions(default=default_expression)),
                    with_column_expressions(
                        pa.field("computed", pa.int64()), ColumnExpressions(generated=expression, generation="virtual")
                    ),
                ]
            ),
        )
    )
    sort_key = SortKey(expression=expression, direction=SortDirection.DESC, null_order=NullOrder.FIRST)
    layout = TableLayout.from_expressions(partition_by=[expression], sorted_by=[sort_key])
    table_args: dict[str, Any] = dict(
        name="table.with.dot", schema_path=expression.schema_path, columns=schema.serialize().to_pybytes()
    )
    table_definition = ConstraintDefinition(kind=ConstraintKind.NOT_NULL, column_paths=[["record", "a.b"]])
    table_constraint = ConstraintInfo(
        ref=CatalogObjectRef(
            kind="constraint",
            schema_path=expression.schema_path,
            owner_table=table_args["name"],
            name="fixture-stable-key",
        ),
        definition=table_definition,
        enforcement=ConstraintEnforcement.ENFORCED,
        validation=ConstraintValidation.ALL_ROWS,
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
        "column_default_expression": default_expression,
        "column_generated_expression": expression,
        "column_expression_metadata": expression_metadata,
        "sort_key": sort_key,
        "layout": layout,
        "empty_layout": TableLayout(),
        "layout_change": TableLayoutChange(mode=LayoutChangeMode.REWRITE, partition_by=[], sorted_by=layout.sorted_by),
        "layout_clear": TableLayoutChange(mode=LayoutChangeMode.FUTURE_WRITES, sorted_by=[]),
        "table_create": TableCreateRequest(
            constraint_definitions=[table_definition.serialize_to_bytes()],
            **table_args,
            attach_opaque_data=b"attach",
            on_conflict=OnConflict.ERROR,
            comment="kept",
            tags={"team": "one"},
            options=values,
            layout=layout,
        ),
        "table_create_legacy": TableCreateRequest(
            constraint_definitions=[], **table_args, attach_opaque_data=b"attach", on_conflict=OnConflict.ERROR
        ),
        "table_info": TableInfo(
            **table_args,
            comment="kept",
            tags={"team": "one"},
            constraints=[table_constraint.serialize_to_bytes()],
            definition_revision=b"revision\0\xff",
            required_filters=[[["optional"], ["record", "a.b"]]],
            options=OptionState(values=values, explicit_names=["optional"]),
            layout=layout,
        ),
    }
    root = CatalogObjectRef(kind="schema", schema_path=[], name="one.two")
    nested = CatalogObjectRef(kind="schema", schema_path=["one.two"], name="nested")
    table = CatalogObjectRef(kind="table", schema_path=["one.two", "nested"], name='table."name')
    trigger = CatalogObjectRef(kind="trigger", schema_path=table.schema_path, name="on.change", owner_table=table.name)
    owned = ObjectDependency(
        dependent=trigger.serialize_to_bytes(), subject=table.serialize_to_bytes(), blocks_alter=True, kind="owned"
    )
    edges = [
        ObjectDependency(dependent=nested.serialize_to_bytes(), subject=root.serialize_to_bytes(), blocks_alter=False),
        ObjectDependency(dependent=table.serialize_to_bytes(), subject=nested.serialize_to_bytes(), blocks_alter=False),
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
    index = CatalogObjectRef(kind="index", schema_path=table.schema_path, name="key.index")
    literal = ObjectDependency(
        dependent=index.serialize_to_bytes(),
        subject=table.serialize_to_bytes(),
        blocks_alter=True,
        subject_column_paths=[["a.b"]],
    )
    nested_column = ObjectDependency(
        dependent=index.serialize_to_bytes(),
        subject=table.serialize_to_bytes(),
        blocks_alter=True,
        subject_column_paths=[["record", "a.b"]],
    )
    generated = ObjectDependency(
        dependent=table.serialize_to_bytes(),
        subject=table.serialize_to_bytes(),
        blocks_alter=True,
        subject_column_paths=[["a.b"], ["record", "a.b"]],
        dependent_column_path=["generated"],
    )
    generated_chain = ObjectDependency(
        dependent=table.serialize_to_bytes(),
        subject=table.serialize_to_bytes(),
        blocks_alter=True,
        subject_column_paths=[["generated"]],
        dependent_column_path=["second.generated"],
    )
    records.update(
        index_ref=index,
        dependency_membership=edges[0],
        dependency_column_literal=literal,
        dependency_column_nested=nested_column,
        dependency_generated=generated,
        dependency_generated_chain=generated_chain,
        dependency_column_snapshot=DependencySnapshot(
            catalog_version=43,
            objects=[table.serialize_to_bytes(), index.serialize_to_bytes()],
            dependencies=[e.serialize_to_bytes() for e in [literal, generated, generated_chain]],
        ),
    )
    records.update(logical_and_macro_records())
    records.update(constraint_records())
    records.update(index_records(values))
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
