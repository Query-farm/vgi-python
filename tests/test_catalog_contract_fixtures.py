# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103

"""The corpus distributed to every SDK retains semantic metadata and raw paths."""

import base64
import json
from pathlib import Path

import pyarrow as pa
import pytest

from vgi.catalog.capabilities import SCHEMA_OPTIONS_CREATE, SCHEMA_OPTIONS_READ, CatalogCapabilities
from vgi.catalog.catalog_interface import CatalogAttachResult, MacroInfo, TableInfo, ViewInfo
from vgi.catalog.column_metadata import ColumnMetadata, column_metadata, validate_column_metadata
from vgi.catalog.dependencies import DependencySnapshot, ObjectDependency
from vgi.catalog.evaluation_context import QueryEvaluationContext
from vgi.catalog.indexes import IndexDefinition, IndexKey
from vgi.catalog.layout import LayoutChangeMode, SortKey, SqlExpression, TableLayout, TableLayoutChange
from vgi.catalog.macros import MacroOverload, MacroParameter
from vgi.catalog.options import OptionChanges, OptionDefinitions, OptionState
from vgi.catalog.sql import column_expressions
from vgi.codegen.catalog_contract_fixtures import attach_records, constraint_records, render
from vgi.column_path import RequiredFilterPaths
from vgi.logical_types import LOGICAL_TYPE_PROFILES, validate_logical_values
from vgi.protocol import MacroCreateRequest, TableCreateRequest
from vgi.query_parameters import QueryParameters
from vgi.typed_values import TypedValueBatch
from vgi.worker import Worker


def test_attach_vectors_preserve_default_schema_absence_and_raw_names() -> None:
    fixtures = {name: base64.b64decode(value) for name, value in json.loads(render())["records"].items()}
    assert CatalogAttachResult.ARROW_SCHEMA.field("default_schema").nullable
    for name, source in attach_records().items():
        restored = CatalogAttachResult.deserialize_from_bytes(fixtures[name])
        assert restored.default_schema == source.default_schema
        assert restored == source
        assert restored.serialize_to_bytes() == fixtures[name]


def test_column_metadata_schema_vectors_preserve_literals_and_reject_invalid_values() -> None:
    fixtures = json.loads(render())["column_metadata_schemas"]
    restored = {}
    for name, payload in fixtures["valid"].items():
        schema = pa.ipc.read_schema(pa.py_buffer(base64.b64decode(payload)))
        validate_column_metadata(schema)
        assert pa.ipc.read_schema(schema.serialize()).equals(schema, check_metadata=True)
        restored[name] = schema
    assert column_metadata(restored["absent"].field(0)) == ColumnMetadata()
    assert column_metadata(restored["empty"].field(0)) == ColumnMetadata(comment="", tags={})
    assert column_metadata(restored["literal"].field(0)) == ColumnMetadata(
        comment="A café column\x00 with a literal dot.", tags={"a.b": 'literal "value"', "": "", "雪": "\x00"}
    )
    assert restored["literal"].metadata == {b"provider": b"retained"}
    metadata = restored["empty"].field(0).metadata
    assert metadata is not None
    assert metadata[b"provider.note"] == b"keep me"
    for payload in fixtures["invalid"].values():
        schema = pa.ipc.read_schema(pa.py_buffer(base64.b64decode(payload)))
        with pytest.raises(ValueError, match="."):
            validate_column_metadata(schema)


def test_canonical_constraint_vectors_preserve_every_field() -> None:
    fixtures = {name: base64.b64decode(value) for name, value in json.loads(render())["records"].items()}
    for name, source in constraint_records().items():
        decoded = type(source).deserialize_from_bytes(fixtures[name])
        assert decoded == source
        assert decoded.serialize_to_bytes() == fixtures[name]


def test_generated_schema_vectors_validate_without_a_capability_offer() -> None:
    fixtures = json.loads(render())["generated_column_schemas"]
    owner = fixtures["owner_schema_path"]
    for payload in fixtures["valid"].values():
        schema = pa.ipc.read_schema(pa.py_buffer(base64.b64decode(payload)))
        before = schema.serialize().to_pybytes()
        Worker._require_column_expression_profiles(schema, CatalogCapabilities(), "read generated columns", owner)
        assert schema.serialize().to_pybytes() == before
        for field in schema:
            generated = column_expressions(field)
            if generated.generated is not None:
                assert generated.generation == "virtual"
                assert generated.generated.schema_path == owner
    for name, payload in fixtures["invalid"].items():
        schema = pa.ipc.read_schema(pa.py_buffer(base64.b64decode(payload)))
        with pytest.raises((ValueError, TypeError, NotImplementedError, pa.ArrowInvalid, pa.ArrowIOError)) as error:
            Worker._require_column_expression_profiles(schema, CatalogCapabilities(), "read generated columns", owner)
        assert str(error.value), name


def test_index_vectors_preserve_keys_typed_options_and_association() -> None:
    fixtures = {name: base64.b64decode(value) for name, value in json.loads(render())["records"].items()}
    definition = IndexDefinition.deserialize_from_bytes(fixtures["index_definition"])
    assert definition.keys == [fixtures["index_key_column"], fixtures["index_key_expression"]]
    assert definition.options.serialize_to_bytes() == fixtures["values"]
    assert IndexKey.deserialize_from_bytes(fixtures["index_key_column"]).column_path == ["a.b"]
    assert IndexKey.deserialize_from_bytes(fixtures["index_key_nested"]).column_path == ["record", "a.b"]
    primary = IndexDefinition.deserialize_from_bytes(fixtures["index_definition_primary"])
    assert primary.associated_constraint is not None
    assert primary.associated_constraint.owner_table == definition.table.name


def test_dependency_vectors_preserve_alter_flags_and_column_identity() -> None:
    fixtures = {name: base64.b64decode(value) for name, value in json.loads(render())["records"].items()}
    membership = ObjectDependency.deserialize_from_bytes(fixtures["dependency_membership"])
    assert membership.blocks_alter is False
    assert membership.subject_column_paths is None and membership.dependent_column_path is None
    literal = ObjectDependency.deserialize_from_bytes(fixtures["dependency_column_literal"])
    nested = ObjectDependency.deserialize_from_bytes(fixtures["dependency_column_nested"])
    assert literal.blocks_alter is True and literal.subject_column_paths == [["a.b"]]
    assert nested.subject_column_paths == [["record", "a.b"]]
    generated = ObjectDependency.deserialize_from_bytes(fixtures["dependency_generated"])
    assert generated.dependent == generated.subject
    assert generated.dependent_column_path == ["generated"]
    assert generated.subject_column_paths == [["a.b"], ["record", "a.b"]]
    chain = ObjectDependency.deserialize_from_bytes(fixtures["dependency_generated_chain"])
    assert chain.dependent_column_path == ["second.generated"]
    assert chain.subject_column_paths == [["generated"]]
    snapshot = DependencySnapshot.deserialize_from_bytes(fixtures["dependency_column_snapshot"])
    assert snapshot.dependencies == [
        fixtures["dependency_column_literal"],
        fixtures["dependency_generated"],
        fixtures["dependency_generated_chain"],
    ]
    assert snapshot.serialize_to_bytes() == fixtures["dependency_column_snapshot"]


def test_query_parameter_corpus_preserves_mapping_types_metadata_and_empty_row() -> None:
    fixtures = json.loads(render())["records"]
    for name in ("query_parameters", "query_empty_parameters"):
        source = QueryParameters.deserialize_from_bytes(base64.b64decode(fixtures[name]))
        restored = QueryParameters.deserialize_from_bytes(source.serialize_to_bytes())
        assert restored.batch.equals(source.batch, check_metadata=True)
        assert restored.batch.num_rows == 1
        assert restored.mode == ("positional" if name == "query_empty_parameters" else "named")
        if name == "query_empty_parameters":
            assert restored.batch.num_columns == 0
        else:
            assert not restored.batch.column("optional")[0].is_valid
            assert restored.schema.field("record").type[0].metadata == {b"nested": b"retained"}


def test_column_requests_preserve_raw_paths_and_definitions() -> None:
    fixtures = json.loads(render())["records"]
    names = [
        name
        for name in fixtures
        if name.startswith(("column_", "not_null_")) and name.endswith(("_literal", "_nested"))
    ]
    assert len(names) == 18
    for name in names:
        with pa.ipc.open_stream(base64.b64decode(fixtures[name])) as reader:
            (row,) = reader.read_all().to_pylist()
        nested = name.endswith("_nested")
        assert row["column_path"] == (["a", 'b."quoted'] if nested else ["a.b"])
        assert row["transaction_opaque_data"] == (b"transaction" if nested else None)
        if "column_definition" in row:
            definition = pa.ipc.read_schema(pa.py_buffer(row["column_definition"]))
            assert definition.field(0).name == row["column_path"][-1]
            if name.startswith("column_add_"):
                expression = column_expressions(definition.field(0)).default
                assert expression is not None and expression.sql == "7"
                assert expression.schema_path == row["schema_path"]
                assert expression.evaluation_context == QueryEvaluationContext()
                metadata = definition.field(0).metadata
                assert metadata is not None
                assert b"default" not in metadata
        if "expression" in row:
            if name == "column_type_change_literal":
                assert row["expression"] is None
            else:
                assert isinstance(row["expression"], bytes)
                expression = SqlExpression.deserialize_from_bytes(row["expression"])
                assert expression.sql == "7" and expression.schema_path == row["schema_path"]
                assert expression.evaluation_context == QueryEvaluationContext()
        if "comment" in row:
            assert row["comment"] == ("kept" if nested else None)


def test_checked_in_corpus_and_contents() -> None:
    path = Path(__file__).resolve().parents[1] / "conformance/catalog-contracts.json"
    assert path.read_text() == render()
    fixtures = {name: base64.b64decode(value) for name, value in json.loads(path.read_text())["records"].items()}
    capabilities = CatalogCapabilities.deserialize_from_bytes(fixtures["capabilities"])
    assert capabilities.read == [SCHEMA_OPTIONS_READ]
    assert capabilities.ddl == [SCHEMA_OPTIONS_CREATE]
    definitions = OptionDefinitions.deserialize_from_bytes(fixtures["definitions"])
    assert definitions.create_names == ["optional", "record"]
    assert definitions.alter_names == ["optional"]
    values = TypedValueBatch.deserialize_from_bytes(fixtures["values"])
    assert definitions.schema.equals(values.schema, check_metadata=True)
    assert definitions.defaults.batch.equals(values.batch, check_metadata=True)
    assert values.schema.metadata == {b"provider": b"catalog-contracts/v1"}
    assert values.schema.field("record").type[0].metadata == {b"nested": b"retained"}
    assert not values.batch.column("optional")[0].is_valid
    changes = OptionChanges.deserialize_from_bytes(fixtures["changes"])
    assert changes.set_values is not None
    assert changes.set_values.batch.equals(values.batch, check_metadata=True)
    assert changes.reset_names == ["reset.me"]
    assert OptionChanges.deserialize_from_bytes(fixtures["reset_only"]).set_values is None
    state = OptionState.deserialize_from_bytes(fixtures["state"])
    assert state.explicit_names == ["optional"]
    assert state.values.batch.equals(values.batch, check_metadata=True)
    assert OptionState.deserialize_from_bytes(fixtures["empty_state"]).values.batch.num_rows == 1
    paths = RequiredFilterPaths.deserialize_from_bytes(fixtures["paths"])
    assert paths.groups == [[["a.b"], ["a", "b"]], [["record", 'literal."name']]]
    assert RequiredFilterPaths.deserialize_from_bytes(fixtures["empty_paths"]).groups == []


def test_layout_corpus_preserves_nested_keys_and_nullable_changes() -> None:
    fixtures = {name: base64.b64decode(value) for name, value in json.loads(render())["records"].items()}
    expression = SqlExpression.deserialize_from_bytes(fixtures["expression"])
    assert expression.schema_path == ["one.two", "nested"]
    key = SortKey.deserialize_from_bytes(fixtures["sort_key"])
    assert key.expression == expression and key.direction == "desc" and key.null_order == "first"
    layout = TableLayout.deserialize_from_bytes(fixtures["layout"])
    assert layout.partition_expressions() == [expression] and layout.sort_keys() == [key]
    change = TableLayoutChange.deserialize_from_bytes(fixtures["layout_change"])
    assert change.mode == LayoutChangeMode.REWRITE and change.partition_by == []
    clear = TableLayoutChange.deserialize_from_bytes(fixtures["layout_clear"])
    assert clear.partition_by is None and clear.sorted_by == []
    request = TableCreateRequest.deserialize_from_bytes(fixtures["table_create"])
    assert request.layout == layout and request.options is not None
    assert request.options.serialize_to_bytes() == fixtures["values"]
    old = TableCreateRequest.deserialize_from_bytes(fixtures["table_create_legacy"])
    assert old.options is None and old.layout is None and old.comment is None and old.tags is None
    info = TableInfo.deserialize_from_bytes(fixtures["table_info"])
    assert info.layout == layout and info.options is not None
    assert info.options.serialize_to_bytes() == fixtures["state"]
    assert info.definition_revision == b"revision\0\xff"
    assert info.constraint_infos()[0].definition.serialize_to_bytes() == request.constraint_definitions[0]
    assert info.constraint_infos()[0].ref.name == "fixture-stable-key"


def test_views_preserve_ordered_raw_aliases() -> None:
    fixtures = json.loads(render())["records"]
    for name, aliases in (("view_info", ["a.b", 'with"quote']), ("view_info_implicit", [])):
        info = ViewInfo.deserialize_from_bytes(base64.b64decode(fixtures[name]))
        assert info.aliases == aliases
        restored = ViewInfo.deserialize_from_bytes(info.serialize_to_bytes())
        assert restored == info


def test_views_preserve_security_modes() -> None:
    fixtures = json.loads(render())["records"]
    for name, security in (("view_info", "regular"), ("view_info_barrier", "barrier")):
        info = ViewInfo.deserialize_from_bytes(base64.b64decode(fixtures[name]))
        assert info.security == security
        assert ViewInfo.deserialize_from_bytes(info.serialize_to_bytes()) == info


def test_logical_type_vectors_preserve_tuple_null_and_absence() -> None:
    fixtures = {name: base64.b64decode(value) for name, value in json.loads(render())["records"].items()}
    vectors = {
        name: TypedValueBatch.deserialize_from_bytes(payload)
        for name, payload in fixtures.items()
        if name.startswith("logical_")
    }
    for values in vectors.values():
        validate_logical_values(values, selected_profiles=LOGICAL_TYPE_PROFILES)
        assert TypedValueBatch.deserialize_from_bytes(values.serialize_to_bytes()).batch.equals(
            values.batch, check_metadata=True
        )
    assert not vectors["logical_tuple"].schema.equals(vectors["logical_struct"].schema, check_metadata=True)
    assert not vectors["logical_variant_sql_null"].batch.column(0)[0].is_valid
    expected = {
        "logical_variant_null": ("010000", "00"),
        "logical_variant_absent": ("010000", "020000"),
        "logical_variant_null_member": ("0101000770726573656e74", "020100000100"),
    }
    for name, (metadata, value) in expected.items():
        field = vectors[name].schema.field(0)
        assert field.metadata == {
            b"ARROW:extension:name": b"arrow.parquet.variant",
            b"ARROW:extension:metadata": b"",
        }
        assert field.type == pa.struct(
            [pa.field("metadata", pa.binary(), nullable=False), pa.field("value", pa.binary())]
        )
        assert vectors[name].batch.column(0)[0].as_py() == {
            "metadata": bytes.fromhex(metadata),
            "value": bytes.fromhex(value),
        }


def test_macro_vectors_preserve_single_and_multiple_overload_families() -> None:
    fixtures = {name: base64.b64decode(value) for name, value in json.loads(render())["records"].items()}
    parameter = MacroParameter.deserialize_from_bytes(fixtures["macro_parameter"])
    assert parameter.declared_type is not None and parameter.declared_type.field(0).type == pa.int64()
    assert parameter.default_value is not None and not parameter.default_value.batch.column(0)[0].is_valid
    expression = MacroParameter.deserialize_from_bytes(fixtures["macro_parameter_expression"])
    assert expression.default_expression is not None and expression.default_expression.sql == "2 + 3"
    assert MacroParameter.deserialize_from_bytes(fixtures["macro_parameter_untyped"]).declared_type is None
    overload = MacroOverload.deserialize_from_bytes(fixtures["macro_overload"])
    assert [parameter.name for parameter in overload.parameters] == ["x", "fallback", "scale"]
    create = MacroCreateRequest.deserialize_from_bytes(fixtures["macro_create"])
    info = MacroInfo.deserialize_from_bytes(fixtures["macro_info"])
    assert len(create.overloads) == 2
    assert len(info.overloads) == 2
    resolved = info.overloads[0].parameters[2]
    assert resolved.default_expression is None
    assert resolved.default_value is not None and resolved.default_value.batch.column(0)[0].as_py() == 5
    single_create = MacroCreateRequest.deserialize_from_bytes(fixtures["macro_create_single"])
    single_info = MacroInfo.deserialize_from_bytes(fixtures["macro_info_single"])
    assert single_create.overloads == single_info.overloads
    assert len(single_info.overloads) == 1
    assert single_info.overloads[0].definition == "x + 1"
    for record_type in (MacroCreateRequest, MacroInfo):
        assert not record_type.ARROW_SCHEMA.field("overloads").nullable
        assert not {"parameters", "definition", "parameter_default_values", "arguments_schema"}.intersection(
            record_type.ARROW_SCHEMA.names
        )
