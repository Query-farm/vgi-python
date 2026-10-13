# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103

"""The corpus distributed to every SDK retains semantic metadata and raw paths."""

import base64
import json
from pathlib import Path

import pyarrow as pa

from vgi.catalog.capabilities import SCHEMA_OPTIONS_CREATE, SCHEMA_OPTIONS_READ, CatalogCapabilities
from vgi.catalog.catalog_interface import TableInfo, ViewInfo
from vgi.catalog.layout import LayoutChangeMode, SortKey, SqlExpression, TableLayout, TableLayoutChange
from vgi.catalog.options import OptionChanges, OptionDefinitions, OptionState
from vgi.codegen.catalog_contract_fixtures import render
from vgi.column_path import RequiredFilterPaths
from vgi.protocol import TableCreateRequest
from vgi.typed_values import TypedValueBatch


def test_column_requests_preserve_raw_paths_and_definitions() -> None:
    fixtures = json.loads(render())["records"]
    names = [name for name in fixtures if name.startswith(("column_", "not_null_"))]
    assert len(names) == 18
    for name in names:
        with pa.ipc.open_stream(base64.b64decode(fixtures[name])) as reader:
            (row,) = reader.read_all().to_pylist()
        nested = name.endswith("_nested")
        assert row["column_path"] == (["a", 'b."quoted'] if nested else ["a.b"])
        assert row["transaction_opaque_data"] == (b"transaction" if nested else None)
        if "column_definition" in row:
            definition = pa.ipc.read_schema(pa.BufferReader(row["column_definition"]))
            assert definition.field(0).name == row["column_path"][-1]
            if name.startswith("column_add_"):
                assert definition.field(0).metadata == {b"default": b"7"}
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
