# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Protocol 2.0 schema-path contract tests."""

import click
import pyarrow as pa
import pytest
from vgi_rpc.utils import deserialize_record_batch

from vgi.arguments import Arguments
from vgi.catalog import (
    AttachOpaqueData,
    Catalog,
    ReadOnlyCatalogInterface,
    ScanBranch,
    ScanFunctionResult,
    Schema,
    SchemaInfo,
    SchemaObjectType,
    Table,
)
from vgi.client.cli_utils import SCHEMA_PATH
from vgi.codegen._common import EXTRA_RESPONSE_TYPES, REQUEST_TYPES, collect_schemas
from vgi.invocation import FunctionType
from vgi.protocol import (
    BindRequest,
    CatalogAttachRequest,
    ClientCapabilities,
    IndexCreateRequest,
    MacroCreateRequest,
    TableCreateRequest,
)
from vgi.schema_path import schema_path_key, sql_qualified_name
from vgi.transactor.server import TransactorImpl
from vgi.worker import Worker


def test_schema_paths_are_list_of_utf8_on_generated_records() -> None:
    """Generated Arrow records expose schema paths as ordered component lists."""
    expected = pa.list_(pa.string())
    assert BindRequest.ARROW_SCHEMA.field("schema_path").type == expected
    assert SchemaInfo.ARROW_SCHEMA.field("path").type == expected
    for request_type in (TableCreateRequest, MacroCreateRequest, IndexCreateRequest):
        assert request_type.ARROW_SCHEMA.field("schema_path").type == expected


def test_every_codegen_schema_uses_v2_schema_path_fields() -> None:
    """Generated SDK contracts contain no legacy scalar owner identifiers."""
    expected = pa.list_(pa.string())
    legacy_names = {"schema_name", "source_schema", "referenced_schema"}
    path_names = {"schema_path", "source_schema_path", "referenced_schema_path"}

    for emitted in collect_schemas(extra_response_types=(*EXTRA_RESPONSE_TYPES, *REQUEST_TYPES)):
        assert legacy_names.isdisjoint(emitted.schema.names), emitted.name
        for field in emitted.schema:
            if field.name in path_names:
                assert field.type == expected, f"{emitted.name}.{field.name}: {field.type}"


def test_bind_request_preserves_nested_path_and_dotted_component() -> None:
    """Serialization retains nesting and never splits a dot inside a component."""
    request = BindRequest(
        function_name="calculate",
        arguments=Arguments(positional=()),
        function_type=FunctionType.SCALAR,
        schema_path=["tenant.with.dot", "analytics", "daily"],
    )
    restored = BindRequest.deserialize_from_bytes(request.serialize_to_bytes())
    assert restored.schema_path == ["tenant.with.dot", "analytics", "daily"]


def test_catalog_attach_round_trips_typed_client_capabilities() -> None:
    """The opaque capabilities blob has one shared, independently generated schema."""
    capabilities = ClientCapabilities(
        engine="duckdb",
        native_formats=["parquet", "csv", "json"],
        catalogs=["ducklake", "iceberg"],
        can_stream=False,
        filter_encodings=["vgi.filters.v1"],
    )
    request = CatalogAttachRequest(
        name="lake",
        options=None,
        data_version_spec=None,
        implementation_version=None,
        client_capabilities=capabilities,
    )

    restored = CatalogAttachRequest.deserialize_from_bytes(request.serialize_to_bytes())
    assert restored.client_capabilities == capabilities
    assert CatalogAttachRequest.ARROW_SCHEMA.field("client_capabilities").type == pa.binary()


def test_manual_scan_records_preserve_both_kinds_of_schema_path() -> None:
    """Manual Arrow records use paths for function and catalog-table branches."""
    function = ScanFunctionResult(
        function_name="scan",
        positional_arguments=[],
        named_arguments={},
        schema_path=["functions", "geo"],
    )
    function_batch, _ = deserialize_record_batch(function.serialize())
    assert ScanFunctionResult.deserialize(function_batch).schema_path == ["functions", "geo"]

    branch = ScanBranch(
        function_name="",
        positional_arguments=[],
        named_arguments={},
        source_catalog="lake",
        source_schema_path=["tenant.with.dot", "bronze", "events"],
        source_table="clicks",
    )
    branch_batch, _ = deserialize_record_batch(branch.serialize())
    restored = ScanBranch.deserialize(branch_batch)
    assert restored.source_schema_path == ["tenant.with.dot", "bronze", "events"]


def test_structural_keys_do_not_flatten_components() -> None:
    """A dotted identifier remains distinct from two nested identifiers."""
    assert schema_path_key(["a.b"]) != schema_path_key(["a", "b"])
    assert schema_path_key(["Analytics", "Daily"]) == schema_path_key(["analytics", "daily"])


def test_catalog_routes_dotted_and_nested_paths_independently() -> None:
    """Declarative catalog registries key objects by component tuple."""

    class NestedCatalog(ReadOnlyCatalogInterface):
        catalog = Catalog(
            name="nested",
            default_schema="root",
            schemas=[
                Schema(path=["root"]),
                Schema(path=["a.b"], tables=[Table(name="events", columns=pa.schema({"kind": pa.string()}))]),
                Schema(path=["a"]),
                Schema(path=["a", "b"], tables=[Table(name="events", columns=pa.schema({"value": pa.int64()}))]),
            ],
        )

    catalog = NestedCatalog()
    attach = AttachOpaqueData(b"nested")
    dotted = catalog.table_get(
        attach_opaque_data=attach,
        transaction_opaque_data=None,
        schema_path=["a.b"],
        name="events",
    )
    nested = catalog.table_get(
        attach_opaque_data=attach,
        transaction_opaque_data=None,
        schema_path=["a", "b"],
        name="events",
    )
    assert dotted is not None and pa.ipc.read_schema(pa.BufferReader(dotted.columns)).names == ["kind"]
    assert nested is not None and pa.ipc.read_schema(pa.BufferReader(nested.columns)).names == ["value"]
    assert [
        item.schema_path
        for item in catalog.schema_contents(
            attach_opaque_data=attach,
            transaction_opaque_data=None,
            path=["a", "b"],
            type=SchemaObjectType.TABLE,
        )
    ] == [["a", "b"]]


def test_declarative_catalog_requires_nested_schema_parents() -> None:
    """A child path cannot be advertised without its materializable parent."""
    with pytest.raises(ValueError, match=r"has no declared parent \['tenant'\]"):
        Catalog(
            name="missing-parent",
            default_schema="main",
            schemas=[Schema(path=["main"]), Schema(path=["tenant", "analytics"])],
        )


def test_wire_schema_enumeration_places_parents_before_children() -> None:
    """Workers publish deterministic root-first schema paths."""

    class NestedWorker(Worker):
        catalog = Catalog(
            name="nested",
            default_schema="z",
            schemas=[Schema(path=["z"]), Schema(path=["a"]), Schema(path=["a", "b"])],
        )

    infos = NestedWorker(quiet=True).catalog_schemas(b"readonly-catalog-").to_infos()
    assert [info.path for info in infos] == [["z"], ["a"], ["a", "b"]]


@pytest.mark.parametrize("path", [[], [""], "main"])
def test_invalid_schema_paths_are_rejected(path: object) -> None:
    """Internal routing refuses empty paths and the removed scalar-string shape."""
    with pytest.raises((TypeError, ValueError)):
        schema_path_key(path)  # type: ignore[arg-type]


def test_sql_qualification_quotes_each_component_separately() -> None:
    """SQL rendering quotes each raw component without changing boundaries."""
    assert sql_qualified_name(["a.b", 'c"d'], "orders") == '"a.b"."c""d"."orders"'


def test_cli_schema_paths_preserve_components() -> None:
    """CLI input uses JSON for nesting and never treats dots as separators."""
    assert SCHEMA_PATH.convert("main", None, None) == ["main"]
    assert SCHEMA_PATH.convert("a.b", None, None) == ["a.b"]
    assert SCHEMA_PATH.convert('  ["a", "b"]', None, None) == ["a", "b"]
    with pytest.raises(click.BadParameter, match="must not be empty"):
        SCHEMA_PATH.convert("", None, None)


def test_duckdb_15_transactor_rejects_nested_schema_paths() -> None:
    """The bundled 1.5 adapter fails explicitly instead of flattening a v2 path."""
    assert TransactorImpl._duckdb_15_schema_path(["main"]) == ["main"]
    with pytest.raises(NotImplementedError, match="one-component schema paths"):
        TransactorImpl._duckdb_15_schema_path(["tenant", "analytics"])
