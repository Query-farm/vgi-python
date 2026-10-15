# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103
"""Live typed-index discovery and refusal across independent SDK workers."""

import json
import os

import pytest

from vgi.catalog import OnConflict, SchemaObjectType
from vgi.catalog.capabilities import INDEX_DEFINITION_CREATE, INDEX_DEFINITION_READ, INDEX_DROP, CatalogCapabilities
from vgi.catalog.dependencies import CatalogObjectRef
from vgi.catalog.indexes import IndexConstraintType, IndexDefinition, IndexKey
from vgi.client import Client
from vgi.protocol import ClientCapabilities
from vgi.typed_values import TypedValueBatch

WORKERS: dict[str, list[str]] = json.loads(os.environ.get("VGI_METADATA_WORKERS", "{}"))
OFFER = ClientCapabilities(
    engine="reference-client",
    native_formats=[],
    catalogs=[],
    can_stream=False,
    filter_encodings=[],
    catalog=CatalogCapabilities(read=[INDEX_DEFINITION_READ], ddl=sorted([INDEX_DEFINITION_CREATE, INDEX_DROP])),
)
UNSUPPORTED = (
    "(?i)unsupported|unimplemented|not implemented|does not support|not supported|capability|negotiated|requires"
)


@pytest.mark.parametrize("sdk", sorted(set(WORKERS) & {"typescript"}))
def test_live_index_discovery_has_explicit_metadata_only_definitions(sdk):
    with Client(server_path=WORKERS[sdk], worker_limit=1) as client:
        token = client.catalog_attach(
            name="example", data_version_spec=None, implementation_version=None, client_capabilities=OFFER
        ).attach_opaque_data
        listed = client.schema_contents(attach_opaque_data=token, path=["data"], type=SchemaObjectType.INDEX)
        assert {item.name for item in listed} == {"idx_numbers_value", "idx_numbers_value_unique"}
        for info in listed:
            fetched = client.index_get(attach_opaque_data=token, schema_path=["data"], name=info.name)
            assert fetched == info
            assert info.definition.table == CatalogObjectRef(kind="table", schema_path=["data"], name="numbers")
            assert info.definition.algorithm == "fixture.metadata"
            assert info.definition.constraint_type == IndexConstraintType.NONE
            assert info.definition.associated_constraint is None
            assert [key.column_path for key in info.definition.key_definitions()] == [["value"]]
            assert info.options.values == info.definition.options == TypedValueBatch.empty()
            assert info.options.explicit_names == []


@pytest.mark.parametrize("sdk", sorted(WORKERS))
def test_unqualified_index_mutations_leave_catalog_unchanged(sdk):
    with Client(server_path=WORKERS[sdk], worker_limit=1) as client:
        attached = client.catalog_attach(
            name="contents_reval", data_version_spec=None, implementation_version=None, client_capabilities=OFFER
        )
        assert attached.selected_capabilities is None or not {INDEX_DEFINITION_CREATE, INDEX_DROP}.intersection(
            attached.selected_capabilities.names()
        )
        token = attached.attach_opaque_data
        version = client.catalog_version(attach_opaque_data=token)
        before = client.contents(attach_opaque_data=token)
        definition = IndexDefinition(
            table=CatalogObjectRef(kind="table", schema_path=["main"], name="items"),
            algorithm="fixture.metadata",
            keys=[IndexKey(column_path=["id"]).serialize_to_bytes()],
            options=TypedValueBatch.empty(),
            constraint_type=IndexConstraintType.NONE,
        )
        for conflict in (OnConflict.ERROR, OnConflict.REPLACE):
            with pytest.raises(Exception, match=UNSUPPORTED):
                client.index_create(
                    attach_opaque_data=token,
                    schema_path=["main"],
                    name="probe",
                    definition=definition,
                    on_conflict=conflict,
                )
        with pytest.raises(Exception, match=UNSUPPORTED):
            client.index_drop(attach_opaque_data=token, schema_path=["main"], name="probe", ignore_not_found=True)
        with pytest.raises(Exception, match=UNSUPPORTED):
            client.index_option_definitions(
                attach_opaque_data=token, table=definition.table, algorithm=definition.algorithm
            )
        assert client.catalog_version(attach_opaque_data=token) == version
        assert client.contents(attach_opaque_data=token) == before
