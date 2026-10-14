# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103
"""Run the same public RPC lifecycle against independently built SDK workers.

Set VGI_METADATA_WORKERS to a JSON object mapping SDK names to worker argv lists.
The workers must expose the ordinary mutable ``contents_reval`` test catalog.
"""

import json
import os
from dataclasses import replace

import pytest

from vgi.catalog import CatalogMetadataChanges, OnConflict, SchemaObjectType
from vgi.catalog.capabilities import SCHEMA_METADATA_ALTER, CatalogCapabilities
from vgi.client import Client
from vgi.protocol import ClientCapabilities

WORKERS: dict[str, list[str]] = json.loads(os.environ.get("VGI_METADATA_WORKERS", "{}"))


@pytest.mark.parametrize("sdk", sorted(WORKERS))
def test_view_metadata_lifecycle_over_public_rpc(sdk: str) -> None:
    with Client(server_path=WORKERS[sdk], worker_limit=1) as client:
        token = client.catalog_attach(
            name="contents_reval",
            data_version_spec=None,
            implementation_version=None,
        ).attach_opaque_data
        target = dict(attach_opaque_data=token, schema_path=["main"], name="view.with.dot")
        client.view_create(
            **target,
            definition="SELECT 1 AS x, 2 AS y",
            aliases=["a.b", 'with"quote'],
            comment="",
            tags={"owner": "one", "empty": ""},
            column_comments={"a.b": "", 'with"quote': "kept"},
        )
        original = client.view_get(**target)
        original_version = client.catalog_version(attach_opaque_data=token)
        assert original.comment == ""
        assert original.tags == {"owner": "one", "empty": ""}
        assert original.column_comments == {"a.b": "", 'with"quote': "kept"}
        assert original.aliases == ["a.b", 'with"quote'] and original.security == "regular"
        assert client.schema_contents(attach_opaque_data=token, path=["main"], type=SchemaObjectType.VIEW) == [original]
        client.view_create(**target, definition="SELECT 9", on_conflict=OnConflict.IGNORE, comment="ignored")
        assert client.view_get(**target) == original
        assert client.catalog_version(attach_opaque_data=token) == original_version
        for key in ("", "a\0b"):
            for mode in (OnConflict.IGNORE, OnConflict.REPLACE):
                with pytest.raises(Exception, match="identifier|alias|empty|NUL"):
                    client.view_create(**target, definition="SELECT 9", on_conflict=mode, column_comments={key: "bad"})
                assert client.view_get(**target) == original
                assert client.catalog_version(attach_opaque_data=token) == original_version
        for path in ([], ["a", "b"], [""], ["a\0b"]):
            with pytest.raises(Exception, match="identifier|column path|component|empty|NUL"):
                client.view_column_comment_set(**target, column_path=path, comment="bad", ignore_not_found=True)
            assert client.view_get(**target) == original
            assert client.catalog_version(attach_opaque_data=token) == original_version
        client.view_column_comment_set(**target, column_path=["a.b"], comment=None)
        client.view_column_comment_set(**target, column_path=['with"quote'], comment="")
        client.view_comment_set(**target, comment=None)
        assert client.view_get(**target) == replace(original, comment=None, column_comments={'with"quote': ""})
        client.view_create(**target, definition="SELECT 3 AS z", on_conflict=OnConflict.REPLACE)
        replaced = client.view_get(**target)
        assert replaced.comment is None and replaced.tags == {} and replaced.column_comments == {}
        assert replaced.aliases == []
        client.view_drop(**target)
        client.view_column_comment_set(**target, column_path=["a.b"], comment="unused", ignore_not_found=True)
        with pytest.raises(Exception, match="not found|does not exist"):
            client.view_column_comment_set(**target, column_path=["a.b"], comment="unused")


@pytest.mark.parametrize("sdk", sorted(WORKERS))
def test_schema_creation_metadata_over_public_rpc(sdk: str) -> None:
    with Client(server_path=WORKERS[sdk], worker_limit=1) as client:
        token = client.catalog_attach(
            name="contents_reval", data_version_spec=None, implementation_version=None
        ).attach_opaque_data
        target = dict(attach_opaque_data=token, path=["a.b"])
        tags = {"owner": "one", "empty": "", "__proto__": "literal", "a.b": "quoted"}
        initial_version = client.catalog_version(attach_opaque_data=token)
        client.schema_create(**target, comment="", tags=tags)
        original = client.schema_get(**target)
        assert original.comment == "" and original.tags == tags
        assert client.catalog_version(attach_opaque_data=token) > initial_version
        client.schema_create(attach_opaque_data=token, path=["a"], comment="parent")
        client.schema_create(attach_opaque_data=token, path=["a", "b"], comment="nested", tags={"owner": "two"})
        assert client.schema_get(attach_opaque_data=token, path=["a", "b"]).comment == "nested"
        assert client.schema_get(**target) == original
        listed = next(s for s in client.schemas(attach_opaque_data=token) if s.path == ["a.b"])
        assert replace(listed, estimated_object_count=original.estimated_object_count) == original
        snapshot = client.contents(attach_opaque_data=token)
        cached = next(s.schema for s in snapshot.schemas if s.schema.path == ["a.b"])
        assert replace(cached, estimated_object_count=original.estimated_object_count) == original
        version = client.catalog_version(attach_opaque_data=token)
        with pytest.raises(Exception, match="already exists"):
            client.schema_create(**target, comment="bad", tags={"bad": "bad"})
        assert client.schema_get(**target) == original
        assert client.catalog_version(attach_opaque_data=token) == version
        client.schema_create(**target, on_conflict=OnConflict.IGNORE, comment="ignored", tags={"ignored": "yes"})
        assert client.schema_get(**target) == original
        assert client.catalog_version(attach_opaque_data=token) == version
        client.schema_create(**target, on_conflict=OnConflict.REPLACE)
        replacement = client.schema_get(**target)
        assert replacement.comment is None and replacement.tags == {}
        assert client.catalog_version(attach_opaque_data=token) > version
        assert client.schema_get(attach_opaque_data=token, path=["a", "b"]).tags == {"owner": "two"}
        client.schema_create(**target, on_conflict=OnConflict.REPLACE, comment="reset", tags=tags)
        replacement = client.schema_get(**target)
        assert replacement.comment == "reset" and replacement.tags == tags
        snapshot = client.contents(attach_opaque_data=token)
        cached = next(s.schema for s in snapshot.schemas if s.schema.path == ["a.b"])
        assert replace(cached, estimated_object_count=replacement.estimated_object_count) == replacement
        client.schema_drop(**target)
        assert client.schema_get(**target) is None
        assert client.schema_get(attach_opaque_data=token, path=["a", "b"]).comment == "nested"


@pytest.mark.parametrize("sdk", sorted(WORKERS))
def test_unqualified_provider_rejects_schema_metadata_mutation(sdk: str) -> None:
    offer = ClientCapabilities(
        engine="reference-client",
        native_formats=[],
        catalogs=[],
        can_stream=False,
        filter_encodings=[],
        catalog=CatalogCapabilities(ddl=[SCHEMA_METADATA_ALTER]),
    )
    with Client(server_path=WORKERS[sdk], worker_limit=1) as client:
        attached = client.catalog_attach(
            name="contents_reval", data_version_spec=None, implementation_version=None, client_capabilities=offer
        )
        token = attached.attach_opaque_data
        assert (
            attached.selected_capabilities is None
            or SCHEMA_METADATA_ALTER not in attached.selected_capabilities.names()
        )
        target = dict(attach_opaque_data=token, path=["main"])
        before = client.schema_get(**target)
        version = client.catalog_version(attach_opaque_data=token)
        with pytest.raises(
            Exception,
            match="(?i)not implemented|unimplemented|not supported|does not support|requires.*vgi.schema.metadata",
        ):
            client.schema_alter_metadata(
                **target, changes=CatalogMetadataChanges(set_comment=True, comment="bad", set_tags={"a": "bad"})
            )
        assert client.schema_get(**target) == before
        assert client.catalog_version(attach_opaque_data=token) == version
