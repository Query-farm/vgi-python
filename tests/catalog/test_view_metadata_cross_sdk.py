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

from vgi.catalog import OnConflict, SchemaObjectType
from vgi.client import Client

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
