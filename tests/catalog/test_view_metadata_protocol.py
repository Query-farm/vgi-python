# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103
"""Creation metadata and comment mutations through the public protocol."""

from dataclasses import replace

import pytest

from vgi.catalog import OnConflict, SchemaObjectType

from .test_view_security_protocol import attach, backend, client  # noqa: F401

pytestmark = pytest.mark.usefixtures("backend")


@pytest.mark.parametrize("security", ["regular", "barrier"])
def test_creation_and_comments_survive_reconnect_commit_and_rollback(security: str) -> None:
    with client() as c:
        token = attach(c)
        target = dict(attach_opaque_data=token, schema_path=["main"], name="v")
        c.view_create(
            **target,
            definition="SELECT 1 AS x, 2 AS y",
            aliases=["a.b", 'with"quote'],
            security=security,
            comment="",
            tags={"team": "one", "empty": ""},
            column_comments={"a.b": "", 'with"quote': "kept"},
        )
        original = c.view_get(**target)
        assert original.comment == "" and original.tags == {"team": "one", "empty": ""}
        assert original.column_comments == {"a.b": "", 'with"quote': "kept"}
        c.view_create(**target, definition="SELECT 9", on_conflict=OnConflict.IGNORE)
        assert c.view_get(**target) == original
        tx = c.catalog_transaction_begin(attach_opaque_data=token)
        c.view_column_comment_set(**target, column_path=["a.b"], comment="new", transaction_opaque_data=tx)
        c.view_comment_set(**target, comment=None, transaction_opaque_data=tx)
        changed = c.view_get(**target, transaction_opaque_data=tx)
        assert changed == replace(original, comment=None, column_comments={"a.b": "new", 'with"quote': "kept"})
        assert c.view_get(**target) == original
    with client() as c:
        c.catalog_transaction_rollback(attach_opaque_data=token, transaction_opaque_data=tx)
        assert c.view_get(**target) == original
        tx = c.catalog_transaction_begin(attach_opaque_data=token)
        c.view_column_comment_set(**target, column_path=["a.b"], comment=None, transaction_opaque_data=tx)
        c.view_column_comment_set(**target, column_path=['with"quote'], comment="", transaction_opaque_data=tx)
        c.catalog_transaction_commit(attach_opaque_data=token, transaction_opaque_data=tx)
    with client() as c:
        current = c.view_get(**target)
        assert current == replace(original, column_comments={'with"quote': ""})
        assert c.schema_contents(attach_opaque_data=token, path=["main"], type=SchemaObjectType.VIEW) == [current]
        c.view_create(**target, definition="SELECT 3 AS z", on_conflict=OnConflict.REPLACE)
        replaced = c.view_get(**target)
        assert replaced.comment is None and replaced.tags == {} and replaced.column_comments == {}
        assert replaced.aliases == [] and replaced.security == "regular"


@pytest.mark.parametrize("path", [[], ["a", "b"], [""], ["a\0b"]])
def test_invalid_view_column_path_rejects_before_mutation(path: list[str]) -> None:
    with client() as c:
        token = attach(c)
        target = dict(attach_opaque_data=token, schema_path=["main"], name="v")
        c.view_create(**target, definition="SELECT 1", aliases=["a.b"], comment="kept")
        original = c.view_get(**target)
        with pytest.raises(Exception, match="identifier|column path|component|empty|NUL"):
            c.view_column_comment_set(**target, column_path=path, comment="bad", ignore_not_found=True)
        assert c.view_get(**target) == original


def test_missing_view_comment_mutation_obeys_ignore_not_found() -> None:
    with client() as c:
        token = attach(c)
        target = dict(attach_opaque_data=token, schema_path=["main"], name="absent", column_path=["a.b"])
        c.view_column_comment_set(**target, comment="unused", ignore_not_found=True)
        with pytest.raises(Exception, match="does not exist"):
            c.view_column_comment_set(**target, comment="unused")
