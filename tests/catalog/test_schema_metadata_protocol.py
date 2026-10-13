# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103
"""Schema creation metadata shares durable snapshots with typed options."""

import pytest

from vgi.catalog import OnConflict
from vgi.catalog.options import OptionChanges

from .schema_options_reference import values
from .test_schema_options_protocol import _attach, _client, backend  # noqa: F401

pytestmark = pytest.mark.usefixtures("backend")


def test_metadata_options_and_conflicts_survive_transaction_reconnect() -> None:
    with _client() as client:
        token = _attach(client).attach_opaque_data
        target = dict(attach_opaque_data=token, path=["a.b"])
        client.schema_create(**target, comment="", tags={"owner": "one"}, options=values(limit=25))
        original = client.schema_get(**target)
        version = client.catalog_version(attach_opaque_data=token)
        client.schema_create(**target, on_conflict=OnConflict.IGNORE, comment="ignored", options=values(limit=3))
        assert client.catalog_version(attach_opaque_data=token) == version
        ignored = client.schema_get(**target)
        assert ignored.comment == "" and ignored.tags == {"owner": "one"}
        assert ignored.options.values.batch.equals(original.options.values.batch)
        tx = client.catalog_transaction_begin(attach_opaque_data=token)
        client.schema_create(**target, on_conflict=OnConflict.REPLACE, transaction_opaque_data=tx)
        pending = client.schema_get(**target, transaction_opaque_data=tx)
        assert pending.comment is None and pending.tags == {} and pending.options.explicit_names == []
        assert client.schema_get(**target).comment == ""
    with _client() as client:
        client.catalog_transaction_rollback(attach_opaque_data=token, transaction_opaque_data=tx)
        restored = client.schema_get(**target)
        assert restored.comment == "" and restored.tags == {"owner": "one"}
        assert restored.options.values.batch.equals(original.options.values.batch)
        assert client.catalog_version(attach_opaque_data=token) == version
        tx = client.catalog_transaction_begin(attach_opaque_data=token)
        client.schema_alter_options(
            **target, transaction_opaque_data=tx, changes=OptionChanges(set_values=values(limit=30))
        )
    with _client() as client:
        client.catalog_transaction_commit(attach_opaque_data=token, transaction_opaque_data=tx)
        updated = client.schema_get(**target)
        assert updated.comment == "" and updated.tags == {"owner": "one"}
        assert updated.options.values.batch.column("limit")[0].as_py() == 30
        tx = client.catalog_transaction_begin(attach_opaque_data=token)
        client.schema_create(
            **target,
            on_conflict=OnConflict.REPLACE,
            comment="committed",
            tags={"owner": "two"},
            transaction_opaque_data=tx,
        )
    with _client() as client:
        client.catalog_transaction_commit(attach_opaque_data=token, transaction_opaque_data=tx)
        replaced = client.schema_get(**target)
        assert replaced.comment == "committed" and replaced.tags == {"owner": "two"}
        assert replaced.options.explicit_names == []
