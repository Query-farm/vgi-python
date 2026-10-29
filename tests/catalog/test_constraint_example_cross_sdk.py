# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103
"""Exercise canonical discovery through independently built example workers."""

from __future__ import annotations

import json
import os

import pyarrow as pa
import pytest

from vgi.catalog import SchemaObjectType
from vgi.catalog.constraints import ConstraintEnforcement, ConstraintValidation, validate_constraint_schema
from vgi.client import Client

WORKERS: dict[str, list[str]] = json.loads(os.environ.get("VGI_METADATA_WORKERS", "{}"))


@pytest.mark.parametrize("sdk", sorted(WORKERS))
def test_example_constraints_are_explicit_stable_untrusted_records(sdk: str) -> None:
    with Client(server_path=WORKERS[sdk], worker_limit=1) as client:
        token = client.catalog_attach(
            name="example", data_version_spec=None, implementation_version=None
        ).attach_opaque_data
        tables = client.schema_contents(attach_opaque_data=token, path=["data"], type=SchemaObjectType.TABLE)
        by_name = {table.name: table for table in tables}
        for name in ("departments", "products", "employees", "projects"):
            listed = by_name[name]
            fetched = client.table_get(attach_opaque_data=token, schema_path=["data"], name=name)
            assert fetched is not None
            assert fetched.constraints == listed.constraints
            infos = fetched.constraint_infos()
            assert infos, (sdk, name)
            assert all(item.enforcement == ConstraintEnforcement.METADATA_ONLY for item in infos)
            assert all(item.validation == ConstraintValidation.UNVALIDATED for item in infos)
            assert not any(item.trusted for item in infos)
            validate_constraint_schema(
                pa.ipc.read_schema(pa.py_buffer(fetched.columns)),
                [item.definition for item in infos],
                trusted=[],
            )
