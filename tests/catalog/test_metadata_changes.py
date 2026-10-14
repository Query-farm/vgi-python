# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103
"""Patch intent, validation and wire shape must survive before any mutation."""

import base64
import json
from typing import Any

import pyarrow as pa
import pytest

from vgi.catalog.metadata_changes import CatalogMetadataChanges
from vgi.codegen.catalog_contract_fixtures import render


def test_patch_retains_omission_null_empty_and_literal_tags() -> None:
    tags = {"a.b": "old", "old": "remove", "OLD": "keep", "": "blank-key"}
    patch = CatalogMetadataChanges(
        set_comment=True, comment="", set_tags={"a.b": "", "__proto__": "literal"}, remove_tags=["old"]
    )
    restored = CatalogMetadataChanges.deserialize_from_bytes(patch.serialize_to_bytes())
    comment, result = restored.apply_to("old", tags)
    assert comment == "" and result == {"a.b": "", "OLD": "keep", "": "blank-key", "__proto__": "literal"}
    assert tags["old"] == "remove" and tags["a.b"] == "old"
    assert CatalogMetadataChanges().apply_to(comment, result) == (comment, result)
    assert CatalogMetadataChanges(set_comment=True).apply_to(comment, result) == (None, result)
    assert CatalogMetadataChanges(remove_tags=["missing"]).apply_to(comment, result) == (comment, result)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"comment": "lost"},
        {"set_comment": None},
        {"set_comment": 1},
        {"comment": 7},
        {"set_tags": None},
        {"set_tags": {"a": None}},
        {"set_tags": {1: "a"}},
        {"remove_tags": None},
        {"remove_tags": [None]},
        {"remove_tags": ["a", "a"]},
        {"set_tags": {"a": "x"}, "remove_tags": ["a"]},
    ],
)
def test_invalid_patches_reject(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        CatalogMetadataChanges(**kwargs)


def test_mutable_patch_members_are_validated_again_at_application() -> None:
    patch = CatalogMetadataChanges(set_tags={"a": "x"})
    patch.remove_tags.append("a")
    original = {"a": "original"}
    with pytest.raises(ValueError, match="SET and REMOVE"):
        patch.apply_to("old", original)
    assert original == {"a": "original"}


@pytest.mark.parametrize(
    "kind", ["missing", "wrong_type", "null_flag", "null_map", "null_removals", "duplicate_keys", "two_rows"]
)
def test_wire_rejects_incomplete_or_ambiguous_patches(kind: str) -> None:
    schema = CatalogMetadataChanges.ARROW_SCHEMA
    row: dict[str, Any] = {"set_comment": True, "comment": "new", "set_tags": [], "remove_tags": []}
    if kind == "missing":
        schema = schema.remove(schema.get_field_index("set_comment"))
    elif kind == "wrong_type":
        schema = schema.set(0, pa.field("set_comment", pa.int32(), nullable=False))
        row["set_comment"] = 1
    elif kind.startswith("null_"):
        row[{"null_flag": "set_comment", "null_map": "set_tags", "null_removals": "remove_tags"}[kind]] = None
    elif kind == "duplicate_keys":
        row["set_tags"] = [("a", "one"), ("a", "two")]
    batch = pa.RecordBatch.from_pylist([row] * (2 if kind == "two_rows" else 1), schema=schema)
    with pytest.raises(ValueError):
        CatalogMetadataChanges.deserialize_from_batch(batch)


def test_shared_corpus_preserves_patch_intent() -> None:
    fixtures = json.loads(render())["records"]
    records = {
        name: CatalogMetadataChanges.deserialize_from_bytes(base64.b64decode(fixtures[name]))
        for name in ("metadata_patch", "metadata_clear_comment", "metadata_noop")
    }
    assert records["metadata_patch"].comment == ""
    assert records["metadata_patch"].set_tags == {"a.b": "", "__proto__": "literal", "": "empty-key"}
    assert records["metadata_patch"].remove_tags == ["old", "OLD"]
    assert records["metadata_clear_comment"].set_comment and records["metadata_clear_comment"].comment is None
    assert not records["metadata_noop"].set_comment


@pytest.mark.parametrize("kind", ["no_batch", "extra_batch", "trailing_bytes"])
def test_patch_ipc_rejects_additional_or_missing_contents(kind: str) -> None:
    schema = CatalogMetadataChanges.ARROW_SCHEMA
    sink = pa.BufferOutputStream()
    batch = pa.RecordBatch.from_pylist(
        [{"set_comment": False, "comment": None, "set_tags": [], "remove_tags": []}], schema=schema
    )
    with pa.ipc.new_stream(sink, schema) as writer:
        if kind != "no_batch":
            writer.write_batch(batch)
        if kind == "extra_batch":
            writer.write_batch(batch)
    payload = sink.getvalue().to_pybytes()
    if kind == "trailing_bytes":
        payload += b"extra"
    with pytest.raises(ValueError):
        CatalogMetadataChanges.deserialize_from_bytes(payload)
