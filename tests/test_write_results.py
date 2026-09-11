# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Tests for the VGI 2.0 writable result-mode contract."""

import pyarrow as pa
import pytest

from vgi.schema_utils import schema
from vgi.write_results import supports_write_result_mode, write_changes_batch, write_result_schema


def test_ordered_result_modes() -> None:
    """A maximum mode promises itself and every lower mode."""
    assert supports_write_result_mode("count", "count")
    assert not supports_write_result_mode("count", "rows")
    assert supports_write_result_mode("rows", "count")
    assert supports_write_result_mode("changes", "rows")
    assert supports_write_result_mode("changes", "changes")


def test_result_schemas() -> None:
    """Every mode has the exact canonical Arrow schema."""
    table = schema(id=pa.int64(), name=pa.string())
    assert write_result_schema("count", table) == pa.schema([pa.field("count", pa.int64(), nullable=False)])
    assert write_result_schema("rows", table) == table
    changes = write_result_schema("changes", table)
    assert changes.names == ["old", "new"]
    assert changes.field("old").nullable
    assert changes.field("old").type == pa.struct(list(table))


@pytest.mark.parametrize(
    ("old", "new"),
    [
        (None, {"id": 1, "name": "inserted"}),
        ({"id": 1, "name": "old"}, {"id": 1, "name": "new"}),
        ({"id": 1, "name": "deleted"}, None),
    ],
)
def test_change_images(old: dict[str, object] | None, new: dict[str, object] | None) -> None:
    """INSERT, UPDATE, and DELETE preserve the required OLD/NEW nullability."""
    table = schema(id=pa.int64(), name=pa.string())
    batch = write_changes_batch(table, [old], [new])
    assert batch.to_pylist() == [{"old": old, "new": new}]


def test_change_image_lengths_must_match() -> None:
    """Every output row must contain one OLD/NEW pair."""
    with pytest.raises(ValueError, match="equal length"):
        write_changes_batch(pa.schema([("id", pa.int64())]), [], [{"id": 1}])
