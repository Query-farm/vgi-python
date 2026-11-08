# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Public write-result helpers: row ids, write_options, and result batches."""

from __future__ import annotations

from types import SimpleNamespace

import pyarrow as pa
import pytest
from vgi_rpc.utils import serialize_record_batch_bytes

import vgi
import vgi.catalog
from vgi.arguments import Arguments
from vgi.write_results import (
    ROW_ID_METADATA_KEY,
    WriteOptions,
    is_row_id_field,
    row_id_column,
    row_id_field,
    user_columns,
    write_changes_batch,
    write_count_batch,
    write_options,
    write_result_batch,
    write_result_schema,
    write_rows_batch,
)

TABLE = pa.schema(
    [pa.field("id", pa.int32(), nullable=False), pa.field("name", pa.string()), row_id_field()],
    metadata={b"k": b"v"},
)
USER = pa.schema([pa.field("id", pa.int32(), nullable=False), pa.field("name", pa.string())])


def test_exported_from_both_packages() -> None:
    """Exported from both packages."""
    for module in (vgi, vgi.catalog):
        for name in (
            "ROW_ID_METADATA_KEY",
            "WriteOptions",
            "row_id_field",
            "user_columns",
            "write_options",
            "write_count_batch",
            "write_rows_batch",
            "write_result_batch",
        ):
            assert name in module.__all__, (module.__name__, name)
            assert getattr(module, name) is getattr(vgi.write_results, name)


def test_row_id_field_defaults_and_overrides() -> None:
    """Row id field defaults and overrides."""
    f = row_id_field()
    assert (f.name, f.type, f.nullable) == ("rowid", pa.int64(), False)
    assert f.metadata == {ROW_ID_METADATA_KEY: b""}
    assert ROW_ID_METADATA_KEY == b"is_row_id"
    custom = row_id_field("rid", pa.int32())
    assert (custom.name, custom.type) == ("rid", pa.int32())
    assert is_row_id_field(custom)
    assert not is_row_id_field(pa.field("rowid", pa.int64()))


def test_user_columns_strips_only_the_row_id() -> None:
    """User columns strips only the row id."""
    stripped = user_columns(TABLE)
    assert stripped.names == ["id", "name"]
    assert stripped.metadata == {b"k": b"v"}
    assert row_id_column(TABLE) == row_id_field()
    assert row_id_column(USER) is None
    assert user_columns(USER) is USER


def test_write_result_schema_excludes_row_id() -> None:
    """Write result schema excludes row id."""
    assert write_result_schema("count", TABLE) == pa.schema([pa.field("count", pa.int64(), nullable=False)])
    assert write_result_schema("rows", TABLE).names == ["id", "name"]
    changes = write_result_schema("changes", TABLE)
    assert changes.names == ["old", "new"]
    assert [f.name for f in changes.field("old").type] == ["id", "name"]
    # A schema without a row id is unchanged.
    assert write_result_schema("rows", USER).equals(USER)


def test_count_and_rows_batches() -> None:
    """Count and rows batches."""
    count = write_count_batch(3)
    assert count.schema == write_result_schema("count", TABLE)
    assert count.to_pylist() == [{"count": 3}]
    rows = write_rows_batch(TABLE, [{"id": 1, "name": "a", "rowid": 1}, {"id": 2}])
    assert rows.schema.names == ["id", "name"]
    assert rows.to_pylist() == [{"id": 1, "name": "a"}, {"id": 2, "name": None}]


def test_changes_batch_ignores_extra_keys() -> None:
    """Changes batch ignores extra keys."""
    batch = write_changes_batch(TABLE, [None, {"id": 1, "name": "a", "rowid": 9}], [{"id": 2, "name": "b"}, None])
    assert batch.to_pylist() == [
        {"old": None, "new": {"id": 2, "name": "b"}},
        {"old": {"id": 1, "name": "a"}, "new": None},
    ]
    with pytest.raises(ValueError, match="equal length"):
        write_changes_batch(TABLE, [None], [])


@pytest.mark.parametrize("mode", ["count", "rows", "changes"])
def test_write_result_batch_matches_schema(mode: str) -> None:
    """Write result batch matches schema."""
    old = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    new = [{"id": 1, "name": "A"}, None]  # an update and a delete
    batch = write_result_batch(mode, TABLE, old, new)  # type: ignore[arg-type]
    assert batch.schema == write_result_schema(mode, TABLE)  # type: ignore[arg-type]
    if mode == "count":
        assert batch.to_pylist() == [{"count": 2}]
    elif mode == "rows":
        # RETURNING shows the row as written: the old image for a delete.
        assert batch.to_pylist() == [{"id": 1, "name": "A"}, {"id": 2, "name": "b"}]
    else:
        assert batch.to_pylist()[1] == {"old": {"id": 2, "name": "b"}, "new": None}


def test_write_result_batch_rejects_missing_images() -> None:
    """Write result batch rejects missing images."""
    with pytest.raises(ValueError, match="old or a new image"):
        write_result_batch("rows", TABLE, [None], [None])
    with pytest.raises(ValueError, match="equal length"):
        write_result_batch("count", TABLE, [None], [])


def _options_bytes(**columns: object) -> bytes:
    return serialize_record_batch_bytes(pa.RecordBatch.from_pylist([columns]))


def _bind_call(named: dict[str, pa.Scalar] | None) -> SimpleNamespace:
    return SimpleNamespace(arguments=Arguments(named=named or {}))


def test_write_options_defaults_without_argument() -> None:
    """Write options defaults without argument."""
    assert write_options(_bind_call(None)) == WriteOptions()
    assert write_options(_bind_call(None)).result_mode == "count"


def test_write_options_decodes_every_field() -> None:
    """Write options decodes every field."""
    raw = _options_bytes(result_mode="rows", on_conflict="update", on_conflict_columns=["id"])
    call = _bind_call({"write_options": pa.scalar(raw)})
    expected = WriteOptions(result_mode="rows", on_conflict="update", on_conflict_columns=["id"])
    # A BindRequest-like object, BindParams-like (bind_call) and ProcessParams-like (init_call.bind_call).
    assert write_options(call) == expected
    assert write_options(SimpleNamespace(bind_call=call)) == expected
    assert write_options(SimpleNamespace(bind_call=None, init_call=SimpleNamespace(bind_call=call))) == expected


def test_write_options_partial_and_invalid() -> None:
    """Write options partial and invalid."""
    assert write_options(_bind_call({"write_options": pa.scalar(_options_bytes(result_mode="changes"))})) == (
        WriteOptions(result_mode="changes")
    )
    with pytest.raises(ValueError, match="Unknown write result mode"):
        write_options(_bind_call({"write_options": pa.scalar(_options_bytes(result_mode="everything"))}))
    with pytest.raises(TypeError, match="write_options"):
        write_options(object())
