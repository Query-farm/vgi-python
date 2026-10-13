"""Required scan paths preserve literal identifiers through table metadata IPC."""

from __future__ import annotations

import pyarrow as pa
import pytest
from vgi_rpc.utils import deserialize_record_batch

from vgi.catalog import TableInfo
from vgi.catalog.descriptors import Table

COLUMNS = pa.schema(
    [
        ("a.b", pa.int64()),
        ("a", pa.struct([("b", pa.int64()), ('quoted."field', pa.int64())])),
        ("id", pa.int64()),
    ]
)


def test_required_filters_wire_round_trip() -> None:
    """AND/OR grouping and each raw component survive the real TableInfo codec."""
    groups = ((("a.b",), ("a", "b")), (("a", 'quoted."field'),))
    info = Table(name="t", columns=COLUMNS, required_filters=groups).to_table_info(["main"])
    batch, _ = deserialize_record_batch(info.serialize_to_bytes())
    assert batch.schema.field("required_filters").type == pa.list_(pa.list_(pa.list_(pa.string())))
    assert TableInfo.deserialize_from_batch(batch).required_filters == [[["a.b"], ["a", "b"]], [["a", 'quoted."field']]]


def test_empty_requirements() -> None:
    """No requirements is an explicit empty outer list."""
    table = Table(name="t", columns=COLUMNS)
    assert table.required_filters == ()
    assert table.to_table_info(["main"]).required_filters == []


@pytest.mark.parametrize(
    "groups",
    [
        ((),),
        (((),),),
        ((("",),),),
        ((("a\0",),),),
        ((("missing",),),),
        ((("a", "missing"),),),
        ((("id", "child"),),),
        ((("a.b",), ("a.b",)),),
    ],
)
def test_malformed_or_unresolved_paths(groups: tuple) -> None:
    """Reject empty, duplicate, NUL, missing and non-struct paths before publication."""
    with pytest.raises(ValueError):
        Table(name="t", columns=COLUMNS, required_filters=groups)


def test_legacy_strings_are_not_a_decoder_fallback() -> None:
    """Old dotted strings need an explicit migration decision."""
    with pytest.raises(TypeError, match="raw identifier components"):
        Table(name="t", columns=COLUMNS, required_filters=(("a.b",),))  # type: ignore[arg-type]
