"""duckdb.spatial/intersects_extent@1 conformance (filter encoding v2 spec 5.11.1).

The reference evaluator must return DuckDB core's `&&` result for every case in
docs/protocol/conformance/intersects_extent_v1.json, for little- and big-endian
WKB on either side. SDKs in other languages run the same cases.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from vgi.filter_v2 import (
    NO_EVALUATION_CONTEXT,
    Call,
    ColumnRef,
    EvaluationContext,
    FunctionIdentity,
    Literal,
    evaluate_expression,
)

CASES = Path(__file__).resolve().parent.parent / "docs" / "protocol" / "conformance" / "intersects_extent_v1.json"
GEOARROW_WKB = {b"ARROW:extension:name": b"geoarrow.wkb", b"ARROW:extension:metadata": b"{}"}
INTERSECTS_EXTENT = FunctionIdentity("duckdb.spatial", "intersects_extent", 1)


def _load() -> dict[str, Any]:
    return json.loads(CASES.read_text())  # type: ignore[no-any-return]


DOC = _load()
GEOMETRIES = {g["name"]: g for g in DOC["geometries"]}
EXPECTED = {(c["left"], c["right"]): c["expected"] for c in DOC["cases"]}


def test_cases_are_complete() -> None:
    """Every ordered pair of geometries has an expected result."""
    names = list(GEOMETRIES)
    assert len(EXPECTED) == len(names) ** 2
    assert all((a, b) in EXPECTED for a in names for b in names)


@pytest.mark.parametrize("right", sorted(GEOMETRIES))
@pytest.mark.parametrize("right_encoding", ["wkb_le", "wkb_be"])
def test_reference_evaluator_matches_duckdb(right: str, right_encoding: str) -> None:
    """`geom && <constant>` over every left geometry, both encodings, equals DuckDB's result."""
    rows = [(name, enc) for name in GEOMETRIES for enc in ("wkb_le", "wkb_be")]
    field = pa.field("geom", pa.binary(), metadata=GEOARROW_WKB)
    batch = pa.RecordBatch.from_arrays(
        [pa.array([bytes.fromhex(GEOMETRIES[name][enc]) for name, enc in rows], type=pa.binary())],
        schema=pa.schema([field]),
    )
    literal_field = pa.field("value_0", pa.binary(), metadata=GEOARROW_WKB)
    expression = Call(
        INTERSECTS_EXTENT,
        (
            ColumnRef(0, "geom", pa.binary()),
            Literal(0, literal_field, pa.scalar(bytes.fromhex(GEOMETRIES[right][right_encoding]), type=pa.binary())),
        ),
    )
    actual = evaluate_expression(expression, batch, EvaluationContext(profile=NO_EVALUATION_CONTEXT)).to_pylist()
    mismatches = [
        f"{name}({enc}) && {right}({right_encoding}): got {got}, want {EXPECTED[(name, right)]}"
        for (name, enc), got in zip(rows, actual, strict=True)
        if got != EXPECTED[(name, right)]
    ]
    assert not mismatches, "\n".join(mismatches)


def _ewkb(iso_le: bytes, srid: int = 4326) -> bytes:
    """Little-endian ISO WKB of a non-collection geometry rewritten as EWKB with an SRID."""
    import struct

    (code,) = struct.unpack_from("<I", iso_le, 1)
    base, dims = code % 1000, code // 1000
    flags = 0x20000000 | (0x80000000 if dims in (1, 3) else 0) | (0x40000000 if dims in (2, 3) else 0)
    return iso_le[:1] + struct.pack("<II", base | flags, srid) + iso_le[5:]


@pytest.mark.parametrize("name", ["pt_half", "pt_z", "pt_zm", "ls_diag", "poly_unit", "poly_hole", "ls_nan_vertex"])
def test_ewkb_matches_iso(name: str) -> None:
    """EWKB (Z/M/SRID flags), which DuckDB 2.0 accepts, gives the same box as ISO WKB."""
    from vgi._wkb_extent import intersects_extent, wkb_extent

    iso = bytes.fromhex(GEOMETRIES[name]["wkb_le"])
    ewkb = _ewkb(iso)
    assert wkb_extent(ewkb) == wkb_extent(iso)
    for other in GEOMETRIES.values():
        assert intersects_extent(ewkb, bytes.fromhex(other["wkb_le"])) == EXPECTED[(name, other["name"])]


@pytest.mark.parametrize(
    "wkb",
    [
        b"",
        bytes.fromhex("01"),
        bytes.fromhex("0201000000"),  # byte order 2
        bytes.fromhex("0108000000"),  # type 8
        bytes.fromhex("01e9030000000000000000f03f"),  # truncated POINT Z
        bytes.fromhex("0101000000000000000000f03f000000000000f03f00"),  # trailing byte
        bytes.fromhex("01e9030080000000000000f03f000000000000f03f0000000000000000"),  # ISO Z plus EWKB Z
        bytes.fromhex("0101000020e610"),  # SRID flag, truncated SRID
        bytes.fromhex("0102000000ffffffff"),  # absurd vertex count
    ],
)
def test_malformed_wkb_raises(wkb: bytes) -> None:
    """Malformed WKB is an error, which a required predicate surfaces and an advisory one ignores."""
    from vgi._wkb_extent import wkb_extent

    with pytest.raises(ValueError):
        wkb_extent(wkb)
