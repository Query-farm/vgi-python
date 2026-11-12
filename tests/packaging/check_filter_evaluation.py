"""Decode and evaluate pushed-down filters with an installed vgi-python wheel.

The ``packaging`` CI job runs this against the built wheel installed with only
a user-facing engine extra (``haybarn`` or ``duckdb``). The dev extra and dev
group pull numpy in, which hid the 0.51.0 bug: every filter evaluation called
the engine's ``create_function``, which needs numpy, and neither engine extra
declared it, so any pushed-down filter failed to bind.

    python -I tests/packaging/check_filter_evaluation.py [--plain-only]

``--plain-only`` skips the intersects_extent case; CI uses it after removing
numpy, to prove an ordinary filter does not need numpy at all. Run it with
``-I`` from outside the checkout so it imports the installed package.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Mapping
from typing import Any

import pyarrow as pa

import vgi
from vgi import deserialize_filters

_METADATA = {
    b"vgi_filter_encoding": b"vgi.filters.v2",
    b"vgi_filter_version": b"2",
    b"vgi_evaluation_context": b"vgi.none.v1",
}
_WKB = {b"ARROW:extension:name": b"geoarrow.wkb", b"ARROW:extension:metadata": b"{}"}


def _column(name: str, index: int) -> dict[str, object]:
    return {"node": "column_ref", "column_index": index, "column_name": name}


def _snapshot(expression: Mapping[str, object], mode: str, *payload: tuple[pa.Field[Any], object]) -> pa.RecordBatch:
    document = {
        "encoding": "vgi.filters.v2",
        "semantics": "vgi.duckdb.standard.v1",
        "kind": "snapshot",
        "predicates": [
            {"id": "query:0", "revision": 0, "mode": mode, "source": "query", "expression": dict(expression)}
        ],
    }
    fields: list[pa.Field[Any]] = [
        pa.field("filter_spec", pa.string(), nullable=False),
        *(field for field, _ in payload),
    ]
    arrays = [pa.array([json.dumps(document)]), *(pa.array([value], type=field.type) for field, value in payload)]
    return pa.RecordBatch.from_arrays(arrays, schema=pa.schema(fields, metadata=_METADATA))  # type: ignore[arg-type]


def check_plain() -> None:
    """``WHERE region = 'west' AND amount > 900``, as the pushdown_sqlite starter receives it."""
    columns: list[pa.Field[Any]] = [pa.field("region", pa.string()), pa.field("amount", pa.int64())]
    schema = pa.schema(columns)
    expression: dict[str, object] = {
        "node": "and",
        "children": [
            {
                "node": "comparison",
                "op": "eq",
                "left": _column("region", 0),
                "right": {"node": "literal", "value_ref": 0},
            },
            {
                "node": "comparison",
                "op": "gt",
                "left": _column("amount", 1),
                "right": {"node": "literal", "value_ref": 1},
            },
        ],
    }
    filters = deserialize_filters(
        _snapshot(
            expression,
            "required",
            (pa.field("value_0", pa.string()), "west"),
            (pa.field("value_1", pa.int64()), 900),
        ),
        output_schema=schema,
    )
    data = pa.RecordBatch.from_pydict(
        {"region": ["west", "west", "east"], "amount": [950, 100, 1000]},
        schema=schema,
    )
    got = filters.evaluate(data).to_pylist()
    assert got == [True, False, False], got
    print("plain filter ok:", got)


def check_intersects_extent() -> None:
    """``geom && POINT(1 1)``: the duckdb.spatial/intersects_extent@1 UDF, which needs numpy."""
    point = bytes.fromhex("0101000000000000000000f03f000000000000f03f")  # POINT(1 1)
    far = bytes.fromhex("010100000000000000000024400000000000002440")  # POINT(10 10)
    schema = pa.schema([pa.field("geom", pa.binary(), metadata=_WKB)])
    expression: dict[str, object] = {
        "node": "call",
        "function": {"namespace": "duckdb.spatial", "name": "intersects_extent", "version": 1},
        "arguments": [_column("geom", 0), {"node": "literal", "value_ref": 0}],
    }
    filters = deserialize_filters(
        _snapshot(expression, "advisory", (pa.field("value_0", pa.binary(), metadata=_WKB), point)),
        output_schema=schema,
        extension_functions=frozenset({("duckdb.spatial", "intersects_extent", 1)}),
    )
    data = pa.RecordBatch.from_arrays([pa.array([point, far, None], pa.binary())], schema=schema)
    got = filters.evaluate(data).to_pylist()
    assert got == [True, False, None], got
    print("intersects_extent filter ok:", got)


def main() -> None:
    """Run the checks against the installed wheel."""
    if "site-packages" not in (vgi.__file__ or ""):
        raise SystemExit(f"vgi imported from {vgi.__file__}, not an installed wheel; run with -I outside the checkout")
    check_plain()
    if "--plain-only" not in sys.argv:
        check_intersects_extent()


if __name__ == "__main__":
    main()
