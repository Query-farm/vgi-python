# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Scan functions backing the ``rff_*`` Tables used by the
``vgi_required_filters_*.test`` sqllogictest matrix.

These fixtures exercise the new ``Table.required_field_filter_paths`` field +
the C++ optimizer extension that enforces it. The five tables form a small
matrix:

* ``rff_simple``  — flat columns, single top-level required path.
* ``rff_struct``  — struct column with two required subfield paths.
* ``rff_nested``  — nested struct with a 3-deep required path.
* ``rff_multi``   — mixed top-level + struct subfield requirements.
* ``rff_none``    — no requirement (control / regression for the fast path).
"""

from __future__ import annotations

import pyarrow as pa

from vgi._test_fixtures.table.catalog_scans import _static_scan_function

# The fixture schemas. These are referenced both by the scan functions below and
# by the Table descriptors registered on the worker.

RFF_SIMPLE_COLUMNS = pa.schema(
    [
        pa.field("a", pa.int64()),
        pa.field("b", pa.int64()),
    ]
)

RFF_STRUCT_COLUMNS = pa.schema(
    [
        pa.field(
            "s",
            pa.struct(
                [
                    pa.field("a", pa.int64()),
                    pa.field("b", pa.int64()),
                ]
            ),
        ),
        pa.field("other", pa.int64()),
    ]
)

RFF_NESTED_COLUMNS = pa.schema(
    [
        pa.field(
            "wrapper",
            pa.struct(
                [
                    pa.field(
                        "mid",
                        pa.struct(
                            [
                                pa.field("leaf", pa.int64()),
                            ]
                        ),
                    ),
                ]
            ),
        ),
    ]
)

RFF_MULTI_COLUMNS = pa.schema(
    [
        pa.field(
            "s",
            pa.struct(
                [
                    pa.field("a", pa.int64()),
                    pa.field("b", pa.int64()),
                ]
            ),
        ),
        pa.field("top", pa.int64()),
    ]
)

RFF_NONE_COLUMNS = pa.schema(
    [
        pa.field("a", pa.int64()),
        pa.field("b", pa.int64()),
    ]
)


RffSimpleScanFunction = _static_scan_function(
    func_name="rff_simple_scan",
    func_description="rff_simple — flat columns (a, b) for required_field_filter_paths tests",
    output_schema=RFF_SIMPLE_COLUMNS,
    data={
        "a": [1, 2, 3],
        "b": [10, 20, 30],
    },
)

RffStructScanFunction = _static_scan_function(
    func_name="rff_struct_scan",
    func_description="rff_struct — STRUCT(s.a, s.b) + other for required_field_filter_paths tests",
    output_schema=RFF_STRUCT_COLUMNS,
    data={
        "s": [
            {"a": 1, "b": 10},
            {"a": 2, "b": 20},
            {"a": 3, "b": 30},
        ],
        "other": [100, 200, 300],
    },
)

RffNestedScanFunction = _static_scan_function(
    func_name="rff_nested_scan",
    func_description="rff_nested — nested STRUCT(wrapper.mid.leaf) for required_field_filter_paths tests",
    output_schema=RFF_NESTED_COLUMNS,
    data={
        "wrapper": [
            {"mid": {"leaf": 1}},
            {"mid": {"leaf": 2}},
            {"mid": {"leaf": 3}},
        ],
    },
)

RffMultiScanFunction = _static_scan_function(
    func_name="rff_multi_scan",
    func_description="rff_multi — top-level + struct subfield required paths",
    output_schema=RFF_MULTI_COLUMNS,
    data={
        "s": [
            {"a": 1, "b": 10},
            {"a": 2, "b": 20},
        ],
        "top": [100, 200],
    },
)

RffNoneScanFunction = _static_scan_function(
    func_name="rff_none_scan",
    func_description="rff_none — control table with no required_field_filter_paths",
    output_schema=RFF_NONE_COLUMNS,
    data={
        "a": [1, 2, 3],
        "b": [10, 20, 30],
    },
)
