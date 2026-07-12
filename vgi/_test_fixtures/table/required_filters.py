# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Scan functions backing the ``rff_*`` required-filter sqllogictest Tables.

Used by the ``vgi_required_filters_*.test`` matrix. These fixtures exercise
the ``Table.required_filters`` field +
the C++ optimizer extension that enforces it. The five tables form a small
matrix:

* ``rff_simple``  — flat columns, single top-level required path.
* ``rff_struct``  — struct column with two required subfield paths.
* ``rff_nested``  — nested struct with a 3-deep required path.
* ``rff_multi``   — mixed top-level + struct subfield requirements.
* ``rff_none``    — no requirement (control / regression for the fast path).
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
from vgi_rpc.rpc import OutputCollector

from vgi._test_fixtures.table._common import _EmptyArgs, _OneShotState
from vgi._test_fixtures.table.catalog_scans import _static_scan_function
from vgi.invocation import BindResponse
from vgi.table_function import (
    BindParams,
    ProcessParams,
    TableFunctionGenerator,
    init_single_worker,
)

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

# rff_rowid — a row-id column (virtual, hidden from SELECT *) alongside a bbox
# struct with required_filters. A `WHERE rowid = N` predicate pushes
# a table_filter keyed by the COLUMN_IDENTIFIER_ROW_ID sentinel (>> column
# count), which the optimizer's required-filter check must skip rather than
# index out of bounds. See required_filters_native.test.
RFF_ROWID_COLUMNS = pa.schema(
    [
        pa.field("row_id", pa.int64(), metadata={b"is_row_id": b""}),
        pa.field(
            "bbox",
            pa.struct(
                [
                    pa.field("xmin", pa.float32()),
                    pa.field("ymin", pa.float32()),
                    pa.field("xmax", pa.float32()),
                    pa.field("ymax", pa.float32()),
                ]
            ),
        ),
        pa.field("other", pa.int64()),
    ]
)


RffSimpleScanFunction = _static_scan_function(
    func_name="rff_simple_scan",
    func_description="rff_simple — flat columns (a, b) for required_filters tests",
    output_schema=RFF_SIMPLE_COLUMNS,
    data={
        "a": [1, 2, 3],
        "b": [10, 20, 30],
    },
)

RffStructScanFunction = _static_scan_function(
    func_name="rff_struct_scan",
    func_description="rff_struct — STRUCT(s.a, s.b) + other for required_filters tests",
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
    func_description="rff_nested — nested STRUCT(wrapper.mid.leaf) for required_filters tests",
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
    func_description="rff_none — control table with no required_filters",
    output_schema=RFF_NONE_COLUMNS,
    data={
        "a": [1, 2, 3],
        "b": [10, 20, 30],
    },
)


# rff_rowid needs projection_pushdown (virtual row-id columns require it), so it
# can't use the one-shot static factory — under projection the emitted batch must
# match the *projected* output schema. Build only the requested columns.
@init_single_worker
class RffRowidScanFunction(TableFunctionGenerator[_EmptyArgs, _OneShotState]):
    """rff_rowid — row_id virtual column + bbox.* required filters."""

    class Meta:
        """Function metadata."""

        name = "rff_rowid_scan"
        description = "rff_rowid — row_id virtual column + bbox.* required filters"
        projection_pushdown = True
        # filter_pushdown routes the WHERE predicates (incl. the rowid filter,
        # keyed by the COLUMN_IDENTIFIER_ROW_ID sentinel) into the scan's
        # table_filters; auto_apply_filters lets the framework apply them so
        # results stay correct without a hand-written filter loop.
        filter_pushdown = True
        auto_apply_filters = True

    @classmethod
    def on_bind(cls, params: BindParams[_EmptyArgs]) -> BindResponse:
        """Return the full output schema (row_id + bbox + other)."""
        return BindResponse(output_schema=RFF_ROWID_COLUMNS)

    @classmethod
    def initial_state(cls, params: ProcessParams[_EmptyArgs]) -> _OneShotState:
        """Create initial state."""
        return _OneShotState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[_EmptyArgs],
        state: _OneShotState,
        out: OutputCollector,
    ) -> None:
        """Emit 10 rows, projecting to whatever columns the scan requested."""
        if state.done:
            out.finish()
            return
        state.done = True
        full: dict[str, Any] = {
            "row_id": list(range(10)),
            "bbox": [{"xmin": float(i), "ymin": 2.0, "xmax": 3.0, "ymax": 4.0} for i in range(10)],
            "other": [i * 10 for i in range(10)],
        }
        columns = {f.name: full[f.name] for f in params.output_schema}
        out.emit(pa.RecordBatch.from_pydict(columns, schema=params.output_schema))
