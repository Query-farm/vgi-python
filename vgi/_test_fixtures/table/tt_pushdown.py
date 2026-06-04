# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Time-travel + filter-pushdown fixtures.

These back ``test/sql/integration/table/time_travel_pushdown.test`` in the C++
repo, which asserts that a table can be partition-pruned (filter pushdown) AND
time-travelled (``AT (VERSION|TIMESTAMP ...)``) in the *same* query — for tables
declared **both** ways:

- ``tt_pushdown_fn`` — **function-backed** (``Table(function=...)``). It reads the
  AT clause from the init request (``params.at_value`` →
  ``init_call.bind_call.at_value``), which only works once the framework threads
  AT onto the bind request. Before that fix this table cannot see AT at all, so
  ``seen_version`` collapses to the current version — the regression guard.
- ``tt_pushdown_cols`` — **columns-based** (``Table(columns=...)`` routed via
  ``table_scan_function_get``). It gets the resolved version as a scan-function
  **argument** (the ``versioned_data`` mechanism) — the native columns-based AT
  path, here to prove the bind-side change didn't regress it.

Both echo ``seen_version`` (the version they actually scanned) and
``pushed_filters`` (the SQL-like predicate DuckDB pushed down), so one query can
assert both signals at once. ``auto_apply_filters`` keeps the result set correct.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, ClassVar

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi._test_fixtures.table._common import _EmptyArgs
from vgi._test_fixtures.table.filters import _format_pushed_filters
from vgi.arguments import Arg
from vgi.schema_utils import schema
from vgi.table_function import (
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)

# Output schema is version-INDEPENDENT (no schema evolution in scope): only the
# row *data* changes per version, so the function-backed table stays inline-bound.
_TT_SCHEMA: pa.Schema = schema(
    id=pa.int64(),
    val=pa.int64(),
    seen_version=pa.int64(),
    pushed_filters=pa.string(),
)

# Per-version row ids (val = id * 10). v2 is a strict superset of v1, so a row
# count difference cleanly proves which version was scanned.
_TT_VERSION_IDS: dict[int, list[int]] = {
    1: [1, 2, 3, 4, 5],
    2: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
}
_TT_CURRENT_VERSION = 2  # default when there is no AT clause


def resolve_tt_version(at_unit: str | None, at_value: str | None) -> int:
    """Resolve an AT clause to one of this fixture's versions (1 or 2).

    - ``None``         → current version (2)
    - ``VERSION => n`` → ``int(n)`` (must be 1 or 2)
    - ``TIMESTAMP``    → year <= 2020 → 1, else 2
    """
    if not at_unit:
        return _TT_CURRENT_VERSION
    unit = at_unit.upper()
    if unit == "VERSION":
        version = int(at_value)  # type: ignore[arg-type]
        if version not in _TT_VERSION_IDS:
            raise ValueError(f"Unknown version {version}; valid: {sorted(_TT_VERSION_IDS)}")
        return version
    if unit == "TIMESTAMP":
        year = int(str(at_value)[:4])
        return 1 if year <= 2020 else 2
    raise ValueError(f"Unsupported at_unit: {at_unit!r}")


@dataclass(kw_only=True)
class _TtState(ArrowSerializableDataclass):
    """State for the time-travel + pushdown fixtures.

    ``seen_version`` / ``pushed_filters`` are serialized (NOT transient): the HTTP
    state-token rehydrate path deserializes state without re-running
    ``initial_state``, so they must survive that round-trip to echo correctly.
    """

    seen_version: int = 0
    pushed_filters: str = "(none)"
    done: bool = False


def _emit_version(
    params: ProcessParams[object], state: _TtState, out: OutputCollector
) -> None:
    """Emit one batch for ``state.seen_version``, projected to the output schema."""
    if state.done:
        out.finish()
        return
    state.done = True
    ids = _TT_VERSION_IDS[state.seen_version]
    full: dict[str, list[object]] = {
        "id": ids,
        "val": [i * 10 for i in ids],
        "seen_version": [state.seen_version] * len(ids),
        "pushed_filters": [state.pushed_filters] * len(ids),
    }
    # projection_pushdown=True: emit only the requested columns.
    columns = {f.name: full[f.name] for f in params.output_schema}
    out.emit(pa.RecordBatch.from_pydict(columns, schema=params.output_schema))


def _pushed_filters_str(params: ProcessParams[object]) -> str:
    assert params.init_call is not None
    pf = params.init_call.pushdown_filters
    jk = params.init_call.join_keys
    filters = (
        TableFunctionGenerator.pushdown_filters(pf, join_keys=jk) if pf is not None else None
    )
    return _format_pushed_filters(filters)


@init_single_worker
@bind_fixed_schema
class TimeTravelPushdownFunction(TableFunctionGenerator[_EmptyArgs, _TtState]):
    """Function-backed time-travel + pushdown scan.

    Reads the AT clause from the **init** request (``params.at_value``) — proving
    the framework now threads AT onto the bind request embedded in init. No
    arguments: the version comes from AT, not from a scan-function argument.
    """

    class Meta:
        name = "tt_pushdown_scan"
        description = "Function-backed time-travel + filter-pushdown scan (reads AT at init)."
        categories = ["generator", "diagnostic", "testing"]
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    FIXED_SCHEMA: ClassVar[pa.Schema] = _TT_SCHEMA

    @classmethod
    def initial_state(cls, params: ProcessParams[_EmptyArgs]) -> _TtState:
        version = resolve_tt_version(params.at_unit, params.at_value)
        return _TtState(seen_version=version, pushed_filters=_pushed_filters_str(params))

    @classmethod
    def process(cls, params: ProcessParams[_EmptyArgs], state: _TtState, out: OutputCollector) -> None:
        _emit_version(params, state, out)


@dataclass(slots=True, frozen=True)
class _TtColsArgs:
    """Argument for the columns-based scan: the resolved version (injected by the
    worker's ``table_scan_function_get`` from the AT clause)."""

    version: Annotated[int, Arg(0, doc="Resolved data version")]


@init_single_worker
@bind_fixed_schema
class TtPushdownColsScanFunction(TableFunctionGenerator[_TtColsArgs, _TtState]):
    """Columns-based time-travel + pushdown scan.

    Receives the version as a scan-function **argument** (the native columns-based
    AT mechanism: the worker resolves AT → version in ``table_scan_function_get``).
    Backs ``example.data.tt_pushdown_cols``.
    """

    class Meta:
        name = "tt_pushdown_cols_scan"
        description = "Columns-based time-travel + filter-pushdown scan (version via arg)."
        categories = ["generator", "diagnostic", "testing"]
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    FIXED_SCHEMA: ClassVar[pa.Schema] = _TT_SCHEMA

    @classmethod
    def initial_state(cls, params: ProcessParams[_TtColsArgs]) -> _TtState:
        return _TtState(seen_version=params.args.version, pushed_filters=_pushed_filters_str(params))

    @classmethod
    def process(cls, params: ProcessParams[_TtColsArgs], state: _TtState, out: OutputCollector) -> None:
        _emit_version(params, state, out)
