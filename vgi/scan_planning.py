"""Protocol 3.2 filtered scan planning helpers.

A table with ``TableInfo.scan_planning = "filtered"`` receives the query's
predicates in ``catalog_table_scan_branches_get``. The worker hook gets them as a
[`ScanPlanningFilters`][] and may use them to return fewer branches or fewer
files. Every predicate is advisory: the client re-applies all of them, so a
worker may ignore any predicate it does not understand, but it must never drop
a branch or file that could hold a matching row.

See ``docs/protocol/scan-planning.md``.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import pyarrow as pa
from vgi_rpc.utils import deserialize_record_batch

if TYPE_CHECKING:
    from vgi.catalog.catalog_interface import ColumnStatistics
    from vgi.table_filter_pushdown import PushdownFilters

__all__ = ["ScanPlanningFilters"]


@dataclass(frozen=True, eq=False)
class ScanPlanningFilters:
    """The query predicates sent with a filtered ``catalog_table_scan_branches_get``.

    Attributes:
        raw: The wire bytes: an Arrow IPC stream holding one VGI Filter Encoding
            v2 snapshot batch. Column indexes refer to the table's declared
            columns.
        table_columns: Resolves the table's declared columns (the schema the
            column indexes refer to). Called at most once, on first decode.

    """

    raw: bytes
    table_columns: Callable[[], pa.Schema | None] = field(default=lambda: None)
    _cache: dict[str, Any] = field(default_factory=dict, repr=False)

    def decode(self) -> PushdownFilters:
        """Decode the predicates against the table's declared columns (cached)."""
        if "decoded" not in self._cache:
            from vgi.table_filter_pushdown import deserialize_filters

            batch, _ = deserialize_record_batch(self.raw)
            self._cache["decoded"] = deserialize_filters(batch, output_schema=self.table_columns())
        return self._cache["decoded"]  # type: ignore[no-any-return]

    def may_match(self, statistics: Iterable[ColumnStatistics]) -> bool:
        """Return False only when the predicates provably exclude every row described by ``statistics``.

        ``statistics`` describes a candidate (a file, a partition, a branch) the
        same way ``ScanBranch.column_statistics`` does: every non-null value of a
        column lies within ``[min, max]``. Use it to prune a manifest before
        returning branches. Columns or predicates this helper cannot reason
        about count as "may match", so the answer errs towards keeping data.
        """
        filters = self.decode()
        return all(_column_may_match(filters, stat) for stat in statistics)


def _as_py(value: pa.Scalar[Any] | None) -> Any:
    if value is None or not value.is_valid:
        return None
    return value.as_py()


def _column_may_match(filters: PushdownFilters, stat: ColumnStatistics) -> bool:
    lo, hi = _as_py(stat.min), _as_py(stat.max)
    if not stat.has_not_null:
        # Entirely NULL: comparison, equality and IN predicates never match.
        return not _has_value_predicate(filters, stat.column_name)
    if lo is None and hi is None:
        return True
    try:
        constant = filters.get_column_constant(stat.column_name)
        if constant is not None:
            c = _as_py(constant)
            if c is not None and ((lo is not None and c < lo) or (hi is not None and c > hi)):
                return False
        in_values = filters.get_column_in_values(stat.column_name)
        if in_values is not None:
            values = [v for v in in_values.to_pylist() if v is not None]
            if values and all((lo is not None and v < lo) or (hi is not None and v > hi) for v in values):
                return False
        bounds = filters.get_column_bounds(stat.column_name)
        if bounds is not None:
            bmin, bmax = _as_py(bounds.min_value), _as_py(bounds.max_value)
            if bmin is not None and hi is not None and (hi < bmin or (hi == bmin and not bounds.min_inclusive)):
                return False
            if bmax is not None and lo is not None and (lo > bmax or (lo == bmax and not bounds.max_inclusive)):
                return False
    except TypeError:
        # Incomparable types (e.g. a predicate literal of another type): keep the candidate.
        return True
    return True


def _has_value_predicate(filters: PushdownFilters, column_name: str) -> bool:
    try:
        return (
            filters.get_column_constant(column_name) is not None
            or filters.get_column_in_values(column_name) is not None
            or filters.get_column_bounds(column_name) is not None
        )
    except TypeError:
        return False
