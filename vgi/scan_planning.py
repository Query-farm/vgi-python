"""Protocol 3.2 filtered scan planning helpers.

A table with ``TableInfo.scan_planning = "filtered"`` receives the query's
predicates in ``catalog_table_scan_branches_get``. The worker hook gets them as a
``ScanPlanningFilters`` and may use them to return fewer branches or fewer
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
        by_name = {stat.column_name: stat for stat in statistics}
        return all(_filter_may_match(f, by_name) for f in self.decode().filters)


def _as_py(value: pa.Scalar[Any] | None) -> Any:
    if value is None or not value.is_valid:
        return None
    return value.as_py()


def _filter_may_match(f: Any, stats: dict[str, ColumnStatistics]) -> bool:
    """Whether filter ``f`` can be true for some row described by ``stats``; unknown shapes return True."""
    from vgi.table_filter_pushdown import (
        AndFilter,
        ComparisonOp,
        ConstantFilter,
        InFilter,
        IsNotNullFilter,
        IsNullFilter,
        OrFilter,
    )

    if isinstance(f, AndFilter):
        return all(_filter_may_match(child, stats) for child in f.children)
    if isinstance(f, OrFilter):
        return any(_filter_may_match(child, stats) for child in f.children)
    stat = stats.get(f.column_name)
    if stat is None:
        return True
    if isinstance(f, IsNullFilter):
        return stat.has_null
    if isinstance(f, IsNotNullFilter):
        return stat.has_not_null
    if not stat.has_not_null:
        # Entirely NULL: comparisons and IN never match.
        return not isinstance(f, ConstantFilter | InFilter)
    lo, hi = _as_py(stat.min), _as_py(stat.max)
    try:
        if isinstance(f, InFilter):
            values = [v for v in f.values.to_pylist() if v is not None]
            return any((lo is None or v >= lo) and (hi is None or v <= hi) for v in values)
        if isinstance(f, ConstantFilter):
            c = _as_py(f.value)
            if c is None:
                return True
            if f.op == ComparisonOp.EQ:
                return (lo is None or c >= lo) and (hi is None or c <= hi)
            if f.op == ComparisonOp.NE:
                return not (lo is not None and hi is not None and lo == hi == c)
            if f.op == ComparisonOp.GT:
                return hi is None or hi > c
            if f.op == ComparisonOp.GE:
                return hi is None or hi >= c
            if f.op == ComparisonOp.LT:
                return lo is None or lo < c
            if f.op == ComparisonOp.LE:
                return lo is None or lo <= c
    except TypeError:
        # Incomparable types (e.g. a predicate literal of another type): keep the candidate.
        return True
    return True
