# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Same-name-in-two-schemas *aggregate* fixtures.

The third member of the schema-disambiguation family, after
:mod:`vgi._test_fixtures.scalar.same_name` (scalar) and
:mod:`vgi._test_fixtures.table_in_out_same_name` (table-in-out + buffering).

Aggregates are the widest surface of the three. Every aggregate RPC —
``aggregate_update`` / ``_combine`` / ``_finalize`` / ``_destructor``, the four
window calls, and the three streaming calls — resolves the function through the
worker's single ``_resolve_aggregate`` entry point, which had no schema to work
with: none of those request types carried one, not even ``AggregateBindRequest``.
So an aggregate name declared in two schemas resolved to whichever the by-name
lookup found first.

Both classes register under ``test_same_name_agg``, in the ``main`` and ``data``
schemas of the ``example`` catalog. Each returns a VARCHAR tagged with its own
schema *and* the aggregated value, so a mis-routed call reads as the wrong tag
rather than a plausible answer, and a call that mis-routes only partway (bind to
one implementation, update/finalize to another) still shows up — the tag is
stamped at ``finalize`` while the accumulation happens in ``update``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass, ArrowType

from vgi.aggregate_function import AggregateFunction
from vgi.arguments import Param, Returns
from vgi.metadata import (
    DistinctDependence,
    FunctionExample,
    NullHandling,
    OrderDependence,
)
from vgi.table_function import ProcessParams

# Deliberately identical in both schemas — the collision is the point.
FUNCTION_NAME = "test_same_name_agg"


@dataclass(kw_only=True)
class SameNameAggState(ArrowSerializableDataclass):
    """Running total for one group."""

    total: Annotated[int, ArrowType(pa.int64())] = 0


class _SameNameAgg(AggregateFunction[SameNameAggState]):
    """Shared body; each subclass supplies the schema it is declared in."""

    #: Schema this implementation is registered into — the tag it stamps.
    OWNING_SCHEMA: str = ""

    @classmethod
    def initial_state(cls, params: ProcessParams[None]) -> SameNameAggState:
        """Start each group at zero."""
        return SameNameAggState()

    @classmethod
    def update(
        cls,
        states: dict[int, SameNameAggState],
        group_ids: pa.Int64Array,
        value: Annotated[pa.Int64Array, Param(doc="Integer value to accumulate")],
    ) -> None:
        """Accumulate each row's value into its group."""
        for gid, v in zip(group_ids.to_pylist(), value.to_pylist(), strict=True):
            if v is None or gid is None:
                continue
            states[gid] = SameNameAggState(total=states[gid].total + v)

    @classmethod
    def combine(
        cls,
        source: SameNameAggState,
        target: SameNameAggState,
        params: ProcessParams[None],
    ) -> SameNameAggState:
        """Merge two partial states."""
        return SameNameAggState(total=source.total + target.total)

    @classmethod
    def finalize(
        cls,
        group_ids: pa.Int64Array,
        states: dict[int, SameNameAggState],
        params: ProcessParams[None],
    ) -> Annotated[pa.RecordBatch, Returns(pa.string())]:
        """Tag each group's total with the owning schema."""
        results = [f"{cls.OWNING_SCHEMA}:{states[gid.as_py()].total}" for gid in group_ids]
        return pa.record_batch({"result": pa.array(results, type=pa.string())})


class SameNameMainAgg(_SameNameAgg):
    """``test_same_name_agg`` as declared in the ``main`` schema."""

    OWNING_SCHEMA = "main"

    class Meta:
        """Function metadata."""

        name = FUNCTION_NAME
        description = "Schema-disambiguation probe; the main-schema aggregate"
        null_handling = NullHandling.SPECIAL
        order_dependent = OrderDependence.NOT_ORDER_DEPENDENT
        distinct_dependent = DistinctDependence.NOT_DISTINCT_DEPENDENT
        examples = [
            FunctionExample(
                sql="SELECT example.main.test_same_name_agg(n) FROM range(3) t(n)",
                description="Returns 'main:3'",
            ),
        ]


class SameNameDataAgg(_SameNameAgg):
    """``test_same_name_agg`` as declared in the ``data`` schema."""

    OWNING_SCHEMA = "data"

    class Meta:
        """Function metadata."""

        name = FUNCTION_NAME
        description = "Schema-disambiguation probe; the data-schema aggregate"
        null_handling = NullHandling.SPECIAL
        order_dependent = OrderDependence.NOT_ORDER_DEPENDENT
        distinct_dependent = DistinctDependence.NOT_DISTINCT_DEPENDENT
        examples = [
            FunctionExample(
                sql="SELECT example.data.test_same_name_agg(n) FROM range(3) t(n)",
                description="Returns 'data:3'",
            ),
        ]
