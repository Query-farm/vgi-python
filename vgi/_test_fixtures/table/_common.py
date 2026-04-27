"""Shared infrastructure for table fixture functions.

Holds the cardinality decorator, the common ``CountdownState`` used by
sequence-style generators, and the ``_BaseSequenceFunction`` base class.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar

import numpy as np
import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi.catalog.catalog_interface import ColumnStatistics
from vgi.table_function import (
    BindParams,
    ProcessParams,
    TableCardinality,
    TableFunctionGenerator,
)


def _cardinality_from_count[T: TableFunctionGenerator[Any, Any]](cls: type[T]) -> type[T]:
    """Class decorator to implement cardinality() based on a 'count' argument."""
    if "cardinality" not in cls.__dict__:  # only inject if subclass hasn't overridden

        def cardinality_impl(cls_: type[T], params: BindParams[Any]) -> TableCardinality:
            count = getattr(params.args, "count", None)
            if not isinstance(count, int) or count < 0:
                raise ValueError(f"Expected a non-negative integer 'count' argument for {cls_.__name__}")
            return TableCardinality(estimate=count, max=count)

        cls.cardinality = classmethod(cardinality_impl)  # type: ignore[assignment]

    return cls


@dataclass(kw_only=True)
class CountdownState(ArrowSerializableDataclass):
    """Mutable state tracking remaining rows and current position."""

    remaining: int
    current_index: int = 0


@dataclass(slots=True, frozen=True)
class _EmptyArgs:
    """No arguments."""


@dataclass(kw_only=True)
class _OneShotState(ArrowSerializableDataclass):
    """State that emits data once."""

    done: bool = False


class _BaseSequenceFunction(TableFunctionGenerator[Any, CountdownState]):
    """Shared logic for SequenceFunction and DoubleSequenceFunction.

    Subclasses provide NUMPY_DTYPE and FIXED_SCHEMA as class variables.
    The args class must have count, batch_size, and increment attributes.
    """

    NUMPY_DTYPE: ClassVar[type[np.generic]]
    STATS_ARROW_TYPE: ClassVar[pa.DataType] = pa.int64()
    STATS_COLUMN_NAME: ClassVar[str] = "n"

    @classmethod
    def initial_state(cls, params: ProcessParams[Any]) -> CountdownState:
        """Create initial state with remaining count."""
        return CountdownState(remaining=params.args.count)

    @classmethod
    def statistics(cls, params: BindParams[Any]) -> list[ColumnStatistics] | None:
        """Exact per-column stats derived from the user's bind args.

        For sequence(count, increment=k): the output column spans
        [0, (count - 1) * increment] with no nulls and count distinct values.
        """
        count = getattr(params.args, "count", None)
        increment = getattr(params.args, "increment", 1)
        if not isinstance(count, int) or count <= 0:
            return []
        max_value = (count - 1) * increment
        return [
            ColumnStatistics(
                column_name=cls.STATS_COLUMN_NAME,
                min=pa.scalar(0, cls.STATS_ARROW_TYPE),
                max=pa.scalar(max_value, cls.STATS_ARROW_TYPE),
                has_null=False,
                has_not_null=True,
                distinct_count=count,
            )
        ]

    @classmethod
    def process(cls, params: ProcessParams[Any], state: CountdownState, out: OutputCollector) -> None:
        """Generate the next batch of the sequence."""
        if state.remaining <= 0:
            out.finish()
            return

        size = min(state.remaining, params.args.batch_size)
        values = np.arange(
            state.current_index * params.args.increment,
            (state.current_index + size) * params.args.increment,
            params.args.increment,
            dtype=cls.NUMPY_DTYPE,
        )

        out.emit(pa.RecordBatch.from_arrays([pa.array(values)], schema=params.output_schema))

        state.current_index += size
        state.remaining -= size
