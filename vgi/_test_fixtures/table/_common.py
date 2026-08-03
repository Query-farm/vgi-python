# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Shared infrastructure for table fixture functions.

Holds the cardinality decorator, the common ``CountdownState``, the
``CountBatchArgs`` base for fixtures that take ``(count, batch_size)``,
and the ``_BaseSequenceFunction`` template-method base class for
countdown-style generators.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Annotated, Any, ClassVar

import numpy as np
import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi.arguments import Arg
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


@dataclass(kw_only=True, slots=True)
class CountdownState:
    """Mutable state tracking remaining rows and current position.

    Deliberately NOT an ``ArrowSerializableDataclass``: this is the worked
    example of a function owning its own state encoding. The state is two
    integers, and Arrow IPC is a columnar container -- a one-row stream pays
    for a schema message, a batch message, an end-of-stream marker and
    alignment padding whatever the payload. Measured here: 416 bytes and 36us
    per encode, against 16 bytes and 0.21us for the same two integers packed
    directly.

    The framework asks only for [`StreamStateCodec`][] -- ``serialize_to_bytes``
    and ``deserialize_from_bytes`` -- and treats the result as opaque bytes
    inside the state token. That is also what lets a Python worker match the
    state encoding of a sibling VGI implementation in another language rather
    than being forced through Arrow.
    """

    remaining: int
    current_index: int = 0

    _STRUCT: ClassVar[struct.Struct] = struct.Struct("<qq")

    def serialize_to_bytes(self) -> bytes:
        """Pack both counters as little-endian int64."""
        return self._STRUCT.pack(self.remaining, self.current_index)

    @classmethod
    def deserialize_from_bytes(cls, data: bytes) -> CountdownState:
        """Unpack a payload written by :meth:`serialize_to_bytes`.

        Raises:
            ValueError: The payload is not the expected width, which means it
                was not written by this codec.

        """
        if len(data) != cls._STRUCT.size:
            msg = f"CountdownState expects {cls._STRUCT.size} bytes, got {len(data)}"
            raise ValueError(msg)
        remaining, current_index = cls._STRUCT.unpack(data)
        return cls(remaining=remaining, current_index=current_index)


@dataclass(frozen=True)
class CountBatchArgs:
    """Standard ``(count, batch_size)`` argument pair for countdown-style fixtures.

    Subclass this to add fixture-specific knobs without re-declaring the two
    common fields. Note: ``slots=True`` is intentionally omitted so subclasses
    can extend cleanly without slot-conflict gymnastics.
    """

    count: Annotated[int, Arg(0, doc="Number of rows to generate", ge=0)]
    batch_size: Annotated[int, Arg("batch_size", default=2048, doc="Batch size for output", ge=1)]


@dataclass(slots=True, frozen=True)
class _EmptyArgs:
    """No arguments."""


@dataclass(kw_only=True)
class _OneShotState(ArrowSerializableDataclass):
    """State that emits data once."""

    done: bool = False


class _BaseSequenceFunction(TableFunctionGenerator[Any, CountdownState]):
    """Template-method base for countdown-style fixture generators.

    Provides ``initial_state``, the countdown bookkeeping in ``process``, and
    a default numpy-arange ``_emit_chunk`` used by SequenceFunction /
    DoubleSequenceFunction. Subclasses with non-arange output (e.g. echoes,
    nested types, row-id sequences) override ``_emit_chunk``.

    ``BATCH_SIZE_FALLBACK`` is used when ``params.args`` has no ``batch_size``
    field — i.e. fixtures that want a fixed batch size rather than a user knob.
    """

    NUMPY_DTYPE: ClassVar[type[np.generic]] = np.int64
    STATS_ARROW_TYPE: ClassVar[pa.DataType] = pa.int64()
    STATS_COLUMN_NAME: ClassVar[str] = "n"
    BATCH_SIZE_FALLBACK: ClassVar[int] = 2048

    @classmethod
    def initial_state(cls, params: ProcessParams[Any]) -> CountdownState:
        """Create initial state with remaining count."""
        return CountdownState(remaining=params.args.count)

    @classmethod
    def statistics(cls, params: BindParams[Any]) -> list[ColumnStatistics] | None:
        """Exact per-column stats derived from the user's bind args.

        For sequence(count, increment=k): the output column spans
        [0, (count - 1) * increment] with no nulls and count distinct values.
        Returns ``None`` (no stats) for fixtures whose output isn't a single
        ``int64`` arange — they should override.
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
        """Run the standard countdown loop; delegate batch contents to ``_emit_chunk``."""
        if state.remaining <= 0:
            out.finish()
            return

        batch_size = getattr(params.args, "batch_size", cls.BATCH_SIZE_FALLBACK)
        size = min(state.remaining, batch_size)
        cls._emit_chunk(params, state, out, state.current_index, size)
        state.current_index += size
        state.remaining -= size

    @classmethod
    def _emit_chunk(
        cls,
        params: ProcessParams[Any],
        state: CountdownState,
        out: OutputCollector,
        start: int,
        size: int,
    ) -> None:
        """Default implementation: numpy arange × increment.

        Subclasses with non-arange output override this hook. ``state`` is
        passed in case subclasses want to track additional info; the standard
        countdown bookkeeping (``current_index``/``remaining``) is handled by
        ``process`` itself, so subclass hooks should NOT mutate them.
        """
        increment = params.args.increment
        values = np.arange(
            start * increment,
            (start + size) * increment,
            increment,
            dtype=cls.NUMPY_DTYPE,
        )
        out.emit(pa.RecordBatch.from_arrays([pa.array(values)], schema=params.output_schema))
