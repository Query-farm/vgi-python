# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Shared aggregate state classes used across multiple submodules."""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Annotated, ClassVar

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass, ArrowType


@dataclass(kw_only=True)
class SumState:
    """Per-group running total, packed as a single little-endian int64.

    Deliberately NOT an ``ArrowSerializableDataclass``: this is the aggregate
    side of the worked example that ``CountdownState`` provides for table
    functions. An aggregate serializes state once per group per batch, and
    Arrow IPC is a columnar container -- a one-row stream pays for a schema
    message, a batch message, an end-of-stream marker and alignment padding
    whatever the payload. This state is one integer; packing it directly costs
    8 bytes.

    The framework asks only for [`StreamStateCodec`][] -- ``serialize_to_bytes``
    and ``deserialize_from_bytes`` -- and treats the result as opaque bytes in
    ``FunctionStorage``. The other fixtures here keep using
    ``ArrowSerializableDataclass``, so both paths stay covered.
    """

    total: int = 0

    _STRUCT: ClassVar[struct.Struct] = struct.Struct("<q")

    def serialize_to_bytes(self) -> bytes:
        """Pack the running total as a little-endian int64."""
        return self._STRUCT.pack(self.total)

    @classmethod
    def deserialize_from_bytes(cls, data: bytes) -> SumState:
        """Unpack a payload written by :meth:`serialize_to_bytes`.

        Args:
            data: The packed bytes, exactly one little-endian int64.

        Returns:
            The restored running total.

        Raises:
            ValueError: The payload is not the expected width, which means it
                was not written by this codec.

        """
        if len(data) != cls._STRUCT.size:
            msg = f"SumState expects {cls._STRUCT.size} bytes, got {len(data)}"
            raise ValueError(msg)
        (total,) = cls._STRUCT.unpack(data)
        return cls(total=total)


@dataclass(kw_only=True)
class ListAggState(ArrowSerializableDataclass):
    values: Annotated[str, ArrowType(pa.string())] = ""
