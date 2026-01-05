"""Shared protocol types for VGI function communication.

This module provides base classes for protocol messages used across
different function types.
"""

from dataclasses import dataclass

import pyarrow as pa

__all__ = ["ProtocolInput"]


@dataclass(frozen=True, slots=True)
class ProtocolInput:
    """Base input sent to function generators via send().

    Attributes:
        batch: The input RecordBatch to process.
        metadata: Optional metadata from the IPC stream.

    """

    batch: pa.RecordBatch
    metadata: pa.KeyValueMetadata | None = None
