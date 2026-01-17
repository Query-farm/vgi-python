"""Shared protocol types for VGI function communication.

This module provides base classes for protocol messages used across
different function types.
"""

from dataclasses import dataclass
from typing import ClassVar, Self

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

    # pa.KeyValueMetadata uses bytes so we define signals as bytes
    _FINALIZE_SIGNAL: ClassVar[bytes] = b"FINALIZE"

    @property
    def is_finalize(self) -> bool:
        """Check if this input signals the FINALIZE phase."""
        return (
            self.metadata is not None
            and self.metadata.get(b"type") == self._FINALIZE_SIGNAL
        )

    @classmethod
    def create_finalize(cls, batch: pa.RecordBatch) -> Self:
        """Create a ProtocolInput that signals the FINALIZE phase.

        This is only sent once so there is no benefit to caching it.
        """
        return cls(
            batch=batch, metadata=pa.KeyValueMetadata({b"type": cls._FINALIZE_SIGNAL})
        )
