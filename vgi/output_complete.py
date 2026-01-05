"""Internal output normalization for VGI function generators.

This module provides the _OutputComplete class used by all function types
to normalize generator yields into a consistent format with guaranteed
non-None batches.

This is an internal module - users should not import from here directly.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa

import vgi.log

if TYPE_CHECKING:
    from vgi.table_function import Output as TableOutput
    from vgi.table_in_out_function import Output as TableInOutOutput

__all__ = ["OutputComplete"]


@dataclass(frozen=True, slots=True)
class OutputComplete:
    """Internal: Output with guaranteed non-None batch.

    Used by the framework to normalize generator yields. When the user yields
    None, Output with None batch, or Message, this class ensures we always
    have a valid RecordBatch for the protocol.

    Attributes:
        batch: Always a valid RecordBatch (never None).
        has_more: If True, generator expects another send() call.
            Only used by TableInOutGeneratorFunction.
        log_message: Present when user yielded Message directly.

    """

    batch: pa.RecordBatch
    has_more: bool = False
    log_message: vgi.log.Message | None = None

    @classmethod
    def from_process_result(
        cls,
        source: "vgi.log.Message | TableOutput | TableInOutOutput | None",
        empty_batch: pa.RecordBatch,
    ) -> "OutputComplete":
        """Create from user's yield value.

        Args:
            source: What the user yielded (Output, Message, or None).
            empty_batch: Empty batch to substitute when needed.

        Returns:
            Normalized output with guaranteed non-None batch.

        """
        if source is None:
            return cls(batch=empty_batch)
        if isinstance(source, vgi.log.Message):
            # When yielding a log message, has_more=True so the caller
            # re-sends the current input after the message is processed
            return cls(batch=empty_batch, has_more=True, log_message=source)
        # source is Output (either TableOutput or TableInOutOutput)
        # TableOutput doesn't have has_more, TableInOutOutput does
        has_more = getattr(source, "has_more", False)
        return cls(
            batch=source.batch if source.batch is not None else empty_batch,
            has_more=has_more,
        )
