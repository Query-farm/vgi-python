# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Explicit Arrow encodings shared by the proposed reporting contracts."""

from datetime import datetime
from typing import Annotated

import pyarrow as pa
from vgi_rpc import ArrowType

Instant = Annotated[datetime, ArrowType(pa.timestamp("us", "UTC"))]
"""An aware datetime, encoded as UTC microseconds on the wire."""

Json = str
"""One JSON value encoded as text; semantic validation belongs to the worker."""

SchemaIpc = Annotated[pa.Schema, ArrowType(pa.binary())]
"""An Arrow schema serialized as one IPC schema message."""

RowIpc = Annotated[pa.RecordBatch, ArrowType(pa.binary())]
"""A one-row batch serialized with its schema as a complete IPC stream."""


def non_null_list(item: pa.DataType) -> ArrowType:
    """Declare a list whose elements cannot be null, independent of Arrow defaults."""
    return ArrowType(pa.list_(pa.field("item", item, nullable=False)))
