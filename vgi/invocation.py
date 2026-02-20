"""Response types for the VGI protocol.

This module defines response dataclasses and the FunctionType enum:

- FunctionType: Enum for scalar, table, and aggregate function types.
- BindResponse: Result of bind phase with output schema.
- BaseInitResponse: Base class for init responses.
- GlobalInitResponse: Result of init phase with max_workers.

Request types (BindRequest, InitRequest) are in ``vgi.protocol``.

"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Annotated

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass, ArrowType

from vgi.metadata import DEFAULT_MAX_WORKERS

__all__ = [
    "FunctionType",
    "BaseInitResponse",
    "BindResponse",
    "GlobalInitResponse",
]


class FunctionType(Enum):
    """Type of function being invoked.

    Used in BindRequest to indicate which function category is being bound,
    allowing the worker to apply appropriate validation and processing.

    """

    AGGREGATE = "aggregate"
    SCALAR = "scalar"
    TABLE = "table"


@dataclass(frozen=True, slots=True, kw_only=True)
class BindResponse(ArrowSerializableDataclass):
    """The result of calling bind() on a function.

    The bind result is created by calling bind() and importantly contains
    the function's output characteristics. It is serialized and sent to the
    client before any data processing begins.

    Attributes:
        output_schema: Arrow schema describing the structure of output batches.
        opaque_data: Serialized data that is opaque to the caller that must
            be passed to any init() invocations.

    """

    output_schema: Annotated[pa.Schema, ArrowType(pa.binary())]
    opaque_data: Annotated[ArrowSerializableDataclass | None, ArrowType(pa.binary())] = None


@dataclass(frozen=True, slots=True, kw_only=True)
class BaseInitResponse(ArrowSerializableDataclass):
    """The result of calling init() on a function.

    Attributes:
        execution_id: A unique id for the function execution.
        opaque_data: Serialized data that is opaque to the caller that must
            be passed to any init() invocations.

    """

    execution_id: bytes = field(default_factory=lambda: uuid.uuid4().bytes)
    opaque_data: Annotated[ArrowSerializableDataclass | None, ArrowType(pa.binary())] = None


@dataclass(frozen=True, slots=True, kw_only=True)
class GlobalInitResponse(BaseInitResponse):
    """The result of calling init() on a function.

    Attributes:
        max_workers: The maximum number of worker processes that may be
            used for this function execution. This allows the function to control
            parallelism.

    """

    max_workers: int = DEFAULT_MAX_WORKERS
