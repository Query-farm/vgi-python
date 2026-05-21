# Copyright 2025, 2026 Query Farm LLC - https://query.farm

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

from vgi.arguments import SecretLookupEntry
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
    TABLE_BUFFERING = "table_buffering"


@dataclass(frozen=True, slots=True, kw_only=True)
class BindResponse(ArrowSerializableDataclass):
    """The result of calling bind() on a function.

    The bind result is created by calling bind() and importantly contains
    the function's output characteristics. It is serialized and sent to the
    client before any data processing begins.

    When ``lookup_secret_types`` is non-empty, this is a **secret scope
    request** rather than a normal bind response. C++ resolves the requested
    secrets and retries bind with ``resolved_secrets_provided=True``. The
    developer never constructs scope requests directly — the framework
    generates them when ``SecretsAccessor`` has pending lookups after
    ``on_bind()`` returns.

    Attributes:
        output_schema: Arrow schema describing the structure of output batches.
        opaque_data: Serialized data that is opaque to the caller that must
            be passed to any init() invocations.
        lookup_secret_types: Secret types for scoped lookup requests (empty = normal response).
        lookup_scopes: Scopes for scoped lookup requests (parallel to lookup_secret_types).
        lookup_names: Names for scoped lookup requests (parallel to lookup_secret_types).

    """

    output_schema: Annotated[pa.Schema, ArrowType(pa.binary())]
    # Wire-facing field — the bytes are produced by the framework calling
    # ``.serialize_to_bytes()`` on the typed ``BindResult.opaque_data`` at
    # the bind→response boundary (see vgi.scalar_function /
    # vgi.table_function / vgi.table_in_out_function). Consumers
    # reconstruct via ``MyConcreteDataclass.deserialize_from_bytes(raw)``;
    # the abstract-base typed-roundtrip can't be done in Python without a
    # class registry, so we kept the wire honest about being bytes.
    opaque_data: Annotated[bytes | None, ArrowType(pa.binary())] = None
    lookup_secret_types: list[str] = field(default_factory=list)
    lookup_scopes: list[str] = field(default_factory=list)
    lookup_names: list[str] = field(default_factory=list)

    @property
    def is_secret_scope_request(self) -> bool:
        """True if this is a secret scope request, not a normal bind response."""
        return len(self.lookup_secret_types) > 0

    @staticmethod
    def secret_scope_request(entries: list[SecretLookupEntry]) -> BindResponse:
        """Create a secret scope request from lookup entries.

        The framework calls this when ``SecretsAccessor`` has pending lookups.
        C++ detects the non-empty ``lookup_secret_types`` and resolves them.
        """
        return BindResponse(
            output_schema=pa.schema([]),
            lookup_secret_types=[e.secret_type for e in entries],
            lookup_scopes=[e.scope or "" for e in entries],
            lookup_names=[e.secret_name or "" for e in entries],
        )

    def secret_scope_entries(self) -> list[SecretLookupEntry]:
        """Convert lookup fields back to SecretLookupEntry objects."""
        return [
            SecretLookupEntry(
                secret_type=t,
                scope=s or None,
                secret_name=n or None,
            )
            for t, s, n in zip(
                self.lookup_secret_types,
                self.lookup_scopes,
                self.lookup_names,
                strict=True,
            )
        ]


@dataclass(frozen=True, slots=True, kw_only=True)
class BaseInitResponse(ArrowSerializableDataclass):
    """The result of calling init() on a function.

    Attributes:
        execution_id: A unique id for the function execution.
        opaque_data: Serialized data that is opaque to the caller that must
            be passed to any init() invocations.

    """

    execution_id: bytes = field(default_factory=lambda: uuid.uuid4().bytes)
    # Wire-facing field — see comment on ``BindResponse.opaque_data``
    # above for the typed-producer / bytes-wire / explicit-consumer
    # contract.
    opaque_data: Annotated[bytes | None, ArrowType(pa.binary())] = None


@dataclass(frozen=True, slots=True, kw_only=True)
class GlobalInitResponse(BaseInitResponse):
    """The result of calling init() on a function.

    Attributes:
        max_workers: The maximum number of worker processes that may be
            used for this function execution. This allows the function to control
            parallelism.

    """

    max_workers: int = DEFAULT_MAX_WORKERS
