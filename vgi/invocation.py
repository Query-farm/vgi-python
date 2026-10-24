# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Response types for the VGI protocol.

This module defines response dataclasses and the `FunctionType` enum:

- `FunctionType`: Enum for scalar, table, and aggregate function types.
- [`BindResponse`][]: Result of bind phase with output schema.
- [`BaseInitResponse`][]: Base class for init responses.
- [`GlobalInitResponse`][]: Result of init phase with max_workers.

Request types ([`BindRequest`][], [`InitRequest`][]) are in ``vgi.protocol``.

"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Annotated, Self

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass, ArrowType, IpcValidation

from vgi.arguments import SecretLookupEntry
from vgi.metadata import DEFAULT_MAX_WORKERS

__all__ = [
    "FunctionType",
    "BaseInitResponse",
    "BindResponse",
    "BindRepeatability",
    "GlobalInitResponse",
]


class FunctionType(Enum):
    """Type of function being invoked.

    Used in [`BindRequest`][] to indicate which function category is being bound,
    allowing the worker to apply appropriate validation and processing.

    """

    AGGREGATE = "aggregate"
    SCALAR = "scalar"
    TABLE = "table"
    TABLE_BUFFERING = "table_buffering"


class BindRepeatability(Enum):
    """Repeatability of equivalent scans derived from one complete bind state.

    ``STABLE_WITHIN_QUERY`` promises the same logical rows and values during a
    single query. It does not combine independently bound sources into one
    snapshot, promise row order or batch boundaries, authorize retries, or
    establish cross-query cache validity. A provider anchors the promise in
    existing opaque bind state or an existing transaction snapshot.
    """

    UNKNOWN = "unknown"
    STABLE_WITHIN_QUERY = "stable_within_query"


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
    generates them when `[`SecretsAccessor`][]` has pending lookups after
    ``on_bind()`` returns.

    Attributes:
        output_schema: Arrow schema describing the structure of output batches.
        opaque_data: Serialized data that is opaque to the caller that must
            be passed to any init() invocations.
        lookup_secret_types: Secret types for scoped lookup requests (empty = normal response).
        lookup_scopes: Scopes for scoped lookup requests (parallel to lookup_secret_types).
        lookup_names: Names for scoped lookup requests (parallel to lookup_secret_types).
        repeatability: Optional provider promise for equivalent scans of this
            complete bind state within one query; unknown makes no promise.

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
    projection_expression_profiles: list[str] = field(default_factory=list)
    repeatability: BindRepeatability = BindRepeatability.UNKNOWN

    def __post_init__(self) -> None:
        """Validate the per-bind offer without granting projection acceptance."""
        if not isinstance(self.repeatability, BindRepeatability):
            raise ValueError("invalid bind repeatability")
        profiles = self.projection_expression_profiles
        if (
            not isinstance(profiles, list)
            or len(profiles) > 64
            or any(
                not isinstance(profile, str)
                or len(profile) > 256
                or re.fullmatch(r"[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)*\.v[1-9][0-9]*", profile) is None
                for profile in profiles
            )
            or len(set(profiles)) != len(profiles)
        ):
            raise ValueError("invalid projection expression profile offer")

    @classmethod
    def deserialize_from_batch(
        cls,
        batch: pa.RecordBatch,
        custom_metadata: pa.KeyValueMetadata | None = None,
        *,
        ipc_validation: IpcValidation = IpcValidation.FULL,
    ) -> Self:
        """Require the successor promise before generic legacy enum decoding."""
        index = batch.schema.get_field_index("repeatability")
        if batch.num_rows != 1 or index < 0:
            raise ValueError("bind repeatability must be present in one row")
        field = batch.schema.field(index)
        if field.nullable or field.type != pa.dictionary(pa.int16(), pa.string()):
            raise ValueError("bind repeatability requires nonnullable dictionary<int16, utf8>")
        value = batch.column(index)[0].as_py()
        if value not in BindRepeatability.__members__:
            raise ValueError("invalid bind repeatability wire value")
        return super(BindResponse, cls).deserialize_from_batch(batch, custom_metadata, ipc_validation=ipc_validation)

    @property
    def is_secret_scope_request(self) -> bool:
        """True if this is a secret scope request, not a normal bind response."""
        return len(self.lookup_secret_types) > 0

    @staticmethod
    def secret_scope_request(entries: list[SecretLookupEntry]) -> BindResponse:
        """Create a secret scope request from lookup entries.

        The framework calls this when `[`SecretsAccessor`][]` has pending lookups.
        C++ detects the non-empty ``lookup_secret_types`` and resolves them.
        """
        return BindResponse(
            output_schema=pa.schema([]),
            lookup_secret_types=[e.secret_type for e in entries],
            lookup_scopes=[e.scope or "" for e in entries],
            lookup_names=[e.secret_name or "" for e in entries],
        )

    def secret_scope_entries(self) -> list[SecretLookupEntry]:
        """Convert lookup fields back to [`SecretLookupEntry`][] objects."""
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
