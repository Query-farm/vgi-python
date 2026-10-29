# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Negotiated catalog query and explicit statement preparation contracts.

SQL encoding, semantic equivalence, and authorization are separate concerns.
These records never execute SQL. A decline is a decision made before execution;
authentication, malformed data and backend failures remain errors.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field, replace
from typing import Any, Self, cast

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.utils import IpcValidation

from vgi.catalog.dependencies import CatalogObjectRef
from vgi.catalog.evaluation_context import QueryEvaluationContext as QueryEvaluationContext
from vgi.query_parameters import read_parameter_schema
from vgi.typed_values import TypedValueBatch

QUERY_ENCODING = "duckdb.sql.v1"
QUERY_EXPLICIT = "vgi.remote.readonly.v1"
QUERY_AUTOMATIC = "vgi.duckdb.relational.v1"
QUERY_STATEMENT = "vgi.remote.statement.v1"
QUERY_EXECUTOR = "__vgi_catalog_query"
STATEMENT_RESULT_KEY = b"vgi.statement.result.v1"
_MODE_PROFILES = {"explicit": QUERY_EXPLICIT, "automatic": QUERY_AUTOMATIC, "statement": QUERY_STATEMENT}
MAX_QUERY_BYTES = 1024 * 1024
MAX_QUERY_REQUEST_BYTES = 8 * 1024 * 1024
MAX_QUERY_SCHEMA_BYTES = 1024 * 1024
MAX_QUERY_TOKEN_BYTES = 64 * 1024


def _text(value: str, label: str, *, maximum: int = 4096, empty: bool = False) -> None:
    if not isinstance(value, str) or "\0" in value or (not empty and not value):
        raise ValueError(f"invalid {label}")
    if len(value.encode("utf-8")) > maximum:
        raise ValueError(f"{label} is too large")


def _integer(value: object, label: str, *, minimum: int = 0, maximum: int = 2**63 - 1) -> None:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"invalid {label}")


def _token(value: object, label: str) -> None:
    if not isinstance(value, bytes) or not 0 < len(value) <= MAX_QUERY_TOKEN_BYTES:
        raise ValueError(f"invalid {label}")


def _names(values: list[str], label: str, *, empty: bool = False) -> None:
    if not isinstance(values, list) or (not empty and not values) or len(values) != len(set(values)):
        raise ValueError(f"{label} must be a nonempty list of unique names")
    for value in values:
        _text(value, label, maximum=256)


def read_query_schema(payload: bytes | None) -> pa.Schema:
    """Read one bounded schema message without accepting a table or trailing data."""
    if not isinstance(payload, bytes) or not 0 < len(payload) <= MAX_QUERY_SCHEMA_BYTES:
        raise ValueError("invalid query schema size")
    source = pa.BufferReader(payload)
    result = pa.ipc.read_schema(cast(Any, source))
    if source.tell() != len(payload):
        raise ValueError("query schema has trailing data")
    return result


def statement_result_kind(schema: pa.Schema) -> str:
    """Validate a statement's fixed result contract before execution.

    A command produces exactly one completion row: nullable int64 affected_rows
    (NULL for DDL). RETURNING produces rows of its declared schema, possibly none.
    Stream EOS establishes statement completion, never transaction commit.
    """
    if not schema.equals(schema.with_metadata(schema.metadata), check_metadata=True):
        raise ValueError("statement output schema metadata keys must be unique")
    kind = (schema.metadata or {}).get(STATEMENT_RESULT_KEY)
    if kind == b"command":
        expected = pa.schema([pa.field("affected_rows", pa.int64())])
        if not schema.remove_metadata().equals(expected, check_metadata=True):
            raise ValueError("statement command requires nullable int64 affected_rows")
    elif kind == b"rows":
        if not len(schema):
            raise ValueError("statement rows require a nonempty output schema")
    else:
        raise ValueError("statement output requires command or rows result metadata")
    return kind.decode("ascii")


class _QueryRecord(ArrowSerializableDataclass):
    @classmethod
    def deserialize_from_bytes(cls, data: bytes, ipc_validation: IpcValidation = IpcValidation.FULL) -> Self:
        """Bound the envelope before nested decoding and require one complete row."""
        if not isinstance(data, bytes) or not 0 < len(data) <= MAX_QUERY_REQUEST_BYTES:
            raise ValueError("invalid query record size")
        return cls.deserialize_from_batch(TypedValueBatch.deserialize_from_bytes(data).batch)

    @classmethod
    def deserialize_from_batch(
        cls,
        batch: pa.RecordBatch,
        custom_metadata: pa.KeyValueMetadata | None = None,
        *,
        ipc_validation: IpcValidation = IpcValidation.FULL,
    ) -> Self:
        """Validate full Arrow buffers, required fields and exact field definitions."""
        if batch.num_rows != 1 or not batch.schema.equals(cls.ARROW_SCHEMA, check_metadata=False):
            raise ValueError(f"{cls.__name__} requires one row with its complete schema")
        if batch.nbytes > MAX_QUERY_REQUEST_BYTES:
            raise ValueError("query record is too large")
        TypedValueBatch(batch)  # Includes required nested fields, not just Arrow buffer validity.
        return super().deserialize_from_batch(batch, custom_metadata, ipc_validation=IpcValidation.FULL)

    def serialize_to_bytes(self) -> bytes:
        """Enforce the same envelope bound for locally constructed requests."""
        if validate := getattr(self, "__post_init__", None):
            validate()
        result = super().serialize_to_bytes()
        if len(result) > MAX_QUERY_REQUEST_BYTES:
            raise ValueError("query record is too large")
        return result


@dataclass(frozen=True, kw_only=True)
class ClientQueryCapabilities(_QueryRecord):
    """An offer, including the actual engine build; never an authorization grant."""

    encodings: list[str]
    semantic_profiles: list[str]
    engine_version: str
    max_query_bytes: int = MAX_QUERY_BYTES

    def __post_init__(self) -> None:
        """Validate offers while allowing unknown names to be ignored in selection."""
        _names(self.encodings, "query encodings", empty=True)
        _names(self.semantic_profiles, "query profiles", empty=True)
        _text(self.engine_version, "client engine version")
        _integer(self.max_query_bytes, "maximum SQL size", minimum=1, maximum=MAX_QUERY_BYTES)


@dataclass(frozen=True, kw_only=True)
class QueryCapabilities(_QueryRecord):
    """An attachment's immutable query agreement, including provider policy revision.

    Providers select automatic support only for certified client engine builds.
    The generic intersection helper cannot establish semantic equivalence.
    """

    encoding: str
    semantic_profiles: list[str]
    execution_modes: list[str]
    backend_engine: str
    backend_version: str
    capability_revision: str
    default_evaluation_context: QueryEvaluationContext
    max_query_bytes: int = MAX_QUERY_BYTES

    def __post_init__(self) -> None:
        """Require internally consistent known execution modes and profiles."""
        if self.encoding != QUERY_ENCODING:
            raise ValueError("unknown selected query encoding")
        _names(self.semantic_profiles, "selected query profiles")
        _names(self.execution_modes, "query execution modes")
        if not set(self.semantic_profiles) <= set(_MODE_PROFILES.values()):
            raise ValueError("unknown selected query profile")
        if set(self.execution_modes) != {
            mode for mode, profile in _MODE_PROFILES.items() if profile in self.semantic_profiles
        }:
            raise ValueError("query modes must match selected semantic profiles")
        for label in ["backend_engine", "backend_version", "capability_revision"]:
            _text(getattr(self, label), label)
        if not isinstance(self.default_evaluation_context, QueryEvaluationContext):
            raise ValueError("query capabilities require a typed evaluation context")
        self.default_evaluation_context.__post_init__()
        _integer(self.max_query_bytes, "maximum SQL size", minimum=1, maximum=MAX_QUERY_BYTES)

    def select(self, offer: ClientQueryCapabilities | None, *, supports_transactions: bool) -> QueryCapabilities | None:
        """Intersect understood offers; never infer automatic support from an engine name."""
        self.__post_init__()
        if type(supports_transactions) is not bool:
            raise ValueError("transaction support must be a boolean")
        if offer is None:
            return None
        offer.__post_init__()
        if self.encoding not in offer.encodings:
            return None
        profiles = set(self.semantic_profiles) & set(offer.semantic_profiles)
        if not supports_transactions:
            profiles.discard(QUERY_AUTOMATIC)
            profiles.discard(QUERY_STATEMENT)
        if not profiles:
            return None
        return replace(
            self,
            semantic_profiles=sorted(profiles),
            execution_modes=sorted(mode for mode, profile in _MODE_PROFILES.items() if profile in profiles),
            max_query_bytes=min(self.max_query_bytes, offer.max_query_bytes),
        )


@dataclass(frozen=True, kw_only=True)
class QueryObjectReference(_QueryRecord):
    """One logical object and its expected schema, with no alias or engine OID."""

    object: CatalogObjectRef
    expected_schema: bytes

    def __post_init__(self) -> None:
        """Retain raw nested schema paths and validate the schema-only envelope."""
        if not isinstance(self.object, CatalogObjectRef):
            raise ValueError("query object reference requires a CatalogObjectRef")
        self.object.__post_init__()
        if len(self.object.schema_path) > 32:
            raise ValueError("query object schema path is too deep")
        read_query_schema(self.expected_schema)


@dataclass(frozen=True, kw_only=True)
class CatalogQueryPrepareRequest(_QueryRecord):
    """Bind a read query and parameter schema without evaluating its result."""

    attach_opaque_data: bytes
    encoding: str
    semantic_profile: str
    mode: str
    capability_revision: str
    query: str
    parameter_schema: bytes
    evaluation_context: QueryEvaluationContext
    transaction_opaque_data: bytes | None = None
    catalog_version: int | None = None
    catalog_etag: str | None = None
    referenced_objects: list[QueryObjectReference] = field(default_factory=list)
    expected_output_schema: bytes | None = None
    required_ordering: str = "unspecified"

    def __post_init__(self) -> None:
        """Validate the mode-specific contract before provider parsing or binding."""
        _token(self.attach_opaque_data, "query attachment")
        if self.transaction_opaque_data is not None:
            _token(self.transaction_opaque_data, "query transaction")
        if self.encoding != QUERY_ENCODING:
            raise ValueError("unknown selected query encoding")
        if _MODE_PROFILES.get(self.mode) != self.semantic_profile:
            raise ValueError("query mode and semantic profile disagree")
        _text(self.capability_revision, "query capability revision")
        _text(self.query, "SQL query", maximum=MAX_QUERY_BYTES)
        read_parameter_schema(self.parameter_schema)
        if not isinstance(self.evaluation_context, QueryEvaluationContext):
            raise ValueError("query requires a typed evaluation context")
        self.evaluation_context.__post_init__()
        if self.required_ordering not in {"unspecified", "query_order"}:
            raise ValueError("unknown required result ordering")
        if self.catalog_version is not None:
            _integer(self.catalog_version, "catalog metadata version")
        if self.catalog_etag is not None:
            _text(self.catalog_etag, "catalog etag", empty=True)
        if not isinstance(self.referenced_objects, list) or len(self.referenced_objects) > 1024:
            raise ValueError("invalid query object manifest")
        seen = set()
        for reference in self.referenced_objects:
            if not isinstance(reference, QueryObjectReference):
                raise ValueError("query manifest requires typed object references")
            reference.__post_init__()
            key = reference.object.key()
            if key in seen:
                raise ValueError("duplicate query object reference")
            seen.add(key)
        if self.expected_output_schema is not None:
            read_query_schema(self.expected_output_schema)
        if self.mode == "automatic":
            if (
                self.transaction_opaque_data is None
                or self.catalog_version is None
                or self.expected_output_schema is None
            ):
                raise ValueError("automatic query requires a transaction, metadata version and output contract")
            if not self.referenced_objects or any(r.object.kind != "table" for r in self.referenced_objects):
                raise ValueError("automatic v1 requires a nonempty table-only object manifest")
        elif self.referenced_objects:
            raise ValueError("explicit query resolves its own objects and requires an empty manifest")
        if self.mode == "statement" and self.transaction_opaque_data is None:
            raise ValueError("statement execution requires a transaction")

    def validate_agreement(self, selected: QueryCapabilities) -> None:
        """Require the immutable encoding, mode, context and policy revision."""
        self.__post_init__()
        selected.__post_init__()
        if (
            self.encoding != selected.encoding
            or self.semantic_profile not in selected.semantic_profiles
            or self.mode not in selected.execution_modes
            or self.capability_revision != selected.capability_revision
        ):
            raise ValueError("query preparation does not match the attachment agreement")
        if len(self.query.encode("utf-8")) > selected.max_query_bytes:
            raise ValueError("query exceeds the selected SQL size limit")
        if self.mode in {"explicit", "statement"} and self.evaluation_context != selected.default_evaluation_context:
            raise ValueError("explicit query must use the attachment's provider evaluation context")


@dataclass(frozen=True, kw_only=True)
class CatalogQueryPrepareResult(_QueryRecord):
    """An exclusive accepted/declined union; decline never masks an execution error."""

    status: str
    reason_code: str | None = None
    reason_detail: str | None = None
    prepared_query_token: bytes | None = None
    output_schema: bytes | None = None
    result_ordering: str | None = None
    validated_catalog_version: int | None = None
    validated_catalog_etag: str | None = None
    context_digest: bytes | None = None
    expires_at_epoch_ms: int | None = None
    estimated_rows: int | None = None
    estimated_bytes: int | None = None

    def __post_init__(self) -> None:
        """Reject incomplete, contradictory and unknown status combinations."""
        if self.status == "declined":
            if not isinstance(self.reason_code, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", self.reason_code):
                raise ValueError("declined query requires a bounded reason code")
            if self.reason_detail is not None:
                _text(self.reason_detail, "query decline detail", empty=True)
            accepted = [
                self.prepared_query_token,
                self.output_schema,
                self.result_ordering,
                self.validated_catalog_version,
                self.validated_catalog_etag,
                self.context_digest,
                self.expires_at_epoch_ms,
                self.estimated_rows,
                self.estimated_bytes,
            ]
            if any(value is not None for value in accepted):
                raise ValueError("declined query cannot carry acceptance fields")
            return
        if self.status != "accepted":
            raise ValueError("unknown query preparation status")
        if self.reason_code is not None or self.reason_detail is not None:
            raise ValueError("accepted query cannot carry decline reasons")
        _token(self.prepared_query_token, "prepared query token")
        read_query_schema(self.output_schema)
        if self.result_ordering not in {"unspecified", "query_order"}:
            raise ValueError("accepted query requires a known result ordering")
        if not isinstance(self.context_digest, bytes) or len(self.context_digest) != 32:
            raise ValueError("accepted query requires a 32-byte context digest")
        _integer(self.expires_at_epoch_ms, "prepared query expiry", minimum=1)
        for name in ["validated_catalog_version", "estimated_rows", "estimated_bytes"]:
            value = getattr(self, name)
            if value is not None:
                _integer(value, name)
        if self.validated_catalog_etag is not None:
            _text(self.validated_catalog_etag, "validated catalog etag", empty=True)

    def validate_request(self, request: CatalogQueryPrepareRequest) -> None:
        """Check context, exact output schema and order before accepting a plan."""
        self.__post_init__()
        request.__post_init__()
        if self.status == "declined":
            return
        if self.context_digest != request.evaluation_context.digest(request.encoding, request.semantic_profile):
            raise ValueError("query preparation context digest mismatch")
        if request.required_ordering == "query_order" and self.result_ordering != "query_order":
            raise ValueError("query preparation lost required ordering")
        if request.mode == "automatic" and self.validated_catalog_version is None:
            raise ValueError("accepted automatic query requires a validated metadata version")
        if request.mode == "statement":
            statement_result_kind(read_query_schema(self.output_schema))
        elif STATEMENT_RESULT_KEY in (read_query_schema(self.output_schema).metadata or {}):
            raise ValueError("statement result metadata requires the statement profile")
        if request.expected_output_schema is not None:
            actual = read_query_schema(self.output_schema)
            expected = read_query_schema(request.expected_output_schema)
            if not actual.equals(expected, check_metadata=True):
                raise ValueError("query preparation output contract mismatch")
