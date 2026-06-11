# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""VGI secret protocol — the wire contract for Orchard's standalone secret service.

Orchard is an independently-deployed microservice that brokers downstream
credentials (S3/HTTP/GCS/…) for a single authenticated account. The DuckDB
extension's ``VgiRemoteSecretStorage`` calls :meth:`VgiSecretProtocol.secret_lookup`
lazily whenever a secret consumer (e.g. httpfs resolving an ``s3://`` path) asks
the secret manager for a credential.

This protocol is **versioned independently** of :class:`vgi.protocol.VgiProtocol`
(the worker/catalog protocol). It has exactly one method and a tiny surface so it
can evolve on its own cadence — see ``protocol_version`` below.

Wire shape
----------
``secret_lookup`` takes the requested ``path`` and ``type`` as direct scalar
parameters (not a wrapped ``request`` dataclass), so the generated C++ builder
``BuildSecretLookupParams(path, type)`` is directly callable without a hand-coded
inner serializer. The response is :class:`SecretLookupResponse`, IPC-serialized
into the unary ``result`` envelope and validated C++-side against
``SecretLookupResultSchema()``.

Identity is carried entirely by the OAuth bearer token on the HTTP request (the
same ``CatalogAuth`` the catalog established at ATTACH) — there is no
account/storage identifier in the request body.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Annotated, Any, ClassVar, Protocol

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass, ArrowType


def encode_secret_values(mapping: dict[str, Any]) -> pa.RecordBatch | None:
    """Build the one-row ``values`` RecordBatch from a Python mapping.

    Each key becomes a column; the cell at row 0 is the secret value. Types are
    inferred by pyarrow (str→utf8, int→int64, bool→bool, dict→struct, list→list,
    …). Pass a ``pa.array([...])`` as a value for explicit control over the type.
    Returns ``None`` for an empty mapping (no values to ship).
    """
    if not mapping:
        return None
    columns: dict[str, pa.Array[Any]] = {}
    for key, value in mapping.items():
        columns[key] = value if isinstance(value, pa.Array) else pa.array([value])
    return pa.RecordBatch.from_pydict(columns)


@dataclass(frozen=True, slots=True, kw_only=True)
class SecretLookupResponse(ArrowSerializableDataclass):
    """Response for :meth:`VgiSecretProtocol.secret_lookup`.

    ``values`` is the secret's key→value map carried as a **one-row RecordBatch**
    (serialized to binary on the wire): each column is a secret key and its row-0
    cell is the value. This lets values be any Arrow/DuckDB type — string, int64,
    bool, struct, list, nested — not just strings. Build it with
    :func:`encode_secret_values`. The C++ side converts each cell to a typed
    DuckDB ``Value`` via the Arrow→DuckDB bridge. ``redact_keys`` lists the subset
    of keys whose values must be redacted by ``duckdb_secrets()`` — honor it or
    values leak.

    ``ttl_seconds`` is the server's suggested cache lifetime. ``expires_at_unix``
    is the *credential's own* hard expiry as a Unix timestamp (0 = no intrinsic
    expiry); the client caches for ``min(ttl_seconds, expires_at_unix - now)`` so
    a short-lived STS token is never served past its own expiry.

    When ``found`` is False every other field is empty/zero and the client caches
    a short-TTL negative entry.
    """

    found: bool
    secret_type: str = ""
    provider: str = ""
    name: str = ""
    scope: list[str] = field(default_factory=list)
    values: Annotated[pa.RecordBatch | None, ArrowType(pa.binary())] = None
    redact_keys: list[str] = field(default_factory=list)
    ttl_seconds: int = 0
    expires_at_unix: int = 0


# ---------------------------------------------------------------------------
# VGI Secret Protocol
# ---------------------------------------------------------------------------


class VgiSecretProtocol(Protocol):
    """Wire protocol for Orchard's standalone secret service.

    A single unary method, ``secret_lookup``. ``vgi_rpc.RpcServer(VgiSecretProtocol,
    impl)`` handles serialization, dispatching, and version enforcement exactly as
    it does for :class:`vgi.protocol.VgiProtocol`.

    Application protocol surface version
    ------------------------------------
    ``protocol_version`` is the canonical semver (MAJOR.MINOR.PATCH) of this
    contract, **independent** of ``VgiProtocol.protocol_version``. The framework
    enforces an exact major+minor match (patch ignored) at the dispatch boundary.
    The C++ extension reads ``VGI_SECRET_PROTOCOL_VERSION`` from
    ``vgi/src/generated/vgi_secret_protocol_version.hpp`` (generated; sibling of
    ``vgi_protocol_version.hpp``) and passes it as a per-call
    ``protocol_version_override`` so it never collides with the worker protocol's
    global version constant.

    Bump rules mirror :class:`vgi.protocol.VgiProtocol`: major for any
    backwards-incompatible change, minor for additive, patch for worker-side fixes.
    """

    protocol_version: ClassVar[str] = "1.0.0"

    def secret_lookup(self, path: str, type: str) -> SecretLookupResponse:  # noqa: A002
        """Resolve the credential for ``path`` of secret ``type``.

        ``type`` is the lowercased DuckDB secret type the consumer probed for
        (``s3`` / ``r2`` / ``gcs`` / ``aws`` / ``http`` / …). Identity comes from
        the OAuth bearer on the transport. Return ``SecretLookupResponse(found=False)``
        when the account has no matching credential.
        """
        ...
