# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Result-cache control metadata (``vgi.cache.*``).

A table function can advertise that its result is cacheable by the client
(the DuckDB extension) by attaching ``vgi.cache.*`` metadata to the **first**
data batch it emits. The vocabulary mirrors HTTP caching (RFC 9111/9110): a
freshness lifetime (``ttl``/``expires``), a reuse ``scope``, validators
(ETag / Last-Modified) for conditional revalidation, and stale-serving grace
windows.

The key strings are the single source of truth shared with the C++ extension
(which reads them by string). :class:`CacheControl` renders a set of these
fields to the ``dict[str, str]`` of ``vgi.cache.*`` keys that rides on batch
``custom_metadata``.

Authors advertise cacheability either by passing a :class:`CacheControl` on
the first ``out.emit(...)`` call::

    from vgi.cache_control import CacheControl

    out.emit(first_batch, cache_control=CacheControl(ttl=300))

or by passing the rendered keys directly via the ``metadata`` kwarg::

    out.emit(first_batch, metadata={"vgi.cache.ttl": "300"})

Booleans render as ``"1"`` (present) and are omitted when false; timestamps
are RFC 3339 UTC strings; durations are integer seconds.
"""

from __future__ import annotations

from dataclasses import dataclass

__all__ = [
    "CACHE_ETAG_KEY",
    "CACHE_EXPIRES_KEY",
    "CACHE_LAST_MODIFIED_KEY",
    "CACHE_NOT_MODIFIED_KEY",
    "CACHE_NO_STORE_KEY",
    "CACHE_PARTITION_SCOPE_KEY",
    "CACHE_PER_VALUE_KEY",
    "CACHE_REVALIDATABLE_KEY",
    "CACHE_SCOPE_KEY",
    "CACHE_STALE_IF_ERROR_KEY",
    "CACHE_STALE_WHILE_REVALIDATE_KEY",
    "CACHE_TTL_KEY",
    "CACHE_SCOPE_CATALOG",
    "CACHE_SCOPE_TRANSACTION",
    "CacheControl",
]

# --- Response-side metadata keys (worker -> client) ------------------------
# Defined once here; the C++ extension reads these exact strings.
CACHE_TTL_KEY = "vgi.cache.ttl"
CACHE_EXPIRES_KEY = "vgi.cache.expires"
CACHE_NO_STORE_KEY = "vgi.cache.no_store"
CACHE_SCOPE_KEY = "vgi.cache.scope"
CACHE_ETAG_KEY = "vgi.cache.etag"
CACHE_LAST_MODIFIED_KEY = "vgi.cache.last_modified"
CACHE_REVALIDATABLE_KEY = "vgi.cache.revalidatable"
CACHE_STALE_WHILE_REVALIDATE_KEY = "vgi.cache.stale_while_revalidate"
CACHE_STALE_IF_ERROR_KEY = "vgi.cache.stale_if_error"
CACHE_NOT_MODIFIED_KEY = "vgi.cache.not_modified"
CACHE_PARTITION_SCOPE_KEY = "vgi.cache.partition_scope"
CACHE_PER_VALUE_KEY = "vgi.cache.per_value"

# --- Reuse-scope values ----------------------------------------------------
CACHE_SCOPE_CATALOG = "catalog"
CACHE_SCOPE_TRANSACTION = "transaction"
_VALID_SCOPES = frozenset({CACHE_SCOPE_CATALOG, CACHE_SCOPE_TRANSACTION})


@dataclass(frozen=True, slots=True, kw_only=True)
class CacheControl:
    """Cacheability advertised by a table function on its first result batch.

    Presence of ``ttl`` **or** ``expires`` is what makes a result cacheable;
    ``no_store`` overrides any freshness key. All fields are optional except
    ``scope`` (which defaults to ``catalog``).

    Attributes:
        ttl: Freshness lifetime in whole seconds, relative to full-result
            receipt (skew-immune; wins over ``expires``).
        expires: Absolute RFC 3339 UTC deadline. Lifetime is
            ``expires - now`` at receipt.
        scope: Reuse scope — ``"catalog"`` (default; reusable across
            transactions within the calling catalog identity) or
            ``"transaction"`` (reused only within the same transaction).
        no_store: Explicit "never cache"; overrides any freshness key.
        etag: Strong validator (opaque quoted string) for conditional
            revalidation.
        last_modified: Weaker RFC 3339 UTC validator; fallback when no ETag.
        revalidatable: The worker can check freshness cheaply without
            recomputing; gates whether the client ever sends a conditional
            request.
        stale_while_revalidate: Grace window (seconds) to serve stale
            immediately while revalidating in the background.
        stale_if_error: Grace window (seconds) to serve stale if a
            revalidation RPC fails.
        not_modified: 304-equivalent — set on a 0-row batch in reply to a
            conditional request to assert the client's stored payload is still
            fresh (the client reuses it instead of re-streaming).
        partition_scope: Opt in to per-partition caching. Only meaningful for a
            ``SINGLE_VALUE_PARTITIONS`` table function; the client ALSO caches
            the result split by partition value (one entry per distinct
            partition-value tuple) so a later ``=``/``IN``-filtered scan reuses
            per-partition entries. Additive to the whole-scan cache.
        per_value: Opt in to per-VALUE memoization. Only meaningful for an
            exchange-mode MAP (a scalar, or a blended table-in-out called via
            correlated ``LATERAL``); the client ALSO memoizes each distinct
            input tuple's output, so the same value serves without the worker
            on a later chunk or query. **Default off, and leave it off unless
            one call is genuinely expensive.** A per-value serve costs a cache
            probe, a decode and an assembly step per distinct value; that only
            pays back when it is cheaper than calling you. For an arithmetic
            map it is roughly 50x slower than just answering the call. Turn it
            on for model inference, geocoding, or a rate-limited remote fetch.
    """

    ttl: int | None = None
    expires: str | None = None
    scope: str = CACHE_SCOPE_CATALOG
    no_store: bool = False
    etag: str | None = None
    last_modified: str | None = None
    revalidatable: bool = False
    stale_while_revalidate: int | None = None
    stale_if_error: int | None = None
    not_modified: bool = False
    partition_scope: bool = False
    per_value: bool = False

    def __post_init__(self) -> None:
        """Validate scope and non-negative durations."""
        if self.scope not in _VALID_SCOPES:
            raise ValueError(f"CacheControl.scope must be one of {sorted(_VALID_SCOPES)}, got {self.scope!r}")
        for name in ("ttl", "stale_while_revalidate", "stale_if_error"):
            value = getattr(self, name)
            if value is not None and value < 0:
                raise ValueError(f"CacheControl.{name} must be >= 0, got {value}")

    def to_metadata(self) -> dict[str, str]:
        """Render to the ``dict[str, str]`` of ``vgi.cache.*`` batch-metadata keys.

        Booleans render as ``"1"`` and are omitted when false; unset optional
        fields are omitted entirely. ``scope`` is always emitted so the client
        never has to infer the default.
        """
        md: dict[str, str] = {}
        if self.ttl is not None:
            md[CACHE_TTL_KEY] = str(self.ttl)
        if self.expires is not None:
            md[CACHE_EXPIRES_KEY] = self.expires
        if self.no_store:
            md[CACHE_NO_STORE_KEY] = "1"
        md[CACHE_SCOPE_KEY] = self.scope
        if self.etag is not None:
            md[CACHE_ETAG_KEY] = self.etag
        if self.last_modified is not None:
            md[CACHE_LAST_MODIFIED_KEY] = self.last_modified
        if self.revalidatable:
            md[CACHE_REVALIDATABLE_KEY] = "1"
        if self.stale_while_revalidate is not None:
            md[CACHE_STALE_WHILE_REVALIDATE_KEY] = str(self.stale_while_revalidate)
        if self.stale_if_error is not None:
            md[CACHE_STALE_IF_ERROR_KEY] = str(self.stale_if_error)
        if self.not_modified:
            md[CACHE_NOT_MODIFIED_KEY] = "1"
        if self.partition_scope:
            md[CACHE_PARTITION_SCOPE_KEY] = "1"
        if self.per_value:
            md[CACHE_PER_VALUE_KEY] = "1"
        return md
