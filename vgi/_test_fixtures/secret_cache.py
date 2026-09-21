# Copyright 2026 Query Farm LLC - https://query.farm

"""Cacheable fixtures whose results depend on a secret.

The C++ result cache keys a secret-dependent result on a fingerprint of the
secrets its bind resolved (never their values), so a result is reused while the
secret is unchanged and recomputed the moment it is rotated, re-scoped or
dropped. Each fixture here reads the ``vgi_example`` secret's ``secret_string``
and advertises cacheability, one per cache path the fingerprint has to reach:

* ``secret_cache_nonce()`` — producer table function; the secret is declared in
  ``Meta.required_secrets``. Also exposed as the ``data.secret_cache_nonce``
  table with ``inline_bind=True``, which covers the client's inline-bind path
  (no bind RPC; the secrets are resolved client-side for init).
* ``secret_cached_scalar(x)`` — scalar, per-value memoized; secret declared via
  a ``Secret()`` annotation.
* ``secret_cached_lateral(x)`` — blended map, per-value memoized, called under
  ``LATERAL``; the secret is requested in ``on_bind`` (the two-phase bind).

Every output carries a ``nonce`` minted only when the worker really runs. It is
random rather than a counter because a pooled worker may run several processes,
and a per-process counter can repeat across them: equal nonces prove a cache
HIT, different ones a MISS, on any pool size.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Annotated, Any, ClassVar, cast

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi.arguments import Arg, OutputLength, Param, Returns, Secret, SecretLookupEntry
from vgi.cache_control import CacheControl
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.scalar_function import ScalarFunction
from vgi.schema_utils import schema
from vgi.table_function import (
    BindParams,
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)
from vgi.table_in_out_function import RowTransformFunction

if TYPE_CHECKING:
    from vgi.protocol import VgiOutputCollector

#: The secret type every fixture here reads. Registered by the C++ extension's
#: test build (``CREATE SECRET ... (TYPE vgi_example, secret_string '...')``).
SECRET_TYPE = "vgi_example"

#: Long enough that TTL never lapses mid-test.
_TTL_SECONDS = 300


def _nonce() -> int:
    """A value unique to this invocation, across every process in a worker pool."""
    return int.from_bytes(os.urandom(7), "big")


def _secret_string(secret: dict[str, Any]) -> str | None:
    """The ``secret_string`` field of a resolved secret, or None when there is none."""
    value = secret.get("secret_string")
    if value is None:
        return None
    return str(value.as_py()) if isinstance(value, pa.Scalar) else str(value)


# ---------------------------------------------------------------------------
# secret_cache_nonce — producer
# ---------------------------------------------------------------------------
@dataclass(slots=True, frozen=True)
class SecretCacheNonceArgs:
    """Arguments for SecretCacheNonceFunction (none)."""


@dataclass(kw_only=True)
class _SecretCacheNonceState(ArrowSerializableDataclass):
    """The one row to emit, minted on a real invocation."""

    secret_string: str | None
    nonce: int
    done: bool = False


@init_single_worker
@bind_fixed_schema
class SecretCacheNonceFunction(TableFunctionGenerator[SecretCacheNonceArgs, _SecretCacheNonceState]):
    """One row: the secret's ``secret_string`` and a per-invocation nonce; cacheable.

    ``initial_state`` runs only on a cache MISS, so the nonce is stable across
    HITs. A rotated secret must MISS and report the new value; restoring the
    original secret must HIT the entry it produced.
    """

    class Meta:
        """Metadata for SecretCacheNonceFunction."""

        name = "secret_cache_nonce"
        description = "One row with a secret's value and a per-invocation nonce; cacheable per secret"
        categories = ["generator", "cache", "secret", "testing"]
        required_secrets = [SecretLookupEntry(secret_type=SECRET_TYPE)]
        examples = [
            FunctionExample(
                sql="SELECT * FROM secret_cache_nonce()",
                description="The nonce is stable while the vgi_example secret is unchanged",
            ),
        ]

    FunctionArguments = SecretCacheNonceArgs
    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(secret_string=pa.string(), nonce=pa.int64())

    @classmethod
    def initial_state(cls, params: ProcessParams[SecretCacheNonceArgs]) -> _SecretCacheNonceState:
        """Read the secret and mint a nonce for this (real) invocation."""
        secret = next(iter(params.secrets.of_type(SECRET_TYPE)), {})
        return _SecretCacheNonceState(secret_string=_secret_string(secret), nonce=_nonce())

    @classmethod
    def process(
        cls,
        params: ProcessParams[SecretCacheNonceArgs],
        state: _SecretCacheNonceState,
        out: OutputCollector,
    ) -> None:
        """Emit the single row once, advertising a cache TTL."""
        if state.done:
            out.finish()
            return
        batch = pa.RecordBatch.from_pydict(
            {"secret_string": [state.secret_string], "nonce": [state.nonce]},
            schema=params.output_schema,
        )
        cast("VgiOutputCollector", out).emit(batch, cache_control=CacheControl(ttl=_TTL_SECONDS))
        state.done = True


# ---------------------------------------------------------------------------
# secret_cached_scalar — scalar, per-value
# ---------------------------------------------------------------------------
class SecretCachedScalarFunction(ScalarFunction):
    """``x`` -> ``'<secret_string>|<nonce>'``, memoized per value per secret.

    With no secret resolved the label is ``'|<nonce>'``.

    One nonce per ``compute`` call, shared by the batch, so a served value keeps
    the nonce of the call that produced it. ``per_value`` is a test choice, as on
    ``cached_double_scalar``: the point is coverage of the tier, not economics.
    """

    CACHE_CONTROL = CacheControl(ttl=_TTL_SECONDS, per_value=True)

    class Meta:
        """Function metadata."""

        name = "secret_cached_scalar"
        description = "Returns '<secret_string>|<nonce>' per value; memoized per value per secret"
        examples = [
            FunctionExample(
                sql="SELECT secret_cached_scalar(1)",
                description="Stable while the vgi_example secret is unchanged",
            ),
        ]

    @classmethod
    def compute(
        cls,
        value: Annotated[pa.Int64Array, Param(doc="Any value; the output ignores it")],
        _length: Annotated[int, OutputLength()],
        vgi_example: Annotated[dict[str, pa.Scalar[Any]] | None, Secret(SECRET_TYPE)] = None,
    ) -> Annotated[pa.StringArray, Returns(pa.string())]:
        """Label every row with the secret's value ('' when none) and this call's nonce.

        The framework omits a ``Secret()`` argument when no such secret exists,
        hence the default: a dropped secret is a state this fixture must serve.
        """
        label = f"{_secret_string(vgi_example or {}) or ''}|{_nonce()}"
        return pa.array([label] * _length, type=pa.string())


# ---------------------------------------------------------------------------
# secret_cached_lateral — blended map, per-value, two-phase secret
# ---------------------------------------------------------------------------
@dataclass(slots=True, frozen=True, kw_only=True)
class _SecretCachedLateralArgs:
    """One positional input column; the output ignores its value."""

    x: Annotated[int, Arg(0, doc="Input column")]


class SecretCachedLateralFunction(RowTransformFunction[_SecretCachedLateralArgs]):
    """1->1 map emitting the secret's ``secret_string`` and a per-call nonce.

    Requests the secret from ``on_bind`` — the two-phase bind, so the secret is
    discovered at bind time rather than declared — and advertises ``per_value``
    so a correlated ``LATERAL`` call is memoized per input value per secret.
    """

    class Meta:
        """Function metadata."""

        name = "secret_cached_lateral"
        description = "Blended map emitting a secret's value and a per-call nonce; memoized per secret"
        categories = ["blended", "cache", "secret", "test"]

    @classmethod
    def on_bind(cls, params: BindParams[_SecretCachedLateralArgs]) -> BindResponse:
        """Request the secret (two-phase) and declare the output columns."""
        params.secrets.get(SECRET_TYPE)
        return BindResponse(output_schema=schema(secret_string=pa.string(), nonce=pa.int64()))

    @classmethod
    def process(
        cls,
        params: ProcessParams[_SecretCachedLateralArgs],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        """Emit one row per input row, all carrying this call's nonce."""
        secret = next(iter(params.secrets.of_type(SECRET_TYPE)), {})
        rows = batch.num_rows
        cast("VgiOutputCollector", out).emit(
            pa.record_batch(
                {
                    "secret_string": pa.array([_secret_string(secret)] * rows, type=pa.string()),
                    "nonce": pa.array([_nonce()] * rows, type=pa.int64()),
                }
            ),
            cache_control=CacheControl(ttl=_TTL_SECONDS, per_value=True),
        )
