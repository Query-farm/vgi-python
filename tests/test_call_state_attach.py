# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""The stream's call state must not carry the attach in decrypted form.

``attach_opaque_data`` routinely carries catalog credentials. It travels
sealed, but serialized *state* does not stay sealed at rest: the access log
records the decrypted state at DEBUG as an audit artifact
(``vgi_rpc.rpc._server``), and on the init turn the recorded response state is
``call_state || cursor``. So a decrypted attach inside
:class:`~vgi.protocol.VgiCallState` would base64 credentials into the log —
precisely what ``loggable_attach_options`` exists to prevent elsewhere — and
would also sit decrypted in the transport's call-state cache.

The sealed envelope is already reachable at
``init_call.bind_call.attach_opaque_data``, so each turn re-opens that instead.
These tests pin both halves: that the plaintext never serializes, and that
re-opening actually works on turns after the first (the reason the decrypted
copy was there originally).
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Annotated, Any

import pyarrow as pa
import pytest
from vgi_rpc.rpc import AuthContext, OutputCollector, RpcServer
from vgi_rpc.rpc._common import _current_transport, _TransportContext
from vgi_rpc.utils import ArrowSerializableDataclass

from vgi.arguments import Arg, Arguments
from vgi.invocation import BindResponse, FunctionType, GlobalInitResponse
from vgi.protocol import (
    BindRequest,
    InitRequest,
    VgiCallState,
    VgiProtocol,
)
from vgi.table_function import (
    BindParams,
    ProcessParams,
    TableFunctionGenerator,
    init_single_worker,
)
from vgi.worker import Worker

_KEY = b"\x11" * 32
_SECRET = b"PGPASSWORD=hunter2"
_UUID = b"\xab" * 16
_DOMAIN = "test-domain"
_PRINCIPAL = "alice"

#: Every ``attach_opaque_data`` the probe saw, one entry per produce tick.
_SEEN: list[bytes | None] = []


@contextlib.contextmanager
def _as_principal(domain: str | None, principal: str | None) -> Iterator[None]:
    """Simulate the transport-set auth context for a single request."""
    auth = AuthContext(domain=domain, authenticated=principal is not None, principal=principal)
    token = _current_transport.set(_TransportContext(auth=auth))
    try:
        yield
    finally:
        _current_transport.reset(token)


@dataclass(kw_only=True)
class _ProbeState(ArrowSerializableDataclass):
    """Remaining tick count."""

    remaining: int = 0


@dataclass(frozen=True)
class _ProbeArgs:
    """Arguments for the probe."""

    count: Annotated[int, Arg(0, doc="ticks", ge=0)]


@init_single_worker
class _AttachProbeFunction(TableFunctionGenerator[_ProbeArgs, _ProbeState]):
    """Records the attach bytes visible on each produce tick.

    Attributes:
        FunctionArguments: Argument dataclass bound to this function.

    """

    FunctionArguments = _ProbeArgs

    class Meta:
        """Metadata for the probe."""

        name = "attach_probe"
        description = "records params.attach_opaque_data per tick"

    @classmethod
    def on_bind(cls, params: BindParams[_ProbeArgs]) -> BindResponse:
        """A single int64 column."""
        return BindResponse(output_schema=pa.schema([("n", pa.int64())]))

    @classmethod
    def initial_state(cls, params: ProcessParams[_ProbeArgs]) -> _ProbeState:
        """Start with the requested tick count."""
        return _ProbeState(remaining=params.args.count)

    @classmethod
    def process(cls, params: ProcessParams[_ProbeArgs], state: _ProbeState, out: OutputCollector) -> None:
        """Record the attach this turn sees, then emit one row."""
        _SEEN.append(bytes(params.attach_opaque_data) if params.attach_opaque_data is not None else None)
        if state.remaining <= 0:
            out.finish()
            return
        state.remaining -= 1
        out.emit(pa.RecordBatch.from_pydict({"n": [state.remaining]}, schema=params.output_schema))


class _ProbeWorker(Worker):
    """Worker exposing only the attach probe."""

    functions = [_AttachProbeFunction]


def _sealed_attach() -> bytes:
    """Seal ``uuid || secret`` for the test principal, as a real attach would be."""
    worker = _ProbeWorker(quiet=True)
    worker._signing_key = _KEY
    with _as_principal(_DOMAIN, _PRINCIPAL):
        return bytes(worker._seal_attach(_UUID + _SECRET))


class TestCallStateCarriesNoPlaintextAttach:
    """The serialized call state must never contain the decrypted attach."""

    def test_plaintext_attach_is_not_a_serialized_field(self) -> None:
        """``VgiCallState`` has no field holding the decrypted attach."""
        import dataclasses

        names = {f.name for f in dataclasses.fields(VgiCallState)}
        assert names == {"init_call", "init_response"}

    def test_secret_absent_from_serialized_call_state(self) -> None:
        """Serializing a call state must not emit the attach plaintext.

        The sealed envelope must still be reachable, since re-opening it is
        how each turn now obtains the plaintext.
        """
        sealed = _sealed_attach()
        bind = BindRequest(
            function_name="attach_probe",
            arguments=Arguments(positional=(pa.scalar(1),)),
            function_type=FunctionType.TABLE,
            input_schema=None,
            attach_opaque_data=sealed,
        )
        call_state = VgiCallState(
            init_call=InitRequest(bind_call=bind, output_schema=pa.schema([("n", pa.int64())])),
            init_response=GlobalInitResponse(execution_id=b"exec"),
        )
        raw = call_state.serialize_to_bytes()

        assert _SECRET not in raw, "decrypted attach must not reach the wire or the access log"
        assert _UUID not in raw, "the storage-sharding UUID is part of the same plaintext"
        assert sealed in raw, "the sealed envelope must remain, since turns re-open it"


class TestAttachSurvivesLaterTurns:
    """Re-opening the seal must work past the init turn, not only at init."""

    @pytest.fixture(autouse=True)
    def _reset(self) -> Iterator[None]:
        _SEEN.clear()
        yield
        _SEEN.clear()

    def test_every_turn_sees_the_attach(self) -> None:
        """A multi-turn HTTP stream sees the catalog bytes on every tick.

        This is the property the decrypted copy was there to guarantee: the
        original comment held that "the auth-scoped seal can't be reopened on
        a later, possibly different-auth, produce/finalize turn". Continuation
        turns are ordinary dispatches carrying the caller's identity, so it
        can — and a regression would show up here as ``None`` (or a raise)
        on every tick after the first.
        """
        from vgi_rpc.http import http_connect, make_sync_client

        sealed = _sealed_attach()

        def authenticate(req: Any) -> AuthContext:
            return AuthContext(domain=_DOMAIN, authenticated=True, principal=_PRINCIPAL)

        worker = _ProbeWorker(quiet=True)
        worker._signing_key = _KEY
        client = make_sync_client(
            RpcServer(VgiProtocol, worker, enable_describe=False),
            token_key=_KEY,
            authenticate=authenticate,
        )

        bind = BindRequest(
            function_name="attach_probe",
            arguments=Arguments(positional=(pa.scalar(4),)),
            function_type=FunctionType.TABLE,
            input_schema=None,
            attach_opaque_data=sealed,
        )
        with http_connect(VgiProtocol, client=client, compression_level=None) as proxy:  # type: ignore[type-abstract]
            resp = proxy.bind(request=bind)
            stream = proxy.init(
                request=InitRequest(
                    bind_call=bind,
                    output_schema=resp.output_schema,
                    bind_opaque_data=resp.opaque_data,
                )
            )
            rows = sum(ab.batch.num_rows for ab in stream)
            stream.close()

        assert rows == 4
        assert len(_SEEN) > 1, "need more than the init turn for this to prove anything"
        # The body sees the catalog bytes with the framework UUID stripped.
        assert all(seen == _SECRET for seen in _SEEN), _SEEN


class TestAttachUnwrapCache:
    """Memoizing the AEAD open must not become a way around the AEAD.

    Opening an attach is a pure function of (envelope, key, AAD), so repeats
    are cacheable — and worth caching, since a catalog stream re-opens the
    same envelope every turn. But the AAD is the *only* thing binding an
    envelope to a principal, and a cache hit skips it. Keyed on the envelope
    alone, anyone holding a stolen envelope would be handed its plaintext.
    """

    @staticmethod
    def _worker() -> _ProbeWorker:
        worker = _ProbeWorker(quiet=True)
        worker._signing_key = _KEY
        return worker

    @staticmethod
    def _auth(principal: str | None) -> AuthContext:
        return AuthContext(domain=_DOMAIN, authenticated=principal is not None, principal=principal)

    def test_same_principal_hits_and_matches(self) -> None:
        """A warm unwrap returns exactly what the cold one did."""
        worker = self._worker()
        sealed = _sealed_attach()
        alice = self._auth(_PRINCIPAL)

        cold = worker._unwrap_attach_full_with_auth(sealed, alice)
        warm = worker._unwrap_attach_full_with_auth(sealed, alice)
        assert cold == _UUID + _SECRET
        assert warm == cold

    def test_other_principal_rejected_even_after_cache_is_warm(self) -> None:
        """A second principal replaying the envelope must not get a hit.

        This is the regression that matters: warming on behalf of one
        principal must not turn the envelope into a bearer token for the next.
        """
        worker = self._worker()
        sealed = _sealed_attach()

        assert worker._unwrap_attach_full_with_auth(sealed, self._auth(_PRINCIPAL)) == _UUID + _SECRET
        with pytest.raises(ValueError):
            worker._unwrap_attach_full_with_auth(sealed, self._auth("bob"))

    def test_anonymous_rejected_even_after_cache_is_warm(self) -> None:
        """An unauthenticated caller gets no hit on an authenticated entry."""
        worker = self._worker()
        sealed = _sealed_attach()

        assert worker._unwrap_attach_full_with_auth(sealed, self._auth(_PRINCIPAL)) == _UUID + _SECRET
        with pytest.raises(ValueError):
            worker._unwrap_attach_full_with_auth(sealed, self._auth(None))

    def test_rejection_is_not_cached_as_a_hit(self) -> None:
        """A failed open must not poison the entry for the rightful principal."""
        worker = self._worker()
        sealed = _sealed_attach()

        with pytest.raises(ValueError):
            worker._unwrap_attach_full_with_auth(sealed, self._auth("bob"))
        assert worker._unwrap_attach_full_with_auth(sealed, self._auth(_PRINCIPAL)) == _UUID + _SECRET

    def test_cache_is_bounded(self) -> None:
        """The map cannot grow without limit as attachments come and go."""
        from vgi.worker import _AttachUnwrapCache

        cache = _AttachUnwrapCache(max_size=4)
        for i in range(20):
            cache.put((bytes([i]), b"id"), bytes([i]))
        assert len(cache._entries) == 4
        # Least-recently-used entries evicted; the newest survive.
        assert cache.get((bytes([19]), b"id")) == bytes([19])
        assert cache.get((bytes([0]), b"id")) is None


class TestMetaWorkerCanOpenTheAttach:
    """A MetaWorker must satisfy the same rehydrate contract a Worker does.

    Since the stream state stopped carrying the attach decrypted, the rehydrate
    path re-opens the sealed envelope on whatever the transport hands it as the
    implementation. Under a MetaWorker that object is *not* a ``Worker`` —
    MetaWorker does not subclass it, it proxies — so the method has to exist
    there too. It did not, and every HTTP continuation of a catalog-backed
    stream died on ``'MetaWorker' object has no attribute
    '_unwrap_attach_full'``. Unit coverage missed it because nothing here drove
    a MetaWorker over HTTP; the integration suite caught it.
    """

    @staticmethod
    def _meta_and_worker() -> tuple[Any, Any]:
        from vgi._test_fixtures.worker import ExampleWorker
        from vgi.meta_worker import MetaWorker

        worker = ExampleWorker(quiet=True)
        worker._signing_key = _KEY
        return MetaWorker([worker]), worker

    def test_meta_worker_exposes_the_rehydrate_hook(self) -> None:
        """The attribute the rehydrate path calls must exist on a MetaWorker."""
        meta, _ = self._meta_and_worker()
        assert hasattr(meta, "_unwrap_attach_full")

    def test_meta_worker_unwraps_identically_to_its_sub_worker(self) -> None:
        """Routing through the MetaWorker yields the sub-worker's plaintext."""
        meta, worker = self._meta_and_worker()
        with _as_principal(_DOMAIN, _PRINCIPAL):
            envelope = bytes(worker._seal_attach(_UUID + _SECRET))
            assert meta._unwrap_attach_full(envelope) == _UUID + _SECRET
            assert meta._unwrap_attach_full(envelope) == worker._unwrap_attach_full(envelope)

    def test_no_attach_passes_through(self) -> None:
        """No catalog context means nothing to open."""
        meta, _ = self._meta_and_worker()
        assert meta._unwrap_attach_full(None) is None
