"""Unit tests for ``vgi.table_buffering_function``.

Currently focused on the ``__init_subclass__`` machinery — specifically
TFinalizeState resolution under non-trivial generic inheritance chains.
"""

from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass

from vgi._test_fixtures.table_in_out import SingleTableArguments
from vgi.table_buffering_function import (
    TableBufferingFunction,
)


@dataclass(frozen=True, slots=True, kw_only=True)
class _StateA(ArrowSerializableDataclass):
    """A concrete finalize-state dataclass."""

    log_id: int = 0

    @classmethod
    def arrow_schema(cls) -> pa.Schema:
        """Wire layout for _StateA."""
        return pa.schema([pa.field("log_id", pa.int64())])


@dataclass(frozen=True, slots=True, kw_only=True)
class _StateB(ArrowSerializableDataclass):
    """A different concrete finalize-state dataclass."""

    cursor: bytes = b""

    @classmethod
    def arrow_schema(cls) -> pa.Schema:
        """Wire layout for _StateB."""
        return pa.schema([pa.field("cursor", pa.binary())])


# Stub implementations that satisfy the abstractmethod requirements without
# doing real work — these tests are purely about class-time resolution.

def _stub_process(cls, batch, params):  # noqa: ARG001 - test stub
    return b""


def _stub_combine(cls, state_ids, params):  # noqa: ARG001 - test stub
    return state_ids


def _stub_finalize(cls, params, finalize_state_id, state, out):  # noqa: ARG001 - test stub
    out.finish()


def _attach_stub_methods(cls: type) -> type:
    cls.process = classmethod(_stub_process)
    cls.combine = classmethod(_stub_combine)
    cls.finalize = classmethod(_stub_finalize)
    return cls


# ---------------------------------------------------------------------------
# Case 1: direct parameterization at the leaf — baseline that the old code
# already handled correctly. Kept as a regression net.
# ---------------------------------------------------------------------------

@_attach_stub_methods
class DirectChild(TableBufferingFunction[SingleTableArguments, _StateA]):
    """Baseline: direct generic parameterization at the leaf class."""

    class Meta:
        """Test fixture metadata."""

        name = "direct_child"


def test_direct_parameterization_resolves() -> None:
    """``Foo(TableBufferingFunction[Args, State])`` resolves State directly."""
    assert DirectChild._finalize_state_class is _StateA


# ---------------------------------------------------------------------------
# Case 2: subclass without re-parameterization — should inherit via the
# normal class-attribute lookup (we don't overwrite cls._finalize_state_class
# when no __orig_bases__ binding is found, returning the _UNCHANGED sentinel).
# ---------------------------------------------------------------------------

@_attach_stub_methods
class NonParameterizedGrandchild(DirectChild):
    """Subclass that doesn't re-parameterize — must inherit DirectChild's state."""

    class Meta:
        """Test fixture metadata."""

        name = "non_parameterized_grandchild"


def test_subclass_without_reparameterization_inherits() -> None:
    """``class Foo(DirectChild): ...`` inherits DirectChild._finalize_state_class."""
    assert NonParameterizedGrandchild._finalize_state_class is _StateA


# ---------------------------------------------------------------------------
# Case 3: generic-through intermediate — the bug the new MRO walk fixes.
#
#     class Mid[X](TableBufferingFunction[Args, X]): ...
#     class Concrete(Mid[State]):  ← state must resolve through Mid's binding
#
# The OLD resolution walked only Concrete.__orig_bases__ = (Mid[State],),
# saw origin=Mid (a TBF subclass, not TBF itself), tried type_args[1] (out
# of range, only one arg), bailed, and left _finalize_state_class as None.
# ---------------------------------------------------------------------------

@_attach_stub_methods
class GenericMid[X: ArrowSerializableDataclass](TableBufferingFunction[SingleTableArguments, X]):
    """Intermediate generic that passes State through unbound."""

    class Meta:
        """Test fixture metadata."""

        name = "generic_mid"


@_attach_stub_methods
class ConcreteFromGenericMid(GenericMid[_StateB]):
    """Leaf that binds the intermediate's TypeVar to a concrete dataclass."""

    class Meta:
        """Test fixture metadata."""

        name = "concrete_from_generic_mid"


def test_generic_through_intermediate_resolves() -> None:
    """``class Concrete(Mid[State])`` resolves State even when Mid is a TBF subclass.

    This is the S5 fix. With the old code, ``_finalize_state_class`` would
    have been ``None`` on ``ConcreteFromGenericMid``; with the MRO walk it
    correctly resolves to ``_StateB``.
    """
    assert ConcreteFromGenericMid._finalize_state_class is _StateB
    # The intermediate generic itself stays unresolved (its TypeVar is still
    # unbound from its own POV; only its leaf children bind it).
    assert GenericMid._finalize_state_class is None


# ---------------------------------------------------------------------------
# Case 4: two-level generic-through chain — substitutions propagate.
#
#     class Mid[X](TableBufferingFunction[Args, X]): ...
#     class Outer[Y](Mid[Y]): ...
#     class Leaf(Outer[State]):  ← Y → State, then X → Y → State.
# ---------------------------------------------------------------------------

@_attach_stub_methods
class GenericOuter[Y: ArrowSerializableDataclass](GenericMid[Y]):
    """Two-level generic-through; rebinds the intermediate's TypeVar to its own."""

    class Meta:
        """Test fixture metadata."""

        name = "generic_outer"


@_attach_stub_methods
class TwoLevelLeaf(GenericOuter[_StateA]):
    """Leaf binding the outer-level TypeVar; must resolve transitively."""

    class Meta:
        """Test fixture metadata."""

        name = "two_level_leaf"


def test_two_level_generic_through_chain_resolves() -> None:
    """Substitutions propagate across multi-level generic-through chains."""
    assert TwoLevelLeaf._finalize_state_class is _StateA
    assert GenericOuter._finalize_state_class is None


# ---------------------------------------------------------------------------
# Case 5: explicit ``None`` finalize state — preserved through the chain.
# ---------------------------------------------------------------------------

@_attach_stub_methods
class NoStateFunc(TableBufferingFunction[SingleTableArguments, None]):
    """Function that opts out of per-tick finalize state."""

    class Meta:
        """Test fixture metadata."""

        name = "no_state_func"


def test_explicit_none_state_resolves_to_none() -> None:
    """``TableBufferingFunction[Args, None]`` resolves to None (no per-tick state)."""
    assert NoStateFunc._finalize_state_class is None


# ===========================================================================
# TableBufferingFinalizeState.on_cancel wiring
#
# Until this landing the buffered finalize path inherited the no-op
# ``StreamState.on_cancel`` from the base, so ``cls.on_cancel(...)`` was
# dead code despite being a documented user-facing API. These tests
# directly drive the protocol-layer ``on_cancel`` and assert it routes
# through the user's classmethod, with the correct arguments and
# resilience to teardown-path failures.
#
# We test at the protocol layer rather than via SQL integration because
# the wire-level cancel is delivered asynchronously by VgiCancelDispatcher
# (one thread writes the cancel batch; the subprocess worker reads it on
# its own schedule), and racing that against a test assertion produces
# flaky tests. The contract that matters — "when the framework calls
# state.on_cancel, the user's classmethod runs with deserialized state"
# — is fully exercised here.
# ===========================================================================


@dataclass(frozen=True, slots=True, kw_only=True)
class _OnCancelProbeState(ArrowSerializableDataclass):
    """State carrying a serialized counter so we can verify deserialization."""

    emitted: int = 0

    @classmethod
    def arrow_schema(cls) -> pa.Schema:
        """Wire layout — single int64 column."""
        return pa.schema([pa.field("emitted", pa.int64())])


# Module-level capture buckets for the fixture's on_cancel to write into.
# Cleared per-test via the fixture decorator below.
_on_cancel_invocations: list[tuple[bytes, int]] = []


def _stub_finalize_emit_then_finish(cls, params, finalize_state_id, state, out):  # noqa: ARG001
    """Fixture-stand-in finalize — irrelevant for the cancel tests."""
    out.finish()


@_attach_stub_methods
class _OnCancelProbe(TableBufferingFunction[SingleTableArguments, _OnCancelProbeState]):
    """Records every on_cancel invocation into ``_on_cancel_invocations``.

    The deserialized ``state.emitted`` lets us prove the on_cancel path
    correctly read the wire-serialized state_blob (not a fresh state).
    """

    class Meta:
        """Test fixture metadata."""

        name = "on_cancel_probe"

    @classmethod
    def on_cancel(
        cls,
        params,  # noqa: ARG003 — protocol layer wires this; we don't inspect it
        finalize_state_id: bytes,
        state,
    ) -> None:
        """Append (finalize_state_id, state.emitted) so tests can assert."""
        emitted = state.emitted if state is not None else -1
        _on_cancel_invocations.append((finalize_state_id, emitted))


class _StubWorker:
    """Stand-in for vgi.worker.Worker — only needs _load_table_buffering_params.

    ``TableBufferingFinalizeState.on_cancel`` reads ``ctx.implementation``
    and calls ``._load_table_buffering_params(stub_request, ctx,
    attach_already_unwrapped=True)`` on it. We mock the return value to
    point at our probe fixture and a dummy params object.
    """

    def __init__(self, func_cls: type, params: object) -> None:
        self._func_cls = func_cls
        self._params = params

    def _load_table_buffering_params(  # noqa: ARG002 - signature mirrored
        self, request, ctx, *, attach_already_unwrapped: bool = False,
    ):
        return self._func_cls, self._params


class _StubCallContext:
    """Minimal CallContext stand-in: just ``implementation`` and ``auth``."""

    def __init__(self, implementation: object) -> None:
        self.implementation = implementation
        self.auth = None


def _build_finalize_state(
    *,
    state_initialized: bool,
    state: _OnCancelProbeState | None,
    finalize_state_id: bytes = b"fid-test",
) -> object:
    """Construct a TableBufferingFinalizeState wired for on_cancel testing."""
    from vgi.protocol import TableBufferingFinalizeState

    blob = state.serialize_to_bytes() if state is not None else b""
    return TableBufferingFinalizeState(
        function_name="on_cancel_probe",
        execution_id=b"exec-test",
        transaction_id=None,
        finalize_state_id=finalize_state_id,
        state_blob=blob,
        state_initialized=state_initialized,
        attach_opaque_data=b"",
    )


def test_on_cancel_invokes_user_classmethod_with_deserialized_state() -> None:
    """Happy path — state_initialized=True, blob deserializes, cls.on_cancel runs."""
    _on_cancel_invocations.clear()

    fstate = _build_finalize_state(
        state_initialized=True,
        state=_OnCancelProbeState(emitted=42),
    )
    ctx = _StubCallContext(implementation=_StubWorker(_OnCancelProbe, params=object()))

    fstate.on_cancel(ctx)

    assert _on_cancel_invocations == [(b"fid-test", 42)]


def test_on_cancel_skips_when_state_uninitialized() -> None:
    """Pre-init cancel — no user state to forward, so cls.on_cancel must not run.

    The protocol's on_cancel returns early when ``state_initialized`` is
    False. This is the "cancel arrived before initial_finalize_state ran"
    case — there's no user state worth synthesizing.
    """
    _on_cancel_invocations.clear()

    fstate = _build_finalize_state(state_initialized=False, state=None)
    ctx = _StubCallContext(implementation=_StubWorker(_OnCancelProbe, params=object()))

    fstate.on_cancel(ctx)

    assert _on_cancel_invocations == []


def test_on_cancel_passes_none_state_when_blob_empty() -> None:
    """State_initialized=True with empty blob → cls.on_cancel(..., state=None).

    Possible if the fixture's ``initial_finalize_state`` returned None;
    state_initialized flips True but state_blob stays empty. The user's
    on_cancel must tolerate a None state.
    """
    _on_cancel_invocations.clear()

    fstate = _build_finalize_state(state_initialized=True, state=None)
    ctx = _StubCallContext(implementation=_StubWorker(_OnCancelProbe, params=object()))

    fstate.on_cancel(ctx)

    # _OnCancelProbe.on_cancel records emitted=-1 for state is None — see
    # the fixture; this asserts the on_cancel WAS called with None.
    assert _on_cancel_invocations == [(b"fid-test", -1)]


def test_on_cancel_swallows_implementation_lookup_failures() -> None:
    """If ctx.implementation is None or load fails, on_cancel is a quiet no-op.

    teardown paths shouldn't crash. The framework's on_cancel hook is
    best-effort by design (vgi-rpc/_server.py catches and logs anything
    that escapes); we make sure nothing escapes in the first place.
    """
    _on_cancel_invocations.clear()

    fstate = _build_finalize_state(
        state_initialized=True,
        state=_OnCancelProbeState(emitted=7),
    )
    # implementation=None covers the run_table_buffering_finalize_tick
    # diagnostic path: produce() raises with a clear message, but
    # on_cancel must silently skip so we don't double-fault during
    # teardown.
    ctx = _StubCallContext(implementation=None)

    fstate.on_cancel(ctx)  # must not raise

    assert _on_cancel_invocations == []


def test_on_cancel_swallows_user_exceptions() -> None:
    """User's on_cancel raising must not propagate — we're on teardown.

    Without the contextlib.suppress in protocol.on_cancel, a user fixture
    that raises in on_cancel would propagate through the framework's
    own on_cancel hook (which already catches but only at the outer
    level), producing log noise during normal LIMIT teardown.
    """
    _on_cancel_invocations.clear()

    @_attach_stub_methods
    class _RaisingProbe(TableBufferingFunction[SingleTableArguments, _OnCancelProbeState]):
        class Meta:
            """Test fixture metadata."""

            name = "raising_probe"

        @classmethod
        def on_cancel(cls, params, finalize_state_id, state):  # noqa: ARG003
            raise RuntimeError("user fixture error during teardown")

    fstate = _build_finalize_state(
        state_initialized=True,
        state=_OnCancelProbeState(emitted=1),
    )
    ctx = _StubCallContext(implementation=_StubWorker(_RaisingProbe, params=object()))

    fstate.on_cancel(ctx)  # must not raise
