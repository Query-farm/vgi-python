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
