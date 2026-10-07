# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""The generated ``vgi.v2`` registries reproduce the reference protocol hash.

One parametrization runs over every backend of the registry generator
(:data:`vgi.codegen._registry_backend.REGISTRY_BACKENDS`). The meaningful
check is not "the generator read the model" but "what it *rendered* derives
back to the reference": each backend's ``derive`` reads its rendered file the
way that SDK's vgi-rpc port does (reflection over signatures for Java and C#,
struct tags for Go, the registered schema values for TypeScript, Rust and
C++), and the rebuilt method list must hash to the live ``VgiProtocol``
digest -- computed here, never a pinned constant, so a protocol change moves
both sides. Each backend's ``tamper`` proves the derivation reads the text.

Drift: each SDK's checked-in file must equal what its backend emits now
(skipped when the sibling checkout is absent; each backend's ``root_env``,
e.g. ``VGI_TYPESCRIPT_ROOT``, overrides the location).
"""

from __future__ import annotations

import os
import re
from pathlib import Path

import pyarrow as pa
import pytest
from vgi_rpc.rpc._protocol_hash import compute_protocol_hash
from vgi_rpc.rpc._types import rpc_methods

from vgi.codegen import cpp_registry, rust_registry, ts_registry
from vgi.codegen._common import GeneratorError
from vgi.codegen._registry import (
    MethodKind,
    camel,
    identifiers,
    preimage_hash,
    protocol_name,
    registry_methods,
)
from vgi.codegen._registry_backend import RegistryBackend, registry_backends
from vgi.protocol import VgiProtocol

_REGEN_HINT = (
    "To regenerate, run:\n"
    "  uv run --project ~/Development/vgi-python python scripts/regen_generated.py\n"
    "\n"
    "Do not redirect a generator into its destination with `>`: the shell\n"
    "truncates the file before the generator runs, so any failure destroys\n"
    "the checked-in artifact. The script renders to memory first."
)

_BACKENDS = registry_backends()
_IDS = [b.key for b in _BACKENDS]


def _live_hash() -> str:
    return str(compute_protocol_hash(protocol_name(), rpc_methods(VgiProtocol)))


def test_model_reproduces_the_live_hash() -> None:
    """The language-neutral model is lossless: its preimage is the reference's."""
    assert preimage_hash(protocol_name(), registry_methods()) == _live_hash()


@pytest.mark.parametrize("backend", _BACKENDS, ids=_IDS)
def test_registry_derives_the_live_hash(backend: RegistryBackend) -> None:
    """The rendered registry, read back as the SDK's vgi-rpc port reads it, is the reference vgi.v2."""
    derived = backend.derived_methods()
    assert sorted(m.name for m in derived) == sorted(m.name for m in registry_methods()), (
        f"{backend.language}: rendered method set differs from vgi.v2"
    )
    assert preimage_hash(protocol_name(), derived) == _live_hash()


@pytest.mark.parametrize("backend", _BACKENDS, ids=_IDS)
def test_tampered_rendering_changes_the_derived_hash(backend: RegistryBackend) -> None:
    """Derivation reads the rendered text: the backend's one-edit tamper moves the digest."""
    text = backend.render()
    tampered = backend.tamper.apply(text)
    assert tampered != text
    assert preimage_hash(protocol_name(), backend.derived_methods(tampered)) != _live_hash()


@pytest.mark.parametrize("backend", _BACKENDS, ids=_IDS)
def test_generator_is_deterministic(backend: RegistryBackend) -> None:
    """Rendering twice produces byte-identical output."""
    assert backend.render() == backend.render()


def _root(backend: RegistryBackend) -> Path:
    override = os.environ.get(backend.root_env)
    if override:
        return Path(override)
    candidates = [Path(__file__).resolve().parents[2] / backend.repo, Path.home() / backend.repo]
    for candidate in candidates:
        if (candidate / backend.target).parent.is_dir():
            return candidate
    return candidates[0]


@pytest.mark.parametrize("backend", _BACKENDS, ids=_IDS)
def test_checked_in_registry_matches_generator(backend: RegistryBackend) -> None:
    """Drift check: the checked-in registry matches what the generator produces."""
    path = _root(backend) / backend.target
    if not path.parent.is_dir():
        pytest.skip(f"{path.parent} not found; set {backend.root_env} or check out {backend.repo}")
    assert path.exists(), f"{path} is missing.\n{_REGEN_HINT}"
    assert path.read_text() == backend.render(), f"{path} is stale.\n{_REGEN_HINT}"


# ---------------------------------------------------------------------------
# The shared core
# ---------------------------------------------------------------------------


def test_backend_keys_and_targets_are_unique() -> None:
    """Each backend has its own id and its own (checkout, target)."""
    assert len(set(_IDS)) == len(_IDS)
    assert len({(b.repo, b.target) for b in _BACKENDS}) == len(_BACKENDS)


def test_identifier_collisions_are_refused() -> None:
    """Two wire names that map to one identifier fail generation."""
    with pytest.raises(GeneratorError, match="both map to the VgiService key fooBar"):
        identifiers(["foo_bar", "foo__bar"], lambda n: camel(n.replace("__", "_")), what="VgiService key")


def test_method_kinds_cover_the_protocol() -> None:
    """Every kind occurs, and a kind agrees with the method type and result."""
    assert {m.kind for m in registry_methods()} == set(MethodKind)
    for m in registry_methods():
        assert (m.kind is MethodKind.STREAM) == m.is_stream
        assert (m.kind is MethodKind.UNARY) == (m.result_field is not None)


def test_a_dropped_registration_changes_the_derived_method_set() -> None:
    """A table row missing (the hand-written registries' old failure) is caught before the hash."""
    text = cpp_registry.render()
    row = text.index('    {.name = "catalog_index_drop",')
    end = text.index("    {.name = ", row + 1)
    derived = cpp_registry.BACKEND.derived_methods(text[:row] + text[end:])
    assert "catalog_index_drop" not in {m.name for m in derived}
    assert preimage_hash(protocol_name(), derived) != _live_hash()


# ---------------------------------------------------------------------------
# Language-specific guarantees
# ---------------------------------------------------------------------------


def test_typescript_rejects_an_unmapped_arrow_type() -> None:
    """A column type with no TypeScript mapping fails generation instead of emitting ``any``."""
    with pytest.raises(GeneratorError, match="no TypeScript mapping"):
        ts_registry._ts_value_type(pa.float16(), "probe")


def test_cpp_registry_payloads_are_the_result_records() -> None:
    """A row's payload is its result record's schema, and only a record result has one."""
    known = cpp_registry.factories(cpp_registry.protocol_schemas_text(cpp_registry.BACKEND.namespace))
    rows = cpp_registry.rows(cpp_registry.render())
    for m in registry_methods():
        payload = rows[m.name].get("payload")
        if m.result_record is not None:
            assert payload is not None, f"{m.name} has no payload"
            assert known[payload].equals(m.result_annotation.ARROW_SCHEMA), m.name  # type: ignore[attr-defined]
        else:
            assert payload is None, f"{m.name} returns raw bytes but declares {payload}"


def test_rust_streams_register_a_state_decoder() -> None:
    """Every method that needs a state decoder gets the HTTP continuation hook, and only those."""
    text = rust_registry.render()
    for m in registry_methods():
        assert (f"fn decode_{m.name}_state(" in text) == m.needs_state_decoder, m.name
        assert bool(re.search(rf"d\.decode_{m.name}_state\(state\)", text)) == m.needs_state_decoder, m.name
