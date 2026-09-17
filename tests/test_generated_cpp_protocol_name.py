# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Drift test for ``vgi.codegen.cpp_protocol_name``.

This test exists because its absence already cost a wire break. The protocol
*versions* are generated and drift-guarded, so a version bump that misses the
C++ tree is a red build. The protocol *names* were hand-written on both sides,
so a rename was a silent misroute instead: every request the DuckDB extension
sent named a protocol the server no longer hosted, and nothing failed until
integration.

So the assertions below are not "the file looks plausible". They are, in order:
the generator agrees with the dispatcher about what the name IS, the name is
routable, and the checked-in header says the same thing.

Skipped when the ``vgi`` repo isn't checked out next to ``vgi-python``; the
other tests still run.
"""

from __future__ import annotations

import io
import os
import re
from pathlib import Path

import pytest
from vgi_rpc.rpc._types import _protocol_wire_name, validate_protocol_name

from vgi.codegen.cpp_protocol_name import (
    current_protocol_name,
    current_secret_protocol_name,
    emit,
)
from vgi.protocol import VgiProtocol
from vgi.secret_protocol import VgiSecretProtocol

_CONSTANTS = {
    "VGI_PROTOCOL_NAME": (VgiProtocol, current_protocol_name),
    "VGI_SECRET_PROTOCOL_NAME": (VgiSecretProtocol, current_secret_protocol_name),
}


def _vgi_generated_path() -> Path:
    """Locate ``vgi/src/generated/vgi_protocol_names.hpp``."""
    override = os.environ.get("VGI_GENERATED_PROTOCOL_NAMES_HPP")
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[2] / "vgi" / "src" / "generated" / "vgi_protocol_names.hpp"


_REGEN_HINT = (
    "To regenerate, run:\n"
    "  uv run --project ~/Development/vgi-python python scripts/regen_generated.py\n"
    "\n"
    "Do not redirect a generator into its destination with `>`: the shell\n"
    "truncates the file before the generator runs, so any failure destroys\n"
    "the checked-in artifact. The script renders to memory first."
)


def _emitted() -> str:
    out = io.StringIO()
    emit(out)
    return out.getvalue()


def _emitted_literal(constant: str) -> str:
    match = re.search(rf'inline constexpr std::string_view {constant} = "([^"]*)";', _emitted())
    assert match is not None, f"generator did not emit {constant}"
    return match.group(1)


def test_generator_is_deterministic() -> None:
    """Running the generator twice produces byte-identical output."""
    assert _emitted() == _emitted()


@pytest.mark.parametrize("constant", sorted(_CONSTANTS))
def test_emitted_value_matches_the_dispatcher(constant: str) -> None:
    """The emitted literal is what vgi-rpc will actually route on.

    Compared against ``_protocol_wire_name`` — the function the server itself
    uses — rather than against ``Protocol.protocol_name``. Those differ in a way
    that matters: the wire name is read from ``vars(protocol)``, so a Protocol
    that subclasses another and forgets to redeclare gets its own class name,
    NOT its parent's routing key. A test written against ``getattr`` would pass
    while the client addressed a protocol the server does not host.
    """
    protocol, _accessor = _CONSTANTS[constant]
    assert _emitted_literal(constant) == _protocol_wire_name(protocol)


@pytest.mark.parametrize("constant", sorted(_CONSTANTS))
def test_emitted_value_is_routable(constant: str) -> None:
    """A name the server would refuse to route must never become a client constant."""
    validate_protocol_name(_emitted_literal(constant))


def test_the_two_protocols_do_not_share_a_routing_key() -> None:
    """Distinct protocols must be distinctly addressable.

    They are co-hostable by construction, and dispatch resolves (protocol,
    method). Were both to answer to one key, a co-hosting server would route
    ``secret_lookup`` by whichever binding registered last.
    """
    assert current_protocol_name() != current_secret_protocol_name()


def test_checked_in_generated_hpp_matches_generator() -> None:
    """The .hpp checked into the vgi repo must match the current generator output."""
    path = _vgi_generated_path()
    if not path.exists():
        pytest.skip(
            f"{path} not found; set VGI_GENERATED_PROTOCOL_NAMES_HPP or check out the vgi repo next to vgi-python"
        )

    checked_in = path.read_text()
    expected = _emitted()

    if checked_in != expected:
        for i, (actual_ch, expected_ch) in enumerate(zip(checked_in, expected, strict=False)):
            if actual_ch != expected_ch:
                window_start = max(0, i - 40)
                window_end = i + 40
                raise AssertionError(
                    f"checked-in {path} differs from generator output at offset {i}.\n"
                    f"  checked-in: {checked_in[window_start:window_end]!r}\n"
                    f"  expected:   {expected[window_start:window_end]!r}\n"
                    f"{_REGEN_HINT}"
                )
        raise AssertionError(
            f"checked-in {path} is shorter/longer than generator output "
            f"({len(checked_in)} vs {len(expected)} chars)\n{_REGEN_HINT}"
        )
