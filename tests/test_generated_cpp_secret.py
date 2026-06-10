# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Drift + determinism tests for the VGI **secret** protocol C++ generators.

Covers the three sibling generators that target
:class:`vgi.secret_protocol.VgiSecretProtocol`:

- ``vgi.codegen.cpp_secret_protocol_version`` → ``vgi_secret_protocol_version.hpp``
- ``vgi.codegen.cpp_secret_schemas``          → ``vgi_secret_protocol_schemas.hpp``
- ``vgi.codegen.cpp_secret_request_builders``  → ``vgi_secret_request_builders.hpp``

Each test enforces that the checked-in header in the sibling ``vgi`` repo is
byte-for-byte what the generator emits right now, and that the generator is
deterministic. If the ``vgi`` repo isn't checked out next to ``vgi-python`` the
drift assertions skip; the determinism checks always run.
"""

from __future__ import annotations

import io
import os
from pathlib import Path

import pytest

from vgi.codegen import (
    cpp_secret_protocol_version,
    cpp_secret_request_builders,
    cpp_secret_schemas,
)

# (generator module, checked-in header filename, env override var)
_CASES = [
    (cpp_secret_protocol_version, "vgi_secret_protocol_version.hpp", "VGI_SECRET_VERSION_HPP"),
    (cpp_secret_schemas, "vgi_secret_protocol_schemas.hpp", "VGI_SECRET_SCHEMAS_HPP"),
    (cpp_secret_request_builders, "vgi_secret_request_builders.hpp", "VGI_SECRET_BUILDERS_HPP"),
]


def _generated_path(filename: str, override_var: str) -> Path:
    override = os.environ.get(override_var)
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[2] / "vgi" / "src" / "generated" / filename


def _emit(module) -> str:  # noqa: ANN001
    buf = io.StringIO()
    module.emit(buf)
    return buf.getvalue()


@pytest.mark.parametrize("module,filename,override_var", _CASES, ids=[c[1] for c in _CASES])
def test_generator_is_deterministic(module, filename: str, override_var: str) -> None:  # noqa: ANN001, ARG001
    """Running each generator twice produces byte-identical output."""
    assert _emit(module) == _emit(module), (
        f"{module.__name__} is non-deterministic — collection order is unstable"
    )


@pytest.mark.parametrize("module,filename,override_var", _CASES, ids=[c[1] for c in _CASES])
def test_checked_in_header_matches_generator(module, filename: str, override_var: str) -> None:  # noqa: ANN001
    """Each checked-in secret header must match current generator output exactly."""
    path = _generated_path(filename, override_var)
    if not path.exists():
        pytest.skip(f"{path} not found; set {override_var} or check out the vgi repo next to vgi-python")
    expected = _emit(module)
    actual = path.read_text()
    assert actual == expected, (
        f"{filename} is stale relative to {module.__name__}.\n"
        f"To regenerate, run:\n"
        f"  uv run --project ~/Development/vgi-python python -m {module.__name__} \\\n"
        f"    > ~/Development/vgi/src/generated/{filename}"
    )
