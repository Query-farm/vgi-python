# Copyright 2025, 2026 Query Farm LLC - https://query.farm

r"""Emit ``vgi_secret_protocol_version.hpp`` — the C++ view of VgiSecretProtocol.protocol_version.

Sibling of :mod:`vgi.codegen.cpp_protocol_version`: identical rendering, but it
sources the version from :class:`vgi.secret_protocol.VgiSecretProtocol` and emits
a distinct constant, ``VGI_SECRET_PROTOCOL_VERSION``. The secret protocol is
versioned independently of the worker/catalog protocol; the C++ extension passes
this value as a per-call ``protocol_version_override`` on secret RPCs so it never
collides with the global ``VGI_PROTOCOL_VERSION``.

Workflow:

    uv run --project ~/Development/vgi-python python -m vgi.codegen.cpp_secret_protocol_version \
        > ~/Development/vgi/src/generated/vgi_secret_protocol_version.hpp

``tests/test_generated_cpp_secret_protocol_version.py`` enforces drift.
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

from vgi.codegen.cpp_protocol_version import emit_version_header
from vgi.secret_protocol import VgiSecretProtocol

if TYPE_CHECKING:
    from typing import TextIO


def current_secret_protocol_version() -> str:
    """Return the canonical protocol_version declared on VgiSecretProtocol.

    Reads via ``vars()`` (not ``getattr``) to match runtime framework semantics —
    a subclass that doesn't redeclare must not silently leak this value.
    """
    value = vars(VgiSecretProtocol).get("protocol_version")
    if not isinstance(value, str):
        raise TypeError(
            f"VgiSecretProtocol.protocol_version must be a str declared as a ClassVar; "
            f"got {type(value).__name__}"
        )
    return value


def emit(out: TextIO) -> None:
    """Emit ``vgi_secret_protocol_version.hpp`` to *out*."""
    emit_version_header(
        out,
        current_secret_protocol_version(),
        constant_name="VGI_SECRET_PROTOCOL_VERSION",
        source_description="VgiSecretProtocol",
        generator_module="vgi.codegen.cpp_secret_protocol_version",
        generator_command="python -m vgi.codegen.cpp_secret_protocol_version",
        regen_command_lines=[
            "uv run --project ~/Development/vgi-python python -m vgi.codegen.cpp_secret_protocol_version \\",
            "  > ~/Development/vgi/src/generated/vgi_secret_protocol_version.hpp",
        ],
    )


def main() -> None:
    """Console-script entry point."""
    emit(sys.stdout)


if __name__ == "__main__":
    main()
