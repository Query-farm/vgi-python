# Copyright 2025, 2026 Query Farm LLC - https://query.farm

r"""Emit ``vgi_protocol_version.hpp`` — the C++ side's view of VgiProtocol.protocol_version.

Sibling of ``vgi.codegen.cpp_constants``: that module emits byte-key constants
sourced from ``vgi_rpc.metadata``; this one emits exactly one string constant
sourced from ``VgiProtocol.protocol_version``.

The two are deliberately separate generators. Byte-key constants are part of
the *wire framing* (vgi-rpc's concern); the protocol version is the
*application surface contract* (VgiProtocol's concern). Mixing them would
couple the generators across unrelated repos.

Workflow:

1. Bump ``VgiProtocol.protocol_version`` in ``vgi/protocol.py``.
2. ``uv run --project ~/Development/vgi-python python -m vgi.codegen.cpp_protocol_version \\
       > ~/Development/vgi/src/generated/vgi_protocol_version.hpp``.
3. Also regenerate ``vgi/protocol_version.txt`` (cross-language SoT for non-Python workers).
4. Commit both regenerated files together.

``tests/test_generated_cpp_protocol_version.py`` enforces drift detection at PR time.
"""

from __future__ import annotations

import io
import sys
from typing import TYPE_CHECKING

from vgi.codegen._common import provenance_comment
from vgi.codegen.protocol_version import current_protocol_version

if TYPE_CHECKING:
    from typing import TextIO


GENERATOR_VERSION = "1"


def emit(out: TextIO) -> None:
    """Emit ``vgi_protocol_version.hpp`` to *out*."""
    proto_version = current_protocol_version()
    # The value is canonical semver — ASCII-only by SEMVER_REGEX construction.
    # Reject anything else loudly so we never silently emit a malformed literal.
    if not all(0x20 <= ord(c) < 0x7F for c in proto_version):
        raise ValueError(f"non-printable byte in protocol_version {proto_version!r}; this is a bug")

    body = io.StringIO()
    body.write("#pragma once\n\n")
    body.write("#include <string_view>\n\n")
    body.write("namespace duckdb {\n")
    body.write("namespace vgi {\n")
    body.write("namespace generated {\n\n")
    body.write("// Application protocol surface version declared by VgiProtocol.\n")
    body.write("// Canonical semver MAJOR.MINOR.PATCH; emitted on every request batch's\n")
    body.write("// custom_metadata under `vgi_rpc.protocol_version` so the server can\n")
    body.write("// enforce an exact major+minor match at the dispatch boundary.\n")
    body.write("// Sourced from VgiProtocol.protocol_version (vgi-python).\n")
    body.write(f'inline constexpr std::string_view VGI_PROTOCOL_VERSION = "{proto_version}";\n\n')
    body.write("} // namespace generated\n")
    body.write("} // namespace vgi\n")
    body.write("} // namespace duckdb\n")

    out.write("// ============================================================================\n")
    out.write(
        provenance_comment(
            generator_module="vgi.codegen.cpp_protocol_version",
            generator_command="python -m vgi.codegen.cpp_protocol_version",
            generator_version=GENERATOR_VERSION,
            regen_command_lines=[
                "uv run --project ~/Development/vgi-python python -m vgi.codegen.cpp_protocol_version \\",
                "  > ~/Development/vgi/src/generated/vgi_protocol_version.hpp",
            ],
            body=body.getvalue(),
        )
    )
    out.write("// ============================================================================\n")
    out.write("\n")
    out.write(body.getvalue())


def main() -> None:
    """Console-script entry point."""
    emit(sys.stdout)


if __name__ == "__main__":
    main()
