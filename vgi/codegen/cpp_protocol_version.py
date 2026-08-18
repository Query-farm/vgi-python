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

import argparse
import io
import sys
from typing import TYPE_CHECKING

from vgi.codegen._common import (
    GeneratorError,
    DEFAULT_CPP_NAMESPACE,
    close_namespace,
    open_namespace,
    parse_cpp_namespace,
    provenance_comment,
)
from vgi.codegen.protocol_version import current_protocol_version

if TYPE_CHECKING:
    from typing import TextIO


GENERATOR_VERSION = "1"


def emit_version_header(
    out: TextIO,
    proto_version: str,
    *,
    constant_name: str,
    source_description: str,
    generator_module: str,
    generator_command: str,
    regen_command_lines: list[str],
    namespace: list[str] | None = None,
) -> None:
    """Render a one-constant ``std::string_view`` protocol-version header.

    Shared by the main protocol generator and the secret protocol generator.
    """
    # The value is canonical semver — ASCII-only by SEMVER_REGEX construction.
    # Reject anything else loudly so we never silently emit a malformed literal.
    if not all(0x20 <= ord(c) < 0x7F for c in proto_version):
        raise ValueError(f"non-printable byte in protocol_version {proto_version!r}; this is a bug")

    if namespace is None:
        namespace = parse_cpp_namespace(DEFAULT_CPP_NAMESPACE)
    body = io.StringIO()
    body.write("#pragma once\n\n")
    body.write("#include <string_view>\n\n")
    body.write(open_namespace(namespace))
    body.write("\n")
    body.write(f"// Application protocol surface version declared by {source_description}.\n")
    body.write("// Canonical semver MAJOR.MINOR.PATCH; emitted on every request batch's\n")
    body.write("// custom_metadata under `vgi_rpc.protocol_version` so the server can\n")
    body.write("// enforce an exact major+minor match at the dispatch boundary.\n")
    body.write(f"// Sourced from {source_description}.protocol_version (vgi-python).\n")
    body.write(f'inline constexpr std::string_view {constant_name} = "{proto_version}";\n\n')
    body.write(close_namespace(namespace))

    out.write("// ============================================================================\n")
    out.write(
        provenance_comment(
            generator_module=generator_module,
            generator_command=generator_command,
            generator_version=GENERATOR_VERSION,
            regen_command_lines=regen_command_lines,
            body=body.getvalue(),
        )
    )
    out.write("// ============================================================================\n")
    out.write("\n")
    out.write(body.getvalue())


def emit(out: TextIO, namespace: list[str] | None = None) -> None:
    """Emit ``vgi_protocol_version.hpp`` to *out*."""
    emit_version_header(
        out,
        current_protocol_version(),
        constant_name="VGI_PROTOCOL_VERSION",
        source_description="VgiProtocol",
        generator_module="vgi.codegen.cpp_protocol_version",
        generator_command="python -m vgi.codegen.cpp_protocol_version",
        regen_command_lines=[
            "uv run --project ~/Development/vgi-python python -m vgi.codegen.cpp_protocol_version \\",
            "  > ~/Development/vgi/src/generated/vgi_protocol_version.hpp",
        ],
        namespace=namespace,
    )


def main() -> None:
    """Console-script entry point."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--namespace",
        default=DEFAULT_CPP_NAMESPACE,
        help=(
            "C++ namespace to emit into, `::`-separated "
            f"(default: {DEFAULT_CPP_NAMESPACE}). VGI is not DuckDB-only: a "
            "standalone worker SDK wants something like `vgi::generated`."
        ),
    )
    args = parser.parse_args()
    try:
        namespace = parse_cpp_namespace(args.namespace)
    except GeneratorError as e:
        print(f"\nerror: {e}\n", file=sys.stderr)
        sys.exit(2)
    emit(sys.stdout, namespace)


if __name__ == "__main__":
    main()
