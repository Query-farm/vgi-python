"""Emit protocol byte-constants for the VGI C++ DuckDB extension.

Companion to ``vgi.codegen.cpp_schemas``. Where that module emits the
full Arrow ``Schema`` factories, this one emits the handful of byte
strings the C++ side needs to produce on the wire (Arrow custom_metadata
keys, mostly).

Single source of truth: ``vgi_rpc.metadata``. Hand-mirroring those bytes
in the C++ tree would silently drift the day someone renames a key.

### Multirepo workflow

Same as cpp_schemas:

1. Run ``uv run --project ~/Development/vgi-python vgi-gen-cpp-constants \
       > ~/Development/vgi/src/generated/vgi_protocol_constants.hpp``.
2. Commit the regenerated file in the ``vgi`` repo on the same branch.

``tests/test_generated_cpp_constants.py`` enforces that the checked-in
file matches what the generator would emit right now.
"""

from __future__ import annotations

import io
import sys
from typing import TYPE_CHECKING, NamedTuple

from vgi_rpc import metadata as _rpc_metadata

from vgi.codegen._common import provenance_comment

if TYPE_CHECKING:
    from typing import TextIO


GENERATOR_VERSION = "1"


class _ByteConstant(NamedTuple):
    """One C++-visible byte constant sourced from the Python vgi_rpc package."""

    cpp_name: str  # emitted as `inline constexpr std::string_view {cpp_name}`
    python_name: str  # key in vgi_rpc.metadata
    description: str  # one-liner for the doc comment


# Ordered list of constants to emit. Add new entries here, regenerate,
# commit; the drift test catches stale headers.
_CONSTANTS: list[_ByteConstant] = [
    _ByteConstant(
        cpp_name="VGI_RPC_STATE_KEY",
        python_name="STATE_KEY",
        description=(
            "Custom-metadata key carrying a base64-encoded stream-state token on HTTP exchange requests and responses."
        ),
    ),
    _ByteConstant(
        cpp_name="VGI_RPC_CANCEL_KEY",
        python_name="CANCEL_KEY",
        description=(
            "Custom-metadata key present on a zero-row input batch to "
            "signal the worker to cancel the current stream. The value "
            "is ignored (presence is the signal)."
        ),
    ),
]


def _as_escaped_cxx_string(value: bytes) -> str:
    """Render a bytes literal as a C++ escaped double-quoted string.

    Only produces printable ASCII (plus standard escapes) since every
    wire constant we emit is an ASCII key name.
    """
    out: list[str] = []
    for byte in value:
        if byte == ord('"'):
            out.append('\\"')
        elif byte == ord("\\"):
            out.append("\\\\")
        elif 0x20 <= byte < 0x7F:
            out.append(chr(byte))
        else:
            # Any non-printable byte is a mistake for a wire-key constant;
            # fail loudly rather than emit an \xNN that might be silently
            # mis-decoded by readers.
            raise ValueError(
                f"non-printable byte {byte:#x} in wire constant; vgi_protocol_constants is for ASCII keys only"
            )
    return '"' + "".join(out) + '"'


def emit(out: TextIO) -> None:
    """Emit the generated header to ``out``."""
    body = io.StringIO()
    body.write("#pragma once\n\n")
    body.write("#include <string_view>\n\n")
    body.write("namespace duckdb {\n")
    body.write("namespace vgi {\n")
    body.write("namespace generated {\n\n")

    for entry in _CONSTANTS:
        value = getattr(_rpc_metadata, entry.python_name)
        if not isinstance(value, bytes):
            raise TypeError(f"vgi_rpc.metadata.{entry.python_name!r} is {type(value).__name__}, expected bytes")
        for doc_line in entry.description.split("\n"):
            body.write(f"// {doc_line}\n")
        body.write(f"// Sourced from vgi_rpc.metadata.{entry.python_name}.\n")
        body.write(f"inline constexpr std::string_view {entry.cpp_name} = {_as_escaped_cxx_string(value)};\n\n")

    body.write("} // namespace generated\n")
    body.write("} // namespace vgi\n")
    body.write("} // namespace duckdb\n")

    out.write("// ============================================================================\n")
    out.write(
        provenance_comment(
            generator_module="vgi.codegen.cpp_constants",
            generator_command="vgi-gen-cpp-constants",
            generator_version=GENERATOR_VERSION,
            regen_command_lines=[
                "uv run --project ~/Development/vgi-python vgi-gen-cpp-constants \\",
                "  > ~/Development/vgi/src/generated/vgi_protocol_constants.hpp",
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
