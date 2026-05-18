"""Emit the canonical VGI protocol_version as a one-line .txt file.

Cross-language source of truth: a Rust/Go/TS worker (or anyone shipping a
non-Python VGI implementation) reads ``vgi/protocol_version.txt`` to know
what value to send on every request batch's ``vgi_rpc.protocol_version``
custom_metadata key. The semver comes from ``VgiProtocol.protocol_version``.

Workflow:

1. Bump ``VgiProtocol.protocol_version`` in ``vgi/protocol.py``.
2. ``uv run vgi-gen-protocol-version > vgi/protocol_version.txt``.
3. Also regenerate ``vgi_protocol_constants.hpp`` in the C++ tree (see
   ``vgi.codegen.cpp_constants``).
4. Commit both regenerated files in the same change.

``tests/test_generated_protocol_version.py`` enforces that the checked-in
file matches what the generator would emit right now — CI catches a stale
``.txt`` immediately.
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

from vgi.protocol import VgiProtocol

if TYPE_CHECKING:
    from typing import TextIO


def current_protocol_version() -> str:
    """Return the canonical protocol_version string declared on VgiProtocol.

    Reads via ``vars()`` — not ``getattr`` — to match the runtime framework
    semantics. Subclasses that don't redeclare must not silently leak the
    parent's value.
    """
    value = vars(VgiProtocol).get("protocol_version")
    if not isinstance(value, str):
        raise TypeError(
            f"VgiProtocol.protocol_version must be a str declared as a ClassVar; "
            f"got {type(value).__name__}"
        )
    return value


def emit(out: TextIO) -> None:
    """Write the protocol_version followed by a trailing newline."""
    out.write(current_protocol_version())
    out.write("\n")


def main() -> None:
    """Console-script entry point."""
    emit(sys.stdout)


if __name__ == "__main__":
    main()
