# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Emit the canonical VGI protocol_version as a one-line .txt file.

Cross-language source of truth: a Rust/Go/TS worker (or anyone shipping a
non-Python VGI implementation) reads ``vgi/protocol_version.txt`` to know
what value to send on every request batch's ``vgi_rpc.protocol_version``
custom_metadata key. The semver comes from ``VgiProtocol.protocol_version``.

Workflow:

1. Set ``VgiProtocol.protocol_name`` and ``protocol_version`` in ``vgi/protocol.py``.
2. Run ``python scripts/regen_generated.py --require-all`` with all SDK roots configured.
3. Commit the regenerated version, names, registries and contract fixtures together.

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
            f"VgiProtocol.protocol_version must be a str declared as a ClassVar; got {type(value).__name__}"
        )
    return value


def supported_window(protocol: type) -> tuple[str, str]:
    """Return the ``(MIN, MAX)`` major.minor window *protocol* supports (``PROTOCOL_VERSIONS_V1`` §7.3).

    The Protocol's ``protocol_version_range`` when it declares one, else exactly
    its ``protocol_version``'s major.minor -- the same rule the vgi-rpc server
    gate and client negotiation apply, read through the same function, so the
    generated constants cannot disagree with what a Python worker advertises.

    Args:
        protocol: A Protocol class declaring ``protocol_version``.

    Returns:
        ``("3.4", "3.4")`` for protocol 3.4.

    Raises:
        TypeError: The protocol declares no string ``protocol_version``.

    """
    from vgi_rpc.protocol_versions import declared_range

    version = vars(protocol).get("protocol_version")
    if not isinstance(version, str):
        raise TypeError(f"{protocol.__name__}.protocol_version must be a str declared as a ClassVar")
    window = declared_range(protocol, version)
    return f"{window.min[0]}.{window.min[1]}", f"{window.max[0]}.{window.max[1]}"


def window_constants(protocol: type, prefix: str) -> dict[str, str]:
    """Return the generated window constants for *protocol*, named ``<prefix>_MIN`` / ``_MAX`` / ``S_CLIENT``.

    ``<prefix>S_CLIENT`` is the ready-made ``vgi_rpc.protocol_versions_client``
    / ``VGI-Client-Protocol-Versions`` value (``vgi.v3=3.4-3.4``).

    Args:
        protocol: A Protocol class declaring ``protocol_name`` and ``protocol_version``.
        prefix: Constant-name prefix, e.g. ``VGI_PROTOCOL_VERSION``.

    Returns:
        Constant name to string value, in emission order.

    """
    from vgi_rpc.rpc._types import _protocol_wire_name

    low, high = supported_window(protocol)
    return {
        f"{prefix}_MIN": low,
        f"{prefix}_MAX": high,
        f"{prefix}S_CLIENT": f"{_protocol_wire_name(protocol)}={low}-{high}",
    }


def emit(out: TextIO) -> None:
    """Write the protocol_version followed by a trailing newline."""
    out.write(current_protocol_version())
    out.write("\n")


def main() -> None:
    """Console-script entry point."""
    emit(sys.stdout)


if __name__ == "__main__":
    main()
