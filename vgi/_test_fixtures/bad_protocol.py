# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Fixture worker that deliberately advertises a mismatched protocol_version.

This fixture exists to exercise the vgi-rpc framework's *application protocol
version* enforcement (added in vgi-rpc 0.18.0) end-to-end, across a real
transport, from both the Python ``Client`` and the C++ DuckDB extension.

The trick is entirely Python-side and needs no extension rebuild: the worker
hands :class:`BadProtocol` (a :class:`~vgi.protocol.VgiProtocol` subclass that
redeclares ``protocol_version`` to an impossible major version) to
``RpcServer`` via :attr:`~vgi.worker.Worker.protocol_class`. vgi-rpc reads the
version with ``vars(protocol).get("protocol_version")`` — reading the class's
own ``__dict__``, not an inherited attribute — so the redeclaration on this
subclass's body is what takes effect.

A normal client (Python ``Client`` or the C++ extension) declares
``protocol_version = "1.0.0"`` and sends it on every request. Because this
worker enforces ``"99.0.0"``, the major versions differ and the dispatch
boundary raises ``ProtocolVersionError`` with a directional "upgrade the
client" message that round-trips back to the caller.

Otherwise this is a drop-in replacement for ``vgi-fixture-worker``: it
inherits every function and the catalog from :class:`ExampleWorker`, so any
request reaches the dispatch boundary (and trips the version check) using the
same SQL the example worker accepts.

Registered as the ``vgi-fixture-bad-protocol-worker`` entry point.
"""

from __future__ import annotations

from typing import ClassVar

from vgi._test_fixtures.worker import ExampleWorker
from vgi.protocol import VgiProtocol

# A major-version bump guarantees a mismatch against any real client's
# "1.0.0" (vgi-rpc compares major+minor exactly, ignoring patch). Declared on
# this class body so ``vars(BadProtocol)["protocol_version"]`` resolves to it.
BAD_PROTOCOL_VERSION = "99.0.0"


class BadProtocol(VgiProtocol):
    """VgiProtocol surface with a deliberately incompatible version."""

    protocol_version: ClassVar[str] = BAD_PROTOCOL_VERSION


class BadProtocolWorker(ExampleWorker):
    """ExampleWorker that serves the example catalog under a bad protocol version."""

    protocol_class: ClassVar[type[VgiProtocol]] = BadProtocol  # type: ignore[type-abstract]


def main() -> None:
    """Run the mismatched-protocol fixture worker process."""
    BadProtocolWorker.main()


if __name__ == "__main__":
    main()
