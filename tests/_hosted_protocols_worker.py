# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""A worker that hosts two extra protocols through ``Worker.hosted_protocols``.

Run as a script so each transport is exercised through its real entry point:

- no flags: ``Worker.main`` -> ``Worker.run`` (stdin/stdout)
- ``--unix PATH`` / ``--tcp HOST:PORT``: ``Worker.main``'s launcher paths
- ``--meta`` (stripped before the rest is parsed): ``MetaWorker.serve``
"""

from __future__ import annotations

import sys
from collections.abc import Sequence
from typing import ClassVar, Protocol

from vgi.worker import Worker


class EchoProtocol(Protocol):
    """First extra protocol."""

    protocol_name: ClassVar[str] = "vgi_test.Echo.v1"

    def echo(self, text: str) -> str:
        """Return *text*, tagged."""
        ...


class CounterProtocol(Protocol):
    """Second extra protocol, to pin ordering."""

    protocol_name: ClassVar[str] = "vgi_test.Counter.v1"

    def count(self, text: str) -> int:
        """Return the length of *text*."""
        ...


class EchoImpl:
    """Implements `EchoProtocol`."""

    def echo(self, text: str) -> str:
        """Return *text*, tagged."""
        return f"echo:{text}"


class CounterImpl:
    """Implements `CounterProtocol`."""

    def count(self, text: str) -> int:
        """Return the length of *text*."""
        return len(text)


class HostingWorker(Worker):
    """Hosts `EchoProtocol` then `CounterProtocol` beside ``vgi.v2``."""

    functions = []

    @classmethod
    def hosted_protocols(cls) -> Sequence[tuple[type, object]]:
        """Return the two extra protocols, Echo first."""
        return ((EchoProtocol, EchoImpl()), (CounterProtocol, CounterImpl()))


if __name__ == "__main__":
    if "--meta" in sys.argv:
        sys.argv.remove("--meta")
        from vgi.meta_worker import MetaWorker

        MetaWorker.serve(HostingWorker)
    else:
        HostingWorker.main()
