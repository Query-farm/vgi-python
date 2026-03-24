"""TransactorClient — connects to a db-transactor subprocess.

Handles auto-spawning the transactor process if one isn't running,
and provides a typed ``vgi_rpc`` proxy for RPC calls.

Usage::

    client = TransactorClient("/path/to/store.duckdb")
    proxy = client.get_proxy()
    proxy.begin(tx_id)
    # ... use proxy.insert(), proxy.scan(), etc.
    proxy.commit(tx_id)
    client.close()

"""

from __future__ import annotations

import hashlib
import logging
import os
import subprocess
import time
from pathlib import Path
from typing import Any

from vgi_rpc.rpc import RpcConnection, UnixTransport

from vgi.transactor.protocol import TransactorProtocol

logger = logging.getLogger("vgi.transactor.client")

_MAX_SPAWN_RETRIES = 50
_SPAWN_RETRY_DELAY = 0.1  # seconds


class TransactorClient:
    """Client that connects to (and optionally spawns) a db-transactor.

    The transactor process is auto-spawned on first use if not already
    running. The socket path is deterministic based on the database path.
    """

    def __init__(self, db_path: str | Path) -> None:
        """Initialize client for the given database path."""
        self._db_path = str(db_path)
        self._socket_path = self._compute_socket_path(self._db_path)
        self._transport: UnixTransport | None = None
        self._connection: RpcConnection | None = None
        self._proxy: Any = None  # The typed proxy returned by RpcConnection.__enter__
        self._process: subprocess.Popen | None = None  # type: ignore[type-arg]

    @staticmethod
    def _compute_socket_path(db_path: str) -> str:
        """Compute a deterministic socket path from the database path."""
        path_hash = hashlib.sha256(db_path.encode()).hexdigest()[:16]
        return f"/tmp/vgi-transactor-{path_hash}.sock"  # noqa: S108

    def get_proxy(self) -> Any:
        """Get the typed RPC proxy, spawning the transactor if needed.

        Returns a proxy implementing TransactorProtocol methods.
        """
        if self._proxy is not None:
            return self._proxy

        self._ensure_server()
        return self._proxy

    def _ensure_server(self) -> None:
        """Connect to existing transactor or spawn a new one."""
        # Try connecting to existing socket first
        if self._try_connect():
            return

        # No server running — spawn one
        self._spawn_server()

        # Wait for it to become available
        for _ in range(_MAX_SPAWN_RETRIES):
            time.sleep(_SPAWN_RETRY_DELAY)
            if self._try_connect():
                return

        raise RuntimeError(
            f"Failed to connect to transactor after spawning (socket: {self._socket_path}, db: {self._db_path})"
        )

    def _try_connect(self) -> bool:
        """Try to connect to an existing transactor socket. Returns True on success."""
        import socket

        if not os.path.exists(self._socket_path):
            return False

        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.connect(self._socket_path)
            self._transport = UnixTransport(sock)
            self._connection = RpcConnection(TransactorProtocol, self._transport)
            self._proxy = self._connection.__enter__()
            logger.info("Connected to transactor: %s", self._socket_path)
            return True
        except (ConnectionRefusedError, FileNotFoundError, OSError):
            return False

    def _spawn_server(self) -> None:
        """Spawn a new transactor subprocess."""
        import sys

        cmd = [
            sys.executable,
            "-m",
            "vgi.transactor.server",
            "--db-path",
            self._db_path,
            "--socket",
            self._socket_path,
        ]
        logger.info("Spawning transactor: %s", " ".join(cmd))
        self._process = subprocess.Popen(  # noqa: S603
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )

    def close(self) -> None:
        """Close the connection and optionally shut down the transactor."""
        import contextlib

        if self._connection is not None:
            with contextlib.suppress(Exception):
                self._connection.__exit__(None, None, None)
            self._connection = None
        if self._transport is not None:
            with contextlib.suppress(Exception):
                self._transport.close()
            self._transport = None
