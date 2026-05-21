# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""TransactorClient — connects to a db-transactor subprocess.

Handles auto-spawning the transactor process if one isn't running,
and provides a typed ``vgi_rpc`` proxy for RPC calls.

The transactor manages multiple databases internally (one per attach_opaque_data),
so a single transactor process serves all catalog attachments.

Usage::

    client = TransactorClient()
    proxy = client.get_proxy()
    proxy.register(attach_opaque_data)
    tx_id = proxy.begin(attach_opaque_data)
    # ... use proxy.insert(), proxy.scan(), etc.
    proxy.commit(attach_opaque_data, tx_id)
    client.close()

"""

from __future__ import annotations

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
_DEFAULT_SOCKET_PATH = "/tmp/vgi-transactor.sock"  # noqa: S108
_DEFAULT_DB_DIR = str(Path("~/.local/state/vgi/databases").expanduser())


class TransactorClient:
    """Client that connects to (and optionally spawns) a db-transactor.

    The transactor process is auto-spawned on first use if not already
    running. A single transactor serves all databases.
    """

    def __init__(self) -> None:
        """Initialize client."""
        self._socket_path = os.environ.get("VGI_TRANSACTOR_SOCKET", _DEFAULT_SOCKET_PATH)
        self._transport: UnixTransport | None = None
        self._connection: RpcConnection[TransactorProtocol] | None = None
        self._proxy: Any = None
        self._process: subprocess.Popen | None = None  # type: ignore[type-arg]

    def get_proxy(self) -> Any:
        """Get the typed RPC proxy, spawning the transactor if needed."""
        if self._proxy is not None:
            return self._proxy
        self._ensure_server()
        return self._proxy

    def _ensure_server(self) -> None:
        """Connect to existing transactor or spawn a new one."""
        if self._try_connect():
            return

        self._spawn_server()

        for _ in range(_MAX_SPAWN_RETRIES):
            time.sleep(_SPAWN_RETRY_DELAY)
            if self._try_connect():
                return

        raise RuntimeError(f"Failed to connect to transactor after spawning (socket: {self._socket_path})")

    def _try_connect(self) -> bool:
        """Try to connect to an existing transactor socket."""
        import socket

        if not os.path.exists(self._socket_path):
            return False

        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.connect(self._socket_path)
            self._transport = UnixTransport(sock)
            self._connection = RpcConnection(TransactorProtocol, self._transport)  # type: ignore[type-abstract]
            self._proxy = self._connection.__enter__()
            logger.info("Connected to transactor: %s", self._socket_path)
            return True
        except (ConnectionRefusedError, FileNotFoundError, OSError):
            return False

    def _spawn_server(self) -> None:
        """Spawn a new transactor subprocess."""
        import sys

        db_dir = os.environ.get("VGI_TRANSACTOR_DB_DIR", _DEFAULT_DB_DIR)
        os.makedirs(db_dir, exist_ok=True)

        cmd = [
            sys.executable,
            "-m",
            "vgi.transactor.server",
            "--db-dir",
            db_dir,
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
        """Close the connection."""
        import contextlib

        if self._connection is not None:
            with contextlib.suppress(Exception):
                self._connection.__exit__(None, None, None)
            self._connection = None
        if self._transport is not None:
            with contextlib.suppress(Exception):
                self._transport.close()
            self._transport = None
