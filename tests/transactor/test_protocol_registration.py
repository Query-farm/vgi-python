# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""The transactor's typed write-result modes must permit RPC server startup."""

from pathlib import Path

from vgi_rpc.rpc import RpcServer

from vgi.transactor.protocol import TransactorProtocol
from vgi.transactor.server import TransactorImpl


def test_typed_write_modes_allow_protocol_registration(tmp_path: Path) -> None:
    """Register the real interface, including INSERT/UPDATE/DELETE signatures."""
    RpcServer(TransactorProtocol, TransactorImpl(str(tmp_path)))
