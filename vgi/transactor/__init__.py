"""db-transactor — transactional database access for VGI workers.

The transactor is a long-lived subprocess that owns a single DuckDB
connection. VGI worker processes communicate with it via ``vgi_rpc``
over Unix domain sockets, using the same streaming exchange patterns
that DuckDB uses with VGI workers.

Architecture::

    VGI Worker(s) ──── vgi_rpc (Unix socket) ──── db-transactor
                                                      │
                                                  DuckDB file

"""

from vgi.transactor.client import TransactorClient
from vgi.transactor.protocol import TransactorProtocol

__all__ = [
    "TransactorClient",
    "TransactorProtocol",
]
