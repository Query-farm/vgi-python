# Copyright 2026 Query Farm LLC - https://query.farm

"""Native BEGIN retry probe: allocate a token, then lose its first response.

Uses the existing diagnostic catalog's persistent active-token files, not a
transaction recovery service. The first BEGIN process exits after allocation
when VGI_TRANSACTION_BEGIN_PROBE_DROP_REPLY=1. A retry would allocate a second
token, leaving the first unknown to the client. No user data is written.
"""

from __future__ import annotations

import os

from tests.catalog.transaction_cleanup_probe import (
    TransactionCleanupCatalog,
    TransactionCleanupWorker,
    _event,
    _locked,
    _root,
)
from vgi.catalog import AttachOpaqueData, TransactionOpaqueData


class BeginReplyProbeCatalog(TransactionCleanupCatalog):
    """Lose one BEGIN response after the provider has allocated its transaction."""

    def catalog_transaction_begin(self, *, attach_opaque_data: AttachOpaqueData) -> TransactionOpaqueData:
        """Persist allocation, then exit once before returning its opaque token."""
        token = super().catalog_transaction_begin(attach_opaque_data=attach_opaque_data)
        lose_response = False
        with _locked() as root:
            marker = root / "begin-response-lost"
            if os.environ.get("VGI_TRANSACTION_BEGIN_PROBE_DROP_REPLY") == "1" and not marker.exists():
                marker.write_text(token.hex(), encoding="utf-8")
                _event(root, "begin", "response_lost", attach_opaque_data, token)
                lose_response = True
        if lose_response:
            os._exit(75)
        return token


class BeginReplyProbeWorker(TransactionCleanupWorker):
    """Reuse the ordinary diagnostic catalog and native subprocess dispatch."""

    catalog_interface = BeginReplyProbeCatalog


def main() -> None:
    """Keep each reproduction's allocation and framework state isolated."""
    root = _root()
    os.environ["VGI_WORKER_SHARED_STORAGE"] = "sqlite"
    os.environ["VGI_WORKER_SQLITE_PATH"] = str(root / "framework.sqlite")
    BeginReplyProbeWorker.main()


if __name__ == "__main__":
    main()
