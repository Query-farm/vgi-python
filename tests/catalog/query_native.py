# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D102, D107
"""Durable query fixture and audit trail for the native DuckDB 1.5 adapter.

Only the finite queries from query_reference are admitted. The extra SQLite
events let the native runner distinguish binding from execution, and inspect
parameter types, transaction owners and cleanup after the worker has exited.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from contextlib import closing
from pathlib import Path
from typing import Any

import pyarrow as pa
from vgi_rpc.rpc import CallContext

from vgi.catalog import AttachOpaqueData, CatalogAttachResult, TransactionOpaqueData
from vgi.catalog.query import (
    CatalogQueryPrepareRequest,
    CatalogQueryPrepareResult,
    ClientQueryCapabilities,
    QueryCapabilities,
)
from vgi.protocol import ClientCapabilities
from vgi.query_parameters import QueryParameters
from vgi.query_provider import QueryExecution, QueryOwner, QueryPreparation
from vgi.worker import Worker

from .query_reference import QueryReferenceCatalog, QueryReferenceProvider


class NativeQueryProvider(QueryReferenceProvider):
    """Add durable observations and explicitly selected failure modes."""

    def __init__(self, catalog: NativeQueryCatalog) -> None:
        self.catalog: NativeQueryCatalog = catalog
        super().__init__(catalog)
        self.mode = os.environ.get("VGI_QUERY_NATIVE_MODE", "")
        self.enabled = self.mode != "disabled"

    def audit(
        self, event: str, owner: QueryOwner, *, handle: bytes | None = None, detail: dict[str, object] | None = None
    ) -> None:
        self.catalog.audit(event, owner.attach_opaque_data, owner.transaction_opaque_data, handle, detail)

    def capabilities(self, *, owner: QueryOwner, offer: ClientQueryCapabilities) -> QueryCapabilities | None:
        self.audit("capabilities", owner)
        return super().capabilities(owner=owner, offer=offer)

    def prepare(self, *, owner: QueryOwner, request: CatalogQueryPrepareRequest) -> CatalogQueryPrepareResult:
        self.audit("prepare_attempt", owner, detail={"query": request.query})
        if self.mode == "prepare_error":
            raise ValueError("native fixture preparation failure")
        result = super().prepare(owner=owner, request=request)
        self.audit("prepare", owner, handle=result.prepared_query_token, detail={"status": result.status})
        if self.mode == "prepare_disconnect":
            # A preparation already exists, but its response never reaches the
            # client. Retrying would allocate a second resource invisibly.
            os._exit(71)
        return result

    def start(
        self, *, owner: QueryOwner, preparation: QueryPreparation, parameters: QueryParameters, execution_id: bytes
    ) -> QueryExecution:
        self.audit(
            "start_attempt",
            owner,
            handle=preparation.result.prepared_query_token,
            detail={
                "query": preparation.request.query,
                "names": parameters.schema.names,
                "types": [str(field.type) for field in parameters.schema],
                "values": [parameters.batch.column(i)[0].as_py() for i in range(parameters.batch.num_columns)],
                "execution_id": execution_id.hex(),
            },
        )
        execution = super().start(
            owner=owner, preparation=preparation, parameters=parameters, execution_id=execution_id
        )
        self.audit("start", owner, handle=execution.handle)
        if self.mode == "start_error":
            # SQL already ran. A lost/error init response must never cause the
            # native client to invoke start again; transaction end reclaims it.
            raise OSError("native fixture ambiguous start failure")
        return execution

    def read(self, *, owner: QueryOwner, handle: bytes, position: int) -> pa.RecordBatch | None:
        self.audit("read", owner, handle=handle, detail={"position": position})
        if self.mode == "read_error":
            raise RuntimeError("native fixture read failure")
        if self.mode == "read_wrong_type":
            return pa.record_batch([pa.array(["wrong result type"])], names=["result"])
        if self.mode == "slow_read" and position == 0:
            # The runner waits for the event, then interrupts the native query.
            # Keep this bounded even if cancellation never reaches the worker.
            if marker := os.environ.get("VGI_QUERY_READ_MARKER"):
                Path(marker).touch()
            time.sleep(3)
        return super().read(owner=owner, handle=handle, position=position)

    def close(self, *, owner: QueryOwner, handle: bytes, reason: str) -> None:
        super().close(owner=owner, handle=handle, reason=reason)
        self.audit("close", owner, handle=handle, detail={"reason": reason})

    def release(self, *, owner: QueryOwner, recipe: bytes) -> None:
        super().release(owner=owner, recipe=recipe)
        self.audit("release", owner, handle=recipe)


class NativeQueryCatalog(QueryReferenceCatalog):
    """Retain the finite provider's durable attachment and transaction rules."""

    def __init__(self) -> None:
        super().__init__()
        with closing(sqlite3.connect(self.database_path)) as db, db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS native_query_events "
                "(id INTEGER PRIMARY KEY, event TEXT, attach BLOB, tx BLOB, handle BLOB, detail TEXT)"
            )
        self.query_provider = NativeQueryProvider(self)

    def audit(
        self,
        event: str,
        attach: bytes,
        tx: bytes | None = None,
        handle: bytes | None = None,
        detail: dict[str, object] | None = None,
    ) -> None:
        with closing(sqlite3.connect(self.database_path)) as db, db:
            db.execute(
                "INSERT INTO native_query_events(event, attach, tx, handle, detail) VALUES (?, ?, ?, ?, ?)",
                (event, attach, tx, handle, json.dumps(detail or {})),
            )

    def catalog_attach(
        self,
        *,
        name: str,
        options: dict[str, Any],
        data_version_spec: str | None,
        implementation_version: str | None,
        client_capabilities: ClientCapabilities | None = None,
        ctx: CallContext | None = None,
    ) -> CatalogAttachResult:
        result = super().catalog_attach(
            name=name,
            options=options,
            data_version_spec=data_version_spec,
            implementation_version=implementation_version,
            client_capabilities=client_capabilities,
            ctx=ctx,
        )
        self.audit("attach", result.attach_opaque_data)
        return result

    def catalog_detach(self, *, attach_opaque_data: AttachOpaqueData) -> None:
        super().catalog_detach(attach_opaque_data=attach_opaque_data)
        self.audit("detach", attach_opaque_data)

    def _finish(self, attach: AttachOpaqueData, tx: TransactionOpaqueData, status: str) -> None:
        super()._finish(attach, tx, status)
        self.audit("transaction_finish", attach, tx, detail={"status": status})


class NativeQueryWorker(Worker):
    """Expose the audited fixture through the normal subprocess transport."""

    catalog_interface = NativeQueryCatalog


if __name__ == "__main__":
    NativeQueryCatalog.database_path = os.environ["VGI_QUERY_REFERENCE_DB"]
    NativeQueryWorker.main()
