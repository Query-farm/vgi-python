"""Writable table infrastructure — transactor proxy and shared helpers.

Provides the ``TransactorProxy`` for connecting to the db-transactor subprocess,
and helper functions used by the generic writable functions in ``writable_generic.py``.
All tables are created dynamically via CREATE TABLE DDL at the client side.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Annotated

import pyarrow as pa
from vgi_rpc import AnnotatedBatch, ArrowSerializableDataclass, Transient

from vgi.schema_utils import schema
from vgi.table_function import BindParams, ProcessParams

if TYPE_CHECKING:
    from vgi.protocol import BindRequest

from vgi.transactor.client import TransactorClient
from vgi.transactor.protocol import TransactorProtocol

__all__ = [
    "TransactorProxy",
    "WritableScanState",
    "transactor_proxy",
]

# Output schema for write functions returning affected row counts.
_COUNT_SCHEMA = schema(count=pa.int64())

# DuckDB's native rowid pseudocolumn, marked with is_row_id metadata so the
# C++ extension knows which column carries the physical row identifier.
_ROWID_FIELD = pa.field("rowid", pa.int64(), metadata={b"is_row_id": b""})


def _parse_write_options(bind_call: BindRequest) -> dict[str, bool | str | list[str]]:
    """Parse the write_options RecordBatch from the bind call's named arguments."""
    defaults: dict[str, bool | str | list[str]] = {
        "return_chunks": False,
        "on_conflict": "throw",
        "on_conflict_columns": [],
    }
    if not (bind_call.arguments and bind_call.arguments.named):
        return defaults
    val = bind_call.arguments.named.get("write_options")
    if val is None:
        return defaults
    from vgi_rpc.utils import deserialize_record_batch

    options_bytes = val.as_py()
    batch, _ = deserialize_record_batch(options_bytes)
    result = dict(defaults)
    if "return_chunks" in batch.schema.names:
        result["return_chunks"] = batch.column("return_chunks")[0].as_py()
    if "on_conflict" in batch.schema.names:
        result["on_conflict"] = batch.column("on_conflict")[0].as_py()
    if "on_conflict_columns" in batch.schema.names:
        result["on_conflict_columns"] = batch.column("on_conflict_columns")[0].as_py()
    return result


def _is_returning(params: BindParams[None]) -> bool:
    """Check if the C++ operator requested RETURNING rows."""
    opts = _parse_write_options(params.bind_call)
    return bool(opts.get("return_chunks", False))


def _get_tx_id(params: ProcessParams[None]) -> bytes:
    """Get transaction_id from the bind request."""
    assert params.init_call is not None
    tx_id = params.init_call.bind_call.transaction_id
    if tx_id:
        return tx_id
    msg = "transaction_id is required but was not provided in the bind request"
    raise ValueError(msg)


def _get_attach_id(params: ProcessParams[None]) -> bytes:
    """Get attach_id from the bind request."""
    assert params.init_call is not None
    attach_id = params.init_call.bind_call.attach_id
    if attach_id:
        return attach_id
    msg = "attach_id is required but was not provided in the bind request"
    raise ValueError(msg)


def _get_pushdown_filters(params: ProcessParams[None]) -> bytes | None:
    """Get pushdown_filters as serialized IPC bytes from params (or None)."""
    assert params.init_call is not None
    pf_batch = params.init_call.pushdown_filters
    if pf_batch is None:
        return None
    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, pf_batch.schema)
    writer.write_batch(pf_batch)
    writer.close()
    return sink.getvalue().to_pybytes()


@dataclass(kw_only=True)
class WritableScanState(ArrowSerializableDataclass):
    """State for writable table scans — holds the live transactor scan iterator."""

    scan_iter: Annotated[Iterator[AnnotatedBatch] | None, Transient()] = None


# ============================================================================
# TransactorProxy — manages the db-transactor connection
# ============================================================================


class TransactorProxy:
    """Manages connections to the shared db-transactor subprocess.

    The transactor manages multiple databases internally (one per attach_id).
    DDL statements are run during register() for each new catalog attachment.
    """

    def __init__(self, ddl_statements: list[str] | None = None) -> None:
        """Initialize the proxy."""
        self._ddl = ddl_statements or []
        self._client: TransactorClient | None = None

    def _get_proxy(self) -> TransactorProtocol:
        """Get the transactor RPC proxy (auto-spawn if needed)."""
        if self._client is None:
            self._client = TransactorClient()
        return self._client.get_proxy()  # type: ignore[no-any-return]

    def register(self, attach_id: bytes, catalog_name: str = "") -> None:
        """Register a new database for this attach_id and run initial DDL."""
        proxy = self._get_proxy()
        proxy.register(attach_id=attach_id, catalog_name=catalog_name, ddl_statements=self._ddl)

    def close(self) -> None:
        """Close the transactor connection."""
        if self._client is not None:
            self._client.close()
            self._client = None


# Module-level proxy — all tables created dynamically via DDL.
transactor_proxy = TransactorProxy()
