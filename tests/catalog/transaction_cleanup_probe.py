# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D102, D106
"""Opt-in transaction lifetime probe; no data atomicity or recovery guarantees.

Run ``python -m tests.catalog.transaction_cleanup_probe`` with required
``VGI_TRANSACTION_CLEANUP_PROBE_DIR=/absolute/isolated/directory``. Attach
catalog ``transaction_cleanup_probe`` and query ``main.items``: one row with
``id=1, label='tx:<active-token-hex>'``. The catalog is unfrozen, and schema
and table comments include the transaction's token, so native metadata lives
in its transaction-local catalog entries. There are no user data mutations.

The harness can atomically replace ``control.json`` with, for example,
``{"commit":"reject"}`` or ``{"rollback":"after_completion_error"}``.
Each operation consumes its own key once. ``reject`` raises before removing
the active transaction; ``after_completion_error`` removes it, then raises
instead of returning a successful response. Omitted keys complete normally.

``active-<token>.json`` files contain only currently open transactions and
their schema comments. There is no completed-transaction or recovery ledger.
``events.jsonl`` records event, phase, token, attach, PID and optional mode.
All files, including framework SQLite storage, stay in the configured root.
This diagnostic targets the unsigned subprocess transport used by the native
harness; it does not qualify HTTP transaction-token unwrapping.
"""

from __future__ import annotations

import fcntl
import json
import os
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi.catalog import (
    AttachOpaqueData,
    Catalog,
    CatalogAttachResult,
    ReadOnlyCatalogInterface,
    Schema,
    SchemaInfo,
    SchemaObjectType,
    Table,
    TableInfo,
    TransactionOpaqueData,
)
from vgi.invocation import BindResponse, GlobalInitResponse
from vgi.table_function import BindParams, InitParams, ProcessParams, TableFunctionGenerator
from vgi.worker import Worker

_ROWS = pa.schema([("id", pa.int64()), ("label", pa.string())])


def _root() -> Path:
    value = os.environ.get("VGI_TRANSACTION_CLEANUP_PROBE_DIR")
    if not value or not Path(value).is_absolute():
        raise ValueError("VGI_TRANSACTION_CLEANUP_PROBE_DIR must name an isolated absolute directory")
    root = Path(value)
    root.mkdir(parents=True, exist_ok=True)
    return root


@contextmanager
def _locked() -> Iterator[Path]:
    root = _root()
    with (root / "state.lock").open("a") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            yield root
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def _write_json(path: Path, value: Any) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(json.dumps(value), encoding="utf-8")
    temporary.replace(path)


def _event(root: Path, event: str, phase: str, attach: bytes, token: bytes | None, **extra: Any) -> None:
    # Callers hold state.lock, including across event append and state changes.
    with (root / "events.jsonl").open("a", encoding="utf-8") as log:
        log.write(
            json.dumps(
                {
                    "event": event,
                    "phase": phase,
                    "attach": attach.hex(),
                    "token": token.hex() if token else None,
                    "pid": os.getpid(),
                    **extra,
                }
            )
            + "\n"
        )


def _active(root: Path, attach: bytes, token: bytes | None) -> dict[str, Any]:
    if not token:
        return {"schema_comment": "outside transaction"}
    path = root / f"active-{token.hex()}.json"
    if not path.exists():
        raise ValueError("transaction_cleanup_probe: transaction is not active")
    data = json.loads(path.read_text(encoding="utf-8"))
    if data["attach"] != attach.hex():
        raise ValueError("transaction_cleanup_probe: transaction belongs to another attachment")
    return data


def _comment(attach: bytes, token: bytes | None) -> str:
    with _locked() as root:
        return str(_active(root, attach, token)["schema_comment"])


def _consume_mode(root: Path, operation: str) -> str:
    path = root / "control.json"
    controls = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    mode = controls.pop(operation, "success")
    if mode not in {"success", "reject", "after_completion_error"}:
        raise ValueError(f"transaction_cleanup_probe: unknown failure mode {mode!r}")
    _write_json(path, controls)
    return str(mode)


@dataclass(kw_only=True)
class _ScanState(ArrowSerializableDataclass):
    """One-row producer state that remains serializable across transport turns."""

    emitted: bool = False


class TransactionCleanupScan(TableFunctionGenerator[None, _ScanState]):
    """Read one diagnostic row only while the requested transaction is active."""

    State = _ScanState

    class Meta:
        name = "transaction_cleanup_scan"
        max_workers = 1

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        del params  # Also used for schema-only catalog introspection.
        return BindResponse(output_schema=_ROWS)

    @classmethod
    def on_init(cls, params: InitParams[None]) -> GlobalInitResponse:
        del params
        return GlobalInitResponse(max_workers=1)

    @classmethod
    def initial_state(cls, params: ProcessParams[None]) -> _ScanState:
        del params
        return _ScanState()

    @classmethod
    def process(cls, params: ProcessParams[None], state: _ScanState, out: OutputCollector) -> None:
        if state.emitted:
            out.finish()
            return
        assert params.init_call is not None
        attach = bytes(params.attach_opaque_data or b"")
        token = params.init_call.bind_call.transaction_opaque_data
        if not attach or not token:
            raise ValueError("transaction_cleanup_probe: scan requires an active transaction")
        with _locked() as root:
            _active(root, attach, token)
            _event(root, "scan", "read", attach, token)
        out.emit(pa.RecordBatch.from_pydict({"id": [1], "label": [f"tx:{token.hex()}"]}, schema=_ROWS))
        state.emitted = True


CATALOG = Catalog(
    name="transaction_cleanup_probe",
    default_schema="main",
    schemas=[
        Schema(
            path=["main"],
            functions=[TransactionCleanupScan],
            tables=[Table(name="items", function=TransactionCleanupScan)],
        )
    ],
)


class TransactionCleanupCatalog(ReadOnlyCatalogInterface):
    """Transaction-local metadata with controllable completion callback failures."""

    catalog = CATALOG
    supports_transactions = True
    catalog_version_frozen = False

    def catalog_attach(self, **kwargs: Any) -> CatalogAttachResult:
        result = super().catalog_attach(**kwargs)
        attach = AttachOpaqueData(uuid.uuid4().bytes)
        with _locked() as root:
            _event(root, "attach", "completed", attach, None)
        return replace(
            result,
            attach_opaque_data=attach,
            attach_opaque_data_required=True,
            supports_transactions=True,
            catalog_version_frozen=False,
            catalog_version=1,
            supports_catalog_contents=False,
        )

    def catalog_version(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        ctx: Any = None,
    ) -> int:
        del ctx
        _comment(attach_opaque_data, transaction_opaque_data)
        return 1

    def catalog_transaction_begin(self, *, attach_opaque_data: AttachOpaqueData) -> TransactionOpaqueData:
        token = TransactionOpaqueData(uuid.uuid4().bytes)
        with _locked() as root:
            _write_json(
                root / f"active-{token.hex()}.json",
                {"attach": attach_opaque_data.hex(), "schema_comment": f"tx:{token.hex()}"},
            )
            _event(root, "begin", "completed", attach_opaque_data, token)
        return token

    def _finish(self, operation: str, attach: bytes, token: bytes) -> None:
        with _locked() as root:
            _event(root, operation, "attempt", attach, token)
            try:
                _active(root, attach, token)
            except ValueError:
                _event(root, operation, "rejected", attach, token, mode="inactive")
                raise
            mode = _consume_mode(root, operation)
            if mode == "reject":
                _event(root, operation, "rejected", attach, token, mode=mode)
                raise RuntimeError(f"transaction_cleanup_probe: {operation} rejected before completion")
            (root / f"active-{token.hex()}.json").unlink()
            _event(root, operation, "completed", attach, token, mode=mode)
            if mode == "after_completion_error":
                _event(root, operation, "response_error", attach, token, mode=mode)
                raise RuntimeError(f"transaction_cleanup_probe: {operation} response error after completion")

    def catalog_transaction_commit(
        self, *, attach_opaque_data: AttachOpaqueData, transaction_opaque_data: TransactionOpaqueData
    ) -> None:
        self._finish("commit", attach_opaque_data, transaction_opaque_data)

    def catalog_transaction_rollback(
        self, *, attach_opaque_data: AttachOpaqueData, transaction_opaque_data: TransactionOpaqueData
    ) -> None:
        self._finish("rollback", attach_opaque_data, transaction_opaque_data)

    def schemas(self, **kwargs: Any) -> list[SchemaInfo]:
        comment = _comment(kwargs["attach_opaque_data"], kwargs["transaction_opaque_data"])
        return [replace(info, comment=comment) for info in super().schemas(**kwargs)]

    def schema_get(self, **kwargs: Any) -> SchemaInfo | None:
        comment = _comment(kwargs["attach_opaque_data"], kwargs["transaction_opaque_data"])
        info = super().schema_get(**kwargs)
        return replace(info, comment=comment) if info is not None else None

    def table_get(self, **kwargs: Any) -> TableInfo | None:
        comment = _comment(kwargs["attach_opaque_data"], kwargs["transaction_opaque_data"])
        info = super().table_get(**kwargs)
        return replace(info, comment=comment) if info is not None else None

    def schema_contents(self, **kwargs: Any) -> Any:
        comment = _comment(kwargs["attach_opaque_data"], kwargs["transaction_opaque_data"])
        result = super().schema_contents(**kwargs)
        if kwargs["type"] == SchemaObjectType.TABLE:
            return [replace(info, comment=comment) for info in result]
        return result


class TransactionCleanupWorker(Worker):
    """Expose the probe through ordinary native subprocess RPC dispatch."""

    catalog = CATALOG
    catalog_interface = TransactionCleanupCatalog


def main() -> None:
    """Confine active transactions, control, events and framework state to one root."""
    root = _root()
    os.environ["VGI_WORKER_SHARED_STORAGE"] = "sqlite"
    os.environ["VGI_WORKER_SQLITE_PATH"] = str(root / "framework.sqlite")
    TransactionCleanupWorker.main()


if __name__ == "__main__":
    main()
