# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D102, D106
"""Opt-in native INSERT/finalize diagnostic; never a transactional provider.

Run with ``VGI_WRITE_FINALIZE_PROBE_DIR=/absolute/isolated/directory`` and
``python -m tests.catalog.write_finalize_probe``. Attach catalog
``write_finalize_probe`` using that worker command. All five tables have
``id BIGINT, label VARCHAR`` and accept INSERT only:

* ``input_result`` returns counts or RETURNING rows during INPUT.
* ``emit_then_raise`` buffers an INPUT response, then raises before returning.
* ``finalize_count`` returns zero during INPUT and the total count at FINALIZE.
* ``finalize_raise`` returns zero during INPUT, then raises at FINALIZE.
* ``deferred_returning`` returns no rows during INPUT and rows at FINALIZE;
  native writes currently reject this final row schema as an invalid count.

The first two functions have no finalize override. The other three advertise
one through the ordinary override detection. A zero-input INSERT should still
invoke an advertised finalize callback; ``finalize_count`` then returns zero.

``events.jsonl`` records callback phases, function name, execution ID and PID.
Per-execution ``rows-*.jsonl`` files retain received input for final output.
The ``buffered_emit`` event means out.emit() returned, not that bytes reached
the client. These files are diagnostic observations, not transactional table
storage. SELECT, rollback, atomicity and recovery are deliberately unsupported.
The launcher also keeps framework SQLite storage in the configured directory.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pyarrow as pa
from vgi_rpc.rpc import OutputCollector

from vgi._test_fixtures.simple_writable import _parse_write_options
from vgi.catalog import Catalog, ReadOnlyCatalogInterface, Schema, Table
from vgi.invocation import BindResponse
from vgi.table_function import BindParams, ProcessParams, TableFunctionGenerator
from vgi.table_in_out_function import TableInOutGenerator
from vgi.worker import Worker
from vgi.write_results import write_result_schema

_ROWS = pa.schema([("id", pa.int64()), ("label", pa.string())])
_COUNT = write_result_schema("count", _ROWS)


def _root() -> Path:
    configured = os.environ.get("VGI_WRITE_FINALIZE_PROBE_DIR")
    if not configured:
        raise ValueError("VGI_WRITE_FINALIZE_PROBE_DIR must name an isolated absolute directory")
    root = Path(configured)
    if not root.is_absolute():
        raise ValueError("VGI_WRITE_FINALIZE_PROBE_DIR must be absolute")
    root.mkdir(parents=True, exist_ok=True)
    return root


def _append(path: Path, value: Any) -> None:
    # Native writes serialize their batch callbacks. One append syscall also
    # keeps diagnostics from distinct worker processes as separate JSON lines.
    payload = (json.dumps(value, separators=(",", ":")) + "\n").encode()
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        if os.write(descriptor, payload) != len(payload):
            raise OSError("Incomplete diagnostic append")
    finally:
        os.close(descriptor)


def _execution(params: ProcessParams[None]) -> str:
    if params.init_response is None:
        raise ValueError("Diagnostic callback requires an execution ID")
    return params.init_response.execution_id.hex()


def _rows_path(params: ProcessParams[None]) -> Path:
    return _root() / f"rows-{_execution(params)}.jsonl"


def _received_rows(params: ProcessParams[None]) -> list[dict[str, Any]]:
    path = _rows_path(params)
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as source:
        return [row for line in source for row in json.loads(line)]


def _event(params: ProcessParams[None], phase: str, **details: Any) -> None:
    assert params.init_call is not None
    _append(
        _root() / "events.jsonl",
        {
            "function": params.init_call.bind_call.function_name,
            "execution_id": _execution(params),
            "pid": os.getpid(),
            "phase": phase,
            **details,
        },
    )


def _count(value: int) -> pa.RecordBatch:
    return pa.RecordBatch.from_pydict({"count": [value]}, schema=_COUNT)


class ProbeScan(TableFunctionGenerator[None, None]):
    """Supply the schema needed to bind INSERT without promising readable rows."""

    class Meta:
        name = "probe_schema_only_scan"

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        del params
        return BindResponse(output_schema=_ROWS)

    @classmethod
    def process(cls, params: ProcessParams[None], state: None, out: OutputCollector) -> None:
        del params, state, out
        raise NotImplementedError("write_finalize_probe: schema-only scan; readable table storage is unsupported")


class _ProbeWrite(TableInOutGenerator[None, None]):
    """One synchronous INPUT response, optionally followed by a callback error."""

    case = "input_result"

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        mode = _parse_write_options(params)["result_mode"]
        if mode not in {"count", "rows"}:
            raise ValueError("Diagnostic write supports count and rows only")
        if cls.case in {"finalize_count", "finalize_raise"} and mode != "count":
            raise ValueError("This diagnostic table supports count only")
        if cls.case == "deferred_returning" and mode != "rows":
            raise ValueError("Use INSERT ... RETURNING * for deferred_returning")
        return BindResponse(output_schema=write_result_schema(mode, _ROWS))

    @classmethod
    def initial_state(cls, params: ProcessParams[None]) -> None:
        _event(params, "input_init")

    @classmethod
    def process(cls, params: ProcessParams[None], state: None, batch: pa.RecordBatch, out: OutputCollector) -> None:
        del state
        mode = _parse_write_options(params)["result_mode"]
        _event(params, "input", rows=batch.num_rows, result_mode=mode)
        _append(_rows_path(params), batch.to_pylist())
        if cls.case == "deferred_returning":
            response = pa.RecordBatch.from_pylist([], schema=_ROWS)
        elif cls.case in {"finalize_count", "finalize_raise"}:
            response = _count(0)
        else:
            response = batch if mode == "rows" else _count(batch.num_rows)
        out.emit(response)
        _event(params, "buffered_emit", rows=response.num_rows)
        if cls.case == "emit_then_raise":
            _event(params, "input_raise")
            raise RuntimeError("write_finalize_probe: process raised after buffered emit")
        _event(params, "input_return")


class InputResult(_ProbeWrite):
    """Normal INPUT result, without a finalize callback."""

    class Meta:
        name = "probe_input_result"


class EmitThenRaise(_ProbeWrite):
    """A buffered response must not escape a failing process callback."""

    case = "emit_then_raise"

    class Meta:
        name = "probe_emit_then_raise"


class _FinalizingWrite(_ProbeWrite):
    """Use the existing optional FINALIZE callback, with no protocol additions."""

    @classmethod
    def finalize(cls, params: ProcessParams[None]) -> list[pa.RecordBatch]:
        rows = _received_rows(params)
        _event(params, "finalize", received_rows=len(rows))
        if cls.case == "finalize_raise":
            _event(params, "finalize_raise")
            raise RuntimeError("write_finalize_probe: finalize raised")
        result = (
            pa.RecordBatch.from_pylist(rows, schema=_ROWS) if cls.case == "deferred_returning" else _count(len(rows))
        )
        _event(params, "finalize_return", rows=result.num_rows, columns=result.schema.names)
        return [result]


class FinalizeCount(_FinalizingWrite):
    """INPUT contributes zero; FINALIZE supplies the total received row count."""

    case = "finalize_count"

    class Meta:
        name = "probe_finalize_count"


class FinalizeRaise(_FinalizingWrite):
    """INPUT completes normally, then the finalize callback fails."""

    case = "finalize_raise"

    class Meta:
        name = "probe_finalize_raise"


class DeferredReturning(_FinalizingWrite):
    """Exercise the unsupported native writable final-row output path."""

    case = "deferred_returning"

    class Meta:
        name = "probe_deferred_returning"


_HANDLERS = [InputResult, EmitThenRaise, FinalizeCount, FinalizeRaise, DeferredReturning]
CATALOG = Catalog(
    name="write_finalize_probe",
    default_schema="main",
    schemas=[
        Schema(
            path=["main"],
            functions=[ProbeScan, *_HANDLERS],
            tables=[
                Table(
                    name=handler.case,
                    function=ProbeScan,
                    insert_function=handler,
                    write_result_modes={"insert": "count" if handler in (FinalizeCount, FinalizeRaise) else "rows"},
                )
                for handler in _HANDLERS
            ],
        )
    ],
)


class WriteFinalizeProbeCatalog(ReadOnlyCatalogInterface):
    """Static diagnostic sinks; no transaction or readable-storage promises."""

    catalog = CATALOG
    supports_transactions = False


class WriteFinalizeProbeWorker(Worker):
    """Serve the opt-in probe over the normal worker transports."""

    catalog = CATALOG
    catalog_interface = WriteFinalizeProbeCatalog


def main() -> None:
    """Run with all diagnostic and framework state inside the requested directory."""
    root = _root()
    os.environ["VGI_WORKER_SHARED_STORAGE"] = "sqlite"
    os.environ["VGI_WORKER_SQLITE_PATH"] = str(root / "framework.sqlite")
    WriteFinalizeProbeWorker.main()


if __name__ == "__main__":
    main()
