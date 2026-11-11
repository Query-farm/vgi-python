# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103

"""Protocol 3.3 write-path wire changes.

1. ``catalog_table_scan_function_get`` is gone; ``catalog_table_scan_branches_get``
   is the only scan-discovery method.
2. ``catalog_table_create`` replies ``TableCreateResponse(created: bool not null)``.
3. The write-options batch carries only ``result_mode``.
"""

from __future__ import annotations

import inspect
from types import SimpleNamespace
from typing import Any

import pyarrow as pa
import pytest
from vgi_rpc.utils import serialize_record_batch_bytes

from vgi.catalog.catalog_interface import CatalogInterface, OnConflict, ReadOnlyCatalogInterface
from vgi.client.catalog_mixin import CatalogClientMixin
from vgi.meta_worker import MetaWorker
from vgi.protocol import TableCreateRequest, TableCreateResponse, VgiProtocol
from vgi.worker import Worker
from vgi.write_results import WriteOptions, write_options

SCHEMA = pa.schema([pa.field("id", pa.int64())])


def _create_request(on_conflict: OnConflict = OnConflict.ERROR) -> TableCreateRequest:
    return TableCreateRequest(
        attach_opaque_data=b"sealed-attach",
        schema_path=["main"],
        name="t",
        columns=SCHEMA.serialize().to_pybytes(),
        on_conflict=on_conflict,
        constraint_definitions=[],
    )


def _worker(monkeypatch: pytest.MonkeyPatch, result: Any) -> tuple[Worker, list[dict[str, Any]]]:
    instance = object.__new__(Worker)
    calls: list[dict[str, Any]] = []

    def table_create(**kwargs: Any) -> Any:
        calls.append(kwargs)
        return result

    provider = SimpleNamespace(table_create=table_create)
    monkeypatch.setattr(instance, "_get_catalog", lambda: provider)
    monkeypatch.setattr(instance, "_unwrap_attach", lambda envelope: b"provider-attach")
    monkeypatch.setattr(instance, "_enrich_catalog_span", lambda **kwargs: None)
    monkeypatch.setattr(instance, "_require_dependency_transaction", lambda *args: None)
    monkeypatch.setattr(instance, "_validate_column_definition_expressions", lambda *args: None)
    monkeypatch.setattr(
        instance, "_effective_catalog_capabilities", lambda envelope: SimpleNamespace(read=[], ddl=[], write=[])
    )
    return instance, calls


# ---------------------------------------------------------------- change 1


def test_scan_function_get_rpc_removed() -> None:
    """No vgi.v3 method, worker handler or meta-worker route for the legacy RPC remains."""
    assert not hasattr(VgiProtocol, "catalog_table_scan_function_get")
    assert not hasattr(Worker, "catalog_table_scan_function_get")
    assert hasattr(VgiProtocol, "catalog_table_scan_branches_get")
    assert "catalog_table_scan_function_get" not in inspect.getsource(MetaWorker)


def test_author_scan_function_api_still_backs_branches_get() -> None:
    """Authors keep ``table_scan_function_get``; the base wraps it as one branch."""
    for cls in (CatalogInterface, ReadOnlyCatalogInterface):
        assert callable(getattr(cls, "table_scan_function_get", None))
        assert callable(getattr(cls, "table_scan_branches_get", None))


def test_client_scan_function_get_reads_the_single_branch() -> None:
    """The client's ``table_scan_function_get`` asks branches_get, never the removed RPC."""
    source = inspect.getsource(CatalogClientMixin)
    assert "proxy.catalog_table_scan_function_get" not in source


# ---------------------------------------------------------------- change 2


def test_table_create_response_schema() -> None:
    field = TableCreateResponse.ARROW_SCHEMA.field("created")
    assert field.type == pa.bool_()
    assert not field.nullable
    assert TableCreateResponse.ARROW_SCHEMA.names == ["created"]


def test_create_method_returns_table_create_response() -> None:
    hints = inspect.signature(VgiProtocol.catalog_table_create).return_annotation
    assert hints in (TableCreateResponse, "TableCreateResponse")


@pytest.mark.parametrize(
    ("returned", "created"),
    [(None, True), (True, True), (False, False)],
)
def test_worker_reports_created(monkeypatch: pytest.MonkeyPatch, returned: Any, created: bool) -> None:
    instance, calls = _worker(monkeypatch, returned)
    response = instance.catalog_table_create(_create_request(OnConflict.IGNORE))
    assert response == TableCreateResponse(created=created)
    assert calls[0]["on_conflict"] == OnConflict.IGNORE


def test_worker_rejects_non_bool_create_result(monkeypatch: pytest.MonkeyPatch) -> None:
    instance, _ = _worker(monkeypatch, "yes")
    with pytest.raises(TypeError, match="table_create must return bool or None"):
        instance.catalog_table_create(_create_request())


@pytest.mark.parametrize("created", [True, False])
def test_meta_worker_relays_created(monkeypatch: pytest.MonkeyPatch, created: bool) -> None:
    """The multi-catalog host returns the routed child's reply (it used to drop it)."""
    child = SimpleNamespace(catalog_table_create=lambda request: TableCreateResponse(created=created))
    meta = object.__new__(MetaWorker)
    monkeypatch.setattr(meta, "_worker_for_attach", lambda envelope, method_name="?": child)
    assert meta.catalog_table_create(_create_request()) == TableCreateResponse(created=created)


def test_table_create_response_round_trips() -> None:
    for value in (True, False):
        response = TableCreateResponse(created=value)
        assert TableCreateResponse.deserialize_from_bytes(response.serialize_to_bytes()) == response


# ---------------------------------------------------------------- change 3


def _call(raw: bytes) -> SimpleNamespace:
    from vgi.arguments import Arguments

    return SimpleNamespace(arguments=Arguments(named={"write_options": pa.scalar(raw)}))


def test_write_options_has_only_result_mode() -> None:
    raw = serialize_record_batch_bytes(pa.RecordBatch.from_pylist([{"result_mode": "rows"}]))
    options = write_options(_call(raw))
    assert options == WriteOptions(result_mode="rows")
    # Deprecated accessors keep the only values DuckDB ever sent.
    assert options.on_conflict == "throw"
    assert options.on_conflict_columns == []
    with pytest.raises(TypeError):
        WriteOptions(result_mode="count", on_conflict="nothing")  # type: ignore[call-arg]


def test_write_options_ignores_removed_columns_from_an_old_peer() -> None:
    raw = serialize_record_batch_bytes(
        pa.RecordBatch.from_pylist([{"result_mode": "count", "on_conflict": "nothing", "on_conflict_columns": ["id"]}])
    )
    options = write_options(_call(raw))
    assert options.result_mode == "count"
    assert options.on_conflict == "throw"
