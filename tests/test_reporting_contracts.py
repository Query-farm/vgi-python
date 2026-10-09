# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Exercise real reporting serializers, reflection, defaults and generated docs.

These tests validate contract mechanics, not the unimplemented worker lifecycle.
Populated samples deliberately exercise every optional branch independently of
the worker's tagged-payload validation.
"""

from __future__ import annotations

import dataclasses
import inspect
import types
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Annotated, Any, Literal, Union, get_args, get_origin, get_type_hints

import pyarrow as pa
import pytest
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import MethodType, rpc_methods
from vgi_rpc.utils import deserialize_record_batch

from vgi.codegen.reporting_docs import render, targets
from vgi.reporting import CONTRACT_MODULES, PROTOCOLS
from vgi.reporting._metadata import FetchedColumn, SqlRead, read_annotation
from vgi.reporting.alerts import Instance
from vgi.reporting.common import Delegation, DelegationRecord, Trigger
from vgi.reporting.reports import FolderRecord, ReportEnvelope, ReportRow, ReportsProtocol, RevisionRow
from vgi.reporting.sql_tasks import SqlTask

_RECORDS = [
    obj
    for module in CONTRACT_MODULES
    for obj in vars(module).values()
    if isinstance(obj, type)
    and obj.__module__ == module.__name__
    and dataclasses.is_dataclass(obj)
    and issubclass(obj, ArrowSerializableDataclass)
]
_NAMES = {
    "vgi.reports.v1": 17,
    "vgi.report_render.v1": 4,
    "vgi.schedules.v1": 16,
    "vgi.sql_tasks.v1": 17,
    "vgi.alerts.v1": 18,
    "vgi.notify.v1": 4,
    "vgi.delegations.v1": 3,
}
_ROOT = Path(__file__).resolve().parents[1]


def _sample(annotation: Any, *, nulls: bool) -> Any:
    """Populate nested records, nullable branches and values vulnerable to lossy codecs."""
    origin = get_origin(annotation)
    args = get_args(annotation)
    if origin is Annotated:
        return _sample(args[0], nulls=nulls)
    if origin in (Union, types.UnionType):
        assert type(None) in args
        return None if nulls else _sample(next(arg for arg in args if arg is not type(None)), nulls=nulls)
    if origin is Literal:
        return args[0]
    if origin is list:
        return [_sample(args[0], nulls=nulls)]
    if annotation is str:
        return "Café / 東京"
    if annotation is int:
        return 9_007_199_254_740_993
    if annotation is float:
        return 1.25
    if annotation is bool:
        return True
    if annotation is bytes:
        return b"\x00\xffbody"
    if annotation is datetime:
        return datetime(2026, 10, 8, 12, 34, 56, 123456, tzinfo=timezone(timedelta(hours=5, minutes=30)))
    if annotation is pa.Schema:
        return pa.schema([pa.field("amount", pa.decimal128(20, 2), nullable=False)])
    if annotation is pa.RecordBatch:
        return pa.record_batch(
            [
                pa.array([9_007_199_254_740_993], type=pa.int64()),
                pa.array([Decimal("123456789012345.67")], type=pa.decimal128(20, 2)),
            ],
            names=["wide_id", "amount"],
        )
    if isinstance(annotation, type) and dataclasses.is_dataclass(annotation):
        hints = get_type_hints(annotation, include_extras=True)
        return annotation(**{f.name: _sample(hints[f.name], nulls=nulls) for f in dataclasses.fields(annotation)})
    raise AssertionError(f"Uncovered contract type: {annotation}")


@pytest.mark.parametrize("record", _RECORDS, ids=lambda cls: f"{cls.__module__.rsplit('.', 1)[-1]}.{cls.__name__}")
@pytest.mark.parametrize("nulls", [False, True], ids=["populated", "nullable"])
def test_ipc_round_trip(record: type[ArrowSerializableDataclass], nulls: bool) -> None:
    """Every record survives real Arrow IPC, including nested records and typed binary fields."""
    original = _sample(record, nulls=nulls)
    batch, metadata = deserialize_record_batch(original.serialize_to_bytes())
    assert batch.schema == record.ARROW_SCHEMA
    decoded = record.deserialize_from_batch(batch, metadata)
    assert decoded == original
    # A second serialization catches decoders returning dicts/bytes instead of the annotated object.
    second, _ = deserialize_record_batch(decoded.serialize_to_bytes())
    assert second.equals(batch)


def test_protocol_inventory_and_envelopes() -> None:
    """All seven independent interfaces are recognized by the actual RPC framework."""
    assert {p.protocol_name: len(rpc_methods(p)) for p in PROTOCOLS} == _NAMES
    for protocol in PROTOCOLS:
        assert protocol.protocol_version == "1.0.0"
        for name, method in rpc_methods(protocol).items():
            assert method.protocol_name == protocol.protocol_name
            assert method.params_schema.names == list(method.param_types)
            assert all(not isinstance(value, (list, dict, set)) for value in method.param_defaults.values())
            if method.method_type == MethodType.UNARY:
                assert method.result_schema == pa.schema([pa.field("result", pa.binary(), nullable=False)])
            else:
                assert method.is_exchange is False
                annotation = read_annotation(getattr(protocol, name))
                assert annotation is not None and annotation.row_type is not None
                assert len(annotation.row_type.ARROW_SCHEMA) > 0


def test_every_effectful_request_has_a_request_id() -> None:
    """Mutation admission always has a stable ID, including transfer and recovery methods."""
    for protocol in PROTOCOLS:
        for name, method in rpc_methods(protocol).items():
            if name.startswith(("get_", "list_", "check_", "preview_", "test_")):
                continue
            schema = method.params_schema
            assert schema.field("request_id") == pa.field("request_id", pa.string(), nullable=False)


def test_sql_annotations_exclude_mutations_and_tests() -> None:
    """The allowlist cannot accidentally expose run, recovery, test or credential mutations."""
    for protocol in PROTOCOLS:
        for name in rpc_methods(protocol):
            annotation = read_annotation(getattr(protocol, name))
            assert (annotation is not None) == name.startswith(("get_", "list_", "check_", "preview_"))
    annotation = read_annotation(ReportsProtocol.list_reports)
    assert annotation is not None
    assert annotation.fetched_columns == (
        FetchedColumn(
            column="body",
            method="get_report",
            arguments=(("report_id", "report_id"), ("revision_id", "revision_served")),
        ),
    )


def test_factories_do_not_share_mutable_defaults() -> None:
    """Independent report/task records cannot contaminate another caller's defaults."""
    one = ReportEnvelope(title="One", body_format="cupola.evidence/1")
    two = ReportEnvelope(title="Two", body_format="cupola.evidence/1")
    one.tags.append("finance")
    assert two.tags == []
    required = {
        f.name: _sample(get_type_hints(SqlTask, include_extras=True)[f.name], nulls=True)
        for f in dataclasses.fields(SqlTask)
        if f.default is dataclasses.MISSING and f.default_factory is dataclasses.MISSING
    }
    task_one, task_two = SqlTask(**required), SqlTask(**required)
    task_one.notify.on.append("success")
    assert task_two.notify.on == ["failing", "recovered", "disabled"]
    assert task_one.incremental is not task_two.incremental


def test_nullability_units_and_binary_detail_contracts() -> None:
    """Pin wire choices that ordinary Python annotations would otherwise infer differently."""
    assert Trigger.ARROW_SCHEMA.field("run_at") == pa.field("run_at", pa.timestamp("us", "UTC"), nullable=True)
    assert Delegation.ARROW_SCHEMA.field("expires_at").type == pa.timestamp("us", "UTC")
    assert Delegation.ARROW_SCHEMA.field("expires_at").nullable
    assert not ReportEnvelope.ARROW_SCHEMA.field("tags").type.value_field.nullable
    assert not Instance.ARROW_SCHEMA.field("allowed_actions").type.value_field.nullable
    assert Instance.ARROW_SCHEMA.field("latest_details") == pa.field("latest_details", pa.binary(), nullable=True)
    assert Instance.ARROW_SCHEMA.field("state").type == pa.string()  # No Enum dictionary encoding.
    assert "grant" not in DelegationRecord.ARROW_SCHEMA.names
    assert "ticket" not in DelegationRecord.ARROW_SCHEMA.names
    assert "grant" in Delegation.ARROW_SCHEMA.names


def test_direct_arguments_and_required_nullable_fields() -> None:
    """Scalar parameters stay visible on the wire; nullable need not mean optional."""
    signature = inspect.signature(ReportsProtocol.get_report)
    bound = signature.bind(None, "r1")
    bound.apply_defaults()
    assert bound.arguments == {"self": None, "report_id": "r1", "revision_id": None}
    with pytest.raises(TypeError, match="report_id"):
        signature.bind(None)
    parameter = inspect.signature(Delegation).parameters["expires_at"]
    assert parameter.default is inspect.Parameter.empty
    publish = inspect.signature(ReportsProtocol.publish)
    assert publish.parameters["revision_id"].default is inspect.Parameter.empty
    assert publish.parameters["expected_published_revision_id"].default is inspect.Parameter.empty
    methods = rpc_methods(ReportsProtocol)
    assert methods["get_report"].params_schema == pa.schema(
        [
            pa.field("report_id", pa.string(), nullable=False),
            pa.field("revision_id", pa.string(), nullable=True),
        ]
    )
    create = methods["create_report"]
    assert create.params_schema.names == ["request_id", "envelope", "body", "message", "ownership", "folder_id"]
    assert create.params_schema.field("envelope") == pa.field("envelope", pa.binary(), nullable=False)
    assert create.param_defaults == {"message": "", "ownership": None, "folder_id": None}
    assert methods["list_reports"].param_defaults["tags"] is None
    assert not methods["list_reports"].params_schema.field("tags").type.value_field.nullable


def test_folder_placement_and_browsing_contract() -> None:
    """Root is nullable, location is not revisioned, and recursive defaults retain whole-library SQL views."""
    assert ReportRow.ARROW_SCHEMA.field("folder_id") == pa.field("folder_id", pa.string(), nullable=True)
    assert FolderRecord.ARROW_SCHEMA.field("parent_folder_id") == pa.field(
        "parent_folder_id", pa.string(), nullable=True
    )
    assert "folder_id" not in RevisionRow.ARROW_SCHEMA.names
    assert "folder_id" not in ReportEnvelope.ARROW_SCHEMA.names
    assert "path" not in ReportEnvelope.ARROW_SCHEMA.names
    methods = rpc_methods(ReportsProtocol)
    for method_name, selector in (("list_reports", "folder_id"), ("list_folders", "parent_folder_id")):
        method = methods[method_name]
        assert method.param_defaults[selector] is None
        assert method.param_defaults["recursive"] is True
        assert method.params_schema.field(selector) == pa.field(selector, pa.string(), nullable=True)
    annotation = read_annotation(ReportsProtocol.list_folders)
    assert annotation is not None and annotation.table == "folders" and annotation.row_type is FolderRecord
    # Moving to root must be explicit; omitting a nullable destination cannot silently move an object.
    for method_name, selector in (("move_report", "folder_id"), ("update_folder", "parent_folder_id")):
        method = methods[method_name]
        assert selector not in method.param_defaults
        assert method.params_schema.field(selector).nullable
        assert method.params_schema.field("expected_version") == pa.field(
            "expected_version", pa.int64(), nullable=False
        )
    assert "recursive" not in methods["delete_folder"].param_types


@pytest.mark.parametrize(("record", "relative"), targets())
def test_generated_reference_is_current_and_deterministic(record: str, relative: str) -> None:
    """Checking in changed Python without its generated reference fails locally and in CI."""
    expected = render(record)
    assert expected == render(record)
    assert (_ROOT / relative).read_text() == expected


def test_invalid_fetched_column_mapping_fails_generation(monkeypatch: pytest.MonkeyPatch) -> None:
    """A typo in the adapter annotation cannot silently become a published SQL contract."""
    annotation = read_annotation(ReportsProtocol.list_reports)
    assert annotation is not None
    monkeypatch.setattr(
        ReportsProtocol.list_reports,
        "__reporting_sql_read__",
        SqlRead(
            row_type=annotation.row_type,
            table="reports",
            fetched_columns=(FetchedColumn("body", "get_report", (("revision_id", "typo"),)),),
        ),
    )
    with pytest.raises(ValueError, match="Invalid fetched-column mapping"):
        render("reports")
