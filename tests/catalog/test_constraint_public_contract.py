# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103

"""Canonical public carriers and fail-closed worker capability boundaries."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace
from typing import Any

import pyarrow as pa
import pytest

from vgi.catalog.capabilities import (
    CONSTRAINT_CREATE,
    CONSTRAINT_ENFORCE,
    CONSTRAINT_MUTATE,
    CONSTRAINT_RECORDS_READ,
    TRANSACTION_DDL,
    CatalogCapabilities,
)
from vgi.catalog.catalog_interface import OnConflict, SerializedSchema, TableInfo
from vgi.catalog.constraints import (
    ConstraintChange,
    ConstraintDefinition,
    ConstraintEnforcement,
    ConstraintInfo,
    ConstraintKind,
    ConstraintOperation,
    ConstraintsApplyRequest,
    ConstraintSnapshot,
    ConstraintValidation,
)
from vgi.catalog.dependencies import CatalogObjectRef
from vgi.exceptions import CatalogCapabilityError
from vgi.logical_types import variant_field
from vgi.meta_worker import MetaWorker
from vgi.protocol import TableCreateRequest
from vgi.worker import Worker

TABLE = CatalogObjectRef(kind="table", schema_path=["a.b", "nested"], name="items")
DEFINITION = ConstraintDefinition(kind=ConstraintKind.NOT_NULL, column_paths=[["a.b"]])
INFO = ConstraintInfo(
    ref=CatalogObjectRef(kind="constraint", schema_path=TABLE.schema_path, owner_table=TABLE.name, name="key-1"),
    definition=DEFINITION,
    enforcement=ConstraintEnforcement.ENFORCED,
    validation=ConstraintValidation.ALL_ROWS,
)
SCHEMA = pa.schema([pa.field("a.b", pa.int64(), nullable=False)])
PROFILES = CatalogCapabilities(
    read=[CONSTRAINT_RECORDS_READ],
    ddl=sorted([CONSTRAINT_CREATE, CONSTRAINT_MUTATE]),
    write=[CONSTRAINT_ENFORCE],
    transaction=[TRANSACTION_DDL],
)


def table_info(**changes: Any) -> TableInfo:
    return replace(
        TableInfo(
            name=TABLE.name,
            schema_path=TABLE.schema_path,
            comment=None,
            tags={},
            columns=SerializedSchema(SCHEMA.serialize().to_pybytes()),
            constraints=[INFO.serialize_to_bytes()],
            definition_revision=b"revision-1",
            insert_function=b"opaque-write-descriptor",
            write_result_modes={"insert": "count"},
        ),
        **changes,
    )


def create_request(**changes: Any) -> TableCreateRequest:
    return replace(
        TableCreateRequest(
            attach_opaque_data=b"sealed-attach",
            schema_path=TABLE.schema_path,
            name=TABLE.name,
            columns=SCHEMA.serialize().to_pybytes(),
            on_conflict=OnConflict.ERROR,
            constraint_definitions=[DEFINITION.serialize_to_bytes()],
        ),
        **changes,
    )


def apply_request(**changes: Any) -> ConstraintsApplyRequest:
    return replace(
        ConstraintsApplyRequest(
            attach_opaque_data=b"sealed-attach",
            table=TABLE,
            expected_revision=b"revision-1",
            changes=[
                ConstraintChange(
                    operation=ConstraintOperation.RENAME, ref=INFO.ref, declared_name="renamed"
                ).serialize_to_bytes()
            ],
        ),
        **changes,
    )


def worker(
    monkeypatch: pytest.MonkeyPatch, selected: CatalogCapabilities = PROFILES, available: CatalogCapabilities = PROFILES
) -> tuple[Worker, SimpleNamespace, list[Any]]:
    instance = object.__new__(Worker)
    calls = []
    provider = SimpleNamespace(
        catalog_capabilities=lambda **kwargs: available,
        table_create=lambda **kwargs: calls.append(kwargs),
        table_get=lambda **kwargs: table_info(),
        table_insert_function_get=lambda **kwargs: calls.append(kwargs),
    )

    def apply(request: ConstraintsApplyRequest) -> ConstraintSnapshot:
        calls.append(request)
        return ConstraintSnapshot(
            table=TABLE, definition_revision=b"revision-2", constraints=[INFO.serialize_to_bytes()]
        )

    provider.table_constraints_apply = apply
    monkeypatch.setattr(instance, "_get_catalog", lambda: provider)
    monkeypatch.setattr(instance, "_catalog_agreement", lambda envelope: selected)
    monkeypatch.setattr(instance, "_unwrap_attach", lambda envelope: b"provider-attach")
    monkeypatch.setattr(instance, "_unwrap_tx_opt", lambda tx, attach: None if tx is None else b"provider-tx")
    monkeypatch.setattr(instance, "_enrich_catalog_span", lambda **kwargs: None)
    return instance, provider, calls


def test_public_carriers_have_one_required_authoritative_list() -> None:
    legacy = {
        "not_null_constraints",
        "unique_constraints",
        "check_constraints",
        "primary_key_constraints",
        "foreign_key_constraints",
    }
    assert not legacy.intersection(TableInfo.ARROW_SCHEMA.names)
    assert not legacy.intersection(TableCreateRequest.ARROW_SCHEMA.names)
    assert not TableInfo.ARROW_SCHEMA.field("constraints").nullable
    assert not TableCreateRequest.ARROW_SCHEMA.field("constraint_definitions").nullable
    assert TableInfo.ARROW_SCHEMA.field("definition_revision").nullable
    assert TableInfo.deserialize_from_bytes(table_info().serialize_to_bytes()) == table_info()
    assert TableCreateRequest.deserialize_from_bytes(create_request().serialize_to_bytes()) == create_request()
    with pytest.raises(ValueError, match="bounded list"):
        table_info(constraints=None)
    with pytest.raises(ValueError, match="bounded list"):
        create_request(constraint_definitions=None)


def test_profiles_require_readback_and_create_requires_enforcement() -> None:
    assert PROFILES.select(None) == CatalogCapabilities()
    assert PROFILES.select(CatalogCapabilities(ddl=[CONSTRAINT_CREATE, CONSTRAINT_MUTATE])) == CatalogCapabilities()
    read_and_create = CatalogCapabilities(read=[CONSTRAINT_RECORDS_READ], ddl=[CONSTRAINT_CREATE])
    assert PROFILES.select(read_and_create).ddl == []


def test_discovery_never_hides_constraints_and_mutation_requires_revision() -> None:
    with pytest.raises(CatalogCapabilityError):
        Worker._table_for_capabilities(table_info(), CatalogCapabilities())
    read_only = CatalogCapabilities(read=[CONSTRAINT_RECORDS_READ])
    result = Worker._table_for_capabilities(table_info(), read_only)
    assert result.constraints == table_info().constraints
    assert result.insert_function is None and result.write_result_modes == {}
    assert Worker._table_for_capabilities(table_info(), PROFILES).insert_function is not None
    with pytest.raises(ValueError, match="definition revision"):
        Worker._table_for_capabilities(table_info(definition_revision=None), PROFILES)


@pytest.mark.parametrize("trusted", [False, True])
def test_discovery_rejects_nullability_that_does_not_match_trust(trusted: bool) -> None:
    info = INFO if trusted else replace(INFO, enforcement=ConstraintEnforcement.METADATA_ONLY)
    wrong = pa.schema([pa.field("a.b", pa.int64(), nullable=trusted)])
    with pytest.raises(ValueError, match="nullability"):
        Worker._table_for_capabilities(
            table_info(columns=wrong.serialize().to_pybytes(), constraints=[info.serialize_to_bytes()]), PROFILES
        )


def test_create_checks_negotiation_and_current_support_before_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    for selected, available in [(CatalogCapabilities(), PROFILES), (PROFILES, CatalogCapabilities())]:
        instance, _, calls = worker(monkeypatch, selected, available)
        with pytest.raises(CatalogCapabilityError):
            instance.catalog_table_create(create_request())
        assert calls == []
    instance, _, calls = worker(monkeypatch)
    instance.catalog_table_create(create_request())
    assert calls[0]["constraint_definitions"] == create_request().constraint_definitions
    assert calls[0]["attach_opaque_data"] == b"provider-attach"


def test_apply_unwraps_context_once_and_rejects_transaction_without_guarantee(monkeypatch: pytest.MonkeyPatch) -> None:
    instance, _, calls = worker(monkeypatch)
    request = apply_request(transaction_opaque_data=b"sealed-tx")
    result = instance.catalog_table_constraints_apply(request)
    assert result.definition_revision == b"revision-2"
    assert len(calls) == 1
    assert calls[0].attach_opaque_data == b"provider-attach"
    assert calls[0].transaction_opaque_data == b"provider-tx"
    assert calls[0].expected_revision == request.expected_revision
    instance, _, calls = worker(monkeypatch, replace(PROFILES, transaction=[]))
    with pytest.raises(CatalogCapabilityError, match=TRANSACTION_DDL):
        instance.catalog_table_constraints_apply(request)
    assert calls == []


def test_unqualified_write_discovery_and_wrong_mutation_owner_reject(monkeypatch: pytest.MonkeyPatch) -> None:
    instance, _, calls = worker(monkeypatch, CatalogCapabilities(read=[CONSTRAINT_RECORDS_READ]))
    with pytest.raises(CatalogCapabilityError, match=CONSTRAINT_ENFORCE):
        instance.catalog_table_insert_function_get(b"sealed-attach", TABLE.schema_path, TABLE.name)
    assert calls == []
    instance, provider, _ = worker(monkeypatch)
    provider.table_constraints_apply = lambda request: ConstraintSnapshot(
        table=replace(TABLE, name="other"), definition_revision=b"revision-2", constraints=[]
    )
    with pytest.raises(ValueError, match="different table"):
        instance.catalog_table_constraints_apply(apply_request())


def test_legacy_replacement_and_not_null_cannot_bypass_canonical_mutation(monkeypatch: pytest.MonkeyPatch) -> None:
    instance, _, calls = worker(monkeypatch, CatalogCapabilities(read=[CONSTRAINT_RECORDS_READ]))
    with pytest.raises(CatalogCapabilityError, match=CONSTRAINT_MUTATE):
        instance.catalog_table_create(create_request(on_conflict=OnConflict.REPLACE, constraint_definitions=[]))
    with pytest.raises(CatalogCapabilityError, match=CONSTRAINT_MUTATE):
        instance.catalog_table_not_null_set(b"sealed-attach", TABLE.schema_path, TABLE.name, ["a.b"])
    assert calls == []


def test_meta_worker_routes_canonical_apply_without_rewriting_attachment(monkeypatch: pytest.MonkeyPatch) -> None:
    instance = object.__new__(MetaWorker)
    request = apply_request()
    expected = ConstraintSnapshot(table=TABLE, definition_revision=b"revision-2", constraints=[])
    seen = []

    def collect(value: ConstraintsApplyRequest) -> ConstraintSnapshot:
        seen.append(value)
        return expected

    child = SimpleNamespace(catalog_table_constraints_apply=collect)

    def route(envelope: bytes, *, method_name: str) -> SimpleNamespace:
        assert envelope == request.attach_opaque_data
        assert method_name == "catalog_table_constraints_apply"
        return child

    monkeypatch.setattr(instance, "_worker_for_attach", route)
    assert instance.catalog_table_constraints_apply(request) == expected
    assert seen == [request]


def test_storage_nullability_is_not_a_synthetic_sql_constraint() -> None:
    from vgi.catalog.constraints import validate_constraint_schema

    schema = pa.schema(
        [
            pa.field("rowid", pa.int64(), nullable=False, metadata={b"is_row_id": b""}),
            variant_field("variant"),
        ]
    )
    validate_constraint_schema(schema, [])
    for path in [["rowid"], ["variant", "metadata"]]:
        with pytest.raises(ValueError, match="storage"):
            validate_constraint_schema(
                schema, [ConstraintDefinition(kind=ConstraintKind.NOT_NULL, column_paths=[path])]
            )
