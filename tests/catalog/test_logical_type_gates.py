# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103
"""Logical annotations must be negotiated before catalog effects or query binding."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock

import pyarrow as pa
import pytest

from vgi.catalog import CatalogAttachResult
from vgi.catalog.capabilities import MACRO_OVERLOADS_READ, CatalogCapabilities
from vgi.catalog.catalog_interface import MacroInfo, MacroType, OnConflict, SerializedSchema, TableInfo
from vgi.catalog.evaluation_context import QueryEvaluationContext
from vgi.catalog.macros import MACRO_PROFILE, MacroOverload, MacroParameter
from vgi.catalog.query import CatalogQueryPrepareRequest, CatalogQueryPrepareResult
from vgi.logical_types import tuple_field, variant_field
from vgi.protocol import CatalogAttachRequest, MacroCreateRequest, TableCreateRequest
from vgi.query_parameters import QueryParameters
from vgi.query_provider import QueryOwner
from vgi.typed_values import TypedValueBatch
from vgi.worker import Worker

from .query_reference import QueryReferenceCatalog, QueryReferenceProvider, QueryReferenceWorker
from .test_query_protocol import CAPS, KEY, request


def schema(name: str = "x") -> pa.Schema:
    return pa.schema([tuple_field(name, [pa.field("element1", pa.int64())])])


def values() -> TypedValueBatch:
    definition = variant_field("x")
    # NULL still has a declared logical type and cannot bypass negotiation.
    return TypedValueBatch(
        pa.RecordBatch.from_arrays([pa.array([None], type=definition.type)], schema=pa.schema([definition]))
    )


def macro(parameter: MacroParameter | None = None, *, legacy: bool = False) -> MacroInfo:
    if legacy:
        parameter = MacroParameter(name="x", default_value=values())
    assert parameter is not None
    return MacroInfo(
        comment=None,
        tags={},
        name="m",
        schema_path=["main"],
        macro_type=MacroType.SCALAR,
        overloads=[
            MacroOverload(
                evaluation_context=QueryEvaluationContext(),
                parameters=[parameter],
                definition="x",
                profile=MACRO_PROFILE,
                schema_path=["main"],
                macro_type="scalar",
            )
        ],
    )


def worker_without_logical_profiles() -> Mock:
    worker = Mock(spec=Worker)
    worker._effective_catalog_capabilities.return_value = CatalogCapabilities(read=[MACRO_OVERLOADS_READ])
    return worker


def test_table_discovery_rejects_unnegotiated_logical_schema() -> None:
    info = TableInfo(
        comment=None,
        tags={},
        name="t",
        schema_path=["main"],
        columns=SerializedSchema(schema().serialize().to_pybytes()),
        constraints=[],
    )
    with pytest.raises(ValueError, match="not negotiated"):
        Worker._table_for_capabilities(info, CatalogCapabilities())


def test_table_creation_rejects_unnegotiated_logical_schema_before_provider() -> None:
    worker = worker_without_logical_profiles()
    create = TableCreateRequest(
        constraint_definitions=[],
        attach_opaque_data=b"attach",
        schema_path=["main"],
        name="t",
        columns=schema().serialize().to_pybytes(),
        on_conflict=OnConflict.ERROR,
    )
    with pytest.raises(ValueError, match="not negotiated"):
        Worker.catalog_table_create(worker, create)
    worker._get_catalog.return_value.table_create.assert_not_called()


@pytest.mark.parametrize("kind", ["declared", "default", "legacy_default"])
def test_macro_discovery_rejects_unnegotiated_logical_parameters(kind: str) -> None:
    parameter = (
        MacroParameter(name="x", declared_type=schema())
        if kind == "declared"
        else MacroParameter(name="x", default_value=values())
    )
    info = macro(parameter, legacy=kind == "legacy_default")
    with pytest.raises(ValueError, match="not negotiated"):
        Worker._macro_for_capabilities(info, CatalogCapabilities(read=[MACRO_OVERLOADS_READ]))


@pytest.mark.parametrize("kind", ["declared", "default", "legacy_default"])
def test_macro_creation_rejects_unnegotiated_logical_parameters_before_provider(kind: str) -> None:
    parameter = (
        MacroParameter(name="x", declared_type=schema())
        if kind == "declared"
        else MacroParameter(name="x", default_value=values())
    )
    info = macro(parameter, legacy=kind == "legacy_default")
    create = MacroCreateRequest(
        attach_opaque_data=b"attach",
        schema_path=info.schema_path,
        name=info.name,
        macro_type=info.macro_type,
        overloads=info.overloads,
        on_conflict=OnConflict.ERROR,
    )
    worker = worker_without_logical_profiles()
    with pytest.raises(ValueError, match="not negotiated"):
        Worker.catalog_macro_create(worker, create)
    worker._get_catalog.return_value.macro_create.assert_not_called()


@pytest.fixture
def query_backend(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[QueryReferenceWorker, CatalogAttachResult]:
    monkeypatch.setattr(QueryReferenceCatalog, "database_path", str(tmp_path / "queries.sqlite"), raising=False)
    worker = QueryReferenceWorker(quiet=True)
    worker._signing_key = KEY
    attached = worker.catalog_attach(
        CatalogAttachRequest(
            name="queries",
            data_version_spec=None,
            implementation_version=None,
            client_capabilities=CAPS,
        )
    )
    return worker, attached


def test_query_preparation_rejects_unnegotiated_parameter_schema_before_provider(
    query_backend: tuple[QueryReferenceWorker, CatalogAttachResult], monkeypatch: pytest.MonkeyPatch
) -> None:
    worker, attached = query_backend
    provider = worker._get_catalog().query_provider
    assert isinstance(provider, QueryReferenceProvider)
    prepare = Mock(wraps=provider.prepare)
    monkeypatch.setattr(provider, "prepare", prepare)
    field = variant_field("value")
    parameters = QueryParameters.from_batch(
        pa.RecordBatch.from_arrays([pa.array([None], type=field.type)], schema=pa.schema([field])),
        mode="named",
    )
    with pytest.raises(ValueError, match="not negotiated"):
        worker.catalog_query_prepare(request(attached, parameters))
    prepare.assert_not_called()


def test_query_preparation_rejects_unnegotiated_output_and_releases_recipe(
    query_backend: tuple[QueryReferenceWorker, CatalogAttachResult], monkeypatch: pytest.MonkeyPatch
) -> None:
    worker, attached = query_backend
    provider = worker._get_catalog().query_provider
    assert isinstance(provider, QueryReferenceProvider)
    original = provider.prepare

    def unexpected_output(*, owner: QueryOwner, request: CatalogQueryPrepareRequest) -> CatalogQueryPrepareResult:
        return replace(original(owner=owner, request=request), output_schema=schema("result").serialize().to_pybytes())

    release = Mock(wraps=provider.release)
    monkeypatch.setattr(provider, "prepare", unexpected_output)
    monkeypatch.setattr(provider, "release", release)
    with pytest.raises(ValueError, match="not negotiated"):
        worker.catalog_query_prepare(request(attached))
    release.assert_called_once()


@pytest.mark.parametrize("operation", ["replace", "drop"])
def test_mandatory_family_mutations_do_not_require_representation_capability(
    query_backend: tuple[QueryReferenceWorker, CatalogAttachResult], monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    worker, attached = query_backend
    catalog = worker._get_catalog()
    info = macro(MacroParameter(name="x"))
    monkeypatch.setattr(catalog, "macro_get", lambda **kwargs: info)
    mutation = Mock()
    monkeypatch.setattr(catalog, "macro_create", mutation)
    monkeypatch.setattr(catalog, "macro_drop", mutation)
    if operation == "replace":
        worker.catalog_macro_create(
            MacroCreateRequest(
                attach_opaque_data=attached.attach_opaque_data,
                schema_path=["main"],
                name="m",
                macro_type=MacroType.SCALAR,
                overloads=info.overloads,
                on_conflict=OnConflict.REPLACE,
            )
        )
    else:
        worker.catalog_macro_drop(
            attach_opaque_data=attached.attach_opaque_data,
            schema_path=["main"],
            name="m",
            ignore_not_found=False,
            transaction_opaque_data=None,
        )
    mutation.assert_called_once()
