# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D101, D102, D103

"""Negotiated macro families through worker dispatch and the public HTTP client.

The fixture is a finite in-process metadata store, not a durable SQL provider.
"""

from dataclasses import replace
from typing import ClassVar

import pyarrow as pa
import pytest
from vgi_rpc.http._testing import _SyncTestClient

from vgi.catalog import MacroInfo, MacroOverload, MacroParameter, MacroType, OnConflict, SchemaObjectType
from vgi.catalog.capabilities import MACRO_OVERLOADS_CREATE, MACRO_OVERLOADS_READ, CatalogCapabilities
from vgi.catalog.evaluation_context import QueryEvaluationContext
from vgi.catalog.layout import SqlExpression
from vgi.catalog.macros import MACRO_DEFAULT_PROFILE, MACRO_PROFILE, canonicalize_macro_defaults
from vgi.client import Client
from vgi.exceptions import CatalogCapabilityError
from vgi.protocol import CatalogAttachRequest, ClientCapabilities, MacroCreateRequest
from vgi.serve import create_app
from vgi.typed_values import TypedValueBatch
from vgi.worker import Worker

from .schema_options_reference import SchemaOptionsCatalog

PROFILES = CatalogCapabilities(read=[MACRO_OVERLOADS_READ], ddl=[MACRO_OVERLOADS_CREATE])
OFFER = ClientCapabilities(
    engine="macro-reference", native_formats=[], catalogs=[], can_stream=False, filter_encodings=[], catalog=PROFILES
)
KEY = b"macro-contract-reference-signing-key"


class MacroCatalog(SchemaOptionsCatalog):
    profiles = PROFILES
    entries: ClassVar[dict[str, bytes]] = {}
    calls: ClassVar[list[str]] = []

    def macro_create(self, **kwargs):
        self.calls.append("legacy")
        self.entries[kwargs["name"]] = MacroInfo(
            comment=None,
            tags={},
            name=kwargs["name"],
            schema_path=kwargs["schema_path"],
            macro_type=kwargs["macro_type"],
            parameters=kwargs["parameters"],
            definition=kwargs["definition"],
            parameter_default_values=kwargs["parameter_default_values"],
            arguments_schema=kwargs["arguments_schema"],
        ).serialize_to_bytes()

    def macro_create_overloads(self, **kwargs):
        self.calls.append("overloads")

        def resolve(overload, parameter):
            if parameter.default_expression is None:
                return parameter.default_value
            if parameter.default_expression.sql != "40 + 2" or parameter.declared_type is not None:
                raise ValueError("finite fixture rejects unsupported default")
            return TypedValueBatch(pa.record_batch([pa.array([42], type=pa.int64())], names=[parameter.name]))

        candidate = canonicalize_macro_defaults(kwargs["overloads"], resolve)
        self.entries[kwargs["name"]] = MacroInfo(
            comment=None,
            tags={},
            name=kwargs["name"],
            schema_path=kwargs["schema_path"],
            macro_type=kwargs["macro_type"],
            parameters=[],
            definition="",
            overloads=candidate,
        ).serialize_to_bytes()

    def macro_get(self, **kwargs):
        data = self.entries.get(kwargs["name"])
        return MacroInfo.deserialize_from_bytes(data) if data is not None else None

    def schema_contents(self, **kwargs):
        kind = kwargs["type"]
        return [
            info
            for data in self.entries.values()
            if (info := MacroInfo.deserialize_from_bytes(data)).schema_path == kwargs["path"]
            and (
                (kind == SchemaObjectType.SCALAR_MACRO and info.macro_type == MacroType.SCALAR)
                or (kind == SchemaObjectType.TABLE_MACRO and info.macro_type == MacroType.TABLE)
            )
        ]

    def catalog_contents(self, **kwargs):
        source = super().catalog_contents(**kwargs)
        return replace(
            source,
            schemas=[
                replace(
                    schema,
                    scalar_macros=self.schema_contents(path=schema.schema.path, type=SchemaObjectType.SCALAR_MACRO),
                )
                for schema in source.schemas
            ],
        )


class MacroWorker(Worker):
    catalog_interface = MacroCatalog


@pytest.fixture
def backend(tmp_path, monkeypatch):
    monkeypatch.setattr(MacroCatalog, "database_path", str(tmp_path / "macro.sqlite"), raising=False)
    monkeypatch.setattr(MacroCatalog, "entries", {})
    monkeypatch.setattr(MacroCatalog, "calls", [])
    worker = MacroWorker(quiet=True)
    worker._signing_key = KEY
    return worker


def attach(worker, offer=OFFER):
    return worker.catalog_attach(
        CatalogAttachRequest(
            name="options",
            data_version_spec=None,
            implementation_version=None,
            client_capabilities=offer,
        )
    ).attach_opaque_data


def family(*, expression=False):
    parameter = MacroParameter(name="x")
    if expression:
        parameter = replace(
            parameter,
            default_expression=SqlExpression(
                evaluation_context=QueryEvaluationContext(),
                sql="40 + 2",
                profile=MACRO_DEFAULT_PROFILE,
                schema_path=["main"],
            ),
        )
    return [
        MacroOverload(
            evaluation_context=QueryEvaluationContext(),
            parameters=[parameter],
            definition="x + 1",
            profile=MACRO_PROFILE,
            schema_path=["main"],
            macro_type="scalar",
        )
    ]


def create(worker, token, *, overloads=None):
    worker.catalog_macro_create(
        MacroCreateRequest(
            attach_opaque_data=token,
            schema_path=["main"],
            name="m",
            macro_type=MacroType.SCALAR,
            parameters=[] if overloads is not None else ["x"],
            definition="" if overloads is not None else "x + 1",
            on_conflict=OnConflict.REPLACE,
            overloads=overloads,
        )
    )


def test_legacy_worker_hook_remains_available_without_negotiation(backend):
    token = attach(backend, None)
    create(backend, token)
    info = backend.catalog_macro_get(token, ["main"], "m").to_optional()
    assert info.parameters == ["x"] and info.definition == "x + 1" and info.overloads is None
    assert MacroCatalog.calls == ["legacy"]


@pytest.mark.parametrize("operation", ["replace", "drop"])
@pytest.mark.parametrize("offer", [None, replace(OFFER, catalog=CatalogCapabilities(read=[MACRO_OVERLOADS_READ]))])
def test_existing_family_cannot_be_replaced_or_dropped_without_create_profile(backend, operation, offer):
    full = attach(backend)
    limited = attach(backend, offer)
    create(backend, full, overloads=family())
    original = MacroCatalog.entries["m"]
    with pytest.raises(CatalogCapabilityError, match=MACRO_OVERLOADS_CREATE):
        if operation == "replace":
            create(backend, limited)
        else:
            backend.catalog_macro_drop(limited, ["main"], "m")
    assert MacroCatalog.entries["m"] == original
    assert MacroCatalog.calls == ["overloads"]


@pytest.mark.parametrize(
    "offer",
    [
        None,
        replace(OFFER, catalog=CatalogCapabilities()),
        replace(OFFER, catalog=CatalogCapabilities(ddl=[MACRO_OVERLOADS_CREATE])),
    ],
)
def test_unnegotiated_create_rejects_before_provider(backend, offer):
    token = attach(backend, offer)
    with pytest.raises(CatalogCapabilityError, match=MACRO_OVERLOADS_CREATE):
        create(backend, token, overloads=family())
    assert MacroCatalog.calls == [] and MacroCatalog.entries == {}


@pytest.mark.parametrize("operation", ["get", "list", "contents"])
def test_unnegotiated_or_withdrawn_read_cannot_lose_overloads(backend, monkeypatch, operation):
    full = attach(backend)
    empty = attach(backend, None)
    create(backend, full, overloads=family())

    def read(token):
        if operation == "get":
            return backend.catalog_macro_get(token, ["main"], "m")
        if operation == "list":
            return backend.catalog_schema_contents_macros(token, ["main"], SchemaObjectType.SCALAR_MACRO)
        return backend.catalog_contents(token)

    assert read(full) is not None
    with pytest.raises(CatalogCapabilityError, match=MACRO_OVERLOADS_READ):
        read(empty)
    monkeypatch.setattr(MacroCatalog, "profiles", CatalogCapabilities())
    with pytest.raises(CatalogCapabilityError, match=MACRO_OVERLOADS_READ):
        read(full)


def test_negotiated_create_routes_new_hook_and_public_rpc_preserves_resolved_defaults(backend):
    app = create_app(MacroWorker, describe=False, signing_key=KEY)
    with Client.from_http("http://vgi.test", httpx_client=_SyncTestClient(app)) as client:
        token = client.catalog_attach(
            name="options", data_version_spec=None, implementation_version=None, client_capabilities=OFFER
        ).attach_opaque_data
        target = dict(attach_opaque_data=token, schema_path=["main"], name="m")
        client.macro_create(
            **target, macro_type=MacroType.SCALAR, parameters=[], definition="", overloads=family(expression=True)
        )
        info = client.macro_get(**target)
        assert info.overloads[0].parameters[0].default_expression is None
        assert info.overloads[0].parameters[0].default_value.batch.column(0)[0].as_py() == 42
        listing = client.schema_contents(attach_opaque_data=token, path=["main"], type=SchemaObjectType.SCALAR_MACRO)
        assert listing[0].serialize_to_bytes() == info.serialize_to_bytes()
    assert MacroCatalog.calls == ["overloads"]


@pytest.mark.parametrize("operation", ["get", "list", "contents"])
def test_discovery_rejects_unresolved_default_expression_even_for_capable_client(backend, operation):
    token = attach(backend)
    MacroCatalog.entries["m"] = MacroInfo(
        comment=None,
        tags={},
        name="m",
        schema_path=["main"],
        macro_type=MacroType.SCALAR,
        parameters=[],
        definition="",
        overloads=family(expression=True),
    ).serialize_to_bytes()
    with pytest.raises(ValueError, match="resolved at creation"):
        if operation == "get":
            backend.catalog_macro_get(token, ["main"], "m")
        elif operation == "list":
            backend.catalog_schema_contents_macros(token, ["main"], SchemaObjectType.SCALAR_MACRO)
        else:
            backend.catalog_contents(token)
