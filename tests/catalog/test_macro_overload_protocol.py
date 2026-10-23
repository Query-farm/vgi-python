# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D101, D102, D103

"""Mandatory macro families over signed HTTP with durable snapshot transactions."""

import base64
import json
import sqlite3
from contextlib import closing
from dataclasses import replace

import pyarrow as pa
import pytest
from vgi_rpc.http._testing import _SyncTestClient

from vgi.catalog import MacroInfo, MacroOverload, MacroParameter, MacroType, OnConflict, SchemaObjectType
from vgi.catalog.capabilities import CatalogCapabilities
from vgi.catalog.evaluation_context import QueryEvaluationContext
from vgi.catalog.layout import SqlExpression
from vgi.catalog.macros import MACRO_DEFAULT_PROFILE, MACRO_PROFILE, canonicalize_macro_defaults
from vgi.client import Client
from vgi.client.catalog_mixin import CatalogClientError
from vgi.protocol import CatalogAttachRequest, MacroCreateRequest
from vgi.serve import create_app
from vgi.typed_values import TypedValueBatch
from vgi.worker import Worker

from .schema_options_reference import SchemaOptionsCatalog

KEY = b"macro-contract-reference-signing-key"


class MacroCatalog(SchemaOptionsCatalog):
    profiles = CatalogCapabilities()

    @staticmethod
    def _macro_key(path, name):
        return "macro:" + json.dumps([path, name])

    def schemas(self, *, attach_opaque_data, transaction_opaque_data):
        with closing(sqlite3.connect(self.database_path)) as db:
            _, data = self._snapshot(db, attach_opaque_data, transaction_opaque_data)
            return [
                self._decode(value, attach_opaque_data) for key, value in data.items() if not key.startswith("macro:")
            ]

    def macro_create(self, **kwargs):
        with closing(sqlite3.connect(self.database_path)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            version, data = self._snapshot(db, kwargs["attach_opaque_data"], kwargs["transaction_opaque_data"])
            key = self._macro_key(kwargs["schema_path"], kwargs["name"])
            if key in data:
                if kwargs["on_conflict"] == OnConflict.IGNORE:
                    return
                if kwargs["on_conflict"] != OnConflict.REPLACE:
                    raise ValueError("macro already exists")
            for overload in kwargs["overloads"]:
                if overload.evaluation_context != QueryEvaluationContext():
                    raise ValueError("finite macro fixture requires default explicit context")
                definitions = {"x + 1", "x + y"} if overload.macro_type == "scalar" else {"SELECT x AS value"}
                if overload.definition not in definitions:
                    raise ValueError("finite macro fixture rejects unsupported definition")

            def resolve(overload, parameter):
                if parameter.default_expression is None:
                    if (
                        parameter.declared_type is not None
                        and parameter.default_value.schema.field(0).type != parameter.declared_type.field(0).type
                    ):
                        raise ValueError("finite fixture rejects unsupported default coercion")
                    return parameter.default_value
                if parameter.default_expression.sql != "40 + 2" or parameter.declared_type is not None:
                    raise ValueError("finite fixture rejects unsupported default")
                return TypedValueBatch(pa.record_batch([pa.array([42], type=pa.int64())], names=[parameter.name]))

            candidate = MacroInfo(
                comment=None,
                tags={},
                name=kwargs["name"],
                schema_path=kwargs["schema_path"],
                macro_type=kwargs["macro_type"],
                overloads=canonicalize_macro_defaults(kwargs["overloads"], resolve),
            )
            data[key] = base64.b64encode(candidate.serialize_to_bytes()).decode()
            self._store(db, version + 1, data, kwargs["transaction_opaque_data"])

    @staticmethod
    def _store(db, version, data, tx):
        if tx is None:
            db.execute("UPDATE catalog SET version=?, data=? WHERE id=1", (version, json.dumps(data)))
        else:
            db.execute("UPDATE transactions SET version=?, data=? WHERE id=?", (version, json.dumps(data), tx))

    def macro_get(self, **kwargs):
        with closing(sqlite3.connect(self.database_path)) as db:
            _, data = self._snapshot(db, kwargs["attach_opaque_data"], kwargs["transaction_opaque_data"])
            value = data.get(self._macro_key(kwargs["schema_path"], kwargs["name"]))
            return MacroInfo.deserialize_from_bytes(base64.b64decode(value)) if value is not None else None

    def macro_drop(self, **kwargs):
        with closing(sqlite3.connect(self.database_path)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            version, data = self._snapshot(db, kwargs["attach_opaque_data"], kwargs["transaction_opaque_data"])
            key = self._macro_key(kwargs["schema_path"], kwargs["name"])
            if key not in data:
                if kwargs["ignore_not_found"]:
                    return
                raise ValueError("macro not found")
            del data[key]
            self._store(db, version + 1, data, kwargs["transaction_opaque_data"])

    def schema_contents(self, **kwargs):
        with closing(sqlite3.connect(self.database_path)) as db:
            _, data = self._snapshot(db, kwargs["attach_opaque_data"], kwargs["transaction_opaque_data"])
            return [
                info
                for key, value in data.items()
                if key.startswith("macro:")
                if (info := MacroInfo.deserialize_from_bytes(base64.b64decode(value))).schema_path == kwargs["path"]
                and kwargs["type"]
                == (
                    SchemaObjectType.SCALAR_MACRO
                    if info.macro_type == MacroType.SCALAR
                    else SchemaObjectType.TABLE_MACRO
                )
            ]

    def catalog_contents(self, **kwargs):
        source = super().catalog_contents(**kwargs)
        return replace(
            source,
            schemas=[
                replace(
                    schema,
                    scalar_macros=self.schema_contents(
                        attach_opaque_data=kwargs["attach_opaque_data"],
                        transaction_opaque_data=None,
                        path=schema.schema.path,
                        type=SchemaObjectType.SCALAR_MACRO,
                    ),
                    table_macros=self.schema_contents(
                        attach_opaque_data=kwargs["attach_opaque_data"],
                        transaction_opaque_data=None,
                        path=schema.schema.path,
                        type=SchemaObjectType.TABLE_MACRO,
                    ),
                )
                for schema in source.schemas
            ],
        )


class MacroWorker(Worker):
    catalog_interface = MacroCatalog


@pytest.fixture
def backend(tmp_path, monkeypatch):
    monkeypatch.setattr(MacroCatalog, "database_path", str(tmp_path / "macro.sqlite"), raising=False)
    worker = MacroWorker(quiet=True)
    worker._signing_key = KEY
    return worker


def client():
    return Client.from_http(
        "http://vgi.test", httpx_client=_SyncTestClient(create_app(MacroWorker, describe=False, signing_key=KEY))
    )


def family(*, expression=False, macro_type="scalar"):
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
            definition="x + 1" if macro_type == "scalar" else "SELECT x AS value",
            profile=MACRO_PROFILE,
            schema_path=["main"],
            macro_type=macro_type,
        )
    ]


def attach(worker):
    return worker.catalog_attach(
        CatalogAttachRequest(name="options", data_version_spec=None, implementation_version=None)
    ).attach_opaque_data


def create(worker, token, overloads=None):
    worker.catalog_macro_create(
        MacroCreateRequest(
            attach_opaque_data=token,
            schema_path=["main"],
            name="m",
            macro_type=MacroType.SCALAR,
            on_conflict=OnConflict.REPLACE,
            overloads=family() if overloads is None else overloads,
        )
    )


@pytest.mark.parametrize("operation", ["get", "list", "contents"])
def test_mandatory_family_read_requires_no_representation_offer(backend, operation):
    token = attach(backend)
    create(backend, token)
    if operation == "get":
        result = backend.catalog_macro_get(token, ["main"], "m").to_optional()
        assert result.overloads == family()
    elif operation == "list":
        assert len(backend.catalog_schema_contents_macros(token, ["main"], SchemaObjectType.SCALAR_MACRO).items) == 1
    else:
        assert backend.catalog_contents(token) is not None


@pytest.mark.parametrize("macro_type", [MacroType.SCALAR, MacroType.TABLE])
def test_http_resolves_defaults_once_and_preserves_complete_family(backend, macro_type):
    with client() as c:
        token = c.catalog_attach(name="options", data_version_spec=None, implementation_version=None).attach_opaque_data
        target = dict(attach_opaque_data=token, schema_path=["main"], name="m")
        c.macro_create(**target, macro_type=macro_type, overloads=family(expression=True, macro_type=macro_type.value))
        info = c.macro_get(**target)
        parameter = info.overloads[0].parameters[0]
        assert parameter.default_expression is None and parameter.default_value.batch.column(0)[0].as_py() == 42
        listing = c.schema_contents(
            attach_opaque_data=token,
            path=["main"],
            type=(SchemaObjectType.SCALAR_MACRO if macro_type == MacroType.SCALAR else SchemaObjectType.TABLE_MACRO),
        )
        assert listing == [info]
    with client() as c:
        assert c.macro_get(**target) == info
        c.macro_create(**target, macro_type=macro_type, overloads=info.overloads, on_conflict=OnConflict.REPLACE)
        assert c.macro_get(**target) == info


def test_atomic_replacement_rollback_and_reconnect_use_persisted_records(backend):
    with client() as c:
        token = c.catalog_attach(name="options", data_version_spec=None, implementation_version=None).attach_opaque_data
        target = dict(attach_opaque_data=token, schema_path=["main"], name="m")
        c.macro_create(**target, macro_type=MacroType.SCALAR, overloads=family())
        original = c.macro_get(**target)
        second = replace(
            family()[0], parameters=[MacroParameter(name="x"), MacroParameter(name="y")], definition="x + y"
        )
        complete = [family()[0], second]
        tx = c.catalog_transaction_begin(attach_opaque_data=token)
        c.macro_create(
            **target,
            transaction_opaque_data=tx,
            macro_type=MacroType.SCALAR,
            overloads=complete,
            on_conflict=OnConflict.REPLACE,
        )
        assert len(c.macro_get(**target, transaction_opaque_data=tx).overloads) == 2
        assert c.macro_get(**target) == original
    with client() as c:
        c.catalog_transaction_rollback(attach_opaque_data=token, transaction_opaque_data=tx)
        assert c.macro_get(**target) == original
        bad = replace(
            family(expression=True)[0],
            parameters=[
                replace(
                    family(expression=True)[0].parameters[0],
                    default_expression=replace(
                        family(expression=True)[0].parameters[0].default_expression, sql="random()"
                    ),
                )
            ],
        )
        before = c.catalog_version(attach_opaque_data=token)
        with pytest.raises(CatalogClientError, match="unsupported default"):
            c.macro_create(
                **target, macro_type=MacroType.SCALAR, overloads=[second, bad], on_conflict=OnConflict.REPLACE
            )
        assert c.macro_get(**target) == original and c.catalog_version(attach_opaque_data=token) == before
        tx = c.catalog_transaction_begin(attach_opaque_data=token)
        c.macro_create(
            **target,
            transaction_opaque_data=tx,
            macro_type=MacroType.SCALAR,
            overloads=complete,
            on_conflict=OnConflict.REPLACE,
        )
        c.catalog_transaction_commit(attach_opaque_data=token, transaction_opaque_data=tx)
    with client() as c:
        assert c.macro_get(**target).overloads == complete
        c.macro_drop(**target)
        assert c.macro_get(**target) is None


def test_discovery_record_rejects_unresolved_default_expression():
    with pytest.raises(ValueError, match="resolved at creation"):
        MacroInfo(
            comment=None,
            tags={},
            name="m",
            schema_path=["main"],
            macro_type=MacroType.SCALAR,
            overloads=family(expression=True),
        )


def test_http_retains_typed_null_docs_and_rejects_unqualified_coercion(backend):
    declared = pa.schema([pa.field("x", pa.int64(), metadata={b"type_note": b"retained"})])
    value = TypedValueBatch(pa.record_batch([pa.array([None], type=pa.int64())], schema=declared))
    parameter = MacroParameter(name="x", declared_type=declared, default_value=value, documentation="")
    member = replace(family()[0], parameters=[parameter])
    with client() as c:
        token = c.catalog_attach(name="options", data_version_spec=None, implementation_version=None).attach_opaque_data
        target = dict(attach_opaque_data=token, schema_path=["main"], name="m")
        c.macro_create(**target, macro_type=MacroType.SCALAR, overloads=[member])
        original = c.macro_get(**target)
        restored = original.overloads[0].parameters[0]
        assert restored.declared_type.equals(declared, check_metadata=True)
        assert restored.default_value.batch.equals(value.batch, check_metadata=True)
        assert restored.has_default and restored.documentation == ""
        bad = replace(parameter, declared_type=pa.schema([pa.field("x", pa.string())]))
        with pytest.raises(CatalogClientError, match="unsupported default coercion"):
            c.macro_create(
                **target,
                macro_type=MacroType.SCALAR,
                overloads=[replace(member, parameters=[bad])],
                on_conflict=OnConflict.REPLACE,
            )
        assert c.macro_get(**target) == original
