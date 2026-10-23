# Copyright 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103

"""Absent catalog defaults survive real attachment and qualified discovery."""

import json
from typing import Any

import pyarrow as pa
import pytest
from vgi_rpc.http._testing import _SyncTestClient

from vgi.arguments import Arguments
from vgi.catalog import Catalog, CatalogAttachResult, ReadOnlyCatalogInterface, Schema, SchemaObjectType, View
from vgi.client import Client
from vgi.client.cli_utils import catalog_attach_result_to_dict
from vgi.invocation import FunctionType
from vgi.protocol import BindRequest, CatalogAttachRequest
from vgi.serve import create_app
from vgi.table_function import TableFunctionGenerator, bind_fixed_schema
from vgi.worker import Worker

KEY = b"nullable-default-schema-signing-key"


def attach_result(**kwargs: Any) -> CatalogAttachResult:
    return CatalogAttachResult(
        attach_opaque_data=b"attachment",
        supports_transactions=False,
        supports_time_travel=False,
        catalog_version_frozen=True,
        catalog_version=1,
        resolved_data_version=None,
        resolved_implementation_version=None,
        **kwargs,
    )


@pytest.mark.parametrize("default_schema", [None, "main", "sales.archive"])
def test_attach_record_round_trip_preserves_optional_raw_default(default_schema: str | None) -> None:
    source = attach_result(default_schema=default_schema)
    restored = CatalogAttachResult.deserialize_from_bytes(source.serialize_to_bytes())
    assert restored.default_schema == default_schema
    assert CatalogAttachResult.ARROW_SCHEMA.field("default_schema").nullable
    assert json.loads(json.dumps(catalog_attach_result_to_dict(restored)))["default_schema"] == default_schema


def test_omitted_default_still_means_main() -> None:
    assert attach_result().default_schema == "main"
    assert Catalog(name="legacy", schemas=[Schema(path=["main"])]).default_schema == "main"
    assert ReadOnlyCatalogInterface()._default_schema_name == "main"


@pytest.mark.parametrize("invalid", ["", "bad\x00name", 42])
def test_invalid_default_rejects_in_both_public_records(invalid: Any) -> None:
    with pytest.raises((TypeError, ValueError), match="default_schema"):
        attach_result(default_schema=invalid)
    with pytest.raises((TypeError, ValueError), match="default_schema"):
        Catalog(name="invalid", default_schema=invalid, schemas=[Schema(path=["main"])])


def test_no_default_allows_empty_catalog_and_dots_do_not_become_nested_paths() -> None:
    assert Catalog(name="empty", default_schema=None).schemas == ()
    with pytest.raises(ValueError, match="not found"):
        Catalog(
            name="nested",
            default_schema="sales.archive",
            schemas=[Schema(path=["sales"]), Schema(path=["sales", "archive"])],
        )


@pytest.mark.parametrize("default_schema", [None, "main", "sales.archive"])
@pytest.mark.parametrize("bulk", [False, True])
def test_signed_http_attach_load_and_qualified_discovery(default_schema: str | None, bulk: bool) -> None:
    descriptor = Catalog(
        name="optional_default",
        default_schema=default_schema,
        schemas=[
            Schema(path=["main"], views=[View(name="answer", definition="SELECT 1")]),
            Schema(path=["sales.archive"], views=[View(name="answer", definition="SELECT 2")]),
        ],
    )

    class Provider(ReadOnlyCatalogInterface):
        catalog = descriptor

        def catalog_attach(self, **kwargs: Any) -> CatalogAttachResult:
            from dataclasses import replace

            return replace(super().catalog_attach(**kwargs), supports_catalog_contents=bulk)

    class CatalogWorker(Worker):
        catalog_interface = Provider

    with Client.from_http(
        "http://vgi.test", httpx_client=_SyncTestClient(create_app(CatalogWorker, describe=False, signing_key=KEY))
    ) as client:
        attached = client.catalog_attach(name="optional_default", data_version_spec=None, implementation_version=None)
        assert attached.default_schema == default_schema
        snapshot = client.load_catalog(attach=attached)
        assert snapshot.source == ("catalog_contents" if bulk else "per_schema")
        assert {tuple(item.schema.path) for item in snapshot.schemas} == {("main",), ("sales.archive",)}
        for path, sql in [(["main"], "SELECT 1"), (["sales.archive"], "SELECT 2")]:
            view = client.view_get(attach_opaque_data=attached.attach_opaque_data, schema_path=path, name="answer")
            assert view is not None and view.definition == sql
            assert client.schema_contents(
                attach_opaque_data=attached.attach_opaque_data, path=path, type=SchemaObjectType.VIEW
            ) == [view]
        assert attached.default_schema == default_schema
        assert catalog_attach_result_to_dict(attached)["default_schema"] == default_schema


@pytest.mark.parametrize("invalid", ["", "bad\x00name"])
def test_worker_validates_provider_result_before_publishing_attachment(invalid: str) -> None:
    calls = []

    class Provider(ReadOnlyCatalogInterface):
        catalog = Catalog(name="invalid", default_schema=None)

        def catalog_attach(self, **kwargs: Any) -> CatalogAttachResult:
            result = attach_result(default_schema=None)
            object.__setattr__(result, "default_schema", invalid)
            return result

        def catalog_capabilities(self, **kwargs: Any) -> Any:
            calls.append("capabilities")
            raise AssertionError("invalid attachment must not be published")

    class CatalogWorker(Worker):
        catalog_interface = Provider

    worker = CatalogWorker(quiet=True)
    worker._signing_key = KEY
    with pytest.raises(ValueError, match="default_schema"):
        worker.catalog_attach(CatalogAttachRequest(name="invalid", data_version_spec=None, implementation_version=None))
    assert calls == []


@bind_fixed_schema
class Probe(TableFunctionGenerator[None]):
    """Minimal registered function used to qualify schema routing."""

    class Meta:
        """Declare the function name used by the routing probes."""

        name = "probe"

    FIXED_SCHEMA = pa.schema([pa.field("value", pa.int64())])


def test_no_default_rejects_functions_without_explicit_schema() -> None:
    class CatalogWorker(Worker):
        catalog = Catalog(name="unscoped", default_schema=None, schemas=[Schema(path=["main"])])
        functions = [Probe]

    with pytest.raises(ValueError, match="explicit schema registration"):
        CatalogWorker._build_registry()


def test_no_default_keeps_explicit_function_registration() -> None:
    class CatalogWorker(Worker):
        catalog = Catalog(
            name="scoped",
            default_schema=None,
            schemas=[Schema(path=["sales.archive"], functions=[Probe])],
        )
        functions = [Probe]

    worker = CatalogWorker(quiet=True)
    assert worker._resolve_function_by_name("probe", schema_path=["sales.archive"]) is Probe
    assert (("sales.archive",), "probe") in worker._build_schema_registry()
    assert not any(path == ("main",) for path, _ in worker._build_schema_registry())


def test_no_default_keeps_ambiguous_function_names_qualified() -> None:
    class OtherProbe(Probe):
        pass

    class CatalogWorker(Worker):
        catalog = Catalog(
            name="scoped",
            default_schema=None,
            schemas=[Schema(path=["first"], functions=[Probe]), Schema(path=["second"], functions=[OtherProbe])],
        )

    worker = CatalogWorker(quiet=True)
    assert worker._resolve_function_by_name("probe", schema_path=["first"]) is Probe
    assert worker._resolve_function_by_name("probe", schema_path=["second"]) is OtherProbe
    with pytest.raises(ValueError, match="[Aa]mbiguous|multiple|schema"):
        worker._resolve_function(
            BindRequest(function_name="probe", arguments=Arguments(), function_type=FunctionType.TABLE)
        )
