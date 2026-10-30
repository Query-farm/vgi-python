# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103

"""Declarative constraint records: builder, DuckDB's ATTACH rules and negotiation.

The DuckDB extension rejects a table at ATTACH unless (a) the constraint read
profile was negotiated, (b) every record is ENFORCED + ALL_ROWS and (c) Arrow
nullability equals membership in the NOT_NULL/PRIMARY_KEY paths. These tests
pin that a declarative catalog satisfies (a) with no hand-written capability
override and that (b) and (c) fail in the worker's own process.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest
from vgi_rpc.http._testing import _SyncTestClient

import vgi.catalog
from vgi import Worker
from vgi.catalog import (
    Catalog,
    ConstraintEnforcement,
    ConstraintInfo,
    ConstraintKind,
    ConstraintValidation,
    ReadOnlyCatalogInterface,
    ReferencedKey,
    Schema,
    Table,
    constraint_record,
)
from vgi.catalog.capabilities import CONSTRAINT_RECORDS_READ, ROW_SCALAR_EXPRESSION, CatalogCapabilities
from vgi.catalog.catalog_interface import CatalogAttachResult, SerializedSchema, TableInfo
from vgi.client import Client
from vgi.column_path import RequiredFilterPaths
from vgi.protocol import ClientCapabilities
from vgi.serve import create_app

KEY = b"constraint-records-signing-key"
PROFILES = [CONSTRAINT_RECORDS_READ, ROW_SCALAR_EXPRESSION]
TRUSTED: dict[str, Any] = {"enforcement": "ENFORCED", "validation": "ALL_ROWS"}


def _offer(read: list[str] | None) -> ClientCapabilities:
    return ClientCapabilities(
        engine="duckdb",
        native_formats=[],
        catalogs=[],
        can_stream=False,
        filter_encodings=[],
        catalog=None if read is None else CatalogCapabilities(read=read),
    )


def _record(name: str, kind: str, columns: list[list[str]], **kwargs: Any) -> ConstraintInfo:
    return constraint_record(["main"], "places", name, kind, columns, **{**TRUSTED, **kwargs})


BBOX = pa.struct([pa.field("xmin", pa.float64(), nullable=False), pa.field("ymax", pa.float64())])
COLUMNS = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("bbox", BBOX),
        pa.field("population", pa.int64()),
    ]
)
RECORDS = (
    _record("places.pk", "PRIMARY_KEY", [["id"]]),
    _record("places.bbox.xmin", "NOT_NULL", [["bbox", "xmin"]]),
    _record("places.population", "CHECK", [], check_sql="population >= 0"),
)


class _PlacesCatalog(ReadOnlyCatalogInterface):
    catalog = Catalog(
        name="places",
        default_schema="main",
        schemas=[Schema(path=["main"], tables=[Table(name="places", columns=COLUMNS, constraints=RECORDS)])],
    )


class _PlacesWorker(Worker):
    catalog_interface = _PlacesCatalog


class _DroppingAttachCatalog(_PlacesCatalog):
    """An attach override that drops the client's offer must still negotiate."""

    def catalog_attach(self, **kwargs: Any) -> CatalogAttachResult:
        kwargs["client_capabilities"] = None
        return super().catalog_attach(**kwargs)


class _DroppingAttachWorker(Worker):
    catalog_interface = _DroppingAttachCatalog


class _PlainCatalog(ReadOnlyCatalogInterface):
    catalog = Catalog(
        name="plain",
        default_schema="main",
        schemas=[Schema(path=["main"], tables=[Table(name="t", columns=pa.schema([("a", pa.int64())]))])],
    )


def _client(worker: type[Worker]) -> Client:
    return Client.from_http(
        "http://vgi.test", httpx_client=_SyncTestClient(create_app(worker, describe=False, signing_key=KEY))
    )


# --- (a) negotiation without a hand-written catalog_capabilities override ---


@pytest.mark.parametrize("worker", [_PlacesWorker, _DroppingAttachWorker])
def test_declarative_records_negotiate_when_offered(worker: type[Worker]) -> None:
    with _client(worker) as client:
        attach = client.catalog_attach(
            name="places",
            data_version_spec=None,
            implementation_version=None,
            client_capabilities=_offer([*PROFILES, "vgi.index.definition.read.v1"]),
        )
        assert attach.catalog_capabilities == CatalogCapabilities(read=sorted(PROFILES))
        assert attach.selected_capabilities == CatalogCapabilities(read=sorted(PROFILES))
        info = client.table_get(attach_opaque_data=attach.attach_opaque_data, schema_path=["main"], name="places")
        assert info is not None
        assert info.constraint_infos() == list(RECORDS)


@pytest.mark.parametrize("read", [None, [], [ROW_SCALAR_EXPRESSION], [CONSTRAINT_RECORDS_READ]])
def test_records_are_not_served_without_their_profiles(read: list[str] | None) -> None:
    with _client(_PlacesWorker) as client:
        attach = client.catalog_attach(
            name="places", data_version_spec=None, implementation_version=None, client_capabilities=_offer(read)
        )
        with pytest.raises(Exception, match="requires negotiated capability"):
            client.table_get(attach_opaque_data=attach.attach_opaque_data, schema_path=["main"], name="places")


def test_catalog_without_records_advertises_nothing() -> None:
    catalog = _PlainCatalog()
    assert catalog.catalog_capabilities(attach_opaque_data=catalog._FIXED_ATTACH_ID) == CatalogCapabilities()
    assert _PlacesCatalog().catalog_capabilities(attach_opaque_data=catalog._FIXED_ATTACH_ID) == CatalogCapabilities(
        read=sorted(PROFILES)
    )


# --- (b) only ENFORCED + ALL_ROWS ---


@pytest.mark.parametrize(
    ("enforcement", "validation"),
    [("METADATA_ONLY", "ALL_ROWS"), ("ENFORCED", "FUTURE_WRITES"), ("ENFORCED", "UNVALIDATED")],
)
def test_untrusted_records_fail_at_definition(enforcement: str, validation: str) -> None:
    record = _record("places.pop.unique", "UNIQUE", [["population"]], enforcement=enforcement, validation=validation)
    with pytest.raises(
        ValueError,
        match=f"Table 'places': constraint 'places.pop.unique' is {enforcement}/{validation}; "
        r"DuckDB accepts only ENFORCED \+ ALL_ROWS",
    ):
        Table(name="places", columns=COLUMNS, constraints=(*RECORDS, record))


def test_untrusted_records_fail_when_served() -> None:
    record = _record("places.pop.unique", "UNIQUE", [["population"]], enforcement="METADATA_ONLY")
    info = TableInfo(
        name="places",
        schema_path=["main"],
        comment=None,
        tags={},
        columns=SerializedSchema(COLUMNS.serialize().to_pybytes()),
        constraints=[item.serialize_to_bytes() for item in (*RECORDS, record)],
    )
    with pytest.raises(ValueError, match="accepts only ENFORCED"):
        Worker._table_for_capabilities(info, CatalogCapabilities(read=sorted(PROFILES)))


def test_record_for_another_table_fails_at_definition() -> None:
    other = constraint_record(["main"], "other", "pk", "PRIMARY_KEY", [["id"]], **TRUSTED)
    with pytest.raises(ValueError, match="Table 'places': constraint 'pk' belongs to table 'other'"):
        Table(name="places", columns=COLUMNS, constraints=(other, *RECORDS[1:]))


# --- (c) Arrow nullability equals NOT_NULL/PRIMARY_KEY membership ---


def test_non_nullable_field_without_record_fails_even_without_constraints() -> None:
    with pytest.raises(ValueError, match=r"Table 't': .*column \['id'\]: the field is non-nullable but no"):
        Table(name="t", columns=pa.schema([pa.field("id", pa.int64(), nullable=False)]))


def test_nullable_field_with_record_fails() -> None:
    record = constraint_record(["main"], "t", "nn", "NOT_NULL", [["id"]], **TRUSTED)
    with pytest.raises(ValueError, match=r"Table 't': .*column \['id'\]: the field is nullable but"):
        Table(name="t", columns=pa.schema([("id", pa.int64())]), constraints=(record,))


def test_nested_struct_child_nullability_is_checked_both_ways() -> None:
    with pytest.raises(ValueError, match=r"column \['bbox', 'xmin'\]: the field is non-nullable but no"):
        Table(name="places", columns=COLUMNS, constraints=(RECORDS[0], RECORDS[2]))
    nullable_child = pa.schema([COLUMNS.field("id"), pa.field("bbox", pa.struct([("xmin", pa.float64())]))])
    with pytest.raises(ValueError, match=r"column \['bbox', 'xmin'\]: the field is nullable but"):
        Table(name="places", columns=nullable_child, constraints=RECORDS[:2])


def test_nested_mismatch_fails_when_served() -> None:
    info = TableInfo(
        name="places",
        schema_path=["main"],
        comment=None,
        tags={},
        columns=SerializedSchema(COLUMNS.serialize().to_pybytes()),
        constraints=[RECORDS[0].serialize_to_bytes()],
    )
    with pytest.raises(ValueError, match=r"Table 'places': Arrow nullability disagrees .* \['bbox', 'xmin'\]"):
        Worker._table_for_capabilities(info, CatalogCapabilities(read=sorted(PROFILES)))


@pytest.mark.parametrize(
    "metadata", [{b"ARROW:extension:name": b"example.point"}, {b"vgi.logical_type.name": b"tuple"}]
)
def test_logical_type_storage_members_are_not_column_paths(metadata: dict[bytes, bytes]) -> None:
    storage = pa.struct([pa.field("x", pa.float64(), nullable=False)])
    Table(name="t", columns=pa.schema([pa.field("point", storage, metadata=metadata)]))


# --- builder ---


def test_builder_round_trips_every_kind() -> None:
    fk = constraint_record(
        ["main"],
        "visits",
        "visits.place.fk",
        ConstraintKind.FOREIGN_KEY,
        [["place_id"]],
        enforcement=ConstraintEnforcement.ENFORCED,
        validation=ConstraintValidation.ALL_ROWS,
        references=ReferencedKey(schema_path=["main"], table="places", columns=[["id"]], constraint_id="places.pk"),
        declared_name="visits_place_fk",
    )
    for record in (*RECORDS, fk):
        assert ConstraintInfo.deserialize_from_bytes(record.serialize_to_bytes()) == record
        assert record.trusted
    check = RECORDS[2].definition.check_expression
    assert check is not None
    assert (check.sql, check.profile, check.schema_path) == ("population >= 0", ROW_SCALAR_EXPRESSION, ["main"])
    assert RECORDS[1].definition.column_paths == [["bbox", "xmin"]]
    assert (RECORDS[1].ref.owner_table, RECORDS[1].ref.name) == ("places", "places.bbox.xmin")
    assert fk.definition.declared_name == "visits_place_fk"
    assert fk.referenced_constraint is not None
    assert (fk.referenced_constraint.owner_table, fk.referenced_constraint.name) == ("places", "places.pk")
    assert fk.definition.foreign_key is not None
    assert fk.definition.foreign_key.referenced_column_paths == [["id"]]


@pytest.mark.parametrize(
    ("kind", "columns", "extra", "error", "match"),
    [
        ("CHECK", [], {}, ValueError, "check_sql is required for CHECK"),
        ("NOT_NULL", [["id"]], {"check_sql": "id > 0"}, ValueError, "check_sql is required for CHECK"),
        ("FOREIGN_KEY", [["id"]], {}, ValueError, "references is required"),
        ("NOT_NULL", ["id"], {}, TypeError, "raw component paths"),
        ("NOT_NULL", [["a"], ["b"]], {}, ValueError, "exactly one column path"),
        ("UNIQUE", [], {}, ValueError, "required arity"),
    ],
)
def test_builder_rejects_malformed_records(
    kind: str, columns: Any, extra: dict[str, Any], error: type[Exception], match: str
) -> None:
    with pytest.raises(error, match=match):
        constraint_record(["main"], "t", "c", kind, columns, **TRUSTED, **extra)


def test_builder_requires_explicit_trust_state() -> None:
    with pytest.raises(TypeError, match="enforcement"):
        constraint_record(["main"], "t", "c", "NOT_NULL", [["id"]], validation="ALL_ROWS")  # type: ignore[call-arg]


def test_required_filter_paths_is_public() -> None:
    assert vgi.catalog.RequiredFilterPaths is RequiredFilterPaths
    schema = pa.schema([("ticker", pa.string())])
    assert RequiredFilterPaths.from_legacy([["ticker"]], schema=schema).groups == [[["ticker"]]]
