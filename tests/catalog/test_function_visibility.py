# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Protocol 3.3 ``FunctionInfo.visibility`` and the table-function listing rule.

Every function a table names is listed so the client has its capabilities;
write functions are always ``table_only``; a scan is ``table_only`` unless the
table sets ``expose_scan`` or the author registers it as an ordinary function.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from vgi.catalog import (
    FUNCTION_VISIBILITIES,
    FUNCTION_VISIBILITY_PUBLIC,
    FUNCTION_VISIBILITY_TABLE_ONLY,
    AttachOpaqueData,
    Catalog,
    FunctionInfo,
    ReadOnlyCatalogInterface,
    Schema,
    SchemaObjectType,
    Table,
)
from vgi.catalog.catalog_interface import FunctionType, SerializedSchema
from vgi.invocation import BindResponse
from vgi.table_function import BindParams, TableFunctionGenerator
from vgi.table_in_out_function import TableInOutGenerator
from vgi.write_results import row_id_field

_FIELDS = [pa.field("id", pa.int64()), row_id_field()]


def _scan(function_name: str) -> type[TableFunctionGenerator[None, None]]:
    class Scan(TableFunctionGenerator[None, None]):
        class Meta:
            name = function_name
            projection_pushdown = True

        @classmethod
        def on_bind(cls, params: BindParams[None]) -> BindResponse:
            del params
            return BindResponse(output_schema=pa.schema(_FIELDS))

    return Scan


def _writer(function_name: str) -> type[TableInOutGenerator[None, None]]:
    class Writer(TableInOutGenerator[None, None]):
        class Meta:
            name = function_name

    return Writer


class _Ordinary(TableFunctionGenerator[None, None]):
    class Meta:
        name = "ordinary"

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        del params
        return BindResponse(output_schema=pa.schema([pa.field("x", pa.int64())]))


def _catalog_interface(catalog: Catalog) -> ReadOnlyCatalogInterface:
    class _Cat(ReadOnlyCatalogInterface):
        pass

    _Cat.catalog = catalog
    return _Cat()


def _listing(cat: ReadOnlyCatalogInterface, path: list[str]) -> dict[str, str | None]:
    infos = cat.schema_contents(
        attach_opaque_data=AttachOpaqueData(b""),
        transaction_opaque_data=None,
        path=path,
        type=SchemaObjectType.TABLE_FUNCTION,
    )
    result: dict[str, str | None] = {}
    for info in infos:
        assert isinstance(info, FunctionInfo)
        assert info.name not in result, f"{info.name} listed twice"
        result[info.name] = info.visibility
    return result


def test_wire_field_is_a_nullable_plain_utf8_string() -> None:
    """``visibility`` is a nullable utf8 column, not a dictionary/enum type, and round-trips."""
    field = FunctionInfo.ARROW_SCHEMA.field("visibility")
    assert field.type == pa.string()
    assert field.nullable
    assert FunctionInfo.ARROW_SCHEMA.names[-1] == "visibility"
    empty = SerializedSchema(pa.schema([]).serialize().to_pybytes())
    for value in (None, FUNCTION_VISIBILITY_PUBLIC, FUNCTION_VISIBILITY_TABLE_ONLY):
        info = FunctionInfo(
            name="f",
            schema_path=["main"],
            function_type=FunctionType.TABLE,
            arguments=empty,
            output_schema=empty,
            comment=None,
            tags={},
            visibility=value,
        )
        assert FunctionInfo.deserialize_from_bytes(info.serialize_to_bytes()).visibility == value
    assert {"public", "table_only"} == FUNCTION_VISIBILITIES


def test_unlisted_scan_and_write_functions_are_listed_table_only() -> None:
    """The scan and write functions a table names are listed as table_only without registration."""
    scan, insert, update, delete = _scan("t_scan"), _writer("t_insert"), _writer("t_update"), _writer("t_delete")
    cat = _catalog_interface(
        Catalog(
            name="c",
            schemas=[
                Schema(
                    path=["main"],
                    functions=[_Ordinary],
                    tables=[
                        Table(
                            name="t",
                            function=scan,
                            insert_function=insert,
                            update_function=update,
                            delete_function=delete,
                        )
                    ],
                )
            ],
        )
    )
    assert _listing(cat, ["main"]) == {
        "ordinary": None,
        "t_scan": FUNCTION_VISIBILITY_TABLE_ONLY,
        "t_insert": FUNCTION_VISIBILITY_TABLE_ONLY,
        "t_update": FUNCTION_VISIBILITY_TABLE_ONLY,
        "t_delete": FUNCTION_VISIBILITY_TABLE_ONLY,
    }
    # The listed scan carries its capabilities: what the client needs for rowid.
    infos = cat.schema_contents(
        attach_opaque_data=AttachOpaqueData(b""),
        transaction_opaque_data=None,
        path=["main"],
        type=SchemaObjectType.TABLE_FUNCTION,
    )
    assert next(i for i in infos if i.name == "t_scan").projection_pushdown is True


def test_expose_scan_makes_the_scan_public_but_never_a_write_function() -> None:
    """expose_scan publishes the scan; write functions stay table_only."""
    scan, update = _scan("t_scan"), _writer("t_update")
    cat = _catalog_interface(
        Catalog(
            name="c",
            schemas=[
                Schema(path=["main"], tables=[Table(name="t", function=scan, update_function=update, expose_scan=True)])
            ],
        )
    )
    assert _listing(cat, ["main"]) == {"t_scan": None, "t_update": FUNCTION_VISIBILITY_TABLE_ONLY}


def test_scan_registered_as_ordinary_function_stays_public() -> None:
    """A scan the author registers in the schema's functions keeps its public listing (once)."""
    scan, update = _scan("t_scan"), _writer("t_update")
    cat = _catalog_interface(
        Catalog(
            name="c",
            schemas=[
                Schema(path=["main"], functions=[scan], tables=[Table(name="t", function=scan, update_function=update)])
            ],
        )
    )
    assert _listing(cat, ["main"]) == {"t_scan": None, "t_update": FUNCTION_VISIBILITY_TABLE_ONLY}


def test_registered_write_function_is_still_table_only() -> None:
    """Registering a write function as an ordinary function does not make it callable."""
    scan, insert = _scan("t_scan"), _writer("t_insert")
    cat = _catalog_interface(
        Catalog(
            name="c",
            schemas=[
                Schema(
                    path=["main"], functions=[insert], tables=[Table(name="t", function=scan, insert_function=insert)]
                )
            ],
        )
    )
    assert _listing(cat, ["main"]) == {
        "t_scan": FUNCTION_VISIBILITY_TABLE_ONLY,
        "t_insert": FUNCTION_VISIBILITY_TABLE_ONLY,
    }


def test_scan_shared_by_hidden_and_exposed_tables_is_public() -> None:
    """One table exposing a shared scan makes it public; it is listed once."""
    scan = _scan("shared_scan")
    cat = _catalog_interface(
        Catalog(
            name="c",
            schemas=[
                Schema(
                    path=["main"],
                    tables=[Table(name="a", function=scan), Table(name="b", function=scan, expose_scan=True)],
                )
            ],
        )
    )
    assert _listing(cat, ["main"]) == {"shared_scan": None}


def test_scan_registered_in_another_schema_is_not_duplicated() -> None:
    """A scan registered elsewhere is resolved there; the table's schema gets no copy."""
    scan = _scan("t_scan")
    cat = _catalog_interface(
        Catalog(
            name="c",
            default_schema="main",
            schemas=[
                Schema(path=["main"], functions=[scan]),
                Schema(path=["data"], tables=[Table(name="t", function=scan)]),
            ],
        )
    )
    assert _listing(cat, ["main"]) == {"t_scan": None}
    assert _listing(cat, ["data"]) == {}
    result = cat.table_scan_function_get(
        attach_opaque_data=AttachOpaqueData(b""),
        transaction_opaque_data=None,
        schema_path=["data"],
        name="t",
        at_unit=None,
        at_value=None,
    )
    assert result.schema_path == ["main"]


def test_auto_listed_scan_resolves_in_the_table_schema() -> None:
    """The redirect of an auto-listed scan claims the table's schema, where it is listed."""
    scan, update = _scan("t_scan"), _writer("t_update")
    cat = _catalog_interface(
        Catalog(
            name="c",
            default_schema="main",
            schemas=[
                Schema(path=["main"]),
                Schema(path=["data"], tables=[Table(name="t", function=scan, update_function=update)]),
            ],
        )
    )
    assert _listing(cat, ["data"]) == {
        "t_scan": FUNCTION_VISIBILITY_TABLE_ONLY,
        "t_update": FUNCTION_VISIBILITY_TABLE_ONLY,
    }
    common = {"attach_opaque_data": AttachOpaqueData(b""), "transaction_opaque_data": None, "schema_path": ["data"]}
    scan_result = cat.table_scan_function_get(**common, name="t", at_unit=None, at_value=None)  # type: ignore[arg-type]
    assert (scan_result.function_name, scan_result.schema_path) == ("t_scan", ["data"])
    update_result = cat.table_update_function_get(**common, name="t")  # type: ignore[arg-type]
    assert (update_result.function_name, update_result.schema_path) == ("t_update", ["data"])


def test_schema_object_counts_include_table_functions() -> None:
    """A schema whose only functions are its tables' does not report table_function=0."""
    scan, update = _scan("t_scan"), _writer("t_update")
    schema = Schema(path=["main"], tables=[Table(name="t", function=scan, update_function=update)])
    counts = schema.to_schema_info(AttachOpaqueData(b"")).estimated_object_count
    assert counts is not None
    assert counts["table_function"] == 2


def test_expose_scan_requires_a_function() -> None:
    """expose_scan on a columns= table is rejected: there is no scan class to publish."""
    with pytest.raises(ValueError, match="expose_scan=True requires function="):
        Table(name="t", columns=pa.schema(_FIELDS), expose_scan=True)


def test_txfix_fixture_contract() -> None:
    """The txfix fixture: items_scan and its three write functions hidden, labels_scan exposed."""
    from vgi._test_fixtures.writable_tx import TxfixCatalog

    hidden = {"items_scan", "items_insert", "items_update", "items_delete"}
    listing = _listing(TxfixCatalog(), ["main"])
    for name in hidden:
        assert listing[name] == FUNCTION_VISIBILITY_TABLE_ONLY
    assert listing["labels_scan"] is None
    assert listing["writes"] is None
    assert {name for name, vis in listing.items() if vis is not None} == hidden
