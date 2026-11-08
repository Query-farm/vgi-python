# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""ReadOnlyCatalogInterface attach hooks, transactions, and writable-table validation."""

from __future__ import annotations

import os
from typing import Annotated, Any, Optional

import pyarrow as pa
import pytest

from vgi.catalog import (
    AttachOpaqueData,
    Catalog,
    ReadOnlyCatalogInterface,
    Schema,
    Table,
    TransactionOpaqueData,
    pack_attach_options,
    unpack_attach_options,
)
from vgi.catalog.attach_option import AttachOption, extract_attach_option_specs
from vgi.exceptions import CatalogReadOnlyError
from vgi.invocation import BindResponse
from vgi.table_function import BindParams, TableFunctionGenerator
from vgi.table_in_out_function import TableInOutGenerator
from vgi.write_results import row_id_field


def _attach(catalog: ReadOnlyCatalogInterface, **options: Any) -> Any:
    return catalog.catalog_attach(
        name=catalog._effective_catalog_name, options=options, data_version_spec=None, implementation_version=None
    )


class _Plain(ReadOnlyCatalogInterface):
    catalog_name = "plain"


class _Options:
    transactional: Annotated[bool, AttachOption(desc="tx")] = False
    token: Annotated[str | None, AttachOption(desc="token", secret=True)] = None


_SPECS = extract_attach_option_specs(_Options)


class _PerAttach(ReadOnlyCatalogInterface):
    catalog_name = "per_attach"
    attach_option_specs = _SPECS

    def attach_opaque_data_for(self, *, name: str, options: dict[str, Any]) -> AttachOpaqueData:
        return AttachOpaqueData(pack_attach_options(options, _SPECS))

    def attach_supports_transactions(
        self, *, name: str, options: dict[str, Any], attach_opaque_data: AttachOpaqueData
    ) -> bool:
        return unpack_attach_options(attach_opaque_data)["transactional"] is True

    def catalog_transaction_begin(self, *, attach_opaque_data: AttachOpaqueData) -> TransactionOpaqueData | None:
        return TransactionOpaqueData(os.urandom(16))

    def catalog_transaction_commit(
        self, *, attach_opaque_data: AttachOpaqueData, transaction_opaque_data: TransactionOpaqueData
    ) -> None:
        return None

    def catalog_transaction_rollback(
        self, *, attach_opaque_data: AttachOpaqueData, transaction_opaque_data: TransactionOpaqueData
    ) -> None:
        return None


class TestAttachDefaults:
    """The default attach: fixed id, no transactions, not required."""

    def test_default_attach_is_stateless(self) -> None:
        """Default attach is stateless."""
        result = _attach(_Plain())
        assert result.attach_opaque_data == ReadOnlyCatalogInterface._FIXED_ATTACH_ID
        assert result.supports_transactions is False
        assert result.attach_opaque_data_required is False

    def test_transaction_hooks_raise_by_default(self) -> None:
        """Transaction hooks raise by default."""
        with pytest.raises(CatalogReadOnlyError):
            _Plain().catalog_transaction_begin(attach_opaque_data=AttachOpaqueData(b"x"))

    def test_class_attribute_without_hooks_is_refused(self) -> None:
        """Class attribute without hooks is refused."""

        class Broken(_Plain):
            supports_transactions = True

        with pytest.raises(TypeError, match="catalog_transaction_begin"):
            _attach(Broken())


class TestPerAttach:
    """attach_opaque_data_for / attach_supports_transactions decide per ATTACH."""

    def test_options_reach_the_attach_bytes(self) -> None:
        """Options reach the attach bytes."""
        result = _attach(_PerAttach(), token="tok-1", TRANSACTIONAL=True)
        # The secret token is never packed into the catalog's own bytes.
        assert unpack_attach_options(result.attach_opaque_data) == {"transactional": True}
        assert b"tok-1" not in result.attach_opaque_data
        assert result.supports_transactions is True
        # Custom bytes carry state: the client must keep them.
        assert result.attach_opaque_data_required is True

    def test_transactions_only_when_asked(self) -> None:
        """Transactions only when asked."""
        result = _attach(_PerAttach(), token="tok-1")
        assert result.supports_transactions is False
        assert unpack_attach_options(result.attach_opaque_data) == {"transactional": False}

    def test_supports_transactions_class_attribute_still_works(self) -> None:
        """Supports transactions class attribute still works."""

        class AlwaysTx(_PerAttach):
            supports_transactions = True

            def attach_supports_transactions(self, **kwargs: Any) -> bool:
                return ReadOnlyCatalogInterface.attach_supports_transactions(self, **kwargs)

        assert _attach(AlwaysTx()).supports_transactions is True


class TestPackAttachOptions:
    """pack_attach_options / unpack_attach_options round-trip."""

    def test_untyped_round_trip(self) -> None:
        """Untyped round trip."""
        packed = pack_attach_options({"A": 1, "b": "x", "c": None, "d": [1, 2]})
        assert unpack_attach_options(packed) == {"a": 1, "b": "x", "c": None, "d": [1, 2]}

    def test_specs_type_and_default(self) -> None:
        """Specs type the options and fill defaults; secret options are dropped."""
        packed = pack_attach_options({"token": "t"}, _SPECS)
        assert unpack_attach_options(packed) == {"transactional": False}

    def test_secret_options_never_packed(self) -> None:
        """A secret option never enters the catalog's own bytes."""
        packed = pack_attach_options({"token": "s3cr3t-value", "transactional": True}, _SPECS)
        assert b"s3cr3t-value" not in packed
        assert unpack_attach_options(packed) == {"transactional": True}
        # Without specs nothing is known to be secret.
        assert b"plain-token" in pack_attach_options({"token": "plain-token"})

    def test_empty_and_foreign_bytes(self) -> None:
        """Empty and foreign bytes."""
        assert unpack_attach_options(pack_attach_options({})) == {}
        assert unpack_attach_options(None) == {}
        assert unpack_attach_options(ReadOnlyCatalogInterface._FIXED_ATTACH_ID) == {}


class TestOptionalAttachOptionType:
    """``T | None`` resolves to T's Arrow type (and Optional[T])."""

    def test_union_none(self) -> None:
        """Union none."""
        specs = {s.name: s for s in _SPECS}
        assert specs["token"].type == pa.string()
        assert specs["token"].default is None
        assert specs["token"].required is False
        assert specs["token"].secret is True

    def test_optional_and_reversed_order(self) -> None:
        """Optional and reversed order."""

        class Opts:
            a: Annotated[Optional[int], AttachOption(desc="a")] = None  # noqa: UP045 - testing the typing form
            b: Annotated[None | float, AttachOption(desc="b")] = None

        types = {s.name: s.type for s in extract_attach_option_specs(Opts)}
        assert types == {"a": pa.int64(), "b": pa.float64()}

    def test_wider_union_still_rejected(self) -> None:
        """Wider union still rejected."""

        class Opts:
            a: Annotated[int | str, AttachOption(desc="a")] = 1

        with pytest.raises(TypeError, match="Cannot resolve Arrow type"):
            extract_attach_option_specs(Opts)

    def test_omitted_default_is_optional_not_required(self) -> None:
        """Omitted default is optional not required."""

        class Opts:
            a: Annotated[str, AttachOption(desc="a")]

        (spec,) = extract_attach_option_specs(Opts)
        assert spec.required is False and spec.default is None


# ---------------------------------------------------------------------------
# Writable-table validation (row-id scan requirements)
# ---------------------------------------------------------------------------


class _Update(TableInOutGenerator[None, None]):
    class Meta:
        name = "t_update"


def _scan(*, pushdown: bool, rowid: bool) -> type[TableFunctionGenerator[None, None]]:
    fields = [pa.field("id", pa.int64())] + ([row_id_field()] if rowid else [])

    class Scan(TableFunctionGenerator[None, None]):
        class Meta:
            name = "t_scan"
            projection_pushdown = pushdown

        @classmethod
        def on_bind(cls, params: BindParams[None]) -> BindResponse:
            del params
            return BindResponse(output_schema=pa.schema(fields))

    return Scan


class TestRowIdScanValidation:
    """Table/Catalog reject UPDATE/DELETE tables DuckDB could never write."""

    def test_valid_table(self) -> None:
        """Valid table."""
        scan = _scan(pushdown=True, rowid=True)
        table = Table(name="t", function=scan, update_function=_Update)
        Catalog(name="c", schemas=[Schema(path=["main"], functions=[scan], tables=[table])])

    def test_missing_row_id_column(self) -> None:
        """Missing row id column."""
        with pytest.raises(ValueError, match="row-id column"):
            Table(name="t", function=_scan(pushdown=True, rowid=False), update_function=_Update)

    def test_missing_projection_pushdown(self) -> None:
        """Missing projection pushdown."""
        with pytest.raises(ValueError, match="projection pushdown"):
            Table(name="t", function=_scan(pushdown=False, rowid=True), delete_function=_Update)

    def test_scan_not_listed_in_schema(self) -> None:
        """Scan not listed in schema."""
        table = Table(name="t", function=_scan(pushdown=True, rowid=True), update_function=_Update)
        with pytest.raises(ValueError, match="must also be listed in the functions"):
            Catalog(name="c", schemas=[Schema(path=["main"], tables=[table])])

    def test_read_only_and_insert_only_tables_unaffected(self) -> None:
        """Read only and insert only tables unaffected."""
        scan = _scan(pushdown=False, rowid=False)
        Catalog(
            name="c",
            schemas=[
                Schema(
                    path=["main"],
                    tables=[Table(name="r", function=scan), Table(name="i", function=scan, insert_function=_Update)],
                )
            ],
        )
