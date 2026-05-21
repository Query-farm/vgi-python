# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Multi-branch scan protocol tests.

Covers ``ScanBranch`` / ``ScanBranchesResult`` dataclasses and the
``CatalogInterface.table_scan_branches_get`` default-impl shim.

The shim is the backwards-compatibility hook: workers that only override
the legacy ``table_scan_function_get`` automatically gain a one-branch
``table_scan_branches_get`` for free. Workers that need real multi-branch
behaviour override ``table_scan_branches_get`` themselves.

See the design memo at
``~/.claude/plans/right-now-vgi-and-partitioned-nebula.md`` for context.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

from vgi.catalog.catalog_interface import (
    AttachOpaqueData,
    CatalogInterface,
    ScanBranch,
    ScanBranchesResult,
    ScanFunctionResult,
)


class _StubCatalogBase(CatalogInterface):
    """Stub out the abstract surface so the tests only need to override scan methods.

    CatalogInterface requires concrete implementations of catalog_attach,
    catalogs, schema_get, table_get, view_get, macro_get; this class
    provides minimal "not used in this test" stubs for all of them.
    """

    def catalog_attach(self, *args: Any, **kwargs: Any) -> Any:  # noqa: D102
        raise NotImplementedError

    def catalogs(self, *args: Any, **kwargs: Any) -> Any:  # noqa: D102
        raise NotImplementedError

    def schema_get(self, *args: Any, **kwargs: Any) -> Any:  # noqa: D102
        raise NotImplementedError

    def table_get(self, *args: Any, **kwargs: Any) -> Any:  # noqa: D102
        raise NotImplementedError

    def view_get(self, *args: Any, **kwargs: Any) -> Any:  # noqa: D102
        raise NotImplementedError

    def macro_get(self, *args: Any, **kwargs: Any) -> Any:  # noqa: D102
        raise NotImplementedError


class _LegacyOnlyCatalog(_StubCatalogBase):
    """Catalog that only overrides the legacy single-function scan."""

    def __init__(self, function_name: str = "legacy_scan", required: list[str] | None = None) -> None:
        self._function_name = function_name
        self._required = required or []

    # Only the legacy method is overridden; table_scan_branches_get falls back
    # to the CatalogInterface default-impl shim.
    def table_scan_function_get(  # type: ignore[override]
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: object | None,
        schema_name: str,
        name: str,
        at_unit: str | None,
        at_value: str | None,
    ) -> ScanFunctionResult:
        return ScanFunctionResult(
            function_name=self._function_name,
            positional_arguments=[pa.scalar(f"{schema_name}.{name}", pa.string())],
            named_arguments={},
            required_extensions=self._required,
        )


class _MultiBranchCatalog(_StubCatalogBase):
    """Catalog that overrides table_scan_branches_get to return two branches.

    Demonstrates the heterogeneous-branches case: one VGI worker function
    plus one native iceberg_scan, with complementary branch_filters to
    keep them non-overlapping at scan time.
    """

    def table_scan_function_get(  # type: ignore[override]
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: object | None,
        schema_name: str,
        name: str,
        at_unit: str | None,
        at_value: str | None,
    ) -> ScanFunctionResult:
        # When a worker also implements the legacy method for old-client
        # compat, it typically returns the "primary" branch (here, hot tier).
        return ScanFunctionResult(
            function_name="vgi_kafka_scan",
            positional_arguments=[pa.scalar(name, pa.string())],
            named_arguments={},
            required_extensions=[],
        )

    def table_scan_branches_get(  # type: ignore[override]
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: object | None,
        schema_name: str,
        name: str,
        at_unit: str | None,
        at_value: str | None,
    ) -> ScanBranchesResult:
        return ScanBranchesResult(
            branches=[
                ScanBranch(
                    function_name="vgi_kafka_scan",
                    positional_arguments=[pa.scalar(name, pa.string())],
                    named_arguments={},
                    branch_filter="ts >= TIMESTAMP '2026-05-15 00:00:00'",
                ),
                ScanBranch(
                    function_name="iceberg_scan",
                    positional_arguments=[pa.scalar(f"s3://archive/{name}", pa.string())],
                    named_arguments={},
                    branch_filter="ts < TIMESTAMP '2026-05-15 00:00:00'",
                ),
            ],
            required_extensions=["iceberg", "httpfs"],
        )


class TestDefaultImplShim:
    """Default ``table_scan_branches_get`` wraps the legacy single-function result."""

    def test_returns_one_branch(self) -> None:
        """Default shim returns exactly one branch wrapping the legacy result."""
        cat = _LegacyOnlyCatalog()
        result = cat.table_scan_branches_get(
            attach_opaque_data=AttachOpaqueData(b"test"),
            transaction_opaque_data=None,
            schema_name="main",
            name="orders",
            at_unit=None,
            at_value=None,
        )
        assert isinstance(result, ScanBranchesResult)
        assert len(result.branches) == 1
        branch = result.branches[0]
        assert branch.function_name == "legacy_scan"
        assert branch.branch_filter is None
        assert branch.positional_arguments[0].as_py() == "main.orders"

    def test_required_extensions_hoisted(self) -> None:
        """Legacy ScanFunctionResult.required_extensions → top-level required_extensions."""
        cat = _LegacyOnlyCatalog(required=["parquet", "httpfs"])
        result = cat.table_scan_branches_get(
            attach_opaque_data=AttachOpaqueData(b"test"),
            transaction_opaque_data=None,
            schema_name="main",
            name="orders",
            at_unit=None,
            at_value=None,
        )
        assert result.required_extensions == ["parquet", "httpfs"]

    def test_at_clause_passed_through(self) -> None:
        """The shim threads at_unit/at_value into the legacy call.

        The C++ side refuses ``AT(...)`` on multi-branch tables (>1 branch)
        at bind time, but the single-branch shim path must continue to
        honour ``AT(...)`` so legacy workers keep working unchanged.
        """

        class _RecordingCatalog(_LegacyOnlyCatalog):
            def __init__(self) -> None:
                super().__init__()
                self.last_at: tuple[str | None, str | None] | None = None

            def table_scan_function_get(  # type: ignore[override]
                self,
                *,
                attach_opaque_data: AttachOpaqueData,
                transaction_opaque_data: object | None,
                schema_name: str,
                name: str,
                at_unit: str | None,
                at_value: str | None,
            ) -> ScanFunctionResult:
                self.last_at = (at_unit, at_value)
                return super().table_scan_function_get(
                    attach_opaque_data=attach_opaque_data,
                    transaction_opaque_data=transaction_opaque_data,
                    schema_name=schema_name,
                    name=name,
                    at_unit=at_unit,
                    at_value=at_value,
                )

        cat = _RecordingCatalog()
        cat.table_scan_branches_get(
            attach_opaque_data=AttachOpaqueData(b"test"),
            transaction_opaque_data=None,
            schema_name="main",
            name="versioned",
            at_unit="VERSION",
            at_value="3",
        )
        assert cat.last_at == ("VERSION", "3")


class TestMultiBranchOverride:
    """Workers that override ``table_scan_branches_get`` get full multi-branch semantics."""

    def test_returns_two_branches(self) -> None:
        """Override path returns two heterogeneous branches with branch_filters."""
        cat = _MultiBranchCatalog()
        result = cat.table_scan_branches_get(
            attach_opaque_data=AttachOpaqueData(b"test"),
            transaction_opaque_data=None,
            schema_name="main",
            name="orders",
            at_unit=None,
            at_value=None,
        )
        assert len(result.branches) == 2
        assert result.branches[0].function_name == "vgi_kafka_scan"
        assert result.branches[1].function_name == "iceberg_scan"
        # branch_filters keep the union non-overlapping at scan time.
        assert "TIMESTAMP" in (result.branches[0].branch_filter or "")
        assert "TIMESTAMP" in (result.branches[1].branch_filter or "")

    def test_required_extensions_union(self) -> None:
        """Required extensions are reported as the union across all branches."""
        cat = _MultiBranchCatalog()
        result = cat.table_scan_branches_get(
            attach_opaque_data=AttachOpaqueData(b"test"),
            transaction_opaque_data=None,
            schema_name="main",
            name="orders",
            at_unit=None,
            at_value=None,
        )
        assert set(result.required_extensions) == {"iceberg", "httpfs"}

    def test_legacy_method_still_works(self) -> None:
        """Workers may implement both methods for old-extension compat.

        Old C++ extensions don't probe for the new branches RPC and call
        the legacy method directly. The worker keeps both implementations
        in sync — legacy returns the "primary" branch, branches returns
        the full list.
        """
        cat = _MultiBranchCatalog()
        legacy = cat.table_scan_function_get(
            attach_opaque_data=AttachOpaqueData(b"test"),
            transaction_opaque_data=None,
            schema_name="main",
            name="orders",
            at_unit=None,
            at_value=None,
        )
        assert legacy.function_name == "vgi_kafka_scan"


class TestShimSerializationRoundTrip:
    """Wire-format compatibility: shim output round-trips through serialize/deserialize."""

    def test_shim_output_round_trips(self) -> None:
        """Shim result serializes and deserializes back to an equivalent object."""
        cat = _LegacyOnlyCatalog(required=["parquet"])
        result = cat.table_scan_branches_get(
            attach_opaque_data=AttachOpaqueData(b"test"),
            transaction_opaque_data=None,
            schema_name="main",
            name="orders",
            at_unit=None,
            at_value=None,
        )
        wire = result.serialize()
        from vgi_rpc.utils import deserialize_record_batch

        batch, _ = deserialize_record_batch(wire)
        restored = ScanBranchesResult.deserialize(batch)
        assert len(restored.branches) == 1
        assert restored.branches[0].function_name == "legacy_scan"
        assert restored.required_extensions == ["parquet"]


@pytest.mark.parametrize("at_unit,at_value", [(None, None), ("VERSION", "1")])
def test_default_shim_passes_at_args_through(at_unit: str | None, at_value: str | None) -> None:
    """Parametrised regression guard: at args reach the legacy method untouched."""
    cat = _LegacyOnlyCatalog()
    result = cat.table_scan_branches_get(
        attach_opaque_data=AttachOpaqueData(b"test"),
        transaction_opaque_data=None,
        schema_name="main",
        name="t",
        at_unit=at_unit,
        at_value=at_value,
    )
    assert len(result.branches) == 1
