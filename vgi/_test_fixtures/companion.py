# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Test fixture: a VGI catalog that advertises a companion catalog + a
multi-branch table whose branches are catalog-table branches on that companion.

Exercises the lakehouse-federation feature end-to-end:

* ``catalog_attach`` advertises ``attach_catalogs=[AttachCatalogInfo(...)]`` so
  the C++ extension attaches a companion catalog (DuckLake / attached DuckDB /
  …) on the client at VGI-attach time.
* the ``data.hot_cold`` table's two branches are **catalog-table branches**
  (``function_name=""`` + ``source_*``) that scan ``<alias>.main.events`` in the
  companion, split hot/cold by ``branch_filter``.

Config via env (so the test controls the companion location):

* ``VGI_TEST_COMPANION_TARGET``  — ATTACH target, e.g.
  ``ducklake:sqlite:/scratch/lake.sqlite`` or ``/scratch/companion.duckdb``.
  When unset, no companion is advertised (the table still exists but binding a
  branch against the missing companion errors — used by the opt-out test).
* ``VGI_TEST_COMPANION_ALIAS``   — companion alias (default ``vgi_companion``).
* ``VGI_TEST_COMPANION_DBTYPE``  — explicit db_type (default: infer from target
  scheme; set ``duckdb`` for a plain ``.duckdb`` file so the scheme allowlist
  admits it).
* ``VGI_TEST_COMPANION_HIDDEN``  — ``1`` to attach the companion hidden.

Run with::

    VGI_TEST_COMPANION_TARGET=ducklake:sqlite:/tmp/lake.sqlite \
        vgi-fixture-companion-worker
"""

from __future__ import annotations

import os

import pyarrow as pa

from vgi.catalog import (
    AttachCatalogInfo,
    Catalog,
    ReadOnlyCatalogInterface,
    ScanBranch,
    ScanBranchesResult,
    Schema,
    Table,
)
from vgi.worker import Worker

_ALIAS = os.environ.get("VGI_TEST_COMPANION_ALIAS", "vgi_companion")
_TARGET = os.environ.get("VGI_TEST_COMPANION_TARGET", "")
_DBTYPE = os.environ.get("VGI_TEST_COMPANION_DBTYPE", "")
_HIDDEN = os.environ.get("VGI_TEST_COMPANION_HIDDEN", "") == "1"

# Optional SECOND companion that fails (disallowed scheme) AFTER the first has
# already attached — exercises the extension's partial-failure cleanup (the
# first companion must not leak). Enabled with VGI_TEST_COMPANION_POISON=1.
_POISON = os.environ.get("VGI_TEST_COMPANION_POISON", "") == "1"


def _cols(**fields: pa.DataType) -> pa.Schema:
    return pa.schema([pa.field(n, t, nullable=True) for n, t in fields.items()])


_COMPANION_CATALOG = Catalog(
    name="companion",
    default_schema="data",
    comment="VGI companion-catalog federation fixture",
    schemas=[
        Schema(
            name="data",
            comment="Multi-branch tables backed by a companion catalog",
            tables=[
                Table(
                    name="hot_cold",
                    columns=_cols(id=pa.int64(), val=pa.string()),
                    comment="Two catalog-table branches on the companion's events table (hot/cold via branch_filter)",
                ),
            ],
        ),
    ],
)


class CompanionCatalog(ReadOnlyCatalogInterface):
    """Advertises a companion catalog + a catalog-table-branch multi-branch table."""

    catalog = _COMPANION_CATALOG

    # Advertised only when a target is configured; empty otherwise (opt-out /
    # missing-companion tests).
    attach_catalogs = (
        (
            [
                AttachCatalogInfo(
                    alias=_ALIAS,
                    target=_TARGET,
                    db_type=_DBTYPE,
                    required=True,
                    hidden=_HIDDEN,
                )
            ]
            + (
                # Second entry with a disallowed scheme ⇒ the extension throws
                # AFTER the first attached ⇒ partial-failure cleanup must release
                # the first.
                [AttachCatalogInfo(alias="vgi_companion_poison", target="ftp://nope/x", required=True)]
                if _POISON
                else []
            )
        )
        if _TARGET
        else []
    )

    def table_scan_branches_get(
        self,
        *,
        attach_opaque_data,  # noqa: ANN001
        transaction_opaque_data,  # noqa: ANN001
        schema_name: str,
        name: str,
        at_unit: str | None = None,
        at_value: str | None = None,
    ) -> ScanBranchesResult:
        del attach_opaque_data, transaction_opaque_data, at_unit, at_value
        if schema_name.lower() == "data" and name.lower() == "hot_cold":
            # Both arms scan the SAME companion table with disjoint branch_filters.
            # A query predicate disjoint with an arm's filter prunes that arm's
            # companion scan from the plan entirely.
            def _arm(branch_filter: str) -> ScanBranch:
                return ScanBranch(
                    function_name="",
                    positional_arguments=[],
                    named_arguments={},
                    branch_filter=branch_filter,
                    source_catalog=_ALIAS,
                    source_schema="main",
                    source_table="events",
                )

            # DuckLake builds its scan from parquet_scan's bind, so the branch
            # needs parquet loaded before it binds. Declaring it here makes the
            # rewriter auto-load it (works even where extension autoload is off).
            return ScanBranchesResult(
                branches=[_arm("id < 100"), _arm("id >= 100")],
                required_extensions=["parquet"],
            )
        msg = f"Unknown multi-branch table: {schema_name}.{name}"
        raise ValueError(msg)


class CompanionCatalogWorker(Worker):
    """Worker serving :class:`CompanionCatalog`."""

    catalog_interface = CompanionCatalog


def main() -> None:
    """Console-script entry point (vgi-fixture-companion-worker)."""
    CompanionCatalogWorker.main()


if __name__ == "__main__":
    main()
