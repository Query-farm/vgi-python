# Copyright 2026 Query Farm LLC - https://query.farm

"""Typed argument bundles shared by catalog protocol tests."""

from __future__ import annotations

from typing import NotRequired, TypedDict

from vgi.catalog.catalog_interface import AttachOpaqueData, TransactionOpaqueData


class CatalogPathArgs(TypedDict):
    """Identify a catalog schema path."""

    attach_opaque_data: AttachOpaqueData
    schema_path: list[str]


class CatalogObjectArgs(CatalogPathArgs):
    """Identify a catalog object."""

    name: str


class CatalogObjectTransactionArgs(CatalogObjectArgs):
    """Identify an object in an optional transaction."""

    transaction_opaque_data: TransactionOpaqueData | None


class CatalogColumnTransactionArgs(CatalogObjectTransactionArgs):
    """Optionally identify a nested column in a transaction."""

    column_path: NotRequired[list[str]]


class CatalogColumnArgs(CatalogObjectArgs):
    """Identify a nested column."""

    column_path: list[str]


class CatalogSchemaArgs(TypedDict):
    """Identify a schema for schema metadata calls."""

    attach_opaque_data: AttachOpaqueData
    path: list[str]


def present[T](value: T | None) -> T:
    """Require an expected catalog result and narrow its optional type."""
    assert value is not None
    return value
