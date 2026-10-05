# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Three small catalogs for the ``catalog_contents`` RPC.

The same two-schema catalog is served under three names, differing only in how
they answer ``catalog_contents`` (see ``vgi/docs/catalog_contents.md``):

``contents_probe``
    Advertises ``supports_catalog_contents`` and serves it normally (the
    ``ReadOnlyCatalogInterface`` default). A client loads it in one RPC.
``contents_broken``
    Advertises ``supports_catalog_contents`` but its ``catalog_contents``
    raises. A client must fall back to ``catalog_schemas`` plus the per-schema
    ``catalog_schema_contents_*`` RPCs and still see the whole catalog.
``contents_legacy``
    Does not advertise it, like an older worker. A client must never call
    ``catalog_contents`` and must use the per-schema RPCs.
``contents_memory``
    A DDL-capable ``InMemoryCatalog`` (non-frozen version, bumped by every
    DDL) that advertises ``catalog_contents``, for invalidation after DDL. Each
    ATTACH gets its own empty catalog (one ``main`` schema), so tests sharing a
    warm worker never see each other's objects.

Every kind the client seeds from ``catalog_contents`` is present at least once
(tables, a view, scalar / aggregate / table functions, scalar and table
macros), split over two schemas (``main`` and ``extra``), so RPC accounting in
``vgi/test/sql/integration/catalog/catalog_contents*.test`` is deterministic.
Served by ``vgi-fixture-worker`` and ``vgi-fixture-http`` (MetaWorker), so the
same tests run over every transport. Their functions re-use the example
catalog's classes (``main.double`` …), so the workers set
``route_unattached_calls = False``: they are reachable only through their own
attach, and an unattached ``main.double`` still routes to the example worker.
"""

from __future__ import annotations

import uuid
from dataclasses import replace
from typing import Any

import pyarrow as pa

from vgi._test_fixtures.aggregate.basic import SumFunction
from vgi._test_fixtures.catalog import CatalogData, InMemoryCatalog, SchemaData
from vgi._test_fixtures.scalar.arithmetic import DoubleFunction
from vgi._test_fixtures.table.sequence import SequenceFunction
from vgi.arguments import Arguments
from vgi.catalog import (
    AttachOpaqueData,
    Catalog,
    CatalogAttachResult,
    CatalogInfo,
    Macro,
    MacroType,
    ReadOnlyCatalogInterface,
    Schema,
    SchemaContentsInfo,
    SchemaInfo,
    Table,
    View,
)
from vgi.worker import Worker

CATALOG_PROBE = "contents_probe"
CATALOG_BROKEN = "contents_broken"
CATALOG_LEGACY = "contents_legacy"
CATALOG_MEMORY = "contents_memory"

BROKEN_MESSAGE = "contents_broken: catalog_contents deliberately fails"


def _catalog(name: str) -> Catalog:
    return Catalog(
        name=name,
        default_schema="main",
        comment=f"catalog_contents test catalog ({name})",
        schemas=[
            Schema(
                path=["main"],
                comment="Every object kind",
                tables=[
                    Table(
                        name="ten",
                        function=SequenceFunction,
                        arguments=Arguments(positional=(pa.scalar(10),)),
                        comment="Integers 0..9",
                    ),
                ],
                views=[View(name="answer", definition="SELECT 42 AS answer", comment="One row")],
                functions=[DoubleFunction, SumFunction, SequenceFunction],
                macros=[
                    Macro(
                        name="contents_triple",
                        macro_type=MacroType.SCALAR,
                        parameters=["x"],
                        definition="x * 3",
                        comment="Triple a value",
                    ),
                    Macro(
                        name="contents_range",
                        macro_type=MacroType.TABLE,
                        parameters=["n"],
                        definition="SELECT * FROM range(n)",
                        comment="Table macro over range(n)",
                    ),
                ],
            ),
            Schema(
                path=["extra"],
                comment="A second schema, tables only",
                tables=[
                    Table(
                        name="five",
                        function=SequenceFunction,
                        arguments=Arguments(positional=(pa.scalar(5),)),
                        comment="Integers 0..4",
                    ),
                ],
            ),
        ],
    )


_CATALOG_PROBE = _catalog(CATALOG_PROBE)
_CATALOG_BROKEN = _catalog(CATALOG_BROKEN)
_CATALOG_LEGACY = _catalog(CATALOG_LEGACY)


class ContentsProbeCatalog(ReadOnlyCatalogInterface):
    """Advertises and serves ``catalog_contents``."""

    catalog = _CATALOG_PROBE
    catalog_name = CATALOG_PROBE


class ContentsBrokenCatalog(ReadOnlyCatalogInterface):
    """Advertises ``catalog_contents`` but fails to serve it."""

    catalog = _CATALOG_BROKEN
    catalog_name = CATALOG_BROKEN

    def catalog_contents(self, *, attach_opaque_data: Any) -> list[SchemaContentsInfo]:
        """Always fail, to drive the client's per-schema fallback."""
        raise RuntimeError(BROKEN_MESSAGE)


class ContentsLegacyCatalog(ReadOnlyCatalogInterface):
    """Does not advertise ``catalog_contents`` (an older worker)."""

    catalog = _CATALOG_LEGACY
    catalog_name = CATALOG_LEGACY

    def catalog_attach(self, **kwargs: Any) -> CatalogAttachResult:
        """Attach as the base class does, without the capability flag."""
        return replace(super().catalog_attach(**kwargs), supports_catalog_contents=False)


class ContentsProbeWorker(Worker):
    """Serves ``contents_probe``."""

    route_unattached_calls = False

    catalog_interface = ContentsProbeCatalog
    catalog_name = CATALOG_PROBE
    catalog = _CATALOG_PROBE


class ContentsBrokenWorker(Worker):
    """Serves ``contents_broken``."""

    route_unattached_calls = False

    catalog_interface = ContentsBrokenCatalog
    catalog_name = CATALOG_BROKEN
    catalog = _CATALOG_BROKEN


class ContentsLegacyWorker(Worker):
    """Serves ``contents_legacy``."""

    route_unattached_calls = False

    catalog_interface = ContentsLegacyCatalog
    catalog_name = CATALOG_LEGACY
    catalog = _CATALOG_LEGACY


class ContentsMemoryCatalog(InMemoryCatalog):
    """DDL-capable in-memory catalog that advertises ``catalog_contents``.

    Every ATTACH of ``contents_memory`` gets a private, empty catalog (one
    ``main`` schema) registered under an internal name, so state never leaks
    between attaches that share one worker process.
    """

    def __init__(self) -> None:
        """Start with no catalogs; each attach creates its own."""
        super().__init__()
        self._catalogs.clear()

    def catalogs(self) -> list[CatalogInfo]:
        """Advertise only the public name (MetaWorker routes on it)."""
        return [CatalogInfo(name=CATALOG_MEMORY, implementation_version=None, data_version_spec=None)]

    def catalog_attach(
        self,
        *,
        name: str,
        options: dict[str, Any],
        data_version_spec: str | None,
        implementation_version: str | None,
        ctx: Any = None,
    ) -> CatalogAttachResult:
        """Attach a fresh private catalog, advertising ``catalog_contents``."""
        if name != CATALOG_MEMORY:
            raise ValueError(f"Unknown catalog: {name!r}. Available: {CATALOG_MEMORY}")
        private = f"{CATALOG_MEMORY}/{uuid.uuid4().hex}"
        catalog = CatalogData(name=private)
        catalog.schemas[("main",)] = SchemaData(
            info=SchemaInfo(attach_opaque_data=AttachOpaqueData(b"\x00" * 16), path=["main"], comment=None, tags={})
        )
        self._catalogs[private] = catalog
        result = super().catalog_attach(
            name=private,
            options=options,
            data_version_spec=data_version_spec,
            implementation_version=implementation_version,
            ctx=ctx,
        )
        return replace(result, supports_catalog_contents=True)

    def catalog_detach(self, *, attach_opaque_data: AttachOpaqueData) -> None:
        """Detach and drop the private catalog."""
        private = self._attachments.pop(attach_opaque_data, None)
        if private is not None:
            self._catalogs.pop(private, None)


class ContentsMemoryWorker(Worker):
    """Serves ``contents_memory``."""

    route_unattached_calls = False

    catalog_interface = ContentsMemoryCatalog
    catalog_name = CATALOG_MEMORY


CONTENTS_WORKERS: list[type[Worker]] = [
    ContentsProbeWorker,
    ContentsBrokenWorker,
    ContentsLegacyWorker,
    ContentsMemoryWorker,
]
