# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Small catalogs for the ``catalog_contents`` RPC.

The same static two-schema catalog is served under three names, differing only
in how they answer ``catalog_contents`` (see ``vgi/docs/catalog_contents.md``):

``contents_probe``
    Advertises ``supports_catalog_contents`` and serves it normally (the
    ``ReadOnlyCatalogInterface`` default: version-frozen, no etag, and cached
    by the worker). A client loads it in one RPC.
``contents_broken``
    Advertises ``supports_catalog_contents`` but its ``catalog_contents``
    raises. A client must fall back to ``catalog_schemas`` plus the per-schema
    ``catalog_schema_contents_*`` RPCs and still see the whole catalog.
``contents_legacy``
    Does not advertise it, like an older worker. A client must never call
    ``catalog_contents`` and must use the per-schema RPCs.

Three DDL-capable ``InMemoryCatalog`` catalogs (version not frozen) advertise
it too. Each ATTACH gets its own empty catalog (one ``main`` schema), so tests
sharing a warm worker never see each other's objects:

``contents_memory``
    Reports ``catalog_version`` 0 ("unknown") and no etag. Exercises the
    client's version-0 rule: the first load of an attach uses
    ``catalog_contents``, but reloads (the client clears a version-0 catalog
    at every transaction start) use the lazy per-schema RPCs instead.
``contents_reval``
    A revalidating catalog with a cheap validator: the etag is
    ``"gen-<n>"``, where ``n`` is the catalog version, bumped by every DDL.
    A matching ``if_none_match`` answers ``not_modified`` without building
    anything. The client revalidates with it at transaction start instead of
    polling ``catalog_version``.
``contents_hash``
    Returns no etag of its own but sets ``catalog_contents_etag =
    "content-hash"``: the worker builds the snapshot on every call and uses
    its SHA-256 as the etag, answering ``not_modified`` when it matches.

The static catalog holds every kind the client seeds from ``catalog_contents``
at least once (tables, a view, scalar / aggregate / table functions, scalar and
table macros), split over two schemas (``main`` and ``extra``), so RPC
accounting in ``vgi/test/sql/integration/catalog/catalog_contents*.test`` is
deterministic. Served by ``vgi-fixture-worker`` and ``vgi-fixture-http``
(MetaWorker), so the same tests run over every transport. Their functions
re-use the example catalog's classes (``main.double`` ...), so every worker
here sets ``route_unattached_calls = False``: they are reachable only through
their own attach, and an unattached ``main.double`` still routes to the
example worker.
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
    CatalogContentsResult,
    CatalogInfo,
    Macro,
    MacroType,
    ReadOnlyCatalogInterface,
    Schema,
    SchemaInfo,
    Table,
    View,
)
from vgi.worker import Worker

CATALOG_PROBE = "contents_probe"
CATALOG_BROKEN = "contents_broken"
CATALOG_LEGACY = "contents_legacy"
CATALOG_MEMORY = "contents_memory"
CATALOG_REVAL = "contents_reval"
CATALOG_HASH = "contents_hash"

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

    def catalog_contents(self, *, attach_opaque_data: Any, if_none_match: str | None = None) -> CatalogContentsResult:
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


class _PrivateMemoryCatalog(InMemoryCatalog):
    """DDL-capable in-memory catalog that advertises ``catalog_contents``.

    Every ATTACH of `public_name` gets a private, empty catalog (one ``main``
    schema) registered under an internal name, so state never leaks between
    attaches that share one worker process.
    """

    public_name: str = ""

    def __init__(self) -> None:
        """Start with no catalogs; each attach creates its own."""
        super().__init__()
        self._catalogs.clear()

    def catalogs(self) -> list[CatalogInfo]:
        """Advertise only the public name (MetaWorker routes on it)."""
        return [CatalogInfo(name=self.public_name, implementation_version=None, data_version_spec=None)]

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
        if name != self.public_name:
            raise ValueError(f"Unknown catalog: {name!r}. Available: {self.public_name}")
        private = f"{self.public_name}/{uuid.uuid4().hex}"
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
        return replace(
            result,
            supports_catalog_contents=True,
            catalog_version=self.catalog_version(
                attach_opaque_data=result.attach_opaque_data, transaction_opaque_data=None
            ),
        )

    def catalog_detach(self, *, attach_opaque_data: AttachOpaqueData) -> None:
        """Detach and drop the private catalog."""
        private = self._attachments.pop(attach_opaque_data, None)
        if private is not None:
            self._catalogs.pop(private, None)


class ContentsMemoryCatalog(_PrivateMemoryCatalog):
    """Private in-memory catalog reporting version 0 and no etag (the version-0 rule)."""

    public_name = CATALOG_MEMORY

    def catalog_version(
        self, *, attach_opaque_data: AttachOpaqueData, transaction_opaque_data: Any, ctx: Any = None
    ) -> int:
        """Always 0: the catalog does not track its version."""
        del attach_opaque_data, transaction_opaque_data, ctx
        return 0


class ContentsRevalCatalog(_PrivateMemoryCatalog):
    """Private in-memory catalog that revalidates with a generation-counter etag."""

    public_name = CATALOG_REVAL

    def catalog_contents(
        self, *, attach_opaque_data: AttachOpaqueData, if_none_match: str | None = None
    ) -> CatalogContentsResult:
        """Answer ``not_modified`` from the generation counter, building only on a miss."""
        etag = f"gen-{self._get_catalog(attach_opaque_data).version}"
        if if_none_match == etag:
            return CatalogContentsResult(etag=etag, not_modified=True)
        built = super().catalog_contents(attach_opaque_data=attach_opaque_data)
        return CatalogContentsResult(schemas=built.schemas, etag=etag)


class ContentsHashCatalog(_PrivateMemoryCatalog):
    """Private in-memory catalog revalidated by the framework's content hash."""

    public_name = CATALOG_HASH
    catalog_contents_etag = "content-hash"


class ContentsMemoryWorker(Worker):
    """Serves ``contents_memory``."""

    route_unattached_calls = False

    catalog_interface = ContentsMemoryCatalog
    catalog_name = CATALOG_MEMORY


class ContentsRevalWorker(Worker):
    """Serves ``contents_reval``."""

    route_unattached_calls = False

    catalog_interface = ContentsRevalCatalog
    catalog_name = CATALOG_REVAL


class ContentsHashWorker(Worker):
    """Serves ``contents_hash``."""

    route_unattached_calls = False

    catalog_interface = ContentsHashCatalog
    catalog_name = CATALOG_HASH


CONTENTS_WORKERS: list[type[Worker]] = [
    ContentsProbeWorker,
    ContentsBrokenWorker,
    ContentsLegacyWorker,
    ContentsMemoryWorker,
    ContentsRevalWorker,
    ContentsHashWorker,
]
