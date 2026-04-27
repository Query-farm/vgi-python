"""Example VGI worker that validates data_version_spec / implementation_version.

Demonstrates the ATTACH-time versioning protocol end-to-end:

* :meth:`VersionedCatalog.catalogs` advertises per-catalog
  ``implementation_version`` and ``data_version_spec`` discovery metadata.
* :meth:`VersionedCatalog.catalog_attach` validates the client's requested
  versions against :data:`SUPPORTED_DATA_VERSIONS` and
  :data:`IMPLEMENTATION_VERSION`. Unsatisfiable requests raise ``ValueError``;
  the client surfaces this as the ATTACH failure. Successful attaches set
  ``ctx.set_cookie("vgi_sticky", ...)`` so any upstream HTTP proxy can pin
  the session.
* :meth:`VersionedCatalog.catalog_version` asserts that subsequent requests
  echo the ``vgi_sticky`` cookie — this proves the extension's HTTP cookie
  jar plumbs Set-Cookie → Cookie round trips. For subprocess transport
  ``ctx.cookies`` is empty and the check is skipped.

Registered as the ``vgi-fixture-versioned-worker`` entry point.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any

from vgi.catalog import (
    AttachId,
    CatalogAttachResult,
    CatalogInfo,
    ReadOnlyCatalogInterface,
    TransactionId,
)
from vgi.catalog.descriptors import Catalog, Schema
from vgi.worker import Worker

if TYPE_CHECKING:
    from vgi_rpc.rpc import CallContext


IMPLEMENTATION_VERSION = "1.0.0"
DATA_VERSION_SPEC = ">=1.0.0,<2.0.0"
SUPPORTED_DATA_VERSIONS: frozenset[str] = frozenset({"1.0.0", "1.1.0", "1.2.0"})
DEFAULT_DATA_VERSION = "1.2.0"
STICKY_COOKIE_NAME = "vgi_sticky"


_VERSIONED_CATALOG = Catalog(
    name="versioned",
    default_schema="main",
    comment="Example catalog demonstrating data_version_spec validation and cookie stickiness",
    tags={},
    schemas=[Schema(name="main", tables=[])],
)


class VersionedCatalog(ReadOnlyCatalogInterface):
    """Catalog interface that validates versions and exercises HTTP cookies."""

    catalog = _VERSIONED_CATALOG

    def catalogs(self) -> list[CatalogInfo]:
        """Advertise a single catalog with its implementation version and data-version range."""
        return [
            CatalogInfo(
                name=_VERSIONED_CATALOG.name,
                implementation_version=IMPLEMENTATION_VERSION,
                data_version_spec=DATA_VERSION_SPEC,
            ),
        ]

    def catalog_attach(
        self,
        *,
        name: str,
        options: dict[str, Any],
        data_version_spec: str | None,
        implementation_version: str | None,
        ctx: CallContext | None = None,
    ) -> CatalogAttachResult:
        """Validate requested versions and pin an HTTP session."""
        del options
        if name != _VERSIONED_CATALOG.name:
            raise ValueError(f"Unknown catalog: {name!r}. Available: {_VERSIONED_CATALOG.name}")

        if implementation_version is not None and implementation_version != IMPLEMENTATION_VERSION:
            raise ValueError(
                f"Unsupported implementation_version {implementation_version!r}; "
                f"this worker serves {IMPLEMENTATION_VERSION!r}",
            )

        if data_version_spec is not None and data_version_spec not in SUPPORTED_DATA_VERSIONS:
            # Exact-match only — production workers would parse the range; this
            # example keeps the matcher trivial so failures are unambiguous.
            raise ValueError(
                f"Unsupported data_version_spec {data_version_spec!r}; "
                f"this worker serves one of {sorted(SUPPORTED_DATA_VERSIONS)}",
            )
        resolved_data_version = data_version_spec if data_version_spec is not None else DEFAULT_DATA_VERSION

        # Pin the session for HTTP routing. On subprocess transport set_cookie
        # raises RuntimeError — ignore it silently so the same worker works
        # under both transports.
        if ctx is not None:
            try:
                ctx.set_cookie(STICKY_COOKIE_NAME, uuid.uuid4().hex)
            except RuntimeError:
                pass

        return CatalogAttachResult(
            attach_id=AttachId(uuid.uuid4().bytes),
            supports_transactions=False,
            supports_time_travel=False,
            catalog_version_frozen=True,
            catalog_version=1,
            attach_id_required=False,
            default_schema=_VERSIONED_CATALOG.default_schema,
            comment=_VERSIONED_CATALOG.comment,
            tags=dict(_VERSIONED_CATALOG.tags),
            resolved_data_version=resolved_data_version,
            resolved_implementation_version=IMPLEMENTATION_VERSION,
        )

    def catalog_version(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        ctx: CallContext | None = None,
    ) -> int:
        """Assert that the routing cookie we set at ATTACH is echoed back."""
        del attach_id, transaction_id
        if ctx is not None and ctx.cookies and STICKY_COOKIE_NAME not in ctx.cookies:
            # Over HTTP, missing the sticky cookie means the client's cookie jar
            # is broken. Bail loudly so the test catches the regression.
            raise ValueError(
                f"expected cookie {STICKY_COOKIE_NAME!r} on follow-up request; got {sorted(ctx.cookies)}",
            )
        return 1


class VersionedWorker(Worker):
    """Worker exposing :class:`VersionedCatalog`."""

    catalog_interface = VersionedCatalog
    catalog = _VERSIONED_CATALOG


def main() -> None:
    """Run the versioned-example worker process."""
    VersionedWorker.main()


if __name__ == "__main__":
    main()
