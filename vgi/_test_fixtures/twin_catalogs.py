# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Two catalogs, one worker, colliding function names.

``twin_a`` and ``twin_b`` are separate VGI catalogs served by the same worker
process (via :class:`vgi.meta_worker.MetaWorker`). Each declares a schema
literally named ``main`` holding a scalar function literally named
``test_same_name_catalog`` — so neither the function name nor the schema name
distinguishes them. Only the catalog does.

Attaching both from the same worker location and calling
``a.main.test_same_name_catalog(1)`` vs ``b.main.test_same_name_catalog(1)`` must
reach different implementations. The routing key is the per-attach
``attach_opaque_data``: MetaWorker encodes the sub-worker index in it, so bind
and init land on the catalog the caller attached rather than on whichever
sub-worker happens to hold the name first.

Companion to :mod:`vgi._test_fixtures.scalar.same_name`, which collides two
names within a *single* catalog across two schemas. Driven by
``vgi/test/sql/integration/scalar/same_name_catalogs.test``.
"""

from __future__ import annotations

from typing import Annotated

import pyarrow as pa

from vgi.arguments import Param, Returns
from vgi.catalog import Catalog, ReadOnlyCatalogInterface, Schema
from vgi.function import Function
from vgi.metadata import FunctionExample
from vgi.scalar_function import ScalarFunction
from vgi.worker import Worker

# Deliberately identical in both catalogs — the collision is the point.
FUNCTION_NAME = "test_same_name_catalog"
SCHEMA_NAME = "main"

CATALOG_A = "twin_a"
CATALOG_B = "twin_b"


def _tag(catalog_name: str, value: pa.Int64Array) -> pa.StringArray:
    """Render ``<catalog_name>:<value>`` for every row, preserving nulls."""
    return pa.array(
        [None if v is None else f"{catalog_name}:{v}" for v in value.to_pylist()],
        type=pa.string(),
    )


class TwinAFunction(ScalarFunction):
    """``test_same_name_catalog`` as served by the ``twin_a`` catalog."""

    class Meta:
        """Function metadata."""

        name = FUNCTION_NAME
        description = "Catalog-disambiguation probe; the twin_a implementation"
        examples = [
            FunctionExample(
                sql="SELECT a.main.test_same_name_catalog(1)",
                description="Returns 'twin_a:1'",
            ),
        ]

    @classmethod
    def compute(
        cls,
        value: Annotated[pa.Int64Array, Param(doc="Integer value to tag")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Tag each value with the owning catalog."""
        return _tag(CATALOG_A, value)


class TwinBFunction(ScalarFunction):
    """``test_same_name_catalog`` as served by the ``twin_b`` catalog."""

    class Meta:
        """Function metadata."""

        name = FUNCTION_NAME
        description = "Catalog-disambiguation probe; the twin_b implementation"
        examples = [
            FunctionExample(
                sql="SELECT b.main.test_same_name_catalog(1)",
                description="Returns 'twin_b:1'",
            ),
        ]

    @classmethod
    def compute(
        cls,
        value: Annotated[pa.Int64Array, Param(doc="Integer value to tag")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Tag each value with the owning catalog."""
        return _tag(CATALOG_B, value)


def _catalog(name: str, function: type[Function]) -> Catalog:
    return Catalog(
        name=name,
        default_schema=SCHEMA_NAME,
        comment=f"Catalog-disambiguation twin ({name})",
        schemas=[
            Schema(
                name=SCHEMA_NAME,
                comment=f"Colliding function name served by {name}",
                functions=[function],
            ),
        ],
    )


_CATALOG_A = _catalog(CATALOG_A, TwinAFunction)
_CATALOG_B = _catalog(CATALOG_B, TwinBFunction)


class TwinACatalog(ReadOnlyCatalogInterface):
    """Catalog interface for ``twin_a``."""

    catalog = _CATALOG_A
    catalog_name = CATALOG_A


class TwinBCatalog(ReadOnlyCatalogInterface):
    """Catalog interface for ``twin_b``."""

    catalog = _CATALOG_B
    catalog_name = CATALOG_B


class TwinAWorker(Worker):
    """Serves the ``twin_a`` catalog."""

    catalog_interface = TwinACatalog
    catalog_name = CATALOG_A
    catalog = _CATALOG_A


class TwinBWorker(Worker):
    """Serves the ``twin_b`` catalog."""

    catalog_interface = TwinBCatalog
    catalog_name = CATALOG_B
    catalog = _CATALOG_B
