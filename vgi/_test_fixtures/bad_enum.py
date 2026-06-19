# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Fixture worker that deliberately advertises an unrecognized enum value.

This fixture exercises the C++ extension's *wire-enum validation* end-to-end:
the catalog-metadata parser (``ParseFunctionInfo`` in
``vgi/src/vgi_catalog_api.cpp``) must reject an enum string it does not
recognize with a loud ``IOException`` rather than silently falling back to a
default. A silent fallback would run with behavior inconsistent with what the
worker declared (e.g. treating a ``SPECIAL`` null-handling function as
``DEFAULT``).

The trick is entirely Python-side and needs no extension rebuild. The normal
metadata path can only ever emit valid enum names because the values come from
typed Python ``Enum`` members. To get a bogus string onto the wire we override
:meth:`ExampleCatalog._function_to_info` for one scalar function (``double``)
and swap its ``null_handling`` for :class:`_BogusNullHandling.WEIRD` — a real
``Enum`` member whose ``.name`` is ``"WEIRD"``. The vgi-rpc serializer converts
any ``Enum`` field to ``value.name`` (see ``ArrowSerializableDataclass``), so
``"WEIRD"`` lands in the ``null_handling`` Arrow column and the C++ parser
trips on it the moment the ``double`` function's metadata is loaded.

Otherwise this is a drop-in replacement for ``vgi-fixture-worker``: every other
function and the catalog are inherited unchanged from :class:`ExampleWorker`,
so any function except ``double`` still resolves normally.

Registered as the ``vgi-fixture-bad-enum-worker`` entry point.
"""

from __future__ import annotations

from dataclasses import replace
from enum import Enum

from vgi._test_fixtures.worker import ExampleCatalog, ExampleWorker
from vgi.catalog.catalog_interface import FunctionInfo

# The scalar function whose null_handling we corrupt. Tests reference this name
# to force the broken metadata onto the parse path.
BAD_ENUM_FUNCTION = "double"


class _BogusNullHandling(Enum):
    """An enum member whose ``.name`` is a value the C++ parser cannot map."""

    WEIRD = "WEIRD"


class BadEnumCatalog(ExampleCatalog):
    """ExampleCatalog that advertises a bogus null_handling for one function."""

    def _function_to_info(self, func_cls: type, schema_name: str) -> FunctionInfo:
        info = super()._function_to_info(func_cls, schema_name)
        if info.name == BAD_ENUM_FUNCTION and info.null_handling is not None:
            # FunctionInfo is frozen; replace() returns a corrupted copy.
            return replace(info, null_handling=_BogusNullHandling.WEIRD)  # type: ignore[arg-type]
        return info


class BadEnumWorker(ExampleWorker):
    """ExampleWorker that serves the example catalog with one bad enum value."""

    catalog_interface = BadEnumCatalog


def main() -> None:
    """Run the bad-enum fixture worker process."""
    BadEnumWorker.main()


if __name__ == "__main__":
    main()
