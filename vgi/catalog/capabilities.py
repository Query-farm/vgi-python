# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Versioned catalog profiles selected separately for each attachment.

An offer is not authorization. Selection is the intersection of the client's
offer, the provider's implementation, and profiles understood by this worker.
An absent offer selects nothing. Existing protocol operations retain their
existing behavior; new operations require their explicit profile.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import ClassVar

from vgi_rpc import ArrowSerializableDataclass

from vgi.exceptions import CatalogCapabilityError

SCHEMA_OPTIONS_READ = "vgi.schema.options.read.v1"
SCHEMA_OPTIONS_CREATE = "vgi.schema.options.create.v1"
SCHEMA_OPTIONS_ALTER = "vgi.schema.options.alter.v1"
ROW_SCALAR_EXPRESSION = "vgi.expression.duckdb.scalar.v1"
TABLE_OPTIONS_READ = "vgi.table.options.read.v1"
TABLE_OPTIONS_CREATE = "vgi.table.options.create.v1"
TABLE_OPTIONS_ALTER = "vgi.table.options.alter.v1"
TABLE_LAYOUT_READ = "vgi.table.layout.read.v1"
TABLE_LAYOUT_CREATE = "vgi.table.layout.create.v1"
TABLE_LAYOUT_ALTER_FUTURE = "vgi.table.layout.alter_future.v1"
TABLE_LAYOUT_ALTER_REWRITE = "vgi.table.layout.alter_rewrite.v1"
TABLE_METADATA_CREATE = "vgi.table.metadata.create.v1"
COLUMN_NESTED_ADD = "vgi.column.nested.add.v1"
COLUMN_NESTED_DROP = "vgi.column.nested.drop.v1"
COLUMN_NESTED_RENAME = "vgi.column.nested.rename.v1"
COLUMN_NESTED_TYPE = "vgi.column.nested.type.v1"
COLUMN_NESTED_DEFAULT = "vgi.column.nested.default.v1"
COLUMN_NESTED_NULLABILITY = "vgi.column.nested.nullability.v1"
COLUMN_NESTED_COMMENT = "vgi.column.nested.comment.v1"
TRANSACTION_DDL = "vgi.transaction.ddl.v1"

_PROFILE = re.compile(r"[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)*\.v[1-9][0-9]*\Z")


@dataclass(frozen=True, kw_only=True)
class CatalogCapabilities(ArrowSerializableDataclass):
    """Catalog read, DDL, write and transaction profiles.

    Lists contain unique, case-sensitive versioned names. Unknown offers are
    ignored, never interpreted as equivalent to a known version. Unknown names
    in a selected agreement are rejected. Selection is canonical (sorted) and
    includes a profile only if its prerequisites were also selected.
    """

    read: list[str] = field(default_factory=list)
    ddl: list[str] = field(default_factory=list)
    write: list[str] = field(default_factory=list)
    transaction: list[str] = field(default_factory=list)

    _known: ClassVar[dict[str, frozenset[str]]] = {
        "read": frozenset({SCHEMA_OPTIONS_READ, TABLE_OPTIONS_READ, TABLE_LAYOUT_READ, ROW_SCALAR_EXPRESSION}),
        "ddl": frozenset(
            {
                COLUMN_NESTED_ADD,
                COLUMN_NESTED_DROP,
                COLUMN_NESTED_RENAME,
                COLUMN_NESTED_TYPE,
                COLUMN_NESTED_DEFAULT,
                COLUMN_NESTED_NULLABILITY,
                COLUMN_NESTED_COMMENT,
                SCHEMA_OPTIONS_CREATE,
                SCHEMA_OPTIONS_ALTER,
                TABLE_OPTIONS_CREATE,
                TABLE_OPTIONS_ALTER,
                TABLE_LAYOUT_CREATE,
                TABLE_LAYOUT_ALTER_FUTURE,
                TABLE_LAYOUT_ALTER_REWRITE,
                TABLE_METADATA_CREATE,
            }
        ),
        "write": frozenset(),
        "transaction": frozenset({TRANSACTION_DDL}),
    }
    _requires: ClassVar[dict[str, frozenset[str]]] = {
        TABLE_OPTIONS_CREATE: frozenset({TABLE_OPTIONS_READ}),
        TABLE_OPTIONS_ALTER: frozenset({TABLE_OPTIONS_READ}),
        TABLE_LAYOUT_CREATE: frozenset({TABLE_LAYOUT_READ}),
        TABLE_LAYOUT_ALTER_FUTURE: frozenset({TABLE_LAYOUT_READ}),
        TABLE_LAYOUT_ALTER_REWRITE: frozenset({TABLE_LAYOUT_READ}),
        SCHEMA_OPTIONS_CREATE: frozenset({SCHEMA_OPTIONS_READ}),
        SCHEMA_OPTIONS_ALTER: frozenset({SCHEMA_OPTIONS_READ}),
    }

    def __post_init__(self) -> None:
        """Validate names without rejecting a newer peer's unknown offers."""
        for category in self._known:
            names = getattr(self, category)
            if not isinstance(names, list) or any(not isinstance(name, str) for name in names):
                raise TypeError(f"{category} capabilities must be a list of profile names")
            if len(names) != len(set(names)) or any(not _PROFILE.fullmatch(name) for name in names):
                raise ValueError(f"{category} capabilities must be unique versioned profile names")
            for other, known in self._known.items():
                if category != other and set(names) & known:
                    raise ValueError(f"capability belongs in {other}, not {category}")

    def names(self) -> frozenset[str]:
        """Return a validated snapshot of all offered or selected names."""
        self.__post_init__()
        return frozenset(name for category in self._known for name in getattr(self, category))

    def select(self, offer: CatalogCapabilities | None) -> CatalogCapabilities:
        """Intersect provider support with an offer and enforce prerequisites."""
        self.__post_init__()
        if offer is None:
            return CatalogCapabilities()
        offer.__post_init__()
        selected = {
            category: set(getattr(self, category)) & set(getattr(offer, category)) & known
            for category, known in self._known.items()
        }
        # Iterate so future profiles may depend on other dependent profiles.
        while True:
            names = set().union(*selected.values())
            removed = {name for name in names if not self._requires.get(name, frozenset()) <= names}
            if not removed:
                break
            selected = {category: names - removed for category, names in selected.items()}
        return CatalogCapabilities(**{category: sorted(names) for category, names in selected.items()})

    def validate_selected(self) -> None:
        """Reject malformed, unknown or incomplete attachment agreements."""
        if self.select(self) != self:
            raise ValueError("selected catalog capabilities contain unknown, unordered or incomplete profiles")

    def require(self, profile: str, *, available: CatalogCapabilities, operation: str) -> None:
        """Check the immutable agreement and current support before effects."""
        self.validate_selected()
        if profile not in available.select(self).names():
            raise CatalogCapabilityError(operation, profile)
