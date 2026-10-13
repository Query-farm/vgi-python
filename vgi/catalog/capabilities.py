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
        "read": frozenset({SCHEMA_OPTIONS_READ}),
        "ddl": frozenset({SCHEMA_OPTIONS_CREATE, SCHEMA_OPTIONS_ALTER}),
        "write": frozenset(),
        "transaction": frozenset({TRANSACTION_DDL}),
    }
    _requires: ClassVar[dict[str, frozenset[str]]] = {
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
