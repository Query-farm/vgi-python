# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Attachment-relative object identities and atomic dependency-drop planning."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Self

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.utils import IpcValidation

from vgi.typed_values import TypedValueBatch


class _DependencyRecord(ArrowSerializableDataclass):
    @classmethod
    def deserialize_from_bytes(cls, data: bytes, ipc_validation: IpcValidation = IpcValidation.FULL) -> Self:
        """Require one complete record, including all nested IPC records."""
        return cls.deserialize_from_batch(TypedValueBatch.deserialize_from_bytes(data).batch)

    @classmethod
    def deserialize_from_batch(
        cls,
        batch: pa.RecordBatch,
        custom_metadata: pa.KeyValueMetadata | None = None,
        *,
        ipc_validation: IpcValidation = IpcValidation.FULL,
    ) -> Self:
        """Reject missing fields, incorrect nullability and extra rows."""
        if batch.num_rows != 1 or not batch.schema.equals(cls.ARROW_SCHEMA, check_metadata=False):
            raise ValueError(f"{cls.__name__} requires one row with its complete schema")
        if any(batch.column(i).null_count for i, f in enumerate(batch.schema) if not f.nullable):
            raise ValueError(f"{cls.__name__} contains NULL in a required field")
        return super().deserialize_from_batch(batch, custom_metadata, ipc_validation=IpcValidation.FULL)


def _identifier(value: object) -> None:
    if not isinstance(value, str) or not value or "\0" in value:
        raise ValueError("object identifiers must be nonempty strings without NUL")


@dataclass(frozen=True, kw_only=True)
class CatalogObjectRef(_DependencyRecord):
    """A resolved object in this attachment, without a client catalog alias.

    For a schema, schema_path names its parents and name is its final component.
    Other objects use their owning schema path. Only table-owned triggers have
    owner_table. Names are raw components; a provider returns canonical spelling
    and resolves request identifiers according to its declared name policy.
    Function and macro references identify the complete overload family.
    """

    kind: str
    schema_path: list[str]
    name: str
    owner_table: str | None = None

    def __post_init__(self) -> None:
        """Validate the namespace without flattening or case-folding names."""
        if self.kind not in {
            "schema",
            "table",
            "view",
            "index",
            "scalar_macro",
            "table_macro",
            "sequence",
            "type",
            "scalar_function",
            "aggregate_function",
            "table_function",
            "window_function",
            "trigger",
        }:
            raise ValueError("unknown catalog object kind")
        if not isinstance(self.schema_path, list):
            raise ValueError("schema_path must be a list of raw identifiers")
        if not self.schema_path and self.kind != "schema":
            raise ValueError("non-schema objects require an owning schema")
        for component in [*self.schema_path, self.name]:
            _identifier(component)
        if self.kind == "trigger":
            _identifier(self.owner_table)
        elif self.owner_table is not None:
            raise ValueError("only triggers have a table-owned namespace")

    def key(self) -> tuple[str, tuple[str, ...], str, str]:
        """Return the full resolved identity, retaining component boundaries."""
        self.__post_init__()
        return self.kind, tuple(self.schema_path), self.name, self.owner_table or ""


@dataclass(frozen=True, kw_only=True)
class ObjectDependency(_DependencyRecord):
    """An edge from dependent to subject, both CatalogObjectRef IPC records.

    ``requires`` blocks RESTRICT; CASCADE deletes the dependent. ``owned``
    automatically deletes the dependent when its owner is dropped, even under
    RESTRICT. Dropping a dependent never drops its subject. Owned objects can
    be dropped explicitly, have at most one owner, and cannot form owner cycles.
    Schema membership and parent schemas use requires, not owned.
    """

    dependent: bytes
    subject: bytes
    kind: str = "requires"

    def __post_init__(self) -> None:
        """Validate endpoints and the complete drop policy."""
        if self.kind not in {"requires", "owned"}:
            raise ValueError("unknown dependency kind")
        if self.dependent_ref().key() == self.subject_ref().key():
            raise ValueError("self dependencies are invalid")

    def dependent_ref(self) -> CatalogObjectRef:
        """Decode the dependent's resolved identity."""
        return CatalogObjectRef.deserialize_from_bytes(self.dependent)

    def subject_ref(self) -> CatalogObjectRef:
        """Decode the subject's resolved identity."""
        return CatalogObjectRef.deserialize_from_bytes(self.subject)


@dataclass(frozen=True, kw_only=True)
class DependencySnapshot(_DependencyRecord):
    """One complete, transaction-visible graph at a catalog metadata version.

    Objects and edges are IPC records. The snapshot is advisory to clients:
    mutations recheck the provider's authoritative graph atomically. A provider
    unable to disclose the complete graph rejects this read profile.
    """

    catalog_version: int
    objects: list[bytes] = field(default_factory=list)
    dependencies: list[bytes] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Reject dangling/duplicate edges, duplicate identities and owner cycles."""
        if type(self.catalog_version) is not int or not 0 <= self.catalog_version < 2**63:
            raise ValueError("catalog_version must be a nonnegative int64")
        refs = self.object_refs()
        keys = {ref.key() for ref in refs}
        if len(keys) != len(refs):
            raise ValueError("duplicate object identity")
        seen = set()
        owners = {}
        for edge in self.edges():
            dependent, subject = edge.dependent_ref().key(), edge.subject_ref().key()
            if dependent not in keys or subject not in keys:
                raise ValueError("dangling dependency endpoint")
            if (dependent, subject) in seen:
                raise ValueError("duplicate or conflicting dependency edge")
            seen.add((dependent, subject))
            if edge.kind == "owned":
                if dependent in owners:
                    raise ValueError("an object cannot have multiple owners")
                owners[dependent] = subject
        for start in owners:
            path = set()
            current = start
            while current in owners:
                if current in path:
                    raise ValueError("ownership cycle")
                path.add(current)
                current = owners[current]

    def object_refs(self) -> list[CatalogObjectRef]:
        """Decode every object, including objects with no dependencies."""
        if not isinstance(self.objects, list):
            raise ValueError("objects must be a list")
        return [CatalogObjectRef.deserialize_from_bytes(value) for value in self.objects]

    def edges(self) -> list[ObjectDependency]:
        """Decode every dependency edge."""
        if not isinstance(self.dependencies, list):
            raise ValueError("dependencies must be a list")
        return [ObjectDependency.deserialize_from_bytes(value) for value in self.dependencies]

    def drop_set(self, target: CatalogObjectRef, *, cascade: bool = False) -> list[CatalogObjectRef]:
        """Plan a complete deletion set without effects, independent of edge order.

        Find the closure of automatic drops first, then check blocking edges.
        CASCADE follows both kinds; reference cycles terminate through the set.
        The provider authorizes every deletion before applying any of them.
        """
        self.__post_init__()
        if type(cascade) is not bool:
            raise ValueError("cascade must be a boolean")
        objects = {ref.key(): ref for ref in self.object_refs()}
        if target.key() not in objects:
            raise ValueError("catalog object does not exist")
        edges = [(e.dependent_ref().key(), e.subject_ref().key(), e.kind) for e in self.edges()]
        dropping = {target.key()}
        while True:
            additional = {d for d, s, kind in edges if s in dropping and (cascade or kind == "owned")}
            if additional <= dropping:
                break
            dropping.update(additional)
        if any(s in dropping and d not in dropping for d, s, _ in edges):
            raise ValueError("cannot drop object because dependent objects exist; use CASCADE")
        return [objects[key] for key in sorted(dropping)]
