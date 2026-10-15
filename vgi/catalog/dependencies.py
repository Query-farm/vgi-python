# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Attachment-relative object identities and atomic dependency-drop planning."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from typing import Literal, Self, TypeAlias

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.utils import IpcValidation

from vgi.column_path import column_path_key, resolve_column_path
from vgi.typed_values import TypedValueBatch

ObjectKey: TypeAlias = tuple[str, tuple[str, ...], str, str]  # noqa: UP040
ColumnKey: TypeAlias = tuple[str, ...]  # noqa: UP040
ScopedKey: TypeAlias = tuple[ObjectKey, ColumnKey | None]  # noqa: UP040
AlterAction: TypeAlias = Literal["rename", "type", "drop"]  # noqa: UP040


class _DependencyRecord(ArrowSerializableDataclass):
    def serialize_to_bytes(self) -> bytes:
        """Revalidate mutable path lists before writing a dependency record."""
        self.__post_init__()
        return super().serialize_to_bytes()

    def __post_init__(self) -> None:
        """Validate the concrete record."""

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
    Other objects use their owning schema path. Table-owned triggers and
    constraints require owner_table. Names are raw components; a provider
    returns canonical spelling and resolves request identifiers according to
    its declared name policy.
    Function and macro references identify the complete overload family.
    For constraints, name is a persisted provider-issued logical key compared
    exactly, never a SQL label or engine OID; declared-name changes retain it.
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
            "constraint",
        }:
            raise ValueError("unknown catalog object kind")
        if not isinstance(self.schema_path, list):
            raise ValueError("schema_path must be a list of raw identifiers")
        if not self.schema_path and self.kind != "schema":
            raise ValueError("non-schema objects require an owning schema")
        for component in [*self.schema_path, self.name]:
            _identifier(component)
        if self.kind in {"trigger", "constraint"}:
            _identifier(self.owner_table)
        elif self.owner_table is not None:
            raise ValueError("only triggers and constraints have a table-owned namespace")

    def key(self) -> ObjectKey:
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
    blocks_alter: bool
    kind: str = "requires"
    subject_column_paths: list[list[str]] | None = None
    dependent_column_path: list[str] | None = None

    def __post_init__(self) -> None:
        """Validate structural identities; schemas must prove actual columns."""
        if self.kind not in {"requires", "owned"}:
            raise ValueError("unknown dependency kind")
        if type(self.blocks_alter) is not bool:
            raise ValueError("blocks_alter must be a boolean")
        dependent, subject = self.dependent_ref(), self.subject_ref()
        if self.dependent_column_path is not None:
            _column_key(self.dependent_column_path)
            if dependent.kind != "table":
                raise ValueError("dependent column paths require a table reference")
        if self.subject_column_paths is not None:
            if not isinstance(self.subject_column_paths, list) or not self.subject_column_paths:
                raise ValueError("subject column paths must be a nonempty list or NULL")
            paths = [_column_key(path) for path in self.subject_column_paths]
            if len(set(paths)) != len(paths):
                raise ValueError("subject column paths must be unique")
            if subject.kind not in {"table", "view"}:
                raise ValueError("subject column paths require a table or view reference")
        if dependent.key() == subject.key() and (
            self.dependent_column_path is None
            or self.subject_column_paths is None
            or any(_overlaps(tuple(self.dependent_column_path), tuple(path)) for path in self.subject_column_paths)
        ):
            raise ValueError("self dependencies require distinct nonoverlapping column targets")

    def dependent_ref(self) -> CatalogObjectRef:
        """Decode the dependent's resolved identity."""
        return CatalogObjectRef.deserialize_from_bytes(self.dependent)

    def subject_ref(self) -> CatalogObjectRef:
        """Decode the subject's resolved identity."""
        return CatalogObjectRef.deserialize_from_bytes(self.subject)

    def identity(self) -> tuple[ObjectKey, ColumnKey | None, ObjectKey]:
        """Return edge identity, excluding the mergeable subject path set."""
        return (
            self.dependent_ref().key(),
            None if self.dependent_column_path is None else _column_key(self.dependent_column_path),
            self.subject_ref().key(),
        )


def _column_key(path: list[str]) -> ColumnKey:
    if not isinstance(path, list):
        raise ValueError("column paths must be lists of raw identifiers")
    return column_path_key(path)


def _overlaps(left: ColumnKey | None, right: ColumnKey | None) -> bool:
    return left is None or right is None or left[: len(right)] == right or right[: len(left)] == left


def _scope_sort_key(scope: ScopedKey) -> tuple[ObjectKey, bool, ColumnKey]:
    return scope[0], scope[1] is not None, scope[1] or ()


def merge_dependencies(edges: list[ObjectDependency]) -> list[ObjectDependency]:
    """Canonicalize identical edges, merging only their protected subject paths.

    NULL protects the whole subject and dominates a narrower set. Explicit
    parent and child paths are retained so schema validation can prove every
    claimed field. Different dependent columns remain separate edges, and
    conflicting kind/ALTER promises are invalid.
    """
    merged: dict[tuple[ObjectKey, ColumnKey | None, ObjectKey], ObjectDependency] = {}
    for edge in edges:
        edge.__post_init__()
        identity = edge.identity()
        previous = merged.get(identity)
        if previous is not None and (previous.kind != edge.kind or previous.blocks_alter != edge.blocks_alter):
            raise ValueError("conflicting dependency kind or blocks_alter flag")
        paths = edge.subject_column_paths
        if previous is not None:
            if previous.subject_column_paths is None or paths is None:
                paths = None
            else:
                paths = previous.subject_column_paths + paths
        if paths is not None:
            keys = sorted({tuple(path) for path in paths})
            paths = [list(key) for key in keys]
        merged[identity] = replace(edge, subject_column_paths=paths)
    return sorted(
        merged.values(),
        key=lambda edge: (_scope_sort_key((edge.identity()[0], edge.identity()[1])), edge.identity()[2]),
    )


@dataclass(frozen=True, kw_only=True)
class ColumnDrop:
    """One column deletion; it never grants permission to delete its table."""

    table: CatalogObjectRef
    column_path: list[str]


@dataclass(frozen=True, kw_only=True)
class SemanticAlterPlan:
    """An advisory mutation closure, requiring provider authorization and atomicity.

    Rename/type plans have no deletion side effects. Column DROP separates
    objects and columns so a generated-column edge cannot erase its table.
    The provider must validate schemas, rewrite metadata, and authorize every
    returned deletion against its transaction-visible graph before effects.
    """

    target: CatalogObjectRef
    column_path: list[str] | None
    action: AlterAction
    drop_objects: list[CatalogObjectRef] = field(default_factory=list)
    drop_columns: list[ColumnDrop] = field(default_factory=list)


def _dependency_cycles(edges: list[ObjectDependency], *, ownership_only: bool) -> None:
    selected = [edge for edge in edges if not ownership_only or edge.kind == "owned"]
    nodes = {(edge.identity()[0], edge.identity()[1]) for edge in selected}
    adjacency: dict[ScopedKey, set[ScopedKey]] = {node: set() for node in nodes}
    for edge in selected:
        dependent, path, subject = edge.identity()
        subjects: list[ColumnKey | None] = (
            [None] if edge.subject_column_paths is None else [tuple(path) for path in edge.subject_column_paths]
        )
        adjacency[(dependent, path)].update(
            node
            for node in nodes
            if node[0] == subject
            and any(
                _overlaps(node[1], path)
                and (ownership_only or path is None or node[1] is not None or subject[0] != "table")
                for path in subjects
            )
        )
        # A table column reads a column value, not every whole-table lifetime
        # dependency. Otherwise T.g -> T.a would enter an unrelated legal
        # T -> index -> T requires/owned cycle. View columns still depend on
        # the whole view definition; ownership validation always follows the
        # full lifetime relationship, regardless of protected column paths.
    for start in nodes:
        # Object-only reference cycles (including backing-index requires/owned
        # pairs) are valid. Any column participating in a cycle is invalid.
        if not ownership_only and start[1] is None:
            continue
        pending = list(adjacency[start])
        visited = set()
        while pending:
            current = pending.pop()
            if current == start:
                raise ValueError("ownership cycle" if ownership_only else "column dependency cycle")
            if current not in visited:
                visited.add(current)
                pending.extend(adjacency[current] - visited)


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
        """Merge edge paths; reject dangling identities, conflicting policies and cycles."""
        if type(self.catalog_version) is not int or not 0 <= self.catalog_version < 2**63:
            raise ValueError("catalog_version must be a nonnegative int64")
        refs = self.object_refs()
        keys = {ref.key() for ref in refs}
        if len(keys) != len(refs):
            raise ValueError("duplicate object identity")
        edges = merge_dependencies(self.edges())
        owners: dict[ScopedKey, ObjectKey] = {}
        for edge in edges:
            dependent, column, subject = edge.identity()
            if dependent not in keys or subject not in keys:
                raise ValueError("dangling dependency endpoint")
            if edge.kind == "owned":
                if (dependent, column) in owners:
                    raise ValueError("an object cannot have multiple owners")
                owners[(dependent, column)] = subject
        _dependency_cycles(edges, ownership_only=True)
        _dependency_cycles(edges, ownership_only=False)
        object.__setattr__(self, "dependencies", [edge.serialize_to_bytes() for edge in edges])

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
        plan = self.plan_alter(target, action="drop", cascade=cascade)
        if plan.drop_columns:
            raise ValueError("object-only drop_set cannot express dependent column drops; use plan_alter")
        return plan.drop_objects

    def validate_columns(self, schemas: Mapping[ObjectKey, pa.Schema]) -> None:
        """Prove all claimed columns using authoritative transaction-visible schemas.

        The snapshot contains no schemas. A structurally valid edge therefore
        does not prove that its columns exist; callers supplying schemas must
        include every object with column-scoped references.
        """
        self.__post_init__()
        for edge in self.edges():
            dependent, path, subject = edge.identity()
            claims = []
            if path is not None:
                claims.append((dependent, path))
            if edge.subject_column_paths is not None:
                claims.extend((subject, tuple(path)) for path in edge.subject_column_paths)
            for key, column in claims:
                schema = schemas.get(key)
                if schema is None:
                    raise ValueError("authoritative schema required for every dependency column")
                resolve_column_path(schema, column)

    def _alter_target(
        self,
        target: CatalogObjectRef,
        column_path: list[str] | None,
        action: AlterAction,
        schemas: Mapping[ObjectKey, pa.Schema] | None,
    ) -> ScopedKey:
        self.__post_init__()
        if action not in {"rename", "type", "drop"}:
            raise ValueError("unknown semantic ALTER action")
        if target.key() not in {ref.key() for ref in self.object_refs()}:
            raise ValueError("catalog object does not exist")
        path = None if column_path is None else _column_key(column_path)
        if path is not None and target.kind != "table":
            raise ValueError("semantic column ALTER requires a table reference")
        if schemas is not None:
            self.validate_columns(schemas)
            if path is not None:
                schema = schemas.get(target.key())
                if schema is None:
                    raise ValueError("authoritative schema required for the ALTER column")
                resolve_column_path(schema, path)
        return target.key(), path

    def alter_blockers(
        self,
        target: CatalogObjectRef,
        *,
        column_path: list[str] | None = None,
        action: AlterAction = "rename",
        schemas: Mapping[ObjectKey, pa.Schema] | None = None,
    ) -> list[ObjectDependency]:
        """Return directly affected edges, preserving raw parent/child protection.

        Rename/type changes honor blocks_alter. DROP must resolve every affected
        edge regardless of that flag. An outgoing generated-column dependency
        does not block dropping that generated column itself.
        """
        subject, path = self._alter_target(target, column_path, action, schemas)
        return [
            edge
            for edge in self.edges()
            if edge.subject_ref().key() == subject
            and (action == "drop" or edge.blocks_alter)
            and (
                edge.subject_column_paths is None
                or any(_overlaps(path, tuple(protected)) for protected in edge.subject_column_paths)
            )
        ]

    def assert_alter_allowed(
        self,
        target: CatalogObjectRef,
        *,
        column_path: list[str] | None = None,
        action: AlterAction = "rename",
        schemas: Mapping[ObjectKey, pa.Schema] | None = None,
    ) -> None:
        """Reject a semantic change that would require resolving dependencies.

        This checks an isolated alteration only. DROP with automatic ownership
        cleanup or CASCADE requires plan_alter and authorization of its closure.
        """
        if self.alter_blockers(target, column_path=column_path, action=action, schemas=schemas):
            raise ValueError("cannot alter object or column because dependent objects or columns exist")

    def plan_alter(
        self,
        target: CatalogObjectRef,
        *,
        column_path: list[str] | None = None,
        action: AlterAction,
        cascade: bool = False,
        schemas: Mapping[ObjectKey, pa.Schema] | None = None,
    ) -> SemanticAlterPlan:
        """Plan rename/type blockers or a complete object/column DROP closure.

        Owned deletions expand before RESTRICT blockers are checked. CASCADE
        follows both kinds. All DROP edges apply even when blocks_alter is
        false. Parent column drops cover their descendants, while a dependent
        column never expands into deletion of its entire table.
        """
        start = self._alter_target(target, column_path, action, schemas)
        if type(cascade) is not bool:
            raise ValueError("cascade must be a boolean")
        if action != "drop":
            self.assert_alter_allowed(target, column_path=column_path, action=action, schemas=schemas)
            return SemanticAlterPlan(target=target, column_path=column_path, action=action)
        objects = {ref.key(): ref for ref in self.object_refs()}
        dropping = {start}
        edges = self.edges()

        def deleted(scope: ScopedKey) -> bool:
            return any(
                key == scope[0] and (path is None or scope[1] is not None and scope[1][: len(path)] == path)
                for key, path in dropping
            )

        def affected(edge: ObjectDependency) -> bool:
            subject = edge.subject_ref().key()
            paths: list[ColumnKey | None] = (
                [None] if edge.subject_column_paths is None else [tuple(path) for path in edge.subject_column_paths]
            )
            return any(
                key == subject and any(_overlaps(path, protected) for protected in paths) for key, path in dropping
            )

        while True:
            additional = {
                (edge.identity()[0], edge.identity()[1])
                for edge in edges
                if affected(edge) and (cascade or edge.kind == "owned")
            }
            if additional <= dropping:
                break
            dropping.update(additional)
        if any(affected(edge) and not deleted((edge.identity()[0], edge.identity()[1])) for edge in edges):
            raise ValueError("cannot drop object or column because dependent objects or columns exist; use CASCADE")
        # A whole-object deletion or a parent column subsumes smaller deletions.
        minimal = {
            scope
            for scope in dropping
            if not any(
                other != scope
                and other[0] == scope[0]
                and (other[1] is None or scope[1] is not None and scope[1][: len(other[1])] == other[1])
                for other in dropping
            )
        }
        return SemanticAlterPlan(
            target=target,
            column_path=None if column_path is None else list(column_path),
            action=action,
            drop_objects=[objects[key] for key, path in sorted(minimal, key=_scope_sort_key) if path is None],
            drop_columns=[
                ColumnDrop(table=objects[key], column_path=list(path))
                for key, path in sorted(minimal, key=_scope_sort_key)
                if path is not None
            ],
        )
