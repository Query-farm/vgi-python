# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Canonical constraint definitions, identities and atomic mutation envelopes.

These records validate the wire contract. Providers still own SQL binding,
identifier resolution, authorization, enforcement, revisions and transactions.
No record alone establishes that a backend enforces a constraint.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Annotated, Self

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass, ArrowType
from vgi_rpc.utils import IpcValidation

from vgi.catalog.dependencies import CatalogObjectRef
from vgi.catalog.sql import SqlExpression
from vgi.column_path import ColumnPath, column_path_key, resolve_column_path
from vgi.typed_values import TypedValueBatch

MAX_CONSTRAINT_BYTES = 8 * 1024 * 1024
MAX_CONSTRAINT_TOKEN_BYTES = 64 * 1024
MAX_CONSTRAINT_ITEMS = 1024
MAX_CONSTRAINT_PATH_DEPTH = 64
MAX_CONSTRAINT_IDENTIFIER_BYTES = 4096


class ConstraintKind(StrEnum):
    """Supported definition variants; each has exactly one payload shape."""

    NOT_NULL = "NOT_NULL"
    UNIQUE = "UNIQUE"
    PRIMARY_KEY = "PRIMARY_KEY"
    CHECK = "CHECK"
    FOREIGN_KEY = "FOREIGN_KEY"


class ConstraintEnforcement(StrEnum):
    """Whether the authority enforces this constraint on every write path."""

    ENFORCED = "ENFORCED"
    METADATA_ONLY = "METADATA_ONLY"


class ConstraintValidation(StrEnum):
    """Coverage of existing data; only ALL_ROWS can justify engine facts."""

    ALL_ROWS = "ALL_ROWS"
    FUTURE_WRITES = "FUTURE_WRITES"
    UNVALIDATED = "UNVALIDATED"


class ForeignKeyMatch(StrEnum):
    """Declared NULL matching; FULL requires its negotiated execution profile."""

    SIMPLE = "SIMPLE"
    FULL = "FULL"


class ReferentialAction(StrEnum):
    """Declared referential action, independently gated by provider support."""

    NO_ACTION = "NO_ACTION"
    RESTRICT = "RESTRICT"
    CASCADE = "CASCADE"
    SET_NULL = "SET_NULL"
    SET_DEFAULT = "SET_DEFAULT"


class ConstraintOperation(StrEnum):
    """Operations in one atomic constraint mutation request."""

    ADD = "ADD"
    DROP = "DROP"
    RENAME = "RENAME"
    REPLACE = "REPLACE"


def _identifier(value: object, label: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or "\0" in value
        or len(value.encode()) > MAX_CONSTRAINT_IDENTIFIER_BYTES
    ):
        raise ValueError(f"{label} must be a nonempty bounded identifier without NUL")


def _token(value: object, label: str) -> None:
    if not isinstance(value, bytes) or not 0 < len(value) <= MAX_CONSTRAINT_TOKEN_BYTES:
        raise ValueError(f"{label} must be nonempty bounded bytes")


def _reference(value: CatalogObjectRef, kind: str) -> None:
    if not isinstance(value, CatalogObjectRef):
        raise TypeError("constraint identities require CatalogObjectRef records")
    value.__post_init__()
    if value.kind != kind:
        raise ValueError(f"expected a {kind} reference")
    if len(value.schema_path) > MAX_CONSTRAINT_PATH_DEPTH:
        raise ValueError("constraint schema path is too deep")
    for name in [*value.schema_path, value.name]:
        _identifier(name, "object identifier")
    if value.owner_table is not None:
        _identifier(value.owner_table, "owner table")


def _paths(paths: list[ColumnPath], *, nonempty: bool = True) -> None:
    if not isinstance(paths, list) or len(paths) > MAX_CONSTRAINT_ITEMS or (nonempty and not paths):
        raise ValueError("constraint column paths must be a bounded list with the required arity")
    keys = []
    for path in paths:
        if not isinstance(path, list) or len(path) > MAX_CONSTRAINT_PATH_DEPTH:
            raise ValueError("constraint paths must be bounded lists of raw components")
        keys.append(column_path_key(path))
        for name in path:
            _identifier(name, "column path component")
    if len(set(keys)) != len(keys):
        raise ValueError("constraint column paths must be unique")


def _same_table(ref: CatalogObjectRef, table: CatalogObjectRef) -> None:
    if ref.schema_path != table.schema_path or ref.owner_table != table.name:
        raise ValueError("constraint identity belongs to another table")


def _definition_owner(definition: ConstraintDefinition, table: CatalogObjectRef) -> None:
    if definition.check_expression is not None and definition.check_expression.schema_path != table.schema_path:
        raise ValueError("CHECK expression must bind in its owning table schema")


def _records(values: list[bytes], *, nonempty: bool = False) -> None:
    if not isinstance(values, list) or len(values) > MAX_CONSTRAINT_ITEMS or (nonempty and not values):
        raise ValueError("constraint records must be a bounded list with the required length")
    if any(not isinstance(value, bytes) or not value for value in values):
        raise ValueError("nested constraint records must be nonempty IPC bytes")
    if sum(map(len, values)) > MAX_CONSTRAINT_BYTES:
        raise ValueError("nested constraint records exceed the envelope limit")


class _ConstraintRecord(ArrowSerializableDataclass):
    @classmethod
    def deserialize_from_bytes(cls, data: bytes, ipc_validation: IpcValidation = IpcValidation.FULL) -> Self:
        """Decode one bounded row, rejecting extra batches and trailing bytes."""
        if not isinstance(data, bytes) or not 0 < len(data) <= MAX_CONSTRAINT_BYTES:
            raise ValueError("invalid constraint record size")
        return cls.deserialize_from_batch(TypedValueBatch.deserialize_from_bytes(data).batch)

    @classmethod
    def deserialize_from_batch(
        cls,
        batch: pa.RecordBatch,
        custom_metadata: pa.KeyValueMetadata | None = None,
        *,
        ipc_validation: IpcValidation = IpcValidation.FULL,
    ) -> Self:
        """Require the complete schema and validate buffers and nested nullability."""
        if batch.num_rows != 1 or not batch.schema.equals(cls.ARROW_SCHEMA, check_metadata=True):
            raise ValueError(f"{cls.__name__} requires one row with its exact schema")
        if batch.nbytes > MAX_CONSTRAINT_BYTES:
            raise ValueError("constraint record exceeds the envelope limit")
        TypedValueBatch(batch)
        try:
            return super().deserialize_from_batch(batch, custom_metadata, ipc_validation=IpcValidation.FULL)
        except KeyError as error:
            raise ValueError(f"{cls.__name__} contains an unknown enum or record field") from error

    def serialize_to_bytes(self) -> bytes:
        """Revalidate mutable lists and nested definitions before sending bytes."""
        self.__post_init__()
        result = super().serialize_to_bytes()
        if len(result) > MAX_CONSTRAINT_BYTES:
            raise ValueError("constraint record exceeds the envelope limit")
        return result

    def __post_init__(self) -> None:
        """Validate the concrete record's discriminated payload."""


@dataclass(frozen=True, kw_only=True)
class ForeignKeyDefinition(_ConstraintRecord):
    """An attachment-relative FK target and explicit immediate match/actions."""

    referenced_table: Annotated[CatalogObjectRef, ArrowType(pa.binary())]
    referenced_column_paths: list[ColumnPath]
    match: ForeignKeyMatch = ForeignKeyMatch.SIMPLE
    on_update: ReferentialAction = ReferentialAction.NO_ACTION
    on_delete: ReferentialAction = ReferentialAction.NO_ACTION

    def __post_init__(self) -> None:
        """Validate the target and ordered referenced key without binding SQL."""
        _reference(self.referenced_table, "table")
        _paths(self.referenced_column_paths)
        ForeignKeyMatch(self.match)
        ReferentialAction(self.on_update)
        ReferentialAction(self.on_delete)


@dataclass(frozen=True, kw_only=True)
class ConstraintDefinition(_ConstraintRecord):
    """Exactly one constraint definition; labels are never mutation identity."""

    kind: ConstraintKind
    column_paths: list[ColumnPath] = field(default_factory=list)
    declared_name: str | None = None
    check_expression: Annotated[SqlExpression | None, ArrowType(pa.binary())] = None
    foreign_key: Annotated[ForeignKeyDefinition | None, ArrowType(pa.binary())] = None

    def __post_init__(self) -> None:
        """Reject conflicting payloads, duplicate paths and malformed FK arity."""
        kind = ConstraintKind(self.kind)
        if self.declared_name is not None:
            _identifier(self.declared_name, "constraint declared name")
        _paths(self.column_paths, nonempty=kind != ConstraintKind.CHECK)
        if kind == ConstraintKind.NOT_NULL and len(self.column_paths) != 1:
            raise ValueError("NOT_NULL requires exactly one column path")
        if kind == ConstraintKind.CHECK:
            if self.column_paths or not isinstance(self.check_expression, SqlExpression):
                raise ValueError("CHECK requires exactly one scalar expression and no column paths")
            self.check_expression.__post_init__()
        elif self.check_expression is not None:
            raise ValueError("only CHECK may carry a check expression")
        if kind == ConstraintKind.FOREIGN_KEY:
            if not isinstance(self.foreign_key, ForeignKeyDefinition):
                raise ValueError("FOREIGN_KEY requires a foreign key definition")
            self.foreign_key.__post_init__()
            if len(self.column_paths) != len(self.foreign_key.referenced_column_paths):
                raise ValueError("foreign key local and referenced arity must match")
        elif self.foreign_key is not None:
            raise ValueError("only FOREIGN_KEY may carry a foreign key definition")

    def validate_schema(self, schema: pa.Schema) -> None:
        """Resolve local raw paths exactly; expression binding belongs to the provider."""
        self.__post_init__()
        for path in self.column_paths:
            resolve_column_path(schema, path)


@dataclass(frozen=True, kw_only=True)
class ConstraintInfo(_ConstraintRecord):
    """Canonical readback with stable logical identity and explicit trust state."""

    ref: Annotated[CatalogObjectRef, ArrowType(pa.binary())]
    definition: Annotated[ConstraintDefinition, ArrowType(pa.binary())]
    enforcement: ConstraintEnforcement
    validation: ConstraintValidation
    referenced_constraint: Annotated[CatalogObjectRef | None, ArrowType(pa.binary())] = None

    def __post_init__(self) -> None:
        """Verify ownership and the exact resolved referenced-key identity."""
        _reference(self.ref, "constraint")
        if not isinstance(self.definition, ConstraintDefinition):
            raise TypeError("constraint info requires a typed definition")
        self.definition.__post_init__()
        ConstraintEnforcement(self.enforcement)
        ConstraintValidation(self.validation)
        if (
            self.definition.check_expression is not None
            and self.definition.check_expression.schema_path != self.ref.schema_path
        ):
            raise ValueError("CHECK expression must bind in its owning table schema")
        if self.definition.kind == ConstraintKind.FOREIGN_KEY:
            if self.referenced_constraint is None:
                raise ValueError("foreign key readback requires its resolved referenced constraint")
            _reference(self.referenced_constraint, "constraint")
            assert self.definition.foreign_key is not None
            _same_table(self.referenced_constraint, self.definition.foreign_key.referenced_table)
            if self.referenced_constraint.key() == self.ref.key():
                raise ValueError("a foreign key cannot reference itself as its unique key")
        elif self.referenced_constraint is not None:
            raise ValueError("only foreign key readback may reference another constraint")

    @property
    def trusted(self) -> bool:
        """Whether declared coverage permits engine facts, assuming an honest provider."""
        return self.enforcement == ConstraintEnforcement.ENFORCED and self.validation == ConstraintValidation.ALL_ROWS


@dataclass(frozen=True, kw_only=True)
class ConstraintChange(_ConstraintRecord):
    """Strict ADD/DROP/RENAME/REPLACE union for one atomic apply request."""

    operation: ConstraintOperation
    ref: Annotated[CatalogObjectRef | None, ArrowType(pa.binary())] = None
    definition: Annotated[ConstraintDefinition | None, ArrowType(pa.binary())] = None
    enforcement: ConstraintEnforcement | None = None
    validation: ConstraintValidation | None = None
    cascade: bool | None = None
    declared_name: str | None = None

    def __post_init__(self) -> None:
        """Require relevant fields and reject every conflicting optional field."""
        operation = ConstraintOperation(self.operation)
        if operation == ConstraintOperation.ADD:
            if self.ref is not None:
                raise ValueError("ADD cannot supply an existing constraint identity")
        elif self.ref is None:
            raise ValueError("constraint mutation requires an existing identity")
        else:
            _reference(self.ref, "constraint")
        if operation in {ConstraintOperation.ADD, ConstraintOperation.REPLACE}:
            if not isinstance(self.definition, ConstraintDefinition):
                raise ValueError("ADD/REPLACE requires a typed definition")
            self.definition.__post_init__()
            if self.enforcement is None or self.validation is None:
                raise ValueError("ADD/REPLACE requires explicit enforcement and validation")
            ConstraintEnforcement(self.enforcement)
            ConstraintValidation(self.validation)
            if self.cascade is not None or self.declared_name is not None:
                raise ValueError("ADD/REPLACE cannot carry DROP/RENAME fields")
        elif self.definition is not None or self.enforcement is not None or self.validation is not None:
            raise ValueError("DROP/RENAME cannot carry definition or enforcement fields")
        if operation == ConstraintOperation.DROP:
            if type(self.cascade) is not bool or self.declared_name is not None:
                raise ValueError("DROP requires an explicit cascade boolean and no declared name")
        elif self.cascade is not None:
            raise ValueError("only DROP may specify cascade")
        if operation == ConstraintOperation.RENAME and self.declared_name is not None:
            _identifier(self.declared_name, "replacement constraint label")


@dataclass(frozen=True, kw_only=True)
class ConstraintSnapshot(_ConstraintRecord):
    """Complete constraint readback at one table semantic-definition revision."""

    table: Annotated[CatalogObjectRef, ArrowType(pa.binary())]
    definition_revision: bytes
    constraints: list[bytes]

    def __post_init__(self) -> None:
        """Reject mixed owners, duplicate stable keys/labels and multiple primary keys."""
        _reference(self.table, "table")
        _token(self.definition_revision, "table definition revision")
        infos = self.constraint_infos()
        keys = set()
        labels = set()
        primary_keys = 0
        for info in infos:
            _same_table(info.ref, self.table)
            if info.ref.key() in keys:
                raise ValueError("duplicate constraint identity in snapshot")
            keys.add(info.ref.key())
            label = info.definition.declared_name
            if label is not None:
                if label in labels:
                    raise ValueError("duplicate declared constraint label in snapshot")
                labels.add(label)
            primary_keys += info.definition.kind == ConstraintKind.PRIMARY_KEY
        if primary_keys > 1:
            raise ValueError("a table cannot have multiple primary keys")

    def constraint_infos(self) -> list[ConstraintInfo]:
        """Decode the bounded ordered readback records without normalizing identities."""
        _records(self.constraints)
        return [ConstraintInfo.deserialize_from_bytes(value) for value in self.constraints]


@dataclass(frozen=True, kw_only=True)
class ConstraintsApplyRequest(_ConstraintRecord):
    """One owner-scoped atomic mutation guarded by an expected table revision."""

    attach_opaque_data: bytes
    table: Annotated[CatalogObjectRef, ArrowType(pa.binary())]
    expected_revision: bytes
    changes: list[bytes]
    transaction_opaque_data: bytes | None = None

    def __post_init__(self) -> None:
        """Validate scope and forbid repeated mutation of the same existing key."""
        _token(self.attach_opaque_data, "attachment")
        _reference(self.table, "table")
        _token(self.expected_revision, "expected table revision")
        if self.transaction_opaque_data is not None:
            _token(self.transaction_opaque_data, "transaction")
        keys = set()
        for change in self.constraint_changes():
            if change.ref is not None:
                _same_table(change.ref, self.table)
                if change.ref.key() in keys:
                    raise ValueError("one request cannot address an existing constraint key twice")
                keys.add(change.ref.key())
            if change.definition is not None:
                _definition_owner(change.definition, self.table)

    def constraint_changes(self) -> list[ConstraintChange]:
        """Decode all ordered changes before an authority attempts effects."""
        _records(self.changes, nonempty=True)
        return [ConstraintChange.deserialize_from_bytes(value) for value in self.changes]
