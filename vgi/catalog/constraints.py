# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Canonical constraint definitions, identities and atomic mutation envelopes.

These records validate the wire contract. Providers still own SQL binding,
identifier resolution, authorization, enforcement, revisions and transactions.
No record alone establishes that a backend enforces a constraint.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Annotated, Any, Self, cast

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass, ArrowType
from vgi_rpc.utils import IpcValidation

from vgi.catalog.capabilities import ROW_SCALAR_EXPRESSION
from vgi.catalog.dependencies import CatalogObjectRef
from vgi.catalog.evaluation_context import QueryEvaluationContext
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


def constraint_definitions(values: list[bytes], table: CatalogObjectRef) -> list[ConstraintDefinition]:
    """Decode the authoritative CREATE list, without inventing names or trust."""
    _reference(table, "table")
    _records(values)
    definitions = [ConstraintDefinition.deserialize_from_bytes(value) for value in values]
    labels = [definition.declared_name for definition in definitions if definition.declared_name is not None]
    if len(labels) != len(set(labels)):
        raise ValueError("duplicate declared constraint label")
    if sum(definition.kind == ConstraintKind.PRIMARY_KEY for definition in definitions) > 1:
        raise ValueError("a table cannot have multiple primary keys")
    for definition in definitions:
        _definition_owner(definition, table)
    return definitions


def constraint_infos(values: list[bytes], table: CatalogObjectRef, revision: bytes | None) -> list[ConstraintInfo]:
    """Validate canonical discovery without requiring mutation support."""
    if revision is not None:
        _token(revision, "table definition revision")
    # Snapshot validation is identical except that read-only discovery may
    # legitimately lack a revision. The placeholder is local validation only.
    snapshot = ConstraintSnapshot(table=table, definition_revision=revision or b"read-only", constraints=values)
    return snapshot.constraint_infos()


def validate_constraint_schema(
    schema: pa.Schema,
    definitions: list[ConstraintDefinition],
    *,
    trusted: list[ConstraintDefinition] | None = None,
    table: str | None = None,
) -> None:
    """Require exact raw paths and agreement between definitions and Arrow nullability.

    For CREATE TABLE requests, whose column definitions the client derives
    from the same constraints: a field (descending through ordinary STRUCTs,
    not logical-type storage) is non-nullable exactly when a NOT_NULL or
    PRIMARY_KEY definition names its path. Served tables do not use this
    check; [`served_table_schema`][] derives their nullability instead.
    ``table`` only labels error messages.
    """
    owner = f"Table '{table}': " if table is not None else ""
    for definition in definitions:
        try:
            definition.validate_schema(schema)
        except ValueError as error:
            raise ValueError(f"{owner}{error}") from error
    required = {
        tuple(path)
        for definition in (definitions if trusted is None else trusted)
        if definition.kind in {ConstraintKind.NOT_NULL, ConstraintKind.PRIMARY_KEY}
        for path in definition.column_paths
    }
    visible_paths: set[tuple[str, ...]] = set()

    def visit(fields: pa.Schema | pa.StructType, prefix: tuple[str, ...]) -> None:
        for column_field in fields:
            if not prefix and b"is_row_id" in (column_field.metadata or {}):
                # Storage identity is not a user-visible SQL constraint target.
                continue
            path = (*prefix, column_field.name)
            visible_paths.add(path)
            if column_field.nullable == (path in required):
                problem = (
                    "the field is nullable but a NOT_NULL/PRIMARY_KEY definition names it"
                    if column_field.nullable
                    else "the field is non-nullable but no NOT_NULL/PRIMARY_KEY definition names it"
                )
                raise ValueError(
                    f"{owner}Arrow nullability disagrees with canonical NOT_NULL/PRIMARY_KEY at column {list(path)!r}: "
                    + problem
                )
            metadata = column_field.metadata or {}
            # A logical type's storage members are not table column paths.
            # In particular Parquet Variant requires a nonnullable metadata
            # child without declaring a SQL NOT NULL constraint on that child.
            if pa.types.is_struct(column_field.type) and not any(
                key == b"ARROW:extension:name" or key.startswith(b"vgi.logical_type.") for key in metadata
            ):
                visit(column_field.type, path)

    visit(schema, ())
    for definition in definitions:
        for path in definition.column_paths:
            if tuple(path) not in visible_paths:
                raise ValueError(
                    f"{owner}constraint path {path!r} addresses a storage identity or logical-type storage member, "
                    "not a table column"
                )


_ROW_ID_KEY = b"is_row_id"


def _row_id_column(schema: pa.Schema) -> int | None:
    """Index of the top-level storage-identity column, as the extension finds it."""
    for index, column_field in enumerate(schema):
        if _ROW_ID_KEY in (column_field.metadata or {}):
            return index
    return None


def _ordinary_struct(column_field: pa.Field[Any]) -> bool:
    """A STRUCT whose children are table column paths, not logical-type storage."""
    if isinstance(column_field.type, pa.BaseExtensionType) or not pa.types.is_struct(column_field.type):
        return False
    return not any(
        key == b"ARROW:extension:name" or key.startswith(b"vgi.logical_type.") for key in column_field.metadata or {}
    )


def _required_paths(constraints: Sequence[ConstraintInfo]) -> set[tuple[str, ...]]:
    return {
        tuple(path)
        for info in constraints
        if info.definition.kind in {ConstraintKind.NOT_NULL, ConstraintKind.PRIMARY_KEY}
        for path in info.definition.column_paths
    }


def served_table_schema(schema: pa.Schema, constraints: Sequence[ConstraintInfo]) -> pa.Schema:
    """Return ``schema`` with nullability derived from the table's records.

    DuckDB requires every field (descending through ordinary STRUCTs) to be
    non-nullable exactly when an ENFORCED + ALL_ROWS NOT_NULL or PRIMARY_KEY
    record names it. Arrow nullability alone is not a claim, so a
    non-nullable field no record names is served nullable, and a field a
    record names is served non-nullable (the record is the worker's
    attestation). The top-level ``is_row_id`` column, logical-type storage
    and extension types are left untouched. Metadata is preserved.
    """
    required = _required_paths(constraints)
    row_id = _row_id_column(schema)

    def rewrite(column_field: pa.Field[Any], path: tuple[str, ...]) -> pa.Field[Any]:
        if _ordinary_struct(column_field):
            children = [
                rewrite(column_field.type.field(i), (*path, column_field.type.field(i).name))
                for i in range(column_field.type.num_fields)
            ]
            column_field = column_field.with_type(pa.struct(children))
        nullable = path not in required
        return column_field if column_field.nullable == nullable else column_field.with_nullable(nullable)

    fields = [
        column_field if index == row_id else rewrite(column_field, (column_field.name,))
        for index, column_field in enumerate(schema)
    ]
    return pa.schema(fields, metadata=cast(Any, schema.metadata))


def _resolve_record_path(schema: pa.Schema, path: ColumnPath, row_id: int | None, *, nested: bool) -> None:
    """Mirror the extension's ``ResolveNotNullPath`` / top-level key resolution."""
    if not nested and len(path) != 1:
        raise ValueError("only NOT_NULL may name a nested column path; keys must name top-level columns")
    matches = [index for index, column_field in enumerate(schema) if column_field.name == path[0]]
    if not matches:
        raise ValueError(f"column path {path!r} names a missing column")
    if matches[0] == row_id:
        raise ValueError(f"constraint paths cannot address storage identities: {path!r} is the is_row_id column")
    column_field = schema.field(matches[0])
    for depth in range(1, len(path)):
        if not _ordinary_struct(column_field):
            raise ValueError(
                f"constraint paths cannot address storage identities or logical-type storage members: {path!r} "
                "must traverse ordinary STRUCT fields"
            )
        children = [i for i in range(column_field.type.num_fields) if column_field.type.field(i).name == path[depth]]
        if len(children) != 1:
            raise ValueError(f"column path {path!r} names a missing or ambiguous nested field")
        column_field = column_field.type.field(children[0])


def validate_constraint_records(
    table: str,
    schema: pa.Schema,
    constraints: Sequence[ConstraintInfo],
    *,
    schema_path: Sequence[str] | None = None,
) -> None:
    """Check a table's records the way the DuckDB extension does at ATTACH.

    Mirrors ``DecodeNativeConstraints``: every record is ENFORCED + ALL_ROWS
    and owned by this table (and ``schema_path``, when given); ids and
    case-insensitive declared names are unique; at most one PRIMARY_KEY;
    column paths resolve (nested paths only for NOT_NULL); FOREIGN_KEY
    targets the same one-component schema with MATCH SIMPLE and NO_ACTION or
    RESTRICT actions; CHECK is a numeric comparison in the extension's
    grammar (checked when a DuckDB engine is installed). Arrow nullability is
    not checked: [`served_table_schema`][] derives it from the records.
    Cross-table FOREIGN_KEY targets are checked by
    [`validate_foreign_key_targets`][].

    Args:
        table: The table name.
        schema: The table's Arrow schema as the provider declares it.
        constraints: The table's records.
        schema_path: The table's schema path; when given, every record must be owned by it.

    Raises:
        ValueError: DuckDB would reject the table at ATTACH. The message
            names the table, constraint id and column path.
    """
    from vgi.catalog._expression_grammar import validate_check_sql

    row_id = _row_id_column(schema)
    ids: set[str] = set()
    labels: set[str] = set()
    primary_keys: list[str] = []
    for info in constraints:
        label = f"Table '{table}': constraint '{info.ref.name}'"
        if info.ref.owner_table != table:
            raise ValueError(f"{label} belongs to table {info.ref.owner_table!r}")
        if schema_path is not None and list(info.ref.schema_path) != list(schema_path):
            raise ValueError(
                f"{label} has schema_path {info.ref.schema_path!r}; it must equal the table's {list(schema_path)!r}"
            )
        if not info.trusted:
            raise ValueError(
                f"{label} is {info.enforcement}/{info.validation}; DuckDB accepts only ENFORCED + ALL_ROWS "
                "records, so state one only when every row the table can return satisfies it"
            )
        if info.ref.name in ids:
            raise ValueError(f"{label}: duplicate constraint id")
        ids.add(info.ref.name)
        definition = info.definition
        if definition.declared_name is not None:
            if definition.declared_name.lower() in labels:
                raise ValueError(f"{label}: duplicate declared name {definition.declared_name!r} (case-insensitive)")
            labels.add(definition.declared_name.lower())
        kind = definition.kind
        for path in definition.column_paths:
            try:
                _resolve_record_path(schema, path, row_id, nested=kind == ConstraintKind.NOT_NULL)
            except ValueError as error:
                raise ValueError(f"{label}: {error}") from error
        if kind == ConstraintKind.PRIMARY_KEY:
            primary_keys.append(info.ref.name)
            if len(primary_keys) > 1:
                raise ValueError(f"Table '{table}': more than one PRIMARY_KEY ({primary_keys!r})")
        elif kind == ConstraintKind.CHECK:
            expression = definition.check_expression
            assert expression is not None
            if expression.profile != ROW_SCALAR_EXPRESSION:
                raise ValueError(f"{label}: CHECK profile must be {ROW_SCALAR_EXPRESSION}")
            if expression.evaluation_context != QueryEvaluationContext():
                raise ValueError(f"{label}: CHECK requires the default evaluation context")
            try:
                validate_check_sql(expression.sql, schema, row_id)
            except ValueError as error:
                raise ValueError(f"{label}: CHECK {expression.sql!r}: {error}") from error
        elif kind == ConstraintKind.FOREIGN_KEY:
            foreign_key = definition.foreign_key
            target = info.referenced_constraint
            assert foreign_key is not None and target is not None
            if list(foreign_key.referenced_table.schema_path) != list(info.ref.schema_path):
                raise ValueError(
                    f"{label}: FOREIGN_KEY must reference a table in the same schema "
                    f"(got {foreign_key.referenced_table.schema_path!r})"
                )
            if foreign_key.match != ForeignKeyMatch.SIMPLE:
                raise ValueError(f"{label}: DuckDB reads only MATCH SIMPLE foreign keys")
            for action in (foreign_key.on_update, foreign_key.on_delete):
                if action not in {ReferentialAction.NO_ACTION, ReferentialAction.RESTRICT}:
                    raise ValueError(f"{label}: DuckDB reads only NO_ACTION/RESTRICT referential actions, not {action}")
            for path in foreign_key.referenced_column_paths:
                if len(path) != 1:
                    raise ValueError(f"{label}: referenced column path {path!r} must name a top-level column")


def validate_foreign_key_targets(tables: Mapping[tuple[tuple[str, ...], str], Sequence[ConstraintInfo]]) -> None:
    """Check every FOREIGN_KEY record against the referenced table's records.

    ``tables`` maps ``(schema_path, table_name)`` to that table's records for
    every table the catalog serves. A FOREIGN_KEY must reference a served
    table whose PRIMARY_KEY or UNIQUE record has the referenced constraint id
    and exactly the referenced column paths.

    Args:
        tables: ``(schema_path, table_name)`` to that table's records.

    Raises:
        ValueError: A FOREIGN_KEY names a missing table or key.
    """
    for (_schema, table), constraints in tables.items():
        for info in constraints:
            if info.definition.kind != ConstraintKind.FOREIGN_KEY:
                continue
            foreign_key = info.definition.foreign_key
            target = info.referenced_constraint
            assert foreign_key is not None and target is not None
            label = f"Table '{table}': constraint '{info.ref.name}'"
            key = (tuple(foreign_key.referenced_table.schema_path), foreign_key.referenced_table.name)
            if key not in tables:
                raise ValueError(f"{label}: referenced table {list(key[0]) + [key[1]]!r} is not served")
            referenced = [item for item in tables[key] if item.ref.name == target.name]
            if not referenced or referenced[0].definition.kind not in {
                ConstraintKind.PRIMARY_KEY,
                ConstraintKind.UNIQUE,
            }:
                raise ValueError(
                    f"{label}: referenced constraint '{target.name}' is not a PRIMARY_KEY or UNIQUE record "
                    f"of table '{key[1]}'"
                )
            if sorted(map(tuple, referenced[0].definition.column_paths)) != sorted(
                map(tuple, foreign_key.referenced_column_paths)
            ):
                raise ValueError(
                    f"{label}: referenced columns {foreign_key.referenced_column_paths!r} differ from "
                    f"'{target.name}' columns {referenced[0].definition.column_paths!r}"
                )


@dataclass(frozen=True, kw_only=True)
class ReferencedKey:
    """The PRIMARY_KEY or UNIQUE key a FOREIGN_KEY record references.

    Attributes:
        schema_path: Schema path of the referenced table.
        table: Referenced table name.
        columns: Raw component paths of the referenced key, in FK column order.
        constraint_id: ``id`` of the referenced table's PRIMARY_KEY or UNIQUE record.
    """

    schema_path: Sequence[str]
    table: str
    columns: Sequence[Sequence[str]]
    constraint_id: str


def _column_paths(columns: Sequence[Sequence[str]], label: str) -> list[ColumnPath]:
    if isinstance(columns, (str, bytes)) or any(isinstance(path, (str, bytes)) for path in columns):
        raise TypeError(f"{label} are raw component paths such as [['id']] or [['bbox', 'xmin']], not names")
    return [list(path) for path in columns]


def constraint_record(
    schema_path: Sequence[str],
    table: str,
    id: str,
    kind: ConstraintKind | str,
    columns: Sequence[Sequence[str]],
    *,
    enforcement: ConstraintEnforcement | str,
    validation: ConstraintValidation | str,
    check_sql: str | None = None,
    references: ReferencedKey | None = None,
    declared_name: str | None = None,
) -> ConstraintInfo:
    """Build one canonical [`ConstraintInfo`][] for ``Table.constraints``.

    The record is the worker's own attestation; the SDK never derives one.
    The DuckDB extension reads only ENFORCED + ALL_ROWS records, so state that
    only when every row the table can ever return satisfies the constraint.

    Args:
        schema_path: Schema path of the owning table.
        table: Owning table name.
        id: Stable provider-owned identity, unique within the table. Never
            derived by the SDK and never a SQL label.
        kind: The constraint kind.
        columns: Raw component paths, e.g. ``[["id"]]`` or
            ``[["bbox", "xmin"]]``; empty for CHECK.
        enforcement: Whether the provider enforces it on every write path.
        validation: Coverage of existing rows.
        check_sql: CHECK only: a DuckDB scalar expression over the table's columns.
        references: FOREIGN_KEY only: the referenced key and its constraint identity.
        declared_name: Optional SQL label (``CONSTRAINT <name>``).

    Returns:
        The validated record.

    Raises:
        ValueError: The arguments do not describe one valid record.
        TypeError: ``columns`` holds names instead of component paths.
    """
    kind = ConstraintKind(kind)
    if (kind == ConstraintKind.CHECK) != (check_sql is not None):
        raise ValueError("check_sql is required for CHECK and only CHECK")
    if (kind == ConstraintKind.FOREIGN_KEY) != (references is not None):
        raise ValueError("references is required for FOREIGN_KEY and only FOREIGN_KEY")
    owner = list(schema_path)
    return ConstraintInfo(
        ref=CatalogObjectRef(kind="constraint", schema_path=owner, name=id, owner_table=table),
        definition=ConstraintDefinition(
            kind=kind,
            column_paths=_column_paths(columns, "columns"),
            declared_name=declared_name,
            check_expression=None
            if check_sql is None
            else SqlExpression(
                sql=check_sql,
                profile=ROW_SCALAR_EXPRESSION,
                schema_path=owner,
                evaluation_context=QueryEvaluationContext(),
            ),
            foreign_key=None
            if references is None
            else ForeignKeyDefinition(
                referenced_table=CatalogObjectRef(
                    kind="table", schema_path=list(references.schema_path), name=references.table
                ),
                referenced_column_paths=_column_paths(references.columns, "references.columns"),
            ),
        ),
        enforcement=ConstraintEnforcement(enforcement),
        validation=ConstraintValidation(validation),
        referenced_constraint=None
        if references is None
        else CatalogObjectRef(
            kind="constraint",
            schema_path=list(references.schema_path),
            name=references.constraint_id,
            owner_table=references.table,
        ),
    )
