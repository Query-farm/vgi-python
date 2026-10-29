# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D102, D103, D107

"""Durable, directly invoked canonical-constraint authority.

This fixture deliberately does not advertise a public catalog capability or
replace legacy TableInfo. SQLite snapshots contain both Arrow data and canonical
constraint IPC. Every candidate is fully validated before publication. Its
closed scope is top-level boolean, signed-integer and string columns, immediate
ENFORCED/ALL_ROWS constraints, and MATCH SIMPLE NO_ACTION/RESTRICT foreign keys.
DuckDB binds/evaluates CHECKs through the shared deterministic expression
validator; the fixture never interprets CHECK text itself. Replacing an existing
FK preserves its exact referenced key; changing that target is explicitly outside
this reference and requires dropping/recreating the FK in separate validated
operations. Default/generated column semantics are outside this reference.
"""

from __future__ import annotations

import base64
import json
import sqlite3
import uuid
from contextlib import closing
from dataclasses import replace
from typing import Any

import duckdb
import pyarrow as pa

from vgi.catalog.constraints import (
    ConstraintDefinition,
    ConstraintEnforcement,
    ConstraintInfo,
    ConstraintKind,
    ConstraintOperation,
    ConstraintsApplyRequest,
    ConstraintSnapshot,
    ConstraintValidation,
    ForeignKeyMatch,
    ReferentialAction,
)
from vgi.catalog.dependencies import CatalogObjectRef

from .expression_reference import evaluate_expression


def _fold(value: str) -> str:
    return value.translate(str.maketrans("ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"))


def _table_key(ref: CatalogObjectRef) -> str:
    ref.__post_init__()
    if ref.kind != "table":
        raise ValueError("expected a table identity")
    return json.dumps([[_fold(part) for part in ref.schema_path], _fold(ref.name)])


def _b64(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def _unb64(value: str) -> bytes:
    return base64.b64decode(value, validate=True)


def _quoted(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _encode_rows(rows: pa.Table) -> str:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, rows.schema) as writer:
        writer.write_table(rows)
    return _b64(sink.getvalue().to_pybytes())


def _infos(record: dict[str, Any]) -> list[ConstraintInfo]:
    return [ConstraintInfo.deserialize_from_bytes(_unb64(value)) for value in record["constraints"]]


def _set_infos(record: dict[str, Any], infos: list[ConstraintInfo]) -> None:
    record["constraints"] = [_b64(info.serialize_to_bytes()) for info in infos]


def _table_ref(record: dict[str, Any]) -> CatalogObjectRef:
    return CatalogObjectRef.deserialize_from_bytes(_unb64(record["table"]))


def _base_schema(record: dict[str, Any]) -> pa.Schema:
    return pa.ipc.read_schema(pa.py_buffer(_unb64(record["schema"])))


def _rows(record: dict[str, Any]) -> pa.Table:
    with pa.ipc.open_stream(_unb64(record["rows"])) as reader:
        return reader.read_all()


def _schema(record: dict[str, Any]) -> pa.Schema:
    required = {
        path[0]
        for info in _infos(record)
        if info.definition.kind in (ConstraintKind.NOT_NULL, ConstraintKind.PRIMARY_KEY)
        for path in info.definition.column_paths
    }
    base = _base_schema(record)
    return pa.schema(
        [field.with_nullable(field.name not in required) for field in base],
        metadata=None if base.metadata is None else {key: value for key, value in (base.metadata).items()},
    )


def _touch(record: dict[str, Any]) -> None:
    record["revision"] = _b64(uuid.uuid4().bytes)


class ConstraintCatalog:
    """Serial SQLite publication with persisted transaction-local snapshots.

    Each direct mutation is an atomic statement: a failed candidate preserves
    earlier transaction work. Tokens are owner-bound; concurrent committed
    changes cause snapshot-commit conflicts. This is reference evidence, not a
    negotiated general statement/savepoint implementation.
    """

    def __init__(self, database_path: str) -> None:
        self.database_path = database_path
        with closing(sqlite3.connect(database_path)) as db, db:
            db.execute("CREATE TABLE IF NOT EXISTS catalog (id INTEGER PRIMARY KEY, version INTEGER, data TEXT)")
            db.execute("INSERT OR IGNORE INTO catalog VALUES (1, 1, '{}')")
            db.execute(
                "CREATE TABLE IF NOT EXISTS transactions "
                "(id BLOB PRIMARY KEY, owner BLOB, base INTEGER, version INTEGER, data TEXT, status TEXT)"
            )

    @staticmethod
    def _snapshot(db: sqlite3.Connection, attach: bytes, tx: bytes | None) -> tuple[int, dict[str, Any]]:
        if not attach:
            raise ValueError("attachment identity is required")
        if tx is None:
            version, data = db.execute("SELECT version, data FROM catalog WHERE id=1").fetchone()
        else:
            row = db.execute(
                "SELECT version, data, status FROM transactions WHERE id=? AND owner=?", (tx, attach)
            ).fetchone()
            if row is None or row[2] != "open":
                raise ValueError("transaction is not open for this owner")
            version, data, _ = row
        return version, json.loads(data)

    def _after_write(self) -> None:
        """Fault-injection point after SQLite writes and before commit."""

    def _publish(self, db: sqlite3.Connection, version: int, data: dict[str, Any], tx: bytes | None) -> None:
        self._validate(data)
        if tx is None:
            db.execute("UPDATE catalog SET version=?, data=? WHERE id=1", (version + 1, json.dumps(data)))
        else:
            db.execute("UPDATE transactions SET version=?, data=? WHERE id=?", (version + 1, json.dumps(data), tx))
        self._after_write()

    def begin(self, attach: bytes) -> bytes:
        tx = uuid.uuid4().bytes
        with closing(sqlite3.connect(self.database_path)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            version, data = self._snapshot(db, attach, None)
            db.execute(
                "INSERT INTO transactions VALUES (?, ?, ?, ?, ?, 'open')",
                (tx, attach, version, version, json.dumps(data)),
            )
        return tx

    def _finish(self, attach: bytes, tx: bytes, status: str) -> None:
        with closing(sqlite3.connect(self.database_path)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT base, version, data, status FROM transactions WHERE id=? AND owner=?", (tx, attach)
            ).fetchone()
            if row is None:
                raise ValueError("unknown transaction")
            base, version, data, previous = row
            if previous == status:
                return
            if previous != "open":
                raise ValueError("transaction is terminal")
            if status == "committed" and version != base:
                current = db.execute("SELECT version FROM catalog WHERE id=1").fetchone()[0]
                if current != base:
                    raise ValueError("transaction conflict")
                db.execute("UPDATE catalog SET version=?, data=? WHERE id=1", (version, data))
            db.execute("UPDATE transactions SET status=? WHERE id=?", (status, tx))
            self._after_write()

    def commit(self, attach: bytes, tx: bytes) -> None:
        self._finish(attach, tx, "committed")

    def rollback(self, attach: bytes, tx: bytes) -> None:
        self._finish(attach, tx, "rolled_back")

    def version(self, attach: bytes, tx: bytes | None = None) -> int:
        with closing(sqlite3.connect(self.database_path)) as db:
            return self._snapshot(db, attach, tx)[0]

    @staticmethod
    def _lookup(data: dict[str, Any], table: CatalogObjectRef) -> dict[str, Any]:
        record = data.get(_table_key(table))
        if record is None:
            raise ValueError("table does not exist")
        assert isinstance(record, dict)
        return record

    @staticmethod
    def _readback(record: dict[str, Any]) -> ConstraintSnapshot:
        return ConstraintSnapshot(
            table=_table_ref(record),
            definition_revision=_unb64(record["revision"]),
            constraints=[info.serialize_to_bytes() for info in _infos(record)],
        )

    def get(self, attach: bytes, table: CatalogObjectRef, tx: bytes | None = None) -> ConstraintSnapshot:
        with closing(sqlite3.connect(self.database_path)) as db:
            _, data = self._snapshot(db, attach, tx)
            return self._readback(self._lookup(data, table))

    def columns(self, attach: bytes, table: CatalogObjectRef, tx: bytes | None = None) -> pa.Schema:
        with closing(sqlite3.connect(self.database_path)) as db:
            _, data = self._snapshot(db, attach, tx)
            return _schema(self._lookup(data, table))

    def rows(self, attach: bytes, table: CatalogObjectRef, tx: bytes | None = None) -> pa.Table:
        with closing(sqlite3.connect(self.database_path)) as db:
            _, data = self._snapshot(db, attach, tx)
            record = self._lookup(data, table)
            return pa.Table.from_arrays(_rows(record).columns, schema=_schema(record))

    @staticmethod
    def _column_names(schema: pa.Schema, paths: list[list[str]]) -> list[str]:
        names = []
        for path in paths:
            if len(path) != 1:
                raise ValueError("nested constraint paths are outside this reference")
            if path[0] not in schema.names:
                raise ValueError("unknown constraint column")
            names.append(path[0])
        return names

    @staticmethod
    def _check_columns(columns: pa.Schema) -> None:
        if not columns.names or len({_fold(name) for name in columns.names}) != len(columns.names):
            raise ValueError("expected nonempty, unambiguous table columns")
        for field in columns:
            if not field.name or "\x00" in field.name:
                raise ValueError("invalid column name")
            if not (
                pa.types.is_boolean(field.type)
                or pa.types.is_signed_integer(field.type)
                or pa.types.is_string(field.type)
            ):
                raise ValueError("unsupported constraint reference column type")
            if any(
                key.startswith((b"vgi.logical_type.", b"ARROW:extension:", b"vgi.column."))
                or key in (b"default", b"generated", b"generation")
                for key in field.metadata or {}
            ):
                raise ValueError("column semantics are outside this reference")

    def _new_info(
        self,
        data: dict[str, Any],
        table: CatalogObjectRef,
        definition: ConstraintDefinition,
        *,
        ref: CatalogObjectRef | None = None,
        previous: ConstraintInfo | None = None,
    ) -> ConstraintInfo:
        definition.__post_init__()
        referenced = None
        if definition.foreign_key is not None:
            fk = definition.foreign_key
            parent = self._lookup(data, fk.referenced_table)
            canonical = _table_ref(parent)
            definition = replace(definition, foreign_key=replace(fk, referenced_table=canonical))
            if previous is not None and previous.referenced_constraint is not None:
                previous_fk = previous.definition.foreign_key
                assert previous_fk is not None
                if (
                    previous_fk.referenced_table != canonical
                    or previous_fk.referenced_column_paths != fk.referenced_column_paths
                ):
                    raise ValueError("changing an existing foreign-key target is outside this reference")
                referenced = previous.referenced_constraint
            else:
                candidates = [
                    item.ref
                    for item in _infos(parent)
                    if item.definition.kind in (ConstraintKind.UNIQUE, ConstraintKind.PRIMARY_KEY)
                    and item.definition.column_paths == fk.referenced_column_paths
                ]
                if not candidates:
                    raise ValueError("foreign key requires an exact referenced UNIQUE/PRIMARY key")
                referenced = candidates[0]
        return ConstraintInfo(
            ref=ref
            or CatalogObjectRef(
                kind="constraint", schema_path=table.schema_path, owner_table=table.name, name=uuid.uuid4().hex
            ),
            definition=definition,
            enforcement=ConstraintEnforcement.ENFORCED,
            validation=ConstraintValidation.ALL_ROWS,
            referenced_constraint=referenced,
        )

    def create(
        self,
        attach: bytes,
        table: CatalogObjectRef,
        columns: pa.Schema,
        definitions: list[ConstraintDefinition],
        *,
        tx: bytes | None = None,
    ) -> ConstraintSnapshot:
        self._check_columns(columns)
        with closing(sqlite3.connect(self.database_path)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            version, data = self._snapshot(db, attach, tx)
            key = _table_key(table)
            if key in data:
                raise ValueError("table already exists")
            base = pa.schema(
                [field.with_nullable(True) for field in columns],
                metadata=None
                if columns.metadata is None
                else {key: value for key, value in (columns.metadata).items()},
            )
            record = {
                "table": _b64(table.serialize_to_bytes()),
                "schema": _b64(base.serialize().to_pybytes()),
                "constraints": [],
                "rows": _encode_rows(pa.Table.from_pylist([], schema=base)),
            }
            _touch(record)
            data[key] = record
            # Keys precede FKs so self references resolve without depending on
            # caller declaration order. Canonical readback retains that order.
            infos = {}
            for index, definition in sorted(
                enumerate(definitions), key=lambda item: item[1].kind == ConstraintKind.FOREIGN_KEY
            ):
                infos[index] = self._new_info(data, table, definition)
                _set_infos(record, list(infos.values()))
            _set_infos(record, [infos[index] for index in range(len(definitions))])
            if not columns.equals(_schema(record), check_metadata=True):
                raise ValueError("Arrow nullability must agree with canonical NOT_NULL/PRIMARY constraints")
            self._publish(db, version, data, tx)
            return self._readback(record)

    def _validate(self, data: dict[str, Any]) -> None:
        """Bind definitions and validate the entire candidate data/dependency state."""
        aliases = {key: f"input_{index}" for index, key in enumerate(data)}
        all_infos = {info.ref.key(): info for record in data.values() for info in _infos(record)}
        with duckdb.connect(config={"enable_external_access": False}) as connection:
            for key, record in data.items():
                connection.register(aliases[key], _rows(record))
            for key, record in data.items():
                table, schema, rows = _table_ref(record), _schema(record), _rows(record)
                infos = _infos(record)
                labels = [
                    _fold(info.definition.declared_name) for info in infos if info.definition.declared_name is not None
                ]
                if len(labels) != len(set(labels)):
                    raise ValueError("duplicate declared constraint name")
                if sum(info.definition.kind == ConstraintKind.PRIMARY_KEY for info in infos) > 1:
                    raise ValueError("only one PRIMARY key is allowed")
                for info in infos:
                    info.__post_init__()
                    if info.ref.schema_path != table.schema_path or info.ref.owner_table != table.name:
                        raise ValueError("constraint identity has the wrong owning table")
                    if (
                        info.enforcement != ConstraintEnforcement.ENFORCED
                        or info.validation != ConstraintValidation.ALL_ROWS
                    ):
                        raise ValueError("only ENFORCED ALL_ROWS is supported")
                    definition = info.definition
                    names = self._column_names(schema, definition.column_paths)
                    relation = _quoted(aliases[key])
                    if definition.kind in (ConstraintKind.NOT_NULL, ConstraintKind.PRIMARY_KEY) and any(
                        rows.column(name).null_count for name in names
                    ):
                        raise ValueError("NOT_NULL constraint failed")
                    if definition.kind in (ConstraintKind.UNIQUE, ConstraintKind.PRIMARY_KEY):
                        columns = ", ".join(_quoted(name) for name in names)
                        present = " AND ".join(f"{_quoted(name)} IS NOT NULL" for name in names)
                        if connection.execute(
                            f"SELECT 1 FROM {relation} WHERE {present} GROUP BY {columns} HAVING count(*) > 1 LIMIT 1"
                        ).fetchone():
                            raise ValueError("UNIQUE constraint failed")
                    if definition.kind == ConstraintKind.CHECK:
                        assert definition.check_expression is not None
                        checked = evaluate_expression(
                            definition.check_expression, rows, table.schema_path, role="check"
                        )
                        if not pa.types.is_boolean(checked.type):
                            raise ValueError("CHECK expression must return BOOLEAN")
                        if any(value is False for value in checked.to_pylist()):
                            raise ValueError("CHECK constraint failed")
                    if definition.kind == ConstraintKind.FOREIGN_KEY:
                        fk = definition.foreign_key
                        assert fk is not None and info.referenced_constraint is not None
                        if (
                            fk.match != ForeignKeyMatch.SIMPLE
                            or fk.on_update not in (ReferentialAction.NO_ACTION, ReferentialAction.RESTRICT)
                            or fk.on_delete not in (ReferentialAction.NO_ACTION, ReferentialAction.RESTRICT)
                        ):
                            raise ValueError("unsupported foreign-key match or action")
                        parent = self._lookup(data, fk.referenced_table)
                        target = all_infos.get(info.referenced_constraint.key())
                        if target is None or target.definition.kind not in (
                            ConstraintKind.UNIQUE,
                            ConstraintKind.PRIMARY_KEY,
                        ):
                            raise ValueError("referenced key dependency is missing")
                        if (
                            target.ref.schema_path != fk.referenced_table.schema_path
                            or target.ref.owner_table != fk.referenced_table.name
                            or target.definition.column_paths != fk.referenced_column_paths
                        ):
                            raise ValueError("referenced key replacement invalidates foreign key")
                        parent_schema = _schema(parent)
                        parent_names = self._column_names(parent_schema, fk.referenced_column_paths)
                        if any(
                            schema.field(local).type != parent_schema.field(remote).type
                            for local, remote in zip(names, parent_names, strict=True)
                        ):
                            raise ValueError("foreign-key column types must match exactly")
                        present = " AND ".join(f"c.{_quoted(name)} IS NOT NULL" for name in names)
                        equal = " AND ".join(
                            f"c.{_quoted(local)} = p.{_quoted(remote)}"
                            for local, remote in zip(names, parent_names, strict=True)
                        )
                        parent_relation = _quoted(aliases[_table_key(fk.referenced_table)])
                        if connection.execute(
                            f"SELECT 1 FROM {relation} c WHERE {present} "
                            f"AND NOT EXISTS (SELECT 1 FROM {parent_relation} p WHERE {equal}) LIMIT 1"
                        ).fetchone():
                            raise ValueError("FOREIGN_KEY constraint failed")

    @staticmethod
    def _drop_constraint(data: dict[str, Any], ref: CatalogObjectRef, cascade: bool) -> None:
        dependents = [
            info.ref
            for record in data.values()
            for info in _infos(record)
            if info.referenced_constraint is not None and info.referenced_constraint.key() == ref.key()
        ]
        if dependents and not cascade:
            raise ValueError("referenced key has dependent foreign keys")
        for dependent in dependents:
            ConstraintCatalog._drop_constraint(data, dependent, True)
        for record in data.values():
            infos = _infos(record)
            retained = [info for info in infos if info.ref.key() != ref.key()]
            if len(retained) != len(infos):
                _set_infos(record, retained)
                _touch(record)
                return
        raise ValueError("constraint does not exist")

    def apply(self, request: ConstraintsApplyRequest) -> ConstraintSnapshot:
        request.__post_init__()
        with closing(sqlite3.connect(self.database_path)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            version, data = self._snapshot(db, request.attach_opaque_data, request.transaction_opaque_data)
            record = self._lookup(data, request.table)
            if _unb64(record["revision"]) != request.expected_revision:
                raise ValueError("stale definition revision")
            original = json.dumps(data, sort_keys=True)
            table = _table_ref(record)
            for change in request.constraint_changes():
                if change.operation in (ConstraintOperation.ADD, ConstraintOperation.REPLACE) and (
                    change.enforcement != ConstraintEnforcement.ENFORCED
                    or change.validation != ConstraintValidation.ALL_ROWS
                ):
                    raise ValueError("only ENFORCED ALL_ROWS is supported")
                infos = _infos(record)
                existing = next(
                    (info for info in infos if change.ref is not None and info.ref.key() == change.ref.key()), None
                )
                if change.operation != ConstraintOperation.ADD and existing is None:
                    raise ValueError("constraint does not exist")
                if change.operation == ConstraintOperation.ADD:
                    assert change.definition is not None
                    infos.append(self._new_info(data, table, change.definition))
                    _set_infos(record, infos)
                elif change.operation == ConstraintOperation.DROP:
                    assert change.ref is not None and change.cascade is not None
                    self._drop_constraint(data, change.ref, change.cascade)
                elif change.operation == ConstraintOperation.RENAME:
                    assert existing is not None
                    replacement = replace(
                        existing, definition=replace(existing.definition, declared_name=change.declared_name)
                    )
                    _set_infos(
                        record, [replacement if info.ref.key() == existing.ref.key() else info for info in infos]
                    )
                elif change.operation == ConstraintOperation.REPLACE:
                    assert existing is not None and change.definition is not None
                    replacement = self._new_info(data, table, change.definition, ref=existing.ref, previous=existing)
                    _set_infos(
                        record, [replacement if info.ref.key() == existing.ref.key() else info for info in infos]
                    )
            if json.dumps(data, sort_keys=True) != original:
                _touch(record)
                self._publish(db, version, data, request.transaction_opaque_data)
            return self._readback(record)

    def replace_rows(self, attach: bytes, table: CatalogObjectRef, rows: pa.Table, *, tx: bytes | None = None) -> None:
        """Replace an entire table atomically, checking incoming and outgoing FKs."""
        rows.validate(full=True)
        with closing(sqlite3.connect(self.database_path)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            version, data = self._snapshot(db, attach, tx)
            record = self._lookup(data, table)
            if not rows.schema.equals(_schema(record), check_metadata=True):
                raise ValueError("write schema differs from canonical table schema")
            record["rows"] = _encode_rows(pa.Table.from_arrays(rows.columns, schema=_base_schema(record)))
            self._publish(db, version, data, tx)

    def drop_table(
        self, attach: bytes, table: CatalogObjectRef, *, cascade: bool = False, tx: bytes | None = None
    ) -> None:
        with closing(sqlite3.connect(self.database_path)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            version, data = self._snapshot(db, attach, tx)
            record = self._lookup(data, table)
            # Table-owned FKs disappear with their table, including self FKs.
            # Remove that owned closure before checking external key blockers.
            owned = sorted(_infos(record), key=lambda info: info.definition.kind != ConstraintKind.FOREIGN_KEY)
            for info in owned:
                # A previous cascade can already have removed a self FK.
                if any(current.ref.key() == info.ref.key() for current in _infos(record)):
                    self._drop_constraint(data, info.ref, cascade)
            del data[_table_key(table)]
            self._publish(db, version, data, tx)

    def rename_table(
        self, attach: bytes, table: CatalogObjectRef, name: str, *, tx: bytes | None = None
    ) -> ConstraintSnapshot:
        with closing(sqlite3.connect(self.database_path)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            version, data = self._snapshot(db, attach, tx)
            record = self._lookup(data, table)
            old = _table_ref(record)
            renamed = replace(old, name=name)
            if _table_key(renamed) in data and _table_key(renamed) != _table_key(old):
                raise ValueError("table already exists")
            record["table"] = _b64(renamed.serialize_to_bytes())
            for candidate in data.values():
                updated = []
                for info in _infos(candidate):
                    ref, definition, referenced = info.ref, info.definition, info.referenced_constraint
                    if ref.schema_path == old.schema_path and ref.owner_table == old.name:
                        ref = replace(ref, owner_table=name)
                    if (
                        referenced is not None
                        and referenced.schema_path == old.schema_path
                        and referenced.owner_table == old.name
                    ):
                        referenced = replace(referenced, owner_table=name)
                    if definition.foreign_key is not None and _table_key(
                        definition.foreign_key.referenced_table
                    ) == _table_key(old):
                        definition = replace(
                            definition, foreign_key=replace(definition.foreign_key, referenced_table=renamed)
                        )
                    updated.append(replace(info, ref=ref, definition=definition, referenced_constraint=referenced))
                if updated != _infos(candidate) or candidate is record:
                    _set_infos(candidate, updated)
                    _touch(candidate)
            del data[_table_key(old)]
            data[_table_key(renamed)] = record
            self._publish(db, version, data, tx)
            return self._readback(record)
