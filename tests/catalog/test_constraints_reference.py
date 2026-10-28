# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103

"""Semantic proofs for canonical constraints, without advertising public RPCs."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import duckdb
import pyarrow as pa
import pytest

from vgi.catalog.capabilities import ROW_SCALAR_EXPRESSION
from vgi.catalog.constraints import (
    ConstraintChange,
    ConstraintDefinition,
    ConstraintEnforcement,
    ConstraintInfo,
    ConstraintKind,
    ConstraintOperation,
    ConstraintsApplyRequest,
    ConstraintSnapshot,
    ConstraintValidation,
    ForeignKeyDefinition,
    ForeignKeyMatch,
    ReferentialAction,
)
from vgi.catalog.dependencies import CatalogObjectRef
from vgi.catalog.evaluation_context import QueryEvaluationContext
from vgi.catalog.sql import SqlExpression

from .constraints_reference import ConstraintCatalog
from .typing_helpers import present

ATTACH = b"constraint-owner"
COLUMNS = pa.schema(
    cast(
        "list[pa.Field[Any]]",
        [pa.field("id", pa.int64()), pa.field("other", pa.int64()), pa.field("label", pa.string())],
    )
)


@pytest.fixture
def catalog(tmp_path: Path) -> ConstraintCatalog:
    return ConstraintCatalog(str(tmp_path / "constraints.sqlite"))


def table(name: str = "items", path: list[str] | None = None) -> CatalogObjectRef:
    return CatalogObjectRef(kind="table", schema_path=path or ["main"], name=name)


def definition(kind: ConstraintKind, *columns: str, name: str | None = None) -> ConstraintDefinition:
    return ConstraintDefinition(kind=kind, column_paths=[[column] for column in columns], declared_name=name)


def check(sql: str, *, owner: CatalogObjectRef | None = None) -> ConstraintDefinition:
    return ConstraintDefinition(
        kind=ConstraintKind.CHECK,
        check_expression=SqlExpression(
            sql=sql,
            profile=ROW_SCALAR_EXPRESSION,
            schema_path=(owner or table()).schema_path,
            evaluation_context=QueryEvaluationContext(),
        ),
    )


def foreign_key(parent: CatalogObjectRef, local: list[str], remote: list[str]) -> ConstraintDefinition:
    return ConstraintDefinition(
        kind=ConstraintKind.FOREIGN_KEY,
        column_paths=[[name] for name in local],
        foreign_key=ForeignKeyDefinition(referenced_table=parent, referenced_column_paths=[[name] for name in remote]),
    )


def add(item: ConstraintDefinition, **kwargs: Any) -> ConstraintChange:
    return ConstraintChange(
        operation=ConstraintOperation.ADD,
        definition=item,
        enforcement=kwargs.get("enforcement", ConstraintEnforcement.ENFORCED),
        validation=kwargs.get("validation", ConstraintValidation.ALL_ROWS),
    )


def mutate(
    catalog: ConstraintCatalog,
    target: CatalogObjectRef,
    *changes: ConstraintChange,
    tx: bytes | None = None,
    revision: bytes | None = None,
) -> ConstraintSnapshot:
    return catalog.apply(
        ConstraintsApplyRequest(
            attach_opaque_data=ATTACH,
            transaction_opaque_data=tx,
            table=target,
            expected_revision=revision or catalog.get(ATTACH, target, tx).definition_revision,
            changes=[change.serialize_to_bytes() for change in changes],
        )
    )


def write(
    catalog: ConstraintCatalog, target: CatalogObjectRef, rows: list[dict[str, Any]], *, tx: bytes | None = None
) -> None:
    catalog.replace_rows(ATTACH, target, pa.Table.from_pylist(rows, schema=catalog.columns(ATTACH, target, tx)), tx=tx)


def test_canonical_create_readback_survives_restart_and_ipc(catalog: ConstraintCatalog) -> None:
    target = table("literal.table", ["literal.schema"])
    columns = COLUMNS.with_metadata({b"provider": b"kept"})
    requested = [definition(ConstraintKind.UNIQUE, "id", name="user_label"), check("id > 0", owner=target)]
    created = catalog.create(ATTACH, target, columns, requested)
    assert created == ConstraintSnapshot.deserialize_from_bytes(created.serialize_to_bytes())
    assert [info.definition for info in created.constraint_infos()] == requested
    assert all(info.trusted and info.ref.owner_table == "literal.table" for info in created.constraint_infos())
    assert all(info.ref.name != "user_label" for info in created.constraint_infos())
    restarted = ConstraintCatalog(catalog.database_path)
    assert restarted.get(ATTACH, target) == created
    assert restarted.columns(ATTACH, target).equals(columns, check_metadata=True)
    write(restarted, target, [{"id": 1, "label": "persisted"}])
    assert ConstraintCatalog(catalog.database_path).rows(ATTACH, target).to_pylist() == [
        {"id": 1, "other": None, "label": "persisted"}
    ]


def test_unique_composite_null_semantics_and_atomic_batch_failure(catalog: ConstraintCatalog) -> None:
    target = table()
    catalog.create(ATTACH, target, COLUMNS, [definition(ConstraintKind.UNIQUE, "id", "other")])
    accepted: list[dict[str, Any]] = [
        {"id": 1, "other": None},
        {"id": 1, "other": None},
        {"id": None, "other": 2},
        {"id": 1, "other": 2},
    ]
    write(catalog, target, accepted)
    before, version = catalog.rows(ATTACH, target), catalog.version(ATTACH)
    with pytest.raises(ValueError, match="UNIQUE"):
        write(catalog, target, [{"id": 9, "other": 9}, {"id": 9, "other": 9}])
    assert catalog.rows(ATTACH, target).equals(before)
    assert catalog.version(ATTACH) == version


def test_primary_and_independent_not_null_drive_discovered_schema(catalog: ConstraintCatalog) -> None:
    target = table()
    columns = COLUMNS.set(0, COLUMNS.field("id").with_nullable(False))
    created = catalog.create(
        ATTACH,
        target,
        columns,
        [definition(ConstraintKind.PRIMARY_KEY, "id"), definition(ConstraintKind.NOT_NULL, "id")],
    )
    primary, not_null = created.constraint_infos()
    with pytest.raises(ValueError, match="NOT_NULL"):
        write(catalog, target, [{"id": None}])
    with pytest.raises(ValueError, match="UNIQUE"):
        write(catalog, target, [{"id": 1}, {"id": 1}])
    write(catalog, target, [{"id": 1}])
    mutate(catalog, target, ConstraintChange(operation=ConstraintOperation.DROP, ref=primary.ref, cascade=False))
    assert not catalog.columns(ATTACH, target).field("id").nullable
    mutate(catalog, target, ConstraintChange(operation=ConstraintOperation.DROP, ref=not_null.ref, cascade=False))
    assert catalog.columns(ATTACH, target).field("id").nullable
    write(catalog, target, [{"id": None}])


@pytest.mark.parametrize("has_constraint", [False, True])
def test_creation_rejects_inconsistent_arrow_nullability(catalog: ConstraintCatalog, has_constraint: bool) -> None:
    columns = COLUMNS if has_constraint else COLUMNS.set(0, COLUMNS.field("id").with_nullable(False))
    constraints = [definition(ConstraintKind.NOT_NULL, "id")] if has_constraint else []
    with pytest.raises(ValueError, match="Arrow nullability"):
        catalog.create(ATTACH, table(), columns, constraints)
    with pytest.raises(ValueError, match="does not exist"):
        catalog.get(ATTACH, table())


def test_check_binds_and_evaluates_typed_null_true_and_false(catalog: ConstraintCatalog) -> None:
    target = table()
    catalog.create(ATTACH, target, COLUMNS, [check("id > 0 AND (label IS NULL OR lower(label) = 'ok')")])
    write(catalog, target, [{"id": None, "label": None}, {"id": 2, "label": "OK"}])
    before = catalog.rows(ATTACH, target)
    assert before.schema.field("id").type == pa.int64()
    with pytest.raises(ValueError, match="CHECK"):
        write(catalog, target, [{"id": -1, "label": "ok"}])
    assert catalog.rows(ATTACH, target).equals(before)
    mutate(catalog, target, add(check("NULL::BOOLEAN")))
    write(catalog, target, [{"id": None, "label": "ok"}])


@pytest.mark.parametrize(
    "sql", ["random() > 0", "(SELECT true)", "missing > 0", "id", "id > 0); DELETE FROM items; SELECT (true"]
)
def test_unqualified_checks_reject_even_on_empty_tables(catalog: ConstraintCatalog, sql: str) -> None:
    version = catalog.version(ATTACH)
    with pytest.raises((ValueError, duckdb.Error)):
        catalog.create(ATTACH, table(), COLUMNS, [check(sql)])
    assert catalog.version(ATTACH) == version


@pytest.mark.parametrize(
    "kind", [ConstraintKind.NOT_NULL, ConstraintKind.UNIQUE, ConstraintKind.PRIMARY_KEY, ConstraintKind.CHECK]
)
def test_add_validates_existing_rows_before_publishing(catalog: ConstraintCatalog, kind: ConstraintKind) -> None:
    target = table()
    catalog.create(ATTACH, target, COLUMNS, [])
    write(catalog, target, [{"id": None}, {"id": -1}, {"id": -1}])
    before, rows, version = catalog.get(ATTACH, target), catalog.rows(ATTACH, target), catalog.version(ATTACH)
    candidate = check("id > 0") if kind == ConstraintKind.CHECK else definition(kind, "id")
    with pytest.raises(ValueError):
        mutate(catalog, target, add(candidate))
    assert catalog.get(ATTACH, target) == before
    assert catalog.rows(ATTACH, target).equals(rows)
    assert catalog.version(ATTACH) == version
    assert catalog.columns(ATTACH, target).field("id").nullable


def test_atomic_apply_stale_revisions_and_stable_label_identity(catalog: ConstraintCatalog) -> None:
    target = table()
    first = catalog.create(ATTACH, target, COLUMNS, [definition(ConstraintKind.UNIQUE, "id", name="old")])
    write(catalog, target, [{"id": 1, "other": None}])
    info = first.constraint_infos()[0]
    renamed = mutate(
        catalog, target, ConstraintChange(operation=ConstraintOperation.RENAME, ref=info.ref, declared_name="new")
    )
    assert renamed.constraint_infos()[0].ref == info.ref
    assert renamed.constraint_infos()[0].definition.declared_name == "new"
    assert renamed.definition_revision != first.definition_revision
    with pytest.raises(ValueError, match="stale"):
        mutate(catalog, target, add(check("id > 0")), revision=first.definition_revision)
    with pytest.raises(ValueError, match="NOT_NULL"):
        mutate(
            catalog,
            target,
            ConstraintChange(operation=ConstraintOperation.RENAME, ref=info.ref, declared_name="not_published"),
            add(definition(ConstraintKind.NOT_NULL, "other")),
        )
    assert catalog.get(ATTACH, target) == renamed
    version = catalog.version(ATTACH)
    unchanged = mutate(
        catalog, target, ConstraintChange(operation=ConstraintOperation.RENAME, ref=info.ref, declared_name="new")
    )
    assert unchanged == renamed and catalog.version(ATTACH) == version


def seed_fk(catalog: ConstraintCatalog) -> tuple[CatalogObjectRef, CatalogObjectRef, ConstraintInfo, ConstraintInfo]:
    parent, child = table("parent"), table("child")
    parent_info = catalog.create(ATTACH, parent, COLUMNS, [definition(ConstraintKind.UNIQUE, "id", "other")])
    write(catalog, parent, [{"id": 1, "other": 2}])
    child_info = catalog.create(ATTACH, child, COLUMNS, [foreign_key(parent, ["id", "other"], ["id", "other"])])
    return parent, child, parent_info.constraint_infos()[0], child_info.constraint_infos()[0]


def test_foreign_key_simple_null_rows_and_referenced_mutations(catalog: ConstraintCatalog) -> None:
    parent, child, _, _ = seed_fk(catalog)
    write(catalog, child, [{"id": 1, "other": 2}, {"id": 999, "other": None}, {"id": None, "other": 999}])
    with pytest.raises(ValueError, match="FOREIGN_KEY"):
        write(catalog, child, [{"id": 999, "other": 2}])
    before = catalog.rows(ATTACH, parent)
    with pytest.raises(ValueError, match="FOREIGN_KEY"):
        write(catalog, parent, [])
    with pytest.raises(ValueError, match="FOREIGN_KEY"):
        write(catalog, parent, [{"id": 1, "other": 3}])
    assert catalog.rows(ATTACH, parent).equals(before)


def test_adding_fk_validates_existing_data_atomically(catalog: ConstraintCatalog) -> None:
    parent, child = table("parent"), table("child")
    catalog.create(ATTACH, parent, COLUMNS, [definition(ConstraintKind.UNIQUE, "id")])
    catalog.create(ATTACH, child, COLUMNS, [])
    write(catalog, child, [{"id": 8}])
    before = catalog.get(ATTACH, child)
    with pytest.raises(ValueError, match="FOREIGN_KEY"):
        mutate(catalog, child, add(foreign_key(parent, ["id"], ["id"])))
    assert catalog.get(ATTACH, child) == before


def test_explicit_fk_target_change_rejects_without_retaining_a_misleading_binding(catalog: ConstraintCatalog) -> None:
    parent, child, key, fk = seed_fk(catalog)
    alternate = table("alternate")
    catalog.create(ATTACH, alternate, COLUMNS, [definition(ConstraintKind.UNIQUE, "id", "other")])
    before = catalog.get(ATTACH, child)
    with pytest.raises(ValueError, match="changing an existing foreign-key target"):
        mutate(
            catalog,
            child,
            ConstraintChange(
                operation=ConstraintOperation.REPLACE,
                ref=fk.ref,
                definition=foreign_key(alternate, ["id", "other"], ["id", "other"]),
                enforcement=ConstraintEnforcement.ENFORCED,
                validation=ConstraintValidation.ALL_ROWS,
            ),
        )
    assert catalog.get(ATTACH, child) == before
    assert before.constraint_infos()[0].referenced_constraint == key.ref
    assert present(before.constraint_infos()[0].definition.foreign_key).referenced_table == parent


def test_drop_self_referencing_table_removes_its_owned_fk_under_restrict(catalog: ConstraintCatalog) -> None:
    target = table()
    created = catalog.create(
        ATTACH,
        target,
        COLUMNS,
        [foreign_key(target, ["other"], ["id"]), definition(ConstraintKind.UNIQUE, "id")],
    )
    assert created.constraint_infos()[0].referenced_constraint == created.constraint_infos()[1].ref
    write(catalog, target, [{"id": 1, "other": 1}])
    catalog.drop_table(ATTACH, target)
    with pytest.raises(ValueError, match="does not exist"):
        catalog.get(ATTACH, target)


def test_fk_dependency_never_retargets_and_cascade_updates_both_revisions(catalog: ConstraintCatalog) -> None:
    parent, child, key, fk = seed_fk(catalog)
    mutate(catalog, parent, add(definition(ConstraintKind.UNIQUE, "id", "other", name="alternate")))
    before_parent, before_child = catalog.get(ATTACH, parent), catalog.get(ATTACH, child)
    with pytest.raises(ValueError, match="dependent foreign keys"):
        mutate(catalog, parent, ConstraintChange(operation=ConstraintOperation.DROP, ref=key.ref, cascade=False))
    with pytest.raises(ValueError, match="replacement invalidates"):
        mutate(
            catalog,
            parent,
            ConstraintChange(
                operation=ConstraintOperation.REPLACE,
                ref=key.ref,
                definition=definition(ConstraintKind.UNIQUE, "label"),
                enforcement=ConstraintEnforcement.ENFORCED,
                validation=ConstraintValidation.ALL_ROWS,
            ),
        )
    assert catalog.get(ATTACH, parent) == before_parent
    assert catalog.get(ATTACH, child).constraint_infos()[0].referenced_constraint == key.ref
    mutate(catalog, parent, ConstraintChange(operation=ConstraintOperation.DROP, ref=key.ref, cascade=True))
    assert catalog.get(ATTACH, child).constraints == []
    assert catalog.get(ATTACH, child).definition_revision != before_child.definition_revision
    assert all(item.ref != key.ref for item in catalog.get(ATTACH, parent).constraint_infos())
    assert fk.referenced_constraint == key.ref


def test_table_rename_rewrites_fk_coordinates_and_recreate_uses_new_keys(catalog: ConstraintCatalog) -> None:
    parent, child, key, _ = seed_fk(catalog)
    renamed = catalog.rename_table(ATTACH, parent, "renamed")
    new_parent = table("renamed")
    assert renamed.constraint_infos()[0].ref.name == key.ref.name
    assert renamed.constraint_infos()[0].ref.owner_table == "renamed"
    child_fk = catalog.get(ATTACH, child).constraint_infos()[0]
    assert child_fk.referenced_constraint == renamed.constraint_infos()[0].ref
    assert present(child_fk.definition.foreign_key).referenced_table == new_parent
    write(catalog, child, [{"id": 1, "other": 2}])
    with pytest.raises(ValueError, match="dependent foreign keys"):
        catalog.drop_table(ATTACH, new_parent)
    catalog.drop_table(ATTACH, new_parent, cascade=True)
    fresh = catalog.create(ATTACH, new_parent, COLUMNS, [definition(ConstraintKind.UNIQUE, "id", "other")])
    assert fresh.constraint_infos()[0].ref.name != key.ref.name
    assert fresh.definition_revision != renamed.definition_revision


def test_rollback_commit_reconnect_and_transaction_conflicts(catalog: ConstraintCatalog) -> None:
    target = table()
    original = catalog.create(ATTACH, target, COLUMNS, [])
    tx = catalog.begin(ATTACH)
    staged = mutate(catalog, target, add(definition(ConstraintKind.UNIQUE, "id")), tx=tx)
    write(catalog, target, [{"id": 1}], tx=tx)
    assert catalog.get(ATTACH, target) == original
    restarted = ConstraintCatalog(catalog.database_path)
    assert restarted.get(ATTACH, target, tx) == staged
    with pytest.raises(ValueError, match="owner"):
        restarted.get(b"another-owner", target, tx)
    restarted.rollback(ATTACH, tx)
    restarted.rollback(ATTACH, tx)
    assert restarted.get(ATTACH, target) == original
    assert restarted.rows(ATTACH, target).num_rows == 0
    with pytest.raises(ValueError, match="terminal"):
        restarted.commit(ATTACH, tx)
    winner, loser = restarted.begin(ATTACH), restarted.begin(ATTACH)
    mutate(restarted, target, add(definition(ConstraintKind.UNIQUE, "id")), tx=winner)
    mutate(restarted, target, add(check("id > 0")), tx=loser)
    write(restarted, target, [{"id": 7}], tx=winner)
    restarted.commit(ATTACH, winner)
    restarted.commit(ATTACH, winner)
    with pytest.raises(ValueError, match="conflict"):
        restarted.commit(ATTACH, loser)
    restarted.rollback(ATTACH, loser)
    persisted = ConstraintCatalog(catalog.database_path)
    assert persisted.get(ATTACH, target).constraint_infos()[0].definition.kind == ConstraintKind.UNIQUE
    assert persisted.rows(ATTACH, target).column("id").to_pylist() == [7]


def test_failed_statement_and_post_write_fault_preserve_prior_state(
    catalog: ConstraintCatalog, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = table()
    catalog.create(ATTACH, target, COLUMNS, [definition(ConstraintKind.UNIQUE, "id")])
    tx = catalog.begin(ATTACH)
    write(catalog, target, [{"id": 1}], tx=tx)
    with pytest.raises(ValueError, match="UNIQUE"):
        write(catalog, target, [{"id": 2}, {"id": 2}], tx=tx)
    catalog.commit(ATTACH, tx)
    before, version = catalog.get(ATTACH, target), catalog.version(ATTACH)

    def fail() -> None:
        raise RuntimeError("injected after SQLite write")

    monkeypatch.setattr(catalog, "_after_write", fail)
    with pytest.raises(RuntimeError, match="injected"):
        mutate(catalog, target, add(check("id > 0")))
    restarted = ConstraintCatalog(catalog.database_path)
    assert restarted.get(ATTACH, target) == before
    assert restarted.version(ATTACH) == version
    assert restarted.rows(ATTACH, target).column("id").to_pylist() == [1]


@pytest.mark.parametrize(
    "metadata_key",
    [b"vgi.column.default.v1", b"vgi.column.generated.v1", b"vgi.column.generation.v1", b"default", b"generated"],
)
def test_unimplemented_column_semantics_reject_before_creation(catalog: ConstraintCatalog, metadata_key: bytes) -> None:
    columns = COLUMNS.set(0, COLUMNS.field("id").with_metadata({metadata_key: b"unimplemented"}))
    version = catalog.version(ATTACH)
    with pytest.raises(ValueError, match="column semantics"):
        catalog.create(ATTACH, table(), columns, [])
    assert catalog.version(ATTACH) == version


@pytest.mark.parametrize("mode", ["metadata", "future", "nested", "match_full", "cascade", "float", "unbound_fk"])
def test_unsupported_reference_scope_rejects_without_publication(catalog: ConstraintCatalog, mode: str) -> None:
    target = table()
    catalog.create(ATTACH, target, COLUMNS, [])
    before, version = catalog.get(ATTACH, target), catalog.version(ATTACH)
    with pytest.raises(ValueError):
        if mode == "metadata":
            mutate(
                catalog,
                target,
                add(definition(ConstraintKind.UNIQUE, "id"), enforcement=ConstraintEnforcement.METADATA_ONLY),
            )
        elif mode == "future":
            mutate(
                catalog,
                target,
                add(definition(ConstraintKind.UNIQUE, "id"), validation=ConstraintValidation.FUTURE_WRITES),
            )
        elif mode == "nested":
            mutate(
                catalog, target, add(ConstraintDefinition(kind=ConstraintKind.UNIQUE, column_paths=[["id", "child"]]))
            )
        elif mode == "float":
            catalog.create(ATTACH, table("floating"), pa.schema([pa.field("x", pa.float64())]), [])
        elif mode == "unbound_fk":
            mutate(catalog, target, add(foreign_key(target, ["id"], ["id"])))
        else:
            # The target key is in the same atomic candidate, but an unsupported
            # action/match must still roll back that otherwise-valid addition.
            fk = foreign_key(target, ["other"], ["id"])
            assert fk.foreign_key is not None
            fk = replace(
                fk,
                foreign_key=replace(
                    fk.foreign_key,
                    match=ForeignKeyMatch.FULL if mode == "match_full" else ForeignKeyMatch.SIMPLE,
                    on_delete=ReferentialAction.CASCADE if mode == "cascade" else ReferentialAction.NO_ACTION,
                ),
            )
            mutate(catalog, target, add(definition(ConstraintKind.UNIQUE, "id")), add(fk))
    assert catalog.get(ATTACH, target) == before
    assert catalog.version(ATTACH) == version
