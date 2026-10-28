# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103

"""Constraint wire unions, exact identities and malformed IPC rejection."""

from __future__ import annotations

from dataclasses import replace
from typing import Any, cast

import pyarrow as pa
import pytest

from vgi.catalog import constraints as contract
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
from vgi.catalog.dependencies import CatalogObjectRef, DependencySnapshot, ObjectDependency
from vgi.catalog.evaluation_context import QueryEvaluationContext
from vgi.catalog.sql import SqlExpression

from .typing_helpers import present


def table(name: str = "items", path: list[str] | None = None) -> CatalogObjectRef:
    return CatalogObjectRef(kind="table", schema_path=path or ["a.b", 'quoted"schema'], name=name)


def identity(key: str = "logical-Key", owner: CatalogObjectRef | None = None) -> CatalogObjectRef:
    owner = owner or table()
    return CatalogObjectRef(kind="constraint", schema_path=owner.schema_path, owner_table=owner.name, name=key)


def definition(
    kind: ConstraintKind = ConstraintKind.UNIQUE, paths: list[list[str]] | None = None, **kwargs: Any
) -> ConstraintDefinition:
    return ConstraintDefinition(kind=kind, column_paths=paths if paths is not None else [["value"]], **kwargs)


def expression() -> SqlExpression:
    return SqlExpression(
        sql='"value" > 0',
        profile="vgi.expression.duckdb.scalar.v1",
        schema_path=table().schema_path,
        evaluation_context=QueryEvaluationContext(timezone="UTC"),
    )


def info(
    key: str = "logical-Key",
    value: ConstraintDefinition | None = None,
    owner: CatalogObjectRef | None = None,
    **kwargs: Any,
) -> ConstraintInfo:
    return ConstraintInfo(
        ref=identity(key, owner),
        definition=value or definition(),
        enforcement=ConstraintEnforcement.ENFORCED,
        validation=ConstraintValidation.ALL_ROWS,
        **kwargs,
    )


def change(value: ConstraintDefinition | None = None, **kwargs: Any) -> ConstraintChange:
    return ConstraintChange(
        operation=ConstraintOperation.ADD,
        definition=value or definition(),
        enforcement=ConstraintEnforcement.ENFORCED,
        validation=ConstraintValidation.ALL_ROWS,
        **kwargs,
    )


def apply(changes: list[ConstraintChange] | None = None, **kwargs: Any) -> ConstraintsApplyRequest:
    return ConstraintsApplyRequest(
        attach_opaque_data=b"attachment",
        table=table(),
        expected_revision=b"revision-1",
        changes=[item.serialize_to_bytes() for item in (changes if changes is not None else [change()])],
        **kwargs,
    )


def snapshot(values: list[ConstraintInfo] | None = None) -> ConstraintSnapshot:
    return ConstraintSnapshot(
        table=table(),
        definition_revision=b"revision-1",
        constraints=[value.serialize_to_bytes() for value in (values if values is not None else [info()])],
    )


def batch(
    record: ConstraintDefinition
    | ForeignKeyDefinition
    | ConstraintInfo
    | ConstraintChange
    | ConstraintsApplyRequest
    | ConstraintSnapshot,
) -> pa.RecordBatch:
    with pa.ipc.open_stream(record.serialize_to_bytes()) as reader:
        return reader.read_next_batch()


def ipc(value: pa.RecordBatch, *, twice: bool = False) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, value.schema) as writer:
        writer.write_batch(value)
        if twice:
            writer.write_batch(value)
    return sink.getvalue().to_pybytes()


def test_all_definition_kinds_roundtrip_with_typed_nested_records() -> None:
    foreign = ForeignKeyDefinition(
        referenced_table=table("parent"),
        referenced_column_paths=[["id.part"], ["nested", "id"]],
        match=ForeignKeyMatch.FULL,
        on_update=ReferentialAction.CASCADE,
        on_delete=ReferentialAction.SET_NULL,
    )
    definitions = [
        definition(ConstraintKind.NOT_NULL),
        definition(),
        definition(ConstraintKind.PRIMARY_KEY, [["value"], ["other"]]),
        definition(ConstraintKind.CHECK, [], check_expression=expression()),
        definition(ConstraintKind.FOREIGN_KEY, [["child.part"], ["nested", "child"]], foreign_key=foreign),
    ]
    for original in definitions:
        restored = ConstraintDefinition.deserialize_from_bytes(original.serialize_to_bytes())
        assert restored == original
    assert ForeignKeyDefinition.deserialize_from_bytes(foreign.serialize_to_bytes()) == foreign
    assert present(definitions[-1].foreign_key).referenced_column_paths == [["id.part"], ["nested", "id"]]


def test_raw_paths_keep_dots_quotes_case_and_struct_boundaries() -> None:
    value = definition(paths=[["a.b"], ["a", "b"], ['quote"name'], ["Name"], ["name"]])
    schema = pa.schema(
        [
            pa.field("a.b", pa.int32()),
            pa.field("a", pa.struct([pa.field("b", pa.int32())])),
            pa.field('quote"name', pa.int32()),
            pa.field("Name", pa.int32()),
            pa.field("name", pa.int32()),
        ]
    )
    value.validate_schema(schema)
    restored = ConstraintDefinition.deserialize_from_bytes(value.serialize_to_bytes())
    assert restored.column_paths == value.column_paths
    with pytest.raises(ValueError, match="missing or ambiguous"):
        value.validate_schema(pa.schema([pa.field("a.b", pa.int32())]))
    with pytest.raises(ValueError, match="non-struct"):
        definition(paths=[["items", "element"]]).validate_schema(pa.schema([pa.field("items", pa.list_(pa.int32()))]))


@pytest.mark.parametrize(
    "kwargs",
    [
        {"kind": "UNKNOWN"},
        {"column_paths": []},
        {"column_paths": [["a"], ["a"]]},
        {"column_paths": [[""]]},
        {"column_paths": [["a\0b"]]},
        {"column_paths": ["a.b"]},
        {"column_paths": [["a"] * (contract.MAX_CONSTRAINT_PATH_DEPTH + 1)]},
        {"declared_name": ""},
        {"declared_name": "x" * (contract.MAX_CONSTRAINT_IDENTIFIER_BYTES + 1)},
        {"kind": ConstraintKind.NOT_NULL, "column_paths": [["a"], ["b"]]},
        {"kind": ConstraintKind.CHECK, "column_paths": []},
        {"kind": ConstraintKind.FOREIGN_KEY},
    ],
)
def test_definition_rejects_invalid_union_and_path_shapes(kwargs: dict[str, Any]) -> None:
    with pytest.raises((TypeError, ValueError)):
        replace(definition(), **kwargs)


def test_definition_rejects_payloads_on_wrong_kind_and_fk_arity() -> None:
    foreign = ForeignKeyDefinition(referenced_table=table("parent"), referenced_column_paths=[["id"]])
    with pytest.raises(ValueError, match="only CHECK"):
        replace(definition(), check_expression=expression())
    with pytest.raises(ValueError, match="only FOREIGN_KEY"):
        replace(definition(), foreign_key=foreign)
    with pytest.raises(ValueError, match="arity"):
        definition(ConstraintKind.FOREIGN_KEY, [["a"], ["b"]], foreign_key=foreign)
    for kwargs in [{"match": "PARTIAL"}, {"on_update": "UNKNOWN"}, {"on_delete": "UNKNOWN"}]:
        with pytest.raises(ValueError):
            replace(foreign, **cast(dict[str, Any], kwargs))
    with pytest.raises(ValueError, match="table reference"):
        replace(foreign, referenced_table=identity())


def test_info_stable_key_is_independent_of_declared_label_and_trust_flags() -> None:
    original = info(value=definition(declared_name="visible SQL label"))
    renamed = replace(original, definition=replace(original.definition, declared_name="new label"))
    assert original.ref.key() == renamed.ref.key()
    assert original.trusted
    assert not replace(original, enforcement=ConstraintEnforcement.METADATA_ONLY).trusted
    assert not replace(original, validation=ConstraintValidation.FUTURE_WRITES).trusted
    assert not replace(original, validation=ConstraintValidation.UNVALIDATED).trusted
    assert ConstraintInfo.deserialize_from_bytes(renamed.serialize_to_bytes()) == renamed
    assert identity("Key").key() != identity("key").key()
    assert identity(owner=table(path=["a.b"])).key() != identity(owner=table(path=["a", "b"])).key()
    with pytest.raises(ValueError):
        replace(original, enforcement=cast(ConstraintEnforcement, "unknown"))
    with pytest.raises(ValueError):
        replace(original, validation=cast(ConstraintValidation, "unknown"))
    with pytest.raises(ValueError, match="constraint reference"):
        replace(original, ref=table())


def test_foreign_key_readback_requires_resolved_key_on_exact_target_table() -> None:
    parent = table("parent")
    foreign = definition(
        ConstraintKind.FOREIGN_KEY,
        foreign_key=ForeignKeyDefinition(referenced_table=parent, referenced_column_paths=[["id"]]),
    )
    value = info(value=foreign, referenced_constraint=identity("parent-primary", parent))
    assert ConstraintInfo.deserialize_from_bytes(value.serialize_to_bytes()) == value
    with pytest.raises(ValueError, match="requires its resolved"):
        replace(value, referenced_constraint=None)
    with pytest.raises(ValueError, match="another table"):
        replace(value, referenced_constraint=identity("parent-primary", table("other")))
    with pytest.raises(ValueError, match="only foreign key"):
        replace(info(), referenced_constraint=value.referenced_constraint)
    self_target = replace(foreign, foreign_key=replace(present(foreign.foreign_key), referenced_table=table()))
    with pytest.raises(ValueError, match="cannot reference itself"):
        info(value=self_target, referenced_constraint=identity())


def test_change_union_roundtrips_all_operations_and_explicit_false() -> None:
    changes = [
        change(),
        ConstraintChange(operation=ConstraintOperation.DROP, ref=identity(), cascade=False),
        ConstraintChange(operation=ConstraintOperation.RENAME, ref=identity(), declared_name=None),
        ConstraintChange(operation=ConstraintOperation.RENAME, ref=identity(), declared_name="new label"),
        replace(change(), operation=ConstraintOperation.REPLACE, ref=identity()),
    ]
    for value in changes:
        assert ConstraintChange.deserialize_from_bytes(value.serialize_to_bytes()) == value
    assert changes[1].cascade is False


@pytest.mark.parametrize(
    "operation,fields",
    [
        ("unknown", {}),
        (ConstraintOperation.ADD, {}),
        (ConstraintOperation.DROP, {"ref": identity()}),
        (ConstraintOperation.DROP, {"ref": identity(), "cascade": 1}),
        (ConstraintOperation.DROP, {"ref": identity(), "cascade": False, "declared_name": "bad"}),
        (ConstraintOperation.RENAME, {}),
        (ConstraintOperation.RENAME, {"ref": identity(), "cascade": False}),
        (ConstraintOperation.RENAME, {"ref": identity(), "definition": definition()}),
        (ConstraintOperation.REPLACE, {"ref": identity()}),
    ],
)
def test_change_union_rejects_missing_or_conflicting_fields(
    operation: ConstraintOperation, fields: dict[str, Any]
) -> None:
    with pytest.raises((TypeError, ValueError)):
        ConstraintChange(operation=operation, **fields)


def test_add_replace_require_explicit_known_enforcement_and_validation() -> None:
    for kwargs in [
        {"ref": identity()},
        {"enforcement": None},
        {"enforcement": "UNKNOWN"},
        {"validation": None},
        {"validation": "UNKNOWN"},
        {"cascade": False},
        {"declared_name": "outside-definition"},
    ]:
        with pytest.raises((TypeError, ValueError)):
            replace(change(), **cast(dict[str, Any], kwargs))


def test_snapshot_and_request_roundtrip_and_enforce_exact_owner() -> None:
    original = snapshot([info("Key"), info("key")])
    restored = ConstraintSnapshot.deserialize_from_bytes(original.serialize_to_bytes())
    assert restored == original
    assert [item.ref.name for item in restored.constraint_infos()] == ["Key", "key"]
    assert snapshot([]).constraint_infos() == []
    request = apply(transaction_opaque_data=b"transaction")
    assert ConstraintsApplyRequest.deserialize_from_bytes(request.serialize_to_bytes()) == request
    assert request.constraint_changes()[0].operation == ConstraintOperation.ADD
    with pytest.raises(ValueError, match="another table"):
        snapshot([info(owner=table("other"))])
    with pytest.raises(ValueError, match="another table"):
        apply([ConstraintChange(operation=ConstraintOperation.DROP, ref=identity(owner=table("other")), cascade=False)])
    with pytest.raises(ValueError, match="owning table schema"):
        wrong_expression = replace(expression(), schema_path=["other"])
        apply([change(definition(ConstraintKind.CHECK, [], check_expression=wrong_expression))])


def test_snapshot_rejects_duplicate_keys_labels_and_primary_keys() -> None:
    with pytest.raises(ValueError, match="duplicate constraint identity"):
        snapshot([info(), info()])
    with pytest.raises(ValueError, match="duplicate declared"):
        snapshot([info("one", definition(declared_name="same")), info("two", definition(declared_name="same"))])
    with pytest.raises(ValueError, match="multiple primary"):
        snapshot(
            [info("one", definition(ConstraintKind.PRIMARY_KEY)), info("two", definition(ConstraintKind.PRIMARY_KEY))]
        )
    with pytest.raises(ValueError, match="key twice"):
        apply(
            [
                ConstraintChange(operation=ConstraintOperation.DROP, ref=identity(), cascade=False),
                ConstraintChange(operation=ConstraintOperation.RENAME, ref=identity(), declared_name="new"),
            ]
        )


@pytest.mark.parametrize("value", [b"", "not-bytes", b"x" * (contract.MAX_CONSTRAINT_TOKEN_BYTES + 1)])
def test_revisions_and_owner_tokens_are_nonempty_bounded_bytes(value: bytes | str) -> None:
    for kwargs in [{"attach_opaque_data": value}, {"expected_revision": value}, {"transaction_opaque_data": value}]:
        with pytest.raises(ValueError):
            replace(apply(), **cast(dict[str, Any], kwargs))
    with pytest.raises(ValueError):
        replace(snapshot(), definition_revision=cast(bytes, value))


def test_empty_or_malformed_nested_records_reject_before_mutation() -> None:
    with pytest.raises(ValueError):
        apply([])
    for values in [[b""], [b"not ipc"], [None]]:
        with pytest.raises((TypeError, ValueError, pa.ArrowInvalid)):
            replace(snapshot(), constraints=cast(list[bytes], values))
        with pytest.raises((TypeError, ValueError, pa.ArrowInvalid)):
            replace(apply(), changes=cast(list[bytes], values))
    with pytest.raises(ValueError, match="exact schema"):
        replace(snapshot(), constraints=[definition().serialize_to_bytes()])


def test_record_decoder_rejects_extra_rows_batches_trailing_bytes_and_unknown_fields() -> None:
    original = definition()
    value = batch(original)
    wrong_batches = [
        pa.RecordBatch.from_arrays(
            [pa.concat_arrays([column, column]) for column in value.columns], schema=value.schema
        ),
        value.select(list(range(value.num_columns - 1))),
        value.append_column("unknown", pa.array([1])),
        value.replace_schema_metadata({b"unexpected": b"metadata"}),
    ]
    for wrong in wrong_batches:
        with pytest.raises(ValueError, match="one row|exact schema"):
            ConstraintDefinition.deserialize_from_bytes(ipc(wrong))
    for wrong_bytes in [ipc(value, twice=True), original.serialize_to_bytes() + b"trailing"]:
        with pytest.raises(ValueError):
            ConstraintDefinition.deserialize_from_bytes(wrong_bytes)


def test_decoder_rejects_wrong_nullability_and_null_required_fields() -> None:
    value = batch(definition())
    fields = list(value.schema)
    fields[0] = fields[0].with_nullable(True)
    wrong = pa.RecordBatch.from_arrays(value.columns, schema=pa.schema(fields))
    with pytest.raises(ValueError, match="exact schema"):
        ConstraintDefinition.deserialize_from_bytes(ipc(wrong))
    columns = list(value.columns)
    columns[0] = pa.array([None], type=value.schema.field(0).type)
    wrong = pa.RecordBatch.from_arrays(columns, schema=value.schema)
    with pytest.raises(ValueError, match="non-nullable"):
        ConstraintDefinition.deserialize_from_bytes(ipc(wrong))


def test_unknown_wire_enum_is_not_silently_defaulted() -> None:
    value = batch(definition())
    wrong = value.set_column(0, value.schema.field(0), pa.array(["UNKNOWN"], type=value.schema.field(0).type))
    with pytest.raises(ValueError):
        ConstraintDefinition.deserialize_from_bytes(ipc(wrong))


def test_mutated_nested_lists_and_oversized_envelopes_revalidate_on_serialization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value = definition()
    value.column_paths.append(["value"])
    with pytest.raises(ValueError, match="unique"):
        value.serialize_to_bytes()
    request = apply()
    request.changes.append(b"not IPC")
    with pytest.raises((ValueError, pa.ArrowInvalid)):
        request.serialize_to_bytes()
    valid = definition()
    encoded = valid.serialize_to_bytes()
    monkeypatch.setattr(contract, "MAX_CONSTRAINT_BYTES", len(encoded) - 1)
    with pytest.raises(ValueError, match="size"):
        ConstraintDefinition.deserialize_from_bytes(encoded)
    with pytest.raises(ValueError, match="envelope"):
        valid.serialize_to_bytes()


def test_constraint_refs_join_existing_owned_backing_index_dependency_closure() -> None:
    owner, constraint = table(), identity()
    index = CatalogObjectRef(kind="index", schema_path=owner.schema_path, name="backing-index")

    def edge(dependent: CatalogObjectRef, subject: CatalogObjectRef, kind: str) -> bytes:
        return ObjectDependency(
            dependent=dependent.serialize_to_bytes(),
            subject=subject.serialize_to_bytes(),
            kind=kind,
            blocks_alter=True,
        ).serialize_to_bytes()

    graph = DependencySnapshot(
        catalog_version=3,
        objects=[value.serialize_to_bytes() for value in [owner, constraint, index]],
        dependencies=[
            edge(constraint, owner, "owned"),
            edge(index, constraint, "owned"),
            edge(constraint, index, "requires"),
        ],
    )
    assert {value.key() for value in graph.drop_set(constraint)} == {constraint.key(), index.key()}
    with pytest.raises(ValueError, match="dependent objects"):
        graph.drop_set(index)
    with pytest.raises(ValueError):
        replace(constraint, owner_table=None)
    with pytest.raises(ValueError):
        replace(owner, owner_table="unexpected")


def test_constraint_fk_paths_and_mixed_backing_index_cycles_preserve_exact_drop_closure() -> None:
    parent, child = table("parent"), table("child")
    key, foreign = identity("primary-key", parent), identity("foreign-key", child)
    backing = CatalogObjectRef(kind="index", schema_path=parent.schema_path, name="backing-index")

    def edge(
        dependent: CatalogObjectRef,
        subject: CatalogObjectRef,
        kind: str = "requires",
        paths: list[list[str]] | None = None,
        blocks: bool = True,
    ) -> bytes:
        return ObjectDependency(
            dependent=dependent.serialize_to_bytes(),
            subject=subject.serialize_to_bytes(),
            kind=kind,
            blocks_alter=blocks,
            subject_column_paths=paths,
        ).serialize_to_bytes()

    graph = DependencySnapshot(
        catalog_version=7,
        objects=[value.serialize_to_bytes() for value in [parent, child, key, foreign, backing]],
        dependencies=[
            edge(key, parent, "owned", [["id.part"], ["nested", "id"]]),
            edge(backing, key, "owned", blocks=False),
            edge(key, backing),
            edge(foreign, child, "owned", [["parent_id"], ["nested", "parent_id"]]),
            edge(foreign, key),
            edge(foreign, parent, paths=[["id.part"], ["nested", "id"]]),
        ],
    )
    schemas = {
        parent.key(): pa.schema(
            [
                pa.field("id.part", pa.int64()),
                pa.field("nested", pa.struct([pa.field("id", pa.int64())])),
                pa.field("id", pa.struct([pa.field("part", pa.int64())])),
            ]
        ),
        child.key(): pa.schema(
            [pa.field("parent_id", pa.int64()), pa.field("nested", pa.struct([pa.field("parent_id", pa.int64())]))]
        ),
    }
    graph.validate_columns(schemas)
    graph.assert_alter_allowed(parent, column_path=["id", "part"], action="type", schemas=schemas)
    for path in [["id.part"], ["nested"], ["nested", "id"]]:
        with pytest.raises(ValueError, match="dependent"):
            graph.assert_alter_allowed(parent, column_path=path, action="type", schemas=schemas)
        with pytest.raises(ValueError, match="dependent"):
            graph.plan_alter(parent, column_path=path, action="drop", schemas=schemas)
        plan = graph.plan_alter(parent, column_path=path, action="drop", cascade=True, schemas=schemas)
        assert {value.key() for value in plan.drop_objects} == {key.key(), foreign.key(), backing.key()}
        assert [(drop.table, drop.column_path) for drop in plan.drop_columns] == [(parent, path)]
    with pytest.raises(ValueError, match="dependent"):
        graph.drop_set(backing)
    assert {value.key() for value in graph.drop_set(backing, cascade=True)} == {
        backing.key(),
        key.key(),
        foreign.key(),
    }
    assert {value.key() for value in graph.drop_set(parent, cascade=True)} == {
        parent.key(),
        key.key(),
        foreign.key(),
        backing.key(),
    }
    # The FK protects exactly the key columns, not every column of its parent.
    foreign_edge = next(
        edge for edge in graph.edges() if edge.dependent_ref() == foreign and edge.subject_ref() == parent
    )
    assert foreign_edge.subject_column_paths == [["id.part"], ["nested", "id"]]
