# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103
"""Object identity, dependency validation and order-independent drop closure."""

from __future__ import annotations

import itertools
from dataclasses import replace
from typing import Any, Literal, cast

import pyarrow as pa
import pytest

from vgi.catalog.dependencies import CatalogObjectRef, DependencySnapshot, ObjectDependency

from .typing_helpers import present


def ref(name: str, kind: str = "table", path: list[str] | None = None, owner: str | None = None) -> CatalogObjectRef:
    return CatalogObjectRef(kind=kind, schema_path=["main"] if path is None else path, name=name, owner_table=owner)


def edge(
    d: CatalogObjectRef,
    s: CatalogObjectRef,
    kind: str = "requires",
    *,
    blocks: bool = True,
    paths: list[list[str]] | None = None,
    column: list[str] | None = None,
) -> ObjectDependency:
    return ObjectDependency(
        dependent=d.serialize_to_bytes(),
        subject=s.serialize_to_bytes(),
        kind=kind,
        blocks_alter=blocks,
        subject_column_paths=paths,
        dependent_column_path=column,
    )


def graph(objects: list[CatalogObjectRef], edges: list[ObjectDependency]) -> DependencySnapshot:
    return DependencySnapshot(
        catalog_version=9,
        objects=[r.serialize_to_bytes() for r in objects],
        dependencies=[e.serialize_to_bytes() for e in edges],
    )


def test_identity_preserves_raw_paths_owner_and_spelling() -> None:
    refs = [
        ref("T", path=["a.b"]),
        ref("T", path=["a", "b"]),
        ref("t", path=["a.b"]),
        ref("fire", "trigger", owner="one"),
        ref("fire", "trigger", owner="two"),
        ref('a."b', "schema", path=[]),
    ]
    assert len({r.key() for r in refs}) == len(refs)
    for r in refs:
        assert CatalogObjectRef.deserialize_from_bytes(r.serialize_to_bytes()) == r


@pytest.mark.parametrize(
    "kwargs",
    [
        dict(kind="unknown"),
        dict(schema_path=[]),
        dict(name=""),
        dict(name="a\0b"),
        dict(schema_path=[""]),
        dict(schema_path="a.b"),
        dict(owner_table="t"),
        dict(kind="trigger"),
        dict(kind="trigger", owner_table=""),
    ],
)
def test_malformed_object_references_reject(kwargs: dict[str, Any]) -> None:
    with pytest.raises((ValueError, TypeError)):
        CatalogObjectRef(**(dict(kind="table", schema_path=["main"], name="t") | kwargs))


def test_restrict_checks_blockers_after_the_complete_owned_closure() -> None:
    a, b, c, external = [ref(n) for n in "abcd"]
    edges = [edge(b, a, "owned"), edge(c, b, "owned"), edge(c, a)]
    for order in itertools.permutations(edges):
        g = graph([a, b, c, external], list(order))
        assert g.drop_set(a) == [a, b, c]
        assert g.drop_set(c) == [c]  # Dropping a dependent never drops its owner.
    blocked = graph([a, b, c, external], [*edges, edge(external, c)])
    with pytest.raises(ValueError, match="dependent"):
        blocked.drop_set(a)
    assert blocked.drop_set(a, cascade=True) == [a, b, c, external]


def test_reference_cycles_cascade_without_recursing_forever() -> None:
    a, b, c = [ref(n) for n in "abc"]
    g = graph([a, b, c], [edge(a, b), edge(b, a), edge(c, b)])
    with pytest.raises(ValueError, match="dependent"):
        g.drop_set(a)
    assert g.drop_set(a, cascade=True) == [a, b, c]
    assert g.drop_set(c) == [c]


@pytest.mark.parametrize(
    "case",
    [
        "duplicate_object",
        "dangling",
        "conflicting_alter",
        "conflicting_edge",
        "owners",
        "owner_cycle",
        "self",
        "kind",
        "version",
    ],
)
def test_invalid_graphs_reject(case: str) -> None:
    a, b, c = [ref(n) for n in "abc"]
    with pytest.raises(ValueError):
        if case == "duplicate_object":
            graph([a, a], [])
        elif case == "dangling":
            graph([a], [edge(a, b)])
        elif case == "conflicting_alter":
            graph([a, b], [edge(a, b), edge(a, b, blocks=False)])
        elif case == "conflicting_edge":
            graph([a, b], [edge(a, b), edge(a, b, "owned")])
        elif case == "owners":
            graph([a, b, c], [edge(a, b, "owned"), edge(a, c, "owned")])
        elif case == "owner_cycle":
            graph([a, b], [edge(a, b, "owned"), edge(b, a, "owned")])
        elif case == "self":
            edge(a, a)
        elif case == "kind":
            edge(a, b, "unknown")
        else:
            replace(graph([a], []), catalog_version=-1)


@pytest.mark.parametrize("shape", ["missing", "nullable", "null", "two_rows", "extra_batch", "trailing"])
def test_wire_requires_complete_single_records(shape: str) -> None:
    r = ref("t")
    schema = r.ARROW_SCHEMA
    row = dict(kind=r.kind, schema_path=r.schema_path, name=r.name, owner_table=r.owner_table)
    if shape == "missing":
        schema = schema.remove(3)
    if shape == "nullable":
        schema = schema.set(0, pa.field("kind", pa.string(), nullable=True))
    if shape == "null":
        row["kind"] = None
    batch = pa.RecordBatch.from_pylist([row] * (2 if shape == "two_rows" else 1), schema=schema)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema) as writer:
        writer.write_batch(batch)
        if shape == "extra_batch":
            writer.write_batch(batch)
    payload = sink.getvalue().to_pybytes() + (b"extra" if shape == "trailing" else b"")
    with pytest.raises(ValueError):
        CatalogObjectRef.deserialize_from_bytes(payload)


def test_mutated_snapshot_is_revalidated_before_planning() -> None:
    a = ref("a")
    g = graph([a], [])
    g.objects.append(a.serialize_to_bytes())
    with pytest.raises(ValueError, match="duplicate"):
        g.drop_set(a)


def test_edges_merge_only_within_full_column_identity_and_canonicalize_paths() -> None:
    table, view = ref("t"), ref("v", "view")
    edges = [
        edge(view, table, paths=[["a", "child"], ["b"]]),
        edge(view, table, paths=[["a"]]),
        edge(table, table, column=["g1"], paths=[["a"]]),
        edge(table, table, column=["g2"], paths=[["a.b"]]),
    ]
    expected = graph([table, view], edges)
    assert len(expected.edges()) == 3
    assert next(item for item in expected.edges() if item.dependent_ref() == view).subject_column_paths == [
        ["a"],
        ["a", "child"],
        ["b"],
    ]
    for order in itertools.permutations(edges):
        assert graph([table, view], [*order, order[0]]).dependencies == expected.dependencies
    merged = graph([table, view], [edge(view, table, paths=[["a"]]), edge(view, table)])
    assert merged.edges()[0].subject_column_paths is None


@pytest.mark.parametrize(
    "kwargs",
    [
        {"blocks_alter": 1},
        {"blocks_alter": None},
        {"subject_column_paths": []},
        {"subject_column_paths": [["a"], ["a"]]},
        {"subject_column_paths": ["a.b"]},
        {"subject_column_paths": [[""]]},
        {"subject_column_paths": [[None]]},
        {"subject_column_paths": [["a\0b"]]},
        {"dependent_column_path": []},
        {"dependent_column_path": "a.b"},
        {"dependent_column_path": [None]},
    ],
)
def test_column_edge_rejects_malformed_flags_and_paths(kwargs: dict[str, Any]) -> None:
    with pytest.raises((ValueError, TypeError)):
        replace(edge(ref("a"), ref("b")), **kwargs)


def test_column_edge_requires_appropriate_object_kinds_and_disjoint_self_targets() -> None:
    table, view, index = ref("t"), ref("v", "view"), ref("i", "index")
    with pytest.raises(ValueError, match="table reference"):
        edge(view, table, column=["generated"], paths=[["value"]])
    with pytest.raises(ValueError, match="table or view"):
        edge(table, index, paths=[["value"]])
    for paths in [None, [["a"]], [["a", "b"]], [["a", "b", "c"]]]:
        with pytest.raises(ValueError, match="self dependencies"):
            edge(table, table, column=["a", "b"], paths=paths)
    assert edge(table, table, column=["a.b"], paths=[["a", "b"]])
    assert edge(table, table, column=["a", "left"], paths=[["a", "right"]])


def test_column_validation_proves_exact_schema_identity_not_just_path_syntax() -> None:
    table = ref("t")
    value = graph([table], [edge(table, table, column=["generated"], paths=[["a.b"], ["a", "b"]])])
    schema = pa.schema(
        [
            pa.field("generated", pa.int64()),
            pa.field("a.b", pa.int64()),
            pa.field("a", pa.struct([pa.field("b", pa.int64())])),
        ]
    )
    value.validate_columns({table.key(): schema})
    assert DependencySnapshot.deserialize_from_bytes(value.serialize_to_bytes()) == value
    with pytest.raises(ValueError, match="authoritative schema"):
        value.validate_columns({})
    with pytest.raises(ValueError, match="missing or ambiguous"):
        value.validate_columns({table.key(): schema.remove(1)})
    with pytest.raises(ValueError, match="non-struct"):
        value.validate_columns({table.key(): schema.set(2, pa.field("a", pa.list_(pa.int64())))})
    with pytest.raises(ValueError, match="missing or ambiguous"):
        value.validate_columns({table.key(): schema.append(schema.field("a.b"))})
    # A valid parent does not excuse a separately claimed nonexistent child.
    invalid_child = graph([table], [edge(table, table, column=["generated"], paths=[["a"], ["a", "missing"]])])
    with pytest.raises(ValueError, match="missing or ambiguous"):
        invalid_child.validate_columns({table.key(): schema})


@pytest.mark.parametrize("action", ["rename", "type", "drop"])
def test_semantic_protection_is_symmetric_for_parent_child_but_not_dotted_names(
    action: Literal["rename", "type", "drop"],
) -> None:
    table, view = ref("t"), ref("v", "view")
    value = graph([table, view], [edge(view, table, paths=[["a", "b"]])])
    for path in [["a"], ["a", "b"], ["a", "b", "child"]]:
        assert value.alter_blockers(table, column_path=path, action=action)
        with pytest.raises(ValueError, match="dependent"):
            value.assert_alter_allowed(table, column_path=path, action=action)
    for path in [["a.b"], ["a", "c"], ["A", "b"]]:
        assert value.alter_blockers(table, column_path=path, action=action) == []
        value.assert_alter_allowed(table, column_path=path, action=action)
    assert value.alter_blockers(table, action=action)


def test_nonblocking_alter_edges_still_protect_drop_and_whole_subject_protects_all_columns() -> None:
    table, view = ref("t"), ref("v", "view")
    value = graph([table, view], [edge(view, table, blocks=False, paths=[["source"]])])
    value.assert_alter_allowed(table, column_path=["source"], action="rename")
    value.assert_alter_allowed(table, column_path=["source"], action="type")
    with pytest.raises(ValueError, match="dependent"):
        value.plan_alter(table, column_path=["source"], action="drop")
    plan = value.plan_alter(table, column_path=["source"], action="drop", cascade=True)
    assert plan.drop_objects == [view]
    assert [(drop.table, drop.column_path) for drop in plan.drop_columns] == [(table, ["source"])]
    whole = graph([table, view], [edge(view, table)])
    assert whole.alter_blockers(table, column_path=["anything", "deep"])


def test_generated_columns_drop_in_order_independent_closure_without_dropping_the_table() -> None:
    table, view, external = ref("t"), ref("v", "view"), ref("other")
    edges = [
        edge(table, table, column=["generated"], paths=[["source"]]),
        edge(table, table, column=["next"], paths=[["generated"]]),
        edge(view, table, paths=[["next"]]),
        edge(external, table, column=["copied"], paths=[["next"]]),
    ]
    for order in itertools.permutations(edges):
        value = graph([table, view, external], list(order))
        with pytest.raises(ValueError, match="dependent"):
            value.plan_alter(table, column_path=["source"], action="drop")
        result = value.plan_alter(table, column_path=["source"], action="drop", cascade=True)
        assert result.drop_objects == [view]
        assert {(drop.table.name, tuple(drop.column_path)) for drop in result.drop_columns} == {
            ("t", ("source",)),
            ("t", ("generated",)),
            ("t", ("next",)),
            ("other", ("copied",)),
        }
    leaf = graph([table], [edge(table, table, column=["generated"], paths=[["source"]])])
    assert leaf.plan_alter(table, column_path=["generated"], action="drop").drop_objects == []
    assert leaf.drop_set(table) == [table]


def test_owned_column_deletions_close_before_restrict_and_parent_drop_subsumes_children() -> None:
    table, auxiliary = ref("t"), ref("aux")
    edges = [
        edge(auxiliary, table, "owned", column=["record", "child"], paths=[["source"]]),
        edge(auxiliary, table, "owned", column=["record"], paths=[["source"]]),
        edge(auxiliary, auxiliary, column=["record", "other"], paths=[["record", "child"]]),
    ]
    value = graph([table, auxiliary], edges)
    plan = value.plan_alter(table, column_path=["source"], action="drop")
    assert plan.drop_objects == []
    assert {(drop.table.name, tuple(drop.column_path)) for drop in plan.drop_columns} == {
        ("t", ("source",)),
        ("aux", ("record",)),
    }


def test_whole_object_drop_refuses_to_flatten_an_external_column_into_table_deletion() -> None:
    table, dependent = ref("t"), ref("dependent")
    value = graph([table, dependent], [edge(dependent, table, column=["generated"], paths=[["source"]])])
    with pytest.raises(ValueError, match="dependent"):
        value.drop_set(table)
    with pytest.raises(ValueError, match="object-only"):
        value.drop_set(table, cascade=True)
    plan = value.plan_alter(table, action="drop", cascade=True)
    assert plan.drop_objects == [table]
    assert [(drop.table, drop.column_path) for drop in plan.drop_columns] == [(dependent, ["generated"])]


def test_column_cycles_include_parent_overlap_and_mixed_object_column_nodes() -> None:
    a, b, view = ref("a"), ref("b"), ref("v", "view")
    scenarios = [
        [edge(a, a, column=["x"], paths=[["y"]]), edge(a, a, column=["y"], paths=[["x"]])],
        [edge(a, b, column=["x"], paths=[["y"]]), edge(b, a, column=["y", "child"], paths=[["x"]])],
        [edge(a, view, column=["x"]), edge(view, a, paths=[["x"]])],
    ]
    for edges in scenarios:
        for order in itertools.permutations(edges):
            with pytest.raises(ValueError, match="column dependency cycle"):
                graph([a, b, view], list(order))
    # Distinct columns in one table must not collapse into an object-level cycle.
    graph([a, b], [edge(a, b, column=["x"], paths=[["y"]]), edge(b, a, column=["y"], paths=[["z"]])])


def test_object_lifetime_cycles_do_not_turn_independent_generated_columns_into_cycles() -> None:
    table, index = ref("t"), ref("i", "index")
    edges = [
        edge(table, index),
        edge(index, table, "owned"),
        edge(table, table, column=["generated"], paths=[["source"]]),
    ]
    for order in itertools.permutations(edges):
        value = graph([table, index], list(order))
        assert value.drop_set(table) == sorted([table, index], key=lambda item: item.key())
    # A view output still depends on its whole definition. An owned view can
    # bind its owner's generated column and form a real evaluation cycle.
    view = ref("v", "view")
    with pytest.raises(ValueError, match="column dependency cycle"):
        graph(
            [table, view],
            [
                edge(table, view, column=["generated"], paths=[["output"]]),
                edge(view, table, "owned", paths=[["generated"]]),
            ],
        )
    # Subject paths scope ALTER protection, never weaken object ownership.
    other = ref("other")
    with pytest.raises(ValueError, match="ownership cycle"):
        graph(
            [table, other], [edge(table, other, "owned", paths=[["id"]]), edge(other, table, "owned", paths=[["id"]])]
        )


def test_semantic_plans_recheck_mutation_target_schemas_and_actions() -> None:
    table = ref("t")
    value = graph([table], [])
    schemas = {table.key(): pa.schema([pa.field("present", pa.int64())])}
    assert value.plan_alter(table, column_path=["present"], action="rename", schemas=schemas).drop_objects == []
    with pytest.raises(ValueError, match="missing or ambiguous"):
        value.plan_alter(table, column_path=["missing"], action="drop", schemas=schemas)
    with pytest.raises(ValueError, match="authoritative schema"):
        value.plan_alter(table, column_path=["present"], action="drop", schemas={})
    with pytest.raises(ValueError, match="does not exist"):
        value.plan_alter(ref("absent"), action="rename")
    with pytest.raises(ValueError, match="unknown semantic"):
        value.plan_alter(table, action=cast(Literal["rename", "type", "drop"], "comment"))
    with pytest.raises(ValueError, match="boolean"):
        value.plan_alter(table, action="drop", cascade=cast(bool, 1))
    dependency = edge(table, table, column=["generated"], paths=[["source"]])
    present(dependency.subject_column_paths).append([])
    with pytest.raises(ValueError, match="at least one"):
        dependency.serialize_to_bytes()


@pytest.mark.parametrize("missing", ["blocks_alter", "subject_column_paths", "dependent_column_path"])
def test_dependency_wire_requires_new_fields_even_when_nullable(missing: str) -> None:
    original = edge(ref("a"), ref("b"))
    with pa.ipc.open_stream(original.serialize_to_bytes()) as reader:
        batch = reader.read_next_batch()
    with pytest.raises(ValueError, match="complete schema"):
        ObjectDependency.deserialize_from_batch(batch.remove_column(batch.schema.get_field_index(missing)))
