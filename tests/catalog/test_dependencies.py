# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103
"""Object identity, dependency validation and order-independent drop closure."""

import itertools
from dataclasses import replace

import pyarrow as pa
import pytest

from vgi.catalog.dependencies import CatalogObjectRef, DependencySnapshot, ObjectDependency


def ref(name: str, kind: str = "table", path: list[str] | None = None, owner: str | None = None) -> CatalogObjectRef:
    return CatalogObjectRef(kind=kind, schema_path=["main"] if path is None else path, name=name, owner_table=owner)


def edge(d: CatalogObjectRef, s: CatalogObjectRef, kind: str = "requires") -> ObjectDependency:
    return ObjectDependency(dependent=d.serialize_to_bytes(), subject=s.serialize_to_bytes(), kind=kind)


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
def test_malformed_object_references_reject(kwargs: dict) -> None:
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
        "duplicate_edge",
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
        elif case == "duplicate_edge":
            graph([a, b], [edge(a, b), edge(a, b)])
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
