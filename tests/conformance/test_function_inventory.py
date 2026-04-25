"""Fail when the example worker registers a function the Python probe can't reach.

The example worker at ``vgi-example-worker`` registers every example function
VGI is meant to support (scalar, table, table-in-out, aggregate, macro, view,
data-scan). This test enumerates them through the catalog protocol and, for
each *category*, asserts that ``Client`` provides a matching invocation
entry point.

This catches drift of the form "a new function type was added to the worker
and nothing in the Python test harness knows how to drive it" — the
aggregate-function gap being the motivating example.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from vgi.catalog.catalog_interface import AttachId, SchemaObjectType
from vgi.client.client import Client

# The catalog the example worker exposes. Hardcoded to match
# ``vgi/examples/worker.py::_EXAMPLE_CATALOG``.
EXAMPLE_CATALOG_NAME = "example"
EXAMPLE_SCHEMA_NAME = "main"


@dataclass(frozen=True)
class TypeCoverage:
    """How ``Client`` reaches a given ``SchemaObjectType``.

    ``client_method`` is the Python attribute that drives the RPC. ``None``
    means no probe wrapper exists; the test then fails with guidance unless
    ``acknowledge_missing`` cites coverage elsewhere.
    """

    client_method: str | None
    acknowledge_missing: str | None = None


# Map each schema object category to the expected ``Client`` probe method.
# Update this when a new category is added.
_COVERAGE_MAP: dict[SchemaObjectType, TypeCoverage] = {
    SchemaObjectType.SCALAR_FUNCTION: TypeCoverage(client_method="scalar_function"),
    SchemaObjectType.TABLE_FUNCTION: TypeCoverage(client_method="table_function"),
    SchemaObjectType.AGGREGATE_FUNCTION: TypeCoverage(
        client_method=None,
        acknowledge_missing=(
            "No Client.aggregate_function yet; aggregates are exercised through "
            "C++ integration/aggregate/* and worker-side tests/test_aggregate_function.py."
        ),
    ),
    SchemaObjectType.SCALAR_MACRO: TypeCoverage(
        client_method=None,
        acknowledge_missing="Macros are invoked via DuckDB — no Python probe needed.",
    ),
    SchemaObjectType.TABLE_MACRO: TypeCoverage(
        client_method=None,
        acknowledge_missing="Macros are invoked via DuckDB — no Python probe needed.",
    ),
    SchemaObjectType.VIEW: TypeCoverage(
        client_method="view_get",
        acknowledge_missing=None,
    ),
    SchemaObjectType.TABLE: TypeCoverage(
        client_method="table_get",
        acknowledge_missing=None,
    ),
    SchemaObjectType.INDEX: TypeCoverage(
        client_method=None,
        acknowledge_missing="Indexes are catalog metadata only; no invocation probe needed.",
    ),
}


@pytest.fixture(scope="module")
def attached_example() -> tuple[str, AttachId]:
    """Attach to the ``example`` catalog once per module and yield ``(worker, attach_id)``."""
    worker = "vgi-example-worker"
    client = Client(worker)
    result = client.catalog_attach(
        name=EXAMPLE_CATALOG_NAME,
        options={},
        data_version_spec=None,
        implementation_version=None,
    )
    return worker, AttachId(result.attach_id)


def test_example_catalog_is_attachable() -> None:
    """Sanity: the example worker advertises a catalog named ``example``."""
    client = Client("vgi-example-worker")
    names = [c.name for c in client.catalogs()]
    assert EXAMPLE_CATALOG_NAME in names, (
        f"Example worker should advertise the {EXAMPLE_CATALOG_NAME!r} catalog; got {names!r}"
    )


def test_every_schema_object_type_is_covered() -> None:
    """Catch new ``SchemaObjectType`` values added without an entry in ``_COVERAGE_MAP``."""
    missing = [t for t in SchemaObjectType if t not in _COVERAGE_MAP]
    assert not missing, (
        f"SchemaObjectType values missing from _COVERAGE_MAP: {[t.name for t in missing]}. "
        "Add a TypeCoverage entry (either a client_method or a documented "
        "acknowledge_missing reason)."
    )


def test_coverage_map_methods_resolve_on_client() -> None:
    """Every ``client_method`` in the coverage map must actually exist on ``Client``."""
    broken: list[str] = []
    for type_, cov in _COVERAGE_MAP.items():
        if cov.client_method is None:
            continue
        if not callable(getattr(Client, cov.client_method, None)):
            broken.append(f"{type_.name} -> Client.{cov.client_method} (missing)")
    assert not broken, "Coverage-map client methods not found on Client:\n  - " + "\n  - ".join(broken)


def test_coverage_gaps_are_documented() -> None:
    """Every ``client_method=None`` entry must explain where the type is exercised."""
    bare = [
        t.name
        for t, cov in _COVERAGE_MAP.items()
        if cov.client_method is None and not (cov.acknowledge_missing or "").strip()
    ]
    assert not bare, f"Coverage gaps without acknowledge_missing reasons: {bare}"


@pytest.mark.parametrize(
    "schema_type",
    [
        SchemaObjectType.SCALAR_FUNCTION,
        SchemaObjectType.TABLE_FUNCTION,
        SchemaObjectType.AGGREGATE_FUNCTION,
    ],
)
def test_example_worker_registers_at_least_one_per_category(
    attached_example: tuple[str, AttachId],
    schema_type: SchemaObjectType,
) -> None:
    """The example worker should register >= 1 function of each invocable category.

    If this fails for a category, either a function was unintentionally
    removed from ``_EXAMPLE_CATALOG`` or the worker's catalog listing is
    broken — both are drift we want to catch.
    """
    _worker, attach_id = attached_example
    client = Client("vgi-example-worker")
    infos = client.schema_contents(
        attach_id=attach_id,
        name=EXAMPLE_SCHEMA_NAME,
        type=schema_type,
    )
    assert len(infos) > 0, (
        f"Example worker should register at least one {schema_type.name}; schema {EXAMPLE_SCHEMA_NAME!r} returned zero."
    )


def test_scalar_function_end_to_end(attached_example: tuple[str, bytes]) -> None:
    """Exercise ``Client.scalar_function`` against a known canonical scalar.

    ``double`` takes one int64 column, doubles it. This covers the scalar
    bind/init/exchange path end-to-end.
    """
    import pyarrow as pa

    from vgi.arguments import Arguments

    worker, _attach_id = attached_example
    input_schema = pa.schema([("x", pa.int64())])
    input_batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3]}, schema=input_schema)

    with Client(worker) as client:
        out_batches = list(
            client.scalar_function(
                function_name="double",
                arguments=Arguments(positional=(pa.scalar("x"),)),
                input=iter([input_batch]),
            )
        )

    rows = [row for b in out_batches for row in b.column("result").to_pylist()]
    assert rows == [2, 4, 6]


def test_table_function_end_to_end(attached_example: tuple[str, bytes]) -> None:
    """Exercise ``Client.table_function`` against the canonical ``sequence`` generator."""
    import pyarrow as pa

    from vgi.arguments import Arguments

    worker, _attach_id = attached_example

    with Client(worker) as client:
        out_batches = list(
            client.table_function(
                function_name="sequence",
                arguments=Arguments(positional=(pa.scalar(5),)),
            )
        )
    total_rows = sum(b.num_rows for b in out_batches)
    assert total_rows == 5
