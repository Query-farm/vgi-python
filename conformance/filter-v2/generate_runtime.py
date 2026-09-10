# Copyright 2026 Query Farm LLC - https://query.farm

"""Generate the checked-in Arrow IPC Filter v2 semantic corpus."""

from __future__ import annotations

import hashlib
import json
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa

from vgi.filter_v2 import DUCKDB_SESSION_CONTEXT, EvaluationContext
from vgi.filter_v2_builder import FilterPayload, build_filter_batch, serialize_filter_batch, type_payload, value_payload
from vgi.table_filter_pushdown import deserialize_filters

ROOT = Path(__file__).resolve().parent
RUNTIME = ROOT / "runtime"


def column(name: str, index: int) -> dict[str, object]:
    """Build a column-reference node."""
    return {"node": "column_ref", "column_index": index, "column_name": name}


def literal(index: int) -> dict[str, object]:
    """Build a literal-reference node."""
    return {"node": "literal", "value_ref": index}


def predicate(
    expression: dict[str, object], *, predicate_id: str = "query:0", mode: str = "required"
) -> dict[str, object]:
    """Wrap an expression as a predicate."""
    return {
        "id": predicate_id,
        "revision": 0,
        "mode": mode,
        "source": "query",
        "expression": expression,
    }


def snapshot(*predicates: dict[str, object]) -> dict[str, object]:
    """Build a Filter v2 snapshot document."""
    return {
        "encoding": "vgi.filters.v2",
        "semantics": "vgi.duckdb.standard.v1",
        "kind": "snapshot",
        "predicates": list(predicates),
    }


def write_batch(path: Path, batch: pa.RecordBatch) -> None:
    """Write one canonical Arrow IPC stream."""
    path.write_bytes(serialize_filter_batch(batch))


def digest(path: Path) -> str:
    """Return a file's SHA-256 digest."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def add_case(
    manifest: list[dict[str, Any]],
    case_id: str,
    input_batch: pa.RecordBatch,
    filter_batch: pa.RecordBatch,
    *,
    join_keys: list[pa.RecordBatch] | None = None,
    error: str | None = None,
    worker: bool = True,
    evaluation_capabilities: list[list[str | None]] | None = None,
    deltas: list[pa.RecordBatch] | None = None,
) -> None:
    """Materialize one corpus case and append its manifest entry."""
    case_dir = RUNTIME / case_id
    case_dir.mkdir(parents=True)
    input_path = case_dir / "input.arrow"
    filter_path = case_dir / "filter.arrow"
    write_batch(input_path, input_batch)
    write_batch(filter_path, filter_batch)
    entry: dict[str, Any] = {
        "id": f"runtime.{case_id}",
        "input": str(input_path.relative_to(ROOT)),
        "filter": str(filter_path.relative_to(ROOT)),
    }
    if worker:
        entry["worker"] = {
            "function": "filter_echo",
            "schema_path": ["main"],
            "arguments": [5],
            "projection_ids": [0, 1],
        }
    if join_keys:
        entry["join_keys"] = []
        for index, keys in enumerate(join_keys):
            key_path = case_dir / f"join-keys-{index}.arrow"
            write_batch(key_path, keys)
            entry["join_keys"].append(str(key_path.relative_to(ROOT)))
    capabilities = tuple((str(profile), fingerprint) for profile, fingerprint in (evaluation_capabilities or []))
    if evaluation_capabilities:
        entry["evaluation_capabilities"] = evaluation_capabilities
    if deltas:
        entry["deltas"] = []
        for index, delta in enumerate(deltas):
            delta_path = case_dir / f"delta-{index}.arrow"
            write_batch(delta_path, delta)
            entry["deltas"].append(str(delta_path.relative_to(ROOT)))
    if error is not None:
        entry["error"] = error
    else:
        filters = deserialize_filters(
            filter_batch,
            output_schema=input_batch.schema,
            join_keys=join_keys,
            evaluation_capabilities=capabilities,
        )
        for delta in deltas or []:
            filters = filters.apply_delta(delta)
        expected_path = case_dir / "expected.arrow"
        write_batch(expected_path, filters.apply(input_batch))
        entry["expected"] = str(expected_path.relative_to(ROOT))
        if worker:
            worker_input = pa.RecordBatch.from_pydict(
                {
                    "n": pa.array(range(5), type=pa.int64()),
                    "s": [f"row_{index}" for index in range(5)],
                    "pushed_filters": ["unused"] * 5,
                }
            )
            worker_filters = deserialize_filters(
                filter_batch,
                output_schema=worker_input.schema,
                join_keys=join_keys,
                evaluation_capabilities=capabilities,
            )
            worker_expected_path = case_dir / "worker-expected.arrow"
            worker_expected = worker_filters.apply(worker_input).select(["n", "s"])
            write_batch(worker_expected_path, worker_expected)
            entry["worker_expected"] = str(worker_expected_path.relative_to(ROOT))
    manifest.append(entry)


def main() -> None:
    """Regenerate all runtime corpus artifacts."""
    if RUNTIME.exists():
        shutil.rmtree(RUNTIME)
    RUNTIME.mkdir()
    cases: list[dict[str, Any]] = []

    numbers = pa.RecordBatch.from_pydict({"n": pa.array([None, -1, 0, 1, 2], type=pa.int64())})
    for op in ("eq", "ne", "lt", "le", "gt", "ge", "distinct_from", "not_distinct_from"):
        expression = {"node": "comparison", "op": op, "left": column("n", 0), "right": literal(0)}
        add_case(
            cases,
            f"comparison-{op}",
            numbers,
            build_filter_batch(snapshot(predicate(expression)), [value_payload(0, 1, pa.int64())]),
        )

    booleans = pa.RecordBatch.from_pydict(
        {"a": pa.array([True, True, False, False, None, None]), "b": pa.array([True, None, True, None, True, None])}
    )
    a = {"node": "comparison", "op": "eq", "left": column("a", 0), "right": literal(0)}
    b = {"node": "comparison", "op": "eq", "left": column("b", 1), "right": literal(0)}
    for node, expression in (
        ("and", {"node": "and", "children": [a, b]}),
        ("or", {"node": "or", "children": [a, b]}),
        ("not", {"node": "not", "expression": a}),
    ):
        add_case(
            cases,
            f"kleene-{node}",
            booleans,
            build_filter_batch(snapshot(predicate(expression)), [value_payload(0, True, pa.bool_())]),
            worker=False,
        )

    for negated in (False, True):
        expression = {"node": "is_null", "expression": column("n", 0), "negated": negated}
        add_case(
            cases, "is-not-null" if negated else "is-null", numbers, build_filter_batch(snapshot(predicate(expression)))
        )

    nested_type = pa.struct([pa.field("middle", pa.struct([pa.field("leaf", pa.int64())]))])
    nested = pa.RecordBatch.from_pydict(
        {"root": [{"middle": {"leaf": 1}}, {"middle": None}, None, {"middle": {"leaf": None}}]},
        schema=pa.schema([pa.field("root", nested_type)]),
    )
    field_ref = {
        "node": "field_ref",
        "expression": {
            "node": "field_ref",
            "expression": column("root", 0),
            "field_index": 0,
            "field_name": "middle",
        },
        "field_index": 0,
        "field_name": "leaf",
    }
    add_case(
        cases,
        "nested-null-propagation",
        nested,
        build_filter_batch(snapshot(predicate({"node": "is_null", "expression": field_ref, "negated": False}))),
        worker=False,
    )

    needles = pa.RecordBatch.from_pydict({"n": pa.array([None, 1, 2, 3], type=pa.int64())})
    for name, values, negated in (
        ("in-with-null", [1, None, 3], False),
        ("not-in-with-null", [1, None, 3], True),
        ("in-empty", [], False),
        ("not-in-empty", [], True),
    ):
        expression = {
            "node": "in",
            "expression": column("n", 0),
            "set": {"kind": "literal", "value_ref": 0},
            "negated": negated,
        }
        add_case(
            cases,
            name,
            needles,
            build_filter_batch(snapshot(predicate(expression)), [value_payload(0, values, pa.list_(pa.int64()))]),
        )

    external = {
        "node": "in",
        "expression": column("n", 0),
        "set": {"kind": "external", "batch_index": 0, "column_index": 1, "column_name": "key"},
        "negated": False,
    }
    keys = pa.RecordBatch.from_pydict({"ignored": [9, 9, 9], "key": pa.array([1, 3, None], type=pa.int64())})
    add_case(
        cases,
        "external-in-nonzero-column",
        needles,
        build_filter_batch(snapshot(predicate(external))),
        join_keys=[keys],
    )

    strings = pa.RecordBatch.from_pydict(
        {
            "s": pa.array([None, "", "alphabet", "beta", "zeta"]),
            "items": pa.array([None, [], ["x"], ["a", "x"], ["z"]]),
        }
    )
    function_cases = {
        "starts-with": ("starts_with", column("s", 0), value_payload(0, "alpha")),
        "ends-with": ("ends_with", column("s", 0), value_payload(0, "ta")),
        "contains": ("contains", column("s", 0), value_payload(0, "pha")),
        "list-contains": ("list_contains", column("items", 1), value_payload(0, "x")),
    }
    for name, (function, argument, payload) in function_cases.items():
        expression = {"node": "call", "function": function, "arguments": [argument, literal(0)]}
        add_case(
            cases,
            f"function-{name}",
            strings,
            build_filter_batch(snapshot(predicate(expression)), [payload]),
            worker=False,
        )

    arithmetic_input = pa.RecordBatch.from_pydict({"n": pa.array([-2, -1, 0, 1, 2, None], type=pa.int64())})
    for op in ("add", "subtract", "multiply"):
        arithmetic = {"node": "arithmetic", "op": op, "left": column("n", 0), "right": literal(0)}
        expression = {"node": "comparison", "op": "gt", "left": arithmetic, "right": literal(1)}
        add_case(
            cases,
            f"arithmetic-{op}",
            arithmetic_input,
            build_filter_batch(
                snapshot(predicate(expression)),
                [value_payload(0, 2, pa.int64()), value_payload(1, 0, pa.int64())],
            ),
            worker=False,
        )
    negated = {"node": "negate", "expression": column("n", 0)}
    add_case(
        cases,
        "arithmetic-negate",
        arithmetic_input,
        build_filter_batch(
            snapshot(predicate({"node": "comparison", "op": "gt", "left": negated, "right": literal(0)})),
            [value_payload(0, 0, pa.int64())],
        ),
        worker=False,
    )

    cast_expression = {
        "node": "comparison",
        "op": "eq",
        "left": {"node": "cast", "expression": column("n", 0), "type_ref": 0},
        "right": literal(0),
    }
    add_case(
        cases,
        "cast-integer-to-double",
        arithmetic_input,
        build_filter_batch(
            snapshot(predicate(cast_expression)),
            [type_payload(0, pa.float64()), value_payload(0, 2.0, pa.float64())],
        ),
        worker=False,
    )

    floats = pa.RecordBatch.from_pydict({"x": pa.array([float("nan"), -0.0, 0.0, 1.0], type=pa.float64())})
    add_case(
        cases,
        "nan-comparison",
        floats,
        build_filter_batch(
            snapshot(predicate({"node": "comparison", "op": "eq", "left": column("x", 0), "right": literal(0)})),
            [value_payload(0, float("nan"), pa.float64())],
        ),
        worker=False,
    )
    add_case(
        cases,
        "negative-zero-membership",
        floats,
        build_filter_batch(
            snapshot(
                predicate(
                    {
                        "node": "in",
                        "expression": column("x", 0),
                        "set": {"kind": "literal", "value_ref": 0},
                        "negated": False,
                    }
                )
            ),
            [value_payload(0, [-0.0], pa.list_(pa.float64()))],
        ),
        worker=False,
    )

    session = EvaluationContext(
        profile=DUCKDB_SESSION_CONTEXT,
        time_zone="America/New_York",
        calendar="gregorian",
        default_collation="binary",
        ieee_floating_point_ops=True,
        integer_division=True,
    )
    division = {
        "node": "comparison",
        "op": "eq",
        "left": {"node": "arithmetic", "op": "divide", "left": column("n", 0), "right": literal(0)},
        "right": literal(1),
    }
    add_case(
        cases,
        "session-integer-division",
        arithmetic_input,
        build_filter_batch(
            snapshot(predicate(division)),
            [value_payload(0, 2, pa.int64()), value_payload(1, 1, pa.int64())],
            context=session,
        ),
        worker=False,
        evaluation_capabilities=[[DUCKDB_SESSION_CONTEXT, None]],
    )

    add_case(cases, "empty-snapshot", numbers, build_filter_batch(snapshot()))

    def delta(*updates: dict[str, object]) -> dict[str, object]:
        return {
            "encoding": "vgi.filters.v2",
            "semantics": "vgi.duckdb.standard.v1",
            "kind": "delta",
            "updates": list(updates),
        }

    upsert_gt = {
        "operation": "upsert",
        "id": "dynamic:0",
        "revision": 1,
        "mode": "advisory",
        "source": "top_n",
        "expression": {"node": "comparison", "op": "gt", "left": column("n", 0), "right": literal(0)},
    }
    add_case(
        cases,
        "delta-upsert-remove-stale",
        numbers,
        build_filter_batch(snapshot()),
        deltas=[
            build_filter_batch(delta(upsert_gt), [value_payload(0, 0, pa.int64())]),
            build_filter_batch(delta({"operation": "remove", "id": "dynamic:0", "revision": 2})),
            build_filter_batch(
                delta(
                    {
                        **upsert_gt,
                        "expression": {
                            "node": "comparison",
                            "op": "eq",
                            "left": column("n", 0),
                            "right": literal(99),
                        },
                    }
                )
            ),
        ],
        worker=False,
    )
    second = {
        "operation": "upsert",
        "id": "dynamic:1",
        "revision": 1,
        "mode": "advisory",
        "source": "join",
        "expression": {"node": "comparison", "op": "lt", "left": column("n", 0), "right": literal(1)},
    }
    add_case(
        cases,
        "delta-atomic-multi-update",
        numbers,
        build_filter_batch(snapshot()),
        deltas=[
            build_filter_batch(
                delta(upsert_gt, second), [value_payload(0, 0, pa.int64()), value_payload(1, 2, pa.int64())]
            )
        ],
        worker=False,
    )
    add_case(
        cases,
        "delta-empty-noop",
        numbers,
        build_filter_batch(snapshot()),
        deltas=[build_filter_batch(delta())],
        worker=False,
    )

    bad_name = {"node": "is_null", "expression": column("wrong", 0), "negated": False}
    add_case(
        cases,
        "error-column-name",
        numbers,
        build_filter_batch(snapshot(predicate(bad_name))),
        error="does not match index",
    )
    bad_index = {"node": "is_null", "expression": column("n", 99), "negated": False}
    add_case(
        cases, "error-column-index", numbers, build_filter_batch(snapshot(predicate(bad_index))), error="out of range"
    )
    add_case(
        cases,
        "error-non-boolean-root",
        numbers,
        build_filter_batch(snapshot(predicate(column("n", 0)))),
        error="predicate root must resolve to BOOLEAN",
    )
    missing_literal = {"node": "comparison", "op": "eq", "left": column("n", 0), "right": literal(99)}
    add_case(
        cases,
        "error-missing-literal",
        numbers,
        build_filter_batch(snapshot(predicate(missing_literal))),
        error="value_99",
    )
    invalid_call = {"node": "call", "function": "starts_with", "arguments": [column("n", 0), literal(0)]}
    add_case(
        cases,
        "error-invalid-overload",
        numbers,
        build_filter_batch(snapshot(predicate(invalid_call)), [value_payload(0, "x")]),
        error="does not bind",
    )
    bad_field_input = {
        "node": "is_null",
        "expression": {
            "node": "field_ref",
            "expression": column("n", 0),
            "field_index": 0,
            "field_name": "x",
        },
        "negated": False,
    }
    add_case(
        cases,
        "error-field-non-struct",
        numbers,
        build_filter_batch(snapshot(predicate(bad_field_input))),
        error="struct type",
    )
    bad_literal_set = {
        "node": "in",
        "expression": column("n", 0),
        "set": {"kind": "literal", "value_ref": 0},
        "negated": False,
    }
    add_case(
        cases,
        "error-in-payload-not-list",
        numbers,
        build_filter_batch(snapshot(predicate(bad_literal_set)), [value_payload(0, 1, pa.int64())]),
        error="list scalar",
    )
    add_case(
        cases,
        "error-in-payload-null-list",
        numbers,
        build_filter_batch(snapshot(predicate(bad_literal_set)), [value_payload(0, None, pa.list_(pa.int64()))]),
        error="list must not be NULL",
    )
    bad_external_column = {
        "node": "in",
        "expression": column("n", 0),
        "set": {"kind": "external", "batch_index": 0, "column_index": 9, "column_name": "key"},
        "negated": False,
    }
    add_case(
        cases,
        "error-external-column-index",
        numbers,
        build_filter_batch(snapshot(predicate(bad_external_column))),
        join_keys=[keys],
        error="out of range",
    )
    bad_external_name = {
        **external,
        "set": {"kind": "external", "batch_index": 0, "column_index": 1, "column_name": "wrong"},
    }
    add_case(
        cases,
        "error-external-column-name",
        numbers,
        build_filter_batch(snapshot(predicate(bad_external_name))),
        join_keys=[keys],
        error="does not match",
    )
    bad_cast = {
        "node": "comparison",
        "op": "eq",
        "left": {"node": "cast", "expression": column("n", 0), "type_ref": 0},
        "right": literal(0),
    }
    add_case(
        cases,
        "error-cast-type-not-null",
        numbers,
        build_filter_batch(
            snapshot(predicate(bad_cast)),
            [
                FilterPayload(pa.field("type_0", pa.int64()), pa.scalar(1, type=pa.int64())),
                value_payload(0, 1, pa.int64()),
            ],
        ),
        error="type_0 must contain a NULL",
    )
    duplicate = predicate({"node": "is_null", "expression": column("n", 0), "negated": False}, predicate_id="dup")
    add_case(
        cases,
        "error-duplicate-predicate-id",
        numbers,
        build_filter_batch(snapshot(duplicate, duplicate)),
        error="duplicate predicate ID",
    )
    unknown_function = {"node": "call", "function": "not_registered", "arguments": [column("n", 0)]}
    add_case(
        cases,
        "error-unknown-standard-function",
        numbers,
        build_filter_batch(snapshot(predicate(unknown_function))),
        error="unknown standard filter function",
    )
    contextual_cast = {
        "node": "comparison",
        "op": "eq",
        "left": {"node": "cast", "expression": column("s", 0), "type_ref": 0},
        "right": literal(0),
    }
    contextual_input = pa.RecordBatch.from_pydict({"s": ["2024-01-01 00:00:00"]})
    add_case(
        cases,
        "error-contextual-cast-without-session",
        contextual_input,
        build_filter_batch(
            snapshot(predicate(contextual_cast)),
            [
                type_payload(0, pa.timestamp("us", tz="UTC")),
                value_payload(0, datetime(2024, 1, 1, tzinfo=UTC), pa.timestamp("us", tz="UTC")),
            ],
        ),
        error="requires vgi.duckdb.session.v1",
        worker=False,
    )

    for entry in cases:
        referenced = [ROOT / entry[key] for key in ("input", "filter", "expected", "worker_expected") if key in entry]
        referenced.extend(ROOT / path for path in entry.get("join_keys", []))
        referenced.extend(ROOT / path for path in entry.get("deltas", []))
        entry["sha256"] = {str(path.relative_to(ROOT)): digest(path) for path in referenced}

    manifest_path = ROOT / "runtime-manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "manifest_version": 1,
                "vgi_protocol_version": "2.0.0",
                "filter_encoding": "vgi.filters.v2",
                "cases": cases,
            },
            indent=2,
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
