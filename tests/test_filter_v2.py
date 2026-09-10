# Copyright 2026 Query Farm LLC - https://query.farm

"""Focused model, validation, and state-machine tests for Filter Encoding v2."""

from __future__ import annotations

import json

import pyarrow as pa
import pytest

from vgi import (
    Call,
    Comparison,
    EvaluationContextCapability,
    FieldRef,
    FilterDeserializationError,
    FilterFunctionCapability,
    FilterSemanticProfile,
    FilterVersionError,
    RuntimeFilterAlgorithmCapability,
    deserialize_filters,
)

_METADATA = {
    b"vgi_filter_encoding": b"vgi.filters.v2",
    b"vgi_filter_version": b"2",
    b"vgi_evaluation_context": b"vgi.none.v1",
}


def _document(predicates: list[dict[str, object]]) -> dict[str, object]:
    return {
        "encoding": "vgi.filters.v2",
        "semantics": "vgi.duckdb.standard.v1",
        "kind": "snapshot",
        "predicates": predicates,
    }


def _predicate(
    expression: dict[str, object],
    *,
    predicate_id: str = "query:0",
    mode: str = "required",
) -> dict[str, object]:
    return {
        "id": predicate_id,
        "revision": 0,
        "mode": mode,
        "source": "query",
        "expression": expression,
    }


def _batch(
    document: dict[str, object] | str,
    *payload: tuple[pa.Field, object],  # type: ignore[type-arg]
    metadata: dict[bytes, bytes] | None = None,
) -> pa.RecordBatch:
    text = document if isinstance(document, str) else json.dumps(document, separators=(",", ":"))
    fields = [pa.field("filter_spec", pa.string(), nullable=False), *(field for field, _ in payload)]
    arrays = [pa.array([text]), *(pa.array([value], type=field.type) for field, value in payload)]
    return pa.RecordBatch.from_arrays(arrays, schema=pa.schema(fields, metadata=metadata or _METADATA))


def _delta(updates: list[dict[str, object]], *payload: tuple[pa.Field, object]) -> pa.RecordBatch:  # type: ignore[type-arg]
    return _batch(
        {
            "encoding": "vgi.filters.v2",
            "semantics": "vgi.duckdb.standard.v1",
            "kind": "delta",
            "updates": updates,
        },
        *payload,
    )


def _column(name: str = "n", index: int = 0) -> dict[str, object]:
    return {"node": "column_ref", "column_index": index, "column_name": name}


def _comparison(value_ref: int = 0, *, op: str = "gt") -> dict[str, object]:
    return {
        "node": "comparison",
        "op": op,
        "left": _column(),
        "right": {"node": "literal", "value_ref": value_ref},
    }


def test_nested_field_refs_are_recursive_and_null_propagating() -> None:
    """Nested structs use authoritative indexes at every level."""
    output_schema = pa.schema(
        [pa.field("record", pa.struct([pa.field("address", pa.struct([pa.field("zip", pa.int64())]))]))]
    )
    expression = {
        "node": "comparison",
        "op": "ge",
        "left": {
            "node": "field_ref",
            "expression": {
                "node": "field_ref",
                "expression": _column("record"),
                "field_index": 0,
                "field_name": "address",
            },
            "field_index": 0,
            "field_name": "zip",
        },
        "right": {"node": "literal", "value_ref": 0},
    }
    filters = deserialize_filters(
        _batch(_document([_predicate(expression)]), (pa.field("value_0", pa.int64()), 10000)),
        output_schema=output_schema,
    )
    root = filters.predicates[0].expression
    assert isinstance(root, Comparison)
    assert isinstance(root.left, FieldRef)
    assert isinstance(root.left.expression, FieldRef)

    data = pa.RecordBatch.from_pylist(
        [
            {"record": {"address": {"zip": 9999}}},
            {"record": {"address": {"zip": 10001}}},
            {"record": None},
            {"record": {"address": None}},
        ],
        schema=output_schema,
    )
    assert filters.evaluate(data).to_pylist() == [False, True, None, None]
    assert filters.apply(data).num_rows == 1


def test_column_and_field_names_validate_authoritative_indexes() -> None:
    """A redundant name mismatch is malformed rather than rebound by name."""
    output_schema = pa.schema([pa.field("actual", pa.int64())])
    batch = _batch(_document([_predicate({"node": "is_null", "expression": _column("wrong"), "negated": False})]))
    with pytest.raises(FilterDeserializationError, match="does not match index"):
        deserialize_filters(batch, output_schema=output_schema)


def test_external_in_is_exact_and_missing_external_data_is_an_error() -> None:
    """External set indexes and validating names are required even for advisory predicates."""
    expression = {
        "node": "in",
        "expression": _column(),
        "set": {"kind": "external", "batch_index": 0, "column_index": 0, "column_name": "n"},
        "negated": False,
    }
    batch = _batch(_document([_predicate(expression, mode="advisory")]))
    with pytest.raises(FilterDeserializationError, match="unavailable"):
        deserialize_filters(batch, output_schema=pa.schema([("n", pa.int64())]))

    keys = pa.RecordBatch.from_pydict({"n": [2, None]})
    filters = deserialize_filters(batch, output_schema=pa.schema([("n", pa.int64())]), join_keys=[keys])
    data = pa.RecordBatch.from_pydict({"n": [1, 2, None]})
    assert filters.evaluate(data).to_pylist() == [None, True, None]


def test_delta_revisions_tombstones_and_stale_payloads() -> None:
    """Higher revisions apply, removals persist, and stale payload references are not resolved."""
    snapshot = _batch(
        _document([_predicate(_comparison(), predicate_id="dynamic:0", mode="advisory")]),
        (pa.field("value_0", pa.int64()), 2),
    )
    state = deserialize_filters(snapshot, output_schema=pa.schema([("n", pa.int64())]))
    update = {
        "operation": "upsert",
        "id": "dynamic:0",
        "revision": 1,
        "mode": "advisory",
        "source": "join",
        "expression": _comparison(),
    }
    changed = state.apply_delta(_delta([update], (pa.field("value_0", pa.int64()), 4)))
    assert changed.apply(pa.RecordBatch.from_pydict({"n": [3, 5]})).column("n").to_pylist() == [5]

    removed = changed.apply_delta(_delta([{"operation": "remove", "id": "dynamic:0", "revision": 2}]))
    assert not removed
    stale = dict(update, revision=1, expression=_comparison(99))
    still_removed = removed.apply_delta(_delta([stale]))
    assert not still_removed
    assert dict(still_removed._v2_state.revisions)["dynamic:0"] == 2  # type: ignore[union-attr]


def test_delta_is_atomic_and_cannot_target_required_ids() -> None:
    """One invalid applicable update rejects the batch without mutating prior state."""
    original = deserialize_filters(
        _batch(_document([_predicate(_comparison())]), (pa.field("value_0", pa.int64()), 2)),
        output_schema=pa.schema([("n", pa.int64())]),
    )
    with pytest.raises(FilterDeserializationError, match="required predicate"):
        original.apply_delta(_delta([{"operation": "remove", "id": "query:0", "revision": 1}]))
    assert original.predicates[0].revision == 0

    advisory = deserialize_filters(
        _batch(
            _document([_predicate(_comparison(), predicate_id="a", mode="advisory")]),
            (pa.field("value_0", pa.int64()), 2),
        ),
        output_schema=pa.schema([("n", pa.int64())]),
    )
    updates = [
        {
            "operation": "upsert",
            "id": "a",
            "revision": 1,
            "mode": "advisory",
            "source": "join",
            "expression": _comparison(),
        },
        {
            "operation": "upsert",
            "id": "b",
            "revision": 1,
            "mode": "advisory",
            "source": "join",
            "expression": _comparison(99),
        },
    ]
    with pytest.raises(FilterDeserializationError, match="value_99"):
        advisory.apply_delta(_delta(updates, (pa.field("value_0", pa.int64()), 4)))
    assert advisory.predicates[0].revision == 0


@pytest.mark.parametrize(
    ("metadata", "message"),
    [
        ({b"vgi_filter_version": b"2", b"vgi_evaluation_context": b"vgi.none.v1"}, "encoding"),
        (
            {
                **_METADATA,
                b"vgi_evaluation_context": b"vgi.duckdb.session.v1",
                b"vgi_time_zone": b"UTC",
            },
            "incomplete",
        ),
        ({**_METADATA, b"vgi_time_zone": b"UTC"}, "forbids"),
    ],
)
def test_evaluation_context_metadata_is_strict(metadata: dict[bytes, bytes], message: str) -> None:
    """Context profiles reject missing, incomplete, and forbidden metadata."""
    batch = _batch(_document([]), metadata=metadata)
    exception = FilterVersionError if message == "encoding" else FilterDeserializationError
    with pytest.raises(exception, match=message):
        deserialize_filters(batch)


def test_duplicate_json_keys_and_v1_field_metadata_are_rejected() -> None:
    """JSON and Arrow envelopes have no compatibility or ambiguity fallback."""
    duplicate = (
        '{"encoding":"vgi.filters.v2","encoding":"vgi.filters.v2",'
        '"semantics":"vgi.duckdb.standard.v1","kind":"snapshot","predicates":[]}'
    )
    with pytest.raises(FilterDeserializationError, match="duplicate JSON"):
        deserialize_filters(_batch(duplicate))

    old_field = pa.field("filter_spec", pa.string(), nullable=False, metadata={b"vgi_filter_version": b"1"})
    old_batch = pa.RecordBatch.from_arrays([pa.array(["[]"])], schema=pa.schema([old_field]))
    with pytest.raises(FilterVersionError, match="encoding"):
        deserialize_filters(old_batch)


def test_v2_capability_models_validate_and_round_trip() -> None:
    """Structured capabilities replace the v1 free-form expression-name list."""
    function = FilterFunctionCapability("duckdb.spatial", "intersects_extent", 1)
    algorithm = RuntimeFilterAlgorithmCapability("duckdb.runtime_filter", "prefix_range", 1)
    context = EvaluationContextCapability("vgi.duckdb.session.v1", "duckdb-icu:fixture")
    assert FilterSemanticProfile.DUCKDB_STANDARD_V1.value == "vgi.duckdb.standard.v1"
    assert FilterFunctionCapability.from_dict(function.to_dict()) == function
    assert RuntimeFilterAlgorithmCapability.from_dict(algorithm.to_dict()) == algorithm
    assert EvaluationContextCapability.from_dict(context.to_dict()) == context
    with pytest.raises(ValueError, match="namespace"):
        FilterFunctionCapability("Not Canonical", "x", 1)


@pytest.mark.parametrize("operator", ["divide", "modulo"])
def test_context_dependent_arithmetic_requires_session_profile(operator: str) -> None:
    """Division and modulo cannot inherit unencoded evaluator session settings."""
    expression = {
        "node": "comparison",
        "op": "gt",
        "left": {
            "node": "arithmetic",
            "op": operator,
            "left": _column(),
            "right": {"node": "literal", "value_ref": 0},
        },
        "right": {"node": "literal", "value_ref": 1},
    }
    batch = _batch(
        _document([_predicate(expression)]),
        (pa.field("value_0", pa.int64()), 2),
        (pa.field("value_1", pa.int64()), 0),
    )
    with pytest.raises(FilterDeserializationError, match="requires vgi.duckdb.session.v1"):
        deserialize_filters(batch, output_schema=pa.schema([("n", pa.int64())]))


def test_session_context_requires_advertisement_and_exact_requested_fingerprint() -> None:
    """A producer-requested provider identity must match the worker capability."""
    metadata = {
        b"vgi_filter_encoding": b"vgi.filters.v2",
        b"vgi_filter_version": b"2",
        b"vgi_evaluation_context": b"vgi.duckdb.session.v1",
        b"vgi_time_zone": b"UTC",
        b"vgi_calendar": b"gregorian",
        b"vgi_default_collation": b"binary",
        b"vgi_ieee_floating_point_ops": b"true",
        b"vgi_integer_division": b"false",
        b"vgi_context_provider_fingerprint": b"producer-a",
    }
    batch = _batch(_document([]), metadata=metadata)
    with pytest.raises(FilterDeserializationError, match="was not advertised"):
        deserialize_filters(batch)
    with pytest.raises(FilterDeserializationError, match="fingerprint"):
        deserialize_filters(batch, evaluation_capabilities=(("vgi.duckdb.session.v1", "worker-b"),))
    filters = deserialize_filters(batch, evaluation_capabilities=(("vgi.duckdb.session.v1", "producer-a"),))
    assert filters.evaluation_context is not None
    assert filters.evaluation_context.provider_fingerprint == "producer-a"


def test_string_comparison_uses_binary_none_or_encoded_session_collation() -> None:
    """Uncollated strings use binary under none and the encoded nonbinary session default."""
    expression = {
        "node": "comparison",
        "op": "eq",
        "left": _column("s"),
        "right": {"node": "literal", "value_ref": 0},
    }
    document = _document([_predicate(expression)])
    payload = (pa.field("value_0", pa.string()), "a")
    output_schema = pa.schema([("s", pa.string())])
    data = pa.RecordBatch.from_pydict({"s": ["A", "a"]})

    binary = deserialize_filters(_batch(document, payload), output_schema=output_schema)
    assert binary.evaluate(data).to_pylist() == [False, True]

    session_metadata = {
        b"vgi_filter_encoding": b"vgi.filters.v2",
        b"vgi_filter_version": b"2",
        b"vgi_evaluation_context": b"vgi.duckdb.session.v1",
        b"vgi_time_zone": b"UTC",
        b"vgi_calendar": b"gregorian",
        b"vgi_default_collation": b"nocase",
        b"vgi_ieee_floating_point_ops": b"true",
        b"vgi_integer_division": b"false",
    }
    nocase = deserialize_filters(
        _batch(document, payload, metadata=session_metadata),
        output_schema=output_schema,
        evaluation_capabilities=(("vgi.duckdb.session.v1", None),),
    )
    assert nocase.evaluate(data).to_pylist() == [True, True]


def test_extension_call_capability_and_options_shape() -> None:
    """Structured function identity is negotiated and options belong on the call."""
    expression = {
        "node": "call",
        "function": {"namespace": "duckdb.spatial", "name": "intersects_extent", "version": 1},
        "arguments": [_column(), {"node": "literal", "value_ref": 0}],
    }
    batch = _batch(
        _document([_predicate(expression, mode="advisory")]),
        (pa.field("value_0", pa.binary()), b"geometry"),
    )
    with pytest.raises(FilterDeserializationError, match="was not advertised"):
        deserialize_filters(batch)
    capability = frozenset({("duckdb.spatial", "intersects_extent", 1)})
    filters = deserialize_filters(batch, extension_functions=capability)
    parsed = filters.predicates[0].expression
    assert isinstance(parsed, Call)
    assert parsed.options is None

    unknown = dict(expression)
    unknown["function"] = {"namespace": "example.filters", "name": "custom", "version": 1}
    with pytest.raises(FilterDeserializationError, match="unknown extension filter function"):
        deserialize_filters(
            _batch(
                _document([_predicate(unknown, mode="advisory")]),
                (pa.field("value_0", pa.binary()), b"geometry"),
            ),
            extension_functions=frozenset({("example.filters", "custom", 1)}),
        )

    with_options = dict(expression, options={"case_sensitive": True})
    with pytest.raises(FilterDeserializationError, match="does not accept options"):
        deserialize_filters(
            _batch(
                _document([_predicate(with_options, mode="advisory")]),
                (pa.field("value_0", pa.binary()), b"geometry"),
            ),
            extension_functions=capability,
        )

    misplaced = dict(expression)
    misplaced["function"] = {
        "namespace": "duckdb.spatial",
        "name": "intersects_extent",
        "version": 1,
        "options": {"case_sensitive": True},
    }
    with pytest.raises(FilterDeserializationError, match="unknown properties"):
        deserialize_filters(
            _batch(
                _document([_predicate(misplaced, mode="advisory")]),
                (pa.field("value_0", pa.binary()), b"geometry"),
            ),
            extension_functions=capability,
        )


def test_runtime_filter_is_root_only_advisory_and_known_unsupported_is_ignored() -> None:
    """Known unadvertised artifacts are ignored only as complete advisory roots."""
    runtime = {
        "node": "runtime_filter",
        "algorithm": {"namespace": "duckdb.runtime_filter", "name": "prefix_range", "version": 1},
        "input": _column(),
        "artifact_ref": 0,
        "null_handling": "reject",
    }
    artifact = (pa.field("artifact_0", pa.binary()), b"opaque")
    ignored = deserialize_filters(
        _batch(_document([_predicate(runtime, mode="advisory")]), artifact),
        output_schema=pa.schema([("n", pa.int64())]),
    )
    assert len(ignored.predicates) == 1
    assert not ignored
    assert ignored.evaluate(pa.RecordBatch.from_pydict({"n": [1, 2]})).to_pylist() == [True, True]

    nested = {"node": "not", "expression": runtime}
    with pytest.raises(FilterDeserializationError, match="predicate root"):
        deserialize_filters(
            _batch(_document([_predicate(nested, mode="advisory")]), artifact),
            output_schema=pa.schema([("n", pa.int64())]),
        )
    with pytest.raises(FilterDeserializationError, match="must be advisory"):
        deserialize_filters(
            _batch(_document([_predicate(runtime)]), artifact),
            output_schema=pa.schema([("n", pa.int64())]),
        )


def test_malformed_advisory_expression_is_rejected_during_decode() -> None:
    """Advisory mode does not turn bind/type errors into silent runtime ignores."""
    bad_boolean = {
        "node": "and",
        "children": [
            {"node": "literal", "value_ref": 0},
            {"node": "literal", "value_ref": 1},
        ],
    }
    with pytest.raises(FilterDeserializationError, match="children must resolve to BOOLEAN"):
        deserialize_filters(
            _batch(
                _document([_predicate(bad_boolean, mode="advisory")]),
                (pa.field("value_0", pa.int64()), 1),
                (pa.field("value_1", pa.bool_()), True),
            )
        )

    bad_call = {
        "node": "call",
        "function": "starts_with",
        "arguments": [{"node": "literal", "value_ref": 0}],
    }
    with pytest.raises(FilterDeserializationError, match="does not bind"):
        deserialize_filters(
            _batch(
                _document([_predicate(bad_call, mode="advisory")]),
                (pa.field("value_0", pa.string()), "abc"),
            ),
            output_schema=pa.schema([]),
        )


def test_evaluation_uses_authoritative_column_identity_when_batch_is_reordered() -> None:
    """Evaluation aliases the validated logical index instead of binding raw names."""
    output_schema = pa.schema([("n", pa.int64()), ("other", pa.int64())])
    filters = deserialize_filters(
        _batch(_document([_predicate(_comparison())]), (pa.field("value_0", pa.int64()), 2)),
        output_schema=output_schema,
    )
    reordered = pa.RecordBatch.from_pydict({"other": [100, 100], "n": [1, 3]})
    assert filters.evaluate(reordered).to_pylist() == [False, True]


def test_multiple_external_set_columns_keep_the_complete_key_batch() -> None:
    """Two independent external references do not overwrite each other's columns."""
    expression = {
        "node": "and",
        "children": [
            {
                "node": "in",
                "expression": _column("a", 0),
                "set": {"kind": "external", "batch_index": 0, "column_index": 0, "column_name": "a"},
                "negated": False,
            },
            {
                "node": "in",
                "expression": _column("b", 1),
                "set": {"kind": "external", "batch_index": 0, "column_index": 1, "column_name": "b"},
                "negated": False,
            },
        ],
    }
    keys = pa.RecordBatch.from_pydict({"a": [1, 2], "b": [10, 20]})
    filters = deserialize_filters(
        _batch(_document([_predicate(expression)])),
        output_schema=pa.schema([("a", pa.int64()), ("b", pa.int64())]),
        join_keys=[keys],
    )
    data = pa.RecordBatch.from_pydict({"a": [1, 1, 3], "b": [10, 30, 20]})
    assert filters.evaluate(data).to_pylist() == [True, False, False]


def test_unknown_arrow_extension_metadata_is_rejected() -> None:
    """Unknown extension semantics cannot silently degrade to their storage type."""
    extension_field = pa.field(
        "value_0",
        pa.binary(),
        metadata={b"ARROW:extension:name": b"example.unknown"},
    )
    with pytest.raises(FilterDeserializationError, match="unknown Arrow extension type"):
        deserialize_filters(
            _batch(_document([_predicate(_comparison())]), (extension_field, b"opaque")),
            output_schema=pa.schema([("n", pa.int64())]),
        )
