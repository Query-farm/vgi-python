# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103

"""Macro declaration fidelity and atomic create/discovery reference behavior."""

from dataclasses import replace

import duckdb
import pyarrow as pa
import pytest

from vgi.argument_spec import macro_arguments_schema
from vgi.catalog.evaluation_context import QueryEvaluationContext
from vgi.catalog.layout import SqlExpression
from vgi.catalog.macros import (
    MACRO_DEFAULT_PROFILE,
    MACRO_PROFILE,
    MacroOverload,
    MacroParameter,
    canonicalize_macro_defaults,
    legacy_macro_fields,
    normalize_macro_overloads,
    validate_macro_overloads,
)
from vgi.typed_values import TypedValueBatch


def literal(name="x", value=None, dtype=None, *, metadata=None):
    dtype = dtype if dtype is not None else pa.int64()
    schema = pa.schema([pa.field(name, dtype, metadata={b"logical": b"keep"})], metadata=metadata)
    return TypedValueBatch(pa.RecordBatch.from_arrays([pa.array([value], type=dtype)], schema=schema))


def overload(parameters=None, **changes):
    return MacroOverload(
        evaluation_context=changes.pop("evaluation_context", QueryEvaluationContext()),
        parameters=parameters if parameters is not None else [MacroParameter(name="x")],
        definition=changes.pop("definition", "x + 1"),
        profile=MACRO_PROFILE,
        schema_path=changes.pop("schema_path", ["a.b", "nested"]),
        macro_type=changes.pop("macro_type", "scalar"),
        **changes,
    )


def fields(**changes):
    return dict(macro_type="scalar", schema_path=["a.b", "nested"], parameters=[], definition="", **changes)


def test_parameter_preserves_declared_type_and_recursive_metadata_without_inference():
    schema = pa.schema(
        [pa.field("x", pa.struct([pa.field("a", pa.int64(), metadata={b"nested": b"identity"})]))],
        metadata={b"schema": b"identity"},
    )
    parameter = MacroParameter(name="x", declared_type=schema, default_value=literal(value=None))
    restored = MacroParameter.deserialize_from_bytes(parameter.serialize_to_bytes())
    assert restored.declared_type.equals(schema, check_metadata=True)
    assert restored.default_value.schema.equals(parameter.default_value.schema, check_metadata=True)
    assert restored.has_default
    assert restored.default_value.batch.column(0)[0].as_py() is None
    assert MacroParameter(name="x", default_value=literal(value=7)).declared_type is None
    assert not MacroParameter(name="x").has_default


def test_expression_default_and_body_keep_raw_nested_owner_and_order():
    expression = SqlExpression(
        evaluation_context=QueryEvaluationContext(),
        sql="40 + 2",
        profile=MACRO_DEFAULT_PROFILE,
        schema_path=["a.b", "nested"],
    )
    original = overload([MacroParameter(name="required"), MacroParameter(name="x", default_expression=expression)])
    copied = MacroOverload.deserialize_from_bytes(original.serialize_to_bytes())
    assert [parameter.name for parameter in copied.parameters] == ["required", "x"]
    assert copied.parameters[1].default_expression == expression
    assert copied.schema_path == ["a.b", "nested"]
    assert copied.parameters[1].default_value is None


@pytest.mark.parametrize(
    "changes",
    [
        {"name": ""},
        {"name": "a\0b"},
        {"declared_type": pa.schema([])},
        {"declared_type": pa.schema([pa.field("other", pa.int32())])},
        {"declared_type": pa.schema([pa.field("x", pa.int32(), nullable=False)])},
        {"declared_type": pa.schema([pa.field("x", pa.null())])},
        {"default_value": literal(name="other")},
        {"documentation": "bad\0doc"},
    ],
)
def test_invalid_parameter_contracts_reject(changes):
    with pytest.raises((ValueError, TypeError)):
        replace(MacroParameter(name="x"), **changes)


def test_default_value_and_expression_cannot_coexist():
    expression = SqlExpression(
        evaluation_context=QueryEvaluationContext(), sql="NULL", profile=MACRO_DEFAULT_PROFILE, schema_path=["main"]
    )
    with pytest.raises(ValueError, match="mutually exclusive"):
        MacroParameter(name="x", default_value=literal(), default_expression=expression)


def test_unknown_default_profile_rejects_before_provider_evaluation():
    with pytest.raises(ValueError, match="unsupported"):
        MacroParameter(
            name="x",
            default_expression=SqlExpression(
                evaluation_context=QueryEvaluationContext(),
                sql="2 + 2",
                profile="unknown.default.v1",
                schema_path=["main"],
            ),
        )


@pytest.mark.parametrize(
    "changes",
    [
        {"macro_type": "aggregate"},
        {"profile": "unknown.macro.v1"},
        {"definition": " "},
        {"definition": "x\0"},
        {"schema_path": []},
        {"schema_path": ["bad\0schema"]},
        {"parameters": [MacroParameter(name="x"), MacroParameter(name="X")]},
        {"parameters": [MacroParameter(name="x", default_value=literal()), MacroParameter(name="y")]},
    ],
)
def test_invalid_overload_contracts_reject(changes):
    with pytest.raises((ValueError, TypeError)):
        replace(overload(), **changes)


def test_default_expression_cannot_rebind_to_another_owner():
    with pytest.raises(ValueError, match="owner schema"):
        overload(
            [
                MacroParameter(
                    name="x",
                    default_expression=SqlExpression(
                        evaluation_context=QueryEvaluationContext(),
                        sql="1",
                        profile=MACRO_DEFAULT_PROFILE,
                        schema_path=["main"],
                    ),
                )
            ]
        )


@pytest.mark.parametrize(
    "changes",
    [
        {"parameters": ["x"]},
        {"definition": "x + 1"},
        {"parameter_default_values": literal().batch},
        {"arguments_schema": pa.schema([])},
    ],
)
def test_dual_legacy_and_new_forms_reject_even_if_first_overload_matches(changes):
    args = fields(overloads=[overload()])
    args.update(changes)
    with pytest.raises(ValueError, match="conflicts"):
        validate_macro_overloads(**args)


def test_collection_is_complete_nonempty_homogeneous_and_owner_relative():
    for collection in [[], [overload(macro_type="table")], [overload(schema_path=["other"])]]:
        with pytest.raises(ValueError):
            validate_macro_overloads(**fields(overloads=collection))
    first = overload()
    second = overload([MacroParameter(name="x"), MacroParameter(name="y")], definition="x + y")
    copied = normalize_macro_overloads(**fields(overloads=[second, first]))
    assert [len(item.parameters) for item in copied] == [2, 1]
    first.parameters.clear()
    assert len(copied[1].parameters) == 1


def test_legacy_upgrade_preserves_typed_null_defaults_without_declaring_their_types():
    defaults = literal("optional", None, metadata={b"source": b"legacy"}).batch
    args = dict(
        macro_type="scalar",
        schema_path=["a.b", "nested"],
        parameters=["required", "optional"],
        definition="coalesce(optional, required)",
        parameter_default_values=defaults,
        arguments_schema=macro_arguments_schema(["required", "optional"], defaults, {"optional": "optional doc"}),
    )
    upgraded = normalize_macro_overloads(**args, evaluation_context=QueryEvaluationContext())
    assert all(parameter.declared_type is None for parameter in upgraded[0].parameters)
    assert upgraded[0].parameters[1].documentation == "optional doc"
    restored = legacy_macro_fields(upgraded, evaluation_context=QueryEvaluationContext())
    assert restored["parameter_default_values"].equals(defaults, check_metadata=True)
    assert restored["arguments_schema"].equals(args["arguments_schema"], check_metadata=True)
    assert restored["parameters"] == args["parameters"]


def test_legacy_documentation_schema_cannot_sneak_in_a_declared_type_or_drop_metadata():
    args = dict(macro_type="scalar", schema_path=["main"], parameters=["x"], definition="x")
    for schema in [
        pa.schema([pa.field("x", pa.int64())]),
        pa.schema([pa.field("x", pa.null())], metadata={b"unknown": b"preserve"}),
    ]:
        with pytest.raises(ValueError, match="unsupported or inconsistent"):
            normalize_macro_overloads(**args, arguments_schema=schema, evaluation_context=QueryEvaluationContext())


@pytest.mark.parametrize("kind", ["family", "declared", "expression", "empty_doc"])
def test_legacy_downgrade_fails_closed_instead_of_losing_information(kind):
    family = [overload()]
    if kind == "family":
        family.append(overload([], definition="42"))
    elif kind == "declared":
        family = [overload([MacroParameter(name="x", declared_type=pa.schema([pa.field("x", pa.int64())]))])]
    elif kind == "expression":
        family = [
            overload(
                [
                    MacroParameter(
                        name="x",
                        default_expression=SqlExpression(
                            evaluation_context=QueryEvaluationContext(),
                            sql="42",
                            profile=MACRO_DEFAULT_PROFILE,
                            schema_path=["a.b", "nested"],
                        ),
                    )
                ]
            )
        ]
    else:
        family = [overload([MacroParameter(name="x", documentation="")])]
    with pytest.raises(ValueError, match="legacy macros"):
        legacy_macro_fields(family)


def test_macro_ipc_requires_exactly_one_complete_record():
    valid = overload().serialize_to_bytes()
    with pytest.raises(ValueError, match="trailing"):
        MacroOverload.deserialize_from_bytes(valid + b"extra")
    with pa.ipc.open_stream(valid) as reader:
        batch = reader.read_next_batch()
    with pytest.raises(ValueError, match="complete schema"):
        MacroOverload.deserialize_from_batch(batch.select(["parameters"]))
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
        writer.write_batch(batch)
    with pytest.raises(ValueError, match="exactly one batch"):
        MacroOverload.deserialize_from_bytes(sink.getvalue().to_pybytes())


def test_serialization_revalidates_mutated_parameter_collection():
    value = overload()
    value.parameters.append(MacroParameter(name="X"))
    with pytest.raises(ValueError, match="unique"):
        value.serialize_to_bytes()


@pytest.mark.parametrize("kind", ["info", "create"])
def test_parent_serialization_revalidates_mutated_nested_family(kind):
    from vgi.catalog import MacroInfo, MacroType, OnConflict
    from vgi.protocol import MacroCreateRequest

    member = overload()
    common = dict(
        name="m",
        schema_path=member.schema_path,
        macro_type=MacroType.SCALAR,
        overloads=[member],
    )
    parent = (
        MacroInfo(**common, comment=None, tags={})
        if kind == "info"
        else MacroCreateRequest(**common, attach_opaque_data=b"attach", on_conflict=OnConflict.ERROR)
    )
    parent.serialize_to_bytes()
    member.parameters.append(MacroParameter(name="X"))
    with pytest.raises(ValueError, match="unique"):
        parent.serialize_to_bytes()


def test_create_discovery_copy_resolves_default_once_with_duckdb_15():
    """A finite reference binds a known constant and stores its resolved value."""
    owner = ["main"]
    expression = SqlExpression(
        evaluation_context=QueryEvaluationContext(), sql="40 + 2", profile=MACRO_DEFAULT_PROFILE, schema_path=owner
    )
    family = [
        overload(
            [MacroParameter(name="x", default_expression=expression)],
            schema_path=owner,
            definition="x + 1",
        )
    ]
    calls = []
    with duckdb.connect() as connection:

        def resolve(item, parameter):
            assert item.schema_path == owner
            assert parameter.default_expression.sql == "40 + 2"
            calls.append(parameter.name)
            # This finite reference evaluates only its one whitelisted constant;
            # a general provider must use the engine's constant-expression binder.
            result = connection.sql('SELECT 40 + 2 AS "x"').arrow().read_next_batch()
            return TypedValueBatch(result)

        created = canonicalize_macro_defaults(family, resolve)
        stored = [item.serialize_to_bytes() for item in created]
        discovered = [MacroOverload.deserialize_from_bytes(data) for data in stored]
        copied = normalize_macro_overloads(**{**fields(overloads=discovered), "schema_path": owner})
        assert copied[0].parameters[0].default_expression is None
        assert copied[0].parameters[0].default_value.batch.column(0)[0].as_py() == 42
        assert calls == ["x"]
        assert family[0].parameters[0].default_expression is not None
        assert copied[0].serialize_to_bytes() == stored[0]
        connection.execute("CREATE MACRO original_macro(x := 40 + 2) AS x + 1")
        connection.execute("CREATE MACRO copied_macro(x := 42) AS x + 1")
        assert connection.sql("SELECT original_macro(), copied_macro()").fetchone() == (43, 43)


def test_default_resolution_failure_does_not_publish_partial_replacement():
    family = [overload([MacroParameter(name="x", default_value=literal(value=1))])]
    existing = [item.serialize_to_bytes() for item in family]

    def reject(item, parameter):
        raise ValueError("default cannot be implicitly cast")

    with pytest.raises(ValueError, match="implicitly cast"):
        canonicalize_macro_defaults(family, reject)
    assert [item.serialize_to_bytes() for item in family] == existing


def test_duckdb_15_owns_typed_overload_resolution_and_duplicate_signature_errors():
    with duckdb.connect() as connection:
        connection.execute("CREATE MACRO typed_macro(x INTEGER) AS x + 1, (x VARCHAR) AS x || '!' ")
        assert connection.sql("SELECT typed_macro(2::INTEGER), typed_macro('v'::VARCHAR)").fetchone() == (3, "v!")
        with pytest.raises(duckdb.Error, match="Ambiguity|same parameters"):
            connection.execute("CREATE MACRO ambiguous_macro(x INTEGER) AS x, (y INTEGER) AS y + 1")
