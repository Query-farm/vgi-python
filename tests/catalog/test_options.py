# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103

"""Typed option changes preserve readback and reject the entire invalid change."""

from decimal import Decimal
from typing import Any

import pyarrow as pa
import pytest

from vgi.catalog.options import OptionChanges, OptionState
from vgi.typed_values import TypedValueBatch


def _defaults() -> TypedValueBatch:
    schema = pa.schema(
        [
            pa.field("label", pa.string(), metadata={b"option": b"display label"}),
            pa.field("limit", pa.int32(), nullable=False),
            pa.field("cost", pa.decimal128(12, 3)),
            pa.field(
                "nested", pa.struct([pa.field("enabled", pa.bool_(), nullable=False, metadata={b"child": b"retained"})])
            ),
        ],
        metadata={b"provider": b"example-options/v1"},
    )
    return TypedValueBatch(
        pa.RecordBatch.from_pylist(
            [{"label": "default", "limit": 10, "cost": Decimal("1.125"), "nested": {"enabled": True}}], schema=schema
        )
    )


def _sets(**values: Any) -> TypedValueBatch:
    defaults = _defaults()
    schema = pa.schema([defaults.schema.field(name) for name in values], metadata=defaults.schema.metadata)
    return TypedValueBatch(pa.RecordBatch.from_pylist([values], schema=schema))


def test_set_null_omission_and_reset_have_distinct_readback() -> None:
    defaults = _defaults()
    original = OptionState(values=defaults)
    changed = OptionChanges(set_values=_sets(label=None, limit=20)).apply_to(original, defaults=defaults)
    assert changed.values.batch.to_pylist()[0] == {
        "label": None,
        "limit": 20,
        "cost": Decimal("1.125"),
        "nested": {"enabled": True},
    }
    assert changed.explicit_names == ["label", "limit"]
    assert original.values.batch.column("limit")[0].as_py() == 10
    assert original.explicit_names == []
    # An empty change leaves both effective values and the overrides intact.
    unchanged = OptionChanges().apply_to(changed, defaults=defaults)
    assert unchanged.values.batch.equals(changed.values.batch, check_metadata=True)
    assert unchanged.explicit_names == changed.explicit_names
    reset = OptionChanges(reset_names=["label"]).apply_to(changed, defaults=defaults)
    assert reset.values.batch.column("label")[0].as_py() == "default"
    assert reset.values.batch.column("limit")[0].as_py() == 20
    assert reset.explicit_names == ["limit"]


def test_setting_default_value_still_creates_an_override() -> None:
    defaults = _defaults()
    result = OptionChanges(set_values=_sets(limit=10)).apply_to(OptionState(values=defaults), defaults=defaults)
    assert result.values.batch.equals(defaults.batch, check_metadata=True)
    assert result.explicit_names == ["limit"]
    assert OptionChanges(reset_names=["limit"]).apply_to(result, defaults=defaults).explicit_names == []


def test_reset_uses_the_current_provider_default() -> None:
    original = OptionState(values=_defaults())
    current_defaults = OptionChanges(set_values=_sets(limit=99)).apply_to(original, defaults=_defaults()).values
    result = OptionChanges(reset_names=["limit"]).apply_to(original, defaults=current_defaults)
    assert result.values.batch.column("limit")[0].as_py() == 99
    assert result.explicit_names == []


def test_wire_round_trip_preserves_nested_metadata_and_typed_null() -> None:
    changes = OptionChanges(set_values=_sets(cost=None, nested={"enabled": False}), reset_names=["limit"])
    restored = OptionChanges.deserialize_from_bytes(changes.serialize_to_bytes())
    assert restored.set_values is not None and changes.set_values is not None
    assert restored.set_values.batch.equals(changes.set_values.batch, check_metadata=True)
    assert restored.reset_names == ["limit"]
    result = restored.apply_to(OptionState(values=_defaults()), defaults=_defaults())
    readback = OptionState.deserialize_from_bytes(result.serialize_to_bytes())
    assert readback.values.batch.equals(result.values.batch, check_metadata=True)
    assert readback.explicit_names == ["cost", "nested"]


@pytest.mark.parametrize("names", [["limit", "limit"], [""], ["bad\x00name"]])
def test_invalid_names_reject(names: list[str]) -> None:
    with pytest.raises(ValueError):
        OptionChanges(reset_names=names)
    with pytest.raises(ValueError):
        OptionState(values=_defaults(), explicit_names=names)


def test_unknown_explicit_name_rejects() -> None:
    with pytest.raises(ValueError, match="absent from values"):
        OptionState(values=_defaults(), explicit_names=["missing"])


def test_set_reset_overlap_rejects() -> None:
    with pytest.raises(ValueError, match="both SET and RESET"):
        OptionChanges(set_values=_sets(label=None), reset_names=["label"])


@pytest.mark.parametrize(
    "changes",
    [
        OptionChanges(reset_names=["missing"]),
        OptionChanges(set_values=TypedValueBatch(pa.record_batch({"missing": [1]}))),
        OptionChanges(set_values=TypedValueBatch(pa.record_batch({"limit": [20]}))),
        OptionChanges(set_values=_sets(limit=20), reset_names=["missing"]),
    ],
)
def test_invalid_changes_leave_original_state_unchanged(changes: OptionChanges) -> None:
    defaults = _defaults()
    current = OptionState(values=defaults)
    before = current.serialize_to_bytes()
    with pytest.raises(ValueError, match="unknown option|expected schema"):
        changes.apply_to(current, defaults=defaults)
    assert current.serialize_to_bytes() == before


def test_read_only_options_reject_before_candidate_application() -> None:
    current = OptionState(values=_defaults())
    changes = OptionChanges(set_values=_sets(label="would change", limit=20))
    with pytest.raises(ValueError, match="not mutable.*limit"):
        changes.apply_to(current, defaults=_defaults(), mutable_names=["label"])
    assert current.values.batch.column("label")[0].as_py() == "default"


@pytest.mark.parametrize("part", ["field", "schema", "nested", "nullability"])
def test_semantic_schema_changes_cannot_be_silently_cast(part: str) -> None:
    defaults = _defaults()
    field = defaults.schema.field("nested")
    metadata = defaults.schema.metadata
    if part == "field":
        field = field.with_metadata({b"encoding": b"unknown"})
    elif part == "schema":
        metadata = {b"provider": b"different"}
    elif part == "nested":
        field = field.with_type(pa.struct([field.type[0].with_metadata({b"child": b"changed"})]))
    else:
        field = field.with_nullable(False)
    values = TypedValueBatch(
        pa.RecordBatch.from_pylist([{"nested": {"enabled": False}}], schema=pa.schema([field], metadata=metadata))
    )
    with pytest.raises(ValueError, match="expected schema"):
        OptionChanges(set_values=values).apply_to(OptionState(values=defaults), defaults=defaults)


def test_empty_option_provider_retains_one_row_and_metadata() -> None:
    defaults = TypedValueBatch.empty(metadata={b"provider": b"no-options"})
    current = OptionState(values=defaults)
    result = OptionChanges().apply_to(current, defaults=defaults)
    assert result.values.batch.equals(defaults.batch, check_metadata=True)


def test_mutated_lists_are_revalidated_before_application() -> None:
    current = OptionState(values=_defaults())
    changes = OptionChanges(set_values=_sets(limit=20))
    changes.reset_names.append("limit")
    with pytest.raises(ValueError, match="both SET and RESET"):
        changes.apply_to(current, defaults=_defaults())


def test_nested_option_payload_rejects_extra_batches() -> None:
    values = _sets(limit=20)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, values.schema) as writer:
        writer.write_batch(values.batch)
        writer.write_batch(values.batch)
    envelope = pa.RecordBatch.from_pylist(
        [{"set_values": sink.getvalue().to_pybytes(), "reset_names": []}], schema=OptionChanges.ARROW_SCHEMA
    )
    with pytest.raises(ValueError, match="exactly one batch"):
        OptionChanges.deserialize_from_bytes(TypedValueBatch(envelope).serialize_to_bytes())
